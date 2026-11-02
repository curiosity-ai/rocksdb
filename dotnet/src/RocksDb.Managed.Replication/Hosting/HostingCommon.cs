using System.Runtime.CompilerServices;
using System.Threading.Channels;
using RocksDb.Managed.Replication.Switchover;

namespace RocksDb.Managed.Replication.Hosting;

/// <summary>
/// One message of a WAL replication stream between hosts: a merged run of write batches (<see cref="WalChunk"/>) tagged
/// with the sending node and its term/epoch, or a terminal "resync required" marker sent when the source no longer has
/// the WAL the follower asked for (it was purged) - the follower must then rebuild from a snapshot.
/// <para><see cref="Data"/> may refer to the producer's buffer and is only valid until the producing enumeration moves
/// on; transports that queue messages copy it (<see cref="Detach"/>).</para>
/// </summary>
public readonly struct WalStreamMessage
{
    public WalStreamMessage(ulong firstSequence, int entryCount, ReadOnlyMemory<byte> data, string sourceId, long sourceTerm, long entryTerm)
    {
        FirstSequence = firstSequence;
        EntryCount = entryCount;
        Data = data;
        SourceId = sourceId;
        SourceTerm = sourceTerm;
        EntryTerm = entryTerm;
        ResyncRequired = false;
        Reason = null;
    }

    private WalStreamMessage(string sourceId, long sourceTerm, string reason)
    {
        SourceId = sourceId;
        SourceTerm = sourceTerm;
        ResyncRequired = true;
        Reason = reason;
    }

    public static WalStreamMessage Resync(string sourceId, long sourceTerm, string reason) => new(sourceId, sourceTerm, reason);

    public ulong FirstSequence { get; }
    public int EntryCount { get; }
    /// <summary>The serialized (merged) write batch.</summary>
    public ReadOnlyMemory<byte> Data { get; }
    /// <summary>The node that streamed it (leader / primary).</summary>
    public string SourceId { get; }
    /// <summary>The source's term (Raft) or epoch (switchover) when it streamed the chunk.</summary>
    public long SourceTerm { get; }
    /// <summary>The term of <see cref="FirstSequence"/> in the source's term log (0 if none).</summary>
    public long EntryTerm { get; }
    public bool ResyncRequired { get; }
    public string? Reason { get; }

    public WalStreamMessage Detach() =>
        ResyncRequired ? this : new WalStreamMessage(FirstSequence, EntryCount, Data.ToArray(), SourceId, SourceTerm, EntryTerm);
}

/// <summary>Source-side producer of <see cref="WalStreamMessage"/>s over a <see cref="WalTailer"/>.</summary>
public static class WalStreamSender
{
    /// <summary>
    /// Streams the WAL of <paramref name="db"/> from <paramref name="startSequence"/> until <paramref name="cancellationToken"/>
    /// is cancelled (e.g. the node stopped leading) or the database closes. <paramref name="tag"/> is asked before every
    /// chunk for the (source id, term) to stamp on it, and ends the stream by returning false.
    /// <paramref name="entryTerm"/> maps a sequence number to its term. When the WAL for <paramref name="startSequence"/>
    /// is gone, a single <see cref="WalStreamMessage.Resync"/> message ends the stream.
    /// </summary>
    public static async IAsyncEnumerable<WalStreamMessage> StreamAsync(RocksDatabase db, ulong startSequence, StreamTagProvider tag,
        Func<ulong, long> entryTerm, int maxChunkBytes, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(db);
        ArgumentNullException.ThrowIfNull(tag);
        ArgumentNullException.ThrowIfNull(entryTerm);
        using WalTailer tailer = new ReplicationSource(db).TailWal(startSequence, maxChunkBytes);
        while (!cancellationToken.IsCancellationRequested)
        {
            if (!tag(out string sourceId, out long term)) yield break;
            WalChunk chunk;
            bool has;
            string? resync = null;
            try
            {
                has = tailer.TryReadChunk(out chunk);
            }
            catch (RocksDbException e) when (e.Code is StatusCode.NotFound or StatusCode.InvalidArgument)
            {
                has = false;
                chunk = default;
                resync = e.Message;
            }
            if (resync != null)
            {
                yield return WalStreamMessage.Resync(sourceId, term, resync);
                yield break;
            }
            if (has)
            {
                yield return new WalStreamMessage(chunk.FirstSequenceNumber, chunk.EntryCount, chunk.Memory, sourceId, term, entryTerm(chunk.FirstSequenceNumber));
                continue;
            }
            bool open;
            try
            {
                open = await tailer.WaitForUpdatesAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                yield break;
            }
            if (!open) yield break;
        }
    }
}

/// <summary>Returns the (source id, term) to stamp on the next chunk, or false to end the stream.</summary>
public delegate bool StreamTagProvider(out string sourceId, out long term);

/// <summary>
/// Coalescing progress reporter for a follower: <see cref="Report"/> is cheap and never blocks the ingest loop; one
/// background loop sends only the newest value through the callback (a lost report is superseded by the next).
/// </summary>
public sealed class ProgressReporter : IAsyncDisposable
{
    private readonly Channel<ulong> _channel = Channel.CreateBounded<ulong>(new BoundedChannelOptions(1)
    {
        FullMode = BoundedChannelFullMode.DropOldest,
        SingleReader = true,
    });
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _loop;

    public ProgressReporter(Func<ulong, CancellationToken, Task> send)
    {
        ArgumentNullException.ThrowIfNull(send);
        _loop = Task.Run(async () =>
        {
            try
            {
                await foreach (ulong seq in _channel.Reader.ReadAllAsync(_cts.Token).ConfigureAwait(false))
                {
                    try
                    {
                        await send(seq, _cts.Token).ConfigureAwait(false);
                    }
                    catch (Exception e) when (e is not OperationCanceledException)
                    {
                        // Best effort: the next report carries newer progress.
                    }
                }
            }
            catch (OperationCanceledException)
            {
            }
        });
    }

    public void Report(ulong sequence) => _channel.Writer.TryWrite(sequence);

    public async ValueTask DisposeAsync()
    {
        _channel.Writer.TryComplete();
        await _cts.CancelAsync().ConfigureAwait(false);
        await _loop.ConfigureAwait(false);
        _cts.Dispose();
    }
}

/// <summary>Order-sensitive digest of a whole database: what converged replicas must agree on.</summary>
/// <param name="KeyCount">Number of live keys.</param>
/// <param name="Hash">FNV-1a over every key and value, in key order.</param>
/// <param name="LatestSequence">The database's latest sequence number.</param>
public readonly record struct DatabaseDigest(long KeyCount, ulong Hash, ulong LatestSequence)
{
    /// <summary>Scans the default column family (port of the ChecksumAsync RPC of rocksdb-sharp's samples).</summary>
    public static DatabaseDigest Compute(RocksDatabase db)
    {
        ArgumentNullException.ThrowIfNull(db);
        const ulong FnvOffset = 14695981039346656037UL;
        const ulong FnvPrime = 1099511628211UL;
        ulong hash = FnvOffset;
        long count = 0;
        ulong latest = db.GetLatestSequenceNumber();
        using (Iterator it = db.NewIterator())
        {
            for (it.SeekToFirst(); it.Valid; it.Next())
            {
                foreach (byte b in it.Key)
                {
                    hash ^= b;
                    hash *= FnvPrime;
                }
                hash ^= 0xFF;
                hash *= FnvPrime;
                foreach (byte b in it.Value)
                {
                    hash ^= b;
                    hash *= FnvPrime;
                }
                hash ^= 0xFE;
                hash *= FnvPrime;
                count++;
            }
        }
        return new DatabaseDigest(count, hash, latest);
    }
}

/// <summary>
/// Guards a host's database instance against being disposed while RPC handlers use it (a resync swaps it). Wraps a
/// <see cref="PrimaryWriteGate"/>: handlers <see cref="TryEnter"/>/<see cref="Exit"/>, a resync <see cref="DrainAsync"/>s
/// (no new users, waits for current ones) and <see cref="Reopen"/>s when the new instance is in place. Replaces the
/// fixed "give in-flight handlers a moment" delays of rocksdb-sharp's sample hosts.
/// </summary>
public sealed class DatabaseUsageGate
{
    private readonly PrimaryWriteGate _gate = new(open: true);

    public bool TryEnter()
    {
        ValueTask<bool> t = _gate.TryEnterAsync(TimeSpan.Zero, CancellationToken.None);
        return t.IsCompletedSuccessfully ? t.Result : t.AsTask().GetAwaiter().GetResult();
    }

    public void Exit() => _gate.Exit();

    public async Task DrainAsync(CancellationToken cancellationToken)
    {
        await _gate.HoldAsync(cancellationToken).ConfigureAwait(false);
        _gate.Close();
    }

    public void Reopen() => _gate.OpenGate();
}
