using System.Runtime.CompilerServices;
using RocksDb.Managed.Replication.Switchover;

namespace RocksDb.Managed.Replication.Hosting;

/// <summary>
/// One message of a WAL replication stream between hosts: a merged run of write batches (<see cref="WalChunk"/>) tagged
/// with the sending node and its term/epoch, or a terminal "resync required" marker sent when the source no longer has
/// the WAL the follower asked for (it was purged) - the follower must then rebuild from a snapshot.
/// <para><see cref="Data"/> may refer to the producer's buffer and is only valid until the producing enumeration moves
/// on; transports that queue messages copy it (<see cref="Detach"/>).</para>
/// <para>
/// A chunk larger than the stream's chunk size (always one single write batch) is sent as consecutive fragments
/// (<see cref="IsFragment"/>), so no message carries more than the chunk size: every fragment
/// carries the batch's <see cref="FirstSequence"/>, <see cref="SourceId"/>, <see cref="SourceTerm"/>,
/// <see cref="EntryTerm"/> and <see cref="TotalLength"/>, its own <see cref="FragmentOffset"/> and its slice of the
/// serialized batch in <see cref="Data"/>; <see cref="EntryCount"/> is 0 on every fragment but the last
/// (<see cref="IsLastFragment"/>). The follower puts them together with a <see cref="WalStreamAssembler"/> and applies
/// the batch only once it is complete. The split exists on the wire only: the WAL, the sequence numbers and the
/// atomicity of the batch are the same as for one message.
/// </para>
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
        SourceFault = false;
        Reason = null;
    }

    private WalStreamMessage(ulong firstSequence, int entryCount, ReadOnlyMemory<byte> data, string sourceId, long sourceTerm, long entryTerm,
        int fragmentOffset, int totalLength)
        : this(firstSequence, entryCount, data, sourceId, sourceTerm, entryTerm)
    {
        FragmentOffset = fragmentOffset;
        TotalLength = totalLength;
    }

    /// <summary>
    /// One fragment of a write batch of <paramref name="totalLength"/> serialized bytes that starts at
    /// <paramref name="firstSequence"/>: <paramref name="data"/> is the batch's bytes from <paramref name="fragmentOffset"/>.
    /// <paramref name="entryCount"/> is the batch's entry count on the last fragment and 0 on the others. A
    /// <paramref name="totalLength"/> of 0 makes an ordinary (unfragmented) message.
    /// </summary>
    public static WalStreamMessage Fragment(ulong firstSequence, int fragmentOffset, int totalLength, int entryCount, ReadOnlyMemory<byte> data,
        string sourceId, long sourceTerm, long entryTerm) =>
        new(firstSequence, entryCount, data, sourceId, sourceTerm, entryTerm, fragmentOffset, totalLength);

    private WalStreamMessage(string sourceId, long sourceTerm, string reason, bool sourceFault)
    {
        SourceId = sourceId;
        SourceTerm = sourceTerm;
        ResyncRequired = true;
        SourceFault = sourceFault;
        Reason = reason;
    }

    public static WalStreamMessage Resync(string sourceId, long sourceTerm, string reason) => new(sourceId, sourceTerm, reason, false);

    /// <summary>
    /// A resync request sent because the source failed to read its own WAL (<see cref="SourceFault"/>), not because of
    /// the follower's position.
    /// </summary>
    public static WalStreamMessage ResyncAfterSourceFault(string sourceId, long sourceTerm, string reason) => new(sourceId, sourceTerm, reason, true);

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
    /// <summary>
    /// With <see cref="ResyncRequired"/>: the source could not read its own WAL. A snapshot helps the follower only if
    /// the stream it opens afterwards gets past the fault, so a follower that has applied nothing since its last rebuild
    /// retries the stream instead of rebuilding again (<see cref="FollowerResyncPolicy"/>).
    /// </summary>
    public bool SourceFault { get; }
    public string? Reason { get; }

    /// <summary>For a fragment: where <see cref="Data"/> starts in the serialized batch. 0 otherwise.</summary>
    public int FragmentOffset { get; }
    /// <summary>For a fragment: the length of the whole serialized batch. 0 for an ordinary message.</summary>
    public int TotalLength { get; }
    /// <summary>Whether this message carries a part of a write batch (<see cref="Fragment"/>) instead of a whole chunk.</summary>
    public bool IsFragment => TotalLength != 0;
    /// <summary>Whether this fragment ends its batch.</summary>
    public bool IsLastFragment => TotalLength != 0 && (long)FragmentOffset + Data.Length == TotalLength;

    public WalStreamMessage Detach() =>
        ResyncRequired ? this : new WalStreamMessage(FirstSequence, EntryCount, Data.ToArray(), SourceId, SourceTerm, EntryTerm, FragmentOffset, TotalLength);
}

/// <summary>
/// Follower-side rule that keeps a resync requested for a fault of the source's own WAL
/// (<see cref="WalStreamMessage.SourceFault"/>) from repeating without end: a rebuild is only worth it if this node has
/// applied something since its last one. Otherwise the snapshot it just restored did not get it past the fault, the next
/// one would not either, and the node retries the stream until the source can read its WAL again.
/// </summary>
public sealed class FollowerResyncPolicy
{
    private const long None = -1;
    // The log end right after the last rebuild, or None.
    private long _rebuiltAtSeq = None;

    /// <summary>Records that a rebuild just completed with the log ending at <paramref name="latestSeq"/>.</summary>
    public void OnRebuilt(ulong latestSeq) => Volatile.Write(ref _rebuiltAtSeq, unchecked((long)latestSeq));

    /// <summary>Whether a resync request <paramref name="message"/> should rebuild a node whose log ends at <paramref name="latestSeq"/>.</summary>
    public bool ShouldRebuild(in WalStreamMessage message, ulong latestSeq) =>
        !message.SourceFault || Volatile.Read(ref _rebuiltAtSeq) != unchecked((long)latestSeq);

    // The log end at the latest stream that broke the fragment protocol, or None.
    private long _streamFaultAtSeq = None;

    /// <summary>
    /// A stream broke the fragment protocol (<see cref="WalAssemblyStatus.Invalid"/>) while this node's log ended at
    /// <paramref name="latestSeq"/>. False the first time at that position: the node drops the stream and attaches
    /// again there (a fault of the connection does not repeat). True when the previous such fault happened at the same
    /// position - nothing was applied in between, so the source sends that batch wrongly every time and attaching
    /// again would repeat it without end: the node rebuilds from a snapshot, which contains the batch as files, and
    /// follows from behind it.
    /// </summary>
    public bool ShouldRebuildAfterStreamFault(ulong latestSeq)
    {
        long at = unchecked((long)latestSeq);
        return Interlocked.Exchange(ref _streamFaultAtSeq, at) == at;
    }
}

/// <summary>Source-side producer of <see cref="WalStreamMessage"/>s over a <see cref="WalTailer"/>.</summary>
public static class WalStreamSender
{
    /// <summary>
    /// Streams the WAL of <paramref name="db"/> from <paramref name="startSequence"/> until <paramref name="cancellationToken"/>
    /// is cancelled (e.g. the node stopped leading) or the database closes. <paramref name="tag"/> is asked before every
    /// chunk for the (source id, term) to stamp on it, and ends the stream by returning false.
    /// <paramref name="entryTerm"/> maps a sequence number to its term. When the WAL for <paramref name="startSequence"/>
    /// is gone, or <paramref name="startSequence"/> is past the end of this database's log (the follower holds entries
    /// this database never wrote), a single <see cref="WalStreamMessage.Resync"/> message ends the stream.
    /// <para>
    /// No message carries more than <paramref name="maxChunkBytes"/> payload bytes: a write batch that does not fit is
    /// never merged with other batches (<see cref="WalTailer.TryReadChunk"/>) and is sent as consecutive
    /// <see cref="WalStreamMessage.Fragment"/>s, slices of the tailer's buffer (nothing is copied); the stream tag
    /// (source id, term) is taken once per batch. The receiver puts them together with a <see cref="WalStreamAssembler"/>.
    /// </para>
    /// </summary>
    public static IAsyncEnumerable<WalStreamMessage> StreamAsync(RocksDatabase db, ulong startSequence, StreamTagProvider tag,
        Func<ulong, long> entryTerm, int maxChunkBytes, CancellationToken cancellationToken) =>
        StreamAsync(db, startSequence, tag, entryTerm, maxChunkBytes, null, cancellationToken);

    /// <summary>
    /// <see cref="StreamAsync(RocksDatabase, ulong, StreamTagProvider, Func{ulong, long}, int, CancellationToken)"/> with a
    /// callback for errors of the source's own WAL that retrying the stream cannot cure:
    /// <list type="bullet">
    /// <item>Corruption (a WAL the follower still needs cannot be read): <paramref name="onSourceFault"/> is called and a
    /// <see cref="WalStreamMessage.ResyncAfterSourceFault"/> message ends the stream - a snapshot does not need the
    /// damaged WAL (the checkpoint flushes the memtables), and the follower streams from the snapshot's end afterwards.
    /// If that stream fails the same way (the source cannot even list its WAL files) the follower does not rebuild
    /// again, see <see cref="FollowerResyncPolicy"/>.</item>
    /// <item>NotSupported (the database cannot be tailed at all, e.g. a write-prepared / write-unprepared transaction
    /// database): <paramref name="onSourceFault"/> is called and the exception is thrown to the consumer. A snapshot
    /// would not help - the stream after it would fail the same way - so no resync is requested.</item>
    /// </list>
    /// </summary>
    public static async IAsyncEnumerable<WalStreamMessage> StreamAsync(RocksDatabase db, ulong startSequence, StreamTagProvider tag,
        Func<ulong, long> entryTerm, int maxChunkBytes, Action<RocksDbException>? onSourceFault,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(db);
        ArgumentNullException.ThrowIfNull(tag);
        ArgumentNullException.ThrowIfNull(entryTerm);
        // A follower that asks for entries past the end of this log holds entries this node never wrote: its log is not
        // a prefix of ours. Waiting until our log grows to that point would then stream our entries on top of its
        // unrelated ones (same sequence numbers, different history), so tell it to rebuild instead.
        ulong latest = db.GetLatestSequenceNumber();
        if (startSequence > latest + 1)
        {
            if (tag(out string id, out long t))
            {
                yield return WalStreamMessage.Resync(id, t,
                    "the follower asks for sequence " + startSequence + " but the source log ends at " + latest + ": the logs diverged");
            }
            yield break;
        }
        using WalTailer tailer = new ReplicationSource(db).TailWal(startSequence, maxChunkBytes);
        while (!cancellationToken.IsCancellationRequested)
        {
            if (!tag(out string sourceId, out long term)) yield break;
            WalChunk chunk;
            bool has;
            string? resync = null;
            bool sourceFault = false;
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
            catch (RocksDbException e) when (e.Code is StatusCode.Corruption)
            {
                onSourceFault?.Invoke(e);
                has = false;
                chunk = default;
                sourceFault = true;
                resync = "the source cannot read its WAL: " + e.Message;
            }
            catch (RocksDbException e) when (e.Code is StatusCode.NotSupported)
            {
                onSourceFault?.Invoke(e);
                throw;
            }
            if (resync != null)
            {
                yield return sourceFault ? WalStreamMessage.ResyncAfterSourceFault(sourceId, term, resync) : WalStreamMessage.Resync(sourceId, term, resync);
                yield break;
            }
            if (has)
            {
                if (chunk.Length > maxChunkBytes)
                {
                    // One write batch larger than a message may be (the tailer never merges such a batch with
                    // others): slices of the tailer's buffer, which stays untouched until the next read.
                    long batchTerm = entryTerm(chunk.FirstSequenceNumber);
                    ReadOnlyMemory<byte> all = chunk.Memory;
                    for (int offset = 0; offset < all.Length; offset += maxChunkBytes)
                    {
                        int n = Math.Min(maxChunkBytes, all.Length - offset);
                        bool last = offset + n == all.Length;
                        yield return WalStreamMessage.Fragment(chunk.FirstSequenceNumber, offset, all.Length, last ? chunk.EntryCount : 0,
                            all.Slice(offset, n), sourceId, term, batchTerm);
                    }
                    continue;
                }
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
/// background loop sends only the newest value through the callback. A report whose send failed is sent again after
/// <see cref="RetryDelay"/> (with the newest value) until one arrives: the last report of a burst is the only statement
/// of the follower's position until the next write, and in a Raft cluster the leader cannot commit a quorum-held entry
/// without it (rocksdb-sharp's hosts report on a 200 ms timer for the same reason).
/// Allocation-free per report (beyond what the callback's transport allocates).
/// </summary>
public sealed class ProgressReporter : IAsyncDisposable
{
    private readonly Func<ulong, CancellationToken, Task> _send;
    private readonly CancellationTokenSource _cts = new();
    private readonly RocksDb.Managed.Util.AsyncBroadcastSignal _signal = new();
    private readonly Task _loop;
    private ulong _latest;
    // 1 while a reported value has not been picked up by the loop.
    private int _pending;
    private volatile bool _completed;

    /// <summary>How long the loop waits before it sends a failed report again.</summary>
    public TimeSpan RetryDelay { get; }

    public ProgressReporter(Func<ulong, CancellationToken, Task> send) : this(send, TimeSpan.FromMilliseconds(200))
    {
    }

    public ProgressReporter(Func<ulong, CancellationToken, Task> send, TimeSpan retryDelay)
    {
        ArgumentNullException.ThrowIfNull(send);
        ArgumentOutOfRangeException.ThrowIfLessThan(retryDelay, TimeSpan.Zero);
        _send = send;
        RetryDelay = retryDelay;
        _loop = Task.Run(RunAsync);
    }

    private async Task RunAsync()
    {
        CancellationToken ct = _cts.Token;
        try
        {
            while (true)
            {
                if (Interlocked.Exchange(ref _pending, 0) == 1)
                {
                    ulong seq = Interlocked.Read(ref _latest);
                    try
                    {
                        await _send(seq, ct).ConfigureAwait(false);
                    }
                    catch (Exception e) when (e is not OperationCanceledException)
                    {
                        // Not delivered: send the newest value again (nothing may follow this report for a long time).
                        if (_completed) return;
                        Interlocked.Exchange(ref _pending, 1);
                        await Task.Delay(RetryDelay, ct).ConfigureAwait(false);
                    }
                    continue;
                }
                if (_completed) return;
                RocksDb.Managed.Util.AsyncBroadcastSignal.Waiter w = _signal.Register();
                // Re-check after registering (Report sets _pending with an interlocked exchange before signalling).
                if (Volatile.Read(ref _pending) == 1 || _completed)
                {
                    _signal.Unregister(w);
                    continue;
                }
                await _signal.WaitAsync(w, ct).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    public void Report(ulong sequence)
    {
        if (_completed) return;
        Interlocked.Exchange(ref _latest, sequence);
        Interlocked.Exchange(ref _pending, 1);
        _signal.SignalAll();
    }

    public async ValueTask DisposeAsync()
    {
        _completed = true;
        _signal.SignalAll();
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
