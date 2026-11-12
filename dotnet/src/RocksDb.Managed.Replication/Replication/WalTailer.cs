using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;

namespace RocksDb.Managed.Replication;

/// <summary>
/// A run of consecutive WAL write batches merged into one serialized write batch: one 12-byte header (first sequence
/// number, summed entry count) followed by every batch's records - the concatenation
/// <c>WriteBatchInternal::Append</c> performs. Applying it on a replica (<see cref="ReplicationConsumer.IngestChunkAsync"/>)
/// advances the replica's sequence number by exactly <see cref="EntryCount"/>, so both databases stay on the same
/// sequence numbers. The bytes are a valid native <c>WriteBatch</c> representation.
/// <para><see cref="Memory"/> points into the tailer's pooled buffer and is only valid until the next read.</para>
/// </summary>
public readonly struct WalChunk
{
    internal WalChunk(ulong firstSequenceNumber, ulong nextSequenceNumber, int entryCount, int batchCount, byte[] buffer, int length)
    {
        FirstSequenceNumber = firstSequenceNumber;
        NextSequenceNumber = nextSequenceNumber;
        EntryCount = entryCount;
        BatchCount = batchCount;
        Buffer = buffer;
        Memory = buffer.AsMemory(0, length);
    }

    /// <summary>The sequence number of the first entry in the chunk.</summary>
    public ulong FirstSequenceNumber { get; }

    /// <summary>The first sequence number not in the chunk - what a replica asks for next.</summary>
    public ulong NextSequenceNumber { get; }

    /// <summary>How many sequence numbers the merged batch consumes.</summary>
    public int EntryCount { get; }

    /// <summary>How many WAL write batches were merged into this chunk.</summary>
    public int BatchCount { get; }

    /// <summary>The serialized merged write batch.</summary>
    public ReadOnlyMemory<byte> Memory { get; }

    /// <summary>
    /// The tailer's buffer holding the chunk: the merged write batch is its first <see cref="Length"/> bytes (rocksdb-sharp
    /// <c>WalChunk.Buffer</c>). Same lifetime as <see cref="Memory"/>; never write to it.
    /// </summary>
    public byte[] Buffer { get; }

    public ReadOnlySpan<byte> Data => Memory.Span;
    public int Length => Memory.Length;

    /// <summary>The chunk as a <see cref="WalBatch"/> (same lifetime).</summary>
    public WalBatch AsWalBatch() => new(Memory);
}

/// <summary>
/// Tails a database's write-ahead log for replication, handing out runs of consecutive write batches merged into
/// <see cref="WalChunk"/>s (port of rocksdb-sharp <c>WalTailer</c>).
/// <para>
/// Managed design: rocksdb-sharp kept a native log iterator alive and, because a native iterator does not follow WAL
/// rotation, reopened it after a "grace period" of seeing the database ahead of an exhausted iterator, and waited for
/// updates by spinning and then polling with <c>Task.Delay(1)</c>. The managed <see cref="TransactionLogIterator"/>
/// follows WAL rotations itself and the database signals new sequence numbers from its write path
/// (<see cref="RocksDatabase.WaitForSequenceAsync"/>), so this tailer opens one iterator for its whole life, never
/// reopens it, and waits without polling. The reopen statistics (<c>Reopens</c>, <c>ReopenTime</c>,
/// <c>ReopenGraceMicroseconds</c>) are gone with the workaround.
/// </para>
/// <para>
/// Errors: a requested sequence number whose WAL was already purged throws <see cref="RocksDbException"/> with
/// <see cref="StatusCode.NotFound"/> - the replica must be rebuilt from a checkpoint; an unreadable WAL throws
/// <see cref="StatusCode.Corruption"/>. Not thread-safe (one reader).
/// Dispose the tailer before closing the database.
/// </para>
/// </summary>
public sealed class WalTailer : IDisposable
{
    /// <summary>A serialized write batch is an 8-byte sequence number, a 4-byte entry count, then the records.</summary>
    private const int HeaderSize = WriteBatch.HeaderSize;

    public const int DefaultMaxChunkBytes = 256 * 1024;

    private readonly RocksDatabase _db;
    private readonly int _maxChunkBytes;

    /// <summary>
    /// Refuses chunk sizes no stream could be built with: a WAL chunk must be larger than a write batch header and a
    /// snapshot chunk must hold at least one byte. Hosts call it when they are created, so a wrong setting fails the
    /// start instead of every stream. Upper bounds belong to the transport (a message size limit).
    /// </summary>
    internal static void ValidateChunkSizes(int maxChunkBytes, int snapshotChunkBytes)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(maxChunkBytes, HeaderSize);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(snapshotChunkBytes, 0);
    }
    private TransactionLogIterator? _iterator;
    private byte[] _buffer;
    private ulong _nextSequenceNumber;
    private bool _started;
    private bool _disposed;

    internal WalTailer(RocksDatabase db, ulong sequenceNumber, int maxChunkBytes)
    {
        ArgumentNullException.ThrowIfNull(db);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(maxChunkBytes, HeaderSize);
        _db = db;
        _maxChunkBytes = maxChunkBytes;
        _buffer = RocksArrayPool<byte>.Shared.Rent(Math.Min(maxChunkBytes, 64 * 1024));
        _nextSequenceNumber = sequenceNumber;
    }

    /// <summary>The first sequence number this tailer has not handed out yet.</summary>
    public ulong NextSequenceNumber => _nextSequenceNumber;

    /// <summary>
    /// How a start sequence number inside a multi-entry write batch is handled. False (default): the first read throws
    /// <see cref="RocksDbException"/> with <see cref="StatusCode.InvalidArgument"/>, since a batch is applied atomically.
    /// True: the first chunk starts with that whole batch (<see cref="WalChunk.FirstSequenceNumber"/> is the batch's own
    /// first sequence, below the requested one), like rocksdb-sharp's <c>WalTailer</c> and <c>GetUpdatesSince</c>; the
    /// receiver must skip the entries it already has. Set it before the first read.
    /// </summary>
    public bool IncludeBatchContainingStart
    {
        get => _includeBatchContainingStart;
        set
        {
            if (_iterator is not null) throw new InvalidOperationException("IncludeBatchContainingStart must be set before the first read.");
            _includeBatchContainingStart = value;
        }
    }

    private bool _includeBatchContainingStart;

    /// <summary>True when the database holds sequence numbers this tailer has not read yet.</summary>
    public bool HasUpdates => _nextSequenceNumber <= _db.GetLatestSequenceNumber();

    /// <summary>How far behind the database's newest sequence number this tailer is.</summary>
    public ulong LagInSequenceNumbers
    {
        get
        {
            ulong latest = _db.GetLatestSequenceNumber();
            return latest < _nextSequenceNumber ? 0 : latest - _nextSequenceNumber + 1;
        }
    }

    /// <summary>
    /// Reads the run of whole write batches available now, up to the chunk size: the chunk ends before a batch that
    /// does not fit into the rest of it, so a chunk is larger than the chunk size only when it is one single write
    /// batch that is larger on its own - what a transport that sends such a batch in pieces relies on
    /// (<see cref="Hosting.WalStreamSender"/>). Returns false when there is nothing newer; then await
    /// <see cref="WaitForUpdatesAsync"/>.
    /// </summary>
    public bool TryReadChunk(out WalChunk chunk)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        chunk = default;
        if (_iterator is null)
        {
            // Asking for a sequence number the database has not reached is an error, not an empty iterator.
            if (!HasUpdates) return false;
            _iterator = _db.GetUpdatesSince(_nextSequenceNumber);
        }
        TransactionLogIterator it = _iterator;
        int entryCount = 0, batchCount = 0, length = HeaderSize;
        ulong firstSequenceNumber = _nextSequenceNumber;
        while (true)
        {
            if (!it.Valid)
            {
                if (entryCount > 0) break;
                it.ThrowIfError();
                // An exhausted iterator picks up records appended since (and follows WAL rotation) when moved again.
                it.Next();
                if (!it.Valid)
                {
                    it.ThrowIfError();
                    if (!_started) ThrowIfStartPurged();
                    else ThrowIfRotationCannotBeFollowed(it);
                    break;
                }
            }
            WalBatch batch = it.Current;
            ulong batchSeq = batch.Sequence;
            ulong afterBatch = batch.LastSequence + 1;
            if (afterBatch > _nextSequenceNumber)
            {
                if (batchSeq != _nextSequenceNumber)
                {
                    if (batchSeq > _nextSequenceNumber)
                    {
                        throw new RocksDbException(StatusCode.NotFound,
                            "NotFound: WAL data for sequence " + _nextSequenceNumber + " is no longer available (next batch starts at " + batchSeq + ")");
                    }
                    // The requested sequence is inside a batch: a batch is applied atomically, so it cannot be split.
                    // Only the start can be inside one; later batches follow on from the previous one.
                    if (_started || !_includeBatchContainingStart)
                    {
                        throw new RocksDbException(StatusCode.InvalidArgument,
                            "Invalid argument: sequence " + _nextSequenceNumber + " is inside the WAL batch " + batchSeq + ".." + (afterBatch - 1));
                    }
                }
                ReadOnlySpan<byte> body = batch.Span[HeaderSize..];
                // Left for the next chunk (the iterator stays on it).
                if (entryCount > 0 && (long)length + body.Length > _maxChunkBytes) break;
                EnsureCapacity((long)length + body.Length);
                body.CopyTo(_buffer.AsSpan(length));
                if (entryCount == 0) firstSequenceNumber = batchSeq;
                length += body.Length;
                entryCount += batch.Count;
                batchCount++;
                _nextSequenceNumber = afterBatch;
                _started = true;
            }
            it.Next();
            if (length >= _maxChunkBytes) break;
        }
        if (entryCount == 0) return false;
        BinaryPrimitives.WriteUInt64LittleEndian(_buffer, firstSequenceNumber);
        BinaryPrimitives.WriteInt32LittleEndian(_buffer.AsSpan(sizeof(ulong)), entryCount);
        chunk = new WalChunk(firstSequenceNumber, _nextSequenceNumber, entryCount, batchCount, _buffer, length);
        return true;
    }

    /// <summary>
    /// Waits until the database publishes a sequence number this tailer has not read (signalled by the write path, no
    /// polling). Returns false if the database was closed. Allocation-free in steady state.
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<bool> WaitForUpdatesAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (HasUpdates)
        {
            // Published but not readable through the WAL (e.g. writes with DisableWal, or manual_wal_flush data still
            // buffered): the write path will not signal again for these, so back off briefly.
            await Task.Delay(1, cancellationToken).ConfigureAwait(false);
            return true;
        }
        return await _db.WaitForSequenceAsync(_nextSequenceNumber, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Streams chunks forever: each is valid until the enumeration moves on. Ends when the database is closed or
    /// <paramref name="cancellationToken"/> is cancelled (then throws <see cref="OperationCanceledException"/>).
    /// </summary>
    public async IAsyncEnumerable<WalChunk> ReadChunksAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (TryReadChunk(out WalChunk chunk))
            {
                yield return chunk;
                continue;
            }
            if (!await WaitForUpdatesAsync(cancellationToken).ConfigureAwait(false)) yield break;
        }
    }

    /// <summary>
    /// Before the first batch: the database is at or past the requested sequence but the iterator found nothing. If no
    /// WAL can contain it any more (all purged, or the oldest starts later), report NotFound now instead of waiting for
    /// a later write to reveal the gap.
    /// </summary>
    private void ThrowIfStartPurged()
    {
        if (!HasUpdates) return;
        IReadOnlyList<WalFileInfo> files = _db.GetSortedWalFiles();
        foreach (WalFileInfo f in files)
        {
            if (f.StartSequence != 0 && f.StartSequence <= _nextSequenceNumber) return;
        }
        throw new RocksDbException(StatusCode.NotFound,
            "NotFound: WAL data for sequence " + _nextSequenceNumber + " is no longer available" +
            (files.Count > 0 ? " (oldest WAL starts at " + files[0].StartSequence + ")" : " (no WAL files)"));
    }

    /// <summary>
    /// After the first batch: the database is ahead, the iterator found nothing and the database writes a WAL the
    /// iterator does not know. The iterator follows rotations by listing the WAL files and treats a failed listing as
    /// "nothing new" - e.g. <see cref="StatusCode.Corruption"/> for a WAL whose first record cannot be read. List them
    /// here so that error reaches the caller instead of the tail waiting forever on a stream that cannot advance.
    /// </summary>
    private void ThrowIfRotationCannotBeFollowed(TransactionLogIterator iterator)
    {
        if (!HasUpdates) return;
        IReadOnlyList<WalFileInfo> known = iterator.Files;
        if (known.Count > 0 && _db.CurrentWalNumber <= known[^1].LogNumber) return;
        _db.GetSortedWalFiles();
    }

    private void EnsureCapacity(long length)
    {
        if (_buffer.Length >= length) return;
        // A serialized write batch is one array.
        if (length > Array.MaxLength)
            throw new RocksDbException(StatusCode.InvalidArgument, "Invalid argument: a WAL chunk of " + length + " bytes does not fit into one buffer");
        long size = Math.Max(_buffer.Length, 1);
        while (size < length) size *= 2;
        byte[] bigger = RocksArrayPool<byte>.Shared.Rent((int)Math.Min(size, Array.MaxLength));
        _buffer.AsSpan().CopyTo(bigger);
        RocksArrayPool<byte>.Shared.Return(_buffer);
        _buffer = bigger;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _iterator?.Dispose();
        _iterator = null;
        RocksArrayPool<byte>.Shared.Return(_buffer);
        _buffer = [];
    }
}
