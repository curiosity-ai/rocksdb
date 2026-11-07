using System.Runtime.CompilerServices;
using RocksDb.Managed.Db;
using RocksDb.Managed.Wal;

namespace RocksDb.Managed;

/// <summary>A WAL file as listed by <see cref="RocksDatabase.GetSortedWalFiles"/> (rocksdb::LogFile / WalFile).</summary>
/// <param name="LogNumber">The WAL file number.</param>
/// <param name="Type">Alive (in wal_dir) or archived (in wal_dir/archive).</param>
/// <param name="StartSequence">Sequence number of the first write batch in the file (0 for the current WAL).</param>
/// <param name="SizeBytes">File size when listed.</param>
public sealed record WalFileInfo(ulong LogNumber, WalFileType Type, ulong StartSequence, ulong SizeBytes)
{
    /// <summary>Path relative to wal_dir, like WalFile::PathName ("/000012.log" or "/archive/000012.log").</summary>
    public string PathName => Type == WalFileType.ArchivedLogFile
        ? "/" + FileNames.ArchivalDirName + "/" + FileNames.LogFileName(LogNumber)
        : "/" + FileNames.LogFileName(LogNumber);
}

/// <summary>
/// One write batch read from the WAL (rocksdb::BatchResult) without copying: <see cref="Data"/> is the serialized
/// WriteBatch (12-byte header with sequence and count, then the records) and is only valid until the producing
/// <see cref="TransactionLogIterator"/> moves. Use <see cref="CopyTo(System.Buffers.IBufferWriter{byte})"/> or
/// <see cref="ToWriteBatch"/> to keep it.
/// </summary>
public readonly struct WalBatch
{
    /// <summary>
    /// Wraps a serialized WriteBatch (e.g. one received over a replication transport). <paramref name="data"/> must hold
    /// at least the 12-byte header (sequence, count).
    /// </summary>
    public WalBatch(ReadOnlyMemory<byte> data)
    {
        if (data.Length < WriteBatch.HeaderSize) ThrowHelper.InvalidArgument("a serialized write batch has at least a 12-byte header");
        Data = data;
    }

    /// <summary>The serialized WriteBatch (same bytes as native <c>WriteBatch::Data()</c>).</summary>
    public ReadOnlyMemory<byte> Data { get; }
    public ReadOnlySpan<byte> Span => Data.Span;
    /// <summary>Sequence number of the first entry.</summary>
    public ulong Sequence => Util.Coding.DecodeFixed64(Data.Span);
    /// <summary>Number of sequence numbers the batch consumes (the WriteBatch count).</summary>
    public int Count => (int)Util.Coding.DecodeFixed32(Data.Span[8..]);
    /// <summary>The last sequence number of the batch (Sequence + Count - 1).</summary>
    public ulong LastSequence => Sequence + (ulong)Count - 1;
    /// <summary>Enumerates the records of the batch.</summary>
    public WriteBatchReader GetReader() => new(Data.Span);
    /// <summary>Copies the batch bytes into <paramref name="destination"/>.</summary>
    public void CopyTo(System.Buffers.IBufferWriter<byte> destination)
    {
        ArgumentNullException.ThrowIfNull(destination);
        System.Buffers.BuffersExtensions.Write(destination, Data.Span);
    }

    /// <summary>A pooled <see cref="WriteBatch"/> holding a copy of this batch (dispose it to return it to the pool).</summary>
    public WriteBatch ToWriteBatch()
    {
        WriteBatch b = WriteBatch.Rent();
        b.SetContents(Data.Span);
        return b;
    }
}

/// <summary>
/// Iterates the write batches of the WAL from a starting sequence number, in sequence order. Port of
/// TransactionLogIteratorImpl (db/transaction_log_impl.cc) over <see cref="FragmentBufferedLogReader"/>, so it can
/// tail the WAL that is still being written: when <see cref="Valid"/> turns false at the end of the available data,
/// calling <see cref="Next"/> (or <see cref="NextAsync"/>) later picks up batches appended since.
/// <para>
/// Managed differences from upstream (better tailing ergonomics):
/// <list type="bullet">
/// <item>When the iterator reaches the end of the newest WAL it knew about while the DB has published newer
/// sequence numbers, it re-lists the WAL files and continues into WALs created after it was opened (upstream
/// returns TryAgain and the caller must open a new iterator).</item>
/// <item><see cref="WaitForUpdatesAsync"/> awaits the next write (signalled by the write path after the WAL append
/// and memtable insert are published) instead of polling.</item>
/// <item>Batches are exposed as <see cref="WalBatch"/> views over the reader's pooled buffers (no allocation per
/// batch).</item>
/// </list>
/// </para>
/// Errors (corruption, gaps in sequence numbers, missing WALs) are reported through <see cref="Error"/> /
/// <see cref="ThrowIfError"/>, like upstream's status(). Not thread-safe.
/// </summary>
public sealed class TransactionLogIterator : IDisposable, IAsyncEnumerable<WalBatch>
{
    private readonly DbImpl _db;
    private readonly bool _verifyChecksums;
    private readonly List<WalFileInfo> _files;
    private readonly Reporter _reporter = new();
    private FragmentBufferedLogReader? _reader;
    private int _currentFileIndex;
    private ulong _startingSequenceNumber;
    private ulong _currentBatchSeq;
    private ulong _currentLastSeq;
    private bool _started;
    private bool _isValid;
    private RocksDbException? _status;
    private ReadOnlyMemory<byte> _currentBatch;
    private bool _disposed;
    // recovery_sequence_generation_: the fenced ranges known when the iterator was created.
    private readonly ulong _recoverySequenceGeneration;

    private sealed class Reporter : LogReporter
    {
        public string? Info;

        public override void Corruption(long bytes, StatusCode code, string reason, ulong logNumber)
        {
            // LogReporter in transaction_log_impl.h only logs ("dropping N bytes; reason").
            Info = "dropping " + bytes + " bytes; " + reason;
        }
    }

    internal TransactionLogIterator(DbImpl db, ulong seq, List<WalFileInfo> files, bool verifyChecksums)
    {
        _db = db;
        _verifyChecksums = verifyChecksums;
        _files = files;
        _startingSequenceNumber = seq;
        _recoverySequenceGeneration = db.WalRecoverySequences.CurrentGeneration;
        SeekToStartSequence(0, strict: false);
    }

    /// <summary>True if <see cref="Current"/> is a batch (started and positioned).</summary>
    public bool Valid => _started && _isValid;

    /// <summary>The current batch; valid until the iterator moves.</summary>
    public WalBatch Current
    {
        get
        {
            if (!Valid) ThrowHelper.InvalidArgument("TransactionLogIterator is not valid");
            return new WalBatch(_currentBatch);
        }
    }

    /// <summary>The error of the last operation (upstream status()), or null.</summary>
    public RocksDbException? Error => _status;

    public void ThrowIfError()
    {
        if (_status is { } e) throw new RocksDbException(e.Code, e.Message, e.SubCode, e);
    }

    /// <summary>
    /// The sequence number the next batch is expected to start with (last read sequence + 1), or the requested start
    /// sequence before the first batch. A replica that applied every batch so far resumes from here.
    /// </summary>
    public ulong NextSequence => _started ? _currentLastSeq + 1 : _startingSequenceNumber;

    /// <summary>The WAL files the iterator reads (grows when it follows WAL rotations).</summary>
    public IReadOnlyList<WalFileInfo> Files => _files;

    // ---- iteration --------------------------------------------------------------------------------------

    /// <summary>Moves to the next batch (TransactionLogIteratorImpl::Next). No-op after an error.</summary>
    public void Next()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_status is null) NextImpl(internalCall: false);
    }

    /// <summary>
    /// Asynchronous <see cref="Next"/>: the WAL reads are done synchronously from the OS cache, so this completes
    /// synchronously; it exists so that callers can uniformly await.
    /// </summary>
    public ValueTask NextAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Next();
        return default;
    }

    /// <summary>
    /// Waits until the DB publishes a sequence number after the last batch this iterator returned (or after the start
    /// sequence before the first batch). Returns false if the DB was closed. Then call <see cref="Next"/>.
    /// </summary>
    public ValueTask<bool> WaitForUpdatesAsync(CancellationToken cancellationToken = default) =>
        _db.WaitForSequenceAsync(NextSequence, cancellationToken);

    /// <summary>
    /// Advances to the next batch, waiting for new writes when the WAL is exhausted. Returns false when the DB is
    /// closed or the iterator hit an error (see <see cref="Error"/>). Allocation-free in steady state (pooled state
    /// machine and wait).
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<bool> MoveNextTailingAsync(CancellationToken cancellationToken)
    {
        if (_started && _isValid) Next();
        while (!Valid)
        {
            if (_status is not null) return false;
            if (!await WaitForUpdatesAsync(cancellationToken).ConfigureAwait(false)) return false;
            Next();
            if (!Valid && _status is null && _db.Versions.LastSequence >= NextSequence)
            {
                // Published but not readable from the WAL (disableWAL writes, or manual_wal_flush data still
                // buffered): wait a little instead of spinning.
                await Task.Delay(1, cancellationToken).ConfigureAwait(false);
            }
        }
        return true;
    }

    /// <summary>
    /// Enumerates the batches available now (starting with the current one), without waiting for new writes. Each
    /// <see cref="WalBatch"/> is valid until the enumeration moves on.
    /// </summary>
    public async IAsyncEnumerator<WalBatch> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        await Task.CompletedTask.ConfigureAwait(false);
        while (Valid)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return Current;
            Next();
        }
        ThrowIfError();
    }

    // ---- port of TransactionLogIteratorImpl ---------------------------------------------------------------

    private bool RestrictedRead(out ReadOnlySpan<byte> record)
    {
        // Don't read if no more complete entries to read from logs.
        if (_currentLastSeq >= _db.Versions.LastSequence)
        {
            record = default;
            return false;
        }
        FragmentBufferedLogReader reader = _reader!;
        if (reader.TryReadRecord(out record)) return true;
        // Managed fix: a recycled WAL is overwritten in place. A tailing reader that stopped at a record of the file's
        // previous use has buffered (or dropped) bytes the writer has replaced since; the DB published newer
        // sequences, so re-read the file from that record on a fresh handle.
        if (!reader.AtStaleTail) return false;
        Env.ISequentialFile f;
        try
        {
            f = _db.OpenWalForRead(_files[_currentFileIndex]);
        }
        catch (Exception e) when (e is RocksDbException or IOException)
        {
            return false;
        }
        reader.Reposition(f);
        return reader.TryReadRecord(out record);
    }

    private void SeekToStartSequence(int startFileIndex, bool strict)
    {
        _started = false;
        _isValid = false;
        if (_files.Count <= startFileIndex) return;
        _currentFileIndex = startFileIndex;
        if (!OpenLogReader(_files[startFileIndex])) return;
        while (RestrictedRead(out ReadOnlySpan<byte> record))
        {
            if (record.Length < WriteBatch.HeaderSize)
            {
                _reporter.Corruption(record.Length, StatusCode.Corruption, "very small log record", LogReporter.UnspecifiedLogNumber);
                continue;
            }
            UpdateCurrentWriteBatch(record);
            if (_status is not null) return;
            if (_currentLastSeq >= _startingSequenceNumber)
            {
                if (strict && _currentBatchSeq != _startingSequenceNumber)
                {
                    _status = new RocksDbException(StatusCode.Corruption, "Corruption: Gap in sequence number. Could not seek to required sequence number");
                    return;
                }
                _isValid = true;
                _started = true; // set started_ as we could seek till starting sequence
                return;
            }
            _isValid = false;
        }
        // Could not find start sequence in first file. Normally this must be the only file. Otherwise log the error
        // and let the iterator return next entry. If strict is set, we want to seek exactly till the start sequence
        // and it should have been present in the file we scanned above.
        if (strict)
        {
            _status = new RocksDbException(StatusCode.Corruption, "Corruption: Gap in sequence number. Could not seek to required sequence number");
        }
        else if (_files.Count != 1)
        {
            _status = new RocksDbException(StatusCode.Corruption, "Corruption: Start sequence was not found, skipping to the next available");
            // Let NextImpl find the next available entry. started_ remains false because we don't want to check for
            // gaps while moving to start sequence.
            NextImpl(internalCall: true);
        }
        else
        {
            StopBeforeRecoveryGap(_files[^1].LogNumber, ulong.MaxValue);
        }
    }

    private void NextImpl(bool internalCall)
    {
        _isValid = false;
        if (!internalCall && !_started)
        {
            // Runs every time until we can seek to the start sequence. (Upstream then reads one more record, skipping
            // the batch it just found; the managed iterator stays on it.)
            if (_files.Count == 0) RefreshFileList();
            SeekToStartSequence(0, strict: false);
            if (!_started && _status is null && RefreshFileList()) SeekToStartSequence(0, strict: false);
            return;
        }
        bool refreshed = false;
        while (true)
        {
            if (_reader is null) return;
            while (RestrictedRead(out ReadOnlySpan<byte> record))
            {
                if (record.Length < WriteBatch.HeaderSize)
                {
                    _reporter.Corruption(record.Length, StatusCode.Corruption, "very small log record", LogReporter.UnspecifiedLogNumber);
                    continue;
                }
                UpdateCurrentWriteBatch(record);
                if (internalCall && !_started) _started = true;
                return;
            }

            // Open the next file.
            if (_currentFileIndex < _files.Count - 1)
            {
                _currentFileIndex++;
                if (!OpenLogReader(_files[_currentFileIndex]))
                {
                    _isValid = false;
                    return;
                }
                continue;
            }
            _isValid = false;
            if (StopBeforeRecoveryGap(_files[^1].LogNumber, ulong.MaxValue)) return;
            if (_currentLastSeq == _db.Versions.LastSequence || refreshed) return;
            // Managed extension: follow WAL rotation. Any WAL created after the current one implies the current one is
            // complete (the writer flushed every record before switching), so drain it once more, then continue.
            refreshed = true;
            if (!RefreshFileList()) return;
        }
    }

    /// <summary>Appends WALs newer than the last known one. Returns true if any was added.</summary>
    private bool RefreshFileList()
    {
        ulong lastKnown = _files.Count > 0 ? _files[^1].LogNumber : 0;
        // No WAL newer than the last known one exists while the DB still writes that one (or an older one): skip the
        // directory listing, which a tailer that keeps reaching the end of the WAL would otherwise pay on every Next.
        ulong current = _db.CurrentWalNumber;
        if (current != 0 && current <= lastKnown) return false;
        List<WalFileInfo> all;
        try
        {
            all = _db.GetSortedWalFiles(includeArchived: true);
        }
        catch (Exception e) when (e is RocksDbException or IOException)
        {
            return false;
        }
        bool added = false;
        foreach (WalFileInfo f in all)
        {
            if (f.LogNumber > lastKnown)
            {
                _files.Add(f);
                added = true;
            }
        }
        return added;
    }

    private bool IsBatchExpected(ulong batchSeq, ulong expectedSeq) => batchSeq == expectedSeq;

    private void UpdateCurrentWriteBatch(ReadOnlySpan<byte> record)
    {
        ulong batchSeq = Util.Coding.DecodeFixed64(record);
        uint count = Util.Coding.DecodeFixed32(record[8..]);
        ulong expectedSeq = _currentLastSeq + 1;
        // If the iterator has started, then confirm that we get continuous batches.
        if (_started && !IsBatchExpected(batchSeq, expectedSeq))
        {
            // A run is contiguous, so a discontinuity ends it (WalIteratorImpl::UpdateCurrentWriteBatch). Note that this
            // is a normal consequence of writes that bypass the WAL (WriteOptions.DisableWal, IngestExternalFile), not
            // necessarily of data loss. The iterator is spent: Next does nothing; open a new one at NextSequence.
            _isValid = false;
            _status = new RocksDbException(StatusCode.NotFound, "NotFound: Gap in sequence numbers");
            return;
        }
        ulong batchLastSeq = batchSeq + count - 1;
        if (StopBeforeRecoveryGap(_files[_currentFileIndex].LogNumber, batchLastSeq)) return;
        _currentBatchSeq = batchSeq;
        _currentLastSeq = batchLastSeq;
        _currentBatch = _reader!.Record;
        _isValid = true;
        _status = null;
    }

    /// <summary>
    /// Port of WalIteratorImpl::StopBeforeRecoveryGap: ends the run (NotFound) before a sequence range whose WAL write
    /// failed. Successful WAL write error recovery leaves that write out of the DB although its record may be complete
    /// in the failed WAL, so the iterator must not return it (and must not pretend the run continues across it). A range
    /// recorded after the iterator was created is applied once the iterator reads the failed WAL or a later one.
    /// Returns true if the iterator stopped.
    /// </summary>
    private bool StopBeforeRecoveryGap(ulong walNumber, ulong batchLastSequence)
    {
        if (!ReachedRecoveryGap(walNumber, batchLastSequence, out WalRecoverySequenceGap gap)) return false;
        _isValid = false;
        _status = new RocksDbException(StatusCode.NotFound,
            "NotFound: Gap in sequence numbers after indeterminate WAL write through sequence " + gap.EndSequence);
        return true;
    }

    /// <summary>True if a batch ending at <paramref name="batchLastSequence"/> read from WAL <paramref name="walNumber"/>
    /// reaches the first fenced range at or after the start sequence (see <see cref="StopBeforeRecoveryGap"/>).</summary>
    private bool ReachedRecoveryGap(ulong walNumber, ulong batchLastSequence, out WalRecoverySequenceGap gap)
    {
        if (!_db.WalRecoverySequences.FirstAtOrAfter(_startingSequenceNumber, out gap)) return false;
        bool knownWhenCreated = gap.Generation <= _recoverySequenceGeneration;
        return batchLastSequence >= gap.StartSequence && (knownWhenCreated || walNumber >= gap.WalNumber);
    }

    private bool OpenLogReader(WalFileInfo file)
    {
        Env.ISequentialFile f;
        try
        {
            f = _db.OpenWalForRead(file);
        }
        catch (RocksDbException e)
        {
            _status = e;
            return false;
        }
        _reader?.Dispose();
        _reader = new FragmentBufferedLogReader(f, _reporter, _verifyChecksums, file.LogNumber);
        return true;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _reader?.Dispose();
        _reader = null;
    }
}
