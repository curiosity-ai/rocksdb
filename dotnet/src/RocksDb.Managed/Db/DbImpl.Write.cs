using System.Diagnostics;
using System.Runtime.CompilerServices;
using RocksDb.Managed.Env;
using RocksDb.Managed.Wal;

namespace RocksDb.Managed.Db;

internal sealed partial class DbImpl
{
    private readonly WriteThread _writeThread;
    private readonly List<WriteThread.Writer> _group = new();       // leader-only
    private readonly WriteBatch _groupBatch = new(64 << 10, 0);     // leader-only: merged WAL record
    private readonly List<ColumnFamilyData> _flushScheduler = new(); // leader-only: CFs whose memtable became full
    // Column families by id for the write path: copy-on-write array, replaced by an unbatched leader.
    private ColumnFamilyData?[] _cfById = [];

    private LogWriter? _log;          // leader-only
    private ulong _logNumber;         // written by leader under mutex
    private bool _logEmpty = true;    // leader-only
    private bool _walDirSynced;       // leader-only
    private bool _hasUnpersistedData; // set when a DisableWal write was applied
    // Cached IsWriteStalled() (recomputed under the mutex whenever memtable counts change) so that the common,
    // unstalled write path does not take the DB mutex.
    private volatile bool _maybeStalled;

    // ---- public entry points ----------------------------------------------------------------------------

    /// <summary>
    /// Writes a batch (DBImpl::WriteImpl). With <paramref name="synchronous"/> the returned task is always complete:
    /// the caller blocks only while waiting for its turn, and performs the group's I/O itself when it leads.
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask WriteAsync(WriteOptions? options, WriteBatch batch, bool validateColumnFamilies, bool synchronous, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(batch);
        ThrowIfDisposed();
        if (_readOnly) ThrowReadOnly();
        if (options is { Sync: true, DisableWal: true }) ThrowHelper.InvalidArgument("Sync writes has to enable WAL.");
        if (batch.DataSize < WriteBatch.HeaderSize) ThrowHelper.Corruption("malformed WriteBatch (too small)");
        if (validateColumnFamilies) ValidateColumnFamilies(batch, options?.IgnoreMissingColumnFamilies ?? false);
        WriteThread.Writer w = WriteThread.Rent(synchronous);
        w.Prepare(batch, options, synchronous);
        try
        {
            if (!_writeThread.JoinBatchGroup(w))
            {
                int state = synchronous ? w.WaitSync() : await w.WaitAsync().ConfigureAwait(false);
                if (state == WriteThread.StateCompleted)
                {
                    if (w.Error is { } e) ThrowWriteError(e);
                    return;
                }
                Debug.Assert(state == WriteThread.StateGroupLeader);
            }
            ValueTask lead = LeadWriteGroupAsync(w, synchronous, cancellationToken);
            if (synchronous)
            {
                Debug.Assert(lead.IsCompleted);
                lead.GetAwaiter().GetResult();
            }
            else
            {
                await lead.ConfigureAwait(false);
            }
            if (w.Error is { } err) ThrowWriteError(err);
        }
        finally
        {
            WriteThread.Return(w, synchronous);
        }
    }

    private static void ThrowWriteError(Exception e)
    {
        // Each writer gets its own exception object (the original may be shared by the whole group).
        if (e is RocksDbException r) throw new RocksDbException(r.Code, r.Message, r.SubCode, r);
        if (e is ObjectDisposedException) ThrowDisposed();
        throw RocksDbException.IOError(e.Message, e);
    }

    /// <summary>
    /// Becomes the write leader without a batch (WriteThread::EnterUnbatched): on return no other write is in progress
    /// until <see cref="WriteThread.ExitAsLeader"/> is called.
    /// </summary>
    internal async ValueTask<WriteThread.Writer> EnterUnbatchedAsync(bool synchronous, CancellationToken cancellationToken)
    {
        WriteThread.Writer w = await EnterUnbatchedCoreAsync(synchronous).ConfigureAwait(false);
        if (IsDisposed)
        {
            // The DB was closed while this operation waited for its turn: the WAL and MANIFEST are gone.
            ExitUnbatched(w, synchronous);
            ThrowDisposed();
        }
        return w;
    }

    /// <summary>EnterUnbatched without the closed-DB check (used by close itself).</summary>
    private async ValueTask<WriteThread.Writer> EnterUnbatchedCoreAsync(bool synchronous)
    {
        WriteThread.Writer w = WriteThread.Rent(synchronous);
        w.Prepare(null, null, synchronous);
        if (!_writeThread.JoinBatchGroup(w))
        {
            int state = synchronous ? w.WaitSync() : await w.WaitAsync().ConfigureAwait(false);
            Debug.Assert(state == WriteThread.StateGroupLeader);
        }
        return w;
    }

    internal void ExitUnbatched(WriteThread.Writer w, bool synchronous)
    {
        _writeThread.ExitAsLeader();
        WriteThread.Return(w, synchronous);
    }

    // ---- leader -----------------------------------------------------------------------------------------

    /// <summary>
    /// Commits the group led by <paramref name="leader"/>: PreprocessWrite, sequence allocation, one WAL record for the
    /// whole group, memtable inserts, publication of the last sequence, then wakes the followers
    /// (DBImpl::WriteImpl, non-pipelined).
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask LeadWriteGroupAsync(WriteThread.Writer leader, bool synchronous, CancellationToken cancellationToken)
    {
        List<WriteThread.Writer> group = _group;
        _writeThread.EnterAsBatchGroupLeader(leader, group);
        try
        {
            // Writers that queued before the DB was closed get leadership after close released it.
            if (IsDisposed) ThrowDisposed();
            ValueTask pre = PreprocessWriteAsync(leader.NoSlowdown, synchronous, cancellationToken);
            if (!pre.IsCompletedSuccessfully) await pre.ConfigureAwait(false);

            ulong total = 0;
            bool needSync = false;
            for (int i = 0; i < group.Count; i++)
            {
                total += (uint)group[i].Batch!.Count;
                needSync |= group[i].Sync;
            }
            ulong first = Versions.AllocateSequence(total);
            ulong seq = first;
            for (int i = 0; i < group.Count; i++)
            {
                WriteBatch b = group[i].Batch!;
                b.Sequence = seq;
                seq += (uint)b.Count;
            }
            ulong last = seq - 1;

            if (!leader.DisableWal)
            {
                ReadOnlyMemory<byte> record;
                if (group.Count == 1)
                {
                    record = leader.Batch!.DataMemory;
                }
                else
                {
                    _groupBatch.SetContents(leader.Batch!.Data);
                    for (int i = 1; i < group.Count; i++) WriteBatch.Append(_groupBatch, group[i].Batch!);
                    record = _groupBatch.DataMemory;
                }
                try
                {
                    LogWriter log = _log!;
                    if (synchronous)
                    {
                        log.AddRecord(record.Span, last);
                        if (needSync)
                        {
                            log.Sync();
                            SyncWalDirIfNeeded();
                        }
                    }
                    else
                    {
                        // No cancellation once the group is being committed: an abandoned AddRecord would leave a torn
                        // record in the middle of the WAL (and fail followers that did not ask for cancellation).
                        await log.AddRecordAsync(record, last, CancellationToken.None).ConfigureAwait(false);
                        if (needSync)
                        {
                            await log.SyncAsync(CancellationToken.None).ConfigureAwait(false);
                            SyncWalDirIfNeeded();
                        }
                    }
                    _logEmpty = false;
                }
                catch (Exception e)
                {
                    // A failed WAL write leaves the WAL in an unknown state: stop accepting writes (bg error).
                    SetBackgroundError(e);
                    throw;
                }
            }
            else if (total > 0)
            {
                Volatile.Write(ref _hasUnpersistedData, true);
            }

            var inserter = new MemTableInserter(this, logNumber: 0, ignoreMissingColumnFamilies: false);
            for (int i = 0; i < group.Count; i++)
            {
                WriteThread.Writer w = group[i];
                inserter.Sequence = w.Batch!.Sequence;
                inserter.IgnoreMissingColumnFamilies = w.IgnoreMissingColumnFamilies;
                try
                {
                    WriteBatch.Iterate(w.Batch.Data, ref inserter);
                }
                catch (Exception e)
                {
                    w.Error = e;
                }
            }
            if (total > 0)
            {
                Versions.SetLastSequence(last);
                NotifySequenceWaiters();
            }
        }
        catch (Exception e)
        {
            for (int i = 0; i < group.Count; i++) group[i].Error ??= e;
        }
        finally
        {
            _writeThread.ExitAsBatchGroupLeader(group);
        }
    }

    private void SyncWalDirIfNeeded()
    {
        if (_walDirSynced) return;
        FileSystem.SyncDirectory(WalDir);
        _walDirSynced = true;
    }

    /// <summary>
    /// Port of DBImpl::PreprocessWrite: background error check, memtable switches for full memtables
    /// (ScheduleFlushes), and write stalls (DelayWrite with the "stop" condition only).
    /// </summary>
    private async ValueTask PreprocessWriteAsync(bool noSlowdown, bool synchronous, CancellationToken cancellationToken)
    {
        RocksDbException? bg = Volatile.Read(ref _bgError);
        if (bg is not null) ThrowBackgroundError(bg);
        if (_flushScheduler.Count > 0)
        {
            for (int i = 0; i < _flushScheduler.Count; i++)
            {
                ColumnFamilyData cfd = _flushScheduler[i];
                if (cfd.Dropped || cfd.Mem.IsEmpty) continue;
                await SwitchMemtableAsync(cfd, synchronous, cancellationToken).ConfigureAwait(false);
            }
            _flushScheduler.Clear();
        }
        if (_maybeStalled) await DelayWriteAsync(noSlowdown, synchronous, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes are stopped or delayed (memtable count, L0 file count, pending compaction bytes; see
    /// <see cref="RecalculateWriteStall"/>). REQUIRES: mutex.
    /// </summary>
    private bool IsWriteStalled() => IsWriteStoppedOrDelayed();

    // ---- memtable switch --------------------------------------------------------------------------------

    /// <summary>
    /// Port of DBImpl::SwitchMemtable: makes the CF's mutable memtable immutable, starts a new WAL (unless the current
    /// one is empty) and schedules a flush. REQUIRES: caller is the write leader (batched or unbatched).
    /// </summary>
    internal async ValueTask SwitchMemtableAsync(ColumnFamilyData cfd, bool synchronous, CancellationToken cancellationToken)
    {
        LogWriter? newLog = null;
        ulong newLogNumber;
        lock (_mutex) newLogNumber = _logNumber;
        if (!_logEmpty || _log is null)
        {
            newLogNumber = Versions.NewFileNumber();
            try
            {
                newLog = CreateWal(newLogNumber);
                LogWriter? old = _log;
                if (old is not null)
                {
                    // Make the old WAL durable before writes continue in the new one, so a crash can't lose its tail while
                    // keeping later writes (upstream syncs closed WALs on the next sync write instead). Not cancellable:
                    // the old WAL is closed only after it was synced.
                    if (synchronous)
                    {
                        old.Sync();
                        old.Dispose();
                    }
                    else
                    {
                        await old.SyncAsync(CancellationToken.None).ConfigureAwait(false);
                        await old.DisposeAsync().ConfigureAwait(false);
                    }
                }
            }
            catch (Exception e)
            {
                // SwitchMemtable: a failure to create the new WAL or to make the old one durable stops writes (bg error).
                if (newLog is not null)
                {
                    try { newLog.Dispose(); }
                    catch (Exception de) when (de is IOException or RocksDbException) { }
                    TryDeleteFile(FileNames.LogFileName(WalDir, newLogNumber));
                }
                SetBackgroundError(e);
                throw;
            }
        }
        lock (_mutex)
        {
            if (newLog is not null)
            {
                _log = newLog;
                _logNumber = newLogNumber;
                _logEmpty = true;
                _walDirSynced = false;
                _aliveWals.Add(newLogNumber);
            }
            MemTable mem = cfd.Mem;
            cfd.Imm.Add(new ImmutableMemTable(mem, newLogNumber));
            cfd.Mem = cfd.ConstructNewMemtable(Versions.LastSequence);
            if (newLog is not null)
            {
                // All this is just optimization to delete logs that are no longer needed: if a CF is empty, it doesn't
                // need the old log to stay alive, so advance its log number (in memory only, like upstream).
                foreach (ColumnFamilyData c in Versions.ColumnFamilies)
                {
                    if (c.Mem.IsEmpty && c.NumNotFlushed == 0 && c.LogNumber < newLogNumber) c.LogNumber = newLogNumber;
                }
            }
            InstallSuperVersion(cfd);
            _maybeStalled = IsWriteStalled();
            EnqueueFlush(cfd);
            MaybeScheduleFlush();
        }
    }

    /// <summary>Creates WAL #<paramref name="number"/> (DBImpl::CreateWAL).</summary>
    private LogWriter CreateWal(ulong number)
    {
        string path = FileNames.LogFileName(WalDir, number);
        var fileOptions = new WritableFileOptions
        {
            BytesPerSync = (long)Math.Min(Options.WalBytesPerSync, long.MaxValue),
            MaxBufferSize = (int)Math.Clamp(Options.WritableFileMaxBufferSize, 64 * 1024, int.MaxValue),
        };
        IWritableFile file = FileSystem.CreateWritable(path, truncate: true, fileOptions);
        try
        {
            var log = new LogWriter(file, number, recycleLogFiles: false, Options.ManualWalFlush, Options.WalCompression, 0);
            log.AddCompressionTypeRecord();
            return log;
        }
        catch
        {
            file.Dispose();
            throw;
        }
    }

    // ---- memtable inserter ------------------------------------------------------------------------------

    /// <summary>
    /// Leader-side column family resolution for inserts (ColumnFamilyMemTablesImpl::Seek). Returns null for unknown or
    /// dropped CFs and for CFs whose data in WAL <paramref name="logNumber"/> was already flushed (recovery).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ColumnFamilyData? CfForInsert(uint id, ulong logNumber, bool ignoreMissing)
    {
        ColumnFamilyData?[] map = _cfById;
        ColumnFamilyData? c = id < (uint)map.Length ? map[id] : null;
        if (c is null || c.Dropped)
        {
            // Writes to dropped column families are ignored (like upstream); unknown ids are an error unless ignored.
            if (c is null && !ignoreMissing && logNumber == 0) ThrowHelper.InvalidArgument("Invalid column family specified in write batch");
            return null;
        }
        // During recovery, skip data that was already flushed for this CF.
        if (logNumber != 0 && logNumber < c.LogNumber) return null;
        return c;
    }

    private void RegisterCfForWrite(ColumnFamilyData cfd)
    {
        ColumnFamilyData?[] old = _cfById;
        var map = new ColumnFamilyData?[Math.Max(old.Length, (int)cfd.Id + 1)];
        old.CopyTo(map, 0);
        map[cfd.Id] = cfd;
        Volatile.Write(ref _cfById, map);
    }

    private void UnregisterCfForWrite(ColumnFamilyData cfd)
    {
        ColumnFamilyData?[] map = (ColumnFamilyData?[])_cfById.Clone();
        if (cfd.Id < (uint)map.Length) map[cfd.Id] = null;
        Volatile.Write(ref _cfById, map);
    }

    /// <summary>Validates the column family ids of a user batch before it is committed (avoids partially applied batches).</summary>
    private void ValidateColumnFamilies(WriteBatch batch, bool ignoreMissing)
    {
        if (ignoreMissing) return;
        var v = new CfValidator(Volatile.Read(ref _cfById));
        WriteBatch.Iterate(batch.Data, ref v);
    }

    private readonly struct CfValidator(ColumnFamilyData?[] map) : IWriteBatchHandler
    {
        private void Check(uint id)
        {
            if (id >= (uint)map.Length || map[id] is null) ThrowHelper.InvalidArgument("Invalid column family specified in write batch");
        }

        public bool Continue() => true;
        public void Put(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => Check(columnFamilyId);
        public void TimedPut(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ulong writeUnixTime) => Check(columnFamilyId);
        public void PutEntity(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> entity) =>
            ThrowHelper.NotSupported("PutEntity (wide columns) is not supported yet");
        public void Delete(uint columnFamilyId, ReadOnlySpan<byte> key) => Check(columnFamilyId);
        public void SingleDelete(uint columnFamilyId, ReadOnlySpan<byte> key) => Check(columnFamilyId);
        public void DeleteRange(uint columnFamilyId, ReadOnlySpan<byte> beginKey, ReadOnlySpan<byte> endKey) => Check(columnFamilyId);
        public void Merge(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => Check(columnFamilyId);
        public void PutBlobIndex(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> blobIndex) =>
            ThrowHelper.NotSupported("Blob files are not supported yet");
        public void LogData(ReadOnlySpan<byte> blob) { }
        public void MarkBeginPrepare(bool unprepare) => ThrowHelper.NotSupported("Two-phase commit is not supported yet");
        public void MarkEndPrepare(ReadOnlySpan<byte> xid) => ThrowHelper.NotSupported("Two-phase commit is not supported yet");
        public void MarkNoop(bool emptyBatch) { }
        public void MarkCommit(ReadOnlySpan<byte> xid) => ThrowHelper.NotSupported("Two-phase commit is not supported yet");
        public void MarkCommitWithTimestamp(ReadOnlySpan<byte> xid, ReadOnlySpan<byte> commitTimestamp) =>
            ThrowHelper.NotSupported("Two-phase commit is not supported yet");
        public void MarkRollback(ReadOnlySpan<byte> xid) => ThrowHelper.NotSupported("Two-phase commit is not supported yet");
    }

    /// <summary>
    /// Applies WriteBatch records to memtables. Port of MemTableInserter (db/write_batch.cc), sequential (not concurrent)
    /// insertion, one sequence number per key.
    /// </summary>
    private struct MemTableInserter : IWriteBatchHandler
    {
        private readonly DbImpl _db;
        private readonly ulong _logNumber;
        public ulong Sequence;
        public bool IgnoreMissingColumnFamilies;

        public MemTableInserter(DbImpl db, ulong logNumber, bool ignoreMissingColumnFamilies)
        {
            _db = db;
            _logNumber = logNumber;
            IgnoreMissingColumnFamilies = ignoreMissingColumnFamilies;
            Sequence = 0;
        }

        public readonly bool Continue() => true;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Add(uint cf, EntryType type, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            ColumnFamilyData? c = _db.CfForInsert(cf, _logNumber, IgnoreMissingColumnFamilies);
            if (c is not null)
            {
                MemTable mem = c.Mem;
                // A duplicate key+seq can only come from replaying an already applied batch; ignore it like upstream's
                // TryAgain handling for non-seq-per-batch writes.
                mem.Add(Sequence, type, key, value);
                if (mem.ShouldScheduleFlush && mem.MarkFlushScheduled()) _db._flushScheduler.Add(c);
            }
            Sequence++;
        }

        public void Put(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => Add(columnFamilyId, EntryType.Value, key, value);

        [SkipLocalsInit]
        public void TimedPut(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ulong writeUnixTime)
        {
            // PackValueAndWriteTime: value | fixed64(write_unix_time), stored as kTypeValuePreferredSeqno.
            byte[] buf = System.Buffers.ArrayPool<byte>.Shared.Rent(value.Length + 8);
            try
            {
                value.CopyTo(buf);
                Util.Coding.EncodeFixed64(buf.AsSpan(value.Length), writeUnixTime);
                Add(columnFamilyId, EntryType.ValuePreferredSeqno, key, buf.AsSpan(0, value.Length + 8));
            }
            finally
            {
                System.Buffers.ArrayPool<byte>.Shared.Return(buf);
            }
        }

        public void PutEntity(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> entity) =>
            ThrowHelper.NotSupported("PutEntity (wide columns) is not supported yet");

        public void Delete(uint columnFamilyId, ReadOnlySpan<byte> key) => Add(columnFamilyId, EntryType.Deletion, key, default);
        public void SingleDelete(uint columnFamilyId, ReadOnlySpan<byte> key) => Add(columnFamilyId, EntryType.SingleDeletion, key, default);
        public void DeleteRange(uint columnFamilyId, ReadOnlySpan<byte> beginKey, ReadOnlySpan<byte> endKey) =>
            Add(columnFamilyId, EntryType.RangeDeletion, beginKey, endKey);
        public void Merge(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => Add(columnFamilyId, EntryType.Merge, key, value);

        public void PutBlobIndex(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> blobIndex) =>
            ThrowHelper.NotSupported("Blob files are not supported yet");

        public readonly void LogData(ReadOnlySpan<byte> blob) { }
        public readonly void MarkBeginPrepare(bool unprepare) => ThrowHelper.NotSupported("Two-phase commit is not supported yet");
        public readonly void MarkEndPrepare(ReadOnlySpan<byte> xid) => ThrowHelper.NotSupported("Two-phase commit is not supported yet");
        public readonly void MarkNoop(bool emptyBatch) { }
        public readonly void MarkCommit(ReadOnlySpan<byte> xid) => ThrowHelper.NotSupported("Two-phase commit is not supported yet");
        public readonly void MarkCommitWithTimestamp(ReadOnlySpan<byte> xid, ReadOnlySpan<byte> commitTimestamp) =>
            ThrowHelper.NotSupported("Two-phase commit is not supported yet");
        public readonly void MarkRollback(ReadOnlySpan<byte> xid) => ThrowHelper.NotSupported("Two-phase commit is not supported yet");
    }
}
