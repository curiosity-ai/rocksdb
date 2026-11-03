using System.Buffers;
using RocksDb.Managed.Db;

namespace RocksDb.Managed.Utilities.Transactions;

/// <summary>
/// A transaction of a WriteUnprepared transaction DB. Port of WriteUnpreparedTxn
/// (utilities/transactions/write_unprepared_txn.cc):
/// <list type="bullet">
/// <item>Before a write, a batch larger than write_batch_flush_threshold is written to the DB (WAL and memtables) as an
/// unprepared section (<c>kTypeBeginUnprepareXID</c> ... EndPrepare(xid)); the transaction remembers the sequence
/// numbers and sub-batch counts of its unprepared batches (<c>unprep_seqs_</c>). Prepare writes the rest as the final,
/// prepared section.</item>
/// <item>Reads see the transaction's own unprepared data (<see cref="WriteUnpreparedTxnReadCallback"/>).</item>
/// <item>Save points taken before a flush are rolled back by writing the keys' values at the save point
/// (<c>RollbackToSavePointInternal</c>); a rollback writes the values the transaction's keys had before it.</item>
/// <item>Commit marks every unprepared batch committed (WAL-only, with the commit-time batch when
/// use_only_the_last_commit_time_batch_for_recovery allows one).</item>
/// <item>With two_write_queues a rollback writes its batch on the main queue and commits it (with the unprepared batches)
/// by a second, data-less write on the WAL-only queue that publishes the commit sequence.</item>
/// </list>
/// Not ported: iterator invalidation at the end of the transaction.
/// </summary>
internal sealed class WriteUnpreparedTxn : WritePreparedTxn
{
    private long _writeBatchFlushThreshold;
    /// <summary>unprep_seqs_: first sequence number -&gt; sub-batch count of each batch written to the DB.</summary>
    private readonly SortedDictionary<ulong, int> _unprepSeqs = new();
    private ulong _lastLogNumber;
    private bool _recoveredTxn;
    /// <summary>largest_validated_seq_: the largest sequence number up to which this transaction's writes were validated.</summary>
    private ulong _largestValidatedSeq;
    // flushed_save_points_ (unprep_seqs_ copy + snapshot at the save point) / unflushed_save_points_ (batch sizes).
    private List<(SortedDictionary<ulong, int> UnprepSeqs, Snapshot Snapshot)>? _flushedSavePoints;
    private List<int>? _unflushedSavePoints;
    private readonly List<Iterator> _activeIterators = new();
    /// <summary>untracked_keys_: keys written without a lock (they must be rolled back too).</summary>
    private readonly Dictionary<uint, List<byte[]>> _untrackedKeys = new();
    private readonly UnpreparedCommitEntryPreReleaseCallback _commitEntry;
    private readonly AddPreparedCallback _addPreparedU;

    public WriteUnpreparedTxn(TransactionDatabase txnDb, WritePreparedTxnDb wpDb, WriteOptions? writeOptions, TransactionOptions options)
        : base(txnDb, wpDb, writeOptions, options)
    {
        _commitEntry = new UnpreparedCommitEntryPreReleaseCallback(wpDb);
        _addPreparedU = new AddPreparedCallback(wpDb);
        _writeBatchFlushThreshold = options.WriteBatchFlushThreshold < 0 ? txnDb.Options.DefaultWriteBatchFlushThreshold : options.WriteBatchFlushThreshold;
    }

    private protected override void Initialize(TransactionOptions options)
    {
        base.Initialize(options);
        _writeBatchFlushThreshold = options.WriteBatchFlushThreshold < 0 ? _txnDb.Options.DefaultWriteBatchFlushThreshold : options.WriteBatchFlushThreshold;
        _unprepSeqs?.Clear();
        _flushedSavePoints = null;
        _unflushedSavePoints = null;
        _recoveredTxn = false;
        _largestValidatedSeq = 0;
        _activeIterators?.Clear();
        _untrackedKeys?.Clear();
    }

    /// <summary>GetUnpreparedSequenceNumbers (tests).</summary>
    internal SortedDictionary<ulong, int> UnpreparedSequenceNumbers => _unprepSeqs;

    /// <summary>recovered_txn_: rebuilt from the WAL at open (its keys are tracked but hold no locks).</summary>
    internal bool IsRecovered => _recoveredTxn;

    /// <summary>GetLastLogNumber: the WAL of the last batch written.</summary>
    internal ulong LastLogNumber => _lastLogNumber;

    // ---- writes (HandleWrite) -----------------------------------------------------------------------------

    private protected override void BeforeWrite()
    {
        PruneIterators();
        if (_activeIterators.Count == 0) MaybeFlushWriteBatchToDb();
    }

    private protected override void AfterWrite()
    {
        if (Snapshot is { } snap) _largestValidatedSeq = Math.Max(_largestValidatedSeq, snap.SequenceNumber);
        else _largestValidatedSeq = _db.Versions.LastSequence;
    }

    private void PruneIterators() => _activeIterators.RemoveAll(static it => it.IsDisposed);

    /// <summary>MaybeFlushWriteBatchToDB: writes the batch as an unprepared batch once it exceeds the threshold.</summary>
    private void MaybeFlushWriteBatchToDb()
    {
        if (_writeBatchFlushThreshold > 0 && _batch.Count > 0 && _batch.DataSize > _writeBatchFlushThreshold)
        {
            FlushWriteBatchToDb(prepared: false, synchronous: true, CancellationToken.None).GetAwaiter().GetResult();
        }
    }

    private ValueTask FlushWriteBatchToDb(bool prepared, bool synchronous, CancellationToken cancellationToken)
    {
        if (!prepared && _unflushedSavePoints is { Count: > 0 }) return FlushWriteBatchWithSavePointToDb(synchronous, cancellationToken);
        return FlushWriteBatchToDbInternal(prepared, synchronous, cancellationToken);
    }

    /// <summary>
    /// FlushWriteBatchToDBInternal: writes the batch as an unprepared (or the final prepared) section, adds its
    /// sub-batches to the prepared heap and records it in unprep_seqs_; an unprepared batch is then cleared.
    /// </summary>
    private async ValueTask FlushWriteBatchToDbInternal(bool prepared, bool synchronous, CancellationToken cancellationToken)
    {
        if (_name is null) throw Invalid("Cannot write to DB without SetName.");
        // Keys written without a lock are remembered for rollback.
        var untracked = new UntrackedKeyHandler(this);
        _batch.Batch.Iterate(ref untracked);
        WriteOptions wo = _writeOptions.DisableWal ? CloneWithWal(_writeOptions) : _writeOptions;
        bool firstPrepareBatch = _logNumber == 0;
        // MarkEndPrepare will change Noop marker to the appropriate marker.
        _batch.Batch.MarkEndPrepare(_nameBytes!, writeAfterCommit: false, unpreparedBatch: !prepared);
        _prepareBatchCnt = _batch.SubBatchCount;
        _addPreparedU.Set(_prepareBatchCnt, firstPrepareBatch);
        _writeContext.Reset();
        _writeContext.BatchCount = _prepareBatchCnt;
        _writeContext.PreRelease = _addPreparedU;
        ulong prepareSeq;
        try
        {
            ValueTask w = _db.WriteAsync(wo, _batch.Batch, validateColumnFamilies: true, _writeContext, synchronous, cancellationToken);
            if (!w.IsCompletedSuccessfully) await w.ConfigureAwait(false);
            _lastLogNumber = _writeContext.LogUsed;
            prepareSeq = _writeContext.SequenceUsed;
        }
        finally
        {
            _writeContext.Reset();
        }
        if (_logNumber == 0) _logNumber = _lastLogNumber;
        // We set the first prepare_seq as the transaction id
        if (PrepareSequence == 0) PrepareSequence = prepareSeq;
        _unprepSeqs[prepareSeq] = _prepareBatchCnt;
        if (!prepared)
        {
            // When unprepared batch is written, need to reset the batch.
            _prepareBatchCnt = 0;
            _batch.Clear();
        }
    }

    /// <summary>UntrackedKeyHandler: remembers the keys of the batch that this transaction did not lock.</summary>
    private readonly struct UntrackedKeyHandler(WriteUnpreparedTxn txn) : IWriteBatchHandler
    {
        private void AddUntrackedKey(uint cf, ReadOnlySpan<byte> key)
        {
            if (txn._tracked.TryGet(cf, key, out _)) return;
            if (!txn._untrackedKeys.TryGetValue(cf, out List<byte[]>? list)) txn._untrackedKeys[cf] = list = new List<byte[]>();
            list.Add(key.ToArray());
        }

        public bool Continue() => true;
        public void Put(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => AddUntrackedKey(columnFamilyId, key);
        public void TimedPut(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ulong writeUnixTime) => AddUntrackedKey(columnFamilyId, key);
        public void PutEntity(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> entity) => AddUntrackedKey(columnFamilyId, key);
        public void Delete(uint columnFamilyId, ReadOnlySpan<byte> key) => AddUntrackedKey(columnFamilyId, key);
        public void SingleDelete(uint columnFamilyId, ReadOnlySpan<byte> key) => AddUntrackedKey(columnFamilyId, key);
        public void DeleteRange(uint columnFamilyId, ReadOnlySpan<byte> beginKey, ReadOnlySpan<byte> endKey) => ThrowHelper.InvalidArgument("DeleteRange in a transaction");
        public void Merge(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            if (txn._wpDb.RollbackMergeOperands) AddUntrackedKey(columnFamilyId, key);
        }
        public void PutBlobIndex(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> blobIndex) => AddUntrackedKey(columnFamilyId, key);
        public void LogData(ReadOnlySpan<byte> blob) { }
        public void MarkBeginPrepare(bool unprepare) => ThrowHelper.InvalidArgument("unexpected marker in a transaction batch");
        public void MarkEndPrepare(ReadOnlySpan<byte> xid) => ThrowHelper.InvalidArgument("unexpected marker in a transaction batch");
        public void MarkNoop(bool emptyBatch)
        {
            if (!emptyBatch) ThrowHelper.InvalidArgument("unexpected marker in a transaction batch");
        }
        public void MarkCommit(ReadOnlySpan<byte> xid) => ThrowHelper.InvalidArgument("unexpected marker in a transaction batch");
        public void MarkCommitWithTimestamp(ReadOnlySpan<byte> xid, ReadOnlySpan<byte> commitTimestamp) => ThrowHelper.InvalidArgument("unexpected marker in a transaction batch");
        public void MarkRollback(ReadOnlySpan<byte> xid) => ThrowHelper.InvalidArgument("unexpected marker in a transaction batch");
    }

    /// <summary>
    /// FlushWriteBatchWithSavePointToDB: the batch is split at the unflushed save points and each part written as
    /// its own unprepared batch, so that each save point becomes a flushed save point (unprep_seqs_ + a snapshot).
    /// </summary>
    private async ValueTask FlushWriteBatchWithSavePointToDb(bool synchronous, CancellationToken cancellationToken)
    {
        List<int> unflushed = _unflushedSavePoints!;
        using var wb = new WriteBatch();
        wb.SetContents(_batch.Batch.Data);
        _batch.Clear();
        int prevBoundary = WriteBatch.HeaderSize;
        for (int i = 0; i < unflushed.Count + 1; i++)
        {
            bool trailingBatch = i == unflushed.Count;
            int currBoundary = trailingBatch ? wb.DataSize : unflushed[i];
            // Construct a batch with just the data between this savepoint and the previous one.
            var handler = new SavePointBatchHandler(_batch);
            wb.Iterate(prevBoundary, currBoundary, ref handler);
            if (_batch.Count > 0) await FlushWriteBatchToDbInternal(prepared: false, synchronous, cancellationToken).ConfigureAwait(false);
            if (!trailingBatch)
            {
                (_flushedSavePoints ??= new()).Add((new SortedDictionary<ulong, int>(_unprepSeqs), _database.GetSnapshot()));
            }
            prevBoundary = currBoundary;
            _batch.Clear();
        }
        unflushed.Clear();
    }

    private readonly struct SavePointBatchHandler(IndexedWriteBatch wb) : IWriteBatchHandler
    {
        public bool Continue() => true;
        public void Put(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => wb.Put(columnFamilyId, key, value);
        public void TimedPut(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ulong writeUnixTime) =>
            ThrowHelper.NotSupported("TimedPut in a transaction is not supported");
        public void PutEntity(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> entity) => wb.PutEntitySerialized(columnFamilyId, key, entity);
        public void Delete(uint columnFamilyId, ReadOnlySpan<byte> key) => wb.Delete(columnFamilyId, key);
        public void SingleDelete(uint columnFamilyId, ReadOnlySpan<byte> key) => wb.SingleDelete(columnFamilyId, key);
        public void DeleteRange(uint columnFamilyId, ReadOnlySpan<byte> beginKey, ReadOnlySpan<byte> endKey) => ThrowHelper.InvalidArgument("DeleteRange in a transaction");
        public void Merge(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => wb.Merge(columnFamilyId, key, value);
        public void PutBlobIndex(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> blobIndex) =>
            ThrowHelper.NotSupported("Blob indexes in a transaction are not supported");
        public void LogData(ReadOnlySpan<byte> blob) => wb.PutLogData(blob);
        public void MarkBeginPrepare(bool unprepare) => ThrowHelper.InvalidArgument("unexpected marker in a transaction batch");
        public void MarkEndPrepare(ReadOnlySpan<byte> xid) => ThrowHelper.InvalidArgument("unexpected marker in a transaction batch");
        public void MarkNoop(bool emptyBatch)
        {
            if (!emptyBatch) ThrowHelper.InvalidArgument("unexpected marker in a transaction batch");
        }
        public void MarkCommit(ReadOnlySpan<byte> xid) => ThrowHelper.InvalidArgument("unexpected marker in a transaction batch");
        public void MarkCommitWithTimestamp(ReadOnlySpan<byte> xid, ReadOnlySpan<byte> commitTimestamp) => ThrowHelper.InvalidArgument("unexpected marker in a transaction batch");
        public void MarkRollback(ReadOnlySpan<byte> xid) => ThrowHelper.InvalidArgument("unexpected marker in a transaction batch");
    }

    // ---- prepare / commit / rollback ----------------------------------------------------------------------

    private protected override ValueTask PrepareInternalAsync(bool synchronous, CancellationToken cancellationToken) =>
        FlushWriteBatchToDb(prepared: true, synchronous, cancellationToken);

    private protected override async ValueTask CommitWithoutPrepareInternalAsync(bool synchronous, CancellationToken cancellationToken)
    {
        if (_unprepSeqs.Count == 0)
        {
            await base.CommitWithoutPrepareInternalAsync(synchronous, cancellationToken).ConfigureAwait(false);
            return;
        }
        // TODO(lth): We should optimize commit without prepare to not perform a prepare under the hood.
        await PrepareInternalAsync(synchronous, cancellationToken).ConfigureAwait(false);
        await CommitInternalAsync(synchronous, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>WriteUnpreparedTxn::CommitInternal: a commit marker that commits every unprepared batch.</summary>
    private protected override async ValueTask CommitInternalAsync(bool synchronous, CancellationToken cancellationToken)
    {
        // A commit-time batch that would go to the memtables (includes_data) is refused, so the commit is one write
        // (do_one_write); with two write queues it runs on the WAL-only queue and its callback publishes the commit.
        WriteBatch working = CommitMarkerWithCommitTimeBatch();
        _commitEntry.Set(_unprepSeqs, 0);
        _writeContext.Reset();
        _writeContext.DisableMemtable = true;
        _writeContext.BatchCount = 1;
        _writeContext.PreRelease = _commitEntry;
        try
        {
            ValueTask w = _db.WriteAsync(_writeOptions, working, validateColumnFamilies: false, _writeContext, synchronous, cancellationToken);
            if (!w.IsCompletedSuccessfully) await w.ConfigureAwait(false);
        }
        finally
        {
            _writeContext.Reset();
        }
        foreach (KeyValuePair<ulong, int> seq in _unprepSeqs) _wpDb.RemovePrepared(seq.Key, seq.Value);
        _unprepSeqs.Clear();
        ReleaseFlushedSavePoints();
        _unflushedSavePoints = null;
    }

    /// <summary>
    /// WriteUnpreparedTxn::RollbackInternal: restores the values (read with a callback that sees neither this
    /// transaction's data nor anything uncommitted) of every key this transaction locked or wrote untracked, followed by
    /// a rollback marker; the write commits the unprepared batches and itself.
    /// </summary>
    private protected override async ValueTask RollbackInternalAsync(bool synchronous, CancellationToken cancellationToken)
    {
        // TODO(lth): Reduce duplicate code with WritePrepared rollback logic.
        using var rollbackBatch = new WriteBatchWithIndex(0, withNoop: false);
        var callback = new WritePreparedTxnReadCallback();
        callback.Init(_wpDb, DbFormat.MaxSequenceNumber, ReadCallback.MinUnCommittedSeq, SnapshotBackup.BackedByDbSnapshot);
        WriteRollbackKeys(_tracked, includeUntracked: true, rollbackBatch, callback, null);
        // The Rollback marker will be used as a batch separator
        rollbackBatch.WriteBatch.MarkRollback(_nameBytes!);
        bool doOneWrite = !TwoWriteQueues;
        // Rollback batch may contain duplicate keys, because tracked_keys_ is not comparator aware.
        int rollbackBatchCnt = rollbackBatch.SubBatchCount;
        _commitEntry.Set(_unprepSeqs, rollbackBatchCnt);
        _writeContext.Reset();
        _writeContext.BatchCount = rollbackBatchCnt;
        if (doOneWrite)
        {
            // The rollback batch does not need AddPrepared since it is written to DB in one shot.
            _writeContext.PreRelease = _commitEntry;
        }
        else
        {
            // Deviation from upstream (which passes no callback here): with two write queues the rollback batch is
            // committed by the 2nd write, so until then it is uncommitted data like the unprepared batches it cancels
            // and must be in the prepared heap, as WritePreparedTxn::RollbackInternal does for its rollback batch.
            // Otherwise once max_evicted_seq passes it, readers and compactions take it as committed while the
            // rolled back batches are not: snapshots see half of the rollback, and a compaction keeps an older
            // version below the "committed" rollback value and zeroes both sequence numbers.
            _addPreparedU.Set(rollbackBatchCnt, firstPrepareBatch: false);
            _writeContext.PreRelease = _addPreparedU;
        }
        ulong prepareSeq;
        try
        {
            ValueTask w = _db.WriteAsync(_writeOptions, rollbackBatch.WriteBatch, validateColumnFamilies: false, _writeContext, synchronous, cancellationToken);
            if (!w.IsCompletedSuccessfully) await w.ConfigureAwait(false);
            prepareSeq = _writeContext.SequenceUsed;
        }
        finally
        {
            _writeContext.Reset();
        }
        if (!doOneWrite)
        {
            // Populate unprep_seqs_ with rollback_batch_cnt, since we treat data in the rollback write batch as just
            // another "unprepared" batch; the 2nd write commits them all and publishes the commit sequence.
            _unprepSeqs[prepareSeq] = rollbackBatchCnt;
            TestHooks.Invoke("WriteUnpreparedTxn::RollbackInternal:BeforeCommit");
            _commitEntry.Set(_unprepSeqs, 0);
            ValueTask c = WriteCommitOnlyAsync(_writeOptions, EmptyBatch(forWal: true), _commitEntry, synchronous, cancellationToken);
            if (!c.IsCompletedSuccessfully) await c.ConfigureAwait(false);
        }
        foreach (KeyValuePair<ulong, int> seq in _unprepSeqs) _wpDb.RemovePrepared(seq.Key, seq.Value);
        _unprepSeqs.Clear();
        ReleaseFlushedSavePoints();
        _unflushedSavePoints = null;
    }

    /// <summary>WriteRollbackKeys: one Put / Delete per key into <paramref name="rollbackBatch"/>.</summary>
    private void WriteRollbackKeys(LockTracker trackedKeys, bool includeUntracked, WriteBatchWithIndex rollbackBatch, ReadCallback callback,
        ReadOptions? options)
    {
        using var scratch = new WriteBatch();
        for (int i = 0; i < trackedKeys.ColumnFamilyCount; i++)
        {
            TrackedColumnFamily c = trackedKeys.GetColumnFamily(i);
            foreach (PooledKey key in c.Keys.Keys) WriteRollbackKey(rollbackBatch, c.Cfd, key.Span, options, callback);
        }
        if (!includeUntracked) return;
        foreach (KeyValuePair<uint, List<byte[]>> cfKeys in _untrackedKeys)
        {
            ColumnFamilyData cfd = _db.GetColumnFamilyDataById(cfKeys.Key);
            foreach (byte[] key in cfKeys.Value) WriteRollbackKey(rollbackBatch, cfd, key, options, callback);
        }
    }

    private void WriteRollbackKey(WriteBatchWithIndex dst, ColumnFamilyData cfd, ReadOnlySpan<byte> key, ReadOptions? options, ReadCallback callback)
    {
        dst.RegisterColumnFamily(cfd.Id, cfd.UserComparator);
        Util.PooledBufferWriter value = Util.ObjectPools.RentWriter();
        try
        {
            if (_db.GetWithCallback(cfd, options, key, callback, value)) dst.Put(cfd.Id, key, value.WrittenSpan);
            else dst.Delete(cfd.Id, key);
        }
        finally
        {
            Util.ObjectPools.ReturnWriter(value);
        }
    }

    private void ReleaseFlushedSavePoints()
    {
        if (_flushedSavePoints is null) return;
        foreach ((_, Snapshot snap) in _flushedSavePoints) snap.Dispose();
        _flushedSavePoints = null;
    }

    private protected override void Clear()
    {
        _unprepSeqs.Clear();
        ReleaseFlushedSavePoints();
        _unflushedSavePoints = null;
        _recoveredTxn = false;
        _largestValidatedSeq = 0;
        _activeIterators.Clear();
        _untrackedKeys.Clear();
        // Recovered transactions hold no locks (unlocking keys this transaction does not hold is a no-op).
        base.Clear();
    }

    /// <summary>
    /// ~WriteUnpreparedTxn: an unprepared transaction that wrote batches to the DB and was neither prepared nor
    /// finished is rolled back.
    /// </summary>
    private protected override void DisposeWritePolicy()
    {
        if (_unprepSeqs.Count > 0 && State is TransactionState.Started or TransactionState.LocksStolen)
        {
            try
            {
                RollbackInternalAsync(synchronous: true, CancellationToken.None).GetAwaiter().GetResult();
                _db.MarkLogAsHavingPrepSectionFlushed(_logNumber);
            }
            catch (Exception e) when (e is RocksDbException or ObjectDisposedException or IOException)
            {
                // Rollback of WriteUnprepared transaction failed in destructor.
            }
        }
        ReleaseFlushedSavePoints();
    }

    // ---- save points --------------------------------------------------------------------------------------

    public override void SetSavePoint()
    {
        base.SetSavePoint();
        (_unflushedSavePoints ??= new List<int>()).Add(_batch.DataSize);
    }

    public override bool RollbackToSavePoint()
    {
        if (_unflushedSavePoints is { Count: > 0 } unflushed)
        {
            bool ok = base.RollbackToSavePoint();
            unflushed.RemoveAt(unflushed.Count - 1);
            return ok;
        }
        if (_flushedSavePoints is { Count: > 0 })
        {
            if (State != TransactionState.Started) throw Invalid("Transaction is beyond state for rollback.");
            RollbackToSavePointInternal();
            return true;
        }
        return false;
    }

    /// <summary>
    /// RollbackToSavePointInternal: writes, as a new unprepared batch, the values the keys tracked since the save point
    /// had at the save point (read at its snapshot with its unprep_seqs_).
    /// </summary>
    private void RollbackToSavePointInternal()
    {
        _batch.Clear();
        (SortedDictionary<ulong, int> topSeqs, Snapshot topSnapshot) = _flushedSavePoints![^1];
        LockTracker trackedKeys = TopSavePointLocks;
        var options = new ReadOptions { Snapshot = topSnapshot };
        var callback = new WriteUnpreparedTxnReadCallback(_wpDb, topSnapshot.SequenceNumber, topSnapshot.MinUncommitted, topSeqs,
            SnapshotBackup.BackedByDbSnapshot);
        using var restore = new WriteBatchWithIndex(0, withNoop: false);
        WriteRollbackKeys(trackedKeys, includeUntracked: false, restore, callback, options);
        var copy = new SavePointBatchHandler(_batch);
        restore.WriteBatch.Iterate(ref copy);
        FlushWriteBatchToDbInternal(prepared: false, synchronous: true, CancellationToken.None).GetAwaiter().GetResult();
        // PessimisticTransaction::RollbackToSavePoint will call also RollbackToSavePoint on write_batch_. Make sure we
        // have a savepoint there to roll back to.
        _batch.SetSavePoint();
        base.RollbackToSavePoint();
        _flushedSavePoints.RemoveAt(_flushedSavePoints.Count - 1);
        topSnapshot.Dispose();
    }

    public override bool PopSavePoint()
    {
        if (_unflushedSavePoints is { Count: > 0 } unflushed)
        {
            bool ok = base.PopSavePoint();
            unflushed.RemoveAt(unflushed.Count - 1);
            return ok;
        }
        if (_flushedSavePoints is { Count: > 0 } flushed)
        {
            // PessimisticTransaction::PopSavePoint will call also PopSavePoint on write_batch_. Make sure we have a
            // savepoint there to roll back to.
            _batch.SetSavePoint();
            bool ok = base.PopSavePoint();
            flushed[^1].Snapshot.Dispose();
            flushed.RemoveAt(flushed.Count - 1);
            return ok;
        }
        return false;
    }

    // ---- reads --------------------------------------------------------------------------------------------

    private WriteUnpreparedTxnReadCallback NewReadCallback(ReadOptions? options, out SnapshotBackup backed, out ulong snapSeq)
    {
        backed = _wpDb.AssignMinMaxSeqs(options?.Snapshot, out ulong minUncommitted, out snapSeq);
        return new WriteUnpreparedTxnReadCallback(_wpDb, snapSeq, minUncommitted, _unprepSeqs, backed);
    }

    /// <summary>WriteUnpreparedTxn::GetImpl's DB part: the read sees this transaction's unprepared batches.</summary>
    private protected override bool GetFromDb(ReadOptions? options, ColumnFamilyData cfd, ReadOnlySpan<byte> key, IBufferWriter<byte> value)
    {
        WriteUnpreparedTxnReadCallback cb = NewReadCallback(options, out SnapshotBackup backed, out ulong snapSeq);
        bool found = _db.GetWithCallback(cfd, options, key, cb, value);
        if (!cb.Valid || !_wpDb.ValidateSnapshot(snapSeq, backed)) throw _wpDb.TryAgain();
        return found;
    }

    private protected override ValueTask<bool> GetFromDbAsync(ReadOptions? options, ColumnFamilyData cfd, ReadOnlyMemory<byte> key, IBufferWriter<byte> value,
        CancellationToken cancellationToken) =>
        new(GetFromDb(options, cfd, key.Span, value));

    /// <summary>WriteUnpreparedTxn::GetEntityImpl's DB part: the read sees this transaction's unprepared batches.</summary>
    private protected override bool GetEntityFromDb(ReadOptions? options, ColumnFamilyData cfd, ReadOnlySpan<byte> key, WideColumns columns)
    {
        WriteUnpreparedTxnReadCallback cb = NewReadCallback(options, out SnapshotBackup backed, out ulong snapSeq);
        bool found = _db.GetEntityWithCallback(cfd, options, key, cb, columns);
        if (!cb.Valid || !_wpDb.ValidateSnapshot(snapSeq, backed)) throw _wpDb.TryAgain();
        return found;
    }

    private protected override ValueTask<bool> GetEntityFromDbAsync(ReadOptions? options, ColumnFamilyData cfd, ReadOnlyMemory<byte> key, WideColumns columns,
        CancellationToken cancellationToken) =>
        new(GetEntityFromDb(options, cfd, key.Span, columns));

    /// <summary>
    /// WriteUnpreparedTxn::MultiGetEntityImpl's DB part: every key read with one read callback (one snapshot) that sees
    /// this transaction's unprepared batches.
    /// </summary>
    private protected override void MultiGetEntityFromDb(ReadOptions? options, ColumnFamilyData cfd, MultiGetBatch keys)
    {
        WriteUnpreparedTxnReadCallback cb = NewReadCallback(options, out SnapshotBackup backed, out ulong snapSeq);
        var results = new (bool Found, byte[]? Entity, RocksDbException? Error)[keys.Count];
        var columns = new WideColumns();
        for (int i = 0; i < keys.Count; i++)
        {
            try
            {
                bool found = _db.GetEntityWithCallback(cfd, options, keys.GetKey(i), cb, columns);
                results[i] = (found, found ? columns.Serialized.ToArray() : null, null);
            }
            catch (RocksDbException e)
            {
                results[i] = (false, null, e);
            }
        }
        if (!cb.Valid || !_wpDb.ValidateSnapshot(snapSeq, backed)) throw _wpDb.TryAgain();
        keys.EntityRun = true;
        keys.BeginExternalRun(cfd.UserComparator);
        for (int i = 0; i < results.Length; i++)
        {
            if (results[i].Error is { } e) keys.SetError(i, e);
            else keys.SetExternalResult(i, results[i].Found, results[i].Entity);
        }
    }

    /// <summary>WriteUnpreparedTxnDB::NewIterator: reads at a snapshot plus this transaction's unprepared batches.</summary>
    private protected override Iterator NewDbIterator(ColumnFamily handle, ColumnFamilyData cfd, ReadOptions? options)
    {
        Snapshot? own = options?.Snapshot is null ? _database.GetSnapshot() : null;
        Snapshot snapshot = options?.Snapshot ?? own!;
        // TODO(lth): Implement reading from unvalidated writes.
        if (_largestValidatedSeq > snapshot.SequenceNumber && _unprepSeqs.Count > 0)
        {
            own?.Dispose();
            throw new RocksDbException(StatusCode.InvalidArgument,
                "Invalid argument: WriteUnprepared iterator creation failed since the transaction has performed unvalidated writes");
        }
        var cb = new WriteUnpreparedTxnReadCallback(_wpDb, snapshot.SequenceNumber, snapshot.MinUncommitted, _unprepSeqs, SnapshotBackup.BackedByDbSnapshot);
        Iterator it = _db.NewIteratorWithCallback(cfd, options, cb, cb.MaxVisibleSeq, own);
        PruneIterators();
        _activeIterators.Add(it);
        return it;
    }

    private protected override ReadCallback NewValidationCallback(ulong snapSeq, ulong minUncommitted) =>
        new WriteUnpreparedTxnReadCallback(_wpDb, snapSeq, minUncommitted, _unprepSeqs, SnapshotBackup.BackedByDbSnapshot);

    // ---- recovery -----------------------------------------------------------------------------------------

    /// <summary>
    /// WriteUnpreparedTxnDB::Initialize for one recovered prepared transaction: every batch becomes an unprepared batch
    /// of the transaction (unprep_seqs_), its keys are tracked for rollback, and the transaction is prepared.
    /// </summary>
    internal override void RebuildPrepared(RecoveredTransaction recovered)
    {
        _recoveredTxn = true;
        _logNumber = recovered.LogNumber;
        PrepareSequence = recovered.Sequence;
        SetName(recovered.Name);
        _prepareBatchCnt = recovered.Batches[0].BatchCount;
        for (int i = 0; i < recovered.Batches.Count; i++)
        {
            RecoveredTransaction.BatchInfo b = recovered.Batches[i];
            _unprepSeqs[recovered.Sequences[i]] = b.BatchCount != 0 ? b.BatchCount : 1;
            var tracker = new TrackKeyHandler(this);
            b.Batch.Iterate(ref tracker);
        }
        _batch.Clear();
        SetState(TransactionState.Prepared);
    }

    /// <summary>RebuildFromWriteBatch's TrackKeyHandler: tracks the keys of a recovered batch (for rollback).</summary>
    private readonly struct TrackKeyHandler(WriteUnpreparedTxn txn) : IWriteBatchHandler
    {
        private void Track(uint cf, ReadOnlySpan<byte> key) =>
            txn.TrackKey(txn._db.GetColumnFamilyDataById(cf), key, DbFormat.MaxSequenceNumber, readOnly: false, exclusive: true);

        public bool Continue() => true;
        public void Put(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => Track(columnFamilyId, key);
        public void TimedPut(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ulong writeUnixTime) => Track(columnFamilyId, key);
        public void PutEntity(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> entity) => Track(columnFamilyId, key);
        public void Delete(uint columnFamilyId, ReadOnlySpan<byte> key) => Track(columnFamilyId, key);
        public void SingleDelete(uint columnFamilyId, ReadOnlySpan<byte> key) => Track(columnFamilyId, key);
        public void DeleteRange(uint columnFamilyId, ReadOnlySpan<byte> beginKey, ReadOnlySpan<byte> endKey) { }
        public void Merge(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            if (txn._wpDb.RollbackMergeOperands) Track(columnFamilyId, key);
        }
        public void PutBlobIndex(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> blobIndex) => Track(columnFamilyId, key);
        public void LogData(ReadOnlySpan<byte> blob) { }
        public void MarkBeginPrepare(bool unprepare) => ThrowHelper.InvalidArgument("unexpected marker in a recovered batch");
        public void MarkEndPrepare(ReadOnlySpan<byte> xid) => ThrowHelper.InvalidArgument("unexpected marker in a recovered batch");
        public void MarkNoop(bool emptyBatch) => ThrowHelper.InvalidArgument("unexpected marker in a recovered batch");
        public void MarkCommit(ReadOnlySpan<byte> xid) => ThrowHelper.InvalidArgument("unexpected marker in a recovered batch");
        public void MarkCommitWithTimestamp(ReadOnlySpan<byte> xid, ReadOnlySpan<byte> commitTimestamp) => ThrowHelper.InvalidArgument("unexpected marker in a recovered batch");
        public void MarkRollback(ReadOnlySpan<byte> xid) => ThrowHelper.InvalidArgument("unexpected marker in a recovered batch");
    }
}

/// <summary>
/// Port of WriteUnpreparedTxnReadCallback (write_unprepared_txn.h): like <see cref="WritePreparedTxnReadCallback"/>,
/// plus the transaction's own unprepared batches are visible.
/// </summary>
internal sealed class WriteUnpreparedTxnReadCallback : ReadCallback
{
    private readonly WritePreparedTxnDb _db;
    private readonly SortedDictionary<ulong, int> _unprepSeqs;
    private ulong _wupSnapshot;
    private bool _snapReleased;

    public WriteUnpreparedTxnReadCallback(WritePreparedTxnDb db, ulong snapshot, ulong minUncommitted, SortedDictionary<ulong, int> unprepSeqs,
        SnapshotBackup backed)
    {
        _db = db;
        _unprepSeqs = unprepSeqs;
        _wupSnapshot = snapshot;
        _minUncommitted = minUncommitted;
        _maxVisibleSeq = CalcMaxVisibleSeq(unprepSeqs, snapshot);
        Backed = backed;
    }

    public SnapshotBackup Backed { get; }

    public bool Valid => !_snapReleased;

    /// <summary>CalcMaxVisibleSeq: the snapshot or the last sequence of the newest unprepared batch, whichever is larger.</summary>
    private static ulong CalcMaxVisibleSeq(SortedDictionary<ulong, int> unprepSeqs, ulong snapshotSeq)
    {
        ulong maxUnprepared = 0;
        foreach (KeyValuePair<ulong, int> e in unprepSeqs) maxUnprepared = e.Key + (ulong)e.Value - 1; // ascending: the last wins
        return Math.Max(maxUnprepared, snapshotSeq);
    }

    protected override bool IsVisibleFullCheck(ulong seq)
    {
        // Since unprep_seqs maps prep_seq => prepare_batch_cnt, to check if seq is in unprep_seqs, we have to check if
        // seq is equal to prep_seq or any of the prepare_batch_cnt seq nums after it.
        foreach (KeyValuePair<ulong, int> e in _unprepSeqs)
        {
            if (e.Key <= seq && seq < e.Key + (ulong)e.Value) return true;
        }
        bool released = false;
        bool ret = _db.IsInSnapshot(seq, _wupSnapshot, _minUncommitted, ref released);
        _snapReleased |= released;
        return ret;
    }

    public override void Refresh(ulong seq)
    {
        _maxVisibleSeq = Math.Max(_maxVisibleSeq, seq);
        _wupSnapshot = seq;
    }
}
