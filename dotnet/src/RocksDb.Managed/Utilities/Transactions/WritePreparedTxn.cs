using System.Runtime.CompilerServices;
using RocksDb.Managed.Db;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Utilities.Transactions;

/// <summary>
/// A transaction of a WritePrepared transaction DB. Port of WritePreparedTxn (utilities/transactions/write_prepared_txn.cc):
/// <list type="bullet">
/// <item>Prepare writes the batch to the WAL <i>and the memtables</i> as a <c>kTypeBeginPersistedPrepareXID</c>
/// section at the prepare sequence number (one sequence per sub-batch); the sub-batches enter the prepared heap before
/// the write is published.</item>
/// <item>Commit writes a commit marker to the WAL (one sequence number, no memtable insert) and records the commit in
/// the commit cache; commit without prepare writes the batch with a trailing Noop and commits it in one write.</item>
/// <item>Rollback of a prepared transaction writes, for every key it wrote, the value the key had before (or a
/// delete) followed by a rollback marker, and commits both.</item>
/// <item>Reads and conflict checks use <see cref="WritePreparedTxnReadCallback"/>.</item>
/// <item>With two_write_queues, writes that carry data use the main queue and register their sub-batches as prepared
/// (AddPreparedCallback); a second, data-less write on the WAL-only queue then commits them and publishes the commit
/// sequence (CommitBatchInternal / RollbackInternal "2nd write").</item>
/// <item>A commit-time batch is only accepted with use_only_the_last_commit_time_batch_for_recovery: it is written to the
/// WAL with the commit marker and kept by the DB as its latest persistent state.</item>
/// </list>
/// </summary>
internal class WritePreparedTxn : PessimisticTransaction
{
    private protected readonly WritePreparedTxnDb _wpDb;

    /// <summary>WritePreparedTxn::GetCoalescingIterator / GetAttributeGroupIterator: NotSupported.</summary>
    private protected override bool SupportsMultiCfIterators => false;
    private readonly AddPreparedCallback _addPrepared;
    private readonly CommitEntryPreReleaseCallback _commitEntry;
    // Two write queues: the AddPreparedCallback of the first write and the callbacks of the second, WAL-only write.
    private readonly AddPreparedCallback _addPreparedData;
    private readonly RollbackPreReleaseCallback _rollbackCommit;
    private WriteBatch? _emptyBatch;
    /// <summary>prepare_batch_cnt_: the number of sub-batches of the prepared batch.</summary>
    private protected int _prepareBatchCnt;

    public WritePreparedTxn(TransactionDatabase txnDb, WritePreparedTxnDb wpDb, WriteOptions? writeOptions, TransactionOptions options)
        : base(txnDb, writeOptions, options)
    {
        _wpDb = wpDb;
        _addPrepared = new AddPreparedCallback(wpDb);
        _commitEntry = new CommitEntryPreReleaseCallback(wpDb);
        _addPreparedData = new AddPreparedCallback(wpDb);
        _rollbackCommit = new RollbackPreReleaseCallback(wpDb);
    }

    /// <summary>Options of the second write of WriteInternal with two write queues (no WAL, no sync).</summary>
    private static readonly WriteOptions s_commitOnlyNoWal = new() { DisableWal = true };

    private protected bool TwoWriteQueues => _db.TwoWriteQueues;

    /// <summary>
    /// The data-less batch of a second write: empty (WriteInternal, without WAL) or, when it goes to the WAL, a
    /// PutLogData("") followed by the Noop batch separator (CommitInternal / RollbackInternal).
    /// </summary>
    private protected WriteBatch EmptyBatch(bool forWal)
    {
        WriteBatch b = _emptyBatch ??= new WriteBatch();
        b.Clear();
        if (forWal)
        {
            b.PutLogData(default);
            // In the absence of Prepare markers, use Noop as a batch separator
            b.InsertNoop();
        }
        return b;
    }

    /// <summary>
    /// The second write with two write queues: a data-less write on the WAL-only queue (one sequence number) whose
    /// pre-release callback commits what the first write wrote and publishes the commit sequence.
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private protected async ValueTask WriteCommitOnlyAsync(WriteOptions options, WriteBatch batch, IPreReleaseCallback callback, bool synchronous,
        CancellationToken cancellationToken)
    {
        _writeContext.Reset();
        _writeContext.DisableMemtable = true;
        _writeContext.BatchCount = 1;
        _writeContext.PreRelease = callback;
        try
        {
            ValueTask w = _db.WriteAsync(options, batch, validateColumnFamilies: false, _writeContext, synchronous, cancellationToken);
            if (!w.IsCompletedSuccessfully) await w.ConfigureAwait(false);
        }
        finally
        {
            _writeContext.Reset();
        }
    }

    /// <summary>
    /// The commit-time batch with the commit marker appended (CommitInternal of WritePreparedTxn / WriteUnpreparedTxn):
    /// a non-empty commit-time batch requires use_only_the_last_commit_time_batch_for_recovery and becomes the latest
    /// persistent state. Returns the batch to write WAL-only.
    /// </summary>
    private protected WriteBatch CommitMarkerWithCommitTimeBatch()
    {
        WriteBatch working = WorkingCommitBatch();
        bool empty = working.Count == 0;
        if (!empty && !_useOnlyTheLastCommitTimeBatchForRecovery)
        {
            throw Invalid("Commit-time-batch can only be used if use_only_the_last_commit_time_batch_for_recovery is true");
        }
        // The Memtable will ignore the Commit marker in non-recovery mode
        working.MarkCommit(NameBytes);
        // When not writing to memtable, we can still cache the latest write batch. The cached batch will be written to
        // memtable in WriteRecoverableState during FlushMemTable.
        if (!empty) working.IsLatestPersistentState = true;
        return working;
    }

    /// <summary>GetId(): the sequence number of the (first) prepared batch, 0 before it was written.</summary>
    internal ulong PrepareSequence { get; private protected set; }

    private protected override void Initialize(TransactionOptions options)
    {
        base.Initialize(options);
        _prepareBatchCnt = 0;
        PrepareSequence = 0;
    }

    // WritePreparedTxn::CommitWithoutPrepareInternal / CommitInternal do not read snapshot_needed_.
    private protected override bool CommitChecksPendingSnapshotRequest => false;

    // WritePrepared transactions do not support user-defined timestamps (TransactionDB::Open fails with them).
    private protected override bool SupportsUserDefinedTimestamps => false;

    /// <summary>WritePreparedTxn::PrepareInternal.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private protected override async ValueTask PrepareInternalAsync(bool synchronous, CancellationToken cancellationToken)
    {
        WriteOptions wo = _writeOptions.DisableWal ? WithWal(_writeOptions) : _writeOptions;
        // For each duplicate key we account for a new sub-batch
        _batch.Batch.MarkEndPrepare(NameBytes, writeAfterCommit: false, unpreparedBatch: false);
        _prepareBatchCnt = _batch.SubBatchCount;
        _addPrepared.Set(_prepareBatchCnt, firstPrepareBatch: true);
        _writeContext.Reset();
        _writeContext.BatchCount = _prepareBatchCnt;
        _writeContext.PreRelease = _addPrepared;
        try
        {
            ValueTask w = _db.WriteAsync(wo, _batch.Batch, validateColumnFamilies: true, _writeContext, synchronous, cancellationToken);
            if (!w.IsCompletedSuccessfully) await w.ConfigureAwait(false);
            _logNumber = _writeContext.LogUsed;
            PrepareSequence = _writeContext.SequenceUsed;
        }
        finally
        {
            _writeContext.Reset();
        }
    }

    /// <summary>
    /// WritePreparedTxn::CommitWithoutPrepareInternal -> WritePreparedTxnDB::WriteInternal: one write committed by its
    /// pre-release callback, or with two write queues the batch as prepared data on the main queue followed by a
    /// WAL-less commit on the second queue.
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private protected override async ValueTask CommitWithoutPrepareInternalAsync(bool synchronous, CancellationToken cancellationToken)
    {
        int batchCnt = _batch.SubBatchCount;
        WriteBatch batch = _batch.Batch;
        if (batch.Count == 0) return;
        bool doOneWrite = !TwoWriteQueues;
        // InsertNoop: the batch boundary (the leading Noop placeholder of 2PC batches is ignored as an empty batch).
        batch.InsertNoop();
        _writeContext.Reset();
        _writeContext.BatchCount = batchCnt;
        if (doOneWrite)
        {
            _commitEntry.Set(DbFormat.MaxSequenceNumber, 0, batchCnt);
            _writeContext.PreRelease = _commitEntry;
        }
        else
        {
            // Since this is not 2pc, there is no need for AddPrepared but having it in the PreReleaseCallback enables an
            // optimization. Refer to SmallestUnCommittedSeq for more details.
            _addPreparedData.Set(batchCnt, firstPrepareBatch: false);
            _writeContext.PreRelease = _addPreparedData;
        }
        try
        {
            ValueTask w = _db.WriteAsync(_writeOptions, batch, validateColumnFamilies: true, _writeContext, synchronous, cancellationToken);
            if (!w.IsCompletedSuccessfully) await w.ConfigureAwait(false);
            PrepareSequence = _writeContext.SequenceUsed;
        }
        finally
        {
            _writeContext.Reset();
        }
        if (doOneWrite) return;
        // else do the 2nd write for commit: an empty batch on the 2nd queue releases the commit sequence to readers.
        _commitEntry.Set(PrepareSequence, batchCnt, 0);
        ValueTask c = WriteCommitOnlyAsync(s_commitOnlyNoWal, EmptyBatch(forWal: false), _commitEntry, synchronous, cancellationToken);
        if (!c.IsCompletedSuccessfully) await c.ConfigureAwait(false);
        // Note: RemovePrepared is called from within PreReleaseCallback
    }

    /// <summary>
    /// WritePreparedTxn::CommitInternal: the commit-time batch (if any) and a commit marker written to the WAL only (one
    /// sequence number); the pre-release callback adds the commit entries of the prepared sub-batches (and, with two
    /// write queues, publishes the commit and removes them from the prepared heap). A commit-time batch whose data
    /// would go to the memtables (includes_data) is refused by <see cref="CommitMarkerWithCommitTimeBatch"/>, so the
    /// commit is always a single write.
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private protected override async ValueTask CommitInternalAsync(bool synchronous, CancellationToken cancellationToken)
    {
        WriteBatch working = CommitMarkerWithCommitTimeBatch();
        ulong prepareSeq = PrepareSequence;
        _commitEntry.Set(prepareSeq, _prepareBatchCnt, 0);
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
        // Note: RemovePrepared should be called after WriteImpl that publishsed the seq. Otherwise
        // SmallestUnCommittedSeq optimization breaks. With two write queues it is called from within the callback.
        if (!TwoWriteQueues) _wpDb.RemovePrepared(prepareSeq, _prepareBatchCnt);
    }

    /// <summary>
    /// WritePreparedTxn::RollbackInternal: for every key of the prepared batch (merged keys only with
    /// rollback_merge_operands) the value visible before the transaction (read with a callback that does not see the
    /// prepared data) is written back, followed by a rollback marker; the write commits the prepared sub-batches and
    /// itself.
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private protected override async ValueTask RollbackInternalAsync(bool synchronous, CancellationToken cancellationToken)
    {
        using var rollbackBatch = new WriteBatch();
        var callback = new WritePreparedTxnReadCallback();
        // disable min_uncommitted optimization
        callback.Init(_wpDb, DbFormat.MaxSequenceNumber, ReadCallback.MinUnCommittedSeq, SnapshotBackup.BackedByDbSnapshot);
        var builder = new RollbackBatchBuilder(this, rollbackBatch, callback);
        _batch.Batch.Iterate(ref builder);
        // The Rollback marker will be used as a batch separator
        rollbackBatch.MarkRollback(NameBytes);
        ulong prepareSeq = PrepareSequence;
        bool doOneWrite = !TwoWriteQueues;
        _writeContext.Reset();
        _writeContext.BatchCount = 1;
        if (doOneWrite)
        {
            // We commit the rolled back prepared batches: they are canceled out by the rollback batch, and the commit
            // entries let live snapshots skip them even below max_evicted_seq.
            _commitEntry.Set(prepareSeq, _prepareBatchCnt, 1);
            _writeContext.PreRelease = _commitEntry;
        }
        else
        {
            _addPreparedData.Set(1, firstPrepareBatch: false);
            _writeContext.PreRelease = _addPreparedData;
        }
        ulong rollbackSeq;
        try
        {
            ValueTask w = _db.WriteAsync(_writeOptions, rollbackBatch, validateColumnFamilies: false, _writeContext, synchronous, cancellationToken);
            if (!w.IsCompletedSuccessfully) await w.ConfigureAwait(false);
            rollbackSeq = _writeContext.SequenceUsed;
        }
        finally
        {
            _writeContext.Reset();
        }
        if (doOneWrite)
        {
            _wpDb.RemovePrepared(prepareSeq, _prepareBatchCnt);
            return;
        }
        // else do the 2nd write for commit: an empty batch on the 2nd queue commits the prepared and the rollback
        // batches and releases the commit sequence number to readers.
        _rollbackCommit.Set(prepareSeq, rollbackSeq, _prepareBatchCnt);
        try
        {
            ValueTask c = WriteCommitOnlyAsync(_writeOptions, EmptyBatch(forWal: true), _rollbackCommit, synchronous, cancellationToken);
            if (!c.IsCompletedSuccessfully) await c.ConfigureAwait(false);
            _wpDb.RemovePrepared(prepareSeq, _prepareBatchCnt);
        }
        finally
        {
            // Note: RemovePrepared for prepared batch is called from within PreReleaseCallback
            _wpDb.RemovePrepared(rollbackSeq, 1);
        }
    }

    /// <summary>
    /// The value of <paramref name="key"/> visible through <paramref name="callback"/> written to
    /// <paramref name="dst"/> as a Put, or a Delete when there is none (RollbackWriteBatchBuilder::Rollback /
    /// WriteRollbackKeys). ShouldRollbackWithSingleDelete is not ported (always Delete).
    /// </summary>
    private protected void WriteRollbackKey(uint cf, ReadOnlySpan<byte> key, ReadOptions? options, ReadCallback callback, WriteBatch dst)
    {
        ColumnFamilyData cfd = _db.GetColumnFamilyDataById(cf);
        PooledBufferWriter value = ObjectPools.RentWriter();
        try
        {
            if (_db.GetWithCallback(cfd, options, key, callback, value)) dst.Put(cf, key, value.WrittenSpan);
            else dst.Delete(cf, key);
        }
        finally
        {
            ObjectPools.ReturnWriter(value);
        }
    }

    /// <summary>RollbackWriteBatchBuilder of WritePreparedTxn::RollbackInternal: one restore per distinct key.</summary>
    private struct RollbackBatchBuilder(WritePreparedTxn txn, WriteBatch dst, ReadCallback callback) : IWriteBatchHandler
    {
        private RollbackKeySet? _keys;

        private void Rollback(uint cf, ReadOnlySpan<byte> key)
        {
            if (!(_keys ??= new RollbackKeySet(txn._db)).Add(cf, key)) return;
            txn.WriteRollbackKey(cf, key, null, callback, dst);
        }

        public readonly bool Continue() => true;
        public void Put(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => Rollback(columnFamilyId, key);
        public void TimedPut(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ulong writeUnixTime) => Rollback(columnFamilyId, key);
        public void PutEntity(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> entity) => Rollback(columnFamilyId, key);
        public void Delete(uint columnFamilyId, ReadOnlySpan<byte> key) => Rollback(columnFamilyId, key);
        public void SingleDelete(uint columnFamilyId, ReadOnlySpan<byte> key) => Rollback(columnFamilyId, key);
        public readonly void DeleteRange(uint columnFamilyId, ReadOnlySpan<byte> beginKey, ReadOnlySpan<byte> endKey) =>
            ThrowHelper.NotSupported("DeleteRange in a transaction is not supported");
        public void Merge(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            if (txn._wpDb.RollbackMergeOperands) Rollback(columnFamilyId, key);
        }
        public readonly void PutBlobIndex(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> blobIndex) =>
            ThrowHelper.NotSupported("Blob indexes in a transaction are not supported");
        public readonly void LogData(ReadOnlySpan<byte> blob) { }
        public readonly void MarkBeginPrepare(bool unprepare) { }
        public readonly void MarkEndPrepare(ReadOnlySpan<byte> xid) { }
        public readonly void MarkNoop(bool emptyBatch) { }
        public readonly void MarkCommit(ReadOnlySpan<byte> xid) { }
        public readonly void MarkCommitWithTimestamp(ReadOnlySpan<byte> xid, ReadOnlySpan<byte> commitTimestamp) { }
        public readonly void MarkRollback(ReadOnlySpan<byte> xid) => ThrowHelper.InvalidArgument("rollback marker in a transaction batch");
    }

    /// <summary>
    /// WritePreparedTxn::ValidateSnapshot: the key conflicts unless its newest version is visible to the snapshot (with
    /// the snapshot's min_uncommitted).
    /// </summary>
    private protected override RocksDbException? ValidateSnapshot(ColumnFamilyData cfd, ReadOnlySpan<byte> key, ref ulong trackedAtSeq)
    {
        Snapshot snapshot = Snapshot!;
        ulong minUncommitted = snapshot.MinUncommitted;
        ulong snapSeq = snapshot.SequenceNumber;
        // tracked_at_seq is either max or the last snapshot with which this key was trackeed so there is no need to
        // apply the IsInSnapshot to this comparison here as tracked_at_seq is not a prepare seq.
        if (trackedAtSeq <= snapSeq)
        {
            // If the key has been previous validated at a sequence number earlier than the curent snapshot's sequence
            // number, we already know it has not been modified.
            return null;
        }
        trackedAtSeq = snapSeq;
        ReadCallback checker = NewValidationCallback(snapSeq, minUncommitted);
        using DbImpl.ConflictCheckView view = _db.AcquireConflictCheckView(cfd);
        return _db.CheckKey(view, key, snapSeq, checker, minUncommitted, cacheOnly: false);
    }

    /// <summary>The read callback of a conflict check at the transaction's snapshot.</summary>
    private protected virtual ReadCallback NewValidationCallback(ulong snapSeq, ulong minUncommitted)
    {
        var cb = new WritePreparedTxnReadCallback();
        cb.Init(_wpDb, snapSeq, minUncommitted, SnapshotBackup.BackedByDbSnapshot);
        return cb;
    }

    /// <summary>
    /// PessimisticTransactionDB::Initialize for WritePrepared: SetId(seq) and RebuildFromWriteBatch
    /// (prepare_batch_cnt_ = SubBatchCnt).
    /// </summary>
    internal override void RebuildPrepared(RecoveredTransaction recovered)
    {
        base.RebuildPrepared(recovered);
        PrepareSequence = recovered.Sequence;
        // RebuildFromWriteBatch: prepare_batch_cnt_ = SubBatchCnt() (equal to the recovered sub-batch count, which the
        // prepared heap was given; that one wins if they ever differ).
        int recoveredCnt = recovered.Batches[0].BatchCount;
        _prepareBatchCnt = recoveredCnt > 0 ? recoveredCnt : _batch.SubBatchCount;
    }
}

/// <summary>
/// The keys a rollback batch builder already restored, per column family (RollbackWriteBatchBuilder::keys_, a
/// std::set&lt;Slice, SetComparator&gt; over the column family's comparator): keys the comparator considers equal are
/// restored once, so the rollback batch has no duplicate key and is one sub-batch.
/// </summary>
internal sealed class RollbackKeySet(DbImpl db)
{
    private readonly Dictionary<uint, ISet<byte[]>> _keys = new();

    /// <summary>Adds the key; false if the column family already has an equal one.</summary>
    public bool Add(uint cf, ReadOnlySpan<byte> key)
    {
        if (!_keys.TryGetValue(cf, out ISet<byte[]>? set))
        {
            Comparator cmp = db.UserComparatorOf(cf);
            set = cmp.CanKeysWithDifferentByteContentsBeEqual
                ? new SortedSet<byte[]>(Comparer<byte[]>.Create((a, b) => cmp.Compare(a, b)))
                : new HashSet<byte[]>(ByteKeyComparer.Instance);
            _keys[cf] = set;
        }
        return set.Add(key.ToArray());
    }
}
