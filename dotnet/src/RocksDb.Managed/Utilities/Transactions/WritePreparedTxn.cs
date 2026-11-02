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
/// </list>
/// Not ported: commit-time batches, two_write_queues.
/// </summary>
internal class WritePreparedTxn : PessimisticTransaction
{
    private protected readonly WritePreparedTxnDb _wpDb;
    private readonly AddPreparedCallback _addPrepared;
    private readonly CommitEntryPreReleaseCallback _commitEntry;
    /// <summary>prepare_batch_cnt_: the number of sub-batches of the prepared batch.</summary>
    private protected int _prepareBatchCnt;

    public WritePreparedTxn(TransactionDatabase txnDb, WritePreparedTxnDb wpDb, WriteOptions? writeOptions, TransactionOptions options)
        : base(txnDb, writeOptions, options)
    {
        _wpDb = wpDb;
        _addPrepared = new AddPreparedCallback(wpDb);
        _commitEntry = new CommitEntryPreReleaseCallback(wpDb);
    }

    /// <summary>GetId(): the sequence number of the (first) prepared batch, 0 before it was written.</summary>
    internal ulong PrepareSequence { get; private protected set; }

    private protected override void Initialize(TransactionOptions options)
    {
        base.Initialize(options);
        _prepareBatchCnt = 0;
        PrepareSequence = 0;
    }

    // WritePrepared transactions do not support user-defined timestamps (TransactionDB::Open fails with them).
    private protected override bool SupportsUserDefinedTimestamps => false;

    /// <summary>WritePreparedTxn::PrepareInternal.</summary>
    private protected override async ValueTask PrepareInternalAsync(bool synchronous, CancellationToken cancellationToken)
    {
        WriteOptions wo = _writeOptions.DisableWal ? CloneWithWal(_writeOptions) : _writeOptions;
        // For each duplicate key we account for a new sub-batch
        _batch.Batch.MarkEndPrepare(_nameBytes!, writeAfterCommit: false, unpreparedBatch: false);
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

    /// <summary>WritePreparedTxn::CommitWithoutPrepareInternal -> WritePreparedTxnDB::WriteInternal.</summary>
    private protected override async ValueTask CommitWithoutPrepareInternalAsync(bool synchronous, CancellationToken cancellationToken)
    {
        int batchCnt = _batch.SubBatchCount;
        WriteBatch batch = _batch.Batch;
        if (batch.Count == 0) return;
        // InsertNoop: the batch boundary (the leading Noop placeholder of 2PC batches is ignored as an empty batch).
        batch.InsertNoop();
        _commitEntry.Set(DbFormat.MaxSequenceNumber, 0, batchCnt);
        _writeContext.Reset();
        _writeContext.BatchCount = batchCnt;
        _writeContext.PreRelease = _commitEntry;
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
    }

    /// <summary>
    /// WritePreparedTxn::CommitInternal: a commit marker written to the WAL only (one sequence number); the
    /// pre-release callback adds the commit entries of the prepared sub-batches.
    /// </summary>
    private protected override async ValueTask CommitInternalAsync(bool synchronous, CancellationToken cancellationToken)
    {
        WriteBatch marker = MarkerBatch();
        marker.MarkCommit(_nameBytes!);
        ulong prepareSeq = PrepareSequence;
        _commitEntry.Set(prepareSeq, _prepareBatchCnt, 0);
        _writeContext.Reset();
        _writeContext.DisableMemtable = true;
        _writeContext.BatchCount = 1;
        _writeContext.PreRelease = _commitEntry;
        try
        {
            ValueTask w = _db.WriteAsync(_writeOptions, marker, validateColumnFamilies: false, _writeContext, synchronous, cancellationToken);
            if (!w.IsCompletedSuccessfully) await w.ConfigureAwait(false);
        }
        finally
        {
            _writeContext.Reset();
        }
        _wpDb.RemovePrepared(prepareSeq, _prepareBatchCnt);
    }

    /// <summary>
    /// WritePreparedTxn::RollbackInternal: for every key of the prepared batch (merged keys only with
    /// rollback_merge_operands) the value visible before the transaction (read with a callback that does not see the
    /// prepared data) is written back, followed by a rollback marker; the write commits the prepared sub-batches and
    /// itself.
    /// </summary>
    private protected override async ValueTask RollbackInternalAsync(bool synchronous, CancellationToken cancellationToken)
    {
        using var rollbackBatch = new WriteBatch();
        var callback = new WritePreparedTxnReadCallback();
        // disable min_uncommitted optimization
        callback.Init(_wpDb, DbFormat.MaxSequenceNumber, ReadCallback.MinUnCommittedSeq, SnapshotBackup.BackedByDbSnapshot);
        var builder = new RollbackBatchBuilder(this, rollbackBatch, callback);
        _batch.Batch.Iterate(ref builder);
        rollbackBatch.MarkRollback(_nameBytes!);
        ulong prepareSeq = PrepareSequence;
        _commitEntry.Set(prepareSeq, _prepareBatchCnt, 1);
        _writeContext.Reset();
        _writeContext.BatchCount = 1;
        _writeContext.PreRelease = _commitEntry;
        try
        {
            ValueTask w = _db.WriteAsync(_writeOptions, rollbackBatch, validateColumnFamilies: false, _writeContext, synchronous, cancellationToken);
            if (!w.IsCompletedSuccessfully) await w.ConfigureAwait(false);
        }
        finally
        {
            _writeContext.Reset();
        }
        _wpDb.RemovePrepared(prepareSeq, _prepareBatchCnt);
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
        private Dictionary<uint, HashSet<byte[]>>? _keys;

        private void Rollback(uint cf, ReadOnlySpan<byte> key)
        {
            _keys ??= new Dictionary<uint, HashSet<byte[]>>();
            if (!_keys.TryGetValue(cf, out HashSet<byte[]>? set)) _keys[cf] = set = new HashSet<byte[]>(ByteKeyComparer.Instance);
            if (!set.Add(key.ToArray())) return;
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
