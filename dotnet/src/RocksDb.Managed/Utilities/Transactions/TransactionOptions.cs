namespace RocksDb.Managed.Utilities.Transactions;

/// <summary>Transaction state (Transaction::TransactionState, include/rocksdb/utilities/transaction.h).</summary>
public enum TransactionState : byte
{
    Started = 0,
    AwaitingPrepare = 1,
    Prepared = 2,
    AwaitingCommit = 3,
    Committed = 4,
    AwaitingRollback = 5,
    RolledBack = 6,
    LocksStolen = 7,
}

/// <summary>When an optimistic transaction validates its reads and writes (OccValidationPolicy).</summary>
public enum OccValidationPolicy : byte
{
    /// <summary>
    /// Validate serially at commit, inside the write group (a write callback run by the group leader). Validation is
    /// single-threaded but needs no extra locks.
    /// </summary>
    ValidateSerial = 0,
    /// <summary>
    /// Validate before entering the write group, holding striped bucket locks for the transaction's keys (taken in a
    /// fixed order, so transactions cannot deadlock). The upstream default.
    /// </summary>
    ValidateParallel = 1,
}

/// <summary>How a pessimistic transaction DB writes data (TxnDBWritePolicy, include/rocksdb/utilities/transaction_db.h).</summary>
public enum TransactionWritePolicy : byte
{
    /// <summary>Data is written to the memtables only at commit (the prepared section is WAL-only).</summary>
    WriteCommitted = 0,
    /// <summary>
    /// Data is written to the memtables at prepare (commit-without-prepare writes it at commit); readers consult an
    /// in-memory commit cache to know whether the data of a sequence number is committed in their snapshot. Commits are
    /// cheap (a commit marker), prepared transactions are large in memory only while uncommitted.
    /// </summary>
    WritePrepared = 1,
    /// <summary>
    /// Like <see cref="WritePrepared"/>, but a transaction whose batch exceeds
    /// <see cref="TransactionOptions.WriteBatchFlushThreshold"/> writes it to the DB before prepare as an unprepared
    /// batch (large transactions do not keep their writes in memory).
    /// </summary>
    WriteUnprepared = 2,
}

/// <summary>Options of <see cref="OptimisticTransactionDatabase.BeginTransaction(WriteOptions, OptimisticTransactionOptions)"/> (OptimisticTransactionOptions).</summary>
public sealed class OptimisticTransactionOptions
{
    /// <summary>Same as calling <see cref="Transaction.SetSnapshot"/> right after the transaction begins.</summary>
    public bool SetSnapshot { get; set; }
}

/// <summary>Options of <see cref="OptimisticTransactionDatabase"/> (OptimisticTransactionDBOptions).</summary>
public sealed class OptimisticTransactionDbOptions
{
    /// <summary>Validation policy; default <see cref="OccValidationPolicy.ValidateParallel"/> like upstream.</summary>
    public OccValidationPolicy ValidatePolicy { get; set; } = OccValidationPolicy.ValidateParallel;

    /// <summary>Number of striped bucket locks used by <see cref="OccValidationPolicy.ValidateParallel"/> (default 2^20).</summary>
    public int OccLockBuckets { get; set; } = 1 << 20;
}

/// <summary>Options of <see cref="TransactionDatabase"/> (TransactionDBOptions, include/rocksdb/utilities/transaction_db.h).</summary>
public sealed class TransactionDbOptions
{
    /// <summary>Maximum number of keys that can be locked at once per column family; 0 or less means no limit.</summary>
    public long MaxNumLocks { get; set; } = -1;

    /// <summary>Number of recent deadlocks kept for <see cref="TransactionDatabase.GetDeadlockInfoBuffer"/>.</summary>
    public int MaxNumDeadlocks { get; set; } = 5;

    /// <summary>Number of sub-tables per column family lock table; more stripes reduce contention.</summary>
    public int NumStripes { get; set; } = 16;

    /// <summary>
    /// custom_mutex_factory: when set, the point lock manager guards each lock-table stripe with a mutex and a
    /// condition variable from this factory instead of its built-in ones (see <see cref="TransactionDbMutexFactory"/>).
    /// Lock waits then block the calling thread, also from the async APIs. Null (default): the built-in lock-free-wait
    /// implementation (asynchronous waits, no allocation per lock). Not used by range locking.
    /// </summary>
    public TransactionDbMutexFactory? CustomMutexFactory { get; set; }

    /// <summary>
    /// Default lock wait timeout of transactions that do not set <see cref="TransactionOptions.LockTimeout"/>
    /// (transaction_lock_timeout, default 1 s). <see cref="Timeout.InfiniteTimeSpan"/> waits forever,
    /// <see cref="TimeSpan.Zero"/> fails immediately.
    /// </summary>
    public TimeSpan TransactionLockTimeout { get; set; } = TimeSpan.FromMilliseconds(1000);

    /// <summary>Lock wait timeout of writes made directly on the <see cref="TransactionDatabase"/> (default_lock_timeout, default 1 s).</summary>
    public TimeSpan DefaultLockTimeout { get; set; } = TimeSpan.FromMilliseconds(1000);

    /// <summary>
    /// Write policy (write_policy). The policy is not persisted: it is implied by the WAL contents, and a DB whose WAL
    /// holds prepared sections of another policy fails to open with NotSupported (empty the WAL, e.g. by committing or
    /// rolling back the prepared transactions and flushing, before changing it).
    /// </summary>
    public TransactionWritePolicy WritePolicy { get; set; } = TransactionWritePolicy.WriteCommitted;

    /// <summary>
    /// WritePrepared / WriteUnprepared: when a transaction is rolled back, also restore the keys it only merged
    /// (rollback_merge_operands; default false: merge operands are left in place).
    /// </summary>
    public bool RollbackMergeOperands { get; set; }

    /// <summary>
    /// WriteUnprepared: default <see cref="TransactionOptions.WriteBatchFlushThreshold"/> of transactions that do not
    /// set one (default_write_batch_flush_threshold; 0 = never flush before prepare).
    /// </summary>
    public long DefaultWriteBatchFlushThreshold { get; set; }

    /// <summary>WritePrepared / WriteUnprepared: log2 of the snapshot cache size (wp_snapshot_cache_bits, default 7).</summary>
    public int WpSnapshotCacheBits { get; set; } = 7;

    /// <summary>
    /// WritePrepared / WriteUnprepared: log2 of the commit cache size (wp_commit_cache_bits, default 23: 8M entries,
    /// 64 MB). Smaller caches evict commit entries sooner (more snapshot bookkeeping).
    /// </summary>
    public int WpCommitCacheBits { get; set; } = 23;

    /// <summary>
    /// Skip locking for batches written directly on the <see cref="TransactionDatabase"/> with Write / WriteAsync
    /// (skip_concurrency_control). Only safe when the application guarantees no conflicts with concurrent transactions.
    /// Like upstream, the single-key writes (Put, Delete, SingleDelete, Merge) still lock their key.
    /// </summary>
    public bool SkipConcurrencyControl { get; set; }

    /// <summary>
    /// enable_udt_validation: whether transactions on column families with user-defined timestamps validate keys
    /// against <see cref="Transaction.SetReadTimestampForValidation"/> (a key whose newest version has a newer timestamp
    /// is a conflict). When disabled, setting a read timestamp is an error. Default true.
    /// </summary>
    public bool EnableUdtValidation { get; set; } = true;

    /// <summary>
    /// lock_mgr_handle: a <see cref="Transactions.RangeLockManager"/> to open the database in range-locking mode (every
    /// lock is a range lock; <see cref="Transaction.GetRangeLock(ColumnFamily?, RangeLockEndpoint, RangeLockEndpoint)"/>
    /// becomes available). Null (default) uses the per-key point lock manager. Requires the WriteCommitted or
    /// WritePrepared write policy. <see cref="MaxNumLocks"/>, <see cref="NumStripes"/> and <see cref="MaxNumDeadlocks"/>
    /// do not apply to range locking (see <see cref="Transactions.RangeLockManager.MaxLockMemory"/>).
    /// </summary>
    public RangeLockManager? RangeLockManager { get; set; }
}

/// <summary>Options of <see cref="TransactionDatabase.BeginTransaction(WriteOptions, TransactionOptions)"/> (TransactionOptions).</summary>
public sealed class TransactionOptions
{
    /// <summary>Same as calling <see cref="Transaction.SetSnapshot"/> right after the transaction begins.</summary>
    public bool SetSnapshot { get; set; }

    /// <summary>Detect deadlocks while waiting for locks (fails the lock request with Busy/Deadlock).</summary>
    public bool DeadlockDetect { get; set; }

    /// <summary>Maximum number of edges followed when searching for a deadlock (default 50).</summary>
    public int DeadlockDetectDepth { get; set; } = 50;

    /// <summary>
    /// Lock wait timeout; null uses <see cref="TransactionDbOptions.TransactionLockTimeout"/>,
    /// <see cref="Timeout.InfiniteTimeSpan"/> waits forever, <see cref="TimeSpan.Zero"/> fails immediately.
    /// </summary>
    public TimeSpan? LockTimeout { get; set; }

    /// <summary>
    /// Time after which the transaction expires if not committed: its locks can then be stolen by other transactions
    /// and it fails with Expired. Null means no expiration.
    /// </summary>
    public TimeSpan? Expiration { get; set; }

    /// <summary>Maximum size of the transaction's write batch in bytes; 0 means no limit.</summary>
    public int MaxWriteBatchSize { get; set; }

    /// <summary>Skip locking and conflict checks entirely (skip_concurrency_control).</summary>
    public bool SkipConcurrencyControl { get; set; }

    /// <summary>Allow <see cref="Transaction.Commit"/> of a named transaction without <see cref="Transaction.Prepare"/> (default true).</summary>
    public bool SkipPrepare { get; set; } = true;

    /// <summary>
    /// WriteUnprepared: once the transaction's batch is larger than this many bytes, it is written to the DB as an
    /// unprepared batch before the next write (write_batch_flush_threshold; the transaction must be named). Negative
    /// uses <see cref="TransactionDbOptions.DefaultWriteBatchFlushThreshold"/>; 0 never flushes.
    /// </summary>
    public long WriteBatchFlushThreshold { get; set; } = -1;

    /// <summary>
    /// write_batch_track_timestamp_size (upstream: "DO NOT USE", a temporary option for MyRocks): the transaction's
    /// <see cref="Transaction.WriteBatch"/> records the timestamp size of every column family written to it with a
    /// timestamp, and the commit timestamp is applied with those sizes first. Supports timestamped writes made directly to
    /// <see cref="Transaction.WriteBatch"/>, bypassing the transaction's write APIs. The managed commit otherwise uses each
    /// column family's own timestamp size, so such writes are timestamped correctly either way.
    /// </summary>
    public bool WriteBatchTrackTimestampSize { get; set; }

    /// <summary>
    /// use_only_the_last_commit_time_batch_for_recovery: (WritePrepared / WriteUnprepared) the commit-time write batch
    /// (<see cref="Transaction.GetCommitTimeWriteBatch"/>) is written to the WAL only and just the last one written is
    /// kept for recovery: the DB inserts it into the memtables before the next memtable switch. Required to use a
    /// commit-time batch with these write policies. Default false.
    /// </summary>
    public bool UseOnlyTheLastCommitTimeBatchForRecovery { get; set; }
}

/// <summary>One transaction waiting in a deadlock cycle (DeadlockInfo).</summary>
public sealed record DeadlockInfo(ulong TransactionId, uint ColumnFamilyId, byte[] WaitingKey, bool Exclusive);

/// <summary>A detected deadlock (DeadlockPath): the cycle, or <see cref="LimitExceeded"/> when the search depth was exceeded.</summary>
public sealed record DeadlockPath(IReadOnlyList<DeadlockInfo> Path, bool LimitExceeded, DateTimeOffset DeadlockTime);

/// <summary>A held key lock (KeyLockInfo).</summary>
public sealed record KeyLockInfo(uint ColumnFamilyId, byte[] Key, IReadOnlyList<ulong> TransactionIds, bool Exclusive);
