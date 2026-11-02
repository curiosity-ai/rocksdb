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

/// <summary>How a pessimistic transaction DB writes data (TxnDBWritePolicy). Only <see cref="WriteCommitted"/> is supported.</summary>
public enum TransactionWritePolicy : byte
{
    /// <summary>Data is written to the memtables only at commit (the prepared section is WAL-only).</summary>
    WriteCommitted = 0,
    /// <summary>Not supported by the managed port.</summary>
    WritePrepared = 1,
    /// <summary>Not supported by the managed port.</summary>
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
    /// Default lock wait timeout of transactions that do not set <see cref="TransactionOptions.LockTimeout"/>
    /// (transaction_lock_timeout, default 1 s). <see cref="Timeout.InfiniteTimeSpan"/> waits forever,
    /// <see cref="TimeSpan.Zero"/> fails immediately.
    /// </summary>
    public TimeSpan TransactionLockTimeout { get; set; } = TimeSpan.FromMilliseconds(1000);

    /// <summary>Lock wait timeout of writes made directly on the <see cref="TransactionDatabase"/> (default_lock_timeout, default 1 s).</summary>
    public TimeSpan DefaultLockTimeout { get; set; } = TimeSpan.FromMilliseconds(1000);

    /// <summary>Write policy; only <see cref="TransactionWritePolicy.WriteCommitted"/> is supported.</summary>
    public TransactionWritePolicy WritePolicy { get; set; } = TransactionWritePolicy.WriteCommitted;

    /// <summary>
    /// Skip locking for writes made directly on the <see cref="TransactionDatabase"/> (skip_concurrency_control). Only
    /// safe when the application guarantees no conflicts with concurrent transactions.
    /// </summary>
    public bool SkipConcurrencyControl { get; set; }
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
}

/// <summary>One transaction waiting in a deadlock cycle (DeadlockInfo).</summary>
public sealed record DeadlockInfo(ulong TransactionId, uint ColumnFamilyId, byte[] WaitingKey, bool Exclusive);

/// <summary>A detected deadlock (DeadlockPath): the cycle, or <see cref="LimitExceeded"/> when the search depth was exceeded.</summary>
public sealed record DeadlockPath(IReadOnlyList<DeadlockInfo> Path, bool LimitExceeded, DateTimeOffset DeadlockTime);

/// <summary>A held key lock (KeyLockInfo).</summary>
public sealed record KeyLockInfo(uint ColumnFamilyId, byte[] Key, IReadOnlyList<ulong> TransactionIds, bool Exclusive);
