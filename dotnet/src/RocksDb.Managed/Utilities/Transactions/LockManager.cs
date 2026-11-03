namespace RocksDb.Managed.Utilities.Transactions;

/// <summary>
/// The lock manager of a <see cref="TransactionDatabase"/>. Port of the LockManager interface
/// (utilities/transactions/lock/lock_manager.h): <see cref="PointLockManager"/> (per-key lock table, the default) or
/// <see cref="RangeLockManager"/> (range locks, selected with <see cref="TransactionDbOptions.RangeLockManager"/>).
/// </summary>
internal abstract class LockManager
{
    /// <summary>IsRangeLockSupported: true for the range lock manager (point locks are then single-key ranges).</summary>
    public abstract bool IsRangeLockSupported { get; }

    /// <summary>
    /// Locks <paramref name="key"/> for <paramref name="txn"/>. Completes with null on success, TimedOut(LockTimeout),
    /// Busy(Deadlock), Aborted(LockLimit) or (range locks) Aborted when the wait was interrupted. The key is copied
    /// before any wait.
    /// </summary>
    public abstract ValueTask<RocksDbException?> TryLock(PessimisticTransaction txn, uint cf, ReadOnlySpan<byte> key, bool exclusive, bool synchronous,
        CancellationToken cancellationToken);

    /// <summary>Releases <paramref name="txn"/>'s lock on one key and lets waiters retry.</summary>
    public abstract void UnLock(PessimisticTransaction txn, uint cf, ReadOnlySpan<byte> key);

    /// <summary>Releases the locks of the keys tracked by <paramref name="tracker"/> (a tracker other than the transaction's own).</summary>
    public abstract void UnLock(PessimisticTransaction txn, LockTracker tracker);

    /// <summary>
    /// Releases every lock of <paramref name="txn"/> (commit, rollback, dispose); <paramref name="tracked"/> is the
    /// transaction's own key tracker.
    /// </summary>
    public abstract void UnLockAll(PessimisticTransaction txn, LockTracker tracked);

    /// <summary>Lets the lock structures of a dropped column family be collected (RemoveColumnFamily).</summary>
    public abstract void RemoveColumnFamily(uint cf);

    /// <summary>Snapshot of the held locks (GetPointLockStatus; range locks report their start keys).</summary>
    public abstract List<KeyLockInfo> GetLockStatusData();

    /// <summary>The most recent deadlocks, newest first (GetDeadlockInfoBuffer; range deadlocks report start keys).</summary>
    public abstract List<DeadlockPath> GetDeadlockInfoBuffer();

    /// <summary>Resizes the deadlock buffer (Resize).</summary>
    public abstract void Resize(int size);
}
