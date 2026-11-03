using System.Collections.Concurrent;
using RocksDb.Managed.Db;

namespace RocksDb.Managed.Utilities.Transactions;

/// <summary>
/// Range-based lock manager of a pessimistic <see cref="TransactionDatabase"/> (RangeLockManagerHandle /
/// RangeTreeLockManager, include/rocksdb/utilities/transaction_db.h,
/// utilities/transactions/lock/range/range_tree/range_tree_lock_manager.cc). Create one and pass it in
/// <see cref="TransactionDbOptions.RangeLockManager"/> to open the database in range-locking mode; keep it to control
/// the lock manager while the database is open.
/// <para>
/// In range-locking mode every lock is a range lock: <see cref="Transaction.GetRangeLock(ColumnFamily?, RangeLockEndpoint, RangeLockEndpoint)"/>
/// locks a range of keys exclusively, and the point locks taken by writes and GetForUpdate are single-key ranges.
/// Ranges of different transactions conflict when they overlap (shared locks too, unless they have identical
/// endpoints). Conflicting requests wait up to the transaction's lock timeout; deadlocks between waiting transactions
/// are always detected (whatever <see cref="TransactionOptions.DeadlockDetect"/> says) and fail the request that closes
/// the cycle with Busy(Deadlock). Locks are released only at commit / rollback: undoing a GetForUpdate or rolling back to
/// a save point keeps them. Locks do not expire.
/// </para>
/// <para>
/// The locks use an estimated amount of memory (<see cref="RangeLockManagerStatus.CurrentLockMemory"/>); when it
/// reaches <see cref="MaxLockMemory"/> (default 64 MiB), lock escalation merges each transaction's adjacent exclusive
/// locks into wider ranges (see <see cref="SetEscalationBarrier"/>); if that does not free enough, lock requests fail
/// with Aborted(LockLimit).
/// </para>
/// Thread-safe. One manager serves one open database at a time; the write policy must be WriteCommitted or
/// WritePrepared.
/// </summary>
public sealed class RangeLockManager
{
    /// <summary>locktree_manager::DEFAULT_MAX_LOCK_MEMORY.</summary>
    public const long DefaultMaxLockMemory = 64L * 1024 * 1024;

    private readonly Lock _attachLock = new();
    private TransactionDatabase? _txnDb;
    private readonly ConcurrentDictionary<uint, RangeLockTree> _trees = new();
    private long _maxLockMemory = DefaultMaxLockMemory;
    private long _currentLockMemory;
    private long _escalationCount;
    private long _retiredWaitCount;
    private Func<RangeLockEndpoint, RangeLockEndpoint, bool>? _barrier;
    private Func<bool>? _isKilled;

    // Escalator (locktree_escalator): one escalation at a time; concurrent requesters wait for it instead of re-running.
    private readonly object _escalatorSync = new();
    private bool _escalatorRunning;
    private long _escalatorGeneration;

    // Deadlock buffer (DeadlockInfoBufferTempl<RangeDeadlockPath>, initial size 10), newest last.
    private readonly Lock _deadlockLock = new();
    private readonly List<RangeDeadlockPath> _deadlocks = new();
    private int _maxDeadlocks = 10;

    public RangeLockManager() => Impl = new RangeLockManagerImpl(this);

    internal RangeLockManagerImpl Impl { get; }

    /// <summary>The lock memory limit in bytes (GetMaxLockMemory).</summary>
    public long MaxLockMemory => Volatile.Read(ref _maxLockMemory);

    /// <summary>
    /// Sets the lock memory limit (SetMaxLockMemory). Returns false (upstream EDOM) and keeps the limit if the locks
    /// currently use more memory than <paramref name="maxLockMemory"/>.
    /// </summary>
    public bool SetMaxLockMemory(long maxLockMemory)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(maxLockMemory);
        lock (_attachLock)
        {
            if (maxLockMemory < Interlocked.Read(ref _currentLockMemory)) return false;
            Volatile.Write(ref _maxLockMemory, maxLockMemory);
            return true;
        }
    }

    /// <summary>
    /// Sets the lock escalation barrier check (SetEscalationBarrierFunc): called with endpoints A &lt; B of two locks of
    /// one transaction that escalation would merge; returning true forbids bridging the gap between them. Called from
    /// threads that acquire locks, while the column family's lock structure is locked: it must be fast, must not throw
    /// and must not use the transaction database. Null removes it.
    /// </summary>
    public void SetEscalationBarrier(Func<RangeLockEndpoint, RangeLockEndpoint, bool>? barrier) => Volatile.Write(ref _barrier, barrier);

    /// <summary>
    /// Sets the kill check (SetIsKilledCallback): while it is set, a waiting lock request polls it every 100 ms (and
    /// before waiting) and fails with Aborted when it returns true. Null removes it.
    /// </summary>
    public void SetIsKilledCallback(Func<bool>? isKilled) => Volatile.Write(ref _isKilled, isKilled);

    /// <summary>
    /// All held range locks (GetRangeLockStatusData), per column family in key order. While a single transaction holds
    /// all locks of a column family, its ranges are reported as acquired (not yet merged), like upstream.
    /// </summary>
    public List<RangeLockInfo> GetRangeLockStatusData()
    {
        var result = new List<RangeLockInfo>();
        foreach (KeyValuePair<uint, RangeLockTree> kv in _trees) kv.Value.DumpLocks(result);
        return result;
    }

    /// <summary>The manager's counters (GetStatus).</summary>
    public RangeLockManagerStatus GetStatus()
    {
        ulong waits = (ulong)Interlocked.Read(ref _retiredWaitCount);
        foreach (KeyValuePair<uint, RangeLockTree> kv in _trees) waits += kv.Value.WaitCount;
        return new RangeLockManagerStatus((ulong)Interlocked.Read(ref _escalationCount), waits, (ulong)Math.Max(0, Interlocked.Read(ref _currentLockMemory)));
    }

    /// <summary>The most recent range-lock deadlocks, newest first (GetRangeDeadlockInfoBuffer).</summary>
    public List<RangeDeadlockPath> GetRangeDeadlockInfoBuffer()
    {
        lock (_deadlockLock)
        {
            var list = new List<RangeDeadlockPath>(_deadlocks);
            list.Reverse();
            return list;
        }
    }

    /// <summary>Changes how many deadlocks are kept (SetRangeDeadlockInfoBufferSize).</summary>
    public void SetRangeDeadlockInfoBufferSize(int size)
    {
        lock (_deadlockLock)
        {
            _maxDeadlocks = Math.Max(0, size);
            while (_deadlocks.Count > _maxDeadlocks) _deadlocks.RemoveAt(0);
        }
    }

    // ---- internals ------------------------------------------------------------------------------------------

    internal void Attach(TransactionDatabase txnDb)
    {
        lock (_attachLock)
        {
            if (_txnDb is not null && _txnDb != txnDb)
            {
                throw new RocksDbException(StatusCode.InvalidArgument, "Invalid argument: the range lock manager is in use by another transaction database");
            }
            _txnDb = txnDb;
        }
    }

    internal void Detach(TransactionDatabase txnDb)
    {
        lock (_attachLock)
        {
            if (_txnDb != txnDb) return;
            _txnDb = null;
            foreach (KeyValuePair<uint, RangeLockTree> kv in _trees) Interlocked.Add(ref _retiredWaitCount, (long)kv.Value.WaitCount);
            _trees.Clear();
            Interlocked.Exchange(ref _currentLockMemory, 0);
        }
    }

    /// <summary>GetLockTreeForCF, creating the tree (AddColumnFamily) with the column family's comparator on first use.</summary>
    internal RangeLockTree GetTree(uint cf)
    {
        if (_trees.TryGetValue(cf, out RangeLockTree? t)) return t;
        TransactionDatabase db = _txnDb ?? throw new ObjectDisposedException(nameof(TransactionDatabase));
        ColumnFamilyData cfd = db.Database.Impl.GetColumnFamilyDataById(cf);
        return _trees.GetOrAdd(cf, static (id, s) => new RangeLockTree(s.Mgr, id, s.Cmp), (Mgr: this, Cmp: cfd.UserComparator));
    }

    internal RangeLockTree? TryGetTree(uint cf) => _trees.TryGetValue(cf, out RangeLockTree? t) ? t : null;

    internal void RemoveColumnFamily(uint cf)
    {
        if (_trees.TryRemove(cf, out RangeLockTree? t)) Interlocked.Add(ref _retiredWaitCount, (long)t.WaitCount);
    }

    internal IEnumerable<KeyValuePair<uint, RangeLockTree>> Trees => _trees;

    internal Func<bool>? IsKilled => Volatile.Read(ref _isKilled);

    internal void NoteMemUsed(long bytes) => Interlocked.Add(ref _currentLockMemory, bytes);

    internal void NoteMemReleased(long bytes) => Interlocked.Add(ref _currentLockMemory, -bytes);

    private bool OutOfLocks => Interlocked.Read(ref _currentLockMemory) >= Volatile.Read(ref _maxLockMemory);

    /// <summary>locktree_manager::check_current_lock_constraints: escalates when out of lock memory.</summary>
    internal int CheckCurrentLockConstraints()
    {
        if (!OutOfLocks) return RangeLockTree.Granted;
        RunEscalation();
        return OutOfLocks ? RangeLockTree.OutOfLocks : RangeLockTree.Granted;
    }

    /// <summary>locktree_escalator::run + escalate_all_locktrees.</summary>
    private void RunEscalation()
    {
        lock (_escalatorSync)
        {
            if (_escalatorRunning)
            {
                long gen = _escalatorGeneration;
                while (_escalatorGeneration == gen) Monitor.Wait(_escalatorSync);
                return;
            }
            _escalatorRunning = true;
        }
        try
        {
            foreach (KeyValuePair<uint, RangeLockTree> kv in _trees) kv.Value.Escalate();
            Interlocked.Increment(ref _escalationCount);
        }
        finally
        {
            lock (_escalatorSync)
            {
                _escalatorRunning = false;
                _escalatorGeneration++;
                Monitor.PulseAll(_escalatorSync);
            }
        }
    }

    /// <summary>OnEscalationBarrierCheck.</summary>
    internal bool IsEscalationBarrier(byte[] a, byte[] b) =>
        Volatile.Read(ref _barrier) is { } f && f(RangeLockTree.Deserialize(a), RangeLockTree.Deserialize(b));

    internal void AddDeadlock(RangeDeadlockPath path)
    {
        lock (_deadlockLock)
        {
            if (_maxDeadlocks == 0) return;
            _deadlocks.Add(path);
            while (_deadlocks.Count > _maxDeadlocks) _deadlocks.RemoveAt(0);
        }
    }
}

/// <summary>
/// The <see cref="LockManager"/> side of a <see cref="RangeLockManager"/> (RangeTreeLockManager::TryLock / UnLock /
/// GetPointLockStatus / GetDeadlockInfoBuffer).
/// </summary>
internal sealed class RangeLockManagerImpl(RangeLockManager handle) : LockManager
{
    public RangeLockManager Handle => handle;

    public override bool IsRangeLockSupported => true;

    /// <summary>RangeLockManagerBase::TryLock: a point lock is a range lock on [key, key].</summary>
    public override ValueTask<RocksDbException?> TryLock(PessimisticTransaction txn, uint cf, ReadOnlySpan<byte> key, bool exclusive, bool synchronous,
        CancellationToken cancellationToken)
    {
        byte[] e = RangeLockTree.Serialize(key, false);
        return TryRangeLock(txn, cf, e, e, exclusive, synchronous, cancellationToken);
    }

    /// <summary>RangeTreeLockManager::TryLock (range): start the request, wait if it is pending, map the result.</summary>
    public ValueTask<RocksDbException?> TryRangeLock(PessimisticTransaction txn, uint cf, byte[] start, byte[] end, bool exclusive, bool synchronous,
        CancellationToken cancellationToken)
    {
        RangeLockTree tree = handle.GetTree(cf);
        var request = new RangeLockRequest(tree, txn, start, end, exclusive, start.AsSpan(1).ToArray());
        var path = new List<RangeDeadlockInfo>();
        if (request.Start(path))
        {
            lock (tree.RequestsMutex) tree.CountWait(false); // lock_request::wait counts every request
            txn.ClearWaitingTxn();
            return new ValueTask<RocksDbException?>(MapResult(request, path));
        }
        return WaitAsync(txn, request, path, synchronous, cancellationToken);
    }

    private async ValueTask<RocksDbException?> WaitAsync(PessimisticTransaction txn, RangeLockRequest request, List<RangeDeadlockInfo> path, bool synchronous,
        CancellationToken cancellationToken)
    {
        // The transaction's lock timeout (µs, negative = none: upstream waits one year).
        long timeout = txn.LockTimeoutMicros;
        long waitMicros = timeout < 0 ? 365L * 24 * 3600 * 1_000_000 : (timeout + 500) / 1000 * 1000;
        try
        {
            await request.WaitAsync(waitMicros, handle.IsKilled, synchronous, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            // Inform the txn that we are no longer waiting
            txn.ClearWaitingTxn();
        }
        return MapResult(request, path);
    }

    private RocksDbException? MapResult(RangeLockRequest request, List<RangeDeadlockInfo> path)
    {
        switch (request.Result)
        {
            case RangeLockTree.Granted:
                return null;
            case RangeLockTree.NotGranted:
                return new RocksDbException(StatusCode.TimedOut, "Operation timed out: Timeout waiting to lock key", StatusSubCode.LockTimeout);
            case RangeLockTree.Interrupted:
                return new RocksDbException(StatusCode.Aborted, "Operation aborted");
            case RangeLockTree.LockDeadlock:
                handle.AddDeadlock(new RangeDeadlockPath(path, LimitExceeded: false, DateTimeOffset.FromUnixTimeMilliseconds(request.StartTimeMs)));
                return new RocksDbException(StatusCode.Busy, "Resource busy: Deadlock", StatusSubCode.Deadlock);
            default:
                return new RocksDbException(StatusCode.Aborted, "Operation aborted: Lock limit", StatusSubCode.LockLimit);
        }
    }

    /// <summary>RangeTreeLockManager::UnLock (one key): releases the transaction's locks overlapping [key, key].</summary>
    public override void UnLock(PessimisticTransaction txn, uint cf, ReadOnlySpan<byte> key)
    {
        if (handle.TryGetTree(cf) is not { } tree) return;
        byte[] e = RangeLockTree.Serialize(key, false);
        tree.ReleaseLocks(txn, [new LockRange(e, e, true)], allTrxLocks: false);
        tree.RetryAll();
    }

    /// <summary>RangeTreeLockManager::UnLock (a tracker other than the transaction's own: all_trx_locks = false).</summary>
    public override void UnLock(PessimisticTransaction txn, LockTracker tracker)
    {
        for (int i = 0; i < tracker.ColumnFamilyCount; i++)
        {
            TrackedColumnFamily c = tracker.GetColumnFamily(i);
            if (c.Keys.Count == 0 || handle.TryGetTree(c.Cfd.Id) is not { } tree) continue;
            var ranges = new List<LockRange>(c.Keys.Count);
            foreach (PooledKey key in c.Keys.Keys)
            {
                byte[] e = RangeLockTree.Serialize(key.Span, false);
                ranges.Add(new LockRange(e, e, true));
            }
            tree.ReleaseLocks(txn, ranges, allTrxLocks: false);
            tree.RetryAll();
        }
    }

    /// <summary>RangeTreeLockManager::UnLock with the transaction's own tracker: releases every range it acquired.</summary>
    public override void UnLockAll(PessimisticTransaction txn, LockTracker tracked) => txn.RangeLocks?.ReleaseLocks(handle, txn, allTrxLocks: true);

    public override void RemoveColumnFamily(uint cf) => handle.RemoveColumnFamily(cf);

    /// <summary>GetPointLockStatus: the start keys of the range locks.</summary>
    public override List<KeyLockInfo> GetLockStatusData()
    {
        List<RangeLockInfo> ranges = handle.GetRangeLockStatusData();
        var result = new List<KeyLockInfo>(ranges.Count);
        foreach (RangeLockInfo r in ranges) result.Add(new KeyLockInfo(r.ColumnFamilyId, r.Start.Key.ToArray(), r.TransactionIds, r.Exclusive));
        return result;
    }

    /// <summary>GetDeadlockInfoBuffer: the range deadlocks, reporting the start keys.</summary>
    public override List<DeadlockPath> GetDeadlockInfoBuffer()
    {
        List<RangeDeadlockPath> data = handle.GetRangeDeadlockInfoBuffer();
        var result = new List<DeadlockPath>(data.Count);
        foreach (RangeDeadlockPath p in data)
        {
            var path = new List<DeadlockInfo>(p.Path.Count);
            foreach (RangeDeadlockInfo d in p.Path) path.Add(new DeadlockInfo(d.TransactionId, d.ColumnFamilyId, d.Start.Key.ToArray(), d.Exclusive));
            result.Add(new DeadlockPath(path, p.LimitExceeded, p.DeadlockTime));
        }
        return result;
    }

    public override void Resize(int size) => handle.SetRangeDeadlockInfoBufferSize(size);
}
