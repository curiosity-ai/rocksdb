using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace RocksDb.Managed.Utilities.Transactions;

/// <summary>
/// Per-key lock table of a <see cref="TransactionDatabase"/>. Port of PointLockManager
/// (utilities/transactions/lock/point/point_lock_manager.cc):
/// <list type="bullet">
/// <item>One lock map per column family, split into <c>num_stripes</c> stripes by key hash; each stripe has a mutex,
/// its locked keys (shared or exclusive, with the holders' transaction ids and an expiration time) and a "condition
/// variable" (the waiters to wake on unlock: every waiter of the stripe re-checks, like NotifyAll).</item>
/// <item>Lock waits honour the transaction's lock timeout and the lock holder's expiration (expired locks can be
/// stolen). Waiting is asynchronous (a <see cref="TaskCompletionSource"/> per wait) unless the caller is synchronous.</item>
/// <item>Deadlock detection (deadlock_detect): a wait-for graph between transaction ids, searched breadth-first up to
/// deadlock_detect_depth before waiting; detected cycles are recorded in a bounded buffer.</item>
/// <item>With <see cref="TransactionDbOptions.CustomMutexFactory"/> each stripe instead uses a
/// <see cref="TransactionDbMutex"/> and a <see cref="TransactionDbCondVar"/> from the factory, and lock waits follow
/// upstream's AcquireWithTimeout exactly (blocking the calling thread). The default path is untouched by this (one null
/// test per TryLock / UnLock).</item>
/// </list>
/// </summary>
internal sealed class PointLockManager : LockManager
{
    private readonly TransactionDatabase _txnDb;
    private readonly int _numStripes;
    private readonly long _maxNumLocks;
    private readonly ConcurrentDictionary<uint, LockMap> _lockMaps = new();
    private readonly TransactionDbMutexFactory? _mutexFactory; // custom_mutex_factory (null = built-in stripe locks)

    // Wait-for graph (wait_txn_map_ / rev_wait_txn_map_), guarded by _waitLock.
    private readonly Lock _waitLock = new();
    private readonly Dictionary<ulong, WaitInfo> _waitTxnMap = new();
    private readonly Dictionary<ulong, int> _revWaitTxnMap = new();
    private readonly List<DeadlockPath> _deadlocks = new(); // newest last
    private int _maxDeadlocks;

    public PointLockManager(TransactionDatabase txnDb, TransactionDbOptions options)
    {
        _txnDb = txnDb;
        _numStripes = Math.Max(1, options.NumStripes);
        _maxNumLocks = options.MaxNumLocks;
        _maxDeadlocks = Math.Max(0, options.MaxNumDeadlocks);
        _mutexFactory = options.CustomMutexFactory;
    }

    /// <summary>A locked key (LockInfo). Recycled by its stripe together with its key buffer.</summary>
    private sealed class LockInfo
    {
        public bool Exclusive;
        public readonly List<ulong> TxnIds = new(1);
        /// <summary>Microseconds (<see cref="NowMicros"/>); 0 = never expires.</summary>
        public long ExpirationTime;
        /// <summary>Storage of the key (the dictionary key points into it).</summary>
        public byte[] KeyBuffer = [];
        /// <summary>Next entry of the stripe's free list.</summary>
        public LockInfo? NextFree;
    }

    private sealed class Stripe
    {
        /// <summary>Recycled lock entries kept per stripe (the lock table's steady-state size).</summary>
        private const int MaxFree = 256;

        public readonly Lock Mutex = new();
        public readonly Dictionary<PooledKey, LockInfo> Keys;
        public readonly Dictionary<PooledKey, LockInfo>.AlternateLookup<ReadOnlySpan<byte>> Lookup;
        public readonly List<TaskCompletionSource> Waiters = new();
        private LockInfo? _free;
        private int _freeCount;

        /// <summary>stripe_mutex / stripe_cv from the custom mutex factory (null with the built-in locks).</summary>
        public readonly TransactionDbMutex? CustomMutex;
        public readonly TransactionDbCondVar? CustomCv;

        public Stripe(TransactionDbMutexFactory? factory)
        {
            Keys = new Dictionary<PooledKey, LockInfo>(PooledKeyComparer.Instance);
            Lookup = Keys.GetAlternateLookup<ReadOnlySpan<byte>>();
            if (factory is not null)
            {
                CustomMutex = factory.AllocateMutex() ?? throw new InvalidOperationException("AllocateMutex returned null");
                CustomCv = factory.AllocateCondVar() ?? throw new InvalidOperationException("AllocateCondVar returned null");
            }
        }

        /// <summary>Adds a lock entry for <paramref name="key"/> (a recycled one when available). REQUIRES: <see cref="Mutex"/>.</summary>
        public LockInfo Add(ReadOnlySpan<byte> key, ulong txnId, bool exclusive, long expirationTime)
        {
            LockInfo? li = _free;
            if (li is not null)
            {
                _free = li.NextFree;
                li.NextFree = null;
                _freeCount--;
            }
            else
            {
                li = new LockInfo();
            }
            if (li.KeyBuffer.Length < key.Length) li.KeyBuffer = new byte[Math.Max(16, (key.Length + 15) & ~15)];
            key.CopyTo(li.KeyBuffer);
            li.Exclusive = exclusive;
            li.ExpirationTime = expirationTime;
            li.TxnIds.Clear();
            li.TxnIds.Add(txnId);
            Keys.Add(new PooledKey(li.KeyBuffer, key.Length), li);
            return li;
        }

        /// <summary>Removes the lock entry of <paramref name="key"/> and recycles it. REQUIRES: <see cref="Mutex"/>.</summary>
        public void Remove(ReadOnlySpan<byte> key)
        {
            if (!Lookup.Remove(key, out _, out LockInfo? li) || _freeCount >= MaxFree) return;
            li.NextFree = _free;
            _free = li;
            _freeCount++;
        }
    }

    private sealed class LockMap(int stripes, TransactionDbMutexFactory? factory)
    {
        public readonly Stripe[] Stripes = CreateStripes(stripes, factory);
        public long LockedKeyCount;

        private static Stripe[] CreateStripes(int n, TransactionDbMutexFactory? factory)
        {
            var s = new Stripe[n];
            for (int i = 0; i < n; i++) s[i] = new Stripe(factory);
            return s;
        }
    }

    private readonly record struct WaitInfo(ulong[] Neighbors, uint CfId, bool Exclusive, byte[] Key);

    private LockMap GetLockMap(uint cf) => _lockMaps.GetOrAdd(cf, static (_, self) => new LockMap(self._numStripes, self._mutexFactory), this);

    private Stripe GetStripe(LockMap map, ReadOnlySpan<byte> key) => map.Stripes[ByteKeyComparer.Bucket(key, 0, map.Stripes.Length)];

    /// <summary>Lets the lock map of a dropped column family be collected (RemoveColumnFamily).</summary>
    public override void RemoveColumnFamily(uint cf) => _lockMaps.TryRemove(cf, out _);

    /// <summary>Monotonic clock in microseconds used for lock timeouts and expirations.</summary>
    public static long NowMicros() => (long)(Stopwatch.GetTimestamp() * (1_000_000.0 / Stopwatch.Frequency));

    // ---- TryLock --------------------------------------------------------------------------------------------

    /// <summary>
    /// Locks <paramref name="key"/> for <paramref name="txn"/> (PointLockManager::TryLock + AcquireWithTimeout).
    /// Completes with null on success, TimedOut(LockTimeout), Busy(Deadlock) or Aborted(LockLimit). The key is copied
    /// before any wait.
    /// </summary>
    public override ValueTask<RocksDbException?> TryLock(PessimisticTransaction txn, uint cf, ReadOnlySpan<byte> key, bool exclusive, bool synchronous,
        CancellationToken cancellationToken)
    {
        LockMap map = GetLockMap(cf);
        Stripe stripe = GetStripe(map, key);
        if (_mutexFactory is not null) return new ValueTask<RocksDbException?>(AcquireWithTimeoutCustom(txn, map, stripe, cf, key, exclusive));
        List<ulong> waitIds = txn.WaitIdsScratch;
        waitIds.Clear();
        RocksDbException? result;
        long expireHint;
        lock (stripe.Mutex)
        {
            result = AcquireLocked(map, stripe, key, txn.Id, txn.ExpirationTime, exclusive, out expireHint, waitIds);
        }
        if (result is null) return default;
        long timeout = txn.LockTimeoutMicros;
        if (timeout == 0)
        {
            // AcquireWithTimeout: "On timeout, persist the lock information so we can debug the contention", also when
            // the transaction does not wait at all (timeout == 0 skips only the retry loop).
            if (result.Code == StatusCode.TimedOut) txn.SetWaitingTxn(waitIds, cf, key.ToArray());
            return new ValueTask<RocksDbException?>(result);
        }
        return AcquireWithTimeoutAsync(txn, map, stripe, cf, key.ToArray(), exclusive, timeout, synchronous, cancellationToken);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<RocksDbException?> AcquireWithTimeoutAsync(PessimisticTransaction txn, LockMap map, Stripe stripe, uint cf, byte[] key, bool exclusive,
        long timeout, bool synchronous, CancellationToken cancellationToken)
    {
        long endTime = timeout > 0 ? NowMicros() + timeout : 0;
        List<ulong> waitIds = new();
        bool timedOut = false;
        RocksDbException? result;
        while (true)
        {
            TaskCompletionSource waiter;
            long expireHint;
            bool counted = false;
            lock (stripe.Mutex)
            {
                waitIds.Clear();
                result = AcquireLocked(map, stripe, key, txn.Id, txn.ExpirationTime, exclusive, out expireHint, waitIds);
                if (result is null || timedOut) break;
                // We are dependent on a transaction to finish, so perform deadlock detection.
                if (waitIds.Count != 0)
                {
                    if (txn.DeadlockDetect)
                    {
                        if (IncrementWaiters(txn, waitIds, key, cf, exclusive)) return new RocksDbException(StatusCode.Busy, "Resource busy: Deadlock", StatusSubCode.Deadlock);
                        counted = true;
                    }
                    txn.SetWaitingTxn(waitIds, cf, key);
                }
                waiter = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                stripe.Waiters.Add(waiter);
            }

            // Decide how long to wait
            long cvEnd = -1;
            if (expireHint > 0 && endTime > 0) cvEnd = Math.Min(expireHint, endTime);
            else if (expireHint > 0) cvEnd = expireHint;
            else if (endTime > 0) cvEnd = endTime;
            try
            {
                if (cvEnd < 0)
                {
                    if (synchronous) waiter.Task.Wait(cancellationToken);
                    else await waiter.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    long now = NowMicros();
                    if (cvEnd > now)
                    {
                        var wait = TimeSpan.FromTicks((cvEnd - now) * 10);
                        if (synchronous) waiter.Task.Wait(wait, cancellationToken);
                        else await waiter.Task.WaitAsync(wait, cancellationToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
                        cancellationToken.ThrowIfCancellationRequested();
                    }
                }
            }
            finally
            {
                if (!waiter.Task.IsCompleted)
                {
                    lock (stripe.Mutex) stripe.Waiters.Remove(waiter);
                }
                if (waitIds.Count != 0)
                {
                    txn.ClearWaitingTxn();
                    if (counted) DecrementWaiters(txn, waitIds);
                }
            }
            // Even though we timed out, we will still make one more attempt to acquire lock (it is possible the lock
            // expired and we were never signaled).
            if (endTime > 0 && NowMicros() >= endTime) timedOut = true;
        }
        if (result is not null) txn.SetWaitingTxn(waitIds, cf, key); // persist the lock information for debugging
        return result;
    }

    /// <summary>
    /// PointLockManager::AcquireWithTimeout over the custom stripe mutex and condition variable
    /// (<see cref="TransactionDbOptions.CustomMutexFactory"/>): lock the stripe (Lock, or TryLockFor(timeout)), try
    /// to acquire, and while the lock is held by others wait on the stripe's condition variable (bounded by the lock
    /// timeout and the holder's expiration), with deadlock detection before each wait. Blocks the calling thread.
    /// Failures of the custom mutex / condition variable (thrown <see cref="RocksDbException"/>s) are returned.
    /// </summary>
    private RocksDbException? AcquireWithTimeoutCustom(PessimisticTransaction txn, LockMap map, Stripe stripe, uint cf, ReadOnlySpan<byte> key,
        bool exclusive)
    {
        TransactionDbMutex mutex = stripe.CustomMutex!;
        TransactionDbCondVar cv = stripe.CustomCv!;
        long timeout = txn.LockTimeoutMicros;
        long endTime = timeout > 0 ? NowMicros() + timeout : 0;
        try
        {
            // If timeout is negative, we wait indefinitely to acquire the lock
            if (timeout < 0) mutex.Lock();
            else if (!mutex.TryLockFor(timeout)) return new RocksDbException(StatusCode.TimedOut, "Operation timed out: Timeout waiting to lock key", StatusSubCode.MutexTimeout);
        }
        catch (RocksDbException e)
        {
            return e; // failed to acquire mutex
        }

        List<ulong> waitIds = txn.WaitIdsScratch;
        waitIds.Clear();
        byte[]? keyCopy = null;
        RocksDbException? result;
        try
        {
            // Acquire lock if we are able to
            result = AcquireLocked(map, stripe, key, txn.Id, txn.ExpirationTime, exclusive, out long expireHint, waitIds);
            if (result is not null && timeout != 0)
            {
                // If we weren't able to acquire the lock, we will keep retrying as long as the timeout allows.
                keyCopy = key.ToArray();
                waitIds = new List<ulong>(waitIds);
                bool timedOut = false;
                do
                {
                    // Decide how long to wait
                    long cvEnd = -1;
                    if (expireHint > 0 && endTime > 0) cvEnd = Math.Min(expireHint, endTime);
                    else if (expireHint > 0) cvEnd = expireHint;
                    else if (endTime > 0) cvEnd = endTime;

                    // We are dependent on a transaction to finish, so perform deadlock detection.
                    if (waitIds.Count != 0)
                    {
                        if (txn.DeadlockDetect && IncrementWaiters(txn, waitIds, keyCopy, cf, exclusive))
                        {
                            return new RocksDbException(StatusCode.Busy, "Resource busy: Deadlock", StatusSubCode.Deadlock);
                        }
                        txn.SetWaitingTxn(waitIds, cf, keyCopy);
                    }

                    bool cvTimedOut = false;
                    RocksDbException? cvFailure = null;
                    try
                    {
                        if (cvEnd < 0)
                        {
                            cv.Wait(mutex); // Wait indefinitely
                        }
                        else
                        {
                            long now = NowMicros();
                            cvTimedOut = cvEnd <= now || !cv.WaitFor(mutex, cvEnd - now); // now >= cv_end_time: already timed out
                        }
                    }
                    catch (RocksDbException e)
                    {
                        cvFailure = e;
                    }

                    if (waitIds.Count != 0)
                    {
                        txn.ClearWaitingTxn();
                        if (txn.DeadlockDetect) DecrementWaiters(txn, waitIds);
                    }
                    if (cvFailure is not null) return cvFailure;
                    // Even though we timed out, we will still make one more attempt to acquire lock below (it is
                    // possible the lock expired and we were never signaled).
                    if (cvTimedOut) timedOut = true;
                    waitIds.Clear();
                    result = AcquireLocked(map, stripe, key, txn.Id, txn.ExpirationTime, exclusive, out expireHint, waitIds);
                } while (result is not null && !timedOut);
            }
        }
        finally
        {
            mutex.Unlock();
        }
        // On timeout, persist the lock information so we can debug the contention
        if (result is { Code: StatusCode.TimedOut }) txn.SetWaitingTxn(waitIds, cf, keyCopy ?? key.ToArray());
        return result;
    }

    /// <summary>
    /// PointLockManager::AcquireLocked. REQUIRES: stripe mutex. On failure returns TimedOut (lock held; the holders are
    /// added to <paramref name="waitIds"/>) or LockLimit; <paramref name="expireTime"/> receives the holder's
    /// expiration time (0 = none).
    /// </summary>
    private RocksDbException? AcquireLocked(LockMap map, Stripe stripe, ReadOnlySpan<byte> key, ulong txnId, long txnExpiration, bool exclusive,
        out long expireTime, List<ulong> waitIds)
    {
        expireTime = 0;
        if (stripe.Lookup.TryGetValue(key, out LockInfo? info))
        {
            // Lock already held
            if (info.Exclusive || exclusive)
            {
                if (info.TxnIds.Count == 1 && info.TxnIds[0] == txnId)
                {
                    // The list contains one txn and we're it, so just take it.
                    info.Exclusive = exclusive;
                    info.ExpirationTime = txnExpiration;
                }
                else if (IsLockExpired(txnId, info, out expireTime))
                {
                    // lock is expired, can steal it
                    info.TxnIds.Clear();
                    info.TxnIds.Add(txnId);
                    info.Exclusive = exclusive;
                    info.ExpirationTime = txnExpiration;
                    // lock_cnt does not change
                }
                else
                {
                    foreach (ulong id in info.TxnIds)
                    {
                        // A transaction is not blocked by itself
                        if (id != txnId) waitIds.Add(id);
                    }
                    return new RocksDbException(StatusCode.TimedOut, "Operation timed out: Timeout waiting to lock key", StatusSubCode.LockTimeout);
                }
            }
            else
            {
                // We are requesting shared access to a shared lock, so just grant it.
                if (!info.TxnIds.Contains(txnId)) info.TxnIds.Add(txnId);
                // Using max means that expiration time never goes down even when a transaction is removed from the list.
                info.ExpirationTime = Math.Max(info.ExpirationTime, txnExpiration);
            }
            return null;
        }
        // Lock not held. Check lock limit
        if (_maxNumLocks > 0 && Volatile.Read(ref map.LockedKeyCount) >= _maxNumLocks)
        {
            return new RocksDbException(StatusCode.Aborted, "Operation aborted: Lock limit", StatusSubCode.LockLimit);
        }
        stripe.Add(key, txnId, exclusive, txnExpiration);
        // Maintain lock count if there is a limit on the number of locks
        if (_maxNumLocks > 0) Interlocked.Increment(ref map.LockedKeyCount);
        return null;
    }

    /// <summary>
    /// PointLockManager::IsLockExpired: true if the lock expired and every other holder's locks could be stolen;
    /// otherwise <paramref name="expireTime"/> is the expiration time to wait for (0 = none).
    /// </summary>
    private bool IsLockExpired(ulong txnId, LockInfo info, out long expireTime)
    {
        expireTime = 0;
        if (info.ExpirationTime == 0) return false;
        bool expired = info.ExpirationTime <= NowMicros();
        if (!expired)
        {
            // return how many microseconds until lock will be expired
            expireTime = info.ExpirationTime;
            return false;
        }
        foreach (ulong id in info.TxnIds)
        {
            if (id == txnId) continue;
            if (!_txnDb.TryStealingExpiredTransactionLocks(id))
            {
                expireTime = 0;
                return false;
            }
        }
        return true;
    }

    // ---- UnLock ---------------------------------------------------------------------------------------------

    /// <summary>Releases <paramref name="txn"/>'s lock on one key and wakes the stripe's waiters (UnLock).</summary>
    public override void UnLock(PessimisticTransaction txn, uint cf, ReadOnlySpan<byte> key)
    {
        if (!_lockMaps.TryGetValue(cf, out LockMap? map)) return; // Column Family must have been dropped.
        Stripe stripe = GetStripe(map, key);
        if (_mutexFactory is not null)
        {
            UnLockCustom(txn.Id, key, stripe, map);
            return;
        }
        TaskCompletionSource[]? wake = null;
        lock (stripe.Mutex)
        {
            UnLockKey(txn.Id, key, stripe, map);
            if (stripe.Waiters.Count > 0)
            {
                wake = stripe.Waiters.ToArray();
                stripe.Waiters.Clear();
            }
        }
        // Signal waiting threads to retry locking
        if (wake is not null)
        {
            foreach (TaskCompletionSource w in wake) w.TrySetResult();
        }
    }

    /// <summary>UnLock with the custom stripe mutex: unlock the key, then signal waiting threads to retry locking.</summary>
    private void UnLockCustom(ulong txnId, ReadOnlySpan<byte> key, Stripe stripe, LockMap map)
    {
        stripe.CustomMutex!.Lock();
        try
        {
            UnLockKey(txnId, key, stripe, map);
        }
        finally
        {
            stripe.CustomMutex.Unlock();
        }
        stripe.CustomCv!.NotifyAll();
    }

    /// <summary>Releases all locks tracked by <paramref name="tracker"/>.</summary>
    public override void UnLock(PessimisticTransaction txn, LockTracker tracker)
    {
        for (int i = 0; i < tracker.ColumnFamilyCount; i++)
        {
            TrackedColumnFamily c = tracker.GetColumnFamily(i);
            foreach (PooledKey key in c.Keys.Keys) UnLock(txn, c.Cfd.Id, key.Span);
        }
    }

    /// <inheritdoc/>
    public override void UnLockAll(PessimisticTransaction txn, LockTracker tracked) => UnLock(txn, tracked);

    /// <inheritdoc/>
    public override bool IsRangeLockSupported => false;

    private void UnLockKey(ulong txnId, ReadOnlySpan<byte> key, Stripe stripe, LockMap map)
    {
        if (!stripe.Lookup.TryGetValue(key, out LockInfo? info)) return; // not locked, or locked by someone else after expiring
        List<ulong> txns = info.TxnIds;
        int i = txns.IndexOf(txnId);
        if (i < 0) return;
        if (txns.Count == 1)
        {
            stripe.Remove(key);
        }
        else
        {
            txns[i] = txns[^1];
            txns.RemoveAt(txns.Count - 1);
        }
        if (_maxNumLocks > 0) Interlocked.Decrement(ref map.LockedKeyCount);
    }

    // ---- deadlock detection ---------------------------------------------------------------------------------

    private void DecrementWaiters(PessimisticTransaction txn, List<ulong> waitIds)
    {
        lock (_waitLock) DecrementWaitersImpl(txn.Id, waitIds);
    }

    private void DecrementWaitersImpl(ulong id, List<ulong> waitIds)
    {
        _waitTxnMap.Remove(id);
        foreach (ulong w in waitIds)
        {
            if (_revWaitTxnMap.TryGetValue(w, out int n))
            {
                if (n <= 1) _revWaitTxnMap.Remove(w);
                else _revWaitTxnMap[w] = n - 1;
            }
        }
    }

    /// <summary>
    /// PointLockManager::IncrementWaiters: records that <paramref name="txn"/> waits for <paramref name="waitIds"/> and
    /// searches the wait-for graph for a path back to <paramref name="txn"/>. Returns true (and undoes the
    /// registration) on a deadlock or when the search exceeds the transaction's deadlock_detect_depth.
    /// </summary>
    private bool IncrementWaiters(PessimisticTransaction txn, List<ulong> waitIds, byte[] key, uint cf, bool exclusive)
    {
        ulong id = txn.Id;
        int depth = txn.DeadlockDetectDepth;
        lock (_waitLock)
        {
            _waitTxnMap[id] = new WaitInfo(waitIds.ToArray(), cf, exclusive, key);
            foreach (ulong w in waitIds)
            {
                _revWaitTxnMap.TryGetValue(w, out int n);
                _revWaitTxnMap[w] = n + 1;
            }
            // No deadlock if nobody is waiting on self.
            if (!_revWaitTxnMap.ContainsKey(id)) return false;

            var queueValues = new ulong[Math.Max(depth, 1)];
            var queueParents = new int[Math.Max(depth, 1)];
            ulong[]? nextIds = _waitTxnMap[id].Neighbors;
            int parent = -1;
            for (int tail = 0, head = 0; head < depth; head++)
            {
                int i = 0;
                if (nextIds is not null)
                {
                    for (; i < nextIds.Length && tail + i < depth; i++)
                    {
                        queueValues[tail + i] = nextIds[i];
                        queueParents[tail + i] = parent;
                    }
                    tail += i;
                }
                // No more items in the list, meaning no deadlock.
                if (tail == head) return false;
                ulong next = queueValues[head];
                if (next == id)
                {
                    var path = new List<DeadlockInfo>();
                    for (int h = head; h != -1; h = queueParents[h])
                    {
                        WaitInfo wi = _waitTxnMap[queueValues[h]];
                        path.Add(new DeadlockInfo(queueValues[h], wi.CfId, wi.Key, wi.Exclusive));
                    }
                    path.Reverse();
                    AddDeadlock(new DeadlockPath(path, LimitExceeded: false, DateTimeOffset.UtcNow));
                    DecrementWaitersImpl(id, waitIds);
                    return true;
                }
                if (!_waitTxnMap.TryGetValue(next, out WaitInfo nextInfo))
                {
                    nextIds = null;
                    continue;
                }
                parent = head;
                nextIds = nextInfo.Neighbors;
            }
            // Wait cycle too big, just assume deadlock.
            AddDeadlock(new DeadlockPath(Array.Empty<DeadlockInfo>(), LimitExceeded: true, DateTimeOffset.UtcNow));
            DecrementWaitersImpl(id, waitIds);
            return true;
        }
    }

    private void AddDeadlock(DeadlockPath path)
    {
        if (_maxDeadlocks == 0) return;
        _deadlocks.Add(path);
        while (_deadlocks.Count > _maxDeadlocks) _deadlocks.RemoveAt(0);
    }

    /// <summary>The most recent deadlocks, newest first (GetDeadlockInfoBuffer).</summary>
    public override List<DeadlockPath> GetDeadlockInfoBuffer()
    {
        lock (_waitLock)
        {
            var list = new List<DeadlockPath>(_deadlocks);
            list.Reverse();
            return list;
        }
    }

    /// <summary>Resizes the deadlock buffer (SetDeadlockInfoBufferSize).</summary>
    public override void Resize(int size)
    {
        lock (_waitLock)
        {
            _maxDeadlocks = Math.Max(0, size);
            while (_deadlocks.Count > _maxDeadlocks) _deadlocks.RemoveAt(0);
        }
    }

    private static void AddLockStatus(uint cf, Stripe s, List<KeyLockInfo> result)
    {
        foreach (KeyValuePair<PooledKey, LockInfo> k in s.Keys)
        {
            result.Add(new KeyLockInfo(cf, k.Key.Span.ToArray(), k.Value.TxnIds.ToArray(), k.Value.Exclusive));
        }
    }

    /// <summary>Snapshot of all held locks (GetPointLockStatus).</summary>
    public override List<KeyLockInfo> GetLockStatusData()
    {
        var result = new List<KeyLockInfo>();
        foreach (KeyValuePair<uint, LockMap> kv in _lockMaps)
        {
            foreach (Stripe s in kv.Value.Stripes)
            {
                if (s.CustomMutex is { } m)
                {
                    m.Lock();
                    try
                    {
                        AddLockStatus(kv.Key, s, result);
                    }
                    finally
                    {
                        m.Unlock();
                    }
                    continue;
                }
                lock (s.Mutex) AddLockStatus(kv.Key, s, result);
            }
        }
        return result;
    }
}
