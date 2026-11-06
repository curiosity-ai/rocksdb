using RocksDb.Managed.Env;
using RocksDb.Managed.Utilities.Transactions;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Transactions;

/// <summary>
/// TransactionDbOptions.CustomMutexFactory (include/rocksdb/utilities/transaction_db_mutex.h) with the point lock
/// manager (utilities/transactions/lock/point/point_lock_manager.cc AcquireWithTimeout / UnLock): an instrumented
/// factory wrapping <see cref="TransactionDbMutexFactory.Default"/> counts every call; lock timeouts, waits woken by
/// unlocks, deadlock detection, expired-lock stealing and failures of the custom primitives.
/// </summary>
[TestClass]
public sealed class TransactionDbMutexTests
{
    private static readonly WriteOptions s_wo = new();

    /// <summary>Counts every call and delegates to the default implementation; can inject timeouts and failures.</summary>
    private sealed class CountingFactory : TransactionDbMutexFactory
    {
        public int Mutexes, CondVars, Locks, TryLocks, Unlocks, Waits, WaitFors, Notifies, NotifyAlls, Held;
        public volatile bool FailTryLock, ThrowOnWait;

        public override TransactionDbMutex AllocateMutex()
        {
            Interlocked.Increment(ref Mutexes);
            return new CountingMutex(this, Default.AllocateMutex());
        }

        public override TransactionDbCondVar AllocateCondVar()
        {
            Interlocked.Increment(ref CondVars);
            return new CountingCondVar(this, Default.AllocateCondVar());
        }
    }

    private sealed class CountingMutex(CountingFactory f, TransactionDbMutex inner) : TransactionDbMutex
    {
        public TransactionDbMutex Inner => inner;

        public override void Lock()
        {
            Interlocked.Increment(ref f.Locks);
            inner.Lock();
            Interlocked.Increment(ref f.Held);
        }

        public override bool TryLockFor(long timeoutMicros)
        {
            Interlocked.Increment(ref f.TryLocks);
            if (f.FailTryLock) return false;
            bool ok = inner.TryLockFor(timeoutMicros);
            if (ok) Interlocked.Increment(ref f.Held);
            return ok;
        }

        public override void Unlock()
        {
            Interlocked.Increment(ref f.Unlocks);
            Interlocked.Decrement(ref f.Held);
            inner.Unlock();
        }
    }

    private sealed class CountingCondVar(CountingFactory f, TransactionDbCondVar inner) : TransactionDbCondVar
    {
        public override void Wait(TransactionDbMutex mutex)
        {
            Interlocked.Increment(ref f.Waits);
            if (f.ThrowOnWait) throw new RocksDbException(StatusCode.Aborted, "injected wait failure");
            Interlocked.Decrement(ref f.Held);
            inner.Wait(((CountingMutex)mutex).Inner);
            Interlocked.Increment(ref f.Held);
        }

        public override bool WaitFor(TransactionDbMutex mutex, long timeoutMicros)
        {
            Interlocked.Increment(ref f.WaitFors);
            if (f.ThrowOnWait) throw new RocksDbException(StatusCode.Aborted, "injected wait failure");
            Interlocked.Decrement(ref f.Held);
            bool r = inner.WaitFor(((CountingMutex)mutex).Inner, timeoutMicros);
            Interlocked.Increment(ref f.Held);
            return r;
        }

        public override void Notify()
        {
            Interlocked.Increment(ref f.Notifies);
            inner.Notify();
        }

        public override void NotifyAll()
        {
            Interlocked.Increment(ref f.NotifyAlls);
            inner.NotifyAll();
        }
    }

    private static TransactionDatabase Open(MemoryFileSystem fs, CountingFactory f, int stripes = 4) =>
        TransactionDatabase.Open(DbPath, NewOptions(fs),
            new TransactionDbOptions { CustomMutexFactory = f, NumStripes = stripes, TransactionLockTimeout = TimeSpan.FromMilliseconds(50) },
            [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, new ColumnFamilyOptions())]);

    private static void AssertFails(StatusCode code, StatusSubCode sub, Action a)
    {
        var e = Assert.ThrowsExactly<RocksDbException>(a);
        Assert.AreEqual(code, e.Code, e.Message);
        Assert.AreEqual(sub, e.SubCode, e.Message);
    }

    private static IReadOnlyList<ulong> WaitingFor(Transaction t) => ((PessimisticTransaction)t).GetWaitingTransactions(out _, out _);

    [TestMethod]
    public void LocksGoThroughTheFactory()
    {
        var f = new CountingFactory();
        using TransactionDatabase db = Open(NewFs(), f);
        using (Transaction t = db.BeginTransaction(s_wo))
        {
            t.Put(B("a"), B("1"));
            t.Put(B("b"), B("2"));
            // One mutex and condition variable per stripe of the column family's lock table.
            Assert.AreEqual(4, f.Mutexes);
            Assert.AreEqual(4, f.CondVars);
            Assert.AreEqual(2, db.GetLockStatusData().Count);
            t.Commit();
        }
        Assert.AreEqual("1", GetString(db.Database, "a"));
        // Uncontended: TryLockFor(timeout) per lock, Lock + NotifyAll per unlock, Lock per status stripe, no waits.
        Assert.AreEqual(2, f.TryLocks);
        Assert.AreEqual(2 + 4, f.Locks);
        Assert.AreEqual(f.TryLocks + f.Locks, f.Unlocks);
        Assert.AreEqual(2, f.NotifyAlls);
        Assert.AreEqual(0, f.Waits + f.WaitFors);
        Assert.AreEqual(0, f.Held);
        Assert.AreEqual(0, db.GetLockStatusData().Count);

        // Writes on the DB itself also lock through the factory (default_lock_timeout).
        db.Put(B("c"), B("3"));
        Assert.AreEqual(3, f.TryLocks);
        Assert.AreEqual(4, f.Mutexes); // the lock table is created once
    }

    [TestMethod]
    public void LockTimeoutWaitsOnTheCondVar()
    {
        var f = new CountingFactory();
        using TransactionDatabase db = Open(NewFs(), f);
        using Transaction t1 = db.BeginTransaction(s_wo);
        using Transaction t2 = db.BeginTransaction(s_wo, new TransactionOptions { LockTimeout = TimeSpan.FromMilliseconds(30) });
        t1.Put(B("k"), B("1"));
        long start = Environment.TickCount64;
        AssertFails(StatusCode.TimedOut, StatusSubCode.LockTimeout, () => t2.Put(B("k"), B("2")));
        Assert.IsTrue(Environment.TickCount64 - start >= 25, "waited for the lock timeout");
        Assert.IsTrue(f.WaitFors >= 1);
        Assert.AreEqual(0, f.Held);
        // The contention is kept for debugging (SetWaitingTxn on timeout).
        CollectionAssert.AreEqual(new[] { t1.Id }, WaitingFor(t2).ToArray());

        // Timeout 0: no wait at all.
        int waits = f.WaitFors;
        using Transaction t3 = db.BeginTransaction(s_wo, new TransactionOptions { LockTimeout = TimeSpan.Zero });
        AssertFails(StatusCode.TimedOut, StatusSubCode.LockTimeout, () => t3.Put(B("k"), B("3")));
        Assert.AreEqual(waits, f.WaitFors);
    }

    [TestMethod]
    public async Task UnlockWakesWaiter()
    {
        var f = new CountingFactory();
        using TransactionDatabase db = Open(NewFs(), f);
        using Transaction t1 = db.BeginTransaction(s_wo);
        // Infinite timeout: the stripe mutex is taken with Lock() and the wait is CondVar.Wait.
        using Transaction t2 = db.BeginTransaction(s_wo, new TransactionOptions { LockTimeout = Timeout.InfiniteTimeSpan });
        t1.Put(B("k"), B("1"));
        // Lock waits block the calling thread with a custom factory, also through the async API.
        Task waiter = Task.Run(() => t2.Put(B("k"), B("2")));
        // The waiting transaction is recorded (SetWaitingTxn) before the condvar wait is entered, so wait for both.
        while (WaitingFor(t2).Count == 0 || Volatile.Read(ref f.Waits) == 0) await Task.Delay(1);
        Assert.IsFalse(waiter.IsCompleted);
        t1.Commit();
        await waiter.WaitAsync(TimeSpan.FromSeconds(30));
        await t2.CommitAsync();
        Assert.AreEqual("2", GetString(db.Database, "k"));
        Assert.AreEqual(0, f.Held);
    }

    [TestMethod]
    public async Task DeadlockIsDetected()
    {
        var f = new CountingFactory();
        using TransactionDatabase db = Open(NewFs(), f);
        var opts = new TransactionOptions { DeadlockDetect = true, LockTimeout = TimeSpan.FromSeconds(30) };
        using Transaction t1 = db.BeginTransaction(s_wo, opts);
        using Transaction t2 = db.BeginTransaction(s_wo, opts);
        t1.Put(B("A"), B("1"));
        t2.Put(B("B"), B("2"));
        Task t1WaitsForB = Task.Run(() => t1.Put(B("B"), B("1")));
        while (WaitingFor(t1).Count == 0) await Task.Delay(1);
        AssertFails(StatusCode.Busy, StatusSubCode.Deadlock, () => t2.Put(B("A"), B("2")));
        List<DeadlockPath> deadlocks = db.GetDeadlockInfoBuffer();
        Assert.AreEqual(1, deadlocks.Count);
        Assert.AreEqual(2, deadlocks[0].Path.Count);
        Assert.AreEqual(t1.Id, deadlocks[0].Path[0].TransactionId);
        Assert.AreEqual("B", S(deadlocks[0].Path[0].WaitingKey));
        Assert.AreEqual(t2.Id, deadlocks[0].Path[1].TransactionId);
        // Breaking the cycle lets t1 proceed.
        t2.Rollback();
        await t1WaitsForB.WaitAsync(TimeSpan.FromSeconds(30));
        t1.Commit();
        Assert.AreEqual("1", GetString(db.Database, "B"));
        Assert.AreEqual(0, f.Held);
    }

    [TestMethod]
    public void ExpiredLockIsStolen()
    {
        var f = new CountingFactory();
        using TransactionDatabase db = Open(NewFs(), f);
        using Transaction t1 = db.BeginTransaction(s_wo, new TransactionOptions { Expiration = TimeSpan.FromMilliseconds(20) });
        using Transaction t2 = db.BeginTransaction(s_wo, new TransactionOptions { LockTimeout = TimeSpan.FromSeconds(10) });
        t1.Put(B("k"), B("1"));
        // t2 waits until t1's expiration (cv_end_time = the holder's expiration), then steals the lock.
        t2.Put(B("k"), B("2"));
        t2.Commit();
        Assert.AreEqual("2", GetString(db.Database, "k"));
        Assert.IsTrue(f.WaitFors >= 1);
        AssertFails(StatusCode.Expired, StatusSubCode.None, () => t1.Commit());
    }

    [TestMethod]
    public void CustomPrimitiveFailuresAreReported()
    {
        var f = new CountingFactory();
        using TransactionDatabase db = Open(NewFs(), f);
        using Transaction t1 = db.BeginTransaction(s_wo);
        using Transaction t2 = db.BeginTransaction(s_wo, new TransactionOptions { LockTimeout = TimeSpan.FromSeconds(10) });

        // TryLockFor timing out: Status::TimedOut(kMutexTimeout).
        f.FailTryLock = true;
        AssertFails(StatusCode.TimedOut, StatusSubCode.MutexTimeout, () => t1.Put(B("k"), B("1")));
        f.FailTryLock = false;
        Assert.AreEqual(0, f.Held);

        // A failing wait ends the lock request with its error.
        t1.Put(B("k"), B("1"));
        f.ThrowOnWait = true;
        var e = Assert.ThrowsExactly<RocksDbException>(() => t2.Put(B("k"), B("2")));
        Assert.AreEqual(StatusCode.Aborted, e.Code);
        f.ThrowOnWait = false;
        Assert.AreEqual(0, f.Held);
        Assert.AreEqual(0, WaitingFor(t2).Count);
        t1.Commit();
        t2.Put(B("k"), B("2"));
        t2.Commit();
        Assert.AreEqual("2", GetString(db.Database, "k"));
    }

    [TestMethod]
    public async Task ConcurrentTransfersKeepTheSum()
    {
        var f = new CountingFactory();
        using TransactionDatabase db = Open(NewFs(), f, stripes: 2);
        const int accounts = 8;
        for (int i = 0; i < accounts; i++) db.Put(B("acct" + i), BitConverter.GetBytes(100L));
        var tasks = new Task[4];
        for (int w = 0; w < tasks.Length; w++)
        {
            int seed = w;
            tasks[w] = Task.Run(() =>
            {
                var rnd = new Random(seed);
                var opts = new TransactionOptions { DeadlockDetect = true, LockTimeout = TimeSpan.FromSeconds(5) };
                for (int n = 0; n < 200; n++)
                {
                    int a = rnd.Next(accounts), b = rnd.Next(accounts);
                    if (a == b) continue;
                    using Transaction t = db.BeginTransaction(s_wo, opts);
                    try
                    {
                        long va = BitConverter.ToInt64(t.GetForUpdate(B("acct" + a))!);
                        long vb = BitConverter.ToInt64(t.GetForUpdate(B("acct" + b))!);
                        t.Put(B("acct" + a), BitConverter.GetBytes(va - 1));
                        t.Put(B("acct" + b), BitConverter.GetBytes(vb + 1));
                        t.Commit();
                    }
                    catch (RocksDbException e) when (e.SubCode is StatusSubCode.Deadlock or StatusSubCode.LockTimeout)
                    {
                        t.Rollback();
                    }
                }
            });
        }
        await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(60));
        long sum = 0;
        for (int i = 0; i < accounts; i++) sum += BitConverter.ToInt64(db.Database.Get(B("acct" + i))!);
        Assert.AreEqual(100L * accounts, sum);
        Assert.AreEqual(0, f.Held);
        Assert.AreEqual(0, db.GetLockStatusData().Count);
    }
}
