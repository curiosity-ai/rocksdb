using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Utilities.Transactions;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Transactions;

/// <summary>
/// Range locking for pessimistic transactions. Ports of utilities/transactions/lock/range/range_locking_test.cc and of
/// the AnyLockManagerTest cases (lock/point/any_lock_manager_test.h) instantiated for the range lock manager, plus
/// prefix ranges, save point / undo semantics and concurrency stress tests.
/// </summary>
[TestClass]
public class RangeLockingTests
{
    private static readonly WriteOptions s_wo = new();

    private sealed class Fixture : IDisposable
    {
        public Fixture(TransactionDbOptions? options = null, ColumnFamilyOptions? cfo = null)
        {
            Fs = NewFs();
            Manager = new RangeLockManager();
            Options = options ?? new TransactionDbOptions();
            Options.RangeLockManager = Manager;
            Db = TransactionDatabase.Open(DbPath, NewOptions(Fs), Options,
                [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, cfo ?? new ColumnFamilyOptions())]);
        }

        public MemoryFileSystem Fs { get; }
        public RangeLockManager Manager { get; }
        public TransactionDbOptions Options { get; }
        public TransactionDatabase Db { get; }

        public Transaction Begin(TimeSpan lockTimeout) => Db.BeginTransaction(s_wo, new TransactionOptions { LockTimeout = lockTimeout });

        public Transaction Begin() => Db.BeginTransaction(s_wo);

        public void Dispose() => Db.Dispose();
    }

    private static readonly TimeSpan s_short = TimeSpan.FromMilliseconds(50);

    private static RangeLockEndpoint E(string key) => new(B(key));

    private static RangeLockEndpoint E(string key, bool infSuffix) => new(B(key), infSuffix);

    private static void AssertFails(StatusCode code, Action a, StatusSubCode sub = StatusSubCode.None)
    {
        var e = Assert.ThrowsExactly<RocksDbException>(a);
        Assert.AreEqual(code, e.Code, e.Message);
        if (sub != StatusSubCode.None) Assert.AreEqual(sub, e.SubCode, e.Message);
    }

    private static void AssertTimesOut(Action a) => AssertFails(StatusCode.TimedOut, a, StatusSubCode.LockTimeout);

    private static IReadOnlyList<ulong> WaitingFor(Transaction t, out uint cf, out byte[]? key) =>
        ((PessimisticTransaction)t).GetWaitingTransactions(out cf, out key);

    /// <summary>Waits (bounded) until <paramref name="t"/> is registered as waiting for a lock.</summary>
    private static void WaitUntilWaiting(Transaction t)
    {
        Assert.IsTrue(SpinWait.SpinUntil(() => WaitingFor(t, out _, out _).Count > 0, TimeSpan.FromSeconds(20)), "transaction never started waiting");
    }

    private static string Show(RangeLockEndpoint e) => S(e.Key.ToArray()) + (e.InfSuffix ? "+" : "");

    // ---- range_locking_test.cc --------------------------------------------------------------------------

    [TestMethod]
    public void BasicRangeLocking()
    {
        using var f = new Fixture(new TransactionDbOptions { TransactionLockTimeout = s_short });
        using Transaction txn0 = f.Begin();
        using Transaction txn1 = f.Begin();
        // Get a range lock
        txn0.GetRangeLock(null, E("a"), E("c"));
        // Check that range Lock inhibits an overlapping range lock
        AssertTimesOut(() => txn1.GetRangeLock(null, E("b"), E("z")));
        // Check that range Lock inhibits an overlapping point lock
        AssertTimesOut(() => txn1.GetForUpdate(B("b")));
        // Get a point lock, check that it inhibits range locks
        txn0.Put(B("n"), B("value"));
        AssertTimesOut(() => txn1.GetRangeLock(null, E("m"), E("p")));
        // Non-overlapping ranges and keys are independent
        txn1.GetRangeLock(null, E("d"), E("m"));
        txn1.Put(B("o"), B("x"));
        txn0.Commit();
        txn1.Rollback();
        Assert.AreEqual("value", GetString(f.Db.Database, "n"));
        Assert.AreEqual(0, f.Manager.GetRangeLockStatusData().Count);
        Assert.AreEqual(0UL, f.Manager.GetStatus().CurrentLockMemory);
    }

    [TestMethod]
    public void PointRangeLockWithReverseComparator()
    {
        using var f = new Fixture(new TransactionDbOptions(), new ColumnFamilyOptions { Comparator = Comparator.ReverseBytewise });
        using Transaction txn0 = f.Begin(s_short);
        using Transaction txn1 = f.Begin(s_short);
        // [infimum("a"), supremum("a")] : same user key, infimum -> supremum.
        txn0.GetRangeLock(null, E("a", false), E("a", true));
        AssertTimesOut(() => txn1.Put(B("a"), B("1")));
        // On a reverse-ordered column family the caller passes flipped ranges: [z, b] covers b..z.
        txn0.GetRangeLock(null, E("z"), E("b"));
        AssertTimesOut(() => txn1.Put(B("m"), B("1")));
        txn1.Put(B("0"), B("1"));
        txn0.Rollback();
        txn1.Put(B("m"), B("1"));
        txn1.Commit();
    }

    [TestMethod]
    public void RangeLockWithTimestampComparator()
    {
        using var f = new Fixture(new TransactionDbOptions(), new ColumnFamilyOptions { Comparator = Comparator.BytewiseWithU64Ts });
        using Transaction txn0 = f.Begin(s_short);
        using Transaction txn1 = f.Begin(s_short);
        // Acquire a range lock [a, c]: endpoints carry no timestamp.
        txn0.GetRangeLock(null, E("a"), E("c"));
        // Overlapping range [b, z] should time out.
        AssertTimesOut(() => txn1.GetRangeLock(null, E("b"), E("z")));
        // Non-overlapping range [d, f] should succeed.
        txn1.GetRangeLock(null, E("d"), E("f"));
        txn0.Rollback();
        txn1.Rollback();
    }

    [TestMethod]
    public void MyRocksLikeUpdate()
    {
        using var f = new Fixture();
        using Transaction txn0 = f.Begin();
        using Transaction txn1 = f.Begin(s_short);
        // Get a range lock for the range we are about to update, then write inside it: granted at once (consolidated).
        txn0.GetRangeLock(null, E("a"), E("c"));
        txn0.Put(B("b"), B("value"));
        txn0.Put(B("a"), B("value"));
        AssertTimesOut(() => txn1.Put(B("b"), B("other")));
        txn0.Commit();
        Assert.AreEqual("value", GetString(f.Db.Database, "b"));
        txn1.Put(B("b"), B("other"));
        txn1.Commit();
    }

    [TestMethod]
    public void UpgradeLockAndGetConflict()
    {
        using var f = new Fixture();
        using Transaction txn0 = f.Begin(TimeSpan.FromMilliseconds(10));
        using Transaction txn1 = f.Begin(TimeSpan.FromMilliseconds(10));
        // Get the shared lock in txn0 and on the same key in txn1
        Assert.IsNull(txn0.GetForUpdate(null, null!, B("a"), exclusive: false));
        Assert.IsNull(txn1.GetForUpdate(null, null!, B("a"), exclusive: false));
        // Now, try getting an exclusive lock that overlaps with the above
        AssertTimesOut(() => txn0.GetRangeLock(null, E("a"), E("b")));
        List<RangeLockInfo> locks = f.Manager.GetRangeLockStatusData();
        Assert.AreEqual(1, locks.Count);
        Assert.IsFalse(locks[0].Exclusive);
        CollectionAssert.AreEquivalent(new[] { txn0.Id, txn1.Id }, locks[0].TransactionIds.ToArray());
        txn0.Rollback();
        txn1.Rollback();
        Assert.AreEqual(0, f.Manager.GetRangeLockStatusData().Count);
        Assert.AreEqual(0UL, f.Manager.GetStatus().CurrentLockMemory);
    }

    [TestMethod]
    public void SnapshotValidation()
    {
        using var f = new Fixture();
        using Transaction txn0 = f.Begin();
        txn0.Put(B("k"), B("initial"));
        txn0.Commit();

        using Transaction txn1 = f.Begin();
        txn1.SetSnapshot();
        Assert.AreEqual("initial", S(txn1.Get(B("k"))));
        txn1.Put(B("k"), B("initial-txn1"));

        using Transaction txn2 = f.Begin();
        txn2.SetSnapshot();
        // This will see the original value as nothing is committed (Get does not lock)
        Assert.AreEqual("initial", S(txn2.Get(B("k"))));
        txn1.Commit();
        // Now, this call should do Snapshot Validation and fail
        AssertFails(StatusCode.Busy, () => txn2.Put(B("k"), B("initial-txn2")));
        txn2.Commit();
        Assert.AreEqual("initial-txn1", GetString(f.Db.Database, "k"));
    }

    [TestMethod]
    public void MultipleTrxLockStatusData()
    {
        using var f = new Fixture();
        using Transaction txn0 = f.Begin();
        using Transaction txn1 = f.Begin();
        txn0.GetRangeLock(null, E("z"), E("z"));
        txn1.GetRangeLock(null, E("b"), E("e"));
        List<RangeLockInfo> s = f.Manager.GetRangeLockStatusData();
        Assert.AreEqual(2, s.Count);
        foreach (RangeLockInfo val in s)
        {
            Assert.AreEqual(f.Db.Database.DefaultColumnFamily.Id, val.ColumnFamilyId);
            Assert.IsFalse(val.Start.InfSuffix);
            Assert.IsFalse(val.End.InfSuffix);
            Assert.IsTrue(val.Exclusive);
            Assert.AreEqual(1, val.TransactionIds.Count);
            if (val.TransactionIds[0] == txn0.Id)
            {
                Assert.AreEqual("z", Show(val.Start));
                Assert.AreEqual("z", Show(val.End));
            }
            else
            {
                Assert.AreEqual(txn1.Id, val.TransactionIds[0]);
                Assert.AreEqual("b", Show(val.Start));
                Assert.AreEqual("e", Show(val.End));
            }
        }
        // The point lock status reports the start keys (GetPointLockStatus).
        List<KeyLockInfo> points = f.Db.GetLockStatusData();
        Assert.AreEqual(2, points.Count);
        Assert.IsTrue(f.Db.IsRangeLocking);
    }

    [TestMethod]
    public void SingleTransactionRangesAreReportedUnmergedUntilAnotherTransactionLocks()
    {
        using var f = new Fixture();
        using Transaction txn0 = f.Begin();
        using Transaction txn1 = f.Begin();
        // Single transaction optimization: the overlapping ranges are kept as acquired.
        txn0.GetRangeLock(null, E("a"), E("c"));
        txn0.GetRangeLock(null, E("b"), E("d"));
        Assert.AreEqual(2, f.Manager.GetRangeLockStatusData().Count);
        // Another transaction's request migrates them into the tree, consolidated.
        txn1.GetRangeLock(null, E("x"), E("y"));
        List<RangeLockInfo> s = f.Manager.GetRangeLockStatusData();
        Assert.AreEqual(2, s.Count);
        Assert.AreEqual("a", Show(s[0].Start));
        Assert.AreEqual("d", Show(s[0].End));
        Assert.AreEqual(txn0.Id, s[0].TransactionIds[0]);
        Assert.AreEqual("x", Show(s[1].Start));
    }

    [TestMethod]
    public void BasicLockEscalation()
    {
        using var f = new Fixture();
        RangeLockManagerStatus counters = f.Manager.GetStatus();
        // Initially not using any lock memory
        Assert.AreEqual(0UL, counters.CurrentLockMemory);
        Assert.AreEqual(0UL, counters.EscalationCount);
        Assert.IsTrue(f.Manager.SetMaxLockMemory(2000));
        using Transaction txn = f.Begin();
        // Get the locks until we hit an escalation
        for (int i = 0; i < 2020; i++)
        {
            string k = i.ToString("D8", System.Globalization.CultureInfo.InvariantCulture);
            txn.GetRangeLock(null, E(k), E(k));
        }
        counters = f.Manager.GetStatus();
        Assert.IsTrue(counters.EscalationCount > 0);
        // The limit is checked before each acquisition, so the last lock may exceed it by its own size (upstream asserts
        // <= 2000 with its native structure sizes; the managed estimate lands on a different escalation cycle).
        Assert.IsTrue(counters.CurrentLockMemory <= 2000 + 400, counters.CurrentLockMemory.ToString());
        // The escalated range covers the gaps between the locked keys.
        using Transaction other = f.Begin(s_short);
        AssertTimesOut(() => other.Put(B("00000005-x"), B("v")));
        // Cannot lower the limit below the memory in use (EDOM)
        Assert.IsFalse(f.Manager.SetMaxLockMemory(1));
        txn.Rollback();
        Assert.AreEqual(0UL, f.Manager.GetStatus().CurrentLockMemory);
        other.Put(B("00000005-x"), B("v"));
    }

    [TestMethod]
    public void LockEscalationBarrier()
    {
        using var f = new Fixture();
        Assert.AreEqual(0UL, f.Manager.GetStatus().EscalationCount);
        f.Manager.SetMaxLockMemory(8000);
        f.Manager.SetEscalationBarrier(static (a, b) => !a.Key.Span[..2].SequenceEqual(b.Key.Span[..2]));
        // Insert enough locks to cause lock escalations to happen
        using Transaction txn = f.Begin();
        const int N = 2000;
        for (int i = 0; i < N; i++)
        {
            string k = i.ToString("D4", System.Globalization.CultureInfo.InvariantCulture);
            txn.GetRangeLock(null, E(k), E(k));
        }
        Assert.IsTrue(f.Manager.GetStatus().EscalationCount > 0);
        // Check that lock escalation was not performed across escalation barriers: another transaction can lock near
        // the barriers.
        using Transaction txn2 = f.Begin(s_short);
        Assert.IsTrue(f.Manager.SetMaxLockMemory(500000));
        for (int i = 100; i < N; i += 100)
        {
            string k = (i - 1).ToString("D4", System.Globalization.CultureInfo.InvariantCulture) + "-a";
            txn2.GetRangeLock(null, E(k), E(k));
        }
        // ...but not inside an escalated range.
        AssertTimesOut(() => txn2.GetRangeLock(null, E("0150-a"), E("0150-a")));
        txn.Rollback();
        txn2.Rollback();
        Assert.AreEqual(0UL, f.Manager.GetStatus().CurrentLockMemory);
    }

    [TestMethod]
    public void OutOfLockMemoryFailsWithLockLimit()
    {
        using var f = new Fixture();
        f.Manager.SetMaxLockMemory(1000);
        // Barrier everywhere: escalation cannot merge anything, so the memory stays exhausted.
        f.Manager.SetEscalationBarrier(static (_, _) => true);
        using Transaction a = f.Begin();
        using Transaction b = f.Begin();
        RocksDbException? failure = null;
        for (int i = 0; i < 100 && failure is null; i++)
        {
            string k = i.ToString("D4", System.Globalization.CultureInfo.InvariantCulture);
            try
            {
                (i % 2 == 0 ? a : b).GetRangeLock(null, E(k), E(k));
            }
            catch (RocksDbException e)
            {
                failure = e;
            }
        }
        Assert.IsNotNull(failure);
        Assert.AreEqual(StatusCode.Aborted, failure.Code);
        Assert.AreEqual(StatusSubCode.LockLimit, failure.SubCode);
        a.Rollback();
        b.Rollback();
        Assert.AreEqual(0UL, f.Manager.GetStatus().CurrentLockMemory);
    }

    [TestMethod]
    public void LockWaitCount()
    {
        using var f = new Fixture();
        using Transaction txn0 = f.Begin(s_short);
        using Transaction txn1 = f.Begin(s_short);
        txn0.GetRangeLock(null, E("a"), E("c"));
        ulong waits1 = f.Manager.GetStatus().LockWaitCount;
        // Attempt to get a conflicting lock
        AssertTimesOut(() => txn1.GetRangeLock(null, E("b"), E("z")));
        // Check that the counter was incremented
        Assert.AreEqual(waits1 + 1, f.Manager.GetStatus().LockWaitCount);
        txn0.Rollback();
        txn1.Rollback();
    }

    [TestMethod]
    public async Task LockWaiteeAccess()
    {
        using var f = new Fixture();
        using Transaction txn0 = f.Begin(TimeSpan.FromSeconds(30));
        using Transaction txn1 = f.Begin(TimeSpan.FromSeconds(30));
        txn0.GetRangeLock(null, E("a"), E("c"));
        // Attempt to get a conflicting lock: waits
        Task waiting = txn1.GetRangeLockAsync(null, E("b"), E("z")).AsTask();
        WaitUntilWaiting(txn1);
        IReadOnlyList<ulong> ids = WaitingFor(txn1, out uint cf, out byte[]? key);
        Assert.AreEqual(0u, cf);
        Assert.AreEqual("b", S(key));
        CollectionAssert.AreEqual(new[] { txn0.Id }, ids.ToArray());
        Assert.IsFalse(waiting.IsCompleted);
        // Release locks: the waiter is granted
        txn0.Rollback();
        await waiting;
        Assert.AreEqual(0, WaitingFor(txn1, out _, out _).Count);
        txn1.Rollback();
    }

    [TestMethod]
    public void KilledCallbackAbortsWait()
    {
        using var f = new Fixture();
        bool killed = false;
        f.Manager.SetIsKilledCallback(() => Volatile.Read(ref killed));
        // Large timeout so a timeout cannot masquerade as the interrupt.
        using Transaction txn0 = f.Begin(TimeSpan.FromSeconds(100));
        using Transaction txn1 = f.Begin(TimeSpan.FromSeconds(100));
        txn0.GetRangeLock(null, E("a"), E("c"));
        Volatile.Write(ref killed, true);
        AssertFails(StatusCode.Aborted, () => txn1.GetRangeLock(null, E("b"), E("z")));
        txn0.Rollback();
        txn1.Rollback();
    }

    [TestMethod]
    public async Task KilledCallbackInterruptsAWaitInProgress()
    {
        using var f = new Fixture();
        int killed = 0;
        f.Manager.SetIsKilledCallback(() => Volatile.Read(ref killed) != 0);
        using Transaction txn0 = f.Begin(TimeSpan.FromSeconds(100));
        using Transaction txn1 = f.Begin(TimeSpan.FromSeconds(100));
        txn0.GetRangeLock(null, E("a"), E("c"));
        Task waiting = txn1.GetRangeLockAsync(null, E("b"), E("z")).AsTask();
        WaitUntilWaiting(txn1);
        Volatile.Write(ref killed, 1);
        var e = await Assert.ThrowsExactlyAsync<RocksDbException>(() => waiting);
        Assert.AreEqual(StatusCode.Aborted, e.Code);
        txn0.Rollback();
    }

    [TestMethod]
    public void KilledCallbackFalseStillTimesOut()
    {
        using var f = new Fixture();
        f.Manager.SetIsKilledCallback(static () => false);
        using Transaction txn0 = f.Begin(s_short);
        using Transaction txn1 = f.Begin(s_short);
        txn0.GetRangeLock(null, E("a"), E("c"));
        AssertTimesOut(() => txn1.GetRangeLock(null, E("b"), E("z")));
        txn0.Rollback();
        txn1.Rollback();
    }

    // ---- AnyLockManagerTest (range lock manager instantiation) ------------------------------------------

    [TestMethod]
    public void ReentrantExclusiveAndSharedLocks()
    {
        using var f = new Fixture();
        using Transaction txn = f.Begin(TimeSpan.Zero);
        txn.GetForUpdate(B("k"));
        txn.GetForUpdate(B("k"));
        txn.GetForUpdate(null, null!, B("s"), exclusive: false);
        txn.GetForUpdate(null, null!, B("s"), exclusive: false);
        txn.Rollback();
        Assert.AreEqual(0, f.Manager.GetRangeLockStatusData().Count);
    }

    [TestMethod]
    public void LockUpgradeAndDowngrade()
    {
        using var f = new Fixture();
        using Transaction txn = f.Begin(TimeSpan.Zero);
        using Transaction other = f.Begin(TimeSpan.Zero);
        other.GetRangeLock(null, E("x"), E("x")); // leave the single transaction mode
        txn.GetForUpdate(null, null!, B("k"), exclusive: false);
        txn.GetForUpdate(null, null!, B("k"), exclusive: true); // upgrade
        AssertTimesOut(() => other.GetForUpdate(null, null!, B("k"), exclusive: false));
        txn.GetForUpdate(null, null!, B("k"), exclusive: false); // stays exclusive
        AssertTimesOut(() => other.GetForUpdate(null, null!, B("k"), exclusive: false));
        txn.Rollback();
        other.Rollback();
    }

    [TestMethod]
    public void LockConflicts()
    {
        using var f = new Fixture();
        using Transaction txn1 = f.Begin(TimeSpan.Zero);
        using Transaction txn2 = f.Begin(TimeSpan.Zero);
        // exclusive-exclusive conflict.
        txn1.GetForUpdate(B("k1"));
        AssertTimesOut(() => txn2.GetForUpdate(B("k1")));
        // exclusive-shared conflict.
        txn1.GetForUpdate(B("k2"));
        AssertTimesOut(() => txn2.GetForUpdate(null, null!, B("k2"), exclusive: false));
        // shared-exclusive conflict.
        txn1.GetForUpdate(null, null!, B("k3"), exclusive: false);
        AssertTimesOut(() => txn2.GetForUpdate(B("k3")));
        // shared-shared: both granted
        txn2.GetForUpdate(null, null!, B("k3"), exclusive: false);
        txn1.Rollback();
        txn2.Rollback();
    }

    [TestMethod]
    public void OverlappingSharedRangesOfDifferentTransactionsConflict()
    {
        using var f = new Fixture();
        using Transaction txn1 = f.Begin(TimeSpan.Zero);
        using Transaction txn2 = f.Begin(TimeSpan.Zero);
        // Shared locks join only on identical endpoints (locktree semantics): the shared point lock of txn1 on "b" and
        // txn1's shared lock on "c" are separate ranges, txn2's shared lock on "b" joins the first.
        txn1.GetForUpdate(null, null!, B("b"), exclusive: false);
        txn1.GetForUpdate(null, null!, B("c"), exclusive: false);
        txn2.GetForUpdate(null, null!, B("b"), exclusive: false);
        List<RangeLockInfo> s = f.Manager.GetRangeLockStatusData();
        Assert.AreEqual(2, s.Count);
        Assert.AreEqual(2, s[0].TransactionIds.Count);
        txn2.Rollback();
        s = f.Manager.GetRangeLockStatusData();
        Assert.AreEqual(1, s[0].TransactionIds.Count);
        Assert.AreEqual(txn1.Id, s[0].TransactionIds[0]);
        txn1.Rollback();
    }

    [TestMethod]
    public async Task DeadlockIsDetected()
    {
        using var f = new Fixture();
        // Range locks detect deadlocks even without TransactionOptions.DeadlockDetect.
        using Transaction txn1 = f.Begin(TimeSpan.FromSeconds(30));
        using Transaction txn2 = f.Begin(TimeSpan.FromSeconds(30));
        txn1.Put(B("k1"), B("1"));
        txn2.Put(B("k2"), B("2"));
        // txn1 tries to lock k2, will be blocked.
        Task t = txn1.PutAsync(B("k2"), B("1")).AsTask();
        WaitUntilWaiting(txn1);
        AssertFails(StatusCode.Busy, () => txn2.Put(B("k1"), B("2")), StatusSubCode.Deadlock);

        List<DeadlockPath> paths = f.Db.GetDeadlockInfoBuffer();
        Assert.AreEqual(1, paths.Count);
        Assert.IsFalse(paths[0].LimitExceeded);
        Assert.AreEqual(2, paths[0].Path.Count);
        Assert.AreEqual(txn1.Id, paths[0].Path[0].TransactionId);
        Assert.AreEqual(0u, paths[0].Path[0].ColumnFamilyId);
        Assert.IsTrue(paths[0].Path[0].Exclusive);
        Assert.AreEqual("k2", S(paths[0].Path[0].WaitingKey));
        Assert.AreEqual(txn2.Id, paths[0].Path[1].TransactionId);
        Assert.AreEqual("k1", S(paths[0].Path[1].WaitingKey));

        List<RangeDeadlockPath> ranges = f.Manager.GetRangeDeadlockInfoBuffer();
        Assert.AreEqual(1, ranges.Count);
        Assert.AreEqual("k2", Show(ranges[0].Path[0].Start));
        Assert.AreEqual("k2", Show(ranges[0].Path[0].End));

        txn2.Rollback();
        await t;
        txn1.Commit();
        Assert.AreEqual("1", GetString(f.Db.Database, "k2"));
    }

    [TestMethod]
    public async Task DeadlockThroughRangesAndThreeTransactions()
    {
        using var f = new Fixture();
        using Transaction t1 = f.Begin(TimeSpan.FromSeconds(30));
        using Transaction t2 = f.Begin(TimeSpan.FromSeconds(30));
        using Transaction t3 = f.Begin(TimeSpan.FromSeconds(30));
        t1.GetRangeLock(null, E("a"), E("c"));
        t2.GetRangeLock(null, E("m"), E("p"));
        t3.GetRangeLock(null, E("x"), E("x", true));
        Task w1 = t1.GetRangeLockAsync(null, E("n"), E("o")).AsTask();   // t1 -> t2
        WaitUntilWaiting(t1);
        Task w2 = t2.PutAsync(B("xyz"), B("2")).AsTask();                // t2 -> t3 (inside the prefix range of "x")
        WaitUntilWaiting(t2);
        AssertFails(StatusCode.Busy, () => t3.GetRangeLock(null, E("b"), E("b")), StatusSubCode.Deadlock); // t3 -> t1
        List<RangeDeadlockPath> d = f.Manager.GetRangeDeadlockInfoBuffer();
        Assert.AreEqual(1, d.Count);
        CollectionAssert.AreEqual(new[] { t1.Id, t2.Id, t3.Id }, d[0].Path.Select(static p => p.TransactionId).ToArray());
        Assert.AreEqual("n", Show(d[0].Path[0].Start));
        Assert.AreEqual("o", Show(d[0].Path[0].End));
        Assert.AreEqual("xyz", Show(d[0].Path[1].Start));
        Assert.AreEqual("b", Show(d[0].Path[2].Start));
        t3.Rollback();
        await w2;
        t2.Rollback();
        await w1;
        t1.Rollback();
        // Resizing the buffer keeps the newest entries
        f.Manager.SetRangeDeadlockInfoBufferSize(0);
        Assert.AreEqual(0, f.Manager.GetRangeDeadlockInfoBuffer().Count);
    }

    [TestMethod]
    public async Task GetWaitingTxnsWithSharedOwners()
    {
        using var f = new Fixture();
        using Transaction txn1 = f.Begin();
        using Transaction txn2 = f.Begin();
        using Transaction txn3 = f.Begin(TimeSpan.FromSeconds(30));
        txn1.GetForUpdate(null, null!, B("k"), exclusive: false);
        txn2.GetForUpdate(null, null!, B("k"), exclusive: false);
        Task t = txn3.PutAsync(B("k"), B("3")).AsTask();
        WaitUntilWaiting(txn3);
        IReadOnlyList<ulong> waiters = WaitingFor(txn3, out uint cf, out byte[]? key);
        Assert.AreEqual(0u, cf);
        Assert.AreEqual("k", S(key));
        CollectionAssert.AreEquivalent(new[] { txn1.Id, txn2.Id }, waiters.ToArray());
        txn1.Rollback();
        // Still waiting for txn2 (the retry after txn1's release reports it)
        Assert.IsTrue(SpinWait.SpinUntil(() => WaitingFor(txn3, out _, out _).Count == 1, TimeSpan.FromSeconds(20)));
        Assert.IsFalse(t.IsCompleted);
        txn2.Rollback();
        await t;
        txn3.Commit();
        Assert.AreEqual("3", GetString(f.Db.Database, "k"));
    }

    // ---- endpoints -----------------------------------------------------------------------------------------

    [TestMethod]
    public void PrefixRangesWithInfiniteSuffix()
    {
        using var f = new Fixture();
        using Transaction owner = f.Begin();
        using Transaction other = f.Begin(TimeSpan.Zero);
        // Every key starting with "ab"
        owner.GetRangeLock(null, E("ab", false), E("ab", true));
        AssertTimesOut(() => other.Put(B("ab"), B("v")));
        AssertTimesOut(() => other.Put(B("abzzzz"), B("v")));
        AssertTimesOut(() => other.Put(B("ab\0"), B("v")));
        other.Put(B("a"), B("v"));
        other.Put(B("aa~"), B("v"));
        other.Put(B("ac"), B("v"));
        // (ab, false) is the first endpoint of the locked prefix range: [a0, ab] overlaps it, [a0, aa+inf] does not.
        AssertTimesOut(() => other.GetRangeLock(null, E("a0"), E("ab")));
        other.GetRangeLock(null, E("a0"), E("aa", true));
        owner.Rollback();
        other.Rollback();
    }

    [TestMethod]
    public void EndpointOrdering()
    {
        // CompareDbtEndpoints on a bytewise comparator
        var tree = new RangeLockTree(new RangeLockManager(), 0, Comparator.Bytewise);
        byte[] a = RangeLockTree.Serialize(B("ab"), false);
        byte[] aInf = RangeLockTree.Serialize(B("ab"), true);
        byte[] abc = RangeLockTree.Serialize(B("abc"), false);
        byte[] abcInf = RangeLockTree.Serialize(B("abc"), true);
        byte[] b = RangeLockTree.Serialize(B("b"), false);
        byte[] empty = RangeLockTree.Serialize([], false);
        byte[] emptyInf = RangeLockTree.Serialize([], true);
        byte[][] sorted = [empty, a, abc, abcInf, aInf, b, emptyInf];
        for (int i = 0; i < sorted.Length; i++)
        {
            for (int j = 0; j < sorted.Length; j++)
            {
                Assert.AreEqual(Math.Sign(i.CompareTo(j)), Math.Sign(tree.Compare(sorted[i], sorted[j])), $"{i} vs {j}");
            }
        }
        RangeLockEndpoint e = RangeLockTree.Deserialize(aInf);
        Assert.AreEqual("ab", S(e.Key.ToArray()));
        Assert.IsTrue(e.InfSuffix);
    }

    // ---- transaction semantics in range mode ------------------------------------------------------------

    [TestMethod]
    public void UndoGetForUpdateAndRollbackToSavePointKeepLocks()
    {
        using var f = new Fixture();
        using Transaction txn = f.Begin();
        using Transaction other = f.Begin(TimeSpan.Zero);
        txn.GetForUpdate(B("a"));
        txn.UndoGetForUpdate(B("a"));
        AssertTimesOut(() => other.Put(B("a"), B("x")));
        txn.SetSavePoint();
        txn.Put(B("b"), B("1"));
        Assert.IsTrue(txn.RollbackToSavePoint());
        AssertTimesOut(() => other.Put(B("b"), B("x")));
        txn.Commit();
        Assert.IsNull(GetString(f.Db.Database, "b"));
        other.Put(B("a"), B("x"));
        other.Put(B("b"), B("x"));
        other.Commit();
    }

    [TestMethod]
    public void DirectWritesAndBatchesTakeRangeLocks()
    {
        using var f = new Fixture(new TransactionDbOptions { DefaultLockTimeout = s_short });
        using Transaction txn = f.Begin();
        txn.GetRangeLock(null, E("k"), E("k", true));
        AssertTimesOut(() => f.Db.Put(B("k1"), B("v")));
        using var batch = new WriteBatch();
        batch.Put(B("a"), B("1"));
        batch.Put(B("k2"), B("2"));
        AssertTimesOut(() => f.Db.Write(null, batch));
        f.Db.Put(B("z"), B("v"));
        txn.Rollback();
        f.Db.Write(null, batch);
        f.Db.Put(B("k1"), B("v"));
        Assert.AreEqual("2", GetString(f.Db.Database, "k2"));
        Assert.AreEqual(0, f.Manager.GetRangeLockStatusData().Count);
        Assert.AreEqual(0UL, f.Manager.GetStatus().CurrentLockMemory);
    }

    [TestMethod]
    public void ColumnFamiliesHaveSeparateLockSpaces()
    {
        using var f = new Fixture();
        ColumnFamily cf = f.Db.Database.CreateColumnFamily("cf1", new ColumnFamilyOptions());
        using Transaction t1 = f.Begin(TimeSpan.Zero);
        using Transaction t2 = f.Begin(TimeSpan.Zero);
        t1.GetRangeLock(cf, E("a"), E("z"));
        t2.GetRangeLock(null, E("a"), E("z"));
        AssertTimesOut(() => t2.Put(cf, B("m"), B("v")));
        List<RangeLockInfo> s = f.Manager.GetRangeLockStatusData();
        CollectionAssert.AreEquivalent(new[] { 0u, cf.Id }, s.Select(static i => i.ColumnFamilyId).ToArray());
        t1.Rollback();
        t2.Rollback();
        f.Db.DropColumnFamily(cf);
    }

    [TestMethod]
    public void UnsupportedConfigurations()
    {
        // GetRangeLock needs the range lock manager
        using (TransactionDatabase db = TransactionDatabase.Open(DbPath, NewOptions(NewFs())))
        {
            using Transaction t = db.BeginTransaction(s_wo);
            AssertFails(StatusCode.NotSupported, () => t.GetRangeLock(null, E("a"), E("b")));
            Assert.IsFalse(db.IsRangeLocking);
        }
        // Range locking is not available with WriteUnprepared
        AssertFails(StatusCode.NotSupported, () => TransactionDatabase.Open(DbPath, NewOptions(NewFs()),
            new TransactionDbOptions { WritePolicy = TransactionWritePolicy.WriteUnprepared, RangeLockManager = new RangeLockManager() }).Dispose());
        // One database at a time
        var mgr = new RangeLockManager();
        using TransactionDatabase first = TransactionDatabase.Open(DbPath, NewOptions(NewFs()), new TransactionDbOptions { RangeLockManager = mgr });
        AssertFails(StatusCode.InvalidArgument, () => TransactionDatabase.Open(DbPath, NewOptions(NewFs()), new TransactionDbOptions { RangeLockManager = mgr }).Dispose());
    }

    [TestMethod]
    public void WritePreparedPolicyWithRangeLocks()
    {
        using var f = new Fixture(new TransactionDbOptions { WritePolicy = TransactionWritePolicy.WritePrepared, WpCommitCacheBits = 10 });
        using Transaction t1 = f.Begin();
        using Transaction t2 = f.Begin(TimeSpan.Zero);
        t1.SetName("xid1");
        t1.GetRangeLock(null, E("a"), E("c"));
        t1.Put(B("b"), B("1"));
        t1.Prepare();
        AssertTimesOut(() => t2.Put(B("a"), B("2")));
        t1.Commit();
        t2.Put(B("a"), B("2"));
        t2.Commit();
        Assert.AreEqual("1", GetString(f.Db.Database, "b"));
        Assert.AreEqual("2", GetString(f.Db.Database, "a"));
    }

    [TestMethod]
    public async Task CancelledWaitLeavesNoPendingRequest()
    {
        using var f = new Fixture();
        using Transaction t1 = f.Begin();
        using Transaction t2 = f.Begin(TimeSpan.FromSeconds(30));
        t1.GetRangeLock(null, E("a"), E("c"));
        using var cts = new CancellationTokenSource();
        Task w = t2.GetRangeLockAsync(null, E("b"), E("b"), cts.Token).AsTask();
        WaitUntilWaiting(t2);
        cts.Cancel();
        await Assert.ThrowsAsync<OperationCanceledException>(() => w);
        t1.Rollback();
        // t2 holds nothing and can lock again
        t2.GetRangeLock(null, E("a"), E("z"));
        Assert.AreEqual(1, f.Manager.GetRangeLockStatusData().Count);
        t2.Rollback();
    }

    // ---- concurrency --------------------------------------------------------------------------------------

    /// <summary>
    /// Many tasks lock two random ranges each (in random order, so deadlocks happen), check that no other task holds
    /// any key of their ranges, move money between two accounts inside a range and commit; deadlocks and timeouts roll
    /// back and retry. The total stays constant and all lock memory is released at the end.
    /// </summary>
    [TestMethod]
    [Timeout(60_000, CooperativeCancellation = true)]
    public async Task ConcurrentRangeTransfersAreMutuallyExclusive()
    {
        using var f = new Fixture();
        const int Accounts = 40;
        const int Tasks = 8;
        const int Iterations = 60;
        static string Acct(int i) => "acct" + i.ToString("D3", System.Globalization.CultureInfo.InvariantCulture);
        for (int i = 0; i < Accounts; i++) f.Db.Put(B(Acct(i)), BitConverter.GetBytes(100L));
        var owners = new int[Accounts];
        int deadlocks = 0, timeouts = 0, commits = 0;
        int seed = Environment.TickCount;
        var tasks = new Task[Tasks];
        for (int t = 0; t < Tasks; t++)
        {
            int me = t + 1;
            tasks[t] = Task.Run(async () =>
            {
                var rnd = new Random(seed + me);
                for (int it = 0; it < Iterations; it++)
                {
                    while (true)
                    {
                        using Transaction txn = f.Db.BeginTransaction(s_wo, new TransactionOptions { LockTimeout = TimeSpan.FromSeconds(2) });
                        var held = new List<int>();
                        try
                        {
                            int lo1 = rnd.Next(Accounts), hi1 = Math.Min(Accounts - 1, lo1 + rnd.Next(4));
                            int lo2 = rnd.Next(Accounts), hi2 = Math.Min(Accounts - 1, lo2 + rnd.Next(4));
                            foreach ((int lo, int hi) in new[] { (lo1, hi1), (lo2, hi2) })
                            {
                                await txn.GetRangeLockAsync(null, E(Acct(lo)), E(Acct(hi)));
                                for (int k = lo; k <= hi; k++)
                                {
                                    int prev = Interlocked.CompareExchange(ref owners[k], me, 0);
                                    if (prev == 0) held.Add(k);
                                    else Assert.AreEqual(me, prev, "account " + k + " locked by two transactions");
                                }
                            }
                            int from = lo1, to = hi1;
                            long a = BitConverter.ToInt64(txn.Get(B(Acct(from)))!);
                            long b = BitConverter.ToInt64(txn.Get(B(Acct(to)))!);
                            if (from != to)
                            {
                                await txn.PutAsync(B(Acct(from)), BitConverter.GetBytes(a - 1));
                                await txn.PutAsync(B(Acct(to)), BitConverter.GetBytes(b + 1));
                            }
                            foreach (int k in held) Volatile.Write(ref owners[k], 0);
                            held.Clear();
                            await txn.CommitAsync();
                            Interlocked.Increment(ref commits);
                            break;
                        }
                        catch (RocksDbException e) when (e.SubCode is StatusSubCode.Deadlock or StatusSubCode.LockTimeout)
                        {
                            if (e.SubCode == StatusSubCode.Deadlock) Interlocked.Increment(ref deadlocks);
                            else Interlocked.Increment(ref timeouts);
                            foreach (int k in held) Volatile.Write(ref owners[k], 0);
                            await txn.RollbackAsync();
                        }
                    }
                }
            });
        }
        await Task.WhenAll(tasks);
        Assert.AreEqual(Tasks * Iterations, commits);
        long total = 0;
        for (int i = 0; i < Accounts; i++) total += BitConverter.ToInt64(f.Db.Database.Get(B(Acct(i)))!);
        Assert.AreEqual(100L * Accounts, total, $"seed={seed} deadlocks={deadlocks} timeouts={timeouts}");
        Assert.AreEqual(0, f.Manager.GetRangeLockStatusData().Count);
        Assert.AreEqual(0UL, f.Manager.GetStatus().CurrentLockMemory);
    }

    /// <summary>Concurrent increments of a few counters through point locks (GetForUpdate) in range mode, sync and async, with escalation pressure.</summary>
    [TestMethod]
    [Timeout(60_000, CooperativeCancellation = true)]
    public async Task ConcurrentPointLockIncrementsWithEscalation()
    {
        using var f = new Fixture();
        f.Manager.SetMaxLockMemory(4_000);
        const int Counters = 5;
        const int Tasks = 6;
        const int Iterations = 150;
        for (int i = 0; i < Counters; i++) f.Db.Put(B("c" + i), BitConverter.GetBytes(0L));
        var tasks = new Task[Tasks];
        for (int t = 0; t < Tasks; t++)
        {
            int me = t;
            tasks[t] = Task.Run(async () =>
            {
                var rnd = new Random(me);
                for (int it = 0; it < Iterations; it++)
                {
                    while (true)
                    {
                        using Transaction txn = f.Db.BeginTransaction(s_wo, new TransactionOptions { LockTimeout = TimeSpan.FromSeconds(5) });
                        try
                        {
                            // Some private keys to grow lock memory and trigger escalations
                            for (int j = 0; j < 5; j++) txn.Put(B("p" + me + "-" + it + "-" + j), B("x"));
                            string key = "c" + rnd.Next(Counters);
                            byte[] v = me % 2 == 0 ? txn.GetForUpdate(B(key))! : (await txn.GetForUpdateAsync(B(key)))!;
                            txn.Put(B(key), BitConverter.GetBytes(BitConverter.ToInt64(v) + 1));
                            await txn.CommitAsync();
                            break;
                        }
                        catch (RocksDbException e) when (e.SubCode is StatusSubCode.Deadlock or StatusSubCode.LockTimeout or StatusSubCode.LockLimit)
                        {
                            await txn.RollbackAsync();
                        }
                    }
                }
            });
        }
        await Task.WhenAll(tasks);
        long total = 0;
        for (int i = 0; i < Counters; i++) total += BitConverter.ToInt64(f.Db.Database.Get(B("c" + i))!);
        Assert.AreEqual((long)Tasks * Iterations, total);
        Assert.IsTrue(f.Manager.GetStatus().EscalationCount > 0);
        Assert.AreEqual(0UL, f.Manager.GetStatus().CurrentLockMemory);
    }
}
