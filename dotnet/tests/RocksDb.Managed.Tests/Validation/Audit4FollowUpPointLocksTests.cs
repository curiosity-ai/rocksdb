using System.Buffers;
using RocksDb.Managed.Env;
using RocksDb.Managed.Utilities.Transactions;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4 (follow-up): PointLockManager.cs against utilities/transactions/lock/point/point_lock_manager.cc.
/// </summary>
[TestClass]
public sealed class Audit4FollowUpPointLocksTests
{
    private static readonly WriteOptions s_wo = new();

    private static TransactionDatabase Open(MemoryFileSystem fs, TransactionDbOptions o) => TransactionDatabase.Open(DbPath, NewOptions(fs), o);

    private static RocksDbException? Lock(Transaction t, ColumnFamily? cf, byte[] key, bool exclusive)
    {
        try
        {
            t.TryGetForUpdate(null, cf!, key, new ArrayBufferWriter<byte>(), exclusive);
            return null;
        }
        catch (RocksDbException e)
        {
            return e;
        }
    }

    private static async Task WaitUntilWaiting(Transaction t)
    {
        var impl = (PessimisticTransaction)t;
        var sw = System.Diagnostics.Stopwatch.StartNew();
        while (impl.GetWaitingTransactions(out _, out _).Count == 0)
        {
            Assert.IsTrue(sw.Elapsed < TimeSpan.FromSeconds(20), "transaction never started waiting");
            await Task.Yield();
        }
    }

    /// <summary>
    /// PointLockManager::AcquireLocked (point_lock_manager.cc:733-806) with lock_timeout 0 (AcquireWithTimeout skips the
    /// retry loop, :635), driven through PessimisticTransaction::TryLock: a brute-force model of the lock table decides
    /// grant / TimedOut(LockTimeout) for shared and exclusive requests and upgrades, and GetLockStatusData must match it.
    /// </summary>
    [TestMethod]
    public void RandomizedLockDecisionsMatchModel()
    {
        int seed = Environment.TickCount;
        var rnd = new Random(seed);
        using TransactionDatabase db = Open(NewFs(), new TransactionDbOptions { NumStripes = 3, TransactionLockTimeout = TimeSpan.Zero });
        ColumnFamily cf1 = db.Database.CreateColumnFamily("cf1", new ColumnFamilyOptions());
        const int Txns = 5, Keys = 7;
        var txns = new Transaction[Txns];
        for (int i = 0; i < Txns; i++) txns[i] = db.BeginTransaction(s_wo);
        // model: (cf, key) -> holders (transaction index) and mode
        var holders = new Dictionary<(int Cf, int Key), (HashSet<int> Ids, bool Exclusive)>();
        try
        {
            for (int step = 0; step < 3000; step++)
            {
                int ti = rnd.Next(Txns);
                string ctx = $"seed={seed} step={step}";
                if (rnd.Next(12) == 0)
                {
                    // Commit / rollback release every lock of the transaction (UnLock(txn, tracker), :984).
                    txns[ti].Rollback();
                    txns[ti].Dispose();
                    txns[ti] = db.BeginTransaction(s_wo);
                    foreach ((int, int) k in holders.Keys.ToArray())
                    {
                        holders[k].Ids.Remove(ti);
                        if (holders[k].Ids.Count == 0) holders.Remove(k);
                    }
                }
                else
                {
                    int cf = rnd.Next(2), key = rnd.Next(Keys);
                    bool exclusive = rnd.Next(2) == 0;
                    bool expectGrant;
                    if (!holders.TryGetValue((cf, key), out (HashSet<int> Ids, bool Exclusive) h))
                    {
                        expectGrant = true;
                        holders[(cf, key)] = (new HashSet<int> { ti }, exclusive);
                    }
                    else if (h.Ids.Contains(ti) && (h.Exclusive || !exclusive))
                    {
                        expectGrant = true; // already locked strongly enough: the lock manager is not asked
                    }
                    else if (h.Exclusive || exclusive)
                    {
                        // "The list contains one txn and we're it, so just take it" (upgrade), else TimedOut.
                        expectGrant = h.Ids.Count == 1 && h.Ids.Contains(ti);
                        if (expectGrant) holders[(cf, key)] = (h.Ids, exclusive);
                    }
                    else
                    {
                        expectGrant = true; // shared request on a shared lock
                        h.Ids.Add(ti);
                    }
                    RocksDbException? e = Lock(txns[ti], cf == 0 ? null : cf1, B("k" + key), exclusive);
                    if (expectGrant)
                    {
                        Assert.IsNull(e, ctx);
                    }
                    else
                    {
                        Assert.IsNotNull(e, ctx);
                        Assert.AreEqual(StatusCode.TimedOut, e.Code, ctx);
                        Assert.AreEqual(StatusSubCode.LockTimeout, e.SubCode, ctx);
                    }
                }
                if (step % 50 == 0 || step == 2999)
                {
                    List<KeyLockInfo> actual = db.GetLockStatusData();
                    Assert.AreEqual(holders.Count, actual.Count, ctx);
                    foreach (KeyLockInfo a in actual)
                    {
                        int key = int.Parse(S(a.Key)!.AsSpan(1), System.Globalization.CultureInfo.InvariantCulture);
                        (HashSet<int> Ids, bool Exclusive) m = holders[(a.ColumnFamilyId == 0 ? 0 : 1, key)];
                        Assert.AreEqual(m.Exclusive, a.Exclusive, ctx);
                        CollectionAssert.AreEquivalent(m.Ids.Select(i => txns[i].Id).ToArray(), a.TransactionIds.ToArray(), ctx);
                    }
                }
            }
        }
        finally
        {
            foreach (Transaction t in txns) t.Dispose();
        }
        Assert.AreEqual(0, db.GetLockStatusData().Count);
    }

    /// <summary>
    /// AcquireLocked :786-802: max_num_locks limits new keys only (a held key reports TimedOut, not the limit); the
    /// failure is Status::LockLimit() = Aborted(kLockLimit); unlocking frees the slot (UnLockKey :830).
    /// </summary>
    [TestMethod]
    public void LockLimitAppliesToNewKeysOnly()
    {
        using TransactionDatabase db = Open(NewFs(), new TransactionDbOptions { MaxNumLocks = 2, TransactionLockTimeout = TimeSpan.Zero });
        using Transaction t1 = db.BeginTransaction(s_wo);
        using Transaction t2 = db.BeginTransaction(s_wo);
        Assert.IsNull(Lock(t1, null, B("a"), true));
        Assert.IsNull(Lock(t1, null, B("b"), true));
        RocksDbException? e = Lock(t2, null, B("c"), true);
        Assert.AreEqual(StatusCode.Aborted, e?.Code);
        Assert.AreEqual(StatusSubCode.LockLimit, e!.SubCode);
        e = Lock(t2, null, B("a"), true);
        Assert.AreEqual(StatusCode.TimedOut, e?.Code);
        Assert.AreEqual(StatusSubCode.LockTimeout, e!.SubCode);
        Assert.IsNull(Lock(t1, null, B("a"), true)); // re-locking an owned key needs no slot
        t1.Rollback();
        Assert.IsNull(Lock(t2, null, B("c"), true));
        Assert.IsNull(Lock(t2, null, B("a"), true));
        Assert.AreEqual(StatusCode.Aborted, Lock(t2, null, B("b"), true)?.Code);
    }

    private static TransactionDatabase OpenForDeadlocks() =>
        Open(NewFs(), new TransactionDbOptions { TransactionLockTimeout = TimeSpan.FromSeconds(60), MaxNumDeadlocks = 5 });

    /// <summary>
    /// IncrementWaiters (:866-959): a 3-cycle t1 -> t2 -> t3 -> t1. With deadlock_detect_depth 2 the breadth-first walk
    /// runs out of depth before reaching the requester ("Wait cycle too big, just assume deadlock": an empty path with
    /// limit_exceeded); with depth 3 the cycle is found and reported in wait order, starting at the oldest edge.
    /// </summary>
    [TestMethod]
    public async Task ThreeCycleDeadlockAndDepthLimit()
    {
        using TransactionDatabase db = OpenForDeadlocks();
        var detect = new TransactionOptions { DeadlockDetect = true };
        using Transaction t1 = db.BeginTransaction(s_wo, detect);
        using Transaction t2 = db.BeginTransaction(s_wo, detect);
        using Transaction t3Shallow = db.BeginTransaction(s_wo, new TransactionOptions { DeadlockDetect = true, DeadlockDetectDepth = 2 });
        t1.Put(B("A"), B("1"));
        t2.Put(B("B"), B("2"));
        t3Shallow.Put(B("C"), B("3"));
        Task w1 = t1.PutAsync(B("B"), B("1")).AsTask();
        await WaitUntilWaiting(t1);
        Task w2 = t2.PutAsync(B("C"), B("2")).AsTask();
        await WaitUntilWaiting(t2);

        var e = await Assert.ThrowsExactlyAsync<RocksDbException>(() => t3Shallow.PutAsync(B("A"), B("3")).AsTask());
        Assert.AreEqual(StatusCode.Busy, e.Code);
        Assert.AreEqual(StatusSubCode.Deadlock, e.SubCode);
        List<DeadlockPath> buffer = db.GetDeadlockInfoBuffer();
        Assert.AreEqual(1, buffer.Count);
        Assert.IsTrue(buffer[0].LimitExceeded);
        Assert.AreEqual(0, buffer[0].Path.Count);
        Assert.IsFalse(w1.IsCompleted);
        Assert.IsFalse(w2.IsCompleted);

        // Same request with depth 3: hand C over to a transaction that searches deep enough.
        using Transaction t3 = db.BeginTransaction(s_wo, new TransactionOptions { DeadlockDetect = true, DeadlockDetectDepth = 3 });
        t3Shallow.Rollback(); // wakes t2, which now gets C
        await w2;
        t2.Rollback(); // wakes t1, which gets B
        await w1;
        // rebuild the cycle: t1 holds A,B; t2 holds nothing
        t2.Put(B("D"), B("2"));
        t3.Put(B("E"), B("3"));
        Task x1 = t1.PutAsync(B("D"), B("1")).AsTask(); // t1 -> t2
        await WaitUntilWaiting(t1);
        Task x2 = t2.PutAsync(B("E"), B("2")).AsTask(); // t2 -> t3
        await WaitUntilWaiting(t2);
        e = await Assert.ThrowsExactlyAsync<RocksDbException>(() => t3.PutAsync(B("A"), B("3")).AsTask()); // t3 -> t1
        Assert.AreEqual(StatusCode.Busy, e.Code);
        Assert.AreEqual(StatusSubCode.Deadlock, e.SubCode);
        buffer = db.GetDeadlockInfoBuffer();
        Assert.AreEqual(2, buffer.Count); // newest first
        Assert.IsFalse(buffer[0].LimitExceeded);
        Assert.AreEqual(3, buffer[0].Path.Count);
        // queue = [t1, t2, t3]; the path is collected from the requester back through the parents and reversed.
        Assert.AreEqual(t1.Id, buffer[0].Path[0].TransactionId);
        Assert.AreEqual("D", S(buffer[0].Path[0].WaitingKey));
        Assert.AreEqual(t2.Id, buffer[0].Path[1].TransactionId);
        Assert.AreEqual("E", S(buffer[0].Path[1].WaitingKey));
        Assert.AreEqual(t3.Id, buffer[0].Path[2].TransactionId);
        Assert.AreEqual("A", S(buffer[0].Path[2].WaitingKey));
        Assert.IsTrue(buffer[0].Path[2].Exclusive);
        Assert.IsTrue(buffer[1].LimitExceeded);

        // The failed request left no trace in the wait-for graph: t3 is not waiting, and breaking the chain works.
        Assert.AreEqual(0, ((PessimisticTransaction)t3).GetWaitingTransactions(out _, out _).Count);
        t3.Rollback();
        await x2;
        t2.Rollback();
        await x1;
        t1.Commit();
        Assert.AreEqual(0, db.GetLockStatusData().Count);
    }

    /// <summary>
    /// IncrementWaiters :896-958: with deadlock_detect_depth 1 a transaction that somebody waits for and that itself
    /// has to wait is reported as a deadlock although there is no cycle (the loop ends after one step: "Wait cycle too
    /// big"); with depth 2 the walk finds the queue exhausted (tail == head) and the transaction waits.
    /// </summary>
    [TestMethod]
    public async Task DepthOneAssumesDeadlockWithoutCycle()
    {
        using TransactionDatabase db = OpenForDeadlocks();
        using Transaction t1 = db.BeginTransaction(s_wo, new TransactionOptions { DeadlockDetect = true });
        using Transaction t4 = db.BeginTransaction(s_wo);
        using Transaction t2 = db.BeginTransaction(s_wo, new TransactionOptions { DeadlockDetect = true, DeadlockDetectDepth = 1 });
        t2.Put(B("B"), B("2"));
        t4.Put(B("D"), B("4"));
        Task w1 = t1.PutAsync(B("B"), B("1")).AsTask(); // t1 -> t2
        await WaitUntilWaiting(t1);
        var e = await Assert.ThrowsExactlyAsync<RocksDbException>(() => t2.PutAsync(B("D"), B("2")).AsTask());
        Assert.AreEqual(StatusCode.Busy, e.Code);
        Assert.AreEqual(StatusSubCode.Deadlock, e.SubCode);
        Assert.IsTrue(db.GetDeadlockInfoBuffer()[0].LimitExceeded);
        t2.Rollback();
        await w1;

        // depth 2, same shape: t1 (holds B) is waited for by t5 and waits for D -> no deadlock, it waits.
        using Transaction t5 = db.BeginTransaction(s_wo, new TransactionOptions { DeadlockDetect = true });
        using Transaction t6 = db.BeginTransaction(s_wo, new TransactionOptions { DeadlockDetect = true, DeadlockDetectDepth = 2 });
        t6.Put(B("F"), B("6"));
        Task w5 = t5.PutAsync(B("F"), B("5")).AsTask(); // t5 -> t6
        await WaitUntilWaiting(t5);
        Task w6 = t6.PutAsync(B("D"), B("6")).AsTask(); // t6 -> t4, no cycle
        await WaitUntilWaiting(t6);
        Assert.IsFalse(w6.IsCompleted);
        Assert.AreEqual(1, db.GetDeadlockInfoBuffer().Count);
        t4.Rollback();
        await w6;
        t6.Rollback();
        await w5;
    }

    /// <summary>
    /// DeadlockInfoBufferTempl (point_lock_manager.h:31-99): a ring of max_num_deadlocks paths, newest first; Resize
    /// to a smaller size keeps the newest paths, to 0 drops everything and records nothing, growing keeps the content.
    /// </summary>
    [TestMethod]
    public async Task DeadlockBufferRingAndResize()
    {
        using TransactionDatabase db = Open(NewFs(), new TransactionDbOptions { TransactionLockTimeout = TimeSpan.FromSeconds(60), MaxNumDeadlocks = 3 });
        var detect = new TransactionOptions { DeadlockDetect = true };
        var victims = new List<ulong>();

        async Task Deadlock()
        {
            using Transaction a = db.BeginTransaction(s_wo, detect);
            using Transaction b = db.BeginTransaction(s_wo, detect);
            a.Put(B("A"), B("1"));
            b.Put(B("B"), B("2"));
            Task w = a.PutAsync(B("B"), B("1")).AsTask();
            await WaitUntilWaiting(a);
            var e = await Assert.ThrowsExactlyAsync<RocksDbException>(() => b.PutAsync(B("A"), B("2")).AsTask());
            Assert.AreEqual(StatusCode.Busy, e.Code);
            victims.Add(b.Id);
            b.Rollback();
            await w;
            a.Rollback();
        }

        ulong[] Victims() => db.GetDeadlockInfoBuffer().Select(p => p.Path[^1].TransactionId).ToArray();

        for (int i = 0; i < 5; i++) await Deadlock();
        CollectionAssert.AreEqual(new[] { victims[4], victims[3], victims[2] }, Victims());
        db.SetDeadlockInfoBufferSize(2);
        CollectionAssert.AreEqual(new[] { victims[4], victims[3] }, Victims());
        db.SetDeadlockInfoBufferSize(4);
        CollectionAssert.AreEqual(new[] { victims[4], victims[3] }, Victims());
        await Deadlock();
        await Deadlock();
        await Deadlock();
        CollectionAssert.AreEqual(new[] { victims[7], victims[6], victims[5], victims[4] }, Victims());
        db.SetDeadlockInfoBufferSize(0);
        Assert.AreEqual(0, db.GetDeadlockInfoBuffer().Count);
        await Deadlock(); // AddNewPath: "if (paths_buffer_.empty()) return;"
        Assert.AreEqual(0, db.GetDeadlockInfoBuffer().Count);
    }

    /// <summary>
    /// PointLockManager::TryLock :581-589: after RemoveColumnFamily the lock map is gone and locking a key of the
    /// dropped column family fails with InvalidArgument("Column family id not found"); UnLock of locks taken before the
    /// drop is a silent no-op (:966 "Column Family must have been dropped").
    /// </summary>
    [TestMethod]
    public void LockingInDroppedColumnFamilyIsInvalidArgument()
    {
        using TransactionDatabase db = Open(NewFs(), new TransactionDbOptions { TransactionLockTimeout = TimeSpan.Zero });
        ColumnFamily cf = db.Database.CreateColumnFamily("cf1", new ColumnFamilyOptions());
        using Transaction t1 = db.BeginTransaction(s_wo);
        using Transaction t2 = db.BeginTransaction(s_wo);
        Assert.IsNull(Lock(t1, cf, B("k"), true));
        db.DropColumnFamily(cf);
        RocksDbException? e = Lock(t2, cf, B("k2"), true);
        Assert.AreEqual(StatusCode.InvalidArgument, e?.Code);
        Assert.AreEqual(0, db.GetLockStatusData().Count);
        t1.Rollback();
        t2.Rollback();
        Assert.AreEqual(0, db.GetLockStatusData().Count);
    }

    private static string Describe(WideColumns c)
    {
        var sb = new System.Text.StringBuilder();
        for (int i = 0; i < c.Count; i++) sb.Append(S(c.GetName(i).ToArray())).Append('=').Append(S(c.GetValue(i).ToArray())).Append(';');
        return sb.ToString();
    }

    private static string Describe(SortedDictionary<string, string> c)
    {
        var sb = new System.Text.StringBuilder();
        foreach (KeyValuePair<string, string> kv in c) sb.Append(kv.Key).Append('=').Append(kv.Value).Append(';');
        return sb.ToString();
    }

    /// <summary>
    /// TransactionBaseImpl::NewMultiCfIterator (transaction_base.cc:491-538) over MultiCfIteratorImpl
    /// (db/multi_cf_iterator_impl.h: SeekCommon, AdvanceIterator, the re-seek in Next / Prev after a direction change),
    /// CoalescingIterator::Coalesce (later column family wins per column name) and AttributeGroupIteratorImpl (groups in
    /// column family order): random committed data plus random uncommitted writes and deletes of the transaction,
    /// random positioning, compared with a model.
    /// </summary>
    [TestMethod]
    public void TransactionMultiCfIteratorsMatchModel()
    {
        int seed = Environment.TickCount;
        var rnd = new Random(seed);
        using TransactionDatabase db = Open(NewFs(), new TransactionDbOptions());
        ColumnFamily[] cfs =
        [
            db.Database.CreateColumnFamily("cf1", new ColumnFamilyOptions()),
            db.Database.CreateColumnFamily("cf2", new ColumnFamilyOptions()),
            db.Database.CreateColumnFamily("cf3", new ColumnFamilyOptions()),
        ];
        string[] names = ["", "a", "b", "c"];
        var model = new SortedDictionary<string, SortedDictionary<string, string>>[cfs.Length];
        for (int i = 0; i < cfs.Length; i++) model[i] = new(StringComparer.Ordinal);
        using Transaction txn = db.BeginTransaction(s_wo);
        int serial = 0;
        for (int phase = 0; phase < 2; phase++) // 0: committed data, 1: the transaction's own writes
        {
            for (int n = 0; n < 40; n++)
            {
                int ci = rnd.Next(cfs.Length);
                string key = "k" + rnd.Next(12);
                string v = "v" + serial++;
                int kind = rnd.Next(phase == 0 ? 2 : 3);
                if (kind == 0)
                {
                    if (phase == 0) cfs[ci].Put(B(key), B(v));
                    else txn.Put(cfs[ci], B(key), B(v));
                    model[ci][key] = new(StringComparer.Ordinal) { [""] = v };
                }
                else if (kind == 1)
                {
                    var cols = new SortedDictionary<string, string>(StringComparer.Ordinal);
                    foreach (string name in names)
                    {
                        if (rnd.Next(2) == 0) cols[name] = v + name;
                    }
                    WideColumn[] wc = cols.Select(kv => new WideColumn(B(kv.Key), B(kv.Value))).ToArray();
                    if (phase == 0) cfs[ci].PutEntity(B(key), wc);
                    else txn.PutEntity(cfs[ci], B(key), wc);
                    model[ci][key] = cols;
                }
                else
                {
                    txn.Delete(cfs[ci], B(key));
                    model[ci].Remove(key);
                }
            }
            if (phase == 0 && rnd.Next(2) == 0) db.Database.Flush();
        }
        string[] keys = model.SelectMany(m => m.Keys).Distinct().OrderBy(k => k, StringComparer.Ordinal).ToArray();

        using TransactionCoalescingIterator co = txn.GetCoalescingIterator(null, cfs);
        using TransactionAttributeGroupIterator ag = txn.GetAttributeGroupIterator(null, cfs);
        int pos = -1; // index into keys, -1 = invalid
        for (int step = 0; step < 600; step++)
        {
            string ctx = $"seed={seed} step={step}";
            int op = rnd.Next(pos < 0 ? 4 : 8);
            string target = "k" + rnd.Next(13) + (rnd.Next(3) == 0 ? "x" : "");
            switch (op)
            {
                case 0:
                    co.SeekToFirst();
                    ag.SeekToFirst();
                    pos = keys.Length > 0 ? 0 : -1;
                    break;
                case 1:
                    co.SeekToLast();
                    ag.SeekToLast();
                    pos = keys.Length - 1;
                    break;
                case 2:
                    co.Seek(B(target));
                    ag.Seek(B(target));
                    pos = Array.FindIndex(keys, k => string.CompareOrdinal(k, target) >= 0);
                    break;
                case 3:
                    co.SeekForPrev(B(target));
                    ag.SeekForPrev(B(target));
                    pos = Array.FindLastIndex(keys, k => string.CompareOrdinal(k, target) <= 0);
                    break;
                case 4 or 5:
                    co.Next();
                    ag.Next();
                    pos = pos + 1 < keys.Length ? pos + 1 : -1;
                    break;
                default:
                    co.Prev();
                    ag.Prev();
                    pos--;
                    break;
            }
            Assert.AreEqual(pos >= 0, co.Valid, ctx);
            Assert.AreEqual(pos >= 0, ag.Valid, ctx);
            if (pos < 0) continue;
            Assert.AreEqual(keys[pos], S(co.Key.ToArray()), ctx);
            Assert.AreEqual(keys[pos], S(ag.Key.ToArray()), ctx);
            var merged = new SortedDictionary<string, string>(StringComparer.Ordinal);
            var groups = new List<string>();
            for (int i = 0; i < cfs.Length; i++)
            {
                if (!model[i].TryGetValue(keys[pos], out SortedDictionary<string, string>? cols)) continue;
                foreach (KeyValuePair<string, string> kv in cols) merged[kv.Key] = kv.Value;
                groups.Add(cfs[i].Id + ":" + Describe(cols));
            }
            Assert.AreEqual(Describe(merged), Describe(co.Columns), ctx);
            Assert.AreEqual(merged.TryGetValue("", out string? dv) ? dv : "", S(co.Value.ToArray()), ctx);
            CollectionAssert.AreEqual(groups, ag.AttributeGroups.Select(g => g.ColumnFamily.Id + ":" + Describe(g.Columns)).ToList(), ctx);
        }
    }

    /// <summary>
    /// OptimisticTransaction::CommitWithParallelValidate (optimistic_transaction.cc:93-148): the bucket locks of the
    /// tracked keys are a set (several keys of one bucket lock it once) and are released on every exit, also when
    /// CheckKeysForConflicts fails. With a single bucket every key shares the lock.
    /// </summary>
    [TestMethod]
    public void ParallelValidationWithOneSharedBucket()
    {
        using OptimisticTransactionDatabase db = OptimisticTransactionDatabase.Open(DbPath, NewOptions(NewFs()),
            new OptimisticTransactionDbOptions { ValidatePolicy = OccValidationPolicy.ValidateParallel, OccLockBuckets = 1 },
            [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, new ColumnFamilyOptions())]);
        ColumnFamily cf1 = db.Database.CreateColumnFamily("cf1", new ColumnFamilyOptions());
        using Transaction t1 = db.BeginTransaction(s_wo);
        using Transaction t2 = db.BeginTransaction(s_wo);
        for (int i = 0; i < 5; i++)
        {
            t1.Put(B("a" + i), B("1"));
            t1.Put(cf1, B("a" + i), B("1"));
        }
        t2.GetForUpdate(B("a3"));
        t2.Put(B("z"), B("2"));
        t1.Commit(); // five keys in two column families, one bucket: locked once
        var e = Assert.ThrowsExactly<RocksDbException>(t2.Commit);
        Assert.AreEqual(StatusCode.Busy, e.Code);
        // A failed commit does not Clear(): the transaction still fails; after a rollback it can be used again.
        e = Assert.ThrowsExactly<RocksDbException>(t2.Commit);
        Assert.AreEqual(StatusCode.Busy, e.Code);
        t2.Rollback();
        t2.Put(B("z"), B("3"));
        t2.Commit(); // the bucket was released by the failed commits
        Assert.AreEqual("3", GetString(db.Database, "z"));
        Assert.AreEqual("1", GetString(cf1, "a4"));
    }
}
