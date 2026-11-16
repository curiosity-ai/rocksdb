using System.Reflection;
using System.Text;
using RocksDb.Managed.Env;
using RocksDb.Managed.Utilities.Backup;
using RocksDb.Managed.Utilities.Transactions;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #8 (the asynchronous implementation), area "AsyncUtilitiesApi": async lock waits of transactions (point and
/// range locks, deadlock detection, cancellation), sync/async parity of transactions, backup, checkpoint and WAL tailing,
/// and an inventory of the public async surface. Reference: the sync managed path and the C++ semantics
/// (utilities/transactions/lock/point/point_lock_manager.cc AcquireWithTimeout, lock/range/range_tree lock_request.cc,
/// pessimistic_transaction.cc, optimistic_transaction.cc, utilities/backup/backup_engine.cc,
/// utilities/checkpoint/checkpoint_impl.cc).
/// </summary>
[TestClass]
public sealed class Audit8AsyncUtilitiesApiTests
{
    private static readonly WriteOptions s_wo = new();
    private static readonly TimeSpan s_guard = TimeSpan.FromSeconds(20);

    private static async Task Guard(Task t, string what)
    {
        if (await Task.WhenAny(t, Task.Delay(s_guard)).ConfigureAwait(false) != t) Assert.Fail(what + " did not complete within " + s_guard);
        await t.ConfigureAwait(false);
    }

    private static void WaitUntilWaiting(Transaction t) =>
        Assert.IsTrue(SpinWait.SpinUntil(() => ((PessimisticTransaction)t).GetWaitingTransactions(out _, out _).Count > 0, s_guard),
            "transaction never started waiting");

    private static TransactionDatabase OpenTxnDb(MemoryFileSystem fs, TransactionDbOptions options) =>
        TransactionDatabase.Open(DbPath, NewOptions(fs), options,
            [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, new ColumnFamilyOptions { MergeOperator = new StringAppendOperator() })]);

    private static string Dump(RocksDatabase db)
    {
        var sb = new StringBuilder();
        using Iterator it = db.NewIterator(null);
        for (it.SeekToFirst(); it.Valid; it.Next()) sb.Append(Encoding.UTF8.GetString(it.Key)).Append('=').Append(Encoding.UTF8.GetString(it.Value)).Append('\n');
        return sb.ToString();
    }

    private static string Outcome(Exception e) => e is RocksDbException r ? "E:" + r.Code + "/" + r.SubCode : "X:" + e.GetType().Name;

    // =====================================================================================================
    // Range locks: a grant racing a cancellation
    // =====================================================================================================

    /// <summary>
    /// Pins: a range lock request that a releasing transaction grants (lock_request::retry from RetryAll, under the
    /// requests mutex) while the waiter is being cancelled must not leave a lock in the tree that the waiter does not
    /// track. Before the fix the waiter threw OperationCanceledException although the tree had granted it the range; the
    /// range was never recorded in the transaction's RangeLockList, so commit / rollback / dispose never released it and
    /// every later request on the range timed out. The race window (cancellation observed, catch block not yet holding
    /// the requests mutex) is forced by holding the tree's requests mutex on the test thread (it is reentrant, so the
    /// grant by the committing transaction runs on the same thread).
    /// </summary>
    [TestMethod]
    public async Task RangeLock_GrantRacingCancellation_DoesNotLeakTheLock()
    {
        MemoryFileSystem fs = NewFs();
        var mgr = new RangeLockManager();
        using TransactionDatabase db = OpenTxnDb(fs, new TransactionDbOptions { RangeLockManager = mgr, TransactionLockTimeout = TimeSpan.FromSeconds(30) });
        using Transaction a = db.BeginTransaction(s_wo);
        a.GetRangeLock(null, new RangeLockEndpoint(B("a")), new RangeLockEndpoint(B("z")));
        Transaction b = db.BeginTransaction(s_wo);
        using var cts = new CancellationTokenSource();
        Task pending = b.GetRangeLockAsync(null, new RangeLockEndpoint(B("b")), new RangeLockEndpoint(B("c")), cts.Token).AsTask();
        WaitUntilWaiting(b);
        RangeLockTree tree = mgr.GetTree(0);
        lock (tree.RequestsMutex)
        {
            // Cancel from another thread (the waiter's continuation may run there and then blocks on the mutex).
            ThreadPool.UnsafeQueueUserWorkItem(static c => c.Cancel(), cts, preferLocal: false);
            Assert.IsTrue(SpinWait.SpinUntil(() => cts.IsCancellationRequested, s_guard));
            // Releases [a, z] and grants b's pending request on this thread.
            a.Commit();
        }
        bool granted;
        try
        {
            await Guard(pending, "range lock wait").ConfigureAwait(false);
            granted = true;
        }
        catch (OperationCanceledException)
        {
            granted = false;
        }
        if (granted)
        {
            // The grant stands: b holds [b, c] until it ends.
            using Transaction probe = db.BeginTransaction(s_wo, new TransactionOptions { LockTimeout = TimeSpan.FromMilliseconds(20) });
            var e = Assert.ThrowsExactly<RocksDbException>(() => probe.GetRangeLock(null, new RangeLockEndpoint(B("b")), new RangeLockEndpoint(B("c"))));
            Assert.AreEqual(StatusCode.TimedOut, e.Code);
        }
        b.Rollback();
        b.Dispose();
        using Transaction c = db.BeginTransaction(s_wo, new TransactionOptions { LockTimeout = TimeSpan.FromMilliseconds(200) });
        c.GetRangeLock(null, new RangeLockEndpoint(B("b")), new RangeLockEndpoint(B("c"))); // TimedOut before the fix (leaked lock)
        c.Put(B("bb"), B("1"));
        c.Commit();
        Assert.AreEqual(0, mgr.GetRangeLockStatusData().Count, "no range lock may survive the transactions that took them");
    }

    // =====================================================================================================
    // Point locks: lock timeouts beyond the range of a timed wait
    // =====================================================================================================

    /// <summary>
    /// Pins: a lock timeout longer than a timed wait can express (int.MaxValue ms, about 24.8 days; TimeSpan.MaxValue
    /// is the usual "forever" idiom) makes a contended lock wait, on the sync (Task.Wait) and async (WaitAsync) paths,
    /// instead of failing with ArgumentOutOfRangeException. Upstream (AcquireWithTimeout) takes any int64 timeout. The
    /// range lock manager already clamped its waits; the point lock manager did not, and between 24.8 and 49.7 days the
    /// sync path threw while the async path waited (a sync/async divergence).
    /// </summary>
    [TestMethod]
    public async Task PointLock_LockTimeoutBeyondTimerRange_WaitsOnSyncAndAsyncPaths()
    {
        MemoryFileSystem fs = NewFs();
        using TransactionDatabase db = OpenTxnDb(fs, new TransactionDbOptions());
        using Transaction holder = db.BeginTransaction(s_wo);
        holder.Put(B("k1"), B("h"));
        holder.Put(B("k2"), B("h"));
        using Transaction syncWaiter = db.BeginTransaction(s_wo, new TransactionOptions { LockTimeout = TimeSpan.FromDays(30) });
        using Transaction asyncWaiter = db.BeginTransaction(s_wo, new TransactionOptions { LockTimeout = TimeSpan.MaxValue });
        Task s = Task.Factory.StartNew(() => syncWaiter.Put(B("k1"), B("s")), CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        Task a = asyncWaiter.PutAsync(B("k2"), B("a")).AsTask();
        await Task.Delay(50).ConfigureAwait(false);
        Assert.IsFalse(s.IsFaulted, s.Exception?.ToString());
        Assert.IsFalse(a.IsFaulted, a.Exception?.ToString());
        WaitUntilWaiting(syncWaiter);
        WaitUntilWaiting(asyncWaiter);
        holder.Commit();
        await Guard(s, "sync lock wait").ConfigureAwait(false);
        await Guard(a, "async lock wait").ConfigureAwait(false);
        syncWaiter.Commit();
        await asyncWaiter.CommitAsync().ConfigureAwait(false);
        Assert.AreEqual("s", S(db.Database.Get(B("k1"))));
        Assert.AreEqual("a", S(db.Database.Get(B("k2"))));
    }

    /// <summary>
    /// Pins: an infinite lock timeout waiting for a holder whose expiration is far away (the wait is bounded by the
    /// holder's expiration, AcquireWithTimeout's cv_end_time = expire_time_hint) waits instead of throwing
    /// ArgumentOutOfRangeException.
    /// </summary>
    [TestMethod]
    public async Task PointLock_FarHolderExpiration_InfiniteWaiterWaits()
    {
        MemoryFileSystem fs = NewFs();
        using TransactionDatabase db = OpenTxnDb(fs, new TransactionDbOptions());
        using Transaction holder = db.BeginTransaction(s_wo, new TransactionOptions { Expiration = TimeSpan.FromDays(60) });
        holder.Put(B("k"), B("h"));
        using Transaction waiter = db.BeginTransaction(s_wo, new TransactionOptions { LockTimeout = Timeout.InfiniteTimeSpan });
        Task a = waiter.PutAsync(B("k"), B("w")).AsTask();
        await Task.Delay(50).ConfigureAwait(false);
        Assert.IsFalse(a.IsFaulted, a.Exception?.ToString());
        WaitUntilWaiting(waiter);
        holder.Commit();
        await Guard(a, "async lock wait").ConfigureAwait(false);
        await waiter.CommitAsync().ConfigureAwait(false);
        Assert.AreEqual("w", S(db.Database.Get(B("k"))));
    }

    /// <summary>
    /// Pins: with <see cref="TransactionDbOptions.CustomMutexFactory"/> = <see cref="TransactionDbMutexFactory.Default"/>
    /// a lock timeout beyond int.MaxValue ms works (the default mutex's TryLockFor passed it to Lock.TryEnter, which
    /// threw ArgumentOutOfRangeException even for an uncontended key, from both the sync and the async API).
    /// </summary>
    [TestMethod]
    public async Task CustomMutex_LongLockTimeout_Works()
    {
        MemoryFileSystem fs = NewFs();
        using TransactionDatabase db = OpenTxnDb(fs, new TransactionDbOptions { CustomMutexFactory = TransactionDbMutexFactory.Default });
        using Transaction t = db.BeginTransaction(s_wo, new TransactionOptions { LockTimeout = TimeSpan.FromDays(30) });
        t.Put(B("k1"), B("1"));
        await t.PutAsync(B("k2"), B("2")).ConfigureAwait(false);
        await t.CommitAsync().ConfigureAwait(false);
        Assert.AreEqual("1", S(db.Database.Get(B("k1"))));
        Assert.AreEqual("2", S(db.Database.Get(B("k2"))));
    }

    // =====================================================================================================
    // Point locks: cancellation, deadlock detection, mixed sync / async contention
    // =====================================================================================================

    /// <summary>
    /// Pins: cancelling an async point-lock wait throws OperationCanceledException, removes the waiter (no waiting-txn
    /// info left), unregisters it from the wait-for graph (a later wait in the opposite direction is a plain timeout, not
    /// a false deadlock) and leaves the transaction usable.
    /// </summary>
    [TestMethod]
    public async Task PointLock_CancelledAsyncWait_CleansUpWaitGraphAndWaiter()
    {
        MemoryFileSystem fs = NewFs();
        using TransactionDatabase db = OpenTxnDb(fs, new TransactionDbOptions());
        var opts = new TransactionOptions { DeadlockDetect = true, LockTimeout = TimeSpan.FromSeconds(30) };
        using Transaction t1 = db.BeginTransaction(s_wo, opts);
        using Transaction t2 = db.BeginTransaction(s_wo, opts);
        t1.Put(B("A"), B("1"));
        t2.Put(B("B"), B("2"));
        using var cts = new CancellationTokenSource();
        Task wait = t2.PutAsync(B("A"), B("x"), cts.Token).AsTask();
        WaitUntilWaiting(t2);
        cts.Cancel();
        await Assert.ThrowsAsync<OperationCanceledException>(() => Guard(wait, "cancelled wait")).ConfigureAwait(false);
        Assert.AreEqual(0, ((PessimisticTransaction)t2).GetWaitingTransactions(out _, out _).Count);
        // t1 -> t2 must be a timeout: t2 no longer waits for t1.
        t1.SetLockTimeout(TimeSpan.FromMilliseconds(50));
        var e = Assert.ThrowsExactly<RocksDbException>(() => t1.Put(B("B"), B("y")));
        Assert.AreEqual(StatusCode.TimedOut, e.Code);
        Assert.AreEqual(StatusSubCode.LockTimeout, e.SubCode);
        Assert.AreEqual(0, db.GetDeadlockInfoBuffer().Count);
        t1.Commit();
        t2.Put(B("A"), B("z")); // t2 still usable
        await t2.CommitAsync().ConfigureAwait(false);
        Assert.AreEqual("z", S(db.Database.Get(B("A"))));
        Assert.AreEqual("2", S(db.Database.Get(B("B"))));
    }

    /// <summary>
    /// Pins: deadlock detection sees async waiters: an async waiter's wait-for edge is visible to a sync requester (and
    /// vice versa), the request closing the cycle fails with Busy(Deadlock) and the path is recorded; the async waiter is
    /// then granted once the other transaction rolls back.
    /// </summary>
    [TestMethod]
    public async Task DeadlockDetection_SeesAsyncWaiters()
    {
        MemoryFileSystem fs = NewFs();
        using TransactionDatabase db = OpenTxnDb(fs, new TransactionDbOptions());
        var opts = new TransactionOptions { DeadlockDetect = true, LockTimeout = TimeSpan.FromSeconds(30) };
        for (int round = 0; round < 2; round++)
        {
            using Transaction t1 = db.BeginTransaction(s_wo, opts);
            using Transaction t2 = db.BeginTransaction(s_wo, opts);
            t1.Put(B("A"), B("1"));
            t2.Put(B("B"), B("2"));
            Task waiting;
            if (round == 0)
            {
                waiting = t1.PutAsync(B("B"), B("1")).AsTask(); // async waiter
            }
            else
            {
                waiting = Task.Factory.StartNew(() => t1.Put(B("B"), B("1")), CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
            }
            WaitUntilWaiting(t1);
            RocksDbException e = round == 0
                ? Assert.ThrowsExactly<RocksDbException>(() => t2.Put(B("A"), B("2")))
                : await Assert.ThrowsAsync<RocksDbException>(async () => await t2.PutAsync(B("A"), B("2")).ConfigureAwait(false)).ConfigureAwait(false);
            Assert.AreEqual(StatusCode.Busy, e.Code);
            Assert.AreEqual(StatusSubCode.Deadlock, e.SubCode);
            t2.Rollback();
            await Guard(waiting, "waiter after deadlock victim rolled back").ConfigureAwait(false);
            t1.Commit();
        }
        Assert.AreEqual(2, db.GetDeadlockInfoBuffer().Count);
    }

    /// <summary>
    /// Pins: sync and async transactions contending on the same hot keys (read-modify-write with GetForUpdate) make
    /// progress and produce the same final state as serial execution (no lost update, no deadlock, no starvation within
    /// the guard).
    /// </summary>
    [TestMethod]
    public async Task MixedSyncAsyncContention_NoLostUpdates()
    {
        MemoryFileSystem fs = NewFs();
        using TransactionDatabase db = OpenTxnDb(fs, new TransactionDbOptions());
        const int Workers = 4, PerWorker = 40, Keys = 3;
        for (int k = 0; k < Keys; k++) db.Put(B("c" + k), B("0"));
        var opts = new TransactionOptions { DeadlockDetect = true, LockTimeout = TimeSpan.FromSeconds(5) };
        int retries = 0;

        void SyncWorker(int w)
        {
            for (int i = 0; i < PerWorker; i++)
            {
                byte[] key = B("c" + ((w + i) % Keys));
                while (true)
                {
                    using Transaction t = db.BeginTransaction(s_wo, opts);
                    try
                    {
                        int n = int.Parse(S(t.GetForUpdate(key))!, System.Globalization.CultureInfo.InvariantCulture);
                        t.Put(key, B((n + 1).ToString(System.Globalization.CultureInfo.InvariantCulture)));
                        t.Commit();
                        break;
                    }
                    catch (RocksDbException e) when (e.Code is StatusCode.Busy or StatusCode.TimedOut)
                    {
                        Interlocked.Increment(ref retries);
                    }
                }
            }
        }

        async Task AsyncWorker(int w)
        {
            await Task.Yield();
            for (int i = 0; i < PerWorker; i++)
            {
                byte[] key = B("c" + ((w + i) % Keys));
                while (true)
                {
                    using Transaction t = db.BeginTransaction(s_wo, opts);
                    try
                    {
                        int n = int.Parse(S(await t.GetForUpdateAsync(key).ConfigureAwait(false))!, System.Globalization.CultureInfo.InvariantCulture);
                        await t.PutAsync(key, B((n + 1).ToString(System.Globalization.CultureInfo.InvariantCulture))).ConfigureAwait(false);
                        await t.CommitAsync().ConfigureAwait(false);
                        break;
                    }
                    catch (RocksDbException e) when (e.Code is StatusCode.Busy or StatusCode.TimedOut)
                    {
                        Interlocked.Increment(ref retries);
                    }
                }
            }
        }

        var tasks = new List<Task>();
        for (int w = 0; w < Workers; w++)
        {
            int id = w;
            tasks.Add(Task.Factory.StartNew(() => SyncWorker(id), CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default));
            tasks.Add(AsyncWorker(id + Workers));
        }
        await Guard(Task.WhenAll(tasks), "contending workers").ConfigureAwait(false);
        int total = 0;
        for (int k = 0; k < Keys; k++) total += int.Parse(S(db.Database.Get(B("c" + k)))!, System.Globalization.CultureInfo.InvariantCulture);
        Assert.AreEqual(2 * Workers * PerWorker, total, "retries: " + retries);
    }

    // =====================================================================================================
    // Sync / async differential programs
    // =====================================================================================================

    private readonly record struct Step(int Slot, int Op, int Key, bool Snapshot, int Flush);

    private static Step[] Program(int seed, int steps)
    {
        var rnd = new Random(seed);
        var s = new Step[steps];
        for (int i = 0; i < steps; i++) s[i] = new Step(rnd.Next(3), rnd.Next(14), rnd.Next(8), rnd.Next(3) == 0, rnd.Next(3));
        return s;
    }

    /// <summary>
    /// A seeded program over three interleaved pessimistic transactions (lock timeout 0, so conflicts fail at once and
    /// deterministically) plus direct writes; returns the per-op outcomes and the final DB contents.
    /// </summary>
    private static async Task<string> RunPessimistic(Step[] program, TransactionWritePolicy policy, bool useAsync)
    {
        MemoryFileSystem fs = NewFs();
        var log = new StringBuilder();
        var txo = new TransactionDbOptions { WritePolicy = policy, TransactionLockTimeout = TimeSpan.Zero, DefaultLockTimeout = TimeSpan.Zero };
        using TransactionDatabase db = OpenTxnDb(fs, txo);
        var txns = new Transaction?[3];
        var named = new bool[3];
        int nameSeq = 0;
        for (int i = 0; i < program.Length; i++)
        {
            Step st = program[i];
            Transaction? t = txns[st.Slot];
            byte[] key = B("k" + st.Key);
            byte[] val = B("v" + i);
            string r = "ok";
            try
            {
                if (t is null && st.Op < 12)
                {
                    txns[st.Slot] = db.BeginTransaction(s_wo, new TransactionOptions
                    {
                        SetSnapshot = st.Snapshot,
                        SkipPrepare = true,
                        WriteBatchFlushThreshold = policy == TransactionWritePolicy.WriteUnprepared && st.Flush == 0 ? 1 : -1,
                    });
                    named[st.Slot] = false;
                    r = "begin";
                }
                else
                {
                    switch (st.Op)
                    {
                        case 0: if (useAsync) await t!.PutAsync(key, val).ConfigureAwait(false); else t!.Put(key, val); break;
                        case 1: if (useAsync) await t!.DeleteAsync(key).ConfigureAwait(false); else t!.Delete(key); break;
                        case 2: if (useAsync) await t!.MergeAsync(key, val).ConfigureAwait(false); else t!.Merge(key, val); break;
                        case 3: r = "gfu=" + S(useAsync ? await t!.GetForUpdateAsync(key).ConfigureAwait(false) : t!.GetForUpdate(key)); break;
                        case 4: r = "get=" + S(useAsync ? await t!.GetAsync(key).ConfigureAwait(false) : t!.Get(key)); break;
                        case 5: t!.SetSavePoint(); break;
                        case 6: r = "rtsp=" + t!.RollbackToSavePoint(); break;
                        case 7:
                        case 8:
                            if (st.Op == 7) { if (useAsync) await t!.CommitAsync().ConfigureAwait(false); else t!.Commit(); }
                            else { if (useAsync) await t!.RollbackAsync().ConfigureAwait(false); else t!.Rollback(); }
                            t!.Dispose();
                            txns[st.Slot] = null;
                            break;
                        case 9:
                            if (!named[st.Slot]) { t!.SetName("n" + nameSeq++); named[st.Slot] = true; }
                            if (useAsync) await t!.PrepareAsync().ConfigureAwait(false); else t!.Prepare();
                            r = "state=" + t.State;
                            break;
                        case 10: if (useAsync) await db.PutAsync(key, val).ConfigureAwait(false); else db.Put(key, val); break;
                        case 11: if (useAsync) await db.MergeAsync(key, val).ConfigureAwait(false); else db.Merge(key, val); break;
                        default: r = "dbget=" + S(useAsync ? await db.Database.GetAsync(key).ConfigureAwait(false) : db.Database.Get(key)); break;
                    }
                }
            }
            catch (Exception e) when (e is RocksDbException or InvalidOperationException or ObjectDisposedException or ArgumentException)
            {
                r = Outcome(e);
            }
            log.Append(i).Append(' ').Append(st.Op).Append(' ').Append(r).Append('\n');
        }
        for (int s = 0; s < txns.Length; s++)
        {
            Transaction? t = txns[s];
            if (t is null) continue;
            try
            {
                if (useAsync) await t.CommitAsync().ConfigureAwait(false); else t.Commit();
                log.Append("final commit ok\n");
            }
            catch (RocksDbException e)
            {
                log.Append("final commit ").Append(Outcome(e)).Append('\n');
            }
            t.Dispose();
        }
        log.Append(Dump(db.Database));
        return log.ToString();
    }

    /// <summary>
    /// Pins: the same seeded transaction program (Put / Delete / Merge / GetForUpdate / Get / save points / Prepare /
    /// Commit / Rollback across three interleaved transactions, plus direct TransactionDatabase writes) gives identical
    /// per-op results (values, error codes and subcodes) and final DB contents through the sync and the async API, for
    /// every write policy.
    /// </summary>
    [TestMethod]
    [DataRow(TransactionWritePolicy.WriteCommitted)]
    [DataRow(TransactionWritePolicy.WritePrepared)]
    [DataRow(TransactionWritePolicy.WriteUnprepared)]
    public async Task Differential_PessimisticProgram_SyncEqualsAsync(TransactionWritePolicy policy)
    {
        for (int seed = 1; seed <= 6; seed++)
        {
            Step[] p = Program(seed * 7919 + (int)policy, 250);
            string sync = await RunPessimistic(p, policy, useAsync: false).ConfigureAwait(false);
            string async = await RunPessimistic(p, policy, useAsync: true).ConfigureAwait(false);
            Assert.AreEqual(sync, async, "seed " + seed + " policy " + policy);
            StringAssert.Contains(sync, "E:TimedOut", "the program must exercise lock conflicts (seed " + seed + ")");
        }
    }

    private static async Task<string> RunOptimistic(Step[] program, OccValidationPolicy validate, bool useAsync)
    {
        MemoryFileSystem fs = NewFs();
        var log = new StringBuilder();
        using OptimisticTransactionDatabase db = OptimisticTransactionDatabase.Open(DbPath, NewOptions(fs),
            new OptimisticTransactionDbOptions { ValidatePolicy = validate, OccLockBuckets = 64 },
            [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, new ColumnFamilyOptions { MergeOperator = new StringAppendOperator() })]);
        var txns = new Transaction?[3];
        for (int i = 0; i < program.Length; i++)
        {
            Step st = program[i];
            Transaction? t = txns[st.Slot];
            byte[] key = B("k" + st.Key);
            byte[] val = B("v" + i);
            string r = "ok";
            try
            {
                if (t is null && st.Op < 12)
                {
                    txns[st.Slot] = db.BeginTransaction(s_wo, new OptimisticTransactionOptions { SetSnapshot = st.Snapshot });
                    r = "begin";
                }
                else
                {
                    switch (st.Op)
                    {
                        case 0: if (useAsync) await t!.PutAsync(key, val).ConfigureAwait(false); else t!.Put(key, val); break;
                        case 1: if (useAsync) await t!.DeleteAsync(key).ConfigureAwait(false); else t!.Delete(key); break;
                        case 2: if (useAsync) await t!.MergeAsync(key, val).ConfigureAwait(false); else t!.Merge(key, val); break;
                        case 3: r = "gfu=" + S(useAsync ? await t!.GetForUpdateAsync(key).ConfigureAwait(false) : t!.GetForUpdate(key)); break;
                        case 4: r = "get=" + S(useAsync ? await t!.GetAsync(key).ConfigureAwait(false) : t!.Get(key)); break;
                        case 5: t!.SetSavePoint(); break;
                        case 6: r = "rtsp=" + t!.RollbackToSavePoint(); break;
                        case 7:
                        case 9:
                            try
                            {
                                if (useAsync) await t!.CommitAsync().ConfigureAwait(false); else t!.Commit();
                            }
                            finally
                            {
                                t!.Dispose();
                                txns[st.Slot] = null;
                            }
                            break;
                        case 8: if (useAsync) await t!.RollbackAsync().ConfigureAwait(false); else t!.Rollback(); t!.Dispose(); txns[st.Slot] = null; break;
                        case 10:
                        case 11: if (useAsync) await db.Database.PutAsync(key, val).ConfigureAwait(false); else db.Database.Put(key, val); break;
                        default: r = "dbget=" + S(useAsync ? await db.Database.GetAsync(key).ConfigureAwait(false) : db.Database.Get(key)); break;
                    }
                }
            }
            catch (Exception e) when (e is RocksDbException or InvalidOperationException or ObjectDisposedException or ArgumentException)
            {
                r = Outcome(e);
            }
            log.Append(i).Append(' ').Append(st.Op).Append(' ').Append(r).Append('\n');
        }
        foreach (Transaction? t in txns) t?.Dispose();
        log.Append(Dump(db.Database));
        return log.ToString();
    }

    /// <summary>
    /// Pins: the optimistic transaction program (both validation policies) gives identical per-op results, commit
    /// conflicts (Busy) and final contents through the sync and the async API.
    /// </summary>
    [TestMethod]
    [DataRow(OccValidationPolicy.ValidateParallel)]
    [DataRow(OccValidationPolicy.ValidateSerial)]
    public async Task Differential_OptimisticProgram_SyncEqualsAsync(OccValidationPolicy validate)
    {
        for (int seed = 1; seed <= 6; seed++)
        {
            Step[] p = Program(seed * 104729 + (int)validate, 250);
            string sync = await RunOptimistic(p, validate, useAsync: false).ConfigureAwait(false);
            string async = await RunOptimistic(p, validate, useAsync: true).ConfigureAwait(false);
            Assert.AreEqual(sync, async, "seed " + seed + " policy " + validate);
            StringAssert.Contains(sync, "E:Busy", "the program must exercise commit conflicts (seed " + seed + ")");
        }
    }

    // =====================================================================================================
    // Backup / checkpoint parity, cancellation
    // =====================================================================================================

    /// <summary>
    /// A file system over <see cref="MemoryFileSystem"/> whose async members always yield first, so the async paths
    /// really complete asynchronously (and a <c>synchronous: true</c> path that wrongly called one would trip
    /// SyncValueTask's assertion in a debug build). Sync members pass through.
    /// </summary>
    private sealed class YieldingFileSystem(MemoryFileSystem inner) : IFileSystem
    {
        public MemoryFileSystem Inner => inner;
        public int AsyncCalls;

        public IWritableFile CreateWritable(string path, bool truncate = true) => new W(this, inner.CreateWritable(path, truncate));
        public IWritableFile CreateWritable(string path, bool truncate, WritableFileOptions options) =>
            new W(this, ((IFileSystem)inner).CreateWritable(path, truncate, options));
        public IWritableFile ReopenWritable(string path, WritableFileOptions options) => new W(this, ((IFileSystem)inner).ReopenWritable(path, options));
        public IRandomAccessFile OpenRandomAccess(string path) => new R(this, inner.OpenRandomAccess(path));
        public IRandomAccessFile OpenRandomAccess(string path, RandomAccessFileOptions options) => new R(this, ((IFileSystem)inner).OpenRandomAccess(path, options));
        public ISequentialFile OpenSequential(string path) => new Q(this, inner.OpenSequential(path));
        public ISequentialFile OpenSequential(string path, SequentialFileOptions options) => new Q(this, ((IFileSystem)inner).OpenSequential(path, options));
        public bool FileExists(string path) => inner.FileExists(path);
        public long GetFileSize(string path) => inner.GetFileSize(path);
        public void DeleteFile(string path) => inner.DeleteFile(path);
        public void RenameFile(string source, string target) => inner.RenameFile(source, target);
        public void CreateDirectoryIfMissing(string path) => inner.CreateDirectoryIfMissing(path);
        public IEnumerable<string> GetChildren(string directory) => inner.GetChildren(directory);
        public IDisposable LockFile(string path) => inner.LockFile(path);
        public void SyncDirectory(string path) => inner.SyncDirectory(path);
        public bool DirectoryExists(string path) => ((IFileSystem)inner).DirectoryExists(path);
        public void DeleteDirectory(string path) => ((IFileSystem)inner).DeleteDirectory(path);
        public void LinkFile(string source, string target) => ((IFileSystem)inner).LinkFile(source, target);
        public DateTimeOffset GetFileModificationTime(string path) => ((IFileSystem)inner).GetFileModificationTime(path);

        private async ValueTask Yield()
        {
            Interlocked.Increment(ref AsyncCalls);
            await Task.Yield();
        }

        private sealed class W(YieldingFileSystem fs, IWritableFile f) : IWritableFile
        {
            public long Size => f.Size;
            public void Append(ReadOnlySpan<byte> data) => f.Append(data);
            public async ValueTask AppendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
            {
                await fs.Yield().ConfigureAwait(false);
                await f.AppendAsync(data, cancellationToken).ConfigureAwait(false);
            }
            public async ValueTask FlushAsync(CancellationToken cancellationToken = default)
            {
                await fs.Yield().ConfigureAwait(false);
                await f.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
            public async ValueTask SyncAsync(CancellationToken cancellationToken = default)
            {
                await fs.Yield().ConfigureAwait(false);
                await f.SyncAsync(cancellationToken).ConfigureAwait(false);
            }
            public void Flush() => f.Flush();
            public void Sync() => f.Sync();
            public void Dispose() => f.Dispose();
            public async ValueTask DisposeAsync()
            {
                await fs.Yield().ConfigureAwait(false);
                await f.DisposeAsync().ConfigureAwait(false);
            }
        }

        private sealed class R(YieldingFileSystem fs, IRandomAccessFile f) : IRandomAccessFile
        {
            public long Length => f.Length;
            public int Read(long offset, Span<byte> destination) => f.Read(offset, destination);
            public async ValueTask<int> ReadAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken = default)
            {
                await fs.Yield().ConfigureAwait(false);
                return await f.ReadAsync(offset, destination, cancellationToken).ConfigureAwait(false);
            }
            public void Dispose() => f.Dispose();
        }

        private sealed class Q(YieldingFileSystem fs, ISequentialFile f) : ISequentialFile
        {
            public int Read(Span<byte> destination) => f.Read(destination);
            public async ValueTask<int> ReadAsync(Memory<byte> destination, CancellationToken cancellationToken = default)
            {
                await fs.Yield().ConfigureAwait(false);
                return await f.ReadAsync(destination, cancellationToken).ConfigureAwait(false);
            }
            public void Skip(long bytes) => f.Skip(bytes);
            public void Dispose() => f.Dispose();
        }
    }

    private static void Fill(RocksDatabase db, int from, int to, int round)
    {
        for (int i = from; i < to; i++) db.Put(Key(i), Value(i, round));
    }

    /// <summary>File name -> contents under <paramref name="dir"/> (recursively), meta files without the timestamp line.</summary>
    private static SortedDictionary<string, string> Tree(IFileSystem fs, string dir)
    {
        var result = new SortedDictionary<string, string>(StringComparer.Ordinal);
        void Walk(string d, string rel)
        {
            foreach (string c in fs.GetChildren(d))
            {
                string p = d + "/" + c;
                if (fs.DirectoryExists(p))
                {
                    Walk(p, rel + c + "/");
                    continue;
                }
                var data = new byte[fs.GetFileSize(p)];
                using (ISequentialFile f = fs.OpenSequential(p))
                {
                    int read = 0;
                    while (read < data.Length) read += f.Read(data.AsSpan(read));
                }
                string text = Convert.ToHexString(data);
                if (rel.StartsWith("meta/", StringComparison.Ordinal))
                {
                    string s = Encoding.UTF8.GetString(data);
                    text = s[(s.IndexOf('\n') + 1)..]; // drop the timestamp
                }
                result[rel + c] = text;
            }
        }
        if (fs.DirectoryExists(dir)) Walk(dir, "");
        return result;
    }

    private static void AssertSameTree(SortedDictionary<string, string> expected, SortedDictionary<string, string> actual, string what)
    {
        CollectionAssert.AreEqual(expected.Keys.ToList(), actual.Keys.ToList(), what + ": file names");
        foreach (KeyValuePair<string, string> kv in expected) Assert.AreEqual(kv.Value, actual[kv.Key], what + ": " + kv.Key);
    }

    /// <summary>
    /// Pins: CreateNewBackup / CreateNewBackupAsync, VerifyBackup / VerifyBackupAsync and RestoreDbFromBackup /
    /// RestoreDbFromBackupAsync produce identical backup directories (meta files minus the timestamp, shared and private
    /// files), identical restored DB directories and identical errors, with several copy workers and a file system whose
    /// async I/O always completes asynchronously. Running the sync calls over the same file system checks that their
    /// synchronous: true paths never use an async member.
    /// </summary>
    [TestMethod]
    [DataRow(1)]
    [DataRow(3)]
    public async Task Backup_SyncAndAsync_ProduceIdenticalDirectoriesAndErrors(int workers)
    {
        var fs = new YieldingFileSystem(NewFs());
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        Fill(db, 0, 300, 1);
        db.Flush();
        Fill(db, 200, 400, 2);
        BackupEngineOptions Opts(string dir) => new(dir) { MaxBackgroundOperations = workers, CallbackTriggerIntervalSize = 1024 };
        int syncProgress = 0, asyncProgress = 0;
        using BackupEngine syncEngine = BackupEngine.Open(Opts("/bsync"), fs);
        using BackupEngine asyncEngine = BackupEngine.Open(Opts("/basync"), fs);
        uint s1 = syncEngine.CreateNewBackup(db, new CreateBackupOptions { FlushBeforeBackup = true, ProgressCallback = () => syncProgress++ });
        int before = fs.AsyncCalls;
        uint a1 = await asyncEngine.CreateNewBackupAsync(db, new CreateBackupOptions { FlushBeforeBackup = true, ProgressCallback = () => Interlocked.Increment(ref asyncProgress) })
            .ConfigureAwait(false);
        Assert.IsTrue(fs.AsyncCalls > before, "the async backup must use async I/O");
        Assert.AreEqual(s1, a1);
        Assert.AreEqual(syncProgress, asyncProgress, "progress callbacks");
        AssertSameTree(Tree(fs, "/bsync"), Tree(fs, "/basync"), "backup dirs");

        syncEngine.VerifyBackup(s1, verifyWithChecksum: true);
        await asyncEngine.VerifyBackupAsync(a1, verifyWithChecksum: true).ConfigureAwait(false);

        syncEngine.RestoreDbFromBackup(s1, "/rsync", "/rsync", new RestoreOptions());
        await asyncEngine.RestoreDbFromBackupAsync(a1, "/rasync", "/rasync", new RestoreOptions()).ConfigureAwait(false);
        AssertSameTree(Tree(fs, "/rsync"), Tree(fs, "/rasync"), "restored dirs");

        // Same errors: corrupt one shared file of each backup the same way.
        foreach (string dir in new[] { "/bsync", "/basync" })
        {
            string shared = dir + "/shared_checksum";
            string victim = fs.GetChildren(shared).OrderBy(x => x, StringComparer.Ordinal).First();
            fs.Inner.WriteAt(shared + "/" + victim, 10, "XXXX"u8);
        }
        var es = Assert.ThrowsExactly<RocksDbException>(() => syncEngine.VerifyBackup(s1, verifyWithChecksum: true));
        var ea = await Assert.ThrowsAsync<RocksDbException>(async () => await asyncEngine.VerifyBackupAsync(a1, verifyWithChecksum: true).ConfigureAwait(false))
            .ConfigureAwait(false);
        Assert.AreEqual(es.Code, ea.Code);
        Assert.AreEqual(es.Message.Replace("/bsync", "/b", StringComparison.Ordinal), ea.Message.Replace("/basync", "/b", StringComparison.Ordinal));
        var rs = Assert.ThrowsExactly<RocksDbException>(() => syncEngine.RestoreDbFromBackup(s1, "/rsync2", "/rsync2", new RestoreOptions()));
        var ra = await Assert.ThrowsAsync<RocksDbException>(async () =>
            await asyncEngine.RestoreDbFromBackupAsync(a1, "/rasync2", "/rasync2", new RestoreOptions()).ConfigureAwait(false)).ConfigureAwait(false);
        Assert.AreEqual(rs.Code, ra.Code);
    }

    /// <summary>
    /// Pins: cancelling CreateNewBackupAsync in the middle of the copies throws OperationCanceledException, records no
    /// backup, leaves nothing behind that a later open does not garbage-collect (no files outside the surviving backup's
    /// set), keeps the engine usable and the DB's file deletions re-enabled. Cancelling RestoreDbFromBackupAsync throws
    /// OperationCanceledException and a later restore into the same directory succeeds.
    /// </summary>
    [TestMethod]
    public async Task Backup_CancelledMidCopy_IsCleanedUpAndRecoverable()
    {
        var fs = new YieldingFileSystem(NewFs());
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        Fill(db, 0, 300, 1);
        db.Flush();
        var opts = new BackupEngineOptions("/backup") { CallbackTriggerIntervalSize = 512 };
        uint first;
        using (BackupEngine be = BackupEngine.Open(opts, fs)) first = be.CreateNewBackup(db, flushBeforeBackup: true);
        SortedDictionary<string, string> afterFirst = Tree(fs, "/backup");
        Fill(db, 300, 600, 2);

        using (BackupEngine be = BackupEngine.Open(opts, fs))
        {
            using var cts = new CancellationTokenSource();
            int calls = 0;
            var co = new CreateBackupOptions { FlushBeforeBackup = true, ProgressCallback = () => { if (++calls == 2) cts.Cancel(); } };
            await Assert.ThrowsAsync<OperationCanceledException>(async () => await be.CreateNewBackupAsync(db, co, cts.Token).ConfigureAwait(false))
                .ConfigureAwait(false);
            Assert.AreEqual(1, be.GetBackupInfo().Count);
        }
        // File deletions were re-enabled: a compaction can delete obsolete files.
        db.CompactRange(null, null);
        using (BackupEngine be = BackupEngine.Open(opts, fs))
        {
            be.GarbageCollect();
            AssertSameTree(afterFirst, Tree(fs, "/backup"), "backup dir after the cancelled backup");
            uint second = await be.CreateNewBackupAsync(db, flushBeforeBackup: true).ConfigureAwait(false);
            Assert.AreEqual(first + 1, second);

            using var cts = new CancellationTokenSource();
            cts.Cancel();
            await Assert.ThrowsAsync<OperationCanceledException>(async () =>
                await be.RestoreDbFromBackupAsync(second, "/restore", "/restore", new RestoreOptions(), cts.Token).ConfigureAwait(false)).ConfigureAwait(false);
            await be.RestoreDbFromBackupAsync(second, "/restore", "/restore", new RestoreOptions()).ConfigureAwait(false);
        }
        using RocksDatabase restored = RocksDatabase.Open("/restore", new DbOptions { FileSystem = fs });
        for (int i = 0; i < 600; i++) CollectionAssert.AreEqual(Value(i, i < 300 ? 1 : 2), restored.Get(Key(i)), "key " + i);
    }

    /// <summary>
    /// Pins: CreateCheckpoint and CreateCheckpointAsync produce the same files with the same contents; a cancelled
    /// CreateCheckpointAsync throws OperationCanceledException and leaves neither the checkpoint nor its staging
    /// directory, so the same directory can be used again.
    /// </summary>
    [TestMethod]
    public async Task Checkpoint_SyncAndAsyncIdentical_CancelledLeavesNothing()
    {
        var fs = new YieldingFileSystem(NewFs());
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        Fill(db, 0, 300, 1);
        db.Flush();
        Fill(db, 300, 350, 1);
        db.Flush();
        db.CreateCheckpoint("/cp_sync");
        await db.CreateCheckpointAsync("/cp_async").ConfigureAwait(false);
        AssertSameTree(Tree(fs, "/cp_sync"), Tree(fs, "/cp_async"), "checkpoints");

        Fill(db, 350, 400, 1);
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        try
        {
            await db.CreateCheckpointAsync("/cp_cancel", cts.Token).ConfigureAwait(false);
            Assert.IsTrue(fs.DirectoryExists("/cp_cancel")); // completed before observing the token
        }
        catch (OperationCanceledException)
        {
            Assert.IsFalse(fs.DirectoryExists("/cp_cancel"));
            Assert.IsFalse(fs.DirectoryExists("/cp_cancel.tmp"));
            await db.CreateCheckpointAsync("/cp_cancel").ConfigureAwait(false);
        }
        using RocksDatabase cp = RocksDatabase.Open("/cp_cancel", new DbOptions { FileSystem = fs });
        for (int i = 0; i < 400; i++) CollectionAssert.AreEqual(Value(i, 1), cp.Get(Key(i)), "key " + i);
    }

    // =====================================================================================================
    // WAL tailing
    // =====================================================================================================

    /// <summary>
    /// Pins: async enumeration of GetUpdatesSince returns the same batches as the sync Next loop; breaking out of
    /// <c>await foreach</c> leaves the iterator on the last yielded batch (a new enumeration starts there, documented
    /// "starting with the current one"); a cancelled enumeration throws OperationCanceledException at the next move and
    /// leaves the iterator usable; disposing the iterator ends a later enumeration with ObjectDisposedException rather
    /// than reading freed state.
    /// </summary>
    [TestMethod]
    public async Task GetUpdatesSince_AsyncEnumeration_MatchesSyncAndHonoursBreakCancelDispose()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        for (int i = 0; i < 20; i++) db.Put(Key(i), Value(i, 1));
        var expected = new List<ulong>();
        using (TransactionLogIterator it = db.GetUpdatesSince(1))
        {
            for (; it.Valid; it.Next()) expected.Add(it.Current.Sequence);
            it.ThrowIfError();
        }
        Assert.AreEqual(20, expected.Count);

        var actual = new List<ulong>();
        using (TransactionLogIterator it = db.GetUpdatesSince(1))
        {
            await foreach (WalBatch b in it.ConfigureAwait(false))
            {
                actual.Add(b.Sequence);
                if (actual.Count == 5) break;
            }
            Assert.AreEqual(expected[4], it.Current.Sequence, "break leaves the iterator on the last yielded batch");
            using var cts = new CancellationTokenSource();
            int n = 0;
            await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            {
                await foreach (WalBatch b in it.WithCancellation(cts.Token).ConfigureAwait(false))
                {
                    if (n++ == 0) continue; // the batch yielded before the break, again
                    actual.Add(b.Sequence);
                    if (actual.Count == 10) cts.Cancel();
                }
            }).ConfigureAwait(false);
            await foreach (WalBatch b in it.ConfigureAwait(false))
            {
                if (b.Sequence == actual[^1]) continue;
                actual.Add(b.Sequence);
            }
        }
        CollectionAssert.AreEqual(expected, actual);

        TransactionLogIterator disposed = db.GetUpdatesSince(1);
        disposed.Dispose();
        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
        {
            await foreach (WalBatch b in disposed.ConfigureAwait(false)) _ = b.Sequence;
        }).ConfigureAwait(false);
    }

    // =====================================================================================================
    // SynchronizationContext: no capture in the library
    // =====================================================================================================

    /// <summary>A SynchronizationContext that runs every posted callback on one dedicated thread.</summary>
    private sealed class SingleThreadContext : SynchronizationContext, IDisposable
    {
        private readonly System.Collections.Concurrent.BlockingCollection<(SendOrPostCallback, object?)> _queue = new();
        private readonly Thread _thread;
        public int Posts;

        public SingleThreadContext()
        {
            _thread = new Thread(() =>
            {
                SetSynchronizationContext(this);
                foreach ((SendOrPostCallback cb, object? state) in _queue.GetConsumingEnumerable()) cb(state);
            }) { IsBackground = true, Name = "audit8-single-thread-context" };
            _thread.Start();
        }

        public override void Post(SendOrPostCallback d, object? state)
        {
            Interlocked.Increment(ref Posts);
            _queue.Add((d, state));
        }

        public override void Send(SendOrPostCallback d, object? state) => throw new NotSupportedException();

        public Task Run(Func<Task> body)
        {
            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            Post(async _ =>
            {
                try
                {
                    await body();
                    tcs.SetResult();
                }
                catch (Exception e)
                {
                    tcs.SetException(e);
                }
            }, null);
            return tcs.Task;
        }

        public void Dispose() => _queue.CompleteAdding();
    }

    /// <summary>
    /// Pins: the library does not capture the caller's SynchronizationContext (every await in src/RocksDb.Managed uses
    /// ConfigureAwait, see <see cref="Sources_EveryAwaitConfiguresTheContext"/>), so on a single-threaded context sync
    /// APIs called from the context thread (blocking it) never wait for a continuation queued to that context, and async
    /// APIs awaited on it complete. Covers the DB, transactions (incl. a contended lock wait released by a sync call on
    /// the context thread), backup with copy workers, checkpoint and restore.
    /// </summary>
    [TestMethod]
    public async Task SingleThreadedSynchronizationContext_NoDeadlock()
    {
        var fs = new YieldingFileSystem(NewFs());
        using var ctx = new SingleThreadContext();
        Task run = ctx.Run(async () =>
        {
            using TransactionDatabase tdb = TransactionDatabase.Open(DbPath, NewOptions(fs), new TransactionDbOptions());
            RocksDatabase db = tdb.Database;
            for (int i = 0; i < 50; i++) await db.PutAsync(Key(i), Value(i, 1));
            db.Put(Key(50), Value(50, 1));
            Assert.IsNotNull(await db.GetAsync(Key(3)));
            await db.FlushAsync();
            db.Flush();

            using Transaction holder = tdb.BeginTransaction(s_wo);
            holder.Put(B("hot"), B("1"));
            using Transaction waiter = tdb.BeginTransaction(s_wo, new TransactionOptions { LockTimeout = TimeSpan.FromSeconds(10) });
            Task wait = waiter.PutAsync(B("hot"), B("2")).AsTask();
            Assert.IsFalse(wait.IsCompleted);
            holder.Commit(); // sync, on the context thread, wakes the async waiter
            await wait;
            await waiter.CommitAsync();
            Assert.AreEqual("2", S(await db.GetAsync(B("hot"))));

            using Transaction t = tdb.BeginTransaction(s_wo);
            t.Put(B("x"), B("1"));
            t.Commit();

            using BackupEngine be = BackupEngine.Open(new BackupEngineOptions("/backup") { MaxBackgroundOperations = 2 }, fs);
            be.CreateNewBackup(db, flushBeforeBackup: true);
            await be.CreateNewBackupAsync(db, flushBeforeBackup: true);
            be.VerifyBackup(1, verifyWithChecksum: true);
            await be.VerifyBackupAsync(2, verifyWithChecksum: true);
            await be.RestoreDbFromLatestBackupAsync("/restored", "/restored", new RestoreOptions());
            db.CreateCheckpoint("/cp1");
            await db.CreateCheckpointAsync("/cp2");
            using TransactionLogIterator it = db.GetUpdatesSince(1);
            await foreach (WalBatch b in it) _ = b.Count;
        });
        await Guard(run, "work on a single-threaded SynchronizationContext").ConfigureAwait(false);
    }

    /// <summary>
    /// Pins (source inventory): every <c>await</c> in src/RocksDb.Managed configures the context (ConfigureAwait), except
    /// the BackgroundThreadPool.SwitchTo awaitable, which moves to a pool thread by design. A missing one would make a
    /// continuation post to the caller's SynchronizationContext.
    /// </summary>
    [TestMethod]
    public void Sources_EveryAwaitConfiguresTheContext()
    {
        string src = FindSourceDir();
        var missing = new List<string>();
        foreach (string file in Directory.EnumerateFiles(src, "*.cs", SearchOption.AllDirectories))
        {
            if (file.Contains("/obj/", StringComparison.Ordinal) || file.Contains("/bin/", StringComparison.Ordinal)) continue;
            string text = File.ReadAllText(file);
            string code = System.Text.RegularExpressions.Regex.Replace(text, "//[^\n]*", m => new string(' ', m.Length));
            foreach (System.Text.RegularExpressions.Match m in System.Text.RegularExpressions.Regex.Matches(code, @"\bawait\b"))
            {
                int depth = 0, j = m.Index + m.Length;
                for (; j < code.Length; j++)
                {
                    char c = code[j];
                    if (c is '(' or '[') depth++;
                    else if (c is ')' or ']') { if (--depth < 0) break; }
                    else if (c is ';' or '{' or ',' && depth == 0) break;
                }
                string expr = code[(m.Index + m.Length)..j];
                if (expr.Contains("ConfigureAwait", StringComparison.Ordinal) || expr.Trim() == "pool.SwitchTo()") continue;
                int line = 1 + text.AsSpan(0, m.Index).Count('\n');
                missing.Add(Path.GetRelativePath(src, file) + ":" + line + ": await" + expr.Trim());
            }
        }
        Assert.AreEqual(0, missing.Count, string.Join("\n", missing));
    }

    private static string FindSourceDir()
    {
        for (string? d = AppContext.BaseDirectory; d is not null; d = Path.GetDirectoryName(d))
        {
            string candidate = Path.Combine(d, "src", "RocksDb.Managed");
            if (Directory.Exists(candidate)) return candidate;
        }
        Assert.Inconclusive("source directory not found");
        return "";
    }

    // =====================================================================================================
    // Public async surface inventory
    // =====================================================================================================

    private static bool IsAsyncReturn(Type t)
    {
        if (t == typeof(Task) || t == typeof(ValueTask)) return true;
        if (!t.IsGenericType) return false;
        Type g = t.GetGenericTypeDefinition();
        return g == typeof(Task<>) || g == typeof(ValueTask<>) || g == typeof(IAsyncEnumerable<>) || g == typeof(IAsyncEnumerator<>);
    }

    private static string Describe(MethodInfo m)
    {
        var sb = new StringBuilder(m.DeclaringType!.Name).Append('.').Append(m.Name).Append('(');
        ParameterInfo[] ps = m.GetParameters();
        for (int i = 0; i < ps.Length; i++)
        {
            if (i > 0) sb.Append(", ");
            Type pt = ps[i].ParameterType;
            sb.Append(pt.IsGenericType ? pt.Name[..pt.Name.IndexOf('`')] + "<" + string.Join(",", pt.GetGenericArguments().Select(a => a.Name)) + ">" : pt.Name);
        }
        return sb.Append(')').ToString();
    }

    private static List<MethodInfo> PublicAsyncMethods()
    {
        var result = new List<MethodInfo>();
        foreach (Type type in typeof(RocksDatabase).Assembly.GetExportedTypes())
        {
            foreach (MethodInfo m in type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly))
            {
                if (m.IsSpecialName || !IsAsyncReturn(m.ReturnType)) continue;
                // Interface contracts implemented by the type: DisposeAsync (IAsyncDisposable), MoveNextAsync / GetAsyncEnumerator.
                if (m.Name is "DisposeAsync" or "MoveNextAsync" or "GetAsyncEnumerator") continue;
                result.Add(m);
            }
        }
        return result;
    }

    /// <summary>
    /// Public async methods without a CancellationToken parameter, each with the reason (audit #8 inventory). A new
    /// public async method must take a token or be added here with a reason.
    /// </summary>
    private static readonly Dictionary<string, string> s_noTokenAllowed = new(StringComparer.Ordinal)
    {
        ["Iterator.ScanAsync()"] = "IAsyncEnumerable: the token is passed to GetAsyncEnumerator (WithCancellation)",
        ["Iterator.ScanAsync(Nullable<ReadOnlyMemory`1>, Nullable<ReadOnlyMemory`1>)"] = "IAsyncEnumerable: the token is passed to GetAsyncEnumerator",
        ["Iterator.ScanAsyncAndDispose(Nullable<ReadOnlyMemory`1>, Nullable<ReadOnlyMemory`1>)"] = "IAsyncEnumerable: the token is passed to GetAsyncEnumerator",
        ["RocksDatabase.CancelAllBackgroundWorkAsync(Boolean)"] = "gap (audit #8): waits for running jobs; upstream CancelAllBackgroundWork has no cancellation either",
        ["RocksDatabase.ResumeAsync()"] = "gap (audit #8): error recovery runs to completion; upstream Resume has no cancellation either",
    };

    /// <summary>
    /// Public async methods without a synchronous counterpart (same name without "Async" on the declaring type or a base
    /// type), each with the reason. A new one must get a sync counterpart or be added here with a reason.
    /// </summary>
    private static readonly Dictionary<string, string> s_noSyncAllowed = new(StringComparer.Ordinal)
    {
        ["ColumnFamily.CompactFilesWithJobInfoAsync(CompactFilesOptions, IEnumerable<UInt64>, Int32, CancellationToken)"] =
            "sync counterpart is CompactFiles(..., out CompactionJobInfo?) (out parameters cannot be async)",
        ["CompactionService.ScheduleAsync(CompactionServiceJobInfo, ReadOnlyMemory<Byte>, CancellationToken)"] = "user-implemented service contract (remote call)",
        ["CompactionService.WaitAsync(String, CancellationToken)"] = "user-implemented service contract (remote call)",
        ["DbIter.PrepareValueAsync(CancellationToken)"] = "the sync path prepares the value lazily on the first Value access",
        ["Iterator.PrepareValueAsync(CancellationToken)"] = "the sync path prepares the value lazily on the first Value access",
        ["Iterator.ScanAsync()"] = "sync scans use the ref struct enumerator / Seek-Next loop",
        ["Iterator.ScanAsync(Nullable<ReadOnlyMemory`1>, Nullable<ReadOnlyMemory`1>)"] = "sync scans use the ref struct enumerator / Seek-Next loop",
        ["Iterator.ScanAsyncAndDispose(Nullable<ReadOnlyMemory`1>, Nullable<ReadOnlyMemory`1>)"] = "sync scans use the ref struct enumerator / Seek-Next loop",
        ["RocksDatabase.TailUpdatesAsync(UInt64, CancellationToken)"] = "waits for future writes: a sync form would block a thread indefinitely",
        ["RocksDatabase.WaitForSequenceAsync(UInt64, CancellationToken)"] = "waits for future writes: a sync form would block a thread indefinitely",
        ["TransactionLogIterator.MoveNextTailingAsync(CancellationToken)"] = "waits for future writes; the sync form is Next() without waiting",
        ["TransactionLogIterator.WaitForUpdatesAsync(CancellationToken)"] = "waits for future writes: a sync form would block a thread indefinitely",
    };

    /// <summary>
    /// Pins the public async surface (reflection over the exported types of RocksDb.Managed): every public method
    /// returning Task / ValueTask / IAsyncEnumerable either takes a CancellationToken and has a synchronous counterpart,
    /// or is listed with the reason it does not. Reports the whole difference on failure.
    /// </summary>
    [TestMethod]
    public void PublicAsyncSurface_Inventory()
    {
        var noToken = new SortedSet<string>(StringComparer.Ordinal);
        var noSync = new SortedSet<string>(StringComparer.Ordinal);
        int count = 0;
        foreach (MethodInfo m in PublicAsyncMethods())
        {
            count++;
            string d = Describe(m);
            if (!m.GetParameters().Any(p => p.ParameterType == typeof(CancellationToken))) noToken.Add(d);
            string baseName = m.Name.EndsWith("Async", StringComparison.Ordinal) ? m.Name[..^5] : m.Name;
            bool hasSync = false;
            for (Type? t = m.DeclaringType; t is not null && !hasSync; t = t.BaseType)
            {
                foreach (MethodInfo s in t.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static))
                {
                    if (IsAsyncReturn(s.ReturnType)) continue;
                    if (s.Name == baseName || s.Name == "Try" + baseName || (baseName.StartsWith("Try", StringComparison.Ordinal) && s.Name == baseName[3..]))
                    {
                        hasSync = true;
                        break;
                    }
                }
            }
            if (!hasSync) noSync.Add(d);
        }
        var report = new StringBuilder();
        foreach (string d in noToken) if (!s_noTokenAllowed.ContainsKey(d)) report.Append("no CancellationToken: ").Append(d).Append('\n');
        foreach (string d in s_noTokenAllowed.Keys) if (!noToken.Contains(d)) report.Append("stale no-token entry: ").Append(d).Append('\n');
        foreach (string d in noSync) if (!s_noSyncAllowed.ContainsKey(d)) report.Append("no sync counterpart: ").Append(d).Append('\n');
        foreach (string d in s_noSyncAllowed.Keys) if (!noSync.Contains(d)) report.Append("stale no-sync entry: ").Append(d).Append('\n');
        Assert.IsTrue(count > 100, "public async methods found: " + count);
        Assert.AreEqual(0, report.Length, "public async methods: " + count + "\n" + report);
    }
}
