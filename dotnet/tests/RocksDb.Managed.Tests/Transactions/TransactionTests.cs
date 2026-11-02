using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Tests.Db;
using RocksDb.Managed.Utilities.Transactions;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Transactions;

/// <summary>Ports of utilities/transactions/transaction_test.cc (WriteCommitted policy) and 2PC recovery cases.</summary>
[TestClass]
public class TransactionTests
{
    /// <summary>The write policy the suite runs with (transaction_test.cc is parameterized over the write policies).</summary>
    protected virtual TransactionWritePolicy Policy => TransactionWritePolicy.WriteCommitted;

    private static readonly WriteOptions s_wo = new();
    private static readonly WriteOptions s_sync = new() { Sync = true };
    private static readonly TimeSpan s_short = TimeSpan.FromMilliseconds(50);

    private TransactionDbOptions TxnDbOptions() => WithPolicy(new() { TransactionLockTimeout = s_short, DefaultLockTimeout = s_short });

    private TransactionDbOptions WithPolicy(TransactionDbOptions o)
    {
        o.WritePolicy = Policy;
        o.WpCommitCacheBits = 10; // small commit cache: exercises eviction and keeps the per-DB memory low
        return o;
    }

    private TransactionDatabase Open(MemoryFileSystem fs, TransactionDbOptions? options = null, ColumnFamilyOptions? cfo = null) =>
        TransactionDatabase.Open(DbPath, NewOptions(fs), options is null ? TxnDbOptions() : WithPolicy(options),
            [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, cfo ?? new ColumnFamilyOptions())]);

    /// <summary>
    /// Power loss: unsynced data is dropped. The crashed instance is abandoned, so first let its background compactions
    /// finish (otherwise they would keep running against the file system after the "crash").
    /// </summary>
    private static void Crash(MemoryFileSystem fs, TransactionDatabase db)
    {
        db.Database.WaitForCompact();
        fs.SimulateCrash();
    }

    private static void AssertFails(StatusCode code, Action a, StatusSubCode sub = StatusSubCode.None)
    {
        var e = Assert.ThrowsExactly<RocksDbException>(a);
        Assert.AreEqual(code, e.Code, e.Message);
        if (sub != StatusSubCode.None) Assert.AreEqual(sub, e.SubCode, e.Message);
    }

    private static async Task AssertFailsAsync(StatusCode code, Func<Task> a, StatusSubCode sub = StatusSubCode.None)
    {
        var e = await Assert.ThrowsExactlyAsync<RocksDbException>(a);
        Assert.AreEqual(code, e.Code, e.Message);
        if (sub != StatusSubCode.None) Assert.AreEqual(sub, e.SubCode, e.Message);
    }

    // ---- basic ---------------------------------------------------------------------------------------------

    [TestMethod]
    public void SuccessTest()
    {
        using TransactionDatabase db = Open(NewFs());
        db.Put(B("foo"), B("bar"));
        db.Put(B("foo2"), B("bar"));
        using Transaction txn = db.BeginTransaction(s_wo);
        Assert.AreEqual("bar", S(txn.GetForUpdate(B("foo"))));
        txn.Put(B("foo"), B("bar2"));
        Assert.AreEqual("bar2", S(txn.GetForUpdate(B("foo"))));
        txn.Commit();
        Assert.AreEqual("bar2", GetString(db.Database, "foo"));
        Assert.AreEqual(0, db.GetLockStatusData().Count); // locks released at commit
    }

    [TestMethod]
    public void WriteConflictTest()
    {
        using TransactionDatabase db = Open(NewFs());
        db.Put(B("foo"), B("A"));
        db.Put(B("foo2"), B("B"));
        using Transaction txn = db.BeginTransaction(s_wo);
        txn.Put(B("foo"), B("A2"));
        txn.Put(B("foo2"), B("B2"));
        // This Put outside of a transaction will conflict with the previous write
        AssertFails(StatusCode.TimedOut, () => db.Put(B("foo"), B("xxx")), StatusSubCode.LockTimeout);
        Assert.AreEqual("A", GetString(db.Database, "foo"));
        txn.Commit();
        Assert.AreEqual("A2", GetString(db.Database, "foo"));
        Assert.AreEqual("B2", GetString(db.Database, "foo2"));
    }

    [TestMethod]
    public void WriteConflictTest2()
    {
        using TransactionDatabase db = Open(NewFs());
        db.Put(B("foo"), B("bar"));
        using Transaction txn = db.BeginTransaction(s_wo, new TransactionOptions { SetSnapshot = true });
        // This Put outside of a transaction will conflict with a later write
        db.Put(B("foo"), B("barz"));
        // Conflicts with write done after snapshot taken
        AssertFails(StatusCode.Busy, () => txn.Put(B("foo"), B("bar2")));
        txn.Put(B("foo2"), B("X"));
        txn.Commit();
        Assert.AreEqual("barz", GetString(db.Database, "foo"));
        Assert.AreEqual("X", GetString(db.Database, "foo2"));
    }

    [TestMethod]
    public void ReadConflictTest()
    {
        using TransactionDatabase db = Open(NewFs());
        db.Put(B("foo"), B("bar"));
        using Transaction txn = db.BeginTransaction(s_wo, new TransactionOptions { SetSnapshot = true });
        var ro = new ReadOptions { Snapshot = txn.Snapshot };
        Assert.AreEqual("bar", S(txn.GetForUpdate(ro, null!, B("foo"), exclusive: true)));
        // This Put outside of a transaction will conflict with the previous read
        AssertFails(StatusCode.TimedOut, () => db.Put(B("foo"), B("barz")));
        txn.Commit();
        db.Put(B("foo"), B("barz"));
        Assert.AreEqual("barz", GetString(db.Database, "foo"));
    }

    [TestMethod]
    public void ValidateSnapshotAgainstTableFiles()
    {
        // The snapshot check (cache_only = false) also finds writes that were flushed and compacted away from memory.
        using TransactionDatabase db = Open(NewFs(), cfo: new ColumnFamilyOptions { MaxWriteBufferSizeToMaintain = 1 });
        db.Put(B("foo"), B("v0"));
        using Transaction txn = db.BeginTransaction(s_wo, new TransactionOptions { SetSnapshot = true });
        db.Put(B("foo"), B("v1"));
        db.Database.Flush();
        db.Database.CompactRange();
        AssertFails(StatusCode.Busy, () => txn.GetForUpdate(B("foo")));
        // A key not written since the snapshot validates fine even though it only lives in table files
        db.Put(B("bar"), B("b"));
        using Transaction txn2 = db.BeginTransaction(s_wo, new TransactionOptions { SetSnapshot = true });
        db.Database.Flush();
        Assert.AreEqual("b", S(txn2.GetForUpdate(B("bar"))));
    }

    [TestMethod]
    public void SharedLocks()
    {
        using TransactionDatabase db = Open(NewFs());
        using Transaction t1 = db.BeginTransaction(s_wo);
        using Transaction t2 = db.BeginTransaction(s_wo);
        using Transaction t3 = db.BeginTransaction(s_wo);
        var sink = new System.Buffers.ArrayBufferWriter<byte>();
        t1.TryGetForUpdate(null, null!, B("foo"), sink, exclusive: false);
        t2.TryGetForUpdate(null, null!, B("foo"), sink, exclusive: false);
        List<KeyLockInfo> locks = db.GetLockStatusData();
        Assert.AreEqual(1, locks.Count);
        Assert.IsFalse(locks[0].Exclusive);
        Assert.AreEqual(2, locks[0].TransactionIds.Count);
        AssertFails(StatusCode.TimedOut, () => t3.TryGetForUpdate(null, null!, B("foo"), sink, exclusive: true));
        // Upgrade fails while another holder exists, succeeds once it's the only holder
        AssertFails(StatusCode.TimedOut, () => t1.Put(B("foo"), B("x")));
        t2.Rollback();
        t1.Put(B("foo"), B("x"));
        t1.Commit();
        Assert.AreEqual("x", GetString(db.Database, "foo"));
    }

    [TestMethod]
    public async Task LockWaitSucceedsWhenHolderCommits()
    {
        using TransactionDatabase db = Open(NewFs(), new TransactionDbOptions { TransactionLockTimeout = TimeSpan.FromSeconds(30) });
        using Transaction t1 = db.BeginTransaction(s_wo);
        using Transaction t2 = db.BeginTransaction(s_wo);
        await t1.PutAsync(B("k"), B("1"));
        Task waiting = t2.PutAsync(B("k"), B("2")).AsTask();
        await Task.Yield();
        Assert.IsFalse(waiting.IsCompleted); // waits asynchronously for t1's lock
        await t1.CommitAsync();
        await waiting;
        await t2.CommitAsync();
        Assert.AreEqual("2", GetString(db.Database, "k"));
    }

    [TestMethod]
    public void ZeroLockTimeoutFailsImmediately()
    {
        using TransactionDatabase db = Open(NewFs());
        using Transaction t1 = db.BeginTransaction(s_wo);
        using Transaction t2 = db.BeginTransaction(s_wo, new TransactionOptions { LockTimeout = TimeSpan.Zero });
        t1.Put(B("k"), B("1"));
        AssertFails(StatusCode.TimedOut, () => t2.Put(B("k"), B("2")), StatusSubCode.LockTimeout);
    }

    [TestMethod]
    public async Task DeadlockDetection()
    {
        using TransactionDatabase db = Open(NewFs(), new TransactionDbOptions { TransactionLockTimeout = TimeSpan.FromSeconds(30) });
        var opts = new TransactionOptions { DeadlockDetect = true };
        using Transaction t1 = db.BeginTransaction(s_wo, opts);
        using Transaction t2 = db.BeginTransaction(s_wo, opts);
        await t1.PutAsync(B("A"), B("1"));
        await t2.PutAsync(B("B"), B("2"));
        Task t1WaitsForB = t1.PutAsync(B("B"), B("1")).AsTask();
        // Wait until t1 is registered as waiting for t2
        while (((PessimisticTransactionView)t1).WaitingFor().Count == 0) await Task.Delay(1);
        await AssertFailsAsync(StatusCode.Busy, () => t2.PutAsync(B("A"), B("2")).AsTask(), StatusSubCode.Deadlock);
        List<DeadlockPath> deadlocks = db.GetDeadlockInfoBuffer();
        Assert.AreEqual(1, deadlocks.Count);
        Assert.IsFalse(deadlocks[0].LimitExceeded);
        Assert.AreEqual(2, deadlocks[0].Path.Count);
        // The cycle as upstream reports it: t1 waits for B (held by t2), t2 waits for A (held by t1).
        Assert.AreEqual(t1.Id, deadlocks[0].Path[0].TransactionId);
        Assert.AreEqual("B", S(deadlocks[0].Path[0].WaitingKey));
        Assert.AreEqual(t2.Id, deadlocks[0].Path[1].TransactionId);
        Assert.AreEqual("A", S(deadlocks[0].Path[1].WaitingKey));
        // Breaking the cycle lets t1 proceed
        await t2.RollbackAsync();
        await t1WaitsForB;
        await t1.CommitAsync();
        Assert.AreEqual("1", GetString(db.Database, "A"));
        Assert.AreEqual("1", GetString(db.Database, "B"));
    }

    [TestMethod]
    public void ExpiredTransactionLocksCanBeStolen()
    {
        using TransactionDatabase db = Open(NewFs());
        using Transaction t1 = db.BeginTransaction(s_wo, new TransactionOptions { Expiration = TimeSpan.FromMilliseconds(1) });
        t1.Put(B("k"), B("1"));
        SpinWait.SpinUntil(() => false, 5); // let t1 expire
        using Transaction t2 = db.BeginTransaction(s_wo);
        t2.Put(B("k"), B("2")); // steals the expired lock
        t2.Commit();
        AssertFails(StatusCode.Expired, t1.Commit);
        Assert.AreEqual("2", GetString(db.Database, "k"));
    }

    [TestMethod]
    public void SavePointReleasesLocks()
    {
        using TransactionDatabase db = Open(NewFs());
        using Transaction txn = db.BeginTransaction(s_wo);
        txn.Put(B("a"), B("1"));
        txn.SetSavePoint();
        txn.Put(B("b"), B("2"));
        txn.GetForUpdate(B("c"));
        Assert.AreEqual(3, db.GetLockStatusData().Count);
        Assert.IsTrue(txn.RollbackToSavePoint());
        Assert.AreEqual(1, db.GetLockStatusData().Count); // b and c unlocked
        db.Put(B("b"), B("direct"));
        AssertFails(StatusCode.TimedOut, () => db.Put(B("a"), B("direct")));
        txn.Commit();
        Assert.AreEqual("1", GetString(db.Database, "a"));
        Assert.AreEqual("direct", GetString(db.Database, "b"));
    }

    [TestMethod]
    public void UndoGetForUpdateReleasesLock()
    {
        using TransactionDatabase db = Open(NewFs());
        using Transaction txn = db.BeginTransaction(s_wo);
        txn.GetForUpdate(B("a"));
        txn.GetForUpdate(B("a"));
        txn.UndoGetForUpdate(B("a"));
        Assert.AreEqual(1, db.GetLockStatusData().Count); // still read once
        txn.UndoGetForUpdate(B("a"));
        Assert.AreEqual(0, db.GetLockStatusData().Count);
        txn.Put(B("b"), B("1"));
        txn.UndoGetForUpdate(B("b")); // a written key stays locked
        Assert.AreEqual(1, db.GetLockStatusData().Count);
    }

    [TestMethod]
    public void WriteBatchWithConcurrencyControl()
    {
        using TransactionDatabase db = Open(NewFs());
        using Transaction txn = db.BeginTransaction(s_wo);
        txn.Put(B("b"), B("txn"));
        using var batch = new WriteBatch();
        batch.Put(B("a"), B("1")).Put(B("b"), B("2"));
        AssertFails(StatusCode.TimedOut, () => db.Write(s_wo, batch));
        Assert.IsNull(GetString(db.Database, "a")); // nothing applied
        Assert.AreEqual(1, db.GetLockStatusData().Count); // batch locks released
        txn.Commit();
        db.Write(s_wo, batch);
        Assert.AreEqual("1", GetString(db.Database, "a"));
        Assert.AreEqual("2", GetString(db.Database, "b"));
    }

    [TestMethod]
    public async Task ConcurrentIncrementsWithGetForUpdate()
    {
        using TransactionDatabase db = Open(NewFs(), new TransactionDbOptions { TransactionLockTimeout = TimeSpan.FromSeconds(30) });
        db.Put(B("counter"), BitConverter.GetBytes(0L));
        const int Workers = 8, PerWorker = 50;
        var tasks = new Task[Workers];
        for (int w = 0; w < Workers; w++)
        {
            tasks[w] = Task.Run(async () =>
            {
                for (int i = 0; i < PerWorker; i++)
                {
                    using Transaction txn = db.BeginTransaction(s_wo);
                    byte[] v = (await txn.GetForUpdateAsync(B("counter")))!;
                    await txn.PutAsync(B("counter"), BitConverter.GetBytes(BitConverter.ToInt64(v) + 1));
                    await txn.CommitAsync();
                }
            });
        }
        await Task.WhenAll(tasks);
        Assert.AreEqual((long)Workers * PerWorker, BitConverter.ToInt64(db.Database.Get(B("counter"))!));
    }

    // ---- two-phase commit --------------------------------------------------------------------------------

    [TestMethod]
    public void TwoPhaseCommitBasic()
    {
        using TransactionDatabase db = Open(NewFs());
        using Transaction txn = db.BeginTransaction(s_wo, new TransactionOptions { SkipPrepare = false });
        AssertFails(StatusCode.InvalidArgument, txn.Prepare); // not named
        txn.SetName("xid");
        AssertFails(StatusCode.InvalidArgument, () => txn.SetName("again"));
        using (Transaction dup = db.BeginTransaction(s_wo)) AssertFails(StatusCode.InvalidArgument, () => dup.SetName("xid"));
        Assert.AreSame(txn, db.GetTransactionByName("xid"));
        txn.Put(B("foo"), B("bar"));
        AssertFails(StatusCode.InvalidArgument, txn.Commit, StatusSubCode.TxnNotPrepared);
        txn.Prepare();
        Assert.AreEqual(TransactionState.Prepared, txn.State);
        Assert.AreEqual(1, db.GetAllPreparedTransactions().Count);
        Assert.IsNull(GetString(db.Database, "foo")); // prepared data is not visible
        txn.Commit();
        Assert.AreEqual(TransactionState.Committed, txn.State);
        Assert.AreEqual("bar", GetString(db.Database, "foo"));
        Assert.AreEqual(0, db.GetAllPreparedTransactions().Count);
        Assert.IsNull(db.GetTransactionByName("xid"));
    }

    [TestMethod]
    public void TwoPhaseRollback()
    {
        using TransactionDatabase db = Open(NewFs());
        using Transaction txn = db.BeginTransaction(s_wo);
        txn.SetName("xid");
        txn.Put(B("foo"), B("bar"));
        txn.Prepare();
        txn.Rollback();
        Assert.AreEqual(TransactionState.RolledBack, txn.State);
        Assert.IsNull(GetString(db.Database, "foo"));
        Assert.AreEqual(0, db.GetLockStatusData().Count);
        AssertFails(StatusCode.InvalidArgument, txn.Commit);
    }

    [TestMethod]
    public void PreparedTransactionRecoveredAfterCrash()
    {
        MemoryFileSystem fs = NewFs();
        ulong prepLog;
        {
            TransactionDatabase db = Open(fs);
            db.Put(B("before"), B("1"));
            Transaction txn = db.BeginTransaction(s_sync);
            txn.SetName("xid#1");
            txn.Put(B("foo"), B("bar"));
            txn.Merge(B("m"), B("op")); // no merge operator needed until read
            txn.Delete(B("before"));
            txn.Prepare();
            prepLog = ((PessimisticTransactionView)txn).LogNumber;
            Assert.AreNotEqual(0UL, prepLog);
            // A second, committed 2PC transaction and a rolled back one
            Transaction t2 = db.BeginTransaction(s_sync);
            t2.SetName("xid#2");
            t2.Put(B("committed"), B("yes"));
            t2.Prepare();
            t2.Commit();
            Transaction t3 = db.BeginTransaction(s_sync);
            t3.SetName("xid#3");
            t3.Put(B("rolledback"), B("yes"));
            t3.Prepare();
            t3.Rollback();
            Crash(fs, db);
        }
        using (TransactionDatabase db = Open(fs))
        {
            Assert.AreEqual("yes", GetString(db.Database, "committed"));
            Assert.IsNull(GetString(db.Database, "rolledback"));
            Assert.IsNull(GetString(db.Database, "foo"));
            Assert.AreEqual("1", GetString(db.Database, "before"));
            List<Transaction> prepared = db.GetAllPreparedTransactions();
            Assert.AreEqual(1, prepared.Count);
            Transaction txn = prepared[0];
            Assert.AreEqual("xid#1", txn.Name);
            Assert.AreEqual(TransactionState.Prepared, txn.State);
            // WriteUnprepared keeps the recovered data in the DB only (the rebuilt batch is cleared).
            Assert.AreEqual(Policy == TransactionWritePolicy.WriteUnprepared ? 0 : 3, txn.Count);
            txn.Commit();
            Assert.AreEqual("bar", GetString(db.Database, "foo"));
            Assert.IsNull(GetString(db.Database, "before"));
        }
        using (TransactionDatabase db = Open(fs))
        {
            Assert.AreEqual(0, db.GetAllPreparedTransactions().Count);
            Assert.AreEqual("bar", GetString(db.Database, "foo"));
            Assert.IsNull(GetString(db.Database, "before"));
            Assert.AreEqual("yes", GetString(db.Database, "committed"));
        }
    }

    [TestMethod]
    public void RecoveredTransactionRollbackPersists()
    {
        MemoryFileSystem fs = NewFs();
        {
            TransactionDatabase db = Open(fs);
            Transaction txn = db.BeginTransaction(s_sync);
            txn.SetName("x");
            txn.Put(B("foo"), B("bar"));
            txn.Prepare();
            Crash(fs, db);
        }
        using (TransactionDatabase db = Open(fs))
        {
            Transaction txn = db.GetAllPreparedTransactions().Single();
            txn.Rollback();
        }
        using (TransactionDatabase db = Open(fs))
        {
            Assert.AreEqual(0, db.GetAllPreparedTransactions().Count);
            Assert.IsNull(GetString(db.Database, "foo"));
        }
    }

    [TestMethod]
    public void PreparedSectionWalSurvivesFlushesAndSwitches()
    {
        // The WAL holding an outstanding prepared section must not be deleted when memtables are flushed and newer
        // WALs are created (LogsWithPrepTracker / min_log_number_to_keep in 2PC mode).
        MemoryFileSystem fs = NewFs();
        {
            TransactionDatabase db = Open(fs);
            Transaction txn = db.BeginTransaction(s_sync);
            txn.SetName("long-running");
            txn.Put(B("p"), B("prepared"));
            txn.Prepare();
            for (int round = 0; round < 4; round++)
            {
                for (int i = 0; i < 50; i++) db.Put(Key(i), Value(i, round));
                db.Database.Flush();
            }
            // Committed 2PC data still only in a memtable keeps its prepare WAL too
            Transaction t2 = db.BeginTransaction(s_sync);
            t2.SetName("committed-in-mem");
            t2.Put(B("c"), B("mem"));
            t2.Prepare();
            db.Put(B("filler"), B("x"));
            db.Database.Flush();
            t2.Commit();
            db.Put(s_sync, null, B("last"), B("x"));
            Crash(fs, db);
        }
        using (TransactionDatabase db = Open(fs))
        {
            Transaction txn = db.GetAllPreparedTransactions().Single();
            Assert.AreEqual("long-running", txn.Name);
            Assert.AreEqual("mem", GetString(db.Database, "c"));
            Assert.AreEqual(S(Value(7, 3)), GetString(db.Database, S(Key(7))!));
            txn.Commit();
            Assert.AreEqual("prepared", GetString(db.Database, "p"));
        }
        using (TransactionDatabase db = Open(fs))
        {
            Assert.AreEqual("prepared", GetString(db.Database, "p"));
            Assert.AreEqual(0, db.GetAllPreparedTransactions().Count);
        }
    }

    [TestMethod]
    public void PlainDbRefusesWalWithPreparedSections()
    {
        MemoryFileSystem fs = NewFs();
        {
            TransactionDatabase db = Open(fs);
            Transaction txn = db.BeginTransaction(s_sync);
            txn.SetName("x");
            txn.Put(B("foo"), B("bar"));
            txn.Prepare();
            Crash(fs, db);
        }
        var e = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbPath, NewOptions(fs)).Dispose());
        Assert.AreEqual(StatusCode.NotSupported, e.Code);
        DbOptions o = NewOptions(fs);
        o.Allow2Pc = true;
        if (Policy != TransactionWritePolicy.WriteCommitted)
        {
            // A plain DB (write-after-commit) refuses WritePrepared / WriteUnprepared sections (WriteBatch::Iterate's
            // write-policy check), and so does a TransactionDB with another write policy.
            e = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbPath, o).Dispose());
            Assert.AreEqual(StatusCode.NotSupported, e.Code);
            StringAssert.Contains(e.Message, "WritePolicy");
            e = Assert.ThrowsExactly<RocksDbException>(() =>
                TransactionDatabase.Open(DbPath, NewOptions(fs), new TransactionDbOptions { WritePolicy = TransactionWritePolicy.WriteCommitted }).Dispose());
            Assert.AreEqual(StatusCode.NotSupported, e.Code);
            using TransactionDatabase same = Open(fs);
            Assert.AreEqual(1, same.GetAllPreparedTransactions().Count);
            return;
        }
        // allow_2pc on a plain DB recovers (and ignores) the prepared section
        using RocksDatabase plain = RocksDatabase.Open(DbPath, o);
        Assert.IsNull(GetString(plain, "foo"));
    }

    [TestMethod]
    public void CommittedTransactionsReplayFromWalAfterCrash()
    {
        MemoryFileSystem fs = NewFs();
        {
            TransactionDatabase db = Open(fs);
            for (int i = 0; i < 20; i++)
            {
                Transaction txn = db.BeginTransaction(s_sync);
                if (i % 2 == 0)
                {
                    txn.SetName("t" + i);
                    txn.Put(Key(i), Value(i, 0));
                    txn.Prepare();
                    db.Put(s_sync, null, B("interleaved" + i), B("x")); // consumes sequence numbers between prepare and commit
                    txn.Commit();
                }
                else
                {
                    txn.Put(Key(i), Value(i, 0));
                    txn.Commit();
                }
            }
            Crash(fs, db);
        }
        using TransactionDatabase db2 = Open(fs);
        for (int i = 0; i < 20; i++) Assert.AreEqual(S(Value(i, 0)), GetString(db2.Database, S(Key(i))!));
        // Sequence numbers after recovery do not reuse those consumed by commits. WritePrepared / WriteUnprepared use one
        // sequence number per batch (prepare, commit marker, plain write) and advance by one after recovery.
        ulong seq = db2.Database.GetLatestSequenceNumber();
        Assert.AreEqual(Policy == TransactionWritePolicy.WriteCommitted ? 20UL + 10UL : 10UL * 3 + 10UL + 1UL, seq);
    }

    /// <summary>Test-only view of the internal pessimistic transaction.</summary>
    private readonly struct PessimisticTransactionView(Transaction txn)
    {
        public static explicit operator PessimisticTransactionView(Transaction t) => new(t);
        private PessimisticTransaction Impl => (PessimisticTransaction)txn;
        public ulong LogNumber => Impl.LogNumber;
        public IReadOnlyList<ulong> WaitingFor() => Impl.GetWaitingTransactions(out _, out _);
    }
}

/// <summary>The transaction_test.cc ports with the WritePrepared policy (TransactionTest instantiated with WRITE_PREPARED).</summary>
[TestClass]
public sealed class WritePreparedPolicyTransactionTests : TransactionTests
{
    protected override TransactionWritePolicy Policy => TransactionWritePolicy.WritePrepared;
}

/// <summary>The transaction_test.cc ports with the WriteUnprepared policy.</summary>
[TestClass]
public sealed class WriteUnpreparedPolicyTransactionTests : TransactionTests
{
    protected override TransactionWritePolicy Policy => TransactionWritePolicy.WriteUnprepared;
}
