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

    /// <summary>
    /// TransactionDB::Write locks the batch's keys with PessimisticTransaction::LockBatch, whose handler only implements
    /// PutCF, PutEntityCF, MergeCF and DeleteCF: a SingleDelete on a non-default column family and any DeleteRange fail
    /// with InvalidArgument (WriteBatch::Handler defaults), and nothing of the batch is written; a SingleDelete on the
    /// default column family is accepted. Found by the managed/native round trip (TransactionRoundTripTests).
    /// </summary>
    [TestMethod]
    public void WriteBatchWithUnsupportedRecordsIsRejected()
    {
        using TransactionDatabase db = Open(NewFs());
        ColumnFamily other = db.Database.CreateColumnFamily("other", new ColumnFamilyOptions());
        db.Put(B("a"), B("1"));
        using (WriteBatch b = db.Database.CreateWriteBatch())
        {
            b.Put(B("x"), B("1"));
            b.SingleDelete(other.Id, B("a"));
            var e = Assert.ThrowsExactly<RocksDbException>(() => db.Write(s_wo, b));
            Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
            StringAssert.Contains(e.Message, "non-default column family and SingleDeleteCF not implemented");
        }
        using (WriteBatch b = db.Database.CreateWriteBatch())
        {
            b.Put(B("x"), B("1"));
            b.DeleteRange(0, B("a"), B("b"));
            var e = Assert.ThrowsExactly<RocksDbException>(() => db.Write(s_wo, b));
            Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
            StringAssert.Contains(e.Message, "DeleteRangeCF not implemented");
        }
        Assert.IsNull(db.Database.Get(B("x")));
        Assert.AreEqual(0, db.GetLockStatusData().Count);
        using (WriteBatch b = db.Database.CreateWriteBatch())
        {
            b.SingleDelete(0, B("a"));
            b.Put(B("x"), B("2"));
            db.Write(s_wo, b);
        }
        Assert.IsNull(db.Database.Get(B("a")));
        Assert.AreEqual("2", GetString(db.Database, "x"));
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

    // ---- recycled lock entries / tracked keys ------------------------------------------------------------

    [TestMethod]
    public void RecycledLockEntriesAndTrackedKeysKeepTheirKeys()
    {
        // Lock entries and tracked-key buffers are recycled: keys of growing and shrinking lengths must never see bytes of
        // a previous, longer key (the recycled buffers are larger than the key).
        using TransactionDatabase db = Open(NewFs());
        Transaction? txn = null;
        string[] keys = ["a", new string('b', 40), "cc", new string('d', 17), "e", new string('f', 100), "g"];
        for (int round = 0; round < 4; round++)
        {
            txn = txn is null ? db.BeginTransaction(s_wo) : db.BeginTransaction(s_wo, new TransactionOptions(), txn);
            foreach (string k in keys) txn.Put(B(k + round), B("v" + round));
            var locked = new HashSet<string>();
            foreach (KeyLockInfo l in db.GetLockStatusData()) locked.Add(S(l.Key)!);
            foreach (string k in keys) Assert.IsTrue(locked.Contains(k + round), "lock on " + k + round);
            Assert.AreEqual(keys.Length, locked.Count);
            // a second transaction cannot take any of them, but can take their prefixes
            using (Transaction other = db.BeginTransaction(s_wo, new TransactionOptions { LockTimeout = TimeSpan.Zero }))
            {
                foreach (string k in keys) AssertFails(StatusCode.TimedOut, () => other.Put(B(k + round), B("x")), StatusSubCode.LockTimeout);
                other.Put(B(keys[1][..10]), B("x"));
                other.Rollback();
            }
            txn.SetSavePoint();
            txn.Put(B("sp-" + new string('z', 30 - round)), B("1"));
            Assert.IsTrue(txn.RollbackToSavePoint());
            Assert.AreEqual(keys.Length, db.GetLockStatusData().Count, "the save point's key is unlocked");
            if (round % 2 == 0) txn.Commit();
            else txn.Rollback();
            Assert.AreEqual(0, db.GetLockStatusData().Count);
        }
        for (int round = 0; round < 4; round++)
        {
            foreach (string k in keys) Assert.AreEqual(round % 2 == 0 ? "v" + round : null, S(db.Database.Get(B(k + round))));
        }
        txn!.Dispose();
    }

    [TestMethod]
    public void SteadyStateTransactionsAllocateNothing()
    {
        // Begin (reusing the transaction object) + puts + rollback touches locks, tracked keys and the indexed batch but
        // not the memtables: it must not allocate once warm. A commit allocates what the same plain batch write does.
        var wo = new WriteOptions { DisableWal = true };
        using TransactionDatabase db = Open(NewFs(), cfo: new ColumnFamilyOptions { WriteBufferSize = 256UL << 20 });
        byte[][] keys = new byte[32][];
        for (int i = 0; i < keys.Length; i++) keys[i] = B("key" + i.ToString("D8", System.Globalization.CultureInfo.InvariantCulture));
        byte[] value = new byte[64];
        var options = new TransactionOptions();
        Transaction txn = db.BeginTransaction(wo, options);
        using var batch = new WriteBatch();
        int next = 0;
        long Run(int count, int mode)
        {
            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int t = 0; t < count; t++)
            {
                if (mode == 2)
                {
                    batch.Clear();
                    for (int i = 0; i < 4; i++) batch.Put(keys[next++ & 31], value);
                    db.Database.Write(wo, batch);
                    continue;
                }
                txn = db.BeginTransaction(wo, options, txn);
                for (int i = 0; i < 4; i++) txn.Put(keys[next++ & 31], value);
                if (mode == 0) txn.Rollback();
                else txn.Commit();
            }
            return GC.GetAllocatedBytesForCurrentThread() - before;
        }
        Run(2000, 0);
        Run(2000, 1);
        Run(2000, 2);
        long rollback = Run(2000, 0);
        long commit = Run(2000, 1), plain = Run(2000, 2);
#if !DEBUG
        Assert.AreEqual(0L, rollback, "begin + put + rollback");
        // The memtable arena grows in blocks: allow one block of difference.
        Assert.IsTrue(commit <= plain + (64 << 10), "commit " + commit + " B vs plain batch " + plain + " B");
#else
        // Debug builds compile async state machines as classes, so the shared sync/async paths allocate there.
        _ = rollback;
        _ = commit;
        _ = plain;
#endif
        txn.Dispose();
    }

    // ---- assume_tracked, MultiGet, indexing, counters, SetLockTimeout ---------------------------------------

    /// <summary>Port of TransactionTest.AssumeExclusiveTracked.</summary>
    [TestMethod]
    public void AssumeExclusiveTracked()
    {
        using TransactionDatabase db = Open(NewFs());
        ColumnFamily cf = db.Database.DefaultColumnFamily;
        using Transaction txn = db.BeginTransaction(s_wo, new TransactionOptions { LockTimeout = TimeSpan.FromMilliseconds(1) });
        txn.SetSnapshot();
        // commit a value after the snapshot is taken
        db.Put(B("foo"), B("bar"));
        var value = new System.Buffers.ArrayBufferWriter<byte>();
        // By default write should fail to the commit after our snapshot
        AssertFails(StatusCode.Busy, () => txn.TryGetForUpdate(null, cf, B("foo"), value, exclusive: true));
        // But the user could direct the db to skip validating the snapshot. The read value then should be the most
        // recently committed
        Assert.IsTrue(txn.TryGetForUpdate(null, cf, B("foo"), value, exclusive: true, doValidate: false));
        Assert.AreEqual("bar", S(value.WrittenSpan.ToArray()));
        // Although ValidateSnapshot is skipped the key must have still got locked
        AssertFails(StatusCode.TimedOut, () => db.Put(B("foo"), B("bar")));
        // By default the write operations should fail due to the commit after the snapshot
        AssertFails(StatusCode.Busy, () => txn.Put(B("foo"), B("bar1")));
        AssertFails(StatusCode.Busy, () => txn.Put(cf, B("foo"), B("bar1"), assumeTracked: false));
        // But the user could direct the db that it already assumes exclusive lock on the key due to the previous
        // GetForUpdate call.
        long keys = txn.NumKeys;
        txn.Put(cf, B("foo"), B("bar1"), assumeTracked: true);
        txn.Merge(cf, B("foo"), B("bar2"), assumeTracked: true);
        txn.Delete(cf, B("foo"), assumeTracked: true);
        txn.SingleDelete(cf, B("foo"), assumeTracked: true);
        Assert.AreEqual(keys, txn.NumKeys);
        Assert.AreEqual(1, txn.NumPuts);
        Assert.AreEqual(1, txn.NumMerges);
        Assert.AreEqual(2, txn.NumDeletes);
        txn.Rollback();
    }

    /// <summary>PessimisticTransaction::TryLock: assume_tracked on a key that is not tracked is InvalidArgument.</summary>
    [TestMethod]
    public void AssumeTrackedOnUntrackedKeyIsInvalidArgument()
    {
        using TransactionDatabase db = Open(NewFs());
        ColumnFamily cf = db.Database.DefaultColumnFamily;
        using (Transaction txn = db.BeginTransaction(s_wo))
        {
            AssertFails(StatusCode.InvalidArgument, () => txn.Put(cf, B("k"), B("v"), assumeTracked: true));
            StringAssert.Contains(Assert.ThrowsExactly<RocksDbException>(() => txn.Merge(cf, B("k"), B("v"), assumeTracked: true)).Message,
                "assume_tracked is set but it is not tracked yet");
            Assert.AreEqual(0, txn.Count);
            Assert.AreEqual(0, txn.NumKeys);
            // The port does not leak the lock taken for the rejected write: others can write the key.
            db.Put(B("k"), B("other"));
            txn.Commit();
        }
        Assert.AreEqual("other", S(db.Database.Get(B("k"))));
    }

    /// <summary>Port of TransactionTest.MultiGetBatchedTest.</summary>
    [TestMethod]
    public void MultiGetBatchedTest()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        o.CreateMissingColumnFamilies = true;
        using TransactionDatabase db = TransactionDatabase.Open(DbPath, o, TxnDbOptions(),
            [new("default", new ColumnFamilyOptions()), new("CF", new ColumnFamilyOptions { MergeOperator = MergeOperator.StringAppend(",") })]);
        ColumnFamily cf = db.Database.GetColumnFamily("CF");
        using (WriteBatch batch = new())
        {
            batch.Put(cf, B("aaa"), B("val1")).Put(cf, B("bbb"), B("val2")).Put(cf, B("ccc"), B("val3")).Put(cf, B("ddd"), B("foo"))
                .Put(cf, B("eee"), B("val5")).Put(cf, B("fff"), B("val6")).Merge(cf, B("ggg"), B("foo"));
            db.Write(s_wo, batch);
        }
        using Transaction txn = db.BeginTransaction(s_wo);
        txn.SetSnapshot();
        var snapshotRead = new ReadOptions { Snapshot = txn.Snapshot };
        txn.Delete(cf, B("bbb"));
        txn.Put(cf, B("ccc"), B("val3_new"));
        txn.Merge(cf, B("ddd"), B("bar"));
        string[] keys = ["aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg"];
        using var mg = new MultiGetBatch();
        foreach (string k in keys) mg.Add(B(k));
        txn.MultiGet(snapshotRead, cf, mg);
        string?[] expected = ["val1", null, "val3_new", "foo,bar", "val5", "val6", "foo"];
        for (int i = 0; i < keys.Length; i++)
        {
            Assert.AreEqual(expected[i] is null ? MultiGetStatus.NotFound : MultiGetStatus.Found, mg.GetStatus(i), keys[i]);
            if (expected[i] is not null) Assert.AreEqual(expected[i], S(mg.GetValue(i).ToArray()), keys[i]);
        }
        // MultiGet does not lock: another writer is not blocked.
        db.Put(null, cf, B("aaa"), B("other"));
    }

    /// <summary>TransactionTest.ColumnFamiliesTest (MultiGetForUpdate part) and PredicateManyPreceders.</summary>
    [TestMethod]
    public void MultiGetForUpdateLocksAllKeys()
    {
        using TransactionDatabase db = Open(NewFs());
        db.Put(B("a"), B("1"));
        db.Put(B("b"), B("2"));
        using Transaction txn = db.BeginTransaction(s_wo);
        txn.Put(B("c"), B("own"));
        using var mg = new MultiGetBatch();
        mg.Add(B("a")).Add(B("b")).Add(B("c")).Add(B("d"));
        txn.MultiGetForUpdate(null, mg);
        CollectionAssert.AreEqual(new[] { MultiGetStatus.Found, MultiGetStatus.Found, MultiGetStatus.Found, MultiGetStatus.NotFound },
            Enumerable.Range(0, 4).Select(mg.GetStatus).ToArray());
        Assert.AreEqual("own", S(mg.GetValue(2).ToArray()));
        Assert.AreEqual(4, txn.NumKeys);
        // Every key (also the absent one) is locked.
        using (Transaction txn2 = db.BeginTransaction(s_wo))
        {
            AssertFails(StatusCode.TimedOut, () => txn2.Put(B("d"), B("x")));
            // A MultiGetForUpdate that cannot lock a key fails as a whole.
            using var mg2 = new MultiGetBatch();
            mg2.Add(B("z")).Add(B("a"));
            AssertFails(StatusCode.TimedOut, () => txn2.MultiGetForUpdate(null, mg2));
        }
        txn.Commit();
        db.Put(B("d"), B("x"));
    }

    /// <summary>Port of TransactionTest.DisableIndexingTest.</summary>
    [TestMethod]
    public void DisableIndexingTest()
    {
        // Skip for write unprepared: it does not solely rely on WBWI for read your own writes.
        if (Policy == TransactionWritePolicy.WriteUnprepared) return;
        using TransactionDatabase db = Open(NewFs());
        using Transaction txn = db.BeginTransaction(s_wo);
        txn.Put(B("A"), B("a"));
        Assert.AreEqual("a", S(txn.Get(B("A"))));
        txn.DisableIndexing();
        Assert.IsFalse(txn.IsIndexingEnabled);
        txn.Put(B("B"), B("b"));
        Assert.IsNull(txn.Get(B("B")));
        using (TransactionIterator it = txn.NewIterator(null))
        {
            it.Seek(B("B"));
            Assert.IsFalse(it.Valid);
        }
        txn.Delete(B("A"));
        Assert.AreEqual("a", S(txn.Get(B("A"))));
        txn.EnableIndexing();
        txn.Put(B("B"), B("bb"));
        using (TransactionIterator it = txn.NewIterator(null))
        {
            it.Seek(B("B"));
            Assert.IsTrue(it.Valid);
            Assert.AreEqual("bb", S(it.Value.ToArray()));
        }
        Assert.AreEqual(3, txn.NumPuts);
        Assert.AreEqual(1, txn.NumDeletes);
    }

    /// <summary>TransactionTest.SavepointTest counters: RollbackToSavePoint restores GetNumPuts / Deletes / Merges.</summary>
    [TestMethod]
    public void CountersFollowSavePoints()
    {
        using TransactionDatabase db = Open(NewFs(), null, new ColumnFamilyOptions { MergeOperator = MergeOperator.StringAppend(",") });
        using Transaction txn = db.BeginTransaction(s_wo);
        txn.Put(B("a"), B("1"));
        txn.SetSavePoint();
        txn.Delete(B("b"));
        txn.SingleDelete(B("c"));
        txn.Merge(B("d"), B("x"));
        txn.PutUntracked(B("e"), B("2"));
        txn.PutEntity(B("f"), [WideColumn.Default(B("w"))]);
        Assert.AreEqual(2, txn.NumPuts);
        Assert.AreEqual(2, txn.NumDeletes);
        Assert.AreEqual(1, txn.NumMerges);
        Assert.AreEqual(1, txn.NumPutEntities);
        Assert.AreEqual(6, txn.NumKeys);
        Assert.IsTrue(txn.RollbackToSavePoint());
        Assert.AreEqual(1, txn.NumPuts);
        Assert.AreEqual(0, txn.NumDeletes);
        Assert.AreEqual(0, txn.NumMerges);
        Assert.AreEqual(0, txn.NumPutEntities);
        Assert.AreEqual(1, txn.NumKeys);
        txn.Commit();
        Assert.IsTrue(txn.ElapsedTime >= TimeSpan.Zero);
    }

    /// <summary>TransactionTest.TimeoutTest (SetLockTimeout / GetElapsedTime part).</summary>
    [TestMethod]
    public void SetLockTimeoutChangesTheWait()
    {
        using TransactionDatabase db = Open(NewFs(), new TransactionDbOptions { TransactionLockTimeout = s_short, DefaultLockTimeout = s_short });
        using Transaction holder = db.BeginTransaction(s_wo);
        holder.Put(B("a"), B("1"));
        using Transaction waiter = db.BeginTransaction(s_wo, new TransactionOptions { LockTimeout = TimeSpan.FromMilliseconds(1) });
        waiter.SetLockTimeout(TimeSpan.FromMilliseconds(300));
        long start = System.Diagnostics.Stopwatch.GetTimestamp();
        AssertFails(StatusCode.TimedOut, () => waiter.Put(B("a"), B("2")));
        Assert.IsTrue(System.Diagnostics.Stopwatch.GetElapsedTime(start) >= TimeSpan.FromMilliseconds(250));
        Assert.IsTrue(waiter.ElapsedTime >= TimeSpan.FromMilliseconds(250));
        // Released by the holder while waiting: the longer timeout lets the waiter win.
        waiter.SetLockTimeout(TimeSpan.FromSeconds(30));
        Task t = Task.Run(() => waiter.Put(B("a"), B("2")));
        holder.Commit();
        t.Wait(TimeSpan.FromSeconds(30));
        waiter.Commit();
        Assert.AreEqual("2", S(db.Database.Get(B("a"))));
    }

    // ---- reads over a DB merge chain spanning memtable and table files --------------------------------------

    /// <summary>
    /// Transaction reads where the batch has merge operands for a key whose DB value is itself an unresolved merge chain
    /// over a Put in a table file, more operands in another file and in the memtable: GetFromBatchAndDB, GetForUpdate,
    /// MultiGet and the iterator over the base all resolve the whole chain, then the transaction's operands.
    /// </summary>
    [TestMethod]
    public void BatchMergesOverDbMergeChainAcrossMemtableAndFiles()
    {
        using TransactionDatabase db = Open(NewFs(), null, new ColumnFamilyOptions { MergeOperator = MergeOperator.StringAppend(",") });
        db.Put(B("k"), B("base"));
        db.Merge(B("k"), B("f1"));
        db.Database.Flush();
        db.Merge(B("k"), B("f2"));
        db.Database.Flush();
        db.Merge(B("k"), B("m1"));          // memtable
        db.Merge(B("n"), B("only-db"));     // a chain without a base value
        db.Database.Flush();
        db.Merge(B("n"), B("mem"));
        using Transaction txn = db.BeginTransaction(s_wo);
        txn.Merge(B("k"), B("t1"));
        txn.Merge(B("k"), B("t2"));
        txn.Merge(B("n"), B("t"));
        const string k = "base,f1,f2,m1,t1,t2";
        const string n = "only-db,mem,t";
        Assert.AreEqual(k, S(txn.Get(B("k"))));
        Assert.AreEqual(n, S(txn.Get(B("n"))));
        Assert.AreEqual(k, S(txn.GetForUpdate(B("k"))));
        using (var mg = new MultiGetBatch())
        {
            mg.Add(B("k")).Add(B("n"));
            txn.MultiGet(null, mg);
            Assert.AreEqual(k, S(mg.GetValue(0).ToArray()));
            Assert.AreEqual(n, S(mg.GetValue(1).ToArray()));
        }
        if (Policy != TransactionWritePolicy.WriteUnprepared)
        {
            using TransactionIterator it = txn.NewIterator(null);
            var seen = new List<string>();
            for (it.SeekToFirst(); it.Valid; it.Next()) seen.Add(S(it.Key.ToArray()) + "=" + S(it.Value.ToArray()));
            CollectionAssert.AreEqual(new[] { "k=" + k, "n=" + n }, seen);
            seen.Clear();
            for (it.SeekToLast(); it.Valid; it.Prev()) seen.Add(S(it.Key.ToArray())!);
            CollectionAssert.AreEqual(new[] { "n", "k" }, seen);
        }
        txn.Commit();
        Assert.AreEqual(k, S(db.Database.Get(B("k"))));
        db.Database.CompactRange();
        Assert.AreEqual(k, S(db.Database.Get(B("k"))));
        Assert.AreEqual(n, S(db.Database.Get(B("n"))));
    }

    private static string? S(byte[]? b) => b is null ? null : System.Text.Encoding.UTF8.GetString(b);

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
