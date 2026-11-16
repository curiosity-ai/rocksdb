using System.Buffers;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Tests.Db;
using RocksDb.Managed.Utilities.Backup;
using RocksDb.Managed.Utilities.Transactions;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #7, utilities under failure and boundary conditions: the transaction state machines
/// (utilities/transactions/pessimistic_transaction.cc, optimistic_transaction.cc), lock waits
/// (lock/point/point_lock_manager.cc), WriteBatchWithIndex (write_batch_with_index.cc), the backup engine under injected
/// I/O faults and damaged files (utilities/backup/backup_engine.cc) and checkpoints
/// (utilities/checkpoint/checkpoint_impl.cc).
/// </summary>
[TestClass]
public sealed class Audit7UtilitiesFailuresTests
{
    private const string DbDir = "/a7db";
    private static readonly WriteOptions s_wo = new();
    private static readonly TimeSpan s_short = TimeSpan.FromMilliseconds(20);

    private static TransactionDatabase OpenTxnDb(IFileSystem fs, TransactionDbOptions? options = null)
    {
        options ??= new TransactionDbOptions();
        options.TransactionLockTimeout = s_short;
        options.DefaultLockTimeout = s_short;
        return TransactionDatabase.Open(DbDir, NewOptions(fs), options);
    }

    private static RocksDbException Fails(StatusCode code, Action a)
    {
        RocksDbException e = Assert.ThrowsExactly<RocksDbException>(a);
        Assert.AreEqual(code, e.Code, e.Message);
        return e;
    }

    // ---- pessimistic transaction state machine ------------------------------------------------------------------

    [TestMethod]
    public void PessimisticStateMachineEdges()
    {
        // PessimisticTransaction::Commit / Rollback / Prepare / SetName (pessimistic_transaction.cc): every call in a
        // state that does not allow it is InvalidArgument (or TxnNotPrepared) and changes nothing.
        MemoryFileSystem fs = NewFs();
        using TransactionDatabase db = OpenTxnDb(fs);

        using (Transaction t = db.BeginTransaction(s_wo))
        {
            t.Put(B("a"), B("1"));
            t.Commit();
            Assert.AreEqual(TransactionState.Committed, t.State);
            StringAssert.Contains(Fails(StatusCode.InvalidArgument, t.Commit).Message, "already been committed");
            StringAssert.Contains(Fails(StatusCode.InvalidArgument, t.Rollback).Message, "already been committed");
            Fails(StatusCode.InvalidArgument, () => t.SetName("late"));
            Fails(StatusCode.InvalidArgument, () => t.RollbackToSavePoint());
        }

        using (Transaction t = db.BeginTransaction(s_wo))
        {
            // Unnamed: cannot prepare.
            StringAssert.Contains(Fails(StatusCode.InvalidArgument, t.Prepare).Message, "not been named");
            t.SetName("x1");
            Fails(StatusCode.InvalidArgument, () => t.SetName("x2"));
            t.Put(B("b"), B("1"));
            t.Prepare();
            StringAssert.Contains(Fails(StatusCode.InvalidArgument, t.Prepare).Message, "already been prepared");
            Fails(StatusCode.InvalidArgument, () => t.RollbackToSavePoint());
            t.Rollback();
            Assert.AreEqual(TransactionState.RolledBack, t.State);
            StringAssert.Contains(Fails(StatusCode.InvalidArgument, t.Commit).Message, "rolledback");
            StringAssert.Contains(Fails(StatusCode.InvalidArgument, t.Prepare).Message, "rolledback");
            StringAssert.Contains(Fails(StatusCode.InvalidArgument, t.Rollback).Message, "not in state for rollback");
        }
        Assert.IsNull(db.Database.Get(B("b")));

        // Names: unique while registered, reusable once the holder committed or was disposed.
        Transaction holder = db.BeginTransaction(s_wo);
        holder.SetName("dup");
        using (Transaction other = db.BeginTransaction(s_wo))
        {
            Fails(StatusCode.InvalidArgument, () => other.SetName("dup"));
            Assert.IsNull(other.Name);
            holder.Dispose();
            other.SetName("dup");
            Assert.AreSame(other, db.GetTransactionByName("dup"));
            other.Put(B("c"), B("1"));
            other.Prepare();
            other.Commit();
            Assert.IsNull(db.GetTransactionByName("dup"));
        }
        using (Transaction again = db.BeginTransaction(s_wo))
        {
            again.SetName("dup");
        }

        // A started transaction without skip_prepare: Commit is TxnNotPrepared, the transaction stays usable.
        using (Transaction t = db.BeginTransaction(s_wo, new TransactionOptions { SkipPrepare = false }))
        {
            t.Put(B("d"), B("1"));
            RocksDbException e = Fails(StatusCode.InvalidArgument, t.Commit);
            Assert.AreEqual(StatusSubCode.TxnNotPrepared, e.SubCode);
            Assert.AreEqual(TransactionState.Started, t.State);
            t.SetName("np");
            t.Prepare();
            t.Commit();
        }
        Assert.AreEqual("1", GetString(db.Database, "d"));

        // Rollback of a started transaction only clears it (state stays Started), so it can be reused and committed.
        using (Transaction t = db.BeginTransaction(s_wo))
        {
            t.Put(B("e"), B("1"));
            t.Rollback();
            Assert.AreEqual(TransactionState.Started, t.State);
            Assert.AreEqual(0, t.Count);
            t.Put(B("f"), B("1"));
            t.Commit();
        }
        Assert.IsNull(db.Database.Get(B("e")));
        Assert.AreEqual("1", GetString(db.Database, "f"));

        // Use after Dispose.
        Transaction gone = db.BeginTransaction(s_wo);
        gone.Put(B("g"), B("1"));
        gone.Dispose();
        gone.Dispose();
        Assert.ThrowsExactly<ObjectDisposedException>(() => gone.Put(B("g"), B("2")));
        Assert.ThrowsExactly<ObjectDisposedException>(gone.Commit);
        Assert.ThrowsExactly<ObjectDisposedException>(gone.Rollback);
        Assert.ThrowsExactly<ObjectDisposedException>(gone.Prepare);
        Assert.ThrowsExactly<ObjectDisposedException>(() => gone.Get(B("g")));
        Assert.ThrowsExactly<ObjectDisposedException>(() => db.BeginTransaction(s_wo, new TransactionOptions(), gone));
        // Its lock on g was released by Dispose.
        using (Transaction t = db.BeginTransaction(s_wo, new TransactionOptions { LockTimeout = TimeSpan.Zero }))
        {
            t.Put(B("g"), B("3"));
            t.Commit();
        }
        Assert.AreEqual("3", GetString(db.Database, "g"));
    }

    [TestMethod]
    public void SavePointsWithoutASavePointAndManySavePoints()
    {
        // TransactionBaseImpl::RollbackToSavePoint / PopSavePoint: NotFound without a save point (bool false here);
        // WriteBatchWithIndex::RollbackToSavePoint over thousands of nested save points.
        using TransactionDatabase db = OpenTxnDb(NewFs());
        using Transaction t = db.BeginTransaction(s_wo);
        Assert.IsFalse(t.RollbackToSavePoint());
        Assert.IsFalse(t.PopSavePoint());
        const int n = 3000;
        for (int i = 0; i < n; i++)
        {
            t.SetSavePoint();
            t.Put(Key(i), Value(i, 1));
        }
        for (int i = n - 1; i >= n / 2; i--)
        {
            Assert.IsTrue(t.RollbackToSavePoint());
            Assert.AreEqual(i, t.Count);
        }
        for (int i = 0; i < n / 2; i++) Assert.IsTrue(t.PopSavePoint());
        Assert.IsFalse(t.PopSavePoint());
        Assert.AreEqual(n / 2, t.Count);
        // Keys locked inside rolled-back save points were released.
        using (Transaction other = db.BeginTransaction(s_wo, new TransactionOptions { LockTimeout = TimeSpan.Zero }))
        {
            other.Put(Key(n - 1), B("o"));
            Fails(StatusCode.TimedOut, () => other.Put(Key(0), B("o")));
        }
        t.Commit();
        Assert.IsNull(db.Database.Get(Key(n / 2)));
        Assert.IsNotNull(db.Database.Get(Key(n / 2 - 1)));
    }

    [TestMethod]
    public void WriteBeyondMaxWriteBatchSizeIsMemoryLimitAndLeavesTheTransactionUsable()
    {
        // TransactionBaseImpl::Put -> WriteBatch::Put with max_bytes (LocalSavePoint::commit): Status::MemoryLimit()
        // = Aborted(MemoryLimit); the record is rolled back and the transaction keeps its earlier writes.
        using TransactionDatabase db = OpenTxnDb(NewFs());
        using Transaction t = db.BeginTransaction(s_wo, new TransactionOptions { MaxWriteBatchSize = 200 });
        t.Put(B("k1"), B("v1"));
        RocksDbException e = Fails(StatusCode.Aborted, () => t.Put(B("k2"), new byte[300]));
        Assert.AreEqual(StatusSubCode.MemoryLimit, e.SubCode);
        Assert.AreEqual(1, t.Count);
        Assert.IsNull(t.Get(B("k2")));
        t.Put(B("k3"), B("v3"));
        t.Commit();
        Assert.AreEqual("v1", GetString(db.Database, "k1"));
        Assert.IsNull(db.Database.Get(B("k2")));
        Assert.AreEqual("v3", GetString(db.Database, "k3"));
    }

    [TestMethod]
    public void LockWaitsEndWithTimedOutAndReleaseOnEveryPath()
    {
        // PointLockManager::AcquireWithTimeout: timeout 0 fails at once, a positive timeout fails with
        // TimedOut(LockTimeout) once it elapses; nothing hangs and nothing stays locked.
        using TransactionDatabase db = OpenTxnDb(NewFs());
        using Transaction holder = db.BeginTransaction(s_wo);
        holder.Put(B("k"), B("h"));
        foreach (TimeSpan timeout in new[] { TimeSpan.Zero, TimeSpan.FromMilliseconds(30) })
        {
            using Transaction w = db.BeginTransaction(s_wo, new TransactionOptions { LockTimeout = timeout });
            var sw = System.Diagnostics.Stopwatch.StartNew();
            RocksDbException e = Fails(StatusCode.TimedOut, () => w.Put(B("k"), B("w")));
            Assert.AreEqual(StatusSubCode.LockTimeout, e.SubCode);
            Assert.IsTrue(sw.Elapsed < TimeSpan.FromSeconds(10));
            e = Fails(StatusCode.TimedOut, () => w.GetForUpdate(B("k")));
            Assert.AreEqual(0L, w.NumKeys);
        }
        // A cancelled async wait leaves no waiter behind; the holder's commit then lets a new waiter in.
        using (Transaction w = db.BeginTransaction(s_wo, new TransactionOptions { LockTimeout = TimeSpan.FromSeconds(-1) }))
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(30));
            Assert.ThrowsExactly<TaskCanceledException>(() => w.PutAsync(B("k"), B("w"), cts.Token).AsTask().GetAwaiter().GetResult());
            Assert.AreEqual(0L, w.NumKeys);
        }
        holder.Commit();
        using (Transaction w = db.BeginTransaction(s_wo, new TransactionOptions { LockTimeout = TimeSpan.Zero }))
        {
            w.Put(B("k"), B("w"));
            w.Commit();
        }
        Assert.AreEqual("w", GetString(db.Database, "k"));
    }

    [TestMethod]
    public void ExceptionFromAMergeOperatorInsideGetForUpdateKeepsTheLockTrackedAndReleasable()
    {
        // TransactionBaseImpl::GetForUpdate: TryLock, then the read. A failing read keeps the lock tracked (released
        // with the transaction), it is never leaked.
        MemoryFileSystem fs = NewFs();
        var options = new TransactionDbOptions();
        var dbo = NewOptions(fs);
        using TransactionDatabase db = TransactionDatabase.Open(DbDir, dbo, options,
            [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, new ColumnFamilyOptions { MergeOperator = new ThrowingMerge() })]);
        db.Database.Put(B("m"), B("base"));
        db.Database.Merge(B("m"), B("op"));
        using (Transaction t = db.BeginTransaction(s_wo))
        {
            Assert.ThrowsExactly<InvalidOperationException>(() => t.GetForUpdate(B("m")));
            Assert.AreEqual(1L, t.NumKeys);
        }
        using (Transaction t = db.BeginTransaction(s_wo, new TransactionOptions { LockTimeout = TimeSpan.Zero }))
        {
            t.Put(B("m"), B("ok"));
            t.Commit();
        }
        Assert.AreEqual("ok", GetString(db.Database, "m"));
    }

    private sealed class ThrowingMerge : MergeOperator
    {
        public override string Name => "ThrowingMerge";

        public override bool FullMerge(ReadOnlySpan<byte> key, bool hasExistingValue, ReadOnlySpan<byte> existingValue, in MergeOperands operands,
            System.Buffers.IBufferWriter<byte> result) => throw new InvalidOperationException("merge failed");
    }

    [TestMethod]
    public void PreparedTransactionsSurviveReopenAndAreResolvedOnce()
    {
        // PessimisticTransactionDB::Initialize: prepared sections in the WAL come back as prepared transactions (named,
        // PREPARED); committing or rolling them back resolves them for good; a transaction left prepared and merely
        // disposed is recovered again.
        MemoryFileSystem fs = NewFs();
        using (TransactionDatabase db = OpenTxnDb(fs))
        {
            foreach (string name in new[] { "c", "r", "keep" })
            {
                Transaction t = db.BeginTransaction(new WriteOptions { Sync = true });
                t.SetName(name);
                t.Put(B("k-" + name), B(name));
                t.Prepare();
                // Abandoned (process "crash"): not committed, not disposed.
            }
        }
        for (int round = 0; round < 2; round++)
        {
            using TransactionDatabase db = OpenTxnDb(fs);
            List<Transaction> prepared = db.GetAllPreparedTransactions();
            var names = prepared.ConvertAll(static t => t.Name!);
            names.Sort(StringComparer.Ordinal);
            CollectionAssert.AreEqual(round == 0 ? new[] { "c", "keep", "r" } : new[] { "keep" }, names);
            foreach (string n in new[] { "c", "r", "keep" }) Assert.IsNull(db.Database.Get(B("k-" + n)) is { } v && n != "c" ? v : null);
            using (Transaction dup = db.BeginTransaction(s_wo)) Fails(StatusCode.InvalidArgument, () => dup.SetName("keep"));
            if (round == 0)
            {
                Transaction c = db.GetTransactionByName("c")!;
                StringAssert.Contains(Fails(StatusCode.InvalidArgument, c.Prepare).Message, "already been prepared");
                c.Commit();
                db.GetTransactionByName("r")!.Rollback();
                Assert.IsNull(db.GetTransactionByName("c"));
                Assert.IsNotNull(db.GetTransactionByName("r"), "a rolled back named transaction stays registered until disposed (upstream too)");
            }
            Assert.AreEqual("c", GetString(db.Database, "k-c"));
            Assert.IsNull(db.Database.Get(B("k-r")));
            Assert.IsNull(db.Database.Get(B("k-keep")));
        }
        using (TransactionDatabase db = OpenTxnDb(fs))
        {
            db.GetTransactionByName("keep")!.Commit();
        }
        using (TransactionDatabase db = OpenTxnDb(fs))
        {
            Assert.AreEqual(0, db.GetAllPreparedTransactions().Count);
            Assert.AreEqual("keep", GetString(db.Database, "k-keep"));
        }
    }

    [TestMethod]
    public void DirectWritesWithTimestampsAreNotSupportedOnTheTransactionDb()
    {
        // pessimistic_transaction_db.h FailIfCfEnablesTs (PessimisticTransactionDB::Put / Delete / SingleDelete / Merge):
        // "Write operation with user timestamp must go through the transaction API instead of TransactionDB." and
        // FailIfBatchHasTs (WriteCommittedTxnDB::Write): "Writes with timestamp must go through transaction API instead
        // of TransactionDB." -- both NotSupported, nothing written.
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        o.CreateMissingColumnFamilies = true;
        using TransactionDatabase db = TransactionDatabase.Open(DbDir, o, new TransactionDbOptions { DefaultLockTimeout = s_short, TransactionLockTimeout = s_short },
            [
                new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, new ColumnFamilyOptions()),
                new ColumnFamilyDescriptor("ts", new ColumnFamilyOptions { Comparator = Comparator.BytewiseWithU64Ts }),
            ]);
        ColumnFamily cf = db.Database.GetColumnFamily("ts");
        Fails(StatusCode.NotSupported, () => db.Put(null, cf, B("k"), B("v")));
        Fails(StatusCode.NotSupported, () => db.Delete(null, cf, B("k")));
        Fails(StatusCode.NotSupported, () => db.SingleDelete(null, cf, B("k")));
        Fails(StatusCode.NotSupported, () => db.Merge(null, cf, B("k"), B("v")));
        Assert.AreEqual(StatusCode.NotSupported,
            Assert.ThrowsExactly<RocksDbException>(() => db.PutAsync(null, cf, B("k"), B("v")).AsTask().GetAwaiter().GetResult()).Code);
        using var batch = new WriteBatch();
        byte[] ts = new byte[8];
        batch.Put(cf, B("k"), ts, B("v"));
        Fails(StatusCode.NotSupported, () => db.Write(null, batch));
        Assert.IsNull(cf.Get(new ReadOptions { Timestamp = new byte[] { 255, 255, 255, 255, 255, 255, 255, 255 } }, B("k")));
        // Plain column families still work.
        db.Put(B("plain"), B("1"));
        Assert.AreEqual("1", GetString(db.Database, "plain"));
    }

    private static ulong NumSnapshots(RocksDatabase db)
    {
        Assert.IsTrue(db.TryGetIntProperty("rocksdb.num-snapshots", out ulong n));
        return n;
    }

    [TestMethod]
    public void TransactionSnapshotsAreReleasedOnceNothingRefersToThem()
    {
        // TransactionBaseImpl::SetSnapshotInternal: snapshot_ is a shared_ptr whose deleter is ReleaseSnapshot; save
        // points copy it (SetSavePoint) and RollbackToSavePoint restores it. ClearSnapshot = snapshot_.reset(). So a
        // snapshot is released when the transaction and its save points stop referring to it, not at destruction only.
        foreach (bool optimistic in new[] { false, true })
        {
            MemoryFileSystem fs = NewFs();
            using TransactionDatabase? pdb = optimistic ? null : OpenTxnDb(fs);
            using OptimisticTransactionDatabase? odb = optimistic ? OptimisticTransactionDatabase.Open(DbDir, NewOptions(fs)) : null;
            RocksDatabase db = optimistic ? odb!.Database : pdb!.Database;
            using Transaction t = optimistic ? odb!.BeginTransaction(s_wo) : pdb!.BeginTransaction(s_wo);
            for (int i = 0; i < 100; i++) t.SetSnapshot();
            Assert.AreEqual(1UL, NumSnapshots(db), "repeated SetSnapshot");
            t.ClearSnapshot();
            Assert.AreEqual(0UL, NumSnapshots(db), "ClearSnapshot");

            t.SetSnapshot();
            Snapshot s1 = t.Snapshot!;
            t.SetSavePoint();
            t.SetSnapshot();
            Assert.AreEqual(2UL, NumSnapshots(db), "the save point holds s1");
            Assert.IsTrue(t.RollbackToSavePoint());
            Assert.AreSame(s1, t.Snapshot);
            Assert.AreEqual(1UL, NumSnapshots(db), "s2 released by the rollback");
            t.SetSavePoint();
            t.ClearSnapshot();
            Assert.AreEqual(1UL, NumSnapshots(db), "still held by the save point");
            Assert.IsTrue(t.PopSavePoint());
            Assert.AreEqual(0UL, NumSnapshots(db), "PopSavePoint");

            t.SetSnapshot();
            t.SetSavePoint();
            t.ClearSnapshot();
            t.Put(B("k"), B("v"));
            t.Commit();
            Assert.AreEqual(0UL, NumSnapshots(db), "Commit clears the save points");
            t.SetSnapshot();
        }
    }

    [TestMethod]
    public void RecoveredPreparedSectionWithARecordTheRebuildCannotTakeFailsTheOpenWithInvalidArgument()
    {
        // PessimisticTransactionDB::Initialize -> TransactionBaseImpl::RebuildFromWriteBatch: the handler overrides
        // Put / PutEntity / Delete / SingleDelete / Merge only; a DeleteRange record falls back to
        // WriteBatch::Handler::DeleteRangeCF = InvalidArgument("DeleteRangeCF not implemented") and the open fails.
        MemoryFileSystem fs = NewFs();
        using (TransactionDatabase db = OpenTxnDb(fs))
        {
            Transaction t = db.BeginTransaction(new WriteOptions { Sync = true });
            t.SetName("dr");
            t.Put(B("a"), B("1"));
            t.WriteBatch.DeleteRange(B("b"), B("c"));
            t.Prepare();
        }
        RocksDbException e = Fails(StatusCode.InvalidArgument, () => OpenTxnDb(fs).Dispose());
        StringAssert.Contains(e.Message, "DeleteRangeCF not implemented");
    }

    // ---- optimistic -----------------------------------------------------------------------------------------------

    [TestMethod]
    public void OptimisticConflictKeepsTheTransactionAndRetryCommitsNothingTwice()
    {
        // OptimisticTransaction::CommitWithSerialValidate / CommitWithParallelValidate: a conflict is Busy and the
        // transaction is not cleared; Commit after Commit writes an empty batch (no state check upstream).
        foreach (OccValidationPolicy policy in new[] { OccValidationPolicy.ValidateSerial, OccValidationPolicy.ValidateParallel })
        {
            MemoryFileSystem fs = NewFs();
            using OptimisticTransactionDatabase db = OptimisticTransactionDatabase.Open(DbDir, NewOptions(fs),
                new OptimisticTransactionDbOptions { ValidatePolicy = policy }, [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, new ColumnFamilyOptions())]);
            using Transaction t = db.BeginTransaction(s_wo);
            t.Put(B("k"), B("t"));
            db.Database.Put(B("k"), B("other"));
            RocksDbException e = Fails(StatusCode.Busy, t.Commit);
            Assert.AreEqual(1, t.Count, policy.ToString());
            Assert.AreEqual("other", GetString(db.Database, "k"));
            t.Rollback();
            t.Commit();
            t.Commit();
            Assert.AreEqual("other", GetString(db.Database, "k"));
            Fails(StatusCode.InvalidArgument, t.Prepare);
            Fails(StatusCode.InvalidArgument, () => t.SetName("n"));
        }
    }

    // ---- WriteBatchWithIndex -----------------------------------------------------------------------------------

    [TestMethod]
    public void WbwiDeleteThenPutAndMergeChainsAcrossBatchAndDb()
    {
        // WriteBatchWithIndex::GetFromBatchAndDB (write_batch_with_index.cc, GetFromBatchAndDB / WBWIIteratorImpl::
        // FindLatestUpdate): the newest Put / Delete in the batch decides; merges newer than it stack on it; merges with
        // nothing below in the batch stack on the DB value.
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbDir, new DbOptions { CreateIfMissing = true, FileSystem = fs },
            [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, new ColumnFamilyOptions { MergeOperator = new ConcatMerge() })]);
        db.Put(B("a"), B("db"));
        db.Put(B("b"), B("db"));
        foreach (bool overwrite in new[] { false, true })
        {
            using var wb = new WriteBatchWithIndex(Comparator.Bytewise, 0, overwrite, 0);
            wb.Delete(B("a")).Put(B("a"), B("p"));
            Assert.AreEqual("p", S(wb.GetFromBatchAndDB(db, B("a"))));
            wb.Merge(B("b"), B("m1")).Merge(B("b"), B("m2"));
            Assert.AreEqual("db,m1,m2", S(wb.GetFromBatchAndDB(db, B("b"))));
            wb.Delete(B("b")).Merge(B("b"), B("m3"));
            Assert.AreEqual("m3", S(wb.GetFromBatchAndDB(db, B("b"))));
            wb.Put(B("a"), B("p2")).Delete(B("a"));
            Assert.IsNull(wb.GetFromBatchAndDB(db, B("a")));
            Assert.IsNull(wb.GetFromBatch(B("a")));
            wb.Merge(B("c"), B("x"));
            RocksDbException e = Fails(StatusCode.MergeInProgress, () => wb.GetFromBatch(null, B("c")));
            Assert.AreEqual("x", S(wb.GetFromBatchAndDB(db, B("c"))));
        }
    }

    private sealed class ConcatMerge : MergeOperator
    {
        public override string Name => "Concat";

        public override bool FullMerge(ReadOnlySpan<byte> key, bool hasExistingValue, ReadOnlySpan<byte> existingValue, in MergeOperands operands,
            System.Buffers.IBufferWriter<byte> result)
        {
            var sb = new System.Text.StringBuilder();
            if (hasExistingValue) sb.Append(System.Text.Encoding.UTF8.GetString(existingValue));
            for (int i = 0; i < operands.Count; i++)
            {
                if (sb.Length > 0) sb.Append(',');
                sb.Append(System.Text.Encoding.UTF8.GetString(operands[i]));
            }
            result.Write(System.Text.Encoding.UTF8.GetBytes(sb.ToString()));
            return true;
        }
    }

    [TestMethod]
    public void WbwiIteratorSeesLaterWritesAndSurvivesClearAndRollback()
    {
        // WBWIIteratorImpl over the skip list: an iterator stays valid while the batch grows (new entries become
        // visible, overwrite_key updates in place). Clear / RollbackToSavePoint rebuild the index; upstream the old
        // iterator is then undefined behaviour -- here it must at least not crash the process with an unexpected
        // exception type when re-positioned.
        using var wb = new WriteBatchWithIndex(Comparator.Bytewise, 0, true, 0);
        for (int i = 0; i < 100; i += 2) wb.Put(Key(i), Value(i, 1));
        WriteBatchWithIndexIterator it = wb.NewIterator();
        it.SeekToFirst();
        Assert.IsTrue(it.Valid);
        for (int i = 1; i < 100; i += 2) wb.Put(Key(i), Value(i, 1));
        wb.Put(Key(0), Value(0, 2));
        int n = 0;
        for (it.SeekToFirst(); it.Valid; it.Next())
        {
            CollectionAssert.AreEqual(Key(n), it.Key.ToArray());
            CollectionAssert.AreEqual(Value(n, n == 0 ? 2 : 1), it.Value.ToArray());
            n++;
        }
        Assert.AreEqual(100, n);

        wb.SetSavePoint();
        for (int i = 100; i < 300; i++) wb.Put(Key(i), Value(i, 1));
        it.Seek(Key(250));
        Assert.IsTrue(it.Valid);
        Assert.IsTrue(wb.RollbackToSavePoint());
        it.SeekToFirst();
        n = 0;
        for (; it.Valid; it.Next()) n++;
        Assert.AreEqual(100, n);
        it.Seek(Key(250));
        Assert.IsFalse(it.Valid);
        wb.Clear();
        it.SeekToFirst();
        Assert.IsFalse(it.Valid);
    }

    // ---- backup engine under faults -----------------------------------------------------------------------------

    private const string BackupDir = "/a7backup";

    [TestMethod]
    public void BackupFailingAtEveryMutationLeavesAnEngineThatOpensCleansUpAndBacksUpAgain()
    {
        // BackupEngineImpl::CreateNewBackupWithMetadata failure path + Initialize / GarbageCollect: whatever single
        // mutation of the backup directory fails (create, append, sync, rename, directory sync, delete), the failed
        // backup is not listed, earlier backups still verify and restore, and a new engine on the directory backs up
        // again; nothing of the failed backup is left after the next garbage collection.
        MemoryFileSystem dbFs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbDir, NewOptions(dbFs));
        for (int i = 0; i < 300; i++) db.Put(Key(i), Value(i, 1));
        db.Flush();
        for (int i = 200; i < 400; i++) db.Put(Key(i), Value(i, 2));

        // Count the mutations of one backup on a scratch copy.
        long opsPerBackup;
        {
            var probe = new FaultInjectionFileSystem(NewFs());
            using BackupEngine be = BackupEngine.Open(new BackupEngineOptions(BackupDir), probe);
            be.CreateNewBackup(db, flushBeforeBackup: false);
            long before = probe.Operations;
            be.CreateNewBackup(db, flushBeforeBackup: true);
            opsPerBackup = probe.Operations - before;
        }
        Assert.IsTrue(opsPerBackup > 5, opsPerBackup.ToString());

        for (long k = 1; k <= opsPerBackup + 2; k++)
        {
            var fs = new FaultInjectionFileSystem(NewFs());
            using (BackupEngine be = BackupEngine.Open(new BackupEngineOptions(BackupDir), fs)) be.CreateNewBackup(db, flushBeforeBackup: false);
            bool failed;
            using (BackupEngine be = BackupEngine.Open(new BackupEngineOptions(BackupDir), fs))
            {
                fs.TransientFailure = true;
                fs.CrashAtOperation = fs.Operations + k;
                try
                {
                    be.CreateNewBackup(db, flushBeforeBackup: true);
                    failed = false;
                }
                catch (RocksDbException)
                {
                    failed = true;
                }
            }
            string? at = fs.CrashedAt;
            fs.Restart();
            using (BackupEngine be = BackupEngine.Open(new BackupEngineOptions(BackupDir), fs))
            {
                List<BackupInfo> infos = be.GetBackupInfo();
                Assert.AreEqual(failed ? 1 : 2, infos.Count, "k=" + k + " at " + at);
                Assert.AreEqual(0, be.GetCorruptedBackups().Count, "k=" + k);
                foreach (BackupInfo bi in infos) be.VerifyBackup(bi.BackupId, verifyWithChecksum: true);
                be.GarbageCollect();
                uint id = be.CreateNewBackup(db, flushBeforeBackup: false);
                be.VerifyBackup(id, verifyWithChecksum: true);
                be.RestoreDbFromBackup(id, "/restored", "/restored", new RestoreOptions());
            }
            using (RocksDatabase r = RocksDatabase.Open("/restored", new DbOptions { FileSystem = fs }))
            {
                for (int i = 0; i < 400; i += 37) CollectionAssert.AreEqual(Value(i, i >= 200 ? 2 : 1), r.Get(Key(i)), "k=" + k + " key " + i);
            }
            // After the GC, every file under the backup directory belongs to a listed backup.
            foreach (string child in fs.Inner.GetChildren(BackupDir + "/private"))
            {
                Assert.IsFalse(child.EndsWith(".tmp", StringComparison.Ordinal), "k=" + k + " " + child);
            }
            Assert.IsFalse(Array.Exists(fs.Inner.GetChildren(BackupDir + "/meta").ToArray(), static c => c.StartsWith('.')), "k=" + k);
        }
    }

    [TestMethod]
    public void RestoreFromDamagedBackupFilesIsCorruptionNeverASilentlyDamagedDb()
    {
        // BackupEngineImpl::RestoreDBFromBackup / VerifyBackup: every restored file is checked against the crc32c of
        // the meta file; a truncated or bit-flipped file is Corruption (a missing one NotFound / Corruption), never a
        // restored DB with damaged content. Randomized (seed logged).
        int seed = Environment.TickCount;
        var rnd = new Random(seed);
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbDir, NewOptions(fs)))
        {
            for (int i = 0; i < 200; i++) db.Put(Key(i), Value(i, 1));
            db.Flush();
            for (int i = 100; i < 250; i++) db.Put(Key(i), Value(i, 2));
            using BackupEngine be = BackupEngine.Open(new BackupEngineOptions(BackupDir), fs);
            be.CreateNewBackup(db, flushBeforeBackup: false);
        }
        var files = new List<string>();
        foreach (string dir in new[] { BackupDir + "/shared_checksum", BackupDir + "/private/1" })
        {
            foreach (string c in fs.GetChildren(dir)) files.Add(dir + "/" + c);
        }
        for (int round = 0; round < 40; round++)
        {
            string file = files[rnd.Next(files.Count)];
            byte[] original = ReadAll(fs, file);
            byte[] damaged;
            int mode = rnd.Next(3);
            if (mode == 0 && original.Length > 0)
            {
                damaged = (byte[])original.Clone();
                damaged[rnd.Next(damaged.Length)] ^= (byte)(1 << rnd.Next(8));
            }
            else if (original.Length > 0)
            {
                damaged = original.AsSpan(0, rnd.Next(original.Length)).ToArray();
            }
            else
            {
                damaged = [1];
            }
            WriteAll(fs, file, damaged);
            string msg = "seed=" + seed + " round=" + round + " file=" + file + " mode=" + mode;
            try
            {
                using BackupEngineReadOnly ro = BackupEngineReadOnly.Open(new BackupEngineOptions(BackupDir), fs);
                RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => ro.VerifyBackup(1, verifyWithChecksum: true), msg);
                Assert.AreEqual(StatusCode.Corruption, e.Code, msg + " " + e.Message);
                e = Assert.ThrowsExactly<RocksDbException>(() => ro.RestoreDbFromBackup(1, "/r", "/r", new RestoreOptions()), msg);
                Assert.AreEqual(StatusCode.Corruption, e.Code, msg + " " + e.Message);
            }
            finally
            {
                WriteAll(fs, file, original);
            }
        }
        using (BackupEngineReadOnly ro = BackupEngineReadOnly.Open(new BackupEngineOptions(BackupDir), fs))
        {
            ro.VerifyBackup(1, verifyWithChecksum: true);
            ro.RestoreDbFromBackup(1, "/r", "/r", new RestoreOptions());
        }
        using RocksDatabase r = RocksDatabase.Open("/r", new DbOptions { FileSystem = fs });
        for (int i = 0; i < 250; i++) CollectionAssert.AreEqual(Value(i, i >= 100 ? 2 : 1), r.Get(Key(i)));
    }

    [TestMethod]
    public void RestoreFailingAtEveryMutationThenSucceeds()
    {
        // BackupEngineImpl::RestoreDBFromBackup: a restore interrupted by an I/O error at any mutation of the
        // destination fails with that error; the next restore into the same directory replaces whatever is there.
        MemoryFileSystem inner = NewFs();
        var fs = new FaultInjectionFileSystem(inner);
        using (RocksDatabase db = RocksDatabase.Open(DbDir, NewOptions(fs)))
        {
            for (int i = 0; i < 100; i++) db.Put(Key(i), Value(i, 1));
            db.Flush();
            for (int i = 50; i < 150; i++) db.Put(Key(i), Value(i, 2));
            using BackupEngine be = BackupEngine.Open(new BackupEngineOptions(BackupDir), fs);
            be.CreateNewBackup(db, flushBeforeBackup: false);
        }
        long start = fs.Operations;
        using (BackupEngineReadOnly ro = BackupEngineReadOnly.Open(new BackupEngineOptions(BackupDir), fs))
        {
            ro.RestoreDbFromBackup(1, "/probe", "/probe", new RestoreOptions());
        }
        long opsPerRestore = fs.Operations - start;
        var swallowed = new List<string>();
        for (long k = 1; k <= opsPerRestore; k++)
        {
            using BackupEngineReadOnly ro = BackupEngineReadOnly.Open(new BackupEngineOptions(BackupDir), fs);
            fs.TransientFailure = true;
            fs.CrashAtOperation = fs.Operations + k;
            try
            {
                ro.RestoreDbFromBackup(1, "/r", "/r", new RestoreOptions());
                if (fs.CrashedAt is { } at) swallowed.Add("k=" + k + " " + at);
            }
            catch (RocksDbException)
            {
            }
            fs.Restart();
            ro.RestoreDbFromBackup(1, "/r", "/r", new RestoreOptions());
            using RocksDatabase r = RocksDatabase.Open("/r", new DbOptions { FileSystem = fs });
            for (int i = 0; i < 150; i += 7) CollectionAssert.AreEqual(Value(i, i >= 50 ? 2 : 1), r.Get(Key(i)), "k=" + k);
        }
        // Upstream ignores these errors too (RestoreDBFromBackup: CreateDirIfMissing(...).PermitUncheckedError();
        // DeleteChildren: DeleteFile(...).PermitUncheckedError()); nothing else may be swallowed.
        foreach (string s in swallowed) Assert.IsTrue(s.Contains(" mkdir ", StringComparison.Ordinal) || s.Contains(" delete ", StringComparison.Ordinal), s);
    }

    [TestMethod]
    public void RestoreWhoseDeletionOfAStaleWalFailsReplaysItLikeNative()
    {
        // BackupEngineImpl::DeleteChildren (backup_engine.cc): "db_fs_->DeleteFile(dir + "/" + f, io_options_,
        // nullptr).PermitUncheckedError();  // ignore errors". When deleting a WAL left in the destination fails, the
        // restore reports success; this pins what the next open then does with that WAL.
        MemoryFileSystem inner = NewFs();
        var fs = new FaultInjectionFileSystem(inner);
        using (RocksDatabase db = RocksDatabase.Open(DbDir, NewOptions(fs)))
        {
            for (int i = 0; i < 50; i++) db.Put(Key(i), Value(i, 1));
            using BackupEngine be = BackupEngine.Open(new BackupEngineOptions(BackupDir), fs);
            be.CreateNewBackup(db, flushBeforeBackup: true);
        }
        // The destination holds another DB with unflushed writes in a WAL numbered above the backup's files.
        using (RocksDatabase other = RocksDatabase.Open("/r", NewOptions(fs)))
        {
            for (int i = 0; i < 20; i++)
            {
                other.Put(B("filler"), B(i.ToString(System.Globalization.CultureInfo.InvariantCulture)));
                other.Flush();
            }
            other.Put(Key(0), B("stale"));
            other.Put(B("only-in-stale-wal"), B("x"));
        }
        fs.FailMatching(".log", FaultOps.Delete, 10, InjectedFaultKind.IOError);
        using (BackupEngineReadOnly ro = BackupEngineReadOnly.Open(new BackupEngineOptions(BackupDir), fs))
        {
            ro.RestoreDbFromBackup(1, "/r", "/r", new RestoreOptions());
        }
        Assert.IsTrue(fs.TargetedFaults > 0);
        fs.ClearFailMatching();
        using RocksDatabase r = RocksDatabase.Open("/r", new DbOptions { FileSystem = fs });
        // Upstream behaviour (native 11.12 does the same, CompatTests Audit7UtilitiesFailuresCompatTests.RestoreOverAnUndeletableStaleWal...):
        // the stale WAL is replayed over the restored data.
        Assert.AreEqual("x", S(r.Get(B("only-in-stale-wal"))));
        Assert.AreEqual("stale", S(r.Get(Key(0))));
    }

    // ---- checkpoint --------------------------------------------------------------------------------------------

    [TestMethod]
    public void CheckpointFailingAtEveryMutationLeavesNoTargetAndSucceedsAfterwards()
    {
        // CheckpointImpl::CreateCheckpoint: on failure "we have to delete the dir and all its children" of the staging
        // directory; the target directory never appears half-filled; an existing target is InvalidArgument.
        long ops = long.MaxValue;
        var swallowed = new List<string>();
        for (long k = 0; k <= ops + 1; k++)
        {
            MemoryFileSystem inner = NewFs();
            var fs = new FaultInjectionFileSystem(inner);
            RocksDatabase db = RocksDatabase.Open(DbDir, NewOptions(fs));
            for (int i = 0; i < 200; i++) db.Put(Key(i), Value(i, 1));
            db.Flush();
            for (int i = 100; i < 300; i++) db.Put(Key(i), Value(i, 2));
            long start = fs.Operations;
            fs.TransientFailure = true;
            if (k > 0) fs.CrashAtOperation = start + k;
            bool ok;
            try
            {
                db.CreateCheckpoint("/cp");
                ok = true;
            }
            catch (RocksDbException)
            {
                ok = false;
            }
            if (k == 0)
            {
                ops = fs.Operations - start;
                Fails(StatusCode.InvalidArgument, () => db.CreateCheckpoint("/cp"));
            }
            string? at = fs.CrashedAt;
            fs.Restart();
            if (ok && at is not null) swallowed.Add("k=" + k + " " + at);
            if (!ok)
            {
                Assert.IsFalse(inner.DirectoryExists("/cp"), "k=" + k + " at " + at + ": target exists after a failed checkpoint");
                Assert.IsFalse(inner.DirectoryExists("/cp.tmp") && new List<string>(inner.GetChildren("/cp.tmp")).Count > 0, "k=" + k + " at " + at);
            }
            db.Dispose();
            db = RocksDatabase.Open(DbDir, NewOptions(fs));
            if (!inner.DirectoryExists("/cp")) db.CreateCheckpoint("/cp");
            db.Dispose();
            using RocksDatabase r = RocksDatabase.Open("/cp", new DbOptions { FileSystem = fs });
            for (int i = 0; i < 300; i += 13) CollectionAssert.AreEqual(Value(i, i >= 100 ? 2 : 1), r.Get(Key(i)), "k=" + k + " at " + at);
        }
        Assert.AreEqual(0, swallowed.Count, string.Join("; ", swallowed));
    }

    private static void DeleteTree(MemoryFileSystem fs, string dir)
    {
        if (!fs.DirectoryExists(dir)) return;
        foreach (string c in fs.GetChildren(dir))
        {
            string p = dir + "/" + c;
            if (fs.DirectoryExists(p)) DeleteTree(fs, p);
            else fs.DeleteFile(p);
        }
        fs.DeleteDirectory(dir);
    }

    private static byte[] ReadAll(IFileSystem fs, string path)
    {
        var data = new byte[fs.GetFileSize(path)];
        using ISequentialFile f = fs.OpenSequential(path);
        int read = 0;
        while (read < data.Length) read += f.Read(data.AsSpan(read));
        return data;
    }

    private static void WriteAll(IFileSystem fs, string path, byte[] data)
    {
        using IWritableFile w = fs.CreateWritable(path, truncate: true);
        w.Append(data);
        w.Sync();
    }
}
