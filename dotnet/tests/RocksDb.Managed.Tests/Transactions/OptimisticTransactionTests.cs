using RocksDb.Managed.Env;
using RocksDb.Managed.Tests.Db;
using RocksDb.Managed.Utilities.Transactions;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Transactions;

/// <summary>Ports of utilities/transactions/optimistic_transaction_test.cc (both validation policies).</summary>
[TestClass]
public sealed class OptimisticTransactionTests
{
    private static readonly WriteOptions s_wo = new();
    private static readonly ReadOptions s_ro = new();

    private static OptimisticTransactionDatabase Open(MemoryFileSystem fs, OccValidationPolicy policy, ColumnFamilyOptions? cfo = null) =>
        OptimisticTransactionDatabase.Open(DbPath, NewOptions(fs), new OptimisticTransactionDbOptions { ValidatePolicy = policy, OccLockBuckets = 64 },
            [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, cfo ?? new ColumnFamilyOptions())]);

    private static string? TxnGet(Transaction txn, string key) => S(txn.Get(B(key)));

    private static void AssertFails(StatusCode code, Action a)
    {
        var e = Assert.ThrowsExactly<RocksDbException>(a);
        Assert.AreEqual(code, e.Code, e.Message);
    }

    [TestMethod]
    [DataRow(OccValidationPolicy.ValidateSerial)]
    [DataRow(OccValidationPolicy.ValidateParallel)]
    public void SuccessTest(OccValidationPolicy policy)
    {
        using OptimisticTransactionDatabase db = Open(NewFs(), policy);
        db.Database.Put(B("foo"), B("bar"));
        db.Database.Put(B("foo2"), B("bar"));
        using Transaction txn = db.BeginTransaction(s_wo);
        Assert.AreEqual("bar", TxnGet(txn, "foo"));
        txn.Put(B("foo"), B("bar2"));
        Assert.AreEqual("bar2", TxnGet(txn, "foo"));
        Assert.AreEqual("bar", GetString(db.Database, "foo")); // not visible before commit
        txn.Commit();
        Assert.AreEqual(TransactionState.Committed, txn.State);
        Assert.AreEqual("bar2", GetString(db.Database, "foo"));
    }

    [TestMethod]
    [DataRow(OccValidationPolicy.ValidateSerial)]
    [DataRow(OccValidationPolicy.ValidateParallel)]
    public void WriteConflictTest(OccValidationPolicy policy)
    {
        using OptimisticTransactionDatabase db = Open(NewFs(), policy);
        db.Database.Put(B("foo"), B("bar"));
        db.Database.Put(B("foo2"), B("bar"));
        using Transaction txn = db.BeginTransaction(s_wo);
        txn.Put(B("foo"), B("bar2"));
        // This Put outside of a transaction will conflict with the previous write
        db.Database.Put(B("foo"), B("barz"));
        Assert.AreEqual("barz", GetString(db.Database, "foo"));
        Assert.AreEqual(1, txn.Count);
        AssertFails(StatusCode.Busy, txn.Commit); // Txn should not commit
        // Verify that transaction did not write anything
        Assert.AreEqual("barz", GetString(db.Database, "foo"));
        Assert.AreEqual("bar", GetString(db.Database, "foo2"));
    }

    [TestMethod]
    [DataRow(OccValidationPolicy.ValidateSerial)]
    [DataRow(OccValidationPolicy.ValidateParallel)]
    public void WriteConflictTest2(OccValidationPolicy policy)
    {
        using OptimisticTransactionDatabase db = Open(NewFs(), policy);
        db.Database.Put(B("foo"), B("bar"));
        using Transaction txn = db.BeginTransaction(s_wo, new OptimisticTransactionOptions { SetSnapshot = true });
        // This Put outside of a transaction will conflict with a later write
        db.Database.Put(B("foo"), B("barz"));
        txn.Put(B("foo"), B("bar2")); // Conflicts with write done after snapshot taken
        AssertFails(StatusCode.Busy, txn.Commit);
        Assert.AreEqual("barz", GetString(db.Database, "foo"));
    }

    [TestMethod]
    [DataRow(OccValidationPolicy.ValidateSerial)]
    [DataRow(OccValidationPolicy.ValidateParallel)]
    public void ReadConflictTest(OccValidationPolicy policy)
    {
        using OptimisticTransactionDatabase db = Open(NewFs(), policy);
        db.Database.Put(B("foo"), B("bar"));
        db.Database.Put(B("foo2"), B("bar"));
        using Transaction txn = db.BeginTransaction(s_wo, new OptimisticTransactionOptions { SetSnapshot = true });
        var ro = new ReadOptions { Snapshot = txn.Snapshot };
        Assert.AreEqual("bar", S(txn.GetForUpdate(ro, null!, B("foo"), exclusive: true)));
        // This Put outside of a transaction will conflict with the previous read
        db.Database.Put(B("foo"), B("barz"));
        Assert.AreEqual("barz", GetString(db.Database, "foo"));
        AssertFails(StatusCode.Busy, txn.Commit);
        Assert.AreEqual("barz", GetString(db.Database, "foo"));
    }

    [TestMethod]
    [DataRow(OccValidationPolicy.ValidateSerial)]
    [DataRow(OccValidationPolicy.ValidateParallel)]
    public void TxnOnlyAndNoConflictOnUntrackedRead(OccValidationPolicy policy)
    {
        using OptimisticTransactionDatabase db = Open(NewFs(), policy);
        db.Database.Put(B("foo"), B("bar"));
        using Transaction txn = db.BeginTransaction(s_wo, new OptimisticTransactionOptions { SetSnapshot = true });
        Assert.AreEqual("bar", TxnGet(txn, "foo")); // plain Get is not tracked
        db.Database.Put(B("foo"), B("barz"));
        txn.Put(B("x"), B("y"));
        txn.Commit();
        Assert.AreEqual("y", GetString(db.Database, "x"));
    }

    [TestMethod]
    [DataRow(OccValidationPolicy.ValidateSerial)]
    [DataRow(OccValidationPolicy.ValidateParallel)]
    public void FlushTest(OccValidationPolicy policy)
    {
        // A conflicting write that was flushed is still detected through the memtable history.
        using OptimisticTransactionDatabase db = Open(NewFs(), policy);
        db.Database.Put(B("foo"), B("bar"));
        using Transaction txn = db.BeginTransaction(s_wo, new OptimisticTransactionOptions { SetSnapshot = true });
        var ro = new ReadOptions { Snapshot = txn.Snapshot };
        Assert.AreEqual("bar", S(txn.GetForUpdate(ro, null!, B("foo"), exclusive: true)));
        txn.Put(B("foo"), B("bar2"));
        db.Database.Put(B("foo"), B("barz"));
        db.Database.Flush(); // flush the conflicting write out of the memtable
        AssertFails(StatusCode.Busy, txn.Commit);
        Assert.AreEqual("barz", GetString(db.Database, "foo"));
    }

    [TestMethod]
    [DataRow(OccValidationPolicy.ValidateSerial)]
    [DataRow(OccValidationPolicy.ValidateParallel)]
    public void FlushTest2NotEnoughHistoryIsTryAgain(OccValidationPolicy policy)
    {
        // With (almost) no memtable history the check cannot be done from memory: TryAgain.
        using OptimisticTransactionDatabase db = Open(NewFs(), policy, new ColumnFamilyOptions { MaxWriteBufferSizeToMaintain = 1 });
        db.Database.Put(B("foo"), B("bar"));
        using Transaction txn = db.BeginTransaction(s_wo, new OptimisticTransactionOptions { SetSnapshot = true });
        txn.Put(B("foo"), B("bar2"));
        db.Database.Put(B("other"), B("x"));
        db.Database.Flush();
        db.Database.Put(B("other"), B("y"));
        AssertFails(StatusCode.TryAgain, txn.Commit);
        Assert.AreEqual("bar", GetString(db.Database, "foo"));
    }

    [TestMethod]
    [DataRow(OccValidationPolicy.ValidateSerial)]
    [DataRow(OccValidationPolicy.ValidateParallel)]
    public void NoSnapshotTest(OccValidationPolicy policy)
    {
        using OptimisticTransactionDatabase db = Open(NewFs(), policy);
        db.Database.Put(B("AAA"), B("bar"));
        using Transaction txn = db.BeginTransaction(s_wo);
        // Modify key after transaction start
        db.Database.Put(B("AAA"), B("bar1"));
        // Read and write without a snapshot: tracked at the current sequence
        Assert.AreEqual("bar1", S(txn.GetForUpdate(B("AAA"))));
        txn.Put(B("AAA"), B("bar2"));
        txn.Commit();
        Assert.AreEqual("bar2", GetString(db.Database, "AAA"));
    }

    [TestMethod]
    [DataRow(OccValidationPolicy.ValidateSerial)]
    [DataRow(OccValidationPolicy.ValidateParallel)]
    public void MultipleSnapshotTest(OccValidationPolicy policy)
    {
        using OptimisticTransactionDatabase db = Open(NewFs(), policy);
        db.Database.Put(B("AAA"), B("bar"));
        db.Database.Put(B("BBB"), B("bar"));
        db.Database.Put(B("CCC"), B("bar"));
        using Transaction txn = db.BeginTransaction(s_wo);
        db.Database.Put(B("AAA"), B("bar1"));
        // Read and write without a snapshot
        Assert.AreEqual("bar1", S(txn.GetForUpdate(B("AAA"))));
        txn.Put(B("AAA"), B("bar2"));
        // Modify BBB before snapshot is taken
        db.Database.Put(B("BBB"), B("bar1"));
        txn.SetSnapshot();
        var ro = new ReadOptions { Snapshot = txn.Snapshot };
        // Read and write with snapshot
        Assert.AreEqual("bar1", S(txn.GetForUpdate(ro, null!, B("BBB"), exclusive: true)));
        txn.Put(B("BBB"), B("bar2"));
        db.Database.Put(B("CCC"), B("bar1"));
        // Set a new snapshot
        txn.SetSnapshot();
        ro.Snapshot = txn.Snapshot;
        // Read and write with snapshot
        Assert.AreEqual("bar1", S(txn.GetForUpdate(ro, null!, B("CCC"), exclusive: true)));
        txn.Put(B("CCC"), B("bar2"));
        txn.Commit();
        Assert.AreEqual("bar2", GetString(db.Database, "AAA"));
        Assert.AreEqual("bar2", GetString(db.Database, "BBB"));
        Assert.AreEqual("bar2", GetString(db.Database, "CCC"));

        // A second transaction whose snapshot predates a write conflicts
        using Transaction txn2 = db.BeginTransaction(s_wo);
        txn2.SetSnapshot();
        db.Database.Put(B("AAA"), B("bar3"));
        txn2.Put(B("AAA"), B("bar4"));
        AssertFails(StatusCode.Busy, txn2.Commit);
    }

    [TestMethod]
    [DataRow(OccValidationPolicy.ValidateSerial)]
    [DataRow(OccValidationPolicy.ValidateParallel)]
    public void ColumnFamiliesTest(OccValidationPolicy policy)
    {
        MemoryFileSystem fs = NewFs();
        using OptimisticTransactionDatabase db = Open(fs, policy);
        ColumnFamily cfa = db.Database.CreateColumnFamily("CFA", new ColumnFamilyOptions());
        ColumnFamily cfb = db.Database.CreateColumnFamily("CFB", new ColumnFamilyOptions());
        using Transaction txn1 = db.BeginTransaction(s_wo, new OptimisticTransactionOptions { SetSnapshot = true });
        using Transaction txn2 = db.BeginTransaction(s_wo, new OptimisticTransactionOptions { SetSnapshot = true });
        txn1.Put(cfa, B("AAA"), B("bar"));
        txn1.Put(cfb, B("AAA"), B("bar"));
        txn1.Put(B("AAA"), B("bar"));
        txn2.Put(cfb, B("AAA"), B("bar2")); // conflicts with txn1 on CFB only
        txn2.Put(cfa, B("BBB"), B("bar2"));
        txn1.Commit();
        AssertFails(StatusCode.Busy, txn2.Commit);
        Assert.AreEqual("bar", GetString(cfa, "AAA"));
        Assert.AreEqual("bar", GetString(cfb, "AAA"));
        Assert.IsNull(GetString(cfa, "BBB"));
        Assert.AreEqual("bar", S(txn1.Get(null, cfa, B("AAA"))));
    }

    [TestMethod]
    [DataRow(OccValidationPolicy.ValidateSerial)]
    [DataRow(OccValidationPolicy.ValidateParallel)]
    public void SavePointAndUndoGetForUpdate(OccValidationPolicy policy)
    {
        using OptimisticTransactionDatabase db = Open(NewFs(), policy);
        db.Database.Put(B("A"), B("a"));
        db.Database.Put(B("B"), B("b"));
        using Transaction txn = db.BeginTransaction(s_wo);
        Assert.IsFalse(txn.RollbackToSavePoint());
        txn.Put(B("A"), B("a1"));
        txn.SetSavePoint();
        txn.Put(B("A"), B("a2"));
        txn.Delete(B("B"));
        txn.GetForUpdate(B("C"));
        Assert.AreEqual("a2", TxnGet(txn, "A"));
        Assert.IsNull(TxnGet(txn, "B"));
        Assert.IsTrue(txn.RollbackToSavePoint());
        Assert.AreEqual("a1", TxnGet(txn, "A"));
        Assert.AreEqual("b", TxnGet(txn, "B"));
        // C was tracked only after the save point: untracked again, so outside writes to it don't conflict
        db.Database.Put(B("C"), B("c"));
        // GetForUpdate + UndoGetForUpdate: no conflict either
        txn.GetForUpdate(B("B"));
        txn.UndoGetForUpdate(B("B"));
        db.Database.Put(B("B"), B("b2"));
        txn.Commit();
        Assert.AreEqual("a1", GetString(db.Database, "A"));
        Assert.AreEqual("b2", GetString(db.Database, "B"));
    }

    [TestMethod]
    public void IteratorMergesBatchAndDb()
    {
        using OptimisticTransactionDatabase db = Open(NewFs(), OccValidationPolicy.ValidateSerial);
        foreach (string k in new[] { "a", "c", "e", "g" }) db.Database.Put(B(k), B("db-" + k));
        db.Database.Flush();
        db.Database.Put(B("e"), B("db-e2"));
        using Transaction txn = db.BeginTransaction(s_wo);
        txn.Put(B("b"), B("tx-b"));
        txn.Put(B("c"), B("tx-c"));
        txn.Delete(B("e"));
        txn.Put(B("h"), B("tx-h"));
        using TransactionIterator it = txn.NewIterator(s_ro);
        var forward = new List<string>();
        for (it.SeekToFirst(); it.Valid; it.Next()) forward.Add(S(it.Key.ToArray()) + "=" + S(it.Value.ToArray()));
        CollectionAssert.AreEqual(new[] { "a=db-a", "b=tx-b", "c=tx-c", "g=db-g", "h=tx-h" }, forward);
        var backward = new List<string>();
        for (it.SeekToLast(); it.Valid; it.Prev()) backward.Add(S(it.Key.ToArray())!);
        CollectionAssert.AreEqual(new[] { "h", "g", "c", "b", "a" }, backward);
        it.Seek(B("d"));
        Assert.AreEqual("g", S(it.Key.ToArray()));
        it.Prev();
        Assert.AreEqual("c", S(it.Key.ToArray()));
        it.Next();
        Assert.AreEqual("g", S(it.Key.ToArray()));
        it.SeekForPrev(B("f"));
        Assert.AreEqual("c", S(it.Key.ToArray()));
        // Writes made while the iterator is open become visible at the next positioning call
        txn.Put(B("d"), B("tx-d"));
        it.Next();
        Assert.AreEqual("d", S(it.Key.ToArray()));
    }

    [TestMethod]
    public void IteratorWithBounds()
    {
        using OptimisticTransactionDatabase db = Open(NewFs(), OccValidationPolicy.ValidateSerial);
        foreach (string k in new[] { "a", "c", "e" }) db.Database.Put(B(k), B(k));
        using Transaction txn = db.BeginTransaction(s_wo);
        txn.Put(B("b"), B("b"));
        txn.Put(B("d"), B("d"));
        txn.Put(B("f"), B("f"));
        using TransactionIterator it = txn.NewIterator(new ReadOptions { IterateLowerBound = B("b"), IterateUpperBound = B("e") });
        var keys = new List<string>();
        for (it.SeekToFirst(); it.Valid; it.Next()) keys.Add(S(it.Key.ToArray())!);
        CollectionAssert.AreEqual(new[] { "b", "c", "d" }, keys);
        keys.Clear();
        for (it.SeekToLast(); it.Valid; it.Prev()) keys.Add(S(it.Key.ToArray())!);
        CollectionAssert.AreEqual(new[] { "d", "c", "b" }, keys);
    }

    [TestMethod]
    public void MergeInTransaction()
    {
        var cfo = new ColumnFamilyOptions { MergeOperator = new StringAppendMergeOperator() };
        using OptimisticTransactionDatabase db = Open(NewFs(), OccValidationPolicy.ValidateSerial, cfo);
        db.Database.Put(B("k"), B("base"));
        using Transaction txn = db.BeginTransaction(s_wo);
        txn.Merge(B("k"), B("m1"));
        txn.Merge(B("k"), B("m2"));
        txn.Merge(B("n"), B("only"));
        Assert.AreEqual("base,m1,m2", TxnGet(txn, "k"));
        Assert.AreEqual("only", TxnGet(txn, "n"));
        using (TransactionIterator it = txn.NewIterator(s_ro))
        {
            it.Seek(B("k"));
            Assert.AreEqual("base,m1,m2", S(it.Value.ToArray()));
        }
        txn.Put(B("k"), B("p"));
        txn.Merge(B("k"), B("m3"));
        Assert.AreEqual("p,m3", TxnGet(txn, "k"));
        txn.Commit();
        Assert.AreEqual("p,m3", GetString(db.Database, "k"));
        Assert.AreEqual("only", GetString(db.Database, "n"));
    }

    [TestMethod]
    [DataRow(OccValidationPolicy.ValidateSerial)]
    [DataRow(OccValidationPolicy.ValidateParallel)]
    public async Task ConcurrentIncrementsAreSerializable(OccValidationPolicy policy)
    {
        using OptimisticTransactionDatabase db = Open(NewFs(), policy);
        db.Database.Put(B("counter"), BitConverter.GetBytes(0L));
        const int Workers = 8, PerWorker = 40;
        int conflicts = 0;
        var tasks = new Task[Workers];
        for (int w = 0; w < Workers; w++)
        {
            tasks[w] = Task.Run(async () =>
            {
                for (int i = 0; i < PerWorker; i++)
                {
                    while (true)
                    {
                        using Transaction txn = db.BeginTransaction(s_wo, new OptimisticTransactionOptions { SetSnapshot = true });
                        byte[] v = (await txn.GetForUpdateAsync(B("counter")))!;
                        await txn.PutAsync(B("counter"), BitConverter.GetBytes(BitConverter.ToInt64(v) + 1));
                        try
                        {
                            await txn.CommitAsync();
                            break;
                        }
                        catch (RocksDbException e) when (e.Code is StatusCode.Busy or StatusCode.TryAgain)
                        {
                            Interlocked.Increment(ref conflicts);
                        }
                    }
                }
            });
        }
        await Task.WhenAll(tasks);
        Assert.AreEqual((long)Workers * PerWorker, BitConverter.ToInt64(db.Database.Get(B("counter"))!));
    }

    [TestMethod]
    public void ReuseTransactionObject()
    {
        using OptimisticTransactionDatabase db = Open(NewFs(), OccValidationPolicy.ValidateParallel);
        Transaction txn = db.BeginTransaction(s_wo);
        txn.Put(B("a"), B("1"));
        txn.Commit();
        Transaction again = db.BeginTransaction(s_wo, new OptimisticTransactionOptions(), txn);
        Assert.AreSame(txn, again);
        Assert.AreEqual(TransactionState.Started, again.State);
        Assert.AreEqual(0, again.Count);
        again.Put(B("b"), B("2"));
        again.Commit();
        again.Dispose();
        Assert.AreEqual("1", GetString(db.Database, "a"));
        Assert.AreEqual("2", GetString(db.Database, "b"));
        using Transaction named = db.BeginTransaction(s_wo);
        AssertFails(StatusCode.InvalidArgument, () => named.SetName("x")); // Optimistic transactions cannot be named.
        AssertFails(StatusCode.InvalidArgument, named.Prepare);
    }
}
