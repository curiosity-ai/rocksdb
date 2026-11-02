using RocksDb.Managed.Env;
using RocksDb.Managed.Utilities.Transactions;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Transactions;

/// <summary>
/// Ports of key cases of utilities/transactions/write_unprepared_transaction_test.cc: unprepared batches written once
/// the batch exceeds write_batch_flush_threshold, read-your-own-writes over them (Get and iterators), commit / rollback
/// of unprepared data, save points across flushes, untracked keys, and recovery after a crash with unprepared and
/// prepared transactions.
/// </summary>
[TestClass]
public sealed class WriteUnpreparedTransactionTests
{
    private static readonly WriteOptions s_wo = new();
    private static readonly WriteOptions s_sync = new() { Sync = true };

    private static TransactionDbOptions WupOptions() => new()
    {
        WritePolicy = TransactionWritePolicy.WriteUnprepared,
        WpCommitCacheBits = 10,
        TransactionLockTimeout = TimeSpan.FromMilliseconds(50),
        DefaultLockTimeout = TimeSpan.FromMilliseconds(50),
    };

    private static TransactionDatabase Open(MemoryFileSystem fs) =>
        TransactionDatabase.Open(DbPath, NewOptions(fs), WupOptions(),
            [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, new ColumnFamilyOptions { DisableAutoCompactions = true })]);

    private static WriteUnpreparedTxn Impl(Transaction t) => (WriteUnpreparedTxn)t;

    private static string Get(TransactionDatabase db, string key) => S(db.Database.Get(B(key))) ?? "NOT_FOUND";

    private static string TxnGet(Transaction t, string key) => S(t.Get(B(key))) ?? "NOT_FOUND";

    private static void Crash(MemoryFileSystem fs, TransactionDatabase db)
    {
        db.Database.WaitForCompact();
        fs.SimulateCrash();
    }

    private static string Scan(Iterator it)
    {
        using (it)
        {
            var parts = new List<string>();
            for (it.SeekToFirst(); it.Valid; it.Next()) parts.Add(S(it.Key.ToArray()) + "=" + S(it.Value.ToArray()));
            return string.Join(",", parts);
        }
    }

    [TestMethod]
    public void UnpreparedBatch()
    {
        // UnpreparedBatch: with a threshold of 1 every write after the first flushes the batch; nothing is visible to
        // other readers until commit; rollback removes everything.
        const int kNumKeys = 10;
        foreach (long batchSize in new long[] { 1, 1_000_000 })
        {
            foreach (bool prepare in new[] { false, true })
            {
                foreach (bool commit in new[] { false, true })
                {
                    using TransactionDatabase db = Open(NewFs());
                    using Transaction txn = db.BeginTransaction(s_wo, new TransactionOptions { WriteBatchFlushThreshold = batchSize });
                    txn.SetName("xid");
                    for (int i = 0; i < kNumKeys; i++)
                    {
                        txn.Put(B("k" + i), B("v" + i));
                        // No flush at the first write (the batch is still empty), so after k puts there are k-1 flushed batches.
                        Assert.AreEqual(batchSize == 1 ? i : 0, Impl(txn).UnpreparedSequenceNumbers.Count);
                    }
                    if (prepare) txn.Prepare();
                    Assert.AreEqual("", Scan(db.Database.NewIterator()));
                    if (commit) txn.Commit();
                    else txn.Rollback();
                    var expected = new List<string>();
                    if (commit) for (int i = 0; i < kNumKeys; i++) expected.Add("k" + i + "=v" + i);
                    Assert.AreEqual(string.Join(",", expected), Scan(db.Database.NewIterator()));
                }
            }
        }
    }

    [TestMethod]
    public void ReadYourOwnWrite()
    {
        // ReadYourOwnWrite: reads through the transaction see its unprepared batches (and its batch), other readers do not.
        using TransactionDatabase db = Open(NewFs());
        db.Put(B("a"), B("a0"));
        db.Put(B("b"), B("b0"));
        using Transaction txn = db.BeginTransaction(s_wo, new TransactionOptions { WriteBatchFlushThreshold = 1 });
        txn.SetName("own");
        txn.Put(B("a"), B("a1"));
        txn.Delete(B("b"));    // flushes the batch with a1
        txn.Put(B("c"), B("c1")); // flushes the batch with the delete of b
        Assert.AreEqual(2, Impl(txn).UnpreparedSequenceNumbers.Count);
        Assert.AreEqual("a1", TxnGet(txn, "a"));
        Assert.AreEqual("NOT_FOUND", TxnGet(txn, "b"));
        Assert.AreEqual("c1", TxnGet(txn, "c"));
        Assert.AreEqual("a0", Get(db, "a"));
        Assert.AreEqual("b0", Get(db, "b"));
        Assert.AreEqual("NOT_FOUND", Get(db, "c"));
        // Iterators over the transaction see the unprepared data too.
        using (TransactionIterator it = txn.NewIterator(new ReadOptions()))
        {
            var seen = new List<string>();
            for (it.SeekToFirst(); it.Valid; it.Next()) seen.Add(S(it.Key.ToArray()) + "=" + S(it.Value.ToArray()));
            Assert.AreEqual("a=a1,c=c1", string.Join(",", seen));
            var back = new List<string>();
            for (it.SeekToLast(); it.Valid; it.Prev()) back.Add(S(it.Key.ToArray())!);
            CollectionAssert.AreEqual(new[] { "c", "a" }, back);
        }
        Assert.AreEqual("a=a0,b=b0", Scan(db.Database.NewIterator()));
        // With a snapshot (taken by the transaction) the own writes are still visible.
        txn.SetSnapshot();
        Assert.AreEqual("a1", S(txn.Get(new ReadOptions { Snapshot = txn.Snapshot }, null!, B("a"))));
        txn.Commit();
        Assert.AreEqual("a=a1,c=c1", Scan(db.Database.NewIterator()));
    }

    [TestMethod]
    public void IterateAndWrite()
    {
        // IterateAndWrite: writes while a transaction iterator is open are not flushed (the iterator stays valid).
        foreach (bool delete in new[] { true, false })
        {
            using TransactionDatabase db = Open(NewFs());
            for (int i = 0; i < 100; i++) db.Put(B(i.ToString("D3")), B(i.ToString("D3")));
            using Transaction txn = db.BeginTransaction(s_wo, new TransactionOptions { WriteBatchFlushThreshold = 1 });
            txn.SetName("iw");
            txn.Put(B("009"), B("a"));
            using (TransactionIterator it = txn.NewIterator(new ReadOptions()))
            {
                for (it.SeekToFirst(); it.Valid; it.Next())
                {
                    string k = S(it.Key.ToArray())!;
                    Assert.AreEqual(k == "009" ? "a" : k, S(it.Value.ToArray()));
                    if (delete) txn.Delete(it.Key);
                    else txn.Put(it.Key, B("b"));
                }
            }
            txn.Commit();
            using Iterator check = db.Database.NewIterator();
            int keys = 0;
            for (check.SeekToFirst(); check.Valid; check.Next(), keys++) Assert.AreEqual("b", S(check.Value.ToArray()));
            Assert.AreEqual(delete ? 0 : 100, keys);
        }
    }

    [TestMethod]
    public void SavePointAcrossFlushes()
    {
        // Save points taken before a flush are rolled back by writing the old values as a new unprepared batch.
        using TransactionDatabase db = Open(NewFs());
        db.Put(B("x"), B("x0"));
        using Transaction txn = db.BeginTransaction(s_wo, new TransactionOptions { WriteBatchFlushThreshold = 1 });
        txn.SetName("sp");
        txn.Put(B("a"), B("a1"));
        txn.SetSavePoint();
        txn.Put(B("b"), B("b1")); // flushes [a] (the save point is at the end of it)
        txn.Put(B("x"), B("x1")); // flushes [b]
        txn.Put(B("c"), B("c1"));
        Assert.IsTrue(Impl(txn).UnpreparedSequenceNumbers.Count >= 2);
        Assert.AreEqual("b1", TxnGet(txn, "b"));
        Assert.AreEqual("x1", TxnGet(txn, "x"));
        Assert.IsTrue(txn.RollbackToSavePoint());
        Assert.AreEqual("a1", TxnGet(txn, "a"));
        Assert.AreEqual("NOT_FOUND", TxnGet(txn, "b"));
        Assert.AreEqual("NOT_FOUND", TxnGet(txn, "c"));
        Assert.AreEqual("x0", TxnGet(txn, "x"));
        Assert.IsFalse(txn.RollbackToSavePoint());
        txn.Put(B("d"), B("d1"));
        txn.Commit();
        Assert.AreEqual("a1", Get(db, "a"));
        Assert.AreEqual("NOT_FOUND", Get(db, "b"));
        Assert.AreEqual("NOT_FOUND", Get(db, "c"));
        Assert.AreEqual("x0", Get(db, "x"));
        Assert.AreEqual("d1", Get(db, "d"));
    }

    [TestMethod]
    public void SavePointThenCommit()
    {
        // SavePoint: a save point followed by writes and a commit (the batch is split at the save point).
        using TransactionDatabase db = Open(NewFs());
        using Transaction txn = db.BeginTransaction(s_wo, new TransactionOptions { WriteBatchFlushThreshold = 1 });
        txn.SetName("sp2");
        txn.SetSavePoint();
        txn.Put(B("a"), B("a"));
        txn.Put(B("b"), B("b"));
        txn.Commit();
        Assert.AreEqual("a", Get(db, "a"));
        Assert.AreEqual("b", Get(db, "b"));
    }

    [TestMethod]
    public void UntrackedKeys()
    {
        // UntrackedKeys: keys written without a lock are rolled back too (to the save point and fully).
        using TransactionDatabase db = Open(NewFs());
        using Transaction txn = db.BeginTransaction(s_wo, new TransactionOptions { WriteBatchFlushThreshold = 1 });
        txn.SetName("u");
        txn.Put(B("a"), B("a"));
        txn.PutUntracked(B("a_untrack"), B("a_untrack"));
        txn.SetSavePoint();
        txn.Put(B("b"), B("b"));
        txn.PutUntracked(B("b_untrack"), B("b_untrack"));
        Assert.AreEqual("a", TxnGet(txn, "a"));
        Assert.AreEqual("a_untrack", TxnGet(txn, "a_untrack"));
        Assert.AreEqual("b", TxnGet(txn, "b"));
        Assert.AreEqual("b_untrack", TxnGet(txn, "b_untrack"));
        // Everything should be rolled back.
        txn.Rollback();
        Assert.AreEqual("NOT_FOUND", TxnGet(txn, "a"));
        Assert.AreEqual("NOT_FOUND", TxnGet(txn, "a_untrack"));
        Assert.AreEqual("NOT_FOUND", TxnGet(txn, "b"));
        Assert.AreEqual("NOT_FOUND", TxnGet(txn, "b_untrack"));
    }

    [TestMethod]
    public void WritesNeedAName()
    {
        // FlushWriteBatchToDBInternal: an unprepared batch can only be written by a named transaction.
        using TransactionDatabase db = Open(NewFs());
        using Transaction txn = db.BeginTransaction(s_wo, new TransactionOptions { WriteBatchFlushThreshold = 1 });
        txn.Put(B("a"), B("1"));
        var e = Assert.ThrowsExactly<RocksDbException>(() => txn.Put(B("b"), B("2")));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
    }

    [TestMethod]
    public void DisposeRollsBackUnpreparedData()
    {
        // ~WriteUnpreparedTxn: an unprepared transaction that wrote to the DB is rolled back when it is dropped.
        using TransactionDatabase db = Open(NewFs());
        db.Put(B("a"), B("a0"));
        Transaction txn = db.BeginTransaction(s_wo, new TransactionOptions { WriteBatchFlushThreshold = 1 });
        txn.SetName("dropped");
        txn.Put(B("a"), B("a1"));
        txn.Put(B("b"), B("b1"));
        Assert.AreEqual(1, Impl(txn).UnpreparedSequenceNumbers.Count);
        txn.Dispose();
        Assert.AreEqual("a0", Get(db, "a"));
        Assert.AreEqual("NOT_FOUND", Get(db, "b"));
        db.Put(B("a"), B("a2")); // the lock was released
        Assert.AreEqual("a2", Get(db, "a"));
    }

    [TestMethod]
    public void RecoveryTest()
    {
        // RecoveryTest: unprepared transactions are rolled back at recovery; prepared ones (with unprepared batches)
        // are recovered and can be committed or rolled back.
        foreach (long batchSize in new long[] { 1, 1_000_000 })
        {
            foreach (bool empty in new[] { true, false })
            {
                foreach (int action in new[] { 0, 1, 2 }) // UNPREPARED, ROLLBACK, COMMIT
                {
                    for (int numBatches = 1; numBatches < 6; numBatches++)
                    {
                        string ctx = "batch=" + batchSize + " empty=" + empty + " action=" + action + " n=" + numBatches;
                        MemoryFileSystem fs = NewFs();
                        {
                            TransactionDatabase db = Open(fs);
                            if (!empty)
                            {
                                for (int i = 0; i < numBatches; i++) db.Put(s_sync, null, B("k" + i), B("before value" + i));
                            }
                            // Write num_batches unprepared batches.
                            Transaction txn = db.BeginTransaction(s_sync, new TransactionOptions { WriteBatchFlushThreshold = batchSize });
                            txn.SetName("xid");
                            for (int i = 0; i < numBatches; i++)
                            {
                                txn.Put(B("k" + i), B("value" + i));
                                Assert.AreEqual(batchSize == 1 ? i : 0, Impl(txn).UnpreparedSequenceNumbers.Count, ctx);
                            }
                            if (action == 0)
                            {
                                // This is done to prevent the destructor from rolling back the transaction for us, since we
                                // want to pretend we crashed and test that recovery does the rollback.
                                Impl(txn).UnpreparedSequenceNumbers.Clear();
                            }
                            else
                            {
                                txn.Prepare();
                            }
                            Crash(fs, db);
                        }
                        using (TransactionDatabase db = Open(fs))
                        {
                            List<Transaction> prepared = db.GetAllPreparedTransactions();
                            Assert.AreEqual(action == 0 ? 0 : 1, prepared.Count, ctx);
                            if (action == 1) prepared[0].Rollback();
                            else if (action == 2) prepared[0].Commit();
                            var expected = new List<string>();
                            if (!empty || action == 2)
                            {
                                for (int i = 0; i < numBatches; i++) expected.Add("k" + i + "=" + (action == 2 ? "value" + i : "before value" + i));
                            }
                            Assert.AreEqual(string.Join(",", expected), Scan(db.Database.NewIterator()), ctx);
                            foreach (Transaction t in prepared) t.Dispose();
                        }
                    }
                }
            }
        }
    }
}
