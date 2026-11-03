using RocksDb.Managed.Db;
using RocksDb.Managed.Db.Iterators;
using RocksDb.Managed.Env;
using RocksDb.Managed.Utilities.Transactions;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Transactions;

/// <summary>
/// Ports of key cases of utilities/transactions/write_prepared_transaction_test.cc: the commit cache, max_evicted_seq,
/// the prepared heap, snapshot visibility across eviction (old_commit_map), rollback, recovery, iterators over
/// uncommitted data, sub-batches (duplicate keys) and compaction with the WritePrepared snapshot checker.
/// </summary>
[TestClass]
public sealed class WritePreparedTransactionTests
{
    private static readonly WriteOptions s_wo = new();
    private static readonly WriteOptions s_sync = new() { Sync = true };

    private static TransactionDbOptions WpOptions(int commitCacheBits = 10, int snapshotCacheBits = 7, bool rollbackMergeOperands = false) => new()
    {
        WritePolicy = TransactionWritePolicy.WritePrepared,
        WpCommitCacheBits = commitCacheBits,
        WpSnapshotCacheBits = snapshotCacheBits,
        RollbackMergeOperands = rollbackMergeOperands,
        TransactionLockTimeout = TimeSpan.FromMilliseconds(50),
        DefaultLockTimeout = TimeSpan.FromMilliseconds(50),
    };

    private static TransactionDatabase Open(MemoryFileSystem fs, TransactionDbOptions? options = null, bool disableAutoCompactions = true)
    {
        var cfo = new ColumnFamilyOptions { DisableAutoCompactions = disableAutoCompactions, MergeOperator = MergeOperator.StringAppend(",") };
        return TransactionDatabase.Open(DbPath, NewOptions(fs), options ?? WpOptions(),
            [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, cfo)]);
    }

    private static WritePreparedTxnDb Wp(TransactionDatabase db) => db.WritePreparedDb!;

    private static ulong PrepareSeq(Transaction t) => ((WritePreparedTxn)t).PrepareSequence;

    private static void Crash(MemoryFileSystem fs, TransactionDatabase db)
    {
        db.Database.WaitForCompact();
        fs.SimulateCrash();
    }

    private static string Get(TransactionDatabase db, string key, Snapshot? snapshot = null) =>
        S(db.Database.Get(new ReadOptions { Snapshot = snapshot }, B(key))) ?? "NOT_FOUND";

    private static void VerifyKeys(TransactionDatabase db, (string Key, string Value)[] expected, Snapshot? snapshot = null)
    {
        foreach ((string k, string v) in expected) Assert.AreEqual(v, Get(db, k, snapshot), "key " + k);
    }

    /// <summary>VerifyInternalKeys: every internal entry (user key, value, sequence, type) in key order.</summary>
    private static void VerifyInternalKeys(TransactionDatabase db, (string Key, string Value, ulong Seq, EntryType Type)[] expected)
    {
        DbImpl impl = db.Database.Impl;
        DbIteratorSources sources = impl.NewIteratorSources(db.Database.DefaultColumnFamily.Data, null, null, impl.Versions.LastSequence, null);
        var actual = new List<(string, string, ulong, EntryType)>();
        using (InternalIterator it = DbIteratorFactory.NewInternalIterator(sources, null))
        {
            for (it.SeekToFirst(); it.Valid; it.Next())
            {
                ReadOnlySpan<byte> k = it.Key;
                DbFormat.UnpackSequenceAndType(DbFormat.ExtractFooter(k), out ulong seq, out EntryType type);
                string uk = System.Text.Encoding.UTF8.GetString(DbFormat.ExtractUserKey(k));
                if (uk is "a" or "z") continue; // dummy keys
                actual.Add((uk, System.Text.Encoding.UTF8.GetString(it.Value), seq, type));
            }
        }
        sources.Cleanup?.Dispose();
        Assert.AreEqual(expected.Length, actual.Count, string.Join("; ", actual));
        for (int i = 0; i < expected.Length; i++) Assert.AreEqual(expected[i], actual[i], "entry " + i + ": " + string.Join("; ", actual));
    }

    // ---- commit cache / IsInSnapshot -----------------------------------------------------------------------

    [TestMethod]
    public void PreparedDataIsInvisibleUntilCommit()
    {
        using TransactionDatabase db = Open(NewFs());
        db.Put(B("foo"), B("v0"));
        using Transaction txn = db.BeginTransaction(s_wo);
        txn.SetName("xid");
        txn.Put(B("foo"), B("v1"));
        txn.Put(B("bar"), B("b1"));
        txn.Prepare();
        ulong prep = PrepareSeq(txn);
        Assert.AreEqual(db.Database.GetLatestSequenceNumber(), prep); // one sequence number for the prepared batch
        // The prepared data is in the memtable but readers do not see it.
        Assert.AreEqual("v0", Get(db, "foo"));
        Assert.AreEqual("NOT_FOUND", Get(db, "bar"));
        Assert.IsTrue(Wp(db).IsPrepared(prep));
        using Snapshot before = db.Database.GetSnapshot();
        Assert.AreEqual(prep, before.MinUncommitted); // SmallestUnCommittedSeq: the prepared batch
        txn.Commit();
        Assert.IsFalse(Wp(db).IsPrepared(prep));
        Assert.IsTrue(Wp(db).TryGetCommitEntry(prep, out ulong commit));
        Assert.AreEqual(prep + 1, commit); // the commit marker consumed the next sequence number
        Assert.AreEqual("v1", Get(db, "foo"));
        Assert.AreEqual("b1", Get(db, "bar"));
        // A snapshot taken before the commit still does not see it.
        Assert.AreEqual("v0", Get(db, "foo", before));
        Assert.AreEqual("NOT_FOUND", Get(db, "bar", before));
    }

    [TestMethod]
    public void CommitCacheEvictionAdvancesMaxEvictedSeq()
    {
        // A commit cache of 4 entries: every commit after the 4th evicts an older one.
        using TransactionDatabase db = Open(NewFs(), WpOptions(commitCacheBits: 2));
        WritePreparedTxnDb wp = Wp(db);
        Assert.AreEqual(0UL, wp.MaxEvictedSeq);
        for (int i = 0; i < 40; i++) db.Put(B("k" + i), B("v" + i));
        Assert.IsTrue(wp.MaxEvictedSeq > 0);
        Assert.IsTrue(wp.MaxEvictedSeq < db.Database.GetLatestSequenceNumber());
        for (int i = 0; i < 40; i++) Assert.AreEqual("v" + i, Get(db, "k" + i));
        bool released = false;
        // Everything at or below max_evicted_seq that is not prepared is committed.
        Assert.IsTrue(wp.IsInSnapshot(1, db.Database.GetLatestSequenceNumber(), ReadCallback.MinUnCommittedSeq, ref released));
        Assert.IsFalse(released);
    }

    [TestMethod]
    public void SnapshotVisibilityAcrossEviction()
    {
        // old_commit_map: a transaction prepared before a snapshot and committed after it must stay invisible to the
        // snapshot after its commit entry is evicted from the commit cache.
        using TransactionDatabase db = Open(NewFs(), WpOptions(commitCacheBits: 1, snapshotCacheBits: 1));
        WritePreparedTxnDb wp = Wp(db);
        db.Put(B("key"), B("old"));
        using Transaction txn = db.BeginTransaction(s_wo);
        txn.SetName("xid");
        txn.Put(B("key"), B("new"));
        txn.Prepare();
        // Several snapshots (more than the snapshot cache) between prepare and commit.
        var snaps = new List<Snapshot>();
        for (int i = 0; i < 5; i++)
        {
            snaps.Add(db.Database.GetSnapshot());
            db.Put(B("filler" + i), B("x")); // advance the sequence
        }
        txn.Commit();
        // Evict the commit entry: many more commits through a 2-entry cache.
        for (int i = 0; i < 50; i++) db.Put(B("e" + i), B("y"));
        Assert.IsFalse(wp.TryGetCommitEntry(PrepareSeq(txn), out _));
        Assert.IsTrue(wp.MaxEvictedSeq >= PrepareSeq(txn));
        Assert.IsTrue(wp.OldCommitMapCount > 0);
        foreach (Snapshot s in snaps) Assert.AreEqual("old", Get(db, "key", s));
        Assert.AreEqual("new", Get(db, "key"));
        // Releasing the snapshots lets the old_commit_map entries go (CleanupReleasedSnapshots on the next update).
        foreach (Snapshot s in snaps) s.Dispose();
        for (int i = 0; i < 50; i++) db.Put(B("f" + i), B("y"));
        Assert.AreEqual(0, wp.OldCommitMapCount);
    }

    [TestMethod]
    public void IsInSnapshotTruthTable()
    {
        // IsInSnapshot: prepare 0 is always visible; data newer than the snapshot is not; data committed after the
        // snapshot is not; data committed before it is.
        using TransactionDatabase db = Open(NewFs());
        WritePreparedTxnDb wp = Wp(db);
        bool released = false;
        Assert.IsTrue(wp.IsInSnapshot(0, 5, ReadCallback.MinUnCommittedSeq, ref released));
        db.Put(B("a"), B("1"));
        ulong s1 = db.Database.GetLatestSequenceNumber();
        Assert.IsTrue(wp.IsInSnapshot(s1, s1, ReadCallback.MinUnCommittedSeq, ref released));
        Assert.IsFalse(wp.IsInSnapshot(s1, s1 - 1, ReadCallback.MinUnCommittedSeq, ref released));
        using Transaction t = db.BeginTransaction(s_wo);
        t.SetName("t");
        t.Put(B("b"), B("2"));
        t.Prepare();
        ulong p = PrepareSeq(t);
        Assert.IsFalse(wp.IsInSnapshot(p, p + 100, ReadCallback.MinUnCommittedSeq, ref released));
        t.Commit();
        ulong c = db.Database.GetLatestSequenceNumber();
        Assert.IsFalse(wp.IsInSnapshot(p, c - 1, ReadCallback.MinUnCommittedSeq, ref released));
        Assert.IsTrue(wp.IsInSnapshot(p, c, ReadCallback.MinUnCommittedSeq, ref released));
        // min_uncommitted: anything below it is committed.
        Assert.IsTrue(wp.IsInSnapshot(p, c - 1, p + 1, ref released));
        Assert.IsFalse(released);
    }

    [TestMethod]
    public void PreparedHeapBasics()
    {
        // PreparedHeap_BasicsTest: in-order pushes, erase of the top and of entries below it.
        var heap = new WritePreparedTxnDb.PreparedHeap();
        Assert.IsTrue(heap.IsEmpty);
        lock (heap.PushPopLock)
        {
            foreach (ulong v in new ulong[] { 14, 15, 16, 17, 18, 19, 20, 21 }) heap.Push(v);
        }
        Assert.AreEqual(14UL, heap.Top);
        heap.Erase(15);
        Assert.AreEqual(14UL, heap.Top);
        heap.Erase(14);
        Assert.AreEqual(16UL, heap.Top); // 15 was erased before
        heap.Erase(18);
        heap.Erase(17);
        Assert.AreEqual(16UL, heap.Top);
        heap.Erase(16);
        Assert.AreEqual(19UL, heap.Top);
        heap.Erase(13); // already popped / never pushed: ignored
        Assert.AreEqual(19UL, heap.Top);
        heap.Erase(19);
        heap.Erase(20);
        heap.Erase(21);
        Assert.IsTrue(heap.IsEmpty);
    }

    [TestMethod]
    public void PreparedBelowMaxBecomesDelayedPrepared()
    {
        // CommitOfDelayedPrepared / AddPreparedBeforeMax: a prepared transaction that falls behind max_evicted_seq moves
        // to delayed_prepared_ and stays invisible until committed.
        using TransactionDatabase db = Open(NewFs(), WpOptions(commitCacheBits: 1));
        WritePreparedTxnDb wp = Wp(db);
        using Transaction txn = db.BeginTransaction(s_wo);
        txn.SetName("slow");
        txn.Put(B("slow"), B("1"));
        txn.Prepare();
        for (int i = 0; i < 20; i++) db.Put(B("k" + i), B("v"));
        Assert.IsTrue(wp.MaxEvictedSeq >= PrepareSeq(txn));
        Assert.AreEqual(1, wp.DelayedPreparedCount);
        Assert.AreEqual("NOT_FOUND", Get(db, "slow"));
        using Snapshot snap = db.Database.GetSnapshot();
        Assert.AreEqual(PrepareSeq(txn), snap.MinUncommitted); // SmallestUnCommittedSeq from delayed_prepared_
        txn.Commit();
        Assert.AreEqual(0, wp.DelayedPreparedCount);
        Assert.AreEqual("1", Get(db, "slow"));
        Assert.AreEqual("NOT_FOUND", Get(db, "slow", snap));
    }

    // ---- sub-batches ---------------------------------------------------------------------------------------

    [TestMethod]
    public void DuplicateKeysUseSubBatches()
    {
        // DuplicateKeys: a batch with a repeated key needs a new sub-batch (sequence number) for the repeat.
        MemoryFileSystem fs = NewFs();
        ulong prep;
        {
            TransactionDatabase db = Open(fs);
            using Transaction txn = db.BeginTransaction(s_wo);
            txn.SetName("dup");
            txn.Put(B("a"), B("1"));
            txn.Put(B("b"), B("1"));
            txn.Put(B("a"), B("2")); // duplicate: a second sub-batch
            txn.Merge(B("c"), B("x"));
            txn.Merge(B("c"), B("y")); // duplicate merge: a third sub-batch
            txn.Prepare();
            prep = PrepareSeq(txn);
            Assert.AreEqual(prep + 2, db.Database.GetLatestSequenceNumber());
            Assert.IsTrue(Wp(db).IsPrepared(prep));
            Assert.AreEqual("NOT_FOUND", Get(db, "a"));
            txn.Commit();
            Assert.AreEqual("2", Get(db, "a"));
            Assert.AreEqual("1", Get(db, "b"));
            Assert.AreEqual("x,y", Get(db, "c"));
            // A plain write with duplicate keys (WriteInternal's SubBatchCounter).
            using WriteBatch b = new();
            b.Put(B("d"), B("1")).Put(B("d"), B("2")).Delete(B("b"));
            db.Write(new WriteOptions { Sync = true }, b);
            Assert.AreEqual("2", Get(db, "d"));
            Assert.AreEqual("NOT_FOUND", Get(db, "b"));
            Crash(fs, db);
        }
        using (TransactionDatabase db = Open(fs))
        {
            Assert.AreEqual("2", Get(db, "a"));
            Assert.AreEqual("x,y", Get(db, "c"));
            Assert.AreEqual("2", Get(db, "d"));
            Assert.AreEqual("NOT_FOUND", Get(db, "b"));
        }
    }

    // ---- rollback ------------------------------------------------------------------------------------------

    [TestMethod]
    public void RollbackRestoresPreviousValues()
    {
        // Rollback: the prepared values are replaced by the values before the transaction (or deletes).
        foreach (bool rollbackMerges in new[] { false, true })
        {
            using TransactionDatabase db = Open(NewFs(), WpOptions(rollbackMergeOperands: rollbackMerges));
            db.Put(B("key1"), B("value1_1"));
            db.Put(B("key2"), B("value2_1"));
            db.Merge(B("key3"), B("value3_1"));
            using Transaction txn = db.BeginTransaction(s_wo);
            txn.SetName("txn");
            txn.Put(B("key1"), B("value1_2"));
            txn.Delete(B("key2"));
            txn.Merge(B("key3"), B("value3_2"));
            txn.Put(B("key4"), B("value4_1"));
            txn.Prepare();
            ulong prep = PrepareSeq(txn);
            txn.Rollback();
            Assert.AreEqual(TransactionState.RolledBack, txn.State);
            Assert.IsFalse(Db(db).IsPrepared(prep));
            VerifyKeys(db, [("key1", "value1_1"), ("key2", "value2_1"), ("key4", "NOT_FOUND")]);
            // The merge operand of a rolled back transaction is restored only with rollback_merge_operands.
            Assert.AreEqual(rollbackMerges ? "value3_1" : "value3_1,value3_2", Get(db, "key3"));
        }

        static WritePreparedTxnDb Db(TransactionDatabase d) => d.WritePreparedDb!;
    }

    [TestMethod]
    public void RollbackSurvivesRecovery()
    {
        MemoryFileSystem fs = NewFs();
        {
            TransactionDatabase db = Open(fs);
            db.Put(s_sync, null, B("k"), B("before"));
            Transaction txn = db.BeginTransaction(new WriteOptions { Sync = true });
            txn.SetName("r");
            txn.Put(B("k"), B("during"));
            txn.Put(B("n"), B("new"));
            txn.Prepare();
            txn.Rollback();
            Crash(fs, db);
        }
        using TransactionDatabase db2 = Open(fs);
        Assert.AreEqual(0, db2.GetAllPreparedTransactions().Count);
        Assert.AreEqual("before", Get(db2, "k"));
        Assert.AreEqual("NOT_FOUND", Get(db2, "n"));
    }

    // ---- recovery ------------------------------------------------------------------------------------------

    [TestMethod]
    public void BasicRecovery()
    {
        // BasicRecovery: prepared transactions become delayed prepared after recovery and stay invisible; a recovered
        // transaction committed before the next crash is committed after it.
        MemoryFileSystem fs = NewFs();
        ulong prep0, prep1, prep2;
        {
            TransactionDatabase db = Open(fs);
            db.Put(new WriteOptions { Sync = true }, null, B("x"), B("0"));
            Transaction t0 = db.BeginTransaction(s_sync);
            t0.SetName("xid1000");
            t0.Put(B("foo01000"), B("bar01000"));
            t0.Prepare();
            prep0 = PrepareSeq(t0);
            Transaction t1 = db.BeginTransaction(s_sync);
            t1.SetName("xid1001");
            t1.Put(B("foo11001"), B("bar"));
            t1.Prepare();
            prep1 = PrepareSeq(t1);
            Assert.AreEqual("NOT_FOUND", Get(db, "foo01000"));
            Crash(fs, db);
        }
        {
            TransactionDatabase db = Open(fs);
            WritePreparedTxnDb wp = Wp(db);
            // After recovery, all the uncommitted txns (0 and 1) should be inserted into delayed_prepared_
            Assert.AreEqual(DbFormat.MaxSequenceNumber, wp.PreparedHeapTop);
            Assert.AreEqual(2, wp.DelayedPreparedCount);
            Assert.IsTrue(wp.IsPrepared(prep0));
            Assert.IsTrue(wp.IsPrepared(prep1));
            Assert.IsTrue(prep0 <= wp.MaxEvictedSeq && prep1 <= wp.MaxEvictedSeq);
            Assert.AreEqual("NOT_FOUND", Get(db, "foo01000"));
            Assert.AreEqual("0", Get(db, "x"));
            Transaction t1 = db.GetTransactionByName("xid1001")!;
            Assert.AreEqual(prep1, PrepareSeq(t1));
            t1.Commit();
            Assert.AreEqual("bar", Get(db, "foo11001"));
            Transaction t2 = db.BeginTransaction(s_sync);
            t2.SetName("xid1002");
            t2.Put(B("foo21002"), B("bar"));
            t2.Prepare();
            prep2 = PrepareSeq(t2);
            Crash(fs, db);
        }
        {
            TransactionDatabase db = Open(fs);
            WritePreparedTxnDb wp = Wp(db);
            // 0 and 2 are prepared and 1 is committed
            Assert.AreEqual(2, wp.DelayedPreparedCount);
            Assert.IsTrue(wp.IsPrepared(prep0));
            Assert.IsFalse(wp.IsPrepared(prep1));
            Assert.IsTrue(wp.IsPrepared(prep2));
            Assert.AreEqual("bar", Get(db, "foo11001"));
            db.GetTransactionByName("xid1000")!.Commit();
            db.GetTransactionByName("xid1002")!.Commit();
            Assert.AreEqual("bar01000", Get(db, "foo01000"));
            db.Dispose();
        }
        using (TransactionDatabase db = Open(fs))
        {
            Assert.AreEqual(0, Wp(db).DelayedPreparedCount);
            Assert.AreEqual("bar01000", Get(db, "foo01000"));
            Assert.AreEqual("bar", Get(db, "foo21002"));
        }
    }

    [TestMethod]
    public void RecoveredPreparedDataFlushedToTablesStaysInvisible()
    {
        // Recovery flushes the memtables (allow_2pc): the prepared data lands in a table file and must stay invisible
        // (and survive: the recovery flush keeps every version, DisableGCSnapshotChecker).
        MemoryFileSystem fs = NewFs();
        {
            TransactionDatabase db = Open(fs);
            db.Put(s_sync, null, B("k"), B("committed"));
            Transaction t = db.BeginTransaction(new WriteOptions { Sync = true });
            t.SetName("p");
            t.Put(B("k"), B("prepared"));
            t.Prepare();
            Crash(fs, db);
        }
        {
            TransactionDatabase db = Open(fs);
            Assert.AreEqual("committed", Get(db, "k"));
            db.Database.Flush();
            db.Database.CompactRange();
            Assert.AreEqual("committed", Get(db, "k"));
            using (Iterator it = db.Database.NewIterator())
            {
                it.SeekToFirst();
                Assert.IsTrue(it.Valid);
                Assert.AreEqual("committed", S(it.Value.ToArray()));
            }
            db.Dispose(); // still prepared
        }
        using (TransactionDatabase db = Open(fs))
        {
            Assert.AreEqual("committed", Get(db, "k"));
            db.GetAllPreparedTransactions().Single().Commit();
            Assert.AreEqual("prepared", Get(db, "k"));
            db.Database.Flush();
            db.Database.CompactRange();
            Assert.AreEqual("prepared", Get(db, "k"));
        }
    }

    // ---- iterators -----------------------------------------------------------------------------------------

    [TestMethod]
    public void IteratorsSkipUncommittedData()
    {
        // Iterate: DB iterators (with or without a snapshot) and transaction iterators see committed data only, plus the
        // transaction's own batch.
        using TransactionDatabase db = Open(NewFs());
        db.Put(B("a"), B("a0"));
        db.Put(B("c"), B("c0"));
        using Transaction prepared = db.BeginTransaction(s_wo);
        prepared.SetName("p");
        prepared.Put(B("a"), B("a1"));
        prepared.Put(B("b"), B("b1"));
        prepared.Delete(B("c"));
        prepared.Prepare();
        using Snapshot snap = db.Database.GetSnapshot();
        Assert.AreEqual("a=a0,c=c0", Scan(db.Database.NewIterator()));
        Assert.AreEqual("a=a0,c=c0", Scan(db.Database.NewIterator(new ReadOptions { Snapshot = snap })));
        // Reverse iteration
        using (Iterator it = db.Database.NewIterator())
        {
            var keys = new List<string>();
            for (it.SeekToLast(); it.Valid; it.Prev()) keys.Add(S(it.Key.ToArray())!);
            CollectionAssert.AreEqual(new[] { "c", "a" }, keys);
        }
        using (Transaction mine = db.BeginTransaction(s_wo))
        {
            mine.Put(B("d"), B("d1"));
            using TransactionIterator ti = mine.NewIterator(new ReadOptions());
            var seen = new List<string>();
            for (ti.SeekToFirst(); ti.Valid; ti.Next()) seen.Add(S(ti.Key.ToArray()) + "=" + S(ti.Value.ToArray()));
            Assert.AreEqual("a=a0,c=c0,d=d1", string.Join(",", seen));
            mine.Rollback();
        }
        prepared.Commit();
        Assert.AreEqual("a=a1,b=b1", Scan(db.Database.NewIterator()));
        Assert.AreEqual("a=a0,c=c0", Scan(db.Database.NewIterator(new ReadOptions { Snapshot = snap })));
        // MultiGet goes through the read callback too.
        byte[]?[] values = db.Database.MultiGet(new ReadOptions { Snapshot = snap }, [B("a"), B("b"), B("c")]);
        Assert.AreEqual("a0", S(values[0]));
        Assert.IsNull(values[1]);
        Assert.AreEqual("c0", S(values[2]));
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
    public async Task AsyncReadsUseTheReadCallback()
    {
        using TransactionDatabase db = Open(NewFs());
        db.Put(B("k"), B("v0"));
        db.Database.Flush();
        using Transaction t = db.BeginTransaction(s_wo);
        t.SetName("p");
        t.Put(B("k"), B("v1"));
        t.Prepare();
        Assert.AreEqual("v0", S(await db.Database.GetAsync(B("k"))));
        t.Commit();
        Assert.AreEqual("v1", S(await db.Database.GetAsync(B("k"))));
    }

    // ---- conflicts -----------------------------------------------------------------------------------------

    [TestMethod]
    public void SnapshotValidationSeesCommitsAfterTheSnapshot()
    {
        // WritePreparedTxn::ValidateSnapshot: a key committed after the transaction's snapshot is a conflict even when
        // its prepare sequence is older than the snapshot.
        using TransactionDatabase db = Open(NewFs());
        using Transaction writer = db.BeginTransaction(s_wo);
        writer.SetName("w");
        writer.Put(B("k"), B("1"));
        writer.Prepare();
        using Transaction reader = db.BeginTransaction(s_wo, new TransactionOptions { SetSnapshot = true });
        writer.Commit();
        var e = Assert.ThrowsExactly<RocksDbException>(() => reader.Put(B("k"), B("2")));
        Assert.AreEqual(StatusCode.Busy, e.Code);
        reader.Put(B("other"), B("ok"));
        reader.Commit();
        Assert.AreEqual("1", Get(db, "k"));
        Assert.AreEqual("ok", Get(db, "other"));
    }

    // ---- compaction ----------------------------------------------------------------------------------------

    [TestMethod]
    public void CompactionShouldKeepUncommittedKeys()
    {
        using TransactionDatabase db = Open(NewFs());
        var snapshots = new List<Snapshot>();
        ulong expectedSeq = 0;
        void AddKey(Action a)
        {
            a();
            expectedSeq++;
            Assert.AreEqual(expectedSeq, db.Database.GetLatestSequenceNumber());
            snapshots.Add(db.Database.GetSnapshot());
        }
        // Each key here represent a standalone test case.
        AddKey(() => db.Database.Put(B("key1"), B("value1_1")));
        AddKey(() => db.Database.Put(B("key2"), B("value2_1")));
        AddKey(() => db.Database.Put(B("key3"), B("value3_1")));
        AddKey(() => db.Database.Put(B("key4"), B("value4_1")));
        AddKey(() => db.Database.Merge(B("key5"), B("value5_1")));
        AddKey(() => db.Database.Merge(B("key5"), B("value5_2")));
        AddKey(() => db.Database.Put(B("key6"), B("value6_1")));
        AddKey(() => db.Database.Put(B("key7"), B("value7_1")));
        db.Database.Flush();
        AddKey(() => db.Database.Delete(B("key6")));
        AddKey(() => db.Database.SingleDelete(B("key7")));

        using Transaction txn = db.BeginTransaction(s_wo);
        txn.SetName("txn");
        txn.Put(B("key1"), B("value1_2"));
        txn.Delete(B("key2"));
        txn.SingleDelete(B("key3"));
        txn.Merge(B("key4"), B("value4_2"));
        txn.Merge(B("key5"), B("value5_3"));
        txn.Put(B("key6"), B("value6_2"));
        txn.Put(B("key7"), B("value7_2"));
        // Prepare but not commit.
        txn.Prepare();
        Assert.AreEqual(++expectedSeq, db.Database.GetLatestSequenceNumber());
        db.Database.Flush();
        foreach (Snapshot s in snapshots) s.Dispose();
        // Dummy keys to avoid compaction trivially move files and get around actual compaction logic.
        db.Database.Put(B("a"), B("dummy"));
        db.Database.Put(B("z"), B("dummy"));
        db.Database.CompactRange();
        VerifyKeys(db,
        [
            ("key1", "value1_1"), ("key2", "value2_1"), ("key3", "value3_1"), ("key4", "value4_1"), ("key5", "value5_1,value5_2"),
            ("key6", "NOT_FOUND"), ("key7", "NOT_FOUND"),
        ]);
        VerifyInternalKeys(db,
        [
            ("key1", "value1_2", expectedSeq, EntryType.Value),
            ("key1", "value1_1", 0, EntryType.Value),
            ("key2", "", expectedSeq, EntryType.Deletion),
            ("key2", "value2_1", 0, EntryType.Value),
            ("key3", "", expectedSeq, EntryType.SingleDeletion),
            ("key3", "value3_1", 0, EntryType.Value),
            ("key4", "value4_2", expectedSeq, EntryType.Merge),
            ("key4", "value4_1", 0, EntryType.Value),
            ("key5", "value5_3", expectedSeq, EntryType.Merge),
            ("key5", "value5_1,value5_2", 0, EntryType.Value),
            ("key6", "value6_2", expectedSeq, EntryType.Value),
            ("key7", "value7_2", expectedSeq, EntryType.Value),
        ]);
        txn.Commit();
        VerifyKeys(db,
        [
            ("key1", "value1_2"), ("key2", "NOT_FOUND"), ("key3", "NOT_FOUND"), ("key4", "value4_1,value4_2"),
            ("key5", "value5_1,value5_2,value5_3"), ("key6", "value6_2"), ("key7", "value7_2"),
        ]);
    }

    /// <summary>
    /// Regression (TwoWriteQueuesTests.CommitsRacingReaders, "snapshot changed" to NOT_FOUND): a compaction's snapshot list
    /// must be taken with its job snapshot, not when the compaction was picked. A snapshot taken in between does not see
    /// a transaction that commits before the job snapshot; without it in the list, the compaction let the newer value
    /// hide the version that snapshot reads, and zeroed the newer value's sequence number, so the snapshot read the
    /// later commit (or nothing once the older versions were gone).
    /// </summary>
    [TestMethod]
    public async Task CompactionSeesSnapshotsTakenAfterItWasPicked()
    {
        using TransactionDatabase db = Open(NewFs());
        db.Database.Put(B("key"), B("v1"));
        db.Database.Flush();
        using Transaction txn = db.BeginTransaction(s_wo);
        txn.SetName("txn");
        txn.Put(B("key"), B("v2"));
        txn.Prepare();
        db.Database.Flush(); // the prepared value goes to a second L0 file

        using var paused = new ManualResetEventSlim(false);
        using var resume = new ManualResetEventSlim(false);
        int hits = 0;
        TestHooks.Set("DBImpl::RunCompaction:Start", () =>
        {
            if (Interlocked.Increment(ref hits) != 1) return;
            paused.Set();
            resume.Wait();
        });
        try
        {
            // Picked (and its snapshots listed) now; runs once resumed.
            Task compaction = Task.Run(() => db.Database.CompactRange());
            Assert.IsTrue(paused.Wait(TimeSpan.FromSeconds(30)), "compaction did not start");
            using Snapshot snapshot = db.Database.GetSnapshot();
            txn.Commit();
            Assert.AreEqual("v1", Get(db, "key", snapshot));
            resume.Set();
            await compaction;
            Assert.AreEqual("v1", Get(db, "key", snapshot));
            Assert.AreEqual("v2", Get(db, "key"));
        }
        finally
        {
            resume.Set();
            TestHooks.ClearAll();
        }
    }

    [TestMethod]
    public void CompactionShouldKeepSnapshotVisibleKeys()
    {
        // Compaction should keep keys visible to a snapshot based on commit sequence, not just prepare sequence.
        using TransactionDatabase db = Open(NewFs());
        ulong expectedSeq = 0;
        using (Transaction txn1 = db.BeginTransaction(s_wo))
        {
            txn1.SetName("txn1");
            txn1.Put(B("key1"), B("value1_1"));
            txn1.Prepare();
            Assert.AreEqual(++expectedSeq, db.Database.GetLatestSequenceNumber());
            txn1.Commit();
            Assert.AreEqual(++expectedSeq, db.Database.GetLatestSequenceNumber());
        }
        // Take a snapshots to avoid keys get evicted before compaction.
        Snapshot snapshot1 = db.Database.GetSnapshot();
        Snapshot snapshot2;
        using (Transaction txn2 = db.BeginTransaction(s_wo))
        {
            txn2.SetName("txn2");
            txn2.Put(B("key2"), B("value2_1"));
            txn2.Prepare();
            Assert.AreEqual(++expectedSeq, db.Database.GetLatestSequenceNumber());
            // txn1 commit before snapshot2 and it is visible to snapshot2. txn2 commit after snapshot2 and it is not visible.
            snapshot2 = db.Database.GetSnapshot();
            txn2.Commit();
            Assert.AreEqual(++expectedSeq, db.Database.GetLatestSequenceNumber());
        }
        Snapshot snapshot3 = db.Database.GetSnapshot();
        db.Database.Put(B("key1"), B("value1_2"));
        ulong seq1 = ++expectedSeq;
        db.Database.Put(B("key2"), B("value2_2"));
        ulong seq2 = ++expectedSeq;
        Assert.AreEqual(expectedSeq, db.Database.GetLatestSequenceNumber());
        db.Database.Flush();
        snapshot1.Dispose();
        snapshot3.Dispose();
        db.Database.Put(B("a"), B("dummy"));
        db.Database.Put(B("z"), B("dummy"));
        db.Database.CompactRange();
        VerifyKeys(db, [("key1", "value1_2"), ("key2", "value2_2")]);
        VerifyKeys(db, [("key1", "value1_1"), ("key2", "NOT_FOUND")], snapshot2);
        VerifyInternalKeys(db,
        [
            ("key1", "value1_2", seq1, EntryType.Value),
            // "value1_1" is visible to snapshot2. Also keys at bottom level visible to earliest snapshot will output with
            // seq = 0.
            ("key1", "value1_1", 0, EntryType.Value),
            ("key2", "value2_2", seq2, EntryType.Value),
        ]);
        snapshot2.Dispose();
    }

    [TestMethod]
    public void SmallestUncommittedOptimization()
    {
        foreach (bool hasRecentPrepare in new[] { true, false })
        {
            // commit_cache_bits = 0: a one-entry commit cache
            using TransactionDatabase db = Open(NewFs(), WpOptions(commitCacheBits: 0));
            db.Put(B("key1"), B("value1"));
            using Transaction transaction = db.BeginTransaction(s_wo);
            transaction.SetName("txn");
            transaction.Delete(B("key1"));
            transaction.Prepare();
            // snapshot1 should get min_uncommitted from prepared_txns_ heap.
            using Snapshot snapshot1 = db.Database.GetSnapshot();
            Assert.AreEqual(PrepareSeq(transaction), snapshot1.MinUncommitted);
            // Add a commit to advance max_evicted_seq and move the prepared transaction into delayed_prepared_ set.
            db.Put(B("key2"), B("value2"));
            Transaction? txn2 = null;
            if (hasRecentPrepare)
            {
                txn2 = db.BeginTransaction(s_wo);
                txn2.SetName("txn2");
                txn2.Put(B("key3"), B("value3"));
                txn2.Prepare();
            }
            // snapshot2 should get min_uncommitted from delayed_prepared_ set.
            using Snapshot snapshot2 = db.Database.GetSnapshot();
            Assert.AreEqual(PrepareSeq(transaction), snapshot2.MinUncommitted);
            transaction.Commit();
            if (txn2 is not null)
            {
                txn2.Commit();
                txn2.Dispose();
            }
            VerifyKeys(db, [("key1", "NOT_FOUND")]);
            VerifyKeys(db, [("key1", "value1")], snapshot1);
            VerifyKeys(db, [("key1", "value1")], snapshot2);
        }
    }

    [TestMethod]
    public void ConcurrentCommitsAndReaders()
    {
        // Writers commit (prepared and unprepared) while readers check that a snapshot never changes: exercises the commit
        // cache eviction races (AddCommitted / IsInSnapshot / AdvanceMaxEvictedSeq) with a tiny cache.
        using TransactionDatabase db = Open(NewFs(), WpOptions(commitCacheBits: 3, snapshotCacheBits: 1), disableAutoCompactions: false);
        for (int i = 0; i < 16; i++) db.Put(B("k" + i), B("0"));
        int stop = 0;
        Exception? failure = null;
        var writers = new Task[3];
        for (int w = 0; w < writers.Length; w++)
        {
            int id = w;
            writers[w] = Task.Run(() =>
            {
                try
                {
                    for (int r = 1; Volatile.Read(ref stop) == 0 && r < 300; r++)
                    {
                        using Transaction t = db.BeginTransaction(s_wo);
                        t.SetName("w" + id + "-" + r);
                        // every writer writes all keys of its slice with the same round number
                        for (int k = id; k < 16; k += writers.Length) t.Put(B("k" + k), B(r.ToString(System.Globalization.CultureInfo.InvariantCulture)));
                        if (r % 2 == 0) t.Prepare();
                        t.Commit();
                    }
                }
                catch (Exception e)
                {
                    failure ??= e;
                }
            });
        }
        try
        {
            for (int i = 0; i < 200 && failure is null; i++)
            {
                using Snapshot s = db.Database.GetSnapshot();
                var first = new string[16];
                for (int k = 0; k < 16; k++) first[k] = Get(db, "k" + k, s);
                // A writer's keys always have the same round in one snapshot.
                for (int w = 0; w < writers.Length; w++)
                {
                    for (int k = w + writers.Length; k < 16; k += writers.Length) Assert.AreEqual(first[w], first[k], "torn transaction in a snapshot");
                }
                for (int k = 0; k < 16; k++) Assert.AreEqual(first[k], Get(db, "k" + k, s), "snapshot changed");
            }
        }
        finally
        {
            Volatile.Write(ref stop, 1);
            Task.WaitAll(writers);
        }
        if (failure is not null) throw new AssertFailedException("writer failed", failure);
    }
}
