using System.Text;
using RocksDb.Managed.Env;
using RocksDb.Managed.Tests.Db;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Features;

/// <summary>Port of key cases of utilities/write_batch_with_index/write_batch_with_index_test.cc plus a randomized model test.</summary>
[TestClass]
public sealed class WriteBatchWithIndexTests
{
    private static ColumnFamilyOptions MergeCf() => new() { MergeOperator = new StringAppendMergeOperator() };

    private static RocksDatabase OpenDb(MemoryFileSystem fs) => RocksDatabase.Open(DbPath, NewOptions(fs), MergeCf());

    private static RocksDatabase OpenDb(MemoryFileSystem fs, IReadOnlyDictionary<string, string> baseData)
    {
        RocksDatabase db = OpenDb(fs);
        foreach (KeyValuePair<string, string> kv in baseData) db.Put(B(kv.Key), B(kv.Value));
        return db;
    }

    private static void AssertIter(BaseDeltaIterator it, string key, string value)
    {
        Assert.IsTrue(it.Valid, "expected " + key);
        Assert.AreEqual(key, Encoding.UTF8.GetString(it.Key));
        Assert.AreEqual(value, Encoding.UTF8.GetString(it.Value));
    }

    private static string? GetFromBatch(WriteBatchWithIndex b, ColumnFamily? cf, string key) => S(b.GetFromBatch(cf, B(key)));

    [TestMethod]
    public void TestGetFromBatch()
    {
        using var batch = new WriteBatchWithIndex();
        Assert.IsNull(batch.GetFromBatch(B("b")));
        batch.Put(B("a"), B("a")).Put(B("b"), B("b")).Put(B("c"), B("c")).Put(B("a"), B("z"))
            .Delete(B("c")).Delete(B("d")).Delete(B("e")).Put(B("e"), B("e"));
        Assert.AreEqual("b", S(batch.GetFromBatch(B("b"))));
        Assert.AreEqual("z", S(batch.GetFromBatch(B("a"))));
        Assert.IsNull(batch.GetFromBatch(B("c")));
        Assert.IsNull(batch.GetFromBatch(B("d")));
        Assert.IsNull(batch.GetFromBatch(B("x")));
        Assert.AreEqual("e", S(batch.GetFromBatch(B("e"))));
        batch.Merge(B("z"), B("z"));
        // No base value in the batch: MergeInProgress.
        Assert.AreEqual(StatusCode.MergeInProgress, Assert.ThrowsExactly<RocksDbException>(() => batch.GetFromBatch(B("z"))).Code);
        batch.Put(B("y"), B("1")).Merge(B("y"), B("2"));
        // No merge operator for a batch-only read without a column family.
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => batch.GetFromBatch(B("y"))).Code);
        Assert.AreEqual("b", S(batch.GetFromBatch(B("b"))));
    }

    [TestMethod]
    public void TestGetFromBatchMergeAndOverwrite()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = OpenDb(fs);
        ColumnFamily cf = db.DefaultColumnFamily;
        foreach (bool overwrite in new[] { false, true })
        {
            using var batch = new WriteBatchWithIndex(Comparator.Bytewise, 0, overwrite, 0);
            Assert.IsNull(GetFromBatch(batch, cf, "x"));
            batch.Put(cf, B("x"), B("X"));
            string expected = "X";
            for (int i = 0; i < 5; i++)
            {
                batch.Merge(cf, B("x"), B(i.ToString()));
                expected += "," + i;
                if (i % 2 == 0) batch.Put(cf, B("y"), B((i / 2).ToString()));
                batch.Merge(cf, B("z"), B("z"));
                Assert.AreEqual(expected, GetFromBatch(batch, cf, "x"));
                Assert.AreEqual((i / 2).ToString(), GetFromBatch(batch, cf, "y"));
                Assert.AreEqual(StatusCode.MergeInProgress, Assert.ThrowsExactly<RocksDbException>(() => GetFromBatch(batch, cf, "z")).Code);
            }
            // TestGetFromBatchMerge2: Put after merges, Delete + merge.
            batch.Put(cf, B("x"), B("new"));
            Assert.AreEqual("new", GetFromBatch(batch, cf, "x"));
            batch.Delete(cf, B("x")).Merge(cf, B("x"), B("after"));
            Assert.AreEqual("after", GetFromBatch(batch, cf, "x"));
            // The index size depends on overwrite_key: one entry per Put/Delete key.
            int entries = 0;
            WriteBatchWithIndexIterator it = batch.NewIterator(cf);
            for (it.SeekToFirst(); it.Valid; it.Next()) entries++;
            // x: Put + 5 merges (the later Put and Delete overwrite the newest entry) + 1 merge; y: 1; z: 5 merges.
            Assert.AreEqual(overwrite ? 7 + 1 + 5 : batch.Count, entries, "overwrite=" + overwrite);
        }
    }

    [TestMethod]
    public void TestGetFromBatchAndDB()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = OpenDb(fs);
        db.Put(B("a"), B("a"));
        db.Put(B("b"), B("b"));
        db.Put(B("c"), B("c"));
        db.Put(B("m"), B("base"));
        db.Flush();
        db.Put(B("n"), B("mem"));
        using var batch = new WriteBatchWithIndex();
        batch.Put(B("a"), B("batch.a")).Delete(B("b"));
        batch.Merge(db.DefaultColumnFamily, B("m"), B("m1")).Merge(db.DefaultColumnFamily, B("m"), B("m2"));
        batch.Merge(db.DefaultColumnFamily, B("n"), B("n1"));
        batch.Merge(db.DefaultColumnFamily, B("new"), B("only"));
        Assert.AreEqual("batch.a", S(batch.GetFromBatchAndDB(db, B("a"))));
        Assert.IsNull(batch.GetFromBatchAndDB(db, B("b")));
        Assert.AreEqual("c", S(batch.GetFromBatchAndDB(db, B("c"))));
        Assert.IsNull(batch.GetFromBatchAndDB(db, B("x")));
        Assert.AreEqual("base,m1,m2", S(batch.GetFromBatchAndDB(db, B("m"))));
        Assert.AreEqual("mem,n1", S(batch.GetFromBatchAndDB(db, B("n"))));
        Assert.AreEqual("only", S(batch.GetFromBatchAndDB(db, B("new"))));
        // Snapshot read options are honored for the DB part.
        using Snapshot snap = db.GetSnapshot();
        db.Put(B("c"), B("c2"));
        Assert.AreEqual("c", S(batch.GetFromBatchAndDB(db, new ReadOptions { Snapshot = snap }, null, B("c"))));
        Assert.AreEqual("c2", S(batch.GetFromBatchAndDB(db, B("c"))));
        // Write the batch and compare with a plain DB read.
        db.Write(batch);
        Assert.AreEqual("batch.a", GetString(db, "a"));
        Assert.IsNull(GetString(db, "b"));
        Assert.AreEqual("base,m1,m2", GetString(db, "m"));
        Assert.AreEqual("mem,n1", GetString(db, "n"));
    }

    /// <summary>WriteBatchWithIndexTest.MultiGetFromBatchAndDB-style cases: batch values, deletes, merges over DB values.</summary>
    [TestMethod]
    public void TestMultiGetFromBatchAndDB()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = OpenDb(fs);
        db.Put(B("a"), B("a"));
        db.Put(B("b"), B("b"));
        db.Put(B("c"), B("c"));
        db.Put(B("m"), B("base"));
        db.Flush();
        db.Put(B("n"), B("mem"));
        using var batch = new WriteBatchWithIndex();
        batch.Put(B("a"), B("batch.a")).Delete(B("b"));
        batch.Merge(db.DefaultColumnFamily, B("m"), B("m1")).Merge(db.DefaultColumnFamily, B("m"), B("m2"));
        batch.Merge(db.DefaultColumnFamily, B("n"), B("n1"));
        batch.Merge(db.DefaultColumnFamily, B("new"), B("only"));
        batch.Delete(B("d")).Merge(db.DefaultColumnFamily, B("d"), B("after-delete"));
        string[] keys = ["a", "b", "c", "x", "m", "n", "new", "d", "a"];
        string?[] expected = ["batch.a", null, "c", null, "base,m1,m2", "mem,n1", "only", "after-delete", "batch.a"];
        using var multi = new MultiGetBatch();
        foreach (string k in keys) multi.Add(B(k));
        for (int round = 0; round < 2; round++)
        {
            batch.MultiGetFromBatchAndDB(db, multi);
            for (int i = 0; i < keys.Length; i++)
            {
                Assert.AreEqual(expected[i], multi.TryGetValue(i, out ReadOnlySpan<byte> v) ? Encoding.UTF8.GetString(v) : null, keys[i]);
                Assert.AreEqual(S(batch.GetFromBatchAndDB(db, B(keys[i]))), expected[i], "single get " + keys[i]);
            }
            Assert.AreEqual(expected.Count(e => e is not null), multi.FoundCount);
        }
        // The DB part honors the snapshot of the read options.
        using Snapshot snap = db.GetSnapshot();
        db.Put(B("c"), B("c2"));
        db.Put(B("m"), B("base2"));
        batch.MultiGetFromBatchAndDB(db, new ReadOptions { Snapshot = snap }, null, multi);
        Assert.AreEqual("c", S(multi.GetValue(2).ToArray()));
        Assert.AreEqual("base,m1,m2", S(multi.GetValue(4).ToArray()));
        batch.MultiGetFromBatchAndDB(db, multi);
        Assert.AreEqual("c2", S(multi.GetValue(2).ToArray()));
        Assert.AreEqual("base2,m1,m2", S(multi.GetValue(4).ToArray()));

        // Merges without a merge operator fail per key; other keys are still answered.
        using RocksDatabase plain = RocksDatabase.Open("/plain", NewOptions(fs));
        using var b2 = new WriteBatchWithIndex();
        b2.Merge(plain.DefaultColumnFamily, B("k"), B("v")).Put(B("p"), B("1"));
        using var m2 = new MultiGetBatch();
        m2.Add(B("k")).Add(B("p"));
        b2.MultiGetFromBatchAndDB(plain, m2);
        Assert.AreEqual(MultiGetStatus.Error, m2.GetStatus(0));
        Assert.AreEqual(MultiGetStatus.Found, m2.GetStatus(1));
    }

    [TestMethod]
    public void TestIteratorWithBase()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        o.CreateMissingColumnFamilies = true;
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, [new("default", MergeCf()), new("cf1", MergeCf())]);
        ColumnFamily cf1 = db.GetColumnFamily("cf1");
        cf1.Put(B("a"), B("aa"));
        cf1.Put(B("c"), B("cc"));
        cf1.Put(B("e"), B("ee"));
        using var batch = new WriteBatchWithIndex();
        using (BaseDeltaIterator it = batch.NewIteratorWithBase(cf1, cf1.NewIterator(), null))
        {
            it.SeekToFirst();
            AssertIter(it, "a", "aa");
            it.Next();
            AssertIter(it, "c", "cc");
            it.Next();
            AssertIter(it, "e", "ee");
            it.Next();
            Assert.IsFalse(it.Valid);
            it.SeekToLast();
            AssertIter(it, "e", "ee");
            it.Prev();
            AssertIter(it, "c", "cc");
            it.Prev();
            AssertIter(it, "a", "aa");
            it.Prev();
            Assert.IsFalse(it.Valid);
            it.Seek(B("b"));
            AssertIter(it, "c", "cc");
            it.Prev();
            AssertIter(it, "a", "aa");
        }
        // Base: b="", cc, f; batch: a (put), b (delete), c, d (put), e (delete); other CF entries ignored.
        cf1.Delete(B("a"));
        cf1.Delete(B("c"));
        cf1.Delete(B("e"));
        cf1.Put(B("b"), B(""));
        cf1.Put(B("cc"), B("cccc"));
        cf1.Put(B("f"), B("ff"));
        batch.Put(db.DefaultColumnFamily, B("zoo"), B("bar"));
        batch.Put(cf1, B("a"), B("aa")).Delete(cf1, B("b")).Put(cf1, B("c"), B("cc")).Put(cf1, B("d"), B("dd")).Delete(cf1, B("e"));
        using (BaseDeltaIterator it = batch.NewIteratorWithBase(cf1, cf1.NewIterator(), null))
        {
            it.SeekToFirst();
            AssertIter(it, "a", "aa");
            it.Next();
            AssertIter(it, "c", "cc");
            it.Next();
            AssertIter(it, "cc", "cccc");
            it.Next();
            AssertIter(it, "d", "dd");
            it.Next();
            AssertIter(it, "f", "ff");
            it.Next();
            Assert.IsFalse(it.Valid);
            it.SeekToLast();
            AssertIter(it, "f", "ff");
            it.Prev();
            AssertIter(it, "d", "dd");
            it.Prev();
            AssertIter(it, "cc", "cccc");
            it.Prev();
            AssertIter(it, "c", "cc");
            it.Next();
            AssertIter(it, "cc", "cccc");
            it.Prev();
            AssertIter(it, "c", "cc");
            it.Prev();
            AssertIter(it, "a", "aa");
            it.Prev();
            Assert.IsFalse(it.Valid);
            it.Seek(B("c"));
            AssertIter(it, "c", "cc");
            it.Seek(B("cb"));
            AssertIter(it, "cc", "cccc");
            it.Seek(B("cc"));
            AssertIter(it, "cc", "cccc");
            it.Next();
            AssertIter(it, "d", "dd");
            it.Seek(B("e"));
            AssertIter(it, "f", "ff");
            it.Prev();
            AssertIter(it, "d", "dd");
            it.Next();
            AssertIter(it, "f", "ff");
        }
    }

    [TestMethod]
    public void TestBoundsCheckingInDeltaIterator()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = OpenDb(fs);
        using var batch = new WriteBatchWithIndex();
        batch.Put(B("a"), B("aa")).Put(B("b"), B("bb")).Put(B("c"), B("cc"));
        foreach ((string lo, string hi) in new[] { ("b", "c"), ("a.1", "c"), ("b", "b.2") })
        {
            var ro = new ReadOptions { IterateLowerBound = B(lo), IterateUpperBound = B(hi) };
            using BaseDeltaIterator it = batch.NewIteratorWithBase(db.DefaultColumnFamily, db.NewIterator(ro), ro);
            it.SeekToFirst();
            Assert.AreEqual("b", S(it.Key.ToArray()));
            it.Prev();
            Assert.IsFalse(it.Valid);
            it.SeekToLast();
            Assert.AreEqual("b", S(it.Key.ToArray()));
            it.Next();
            Assert.IsFalse(it.Valid);
            it.Seek(B("a"));
            Assert.AreEqual("b", S(it.Key.ToArray()));
            it.Seek(B("b"));
            Assert.AreEqual("b", S(it.Key.ToArray()));
            it.Seek(B("c"));
            Assert.IsFalse(it.Valid);
            it.SeekForPrev(B("c"));
            Assert.AreEqual("b", S(it.Key.ToArray()));
            it.SeekForPrev(B("b"));
            Assert.AreEqual("b", S(it.Key.ToArray()));
            it.SeekForPrev(B("a"));
            Assert.IsFalse(it.Valid);
            it.SeekForPrev(B("a.1"));
            Assert.IsFalse(it.Valid);
            it.Seek(B("b.1"));
            Assert.IsFalse(it.Valid);
        }
    }

    [TestMethod]
    public void TestIteratorWithBaseReverseComparator()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { Comparator = Comparator.ReverseBytewise });
        db.Put(B("a"), B("aa"));
        db.Put(B("c"), B("cc"));
        using var batch = new WriteBatchWithIndex(Comparator.ReverseBytewise, 0, false, 0);
        batch.Put(db.DefaultColumnFamily, B("b"), B("bb")).Delete(db.DefaultColumnFamily, B("c")).Put(db.DefaultColumnFamily, B("d"), B("dd"));
        using BaseDeltaIterator it = batch.NewIteratorWithBase(db.DefaultColumnFamily, db.NewIterator(), null);
        var keys = new StringBuilder();
        for (it.SeekToFirst(); it.Valid; it.Next()) keys.Append(S(it.Key.ToArray()));
        Assert.AreEqual("dba", keys.ToString());
        keys.Clear();
        for (it.SeekToLast(); it.Valid; it.Prev()) keys.Append(S(it.Key.ToArray()));
        Assert.AreEqual("abd", keys.ToString());
    }

    [TestMethod]
    public void SavePointTest()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = OpenDb(fs);
        ColumnFamily cf = db.DefaultColumnFamily;
        using var batch = new WriteBatchWithIndex();
        Assert.IsFalse(batch.RollbackToSavePoint());
        batch.Put(B("A"), B("a")).Put(B("B"), B("b"));
        batch.SetSavePoint();
        batch.Put(B("A"), B("a2")).Delete(B("B")).Put(B("C"), B("c")).Merge(cf, B("A"), B("m"));
        Assert.AreEqual("a2,m", GetFromBatch(batch, cf, "A"));
        Assert.IsNull(GetFromBatch(batch, cf, "B"));
        batch.SetSavePoint();
        batch.Put(B("D"), B("d"));
        Assert.IsTrue(batch.RollbackToSavePoint());
        Assert.IsNull(GetFromBatch(batch, cf, "D"));
        Assert.AreEqual("c", GetFromBatch(batch, cf, "C"));
        Assert.IsTrue(batch.RollbackToSavePoint());
        Assert.AreEqual("a", GetFromBatch(batch, cf, "A"));
        Assert.AreEqual("b", GetFromBatch(batch, cf, "B"));
        Assert.IsNull(GetFromBatch(batch, cf, "C"));
        Assert.AreEqual(2, batch.Count);
        batch.SetSavePoint();
        Assert.IsTrue(batch.PopSavePoint());
        Assert.IsFalse(batch.RollbackToSavePoint());
        batch.Clear();
        Assert.AreEqual(0, batch.Count);
        Assert.IsNull(GetFromBatch(batch, cf, "A"));
    }

    [TestMethod]
    public void DeleteRangeIsNotSupportedAndEntitiesWork()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = OpenDb(fs);
        ColumnFamily cf = db.DefaultColumnFamily;
        using var batch = new WriteBatchWithIndex();
        Assert.AreEqual(StatusCode.NotSupported, Assert.ThrowsExactly<RocksDbException>(() => batch.DeleteRange(null, B("a"), B("b"))).Code);
        batch.PutEntity(cf, B("e"), [new WideColumn(B(""), B("d")), new WideColumn(B("c"), B("1"))]);
        batch.Merge(cf, B("e"), B("m"));
        Assert.AreEqual("d,m", GetFromBatch(batch, cf, "e"));
        var cols = new WideColumns();
        Assert.IsTrue(batch.TryGetEntityFromBatch(cf, B("e"), cols));
        Assert.AreEqual(2, cols.Count);
        Assert.AreEqual("d,m", S(cols.DefaultColumnValue.ToArray()));
        using (BaseDeltaIterator it = batch.NewIteratorWithBase(cf, db.NewIterator(), null))
        {
            it.SeekToFirst();
            Assert.IsTrue(it.IsEntity);
            Assert.AreEqual("d,m", S(it.Value.ToArray()));
            Assert.AreEqual(2, it.Columns.Count);
        }
        db.Write(batch);
        Assert.AreEqual(2, db.GetEntity(B("e"))!.Count);
        Assert.AreEqual("d,m", GetString(db, "e"));
    }

    [TestMethod]
    public void RandomizedBaseDeltaIteratorAgainstModel()
    {
        int seed = Environment.TickCount;
        var rnd = new Random(seed);
        string ctx = "seed=" + seed;
        for (int round = 0; round < 6; round++)
        {
            MemoryFileSystem fs = NewFs();
            using RocksDatabase db = OpenDb(fs);
            ColumnFamily cf = db.DefaultColumnFamily;
            var model = new SortedDictionary<string, string>(StringComparer.Ordinal);
            string K(int i) => "k" + i.ToString("D3");
            for (int i = 0; i < 60; i++)
            {
                string k = K(rnd.Next(80));
                string v = "b" + i;
                db.Put(B(k), B(v));
                model[k] = v;
            }
            if (round % 2 == 0) db.Flush();
            using var batch = new WriteBatchWithIndex(Comparator.Bytewise, 0, overwriteKey: round % 3 == 0, 0);
            for (int i = 0; i < 80; i++)
            {
                string k = K(rnd.Next(80));
                switch (rnd.Next(4))
                {
                    case 0:
                        batch.Put(cf, B(k), B("p" + i));
                        model[k] = "p" + i;
                        break;
                    case 1:
                        batch.Delete(cf, B(k));
                        model.Remove(k);
                        break;
                    default:
                        batch.Merge(cf, B(k), B("m" + i));
                        model[k] = model.TryGetValue(k, out string? old) ? old + ",m" + i : "m" + i;
                        break;
                }
            }
            var expected = model.Select(static kv => kv.Key + "=" + kv.Value).ToList();
            using BaseDeltaIterator it = batch.NewIteratorWithBase(cf, db.NewIterator(), null);
            var fwd = new List<string>();
            for (it.SeekToFirst(); it.Valid; it.Next()) fwd.Add(S(it.Key.ToArray()) + "=" + S(it.Value.ToArray()));
            CollectionAssert.AreEqual(expected, fwd, ctx);
            var bwd = new List<string>();
            for (it.SeekToLast(); it.Valid; it.Prev()) bwd.Add(S(it.Key.ToArray()) + "=" + S(it.Value.ToArray()));
            bwd.Reverse();
            CollectionAssert.AreEqual(expected, bwd, ctx);
            // Random seeks with direction changes.
            for (int i = 0; i < 40; i++)
            {
                string target = K(rnd.Next(85));
                it.Seek(B(target));
                string? exp = model.Keys.FirstOrDefault(k => string.CompareOrdinal(k, target) >= 0);
                Assert.AreEqual(exp, it.Valid ? S(it.Key.ToArray()) : null, ctx + " seek " + target);
                if (it.Valid)
                {
                    it.Prev();
                    string? prev = model.Keys.LastOrDefault(k => string.CompareOrdinal(k, exp!) < 0);
                    Assert.AreEqual(prev, it.Valid ? S(it.Key.ToArray()) : null, ctx + " prev after seek " + target);
                }
                it.SeekForPrev(B(target));
                string? expPrev = model.Keys.LastOrDefault(k => string.CompareOrdinal(k, target) <= 0);
                Assert.AreEqual(expPrev, it.Valid ? S(it.Key.ToArray()) : null, ctx + " seekforprev " + target);
                if (it.Valid)
                {
                    it.Next();
                    string? next = model.Keys.FirstOrDefault(k => string.CompareOrdinal(k, expPrev!) > 0);
                    Assert.AreEqual(next, it.Valid ? S(it.Key.ToArray()) : null, ctx + " next after seekforprev " + target);
                }
            }
            foreach (KeyValuePair<string, string> kv in model) Assert.AreEqual(kv.Value, S(batch.GetFromBatchAndDB(db, B(kv.Key))), ctx + " " + kv.Key);
            db.Write(batch);
            using Iterator dbIt = db.NewIterator();
            var after = new List<string>();
            for (dbIt.SeekToFirst(); dbIt.Valid; dbIt.Next()) after.Add(S(dbIt.Key.ToArray()) + "=" + S(dbIt.Value.ToArray()));
            CollectionAssert.AreEqual(expected, after, ctx);
        }
    }

    /// <summary>
    /// Batch merges on top of a DB value that is an unresolved merge chain spanning two table files and the memtable
    /// (GetFromBatchAndDB, MultiGetFromBatchAndDB, GetEntityFromBatchAndDB and the base-delta iterator), also read at a
    /// snapshot taken in the middle of the chain.
    /// </summary>
    [TestMethod]
    public void BatchMergesOverDbMergeChainAcrossMemtableAndFiles()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { MergeOperator = MergeOperator.StringAppend(",") });
        db.Put(B("k"), B("base"));
        db.Merge(B("k"), B("f1"));
        db.Flush();
        db.Merge(B("k"), B("f2"));
        db.Merge(B("n"), B("n1"));
        db.Flush();
        using Snapshot snap = db.GetSnapshot();
        db.Merge(B("k"), B("m1"));
        db.Merge(B("n"), B("n2"));
        using var batch = new WriteBatchWithIndex(Comparator.Bytewise, 0, true, 0);
        batch.Merge(B("k"), B("b1"));
        batch.Merge(B("k"), B("b2"));
        batch.Merge(B("n"), B("b"));
        foreach ((ReadOptions? ro, string k, string n) in new (ReadOptions?, string, string)[]
                 {
                     (null, "base,f1,f2,m1,b1,b2", "n1,n2,b"),
                     (new ReadOptions { Snapshot = snap }, "base,f1,f2,b1,b2", "n1,b"),
                 })
        {
            string ctx = ro is null ? "latest" : "snapshot";
            Assert.AreEqual(k, S(batch.GetFromBatchAndDB(db, ro, null, B("k"))), ctx);
            Assert.AreEqual(n, S(batch.GetFromBatchAndDB(db, ro, null, B("n"))), ctx);
            using (var mg = new MultiGetBatch())
            {
                mg.Add(B("n")).Add(B("k")).Add(B("absent"));
                batch.MultiGetFromBatchAndDB(db, ro, null, mg);
                Assert.AreEqual(n, S(mg.GetValue(0).ToArray()), ctx);
                Assert.AreEqual(k, S(mg.GetValue(1).ToArray()), ctx);
                Assert.AreEqual(MultiGetStatus.NotFound, mg.GetStatus(2), ctx);
            }
            WideColumns? e = batch.GetEntityFromBatchAndDB(db, ro, null, B("k"));
            Assert.IsNotNull(e, ctx);
            Assert.AreEqual(k, S(e.DefaultColumnValue.ToArray()), ctx);
            using BaseDeltaIterator it = batch.NewIteratorWithBase(db.DefaultColumnFamily, db.NewIterator(ro), ro);
            var seen = new List<string>();
            for (it.SeekToFirst(); it.Valid; it.Next()) seen.Add(S(it.Key.ToArray()) + "=" + S(it.Value.ToArray()));
            CollectionAssert.AreEqual(new[] { "k=" + k, "n=" + n }, seen, ctx);
            seen.Clear();
            for (it.SeekToLast(); it.Valid; it.Prev()) seen.Add(S(it.Key.ToArray()) + "=" + S(it.Value.ToArray()));
            CollectionAssert.AreEqual(new[] { "n=" + n, "k=" + k }, seen, ctx);
        }
        db.Write(batch.WriteBatch);
        db.CompactRange();
        Assert.AreEqual("base,f1,f2,m1,b1,b2", S(db.Get(B("k"))));
        Assert.AreEqual("n1,n2,b", S(db.Get(B("n"))));
    }
}
