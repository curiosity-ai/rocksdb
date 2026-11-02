using System.Diagnostics;
using System.Reflection;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>Batched MultiGet (<see cref="MultiGetBatch"/>) against single-key Get (spirit of db/db_basic_test.cc MultiGet tests).</summary>
[TestClass]
public sealed class DbMultiGetTests
{
    private static ColumnFamilyOptions SmallLsmOptions() => new()
    {
        MergeOperator = new StringAppendMergeOperator(),
        WriteBufferSize = 64 << 10,
        TargetFileSizeBase = 64 << 10,
        MaxBytesForLevelBase = 256 << 10,
    };

    /// <summary>Asserts that every key of <paramref name="batch"/> has the outcome of a single-key Get.</summary>
    private static void AssertMatchesGet(RocksDatabase db, ReadOptions? options, MultiGetBatch batch)
    {
        int found = 0;
        for (int i = 0; i < batch.Count; i++)
        {
            byte[]? expected = db.Get(options, batch.GetKey(i));
            if (expected is null)
            {
                Assert.AreEqual(MultiGetStatus.NotFound, batch.GetStatus(i), "key " + S(batch.GetKey(i).ToArray()));
                Assert.IsTrue(batch.GetValue(i).IsEmpty);
            }
            else
            {
                Assert.AreEqual(MultiGetStatus.Found, batch.GetStatus(i), "key " + S(batch.GetKey(i).ToArray()));
                CollectionAssert.AreEqual(expected, batch.GetValue(i).ToArray());
                CollectionAssert.AreEqual(expected, batch.GetValueMemory(i).ToArray());
                found++;
            }
            Assert.IsNull(batch.GetError(i));
        }
        Assert.AreEqual(found, batch.FoundCount);
    }

    private static void FillBatch(MultiGetBatch batch, Random rnd, int keySpace, int count)
    {
        batch.Clear();
        for (int i = 0; i < count; i++) batch.Add(Key(rnd.Next(keySpace + 10))); // a few keys past the key space
    }

    [TestMethod]
    public async Task RandomizedAgainstGetAcrossMemtablesAndLevels()
    {
        int seed = Environment.TickCount;
        string trace = "seed=" + seed;
        var rnd = new Random(seed);
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), SmallLsmOptions());
        const int keySpace = 3000;
        using var batch = new MultiGetBatch();
        Snapshot? snap = null;
        for (int round = 0; round < 6; round++)
        {
            for (int i = 0; i < 2000; i++)
            {
                int k = rnd.Next(keySpace);
                switch (rnd.Next(10))
                {
                    case 0: db.Delete(Key(k)); break;
                    case 1: db.Merge(Key(k), B("m" + round)); break;
                    case 2 when i % 400 == 0: db.DeleteRange(Key(k), Key(k + 20)); break;
                    default: db.Put(Key(k), Value(k, round)); break;
                }
            }
            if (round == 2) snap = db.GetSnapshot();
            if (round % 2 == 1) db.Flush();
            foreach (ReadOptions? ro in new[] { null, snap is null ? null : new ReadOptions { Snapshot = snap } })
            {
                for (int b = 0; b < 5; b++)
                {
                    FillBatch(batch, rnd, keySpace, 1 + rnd.Next(64));
                    db.MultiGet(ro, batch);
                    AssertMatchesGet(db, ro, batch);
                    await db.MultiGetAsync(ro, batch);
                    AssertMatchesGet(db, ro, batch);
                }
            }
        }
        db.CompactRange();
        FillBatch(batch, rnd, keySpace, 200);
        db.MultiGet(batch);
        AssertMatchesGet(db, null, batch);
        snap?.Dispose();
        Assert.IsTrue(db.GetProperty("rocksdb.num-files-at-level0") is not null, trace);
    }

    [TestMethod]
    public void DuplicateAndUnsortedKeysKeepInsertionOrder()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        db.Put(B("b"), B("2"));
        db.Put(B("a"), B("1"));
        db.Flush();
        db.Put(B("c"), B("3"));
        using var batch = new MultiGetBatch(1);
        batch.Add(B("c")).Add(B("a")).Add(B("zz")).Add(B("a")).Add(B("b"));
        db.MultiGet(batch);
        Assert.AreEqual(5, batch.Count);
        Assert.AreEqual(4, batch.FoundCount);
        Assert.AreEqual("3", S(batch.GetValue(0).ToArray()));
        Assert.AreEqual("1", S(batch.GetValue(1).ToArray()));
        Assert.AreEqual(MultiGetStatus.NotFound, batch.GetStatus(2));
        Assert.AreEqual("1", S(batch.GetValue(3).ToArray()));
        Assert.AreEqual("2", S(batch.GetValue(4).ToArray()));
        Assert.AreEqual("zz", S(batch.GetKey(2).ToArray()));

        // Adding a key invalidates the results of the previous run.
        batch.Add(B("b"));
        Assert.AreEqual(MultiGetStatus.NotFound, batch.GetStatus(0));
        batch.Clear();
        Assert.AreEqual(0, batch.Count);
        db.MultiGet(batch); // empty batch is a no-op
        Assert.ThrowsExactly<ArgumentOutOfRangeException>(() => batch.GetStatus(0));
    }

    [TestMethod]
    public void MemtableTierReportsIncompleteMergePerKey()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { MergeOperator = new StringAppendMergeOperator() });
        db.Put(B("base"), B("v"));
        db.Put(B("flushed"), B("f"));
        db.Flush();
        db.Merge(B("base"), B("m"));
        db.Put(B("mem"), B("x"));
        using var batch = new MultiGetBatch();
        batch.Add(B("base")).Add(B("mem")).Add(B("flushed"));
        db.MultiGet(new ReadOptions { ReadTier = ReadTier.MemtableTier }, batch);
        Assert.AreEqual(MultiGetStatus.Error, batch.GetStatus(0));
        Assert.AreEqual(StatusCode.Incomplete, batch.GetError(0)!.Code);
        Assert.AreEqual("x", S(batch.GetValue(1).ToArray()));
        Assert.AreEqual(MultiGetStatus.NotFound, batch.GetStatus(2));

        db.MultiGet(batch);
        Assert.AreEqual("v,m", S(batch.GetValue(0).ToArray()));
        Assert.AreEqual("f", S(batch.GetValue(2).ToArray()));
        Assert.AreEqual(3, batch.FoundCount);
    }

    [TestMethod]
    public void RecordsMultiGetStatistics()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        var st = new Statistics();
        o.Statistics = st;
        using RocksDatabase db = RocksDatabase.Open(DbPath, o);
        db.Put(B("a"), B("12345"));
        db.Flush();
        db.Put(B("b"), B("xy"));
        using var batch = new MultiGetBatch();
        batch.Add(B("a")).Add(B("b")).Add(B("c"));
        db.MultiGet(batch);
        Assert.AreEqual(1UL, st.GetTickerCount(Ticker.NumberMultigetCalls));
        Assert.AreEqual(3UL, st.GetTickerCount(Ticker.NumberMultigetKeysRead));
        Assert.AreEqual(2UL, st.GetTickerCount(Ticker.NumberMultigetKeysFound));
        Assert.AreEqual(7UL, st.GetTickerCount(Ticker.NumberMultigetBytesRead));
    }

    [TestMethod]
    public void LegacyArrayOverloadUsesBatch()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        for (int i = 0; i < 100; i++) db.Put(Key(i), Value(i, 0));
        db.Flush();
        byte[]?[] r = db.MultiGet([Key(5), Key(500), Key(7)]);
        CollectionAssert.AreEqual(Value(5, 0), r[0]);
        Assert.IsNull(r[1]);
        CollectionAssert.AreEqual(Value(7, 0), r[2]);
        Assert.AreEqual(0, db.MultiGet(Array.Empty<byte[]>()).Length);
    }

    [TestMethod]
    public void SteadyStateDoesNotAllocate()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        for (int i = 0; i < 2000; i++) db.Put(Key(i), Value(i, 0));
        db.Flush();
        for (int i = 0; i < 2000; i += 3) db.Put(Key(i), Value(i, 1));
        using var batch = new MultiGetBatch(32);
        for (int i = 0; i < 32; i++) batch.Add(Key(i * 61 % 2100));
        for (int i = 0; i < 20; i++) db.MultiGet(batch); // warm up (block cache, pools, JIT)
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 50; i++) db.MultiGet(batch);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        // A Debug build of the library compiles async state machines (the batched lookup runs through one with
        // synchronous: true) as classes, so the zero-allocation guarantee only holds for optimized builds.
        bool optimized = typeof(RocksDatabase).Assembly.GetCustomAttribute<DebuggableAttribute>() is not { IsJITOptimizerDisabled: true };
        if (optimized) Assert.AreEqual(0L, allocated);
        AssertMatchesGet(db, null, batch);
    }
}
