using RocksDb.Managed.Db;
using RocksDb.Managed.Tests.Db;
using RocksDb.Managed.Cache;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Cache;

/// <summary>
/// DBOptions::row_cache: ports of DBTest.RowCache (db_test.cc), DBTest2.RowCacheSnapshot (db_etc2_test.cc),
/// DBRangeDelTest.RowCache (db_range_del_test.cc) and DBMultiGetRowCacheTest.MultiGetBatched (db_basic_test.cc).
/// </summary>
[TestClass]
public sealed class RowCacheTests
{
    private static (RocksDatabase Db, Statistics Stats) Open(MemoryFileSystem fs, RowCache cache, ColumnFamilyOptions? cfo = null)
    {
        var st = new Statistics();
        DbOptions o = NewOptions(fs);
        o.Statistics = st;
        o.RowCache = cache;
        return (RocksDatabase.Open(DbPath, o, cfo ?? new ColumnFamilyOptions()), st);
    }

    private static (ulong Hit, ulong Miss) Ticks(Statistics st) => (st.GetTickerCount(Ticker.RowCacheHit), st.GetTickerCount(Ticker.RowCacheMiss));

    [TestMethod]
    public void RowCache()
    {
        (RocksDatabase db, Statistics st) = Open(NewFs(), new RowCache(8192));
        using (db)
        {
            db.Put(B("foo"), B("bar"));
            db.Flush();
            Assert.AreEqual((0UL, 0UL), Ticks(st));
            Assert.AreEqual("bar", GetString(db, "foo"));
            Assert.AreEqual((0UL, 1UL), Ticks(st));
            Assert.AreEqual("bar", GetString(db, "foo"));
            Assert.AreEqual((1UL, 1UL), Ticks(st));
            // Async lookups use the row cache too.
            Assert.AreEqual("bar", S(db.GetAsync(B("foo")).AsTask().GetAwaiter().GetResult()));
            Assert.AreEqual((2UL, 1UL), Ticks(st));
            // Memtable hits never reach the table cache; a key absent from the file is not cached.
            db.Put(B("mem"), B("x"));
            Assert.AreEqual("x", GetString(db, "mem"));
            Assert.IsNull(GetString(db, "fop"));
            Assert.IsNull(GetString(db, "fop"));
            Assert.AreEqual((2UL, 3UL), Ticks(st));
        }
    }

    [TestMethod]
    public void RowCacheSnapshot()
    {
        (RocksDatabase db, Statistics st) = Open(NewFs(), new RowCache(8 * 8192));
        using (db)
        {
            db.Put(B("foo"), B("bar1"));
            using Snapshot s1 = db.GetSnapshot();
            db.Put(B("foo"), B("bar2"));
            db.Flush();
            db.Put(B("foo2"), B("bar"));
            using Snapshot s2 = db.GetSnapshot();
            db.Put(B("foo3"), B("bar"));
            using Snapshot s3 = db.GetSnapshot();

            Assert.AreEqual((0UL, 0UL), Ticks(st));
            Assert.AreEqual("bar2", GetString(db, "foo"));
            Assert.AreEqual((0UL, 1UL), Ticks(st));
            Assert.AreEqual("bar2", GetString(db, "foo"));
            Assert.AreEqual((1UL, 1UL), Ticks(st));
            Assert.AreEqual("bar1", GetString(db, "foo", s1));
            Assert.AreEqual((1UL, 2UL), Ticks(st));
            Assert.AreEqual("bar2", GetString(db, "foo", s2));
            Assert.AreEqual((2UL, 2UL), Ticks(st));
            Assert.AreEqual("bar1", GetString(db, "foo", s1));
            Assert.AreEqual((3UL, 2UL), Ticks(st));
            Assert.AreEqual("bar2", GetString(db, "foo", s3));
            Assert.AreEqual((4UL, 2UL), Ticks(st));
        }
    }

    [TestMethod]
    public void DeleteRangeIsNotSupportedWithRowCache()
    {
        (RocksDatabase db, _) = Open(NewFs(), new RowCache(8 << 10));
        using (db)
        {
            db.Put(B("k3"), B("val"));
            Assert.AreEqual(StatusCode.NotSupported, Assert.ThrowsExactly<RocksDbException>(() => db.DeleteRange(B("k3"), B("k5"))).Code);
            using WriteBatch wb = db.CreateWriteBatch();
            wb.Put(B("k6"), B("abc")).DeleteRange(B("k1"), B("k5"));
            Assert.AreEqual(StatusCode.NotSupported, Assert.ThrowsExactly<RocksDbException>(() => db.Write(wb)).Code);
            Assert.AreEqual("val", GetString(db, "k3"));
            // The check happens before the write: the DB stays writable.
            db.Put(B("k5"), B("foo"));
        }
    }

    [TestMethod]
    public void MergeChainsAndDeletionsReplayFromTheRowCache()
    {
        var cfo = new ColumnFamilyOptions { MergeOperator = new StringAppendMergeOperator() };
        (RocksDatabase db, Statistics st) = Open(NewFs(), new RowCache(1 << 20), cfo);
        using (db)
        {
            db.Put(B("m"), B("a"));
            db.Merge(B("m"), B("b"));
            db.Put(B("d"), B("x"));
            db.Flush();
            db.Merge(B("m"), B("c"));
            db.Delete(B("d"));
            db.Flush();
            for (int pass = 0; pass < 2; pass++)
            {
                Assert.AreEqual("a,b,c", GetString(db, "m"), "pass " + pass);
                Assert.IsNull(GetString(db, "d"), "pass " + pass);
            }
            // pass 0: m misses in both files, d misses in the newer one (its deletion ends the lookup); pass 1 hits.
            Assert.AreEqual((3UL, 3UL), Ticks(st));
        }
    }

    [TestMethod]
    public void MultiGetBatched()
    {
        (RocksDatabase db, Statistics st) = Open(NewFs(), new RowCache(1 << 20));
        using (db)
        {
            for (int i = 0; i < 10; i++) db.Put(Key(i), Value(i, 0));
            db.Flush();
            using var batch = new MultiGetBatch();
            for (int i = 0; i < 10; i += 2) batch.Add(Key(i));
            db.MultiGet(batch);
            Assert.AreEqual((0UL, 5UL), Ticks(st));
            Assert.AreEqual(5, batch.FoundCount);
            // Single-key Gets hit the entries the MultiGet cached, and vice versa.
            Assert.AreEqual(S(Value(4, 0)), S(db.Get(Key(4))));
            Assert.AreEqual((1UL, 5UL), Ticks(st));
            Assert.IsNotNull(db.Get(Key(5)));
            batch.Add(Key(5)).Add(Key(7));
            db.MultiGetAsync(batch).AsTask().GetAwaiter().GetResult();
            Assert.AreEqual(7, batch.FoundCount);
            for (int i = 0; i < batch.Count; i++) Assert.AreEqual(S(db.Get(batch.GetKey(i))), S(batch.GetValue(i).ToArray()));
            Assert.AreEqual((1UL + 6 + 7, 5UL + 1 + 1), Ticks(st));
        }
    }

    [TestMethod]
    public void EvictsByCapacityAndIsSharedAcrossDbs()
    {
        var cache = new RowCache(16 * 1024);
        (RocksDatabase db, _) = Open(NewFs(), cache);
        (RocksDatabase db2, _) = Open(NewFs(), cache);
        using (db)
        using (db2)
        {
            for (int i = 0; i < 500; i++)
            {
                db.Put(Key(i), Value(i, 1));
                db2.Put(Key(i), Value(i, 2));
            }
            db.Flush();
            db2.Flush();
            for (int i = 0; i < 500; i++)
            {
                Assert.AreEqual(S(Value(i, 1)), S(db.Get(Key(i))));
                Assert.AreEqual(S(Value(i, 2)), S(db2.Get(Key(i))));
            }
            Assert.IsTrue(cache.Usage <= cache.Capacity, cache.Usage + " > " + cache.Capacity);
            Assert.IsTrue(cache.Count > 0 && cache.Count < 1000);
            // The two DBs' entries never mix (distinct key prefixes per table cache).
            for (int i = 0; i < 500; i++) Assert.AreEqual(S(Value(i, 2)), S(db2.Get(Key(i))));
            cache.Clear();
            Assert.AreEqual(0, cache.Count);
            Assert.AreEqual(0L, cache.Usage);
        }
    }
}
