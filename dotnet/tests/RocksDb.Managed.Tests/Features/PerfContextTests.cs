using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Features;

/// <summary>PerfContext / IOStatsContext (port of the spirit of db/perf_context_test.cc).</summary>
[TestClass]
public sealed class PerfContextTests
{
    /// <summary>Runs <paramref name="body"/> at <paramref name="level"/> with fresh contexts, restoring Disable afterwards.</summary>
    private static void AtLevel(PerfLevel level, Action body)
    {
        PerfContext.Level = level;
        try
        {
            PerfContext.Current.Reset();
            IOStatsContext.Current.Reset();
            body();
        }
        finally
        {
            PerfContext.Level = PerfLevel.Disable;
        }
    }

    private static ulong P(PerfMetric m) => PerfContext.Current[m];

    private static RocksDatabase OpenFilled(IFileSystem fs, string path, int n)
    {
        var o = new DbOptions { CreateIfMissing = true, FileSystem = fs };
        var cf = new ColumnFamilyOptions
        {
            Compression = RocksDb.Managed.Compression.CompressionType.None,
            TableOptions = new BlockBasedTableOptions { FilterPolicy = new RocksDb.Managed.Table.Filters.BloomFilterPolicy(10) },
        };
        RocksDatabase db = RocksDatabase.Open(path, o, cf);
        for (int i = 0; i < n; i++) db.Put(Key(i), Value(i, 0));
        db.Flush();
        return db;
    }

    [TestMethod]
    public void DisabledByDefaultAndNothingCounted()
    {
        Assert.AreEqual(PerfLevel.Disable, PerfContext.Level);
        using RocksDatabase db = OpenFilled(NewFs(), DbPath, 100);
        PerfContext.Current.Reset();
        Assert.IsNotNull(db.Get(Key(5)));
        Assert.AreEqual("", PerfContext.Current.ToString(excludeZeroCounters: true));
        var e = Assert.ThrowsExactly<ArgumentOutOfRangeException>(() => PerfContext.Level = PerfLevel.Uninitialized);
        Assert.AreEqual("value", e.ParamName);
    }

    [TestMethod]
    public void GetCountsComparisonsMemtablesBlocksAndFilters()
    {
        using RocksDatabase db = OpenFilled(NewFs(), DbPath, 1000);
        db.Put(B("in-mem"), B("v"));
        AtLevel(PerfLevel.EnableCount, () =>
        {
            Assert.IsNotNull(db.Get(Key(500)));
            Assert.AreEqual(1UL, P(PerfMetric.GetFromMemtableCount), "one memtable searched");
            Assert.IsTrue(P(PerfMetric.UserKeyComparisonCount) > 0);
            Assert.AreEqual(1UL, P(PerfMetric.BloomSstHitCount));
            Assert.IsTrue(P(PerfMetric.BlockReadCount) >= 1, PerfContext.Current.ToString(true));
            Assert.IsTrue(P(PerfMetric.BlockReadByte) > 0);
            Assert.AreEqual(0UL, P(PerfMetric.BlockReadTime), "times need EnableTimeExceptForMutex");

            PerfContext.Current.Reset();
            Assert.IsNotNull(db.Get(Key(500)));
            Assert.IsTrue(P(PerfMetric.BlockCacheHitCount) >= 1, "the data block is cached now");
            Assert.AreEqual(0UL, P(PerfMetric.BlockReadCount));

            PerfContext.Current.Reset();
            Assert.IsNull(db.Get(B("key000500x"))); // inside the file's key range: the filter is consulted
            Assert.AreEqual(1UL, P(PerfMetric.BloomSstMissCount));

            PerfContext.Current.Reset();
            Assert.IsNotNull(db.Get(B("in-mem")));
            Assert.AreEqual(0UL, P(PerfMetric.BloomSstHitCount) + P(PerfMetric.BloomSstMissCount), "found in the memtable");
        });
    }

    [TestMethod]
    public void IterationCountsSeeksNextsAndSkippedEntries()
    {
        using RocksDatabase db = OpenFilled(NewFs(), DbPath, 100);
        for (int i = 0; i < 10; i++) db.Delete(Key(i));
        db.Put(Key(50), Value(50, 1)); // an older version of key 50 is in the table
        AtLevel(PerfLevel.EnableCount, () =>
        {
            using Iterator it = db.NewIterator();
            it.SeekToFirst();
            CollectionAssert.AreEqual(Key(10), it.Key.ToArray());
            Assert.AreEqual(1UL, P(PerfMetric.IterSeekCount));
            Assert.AreEqual(10UL, P(PerfMetric.InternalDeleteSkippedCount), PerfContext.Current.ToString(true));
            Assert.IsTrue(P(PerfMetric.InternalKeySkippedCount) >= 10, "the deleted keys' table entries");
            PerfContext.Current.Reset();
            it.Seek(Key(50));
            Assert.AreEqual(1UL, P(PerfMetric.IterSeekCount));
            Assert.AreEqual(2UL, P(PerfMetric.SeekChildSeekCount), "memtable + L0 file");
            Assert.AreEqual(1UL, P(PerfMetric.SeekOnMemtableCount));
            it.Next();
            Assert.AreEqual(1UL, P(PerfMetric.IterNextCount));
            CollectionAssert.AreEqual(Key(51), it.Key.ToArray());
            Assert.IsTrue(P(PerfMetric.InternalKeySkippedCount) >= 1, "the older version of key 50");
            it.Prev();
            Assert.AreEqual(1UL, P(PerfMetric.IterPrevCount));
        });
    }

    [TestMethod]
    public void TimesAndWritePathAtEnableTime()
    {
        using RocksDatabase db = OpenFilled(NewFs(), DbPath, 100);
        AtLevel(PerfLevel.EnableTimeExceptForMutex, () =>
        {
            db.Put(B("k"), B("v"));
            Assert.IsTrue(P(PerfMetric.WriteWalTime) > 0, PerfContext.Current.ToString(true));
            Assert.IsTrue(P(PerfMetric.WriteMemtableTime) > 0);
            Assert.IsNotNull(db.Get(Key(7)));
            Assert.IsTrue(P(PerfMetric.GetFromMemtableTime) > 0);
            Assert.IsTrue(P(PerfMetric.GetFromOutputFilesTime) > 0);
            using Iterator it = db.NewIterator();
            it.Seek(Key(3));
            Assert.IsTrue(P(PerfMetric.SeekInternalSeekTime) > 0);
        });
    }

    [TestMethod]
    public void ToStringListsUpstreamNamesInOrder()
    {
        using RocksDatabase db = OpenFilled(NewFs(), DbPath, 10);
        AtLevel(PerfLevel.EnableCount, () =>
        {
            Assert.IsNotNull(db.Get(Key(1)));
            string all = PerfContext.Current.ToString();
            StringAssert.StartsWith(all, "user_key_comparison_count = ");
            StringAssert.Contains(all, ", block_cache_hit_count = ");
            Assert.IsTrue(all.EndsWith("multiscan_io_coalesced_nonadjacent = 0", StringComparison.Ordinal), all[^60..]);
            string nonZero = PerfContext.Current.ToString(excludeZeroCounters: true);
            StringAssert.Contains(nonZero, "get_from_memtable_count = 1");
            Assert.IsFalse(nonZero.Contains(" = 0", StringComparison.Ordinal), nonZero);
        });
    }

    [TestMethod]
    public void ContextsAreThreadLocal()
    {
        using RocksDatabase db = OpenFilled(NewFs(), DbPath, 10);
        AtLevel(PerfLevel.EnableCount, () =>
        {
            var t = new Thread(() =>
            {
                // This thread is at the default level: nothing is counted for it, nothing leaks into the other.
                Assert.AreEqual(PerfLevel.Disable, PerfContext.Level);
                for (int i = 0; i < 10; i++) db.Get(Key(i));
                Assert.AreEqual(0UL, PerfContext.Current[PerfMetric.GetFromMemtableCount]);
            });
            t.Start();
            t.Join();
            Assert.AreEqual(0UL, P(PerfMetric.GetFromMemtableCount));
            db.Get(Key(1));
            Assert.AreEqual(1UL, P(PerfMetric.GetFromMemtableCount));
        });
    }

    [TestMethod]
    public void IOStatsCountsFileBytesAndSyncTime()
    {
        string dir = Path.Combine(Path.GetTempPath(), "rocksdb-managed-perf-" + Guid.NewGuid().ToString("N"));
        try
        {
            AtLevel(PerfLevel.EnableTimeExceptForMutex, () =>
            {
                using RocksDatabase db = OpenFilled(LocalFileSystem.Instance, dir, 200);
                IOStatsContext io = IOStatsContext.Current;
                Assert.IsTrue(io[IOStatsMetric.BytesWritten] > 0, io.ToString(true));
                io.Reset();
                db.Put(new WriteOptions { Sync = true }, B("k"), B("v"));
                Assert.IsTrue(io[IOStatsMetric.BytesWritten] > 0);
                Assert.IsTrue(io[IOStatsMetric.FsyncNanos] > 0, io.ToString(true));
                io.Reset();
                Assert.IsNotNull(db.Get(new ReadOptions { FillCache = false }, Key(100)));
                Assert.IsTrue(io[IOStatsMetric.BytesRead] > 0, io.ToString(true));
                StringAssert.StartsWith(io.ToString(), "bytes_read = ");
            });
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch (IOException) { }
        }
    }
}
