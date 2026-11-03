using System.Text.RegularExpressions;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Features;

/// <summary>
/// GET_HIT_L0 / L1 / L2_AND_UP (Version::Get), file read histograms (RandomAccessFileReader: SST_READ_MICROS and the
/// per-activity FILE_READ_*_MICROS), the interval columns of rocksdb.cfstats / rocksdb.dbstats and the no-I/O
/// KeyMayExist (kBlockCacheTier).
/// </summary>
[TestClass]
public sealed class StatisticsDetailTests
{
    private static string WriteSst(IFileSystem fs, string path, params string[] keys)
    {
        using var w = new SstFileWriter(new ColumnFamilyOptions(), fs);
        w.Open(path);
        foreach (string k in keys) w.Put(B(k), B(k + "@" + path));
        w.Finish();
        return path;
    }

    [TestMethod]
    public async Task GetHitTickersCountTheLevelOfTheHitFile()
    {
        MemoryFileSystem fs = NewFs();
        fs.CreateDirectoryIfMissing("/ext");
        var stats = new Statistics();
        DbOptions o = NewOptions(fs);
        o.Statistics = stats;
        var cfo = new ColumnFamilyOptions { NumLevels = 3, LevelCompactionDynamicLevelBytes = false, DisableAutoCompactions = true };
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, cfo);
        // Each ingestion overlaps the previous file: L2, then L1, then L0.
        db.IngestExternalFiles([WriteSst(fs, "/ext/1.sst", "a", "b", "c")], new IngestExternalFileOptions());
        db.IngestExternalFiles([WriteSst(fs, "/ext/2.sst", "b", "c")], new IngestExternalFileOptions());
        db.IngestExternalFiles([WriteSst(fs, "/ext/3.sst", "c")], new IngestExternalFileOptions());
        CollectionAssert.AreEquivalent(new[] { 0, 1, 2 }, db.GetLiveFilesMetadata().Select(f => f.Level).ToArray());
        db.Put(B("m"), B("mem"));

        Assert.AreEqual("a@/ext/1.sst", GetString(db, "a"));
        Assert.AreEqual("b@/ext/2.sst", GetString(db, "b"));
        Assert.AreEqual("c@/ext/3.sst", GetString(db, "c"));
        Assert.AreEqual("mem", GetString(db, "m"));
        Assert.IsNull(GetString(db, "zz"));
        Assert.AreEqual(1UL, stats.GetTickerCount(Ticker.GetHitL0));
        Assert.AreEqual(1UL, stats.GetTickerCount(Ticker.GetHitL1));
        Assert.AreEqual(1UL, stats.GetTickerCount(Ticker.GetHitL2AndUp));
        Assert.AreEqual(1UL, stats.GetTickerCount(Ticker.MemtableHit));

        // Async lookups record them too.
        Assert.IsNotNull(await db.GetAsync(B("a")));
        Assert.IsNotNull(await db.GetAsync(B("c")));
        Assert.AreEqual(2UL, stats.GetTickerCount(Ticker.GetHitL0));
        Assert.AreEqual(2UL, stats.GetTickerCount(Ticker.GetHitL2AndUp));
    }

    [TestMethod]
    public void FileReadHistogramsByActivity()
    {
        MemoryFileSystem fs = NewFs();
        var stats = new Statistics();
        DbOptions o = NewOptions(fs);
        o.Statistics = stats;
        var cfo = new ColumnFamilyOptions { TableOptions = new BlockBasedTableOptions { NoBlockCache = true } };
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o, cfo))
        {
            for (int i = 0; i < 200; i++) db.Put(Key(i), Value(i, 0));
            db.Flush();
        }
        stats.Reset();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o, cfo))
        {
            ulong sstBefore = stats.GetHistogramData(Histogram.SstReadMicros).Count;
            Assert.IsNotNull(db.Get(Key(5)));
            Assert.IsTrue(stats.GetHistogramData(Histogram.FileReadGetMicros).Count > 0);
            Assert.IsTrue(stats.GetHistogramData(Histogram.SstReadMicros).Count > sstBefore);

            using (Iterator it = db.NewIterator())
            {
                int n = 0;
                for (it.SeekToFirst(); it.Valid; it.Next()) n++;
                Assert.AreEqual(200, n);
            }
            Assert.IsTrue(stats.GetHistogramData(Histogram.FileReadDbIteratorMicros).Count > 0);

            using var batch = new MultiGetBatch();
            batch.Add(Key(7));
            batch.Add(Key(150));
            db.MultiGet(batch);
            Assert.IsTrue(stats.GetHistogramData(Histogram.FileReadMultigetMicros).Count > 0);

            for (int i = 0; i < 200; i++) db.Put(Key(i), Value(i, 1));
            db.Flush();
            db.CompactRange();
            Assert.IsTrue(stats.GetHistogramData(Histogram.FileReadCompactionMicros).Count > 0);
        }
        // Without statistics nothing is wrapped (and nothing recorded).
        stats.Reset();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo))
        {
            Assert.IsNotNull(db.Get(Key(5)));
        }
        Assert.AreEqual(0UL, stats.GetHistogramData(Histogram.SstReadMicros).Count);
    }

    private static double Field(string text, string pattern)
    {
        Match m = Regex.Match(text, pattern);
        Assert.IsTrue(m.Success, "no match for " + pattern + " in\n" + text);
        return double.Parse(m.Groups[1].Value, System.Globalization.CultureInfo.InvariantCulture);
    }

    [TestMethod]
    public void CfStatsAndDbStatsReportIntervals()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        for (int i = 0; i < 100; i++) db.Put(Key(i), Value(i, 0));
        db.Flush();
        string first = db.GetProperty("rocksdb.cfstats")!;
        StringAssert.Contains(first, "\n Int ");
        Assert.IsTrue(Field(first, @"Flush\(GB\): cumulative ([0-9.]+)") > 0 || first.Contains("Flush(GB): cumulative 0.000", StringComparison.Ordinal));
        // The flush happened in the first interval only: the next periodic dump's interval is empty.
        for (int i = 0; i < 10; i++) db.Put(Key(i), Value(i, 1));
        string second = db.GetProperty("rocksdb.cfstats")!;
        Assert.AreEqual(0.0, Field(second, @"Flush\(GB\): cumulative [0-9.]+, interval ([0-9.]+)"));
        Assert.AreEqual(0.0, Field(second, @"Interval compaction: ([0-9.]+) GB write"));
        // cfstats-no-file-histogram does not start a new interval.
        db.Flush();
        string noHist1 = db.GetProperty("rocksdb.cfstats-no-file-histogram")!;
        string noHist2 = db.GetProperty("rocksdb.cfstats-no-file-histogram")!;
        double i1 = Field(noHist1, @"Interval compaction: ([0-9.]+) GB write");
        Assert.AreEqual(i1, Field(noHist2, @"Interval compaction: ([0-9.]+) GB write"));

        // dbstats: every dump starts an interval.
        string d1 = db.GetProperty("rocksdb.dbstats")!;
        Assert.AreEqual(110.0, Field(d1, @"Cumulative writes: ([0-9]+) writes"));
        for (int i = 0; i < 5; i++) db.Put(Key(i), Value(i, 2));
        string d2 = db.GetProperty("rocksdb.dbstats")!;
        Assert.AreEqual(115.0, Field(d2, @"Cumulative writes: ([0-9]+) writes"));
        Assert.AreEqual(5.0, Field(d2, @"Interval writes: ([0-9]+) writes"));
        Assert.AreEqual(5.0, Field(d2, @"Interval WAL: ([0-9]+) writes"));
        StringAssert.Contains(d2, "Interval stall: 00:00:0");
        string d3 = db.GetProperty("rocksdb.dbstats")!;
        Assert.AreEqual(0.0, Field(d3, @"Interval writes: ([0-9]+) writes"));
    }

    [TestMethod]
    public void KeyMayExistDoesNoFileIo()
    {
        MemoryFileSystem fs = NewFs();
        var stats = new Statistics();
        DbOptions o = NewOptions(fs);
        o.Statistics = stats;
        var cfo = new ColumnFamilyOptions
        {
            TableOptions = new BlockBasedTableOptions
            {
                FilterPolicy = new RocksDb.Managed.Table.Filters.BloomFilterPolicy(10),
                CacheIndexAndFilterBlocks = true,
                PinL0FilterAndIndexBlocksInCache = false,
                PinTopLevelIndexAndFilter = false,
            },
        };
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o, cfo))
        {
            for (int i = 0; i < 500; i++) db.Put(Key(i), Value(i, 0));
            db.Flush();
            db.CompactRange();
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o, cfo))
        {
            ulong reads = stats.GetHistogramData(Histogram.SstReadMicros).Count;
            // Whether or not the open prefetched the filter into the block cache, KeyMayExist reads nothing (a filter
            // that is not cached answers "may exist", upstream's Incomplete).
            db.KeyMayExist(B("missing"));
            Assert.IsTrue(db.KeyMayExist(Key(3)));
            Assert.AreEqual(reads, stats.GetHistogramData(Histogram.SstReadMicros).Count, "KeyMayExist must not read the file");
            // A Get loads the filter into the block cache; then the filter answers.
            Assert.IsNull(db.Get(B("missing-too")));
            int negatives = 0;
            reads = stats.GetHistogramData(Histogram.SstReadMicros).Count;
            for (int i = 0; i < 100; i++)
            {
                if (!db.KeyMayExist(B("missing" + i))) negatives++;
                Assert.IsTrue(db.KeyMayExist(Key(i)));
            }
            Assert.AreEqual(reads, stats.GetHistogramData(Histogram.SstReadMicros).Count);
            Assert.IsTrue(negatives > 80, "negatives " + negatives);
        }
    }
}
