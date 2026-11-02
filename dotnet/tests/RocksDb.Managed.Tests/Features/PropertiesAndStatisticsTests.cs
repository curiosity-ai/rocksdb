using System.Buffers;
using System.Text;
using System.Text.RegularExpressions;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Features;

/// <summary>KeyMayExist, approximate sizes, extended properties, Statistics and the info LOG.</summary>
[TestClass]
public sealed class PropertiesAndStatisticsTests
{
    private static ColumnFamilyOptions BloomCf() => new()
    {
        Compression = RocksDb.Managed.Compression.CompressionType.None,
        TableOptions = new BlockBasedTableOptions { FilterPolicy = "bloomfilter:10:false" },
    };

    private static void FillAndFlush(RocksDatabase db, int n, int valueSize)
    {
        for (int i = 0; i < n; i++) db.Put(Key(i), B(new string((char)('a' + i % 26), valueSize)));
        db.Flush();
    }

    [TestMethod]
    public void KeyMayExistMemtableAndFilters()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), BloomCf());
        Assert.IsFalse(db.KeyMayExist(B("nothing")));
        db.Put(B("mem"), B("v1"));
        var w = new ArrayBufferWriter<byte>();
        Assert.IsTrue(db.KeyMayExist(null, B("mem"), w, out bool found));
        Assert.IsTrue(found);
        Assert.AreEqual("v1", Encoding.UTF8.GetString(w.WrittenSpan));
        db.Delete(B("mem"));
        Assert.IsFalse(db.KeyMayExist(B("mem")));

        FillAndFlush(db, 1000, 10);
        int falsePositives = 0;
        for (int i = 0; i < 1000; i++)
        {
            Assert.IsTrue(db.KeyMayExist(null, Key(i), null, out found));
            Assert.IsFalse(found); // in a table file: value not read
            if (db.KeyMayExist(B("missing" + i))) falsePositives++;
        }
        Assert.IsTrue(falsePositives < 50, "false positives " + falsePositives);
        // A snapshot taken before a key was written does not see it in the memtable.
        using Snapshot s = db.GetSnapshot();
        db.Put(B("later"), B("x"));
        Assert.IsFalse(db.KeyMayExist(new ReadOptions { Snapshot = s }, B("later"), null, out _));
    }

    [TestMethod]
    public void ApproximateSizesAndMemTableStats()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), BloomCf());
        FillAndFlush(db, 2000, 500);
        ulong total = 0;
        foreach (LiveFileMetadata f in db.GetLiveFilesMetadata()) total += f.Size;
        ulong[] sizes = db.GetApproximateSizes([new KeyRange(Key(0), Key(2000)), new KeyRange(Key(0), Key(1000)), new KeyRange(B("zzz"), B("zzzz"))]);
        Assert.IsTrue(sizes[0] > total * 9 / 10 && sizes[0] <= total, sizes[0] + " vs " + total);
        Assert.IsTrue(sizes[1] > total * 4 / 10 && sizes[1] < total * 6 / 10, sizes[1] + " vs " + total);
        Assert.AreEqual(0UL, sizes[2]);
        Assert.AreEqual(sizes[1], db.DefaultColumnFamily.GetApproximateSize(Key(0), Key(1000)));

        for (int i = 0; i < 100; i++) db.Put(B("m" + i), B(new string('v', 100)));
        (ulong count, ulong size) = db.GetApproximateMemTableStats(B("m"), B("n"));
        Assert.IsTrue(count > 0 && count <= 100, count.ToString());
        Assert.IsTrue(size > 0);
        ulong[] mem = db.GetApproximateSizes(new SizeApproximationOptions { IncludeMemtables = true, IncludeFiles = false }, [new KeyRange(B("m"), B("n"))]);
        Assert.AreEqual(size, mem[0]);
        var e = Assert.ThrowsExactly<RocksDbException>(() =>
            db.GetApproximateSizes(new SizeApproximationOptions { IncludeFiles = false }, [new KeyRange(B("a"), B("b"))]));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
    }

    [TestMethod]
    public void ExtendedProperties()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), BloomCf());
        FillAndFlush(db, 1000, 100);
        FillAndFlush(db, 1000, 100); // overwrite: two L0 files
        db.Put(B("x"), B("y"));
        Assert.IsTrue(db.GetIntProperty("rocksdb.cur-size-all-mem-tables") > 0);
        Assert.IsTrue(db.GetIntProperty("rocksdb.size-all-mem-tables") > 0);
        ulong live = db.GetIntProperty("rocksdb.estimate-live-data-size")!.Value;
        ulong total = db.GetIntProperty("rocksdb.total-sst-files-size")!.Value;
        Assert.IsTrue(live > 0 && live < total, live + " " + total); // the two L0 files overlap: only one counts
        db.CompactRange();
        Assert.AreEqual(db.GetIntProperty("rocksdb.total-sst-files-size"), db.GetIntProperty("rocksdb.estimate-live-data-size"));
        Assert.AreEqual((ulong)(32 << 20), db.GetIntProperty("rocksdb.block-cache-capacity"));
        Assert.IsNotNull(db.GetIntProperty("rocksdb.block-cache-usage"));
        Assert.IsTrue(db.GetIntProperty("rocksdb.num-live-versions") >= 1);
        Assert.AreEqual(0UL, db.GetIntProperty("rocksdb.oldest-snapshot-time"));
        using (Snapshot s = db.GetSnapshot())
        {
            Assert.IsTrue(db.GetIntProperty("rocksdb.oldest-snapshot-time") > 0);
        }
        Assert.IsFalse(db.TryGetIntProperty("rocksdb.cfstats", out _));
        Assert.IsTrue(db.TryGetAggregatedIntProperty("rocksdb.estimate-num-keys", out ulong keys) && keys > 0);

        string cfstats = db.GetProperty("rocksdb.cfstats")!;
        StringAssert.Contains(cfstats, "** Compaction Stats [default] **");
        StringAssert.Contains(cfstats, "Level    Files   Size     Score Read(GB)");
        StringAssert.Contains(cfstats, "Flush(GB): cumulative");
        StringAssert.Contains(db.GetProperty("rocksdb.dbstats")!, "Cumulative writes: 2001 writes, 2001 keys");
        string sst = db.GetProperty("rocksdb.sstables")!;
        StringAssert.Contains(sst, "--- level 6 --- version# ");
        StringAssert.Matches(sst, new Regex(@" \d+:\d+\[\d+ \.\. \d+\]\['[0-9A-F]+' seq:\d+, type:\d \.\. '[0-9A-F]+' seq:\d+, type:\d\]"));
        StringAssert.Contains(db.GetProperty("rocksdb.aggregated-table-properties")!, "# entries=1001; ");

        Dictionary<string, string> map = db.GetMapProperty("rocksdb.cfstats")!;
        Assert.AreEqual("1.000000", map["compaction.L6.NumFiles"]);
        Assert.IsTrue(map.ContainsKey("compaction.Sum.WriteGB"));
        Assert.AreEqual("1001", db.GetMapProperty("rocksdb.aggregated-table-properties")!["num_entries"]);
        Assert.AreEqual("1001", db.GetMapProperty("rocksdb.aggregated-table-properties-at-level6")!["num_entries"]);
        Assert.AreEqual("2001", db.GetMapProperty("rocksdb.dbstats")!["db.user_keys_written"]);

        Dictionary<string, RocksDb.Managed.Table.TableProperties> props = db.GetPropertiesOfAllTables();
        Assert.AreEqual(1, props.Count);
        foreach (KeyValuePair<string, RocksDb.Managed.Table.TableProperties> kv in props)
        {
            StringAssert.EndsWith(kv.Key, ".sst");
            Assert.AreEqual(1001UL, kv.Value.NumEntries);
        }
    }

    [TestMethod]
    public void HistogramBucketsMatchUpstream()
    {
        // HistogramBucketMapper: 109 buckets starting 1, 2, 3, 4, 6, 10, 15, 22, 34, 51, 76, 110, 170, 250 ...
        Assert.AreEqual(109, HistogramBuckets.Count);
        CollectionAssert.AreEqual(new ulong[] { 1, 2, 3, 4, 6, 10, 15, 22, 34, 51, 76, 110, 170, 250 }, HistogramBuckets.Limits[..14]);
        Assert.AreEqual(0, HistogramBuckets.IndexForValue(0));
        Assert.AreEqual(4, HistogramBuckets.IndexForValue(5)); // lower_bound -> 6
        Assert.AreEqual(108, HistogramBuckets.IndexForValue(ulong.MaxValue));

        var st = new Statistics();
        for (ulong v = 1; v <= 100; v++) st.RecordInHistogram(Histogram.DbGet, v);
        HistogramData d = st.GetHistogramData(Histogram.DbGet);
        Assert.AreEqual(100UL, d.Count);
        Assert.AreEqual(5050UL, d.Sum);
        Assert.AreEqual(100.0, d.Max);
        Assert.AreEqual(1.0, d.Min);
        Assert.AreEqual(50.5, d.Average, 1e-9);
        Assert.IsTrue(d.Median is > 45 and < 56, d.Median.ToString());
        StringAssert.StartsWith(st.GetHistogramString(Histogram.DbGet), "Count: 100 Average: 50.5000");
        st.StatsLevel = StatsLevel.ExceptHistogramOrTimers;
        st.RecordInHistogram(Histogram.DbGet, 5);
        st.RecordTick(Ticker.BytesRead, 7);
        Assert.AreEqual(100UL, st.GetHistogramData(Histogram.DbGet).Count);
        Assert.AreEqual(7UL, st.GetAndResetTickerCount(Ticker.BytesRead));
        Assert.AreEqual(0UL, st.GetTickerCount(Ticker.BytesRead));
        st.StatsLevel = StatsLevel.DisableAll;
        st.RecordTick(Ticker.BytesRead, 7);
        Assert.AreEqual(0UL, st.GetTickerCount(Ticker.BytesRead));
    }

    [TestMethod]
    public void StatisticsRecordedByTheDb()
    {
        MemoryFileSystem fs = NewFs();
        var stats = new Statistics();
        DbOptions o = NewOptions(fs);
        o.Statistics = stats;
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, BloomCf());
        Assert.AreSame(stats, db.Statistics);
        for (int i = 0; i < 100; i++) db.Put(Key(i), B("value"));
        Assert.AreEqual(100UL, stats.GetTickerCount(Ticker.NumberKeysWritten));
        Assert.IsTrue(stats.GetTickerCount(Ticker.BytesWritten) > 0);
        Assert.AreEqual(100UL, stats.GetTickerCount(Ticker.WriteDoneBySelf) + stats.GetTickerCount(Ticker.WriteDoneByOther));
        Assert.IsTrue(stats.GetTickerCount(Ticker.WalFileBytes) > 0);
        Assert.AreEqual("value", GetString(db, "key000001"));
        Assert.AreEqual(1UL, stats.GetTickerCount(Ticker.MemtableHit));
        db.Flush();
        Assert.IsTrue(stats.GetTickerCount(Ticker.FlushWriteBytes) > 0);
        Assert.AreEqual(1UL, stats.GetHistogramData(Histogram.FlushTime).Count);
        Assert.AreEqual("value", GetString(db, "key000002"));
        Assert.IsNull(GetString(db, "absent"));
        Assert.AreEqual(3UL, stats.GetTickerCount(Ticker.NumberKeysRead));
        Assert.AreEqual(2UL, stats.GetTickerCount(Ticker.MemtableMiss));
        Assert.IsTrue(stats.GetTickerCount(Ticker.BlockCacheMiss) > 0);
        Assert.IsTrue(stats.GetTickerCount(Ticker.BloomFilterUseful) + stats.GetTickerCount(Ticker.BloomFilterFullPositive) > 0);
        Assert.AreEqual(3UL, stats.GetHistogramData(Histogram.DbGet).Count);
        Assert.AreEqual(100UL, stats.GetHistogramData(Histogram.DbWrite).Count);
        using (Iterator it = db.NewIterator())
        {
            int n = 0;
            for (it.SeekToFirst(); it.Valid; it.Next()) n++;
            Assert.AreEqual(100, n);
        }
        Assert.AreEqual(1UL, stats.GetTickerCount(Ticker.NumberDbSeek));
        Assert.AreEqual(100UL, stats.GetTickerCount(Ticker.NumberDbNext));
        Assert.AreEqual(99UL, stats.GetTickerCount(Ticker.NumberDbNextFound));
        Assert.AreEqual(1UL, stats.GetTickerCount(Ticker.NoIteratorCreated));
        Assert.AreEqual(1UL, stats.GetTickerCount(Ticker.NoIteratorDeleted));
        db.CompactRange();
        Assert.IsTrue(stats.GetTickerCount(Ticker.CompactReadBytes) > 0 || stats.GetHistogramData(Histogram.CompactionTime).Count == 0);

        string s = stats.ToString();
        StringAssert.StartsWith(s, "rocksdb.block.cache.miss COUNT : ");
        StringAssert.Contains(s, "\nrocksdb.number.keys.written COUNT : 100\n");
        StringAssert.Matches(s, new Regex(@"\nrocksdb\.db\.get\.micros P50 : \d+\.\d{6} P95 : \d+\.\d{6} P99 : \d+\.\d{6} P100 : \d+\.\d{6} COUNT : 3 SUM : \d+\n"));
        Assert.AreEqual(100UL, stats.GetTickerMap()["rocksdb.number.keys.written"]);
        stats.Reset();
        Assert.AreEqual(0UL, stats.GetTickerCount(Ticker.NumberKeysWritten));
        Assert.AreEqual(0UL, stats.GetTickerCount(Ticker.BlockCacheMiss));
    }

    private sealed class CollectingLogger() : InfoLogger(InfoLogLevel.Info)
    {
        public readonly List<string> Lines = new();
        protected override void LogCore(InfoLogLevel level, string message)
        {
            lock (Lines) Lines.Add(message);
        }
        public bool Any(string s)
        {
            lock (Lines) return Lines.Exists(l => l.Contains(s, StringComparison.Ordinal));
        }
    }

    [TestMethod]
    public void CustomInfoLoggerReceivesEvents()
    {
        MemoryFileSystem fs = NewFs();
        var log = new CollectingLogger();
        DbOptions o = NewOptions(fs);
        o.InfoLog = log;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o))
        {
            Assert.IsTrue(log.Any("Options.max_open_files: -1"));
            Assert.IsTrue(log.Any("--------------- Options for column family [default]:"));
            Assert.IsTrue(log.Any("Options.write_buffer_size: 67108864"));
            db.Put(B("k"), B("v"));
            db.Flush();
            Assert.IsTrue(log.Any("\"event\": \"flush_started\""));
            Assert.IsTrue(log.Any("\"event\": \"flush_finished\""));
            Assert.IsTrue(log.Any("\"event\": \"table_file_creation\""));
        }
        Assert.IsTrue(log.Any("Shutdown complete"));
        Assert.IsFalse(fs.FileExists(DbPath + "/LOG"));
        log.Level = InfoLogLevel.Warn;
        int n = log.Lines.Count;
        log.Log(InfoLogLevel.Info, "dropped");
        log.Log(InfoLogLevel.Error, "kept");
        Assert.AreEqual(n + 1, log.Lines.Count);
        Assert.AreEqual("[ERROR] kept", log.Lines[^1]);
    }

    private static string ReadText(MemoryFileSystem fs, string path)
    {
        using ISequentialFile f = fs.OpenSequential(path);
        var ms = new MemoryStream();
        var buf = new byte[4096];
        int n;
        while ((n = f.Read(buf)) > 0) ms.Write(buf, 0, n);
        return Encoding.UTF8.GetString(ms.ToArray());
    }

    private static List<string> OldLogs(MemoryFileSystem fs) =>
        fs.GetChildren(DbPath).Where(static c => c.StartsWith("LOG.old.", StringComparison.Ordinal)).ToList();

    [TestMethod]
    public void LogFileWrittenRotatedAndTrimmed()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        o.KeepLogFileNum = 2;
        for (int i = 0; i < 4; i++)
        {
            using RocksDatabase db = RocksDatabase.Open(DbPath, o);
            db.Put(B("k"), B("v" + i));
            db.Flush();
        }
        string text = ReadText(fs, DbPath + "/LOG");
        StringAssert.Matches(text, new Regex(@"^\d{4}/\d{2}/\d{2}-\d{2}:\d{2}:\d{2}\.\d{6} [0-9a-f]+ RocksDB version: "));
        StringAssert.Contains(text, "Options.create_if_missing: true");
        StringAssert.Contains(text, "flush_finished");
        StringAssert.Contains(text, "Shutdown complete");
        Assert.AreEqual(2, OldLogs(fs).Count);

        // Size-based rolling (max_log_file_size): headers are replayed into each new file.
        MemoryFileSystem fs2 = NewFs();
        DbOptions o2 = NewOptions(fs2);
        o2.MaxLogFileSize = 4096;
        o2.KeepLogFileNum = 100;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o2))
        {
            for (int i = 0; i < 20; i++)
            {
                db.Put(B("k" + i), B("v"));
                db.Flush();
            }
        }
        Assert.IsTrue(OldLogs(fs2).Count >= 2, OldLogs(fs2).Count.ToString());
        StringAssert.Contains(ReadText(fs2, DbPath + "/LOG"), "RocksDB version: ");

        // info_log_level = WARN: nothing at INFO level is written.
        MemoryFileSystem fs3 = NewFs();
        DbOptions o3 = NewOptions(fs3);
        o3.InfoLogLevel = InfoLogLevel.Warn;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o3))
        {
            db.Put(B("k"), B("v"));
            db.Flush();
        }
        string warnOnly = ReadText(fs3, DbPath + "/LOG");
        Assert.IsFalse(warnOnly.Contains("flush_finished", StringComparison.Ordinal));
        StringAssert.Contains(warnOnly, "Options.info_log_level: WARN_LEVEL"); // header lines are always written
    }

    [TestMethod]
    public void PeriodicStatsDumpWritesStatistics()
    {
        MemoryFileSystem fs = NewFs();
        var log = new CollectingLogger();
        DbOptions o = NewOptions(fs);
        o.InfoLog = log;
        o.Statistics = new Statistics();
        using RocksDatabase db = RocksDatabase.Open(DbPath, o);
        db.Put(B("k"), B("v"));
        db.DumpStats();
        Assert.IsTrue(log.Any("------- DUMPING STATS -------"));
        Assert.IsTrue(log.Any("** DB Stats **"));
        Assert.IsTrue(log.Any("STATISTICS:"));
    }
}
