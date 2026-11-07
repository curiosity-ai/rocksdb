using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using RocksDb.Managed.CompatTests.Infrastructure;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>
/// Size estimates, properties, Statistics text and the info LOG compared with native RocksDB on the same files.
/// </summary>
[TestClass]
public sealed class PropertiesStatisticsCompatTests
{
    private static byte[] K(int i) => Encoding.UTF8.GetBytes("key" + i.ToString("D6", CultureInfo.InvariantCulture));

    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    private static RocksDbSharp.RocksDb OpenNative(string path, bool statistics)
    {
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetCompression(RocksDbSharp.Compression.No);
        if (statistics) o.EnableStatistics();
        s_keepAlive.Add(o);
        return RocksDbSharp.RocksDb.Open(o, path);
    }

    private static unsafe ulong NativeApproximateSize(RocksDbSharp.RocksDb db, byte[] start, byte[] limit)
    {
        ulong size = 0;
        fixed (byte* s = start)
        fixed (byte* l = limit)
        {
            IntPtr sp = (IntPtr)s, lp = (IntPtr)l;
            UIntPtr sl = (UIntPtr)start.Length, ll = (UIntPtr)limit.Length;
            RocksDbSharp.Native.Instance.rocksdb_approximate_sizes(db.Handle, 1, (IntPtr)(&sp), (IntPtr)(&sl), (IntPtr)(&lp), (IntPtr)(&ll), (IntPtr)(&size));
        }
        return size;
    }

    [TestMethod]
    public void ApproximateSizesAndTablePropertiesMatchNative()
    {
        using var dir = new TempDir("approx");
        (int, int)[] ranges = [(0, 5000), (0, 2500), (1234, 4321), (4999, 5000), (6000, 7000)];
        var nativeSizes = new List<ulong>();
        string nativeAgg;
        using (RocksDbSharp.RocksDb db = OpenNative(dir.Path, statistics: false))
        {
            for (int i = 0; i < 5000; i++) db.Put(K(i), Encoding.UTF8.GetBytes(new string((char)('a' + i % 26), 200 + i % 100)));
            db.CompactRange((byte[]?)null, null);
            foreach ((int a, int b) in ranges) nativeSizes.Add(NativeApproximateSize(db, K(a), K(b)));
            nativeAgg = db.GetProperty("rocksdb.aggregated-table-properties");
        }
        using (RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions()))
        {
            var managed = new List<ulong>();
            foreach ((int a, int b) in ranges) managed.Add(db.GetApproximateSizes([new KeyRange(K(a), K(b))])[0]);
            CollectionAssert.AreEqual(nativeSizes, managed, string.Join(",", nativeSizes) + " vs " + string.Join(",", managed));
            string agg = db.GetProperty("rocksdb.aggregated-table-properties")!;
            // The managed string is the leading, aggregatable part of TableProperties::ToString.
            StringAssert.StartsWith(nativeAgg, agg);
            Assert.AreEqual(5000UL, db.GetPropertiesOfAllTables().Values.Sum(static p => (long)p.NumEntries) is long n ? (ulong)n : 0);
        }
    }

    private static readonly Regex s_tickerLine = new(@"^(\S+) COUNT : \d+$");
    private static readonly Regex s_histogramLine = new(@"^(\S+) P50 : \d+\.\d{6} P95 : \d+\.\d{6} P99 : \d+\.\d{6} P100 : \d+\.\d{6} COUNT : \d+ SUM : \d+$");

    private static (List<string> Tickers, List<string> Histograms) ParseStatistics(string s)
    {
        var t = new List<string>();
        var h = new List<string>();
        foreach (string line in s.Split('\n', StringSplitOptions.RemoveEmptyEntries))
        {
            Match m = s_histogramLine.Match(line);
            if (m.Success)
            {
                h.Add(m.Groups[1].Value);
                continue;
            }
            m = s_tickerLine.Match(line);
            Assert.IsTrue(m.Success, "unexpected statistics line: " + line);
            t.Add(m.Groups[1].Value);
        }
        return (t, h);
    }

    [TestMethod]
    public void StatisticsStringHasNativeFormat()
    {
        using var dir = new TempDir("stats");
        string nativeText;
        var no = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).EnableStatistics();
        s_keepAlive.Add(no);
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(no, dir.Path))
        {
            db.Put(K(1), K(1));
            db.Get(K(1));
            nativeText = no.GetStatisticsString();
        }
        var stats = new Statistics();
        using (RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions { Statistics = stats }))
        {
            db.Put(K(1), K(1));
            db.Get(K(1));
        }
        (List<string> nt, List<string> nh) = ParseStatistics(nativeText);
        (List<string> mt, List<string> mh) = ParseStatistics(stats.ToString());
        // Same line formats; the managed lists follow the C++ tree (11.14), the package is 11.12: compare the overlap.
        var mts = new HashSet<string>(mt);
        var mhs = new HashSet<string>(mh);
        int tickerOverlap = nt.Count(mts.Contains), histOverlap = nh.Count(mhs.Contains);
        Assert.IsTrue(tickerOverlap >= nt.Count * 9 / 10, tickerOverlap + "/" + nt.Count);
        Assert.IsTrue(histOverlap >= nh.Count * 9 / 10, histOverlap + "/" + nh.Count);
        // Common names appear in the same relative order.
        var commonNative = nt.Where(mts.Contains).ToList();
        var nts = new HashSet<string>(nt);
        var commonManaged = mt.Where(nts.Contains).ToList();
        CollectionAssert.AreEqual(commonNative, commonManaged);
        StringAssert.Contains(stats.ToString(), "rocksdb.number.keys.written COUNT : 1\n");
    }

    [TestMethod]
    public void InfoLogRotationInteroperates()
    {
        using var dir = new TempDir("log");
        using (RocksDbSharp.RocksDb db = OpenNative(dir.Path, statistics: false)) db.Put(K(1), K(1));
        Assert.IsTrue(File.Exists(System.IO.Path.Combine(dir.Path, "LOG")));
        using (RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions())) db.Put(K(2), K(2));
        string log = File.ReadAllText(System.IO.Path.Combine(dir.Path, "LOG"));
        StringAssert.Contains(log, "RocksDb.Managed");
        Assert.AreEqual(1, Directory.GetFiles(dir.Path, "LOG.old.*").Length); // the native LOG was rotated
        using (RocksDbSharp.RocksDb db = OpenNative(dir.Path, statistics: false))
        {
            Assert.AreEqual("key000002", Encoding.UTF8.GetString(db.Get(K(2))));
        }
        Assert.AreEqual(2, Directory.GetFiles(dir.Path, "LOG.old.*").Length); // and native rotated ours
    }
    /// <summary>
    /// rocksdb.estimate-num-keys (InternalStats::HandleEstimateNumKeys): memtable deletes of keys that live in tables
    /// cancel those keys, so the sum is clamped once and not per part. Same workload on both sides, three LSM shapes.
    /// </summary>
    [TestMethod]
    public void EstimateNumKeysMatchesNative()
    {
        using var nativeDir = new TempDir("enk-n");
        using var managedDir = new TempDir("enk-m");
        var nativeValues = new List<string>();
        var no = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetDisableAutoCompactions(1);
        s_keepAlive.Add(no);
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(no, nativeDir.Path))
        {
            var flush = new RocksDbSharp.FlushOptions().SetWaitForFlush(true);
            for (int i = 0; i < 100; i++) db.Put(K(i), K(i));
            db.Flush(flush);
            for (int i = 0; i < 50; i++) db.Remove(K(i));
            nativeValues.Add(db.GetProperty("rocksdb.estimate-num-keys"));
            db.Flush(flush);
            nativeValues.Add(db.GetProperty("rocksdb.estimate-num-keys"));
            for (int i = 0; i < 80; i++) db.Remove(K(1000 + i)); // deletes of keys that never existed
            nativeValues.Add(db.GetProperty("rocksdb.estimate-num-keys"));
        }
        var managedValues = new List<string>();
        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true }, new ColumnFamilyOptions { DisableAutoCompactions = true }))
        {
            for (int i = 0; i < 100; i++) db.Put(K(i), K(i));
            db.Flush();
            for (int i = 0; i < 50; i++) db.Delete(K(i));
            managedValues.Add(db.GetProperty(DbProperties.EstimateNumKeys)!);
            db.Flush();
            managedValues.Add(db.GetProperty(DbProperties.EstimateNumKeys)!);
            for (int i = 0; i < 80; i++) db.Delete(K(1000 + i));
            managedValues.Add(db.GetProperty(DbProperties.EstimateNumKeys)!);
        }
        CollectionAssert.AreEqual(new[] { "50", "50", "0" }, nativeValues, string.Join(",", nativeValues));
        CollectionAssert.AreEqual(nativeValues, managedValues, string.Join(",", managedValues));
    }

    // Properties whose values are deterministic for the same workload, compared as strings (null = not a property).
    private static readonly string[] s_exactProperties =
    [
        "rocksdb.num-files-at-level0", "rocksdb.num-files-at-level6", "rocksdb.num-files-at-level7", "rocksdb.num-files-at-level99",
        "rocksdb.compression-ratio-at-level1", "rocksdb.compression-ratio-at-level7",
        "rocksdb.cf-write-stall-stats", "rocksdb.db-write-stall-stats", "rocksdb.num-immutable-mem-table-flushed",
        "rocksdb.num-running-bottom-compactions", "rocksdb.num-running-compaction-sorted-runs", "rocksdb.compaction-abort-count",
        "rocksdb.num-unscheduled-compactions", "rocksdb.is-file-deletions-enabled", "rocksdb.min-obsolete-sst-number-to-keep",
        "rocksdb.obsolete-sst-files-size", "rocksdb.estimate-oldest-key-time", "rocksdb.num-running-remote-compactions",
        "rocksdb.live-sst-files-size-at-temperature1", "rocksdb.live-sst-files-size-at-temperatureX",
    ];

    private static ulong LatestWalNumber(string dir) =>
        Directory.GetFiles(dir, "*.log").Select(static p => ulong.Parse(System.IO.Path.GetFileNameWithoutExtension(p), CultureInfo.InvariantCulture)).Max();

    private static Dictionary<string, string?> NativeProperties(RocksDbSharp.RocksDb db, IEnumerable<string> names)
    {
        var d = new Dictionary<string, string?>(StringComparer.Ordinal);
        foreach (string n in names) d[n] = db.GetProperty(n);
        return d;
    }

    private static Dictionary<string, string?> ManagedProperties(RocksDatabase db, IEnumerable<string> names)
    {
        var d = new Dictionary<string, string?>(StringComparer.Ordinal);
        foreach (string n in names) d[n] = db.GetProperty(n);
        return d;
    }

    /// <summary>
    /// The properties added for the second review (db/internal_stats.cc ppt_name_to_info): same workload, same values
    /// where they are deterministic; the compression ratio and the file-dependent ones within the file size difference.
    /// </summary>
    [TestMethod]
    public void AdditionalPropertiesMatchNative()
    {
        using var nativeDir = new TempDir("props-n");
        using var managedDir = new TempDir("props-m");
        Dictionary<string, string?> n1, n2, m1, m2;
        string? nRatio0, mRatio0, nLive, mLive, nTemp0, mTemp0, nMinLog, mMinLog, nFastCache, mFastCache;
        var no = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetCompression(RocksDbSharp.Compression.No).SetDisableAutoCompactions(1);
        s_keepAlive.Add(no);
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(no, nativeDir.Path))
        {
            var flush = new RocksDbSharp.FlushOptions().SetWaitForFlush(true);
            for (int i = 0; i < 2000; i++) db.Put(K(i), Encoding.UTF8.GetBytes(new string('v', 100)));
            db.Flush(flush);
            n1 = NativeProperties(db, s_exactProperties);
            nRatio0 = db.GetProperty("rocksdb.compression-ratio-at-level0");
            nLive = db.GetProperty("rocksdb.live-sst-files-size");
            nTemp0 = db.GetProperty("rocksdb.live-sst-files-size-at-temperature0");
            nMinLog = db.GetProperty("rocksdb.min-log-number-to-keep");
            nFastCache = db.GetProperty("rocksdb.fast-block-cache-entry-stats");
            RocksDbSharp.Native.Instance.rocksdb_disable_file_deletions(db.Handle);
            n2 = NativeProperties(db, ["rocksdb.is-file-deletions-enabled"]);
        }
        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true },
            new ColumnFamilyOptions { Compression = RocksDb.Managed.Compression.CompressionType.None, DisableAutoCompactions = true }))
        {
            for (int i = 0; i < 2000; i++) db.Put(K(i), Encoding.UTF8.GetBytes(new string('v', 100)));
            db.Flush();
            m1 = ManagedProperties(db, s_exactProperties);
            mRatio0 = db.GetProperty(DbProperties.CompressionRatioAtLevelPrefix + "0");
            mLive = db.GetProperty(DbProperties.LiveSstFilesSize);
            mTemp0 = db.GetProperty(DbProperties.LiveSstFilesSizeAtTemperaturePrefix + "0");
            mMinLog = db.GetProperty(DbProperties.MinLogNumberToKeep);
            mFastCache = db.GetProperty(DbProperties.FastBlockCacheEntryStats);
            db.DisableFileDeletions();
            m2 = ManagedProperties(db, ["rocksdb.is-file-deletions-enabled"]);
        }
        foreach (string name in s_exactProperties) Assert.AreEqual(n1[name], m1[name], name);
        Assert.AreEqual(n2["rocksdb.is-file-deletions-enabled"], m2["rocksdb.is-file-deletions-enabled"]);
        Assert.AreEqual("0", m2["rocksdb.is-file-deletions-enabled"]);
        Assert.IsNull(m1["rocksdb.num-files-at-level7"]);
        Assert.AreEqual("-1.000000", m1["rocksdb.compression-ratio-at-level1"]);
        double nr = double.Parse(nRatio0!, CultureInfo.InvariantCulture), mr = double.Parse(mRatio0!, CultureInfo.InvariantCulture);
        Assert.IsTrue(nr > 1 && Math.Abs(nr - mr) / nr < 0.05, nRatio0 + " vs " + mRatio0);
        Assert.AreEqual(nLive, nTemp0, "every file is kUnknown (0)");
        Assert.AreEqual(mLive, mTemp0);
        // Both: the WAL created by the flush (the only live one); the file numbers themselves differ between the two.
        Assert.AreEqual(LatestWalNumber(nativeDir.Path).ToString(CultureInfo.InvariantCulture), nMinLog, "native min-log-number-to-keep");
        Assert.AreEqual(LatestWalNumber(managedDir.Path).ToString(CultureInfo.InvariantCulture), mMinLog, "managed min-log-number-to-keep");
        StringAssert.StartsWith(nFastCache, "Block cache ");
        StringAssert.StartsWith(mFastCache, "Block cache ");
        StringAssert.Contains(nFastCache, "\nBlock cache entry stats(count,size,portion):");
        StringAssert.Contains(mFastCache, "\nBlock cache entry stats(count,size,portion):");
    }

    /// <summary>
    /// rocksdb.cf-write-stall-stats counts L0 slowdowns (ColumnFamilyData::RecalculateWriteStallConditions): the
    /// second flush installs a version with 2 L0 files (level0_slowdown_writes_trigger = 2) before any compaction can
    /// start, so both sides count delays.
    /// </summary>
    [TestMethod]
    public void WriteStallCountsMatchNative()
    {
        using var nativeDir = new TempDir("stall-n");
        using var managedDir = new TempDir("stall-m");
        string nStall, mStall;
        var no = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetCompression(RocksDbSharp.Compression.No)
            .SetLevel0SlowdownWritesTrigger(2).SetLevel0StopWritesTrigger(100).SetLevel0FileNumCompactionTrigger(2);
        s_keepAlive.Add(no);
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(no, nativeDir.Path))
        {
            var flush = new RocksDbSharp.FlushOptions().SetWaitForFlush(true);
            for (int f = 0; f < 3; f++)
            {
                for (int i = 0; i < 100; i++) db.Put(K(i), K(i + f));
                db.Flush(flush);
            }
            nStall = db.GetProperty("rocksdb.cf-write-stall-stats");
        }
        var cfo = new ColumnFamilyOptions
        {
            Compression = RocksDb.Managed.Compression.CompressionType.None, Level0SlowdownWritesTrigger = 2, Level0StopWritesTrigger = 100,
            Level0FileNumCompactionTrigger = 2,
        };
        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true }, cfo))
        {
            for (int f = 0; f < 3; f++)
            {
                for (int i = 0; i < 100; i++) db.Put(K(i), K(i + f));
                db.Flush();
            }
            mStall = db.GetProperty(DbProperties.CfWriteStallStats)!;
            Assert.AreEqual(mStall, db.GetProperty(DbProperties.CfStatsNoFileHistogram)!.Split('\n').First(l => l.StartsWith("Write Stall", StringComparison.Ordinal)) + "\n");
            Assert.AreEqual(db.GetMapProperty(DbProperties.CfWriteStallStats)!["l0-file-count-limit-delays"],
                db.GetMapProperty(DbProperties.CfStats)!["l0-file-count-limit-delays"]);
        }
        Dictionary<string, string> Parse(string s)
        {
            Assert.IsTrue(s.StartsWith("Write Stall (count): ", StringComparison.Ordinal) && s.EndsWith('\n'), s);
            var d = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach (string kv in s["Write Stall (count): ".Length..^1].Split(", "))
            {
                string[] p = kv.Split(": ");
                d.Add(p[0], p[1]);
            }
            return d;
        }
        Dictionary<string, string> n = Parse(nStall), m = Parse(mStall);
        CollectionAssert.AreEqual(n.Keys.ToList(), m.Keys.ToList(), nStall + " vs " + mStall);
        foreach (string k in new[] { "l0-file-count-limit-delays", "total-delays" })
        {
            Assert.AreNotEqual("0", n[k], k + ": " + nStall);
            Assert.AreNotEqual("0", m[k], k + ": " + mStall);
        }
        foreach (string k in new[] { "memtable-limit-delays", "memtable-limit-stops", "pending-compaction-bytes-stops", "total-stops" })
        {
            Assert.AreEqual(n[k], m[k], k);
        }
    }

    /// <summary>
    /// rocksdb.total-sst-files-size includes the files of a version pinned by an iterator
    /// (ColumnFamilyData::GetTotalSstFilesSize); live-sst-files-size only the current version's.
    /// </summary>
    [TestMethod]
    public void TotalSstFilesSizeCountsPinnedVersionsLikeNative()
    {
        using var nativeDir = new TempDir("total-n");
        using var managedDir = new TempDir("total-m");
        (ulong Total, ulong Live, ulong LiveBefore, ulong TotalAfterRelease) nSizes, mSizes;
        var no = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetCompression(RocksDbSharp.Compression.No).SetDisableAutoCompactions(1);
        s_keepAlive.Add(no);
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(no, nativeDir.Path))
        {
            var flush = new RocksDbSharp.FlushOptions().SetWaitForFlush(true);
            for (int f = 0; f < 3; f++)
            {
                for (int i = 0; i < 100; i++) db.Put(K(i), K(i + f));
                db.Flush(flush);
            }
            ulong P(string name) => ulong.Parse(db.GetProperty(name), CultureInfo.InvariantCulture);
            ulong before = P("rocksdb.live-sst-files-size");
            Assert.AreEqual(before, P("rocksdb.total-sst-files-size"));
            ulong total, live;
            using (RocksDbSharp.Iterator it = db.NewIterator())
            {
                db.CompactRange((byte[]?)null, null);
                total = P("rocksdb.total-sst-files-size");
                live = P("rocksdb.live-sst-files-size");
            }
            nSizes = (total, live, before, P("rocksdb.total-sst-files-size"));
        }
        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true },
            new ColumnFamilyOptions { Compression = RocksDb.Managed.Compression.CompressionType.None, DisableAutoCompactions = true }))
        {
            for (int f = 0; f < 3; f++)
            {
                for (int i = 0; i < 100; i++) db.Put(K(i), K(i + f));
                db.Flush();
            }
            ulong P(string name) => ulong.Parse(db.GetProperty(name)!, CultureInfo.InvariantCulture);
            ulong before = P(DbProperties.LiveSstFilesSize);
            Assert.AreEqual(before, P(DbProperties.TotalSstFilesSize));
            ulong total, live;
            using (Iterator it = db.NewIterator())
            {
                db.CompactRange(null, null);
                total = P(DbProperties.TotalSstFilesSize);
                live = P(DbProperties.LiveSstFilesSize);
            }
            mSizes = (total, live, before, P(DbProperties.TotalSstFilesSize));
        }
        Assert.AreEqual(nSizes.Total, nSizes.Live + nSizes.LiveBefore, "native: pinned + current");
        Assert.AreEqual(mSizes.Total, mSizes.Live + mSizes.LiveBefore, "managed: pinned + current");
        Assert.AreEqual(nSizes.Live, nSizes.TotalAfterRelease, "native: the pinned version is gone");
        Assert.AreEqual(mSizes.Live, mSizes.TotalAfterRelease, "managed: the pinned version is gone");
    }


    private static ulong TickerValue(string statistics, string name)
    {
        Match m = Regex.Match(statistics, "^" + Regex.Escape(name) + " COUNT : (\\d+)$", RegexOptions.Multiline);
        Assert.IsTrue(m.Success, name);
        return ulong.Parse(m.Groups[1].Value, CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// GET_HIT_L0 / L1 / L2_AND_UP for MultiGet (Version::MultiGetFromSST): the same LSM shape (keys in L0 and in the
    /// bottommost level) and MultiGet calls give the same counts on both sides.
    /// </summary>
    [TestMethod]
    public void MultiGetHitTickersMatchNative()
    {
        using var nativeDir = new TempDir("mget-hit-n");
        using var managedDir = new TempDir("mget-hit-m");
        byte[][] keys = [K(1), K(50), K(150), K(199), K(500)];
        string nativeText;
        var no = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).EnableStatistics();
        s_keepAlive.Add(no);
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(no, nativeDir.Path))
        {
            for (int i = 0; i < 100; i++) db.Put(K(i), K(i));
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            db.CompactRange(Array.Empty<byte>(), null);
            for (int i = 100; i < 200; i++) db.Put(K(i), K(i));
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            db.MultiGet(keys);
            db.MultiGet(keys);
            nativeText = no.GetStatisticsString();
        }
        var stats = new Statistics();
        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true, Statistics = stats }))
        {
            for (int i = 0; i < 100; i++) db.Put(K(i), K(i));
            db.Flush();
            db.CompactRange(null, null);
            for (int i = 100; i < 200; i++) db.Put(K(i), K(i));
            db.Flush();
            db.MultiGet(keys);
            db.MultiGet(keys);
        }
        string managedText = stats.ToString();
        foreach (string name in new[] { "rocksdb.l0.hit", "rocksdb.l1.hit", "rocksdb.l2andup.hit" })
        {
            Assert.AreEqual(TickerValue(nativeText, name), TickerValue(managedText, name), name);
        }
        Assert.AreEqual(4UL, TickerValue(managedText, "rocksdb.l0.hit"));
        Assert.AreEqual(4UL, TickerValue(managedText, "rocksdb.l2andup.hit"));
    }
}
