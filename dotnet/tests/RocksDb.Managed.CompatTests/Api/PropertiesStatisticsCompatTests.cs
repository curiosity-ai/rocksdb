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
