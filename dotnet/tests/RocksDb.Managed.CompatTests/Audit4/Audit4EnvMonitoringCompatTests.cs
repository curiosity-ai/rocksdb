using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using RocksDb.Managed.CompatTests.Infrastructure;

namespace RocksDb.Managed.CompatTests.Audit4;

/// <summary>
/// Audit #4, slice EnvMonitoring: DB properties and histogram text compared with native RocksDB on the same workload
/// (db/internal_stats.cc, monitoring/histogram.cc).
/// </summary>
[TestClass]
public sealed class Audit4EnvMonitoringCompatTests
{
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    private static byte[] K(int i) => Encoding.UTF8.GetBytes("key" + i.ToString("D6", CultureInfo.InvariantCulture));

    private static readonly byte[] s_value = Encoding.UTF8.GetBytes(new string('v', 100));

    // Properties whose value is fully determined by the workload (null = "not a property" on both sides).
    private static readonly string[] s_exact =
    [
        "rocksdb.num-entries-active-mem-table", "rocksdb.num-deletes-active-mem-table", "rocksdb.num-entries-imm-mem-tables",
        "rocksdb.num-deletes-imm-mem-tables", "rocksdb.num-immutable-mem-table", "rocksdb.mem-table-flush-pending",
        "rocksdb.compaction-pending", "rocksdb.estimate-num-keys", "rocksdb.num-snapshots", "rocksdb.oldest-snapshot-sequence",
        "rocksdb.base-level", "rocksdb.is-write-stopped", "rocksdb.actual-delayed-write-rate", "rocksdb.background-errors",
        // (not num-running-flushes: native still counts the flush job for a moment after Flush(wait) returns)
        "rocksdb.num-running-compactions", "rocksdb.num-blob-files", "rocksdb.live-blob-file-size",
        "rocksdb.total-blob-file-size", "rocksdb.live-blob-file-garbage-size", "rocksdb.num-files-at-level0", "rocksdb.num-files-at-level1",
        "rocksdb.num-files-at-level6", "rocksdb.num-live-versions",
        // Names that are not properties: unknown, missing / malformed numeric suffixes (ConsumeDecimalNumber && suffix.empty()).
        "rocksdb.nonexistent", "nonexistent", "", "rocksdb.", "rocksdb.num-files-at-level", "rocksdb.num-files-at-level1x",
        "rocksdb.num-files-at-level-1", "rocksdb.num-files-at-level+1", "rocksdb.num-files-at-level 1", "rocksdb.num-files-at-level01",
        "rocksdb.num-files-at-level18446744073709551616", "rocksdb.aggregated-table-properties-at-level9",
        "rocksdb.aggregated-table-properties-at-level", "rocksdb.compression-ratio-at-level", "rocksdb.compression-ratio-at-levelx",
        "rocksdb.live-sst-files-size-at-temperature", "rocksdb.num-snapshots ", "rocksdb.estimate-num-keys0", "rocksdb.levelstatsx",
    ];

    private sealed class Stage
    {
        public readonly Dictionary<string, string?> Exact = new(StringComparer.Ordinal);
        public string? LevelStats, LiveSize, TotalSize, LiveDataSize, PendingBytes;
    }

    private static void Capture(Stage s, Func<string, string?> get)
    {
        foreach (string n in s_exact) s.Exact[n] = get(n);
        s.LevelStats = get("rocksdb.levelstats");
        s.LiveSize = get("rocksdb.live-sst-files-size");
        s.TotalSize = get("rocksdb.total-sst-files-size");
        s.LiveDataSize = get("rocksdb.estimate-live-data-size");
        s.PendingBytes = get("rocksdb.estimate-pending-compaction-bytes");
    }

    /// <summary>The workload as (put, delete, flush, snapshot, release) callbacks; a stage is captured after each step.</summary>
    private static List<Stage> Run(Action<byte[]> put, Action<byte[]> delete, Action flush, Action snapshot, Action release, Func<string, string?> get)
    {
        var stages = new List<Stage>();
        void Snap()
        {
            var s = new Stage();
            Capture(s, get);
            stages.Add(s);
        }
        Snap();                                         // 0: empty DB
        for (int i = 0; i < 100; i++) put(K(i));
        for (int i = 0; i < 10; i++) delete(K(i));
        for (int i = 50; i < 55; i++) put(K(i));        // overwrites
        snapshot();
        Snap();                                         // 1: memtable only, one snapshot
        flush();
        Snap();                                         // 2: one L0 file
        for (int i = 10; i < 30; i++) delete(K(i));     // deletes of keys that live in the table
        for (int i = 200; i < 230; i++) put(K(i));
        Snap();                                         // 3: table + memtable deletes
        release();
        snapshot();
        flush();
        Snap();                                         // 4: two L0 files
        for (int r = 0; r < 2; r++)
        {
            for (int i = 300 + r * 10; i < 310 + r * 10; i++) put(K(i));
            delete(K(1000 + r));                        // delete of a key that never existed
            flush();
        }
        release();
        Snap();                                         // 5: four L0 files (level0_file_num_compaction_trigger), no snapshot
        return stages;
    }

    /// <summary>
    /// InternalStats property handlers (db/internal_stats.cc: HandleNumEntriesActiveMemTable ... HandleLevelStats,
    /// HandleEstimateNumKeys, HandleCompactionPending, HandleNumFilesAtLevel's suffix parsing, unknown names): the same
    /// writes, deletes, snapshots and flushes on native and managed give the same values at every step.
    /// </summary>
    [TestMethod]
    public void PropertiesMatchNativeAtEveryStepOfAWorkload()
    {
        using var nativeDir = new TempDir("a4props-n");
        using var managedDir = new TempDir("a4props-m");
        List<Stage> native, managed;
        var no = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetCompression(RocksDbSharp.Compression.No).SetDisableAutoCompactions(1);
        s_keepAlive.Add(no);
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(no, nativeDir.Path))
        {
            var fo = new RocksDbSharp.FlushOptions().SetWaitForFlush(true);
            RocksDbSharp.Snapshot? snap = null;
            native = Run(k => db.Put(k, s_value), k => db.Remove(k), () => db.Flush(fo), () => snap = db.CreateSnapshot(),
                () => { snap!.Dispose(); snap = null; }, n => db.GetProperty(n));
        }
        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true },
            new ColumnFamilyOptions { Compression = RocksDb.Managed.Compression.CompressionType.None, DisableAutoCompactions = true }))
        {
            Snapshot? snap = null;
            managed = Run(k => db.Put(k, s_value), k => db.Delete(k), () => db.Flush(), () => snap = db.GetSnapshot(),
                () => { snap!.Dispose(); snap = null; }, n => db.GetProperty(n));
        }
        Assert.AreEqual(native.Count, managed.Count);
        for (int i = 0; i < native.Count; i++)
        {
            foreach (string name in s_exact) Assert.AreEqual(native[i].Exact[name], managed[i].Exact[name], "stage " + i + ": " + name);
            Assert.AreEqual(LevelFilesAndSizes(native[i].LevelStats), LevelFilesAndSizes(managed[i].LevelStats), "stage " + i + ": levelstats");
            // File sizes differ by a few bytes between the two writers (properties block): compare within 5%.
            AssertClose(native[i].LiveSize, managed[i].LiveSize, "stage " + i + ": live-sst-files-size");
            AssertClose(native[i].TotalSize, managed[i].TotalSize, "stage " + i + ": total-sst-files-size");
            AssertClose(native[i].LiveDataSize, managed[i].LiveDataSize, "stage " + i + ": estimate-live-data-size");
            AssertClose(native[i].PendingBytes, managed[i].PendingBytes, "stage " + i + ": estimate-pending-compaction-bytes");
        }
        // Sanity of the reference itself: the last stage needs a compaction, the names that are not properties are null.
        Assert.AreEqual("1", native[^1].Exact["rocksdb.compaction-pending"]);
        Assert.AreEqual("4", native[^1].Exact["rocksdb.num-files-at-level0"]);
        Assert.IsNull(native[^1].Exact["rocksdb.num-files-at-level1x"]);
        Assert.AreEqual("1", native[1].Exact["rocksdb.num-snapshots"]);
    }

    /// <summary>
    /// The level, files and size(MB) columns of "rocksdb.levelstats" (HandleLevelStats: "%3d %8d %8.0f\n"). The managed
    /// text has an extra score column by design, which is not compared.
    /// </summary>
    private static string LevelFilesAndSizes(string? levelStats)
    {
        Assert.IsNotNull(levelStats);
        var sb = new StringBuilder();
        string[] lines = levelStats.Split('\n', StringSplitOptions.RemoveEmptyEntries);
        Assert.IsTrue(lines.Length > 2 && lines[0].StartsWith("Level Files Size(MB)", StringComparison.Ordinal), levelStats);
        for (int i = 2; i < lines.Length; i++)
        {
            string[] cols = lines[i].Split(' ', StringSplitOptions.RemoveEmptyEntries);
            sb.Append(cols[0]).Append(' ').Append(cols[1]).Append(' ').Append(cols[2]).Append('\n');
        }
        return sb.ToString();
    }

    private static void AssertClose(string? native, string? managed, string what)
    {
        Assert.IsNotNull(native, what);
        Assert.IsNotNull(managed, what);
        double n = double.Parse(native, CultureInfo.InvariantCulture), m = double.Parse(managed, CultureInfo.InvariantCulture);
        if (n == 0 || m == 0) Assert.AreEqual(n, m, what);
        else Assert.IsTrue(Math.Abs(n - m) / n < 0.05, what + ": native " + native + " managed " + managed);
    }

    private static int OldInfoLogs(string dir) => Directory.GetFiles(dir, "LOG.old.*").Length;

    /// <summary>
    /// keep_log_file_num counts the current LOG: DBImpl::PurgeObsoleteFiles deletes old info logs while
    /// "old_info_log_file_count >= keep_log_file_num" (db/db_impl/db_impl_files.cc) and AutoRollLogger::TrimOldLogFiles
    /// while "old_log_files_.size() >= kKeepLogFileNum" (logging/auto_roll_logger.cc), so keep_log_file_num - 1 files
    /// named LOG.old.* remain. Both sides are opened five times with keep_log_file_num = 3.
    /// </summary>
    [TestMethod]
    public void KeepLogFileNumKeepsTheSameNumberOfOldLogsAsNative()
    {
        using var nativeDir = new TempDir("a4log-n");
        using var managedDir = new TempDir("a4log-m");
        var no = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetKeepLogFileNum(3);
        s_keepAlive.Add(no);
        for (int i = 0; i < 5; i++)
        {
            using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(no, nativeDir.Path)) db.Put(K(i), s_value);
            using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true, KeepLogFileNum = 3 })) db.Put(K(i), s_value);
        }
        Assert.AreEqual(2, OldInfoLogs(nativeDir.Path), "native");
        Assert.AreEqual(OldInfoLogs(nativeDir.Path), OldInfoLogs(managedDir.Path), "managed");
    }

    // HistogramStat::ToString bucket line: "%c %7llu, %7llu ] %8llu %7.3f%% %7.3f%% " + hash marks.
    private static readonly Regex s_bucketLine = new(@"^[\[\(] +\d+, +\d+ \] +\d+ +\d+\.\d{3}% +\d+\.\d{3}% #*$", RegexOptions.CultureInvariant);

    private static List<string> BucketLines(string? histogramText)
    {
        Assert.IsNotNull(histogramText);
        var lines = new List<string>();
        foreach (string line in histogramText.Split('\n'))
        {
            if (line.Length > 0 && (line[0] == '[' || line[0] == '(')) lines.Add(line);
        }
        return lines;
    }

    /// <summary>
    /// HistogramStat::ToString (monitoring/histogram.cc) through "rocksdb.cf-file-histogram" (DumpCFFileHistogram): the
    /// bucket lines of native and managed have the same shape, "( left, right ] count pct% cum% ###".
    /// </summary>
    [TestMethod]
    public void FileReadHistogramBucketLinesHaveNativeFormat()
    {
        using var nativeDir = new TempDir("a4hist-n");
        using var managedDir = new TempDir("a4hist-m");
        string? nativeText, managedText;
        var no = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetCompression(RocksDbSharp.Compression.No);
        no.EnableStatistics();
        s_keepAlive.Add(no);
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(no, nativeDir.Path))
        {
            for (int i = 0; i < 500; i++) db.Put(K(i), s_value);
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            for (int i = 0; i < 500; i += 7) Assert.IsNotNull(db.Get(K(i)));
            nativeText = db.GetProperty("rocksdb.cf-file-histogram");
        }
        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true, Statistics = new Statistics() },
            new ColumnFamilyOptions { Compression = RocksDb.Managed.Compression.CompressionType.None }))
        {
            for (int i = 0; i < 500; i++) db.Put(K(i), s_value);
            db.Flush();
            for (int i = 0; i < 500; i += 7) Assert.IsNotNull(db.Get(K(i)));
            managedText = db.GetProperty(DbProperties.CfFileHistogram);
        }
        List<string> nativeLines = BucketLines(nativeText), managedLines = BucketLines(managedText);
        Assert.IsTrue(nativeLines.Count > 0, "native recorded no file reads:\n" + nativeText);
        Assert.IsTrue(managedLines.Count > 0, "managed recorded no file reads:\n" + managedText);
        foreach (string line in nativeLines) Assert.IsTrue(s_bucketLine.IsMatch(line), "native line: '" + line + "'");
        foreach (string line in managedLines) Assert.IsTrue(s_bucketLine.IsMatch(line), "managed line: '" + line + "'");
    }
}
