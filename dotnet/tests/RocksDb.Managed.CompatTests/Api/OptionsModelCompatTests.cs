using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Env;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>
/// The options model against native 11.12: strict parsing of native's default OPTIONS files with every value written
/// back unchanged, struct option updates (OptionTypeInfo::ParseStruct), validation of SetOptions, the DBOptions
/// sanitization of max_open_files / delayed_write_rate, and the stats history column family in both directions.
/// </summary>
[TestClass]
public sealed class OptionsModelCompatTests
{
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();
    private const string StatsCf = "___rocksdb_stats_history___";

    private static KeyValuePair<string, string> P(string k, string v) => new(k, v);

    private static string LatestPath(string dir) => OptionsFile.FindLatestOptionsFile(LocalFileSystem.Instance, dir)!;

    private static OptionsFile Latest(string dir, bool strict) => OptionsFile.Load(LocalFileSystem.Instance, LatestPath(dir), preserveUnknownOptions: !strict);

    /// <summary>Statements of each section of an OPTIONS file: section title -> (name -> raw value).</summary>
    private static Dictionary<string, Dictionary<string, string>> Sections(string text)
    {
        var result = new Dictionary<string, Dictionary<string, string>>(StringComparer.Ordinal);
        Dictionary<string, string>? current = null;
        foreach (string raw in text.Split('\n'))
        {
            string line = raw.Trim();
            if (line.Length == 0 || line[0] == '#') continue;
            if (line[0] == '[')
            {
                current = new Dictionary<string, string>(StringComparer.Ordinal);
                result[line] = current;
                continue;
            }
            int eq = line.IndexOf('=');
            current![line[..eq]] = line[(eq + 1)..];
        }
        return result;
    }

    private static RocksDbSharp.DbOptions NativeOptions()
    {
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetCreateMissingColumnFamilies(true);
        s_keepAlive.Add(o);
        return o;
    }

    /// <summary>
    /// disallow_memtable_writes is typed by another work stream; until it is, it is the one native option the strict
    /// parse may not know.
    /// </summary>
    private static string WithoutPendingOptions(string text) =>
        new ColumnFamilyOptions().GetOption("disallow_memtable_writes") is null
            ? string.Join('\n', text.Split('\n').Where(static l => !l.TrimStart().StartsWith("disallow_memtable_writes=", StringComparison.Ordinal)))
            : text;

    [TestMethod]
    public void NativeDefaultOptionsFilesLoadStrictlyAndRoundTripEveryValue()
    {
        using var dir = new TempDir("opt-strict");
        RocksDbSharp.DbOptions o = NativeOptions();
        RocksDbSharp.Native.Instance.rocksdb_options_set_persist_stats_to_disk(o.Handle, 1);
        var cfs = new RocksDbSharp.ColumnFamilies { { "other", new RocksDbSharp.ColumnFamilyOptions() } };
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(o, dir.Path, cfs)) db.Put("k", "v");

        string nativeText = File.ReadAllText(LatestPath(dir.Path));
        OptionsFile parsed = OptionsFile.Parse(WithoutPendingOptions(nativeText), preserveUnknownOptions: false);
        Assert.AreEqual(0, parsed.DbOptions.UnknownOptions.Count);
        CollectionAssert.AreEquivalent(new[] { "default", "other", StatsCf }, parsed.ColumnFamilies.Select(static c => c.Name).ToArray());

        // Every value native wrote is written back byte-identically by the managed model (struct members in native's order).
        Dictionary<string, Dictionary<string, string>> native = Sections(nativeText);
        Dictionary<string, Dictionary<string, string>> managed = Sections(parsed.Serialize());
        foreach ((string section, Dictionary<string, string> nativeValues) in native)
        {
            if (section == "[Version]") continue;
            Assert.IsTrue(managed.TryGetValue(section, out Dictionary<string, string>? managedValues), section);
            foreach ((string name, string value) in nativeValues)
            {
                if (name == "disallow_memtable_writes" && !managedValues.ContainsKey(name)) continue;
                Assert.IsTrue(managedValues.TryGetValue(name, out string? mv), section + " " + name + " not written back");
                Assert.AreEqual(value, mv, section + " " + name);
            }
        }

        // Typed values.
        DbOptions d = parsed.DbOptions;
        Assert.IsTrue(d.PersistStatsToDisk);
        Assert.AreEqual(500, d.MaxManifestSpaceAmpPct);
        Assert.IsTrue(d.EnforceWriteBufferManagerDuringRecovery);
        Assert.AreEqual(10000UL, d.FollowerRefreshCatchupPeriodMs);
        Assert.AreEqual(Temperature.Unknown, d.WalWriteTemperature);
        ColumnFamilyOptions stats = parsed.FindColumnFamily(StatsCf)!;
        Assert.AreEqual(2UL << 20, stats.WriteBufferSize);
        Assert.AreEqual(PinningTier.Fallback, stats.TableOptions.MetadataCacheOptions.PartitionPinning);
        Assert.IsFalse(stats.CompressionOpts.AutoSkip);
        Assert.AreEqual(896, stats.BlobCompressionOpts.MaxCompressedBytesPerKb);
    }

    [TestMethod]
    public void StructSetOptionsMatchNative()
    {
        using var nativeDir = new TempDir("opt-struct-n");
        using var managedDir = new TempDir("opt-struct-m");
        KeyValuePair<string, string>[] changes =
        [
            P("compaction_options_universal", "{max_merge_width=9}"), P("compaction_options_universal.min_merge_width", "3"),
            P("compression_opts", "{max_dict_bytes=4096}"), P("compression_opts.zstd_max_train_bytes", "65536"),
            P("compaction_options_fifo.max_table_files_size", "12345"),
        ];
        var ncf = new RocksDbSharp.ColumnFamilyOptions().SetCompactionStyle(RocksDbSharp.Compaction.Universal);
        RocksDbSharp.Native.Instance.rocksdb_options_set_universal_compaction_options(ncf.Handle, NativeUniversal(sizeRatio: 7));
        RocksDbSharp.Native.Instance.rocksdb_options_set_compression_options(ncf.Handle, -14, 5, 0, 0);
        s_keepAlive.Add(ncf);
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(NativeOptions(), nativeDir.Path, new RocksDbSharp.ColumnFamilies(ncf)))
        {
            db.SetOptions(changes);
            Assert.ThrowsExactly<RocksDbSharp.RocksDbException>(() => db.SetOptions([P("compaction_options_universal", "{bogus=1}")]));
            Assert.ThrowsExactly<RocksDbSharp.RocksDbException>(() => db.SetOptions([P("compaction_options_universal.bogus", "1")]));
        }
        var mcf = new ColumnFamilyOptions { CompactionStyle = CompactionStyle.Universal };
        mcf.CompactionOptionsUniversal.SizeRatio = 7;
        mcf.CompressionOpts.Level = 5;
        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true }, mcf))
        {
            db.SetOptions(changes);
            Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => db.SetOptions([P("compaction_options_universal", "{bogus=1}")])).Code);
            Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => db.SetOptions([P("compaction_options_universal.bogus", "1")])).Code);
        }
        ColumnFamilyOptions n = Latest(nativeDir.Path, strict: false).FindColumnFamily("default")!;
        ColumnFamilyOptions m = Latest(managedDir.Path, strict: false).FindColumnFamily("default")!;
        foreach (string name in new[] { "compaction_options_universal", "compression_opts", "compaction_options_fifo" })
        {
            Assert.AreEqual(n.GetOption(name), m.GetOption(name), name);
        }
        Assert.AreEqual(7u, m.CompactionOptionsUniversal.SizeRatio);
        Assert.AreEqual(9u, m.CompactionOptionsUniversal.MaxMergeWidth);
        Assert.AreEqual(3u, m.CompactionOptionsUniversal.MinMergeWidth);
        Assert.AreEqual(5, m.CompressionOpts.Level);
        Assert.AreEqual(12345UL, m.CompactionOptionsFifo.MaxTableFilesSize);
    }

    private static IntPtr NativeUniversal(int sizeRatio)
    {
        IntPtr u = RocksDbSharp.Native.Instance.rocksdb_universal_compaction_options_create();
        RocksDbSharp.Native.Instance.rocksdb_universal_compaction_options_set_size_ratio(u, sizeRatio);
        return u;
    }

    [TestMethod]
    public void RejectedSetOptionsMatchesNative()
    {
        using var nativeDir = new TempDir("opt-reject-n");
        using var managedDir = new TempDir("opt-reject-m");
        KeyValuePair<string, string>[] bad = [P("write_buffer_size", "1048576"), P("memtable_protection_bytes_per_key", "3")];
        string nativeBefore, managedBefore;
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(NativeOptions(), nativeDir.Path))
        {
            nativeBefore = LatestPath(nativeDir.Path);
            Assert.ThrowsExactly<RocksDbSharp.RocksDbException>(() => db.SetOptions(bad));
            Assert.AreEqual(nativeBefore, LatestPath(nativeDir.Path));
            db.SetOptions([P("memtable_protection_bytes_per_key", "4")]);
        }
        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true }))
        {
            managedBefore = LatestPath(managedDir.Path);
            Assert.AreEqual(StatusCode.NotSupported, Assert.ThrowsExactly<RocksDbException>(() => db.SetOptions(bad)).Code);
            Assert.AreEqual(managedBefore, LatestPath(managedDir.Path));
            db.SetOptions([P("memtable_protection_bytes_per_key", "4")]);
        }
        ColumnFamilyOptions n = Latest(nativeDir.Path, strict: false).FindColumnFamily("default")!;
        ColumnFamilyOptions m = Latest(managedDir.Path, strict: false).FindColumnFamily("default")!;
        Assert.AreEqual(n.GetOption("write_buffer_size"), m.GetOption("write_buffer_size"));
        Assert.AreEqual("4", n.GetOption("memtable_protection_bytes_per_key"));
        Assert.AreEqual("4", m.GetOption("memtable_protection_bytes_per_key"));
    }

    [TestMethod]
    public void SetDbOptionsNoOpAndWalSwitchMatchNative()
    {
        using var nativeDir = new TempDir("opt-setdb-n");
        using var managedDir = new TempDir("opt-setdb-m");
        static void NativeSetDb(RocksDbSharp.RocksDb db, string k, string v) =>
            RocksDbSharp.Native.Instance.rocksdb_set_db_options(db.Handle, 1, [k], [v]);
        bool nativeNoOpWrote, nativeFlushed;
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(NativeOptions(), nativeDir.Path))
        {
            NativeSetDb(db, "bytes_per_sync", "1048576");
            string before = LatestPath(nativeDir.Path);
            NativeSetDb(db, "max_background_jobs", "2");
            nativeNoOpWrote = before != LatestPath(nativeDir.Path);
            for (int i = 0; i < 100; i++) db.Put("key" + i, "value" + i);
            NativeSetDb(db, "max_total_wal_size", "1");
            // SwitchWAL switched the memtable (its flush runs in the background).
            nativeFlushed = db.GetProperty("rocksdb.num-entries-active-mem-table") == "0";
        }
        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true }))
        {
            db.SetDBOptions([P("bytes_per_sync", "1048576")]);
            string before = LatestPath(managedDir.Path);
            db.SetDBOptions([P("max_background_jobs", "2")]);
            Assert.AreEqual(nativeNoOpWrote, before != LatestPath(managedDir.Path), "no-op SetDBOptions writes an OPTIONS file");
            Assert.IsFalse(nativeNoOpWrote);
            for (int i = 0; i < 100; i++) db.Put(Encoding.UTF8.GetBytes("key" + i), Encoding.UTF8.GetBytes("value" + i));
            db.SetDBOptions([P("max_total_wal_size", "1")]);
            db.WaitForCompact();
            Assert.AreEqual(nativeFlushed, db.GetProperty("rocksdb.num-entries-active-mem-table") == "0");
            Assert.IsTrue(nativeFlushed);
        }
    }

    [TestMethod]
    public void MaxOpenFilesAndDelayedWriteRateSanitizedLikeNative()
    {
        foreach (int maxOpenFiles in new[] { 5, 25 })
        {
            using var nativeDir = new TempDir("opt-mof-n");
            using var managedDir = new TempDir("opt-mof-m");
            RocksDbSharp.DbOptions o = NativeOptions().SetMaxOpenFiles(maxOpenFiles);
            IntPtr limiter = RocksDbSharp.Native.Instance.rocksdb_ratelimiter_create(3 << 20, 100_000, 10);
            RocksDbSharp.Native.Instance.rocksdb_options_set_ratelimiter(o.Handle, limiter);
            RocksDbSharp.Native.Instance.rocksdb_options_set_delayed_write_rate(o.Handle, 0);
            using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(o, nativeDir.Path)) db.Put("k", "v");
            RocksDbSharp.Native.Instance.rocksdb_ratelimiter_destroy(limiter);

            using var managedLimiter = new GenericRateLimiter(3 << 20);
            var mo = new DbOptions { CreateIfMissing = true, MaxOpenFiles = maxOpenFiles, RateLimiter = managedLimiter, DelayedWriteRate = 0 };
            using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, mo))
            {
                db.Put("k"u8, "v"u8);
                Assert.AreEqual(Math.Max(20, maxOpenFiles) - 10, db.DefaultColumnFamily.Db.SharedTableCache.Capacity);
            }
            DbOptions n = Latest(nativeDir.Path, strict: false).DbOptions, m = Latest(managedDir.Path, strict: false).DbOptions;
            foreach (string name in new[] { "max_open_files", "delayed_write_rate", "bytes_per_sync" })
            {
                Assert.AreEqual(n.GetOption(name), m.GetOption(name), name + " (max_open_files=" + maxOpenFiles + ")");
            }
            Assert.AreEqual(Math.Max(20, maxOpenFiles), m.MaxOpenFiles);
            Assert.AreEqual((ulong)(3 << 20), m.DelayedWriteRate);
        }
    }

    [TestMethod]
    public void NativeStatsHistoryOpensAndReadsInManaged()
    {
        using var dir = new TempDir("stats-n");
        RocksDbSharp.DbOptions o = NativeOptions().EnableStatistics();
        RocksDbSharp.Native.Instance.rocksdb_options_set_persist_stats_to_disk(o.Handle, 1);
        RocksDbSharp.Native.Instance.rocksdb_options_set_stats_persist_period_sec(o.Handle, 1);
        // The first open creates the stats column family; the second names it to get a handle (naming it at creation
        // would make native recreate it, as it then has no version keys).
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(o, dir.Path)) db.Put("k", "v");
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(o, dir.Path, StatsColumnFamilies()))
        {
            // Native persists on its own timer: wait for at least one delta sample (the first run only sets the baseline).
            DateTime deadline = DateTime.UtcNow.AddSeconds(30);
            RocksDbSharp.ColumnFamilyHandle stats = db.GetColumnFamily(StatsCf);
            int i = 0;
            while (DateTime.UtcNow < deadline && !HasNativeSample(db, stats))
            {
                db.Put("k" + i++, "v");
                Thread.Sleep(100);
            }
            Assert.IsTrue(HasNativeSample(db, stats), "native wrote no stats sample");
        }
        // Managed opens the native DB without naming the stats column family (it used to fail with
        // "Column families not opened") and reads the history native wrote.
        using (RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions()))
        {
            Assert.IsTrue(db.TryGetColumnFamily(StatsCf, out _));
        }
        using (RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions { PersistStatsToDisk = true, StatsPersistPeriodSec = 0 }))
        {
            IReadOnlyList<StatsHistorySlice> h = db.GetStatsHistory(0, ulong.MaxValue);
            Assert.IsTrue(h.Count >= 1);
            Assert.IsTrue(h[0].Stats.ContainsKey(Statistics.GetTickerName(Ticker.NumberKeysWritten)));
            Assert.IsTrue(h[0].Time > 1_600_000_000UL, h[0].Time.ToString(System.Globalization.CultureInfo.InvariantCulture));
        }
    }

    private static RocksDbSharp.ColumnFamilies StatsColumnFamilies()
    {
        var cfo = new RocksDbSharp.ColumnFamilyOptions();
        s_keepAlive.Add(cfo);
        return new RocksDbSharp.ColumnFamilies { { StatsCf, cfo } };
    }

    private static bool HasNativeSample(RocksDbSharp.RocksDb db, RocksDbSharp.ColumnFamilyHandle cf)
    {
        using RocksDbSharp.Iterator it = db.NewIterator(cf);
        for (it.SeekToFirst(); it.Valid(); it.Next())
        {
            if (it.StringKey().Contains('#', StringComparison.Ordinal)) return true;
        }
        return false;
    }

    [TestMethod]
    public void ManagedStatsHistoryOpensAndIsKeptByNative()
    {
        using var dir = new TempDir("stats-m");
        var mo = new DbOptions { CreateIfMissing = true, PersistStatsToDisk = true, StatsPersistPeriodSec = 0, Statistics = new Statistics() };
        string keysWritten = Statistics.GetTickerName(Ticker.NumberKeysWritten);
        using (RocksDatabase db = RocksDatabase.Open(dir.Path, mo))
        {
            db.Put("a"u8, "1"u8);
            db.DefaultColumnFamily.Db.PersistStats(1_700_000_000);
            db.Put("b"u8, "1"u8);
            db.Put("c"u8, "1"u8);
            db.DefaultColumnFamily.Db.PersistStats(1_700_000_600);
        }
        // Native opens the managed DB without naming the column family, and with persist_stats_to_disk accepts the
        // managed version keys (it would drop and recreate the column family otherwise): the managed sample survives.
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions(), dir.Path))
        {
            Assert.AreEqual("1", db.Get("a"));
        }
        RocksDbSharp.DbOptions o = NativeOptions();
        RocksDbSharp.Native.Instance.rocksdb_options_set_persist_stats_to_disk(o.Handle, 1);
        RocksDbSharp.Native.Instance.rocksdb_options_set_stats_persist_period_sec(o.Handle, 0);
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(o, dir.Path, StatsColumnFamilies()))
        {
            RocksDbSharp.ColumnFamilyHandle stats = db.GetColumnFamily(StatsCf);
            Assert.AreEqual("1", db.Get("__persistent_stats_format_version__", stats));
            Assert.AreEqual("2", db.Get("1700000600#" + keysWritten, stats));
        }
    }
}
