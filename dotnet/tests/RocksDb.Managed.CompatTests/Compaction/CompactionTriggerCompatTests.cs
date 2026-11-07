using System.Runtime.InteropServices;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Env;

namespace RocksDb.Managed.CompatTests.Compaction;

/// <summary>
/// Compaction triggers compared with native RocksDB 11.12 (rocksdb-sharp): read-triggered compaction
/// (read_triggered_compaction_threshold with the sampled reads of monitoring/file_read_sample.h, and the
/// max_compaction_trigger_wakeup_seconds DB option), and the CompactRange first-level selection with an SST partitioner
/// (a partition boundary inside the range makes file-level overlap sufficient; without one, keys inside the files are
/// checked: DBImpl::CompactRangeInternal / Version::OverlapWithLevelIterator).
/// </summary>
[TestClass]
public sealed class CompactionTriggerCompatTests
{
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);
    private static string K(int i) => "key" + i.ToString("D6", System.Globalization.CultureInfo.InvariantCulture);

    private static List<(int Level, ulong Size)> NativeLevels(RocksDbSharp.RocksDb db)
    {
        var l = new List<(int, ulong)>();
        foreach (RocksDbSharp.LiveFileMetadata f in db.GetLiveFilesMetadata()) l.Add((f.FileMetadata.FileLevel, f.FileMetadata.FileSize));
        l.Sort();
        return l;
    }

    private static List<(int Level, ulong Size)> ManagedLevels(RocksDatabase db)
    {
        var l = new List<(int, ulong)>();
        foreach (LiveFileMetadata f in db.GetLiveFilesMetadata()) l.Add((f.Level, f.Size));
        l.Sort();
        return l;
    }

    private static void AssertSameLevels(List<(int Level, ulong Size)> native, List<(int Level, ulong Size)> managed, string what)
    {
        Assert.AreEqual(native.Count, managed.Count, what + ": file count");
        for (int i = 0; i < native.Count; i++) Assert.AreEqual(native[i].Level, managed[i].Level, what + ": level of file " + i);
    }

    private static void NativeWaitForCompact(RocksDbSharp.RocksDb db)
    {
        RocksDbSharp.Native native = RocksDbSharp.Native.Instance;
        IntPtr wfc = native.rocksdb_wait_for_compact_options_create();
        try
        {
            native.rocksdb_wait_for_compact(db.Handle, wfc, out IntPtr err);
            if (err != IntPtr.Zero) Assert.Fail("native WaitForCompact failed");
        }
        finally
        {
            native.rocksdb_wait_for_compact_options_destroy(wfc);
        }
    }

    /// <summary>rocksdb_load_latest_options (ignore_unknown_options = false) of a DB with only the default CF.</summary>
    private static T NativeLoadOptions<T>(string dbPath, Func<IntPtr, IntPtr, T> read)
    {
        RocksDbSharp.Native native = RocksDbSharp.Native.Instance;
        IntPtr path = Marshal.StringToHGlobalAnsi(dbPath);
        IntPtr env = native.rocksdb_create_default_env();
        IntPtr cache = native.rocksdb_cache_create_lru((UIntPtr)(1 << 20));
        IntPtr outs = Marshal.AllocHGlobal(IntPtr.Size * 4);
        try
        {
            for (int i = 0; i < 4; i++) Marshal.WriteIntPtr(outs, i * IntPtr.Size, IntPtr.Zero);
            IntPtr dbOptsOut = outs, numOut = outs + IntPtr.Size, namesOut = outs + 2 * IntPtr.Size, cfOptsOut = outs + 3 * IntPtr.Size;
            native.rocksdb_load_latest_options(path, env, 0, cache, dbOptsOut, numOut, namesOut, cfOptsOut);
            int num = (int)Marshal.ReadIntPtr(numOut);
            Assert.AreEqual(1, num);
            T result = read(Marshal.ReadIntPtr(dbOptsOut), Marshal.ReadIntPtr(Marshal.ReadIntPtr(cfOptsOut)));
            native.rocksdb_load_latest_options_destroy(Marshal.ReadIntPtr(dbOptsOut), Marshal.ReadIntPtr(namesOut), Marshal.ReadIntPtr(cfOptsOut), (UIntPtr)num);
            return result;
        }
        finally
        {
            Marshal.FreeHGlobal(outs);
            Marshal.FreeHGlobal(path);
            native.rocksdb_cache_destroy(cache);
            native.rocksdb_env_destroy(env);
        }
    }

    // ---- read-triggered compaction ------------------------------------------------------------------------------

    private const int ReadTriggerLookups = 20000; // P(no lookup sampled at 1/1024) = e^-19.5

    private static RocksDbSharp.DbOptions NativeReadTriggeredOptions(double threshold)
    {
        RocksDbSharp.Native native = RocksDbSharp.Native.Instance;
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetCompression(RocksDbSharp.Compression.No);
        native.rocksdb_options_set_num_levels(o.Handle, 3);
        native.rocksdb_options_set_level0_file_num_compaction_trigger(o.Handle, 10);
        native.rocksdb_options_set_read_triggered_compaction_threshold(o.Handle, threshold);
        native.rocksdb_options_set_max_compaction_trigger_wakeup_seconds(o.Handle, 3600);
        native.rocksdb_options_set_disable_auto_compactions(o.Handle, 1);
        s_keepAlive.Add(o);
        return o;
    }

    private static ColumnFamilyOptions ManagedReadTriggeredOptions(double threshold) => new()
    {
        NumLevels = 3,
        Level0FileNumCompactionTrigger = 10,
        ReadTriggeredCompactionThreshold = threshold,
        DisableAutoCompactions = true,
        Compression = RocksDb.Managed.Compression.CompressionType.None,
    };

    /// <summary>
    /// db_compaction_test.cc ReadTriggeredCompaction, driven by real (randomly sampled) reads: 10 keys at L2, a
    /// tombstone file for half of them in L0, then many lookups of the deleted keys. Enabling auto compactions
    /// recomputes the score: both engines mark the L0 file (collapsible reads per byte above the threshold) and compact
    /// it into L2, ending with the same LSM shape. With threshold 0 neither compacts. Both read each other's OPTIONS.
    /// </summary>
    [TestMethod]
    [DataRow(0.001, DisplayName = "threshold 0.001")]
    [DataRow(0.0, DisplayName = "disabled")]
    public void ReadTriggeredCompactionMatchesNative(double threshold)
    {
        using var nativeDir = new TempDir("rtc-native");
        using var managedDir = new TempDir("rtc-managed");
        List<(int, ulong)> nativeLevels;
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(NativeReadTriggeredOptions(threshold), nativeDir.Path))
        {
            for (int i = 0; i < 10; i++) db.Put(U(K(i)), U(new string('v', 1024)));
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            db.CompactRange((byte[]?)null!, null!);
            for (int i = 0; i < 5; i++) db.Remove(U(K(i)));
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            for (int r = 0; r < ReadTriggerLookups; r++) Assert.IsNull(db.Get(U(K(r % 5))));
            db.SetOptions(new Dictionary<string, string> { ["disable_auto_compactions"] = "false" });
            NativeWaitForCompact(db);
            nativeLevels = NativeLevels(db);
        }
        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true, MaxCompactionTriggerWakeupSeconds = 3600 },
                   ManagedReadTriggeredOptions(threshold)))
        {
            for (int i = 0; i < 10; i++) db.Put(U(K(i)), U(new string('v', 1024)));
            db.Flush();
            db.CompactRange();
            for (int i = 0; i < 5; i++) db.Delete(U(K(i)));
            db.Flush();
            for (int r = 0; r < ReadTriggerLookups; r++) Assert.IsNull(db.Get(U(K(r % 5))));
            db.SetOptions(new Dictionary<string, string> { ["disable_auto_compactions"] = "false" });
            db.WaitForCompact();
            AssertSameLevels(nativeLevels, ManagedLevels(db), "after read-triggered compaction");
        }
        int nativeL0 = 0;
        foreach ((int level, ulong _) in nativeLevels) nativeL0 += level == 0 ? 1 : 0;
        Assert.AreEqual(threshold > 0 ? 0 : 1, nativeL0, "native compacted the read-hot tombstone file iff enabled");

        // OPTIONS: managed reads native's typed values; native loads managed's file without unknown options.
        OptionsFile nativeFile = OptionsFile.Load(LocalFileSystem.Instance, OptionsFile.FindLatestOptionsFile(LocalFileSystem.Instance, nativeDir.Path)!,
            preserveUnknownOptions: true);
        Assert.AreEqual(threshold, nativeFile.FindColumnFamily("default")!.ReadTriggeredCompactionThreshold);
        Assert.AreEqual(3600UL, nativeFile.DbOptions.MaxCompactionTriggerWakeupSeconds);
        (double t, ulong wakeup) = NativeLoadOptions(managedDir.Path, static (dbo, cfo) =>
            (RocksDbSharp.Native.Instance.rocksdb_options_get_read_triggered_compaction_threshold(cfo),
             RocksDbSharp.Native.Instance.rocksdb_options_get_max_compaction_trigger_wakeup_seconds(dbo)));
        Assert.AreEqual(threshold, t);
        Assert.AreEqual(3600UL, wakeup);
    }
    // ---- CompactRange with an SST partitioner ---------------------------------------------------------------------

    /// <summary>
    /// One L0 file with the keys "001" and "111", then CompactRange(begin, end) with an optional fixed-prefix
    /// partitioner (length 1), on both engines: the resulting level of every file must match.
    /// </summary>
    [TestMethod]
    [DataRow(0, "011", "101", DisplayName = "no partitioner, gap inside the file")]
    [DataRow(0, "100", "200", DisplayName = "no partitioner, key in range")]
    [DataRow(1, "011", "101", DisplayName = "partition boundary in range")]
    [DataRow(1, "011", "055", DisplayName = "no partition boundary in range")]
    public void CompactRangePartitionStraddleMatchesNative(int prefixLength, string begin, string end)
    {
        using var nativeDir = new TempDir("cr-native");
        using var managedDir = new TempDir("cr-managed");
        RocksDbSharp.Native native = RocksDbSharp.Native.Instance;
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetCompression(RocksDbSharp.Compression.No);
        native.rocksdb_options_set_disable_auto_compactions(o.Handle, 1);
        IntPtr factory = IntPtr.Zero;
        if (prefixLength > 0)
        {
            factory = native.rocksdb_sst_partitioner_fixed_prefix_factory_create((UIntPtr)prefixLength);
            native.rocksdb_options_set_sst_partitioner_factory(o.Handle, factory);
        }
        s_keepAlive.Add(o);
        List<(int, ulong)> nativeLevels;
        try
        {
            using RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(o, nativeDir.Path);
            db.Put(U("001"), U("a"));
            db.Put(U("111"), U("b"));
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            db.CompactRange(U(begin), U(end));
            nativeLevels = NativeLevels(db);
        }
        finally
        {
            if (factory != IntPtr.Zero) native.rocksdb_sst_partitioner_factory_destroy(factory);
        }
        var cf = new ColumnFamilyOptions
        {
            DisableAutoCompactions = true,
            Compression = RocksDb.Managed.Compression.CompressionType.None,
            SstPartitionerFactory = prefixLength > 0 ? new SstPartitionerFixedPrefixFactory(prefixLength) : null,
        };
        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true }, cf))
        {
            db.Put(U("001"), U("a"));
            db.Put(U("111"), U("b"));
            db.Flush();
            db.CompactRange(U(begin), U(end));
            AssertSameLevels(nativeLevels, ManagedLevels(db), "after CompactRange");
        }
    }
    // ---- preclude_last_level_data_seconds / seqno-to-time mapping ----------------------------------------------

    private const ulong PrecludeSeconds = 86400;

    private static RocksDbSharp.DbOptions NativeTieredOptions(ulong precludeSeconds, bool universal)
    {
        RocksDbSharp.Native native = RocksDbSharp.Native.Instance;
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetCompression(RocksDbSharp.Compression.No);
        native.rocksdb_options_set_num_levels(o.Handle, 7);
        native.rocksdb_options_set_preclude_last_level_data_seconds(o.Handle, precludeSeconds);
        if (universal)
        {
            o.SetCompactionStyle(RocksDbSharp.Compaction.Universal);
            native.rocksdb_options_set_level0_file_num_compaction_trigger(o.Handle, 4);
        }
        else
        {
            native.rocksdb_options_set_disable_auto_compactions(o.Handle, 1);
        }
        s_keepAlive.Add(o);
        return o;
    }

    private static ColumnFamilyOptions ManagedTieredOptions(ulong precludeSeconds, bool universal) => new()
    {
        NumLevels = 7,
        PrecludeLastLevelDataSeconds = precludeSeconds,
        CompactionStyle = universal ? CompactionStyle.Universal : CompactionStyle.Level,
        Level0FileNumCompactionTrigger = universal ? 4 : 4,
        DisableAutoCompactions = !universal,
        Compression = RocksDb.Managed.Compression.CompressionType.None,
    };

    private static byte[] Key(int i) => U("key" + i.ToString("D6", System.Globalization.CultureInfo.InvariantCulture));

    /// <summary>
    /// preclude_last_level_data_seconds on a new DB (leveled, 7 levels): both engines pre-allocate 100 sequence numbers
    /// (PrepopulateSeqnoToTimeMapping), write the seqno-to-time mapping into flushed tables, and a full CompactRange keeps
    /// the fresh data on the proximal level L5 (per-key placement). Each engine also honours the mapping the other
    /// wrote: native compacts a managed DB like its own and vice versa. Without the option data goes to L6.
    /// </summary>
    [TestMethod]
    [DataRow(PrecludeSeconds, DisplayName = "preclude 1 day")]
    [DataRow(0UL, DisplayName = "disabled")]
    public void PrecludeLastLevelPlacementMatchesNative(ulong precludeSeconds)
    {
        using var nativeDir = new TempDir("tier-native");
        using var managedDir = new TempDir("tier-managed");
        List<(int, ulong)> nativeLevels;
        ulong nativeSeq;
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(NativeTieredOptions(precludeSeconds, universal: false), nativeDir.Path))
        {
            Assert.AreEqual(precludeSeconds > 0 ? 100UL : 0UL, db.GetLatestSequenceNumber(), "native pre-allocated seqnos");
            for (int r = 0; r < 2; r++)
            {
                for (int i = 0; i < 200; i++) db.Put(Key(i), U("value-" + i + "-" + r));
                db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            }
            db.CompactRange((byte[]?)null!, null!);
            nativeLevels = NativeLevels(db);
            nativeSeq = db.GetLatestSequenceNumber();
        }
        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true }, ManagedTieredOptions(precludeSeconds, false)))
        {
            Assert.AreEqual(precludeSeconds > 0 ? 100UL : 0UL, db.GetLatestSequenceNumber(), "managed pre-allocated seqnos");
            for (int r = 0; r < 2; r++)
            {
                for (int i = 0; i < 200; i++) db.Put(Key(i), U("value-" + i + "-" + r));
                db.Flush();
            }
            db.CompactRange();
            AssertSameLevels(nativeLevels, ManagedLevels(db), "after CompactRange");
            Assert.AreEqual(nativeSeq, db.GetLatestSequenceNumber());
        }
        int expectedLevel = precludeSeconds > 0 ? 5 : 6;
        foreach ((int level, ulong _) in nativeLevels) Assert.AreEqual(expectedLevel, level);
        if (precludeSeconds == 0) return;

        // Cross-engine: each compacts the other's DB (after one more overlapping flush) using the mapping in its tables.
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(NativeTieredOptions(precludeSeconds, universal: false), managedDir.Path))
        {
            for (int i = 0; i < 200; i += 2) db.Put(Key(i), U("native-" + i));
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            db.CompactRange((byte[]?)null!, null!);
            foreach ((int level, ulong _) in NativeLevels(db)) Assert.AreEqual(5, level, "native keeps managed's fresh data above the last level");
            Assert.AreEqual("native-10", Encoding.UTF8.GetString(db.Get(Key(10))));
            Assert.AreEqual("value-11-1", Encoding.UTF8.GetString(db.Get(Key(11))));
        }
        using (RocksDatabase db = RocksDatabase.Open(nativeDir.Path, new DbOptions(), ManagedTieredOptions(precludeSeconds, false)))
        {
            for (int i = 0; i < 200; i += 2) db.Put(Key(i), U("managed-" + i));
            db.Flush();
            db.CompactRange();
            foreach ((int level, ulong _) in ManagedLevels(db)) Assert.AreEqual(5, level, "managed keeps native's fresh data above the last level");
            Assert.AreEqual("managed-10", Encoding.UTF8.GetString(db.Get(Key(10))!));
            Assert.AreEqual("value-11-1", Encoding.UTF8.GetString(db.Get(Key(11))!));
        }
        // Reopened without the option, the next full compaction moves everything to the last level in both engines.
        using (RocksDatabase db = RocksDatabase.Open(nativeDir.Path, new DbOptions(), ManagedTieredOptions(0, false)))
        {
            db.CompactRange();
            foreach ((int level, ulong _) in ManagedLevels(db)) Assert.AreEqual(6, level);
        }
    }

    /// <summary>
    /// Universal compaction with preclude_last_level_data_seconds in lock step (flush, wait for compactions): the
    /// size-amplification picker leaves the last sorted run out (ShouldSkipLastSortedRunForSizeAmpCompaction) and
    /// per-key placement keeps fresh data on the proximal level; every LSM shape matches native.
    /// </summary>
    [TestMethod]
    public void UniversalPrecludeLockStepMatchesNative()
    {
        using var nativeDir = new TempDir("tier-uni-native");
        using var managedDir = new TempDir("tier-uni-managed");
        const int Steps = 12;
        var nativeShapes = new List<List<(int, ulong)>>();
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(NativeTieredOptions(PrecludeSeconds, universal: true), nativeDir.Path))
        {
            for (int s = 0; s < Steps; s++)
            {
                for (int i = 0; i < 300; i++) db.Put(Key((i * 7 + s * 31) % 1000), U("v" + s + "-" + i));
                db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
                NativeWaitForCompact(db);
                nativeShapes.Add(NativeLevels(db));
            }
        }
        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true }, ManagedTieredOptions(PrecludeSeconds, true)))
        {
            for (int s = 0; s < Steps; s++)
            {
                for (int i = 0; i < 300; i++) db.Put(Key((i * 7 + s * 31) % 1000), U("v" + s + "-" + i));
                db.Flush();
                db.WaitForCompact();
                AssertSameLevels(nativeShapes[s], ManagedLevels(db), "step " + s);
            }
        }
    }
}
