using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;

namespace RocksDb.Managed.CompatTests.Compaction;

/// <summary>
/// Compatibility of the universal / FIFO compaction features with native RocksDB (rocksdb-sharp): files marked for
/// compaction by native's CompactOnDeletionCollector are picked by the managed universal picker exactly like native
/// does (delete-triggered compaction, compaction-pending clears), FIFO temperature changes written by managed are
/// reported by native and native file temperatures by managed, and a sequence gap (a write without WAL) ends both
/// engines' WAL iterators with NotFound.
/// </summary>
[TestClass]
public sealed class CompactionStyleCompatTests
{
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);
    private static string K(int i) => "key" + i.ToString("D6", System.Globalization.CultureInfo.InvariantCulture);

    private static RocksDbSharp.DbOptions NativeUniversalOptions(bool disableAutoCompactions)
    {
        RocksDbSharp.Native native = RocksDbSharp.Native.Instance;
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetCompactionStyle(RocksDbSharp.Compaction.Universal)
            .SetCompression(RocksDbSharp.Compression.No);
        // NewCompactOnDeletionCollectorFactory(window 128, trigger 32): files with dense tombstones are marked.
        native.rocksdb_options_add_compact_on_deletion_collector_factory(o.Handle, (UIntPtr)128, (UIntPtr)32);
        native.rocksdb_options_set_disable_auto_compactions(o.Handle, disableAutoCompactions ? 1 : 0);
        s_keepAlive.Add(o);
        return o;
    }

    private static ColumnFamilyOptions ManagedUniversal(bool disableAutoCompactions) => new()
    {
        CompactionStyle = CompactionStyle.Universal,
        Compression = RocksDb.Managed.Compression.CompressionType.None,
        DisableAutoCompactions = disableAutoCompactions,
    };

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

    /// <summary>
    /// Native writes a delete-heavy L0 file marked for compaction (MANIFEST need_compaction) under universal compaction
    /// with auto compactions disabled. Opened with auto compactions, both engines report compaction-pending, run the
    /// delete-triggered compaction (PickDeleteTriggeredCompaction / BuildCompactionToNextLevel: L0 to the last level)
    /// and end with the same LSM shape and nothing pending.
    /// </summary>
    [TestMethod]
    public void NativeMarkedFilesAreCompactedLikeNative()
    {
        using var dir = new TempDir("marked-native");
        using var managedCopy = new TempDir("marked-managed");
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(NativeUniversalOptions(disableAutoCompactions: true), dir.Path))
        {
            for (int i = 0; i < 2000; i++) db.Put(U(K(i)), U("value-" + i));
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            for (int i = 0; i < 2000; i += 2) db.Remove(U(K(i)));
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            Assert.AreEqual("1", db.GetProperty("rocksdb.compaction-pending"), "native marked the tombstone file");
        }
        foreach (string f in Directory.GetFiles(dir.Path)) File.Copy(f, Path.Combine(managedCopy.Path, Path.GetFileName(f)));

        // Managed sees the marked file (it used to leave compaction-pending at 1 without picking it).
        using (RocksDatabase db = RocksDatabase.Open(managedCopy.Path, new DbOptions(), ManagedUniversal(disableAutoCompactions: true)))
        {
            Assert.AreEqual("1", db.GetProperty(DbProperties.CompactionPending));
        }
        List<(int Level, ulong Size)> nativeLevels;
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(NativeUniversalOptions(disableAutoCompactions: false), dir.Path))
        {
            NativeWaitForCompact(db);
            Assert.AreEqual("0", db.GetProperty("rocksdb.compaction-pending"));
            nativeLevels = NativeLevels(db);
        }
        using (RocksDatabase db = RocksDatabase.Open(managedCopy.Path, new DbOptions(), ManagedUniversal(disableAutoCompactions: false)))
        {
            db.WaitForCompact();
            Assert.AreEqual("0", db.GetProperty(DbProperties.CompactionPending));
            List<(int Level, ulong Size)> managedLevels = ManagedLevels(db);
            Assert.AreEqual(nativeLevels.Count, managedLevels.Count, "files");
            for (int i = 0; i < nativeLevels.Count; i++) Assert.AreEqual(nativeLevels[i].Level, managedLevels[i].Level, "level of file " + i);
            Assert.AreEqual(1000, Count(db));
        }
        // And the managed result is readable by native.
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(NativeUniversalOptions(disableAutoCompactions: true), managedCopy.Path))
        {
            int n = 0;
            using RocksDbSharp.Iterator it = db.NewIterator();
            for (it.SeekToFirst(); it.Valid(); it.Next()) n++;
            Assert.AreEqual(1000, n);
        }
    }

    private static int Count(RocksDatabase db)
    {
        int n = 0;
        using Iterator it = db.NewIterator();
        for (it.SeekToFirst(); it.Valid; it.Next()) n++;
        return n;
    }

    /// <summary>
    /// Temperatures are MANIFEST metadata (NewFileCustomTag kTemperature): files native flushes with
    /// default_write_temperature are reported by managed, and the temperature a managed FIFO compaction assigns
    /// (file_temperature_age_thresholds, kChangeTemperature) is reported by native (GetLiveFilesStorageInfo).
    /// </summary>
    [TestMethod]
    public void FifoTemperatureChangeIsVisibleToNative()
    {
        using var dir = new TempDir("fifo-temperature");
        RocksDbSharp.Native native = RocksDbSharp.Native.Instance;
        var nativeOptions = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetCompactionStyle(RocksDbSharp.Compaction.Fifo);
        native.rocksdb_options_set_num_levels(nativeOptions.Handle, 1);
        native.rocksdb_options_set_default_write_temperature(nativeOptions.Handle, (int)Temperature.Warm);
        s_keepAlive.Add(nativeOptions);
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(nativeOptions, dir.Path))
        {
            db.Put(U("a"), U("1"));
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
        }
        var cf = new ColumnFamilyOptions { CompactionStyle = CompactionStyle.Fifo, NumLevels = 1 };
        ulong created;
        using (RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions(), cf))
        {
            List<LiveFileMetadata> files = db.GetLiveFilesMetadata();
            Assert.AreEqual(1, files.Count);
            Assert.AreEqual(Temperature.Warm, files[0].Temperature, "native default_write_temperature in the MANIFEST");
            created = files[0].FileCreationTime;
            Assert.AreNotEqual(0UL, created);
        }
        cf.CompactionOptionsFifo.FileTemperatureAgeThresholds = [new FileTemperatureAge(Temperature.Cold, 1)];
        using (RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions(), cf))
        {
            // The native file's newest key time is its flush time; once it is older than 1 s, the next compaction pick
            // (at the latest after the next flush) rewrites it as cold. The new file is younger than the threshold.
            WaitUntilUnixSecondPasses(created + 1);
            db.Put(U("b"), U("2"));
            db.Flush();
            db.WaitForCompact();
            List<LiveFileMetadata> after = db.GetLiveFilesMetadata();
            Assert.AreEqual(2, after.Count);
            int cold = 0;
            foreach (LiveFileMetadata f in after) cold += f.Temperature == Temperature.Cold ? 1 : 0;
            Assert.AreEqual(1, cold, "the old file was rewritten as cold");
        }
        var reopen = new RocksDbSharp.DbOptions().SetCompactionStyle(RocksDbSharp.Compaction.Fifo);
        native.rocksdb_options_set_num_levels(reopen.Handle, 1);
        native.rocksdb_options_set_disable_auto_compactions(reopen.Handle, 1);
        s_keepAlive.Add(reopen);
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(reopen, dir.Path))
        {
            IntPtr opts = native.rocksdb_livefiles_storage_info_options_create();
            IntPtr info = native.rocksdb_get_livefiles_storage_info(db.Handle, opts);
            try
            {
                int cold = 0, tables = 0;
                ulong n = (ulong)native.rocksdb_livefiles_storage_info_count(info);
                for (ulong i = 0; i < n; i++)
                {
                    if (native.rocksdb_livefiles_storage_info_file_type(info, (UIntPtr)i) != 2) continue; // kTableFile
                    tables++;
                    if (native.rocksdb_livefiles_storage_info_temperature(info, (UIntPtr)i) == (int)Temperature.Cold) cold++;
                }
                Assert.AreEqual(2, tables);
                Assert.AreEqual(1, cold, "native reads the temperature the managed compaction recorded");
            }
            finally
            {
                native.rocksdb_livefiles_storage_info_destroy(info);
                native.rocksdb_livefiles_storage_info_options_destroy(opts);
            }
            Assert.AreEqual("1", Encoding.UTF8.GetString(db.Get(U("a"))));
        }
    }

    private static void WaitUntilUnixSecondPasses(ulong second)
    {
        // Wall-clock based feature: poll the clock (no fixed sleep).
        var sw = System.Diagnostics.Stopwatch.StartNew();
        while ((ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds() <= second)
        {
            if (sw.Elapsed > TimeSpan.FromSeconds(10)) Assert.Fail("clock did not advance");
            Thread.Sleep(20);
        }
    }

    /// <summary>
    /// WalIteratorImpl::UpdateCurrentWriteBatch (11.12 and main): a discontinuity in sequence numbers (a write with
    /// disableWAL) ends the run with NotFound "Gap in sequence numbers"; the iterator is spent.
    /// </summary>
    [TestMethod]
    public void WalIteratorStopsAtSequenceGapLikeNative()
    {
        using var nativeDir = new TempDir("gap-native");
        using var managedDir = new TempDir("gap-managed");
        var nativeOptions = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        s_keepAlive.Add(nativeOptions);
        string nativeError;
        int nativeBatches = 0;
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(nativeOptions, nativeDir.Path))
        {
            db.Put(U("a"), U("1"));
            db.Put(U("b"), U("2"), null, new RocksDbSharp.WriteOptions().DisableWal(1));
            db.Put(U("c"), U("3"));
            using RocksDbSharp.TransactionLogIterator it = db.GetUpdatesSince(1);
            for (; it.Valid(); it.Next()) nativeBatches++;
            try
            {
                it.Status();
                nativeError = "";
            }
            catch (RocksDbSharp.RocksDbException e)
            {
                nativeError = e.Message;
            }
        }
        Assert.AreEqual(1, nativeBatches);
        StringAssert.Contains(nativeError, "Gap in sequence numbers");

        using RocksDatabase mdb = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true });
        mdb.Put(U("a"), U("1"));
        mdb.Put(new WriteOptions { DisableWal = true }, U("b"), U("2"));
        mdb.Put(U("c"), U("3"));
        using TransactionLogIterator mit = mdb.GetUpdatesSince(1);
        int managedBatches = 0;
        for (; mit.Valid; mit.Next()) managedBatches++;
        Assert.AreEqual(nativeBatches, managedBatches);
        RocksDbException? error = mit.Error;
        Assert.IsNotNull(error);
        Assert.AreEqual(StatusCode.NotFound, error.Code);
        StringAssert.Contains(error.Message, "Gap in sequence numbers");
        // Spent: Next does not resume.
        mit.Next();
        Assert.IsFalse(mit.Valid);
    }
}
