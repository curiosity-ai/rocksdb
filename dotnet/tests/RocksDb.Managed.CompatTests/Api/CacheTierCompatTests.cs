using System.Runtime.InteropServices;
using RocksDb.Managed.Cache;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Env;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>
/// Cache-layer settings against native RocksDB through the C API of the NuGet librocksdb: db_write_buffer_size and
/// lowest_used_cache_tier round-trip through OPTIONS files in both directions (native loads managed's file with
/// rocksdb_load_latest_options, managed parses native's), and a shared WriteBufferManager (rocksdb_write_buffer_manager_*)
/// triggers flushes, charges a cache and reports usage like the managed one, on DBs either side can read.
/// </summary>
[TestClass]
public sealed unsafe class CacheTierCompatTests
{
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();
    private static readonly Lazy<IntPtr> s_library = new(LoadLibrary);

    private static IntPtr LoadLibrary()
    {
        _ = RocksDbSharp.Native.Instance;
        foreach (string line in File.ReadLines("/proc/self/maps"))
        {
            int slash = line.IndexOf('/', StringComparison.Ordinal);
            if (slash < 0) continue;
            string path = line[slash..];
            if (Path.GetFileName(path).StartsWith("librocksdb", StringComparison.Ordinal)) return NativeLibrary.Load(path);
        }
        throw new InvalidOperationException("librocksdb is not loaded");
    }

    private static IntPtr Export(string name) => NativeLibrary.GetExport(s_library.Value, name);

    private static bool HasExport(string name) => NativeLibrary.TryGetExport(s_library.Value, name, out _);

    private static OptionsFile Latest(string dir) =>
        OptionsFile.Load(LocalFileSystem.Instance, OptionsFile.FindLatestOptionsFile(LocalFileSystem.Instance, dir)!, preserveUnknownOptions: true);

    private static void SetLowestUsedCacheTier(RocksDbSharp.DbOptions o, int tier) =>
        ((delegate* unmanaged<IntPtr, int, void>)Export("rocksdb_options_set_lowest_used_cache_tier"))(o.Handle, tier);

    /// <summary>rocksdb_load_latest_options: the native view of a DB's latest OPTIONS file (db_write_buffer_size, lowest_used_cache_tier).</summary>
    private static (ulong DbWriteBufferSize, int LowestUsedCacheTier) NativeLoadLatest(string dir)
    {
        IntPtr env = RocksDbSharp.Native.Instance.rocksdb_create_default_env();
        IntPtr dbOptions, names, cfOptions, err = IntPtr.Zero;
        nuint numCfs;
        byte[] path = System.Text.Encoding.UTF8.GetBytes(dir + "\0");
        fixed (byte* p = path)
        {
            ((delegate* unmanaged<byte*, IntPtr, byte, IntPtr, IntPtr*, nuint*, IntPtr*, IntPtr*, IntPtr*, void>)Export("rocksdb_load_latest_options"))(
                p, env, 0, IntPtr.Zero, &dbOptions, &numCfs, &names, &cfOptions, &err);
        }
        if (err != IntPtr.Zero) Assert.Fail("rocksdb_load_latest_options: " + Marshal.PtrToStringUTF8(err));
        ulong wbs = ((delegate* unmanaged<IntPtr, nuint>)Export("rocksdb_options_get_db_write_buffer_size"))(dbOptions);
        int tier = ((delegate* unmanaged<IntPtr, int>)Export("rocksdb_options_get_lowest_used_cache_tier"))(dbOptions);
        ((delegate* unmanaged<IntPtr, IntPtr, IntPtr, nuint, void>)Export("rocksdb_load_latest_options_destroy"))(dbOptions, names, cfOptions, numCfs);
        RocksDbSharp.Native.Instance.rocksdb_env_destroy(env);
        return (wbs, tier);
    }

    [TestMethod]
    public void DbWriteBufferSizeAndLowestUsedCacheTierRoundTripWithNative()
    {
        using var nativeDir = new TempDir("wbmopt-n");
        using var managedDir = new TempDir("wbmopt-m");
        // Native writes the options; managed parses them into typed properties.
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetDbWriteBufferSize(4 << 20);
        SetLowestUsedCacheTier(o, (int)CacheTier.VolatileTier);
        s_keepAlive.Add(o);
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(o, nativeDir.Path)) db.Put("k"u8.ToArray(), "v"u8.ToArray());
        OptionsFile n = Latest(nativeDir.Path);
        Assert.AreEqual(4UL << 20, n.DbOptions.DbWriteBufferSize);
        Assert.AreEqual(CacheTier.VolatileTier, n.DbOptions.LowestUsedCacheTier);
        Assert.AreEqual("kVolatileTier", n.DbOptions.GetOption("lowest_used_cache_tier"));

        // Managed writes them; native loads managed's OPTIONS file.
        var mo = new DbOptions { CreateIfMissing = true, DbWriteBufferSize = 8 << 20, LowestUsedCacheTier = CacheTier.VolatileCompressedTier };
        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, mo)) db.Put("k"u8, "v"u8);
        Assert.AreEqual("kVolatileCompressedTier", Latest(managedDir.Path).DbOptions.GetOption("lowest_used_cache_tier"));
        (ulong wbs, int tier) = NativeLoadLatest(managedDir.Path);
        Assert.AreEqual(8UL << 20, wbs);
        Assert.AreEqual((int)CacheTier.VolatileCompressedTier, tier);
        // The default (kNonVolatileBlockTier / 0) round-trips too.
        Assert.AreEqual((0UL, (int)CacheTier.NonVolatileBlockTier), NativeLoadLatestDefaults());

        // Each side opens the other's DB with the persisted settings.
        using (RocksDatabase db = RocksDatabase.Open(nativeDir.Path, n.DbOptions)) Assert.AreEqual("v", System.Text.Encoding.UTF8.GetString(db.Get("k"u8)!));
        var o2 = new RocksDbSharp.DbOptions();
        s_keepAlive.Add(o2);
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(o2, managedDir.Path)) Assert.AreEqual("v", db.Get("k"));
    }

    private static (ulong, int) NativeLoadLatestDefaults()
    {
        using var dir = new TempDir("wbmopt-d");
        using (RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions { CreateIfMissing = true })) db.Put("k"u8, "v"u8);
        return NativeLoadLatest(dir.Path);
    }

    // ---- shared write buffer manager ---------------------------------------------------------------------------

    private static IntPtr NativeWbmCreate(long size, bool allowStall) =>
        ((delegate* unmanaged<nuint, byte, IntPtr>)Export("rocksdb_write_buffer_manager_create"))((nuint)size, allowStall ? (byte)1 : (byte)0);

    private static IntPtr NativeWbmCreateWithCache(long size, IntPtr cache) =>
        ((delegate* unmanaged<nuint, IntPtr, byte, IntPtr>)Export("rocksdb_write_buffer_manager_create_with_cache"))((nuint)size, cache, 0);

    private static long NativeWbm(string fn, IntPtr wbm) => (long)((delegate* unmanaged<IntPtr, nuint>)Export(fn))(wbm);

    private static void SetNativeWbm(RocksDbSharp.DbOptions o, IntPtr wbm) =>
        ((delegate* unmanaged<IntPtr, IntPtr, void>)Export("rocksdb_options_set_write_buffer_manager"))(o.Handle, wbm);

    private static int L0Files(RocksDbSharp.RocksDb db) => int.Parse(db.GetProperty("rocksdb.num-files-at-level0"), System.Globalization.CultureInfo.InvariantCulture);

    private static void WaitFor(Func<bool> condition)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        while (!condition() && sw.ElapsedMilliseconds < 30_000) Thread.Sleep(5);
        Assert.IsTrue(condition(), "condition not reached");
    }

    [TestMethod]
    public void SharedWriteBufferManagerFlushesLikeNative()
    {
        using var nativeA = new TempDir("wbm-na");
        using var nativeB = new TempDir("wbm-nb");
        using var managedA = new TempDir("wbm-ma");
        using var managedB = new TempDir("wbm-mb");
        const long limit = 2 << 20;
        byte[] value = new byte[1000];
        IntPtr nwbm = NativeWbmCreate(limit, allowStall: false);
        Assert.AreEqual(limit, NativeWbm("rocksdb_write_buffer_manager_buffer_size", nwbm));
        Assert.IsTrue(((delegate* unmanaged<IntPtr, byte>)Export("rocksdb_write_buffer_manager_enabled"))(nwbm) != 0);
        var wbm = new WriteBufferManager(limit, null, allowStall: false);
        Assert.AreEqual(limit, wbm.BufferSize);

        // Two DBs on each side share one 2 MiB manager with 64 MiB memtables: only the manager can trigger flushes.
        int nativeL0;
        {
            RocksDbSharp.RocksDb Open(string path)
            {
                var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetWriteBufferSize(64 << 20);
                SetNativeWbm(o, nwbm);
                s_keepAlive.Add(o);
                return RocksDbSharp.RocksDb.Open(o, path);
            }
            using RocksDbSharp.RocksDb a = Open(nativeA.Path), b = Open(nativeB.Path);
            for (int i = 0; i < 1500; i++) a.Put(System.Text.Encoding.ASCII.GetBytes("key" + i.ToString("D6")), value);
            Assert.IsTrue(NativeWbm("rocksdb_write_buffer_manager_memory_usage", nwbm) > 1 << 20);
            for (int i = 0; i < 1500; i++) b.Put(System.Text.Encoding.ASCII.GetBytes("key" + i.ToString("D6")), value);
            WaitFor(() => L0Files(a) + L0Files(b) > 0);
            nativeL0 = L0Files(a) + L0Files(b);
        }
        int managedL0;
        {
            RocksDatabase Open(string path) => RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true, WriteBufferManager = wbm },
                new ColumnFamilyOptions { WriteBufferSize = 64 << 20 });
            using RocksDatabase a = Open(managedA.Path), b = Open(managedB.Path);
            for (int i = 0; i < 1500; i++) a.Put(System.Text.Encoding.ASCII.GetBytes("key" + i.ToString("D6")), value);
            Assert.IsTrue(wbm.MemoryUsage > 1 << 20);
            for (int i = 0; i < 1500; i++) b.Put(System.Text.Encoding.ASCII.GetBytes("key" + i.ToString("D6")), value);
            int Files(RocksDatabase d) => int.Parse(d.GetProperty("rocksdb.num-files-at-level0")!, System.Globalization.CultureInfo.InvariantCulture);
            WaitFor(() => Files(a) + Files(b) > 0);
            managedL0 = Files(a) + Files(b);
        }
        Assert.IsTrue(nativeL0 >= 1 && managedL0 >= 1, "native " + nativeL0 + " managed " + managedL0);
        Assert.AreEqual(0, wbm.MemoryUsage, "closed DBs release their memtables");
        ((delegate* unmanaged<IntPtr, void>)Export("rocksdb_write_buffer_manager_destroy"))(nwbm);

        // The DBs written under the managers are readable by the other side.
        using (RocksDatabase db = RocksDatabase.Open(nativeA.Path, new DbOptions())) Assert.AreEqual(1000, db.Get("key000007"u8)!.Length);
        var o3 = new RocksDbSharp.DbOptions();
        s_keepAlive.Add(o3);
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(o3, managedB.Path)) Assert.AreEqual(1000, db.Get(System.Text.Encoding.ASCII.GetBytes("key000007")).Length);
    }

    [TestMethod]
    public void CostToCacheReservesPlaceholdersLikeNative()
    {
        const long dummy = CacheReservationManager.DummyEntrySize;
        IntPtr ncache = RocksDbSharp.Native.Instance.rocksdb_cache_create_lru(64 << 20);
        IntPtr nwbm = NativeWbmCreateWithCache(0, ncache);
        Assert.IsTrue(((delegate* unmanaged<IntPtr, byte>)Export("rocksdb_write_buffer_manager_cost_to_cache"))(nwbm) != 0);
        BlockCache cache = BlockCache.CreateLru(64 << 20);
        var wbm = new WriteBufferManager(0, cache, allowStall: false);
        byte[] value = new byte[1000];
        using var nativeDir = new TempDir("wbmc-n");
        using var managedDir = new TempDir("wbmc-m");
        long nativeReserved, managedReserved, nativeUsage, managedUsage;
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        SetNativeWbm(o, nwbm);
        s_keepAlive.Add(o);
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(o, nativeDir.Path))
        {
            for (int i = 0; i < 3000; i++) db.Put(System.Text.Encoding.ASCII.GetBytes("key" + i.ToString("D6")), value);
            nativeReserved = NativeWbm("rocksdb_write_buffer_manager_dummy_entries_in_cache_usage", nwbm);
            nativeUsage = NativeWbm("rocksdb_write_buffer_manager_memory_usage", nwbm);
        }
        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true, WriteBufferManager = wbm }))
        {
            for (int i = 0; i < 3000; i++) db.Put(System.Text.Encoding.ASCII.GetBytes("key" + i.ToString("D6")), value);
            managedReserved = wbm.DummyEntriesInCacheUsage;
            managedUsage = wbm.MemoryUsage;
            Assert.IsTrue(cache.PinnedUsage >= managedReserved);
        }
        // Both charge the memtable arena in 256 KiB placeholders covering the usage; the arena sizes agree within 25%.
        Assert.AreEqual(0, nativeReserved % dummy);
        Assert.AreEqual(0, managedReserved % dummy);
        Assert.IsTrue(nativeReserved >= nativeUsage && managedReserved >= managedUsage);
        Assert.IsTrue(Math.Abs(nativeUsage - managedUsage) <= nativeUsage / 4, "native " + nativeUsage + " managed " + managedUsage);
        ((delegate* unmanaged<IntPtr, void>)Export("rocksdb_write_buffer_manager_destroy"))(nwbm);
        RocksDbSharp.Native.Instance.rocksdb_cache_destroy(ncache);
    }

    [TestMethod]
    public void NativeWriteBufferManagerSetters()
    {
        IntPtr nwbm = NativeWbmCreate(1 << 20, allowStall: false);
        ((delegate* unmanaged<IntPtr, nuint, void>)Export("rocksdb_write_buffer_manager_set_buffer_size"))(nwbm, 4 << 20);
        ((delegate* unmanaged<IntPtr, byte, void>)Export("rocksdb_write_buffer_manager_set_allow_stall"))(nwbm, 1);
        var wbm = new WriteBufferManager(1 << 20, null, allowStall: false);
        wbm.SetBufferSize(4 << 20);
        wbm.SetAllowStall(true);
        Assert.AreEqual(NativeWbm("rocksdb_write_buffer_manager_buffer_size", nwbm), wbm.BufferSize);
        if (HasExport("rocksdb_write_buffer_manager_flush_policy"))
        {
            int policy = ((delegate* unmanaged<IntPtr, int>)Export("rocksdb_write_buffer_manager_flush_policy"))(nwbm);
            Assert.AreEqual((int)WriteBufferFlushPolicy.FlushOldest, policy);
            Assert.AreEqual(WriteBufferFlushPolicy.FlushOldest, wbm.FlushPolicy);
            ((delegate* unmanaged<IntPtr, int, void>)Export("rocksdb_write_buffer_manager_set_flush_policy"))(nwbm, 1);
            wbm.SetFlushPolicy(WriteBufferFlushPolicy.FlushLargest);
            Assert.AreEqual((int)wbm.FlushPolicy, ((delegate* unmanaged<IntPtr, int>)Export("rocksdb_write_buffer_manager_flush_policy"))(nwbm));
        }
        ((delegate* unmanaged<IntPtr, void>)Export("rocksdb_write_buffer_manager_destroy"))(nwbm);
    }
}
