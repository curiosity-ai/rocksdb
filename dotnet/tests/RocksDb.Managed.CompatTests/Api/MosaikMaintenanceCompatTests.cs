using System.Buffers.Binary;
using System.Globalization;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Table.Filters;
using RocksDb.Managed.Utilities.Backup;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>
/// The maintenance, introspection, backup and lifecycle calls of the main downstream user (Mosaik's RocksDbBase /
/// RocksDbStorage / SafeRocksDb) run on native RocksDB (rocksdb-sharp, the exact C API calls Mosaik makes) and on the
/// managed port with the same option set: a separate WAL dir, statistics, a shared write buffer manager (allow_stall
/// false), a shared LRU block cache holding partitioned index/filter blocks, a point-lookup family and the replica
/// change tracker family (8-byte little-endian ulong comparator, 16-byte fixed prefix extractor, Bloom filter without
/// whole-key filtering).
/// </summary>
[TestClass]
public sealed class MosaikMaintenanceCompatTests
{
    private static readonly RocksDbSharp.Native N = RocksDbSharp.Native.Instance;
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    private const string ComparatorName = "Mosaik.GraphDB.ulong_Comparator";
    private const long CacheCapacity = 64L << 20;
    private const long WriteBufferLimit = 64L << 20;
    private static readonly string[] s_families = ["nodes", "content", "tracker"];

    /// <summary>Mosaik's ulong_Comparator: the first 8 bytes as a little-endian ulong.</summary>
    private sealed class ManagedUlongComparator : Comparator
    {
        public override string Name => ComparatorName;
        public override int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) =>
            BinaryPrimitives.ReadUInt64LittleEndian(a).CompareTo(BinaryPrimitives.ReadUInt64LittleEndian(b));
    }

    private sealed unsafe class NativeUlongComparator : RocksDbSharp.Comparator
    {
        public string Name => ComparatorName;
        public int Compare(IntPtr a, UIntPtr alen, IntPtr b, UIntPtr blen) =>
            BinaryPrimitives.ReadUInt64LittleEndian(new ReadOnlySpan<byte>((void*)a, (int)alen))
                .CompareTo(BinaryPrimitives.ReadUInt64LittleEndian(new ReadOnlySpan<byte>((void*)b, (int)blen)));
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Native side: the calls of Mosaik's DBOptionsExt, RocksDbBase.CreateTableOptions and
    // SafeRocksDb.ConfigureForChangeTracker
    // ---------------------------------------------------------------------------------------------------------------

    private sealed class NativeEnv : IDisposable
    {
        public RocksDbSharp.RocksDb Db = null!;
        public RocksDbSharp.DbOptions Options = null!;
        public IntPtr Wbm;
        public readonly Dictionary<string, RocksDbSharp.ColumnFamilyHandle> Cf = new(StringComparer.Ordinal);

        public RocksDbSharp.ColumnFamilyHandle this[string name] => Cf[name];

        public void Dispose() => Db?.Dispose();
    }

    private static RocksDbSharp.BlockBasedTableOptions NativeTableOptions(RocksDbSharp.Cache cache, bool bloom, bool wholeKey = true)
    {
        var t = new RocksDbSharp.BlockBasedTableOptions().SetBlockCache(cache);
        N.rocksdb_block_based_options_set_cache_index_and_filter_blocks(t.Handle, 1);
        N.rocksdb_block_based_options_set_cache_index_and_filter_blocks_with_high_priority(t.Handle, 1);
        N.rocksdb_block_based_options_set_pin_l0_filter_and_index_blocks_in_cache(t.Handle, 1);
        N.rocksdb_block_based_options_set_index_type(t.Handle, RocksDbSharp.BlockBasedTableIndexType.TwoLevelIndex);
        N.rocksdb_block_based_options_set_partition_filters(t.Handle, 1);
        N.rocksdb_block_based_options_set_pin_top_level_index_and_filter(t.Handle, 1);
        if (bloom) t.SetFilterPolicy(RocksDbSharp.BloomFilterPolicy.Create(10, false));
        if (!wholeKey) t.SetWholeKeyFiltering(false);
        s_keepAlive.Add(t);
        return t;
    }

    private static RocksDbSharp.DbOptions NativeDbOptions(string walDir, IntPtr wbm)
    {
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetCreateMissingColumnFamilies(true).SetWalDir(walDir);
        o.EnableStatistics();
        N.rocksdb_options_set_write_buffer_manager(o.Handle, wbm);
        o.SetWalRecoveryMode(RocksDbSharp.Recovery.SkipAnyCorruptedRecords);
        s_keepAlive.Add(o);
        return o;
    }

    private static NativeEnv OpenNative(string path, string walDir)
    {
        RocksDbSharp.Cache cache = RocksDbSharp.Cache.CreateLru((ulong)CacheCapacity);
        s_keepAlive.Add(cache);
        var env = new NativeEnv { Wbm = N.rocksdb_write_buffer_manager_create((UIntPtr)(ulong)WriteBufferLimit, 0) };
        env.Options = NativeDbOptions(walDir, env.Wbm);

        var nodes = new RocksDbSharp.ColumnFamilyOptions().SetBlockBasedTableFactory(NativeTableOptions(cache, bloom: false));
        RocksDbSharp.BlockBasedTableOptions pointTable = NativeTableOptions(cache, bloom: true);
        N.rocksdb_block_based_options_set_data_block_index_type(pointTable.Handle, (int)RocksDbSharp.BlockBasedTableDataBlockIndexType.BinarySearchAndHash);
        N.rocksdb_block_based_options_set_data_block_hash_table_util_ratio(pointTable.Handle, 0.75);
        var content = new RocksDbSharp.ColumnFamilyOptions().SetBlockBasedTableFactory(pointTable).SetMemtablePrefixBloomSizeRatio(0.02)
            .SetCompression(RocksDbSharp.Compression.Lz4);
        N.rocksdb_options_set_memtable_whole_key_filtering(content.Handle, 1);
        var tracker = new RocksDbSharp.ColumnFamilyOptions().SetPrefixExtractor(RocksDbSharp.SliceTransform.CreateFixedPrefix(16))
            .SetBlockBasedTableFactory(NativeTableOptions(cache, bloom: true, wholeKey: false)).SetOptimizeFiltersForHits(1);
        tracker.SetComparator(new NativeUlongComparator());
        var families = new RocksDbSharp.ColumnFamilies { { "nodes", nodes }, { "content", content }, { "tracker", tracker } };
        s_keepAlive.Add(families);
        env.Db = RocksDbSharp.RocksDb.Open(env.Options, path, families);
        foreach (string f in s_families) env.Cf[f] = env.Db.GetColumnFamily(f);
        return env;
    }

    /// <summary>RocksDbBase.FlushRocksDBMemory: rocksdb_flush (the default column family) with wait = true.</summary>
    private static void NativeFlush(RocksDbSharp.RocksDb db)
    {
        IntPtr fo = N.rocksdb_flushoptions_create();
        N.rocksdb_flushoptions_set_wait(fo, 1);
        try { N.rocksdb_flush(db.Handle, fo); }
        finally { N.rocksdb_flushoptions_destroy(fo); }
    }

    private static void NativeFlushCf(RocksDbSharp.RocksDb db, RocksDbSharp.ColumnFamilyHandle cf)
    {
        IntPtr fo = N.rocksdb_flushoptions_create();
        N.rocksdb_flushoptions_set_wait(fo, 1);
        try { N.rocksdb_flush_cf(db.Handle, fo, cf.Handle); }
        finally { N.rocksdb_flushoptions_destroy(fo); }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Managed side
    // ---------------------------------------------------------------------------------------------------------------

    private static BlockBasedTableOptions ManagedTableOptions(Cache.BlockCache cache, bool bloom, bool wholeKey = true) => new()
    {
        BlockCache = cache,
        CacheIndexAndFilterBlocks = true,
        CacheIndexAndFilterBlocksWithHighPriority = true,
        PinL0FilterAndIndexBlocksInCache = true,
        IndexType = IndexType.TwoLevelIndexSearch,
        PartitionFilters = true,
        PinTopLevelIndexAndFilter = true,
        FilterPolicy = bloom ? new BloomFilterPolicy(10) : null,
        WholeKeyFiltering = wholeKey,
    };

    private sealed class ManagedEnv : IDisposable
    {
        public RocksDatabase Db = null!;
        public WriteBufferManager Wbm = null!;
        public DbOptions Options = null!;
        public ColumnFamily this[string name] => Db.GetColumnFamily(name);
        public void Dispose() => Db?.Dispose();
    }

    private static (DbOptions Options, List<ColumnFamilyDescriptor> Families) ManagedOptions(string walDir)
    {
        Cache.BlockCache cache = RocksDb.Managed.Cache.BlockCache.CreateLru(CacheCapacity);
        var o = new DbOptions
        {
            CreateIfMissing = true, CreateMissingColumnFamilies = true, WalDir = walDir, Statistics = new Statistics(),
            WriteBufferManager = new WriteBufferManager(WriteBufferLimit, null, allowStall: false), WalRecoveryMode = WalRecoveryMode.SkipAnyCorruptedRecords,
        };
        BlockBasedTableOptions point = ManagedTableOptions(cache, bloom: true);
        point.DataBlockIndexType = DataBlockIndexType.BinaryAndHash;
        point.DataBlockHashTableUtilRatio = 0.75;
        var cfs = new List<ColumnFamilyDescriptor>
        {
            new("default", new ColumnFamilyOptions()),
            new("nodes", new ColumnFamilyOptions { TableOptions = ManagedTableOptions(cache, bloom: false) }),
            new("content", new ColumnFamilyOptions
            {
                TableOptions = point, MemtablePrefixBloomSizeRatio = 0.02, MemtableWholeKeyFiltering = true,
                Compression = RocksDb.Managed.Compression.CompressionType.LZ4,
            }),
            new("tracker", new ColumnFamilyOptions
            {
                PrefixExtractor = SliceTransform.CreateFixedPrefix(16), TableOptions = ManagedTableOptions(cache, bloom: true, wholeKey: false),
                OptimizeFiltersForHits = true, Comparator = new ManagedUlongComparator(),
            }),
        };
        return (o, cfs);
    }

    private static ManagedEnv OpenManaged(string path, string walDir)
    {
        (DbOptions o, List<ColumnFamilyDescriptor> cfs) = ManagedOptions(walDir);
        return new ManagedEnv { Db = RocksDatabase.Open(path, o, cfs), Wbm = o.WriteBufferManager!, Options = o };
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Workload
    // ---------------------------------------------------------------------------------------------------------------

    private static byte[] Key(string prefix, int i) => Encoding.ASCII.GetBytes(prefix + i.ToString("D8", CultureInfo.InvariantCulture));

    private static byte[] Value(int i, int size)
    {
        var v = new byte[size];
        for (int j = 0; j < v.Length; j++) v[j] = (byte)('a' + (i * 7 + j) % 13);
        return v;
    }

    /// <summary>A change tracker key: the 8-byte little-endian version, then 8 bytes of payload.</summary>
    private static byte[] TrackerKey(ulong version)
    {
        var k = new byte[16];
        BinaryPrimitives.WriteUInt64LittleEndian(k, version);
        BinaryPrimitives.WriteUInt64LittleEndian(k.AsSpan(8), version * 31);
        return k;
    }

    private static byte[] Version(ulong v)
    {
        var k = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(k, v);
        return k;
    }

    /// <summary>Calls put(cf, key, value) / del(cf, key) / flush(cf, null = DB-level flush) in the same order on both engines.</summary>
    private static void Workload(Action<string, byte[], byte[]> put, Action<string, byte[]> del, Action<string?> flush)
    {
        for (int i = 0; i < 3000; i++) put("nodes", Key("n", i), Value(i, 60));
        for (int i = 0; i < 1500; i++) put("content", Key("c", i), Value(i, 300));
        for (ulong v = 1; v <= 2000; v++) put("tracker", TrackerKey(v), Value((int)v, 40));
        for (int i = 0; i < 200; i++) put("default", Key("d", i), Value(i, 20));
        flush("nodes");
        flush("content");
        flush("tracker");
        flush(null);
        for (int i = 0; i < 3000; i += 3) del("nodes", Key("n", i));
        for (int i = 3000; i < 3400; i++) put("nodes", Key("n", i), Value(i, 60));
        for (ulong v = 1; v <= 500; v++) del("tracker", TrackerKey(v));
        flush("tracker");
        for (ulong v = 2001; v <= 2300; v++) put("tracker", TrackerKey(v), Value((int)v, 40));
        for (int i = 0; i < 50; i++) del("content", Key("c", i));
    }

    // Mosaik's RocksDbBase.UpdateStats (DB and every column family) and TemporaryMemoryCapture (per column family):
    // the counters, equal on both engines for the same workload.
    private static readonly string[] s_exact =
    [
        "rocksdb.num-immutable-mem-table", "rocksdb.mem-table-flush-pending", "rocksdb.num-running-flushes", "rocksdb.compaction-pending",
        "rocksdb.num-running-compactions", "rocksdb.background-errors", "rocksdb.num-entries-active-mem-table", "rocksdb.num-entries-imm-mem-tables",
        "rocksdb.num-deletes-active-mem-table", "rocksdb.num-deletes-imm-mem-tables", "rocksdb.estimate-num-keys", "rocksdb.num-snapshots",
        "rocksdb.num-live-versions", "rocksdb.obsolete-sst-files-size", "rocksdb.base-level", "rocksdb.estimate-pending-compaction-bytes",
        "rocksdb.actual-delayed-write-rate", "rocksdb.is-write-stopped", "rocksdb.estimate-oldest-key-time", "rocksdb.block-cache-capacity",
        "rocksdb.num-files-at-level0",
    ];

    // Sizes of table files: same entries and block layout, compared within 15 %.
    private static readonly string[] s_fileSizes = ["rocksdb.estimate-live-data-size", "rocksdb.live-sst-files-size", "rocksdb.total-sst-files-size"];

    // Memory estimates: arena, reader object and cache handle overheads differ; only checked to be numbers on both sides
    // (Mosaik's long.TryParse turns a missing property into -1) and reported.
    private static readonly string[] s_memory =
    [
        "rocksdb.cur-size-active-mem-table", "rocksdb.cur-size-all-mem-tables", "rocksdb.size-all-mem-tables", "rocksdb.estimate-table-readers-mem",
        "rocksdb.block-cache-usage", "rocksdb.block-cache-pinned-usage", "rocksdb.current-super-version-number", "rocksdb.oldest-snapshot-time",
    ];

    private static long Parse(string? s) => long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out long v) ? v : -1;

    private static IEnumerable<string> AllProperties => s_exact.Concat(s_fileSizes).Concat(s_memory);

    /// <summary>
    /// Every property Mosaik reads (RocksDbBase.UpdateStats for the DB and each column family, TemporaryMemoryCapture per
    /// column family, rocksdb.dbstats, the statistics string) has the same meaning, unit and format on both engines after
    /// the same workload: the counters are equal, the table file sizes agree within the table byte differences, every
    /// memory estimate is a number on both sides, and MemoryUtil (DB added, no cache) and the write buffer manager move
    /// the same way.
    /// </summary>
    [TestMethod]
    public void MosaikPropertiesMatchNative()
    {
        using var nDir = new TempDir("mosaik-props-n");
        using var mDir = new TempDir("mosaik-props-m");
        var report = new StringBuilder();
        var nativeValues = new Dictionary<string, string?>(StringComparer.Ordinal);
        var managedValues = new Dictionary<string, string?>(StringComparer.Ordinal);
        string nDbStats, mDbStats, nStatsString, mStatsString;
        long nWbmBefore, nWbmAfter, mWbmBefore, mWbmAfter;
        (ulong MemTotal, ulong MemUnflushed, ulong Readers, ulong Cache) nUsage, mUsage;

        using (NativeEnv n = OpenNative(Path.Combine(nDir.Path, "db"), Path.Combine(nDir.Path, "journal")))
        {
            Workload((cf, k, v) => n.Db.Put(k, v, cf == "default" ? null : n[cf]), (cf, k) => n.Db.Remove(k, cf == "default" ? null : n[cf]),
                cf => { if (cf is null) NativeFlush(n.Db); else NativeFlushCf(n.Db, n[cf]); });
            for (int i = 0; i < 3400; i += 7) n.Db.Get(Key("n", i), n["nodes"]);
            for (int i = 0; i < 1500; i += 7) n.Db.Get(Key("c", i), n["content"]);
            RocksDbSharp.Snapshot snap = n.Db.CreateSnapshot();
            foreach (string cfName in new[] { "default" }.Concat(s_families))
            {
                foreach (string p in AllProperties) nativeValues[cfName + "|" + p] = cfName == "default" ? n.Db.GetProperty(p) : n.Db.GetProperty(p, n[cfName]);
            }
            nDbStats = n.Db.GetProperty("rocksdb.dbstats");
            nStatsString = n.Options.GetStatisticsString();
            nUsage = NativeMemoryUsage(n.Db);
            nWbmBefore = (long)(ulong)N.rocksdb_write_buffer_manager_memory_usage(n.Wbm);
            snap.Dispose();
            foreach (string f in s_families) NativeFlushCf(n.Db, n[f]);
            NativeFlush(n.Db);
            nWbmAfter = (long)(ulong)N.rocksdb_write_buffer_manager_memory_usage(n.Wbm);
        }

        using (ManagedEnv m = OpenManaged(Path.Combine(mDir.Path, "db"), Path.Combine(mDir.Path, "journal")))
        {
            Workload((cf, k, v) => m[cf].Put(k, v), (cf, k) => m[cf].Delete(k), cf => { if (cf is null) m.Db.Flush(); else m[cf].Flush(); });
            for (int i = 0; i < 3400; i += 7) m["nodes"].Get(Key("n", i));
            for (int i = 0; i < 1500; i += 7) m["content"].Get(Key("c", i));
            Snapshot snap = m.Db.GetSnapshot();
            foreach (string cfName in new[] { "default" }.Concat(s_families))
            {
                foreach (string p in AllProperties) managedValues[cfName + "|" + p] = cfName == "default" ? m.Db.GetProperty(p) : m[cfName].GetProperty(p);
            }
            mDbStats = m.Db.GetProperty("rocksdb.dbstats")!;
            mStatsString = m.Options.Statistics!.ToString();
            MemoryUsageByType u = MemoryUtil.GetApproximateMemoryUsageByType([m.Db], []);
            mUsage = (u.MemTableTotal, u.MemTableUnflushed, u.TableReadersTotal, u.CacheTotal);
            mWbmBefore = m.Wbm.MemoryUsage;
            snap.Dispose();
            foreach (string f in s_families) m[f].Flush();
            m.Db.Flush();
            mWbmAfter = m.Wbm.MemoryUsage;
        }

        foreach (string key in nativeValues.Keys)
        {
            report.Append(key).Append(": native=").Append(nativeValues[key] ?? "(null)").Append(" managed=").Append(managedValues[key] ?? "(null)").Append('\n');
        }
        report.Append("memory usage native=").Append(nUsage).Append(" managed=").Append(mUsage).Append('\n');
        report.Append("wbm native=").Append(nWbmBefore).Append("->").Append(nWbmAfter).Append(" managed=").Append(mWbmBefore).Append("->").Append(mWbmAfter).Append('\n');
        string r = report.ToString();
        Console.WriteLine(r);

        foreach (string cfName in new[] { "default" }.Concat(s_families))
        {
            foreach (string p in s_exact) Assert.AreEqual(nativeValues[cfName + "|" + p], managedValues[cfName + "|" + p], cfName + " " + p + "\n" + r);
            foreach (string p in s_fileSizes)
            {
                long nv = Parse(nativeValues[cfName + "|" + p]), mv = Parse(managedValues[cfName + "|" + p]);
                Assert.IsTrue(nv > 0 && mv > 0 && Math.Abs(nv - mv) <= nv * 0.15, cfName + " " + p + "\n" + r);
            }
            foreach (string p in s_memory)
            {
                Assert.IsTrue(Parse(nativeValues[cfName + "|" + p]) >= 0, "native " + cfName + " " + p + "\n" + r);
                Assert.IsTrue(Parse(managedValues[cfName + "|" + p]) >= 0, "managed " + cfName + " " + p + "\n" + r);
            }
        }

        // The oldest snapshot time is a unix time in seconds on both sides.
        long now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        Assert.IsTrue(Math.Abs(now - Parse(managedValues["default|rocksdb.oldest-snapshot-time"])) < 300, r);
        Assert.IsTrue(Math.Abs(now - Parse(nativeValues["default|rocksdb.oldest-snapshot-time"])) < 300, r);

        // rocksdb.dbstats: same layout (Mosaik logs it as text).
        StringAssert.StartsWith(nDbStats, "\n** DB Stats **\nUptime(secs):");
        StringAssert.StartsWith(mDbStats, "\n** DB Stats **\nUptime(secs):");
        CollectionAssert.AreEqual(DbStatsLabels(nDbStats), DbStatsLabels(mDbStats), nDbStats + "\nvs\n" + mDbStats);

        // DbOptions.GetStatisticsString (Mosaik's GetStatistics) is Statistics::ToString on both sides.
        StringAssert.Contains(nStatsString, "rocksdb.block.cache.miss COUNT : ");
        StringAssert.Contains(mStatsString, "rocksdb.block.cache.miss COUNT : ");

        // MemoryUtil with only the DB added (no caches): the cache total is 0 on both, the rest the aggregated properties.
        Assert.AreEqual(0UL, nUsage.Cache);
        Assert.AreEqual(0UL, mUsage.Cache);
        Assert.IsTrue(nUsage.MemTotal > 0 && mUsage.MemTotal > 0 && nUsage.Readers > 0 && mUsage.Readers > 0, r);

        // The write buffer manager charges the memtables and releases them once flushed.
        Assert.IsTrue(nWbmBefore > 0 && mWbmBefore > 0, r);
        Assert.IsTrue(nWbmAfter < nWbmBefore, r);
        Assert.IsTrue(mWbmAfter < mWbmBefore, r);
    }

    private static List<string> DbStatsLabels(string s)
    {
        var labels = new List<string>();
        foreach (string line in s.Split('\n'))
        {
            int colon = line.IndexOf(':');
            labels.Add(colon < 0 ? line : line[..colon]);
        }
        return labels;
    }

    /// <summary>RocksDbBase.UpdateMemoryUsage: only the DB is added to the consumers, no cache.</summary>
    private static (ulong, ulong, ulong, ulong) NativeMemoryUsage(RocksDbSharp.RocksDb db)
    {
        IntPtr consumers = N.rocksdb_memory_consumers_create();
        try
        {
            N.rocksdb_memory_consumers_add_db(consumers, db.Handle);
            IntPtr usage = N.rocksdb_approximate_memory_usage_create(consumers);
            try
            {
                return (N.rocksdb_approximate_memory_usage_get_mem_table_total(usage), N.rocksdb_approximate_memory_usage_get_mem_table_unflushed(usage),
                    N.rocksdb_approximate_memory_usage_get_mem_table_readers_total(usage), N.rocksdb_approximate_memory_usage_get_cache_total(usage));
            }
            finally
            {
                N.rocksdb_approximate_memory_usage_destroy(usage);
            }
        }
        finally
        {
            N.rocksdb_memory_consumers_destroy(consumers);
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Write buffer manager with many idle column families
    // ---------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Mosaik opens about 35 column families per database, most of them idle, under one write buffer manager of 32 MiB in
    /// low-memory mode (allow_stall = false). An empty memtable costs the manager the arena's 2 KiB inline block on
    /// native RocksDB (memory/arena.cc), so the idle families charge almost nothing and a trickle of small writes to one
    /// family never makes the manager ask for a flush. The managed arena had no inline block: the first allocation (the
    /// skip list heads, at memtable creation) took a whole arena block (1 MiB = min(1 MiB, write_buffer_size / 8)), so
    /// 40 idle families charged 40 MiB, above the 28 MiB mutable limit, and every write flushed a memtable.
    /// </summary>
    [TestMethod]
    public void IdleColumnFamiliesDoNotFillTheWriteBufferManager()
    {
        const int families = 40;
        const long limit = 32L << 20;
        using var nDir = new TempDir("mosaik-wbm-n");
        using var mDir = new TempDir("mosaik-wbm-m");
        long nOpen, nWritten, mOpen, mWritten;
        string? nL0, mL0;

        IntPtr wbm = N.rocksdb_write_buffer_manager_create((UIntPtr)(ulong)limit, 0);
        RocksDbSharp.DbOptions no = NativeDbOptions(Path.Combine(nDir.Path, "journal"), wbm);
        var nFamilies = new RocksDbSharp.ColumnFamilies();
        for (int i = 0; i < families; i++) nFamilies.Add("cf" + i.ToString(CultureInfo.InvariantCulture), new RocksDbSharp.ColumnFamilyOptions());
        s_keepAlive.Add(nFamilies);
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(no, Path.Combine(nDir.Path, "db"), nFamilies))
        {
            nOpen = (long)(ulong)N.rocksdb_write_buffer_manager_memory_usage(wbm);
            RocksDbSharp.ColumnFamilyHandle cf = db.GetColumnFamily("cf0");
            for (int i = 0; i < 2000; i++) db.Put(Key("k", i), Value(i, 100), cf);
            nWritten = (long)(ulong)N.rocksdb_write_buffer_manager_memory_usage(wbm);
            nL0 = db.GetProperty("rocksdb.num-files-at-level0", cf);
        }

        var mwbm = new WriteBufferManager(limit, null, allowStall: false);
        var mFamilies = new List<ColumnFamilyDescriptor> { new("default", new ColumnFamilyOptions()) };
        for (int i = 0; i < families; i++) mFamilies.Add(new("cf" + i.ToString(CultureInfo.InvariantCulture), new ColumnFamilyOptions()));
        var mo = new DbOptions { CreateIfMissing = true, CreateMissingColumnFamilies = true, WalDir = Path.Combine(mDir.Path, "journal"), WriteBufferManager = mwbm };
        using (RocksDatabase db = RocksDatabase.Open(Path.Combine(mDir.Path, "db"), mo, mFamilies))
        {
            mOpen = mwbm.MemoryUsage;
            ColumnFamily cf = db.GetColumnFamily("cf0");
            for (int i = 0; i < 2000; i++) cf.Put(Key("k", i), Value(i, 100));
            mWritten = mwbm.MemoryUsage;
            mL0 = cf.GetProperty("rocksdb.num-files-at-level0");
        }
        string r = "native open=" + nOpen + " written=" + nWritten + " L0=" + nL0 + "; managed open=" + mOpen + " written=" + mWritten + " L0=" + mL0;
        Console.WriteLine(r);
        Assert.AreEqual("0", nL0, r);
        Assert.AreEqual(nL0, mL0, r);
        // Native: the inline block of each of the 41 memtables.
        Assert.AreEqual((families + 1) * 2048L, nOpen, r);
        Assert.AreEqual(nOpen, mOpen, r);
        Assert.IsTrue(Math.Abs(nWritten - mWritten) <= (1L << 20), r);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // CompactRange / DeleteFilesInRange
    // ---------------------------------------------------------------------------------------------------------------

    /// <summary>Files per level, then the entries / deletions / range deletions of the column family's tables.</summary>
    private static string Shape(Func<string, string?> property)
    {
        var sb = new StringBuilder();
        for (int l = 0; l < 7; l++) sb.Append('L').Append(l).Append('=').Append(property("rocksdb.num-files-at-level" + l.ToString(CultureInfo.InvariantCulture))).Append(' ');
        string agg = property("rocksdb.aggregated-table-properties") ?? "";
        foreach (string name in new[] { "# entries", "# deletions", "# range deletions" })
        {
            System.Text.RegularExpressions.Match m = System.Text.RegularExpressions.Regex.Match(agg, "(?:^|; )" + name + "=(\\d+)");
            sb.Append(name).Append('=').Append(m.Success ? m.Groups[1].Value : "?").Append(' ');
        }
        return sb.ToString();
    }

    private static List<string> NativeKeys(RocksDbSharp.RocksDb db, RocksDbSharp.ColumnFamilyHandle? cf)
    {
        var list = new List<string>();
        using RocksDbSharp.Iterator it = db.NewIterator(cf);
        for (it.SeekToFirst(); it.Valid(); it.Next()) list.Add(Convert.ToHexString(it.Key()) + "=" + Convert.ToHexString(it.Value()));
        return list;
    }

    private static List<string> ManagedKeys(ColumnFamily cf)
    {
        var list = new List<string>();
        using Iterator it = cf.NewIterator();
        for (it.SeekToFirst(); it.Valid; it.Next()) list.Add(Convert.ToHexString(it.Key) + "=" + Convert.ToHexString(it.Value));
        return list;
    }

    private static byte[] QueueKey(ulong uid64, int i)
    {
        var k = new byte[24];
        BinaryPrimitives.WriteUInt64LittleEndian(k, uid64);
        BinaryPrimitives.WriteInt64BigEndian(k.AsSpan(8), i);
        return k;
    }

    /// <summary>
    /// The maintenance sequences Mosaik runs, step by step on both engines, with the same LSM shape (files per level,
    /// entries, deletions, range deletions) and the same contents after each step:
    /// <list type="bullet">
    /// <item>RocksDbBase acknowledging replicas: point deletes of the acknowledged change-tracker versions, then
    /// CompactRange(8-byte version 0, 8-byte last acknowledged version) on the ulong-comparator family (16-byte keys,
    /// 16-byte fixed prefix extractor: the 8-byte bounds are outside the prefix domain).</item>
    /// <item>RocksDbStorage.ClearDeprecatedChangeLog: rocksdb_delete_file_in_range_cf(8 x 00, 8 x FF) (include_end =
    /// true), rocksdb_delete_range_cf over the same range, CompactRange(null, null): nothing may survive, data in L0
    /// and in the memtable included.</item>
    /// <item>DeleteFilesInRange with the end key equal to a file's largest key (include_end = true deletes the file).</item>
    /// <item>RocksDbIndexStorage.ClearToIndexQueue: rocksdb_compact_range_cf_opt with exclusive_manual_compaction =
    /// false and target_level = -1 over [uid64 + 16 x 00, uid64 + 16 x FF].</item>
    /// </list>
    /// </summary>
    [TestMethod]
    public void MosaikCompactionSequencesMatchNative()
    {
        using var nDir = new TempDir("mosaik-compact-n");
        using var mDir = new TempDir("mosaik-compact-m");
        var nShapes = new List<string>();
        var mShapes = new List<string>();
        var nKeys = new List<List<string>>();
        var mKeys = new List<List<string>>();
        byte[] zero8 = new byte[8], ff8 = [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];
        byte[] queueFrom = new byte[24], queueTo = new byte[24];
        BinaryPrimitives.WriteUInt64LittleEndian(queueFrom, 77);
        queueTo.AsSpan().Fill(0xFF);
        BinaryPrimitives.WriteUInt64LittleEndian(queueTo, 77);

        using (NativeEnv n = OpenNative(Path.Combine(nDir.Path, "db"), Path.Combine(nDir.Path, "journal")))
        {
            RocksDbSharp.RocksDb db = n.Db;
            void Step(string cf)
            {
                nShapes.Add(cf + ": " + Shape(p => db.GetProperty(p, n[cf])));
                nKeys.Add(NativeKeys(db, n[cf]));
            }
            // Change tracker: three L0 files of versions, acknowledged versions deleted, then compacted.
            for (ulong part = 0; part < 3; part++)
            {
                for (ulong v = part * 1000 + 1; v <= part * 1000 + 1000; v++) db.Put(TrackerKey(v), Value((int)v, 40), n["tracker"]);
                NativeFlushCf(db, n["tracker"]);
            }
            Step("tracker");
            for (ulong v = 1; v <= 1500; v++) db.Remove(TrackerKey(v), n["tracker"]);
            db.CompactRange(Version(0), Version(1500), n["tracker"]);
            Step("tracker");
            for (ulong v = 1501; v <= 2500; v++) db.Remove(TrackerKey(v), n["tracker"]);
            NativeFlushCf(db, n["tracker"]);
            db.CompactRange(Version(0), Version(2500), n["tracker"]);
            Step("tracker");

            // Changelog clear on a bytewise family with files on the last level, in L0 and in the memtable.
            for (int i = 0; i < 2000; i++) db.Put(Key("n", i), Value(i, 60), n["nodes"]);
            db.CompactRange((byte[]?)null, null, n["nodes"]);
            for (int i = 2000; i < 2500; i++) db.Put(Key("n", i), Value(i, 60), n["nodes"]);
            NativeFlushCf(db, n["nodes"]);
            for (int i = 2500; i < 2600; i++) db.Put(Key("n", i), Value(i, 60), n["nodes"]);
            Step("nodes");
            N.rocksdb_delete_file_in_range_cf(db.Handle, n["nodes"].Handle, zero8, (UIntPtr)8, ff8, (UIntPtr)8, out IntPtr err);
            Assert.AreEqual(IntPtr.Zero, err);
            Step("nodes");
            var wo = new RocksDbSharp.WriteOptions();
            N.rocksdb_delete_range_cf(db.Handle, wo.Handle, n["nodes"].Handle, zero8, (UIntPtr)8, ff8, (UIntPtr)8, out err);
            Assert.AreEqual(IntPtr.Zero, err);
            db.CompactRange((byte[]?)null, null, n["nodes"]);
            Step("nodes");

            // DeleteFilesInRange ending exactly at the largest key of the last-level file.
            for (int i = 0; i < 1000; i++) db.Put(Key("n", i), Value(i, 60), n["nodes"]);
            db.CompactRange((byte[]?)null, null, n["nodes"]);
            byte[] first = Key("n", 0), last = Key("n", 999);
            N.rocksdb_delete_file_in_range_cf(db.Handle, n["nodes"].Handle, first, (UIntPtr)first.Length, last, (UIntPtr)last.Length, out err);
            Assert.AreEqual(IntPtr.Zero, err);
            Step("nodes");

            // The to-index queue: entries of two indexes, one cleared and compacted with the non-exclusive options.
            for (int i = 0; i < 1500; i++)
            {
                db.Put(QueueKey(77, i), Value(i, 30), n["content"]);
                db.Put(QueueKey(78, i), Value(i, 30), n["content"]);
            }
            NativeFlushCf(db, n["content"]);
            for (int i = 0; i < 1500; i++) db.Remove(QueueKey(77, i), n["content"]);
            IntPtr co = N.rocksdb_compactoptions_create();
            N.rocksdb_compactoptions_set_exclusive_manual_compaction(co, 0);
            N.rocksdb_compactoptions_set_target_level(co, -1);
            N.rocksdb_compact_range_cf_opt(db.Handle, n["content"].Handle, co, queueFrom, (UIntPtr)queueFrom.Length, queueTo, (UIntPtr)queueTo.Length);
            N.rocksdb_compactoptions_destroy(co);
            Step("content");
        }

        using (ManagedEnv m = OpenManaged(Path.Combine(mDir.Path, "db"), Path.Combine(mDir.Path, "journal")))
        {
            void Step(string cf)
            {
                mShapes.Add(cf + ": " + Shape(p => m[cf].GetProperty(p)));
                mKeys.Add(ManagedKeys(m[cf]));
            }
            ColumnFamily tracker = m["tracker"], nodes = m["nodes"], content = m["content"];
            for (ulong part = 0; part < 3; part++)
            {
                for (ulong v = part * 1000 + 1; v <= part * 1000 + 1000; v++) tracker.Put(TrackerKey(v), Value((int)v, 40));
                tracker.Flush();
            }
            Step("tracker");
            for (ulong v = 1; v <= 1500; v++) tracker.Delete(TrackerKey(v));
            tracker.CompactRange(Version(0), Version(1500));
            Step("tracker");
            for (ulong v = 1501; v <= 2500; v++) tracker.Delete(TrackerKey(v));
            tracker.Flush();
            tracker.CompactRange(Version(0), Version(2500));
            Step("tracker");

            for (int i = 0; i < 2000; i++) nodes.Put(Key("n", i), Value(i, 60));
            nodes.CompactRange(null, null);
            for (int i = 2000; i < 2500; i++) nodes.Put(Key("n", i), Value(i, 60));
            nodes.Flush();
            for (int i = 2500; i < 2600; i++) nodes.Put(Key("n", i), Value(i, 60));
            Step("nodes");
            nodes.DeleteFilesInRange(zero8, ff8, includeEnd: true);
            Step("nodes");
            nodes.DeleteRange(zero8, ff8);
            nodes.CompactRange(null, null);
            Step("nodes");

            for (int i = 0; i < 1000; i++) nodes.Put(Key("n", i), Value(i, 60));
            nodes.CompactRange(null, null);
            nodes.DeleteFilesInRange(Key("n", 0), Key("n", 999), includeEnd: true);
            Step("nodes");

            for (int i = 0; i < 1500; i++)
            {
                content.Put(QueueKey(77, i), Value(i, 30));
                content.Put(QueueKey(78, i), Value(i, 30));
            }
            content.Flush();
            for (int i = 0; i < 1500; i++) content.Delete(QueueKey(77, i));
            content.CompactRange(new CompactRangeOptions { ExclusiveManualCompaction = false, TargetLevel = -1 }, queueFrom, queueTo);
            Step("content");
        }

        string report = string.Join("\n", nShapes.Zip(mShapes, static (a, b) => "native  " + a + "\nmanaged " + b));
        Console.WriteLine(report);
        CollectionAssert.AreEqual(nShapes, mShapes, report);
        Assert.AreEqual(nKeys.Count, mKeys.Count);
        for (int i = 0; i < nKeys.Count; i++) CollectionAssert.AreEqual(nKeys[i], mKeys[i], "step " + i + "\n" + report);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // BackupEngine
    // ---------------------------------------------------------------------------------------------------------------

    private readonly record struct BackupRow(uint Id, uint Files, ulong Size, long Timestamp);

    /// <summary>
    /// RocksDbBase.Backup: rocksdb_backup_engine_open(DB options, dir), create_new_backup_flush(flush = true),
    /// purge_old_backups(2), get_backup_info.
    /// </summary>
    private static List<BackupRow> NativeMosaikBackup(NativeEnv n, string dir)
    {
        IntPtr be = N.rocksdb_backup_engine_open(n.Options.Handle, dir);
        IntPtr info = IntPtr.Zero;
        try
        {
            N.rocksdb_backup_engine_create_new_backup_flush(be, n.Db.Handle, 1);
            N.rocksdb_backup_engine_purge_old_backups(be, 2);
            info = N.rocksdb_backup_engine_get_backup_info(be);
            var rows = new List<BackupRow>();
            int count = N.rocksdb_backup_engine_info_count(info);
            for (int i = 0; i < count; i++)
            {
                rows.Add(new BackupRow(N.rocksdb_backup_engine_info_backup_id(info, i), N.rocksdb_backup_engine_info_number_files(info, i),
                    N.rocksdb_backup_engine_info_size(info, i), N.rocksdb_backup_engine_info_timestamp(info, i)));
            }
            return rows;
        }
        finally
        {
            if (info != IntPtr.Zero) N.rocksdb_backup_engine_info_destroy(info);
            N.rocksdb_backup_engine_close(be);
        }
    }

    private static List<BackupRow> ManagedMosaikBackup(ManagedEnv m, string dir)
    {
        using BackupEngine be = BackupEngine.Open(new BackupEngineOptions(dir));
        be.CreateNewBackup(m.Db, flushBeforeBackup: true);
        be.PurgeOldBackups(2);
        var rows = new List<BackupRow>();
        foreach (BackupInfo i in be.GetBackupInfo()) rows.Add(new BackupRow(i.BackupId, i.NumberFiles, i.Size, i.Timestamp));
        return rows;
    }

    /// <summary>RocksDbBase.RestoreFrom: restore_db_from_latest_backup (index &lt; 0) or restore_db_from_backup, default restore options.</summary>
    private static void NativeMosaikRestore(string backupDir, string dbDir, string walDir, int backupIndex)
    {
        var o = new RocksDbSharp.DbOptions();
        IntPtr be = N.rocksdb_backup_engine_open(o.Handle, backupDir);
        IntPtr ro = N.rocksdb_restore_options_create();
        try
        {
            if (backupIndex < 0) N.rocksdb_backup_engine_restore_db_from_latest_backup(be, dbDir, walDir, ro);
            else N.rocksdb_backup_engine_restore_db_from_backup(be, dbDir, walDir, ro, (uint)backupIndex);
        }
        finally
        {
            N.rocksdb_backup_engine_close(be);
            N.rocksdb_restore_options_destroy(ro);
        }
    }

    private static void ManagedMosaikRestore(string backupDir, string dbDir, string walDir, int backupIndex)
    {
        using BackupEngineReadOnly be = BackupEngineReadOnly.Open(new BackupEngineOptions(backupDir));
        if (backupIndex < 0) be.RestoreDbFromLatestBackup(dbDir, walDir, new RestoreOptions());
        else be.RestoreDbFromBackup((uint)backupIndex, dbDir, walDir, new RestoreOptions());
    }

    /// <summary>
    /// The three backup steps (puts, deletes, overwrites in several families; data left in the memtables each time,
    /// so flush_before_backup matters), each followed by <paramref name="backup"/>, then a write no backup has.
    /// </summary>
    private static void BackupWorkload(Action<string, byte[], byte[]> put, Action<string, byte[]> del, Action backup)
    {
        for (int i = 0; i < 1000; i++) put("nodes", Key("n", i), Value(i, 60));
        backup();
        for (ulong v = 1; v <= 500; v++) put("tracker", TrackerKey(v), Value((int)v, 40));
        for (int i = 0; i < 300; i++) put("content", Key("c", i), Value(i, 300));
        backup();
        for (int i = 0; i < 500; i++) put("nodes", Key("n", i), Value(i + 7, 60));
        for (int i = 500; i < 600; i++) del("nodes", Key("n", i));
        for (ulong v = 1; v <= 100; v++) del("tracker", TrackerKey(v));
        backup();
        put("nodes", Key("n", 9999), Value(9999, 60));
    }

    /// <summary>The expected contents after backup <paramref name="step"/> (1-based) as hex key=value lines per family.</summary>
    private static Dictionary<string, List<string>> ExpectedAfter(int step)
    {
        var state = new Dictionary<string, SortedDictionary<string, string>>(StringComparer.Ordinal)
        {
            ["nodes"] = new(StringComparer.Ordinal), ["content"] = new(StringComparer.Ordinal), ["tracker"] = new(StringComparer.Ordinal),
        };
        int backups = 0;
        bool stop = false;
        BackupWorkload((cf, k, v) => { if (!stop) state[cf][Convert.ToHexString(k)] = Convert.ToHexString(v); },
            (cf, k) => { if (!stop) state[cf].Remove(Convert.ToHexString(k)); },
            () => { if (++backups == step) stop = true; });
        var result = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        foreach ((string cf, SortedDictionary<string, string> kv) in state)
        {
            var lines = new List<string>();
            foreach ((string k, string v) in kv) lines.Add(k + "=" + v);
            // The tracker family is ordered by the little-endian version, not bytewise.
            if (cf == "tracker") lines.Sort((a, b) => BinaryPrimitives.ReadUInt64LittleEndian(Convert.FromHexString(a[..16])).CompareTo(BinaryPrimitives.ReadUInt64LittleEndian(Convert.FromHexString(b[..16]))));
            result[cf] = lines;
        }
        return result;
    }

    /// <summary>A destination that already holds files: an info LOG, a stale table file, a stale WAL and a file RocksDB does not name.</summary>
    private static (string Db, string Wal) PopulatedDestination(string root)
    {
        string db = Path.Combine(root, "db"), wal = Path.Combine(root, "journal");
        Directory.CreateDirectory(db);
        Directory.CreateDirectory(wal);
        File.WriteAllText(Path.Combine(db, "LOG"), "old log");
        File.WriteAllText(Path.Combine(db, "000077.sst"), "stale table");
        File.WriteAllText(Path.Combine(db, "keep.txt"), "not a RocksDB file");
        File.WriteAllText(Path.Combine(wal, "000099.log"), "stale wal");
        return (db, wal);
    }

    /// <summary>The names in the destination directories, with the file numbers RocksDB chose replaced by kinds.</summary>
    private static string DestinationShape(string db, string wal)
    {
        static string Kind(string name)
        {
            if (name.StartsWith("MANIFEST-", StringComparison.Ordinal)) return "MANIFEST";
            if (name.StartsWith("OPTIONS-", StringComparison.Ordinal)) return "OPTIONS";
            string ext = Path.GetExtension(name);
            return ext is ".sst" or ".log" ? "*" + ext : name;
        }
        var dbNames = Directory.GetFiles(db).Select(p => Kind(Path.GetFileName(p))).Order(StringComparer.Ordinal);
        var walNames = Directory.GetFiles(wal).Select(p => Kind(Path.GetFileName(p))).Order(StringComparer.Ordinal);
        return "db: " + string.Join(",", dbNames.Distinct()) + " | wal: " + string.Join(",", walNames.Distinct())
            + " | stale sst kept: " + File.Exists(Path.Combine(db, "000077.sst")) + " | stale wal kept: " + File.Exists(Path.Combine(wal, "000099.log"));
    }

    private static void CheckRestoredWithManaged(string db, string wal, int step, string what)
    {
        (DbOptions o, List<ColumnFamilyDescriptor> cfs) = ManagedOptions(wal);
        o.CreateIfMissing = false;
        using RocksDatabase restored = RocksDatabase.Open(db, o, cfs);
        Dictionary<string, List<string>> expected = ExpectedAfter(step);
        foreach (string cf in s_families) CollectionAssert.AreEqual(expected[cf], ManagedKeys(restored.GetColumnFamily(cf)), what + " " + cf);
    }

    private static void CheckRestoredWithNative(string db, string wal, int step, string what)
    {
        using NativeEnv n = OpenNative(db, wal);
        Dictionary<string, List<string>> expected = ExpectedAfter(step);
        foreach (string cf in s_families) CollectionAssert.AreEqual(expected[cf], NativeKeys(n.Db, n[cf]), what + " " + cf);
    }

    /// <summary>
    /// RocksDbBase.Backup / RestoreFrom on both engines: three flushed backups of a DB with a separate WAL dir, purged to
    /// the newest two after each; the info rows (ids, file counts, sizes, timestamps) agree; each engine restores the
    /// other's latest backup and an older one by id into destinations that already hold files, removes the same files
    /// (all of them), puts the WALs into the WAL dir, and the restored DB has exactly the
    /// contents at backup time (not the write made after the last backup) when opened by either engine.
    /// </summary>
    [TestMethod]
    public void MosaikBackupAndRestoreMatchNative()
    {
        using var root = new TempDir("mosaik-backup");
        string nBackups = Path.Combine(root.Path, "backups-n"), mBackups = Path.Combine(root.Path, "backups-m");
        var nRows = new List<List<BackupRow>>();
        var mRows = new List<List<BackupRow>>();
        long before = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        Directory.CreateDirectory(Path.Combine(root.Path, "src-n"));
        Directory.CreateDirectory(Path.Combine(root.Path, "src-m"));

        using (NativeEnv n = OpenNative(Path.Combine(root.Path, "src-n", "db"), Path.Combine(root.Path, "src-n", "journal")))
        {
            BackupWorkload((cf, k, v) => n.Db.Put(k, v, n[cf]), (cf, k) => n.Db.Remove(k, n[cf]), () => nRows.Add(NativeMosaikBackup(n, nBackups)));
        }
        using (ManagedEnv m = OpenManaged(Path.Combine(root.Path, "src-m", "db"), Path.Combine(root.Path, "src-m", "journal")))
        {
            BackupWorkload((cf, k, v) => m[cf].Put(k, v), (cf, k) => m[cf].Delete(k), () => mRows.Add(ManagedMosaikBackup(m, mBackups)));
        }
        long after = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

        string rows = "native: " + string.Join(" / ", nRows.Select(r => string.Join(",", r))) + "\nmanaged: " + string.Join(" / ", mRows.Select(r => string.Join(",", r)));
        rows += "\nnative meta 3:\n" + File.ReadAllText(Path.Combine(nBackups, "meta", "3")) + "\nmanaged meta 3:\n" + File.ReadAllText(Path.Combine(mBackups, "meta", "3"));
        Console.WriteLine(rows);
        Assert.AreEqual(3, nRows.Count);
        Assert.AreEqual(3, mRows.Count);
        for (int b = 0; b < 3; b++)
        {
            CollectionAssert.AreEqual(nRows[b].Select(r => r.Id).ToList(), mRows[b].Select(r => r.Id).ToList(), rows);
            for (int i = 0; i < nRows[b].Count; i++)
            {
                BackupRow nr = nRows[b][i], mr = mRows[b][i];
                Assert.AreEqual(nr.Files, mr.Files, "number_files of backup " + nr.Id + "\n" + rows);
                Assert.IsTrue(Math.Abs((double)nr.Size - mr.Size) <= nr.Size * 0.2, "size of backup " + nr.Id + "\n" + rows);
                Assert.IsTrue(nr.Timestamp >= before && nr.Timestamp <= after && mr.Timestamp >= before && mr.Timestamp <= after, rows);
            }
        }
        CollectionAssert.AreEqual(new uint[] { 2, 3 }, mRows[2].Select(r => r.Id).ToArray(), "purge keeps the newest two");

        // Each engine restores the other's latest backup, and its own (control), into populated destinations.
        var shapes = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach ((string name, Action<string, string, string, int> restore, string from) in new (string, Action<string, string, string, int>, string)[]
                 {
                     ("native<-native", NativeMosaikRestore, nBackups), ("native<-managed", NativeMosaikRestore, mBackups),
                     ("managed<-managed", ManagedMosaikRestore, mBackups), ("managed<-native", ManagedMosaikRestore, nBackups),
                 })
        {
            (string db, string wal) = PopulatedDestination(Path.Combine(root.Path, name.Replace('<', '_')));
            restore(from, db, wal, -1);
            shapes[name] = DestinationShape(db, wal);
            CheckRestoredWithManaged(db, wal, 3, name);
            CheckRestoredWithNative(db, wal, 3, name);
        }
        string shapeReport = string.Join("\n", shapes.Select(kv => kv.Key + ": " + kv.Value));
        Console.WriteLine(shapeReport);
        Assert.AreEqual(shapes["native<-native"], shapes["managed<-managed"], shapeReport);
        Assert.AreEqual(shapes["native<-managed"], shapes["managed<-native"], shapeReport);
        Assert.AreEqual(shapes["native<-native"], shapes["managed<-native"], shapeReport);
        // DeleteChildren removes every child of the destination, also files RocksDB does not name.
        Assert.IsFalse(shapes["managed<-native"].Contains("keep.txt", StringComparison.Ordinal), shapeReport);

        // An older backup by id (restore_db_from_backup), across engines both ways.
        (string db2, string wal2) = PopulatedDestination(Path.Combine(root.Path, "by-id-m"));
        ManagedMosaikRestore(nBackups, db2, wal2, 2);
        CheckRestoredWithNative(db2, wal2, 2, "managed restore of native backup 2");
        (string db3, string wal3) = PopulatedDestination(Path.Combine(root.Path, "by-id-n"));
        NativeMosaikRestore(mBackups, db3, wal3, 2);
        CheckRestoredWithManaged(db3, wal3, 2, "native restore of managed backup 2");

        // A purged backup: both engines refuse it.
        (string db4, string wal4) = PopulatedDestination(Path.Combine(root.Path, "purged"));
        var ex = Assert.ThrowsExactly<RocksDbException>(() => ManagedMosaikRestore(nBackups, db4, wal4, 1));
        Assert.AreEqual(StatusCode.NotFound, ex.Code);
        Assert.ThrowsExactly<RocksDbSharp.RocksDbException>(() => NativeMosaikRestore(mBackups, db4, wal4, 1));
    }
}
