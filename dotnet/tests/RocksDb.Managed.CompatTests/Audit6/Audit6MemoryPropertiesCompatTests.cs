using System.Globalization;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Table.Filters;

namespace RocksDb.Managed.CompatTests.Audit6;

/// <summary>
/// Audit #6: the memory numbers a host reads to size its caches (the memtable sizes, the table reader estimate, the block
/// cache usage), side by side with native RocksDB on the same workload: three column families sharing one LRU cache and
/// a write buffer manager, two-level indexes and partitioned filters cached in the block cache, three level-0 files per
/// family, reads through the cache, then writes left in the memtables. The default column family is left empty and uses
/// the default block cache.
/// </summary>
[TestClass]
public sealed class Audit6MemoryPropertiesCompatTests
{
    private static readonly RocksDbSharp.Native N = RocksDbSharp.Native.Instance;
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();
    private const long CacheCapacity = 64L << 20;
    private const long WriteBufferLimit = 64L << 20;
    private static readonly string[] s_families = ["nodes", "content", "edges"];

    private const string CurSizeActive = "rocksdb.cur-size-active-mem-table";
    private const string CurSizeAll = "rocksdb.cur-size-all-mem-tables";
    private const string SizeAll = "rocksdb.size-all-mem-tables";
    private const string TableReadersMem = "rocksdb.estimate-table-readers-mem";
    private const string CacheUsage = "rocksdb.block-cache-usage";
    private const string CacheCapacityProperty = "rocksdb.block-cache-capacity";

    private static readonly string[] s_properties =
    [
        CurSizeActive, CurSizeAll, SizeAll, TableReadersMem, CacheUsage, "rocksdb.block-cache-pinned-usage", CacheCapacityProperty,
        "rocksdb.live-sst-files-size", "rocksdb.num-files-at-level0", "rocksdb.estimate-num-keys",
    ];

    public TestContext TestContext { get; set; } = null!;

    private static RocksDbSharp.BlockBasedTableOptions NativeTableOptions(RocksDbSharp.Cache cache, bool bloom)
    {
        var t = new RocksDbSharp.BlockBasedTableOptions().SetBlockCache(cache);
        N.rocksdb_block_based_options_set_cache_index_and_filter_blocks(t.Handle, 1);
        N.rocksdb_block_based_options_set_cache_index_and_filter_blocks_with_high_priority(t.Handle, 1);
        N.rocksdb_block_based_options_set_pin_l0_filter_and_index_blocks_in_cache(t.Handle, 1);
        N.rocksdb_block_based_options_set_index_type(t.Handle, RocksDbSharp.BlockBasedTableIndexType.TwoLevelIndex);
        N.rocksdb_block_based_options_set_partition_filters(t.Handle, 1);
        N.rocksdb_block_based_options_set_pin_top_level_index_and_filter(t.Handle, 1);
        if (bloom) t.SetFilterPolicy(RocksDbSharp.BloomFilterPolicy.Create(10, false));
        s_keepAlive.Add(t);
        return t;
    }

    private static BlockBasedTableOptions ManagedTableOptions(Cache.BlockCache cache, bool bloom) => new()
    {
        BlockCache = cache,
        CacheIndexAndFilterBlocks = true,
        CacheIndexAndFilterBlocksWithHighPriority = true,
        PinL0FilterAndIndexBlocksInCache = true,
        IndexType = IndexType.TwoLevelIndexSearch,
        PartitionFilters = true,
        PinTopLevelIndexAndFilter = true,
        FilterPolicy = bloom ? new BloomFilterPolicy(10) : null,
    };

    private static byte[] Key(string prefix, int i) => Encoding.ASCII.GetBytes(prefix + i.ToString("D8", CultureInfo.InvariantCulture));

    private static byte[] Value(int i, int size)
    {
        var v = new byte[size];
        for (int j = 0; j < v.Length; j++) v[j] = (byte)('a' + (i * 7 + j) % 13);
        return v;
    }

    private static long Parse(string? s) => long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out long v) ? v : -1;

    private static void Workload(Action<string, byte[], byte[]> put, Action<string> flush, Action<string, byte[]> get)
    {
        for (int round = 0; round < 3; round++)
        {
            for (int i = round * 2000; i < (round + 1) * 2000; i++)
            {
                put("nodes", Key("n", i), Value(i, 60));
                put("edges", Key("e", i), Value(i, 30));
                if (i % 2 == 0) put("content", Key("c", i), Value(i, 400));
            }
            foreach (string f in s_families) flush(f);
        }
        for (int i = 0; i < 6000; i += 2)
        {
            get("nodes", Key("n", i));
            get("content", Key("c", i));
            get("edges", Key("e", i));
        }
        for (int i = 6000; i < 7000; i++) put("nodes", Key("n", i), Value(i, 60));
    }

    /// <summary>The properties of every family ("family|property") and of the DB ("db|property"), on native RocksDB.</summary>
    private static Dictionary<string, long> RunNative(string path)
    {
        var values = new Dictionary<string, long>();
        RocksDbSharp.Cache cache = RocksDbSharp.Cache.CreateLru((ulong)CacheCapacity);
        s_keepAlive.Add(cache);
        IntPtr wbm = N.rocksdb_write_buffer_manager_create((UIntPtr)(ulong)WriteBufferLimit, 0);
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetCreateMissingColumnFamilies(true);
        o.EnableStatistics();
        N.rocksdb_options_set_write_buffer_manager(o.Handle, wbm);
        s_keepAlive.Add(o);
        var fam = new RocksDbSharp.ColumnFamilies();
        foreach (string f in s_families)
            fam.Add(f, new RocksDbSharp.ColumnFamilyOptions().SetBlockBasedTableFactory(NativeTableOptions(cache, bloom: f != "nodes")));
        s_keepAlive.Add(fam);
        using (var db = RocksDbSharp.RocksDb.Open(o, path, fam))
        {
            var cf = s_families.ToDictionary(f => f, f => db.GetColumnFamily(f));
            Workload((f, k, v) => db.Put(k, v, cf[f]), f =>
            {
                IntPtr fo = N.rocksdb_flushoptions_create();
                N.rocksdb_flushoptions_set_wait(fo, 1);
                N.rocksdb_flush_cf(db.Handle, fo, cf[f].Handle);
                N.rocksdb_flushoptions_destroy(fo);
            }, (f, k) => db.Get(k, cf[f]));
            foreach (string f in s_families)
            {
                foreach (string p in s_properties) values[f + "|" + p] = Parse(db.GetProperty(p, cf[f]));
            }
            foreach (string p in s_properties) values["db|" + p] = Parse(db.GetProperty(p));
        }
        N.rocksdb_write_buffer_manager_destroy(wbm);
        return values;
    }

    private static Dictionary<string, long> RunManaged(string path)
    {
        var values = new Dictionary<string, long>();
        Cache.BlockCache cache = Cache.BlockCache.CreateLru(CacheCapacity);
        var wbm = new WriteBufferManager(WriteBufferLimit, null, allowStall: false);
        var o = new DbOptions { CreateIfMissing = true, CreateMissingColumnFamilies = true, Statistics = new Statistics(), WriteBufferManager = wbm };
        var fam = new List<ColumnFamilyDescriptor>();
        foreach (string f in s_families) fam.Add(new(f, new ColumnFamilyOptions { TableOptions = ManagedTableOptions(cache, bloom: f != "nodes") }));
        using var db = RocksDatabase.Open(path, o, fam);
        var cf = s_families.ToDictionary(f => f, f => db.GetColumnFamily(f));
        Workload((f, k, v) => cf[f].Put(k, v), f => cf[f].Flush(), (f, k) => cf[f].Get(k));
        foreach (string f in s_families)
        {
            foreach (string p in s_properties) values[f + "|" + p] = Parse(cf[f].GetProperty(p));
        }
        foreach (string p in s_properties) values["db|" + p] = Parse(db.GetProperty(p));
        return values;
    }

    /// <summary>
    /// The active memtable is reported by its cached arena allocation (ApproximateMemoryUsageFast, as
    /// InternalStats::HandleCurSizeActiveMemTable): the 2 KiB inline block when empty, the arena blocks once written.
    /// The table reader estimate counts Rep, the reader objects and the table properties also when the index and filter
    /// live in the block cache. An empty default block cache (AutoHyperClockTable) charges one page of slots.
    /// </summary>
    [TestMethod]
    public void MemoryPropertiesMatchNative()
    {
        using var nativeDir = new TempDir("a6mem-n");
        using var managedDir = new TempDir("a6mem-m");
        Dictionary<string, long> native = RunNative(nativeDir.Path);
        Dictionary<string, long> managed = RunManaged(managedDir.Path);

        var report = new StringBuilder();
        foreach (string k in native.Keys)
        {
            report.Append(k.PadRight(48)).Append(" native=").Append(native[k].ToString(CultureInfo.InvariantCulture).PadLeft(10))
                .Append(" managed=").Append(managed[k].ToString(CultureInfo.InvariantCulture).PadLeft(10)).Append('\n');
        }
        TestContext.WriteLine(report.ToString());
        string r = report.ToString();

        foreach (string scope in (string[])["nodes", "content", "edges", "db"])
        {
            // The memtables: the same arena blocks on both engines. Native counts each arena block by its
            // malloc_usable_size (Arena::AllocateNewBlock), which glibc rounds up by a few bytes depending on the heap's
            // state (8 for the 1 MiB block here); the port counts the requested size.
            foreach (string p in (string[])[CurSizeActive, CurSizeAll, SizeAll])
            {
                long n = native[scope + "|" + p], m = managed[scope + "|" + p];
                Assert.IsTrue(m <= n && n - m <= 64, scope + " " + p + "\n" + r);
            }
        }
        Assert.AreEqual(2048, managed["content|" + CurSizeActive], "an empty memtable is its inline arena block\n" + r);

        foreach (string f in s_families)
        {
            // Both engines run on the same host, so the table properties (db id, host name, compression options) have the
            // same sizes. The port charges sizeof(BlockBasedTable::Rep) of the C++ reference (944 bytes, 952 usable);
            // the 11.12 package's Rep is 920 bytes (928 usable): 32 bytes per file.
            long n = native[f + "|" + TableReadersMem], m = managed[f + "|" + TableReadersMem];
            long files = managed[f + "|rocksdb.num-files-at-level0"];
            Assert.IsTrue(n > 0 && Math.Abs(m - n) <= 64 * files, f + " " + TableReadersMem + "\n" + r);
        }
        Assert.IsTrue(managed["content|" + TableReadersMem] > managed["nodes|" + TableReadersMem], "the partitioned filter reader is counted\n" + r);

        // The default column family's default cache, empty: HyperClockCacheOptions(32 MiB) with the automatic table.
        Assert.AreEqual(native["db|" + CacheCapacityProperty], managed["db|" + CacheCapacityProperty], r);
        Assert.AreEqual(native["db|" + CacheUsage], managed["db|" + CacheUsage], "empty default block cache\n" + r);
    }
}
