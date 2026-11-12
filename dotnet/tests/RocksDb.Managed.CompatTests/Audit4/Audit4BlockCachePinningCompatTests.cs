using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using RocksDb.Managed.Cache;
using RocksDb.Managed.Monitoring;
using RocksDb.Managed.Table.Filters;

namespace RocksDb.Managed.CompatTests.Audit4;

/// <summary>
/// Audit #4: pinning of the index and filter blocks in the block cache and the BLOCK_CACHE_*_ADD_REDUNDANT tickers,
/// compared with native librocksdb. The same deterministic workload (flushes, point lookups, MultiGet, scans, a full
/// compaction) runs on both engines for several combinations of cache_index_and_filter_blocks,
/// pin_l0_filter_and_index_blocks_in_cache, pin_top_level_index_and_filter and metadata_cache_options, with an
/// unpartitioned and with a partitioned index / filter, and the BLOCK_CACHE_{INDEX,FILTER}_{HIT,MISS,ADD} and
/// *_ADD_REDUNDANT tickers are compared after every step.
/// </summary>
[TestClass]
public sealed class Audit4BlockCachePinningCompatTests
{
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    private const int Keys = 3000;

    private static readonly string[] s_compared =
    [
        "rocksdb.block.cache.index.miss", "rocksdb.block.cache.index.hit", "rocksdb.block.cache.index.add",
        "rocksdb.block.cache.filter.miss", "rocksdb.block.cache.filter.hit", "rocksdb.block.cache.filter.add",
        "rocksdb.block.cache.add.redundant", "rocksdb.block.cache.index.add.redundant", "rocksdb.block.cache.filter.add.redundant",
        "rocksdb.block.cache.data.add.redundant", "rocksdb.block.cache.compression.dict.add.redundant",
    ];

    private static byte[] K(int i) => Encoding.ASCII.GetBytes("key" + i.ToString("D6", CultureInfo.InvariantCulture));

    private static byte[] V(int i, int gen) => Encoding.ASCII.GetBytes(
        "{\"id\":" + i + ",\"gen\":" + gen + ",\"name\":\"customer-" + (i % 97) + "\",\"country\":\""
        + (i % 3 == 0 ? "Portugal" : i % 3 == 1 ? "Germany" : "Brazil") + "\"}");

    private static Dictionary<string, ulong> Tickers(string statistics)
    {
        var d = new Dictionary<string, ulong>(StringComparer.Ordinal);
        foreach (Match m in Regex.Matches(statistics, "^(\\S+) COUNT : (\\d+)$", RegexOptions.Multiline))
            d[m.Groups[1].Value] = ulong.Parse(m.Groups[2].Value, CultureInfo.InvariantCulture);
        return d;
    }

    /// <summary>The block-based table options under test.</summary>
    private sealed record Mode
    {
        /// <summary>kTwoLevelIndexSearch with partition_filters, else kBinarySearch with a full filter.</summary>
        public bool Partitioned { get; init; }
        public bool CacheMeta { get; init; }
        public bool PinL0 { get; init; }
        public bool PinTopLevel { get; init; } = true;
        public PinningTier TopLevel { get; init; } = PinningTier.Fallback;
        public PinningTier Partition { get; init; } = PinningTier.Fallback;
        public PinningTier Unpartitioned { get; init; } = PinningTier.Fallback;
        public bool HyperClock { get; init; }

        public override string ToString() =>
            (Partitioned ? "partitioned" : "unpartitioned") + " cache_index_and_filter_blocks=" + CacheMeta + " pin_l0=" + PinL0 + " pin_top_level=" + PinTopLevel
            + " top_level_index_pinning=" + TopLevel + " partition_pinning=" + Partition + " unpartitioned_pinning=" + Unpartitioned
            + (HyperClock ? " HyperClockCache" : " LRUCache");
    }

    private static readonly List<byte[]> s_multiGetKeys = MakeMultiGetKeys();

    private static List<byte[]> MakeMultiGetKeys()
    {
        var keys = new List<byte[]>();
        for (int i = 0; i < Keys; i += 211) keys.Add(K(i));
        return keys;
    }

    // Each step records the tickers after it, so that a difference is attributed to a step.
    private static List<(string Step, Dictionary<string, ulong> Tickers)> NativeWorkload(string path, Mode mode)
    {
        RocksDbSharp.Native n = RocksDbSharp.Native.Instance;
        var bbto = new RocksDbSharp.BlockBasedTableOptions().SetBlockSize(512UL).SetFilterPolicy(RocksDbSharp.BloomFilterPolicy.Create(10, false));
        object cache;
        if (mode.HyperClock)
        {
            // The wrapper has no HyperClockCache factory: the C API handle is set directly (and kept for the process).
            IntPtr handle = n.rocksdb_cache_create_hyper_clock((UIntPtr)(64UL << 20), (UIntPtr)512);
            n.rocksdb_block_based_options_set_block_cache(bbto.Handle, handle);
            cache = handle;
        }
        else
        {
            RocksDbSharp.Cache lru = RocksDbSharp.Cache.CreateLru(64UL << 20);
            bbto.SetBlockCache(lru);
            cache = lru;
        }
        if (mode.Partitioned)
        {
            n.rocksdb_block_based_options_set_index_type(bbto.Handle, RocksDbSharp.BlockBasedTableIndexType.TwoLevelIndex);
            n.rocksdb_block_based_options_set_partition_filters(bbto.Handle, RocksDbSharp.Native.MarshalBool(true));
            n.rocksdb_block_based_options_set_metadata_block_size(bbto.Handle, 512);
        }
        n.rocksdb_block_based_options_set_cache_index_and_filter_blocks(bbto.Handle, RocksDbSharp.Native.MarshalBool(mode.CacheMeta));
        n.rocksdb_block_based_options_set_pin_l0_filter_and_index_blocks_in_cache(bbto.Handle, RocksDbSharp.Native.MarshalBool(mode.PinL0));
        n.rocksdb_block_based_options_set_pin_top_level_index_and_filter(bbto.Handle, RocksDbSharp.Native.MarshalBool(mode.PinTopLevel));
        n.rocksdb_block_based_options_set_top_level_index_pinning_tier(bbto.Handle, (int)mode.TopLevel);
        n.rocksdb_block_based_options_set_partition_pinning_tier(bbto.Handle, (int)mode.Partition);
        n.rocksdb_block_based_options_set_unpartitioned_pinning_tier(bbto.Handle, (int)mode.Unpartitioned);
        var no = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).EnableStatistics();
        no.SetCompression(RocksDbSharp.Compression.No);
        no.SetDisableAutoCompactions(1);
        no.SetBlockBasedTableFactory(bbto);
        s_keepAlive.Add(no);
        s_keepAlive.Add(bbto);
        s_keepAlive.Add(cache);
        var flush = new RocksDbSharp.FlushOptions().SetWaitForFlush(true);
        var steps = new List<(string, Dictionary<string, ulong>)>();
        void Step(string name) => steps.Add((name, Tickers(no.GetStatisticsString())));
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(no, path))
        {
            for (int i = 0; i < Keys; i++) db.Put(K(i), V(i, 0));
            db.Flush(flush);
            Step("flush 1");
            for (int i = 0; i < 20; i++) db.Get(K(i * 149));
            Step("gets on L0");
            for (int i = 0; i < 20; i++) db.Get(K(Keys + i));
            Step("absent gets on L0");
            db.MultiGet(s_multiGetKeys.ToArray());
            Step("multiget on L0");
            using (RocksDbSharp.Iterator it = db.NewIterator())
            {
                it.SeekToFirst();
                for (int i = 0; i < 700 && it.Valid(); i++) it.Next();
                it.Seek(K(2000));
                for (int i = 0; i < 30 && it.Valid(); i++) it.Next();
            }
            Step("scan on L0");
            for (int i = 0; i < Keys / 2; i++) db.Put(K(i * 2), V(i * 2, 1));
            db.Flush(flush);
            Step("flush 2");
            for (int i = 0; i < 20; i++) db.Get(K(i * 131));
            Step("gets on two L0 files");
            db.CompactRange(Array.Empty<byte>(), null);
            Step("compaction");
            for (int i = 0; i < 20; i++) db.Get(K(i * 113));
            Step("gets after compaction");
            db.MultiGet(s_multiGetKeys.ToArray());
            Step("multiget after compaction");
            using (RocksDbSharp.Iterator it = db.NewIterator())
            {
                int count = 0;
                for (it.SeekToFirst(); it.Valid(); it.Next()) count++;
                Assert.AreEqual(Keys, count);
            }
            Step("scan after compaction");
        }
        // The block cache outlives the DB, so the reopened DB finds what the first one left in it.
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(no, path))
        {
            Step("reopen");
            for (int i = 0; i < 20; i++) db.Get(K(i * 101));
            Step("gets after reopen");
            for (int i = 0; i < Keys / 3; i++) db.Put(K(i * 3), V(i * 3, 2));
            db.Flush(flush);
            Step("flush 3");
        }
        // Level-0 files prefetch at open (prefetch_all = prefetch_index_and_filter_in_cache || level == 0).
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(no, path))
        {
            Step("reopen with a level-0 file");
            for (int i = 0; i < 20; i++) db.Get(K(i * 99));
            Step("gets after reopen with a level-0 file");
        }
        return steps;
    }

    private static List<(string Step, Dictionary<string, ulong> Tickers)> ManagedWorkload(string path, Mode mode)
    {
        var stats = new Statistics();
        var cf = new ColumnFamilyOptions { Compression = RocksDb.Managed.Compression.CompressionType.None, DisableAutoCompactions = true };
        BlockBasedTableOptions t = cf.TableOptions;
        t.BlockSize = 512;
        t.BlockCache = mode.HyperClock
            ? BlockCache.CreateHyperClock(new HyperClockCacheOptions { Capacity = 64L << 20, EstimatedEntryCharge = 512 })
            : BlockCache.CreateLru(64L << 20);
        t.FilterPolicy = new BloomFilterPolicy(10);
        if (mode.Partitioned)
        {
            t.IndexType = IndexType.TwoLevelIndexSearch;
            t.PartitionFilters = true;
            t.MetadataBlockSize = 512;
        }
        t.CacheIndexAndFilterBlocks = mode.CacheMeta;
        t.PinL0FilterAndIndexBlocksInCache = mode.PinL0;
        t.PinTopLevelIndexAndFilter = mode.PinTopLevel;
        t.MetadataCacheOptions.TopLevelIndexPinning = mode.TopLevel;
        t.MetadataCacheOptions.PartitionPinning = mode.Partition;
        t.MetadataCacheOptions.UnpartitionedPinning = mode.Unpartitioned;
        var steps = new List<(string, Dictionary<string, ulong>)>();
        void Step(string name) => steps.Add((name, Tickers(stats.ToString())));
        var dbOptions = new DbOptions { CreateIfMissing = true, Statistics = stats };
        using (RocksDatabase db = RocksDatabase.Open(path, dbOptions, cf))
        {
            for (int i = 0; i < Keys; i++) db.Put(K(i), V(i, 0));
            db.Flush();
            Step("flush 1");
            for (int i = 0; i < 20; i++) db.Get(K(i * 149));
            Step("gets on L0");
            for (int i = 0; i < 20; i++) db.Get(K(Keys + i));
            Step("absent gets on L0");
            db.MultiGet(s_multiGetKeys);
            Step("multiget on L0");
            using (Iterator it = db.NewIterator())
            {
                it.SeekToFirst();
                for (int i = 0; i < 700 && it.Valid; i++) it.Next();
                it.Seek(K(2000));
                for (int i = 0; i < 30 && it.Valid; i++) it.Next();
            }
            Step("scan on L0");
            for (int i = 0; i < Keys / 2; i++) db.Put(K(i * 2), V(i * 2, 1));
            db.Flush();
            Step("flush 2");
            for (int i = 0; i < 20; i++) db.Get(K(i * 131));
            Step("gets on two L0 files");
            db.CompactRange(null, null);
            Step("compaction");
            for (int i = 0; i < 20; i++) db.Get(K(i * 113));
            Step("gets after compaction");
            db.MultiGet(s_multiGetKeys);
            Step("multiget after compaction");
            using (Iterator it = db.NewIterator())
            {
                int count = 0;
                for (it.SeekToFirst(); it.Valid; it.Next()) count++;
                Assert.AreEqual(Keys, count);
            }
            Step("scan after compaction");
        }
        using (RocksDatabase db = RocksDatabase.Open(path, dbOptions, cf))
        {
            Step("reopen");
            for (int i = 0; i < 20; i++) db.Get(K(i * 101));
            Step("gets after reopen");
            for (int i = 0; i < Keys / 3; i++) db.Put(K(i * 3), V(i * 3, 2));
            db.Flush();
            Step("flush 3");
        }
        // Level-0 files prefetch at open (prefetch_all = prefetch_index_and_filter_in_cache || level == 0).
        using (RocksDatabase db = RocksDatabase.Open(path, dbOptions, cf))
        {
            Step("reopen with a level-0 file");
            for (int i = 0; i < 20; i++) db.Get(K(i * 99));
            Step("gets after reopen with a level-0 file");
        }
        return steps;
    }

    private static void Compare(Mode mode)
    {
        string root = Path.Combine(Path.GetTempPath(), "audit4-pinning-compat-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        try
        {
            List<(string Step, Dictionary<string, ulong> Tickers)> native = NativeWorkload(Path.Combine(root, "native"), mode);
            List<(string Step, Dictionary<string, ulong> Tickers)> managed = ManagedWorkload(Path.Combine(root, "managed"), mode);
            Assert.AreEqual(native.Count, managed.Count);
            var differences = new StringBuilder();
            for (int i = 0; i < native.Count; i++)
            {
                foreach (string ticker in s_compared)
                {
                    if (native[i].Tickers[ticker] != managed[i].Tickers[ticker])
                        differences.Append("\n  after ").Append(native[i].Step).Append(": ").Append(ticker).Append(" native=")
                            .Append(native[i].Tickers[ticker]).Append(" managed=").Append(managed[i].Tickers[ticker]);
                }
            }
            Assert.AreEqual("", differences.ToString(), mode.ToString());
            // The workload exercises what the mode is about.
            Dictionary<string, ulong> last = native[^1].Tickers;
            if (mode.CacheMeta || mode.Partitioned)
            {
                Assert.IsTrue(last["rocksdb.block.cache.index.add"] > 0, "index blocks went through the block cache: " + mode);
                Assert.IsTrue(last["rocksdb.block.cache.filter.add"] > 0, "filter blocks went through the block cache: " + mode);
            }
            else
            {
                Assert.AreEqual(0UL, last["rocksdb.block.cache.index.add"] + last["rocksdb.block.cache.filter.add"], mode.ToString());
            }
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, true);
        }
    }

    /// <summary>
    /// BinarySearchIndexReader::Create / FullFilterBlockReader::Create with use_cache and no pin: GetOrReadIndexBlock /
    /// GetOrReadFilterBlock look the blocks up on every use.
    /// </summary>
    [TestMethod]
    public void UnpinnedIndexAndFilterTickersMatchNative() => Compare(new Mode { CacheMeta = true });

    /// <summary>The same with a HyperClockCache.</summary>
    [TestMethod]
    public void UnpinnedIndexAndFilterTickersMatchNativeWithHyperClockCache() => Compare(new Mode { CacheMeta = true, HyperClock = true });

    /// <summary>
    /// pin_unpartitioned in BlockBasedTable::PrefetchIndexAndFilterBlocks with pin_l0_filter_and_index_blocks_in_cache:
    /// flushed L0 files pin their index and filter, the compaction output on a lower level does not.
    /// </summary>
    [TestMethod]
    public void L0PinnedIndexAndFilterTickersMatchNative() => Compare(new Mode { CacheMeta = true, PinL0 = true });

    /// <summary>metadata_cache_options.unpartitioned_pinning = kAll: every table pins its index and filter.</summary>
    [TestMethod]
    public void AlwaysPinnedIndexAndFilterTickersMatchNative() => Compare(new Mode { CacheMeta = true, Unpartitioned = PinningTier.All });

    /// <summary>metadata_cache_options.unpartitioned_pinning = kNone overrides pin_l0_filter_and_index_blocks_in_cache.</summary>
    [TestMethod]
    public void NeverPinnedIndexAndFilterTickersMatchNative() => Compare(new Mode { CacheMeta = true, PinL0 = true, Unpartitioned = PinningTier.None });

    /// <summary>use_cache = false: the table reader owns the index and the filter, no tickers.</summary>
    [TestMethod]
    public void OwnedIndexAndFilterRecordNothingLikeNative() => Compare(new Mode());

    /// <summary>
    /// PartitionIndexReader / PartitionedFilterBlockReader::CacheDependencies with the defaults: the top levels are
    /// owned by the reader, the partitions are loaded into the block cache at open and looked up per read.
    /// </summary>
    [TestMethod]
    public void PartitionTickersMatchNative() => Compare(new Mode { Partitioned = true });

    /// <summary>cache_index_and_filter_blocks with the default pin_top_level_index_and_filter: the top levels are pinned cache entries.</summary>
    [TestMethod]
    public void PartitionTickersMatchNativeWithCachedTopLevels() => Compare(new Mode { Partitioned = true, CacheMeta = true });

    /// <summary>top_level_index_pinning = kNone: the top levels are looked up on every use, next to the partitions.</summary>
    [TestMethod]
    public void PartitionTickersMatchNativeWithUnpinnedTopLevels() =>
        Compare(new Mode { Partitioned = true, CacheMeta = true, TopLevel = PinningTier.None });

    /// <summary>pin_top_level_index_and_filter = false is the kFallback of top_level_index_pinning.</summary>
    [TestMethod]
    public void PartitionTickersMatchNativeWithoutPinTopLevel() => Compare(new Mode { Partitioned = true, CacheMeta = true, PinTopLevel = false });

    /// <summary>partition_pinning = kAll: every table pins its partitions (partition_map_ / filter_map_), with and without cache_index_and_filter_blocks.</summary>
    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void PinnedPartitionTickersMatchNative(bool cacheMeta) => Compare(new Mode { Partitioned = true, CacheMeta = cacheMeta, Partition = PinningTier.All });

    /// <summary>pin_l0_filter_and_index_blocks_in_cache is the kFallback of partition_pinning: flushed L0 files pin their partitions.</summary>
    [TestMethod]
    public void L0PinnedPartitionTickersMatchNative() => Compare(new Mode { Partitioned = true, CacheMeta = true, PinL0 = true });
}
