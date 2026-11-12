using System.Buffers;
using System.Globalization;
using System.Text;
using RocksDb.Managed.Cache;
using RocksDb.Managed.Db;
using RocksDb.Managed.Monitoring;
using RocksDb.Managed.Table;
using RocksDb.Managed.Table.Filters;
using RocksDb.Managed.Tests.Table;
using CompressionType = RocksDb.Managed.Compression.CompressionType;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4: the BLOCK_CACHE_*_ADD_REDUNDANT tickers (BlockBasedTable::UpdateCacheInsertionMetrics with Cache::Insert
/// returning Status::OkOverwritten) and the pinning of the index and filter blocks in the block cache
/// (BlockBasedTable::PrefetchIndexAndFilterBlocks, the index / filter readers' Create, GetOrReadIndexBlock /
/// GetOrReadFilterBlock and CacheDependencies). Each test names the C++ function it pins.
/// </summary>
[TestClass]
public sealed class Audit4BlockCachePinningTests
{
    private const int Count = 2000;

    private static readonly FilterPolicy s_policy = new BloomFilterPolicy(10);

    private static byte[] UserKey(int i) => Encoding.ASCII.GetBytes("key" + i.ToString("D6", CultureInfo.InvariantCulture));

    private static byte[] Value(int i) => Encoding.ASCII.GetBytes(
        "{\"id\":" + i + ",\"name\":\"customer-" + (i % 97) + "\",\"country\":\"" + (i % 3 == 0 ? "Portugal" : i % 3 == 1 ? "Germany" : "Brazil") + "\"}");

    private enum Shape
    {
        /// <summary>Binary search index and full filter.</summary>
        Unpartitioned,
        /// <summary>Two-level index and partitioned filter.</summary>
        Partitioned,
        /// <summary>Binary search index, full filter and a ZSTD compression dictionary.</summary>
        Dictionary,
    }

    private static byte[] BuildTable(Shape shape)
    {
        var entries = new List<(byte[], byte[])>(Count);
        for (int i = 0; i < Count; i++) entries.Add((TableTestUtil.IKey(UserKey(i), (ulong)(100 + i)), Value(i)));
        var format = new TableFormatOptions
        {
            BlockSize = 512,
            FilterPolicy = s_policy,
            IndexType = shape == Shape.Partitioned ? IndexType.TwoLevelIndexSearch : IndexType.BinarySearch,
            PartitionFilters = shape == Shape.Partitioned,
            MetadataBlockSize = 512,
        };
        TableBuilderOptions tbo = shape == Shape.Dictionary
            ? new TableBuilderOptions { CompressionType = CompressionType.Zstd, CompressionOptions = new CompressionOptions { MaxDictBytes = 4096 } }
            : new TableBuilderOptions();
        return TableTestUtil.BuildTable(entries, format, tbo);
    }

    /// <summary>
    /// A cache whose lookups can be made to miss while the entries stay in it, so that the insert that follows the miss
    /// finds the key present: the deterministic form of two readers racing to load the same block.
    /// </summary>
    private sealed class MissInjectingCache(BlockCache inner) : BlockCache
    {
        public bool FailLookups;

        public override string Name => inner.Name;
        public override long Capacity => inner.Capacity;
        public override void SetCapacity(long capacity) => inner.SetCapacity(capacity);
        public override bool StrictCapacityLimit => inner.StrictCapacityLimit;
        public override void SetStrictCapacityLimit(bool strictCapacityLimit) => inner.SetStrictCapacityLimit(strictCapacityLimit);
        public override long Usage => inner.Usage;
        public override long PinnedUsage => inner.PinnedUsage;
        public override int Count => inner.Count;

        public override bool TryLookup(in CacheKey key, out CachedBlock block)
        {
            if (FailLookups)
            {
                block = default;
                return false;
            }
            return inner.TryLookup(key, out block);
        }

        public override bool TryInsert(in CacheKey key, in CacheValue value, long charge, CachePriority priority, out CachedBlock block) =>
            inner.TryInsert(key, value, charge, priority, out block);

        public override bool Insert(in CacheKey key, in CacheValue value, long charge, CachePriority priority) => inner.Insert(key, value, charge, priority);
        public override bool Erase(in CacheKey key) => inner.Erase(key);
        public override void EraseUnreferencedEntries() => inner.EraseUnreferencedEntries();
    }

    private static BlockCache NewCache(bool hyperClock) => hyperClock
        ? BlockCache.CreateHyperClock(new HyperClockCacheOptions { Capacity = 64L << 20, EstimatedEntryCharge = 512 })
        : BlockCache.CreateLru(64L << 20);

    /// <summary>A table reader with its block cache and statistics.</summary>
    private sealed class Fixture : IDisposable
    {
        public readonly byte[] Data;
        public readonly BlockCache Cache;
        public readonly Statistics Stats = new();
        public BlockBasedTableReader Reader = null!;

        public Fixture(byte[] data, BlockCache? cache = null)
        {
            Data = data;
            Cache = cache ?? BlockCache.CreateLru(64L << 20);
        }

        public TableReaderOptions Options(bool cacheIndexAndFilter) => new()
        {
            BlockCache = Cache,
            CacheIndexAndFilterBlocks = cacheIndexAndFilter,
            FilterPolicy = s_policy,
            Statistics = Stats.BlockCacheCounters,
            // A stable base cache key, so that two readers of the same file share their entries.
            DbSessionId = "AUDIT4PINNING0000001",
            FileNumber = 14,
        };

        public Fixture Open(TableReaderOptions options)
        {
            Reader = TableTestUtil.Open(Data, options);
            return this;
        }

        public ulong T(Ticker ticker) => Stats.GetTickerCount(ticker);

        public (ulong Hit, ulong Miss, ulong Add) Index => (T(Ticker.BlockCacheIndexHit), T(Ticker.BlockCacheIndexMiss), T(Ticker.BlockCacheIndexAdd));
        public (ulong Hit, ulong Miss, ulong Add) Filter => (T(Ticker.BlockCacheFilterHit), T(Ticker.BlockCacheFilterMiss), T(Ticker.BlockCacheFilterAdd));

        public void AssertIndex(ulong hit, ulong miss, ulong add, string what) => Assert.AreEqual((hit, miss, add), Index, what + ": BLOCK_CACHE_INDEX_{HIT,MISS,ADD}");
        public void AssertFilter(ulong hit, ulong miss, ulong add, string what) => Assert.AreEqual((hit, miss, add), Filter, what + ": BLOCK_CACHE_FILTER_{HIT,MISS,ADD}");

        public void AssertGet(int i, TableReadFlags flags = TableReadFlags.None)
        {
            var w = new ArrayBufferWriter<byte>();
            TableGetResult r = Reader.Get(TableTestUtil.IKey(UserKey(i), DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek), w, flags);
            Assert.IsTrue(r.Found, "key " + i);
            Assert.AreEqual(Encoding.ASCII.GetString(Value(i)), Encoding.ASCII.GetString(w.WrittenSpan), "value " + i);
        }

        public async Task AssertGetAsync(int i, TableReadFlags flags = TableReadFlags.None)
        {
            var w = new ArrayBufferWriter<byte>();
            TableGetResult r = await Reader.GetAsync(TableTestUtil.IKey(UserKey(i), DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek), w, flags,
                CancellationToken.None);
            Assert.IsTrue(r.Found, "key " + i);
            Assert.AreEqual(Encoding.ASCII.GetString(Value(i)), Encoding.ASCII.GetString(w.WrittenSpan), "value " + i);
        }

        /// <summary>Seeks to key <paramref name="from"/> and reads <paramref name="n"/> entries.</summary>
        public void AssertSeekScan(int from, int n, TableReadFlags flags = TableReadFlags.None)
        {
            using BlockBasedTableIterator it = Reader.NewIterator(flags);
            it.Seek(TableTestUtil.IKey(UserKey(from), DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek));
            for (int i = from; i < from + n; i++, it.Next())
            {
                Assert.IsTrue(it.Valid, "scan " + i);
                Assert.AreEqual(Encoding.ASCII.GetString(Value(i)), Encoding.ASCII.GetString(it.Value), "scan value " + i);
            }
        }

        public void Dispose() => Reader?.Dispose();
    }

    private static BlockHandle FullFilterHandle(BlockBasedTableReader reader) =>
        reader.MetaBlocks[BlockBasedTableConstants.FullFilterBlockPrefix + s_policy.CompatibilityName];

    // ---------------------------------------------------------------------------------------------
    // BLOCK_CACHE_*_ADD_REDUNDANT

    /// <summary>
    /// LRUCacheShard::InsertItem (cache/lru_cache.cc): an insert that replaces an entry with the same key returns
    /// Status::OkOverwritten; a first insert, and an insert after the entry was erased, return OK.
    /// </summary>
    [TestMethod]
    public void LruCacheReportsAnOverwritingInsertAsRedundant()
    {
        BlockCache cache = BlockCache.CreateLru(1 << 20);
        var key = new CacheKey(7, 9);
        Assert.IsTrue(cache.TryInsert(key, CacheValue.FromObject(new object()), 100, CachePriority.Low, out CachedBlock first));
        Assert.IsFalse(first.InsertedRedundantly, "first insert");
        Assert.IsTrue(cache.TryInsert(key, CacheValue.FromObject(new object()), 100, CachePriority.Low, out CachedBlock second));
        Assert.IsTrue(second.InsertedRedundantly, "the key was present");
        Assert.IsTrue(cache.TryInsert(new CacheKey(7, 10), CacheValue.FromObject(new object()), 100, CachePriority.Low, out CachedBlock other));
        Assert.IsFalse(other.InsertedRedundantly, "another key");
        first.Dispose();
        second.Dispose();
        other.Dispose();
        Assert.IsTrue(cache.Erase(key));
        Assert.IsTrue(cache.TryInsert(key, CacheValue.FromObject(new object()), 100, CachePriority.Low, out CachedBlock third));
        Assert.IsFalse(third.InsertedRedundantly, "after erase");
        // The entry object is recycled: a later insert of a new key through it must not inherit the flag.
        third.Dispose();
        Assert.IsTrue(cache.TryInsert(key, CacheValue.FromObject(new object()), 100, CachePriority.Low, out CachedBlock fourth));
        Assert.IsTrue(fourth.InsertedRedundantly);
        fourth.Dispose();
        cache.EraseUnreferencedEntries();
        Assert.IsTrue(cache.TryInsert(new CacheKey(8, 1), CacheValue.FromObject(new object()), 100, CachePriority.Low, out CachedBlock fresh));
        Assert.IsFalse(fresh.InsertedRedundantly, "a recycled entry starts clean");
        fresh.Dispose();
    }

    /// <summary>
    /// BaseClockTable::Insert (cache/clock_cache.cc): Status::OkOverwritten for every insert that did not go into the
    /// table and was handed out as a standalone entry: here because the key was already present. The entry that is in
    /// the table stays the one lookups find.
    /// </summary>
    [TestMethod]
    public void HyperClockCacheReportsAStandaloneInsertAsRedundant()
    {
        BlockCache cache = BlockCache.CreateHyperClock(new HyperClockCacheOptions { Capacity = 1 << 20, EstimatedEntryCharge = 100 });
        var key = new CacheKey(7, 9);
        object original = new();
        Assert.IsTrue(cache.TryInsert(key, CacheValue.FromObject(original), 100, CachePriority.Low, out CachedBlock first));
        Assert.IsFalse(first.InsertedRedundantly, "first insert");
        Assert.IsTrue(cache.TryInsert(key, CacheValue.FromObject(new object()), 100, CachePriority.Low, out CachedBlock second));
        Assert.IsTrue(second.InsertedRedundantly, "the key was present: standalone");
        Assert.IsTrue(cache.TryLookup(key, out CachedBlock found));
        Assert.AreSame(original, found.Value, "the table keeps the first entry");
        Assert.IsFalse(found.InsertedRedundantly);
        found.Dispose();
        first.Dispose();
        second.Dispose();
        Assert.IsTrue(cache.TryInsert(new CacheKey(7, 10), CacheValue.FromObject(new object()), 100, CachePriority.Low, out CachedBlock other));
        Assert.IsFalse(other.InsertedRedundantly, "another key");
        other.Dispose();
    }

    /// <summary>
    /// BlockBasedTable::UpdateCacheInsertionMetrics with redundant = s.IsOkOverwritten(): a block whose lookup missed
    /// but whose insert found the key present records BLOCK_CACHE_ADD_REDUNDANT and the block type's
    /// BLOCK_CACHE_{INDEX,FILTER,DATA,COMPRESSION_DICT}_ADD_REDUNDANT, next to the usual ADD tickers; a normal insert
    /// records none. Both cache implementations.
    /// </summary>
    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void RedundantInsertsAreCountedForEveryBlockKind(bool hyperClock)
    {
        var cache = new MissInjectingCache(NewCache(hyperClock));
        using var f = new Fixture(BuildTable(Shape.Dictionary), cache);
        f.Open(f.Options(true));
        f.AssertGet(5);
        Ticker[] redundant =
        [
            Ticker.BlockCacheAddRedundant, Ticker.BlockCacheIndexAddRedundant, Ticker.BlockCacheFilterAddRedundant,
            Ticker.BlockCacheDataAddRedundant, Ticker.BlockCacheCompressionDictAddRedundant,
        ];
        foreach (Ticker t in redundant) Assert.AreEqual(0UL, f.T(t), t + " after regular inserts");
        Assert.AreEqual(4UL, f.T(Ticker.BlockCacheAdd), "index, filter, dictionary, one data block");

        // Every lookup misses although the entries are there: each block is read again and its insert is redundant.
        cache.FailLookups = true;
        f.AssertGet(5);
        cache.FailLookups = false;
        Assert.AreEqual(1UL, f.T(Ticker.BlockCacheIndexAddRedundant), "BLOCK_CACHE_INDEX_ADD_REDUNDANT");
        Assert.AreEqual(1UL, f.T(Ticker.BlockCacheFilterAddRedundant), "BLOCK_CACHE_FILTER_ADD_REDUNDANT");
        Assert.AreEqual(1UL, f.T(Ticker.BlockCacheDataAddRedundant), "BLOCK_CACHE_DATA_ADD_REDUNDANT");
        Assert.AreEqual(1UL, f.T(Ticker.BlockCacheCompressionDictAddRedundant), "BLOCK_CACHE_COMPRESSION_DICT_ADD_REDUNDANT");
        Assert.AreEqual(4UL, f.T(Ticker.BlockCacheAddRedundant), "BLOCK_CACHE_ADD_REDUNDANT");
        // A redundant insert is an insert too.
        Assert.AreEqual(8UL, f.T(Ticker.BlockCacheAdd), "BLOCK_CACHE_ADD");
        Assert.AreEqual(2UL, f.T(Ticker.BlockCacheIndexAdd));
        Assert.AreEqual(2UL, f.T(Ticker.BlockCacheFilterAdd));
        Assert.AreEqual(2UL, f.T(Ticker.BlockCacheDataAdd));
        Assert.AreEqual(2UL, f.T(Ticker.BlockCacheCompressionDictAdd));

        // Reads keep working and hit again.
        ulong hits = f.T(Ticker.BlockCacheHit);
        f.AssertGet(5);
        Assert.AreEqual(hits + 4, f.T(Ticker.BlockCacheHit));
        Assert.AreEqual(4UL, f.T(Ticker.BlockCacheAddRedundant));
    }

    /// <summary>
    /// The same for the blocks loaded by an iterator and by the async Get (BlockBasedTable::NewDataBlockIterator /
    /// RetrieveBlock -> PutDataBlockToCache -> UpdateCacheInsertionMetrics), and for index and filter partitions.
    /// </summary>
    [TestMethod]
    public async Task RedundantInsertsAreCountedForPartitionsScansAndAsyncReads()
    {
        var cache = new MissInjectingCache(NewCache(hyperClock: false));
        using var f = new Fixture(BuildTable(Shape.Partitioned), cache);
        f.Open(f.Options(false));
        f.AssertGet(5);
        Assert.AreEqual(0UL, f.T(Ticker.BlockCacheAddRedundant));

        cache.FailLookups = true;
        await f.AssertGetAsync(5);
        Assert.AreEqual(1UL, f.T(Ticker.BlockCacheIndexAddRedundant), "the index partition");
        Assert.AreEqual(1UL, f.T(Ticker.BlockCacheFilterAddRedundant), "the filter partition");
        Assert.AreEqual(1UL, f.T(Ticker.BlockCacheDataAddRedundant), "the data block");
        f.AssertSeekScan(5, 1);
        cache.FailLookups = false;
        Assert.AreEqual(2UL, f.T(Ticker.BlockCacheIndexAddRedundant), "scan: the index partition");
        Assert.AreEqual(2UL, f.T(Ticker.BlockCacheDataAddRedundant), "scan: the data block");
        Assert.AreEqual(5UL, f.T(Ticker.BlockCacheAddRedundant));
    }

    // ---------------------------------------------------------------------------------------------
    // Pinning decisions

    /// <summary>
    /// The is_pinned / pin_top_level_index / pin_partition / pin_unpartitioned computation of
    /// BlockBasedTable::PrefetchIndexAndFilterBlocks: kAll, kNone, kFlushedAndSimilar (level 0 and file_size &lt;=
    /// max_file_size_for_l0_meta_pin), and kFallback mapping top_level_index_pinning to pin_top_level_index_and_filter
    /// and the other two to pin_l0_filter_and_index_blocks_in_cache.
    /// </summary>
    [TestMethod]
    public void PinningTiersResolveThroughTheLegacyBooleans()
    {
        const long FileSize = 1000;
        var cases = new (PinningTier Tier, bool PinL0, bool PinTop, int Level, long MaxSize, bool Top, bool Other)[]
        {
            // tier, pin_l0, pin_top_level, level, max size -> top-level pinned, partition / unpartitioned pinned
            (PinningTier.Fallback, false, true, 3, 0, true, false),      // the defaults
            (PinningTier.Fallback, false, false, 0, FileSize, false, false),
            (PinningTier.Fallback, true, false, 0, FileSize, false, true),
            (PinningTier.Fallback, true, true, 0, FileSize - 1, true, false),
            (PinningTier.Fallback, true, true, 1, FileSize, true, false),
            (PinningTier.Fallback, true, true, -1, FileSize, true, false),
            (PinningTier.All, false, false, 5, 0, true, true),
            (PinningTier.None, true, true, 0, long.MaxValue, false, false),
            (PinningTier.FlushedAndSimilar, false, false, 0, FileSize, true, true),
            (PinningTier.FlushedAndSimilar, true, true, 0, FileSize - 1, false, false),
            (PinningTier.FlushedAndSimilar, true, true, 1, FileSize, false, false),
        };
        foreach (var c in cases)
        {
            var o = new TableReaderOptions
            {
                TopLevelIndexPinning = c.Tier, PartitionPinning = c.Tier, UnpartitionedPinning = c.Tier,
                PinL0FilterAndIndexBlocksInCache = c.PinL0, PinTopLevelIndexAndFilter = c.PinTop, Level = c.Level, MaxFileSizeForL0MetaPin = c.MaxSize,
            };
            MetadataPins pins = MetadataPins.Compute(o, FileSize);
            Assert.AreEqual(c.Top, pins.TopLevelIndex, "top level: " + c);
            Assert.AreEqual(c.Other, pins.Partition, "partition: " + c);
            Assert.AreEqual(c.Other, pins.Unpartitioned, "unpartitioned: " + c);
        }

        // The three tiers are independent.
        MetadataPins mixed = MetadataPins.Compute(new TableReaderOptions
        {
            TopLevelIndexPinning = PinningTier.None, PartitionPinning = PinningTier.All, UnpartitionedPinning = PinningTier.FlushedAndSimilar, Level = 2,
        }, FileSize);
        Assert.AreEqual(new MetadataPins(false, true, false), mixed);
    }

    // ---------------------------------------------------------------------------------------------
    // Unpartitioned index and full filter

    /// <summary>
    /// BinarySearchIndexReader::Create / FullFilterBlockReader::Create with use_cache, prefetch and no pin (the default
    /// with cache_index_and_filter_blocks): one miss + add each at open, the entries are released (no pinned usage),
    /// and IndexReaderCommon::GetOrReadIndexBlock / FilterBlockReaderCommon::GetOrReadFilterBlock look them up on every
    /// use: the index once per Get and once per iterator, the filter once per filter probe.
    /// </summary>
    [TestMethod]
    public async Task UnpinnedIndexAndFilterAreLookedUpOnEveryUse()
    {
        using var f = new Fixture(BuildTable(Shape.Unpartitioned));
        f.Open(f.Options(true));
        f.AssertIndex(0, 1, 1, "open");
        f.AssertFilter(0, 1, 1, "open");
        Assert.IsTrue(f.Cache.Usage > 0, "the index and filter are charged to the cache");
        Assert.AreEqual(0L, f.Cache.PinnedUsage, "nothing stays pinned after the open");
        Assert.IsTrue(f.Reader.IndexAndFilterInCache);

        f.AssertGet(10);
        f.AssertIndex(1, 1, 1, "first Get");
        f.AssertFilter(1, 1, 1, "first Get");
        f.AssertGet(10);
        await f.AssertGetAsync(11);
        f.AssertIndex(3, 1, 1, "three Gets");
        f.AssertFilter(3, 1, 1, "three Gets");
        Assert.AreEqual(0L, f.Cache.PinnedUsage, "a Get releases what it looked up");

        // An iterator looks the index up once, when it is created (BlockBasedTable::NewIterator -> NewIndexIterator),
        // and holds it while it lives; it does not probe the filter without a prefix extractor.
        f.AssertSeekScan(100, 300);
        f.AssertSeekScan(900, 5);
        f.AssertIndex(5, 1, 1, "two iterators");
        f.AssertFilter(3, 1, 1, "two iterators");
        using (BlockBasedTableIterator it = f.Reader.NewIterator())
        {
            Assert.IsTrue(f.Cache.PinnedUsage > 0, "the live iterator pins the index block");
        }
        Assert.AreEqual(0L, f.Cache.PinnedUsage);

        // A key the filter excludes probes the filter and never reaches the index.
        var w = new ArrayBufferWriter<byte>();
        ulong indexHits = f.Index.Hit;
        int negatives = 0;
        for (int i = 0; i < 20; i++)
        {
            if (!f.Reader.Get(TableTestUtil.IKey(Encoding.ASCII.GetBytes("absent" + i), DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek), w).Found
                && f.Index.Hit == indexHits)
            {
                negatives++;
            }
            indexHits = f.Index.Hit;
        }
        Assert.IsTrue(negatives >= 15, "the bloom filter excludes most absent keys without an index lookup");
        f.AssertFilter(3 + 20, 1, 1, "twenty filter probes");

        // IndexReaderCommon::ApproximateMemoryUsage: an index in the block cache is not table reader memory.
        using BlockBasedTableReader owned = TableTestUtil.Open(f.Data, f.Options(false));
        Assert.IsTrue(owned.ApproximateMemoryUsage > f.Reader.ApproximateMemoryUsage, "the owning reader counts its index and filter");
    }

    /// <summary>
    /// BinarySearchIndexReader::Create / FullFilterBlockReader::Create with pin (unpartitioned_pinning = kAll): the
    /// reader keeps the cache entries, so GetOrReadIndexBlock / GetOrReadFilterBlock return them without a lookup (no
    /// hit ticker per read), they stay charged to the cache as pinned usage, and they survive the eviction of
    /// everything unreferenced. Closing the reader releases them.
    /// </summary>
    [TestMethod]
    public async Task PinnedIndexAndFilterStayInTheCacheAndAreNeverLookedUpAgain()
    {
        using var f = new Fixture(BuildTable(Shape.Unpartitioned));
        f.Open(f.Options(true) with { UnpartitionedPinning = PinningTier.All });
        f.AssertIndex(0, 1, 1, "open");
        f.AssertFilter(0, 1, 1, "open");
        long indexCharge = f.Reader.IndexBlock.Charge;
        long filterSize = (long)FullFilterHandle(f.Reader).Size;
        Assert.IsTrue(f.Cache.PinnedUsage >= indexCharge + filterSize, "the pinned index and filter are charged to the cache");
        Assert.AreEqual(f.Cache.Usage, f.Cache.PinnedUsage, "nothing else is in the cache yet");
        Assert.IsNull(f.Reader.PinnedFilterBuffer, "the filter belongs to the cache, not to the reader");

        for (int i = 0; i < Count; i += 37) f.AssertGet(i);
        await f.AssertGetAsync(3);
        f.AssertSeekScan(50, 200);
        f.AssertIndex(0, 1, 1, "pinned: no index lookup after the open");
        f.AssertFilter(0, 1, 1, "pinned: no filter lookup after the open");

        long pinned = f.Cache.PinnedUsage;
        f.Cache.EraseUnreferencedEntries();
        Assert.AreEqual(pinned, f.Cache.PinnedUsage, "still pinned after eviction");
        Assert.AreEqual(pinned, f.Cache.Usage, "only the pinned entries are left");
        for (int i = 0; i < Count; i += 37) f.AssertGet(i);
        f.AssertIndex(0, 1, 1, "after eviction");
        f.AssertFilter(0, 1, 1, "after eviction");

        // IndexReaderCommon / FilterBlockReaderCommon::ApproximateMemoryUsage: only blocks the reader owns.
        using (BlockBasedTableReader owned = TableTestUtil.Open(f.Data, f.Options(false)))
        {
            Assert.IsTrue(owned.ApproximateMemoryUsage >= (ulong)(indexCharge + filterSize));
            Assert.IsTrue(f.Reader.ApproximateMemoryUsage < (ulong)indexCharge, "pinned in the cache is not table reader memory");
        }

        // A second reader of the same file finds both entries (hits) instead of reading them.
        using BlockBasedTableReader second = TableTestUtil.Open(f.Data, f.Options(true) with { UnpartitionedPinning = PinningTier.All });
        f.AssertIndex(1, 1, 1, "second open");
        f.AssertFilter(1, 1, 1, "second open");

        f.Reader.Dispose();
        second.Dispose();
        Assert.AreEqual(0L, f.Cache.PinnedUsage, "closing the readers releases the pins");
        f.Cache.EraseUnreferencedEntries();
        Assert.AreEqual(0L, f.Cache.Usage);
        f.Reader = TableTestUtil.Open(f.Data, f.Options(true));
        f.AssertIndex(1, 2, 2, "reopen after eviction");
        f.AssertFilter(1, 2, 2, "reopen after eviction");
    }

    /// <summary>
    /// pin_unpartitioned in BlockBasedTable::PrefetchIndexAndFilterBlocks through kFallback: with
    /// pin_l0_filter_and_index_blocks_in_cache the index and filter of a level-0 file of at most
    /// max_file_size_for_l0_meta_pin bytes are pinned; the same file at level 1, or a larger one, is not. Also the
    /// HyperClockCache.
    /// </summary>
    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void PinL0FilterAndIndexBlocksPinsFlushedFilesOnly(bool hyperClock)
    {
        byte[] data = BuildTable(Shape.Unpartitioned);
        var cases = new (int Level, long MaxSize, bool Pinned)[] { (0, data.Length, true), (1, data.Length, false), (0, data.Length - 1, false) };
        foreach ((int level, long maxSize, bool pinned) in cases)
        {
            using var f = new Fixture(data, NewCache(hyperClock));
            f.Open(f.Options(true) with { PinL0FilterAndIndexBlocksInCache = true, Level = level, MaxFileSizeForL0MetaPin = maxSize });
            string what = "level " + level + " max " + maxSize;
            f.AssertIndex(0, 1, 1, what);
            f.AssertFilter(0, 1, 1, what);
            Assert.AreEqual(pinned, f.Cache.PinnedUsage > 0, what + ": pinned usage after the open");
            f.AssertGet(3);
            f.AssertGet(4);
            f.AssertIndex(pinned ? 0UL : 2UL, 1, 1, what + ": Get");
            f.AssertFilter(pinned ? 0UL : 2UL, 1, 1, what + ": Get");
        }
    }

    /// <summary>
    /// IndexReaderCommon::GetOrReadIndexBlock / FilterBlockReaderCommon::GetOrReadFilterBlock after the entries were
    /// evicted: ReadIndexBlock / ReadFilterBlock miss, re-read the block (BlockBasedTable::RetrieveBlock) and insert
    /// it again.
    /// </summary>
    [TestMethod]
    public async Task EvictedIndexAndFilterAreReadAndInsertedAgain()
    {
        using var f = new Fixture(BuildTable(Shape.Unpartitioned));
        f.Open(f.Options(true));
        ulong indexBytes = f.T(Ticker.BlockCacheIndexBytesInsert);
        ulong filterBytes = f.T(Ticker.BlockCacheFilterBytesInsert);
        Assert.IsTrue(indexBytes > 0 && filterBytes > 0);

        f.Cache.EraseUnreferencedEntries();
        Assert.AreEqual(0L, f.Cache.Usage, "unpinned: everything can be evicted");
        f.AssertGet(42);
        f.AssertIndex(0, 2, 2, "sync re-read");
        f.AssertFilter(0, 2, 2, "sync re-read");
        Assert.AreEqual(2 * indexBytes, f.T(Ticker.BlockCacheIndexBytesInsert), "the same charge is inserted again");
        Assert.AreEqual(2 * filterBytes, f.T(Ticker.BlockCacheFilterBytesInsert));
        f.AssertGet(43);
        f.AssertIndex(1, 2, 2, "cached again");
        f.AssertFilter(1, 2, 2, "cached again");

        f.Cache.EraseUnreferencedEntries();
        await f.AssertGetAsync(200);
        f.AssertIndex(1, 3, 3, "async re-read");
        f.AssertFilter(1, 3, 3, "async re-read");

        f.Cache.EraseUnreferencedEntries();
        f.AssertSeekScan(0, Count);
        f.AssertIndex(1, 4, 4, "one re-read for the whole scan");
        f.AssertFilter(1, 3, 3, "a scan does not use the filter");
    }

    /// <summary>
    /// BlockBasedTable::RetrieveBlock with read_tier = kBlockCacheTier: an unpinned index block that is not cached makes
    /// the read Incomplete ("no blocking io") even when the data block is cached, records the miss and inserts
    /// nothing; an unpinned filter that is not cached answers "may match" (FullFilterBlockReader::MayMatch) and the
    /// read goes on. A pinned index and filter serve the read whatever was evicted.
    /// </summary>
    [TestMethod]
    public async Task BlockCacheTierReadNeedsTheUnpinnedIndexInTheCache()
    {
        using var f = new Fixture(BuildTable(Shape.Unpartitioned));
        f.Open(f.Options(true));
        f.AssertGet(7);                                      // caches the data block of key 7
        f.AssertGet(7, TableReadFlags.BlockCacheTier);       // index, filter and data block cached
        f.AssertIndex(2, 1, 1, "cached");
        f.AssertFilter(2, 1, 1, "cached");

        // Drop only the filter entry: the probe misses, answers "may match", and the read is served.
        CacheKey filterKey = f.Reader.BaseCacheKey.WithOffsetForMinSizeRecord(FullFilterHandle(f.Reader).Offset);
        Assert.IsTrue(f.Cache.Erase(filterKey), "the filter entry was cached");
        f.AssertGet(7, TableReadFlags.BlockCacheTier);
        f.AssertFilter(2, 2, 1, "no-io filter miss inserts nothing");
        f.AssertIndex(3, 1, 1, "the read went on to the index");

        // Drop only the index entry: the data block stays cached, the read is Incomplete.
        CacheKey indexKey = f.Reader.BaseCacheKey.WithOffsetForMinSizeRecord(f.Reader.IndexHandle.Offset);
        Assert.IsTrue(f.Cache.Erase(indexKey), "the index entry was cached");
        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => f.AssertGet(7, TableReadFlags.BlockCacheTier));
        Assert.AreEqual(StatusCode.Incomplete, ex.Code);
        ex = await Assert.ThrowsExactlyAsync<RocksDbException>(() => f.AssertGetAsync(7, TableReadFlags.BlockCacheTier));
        Assert.AreEqual(StatusCode.Incomplete, ex.Code);
        ex = Assert.ThrowsExactly<RocksDbException>(() => f.AssertSeekScan(7, 1, TableReadFlags.BlockCacheTier));
        Assert.AreEqual(StatusCode.Incomplete, ex.Code);
        f.AssertIndex(3, 4, 1, "three no-io misses, nothing inserted");

        f.AssertGet(7);                                      // I/O allowed: both are re-read and inserted
        f.AssertIndex(3, 5, 2, "re-read");
        f.AssertFilter(2, 5, 2, "re-read");
        f.AssertGet(7, TableReadFlags.BlockCacheTier);
        f.AssertIndex(4, 5, 2, "served from the cache again");

        // Pinned: the index and filter cannot be evicted, so a no-io read only needs the data block.
        using var p = new Fixture(f.Data, BlockCache.CreateLru(64L << 20));
        p.Open(p.Options(true) with { UnpartitionedPinning = PinningTier.All });
        p.AssertGet(7);
        Assert.IsFalse(p.Cache.Erase(p.Reader.BaseCacheKey.WithOffsetForMinSizeRecord(p.Reader.IndexHandle.Offset)) && p.Cache.PinnedUsage == 0,
            "erasing a pinned entry does not free it");
        p.AssertGet(7, TableReadFlags.BlockCacheTier);
        p.Cache.EraseUnreferencedEntries();                  // evicts the data block
        ex = Assert.ThrowsExactly<RocksDbException>(() => p.AssertGet(7, TableReadFlags.BlockCacheTier));
        Assert.AreEqual(StatusCode.Incomplete, ex.Code);
        p.AssertIndex(0, 1, 1, "pinned: the Incomplete came from the data block");
    }

    /// <summary>
    /// BlockBasedTable::MaybeReadBlockAndLoadToCache with fill_cache = false for an unpinned index / filter: a miss
    /// reads the block for this read only and does not insert it, so the next read misses again.
    /// </summary>
    [TestMethod]
    public void FillCacheFalseReadsTheIndexAndFilterWithoutInsertingThem()
    {
        using var f = new Fixture(BuildTable(Shape.Unpartitioned));
        f.Open(f.Options(true));
        f.Cache.EraseUnreferencedEntries();
        f.AssertGet(5, TableReadFlags.DontFillCache);
        f.AssertGet(5, TableReadFlags.DontFillCache);
        f.AssertIndex(0, 3, 1, "two fill_cache=false reads");
        f.AssertFilter(0, 3, 1, "two fill_cache=false reads");
        Assert.AreEqual(0L, f.Cache.Usage, "nothing was inserted");
        f.AssertGet(5);
        f.AssertIndex(0, 4, 2, "a filling read inserts them");
        f.AssertFilter(0, 4, 2, "a filling read inserts them");
        f.AssertGet(5, TableReadFlags.DontFillCache);
        f.AssertIndex(1, 4, 2, "fill_cache=false still uses cached blocks");
        f.AssertFilter(1, 4, 2, "fill_cache=false still uses cached blocks");
    }

    /// <summary>
    /// prefetch_index = prefetch_all || pin_index (and the same for the filter) in
    /// BlockBasedTable::PrefetchIndexAndFilterBlocks: without prefetch_index_and_filter_in_cache an unpinned index and
    /// filter are not touched at open and are first read by the first read; pinned ones are loaded at open all the same.
    /// </summary>
    [TestMethod]
    public void WithoutPrefetchOnlyPinnedBlocksAreLoadedAtOpen()
    {
        byte[] data = BuildTable(Shape.Unpartitioned);
        using (var f = new Fixture(data))
        {
            f.Open(f.Options(true) with { PrefetchIndexAndFilterInCache = false });
            f.AssertIndex(0, 0, 0, "open without prefetch");
            f.AssertFilter(0, 0, 0, "open without prefetch");
            Assert.AreEqual(0L, f.Cache.Usage);
            f.AssertGet(1);
            f.AssertIndex(0, 1, 1, "first read");
            f.AssertFilter(0, 1, 1, "first read");
            f.AssertGet(1);
            f.AssertIndex(1, 1, 1, "second read");
        }
        using (var f = new Fixture(data))
        {
            f.Open(f.Options(true) with { PrefetchIndexAndFilterInCache = false, UnpartitionedPinning = PinningTier.All });
            f.AssertIndex(0, 1, 1, "pinned: loaded at open");
            f.AssertFilter(0, 1, 1, "pinned: loaded at open");
            Assert.IsTrue(f.Cache.PinnedUsage > 0);
            f.AssertGet(1);
            f.AssertIndex(0, 1, 1, "pinned: no lookup");
        }
    }

    /// <summary>
    /// BinarySearchIndexReader::Create returns the status of ReadIndexBlock: an index block with a bad checksum fails
    /// BlockBasedTable::Open with cache_index_and_filter_blocks, pinned or not. FullFilterBlockReader::Create ignores
    /// the error of its prefetch (IGNORE_STATUS_IF_ERROR) and returns no reader: the table opens without a filter.
    /// </summary>
    [TestMethod]
    public void CorruptIndexFailsTheOpenAndCorruptFilterIsIgnored()
    {
        byte[] good = BuildTable(Shape.Unpartitioned);
        int indexOffset, filterOffset;
        using (BlockBasedTableReader r = TableTestUtil.Open(good, new TableReaderOptions { FilterPolicy = s_policy }))
        {
            indexOffset = checked((int)r.IndexHandle.Offset);
            filterOffset = checked((int)FullFilterHandle(r).Offset);
        }
        foreach (PinningTier tier in new[] { PinningTier.None, PinningTier.All })
        {
            byte[] bad = (byte[])good.Clone();
            bad[indexOffset + 3] ^= 0x40;
            using var f = new Fixture(bad);
            RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => TableTestUtil.Open(bad, f.Options(true) with { UnpartitionedPinning = tier }));
            Assert.AreEqual(StatusCode.Corruption, ex.Code, "index, " + tier);
            f.AssertIndex(0, 1, 0, "index, " + tier);
            Assert.AreEqual(0L, f.Cache.PinnedUsage, "a failed open leaves nothing pinned");

            bad = (byte[])good.Clone();
            bad[filterOffset + 3] ^= 0x40;
            using var g = new Fixture(bad);
            g.Open(g.Options(true) with { UnpartitionedPinning = tier });
            g.AssertFilter(0, 1, 0, "filter, " + tier);
            Assert.IsFalse(g.Reader.HasFilter, "no filter reader was created");
            g.AssertGet(12);
            Assert.IsTrue(g.Reader.KeyMayMatch("absent"u8), "without a filter every key may match");
            g.AssertFilter(0, 1, 0, "the filter is not looked up again");
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Partitioned index and filter

    /// <summary>
    /// PartitionIndexReader::CacheDependencies / PartitionedFilterBlockReader::CacheDependencies with prefetch_all and
    /// no pin_partition (the defaults, cache_index_and_filter_blocks off): the top levels are owned by the reader,
    /// every partition is loaded into the block cache at open (a miss + add each) and released, and each read looks
    /// its index partition and filter partition up (PartitionIndexReader::NewIterator / GetFilterPartitionBlock).
    /// </summary>
    [TestMethod]
    public void PartitionsAreLoadedIntoTheCacheAtOpenAndLookedUpPerRead()
    {
        using var f = new Fixture(BuildTable(Shape.Partitioned));
        f.Open(f.Options(false));
        ulong indexPartitions = f.Reader.Properties.IndexPartitions;
        Assert.IsTrue(indexPartitions > 3, "the table has several index partitions");
        f.AssertIndex(0, indexPartitions, indexPartitions, "open");
        ulong filterPartitions = f.Filter.Miss;
        Assert.IsTrue(filterPartitions > 1, "the table has several filter partitions");
        f.AssertFilter(0, filterPartitions, filterPartitions, "open");
        Assert.AreEqual(0L, f.Cache.PinnedUsage, "not pinned");
        Assert.AreEqual(0, f.Reader.PinnedIndexPartitionCount);
        Assert.AreEqual(0, f.Reader.PinnedFilterPartitionCount);

        f.AssertGet(10);
        f.AssertGet(Count - 1);
        f.AssertIndex(2, indexPartitions, indexPartitions, "one index partition lookup per Get");
        f.AssertFilter(2, filterPartitions, filterPartitions, "one filter partition lookup per Get");

        // Evicted partitions are read again on demand.
        f.Cache.EraseUnreferencedEntries();
        f.AssertGet(10);
        f.AssertIndex(2, indexPartitions + 1, indexPartitions + 1, "re-read");
        f.AssertFilter(2, filterPartitions + 1, filterPartitions + 1, "re-read");
    }

    /// <summary>
    /// CacheDependencies with pin (partition_pinning = kAll): the reader keeps every partition (partition_map_ /
    /// filter_map_), reads use them without a lookup, and they stay in the cache as pinned usage. Applies with and
    /// without cache_index_and_filter_blocks ("the partitions of partitioned index are always stored in cache").
    /// </summary>
    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task PinnedPartitionsAreNeverLookedUpAgain(bool cacheIndexAndFilter)
    {
        using var f = new Fixture(BuildTable(Shape.Partitioned));
        f.Open(f.Options(cacheIndexAndFilter) with { PartitionPinning = PinningTier.All });
        ulong indexPartitions = f.Reader.Properties.IndexPartitions;
        // With cache_index_and_filter_blocks the top levels go through the cache too (pinned by default).
        ulong top = cacheIndexAndFilter ? 1UL : 0UL;
        f.AssertIndex(0, indexPartitions + top, indexPartitions + top, "open");
        Assert.AreEqual((int)indexPartitions, f.Reader.PinnedIndexPartitionCount);
        int filterPartitions = f.Reader.PinnedFilterPartitionCount;
        Assert.IsTrue(filterPartitions > 1);
        f.AssertFilter(0, (ulong)filterPartitions + top, (ulong)filterPartitions + top, "open");
        Assert.AreEqual(f.Cache.Usage, f.Cache.PinnedUsage, "everything loaded at open is pinned");

        for (int i = 0; i < Count; i += 41) f.AssertGet(i);
        await f.AssertGetAsync(Count - 1);
        f.AssertSeekScan(0, Count);
        f.AssertIndex(0, indexPartitions + top, indexPartitions + top, "pinned: no index lookups");
        f.AssertFilter(0, (ulong)filterPartitions + top, (ulong)filterPartitions + top, "pinned: no filter lookups");

        long pinned = f.Cache.PinnedUsage;
        f.Cache.EraseUnreferencedEntries();
        Assert.AreEqual(pinned, f.Cache.Usage, "the pinned partitions survive eviction");
        f.AssertGet(77, TableReadFlags.DontFillCache);
        f.AssertIndex(0, indexPartitions + top, indexPartitions + top, "after eviction");

        f.Reader.Dispose();
        Assert.AreEqual(0L, f.Cache.PinnedUsage, "closing the reader releases the partitions");
    }

    /// <summary>
    /// PartitionIndexReader::Create / PartitionedFilterBlockReader::Create with use_cache: the top-level blocks are
    /// cache entries, pinned under top_level_index_pinning (kFallback: pin_top_level_index_and_filter, default true).
    /// Unpinned (kNone) they are looked up by CacheDependencies at open (a hit) and by every read, next to the
    /// partition.
    /// </summary>
    [TestMethod]
    public void TopLevelIndexPinningDecidesWhetherTheTopLevelsAreLookedUp()
    {
        byte[] data = BuildTable(Shape.Partitioned);
        using (var f = new Fixture(data))
        {
            f.Open(f.Options(true));
            ulong ip = f.Reader.Properties.IndexPartitions;
            f.AssertIndex(0, ip + 1, ip + 1, "pinned top level: open");
            ulong fp = f.Filter.Miss - 1;
            f.AssertFilter(0, fp + 1, fp + 1, "pinned top level: open");
            Assert.IsTrue(f.Cache.PinnedUsage > 0, "the top levels are pinned");
            Assert.IsTrue(f.Cache.PinnedUsage < f.Cache.Usage, "the partitions are not");
            f.AssertGet(10);
            f.AssertIndex(1, ip + 1, ip + 1, "pinned top level: only the partition is looked up");
            f.AssertFilter(1, fp + 1, fp + 1, "pinned top level: only the partition is looked up");
            f.Cache.EraseUnreferencedEntries();
            Assert.AreEqual(f.Cache.Usage, f.Cache.PinnedUsage, "the top levels survive eviction");
        }
        foreach ((PinningTier tier, bool pinTop) in new[] { (PinningTier.None, true), (PinningTier.Fallback, false) })
        {
            using var f = new Fixture(data);
            f.Open(f.Options(true) with { TopLevelIndexPinning = tier, PinTopLevelIndexAndFilter = pinTop });
            ulong ip = f.Reader.Properties.IndexPartitions;
            f.AssertIndex(1, ip + 1, ip + 1, "unpinned top level: open (CacheDependencies looks the top level up)");
            ulong fp = f.Filter.Miss - 1;
            f.AssertFilter(1, fp + 1, fp + 1, "unpinned top level: open");
            Assert.AreEqual(0L, f.Cache.PinnedUsage);
            f.AssertGet(10);
            f.AssertIndex(3, ip + 1, ip + 1, "unpinned top level: top level and partition are looked up");
            f.AssertFilter(3, fp + 1, fp + 1, "unpinned top level: top level and partition are looked up");
            f.Cache.EraseUnreferencedEntries();
            Assert.AreEqual(0L, f.Cache.Usage);
            f.AssertGet(10);
            f.AssertIndex(3, ip + 3, ip + 3, "both levels are re-read");
            f.AssertFilter(3, fp + 3, fp + 3, "both levels are re-read");
        }
    }

    /// <summary>
    /// FilterBlockReaderCommon::GetOrReadFilterBlock with read_tier = kBlockCacheTier for a top-level filter index that
    /// is not pinned: the block cache is looked up (no_io only forbids the read), so a cached top level and partition
    /// still filter an absent key; once the top level is evicted the probe answers "may match" and inserts nothing.
    /// </summary>
    [TestMethod]
    public void BlockCacheTierProbeUsesTheUnpinnedTopLevelFilterIndexFromTheCache()
    {
        using var f = new Fixture(BuildTable(Shape.Partitioned));
        f.Open(f.Options(true) with { TopLevelIndexPinning = PinningTier.None });
        Assert.AreEqual(0L, f.Cache.PinnedUsage);
        byte[] absent = TableTestUtil.IKey(Encoding.ASCII.GetBytes("key000010x"), DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
        (ulong hit, ulong miss, ulong add) = f.Filter;
        Assert.IsFalse(f.Reader.InternalKeyMayMatch(absent, TableReadFlags.BlockCacheTier), "the cached filter rejects the key without I/O");
        f.AssertFilter(hit + 2, miss, add, "top level and partition looked up");

        f.Cache.EraseUnreferencedEntries();
        Assert.IsTrue(f.Reader.InternalKeyMayMatch(absent, TableReadFlags.BlockCacheTier), "top level not cached: may match");
        f.AssertFilter(hit + 2, miss + 1, add, "a no-io miss of the top level, nothing inserted");
        Assert.AreEqual(0L, f.Cache.Usage);
        Assert.IsFalse(f.Reader.InternalKeyMayMatch(absent, TableReadFlags.None), "with I/O both levels are re-read");
        f.AssertFilter(hit + 2, miss + 3, add + 2, "re-read");
        Assert.IsFalse(f.Reader.InternalKeyMayMatch(absent, TableReadFlags.BlockCacheTier));
        f.AssertFilter(hit + 4, miss + 3, add + 2, "served from the cache again");
    }

    /// <summary>
    /// BlockBasedTable::Open: prefetch_all = prefetch_index_and_filter_in_cache || level == 0. A level-0 table warms
    /// the block cache at open even when the caller asked for no prefetch (the tables DB::Open loads); other levels
    /// load nothing unpinned.
    /// </summary>
    [TestMethod]
    public void LevelZeroTablesAlwaysPrefetch()
    {
        foreach (Shape shape in new[] { Shape.Unpartitioned, Shape.Partitioned })
        {
            byte[] data = BuildTable(shape);
            using (var f = new Fixture(data))
            {
                f.Open(f.Options(true) with { PrefetchIndexAndFilterInCache = false, PinTopLevelIndexAndFilter = false, Level = 1 });
                f.AssertIndex(0, 0, 0, shape + " level 1: open");
                f.AssertFilter(0, 0, 0, shape + " level 1: open");
                Assert.AreEqual(0L, f.Cache.Usage);
            }
            using (var f = new Fixture(data))
            {
                f.Open(f.Options(true) with { PrefetchIndexAndFilterInCache = false, PinTopLevelIndexAndFilter = false, Level = 0 });
                Assert.IsTrue(f.Index.Add > 0 && f.Index.Add == f.Index.Miss, shape + " level 0: the index is loaded at open");
                Assert.IsTrue(f.Filter.Add > 0 && f.Filter.Add == f.Filter.Miss, shape + " level 0: the filter is loaded at open");
                Assert.AreEqual(0L, f.Cache.PinnedUsage, shape + " level 0: nothing is pinned");
                ulong indexMiss = f.Index.Miss;
                ulong filterMiss = f.Filter.Miss;
                f.AssertGet(10);
                Assert.AreEqual(indexMiss, f.Index.Miss, shape + " level 0: the first read finds the index cached");
                Assert.AreEqual(filterMiss, f.Filter.Miss, shape + " level 0: the first read finds the filter cached");
            }
        }
    }

    /// <summary>
    /// prefetch_all || pin_partition guards CacheDependencies in BlockBasedTable::PrefetchIndexAndFilterBlocks: without
    /// prefetch_index_and_filter_in_cache and without pinning no partition is loaded at open.
    /// </summary>
    [TestMethod]
    public void WithoutPrefetchPartitionsAreLoadedOnDemand()
    {
        using var f = new Fixture(BuildTable(Shape.Partitioned));
        f.Open(f.Options(false) with { PrefetchIndexAndFilterInCache = false });
        f.AssertIndex(0, 0, 0, "open");
        f.AssertFilter(0, 0, 0, "open");
        f.AssertGet(10);
        f.AssertIndex(0, 1, 1, "first read loads its index partition");
        f.AssertFilter(0, 1, 1, "first read loads its filter partition");
    }

    /// <summary>
    /// PartitionedFilterBlockReader::CacheDependencies without a block cache: BlockBasedTable::MaybeReadBlockAndLoadToCache
    /// is a no-op, so no partition is loaded or kept at open and GetFilterPartitionBlock reads the partition for every
    /// probe. The port loads every partition at open and owns them for the reader's lifetime.
    /// </summary>
    [TestMethod]
    [Ignore("audit4: without a native BlockCache the reader owns all filter partitions; C++ keeps none and reads the partition on every probe")]
    public void WithoutABlockCacheNoFilterPartitionIsKept()
    {
        byte[] data = BuildTable(Shape.Partitioned);
        using BlockBasedTableReader reader = TableTestUtil.Open(data, new TableReaderOptions { FilterPolicy = s_policy });
        Assert.AreEqual(0, reader.PinnedFilterPartitionCount, "filter_map_ stays empty");
        using BlockBasedTableReader noFilter = TableTestUtil.Open(data, new TableReaderOptions());
        Assert.IsTrue(reader.ApproximateMemoryUsage - noFilter.ApproximateMemoryUsage < 1024, "only the top-level filter index is reader memory");
    }

    // ---------------------------------------------------------------------------------------------
    // Through the DB

    /// <summary>
    /// The options reach BlockBasedTable::Open through the table factory (BlockBasedTableOptions::metadata_cache_options
    /// and pin_l0_filter_and_index_blocks_in_cache, TableReaderOptions::level / max_file_size_for_l0_meta_pin): a
    /// flushed level-0 file pins its index and filter under pin_l0 or unpartitioned_pinning = kAll, so Gets record no
    /// index / filter hit; with the defaults every Get records one of each.
    /// </summary>
    [TestMethod]
    public void DatabaseReadsFollowThePinningOptions()
    {
        var cases = new (bool PinL0, PinningTier Tier, bool Pinned)[]
        {
            (false, PinningTier.Fallback, false), (true, PinningTier.Fallback, true), (false, PinningTier.All, true), (true, PinningTier.None, false),
        };
        foreach ((bool pinL0, PinningTier tier, bool pinned) in cases)
        {
            string path = Path.Combine(Path.GetTempPath(), "audit4-pinning-" + Guid.NewGuid().ToString("N"));
            try
            {
                var stats = new Statistics();
                var cf = new ColumnFamilyOptions { DisableAutoCompactions = true };
                cf.TableOptions.FilterPolicy = new BloomFilterPolicy(10);
                cf.TableOptions.CacheIndexAndFilterBlocks = true;
                cf.TableOptions.PinL0FilterAndIndexBlocksInCache = pinL0;
                cf.TableOptions.MetadataCacheOptions.UnpartitionedPinning = tier;
                using RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true, Statistics = stats }, cf);
                for (int i = 0; i < 500; i++) db.Put(UserKey(i), Value(i));
                db.Flush();
                string what = "pinL0=" + pinL0 + " tier=" + tier;
                Assert.IsNotNull(db.Get(UserKey(0)), what); // the table is open
                ulong indexHit = stats.GetTickerCount(Ticker.BlockCacheIndexHit);
                ulong filterHit = stats.GetTickerCount(Ticker.BlockCacheFilterHit);
                for (int i = 1; i <= 10; i++) Assert.IsNotNull(db.Get(UserKey(i * 7)), what);
                Assert.AreEqual(pinned ? indexHit : indexHit + 10, stats.GetTickerCount(Ticker.BlockCacheIndexHit), what + ": BLOCK_CACHE_INDEX_HIT");
                Assert.AreEqual(pinned ? filterHit : filterHit + 10, stats.GetTickerCount(Ticker.BlockCacheFilterHit), what + ": BLOCK_CACHE_FILTER_HIT");
                Assert.AreEqual(1UL, stats.GetTickerCount(Ticker.BlockCacheIndexAdd), what + ": one index insert");
                Assert.AreEqual(1UL, stats.GetTickerCount(Ticker.BlockCacheFilterAdd), what + ": one filter insert");
                Assert.AreEqual(0UL, stats.GetTickerCount(Ticker.BlockCacheAddRedundant), what);
            }
            finally
            {
                if (Directory.Exists(path)) Directory.Delete(path, true);
            }
        }
    }

    private static ColumnFamilyOptions DbColumnFamilyOptions(bool partitioned, bool cacheIndexAndFilter, BlockCache? cache = null)
    {
        var cf = new ColumnFamilyOptions { DisableAutoCompactions = true };
        cf.TableOptions.BlockSize = 512;
        cf.TableOptions.FilterPolicy = new BloomFilterPolicy(10);
        cf.TableOptions.CacheIndexAndFilterBlocks = cacheIndexAndFilter;
        cf.TableOptions.BlockCache = cache;
        if (partitioned)
        {
            cf.TableOptions.IndexType = IndexType.TwoLevelIndexSearch;
            cf.TableOptions.PartitionFilters = true;
            cf.TableOptions.MetadataBlockSize = 512;
        }
        return cf;
    }

    /// <summary>
    /// BuildTable (db/builder.cc, "Verify that the table is usable"): the flush creates an iterator over the table it
    /// wrote (table_cache-&gt;NewIterator), which opens the table (index and filter miss + add) and, with the index in
    /// the block cache unpinned, looks the index up once (BlockBasedTable::NewIterator -&gt; GetOrReadIndexBlock).
    /// </summary>
    [TestMethod]
    public void FlushOpensTheNewTableAndCreatesAnIteratorOverIt()
    {
        string path = Path.Combine(Path.GetTempPath(), "audit4-pinning-flush-" + Guid.NewGuid().ToString("N"));
        try
        {
            var stats = new Statistics();
            using RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true, Statistics = stats }, DbColumnFamilyOptions(false, true));
            for (int i = 0; i < 500; i++) db.Put(UserKey(i), Value(i));
            db.Flush();
            Assert.AreEqual(1UL, stats.GetTickerCount(Ticker.BlockCacheIndexMiss), "open");
            Assert.AreEqual(1UL, stats.GetTickerCount(Ticker.BlockCacheIndexAdd), "open");
            Assert.AreEqual(1UL, stats.GetTickerCount(Ticker.BlockCacheIndexHit), "the validation iterator");
            Assert.AreEqual(1UL, stats.GetTickerCount(Ticker.BlockCacheFilterMiss), "open");
            Assert.AreEqual(0UL, stats.GetTickerCount(Ticker.BlockCacheFilterHit), "an iterator does not probe the filter");
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, true);
        }
    }

    /// <summary>
    /// BlockBasedTable::FullFilterKeysMayMatch for a MultiGet batch: FullFilterBlockReader::MayMatch(MultiGetRange*)
    /// calls GetOrReadFilterBlock once for the whole batch, and PartitionedFilterBlockReader::MayMatch(MultiGetRange*)
    /// calls MayMatchPartition (GetFilterPartitionBlock) once per run of keys in the same partition; the index is
    /// looked up once per batch (one index iterator).
    /// </summary>
    [TestMethod]
    public void MultiGetLooksTheFilterUpOncePerBatchAndPartition()
    {
        foreach (bool partitioned in new[] { false, true })
        {
            string path = Path.Combine(Path.GetTempPath(), "audit4-pinning-multiget-" + Guid.NewGuid().ToString("N"));
            try
            {
                var stats = new Statistics();
                // Unpartitioned: index and filter in the block cache, unpinned. Partitioned: the defaults (top levels owned).
                using RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true, Statistics = stats },
                    DbColumnFamilyOptions(partitioned, cacheIndexAndFilter: !partitioned));
                for (int i = 0; i < Count; i++) db.Put(UserKey(i), Value(i));
                db.Flush();
                string what = partitioned ? "partitioned" : "unpartitioned";

                // Neighbouring keys: one filter (partition) lookup for the batch.
                var near = new List<byte[]>();
                for (int i = 0; i < 8; i++) near.Add(UserKey(1000 + i));
                ulong filterHit = stats.GetTickerCount(Ticker.BlockCacheFilterHit);
                ulong indexHit = stats.GetTickerCount(Ticker.BlockCacheIndexHit);
                byte[]?[] values = db.MultiGet(near);
                for (int i = 0; i < near.Count; i++) Assert.AreEqual(Encoding.ASCII.GetString(Value(1000 + i)), Encoding.ASCII.GetString(values[i]!), what);
                Assert.AreEqual(filterHit + 1, stats.GetTickerCount(Ticker.BlockCacheFilterHit), what + ": one filter lookup for eight keys");
                // One index iterator per batch; PartitionedIndexIterator::InitPartitionedIndexBlock keeps the partition
                // it has loaded while the keys stay in it.
                ulong indexLookups = stats.GetTickerCount(Ticker.BlockCacheIndexHit) - indexHit;
                Assert.IsTrue(indexLookups >= 1 && indexLookups <= 2, what + ": the index is looked up per batch (and per index partition), not per key: " + indexLookups);
                Assert.AreEqual(8UL, stats.GetTickerCount(Ticker.BloomFilterFullPositive), what + ": every key was probed");

                // Keys spread over the file: still one lookup of a full filter; one per partition touched otherwise.
                var spread = new List<byte[]>();
                for (int i = 0; i < Count; i += 100) spread.Add(UserKey(i));
                filterHit = stats.GetTickerCount(Ticker.BlockCacheFilterHit);
                values = db.MultiGet(spread);
                for (int i = 0; i < spread.Count; i++) Assert.IsNotNull(values[i], what);
                ulong filterLookups = stats.GetTickerCount(Ticker.BlockCacheFilterHit) - filterHit;
                if (partitioned) Assert.IsTrue(filterLookups > 1 && filterLookups < (ulong)spread.Count, "one lookup per filter partition touched: " + filterLookups);
                else Assert.AreEqual(1UL, filterLookups, "one lookup of the full filter for twenty keys");
                Assert.AreEqual(0UL, stats.GetTickerCount(Ticker.BlockCacheFilterMiss) - stats.GetTickerCount(Ticker.BlockCacheFilterAdd), what);
            }
            finally
            {
                if (Directory.Exists(path)) Directory.Delete(path, true);
            }
        }
    }

    /// <summary>
    /// VersionEditHandler::LoadTables(cfd, prefetch_index_and_filter_in_cache = false, is_initial_load = true): the
    /// tables DB::Open loads do not warm the block cache, except level-0 files (prefetch_all =
    /// prefetch_index_and_filter_in_cache || level == 0). Below level 0 an unpinned index and filter are first read by
    /// the first read, while pinned ones (prefetch_index = prefetch_all || pin_index) are loaded at open.
    /// </summary>
    [TestMethod]
    public void DatabaseOpenLoadsOnlyPinnedMetadataBlocksBelowLevelZero()
    {
        foreach ((bool pinned, bool levelZero) in new[] { (false, false), (true, false), (false, true), (true, true) })
        {
            string path = Path.Combine(Path.GetTempPath(), "audit4-pinning-reopen-" + Guid.NewGuid().ToString("N"));
            try
            {
                BlockCache cache = BlockCache.CreateLru(64L << 20);
                ColumnFamilyOptions cf = DbColumnFamilyOptions(false, true, cache);
                cf.DisableAutoCompactions = true;
                if (pinned) cf.TableOptions.MetadataCacheOptions.UnpartitionedPinning = PinningTier.All;
                using (RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true }, cf))
                {
                    for (int i = 0; i < 500; i++) db.Put(UserKey(i), Value(i));
                    db.Flush();
                    if (!levelZero) db.CompactRange(null, null);
                    Assert.AreEqual(levelZero ? "1" : "0", db.GetProperty("rocksdb.num-files-at-level0"));
                }
                cache.EraseUnreferencedEntries();
                Assert.AreEqual(0L, cache.Usage, "the closed DB left nothing pinned");

                var stats = new Statistics();
                string what = "pinned=" + pinned + " levelZero=" + levelZero;
                using (RocksDatabase db = RocksDatabase.Open(path, new DbOptions { Statistics = stats }, cf))
                {
                    ulong atOpen = pinned || levelZero ? 1UL : 0UL;
                    Assert.AreEqual(atOpen, stats.GetTickerCount(Ticker.BlockCacheIndexMiss), what + ": index miss at open");
                    Assert.AreEqual(atOpen, stats.GetTickerCount(Ticker.BlockCacheFilterMiss), what + ": filter miss at open");
                    Assert.AreEqual(pinned, cache.PinnedUsage > 0, what + ": pinned usage after open");
                    Assert.IsNotNull(db.Get(UserKey(3)), what);
                    Assert.AreEqual(1UL, stats.GetTickerCount(Ticker.BlockCacheIndexMiss), what + ": index miss after the first read");
                    Assert.AreEqual(1UL, stats.GetTickerCount(Ticker.BlockCacheIndexAdd), what);
                    Assert.AreEqual(1UL, stats.GetTickerCount(Ticker.BlockCacheFilterMiss), what + ": filter miss after the first read");
                    Assert.IsNotNull(db.Get(UserKey(4)), what);
                    Assert.AreEqual(pinned ? 0UL : levelZero ? 2UL : 1UL, stats.GetTickerCount(Ticker.BlockCacheIndexHit), what + ": second read");
                }
            }
            finally
            {
                if (Directory.Exists(path)) Directory.Delete(path, true);
            }
        }
    }
}
