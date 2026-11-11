using System.Buffers;
using System.Globalization;
using System.Text;
using RocksDb.Managed.Cache;
using RocksDb.Managed.Db;
using RocksDb.Managed.Monitoring;
using RocksDb.Managed.Table;
using RocksDb.Managed.Tests.Table;
using CompressionType = RocksDb.Managed.Compression.CompressionType;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4: the compression dictionary block in the block cache (table/block_based/uncompression_dict_reader.cc and
/// its call sites in block_based_table_reader.cc). Each test names the C++ function it pins.
/// </summary>
[TestClass]
public sealed class Audit4CompressionDictCacheTests
{
    private const int Count = 400;

    private static byte[] UserKey(int i) => Encoding.ASCII.GetBytes("key" + i.ToString("D5", CultureInfo.InvariantCulture));

    // JSON-like values sharing structure across blocks, so that the dictionary is used.
    private static byte[] Value(int i) => Encoding.ASCII.GetBytes(
        "{\"id\":" + i + ",\"name\":\"customer-" + (i % 97) + "\",\"email\":\"user" + (i % 89) + "@example.com\",\"country\":\""
        + (i % 3 == 0 ? "Portugal" : i % 3 == 1 ? "Germany" : "Brazil") + "\",\"active\":" + (i % 2 == 0 ? "true" : "false") + "}");

    private static byte[] BuildDictionaryTable()
    {
        var entries = new List<(byte[], byte[])>(Count);
        for (int i = 0; i < Count; i++) entries.Add((TableTestUtil.IKey(UserKey(i), (ulong)(100 + i)), Value(i)));
        return TableTestUtil.BuildTable(entries, new TableFormatOptions { BlockSize = 512 },
            new TableBuilderOptions { CompressionType = CompressionType.Zstd, CompressionOptions = new CompressionOptions { MaxDictBytes = 4096 } });
    }

    /// <summary>A table reader over a dictionary-compressed table with its block cache and statistics.</summary>
    private sealed class Fixture : IDisposable
    {
        public readonly byte[] Data;
        public readonly BlockCache Cache = BlockCache.CreateLru(64L << 20);
        public readonly Statistics Stats = new();
        public BlockBasedTableReader Reader = null!;

        public Fixture(byte[]? data = null) => Data = data ?? BuildDictionaryTable();

        public TableReaderOptions Options(bool cacheIndexAndFilter) => new()
        {
            BlockCache = Cache,
            CacheIndexAndFilterBlocks = cacheIndexAndFilter,
            Statistics = Stats.BlockCacheCounters,
            // A stable base cache key, so that two readers of the same file share their entries.
            DbSessionId = "AUDIT4DICTCACHE00001",
            FileNumber = 12,
        };

        public Fixture Open(TableReaderOptions options)
        {
            Reader = TableTestUtil.Open(Data, options);
            return this;
        }

        public ulong Miss => Stats.GetTickerCount(Ticker.BlockCacheCompressionDictMiss);
        public ulong Hit => Stats.GetTickerCount(Ticker.BlockCacheCompressionDictHit);
        public ulong Add => Stats.GetTickerCount(Ticker.BlockCacheCompressionDictAdd);
        public ulong BytesInsert => Stats.GetTickerCount(Ticker.BlockCacheCompressionDictBytesInsert);

        public void AssertTickers(ulong miss, ulong hit, ulong add, string what)
        {
            Assert.AreEqual(miss, Miss, what + ": BLOCK_CACHE_COMPRESSION_DICT_MISS");
            Assert.AreEqual(hit, Hit, what + ": BLOCK_CACHE_COMPRESSION_DICT_HIT");
            Assert.AreEqual(add, Add, what + ": BLOCK_CACHE_COMPRESSION_DICT_ADD");
        }

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

        public int Scan(TableReadFlags flags = TableReadFlags.None)
        {
            using BlockBasedTableIterator it = Reader.NewIterator(flags);
            int n = 0;
            for (it.SeekToFirst(); it.Valid; it.Next())
            {
                Assert.AreEqual(Encoding.ASCII.GetString(Value(n)), Encoding.ASCII.GetString(it.Value), "scan value " + n);
                n++;
            }
            return n;
        }

        public void Dispose()
        {
            Reader?.Dispose();
        }
    }

    /// <summary>The size of the table's dictionary (read with a reader that owns it, so that nothing is recorded).</summary>
    private static int DictionaryLength(byte[] data)
    {
        using BlockBasedTableReader r = TableTestUtil.Open(data);
        Assert.IsFalse(r.CompressionDictionary.IsEmpty, "the table has a compression dictionary");
        return r.CompressionDictionary.Length;
    }

    /// <summary>The offset of the first byte of the dictionary block in the file.</summary>
    private static int DictionaryOffset(byte[] data)
    {
        using BlockBasedTableReader r = TableTestUtil.Open(data);
        return checked((int)r.MetaBlocks[BlockBasedTableConstants.CompressionDictBlockName].Offset);
    }

    /// <summary>
    /// UncompressionDictReader::Create with use_cache = false (cache_index_and_filter_blocks off): the reader owns the
    /// dictionary, nothing is looked up or inserted, and UncompressionDictReader::ApproximateMemoryUsage counts it.
    /// </summary>
    [TestMethod]
    public void OwnedDictionaryRecordsNoTickersAndIsCountedInTableMemory()
    {
        using var cached = new Fixture();
        cached.Open(cached.Options(false));
        long usageAfterOpen = cached.Cache.Usage;
        for (int i = 0; i < Count; i += 7) cached.AssertGet(i);
        Assert.AreEqual(Count, cached.Scan());
        cached.AssertTickers(0, 0, 0, "owned");
        Assert.AreEqual(0UL, cached.BytesInsert);
        Assert.IsTrue(cached.Reader.ApproximateMemoryUsage >= (ulong)cached.Reader.CompressionDictionary.Length, "the owned dictionary is table reader memory");
        Assert.IsTrue(cached.Cache.Usage > usageAfterOpen, "data blocks still go to the block cache");
    }

    /// <summary>
    /// UncompressionDictReader::Create with use_cache, prefetch and no pin: one miss + add at open
    /// (BlockBasedTable::UpdateCacheMissMetrics / UpdateCacheInsertionMetrics), the entry is released, and
    /// GetOrReadUncompressionDictionary then looks it up for every data block retrieval (NewDataBlockIterator), whether
    /// the data block is cached or not (UpdateCacheHitMetrics). The totals BLOCK_CACHE_{MISS,HIT,ADD} include it.
    /// </summary>
    [TestMethod]
    public async Task UnpinnedDictionaryIsLookedUpForEveryDataBlockRetrieval()
    {
        using var f = new Fixture();
        f.Open(f.Options(true));
        f.AssertTickers(1, 0, 1, "open");
        int dictLength = DictionaryLength(f.Data);
        Assert.IsTrue(f.BytesInsert >= (ulong)dictLength, "bytes insert covers the dictionary");
        ulong afterProperty = f.Hit;
        Assert.AreEqual(0UL, afterProperty);

        f.AssertGet(10);
        Assert.AreEqual(afterProperty + 1, f.Hit, "first Get: data block miss, dictionary hit");
        f.AssertGet(10);
        Assert.AreEqual(afterProperty + 2, f.Hit, "second Get: data block hit, dictionary hit all the same");
        await f.AssertGetAsync(10);
        Assert.AreEqual(afterProperty + 3, f.Hit, "async Get");

        ulong before = f.Hit;
        ulong dataLookups = f.Stats.GetTickerCount(Ticker.BlockCacheDataHit) + f.Stats.GetTickerCount(Ticker.BlockCacheDataMiss);
        Assert.AreEqual(Count, f.Scan());
        dataLookups = f.Stats.GetTickerCount(Ticker.BlockCacheDataHit) + f.Stats.GetTickerCount(Ticker.BlockCacheDataMiss) - dataLookups;
        Assert.IsTrue(dataLookups > 20, "the table has many data blocks");
        Assert.AreEqual(before + dataLookups, f.Hit, "a scan looks the dictionary up once per data block");
        f.AssertTickers(1, f.Hit, 1, "no further miss or add");

        // The totals count the dictionary lookups like any other block.
        ulong totalHit = f.Stats.GetTickerCount(Ticker.BlockCacheHit);
        ulong others = f.Stats.GetTickerCount(Ticker.BlockCacheDataHit) + f.Stats.GetTickerCount(Ticker.BlockCacheIndexHit)
            + f.Stats.GetTickerCount(Ticker.BlockCacheFilterHit);
        Assert.AreEqual(others + f.Hit, totalHit, "BLOCK_CACHE_HIT");
        Assert.AreEqual(f.Stats.GetTickerCount(Ticker.BlockCacheDataMiss) + f.Stats.GetTickerCount(Ticker.BlockCacheIndexMiss)
            + f.Stats.GetTickerCount(Ticker.BlockCacheFilterMiss) + 1, f.Stats.GetTickerCount(Ticker.BlockCacheMiss), "BLOCK_CACHE_MISS");
        Assert.AreEqual(f.Stats.GetTickerCount(Ticker.BlockCacheDataAdd) + f.Stats.GetTickerCount(Ticker.BlockCacheIndexAdd)
            + f.Stats.GetTickerCount(Ticker.BlockCacheFilterAdd) + 1, f.Stats.GetTickerCount(Ticker.BlockCacheAdd), "BLOCK_CACHE_ADD");
        // UncompressionDictReader::ApproximateMemoryUsage: a dictionary in the block cache is not table reader memory.
        Assert.IsTrue(f.Reader.ApproximateMemoryUsage < (ulong)dictLength, "not counted as table reader memory");

        // The dictionary dump of BlockBasedTable::DumpTable goes through GetOrReadUncompressionDictionary too.
        ulong beforeDump = f.Hit;
        Assert.AreEqual(dictLength, f.Reader.CompressionDictionary.Length);
        Assert.AreEqual(beforeDump + 1, f.Hit, "CompressionDictionary is a lookup");
    }

    /// <summary>
    /// UncompressionDictReader::Create with pin: the entry stays referenced by the reader, so
    /// GetOrReadUncompressionDictionary returns it without a lookup (no hit ticker per read), and it is charged to the
    /// cache and survives eviction of everything unreferenced.
    /// </summary>
    [TestMethod]
    public void PinnedDictionaryIsChargedToTheCacheAndNeverLookedUpAgain()
    {
        using var f = new Fixture();
        f.Open(f.Options(true) with { UnpartitionedPinning = PinningTier.All });
        f.AssertTickers(1, 0, 1, "open");
        Assert.IsTrue(f.Cache.PinnedUsage >= f.Reader.CompressionDictionary.Length, "the pinned dictionary is charged to the cache");
        Assert.IsTrue((ulong)f.Cache.PinnedUsage >= f.BytesInsert, "pinned usage covers the inserted charge");
        for (int i = 0; i < Count; i += 5) f.AssertGet(i);
        Assert.AreEqual(Count, f.Scan());
        f.Cache.EraseUnreferencedEntries();
        Assert.IsTrue(f.Cache.PinnedUsage >= f.Reader.CompressionDictionary.Length, "still pinned after eviction");
        for (int i = 0; i < Count; i += 5) f.AssertGet(i);
        f.AssertTickers(1, 0, 1, "pinned: no lookups after open");

        // A second reader of the same file finds the entry (hit) instead of reading it.
        using BlockBasedTableReader second = TableTestUtil.Open(f.Data, f.Options(true) with { UnpartitionedPinning = PinningTier.All });
        f.AssertTickers(1, 1, 1, "second open");

        f.Reader.Dispose();
        second.Dispose();
        f.Cache.EraseUnreferencedEntries();
        Assert.AreEqual(0L, f.Cache.PinnedUsage, "closing the readers releases the pin");
        f.Reader = TableTestUtil.Open(f.Data, f.Options(true));
        f.AssertTickers(2, 1, 2, "reopen after eviction");
    }

    /// <summary>
    /// pin_unpartitioned in BlockBasedTable::PrefetchIndexAndFilterBlocks: unpartitioned_pinning kAll / kNone /
    /// kFlushedAndSimilar, and kFallback following pin_l0_filter_and_index_blocks_in_cache; kFlushedAndSimilar means
    /// level 0 and file_size &lt;= max_file_size_for_l0_meta_pin (maybe_flushed).
    /// </summary>
    [TestMethod]
    public void PinningFollowsUnpartitionedPinningAndTheL0Fallback()
    {
        var cases = new (PinningTier Tier, bool PinL0, int Level, long MaxSize, bool Pinned)[]
        {
            (PinningTier.All, false, 3, 0, true),
            (PinningTier.None, true, 0, long.MaxValue, false),
            (PinningTier.FlushedAndSimilar, false, 0, 1000, true),
            (PinningTier.FlushedAndSimilar, false, 0, 999, false),
            (PinningTier.FlushedAndSimilar, false, 1, 1000, false),
            (PinningTier.FlushedAndSimilar, false, -1, 1000, false),
            (PinningTier.Fallback, true, 0, 1000, true),
            (PinningTier.Fallback, true, 0, 999, false),
            (PinningTier.Fallback, true, 2, 1000, false),
            (PinningTier.Fallback, false, 0, 1000, false),
        };
        foreach (var c in cases)
        {
            var o = new TableReaderOptions
            {
                UnpartitionedPinning = c.Tier, PinL0FilterAndIndexBlocksInCache = c.PinL0, Level = c.Level, MaxFileSizeForL0MetaPin = c.MaxSize,
            };
            Assert.AreEqual(c.Pinned, BlockBasedTableReader.PinUnpartitioned(o, 1000), c.ToString());
        }

        // End to end: a level-0 file under pin_l0_filter_and_index_blocks_in_cache is pinned, the same file at level 1 is not.
        byte[] data = BuildDictionaryTable();
        foreach (int level in new[] { 0, 1 })
        {
            using var f = new Fixture(data);
            f.Open(f.Options(true) with { PinL0FilterAndIndexBlocksInCache = true, Level = level, MaxFileSizeForL0MetaPin = data.Length });
            ulong before = f.Hit;
            f.AssertGet(3);
            Assert.AreEqual(level == 0 ? before : before + 1, f.Hit, "level " + level);
        }
    }

    /// <summary>
    /// UncompressionDictReader::GetOrReadUncompressionDictionary after the entry was evicted: ReadUncompressionDictionary
    /// misses, re-reads the block (BlockBasedTable::RetrieveBlock) and inserts it again.
    /// </summary>
    [TestMethod]
    public async Task EvictedDictionaryIsReadAndInsertedAgain()
    {
        using var f = new Fixture();
        f.Open(f.Options(true));
        ulong bytesAtOpen = f.BytesInsert;
        f.Cache.EraseUnreferencedEntries();
        f.AssertGet(42);
        f.AssertTickers(2, 0, 2, "sync re-read");
        Assert.AreEqual(2 * bytesAtOpen, f.BytesInsert, "the same charge is inserted again");
        f.AssertGet(43);
        Assert.AreEqual(2UL, f.Miss);

        f.Cache.EraseUnreferencedEntries();
        await f.AssertGetAsync(200);
        Assert.AreEqual(3UL, f.Miss, "async re-read");
        Assert.AreEqual(3UL, f.Add);

        f.Cache.EraseUnreferencedEntries();
        Assert.AreEqual(Count, f.Scan());
        Assert.AreEqual(4UL, f.Miss, "one re-read for the whole scan");
        Assert.AreEqual(4UL, f.Add);
    }

    /// <summary>
    /// BlockBasedTable::RetrieveBlock with read_tier = kBlockCacheTier for the dictionary block: a dictionary that is not
    /// cached makes the read Incomplete ("no blocking io") even when the data block is cached, records the miss and
    /// inserts nothing; a cached dictionary and data block serve the read.
    /// </summary>
    [TestMethod]
    public async Task BlockCacheTierReadNeedsTheDictionaryInTheCache()
    {
        using var f = new Fixture();
        f.Open(f.Options(true));
        f.AssertGet(7);                                      // caches the data block of key 7
        f.AssertGet(7, TableReadFlags.BlockCacheTier);       // dictionary and data block cached
        ulong hits = f.Hit;

        // Drop only the dictionary entry: the data block stays cached.
        ulong dictOffset = f.Reader.MetaBlocks[BlockBasedTableConstants.CompressionDictBlockName].Offset;
        Assert.IsTrue(f.Cache.Erase(f.Reader.BaseCacheKey.WithOffsetForMinSizeRecord(dictOffset)), "the dictionary entry was cached");

        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => f.AssertGet(7, TableReadFlags.BlockCacheTier));
        Assert.AreEqual(StatusCode.Incomplete, ex.Code);
        ex = await Assert.ThrowsExactlyAsync<RocksDbException>(() => f.AssertGetAsync(7, TableReadFlags.BlockCacheTier));
        Assert.AreEqual(StatusCode.Incomplete, ex.Code);
        ex = Assert.ThrowsExactly<RocksDbException>(() => f.Scan(TableReadFlags.BlockCacheTier));
        Assert.AreEqual(StatusCode.Incomplete, ex.Code);
        f.AssertTickers(4, hits, 1, "three no-io misses, nothing inserted");

        f.AssertGet(7);                                      // I/O allowed: re-read and inserted
        f.AssertTickers(5, hits, 2, "re-read");
        f.AssertGet(7, TableReadFlags.BlockCacheTier);
        f.AssertTickers(5, hits + 1, 2, "served from the cache again");
    }

    /// <summary>
    /// BlockBasedTable::MaybeReadBlockAndLoadToCache / RetrieveBlock with fill_cache = false for the dictionary block: a
    /// miss reads the block for this read only and does not insert it, so the next read misses again.
    /// </summary>
    [TestMethod]
    public void FillCacheFalseReadsTheDictionaryWithoutInsertingIt()
    {
        using var f = new Fixture();
        f.Open(f.Options(true));
        f.Cache.EraseUnreferencedEntries();
        long usage = f.Cache.Usage;
        f.AssertGet(5, TableReadFlags.DontFillCache);
        f.AssertTickers(2, 0, 1, "first fill_cache=false read");
        f.AssertGet(5, TableReadFlags.DontFillCache);
        f.AssertTickers(3, 0, 1, "second fill_cache=false read");
        Assert.AreEqual(usage, f.Cache.Usage, "nothing was inserted");
        f.AssertGet(5);
        f.AssertTickers(4, 0, 2, "a filling read inserts it");
        f.AssertGet(5, TableReadFlags.DontFillCache);
        f.AssertTickers(4, 1, 2, "fill_cache=false still uses a cached dictionary");
    }

    /// <summary>
    /// UncompressionDictReader::Create returning the status of ReadUncompressionDictionary: a dictionary block with a
    /// bad checksum fails BlockBasedTable::Open (PrefetchIndexAndFilterBlocks) in both modes; and
    /// GetOrReadUncompressionDictionary propagates the same error to a read that has to re-read it.
    /// </summary>
    [TestMethod]
    public void CorruptDictionaryBlockFailsTheOpenAndTheReRead()
    {
        byte[] good = BuildDictionaryTable();
        int offset = DictionaryOffset(good);
        foreach (bool cacheMeta in new[] { false, true })
        {
            byte[] bad = (byte[])good.Clone();
            bad[offset + 3] ^= 0x40;
            using var f = new Fixture(bad);
            RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => TableTestUtil.Open(bad, f.Options(cacheMeta)));
            Assert.AreEqual(StatusCode.Corruption, ex.Code, "open, cache_index_and_filter_blocks=" + cacheMeta);
            Assert.AreEqual(cacheMeta ? 1UL : 0UL, f.Miss);
            Assert.AreEqual(0UL, f.Add);
        }

        // The file is damaged after the reader was opened and the cached dictionary evicted.
        byte[] data = (byte[])good.Clone();
        using var g = new Fixture(data);
        g.Open(g.Options(true));
        g.AssertGet(9);
        g.Cache.EraseUnreferencedEntries();
        data[offset + 3] ^= 0x40;
        RocksDbException reread = Assert.ThrowsExactly<RocksDbException>(() => g.AssertGet(9));
        Assert.AreEqual(StatusCode.Corruption, reread.Code);
        reread = Assert.ThrowsExactly<RocksDbException>(() => g.Scan());
        Assert.AreEqual(StatusCode.Corruption, reread.Code);
        g.AssertTickers(3, 1, 1, "failed re-reads insert nothing");
        data[offset + 3] ^= 0x40;
        g.AssertGet(9);
        g.AssertTickers(4, 1, 2, "readable again");
    }

    private static (RocksDatabase Db, Statistics Stats) OpenDb(string path, bool cacheMeta, bool pinL0)
    {
        var stats = new Statistics();
        var cf = new ColumnFamilyOptions { Compression = CompressionType.Zstd, DisableAutoCompactions = true };
        cf.CompressionOpts.MaxDictBytes = 4096;
        cf.TableOptions.BlockSize = 512;
        cf.TableOptions.CacheIndexAndFilterBlocks = cacheMeta;
        cf.TableOptions.PinL0FilterAndIndexBlocksInCache = pinL0;
        return (RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true, Statistics = stats }, cf), stats);
    }

    /// <summary>
    /// BlockBasedTable::MultiGet (dict / dict_inited): the dictionary is looked up once per file for the whole batch,
    /// not once per data block; with pin_l0_filter_and_index_blocks_in_cache a flushed L0 file pins it
    /// (PrefetchIndexAndFilterBlocks, pin_unpartitioned) and neither Get nor MultiGet looks it up; without
    /// cache_index_and_filter_blocks nothing is recorded. Options reach the reader through the table factory.
    /// </summary>
    [TestMethod]
    public void DatabaseReadsLookTheDictionaryUpOncePerMultiGetBatchUnlessPinned()
    {
        foreach ((bool cacheMeta, bool pinL0) in new[] { (true, false), (true, true), (false, false) })
        {
            string path = Path.Combine(Path.GetTempPath(), "audit4-dict-cache-" + Guid.NewGuid().ToString("N"));
            try
            {
                (RocksDatabase db, Statistics stats) = OpenDb(path, cacheMeta, pinL0);
                using (db)
                {
                    for (int i = 0; i < Count; i++) db.Put(UserKey(i), Value(i));
                    db.Flush();
                    string what = "cacheMeta=" + cacheMeta + " pinL0=" + pinL0;
                    Assert.IsNotNull(db.Get(UserKey(0)), what); // the table is open
                    ulong hit0 = stats.GetTickerCount(Ticker.BlockCacheCompressionDictHit);
                    ulong miss0 = stats.GetTickerCount(Ticker.BlockCacheCompressionDictMiss);
                    Assert.AreEqual(cacheMeta ? 1UL : 0UL, stats.GetTickerCount(Ticker.BlockCacheCompressionDictAdd), what + ": add");

                    Assert.AreEqual(Encoding.ASCII.GetString(Value(11)), Encoding.ASCII.GetString(db.Get(UserKey(11))!), what);
                    ulong hit1 = stats.GetTickerCount(Ticker.BlockCacheCompressionDictHit);
                    Assert.AreEqual(cacheMeta && !pinL0 ? hit0 + 1 : hit0, hit1, what + ": Get");

                    var keys = new List<byte[]>();
                    for (int i = 0; i < Count; i += 40) keys.Add(UserKey(i)); // ten keys in ten different data blocks
                    byte[]?[] values = db.MultiGet(keys);
                    for (int i = 0; i < keys.Count; i++) Assert.AreEqual(Encoding.ASCII.GetString(Value(i * 40)), Encoding.ASCII.GetString(values[i]!), what);
                    Assert.AreEqual(cacheMeta && !pinL0 ? hit1 + 1 : hit1, stats.GetTickerCount(Ticker.BlockCacheCompressionDictHit), what + ": MultiGet");
                    Assert.AreEqual(miss0, stats.GetTickerCount(Ticker.BlockCacheCompressionDictMiss), what + ": no miss after the flush");
                }
            }
            finally
            {
                if (Directory.Exists(path)) Directory.Delete(path, true);
            }
        }
    }
}
