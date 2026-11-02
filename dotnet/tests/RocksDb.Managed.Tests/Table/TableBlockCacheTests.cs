using RocksDb.Managed.Cache;
using RocksDb.Managed.Db;
using RocksDb.Managed.Table;
using RocksDb.Managed.Table.Filters;
using RocksDb.Managed.Util;
using static RocksDb.Managed.Tests.Table.TableTestUtil;

namespace RocksDb.Managed.Tests.Table;

/// <summary>
/// Block cache wiring of <see cref="BlockBasedTableReader"/> (BlockBasedTable::RetrieveBlock / GetCacheKey /
/// SetupBaseCacheKey; cf. table/block_based/block_based_table_reader_test.cc and db/db_block_cache_test.cc).
/// </summary>
[TestClass]
public sealed class TableBlockCacheTests
{
    private const int Count = 2000;

    private static BlockCache NewCache(string kind, long capacity, bool strict = false) => kind == "lru"
        ? BlockCache.CreateLru(new LruCacheOptions { Capacity = capacity, StrictCapacityLimit = strict, NumShardBits = 0 })
        : BlockCache.CreateHyperClock(new HyperClockCacheOptions { Capacity = capacity, EstimatedEntryCharge = 1024, StrictCapacityLimit = strict, NumShardBits = 0 });

    private static (List<(byte[] Key, byte[] Value)> Entries, byte[] Data) Table(int seed, FilterPolicy? policy = null, TableBuilderOptions? tbo = null)
    {
        List<(byte[] Key, byte[] Value)> entries = MakeEntries(Count, seed);
        return (entries, BuildTable(entries, new TableFormatOptions { BlockSize = 512, FilterPolicy = policy }, tbo));
    }

    /// <summary>Gets every <paramref name="step"/>-th key and checks the value.</summary>
    private static void GetAll(BlockBasedTableReader reader, List<(byte[] Key, byte[] Value)> entries, int step, TableReadFlags flags = TableReadFlags.None)
    {
        using var value = new PooledBufferWriter();
        for (int i = 0; i < entries.Count; i += step)
        {
            value.Clear();
            TableGetResult r = reader.Get(LookupKey(UserKeyOf(entries[i].Key)), value, flags);
            Assert.IsTrue(r.Found, "key " + i);
            CollectionAssert.AreEqual(entries[i].Value, value.WrittenSpan.ToArray(), "key " + i);
        }
        Assert.IsFalse(reader.Get(LookupKey("key00000001"), null, flags).Found);
    }

    private static void ScanAll(BlockBasedTableReader reader, List<(byte[] Key, byte[] Value)> entries)
    {
        using BlockBasedTableIterator it = reader.NewIterator();
        int n = 0;
        for (it.SeekToFirst(); it.Valid; it.Next(), n++)
        {
            CollectionAssert.AreEqual(entries[n].Key, it.Key.ToArray());
            CollectionAssert.AreEqual(entries[n].Value, it.Value.ToArray());
        }
        Assert.AreEqual(entries.Count, n);
    }

    [TestMethod]
    [DataRow("lru")]
    [DataRow("hyperclock")]
    public void HitsAndMissesAreCounted(string kind)
    {
        (List<(byte[] Key, byte[] Value)> entries, byte[] data) = Table(1);
        BlockCache cache = NewCache(kind, 64L << 20);
        var stats = new BlockCacheStatistics();
        var file = new MemRandomAccessFile(data);
        using var reader = BlockBasedTableReader.Open(file, new TableReaderOptions { BlockCache = cache, Statistics = stats });
        Assert.IsFalse(reader.IndexAndFilterInCache);

        GetAll(reader, entries, 1);
        long misses = stats.DataMisses;
        Assert.AreEqual((long)reader.Properties.NumDataBlocks, misses, "each data block is read exactly once");
        Assert.AreEqual(misses, stats.Get(BlockCacheCounter.DataAdd));
        Assert.AreEqual(misses, cache.Count);
        Assert.IsTrue(stats.Get(BlockCacheCounter.BytesInserted) > 0);
        Assert.AreEqual(0, cache.PinnedUsage, "no handle may leak");

        int reads = file.Reads;
        long hits = stats.DataHits;
        GetAll(reader, entries, 1);
        ScanAll(reader, entries);
        Assert.AreEqual(reads, file.Reads, "second pass is served from the cache");
        Assert.AreEqual(misses, stats.DataMisses);
        Assert.IsTrue(stats.DataHits > hits + Count);
        Assert.AreEqual(0, cache.PinnedUsage);
        Assert.AreEqual(0, stats.IndexMisses + stats.FilterMisses + stats.FilterHits);
    }

    [TestMethod]
    public void DontFillCacheLooksUpButDoesNotInsert()
    {
        (List<(byte[] Key, byte[] Value)> entries, byte[] data) = Table(2);
        BlockCache cache = NewCache("hyperclock", 64L << 20);
        var stats = new BlockCacheStatistics();
        using var reader = BlockBasedTableReader.Open(new MemRandomAccessFile(data), new TableReaderOptions { BlockCache = cache, Statistics = stats });
        GetAll(reader, entries, 7, TableReadFlags.DontFillCache);
        Assert.AreEqual(0, cache.Count);
        Assert.IsTrue(stats.DataMisses > 0);
        using (BlockBasedTableIterator it = reader.NewIterator(TableReadFlags.DontFillCache))
        {
            for (it.SeekToFirst(); it.Valid; it.Next()) { }
        }
        Assert.AreEqual(0, cache.Count);
        // Blocks cached by a filling read are found by a non-filling one.
        GetAll(reader, entries, 7);
        long misses = stats.DataMisses;
        GetAll(reader, entries, 7, TableReadFlags.DontFillCache);
        Assert.AreEqual(misses, stats.DataMisses);
    }

    [TestMethod]
    [DataRow("lru")]
    [DataRow("hyperclock")]
    public void IndexAndFilterBlocksInCache(string kind)
    {
        var policy = new ExactSetFilterPolicy();
        (List<(byte[] Key, byte[] Value)> entries, byte[] data) = Table(3, policy);
        BlockCache cache = NewCache(kind, 64L << 20);
        var stats = new BlockCacheStatistics();
        var options = new TableReaderOptions { BlockCache = cache, Statistics = stats, FilterPolicy = policy, CacheIndexAndFilterBlocks = true };
        using var reader = BlockBasedTableReader.Open(new MemRandomAccessFile(data), options);
        Assert.IsTrue(reader.IndexAndFilterInCache);
        Assert.IsTrue(reader.HasFilter);
        Assert.AreEqual(2, cache.Count, "index and filter prefetched into the cache at open");
        Assert.AreEqual(1, stats.Get(BlockCacheCounter.IndexAdd));
        Assert.AreEqual(1, stats.Get(BlockCacheCounter.FilterAdd));

        GetAll(reader, entries, 3);
        Assert.AreEqual(0, stats.IndexMisses);
        Assert.AreEqual(0, stats.FilterMisses);
        Assert.IsTrue(stats.IndexHits > 0);
        Assert.IsTrue(stats.FilterHits > 0);
        // Filtered out: the filter answers without touching the index.
        long indexHits = stats.IndexHits;
        Assert.IsFalse(reader.Get(LookupKey("absent"), null).Found);
        Assert.AreEqual(indexHits, stats.IndexHits);

        // Everything evicted: index and filter are re-read on demand and re-inserted.
        cache.EraseUnreferencedEntries();
        Assert.AreEqual(0, cache.Count);
        GetAll(reader, entries, 5);
        ScanAll(reader, entries);
        Assert.AreEqual(1, stats.IndexMisses);
        Assert.AreEqual(1, stats.FilterMisses);
        Assert.AreEqual(0, cache.PinnedUsage);

        // Without a native cache the option is ignored (the reader owns index and filter).
        using var plain = BlockBasedTableReader.Open(new MemRandomAccessFile(data), options with { BlockCache = NoBlockCache.Instance });
        Assert.IsFalse(plain.IndexAndFilterInCache);
        GetAll(plain, entries, 11);
    }

    [TestMethod]
    public void HighPriorityForIndexAndFilter()
    {
        var policy = new ExactSetFilterPolicy();
        (List<(byte[] Key, byte[] Value)> entries, byte[] data) = Table(4, policy);
        // LRU with a high-priority pool: a long data-block scan must not evict the index/filter.
        BlockCache cache = BlockCache.CreateLru(new LruCacheOptions { Capacity = 128 << 10, NumShardBits = 0, HighPriPoolRatio = 0.5 });
        var stats = new BlockCacheStatistics();
        using var reader = BlockBasedTableReader.Open(new MemRandomAccessFile(data),
            new TableReaderOptions { BlockCache = cache, Statistics = stats, FilterPolicy = policy, CacheIndexAndFilterBlocks = true });
        for (int pass = 0; pass < 3; pass++) ScanAll(reader, entries);
        Assert.IsTrue(stats.DataMisses > (long)reader.Properties.NumDataBlocks, "the cache is smaller than the data");
        GetAll(reader, entries, 13);
        Assert.AreEqual(0, stats.IndexMisses);
        Assert.AreEqual(0, stats.FilterMisses);
    }

    [TestMethod]
    [DataRow("lru")]
    [DataRow("hyperclock")]
    public void EvictionKeepsResultsCorrect(string kind)
    {
        (List<(byte[] Key, byte[] Value)> entries, byte[] data) = Table(5);
        BlockCache cache = NewCache(kind, 8 << 10);
        var stats = new BlockCacheStatistics();
        using var reader = BlockBasedTableReader.Open(new MemRandomAccessFile(data), new TableReaderOptions { BlockCache = cache, Statistics = stats });
        for (int pass = 0; pass < 3; pass++)
        {
            GetAll(reader, entries, 1);
            ScanAll(reader, entries);
        }
        Assert.IsTrue(stats.DataMisses > 2 * (long)reader.Properties.NumDataBlocks, "blocks must have been evicted and re-read");
        Assert.IsTrue(cache.Usage <= cache.Capacity + (2 << 10), "usage " + cache.Usage);
        Assert.AreEqual(0, cache.PinnedUsage);
    }

    [TestMethod]
    [DataRow("lru")]
    [DataRow("hyperclock")]
    public void StrictCapacityFallsBackToUncachedBlocks(string kind)
    {
        (List<(byte[] Key, byte[] Value)> entries, byte[] data) = Table(6);
        BlockCache cache = NewCache(kind, 1, strict: true);
        var stats = new BlockCacheStatistics();
        using var reader = BlockBasedTableReader.Open(new MemRandomAccessFile(data), new TableReaderOptions { BlockCache = cache, Statistics = stats });
        GetAll(reader, entries, 9);
        ScanAll(reader, entries);
        Assert.IsTrue(stats.Get(BlockCacheCounter.AddFailure) > 0);
        Assert.AreEqual(0, cache.Count);
    }

    [TestMethod]
    public void FilesWithoutIdentityNeverShareKeys()
    {
        // Two different tables with identical (empty) identity properties sharing one cache.
        (List<(byte[] Key, byte[] Value)> e1, byte[] d1) = Table(7);
        (List<(byte[] Key, byte[] Value)> e2, byte[] d2) = Table(8);
        BlockCache cache = NewCache("hyperclock", 64L << 20);
        using var r1 = BlockBasedTableReader.Open(new MemRandomAccessFile(d1), new TableReaderOptions { BlockCache = cache });
        using var r2 = BlockBasedTableReader.Open(new MemRandomAccessFile(d2), new TableReaderOptions { BlockCache = cache });
        Assert.AreNotEqual(r1.BaseCacheKey.CommonPrefix, r2.BaseCacheKey.CommonPrefix);
        for (int i = 0; i < 2; i++)
        {
            GetAll(r1, e1, 3);
            GetAll(r2, e2, 3);
        }
        // Same current session id but different file numbers: distinct; same file number: same key.
        TableProperties none = r1.Properties;
        Assert.AreNotEqual(
            BlockBasedTableReader.SetupBaseCacheKey(none, "ABCDEFGHIJ0123456789", 5).CommonPrefix,
            BlockBasedTableReader.SetupBaseCacheKey(none, "ABCDEFGHIJ0123456789", 6).CommonPrefix);
        Assert.AreEqual(
            BlockBasedTableReader.SetupBaseCacheKey(none, "ABCDEFGHIJ0123456789", 5).WithOffset(0),
            BlockBasedTableReader.SetupBaseCacheKey(none, "ABCDEFGHIJ0123456789", 5).WithOffset(0));
    }

    [TestMethod]
    public void StableKeysFromPropertiesAreSharedAcrossReopen()
    {
        var tbo = new TableBuilderOptions { DbId = "db-id-1", DbSessionId = "SESSION0123456789ABC", FileNumber = 42 };
        (List<(byte[] Key, byte[] Value)> entries, byte[] data) = Table(9, null, tbo);
        BlockCache cache = NewCache("hyperclock", 64L << 20);
        var stats = new BlockCacheStatistics();
        var options = new TableReaderOptions { BlockCache = cache, Statistics = stats, DbSessionId = "OTHERSESSION00000000", FileNumber = 7 };
        using (var r = BlockBasedTableReader.Open(new MemRandomAccessFile(data), options))
        {
            Assert.AreEqual(new OffsetableCacheKey("db-id-1", "SESSION0123456789ABC", 42).CommonPrefix, r.BaseCacheKey.CommonPrefix);
            GetAll(r, entries, 1);
        }
        long misses = stats.DataMisses;
        var file = new MemRandomAccessFile(data);
        using (var r = BlockBasedTableReader.Open(file, options))
        {
            int reads = file.Reads;
            GetAll(r, entries, 1);
            Assert.AreEqual(reads, file.Reads, "a reopened reader of the same file hits the entries of the first");
        }
        Assert.AreEqual(misses, stats.DataMisses);
    }

    [TestMethod]
    [DataRow("lru", 64 << 20)]
    [DataRow("hyperclock", 64 << 20)]
    [DataRow("lru", 16 << 10)]
    [DataRow("hyperclock", 16 << 10)]
    public void ConcurrentGetsAndScansOnOneReader(string kind, int capacity)
    {
        var policy = new ExactSetFilterPolicy();
        (List<(byte[] Key, byte[] Value)> entries, byte[] data) = Table(10, policy);
        BlockCache cache = NewCache(kind, capacity);
        var stats = new BlockCacheStatistics();
        using var reader = BlockBasedTableReader.Open(new MemRandomAccessFile(data),
            new TableReaderOptions { BlockCache = cache, Statistics = stats, FilterPolicy = policy, CacheIndexAndFilterBlocks = capacity > 1 << 20 });
        const int Threads = 6;
        var errors = new System.Collections.Concurrent.ConcurrentQueue<Exception>();
        var threads = new Thread[Threads];
        for (int t = 0; t < Threads; t++)
        {
            int id = t;
            threads[t] = new Thread(() =>
            {
                try
                {
                    var rnd = new Random(id);
                    using var value = new PooledBufferWriter();
                    for (int op = 0; op < 3000; op++)
                    {
                        int i = rnd.Next(entries.Count);
                        if (op % 500 == 499)
                        {
                            using BlockBasedTableIterator it = reader.NewIterator();
                            it.Seek(entries[i].Key);
                            for (int n = i; n < Math.Min(i + 50, entries.Count); n++, it.Next())
                            {
                                if (!it.Valid || !it.Key.SequenceEqual(entries[n].Key) || !it.Value.SequenceEqual(entries[n].Value))
                                    throw new InvalidOperationException("scan mismatch at " + n);
                            }
                            continue;
                        }
                        value.Clear();
                        TableGetResult r = reader.Get(LookupKey(UserKeyOf(entries[i].Key)), value);
                        if (!r.Found || !value.WrittenSpan.SequenceEqual(entries[i].Value)) throw new InvalidOperationException("get mismatch at " + i);
                    }
                }
                catch (Exception ex)
                {
                    errors.Enqueue(ex);
                }
            });
            threads[t].Start();
        }
        foreach (Thread th in threads) th.Join();
        Assert.IsTrue(errors.IsEmpty, errors.TryPeek(out Exception? e) ? e.ToString() : "");
        Assert.IsTrue(stats.DataHits + stats.DataMisses >= Threads * (3000 - 6), "every Get touches a data block");
        Assert.AreEqual(0, cache.PinnedUsage);
    }

    [TestMethod]
    public async Task AsyncGetsUseTheCache()
    {
        (List<(byte[] Key, byte[] Value)> entries, byte[] data) = Table(11);
        BlockCache cache = NewCache("hyperclock", 64L << 20);
        var stats = new BlockCacheStatistics();
        using var reader = await BlockBasedTableReader.OpenAsync(new MemRandomAccessFile(data), new TableReaderOptions { BlockCache = cache, Statistics = stats });
        using var value = new PooledBufferWriter();
        for (int pass = 0; pass < 2; pass++)
        {
            for (int i = 0; i < entries.Count; i += 17)
            {
                value.Clear();
                TableGetResult r = await reader.GetAsync(LookupKey(UserKeyOf(entries[i].Key)), value);
                Assert.IsTrue(r.Found);
                CollectionAssert.AreEqual(entries[i].Value, value.WrittenSpan.ToArray());
            }
        }
        Assert.IsTrue(stats.DataHits > 0);
        long misses = stats.DataMisses;
        using BlockBasedTableIterator it = reader.NewIterator();
        int n = 0;
        await it.SeekToFirstAsync();
        while (it.Valid)
        {
            n++;
            await it.NextAsync();
        }
        Assert.AreEqual(entries.Count, n);
        Assert.AreEqual((long)reader.Properties.NumDataBlocks, stats.DataMisses, "the scan reads each remaining block once");
        Assert.IsTrue(stats.DataMisses >= misses);
        Assert.AreEqual(0, cache.PinnedUsage);
    }

    [TestMethod]
    public void BlockCacheAlsoWorksThroughTheLegacySeam()
    {
        BlockCache cache = NewCache("lru", 1 << 20);
        IBlockCache seam = cache;
        Block b = Block.CreateCopy(new byte[] { 0, 0, 0, 0, 1, 0, 0, 0 }); // empty block: one restart at 0
        var key = new BlockCacheKey(1, 100);
        Assert.IsFalse(seam.TryGet(key, out _));
        seam.Insert(key, b);
        Assert.IsTrue(seam.TryGet(key, out Block? got));
        Assert.AreSame(b, got);
        got!.Release();
        b.Release();
        cache.EraseUnreferencedEntries(); // drops the cache's reference
        Assert.AreEqual(0, cache.Count);
    }
}
