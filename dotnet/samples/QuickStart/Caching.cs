using RocksDb.Managed;
using RocksDb.Managed.Cache;
using static QuickStart.Show;

namespace QuickStart;

/// <summary>Block cache tiers and memtable memory budgets.</summary>
internal static class Caching
{
    public static Task CacheTiersAndWriteBufferManager(string path)
    {
        // <snippet:cache-tiers>
        // One memory budget for blocks and memtables: a 64 MiB tiered cache (primary HyperClock + compressed secondary tier,
        // 25% of the budget) and a write buffer manager that charges memtables to it and caps them at 16 MiB.
        BlockCache cache = BlockCache.CreateTiered(new TieredCacheOptions
        {
            TotalCapacity = 64L << 20,
            CompressedSecondaryRatio = 0.25,
            CacheType = PrimaryCacheType.HyperClock,
            HyperClockCacheOptions = new HyperClockCacheOptions { EstimatedEntryCharge = BlockCache.EstimateBlockCharge(4096) },
        })!;
        var writeBufferManager = new WriteBufferManager(16L << 20, cache, allowStall: true);
        var stats = new Statistics();
        var dbOptions = new DbOptions { CreateIfMissing = true, WriteBufferManager = writeBufferManager, Statistics = stats };
        var cfOptions = new ColumnFamilyOptions { TableOptions = new BlockBasedTableOptions { BlockCache = cache } };

        using (RocksDatabase db = RocksDatabase.Open(path, dbOptions, cfOptions))
        {
            for (int i = 0; i < 10_000; i++) db.Put(System.Text.Encoding.UTF8.GetBytes("key" + i), new byte[200]);
            db.Flush();
            _ = db.Get("key42"u8);
            Console.WriteLine($"memtables: {writeBufferManager.MemoryUsage} bytes, charged to the cache: {writeBufferManager.DummyEntriesInCacheUsage}");
            Console.WriteLine($"secondary cache hits: {stats.GetTickerCount(Ticker.SecondaryCacheHits)}");
        }
        // Resize the whole budget, or the compressed share, at runtime.
        BlockCache.UpdateTieredCache(cache, 128L << 20, 0.3, TieredAdmissionPolicy.Max);
        writeBufferManager.SetBufferSize(32L << 20);

        // What hit rate would a 1 GiB cache give? A SimCache tracks keys only, at no memory cost for values.
        SimCache sim = BlockCache.CreateSimCache(BlockCache.CreateLru(8L << 20), 1L << 30, numShardBits: -1);
        using (RocksDatabase db = RocksDatabase.Open(path, new DbOptions(),
                   new ColumnFamilyOptions { TableOptions = new BlockBasedTableOptions { BlockCache = sim } }))
        {
            for (int i = 0; i < 10_000; i += 7) _ = db.Get(System.Text.Encoding.UTF8.GetBytes("key" + i));
        }
        Console.Write(sim.ToString());   // SimCache MISSes / HITs / HITRATE
        // </snippet:cache-tiers>
        Line($"tiered capacity={cache.Capacity}, wbm buffer={writeBufferManager.BufferSize}, sim lookups={sim.HitCount + sim.MissCount}");
        Check(cache.Capacity == 128L << 20 && writeBufferManager.BufferSize == 32L << 20, "dynamic updates applied");
        Check(sim.HitCount + sim.MissCount > 0, "sim cache counted lookups");
        return Task.CompletedTask;
    }
}
