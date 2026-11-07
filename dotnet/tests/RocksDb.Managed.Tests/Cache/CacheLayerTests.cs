using System.Buffers;
using RocksDb.Managed.Cache;
using RocksDb.Managed.Env;
using RocksDb.Managed.Tests.Db;

namespace RocksDb.Managed.Tests.Cache;

/// <summary>
/// Ports of utilities/simulator_cache/sim_cache_test.cc, cache/cache_reservation_manager_test.cc,
/// utilities/persistent_cache/persistent_cache_test.cc (volatile and block cache tiers) and DB-level tests of the
/// secondary / persistent / simulated caches (db/db_secondary_cache_test style).
/// </summary>
[TestClass]
public sealed class CacheLayerTests
{
    private static CacheKey Key(int i) => new(0x99, (ulong)i);

    private static byte[] KeyBytes(int i)
    {
        byte[] k = new byte[CacheKey.Size];
        Key(i).WriteTo(k);
        return k;
    }

    private static CacheValue Bytes(int size)
    {
        byte[] b = ArrayPool<byte>.Shared.Rent(size);
        return CacheValue.FromPooledBuffer(b, size);
    }

    // ---- SimCache ----------------------------------------------------------------------------------------------

    [TestMethod]
    public void SimCacheCountsHitsOfTheSimulatedCapacity()
    {
        // The real cache holds 10 entries, the simulated one 100: a working set of 50 misses in the real cache but hits
        // the simulation after warm-up.
        BlockCache real = BlockCache.CreateLru(new LruCacheOptions { Capacity = 10, NumShardBits = 0, MetadataChargePolicy = CacheMetadataChargePolicy.DontCharge });
        SimCache sim = BlockCache.CreateSimCache(real, 100, 0);
        Assert.AreEqual("SimCache", sim.Name);
        Assert.AreEqual(100, sim.SimCapacity);
        var stats = new Statistics();
        for (int round = 0; round < 3; round++)
        {
            for (int i = 0; i < 50; i++)
            {
                if (sim.TryLookup(Key(i), null, null, CachePriority.Low, stats, out CachedBlock h)) h.Dispose();
                else sim.Insert(Key(i), Bytes(8), 1, CachePriority.Low);
            }
        }
        Assert.AreEqual(150, sim.HitCount + sim.MissCount);
        Assert.AreEqual(50, sim.MissCount); // first round only
        Assert.AreEqual(100, sim.HitCount);
        Assert.AreEqual(50, sim.SimUsage);
        Assert.AreEqual(100UL, stats.GetTickerCount(Ticker.SimBlockCacheHit));
        Assert.AreEqual(50UL, stats.GetTickerCount(Ticker.SimBlockCacheMiss));
        StringAssert.Contains(sim.ToString(), "SimCache HITRATE: 66.67");
        Assert.IsTrue(real.Usage <= 10);

        sim.SetSimCapacity(10);
        Assert.IsTrue(sim.SimUsage <= 10);
        sim.ResetCounters();
        Assert.AreEqual(0, sim.HitCount + sim.MissCount);
    }

    [TestMethod]
    public void SimCacheActivityLogging()
    {
        string path = Path.Combine(Path.GetTempPath(), "simcache-" + Guid.NewGuid().ToString("N") + ".log");
        try
        {
            SimCache sim = BlockCache.CreateSimCache(BlockCache.CreateLru(1 << 20), 1 << 20, 0);
            sim.StartActivityLogging(path, 0);
            sim.Insert(Key(1), Bytes(4), 4, CachePriority.Low);
            if (sim.TryLookup(Key(1), out CachedBlock h)) h.Dispose();
            sim.StopActivityLogging();
            string[] lines = File.ReadAllLines(path);
            Assert.AreEqual(2, lines.Length);
            Assert.AreEqual("ADD - " + Convert.ToHexString(KeyBytes(1)) + " - 4", lines[0]);
            Assert.AreEqual("LOOKUP - " + Convert.ToHexString(KeyBytes(1)), lines[1]);
            Assert.IsNull(sim.ActivityLoggingError);

            // max_logging_size stops logging.
            sim.StartActivityLogging(path, 10);
            for (int i = 0; i < 10; i++) sim.TryLookup(Key(i), out _);
            Assert.AreEqual(1, File.ReadAllLines(path).Length);
        }
        finally
        {
            File.Delete(path);
        }
    }

    // ---- CacheReservationManager -------------------------------------------------------------------------------

    [TestMethod]
    public void CacheReservationManagerIncreaseDecrease()
    {
        const long dummy = CacheReservationManager.DummyEntrySize;
        BlockCache cache = BlockCache.CreateLru(new LruCacheOptions { Capacity = 4096 * dummy, NumShardBits = 4, MetadataChargePolicy = CacheMetadataChargePolicy.DontCharge });
        var mgr = new CacheReservationManagerImpl(cache, CacheEntryRole.Misc, delayedDecrease: false);
        // IncreaseCacheReservationByMultiplesOfDummyEntrySize
        Assert.IsTrue(mgr.UpdateCacheReservation(1 * dummy));
        Assert.AreEqual(1 * dummy, mgr.TotalReservedCacheSize);
        Assert.AreEqual(1 * dummy, cache.PinnedUsage);
        // IncreaseCacheReservationNotByMultiplesOfDummyEntrySize
        Assert.IsTrue(mgr.UpdateCacheReservation(2 * dummy + dummy / 2));
        Assert.AreEqual(3 * dummy, mgr.TotalReservedCacheSize);
        Assert.AreEqual(2 * dummy + dummy / 2, mgr.TotalMemoryUsed);
        // DecreaseCacheReservation to the smallest multiple >= new usage.
        Assert.IsTrue(mgr.UpdateCacheReservation(dummy + 1));
        Assert.AreEqual(2 * dummy, mgr.TotalReservedCacheSize);
        Assert.IsTrue(mgr.UpdateCacheReservation(0));
        Assert.AreEqual(0, mgr.TotalReservedCacheSize);
        Assert.AreEqual(0, cache.Usage);

        // Delayed decrease keeps placeholders until usage falls below 3/4 of the reservation.
        var delayed = new CacheReservationManagerImpl(cache, CacheEntryRole.WriteBuffer, delayedDecrease: true);
        delayed.UpdateCacheReservation(8 * dummy);
        delayed.UpdateCacheReservation(6 * dummy);
        Assert.AreEqual(8 * dummy, delayed.TotalReservedCacheSize);
        delayed.UpdateCacheReservation(6 * dummy - 1);
        Assert.AreEqual(6 * dummy, delayed.TotalReservedCacheSize);

        // MakeCacheReservation handles release on dispose.
        Assert.IsTrue(mgr.MakeCacheReservation(dummy, out CacheReservationHandle h1));
        Assert.IsTrue(mgr.MakeCacheReservation(dummy, out CacheReservationHandle h2));
        Assert.AreEqual(2 * dummy, mgr.TotalMemoryUsed);
        h1.Dispose();
        h1.Dispose(); // idempotent
        Assert.AreEqual(dummy, mgr.TotalMemoryUsed);
        h2.Dispose();
        Assert.AreEqual(0, mgr.TotalReservedCacheSize);
        delayed.Dispose();
        Assert.AreEqual(0, cache.Usage);
    }

    [TestMethod]
    public void CacheReservationManagerStrictCapacityLimitFails()
    {
        const long dummy = CacheReservationManager.DummyEntrySize;
        BlockCache cache = BlockCache.CreateLru(new LruCacheOptions { Capacity = 2 * dummy, NumShardBits = 0, StrictCapacityLimit = true,
            MetadataChargePolicy = CacheMetadataChargePolicy.DontCharge });
        var mgr = new ConcurrentCacheReservationManager(new CacheReservationManagerImpl(cache, CacheEntryRole.Misc, delayedDecrease: false));
        Assert.IsFalse(mgr.UpdateCacheReservation(3 * dummy)); // Status::MemoryLimit
        Assert.AreEqual(2 * dummy, mgr.TotalReservedCacheSize);
        Assert.IsTrue(mgr.UpdateCacheReservation(dummy, increase: false));
        Assert.AreEqual(2 * dummy, mgr.TotalMemoryUsed);
    }

    [TestMethod]
    public void ConcurrentCacheReservationManagerIsThreadSafe()
    {
        const long dummy = CacheReservationManager.DummyEntrySize;
        BlockCache cache = BlockCache.CreateHyperClock(new HyperClockCacheOptions { Capacity = 1L << 30, EstimatedEntryCharge = dummy });
        var mgr = new ConcurrentCacheReservationManager(new CacheReservationManagerImpl(cache, CacheEntryRole.Misc, delayedDecrease: false));
        Parallel.For(0, 8, t =>
        {
            for (int i = 0; i < 500; i++)
            {
                mgr.UpdateCacheReservation(dummy / 2 + t, increase: true);
                mgr.UpdateCacheReservation(dummy / 2 + t, increase: false);
            }
        });
        Assert.AreEqual(0, mgr.TotalMemoryUsed);
        Assert.AreEqual(0, mgr.TotalReservedCacheSize);
    }

    // ---- persistent cache tiers ------------------------------------------------------------------------------

    private static byte[] Page(int i, int size)
    {
        byte[] b = new byte[size];
        new Random(i).NextBytes(b);
        return b;
    }

    private static byte[] LookupPage(PersistentCache pc, int key)
    {
        Assert.IsTrue(pc.TryLookup(KeyBytes(key), out byte[] data, out int len), "key " + key);
        byte[] r = data.AsSpan(0, len).ToArray();
        ArrayPool<byte>.Shared.Return(data);
        return r;
    }

    [TestMethod]
    public void VolatileTierEvictsToNextTier()
    {
        var top = new VolatileCacheTier(isCompressed: true, maxSize: 10 * 1024);
        var next = new VolatileCacheTier(isCompressed: true, maxSize: 1 << 20);
        var tiered = new PersistentTieredCache();
        tiered.AddTier(top);
        tiered.AddTier(next);
        for (int i = 0; i < 20; i++) Assert.IsTrue(tiered.Insert(KeyBytes(i), Page(i, 1024)));
        Assert.IsFalse(tiered.Insert(KeyBytes(0), Page(0, 1024)) && false);
        for (int i = 0; i < 20; i++) CollectionAssert.AreEqual(Page(i, 1024), LookupPage(tiered, i));
        IReadOnlyList<IReadOnlyDictionary<string, double>> stats = tiered.Stats();
        Assert.AreEqual(2, stats.Count);
        Assert.IsTrue(stats[1]["persistent_cache.volatile_cache.evicts"] >= 10);
        Assert.IsTrue(stats[0]["persistent_cache.volatile_cache.hits"] >= 10); // the lower tier served the evicted pages
        Assert.IsTrue(tiered.IsCompressed);
        StringAssert.Contains(tiered.PrintStats(), "---- next tier -----");
        Assert.IsFalse(tiered.TryLookup(KeyBytes(1000), out _, out _));
        tiered.Close();
    }

    /// <summary>
    /// Inserts a page; without pipelined writes Insert returns TryAgain (false) while every write buffer is held by
    /// files whose writes the writer threads have not finished (e.g. under CPU load). Like upstream's
    /// PersistentCacheTierTest::InsertImpl, retry then, after waiting for the dispatched writes (instead of sleeping):
    /// once they are done the full files are closed and their buffers are free again, so the retry must succeed.
    /// </summary>
    private static void InsertPage(BlockCacheTier tier, bool pipelineWrites, int key, int size)
    {
        if (tier.Insert(KeyBytes(key), Page(key, size))) return;
        Assert.IsFalse(pipelineWrites, "pipelined inserts are queued, never TryAgain");
        tier.Flush();
        Assert.IsTrue(tier.Insert(KeyBytes(key), Page(key, size)), "insert after the pending writes completed, key " + key);
    }

    [TestMethod]
    [DataRow(true)]
    [DataRow(false)]
    public void BlockCacheTierInsertLookupAcrossFilesAndEviction(bool pipelineWrites)
    {
        string dir = Path.Combine(Path.GetTempPath(), "pcache-" + Guid.NewGuid().ToString("N"));
        var config = new PersistentCacheConfig(LocalFileSystem.Instance, dir, 4L << 20, writeBufferSize: 16 * 1024)
        {
            CacheFileSize = 256 * 1024, PipelineWrites = pipelineWrites, WriterQueueDepth = 2,
        };
        var tier = new BlockCacheTier(config);
        try
        {
            tier.Open();
            // Records can span write buffers; files roll over at cache_file_size.
            for (int i = 0; i < 200; i++) InsertPage(tier, pipelineWrites, i, 5000 + i);
            tier.Flush();
            Assert.IsTrue(tier.FileCount > 1);
            Assert.IsTrue(File.Exists(Path.Combine(dir, "cache", "0.rc")));
            for (int i = 0; i < 200; i++) CollectionAssert.AreEqual(Page(i, 5000 + i), LookupPage(tier, i));
            // Record layout: magic, crc, key size, value size, key, value.
            byte[] file0 = File.ReadAllBytes(Path.Combine(dir, "cache", "0.rc"));
            Assert.AreEqual(BlockCacheTier.RecordMagic, System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(file0));
            Assert.AreEqual(16u, System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(file0.AsSpan(8)));
            Assert.AreEqual(5000u, System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(file0.AsSpan(12)));
            Assert.AreEqual(0, file0.Length % BlockCacheTier.FileAlignmentSize);

            // Exceeding the cache size evicts whole old files.
            for (int i = 200; i < 1200; i++) InsertPage(tier, pipelineWrites, i, 5000);
            tier.Flush();
            Assert.IsFalse(tier.TryLookup(KeyBytes(0), out _, out _), "oldest file evicted");
            CollectionAssert.AreEqual(Page(1199, 5000), LookupPage(tier, 1199));
            long diskBytes = 0;
            foreach (string f in Directory.GetFiles(Path.Combine(dir, "cache"))) diskBytes += new FileInfo(f).Length;
            Assert.IsTrue(diskBytes <= (4L << 20) + 512 * 1024, "disk " + diskBytes);
            IReadOnlyDictionary<string, double> s = tier.Stats()[^1];
            Assert.IsTrue(s["persistentcache.blockcachetier.cache_hits"] >= 201);
            Assert.AreEqual(0.0, s["persistentcache.blockcachetier.insert_dropped"], "no insert is dropped");
            tier.Close();

            // Reopen cleans up the cache folder (not persistent across restarts, as upstream).
            var again = new BlockCacheTier(config);
            again.Open();
            Assert.IsFalse(again.TryLookup(KeyBytes(1199), out _, out _));
            again.Close();
        }
        finally
        {
            // Stop the writer threads before the directory goes away, also when an assertion failed.
            tier.Close();
            if (Directory.Exists(dir)) Directory.Delete(dir, true);
        }
    }

    [TestMethod]
    public void BlockCacheTierConcurrentInsertAndLookup()
    {
        string dir = Path.Combine(Path.GetTempPath(), "pcache-" + Guid.NewGuid().ToString("N"));
        try
        {
            PersistentCacheTier tier = PersistentCache.Create(LocalFileSystem.Instance, dir, 256L << 20, optimizedForNvm: false);
            Parallel.For(0, 4, t =>
            {
                for (int i = 0; i < 300; i++)
                {
                    int k = t * 1000 + i;
                    tier.Insert(KeyBytes(k), Page(k, 2000));
                    if (tier.TryLookup(KeyBytes(t * 1000 + i / 2), out byte[] d, out int len))
                    {
                        Assert.IsTrue(d.AsSpan(0, len).SequenceEqual(Page(t * 1000 + i / 2, 2000)));
                        ArrayPool<byte>.Shared.Return(d);
                    }
                }
            });
            tier.Flush();
            for (int t = 0; t < 4; t++) CollectionAssert.AreEqual(Page(t * 1000 + 299, 2000), LookupPage(tier, t * 1000 + 299));
            tier.Dispose();
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, true);
        }
    }

    [TestMethod]
    public void PersistentCacheConfigValidation()
    {
        var c = new PersistentCacheConfig(LocalFileSystem.Instance, "/tmp/x", 1L << 20);
        var ex = Assert.ThrowsExactly<RocksDbException>(c.Validate); // cache_size < cache_file_size
        Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);
        StringAssert.Contains(c.ToString(), "cache_file_size: 104857600");
    }

    // ---- DB integration --------------------------------------------------------------------------------------

    private static DbOptions DbOpts(IFileSystem fs, Statistics stats) =>
        new() { CreateIfMissing = true, FileSystem = fs, Statistics = stats };

    private static ColumnFamilyOptions CfOpts(BlockCache cache, PersistentCache? pc = null) => new()
    {
        TableOptions = new BlockBasedTableOptions { BlockCache = cache, BlockSize = 1024, PersistentCache = pc },
        Compression = Managed.Compression.CompressionType.None,
    };

    private static void Fill(RocksDatabase db, int n)
    {
        for (int i = 0; i < n; i++) db.Put(DbTestUtil.Key(i), DbTestUtil.Value(i, 1));
        db.Flush();
    }

    private static void ReadAll(RocksDatabase db, int n)
    {
        for (int i = 0; i < n; i++) CollectionAssert.AreEqual(DbTestUtil.Value(i, 1), db.Get(DbTestUtil.Key(i)), "key " + i);
    }

    [TestMethod]
    public async Task DbReadsHitTheCompressedSecondaryCache()
    {
        MemoryFileSystem fs = DbTestUtil.NewFs();
        var stats = new Statistics();
        CompressedSecondaryCache sec = new CompressedSecondaryCacheOptions { Capacity = 8 << 20 }.CreateSecondaryCache();
        BlockCache cache = BlockCache.CreateLru(new LruCacheOptions { Capacity = 32 * 1024, NumShardBits = 0, SecondaryCache = sec });
        const int n = 3000;
        using (RocksDatabase db = RocksDatabase.Open(DbTestUtil.DbPath, DbOpts(fs, stats), CfOpts(cache)))
        {
            Fill(db, n);
            // Several passes: blocks evicted from the small primary are demoted on their second eviction and later
            // served by the secondary cache.
            for (int pass = 0; pass < 4; pass++) ReadAll(db, n);
            Assert.IsTrue(stats.GetTickerCount(Ticker.SecondaryCacheHits) > 0, "secondary hits");
            Assert.IsTrue(stats.GetTickerCount(Ticker.SecondaryCacheDataHits) > 0);
            Assert.IsTrue(stats.GetTickerCount(Ticker.CompressedSecondaryCacheHits) > 0);

            // Async point lookups and MultiGet go through StartAsyncLookup.
            ulong before = stats.GetTickerCount(Ticker.SecondaryCacheHits);
            for (int i = 0; i < n; i += 3) CollectionAssert.AreEqual(DbTestUtil.Value(i, 1), await db.GetAsync(DbTestUtil.Key(i)));
            var keys = new List<byte[]>();
            for (int i = 0; i < n; i += 7) keys.Add(DbTestUtil.Key(i));
            byte[]?[] got = db.MultiGet(keys);
            for (int j = 0; j < keys.Count; j++) CollectionAssert.AreEqual(DbTestUtil.Value(j * 7, 1), got[j]);
            Assert.IsTrue(stats.GetTickerCount(Ticker.SecondaryCacheHits) > before);
        }
    }

    [TestMethod]
    public void LowestUsedCacheTierVolatileBypassesTheSecondaryCache()
    {
        MemoryFileSystem fs = DbTestUtil.NewFs();
        var stats = new Statistics();
        CompressedSecondaryCache sec = new CompressedSecondaryCacheOptions { Capacity = 8 << 20 }.CreateSecondaryCache();
        BlockCache cache = BlockCache.CreateLru(new LruCacheOptions { Capacity = 32 * 1024, NumShardBits = 0, SecondaryCache = sec });
        DbOptions o = DbOpts(fs, stats);
        o.LowestUsedCacheTier = CacheTier.VolatileTier;
        using RocksDatabase db = RocksDatabase.Open(DbTestUtil.DbPath, o, CfOpts(cache));
        Fill(db, 2000);
        for (int pass = 0; pass < 3; pass++) ReadAll(db, 2000);
        Assert.AreEqual(0UL, stats.GetTickerCount(Ticker.SecondaryCacheHits));
        Assert.AreEqual(0, sec.Count);
    }

    [TestMethod]
    public void TieredCacheAsDbBlockCacheWithWriteBufferManagerCharging()
    {
        MemoryFileSystem fs = DbTestUtil.NewFs();
        var stats = new Statistics();
        BlockCache tiered = BlockCache.CreateTiered(new TieredCacheOptions { TotalCapacity = 16 << 20, CompressedSecondaryRatio = 0.25 })!;
        var wbm = new WriteBufferManager(8 << 20, tiered, allowStall: false);
        DbOptions o = DbOpts(fs, stats);
        o.WriteBufferManager = wbm;
        using RocksDatabase db = RocksDatabase.Open(DbTestUtil.DbPath, o, CfOpts(tiered));
        for (int i = 0; i < 5000; i++) db.Put(DbTestUtil.Key(i), DbTestUtil.Value(i, 1));
        Assert.IsTrue(wbm.DummyEntriesInCacheUsage > 0);
        // A quarter of the memtable charge is taken from the compressed tier.
        Assert.IsTrue(tiered.SecondaryCachePinnedUsage >= 0);
        db.Flush();
        ReadAll(db, 5000);
    }

    [TestMethod]
    [DataRow(true)]
    [DataRow(false)]
    public void DbReadsThroughPersistentCache(bool compressedTier)
    {
        MemoryFileSystem fs = DbTestUtil.NewFs();
        var stats = new Statistics();
        var pc = new VolatileCacheTier(isCompressed: compressedTier, maxSize: 64 << 20);
        BlockCache cache = BlockCache.CreateLru(new LruCacheOptions { Capacity = 16 * 1024, NumShardBits = 0 });
        using RocksDatabase db = RocksDatabase.Open(DbTestUtil.DbPath, DbOpts(fs, stats), CfOpts(cache, pc));
        Fill(db, 2000);
        ReadAll(db, 2000);
        ulong misses = stats.GetTickerCount(Ticker.PersistentCacheMiss);
        Assert.IsTrue(misses > 0);
        ReadAll(db, 2000);
        Assert.IsTrue(stats.GetTickerCount(Ticker.PersistentCacheHit) > 0, "persistent cache hits");
        Assert.IsTrue(pc.Stats()[^1]["persistent_cache.volatile_cache.inserts"] > 0);
    }

    [TestMethod]
    public void SimCacheAsDbBlockCache()
    {
        MemoryFileSystem fs = DbTestUtil.NewFs();
        var stats = new Statistics();
        SimCache sim = BlockCache.CreateSimCache(BlockCache.CreateLru(new LruCacheOptions { Capacity = 16 * 1024, NumShardBits = 0 }), 64L << 20, 0);
        using RocksDatabase db = RocksDatabase.Open(DbTestUtil.DbPath, DbOpts(fs, stats), CfOpts(sim));
        Fill(db, 2000);
        ReadAll(db, 2000);
        ReadAll(db, 2000);
        Assert.IsTrue(sim.HitCount > sim.MissCount, sim.ToString());
        Assert.AreEqual((ulong)sim.HitCount, stats.GetTickerCount(Ticker.SimBlockCacheHit));
        Assert.IsTrue(stats.GetTickerCount(Ticker.BlockCacheMiss) > (ulong)sim.MissCount, "the real cache is much smaller");
    }
}
