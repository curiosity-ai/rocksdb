using RocksDb.Managed.Cache;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Tests.Db;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Regression tests for discrepancies found by audit #4 in the cache layer (Cache/*, the table reader's cache tiers).
/// </summary>
[TestClass]
public sealed class Audit4CacheTests
{
    private const string DbPath = "/audit4cache";

    private static CacheKey Key(int k) => new(0xA4CAC4E, (ulong)k);

    // ---- HyperClockCache: initial countdown of an insert that returns a handle -------------------------------------

    private static (ulong Acquire, ulong Release) Counters(ClockSlot s)
    {
        ulong meta = s.LoadMeta();
        return ((meta >> ClockSlot.AcquireCounterShift) & ClockSlot.CounterMask, (meta >> ClockSlot.ReleaseCounterShift) & ClockSlot.CounterMask);
    }

    [TestMethod]
    [DataRow(CachePriority.High, 3)]
    [DataRow(CachePriority.Low, 2)]
    [DataRow(CachePriority.Bottom, 1)]
    public void HyperClockInsertWithHandleStartsAtTheInitialCountdown(CachePriority priority, int countdown)
    {
        // FinishSlotInsert (cache/clock_cache.cc): acquire counter = initial_countdown, release counter =
        // initial_countdown - keep_ref. Releasing the handle (Release with useful = true adds one release) leaves both
        // at the initial countdown: the same clock state as an insert without a handle. The port stored
        // initial_countdown + 1 / initial_countdown, so every entry inserted with a handle (what the table reader does)
        // got one extra life and low priority entries aged exactly like high priority ones (ClockUpdate clips at
        // kMaxCountdown - 1).
        var c = new HyperClockCache(new HyperClockCacheOptions
        {
            Capacity = 1 << 20, EstimatedEntryCharge = 1024, NumShardBits = 0, MetadataChargePolicy = CacheMetadataChargePolicy.DontCharge,
        });
        Assert.IsTrue(c.TryInsert(Key(1), CacheValue.FromObject("a"), 100, priority, out CachedBlock h));
        var slot = (ClockSlot)h.Entry!;
        Assert.AreEqual(((ulong)countdown, (ulong)countdown - 1), Counters(slot), "while the handle is held");
        Assert.AreEqual(1UL, ClockSlot.Refcount(slot.LoadMeta()));
        h.Dispose();
        Assert.AreEqual(((ulong)countdown, (ulong)countdown), Counters(slot), "after releasing the handle");

        // The same entry inserted without a handle, then pinned by a lookup.
        Assert.IsTrue(c.Insert(Key(2), CacheValue.FromObject("b"), 100, priority));
        Assert.IsTrue(c.TryLookup(Key(2), out CachedBlock h2));
        var slot2 = (ClockSlot)h2.Entry!;
        Assert.AreEqual(((ulong)countdown + 1, (ulong)countdown), Counters(slot2));
        h2.Dispose();
    }

    [TestMethod]
    public void HyperClockEvictsLowPriorityBeforeHighPriorityForHandleInserts()
    {
        // GetInitialCountdown + ClockUpdate: with countdowns 3 (high) and 2 (low), a sweep that has to free entries
        // reaches the low priority entries one pass before the high priority ones. 32 high + 32 low entries inserted
        // with handles (released at once) fill the cache; 8 more inserts each evict one entry: all of them low priority.
        const int charge = 1000, perPriority = 32;
        var c = new HyperClockCache(new HyperClockCacheOptions
        {
            Capacity = 2 * perPriority * charge, EstimatedEntryCharge = charge, NumShardBits = 0,
            MetadataChargePolicy = CacheMetadataChargePolicy.DontCharge,
        });
        for (int i = 0; i < 2 * perPriority; i++)
        {
            Assert.IsTrue(c.TryInsert(Key(i), CacheValue.FromObject("v"), charge, i % 2 == 0 ? CachePriority.High : CachePriority.Low, out CachedBlock h));
            h.Dispose();
        }
        Assert.AreEqual(2 * perPriority, c.Count);
        for (int i = 0; i < 8; i++) Assert.IsTrue(c.Insert(Key(1000 + i), CacheValue.FromObject("n"), charge, CachePriority.Low));
        int high = 0, low = 0;
        for (int i = 0; i < 2 * perPriority; i++)
        {
            if (!c.TryLookup(Key(i), out CachedBlock h)) continue;
            h.Dispose();
            if (i % 2 == 0) high++;
            else low++;
        }
        Assert.AreEqual(perPriority, high, "no high priority entry is evicted while low priority ones remain");
        Assert.AreEqual(perPriority - 8, low);
    }

    // ---- tiered cache: placeholder accounting ----------------------------------------------------------------------

    [TestMethod]
    public void TieredLruCacheCountsPlaceholdersWithoutTheirMetadataCharge()
    {
        // CacheWithSecondaryAdapter::Insert / Release (cache/secondary_cache_adapter.cc): placeholder_usage_ moves by
        // target_->GetCharge(handle), which for the LRU cache is the charge without the handle's metadata charge
        // (LRUHandle::GetCharge), i.e. exactly kSizeDummyEntry = 256 KiB per placeholder. The port added the entry's
        // total charge (256 KiB + 88), so the 1 MiB reservation chunks were reached early once enough placeholders
        // existed. 2983 placeholders = 745.75 MiB: reserved_usage_ is 745 MiB and, at ratio 0.5, 372.5 MiB of them are
        // taken from the secondary cache. With the metadata included the sum passes 746 MiB (373 MiB).
        const long mib = 1 << 20;
        BlockCache? tiered = BlockCache.CreateTiered(new TieredCacheOptions
        {
            CacheType = PrimaryCacheType.Lru, LruCacheOptions = new LruCacheOptions { NumShardBits = 0 },
            CompressedSecondaryCacheOptions = new CompressedSecondaryCacheOptions { NumShardBits = 0 },
            TotalCapacity = 4096 * mib, CompressedSecondaryRatio = 0.5,
        });
        Assert.IsNotNull(tiered);
        Assert.AreEqual(0, tiered.SecondaryCachePinnedUsage);
        using var mgr = new CacheReservationManagerImpl(tiered, CacheEntryRole.WriteBuffer, delayedDecrease: false);
        Assert.IsTrue(mgr.UpdateCacheReservation(2983 * CacheReservationManager.DummyEntrySize));
        Assert.AreEqual(745 * mib / 2, tiered.SecondaryCachePinnedUsage);
        // One more placeholder completes the 746th MiB.
        Assert.IsTrue(mgr.UpdateCacheReservation(2984 * CacheReservationManager.DummyEntrySize));
        Assert.AreEqual(746 * mib / 2, tiered.SecondaryCachePinnedUsage);
        // Releasing everything returns the secondary cache's share (Release: Inflate + reservation increase).
        Assert.IsTrue(mgr.UpdateCacheReservation(0));
        Assert.AreEqual(0, tiered.SecondaryCachePinnedUsage);
    }

    // ---- persistent cache: fill_cache ------------------------------------------------------------------------------

    private static double PersistentInserts(VolatileCacheTier pc) => pc.Stats()[^1]["persistent_cache.volatile_cache.inserts"];

    [TestMethod]
    [DataRow(true, false)]
    [DataRow(false, false)]
    [DataRow(true, true)]
    [DataRow(false, true)]
    public async Task PersistentCacheIsNotFilledByReadsWithoutFillCache(bool compressedTier, bool asyncReads)
    {
        // BlockFetcher::InsertCompressedBlockToPersistentCacheIfNeeded / InsertUncompressedBlockToPersistentCacheIfNeeded
        // (table/block_fetcher.cc): blocks read from the file go to the persistent cache only with
        // read_options.fill_cache. The port inserted them unconditionally.
        MemoryFileSystem fs = DbTestUtil.NewFs();
        var pc = new VolatileCacheTier(isCompressed: compressedTier, maxSize: 64 << 20);
        BlockCache cache = BlockCache.CreateLru(new LruCacheOptions { Capacity = 8 << 20, NumShardBits = 0 });
        var cf = new ColumnFamilyOptions
        {
            TableOptions = new BlockBasedTableOptions { BlockCache = cache, BlockSize = 1024, PersistentCache = pc },
            Compression = Managed.Compression.CompressionType.None,
        };
        using RocksDatabase db = RocksDatabase.Open(DbPath, DbTestUtil.NewOptions(fs), cf);
        const int n = 2000;
        for (int i = 0; i < n; i++) db.Put(DbTestUtil.Key(i), DbTestUtil.Value(i, 1));
        db.Flush();

        // Opens the table (index / filter reads may fill the persistent cache, as upstream's default ReadOptions do).
        Assert.IsNotNull(db.Get(DbTestUtil.Key(0)));
        double before = PersistentInserts(pc);

        var noFill = new ReadOptions { FillCache = false };
        var w = new System.Buffers.ArrayBufferWriter<byte>();
        for (int i = 0; i < n; i++)
        {
            if (asyncReads)
            {
                w.Clear();
                Assert.IsTrue(await db.TryGetAsync(noFill, DbTestUtil.Key(i), w));
                CollectionAssert.AreEqual(DbTestUtil.Value(i, 1), w.WrittenSpan.ToArray());
            }
            else
            {
                CollectionAssert.AreEqual(DbTestUtil.Value(i, 1), db.Get(noFill, DbTestUtil.Key(i)));
            }
        }
        Assert.AreEqual(before, PersistentInserts(pc), "fill_cache = false must not insert into the persistent cache");

        // With fill_cache the data blocks are inserted (the block cache does not hold them: the reads above did not fill it).
        for (int i = 0; i < n; i++) CollectionAssert.AreEqual(DbTestUtil.Value(i, 1), db.Get(DbTestUtil.Key(i)));
        Assert.IsTrue(PersistentInserts(pc) >= before + 20, "fill_cache = true inserts the data blocks");
    }

    // ---- cache entry stats snapshots -------------------------------------------------------------------------------

    [TestMethod]
    public void CacheEntryRoleStatsSnapshotsDoNotShareTheCollectorsArrays()
    {
        // CacheEntryStatsCollector::GetStats (cache/cache_entry_stats.h) copies the saved stats by value. The port's
        // Clone shared the per-role arrays with the collector's working copy, so the next collection rewrote (and a
        // caller could corrupt) snapshots that were already handed out.
        BlockCache cache = BlockCache.CreateLru(new LruCacheOptions { Capacity = 1 << 20, NumShardBits = 0 });
        var helper = new BytesCacheItemHelper(CacheEntryRole.DataBlock);
        Assert.IsTrue(cache.Insert(Key(1), CacheValue.FromBuffer(new byte[10], 10).WithHelper(helper), 10, CachePriority.Low));
        CacheEntryRoleStats a = CacheEntryRoleStats.Collect(cache, fast: false);
        Assert.AreEqual(1UL, a.EntryCounts[(int)CacheEntryRole.DataBlock]);
        Assert.AreEqual(10UL, a.TotalCharges[(int)CacheEntryRole.DataBlock]);
        a.EntryCounts[(int)CacheEntryRole.DataBlock] = 777;
        a.TotalCharges[(int)CacheEntryRole.DataBlock] = 777;
        // Within the minimum collection interval: a copy of the last collection.
        CacheEntryRoleStats b = CacheEntryRoleStats.Collect(cache, fast: false);
        Assert.AreNotSame(a.EntryCounts, b.EntryCounts);
        Assert.AreNotSame(a.TotalCharges, b.TotalCharges);
        Assert.AreEqual(1UL, b.EntryCounts[(int)CacheEntryRole.DataBlock]);
        Assert.AreEqual(10UL, b.TotalCharges[(int)CacheEntryRole.DataBlock]);
        Assert.AreEqual(1UL, b.CopiesOfLastCollection);
        Assert.AreEqual(a.CollectionCount, b.CollectionCount);
    }

    // ---- pinned values stay intact under concurrent eviction (pooled buffers) ----------------------------------------

    private const int ValueLength = 96;

    private static byte Pattern(int key, int i) => (byte)(key * 31 + i * 7 + 3);

    private static CacheValue PooledValue(int key, CacheItemHelper helper)
    {
        byte[] buf = RocksArrayPool<byte>.Shared.Rent(ValueLength);
        for (int i = 0; i < ValueLength; i++) buf[i] = Pattern(key, i);
        return CacheValue.FromPooledBuffer(buf, ValueLength).WithHelper(helper);
    }

    private static bool Intact(in CachedBlock h, int key)
    {
        ReadOnlySpan<byte> s = h.Span;
        if (s.Length != ValueLength) return false;
        for (int i = 0; i < s.Length; i++)
        {
            if (s[i] != Pattern(key, i)) return false;
        }
        return true;
    }

    private static BlockCache NewStressCache(int kind)
    {
        // Room for about 20 entries: nearly every insert evicts.
        const long capacity = 20 * ValueLength;
        return kind switch
        {
            0 => BlockCache.CreateLru(new LruCacheOptions { Capacity = capacity, NumShardBits = 1, MetadataChargePolicy = CacheMetadataChargePolicy.DontCharge }),
            1 => BlockCache.CreateHyperClock(new HyperClockCacheOptions
            {
                Capacity = capacity, EstimatedEntryCharge = ValueLength, NumShardBits = 0, MetadataChargePolicy = CacheMetadataChargePolicy.DontCharge,
            }),
            2 => BlockCache.CreateHyperClock(new HyperClockCacheOptions
            {
                Capacity = 64 * capacity, EstimatedEntryCharge = 0, MinAvgEntryCharge = 8, NumShardBits = 0,
                MetadataChargePolicy = CacheMetadataChargePolicy.DontCharge,
            }),
            3 => BlockCache.CreateLru(new LruCacheOptions
            {
                Capacity = capacity, NumShardBits = 0, MetadataChargePolicy = CacheMetadataChargePolicy.DontCharge,
                SecondaryCache = new CompressedSecondaryCacheOptions { Capacity = 40 * ValueLength, NumShardBits = 0 }.CreateSecondaryCache(),
            }),
            _ => BlockCache.CreateHyperClock(new HyperClockCacheOptions
            {
                Capacity = capacity, EstimatedEntryCharge = ValueLength, NumShardBits = 0, MetadataChargePolicy = CacheMetadataChargePolicy.DontCharge,
                SecondaryCache = new CompressedSecondaryCacheOptions { Capacity = 40 * ValueLength, NumShardBits = 0 }.CreateSecondaryCache(),
            }),
        };
    }

    private sealed class StressState
    {
        public int Failures;
        public long Hits;
        public string? FirstFailure;

        public void Fail(string what, int key)
        {
            Interlocked.Increment(ref Failures);
            Interlocked.CompareExchange(ref FirstFailure, what + " key " + key, null);
        }
    }

    private static void StressWorker(BlockCache cache, CacheItemHelper helper, StressState state, int seed, int keys, int ops)
    {
        var rnd = new Random(seed);
        var held = new (CachedBlock Handle, int Key)[4];
        try
        {
            for (int op = 0; op < ops && Volatile.Read(ref state.Failures) == 0; op++)
            {
                int key = rnd.Next(keys);
                int slot = rnd.Next(held.Length);
                // Re-check and release what this slot pinned some operations ago.
                if (held[slot].Handle.HasValue)
                {
                    if (!Intact(held[slot].Handle, held[slot].Key)) state.Fail("pinned value changed", held[slot].Key);
                    cache.Release(ref held[slot].Handle, eraseIfLastRef: rnd.Next(16) == 0);
                }
                switch (rnd.Next(10))
                {
                    case < 5:
                        if (cache.TryLookup(Key(key), helper, null, CachePriority.Low, null, out CachedBlock h))
                        {
                            Interlocked.Increment(ref state.Hits);
                            if (!Intact(h, key)) state.Fail("lookup returned a wrong value", key);
                            held[slot] = (h, key);
                        }
                        break;
                    case < 8:
                        {
                            CacheValue v = PooledValue(key, helper);
                            if (cache.TryInsert(Key(key), v, ValueLength, (CachePriority)rnd.Next(3), out CachedBlock ih))
                            {
                                if (!Intact(ih, key)) state.Fail("inserted value changed", key);
                                held[slot] = (ih, key);
                            }
                            else
                            {
                                RocksArrayPool<byte>.Shared.Return(v.Buffer!); // MemoryLimit: the caller keeps the value
                            }
                            break;
                        }
                    case 8:
                        cache.Insert(Key(key), PooledValue(key, helper), ValueLength, CachePriority.Low);
                        break;
                    default:
                        cache.Erase(Key(key));
                        break;
                }
            }
        }
        catch (Exception e)
        {
            state.Fail(e.GetType().Name + ": " + e.Message, -1);
        }
        finally
        {
            for (int i = 0; i < held.Length; i++)
            {
                if (!held[i].Handle.HasValue) continue;
                if (!Intact(held[i].Handle, held[i].Key)) state.Fail("pinned value changed", held[i].Key);
                cache.Release(ref held[i].Handle, eraseIfLastRef: false);
            }
        }
    }

    [TestMethod]
    [DataRow(0, DisplayName = "PinnedValuesStayIntact LRU")]
    [DataRow(1, DisplayName = "PinnedValuesStayIntact HyperClock fixed table")]
    [DataRow(2, DisplayName = "PinnedValuesStayIntact HyperClock automatic table")]
    [DataRow(3, DisplayName = "PinnedValuesStayIntact LRU + compressed secondary")]
    [DataRow(4, DisplayName = "PinnedValuesStayIntact HyperClock + compressed secondary")]
    public void PinnedValuesStayIntactUnderConcurrentEviction(int kind)
    {
        // The invariant behind Cache::Lookup / Release (LRUCacheShard::Release, FixedHyperClockTable::Release / Evict):
        // an entry's value is freed only after its last reference is gone. In the port "freed" means the buffer goes back
        // to RocksArrayPool, where another insert rents and overwrites it, so a premature or double free shows up as
        // a pinned value whose bytes changed (or belong to another key).
        BlockCache cache = NewStressCache(kind);
        var helper = new BytesCacheItemHelper(CacheEntryRole.DataBlock);
        const int threads = 4, keys = 200, opsPerThread = 40_000;
        var state = new StressState();
        var workers = new Thread[threads];
        for (int t = 0; t < threads; t++)
        {
            int seed = 1234 + t;
            workers[t] = new Thread(() => StressWorker(cache, helper, state, seed, keys, opsPerThread));
        }
        foreach (Thread w in workers) w.Start();
        foreach (Thread w in workers) w.Join();
        Assert.AreEqual(0, state.Failures, state.FirstFailure);
        Assert.IsTrue(state.Hits > 0, "the workload produced no hits");
        // Every handle was released: nothing stays pinned, and all entries can be dropped.
        Assert.AreEqual(0, cache.PinnedUsage);
        cache.EraseUnreferencedEntries();
        Assert.AreEqual(0, cache.Count);
        Assert.AreEqual(0, cache.Usage);
    }
}
