using RocksDb.Managed.Cache;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4, automatic HyperClockCache table: cache/clock_cache.{h,cc} (AutoHyperClockTable, BaseClockTable,
/// ClockCacheShard) read against Cache/HyperClockCache.cs. The port's table structure differs on purpose (no chains);
/// these tests cover behaviour a caller can observe.
/// </summary>
[TestClass]
public sealed class Audit4AutoHyperClockTests
{
    private static CacheKey Key(ulong space, int k) => new(0xA4A07000UL + space, (ulong)k);

    private static HyperClockCache NewAuto(long capacity, bool strict, CacheMetadataChargePolicy policy, int shardBits = 0) =>
        new(new HyperClockCacheOptions
        {
            Capacity = capacity, EstimatedEntryCharge = 0, NumShardBits = shardBits, StrictCapacityLimit = strict,
            MetadataChargePolicy = policy,
        });

    private static bool Contains(HyperClockCache c, in CacheKey key)
    {
        if (!c.TryLookup(key, out CachedBlock h)) return false;
        h.Dispose();
        return true;
    }

    private static long Metadata(HyperClockCache c) => (long)c.TableSize * HyperClockCache.SlotMetadataCharge;

    // ---- findings ---------------------------------------------------------------------------------------------------

    /// <summary>
    /// AutoHyperClockTable::Evict (cache/clock_cache.cc:3512-3526): an eviction run may circle the table
    /// kMaxCountdown + 1 times, so an unreferenced entry at the highest countdown (3) is aged three times and freed on
    /// the fourth visit: an insert into a full cache of unpinned entries always finds room. The port used the fixed
    /// table's bound (kMaxCountdown circles and one step, FixedHyperClockTable::Evict): with every entry at countdown 3
    /// only the first four slots were visited a fourth time, and with a strict capacity limit the insert failed
    /// (MemoryLimit) although nothing was pinned.
    /// </summary>
    [TestMethod]
    public void EvictionCirclesOftenEnoughToFreeAnUnpinnedEntryAtTheHighestCountdown()
    {
        const int entries = 10, charge = 1000;
        for (ulong space = 0; space < 24; space++)
        {
            HyperClockCache c = NewAuto(entries * charge, strict: true, CacheMetadataChargePolicy.DontCharge);
            for (int i = 0; i < entries; i++)
                Assert.IsTrue(c.Insert(Key(space, i), CacheValue.FromObject("v"), charge, CachePriority.High));
            Assert.AreEqual((long)entries * charge, c.Usage);
            Assert.AreEqual(0, c.PinnedUsage);
            // Full, nothing pinned: the insert evicts one entry.
            Assert.IsTrue(c.Insert(Key(space, entries), CacheValue.FromObject("v"), charge, CachePriority.High), "key space " + space);
            Assert.IsTrue(Contains(c, Key(space, entries)), "key space " + space);
            // One clock step (four slots) is freed at a time: at least one entry, at most four.
            Assert.IsTrue(c.Count <= entries && c.Count > entries - 4, c.Count.ToString());
            Assert.AreEqual((long)c.Count * charge, c.Usage);
            Assert.AreEqual(0, c.EvictionEffortExceededCount);
        }
    }

    /// <summary>
    /// BaseClockTable::Insert (cache/clock_cache.cc:591-594) with AutoHyperClockTable::GrowIfNeeded (2128-2156): an
    /// insert only has to evict for occupancy when the table cannot grow any more; the usage plays no part. The port
    /// grew only while the usage was below the capacity: with the cache over its capacity because the entries are
    /// pinned, the table stayed small, filled up to its occupancy limit, and every later insert became a standalone
    /// entry that no lookup can find (or was dropped when no handle was asked for).
    /// </summary>
    [TestMethod]
    [DataRow(CacheMetadataChargePolicy.DontCharge)]
    [DataRow(CacheMetadataChargePolicy.FullCharge)]
    public void TableGrowsWhenPinnedEntriesHoldTheCacheOverItsCapacity(CacheMetadataChargePolicy policy)
    {
        const int entries = 300, charge = 16 * 1024;
        HyperClockCache c = NewAuto(1 << 20, strict: false, policy);
        var handles = new CachedBlock[entries];
        for (int i = 0; i < entries; i++)
            Assert.IsTrue(c.TryInsert(Key(100, i), CacheValue.FromObject("v"), charge, CachePriority.Low, out handles[i]));
        // Upstream: up to 0.6 * capacity / (min_avg_entry_charge * 0.6 [+ 64]) entries fit the table (> 1800 here).
        Assert.AreEqual(entries, c.Count);
        for (int i = 0; i < entries; i++) Assert.IsTrue(Contains(c, Key(100, i)), i.ToString());
        // Without a handle the entry is inserted as well (over capacity: nothing can be evicted).
        Assert.IsTrue(c.Insert(Key(100, entries), CacheValue.FromObject("v"), charge, CachePriority.Low));
        Assert.IsTrue(Contains(c, Key(100, entries)));
        Assert.AreEqual((long)(entries + 1) * charge + (policy == CacheMetadataChargePolicy.FullCharge ? Metadata(c) : 0), c.Usage);
        foreach (CachedBlock h in handles) h.Dispose();
        Assert.AreEqual(0, c.PinnedUsage);
        c.EraseUnreferencedEntries();
        Assert.AreEqual(0, c.Count);
        Assert.AreEqual(policy == CacheMetadataChargePolicy.FullCharge ? Metadata(c) : 0, c.Usage);
    }

    /// <summary>
    /// ClockCacheShard::ApplyToSomeEntries (cache/clock_cache.cc:1172-1203, "entries will generally stay in their
    /// existing slots"): upstream's automatic table never moves an entry, so a traversal that runs while the table grows
    /// reports every entry that is present throughout exactly once. The port's grow rehashes the entries into a new
    /// array and the traversal continued in that array at its old slot index: entries were reported twice or skipped.
    /// </summary>
    [TestMethod]
    public void ApplyToAllEntriesReportsEachEntryOnceWhileTheTableGrows()
    {
        const int stable = 40, more = 400;
        HyperClockCache c = NewAuto(64 << 20, strict: false, CacheMetadataChargePolicy.DontCharge);
        for (int i = 0; i < stable; i++) Assert.IsTrue(c.Insert(Key(200, i), CacheValue.FromObject("s"), 100, CachePriority.Low));
        Assert.AreEqual(64, c.TableSize);
        var seen = new int[stable];
        int calls = 0;
        c.ApplyToAllEntries((c, seen), ((HyperClockCache c, int[] seen) state, in CacheKey key, in CacheValue value, long charge, CacheItemHelper? helper) =>
        {
            if (key.FileNumEtc64 == Key(200, 0).FileNumEtc64) state.seen[(int)key.OffsetEtc64]++;
            // Half way through, other inserts grow the table several times.
            if (++calls == stable / 2)
            {
                for (int i = 0; i < more; i++)
                    Assert.IsTrue(state.c.Insert(Key(201, i), CacheValue.FromObject("m"), 100, CachePriority.Low));
            }
        }, averageEntriesPerLock: 1);
        Assert.IsTrue(c.TableSize >= 512, c.TableSize.ToString());
        for (int i = 0; i < stable; i++) Assert.AreEqual(1, seen[i], "entry " + i);
    }

    /// <summary>
    /// GetStartingLength / AutoHyperClockTable::CalcMaxUsableLength (cache/clock_cache.cc:1711-1719, 3535-3550): the
    /// maximum table size is rounded up to a page of slots (64), and a shard of at most 4096 bytes starts with 4
    /// slots (native: rocksdb_cache_get_table_address_count, see Audit4AutoHyperClockCompatTests). The port sized a
    /// small shard's table from capacity / min_avg_entry_charge alone (16 slots for 3000 bytes), so entries much
    /// smaller than min_avg_entry_charge were evicted for occupancy although they fit the capacity.
    /// </summary>
    [TestMethod]
    public void SmallShardStartsWithFourSlotsAndGrowsToAPage()
    {
        HyperClockCache c = NewAuto(3000, strict: false, CacheMetadataChargePolicy.DontCharge);
        Assert.AreEqual(4, c.TableSize);
        // 30 entries of 10 bytes: below upstream's occupancy limit for 64 slots (39) and far below the capacity.
        for (int i = 0; i < 30; i++) Assert.IsTrue(c.Insert(Key(600, i), CacheValue.FromObject("v"), 10, CachePriority.Low));
        Assert.AreEqual(64, c.TableSize);
        Assert.AreEqual(30, c.Count);
        for (int i = 0; i < 30; i++) Assert.IsTrue(Contains(c, Key(600, i)), i.ToString());
        Assert.AreEqual(300, c.Usage);
        // The table cannot grow beyond its maximum: further small entries evict for occupancy.
        for (int i = 30; i < 200; i++) Assert.IsTrue(c.Insert(Key(600, i), CacheValue.FromObject("v"), 10, CachePriority.Low));
        Assert.AreEqual(64, c.TableSize);
        Assert.IsTrue(c.Count <= 64 * 0.84, c.Count.ToString());
        Assert.AreEqual(c.Count * 10L, c.Usage);
        Assert.AreEqual(64, NewAuto(4097, strict: false, CacheMetadataChargePolicy.FullCharge).TableSize);
    }

    // ---- verified branches without a test -----------------------------------------------------------------------------

    /// <summary>
    /// AutoHyperClockTable::Erase / TryEraseHandle / Release (cache/clock_cache.cc:3307-3410): an erased entry that is
    /// still referenced becomes invisible and is freed by its last release, also when the table grew in between (the
    /// invisible entry moves with the others); usage and occupancy return to the table's own charge.
    /// </summary>
    [TestMethod]
    public void ErasedPinnedEntryIsFreedByItsLastReleaseAfterAGrow()
    {
        HyperClockCache c = NewAuto(8 << 20, strict: false, CacheMetadataChargePolicy.FullCharge);
        var freed = new List<int>();
        Assert.IsTrue(c.TryInsert(Key(300, 0), CacheValue.FromObject(new Tracked(0, freed)), 500, CachePriority.Low, out CachedBlock pinned));
        Assert.IsTrue(c.TryLookup(Key(300, 0), out CachedBlock second));
        Assert.IsTrue(c.Erase(Key(300, 0)));
        Assert.IsFalse(Contains(c, Key(300, 0)));
        int before = c.TableSize;
        for (int i = 1; i <= 500; i++) Assert.IsTrue(c.Insert(Key(300, i), CacheValue.FromObject(new Tracked(i, freed)), 500, CachePriority.Low));
        Assert.IsTrue(c.TableSize > before);
        Assert.AreEqual(501, c.Count);
        Assert.AreEqual(500 + HyperClockCache.SlotMetadataCharge, c.PinnedUsage);
        Assert.AreEqual(0, ((Tracked)pinned.Value!).V);
        second.Dispose();
        Assert.AreEqual(0, freed.Count);
        pinned.Dispose();
        CollectionAssert.AreEqual(new[] { 0 }, freed);
        Assert.AreEqual(500, c.Count);
        Assert.AreEqual(500L * 500 + Metadata(c), c.Usage);
        // A key inserted again after the erase is a new entry.
        Assert.IsTrue(c.Insert(Key(300, 0), CacheValue.FromObject(new Tracked(-1, freed)), 500, CachePriority.Low));
        Assert.IsTrue(c.TryLookup(Key(300, 0), out CachedBlock again));
        Assert.AreEqual(-1, ((Tracked)again.Value!).V);
        again.Release(eraseIfLastRef: true);
        Assert.IsFalse(Contains(c, Key(300, 0)));
        Assert.AreEqual(500, c.Count);
    }

    /// <summary>
    /// BaseClockTable::ChargeUsageMaybeEvictStrict (cache/clock_cache.cc:438-491) on the automatic table: an entry
    /// larger than the shard fails; with everything pinned an insert fails and leaves usage and occupancy unchanged
    /// (no standalone entry under a strict limit); after a release the same insert evicts and succeeds.
    /// </summary>
    [TestMethod]
    public void StrictLimitFailsWithoutSideEffectsWhenEverythingIsPinned()
    {
        const int entries = 20, charge = 1000;
        HyperClockCache c = NewAuto(entries * charge, strict: true, CacheMetadataChargePolicy.DontCharge);
        Assert.IsFalse(c.TryInsert(Key(400, -1), CacheValue.FromObject("big"), entries * charge + 1, CachePriority.Low, out _));
        var handles = new CachedBlock[entries];
        for (int i = 0; i < entries; i++)
            Assert.IsTrue(c.TryInsert(Key(400, i), CacheValue.FromObject("v"), charge, CachePriority.Low, out handles[i]));
        Assert.IsFalse(c.TryInsert(Key(400, entries), CacheValue.FromObject("v"), charge, CachePriority.Low, out _));
        Assert.IsFalse(c.Insert(Key(400, entries), CacheValue.FromObject("v"), charge, CachePriority.Low));
        Assert.AreEqual((long)entries * charge, c.Usage);
        Assert.AreEqual((long)entries * charge, c.PinnedUsage);
        Assert.AreEqual(entries, c.Count);
        // A strict limit implies no eviction effort cap.
        Assert.AreEqual(0, c.EvictionEffortExceededCount);
        handles[7].Dispose();
        Assert.IsTrue(c.TryInsert(Key(400, entries), CacheValue.FromObject("v"), charge, CachePriority.Low, out CachedBlock h));
        h.Dispose();
        Assert.IsFalse(Contains(c, Key(400, 7)));
        Assert.AreEqual((long)entries * charge, c.Usage);
        Assert.AreEqual(entries, c.Count);
        for (int i = 0; i < entries; i++)
        {
            if (i != 7) handles[i].Dispose();
        }
        Assert.AreEqual(0, c.PinnedUsage);
    }

    /// <summary>
    /// Lookup / Insert / Erase / Release(erase_if_last_ref) / eviction / grow running together on the automatic table:
    /// a lookup never returns another key's value, a value is freed exactly once and never while a handle to it is
    /// held, and afterwards usage, occupancy and pinned usage agree with what is left.
    /// </summary>
    [TestMethod]
    [Timeout(60_000)]
    public void ConcurrentOperationsWithEvictionAndGrowthKeepValuesAndAccountingConsistent()
    {
        const int threads = 4, keys = 6000, ops = 60_000, charge = 300;
        // Two shards, 1 MiB each: room for about 2900 entries per shard, so the tables grow and then evict.
        HyperClockCache c = NewAuto(2 << 20, strict: false, CacheMetadataChargePolicy.FullCharge, shardBits: 1);
        long created = 0, freed = 0;
        int errors = 0;
        Parallel.For(0, threads, t =>
        {
            var rnd = new Random(1234 + t);
            for (int i = 0; i < ops; i++)
            {
                int k = rnd.Next(keys);
                switch (rnd.Next(10))
                {
                    case 0:
                        c.Erase(Key(500, k));
                        break;
                    case 1:
                    case 2:
                    case 3:
                    {
                        var v = new Counted(k, () => Interlocked.Increment(ref freed));
                        Interlocked.Increment(ref created);
                        if (rnd.Next(2) == 0)
                        {
                            c.Insert(Key(500, k), CacheValue.FromObject(v), charge, (CachePriority)rnd.Next(3));
                        }
                        else if (c.TryInsert(Key(500, k), CacheValue.FromObject(v), charge, CachePriority.Low, out CachedBlock ins))
                        {
                            if (((Counted)ins.Value!).K != k || v.Freed != 0) Interlocked.Increment(ref errors);
                            ins.Release(eraseIfLastRef: rnd.Next(8) == 0);
                        }
                        else
                        {
                            Interlocked.Increment(ref errors); // not a strict limit: an insert with a handle succeeds
                        }
                        break;
                    }
                    default:
                        if (c.TryLookup(Key(500, k), out CachedBlock h))
                        {
                            var v = (Counted)h.Value!;
                            if (v.K != k || v.Freed != 0) Interlocked.Increment(ref errors);
                            Thread.SpinWait(rnd.Next(20));
                            if (v.Freed != 0) Interlocked.Increment(ref errors);
                            h.Release(eraseIfLastRef: rnd.Next(16) == 0);
                        }
                        break;
                }
            }
        });
        Assert.AreEqual(0, errors);
        Assert.AreEqual(0, c.PinnedUsage);
        Assert.IsTrue(c.TableSize > 64);
        int live = 0;
        c.ApplyToAllEntries(0, (int _, in CacheKey key, in CacheValue value, long ch, CacheItemHelper? helper) => live++, 64);
        // An erase that races with the last release can leave an invisible, unreferenced entry for the next eviction
        // (the "WART" in Unref, cache/clock_cache.cc:88): it occupies a slot but is not reported.
        Assert.IsTrue(live <= c.Count, live + " > " + c.Count);
        Assert.AreEqual(created - c.Count, Volatile.Read(ref freed));
        c.EraseUnreferencedEntries();
        Assert.AreEqual(0, c.Count);
        Assert.AreEqual(created, Volatile.Read(ref freed));
        Assert.AreEqual(0, c.Usage % (64L * HyperClockCache.SlotMetadataCharge)); // only the tables' metadata is left
        Assert.IsTrue(c.Usage >= 2L * 64 * HyperClockCache.SlotMetadataCharge);
    }

    private sealed class Tracked(int v, List<int> freed) : IDisposable
    {
        public readonly int V = v;

        public void Dispose()
        {
            lock (freed) freed.Add(V);
        }
    }

    private sealed class Counted(int k, Action onFreed) : IDisposable
    {
        public readonly int K = k;
        public int Freed;

        public void Dispose()
        {
            if (Interlocked.Increment(ref Freed) == 1) onFreed();
        }
    }
}
