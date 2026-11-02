using RocksDb.Managed.Cache;

namespace RocksDb.Managed.Tests.Cache;

/// <summary>Ports of cache/cache_test.cc (HyperClock flavour) and parts of clock_cache_test.cc.</summary>
[TestClass]
public sealed class HyperClockCacheTests
{
    private const int CacheSize = 1000;
    private readonly List<int> _deleted = [];

    private sealed class TrackedValue(int v, List<int> deleted) : IDisposable
    {
        public readonly int V = v;

        public void Dispose()
        {
            lock (deleted) deleted.Add(V);
        }
    }

    private static CacheKey Key(int k) => new(0xABCD, (ulong)k);

    private static HyperClockCache NewCache(long capacity, int shardBits, bool strict) =>
        new(new HyperClockCacheOptions
        {
            Capacity = capacity,
            EstimatedEntryCharge = 1,
            NumShardBits = shardBits,
            StrictCapacityLimit = strict,
            MetadataChargePolicy = CacheMetadataChargePolicy.DontCharge,
        });

    private CacheValue V(int v) => CacheValue.FromObject(new TrackedValue(v, _deleted));

    private static int Lookup(BlockCache cache, int key)
    {
        if (!cache.TryLookup(Key(key), out CachedBlock h)) return -1;
        using (h) return ((TrackedValue)h.Value!).V;
    }

    private void Insert(BlockCache cache, int key, int value, int charge = 1) =>
        cache.Insert(Key(key), V(value), charge, CachePriority.High);

    [TestMethod]
    public void HitAndMissDoesNotOverwrite()
    {
        HyperClockCache c = NewCache(CacheSize, 0, false);
        Assert.AreEqual(-1, Lookup(c, 100));
        Insert(c, 100, 101);
        Assert.AreEqual(101, Lookup(c, 100));
        Assert.AreEqual(-1, Lookup(c, 200));
        Insert(c, 200, 201);
        Assert.AreEqual(201, Lookup(c, 200));
        Insert(c, 100, 102); // ClockCache usually doesn't overwrite on Insert
        Assert.AreEqual(101, Lookup(c, 100));
        CollectionAssert.AreEqual(new[] { 102 }, _deleted);
    }

    [TestMethod]
    public void EraseAndPinned()
    {
        HyperClockCache c = NewCache(CacheSize, 0, false);
        Assert.IsFalse(c.Erase(Key(200)));
        Insert(c, 100, 101);
        Insert(c, 200, 201);
        Assert.IsTrue(c.Erase(Key(100)));
        Assert.AreEqual(-1, Lookup(c, 100));
        Assert.AreEqual(201, Lookup(c, 200));
        CollectionAssert.AreEqual(new[] { 101 }, _deleted);

        Assert.IsTrue(c.TryLookup(Key(200), out CachedBlock h1));
        Assert.IsTrue(c.TryLookup(Key(200), out CachedBlock h2));
        Assert.AreEqual(1, c.PinnedUsage);
        c.Erase(Key(200));
        Assert.AreEqual(-1, Lookup(c, 200));
        h1.Dispose();
        Assert.AreEqual(1, _deleted.Count);
        Assert.AreEqual(201, ((TrackedValue)h2.Value!).V);
        h2.Dispose();
        CollectionAssert.AreEqual(new[] { 101, 201 }, _deleted);
        Assert.AreEqual(0, c.Usage);
        Assert.AreEqual(0, c.Count);
    }

    [TestMethod]
    public void DuplicateInsertWithHandleIsStandalone()
    {
        HyperClockCache c = NewCache(CacheSize, 0, false);
        Insert(c, 1, 10);
        Assert.IsTrue(c.TryInsert(Key(1), V(11), 1, CachePriority.Low, out CachedBlock h));
        Assert.AreEqual(11, ((TrackedValue)h.Value!).V);
        Assert.AreEqual(10, Lookup(c, 1)); // visible entry unchanged
        Assert.AreEqual(2, c.Usage);
        h.Dispose();
        CollectionAssert.AreEqual(new[] { 11 }, _deleted);
        Assert.AreEqual(1, c.Usage);
    }

    [TestMethod]
    public void EvictionPolicyKeepsFrequentlyUsed()
    {
        HyperClockCache c = NewCache(CacheSize, 0, false);
        Insert(c, 100, 101);
        Insert(c, 200, 201);
        for (int i = 0; i < 2 * CacheSize; i++)
        {
            Insert(c, 1000 + i, 2000 + i);
            Assert.AreEqual(101, Lookup(c, 100));
        }
        Assert.AreEqual(101, Lookup(c, 100));
        Assert.AreEqual(-1, Lookup(c, 200));
        Assert.IsTrue(c.Usage <= CacheSize + 1, "usage " + c.Usage);
    }

    [TestMethod]
    public void EvictionPolicyRef()
    {
        HyperClockCache c = NewCache(CacheSize, 0, false);
        for (int i = 0; i < 4; i++) Insert(c, 100 + i, 101 + i);
        for (int i = 0; i < 4; i++) Insert(c, 200 + i, 101 + i);
        var handles = new CachedBlock[4];
        for (int i = 0; i < 4; i++) Assert.IsTrue(c.TryLookup(Key(200 + i), out handles[i]));
        for (int i = 0; i < 4; i++) Insert(c, 300 + i, 101 + i);
        for (int i = 0; i < 100 * CacheSize; i++) Insert(c, 1000 + i, 2000 + i);
        for (int i = 0; i < 4; i++)
        {
            Assert.AreEqual(-1, Lookup(c, 100 + i));
            Assert.AreEqual(-1, Lookup(c, 300 + i));
            Assert.AreEqual(101 + i, Lookup(c, 200 + i));
        }
        for (int i = 0; i < 4; i++) handles[i].Dispose();
    }

    [TestMethod]
    public void HeavyEntries()
    {
        HyperClockCache c = NewCache(CacheSize, 0, false);
        int added = 0, index = 0;
        while (added < 2 * CacheSize)
        {
            int weight = (index & 1) != 0 ? 1 : 10;
            Insert(c, index, 1000 + index, weight);
            added += weight;
            index++;
        }
        int cachedWeight = 0;
        for (int i = 0; i < index; i++)
        {
            int r = Lookup(c, i);
            if (r >= 0)
            {
                cachedWeight += (i & 1) != 0 ? 1 : 10;
                Assert.AreEqual(1000 + i, r);
            }
        }
        Assert.IsTrue(cachedWeight <= CacheSize + CacheSize / 10, "cached weight " + cachedWeight);
    }

    [TestMethod]
    public void ReleaseAndEraseAndStrictLimit()
    {
        HyperClockCache c = NewCache(5, 0, false);
        Assert.IsTrue(c.TryInsert(Key(100), V(100), 1, CachePriority.Low, out CachedBlock h));
        Assert.AreEqual(1, c.Usage);
        h.Release(eraseIfLastRef: true);
        CollectionAssert.AreEqual(new[] { 100 }, _deleted);
        Assert.AreEqual(0, c.Usage);

        HyperClockCache strict = NewCache(5, 0, true);
        var handles = new CachedBlock[5];
        for (int i = 0; i < 5; i++) Assert.IsTrue(strict.TryInsert(Key(i), V(i), 1, CachePriority.Low, out handles[i]));
        Assert.IsFalse(strict.TryInsert(Key(10), V(10), 1, CachePriority.Low, out _));
        Assert.AreEqual(5, strict.Usage);
        handles[0].Dispose();
        // Key 0 is now unreferenced but still has clock countdown left; a strict insert sweeps the clock at most
        // kMaxCountdown times per attempt, so it may take a second attempt to age it out (as in C++).
        CachedBlock h11 = default;
        bool inserted = false;
        for (int attempt = 0; attempt < 3 && !inserted; attempt++)
            inserted = strict.TryInsert(Key(11), CacheValue.FromObject(new object()), 1, CachePriority.Low, out h11);
        Assert.IsTrue(inserted);
        Assert.AreEqual(-1, Lookup(strict, 0));
        Assert.AreEqual(5, strict.Usage);
        h11.Dispose();
        for (int i = 1; i < 5; i++) handles[i].Dispose();
        Assert.AreEqual(0, strict.PinnedUsage);
    }

    [TestMethod]
    public void DoubleReleaseDetected()
    {
        HyperClockCache c = NewCache(CacheSize, 0, false);
        Insert(c, 1, 1);
        Assert.IsTrue(c.TryLookup(Key(1), out CachedBlock h));
        CachedBlock copy = h;
        h.Dispose();
        Assert.ThrowsExactly<InvalidOperationException>(() => copy.Dispose());
    }

    [TestMethod]
    public void PooledBuffersAndEraseUnreferenced()
    {
        var c = new HyperClockCache(new HyperClockCacheOptions { Capacity = 1 << 20, EstimatedEntryCharge = 4096 });
        for (int i = 0; i < 100; i++)
        {
            byte[] b = System.Buffers.ArrayPool<byte>.Shared.Rent(4096);
            b[0] = (byte)i;
            Assert.IsTrue(c.Insert(Key(i), CacheValue.FromPooledBuffer(b, 4096), 4096, CachePriority.Low));
        }
        Assert.IsTrue(c.TryLookup(Key(7), out CachedBlock h));
        Assert.AreEqual(7, h.Span[0]);
        c.EraseUnreferencedEntries();
        Assert.AreEqual(1, c.Count);
        Assert.AreEqual(4096 + LruCache.EntryMetadataCharge, c.Usage);
        h.Dispose();
        c.EraseUnreferencedEntries();
        Assert.AreEqual(0, c.Count);
        Assert.AreEqual(0, c.Usage);
    }

    [TestMethod]
    public void ConcurrentStress()
    {
        var c = new HyperClockCache(new HyperClockCacheOptions { Capacity = 64 * 1024, EstimatedEntryCharge = 64, NumShardBits = 2 });
        const int threads = 4;
        DateTime deadline = DateTime.UtcNow.AddSeconds(2);
        var errors = new List<Exception>();
        var workers = new Thread[threads];
        for (int t = 0; t < threads; t++)
        {
            int seed = t;
            workers[t] = new Thread(() =>
            {
                try
                {
                    var rnd = new Random(seed);
                    var held = new List<CachedBlock>();
                    while (DateTime.UtcNow < deadline)
                    {
                        for (int i = 0; i < 1000; i++)
                        {
                            int k = rnd.Next(0, 3000);
                            int op = rnd.Next(0, 10);
                            if (op < 6)
                            {
                                if (c.TryLookup(Key(k), out CachedBlock h))
                                {
                                    ReadOnlySpan<byte> s = h.Span;
                                    if (s.Length != 64 || s[0] != (byte)k || s[63] != (byte)k) throw new InvalidOperationException("corrupt value");
                                    if (held.Count < 8 && rnd.Next(4) == 0) held.Add(h);
                                    else h.Dispose();
                                }
                            }
                            else if (op < 9)
                            {
                                byte[] buf = System.Buffers.ArrayPool<byte>.Shared.Rent(64);
                                buf.AsSpan(0, 64).Fill((byte)k);
                                if (rnd.Next(2) == 0)
                                {
                                    c.Insert(Key(k), CacheValue.FromPooledBuffer(buf, 64), 64, CachePriority.Low);
                                }
                                else if (c.TryInsert(Key(k), CacheValue.FromPooledBuffer(buf, 64), 64, CachePriority.High, out CachedBlock h))
                                {
                                    if (!h.Span.SequenceEqual(buf.AsSpan(0, 64))) throw new InvalidOperationException("corrupt insert");
                                    h.Dispose();
                                }
                                else
                                {
                                    System.Buffers.ArrayPool<byte>.Shared.Return(buf);
                                }
                            }
                            else
                            {
                                c.Erase(Key(k));
                            }
                            if (held.Count > 0 && rnd.Next(3) == 0)
                            {
                                CachedBlock x = held[^1];
                                held.RemoveAt(held.Count - 1);
                                x.Dispose();
                            }
                        }
                    }
                    foreach (CachedBlock x in held) x.Dispose();
                }
                catch (Exception e)
                {
                    lock (errors) errors.Add(e);
                }
            });
            workers[t].Start();
        }
        foreach (Thread w in workers) w.Join();
        if (errors.Count > 0) throw errors[0];
        Assert.AreEqual(0, c.PinnedUsage);
        c.EraseUnreferencedEntries();
        Assert.AreEqual(0, c.Count);
        Assert.AreEqual(0, c.Usage);
    }
}
