using RocksDb.Managed.Cache;

namespace RocksDb.Managed.Tests.Cache;

/// <summary>Ports of cache/cache_test.cc (CacheTest, LRU flavour) and cache/lru_cache_test.cc.</summary>
[TestClass]
public sealed class LruCacheTests
{
    private const int CacheSize = 1000;
    private const int NumShardBits = 4;

    private readonly List<int> _deleted = [];

    /// <summary>Value whose disposal records it in <see cref="_deleted"/> (the test Deleter).</summary>
    private sealed class TrackedValue(int v, List<int> deleted) : IDisposable
    {
        public readonly int V = v;

        public void Dispose()
        {
            lock (deleted) deleted.Add(V);
        }
    }

    private static CacheKey Key(int k) => new(0x1234, (ulong)k);

    private static BlockCache NewCache(long capacity, int shardBits, bool strict, double highPri = 0.5, double lowPri = 0.0) =>
        BlockCache.CreateLru(new LruCacheOptions
        {
            Capacity = capacity,
            NumShardBits = shardBits,
            StrictCapacityLimit = strict,
            HighPriPoolRatio = highPri,
            LowPriPoolRatio = lowPri,
            MetadataChargePolicy = CacheMetadataChargePolicy.DontCharge,
        });

    private CacheValue V(int v) => CacheValue.FromObject(new TrackedValue(v, _deleted));

    private static int Lookup(BlockCache cache, int key)
    {
        if (!cache.TryLookup(Key(key), out CachedBlock h)) return -1;
        using (h) return ((TrackedValue)h.Value!).V;
    }

    private void Insert(BlockCache cache, int key, int value, int charge = 1, CachePriority pri = CachePriority.High) =>
        Assert.IsTrue(cache.Insert(Key(key), V(value), charge, pri));

    [TestMethod]
    public void HitAndMiss()
    {
        BlockCache c = NewCache(CacheSize, NumShardBits, false);
        Assert.AreEqual(-1, Lookup(c, 100));
        Insert(c, 100, 101);
        Assert.AreEqual(101, Lookup(c, 100));
        Assert.AreEqual(-1, Lookup(c, 200));
        Insert(c, 200, 201);
        Assert.AreEqual(101, Lookup(c, 100));
        Assert.AreEqual(201, Lookup(c, 200));
        Insert(c, 100, 102);
        Assert.AreEqual(102, Lookup(c, 100));
        Assert.AreEqual(201, Lookup(c, 200));
        Assert.AreEqual(-1, Lookup(c, 300));
        CollectionAssert.AreEqual(new[] { 101 }, _deleted);
    }

    [TestMethod]
    public void Erase()
    {
        BlockCache c = NewCache(CacheSize, NumShardBits, false);
        Assert.IsFalse(c.Erase(Key(200)));
        Insert(c, 100, 101);
        Insert(c, 200, 201);
        Assert.IsTrue(c.Erase(Key(100)));
        Assert.AreEqual(-1, Lookup(c, 100));
        Assert.AreEqual(201, Lookup(c, 200));
        CollectionAssert.AreEqual(new[] { 101 }, _deleted);
        c.Erase(Key(100));
        CollectionAssert.AreEqual(new[] { 101 }, _deleted);
    }

    [TestMethod]
    public void EntriesArePinned()
    {
        BlockCache c = NewCache(CacheSize, NumShardBits, false);
        Insert(c, 100, 101);
        Assert.IsTrue(c.TryLookup(Key(100), out CachedBlock h1));
        Assert.AreEqual(101, ((TrackedValue)h1.Value!).V);
        Assert.AreEqual(1, c.Usage);

        Insert(c, 100, 102);
        Assert.IsTrue(c.TryLookup(Key(100), out CachedBlock h2));
        Assert.AreEqual(102, ((TrackedValue)h2.Value!).V);
        Assert.AreEqual(0, _deleted.Count);
        Assert.AreEqual(2, c.Usage);

        h1.Dispose();
        CollectionAssert.AreEqual(new[] { 101 }, _deleted);
        Assert.AreEqual(1, c.Usage);

        c.Erase(Key(100));
        Assert.AreEqual(-1, Lookup(c, 100));
        Assert.AreEqual(1, _deleted.Count);
        Assert.AreEqual(1, c.Usage);

        h2.Dispose();
        CollectionAssert.AreEqual(new[] { 101, 102 }, _deleted);
        Assert.AreEqual(0, c.Usage);
    }

    [TestMethod]
    public void EvictionPolicy()
    {
        BlockCache c = NewCache(CacheSize, NumShardBits, false);
        Insert(c, 100, 101);
        Insert(c, 200, 201);
        for (int i = 0; i < 2 * CacheSize; i++)
        {
            Insert(c, 1000 + i, 2000 + i);
            Assert.AreEqual(101, Lookup(c, 100));
        }
        Assert.AreEqual(101, Lookup(c, 100));
        Assert.AreEqual(-1, Lookup(c, 200));
    }

    [TestMethod]
    public void ExternalRefPinsEntries()
    {
        BlockCache c = NewCache(CacheSize, NumShardBits, false);
        Insert(c, 100, 101);
        Assert.IsTrue(c.TryLookup(Key(100), out CachedBlock h));
        CachedBlock h2 = h.AddRef();
        Assert.AreEqual(1, c.Usage);
        for (int i = 0; i < 3; i++)
        {
            if (i == 1) h2.Dispose();
            if (i == 2) h.Dispose();
            for (int j = 0; j < 2 * CacheSize + 100; j++) Insert(c, 1000 + j, 2000 + j);
            if (i < 2) Assert.AreEqual(101, Lookup(c, 100));
        }
        Assert.AreEqual(-1, Lookup(c, 100));
    }

    [TestMethod]
    public void EvictionPolicyRef()
    {
        BlockCache c = NewCache(CacheSize, NumShardBits, false);
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
    public void EvictEmptyCacheAndErasedHandleState()
    {
        BlockCache tiny = NewCache(1, 0, false);
        Assert.IsFalse(tiny.Insert(Key(1000), V(1), 10, CachePriority.Low)); // freed immediately
        CollectionAssert.AreEqual(new[] { 1 }, _deleted);

        BlockCache c = NewCache(CacheSize, NumShardBits, false);
        Insert(c, 100, 1000);
        Assert.IsTrue(c.TryLookup(Key(100), out CachedBlock h1));
        Assert.IsTrue(c.TryLookup(Key(100), out CachedBlock h2));
        c.Erase(Key(100));
        Assert.AreEqual(-1, Lookup(c, 100));
        h1.Dispose();
        Assert.AreEqual(-1, Lookup(c, 100));
        Assert.AreEqual(1000, ((TrackedValue)h2.Value!).V);
        h2.Dispose();
        CollectionAssert.AreEqual(new[] { 1, 1000 }, _deleted);
    }

    [TestMethod]
    public void EraseFromDeleter()
    {
        BlockCache c = NewCache(10, 0, false);
        Assert.IsTrue(c.Insert(Key(1234), CacheValue.FromObject(new object()), 1, CachePriority.Low));
        Assert.IsTrue(c.Insert(Key(5678), CacheValue.FromObject(new ActionOnDispose(() => c.Erase(Key(1234)))), 1, CachePriority.Low));
        c.Erase(Key(5678));
        Assert.IsFalse(c.TryLookup(Key(1234), out _));
        Assert.IsFalse(c.TryLookup(Key(5678), out _));
    }

    private sealed class ActionOnDispose(Action a) : IDisposable
    {
        public void Dispose() => a();
    }

    [TestMethod]
    public void HeavyEntries()
    {
        BlockCache c = NewCache(CacheSize, NumShardBits, false);
        const int light = 1, heavy = 10;
        int added = 0, index = 0;
        while (added < 2 * CacheSize)
        {
            int weight = (index & 1) != 0 ? light : heavy;
            Insert(c, index, 1000 + index, weight);
            added += weight;
            index++;
        }
        int cachedWeight = 0;
        for (int i = 0; i < index; i++)
        {
            int weight = (i & 1) != 0 ? light : heavy;
            int r = Lookup(c, i);
            if (r >= 0)
            {
                cachedWeight += weight;
                Assert.AreEqual(1000 + i, r);
            }
        }
        Assert.IsTrue(cachedWeight <= CacheSize + CacheSize / 10, "cached weight " + cachedWeight);
    }

    [TestMethod]
    public void NewIdAndReleaseAndErase()
    {
        BlockCache c = NewCache(5, 0, false);
        Assert.AreNotEqual(c.NewId(), c.NewId());
        Assert.IsTrue(c.TryInsert(Key(100), V(100), 1, CachePriority.Low, out CachedBlock h));
        Assert.AreEqual(5, c.Capacity);
        Assert.AreEqual(1, c.Usage);
        h.Release(eraseIfLastRef: true);
        CollectionAssert.AreEqual(new[] { 100 }, _deleted);
        Assert.AreEqual(0, c.Usage);

        Assert.IsTrue(c.TryInsert(Key(101), V(101), 1, CachePriority.Low, out h));
        h.Dispose(); // ReleaseWithoutErase: stays cached while there is free capacity
        Assert.AreEqual(1, _deleted.Count);
        Assert.AreEqual(101, Lookup(c, 101));
    }

    [TestMethod]
    public void SetCapacity()
    {
        BlockCache c = NewCache(5, 0, false);
        var handles = new CachedBlock[10];
        for (int i = 0; i < 5; i++) Assert.IsTrue(c.TryInsert(Key(i + 1), V(i + 1), 1, CachePriority.Low, out handles[i]));
        Assert.AreEqual(5, c.Usage);
        c.SetCapacity(10);
        Assert.AreEqual(10, c.Capacity);
        Assert.AreEqual(5, c.Usage);
        for (int i = 5; i < 10; i++) Assert.IsTrue(c.TryInsert(Key(i + 1), V(i + 1), 1, CachePriority.Low, out handles[i]));
        Assert.AreEqual(10, c.Usage);
        for (int i = 0; i < 5; i++) handles[i].Dispose();
        Assert.AreEqual(10, c.Usage);
        c.SetCapacity(7);
        Assert.AreEqual(7, c.Capacity);
        Assert.AreEqual(7, c.Usage);
        for (int i = 5; i < 10; i++) handles[i].Dispose();
        Assert.AreEqual(7, c.Usage);
        Assert.AreEqual(0, c.PinnedUsage);
    }

    [TestMethod]
    public void SetStrictCapacityLimit()
    {
        // With non-strict limit, inserts with handle succeed beyond capacity; with strict, they fail.
        BlockCache c = NewCache(5, 0, false);
        var handles = new CachedBlock[10];
        for (int i = 0; i < 10; i++) Assert.IsTrue(c.TryInsert(Key(i + 1), V(i + 1), 1, CachePriority.Low, out handles[i]));
        Assert.AreEqual(10, c.Usage);
        c.SetStrictCapacityLimit(true);
        CacheValue v = V(11);
        Assert.IsFalse(c.TryInsert(Key(11), v, 1, CachePriority.Low, out CachedBlock failed));
        Assert.IsFalse(failed.HasValue);
        Assert.AreEqual(0, _deleted.Count); // caller keeps ownership on failure
        Assert.IsFalse(c.Insert(Key(11), v, 1, CachePriority.Low)); // no handle: owned and freed
        CollectionAssert.AreEqual(new[] { 11 }, _deleted);
        for (int i = 0; i < 10; i++) handles[i].Dispose();

        BlockCache c2 = NewCache(5, 0, true);
        for (int i = 0; i < 5; i++) Assert.IsTrue(c2.TryInsert(Key(i + 1), V(i + 1), 1, CachePriority.Low, out handles[i]));
        Assert.IsFalse(c2.TryInsert(Key(6), V(6), 1, CachePriority.Low, out _));
        Assert.AreEqual(5, c2.Usage);
        for (int i = 0; i < 5; i++) handles[i].Dispose();
    }

    [TestMethod]
    public void OverCapacity()
    {
        const int n = 10;
        BlockCache c = NewCache(n, 0, false);
        var handles = new CachedBlock[n + 1];
        for (int i = 0; i < n + 1; i++) Assert.IsTrue(c.TryInsert(Key(i + 1), V(i + 1), 1, CachePriority.Low, out handles[i]));
        Assert.AreEqual(n + 1, c.Usage);
        for (int i = 0; i < n + 1; i++) handles[i].Dispose();
        // The last released entry is removed since usage > capacity at release time; then usage == capacity.
        Assert.AreEqual(n, c.Usage);
        Assert.AreEqual(1, _deleted.Count);
        Assert.AreEqual(1, _deleted[0]);
    }

    [TestMethod]
    public void DefaultShardBits()
    {
        // Mirrors CacheTest.DefaultShardBits for the 512 KiB LRU minimum shard size.
        Assert.AreEqual(2, ((LruCache)BlockCache.CreateLru(1L << 20)).NumShards);
        Assert.AreEqual(0, BlockCache.GetDefaultCacheShardBits(1000, 512 * 1024));
        Assert.AreEqual(2, BlockCache.GetDefaultCacheShardBits(2L << 20, 512 * 1024));
        Assert.AreEqual(6, BlockCache.GetDefaultCacheShardBits(1L << 40, 512 * 1024));
    }

    [TestMethod]
    public void PooledBuffersReturnedAndSpanAccess()
    {
        BlockCache c = NewCache(1 << 20, 2, false);
        byte[] rented = RocksArrayPool<byte>.Shared.Rent(100);
        "hello block"u8.CopyTo(rented);
        Assert.IsTrue(c.TryInsert(Key(1), CacheValue.FromPooledBuffer(rented, 11), 11, CachePriority.Low, out CachedBlock h));
        Assert.IsTrue(h.Span.SequenceEqual("hello block"u8));
        Assert.AreEqual(11, h.Length);
        Assert.AreEqual(11, h.Memory.Length);
        Assert.AreEqual(Key(1), h.Key);
        h.Dispose();
        h.Dispose(); // idempotent on the same variable
        Assert.ThrowsExactly<InvalidOperationException>(() => _ = h.Length);
        Assert.IsTrue(c.TryLookup(Key(1), out CachedBlock h2));
        CachedBlock copy = h2;
        h2.Dispose();
        Assert.ThrowsExactly<InvalidOperationException>(() => copy.Dispose()); // double release via a copy
        c.EraseUnreferencedEntries();
        Assert.AreEqual(0, c.Count);
        Assert.AreEqual(0, c.Usage);
    }

    [TestMethod]
    public void MetadataChargeIsAccounted()
    {
        BlockCache c = BlockCache.CreateLru(new LruCacheOptions { Capacity = 1 << 20, NumShardBits = 0 });
        Assert.IsTrue(c.Insert(Key(1), CacheValue.FromObject(new object()), 100, CachePriority.Low));
        Assert.AreEqual(100 + LruCache.EntryMetadataCharge, c.Usage);
    }

    // ---- lru_cache_test.cc ----

    private static string Name(CacheKey k) => ((char)k.OffsetEtc64).ToString();

    private static void ValidateLruList(BlockCache cache, string[] keys, int numHigh, int numLow, int numBottom)
    {
        List<(CacheKey Key, int Pool)> list = ((LruCache)cache).GetShardForTest(Key('a')).GetLruListForTest();
        Assert.AreEqual(keys.Length, list.Count, "lru list: " + string.Join(",", list.ConvertAll(e => Name(e.Key))));
        int h = 0, l = 0, b = 0;
        int lastPool = 0;
        for (int i = 0; i < keys.Length; i++)
        {
            Assert.AreEqual(keys[i], Name(list[i].Key));
            Assert.IsTrue(list[i].Pool >= lastPool, "pools must be ordered bottom < low < high");
            lastPool = list[i].Pool;
            if (list[i].Pool == 2) h++;
            else if (list[i].Pool == 1) l++;
            else b++;
        }
        Assert.AreEqual(numHigh, h, "high");
        Assert.AreEqual(numLow, l, "low");
        Assert.AreEqual(numBottom, b, "bottom");
    }

    private static void InsertChar(BlockCache c, char ch, CachePriority pri = CachePriority.Low) =>
        Assert.IsTrue(c.Insert(Key(ch), CacheValue.FromObject(new object()), 1, pri));

    private static bool LookupChar(BlockCache c, char ch)
    {
        if (!c.TryLookup(Key(ch), out CachedBlock h)) return false;
        h.Dispose();
        return true;
    }

    [TestMethod]
    public void BasicLru()
    {
        BlockCache c = NewCache(5, 0, false, 0.0, 1.0);
        for (char ch = 'a'; ch <= 'e'; ch++) InsertChar(c, ch);
        ValidateLruList(c, ["a", "b", "c", "d", "e"], 0, 5, 0);
        for (char ch = 'x'; ch <= 'z'; ch++) InsertChar(c, ch);
        ValidateLruList(c, ["d", "e", "x", "y", "z"], 0, 5, 0);
        Assert.IsFalse(LookupChar(c, 'b'));
        Assert.IsTrue(LookupChar(c, 'e'));
        ValidateLruList(c, ["d", "x", "y", "z", "e"], 0, 5, 0);
        Assert.IsTrue(LookupChar(c, 'z'));
        ValidateLruList(c, ["d", "x", "y", "e", "z"], 0, 5, 0);
        c.Erase(Key('x'));
        ValidateLruList(c, ["d", "y", "e", "z"], 0, 4, 0);
        Assert.IsTrue(LookupChar(c, 'd'));
        ValidateLruList(c, ["y", "e", "z", "d"], 0, 4, 0);
        InsertChar(c, 'u');
        ValidateLruList(c, ["y", "e", "z", "d", "u"], 0, 5, 0);
        InsertChar(c, 'v');
        ValidateLruList(c, ["e", "z", "d", "u", "v"], 0, 5, 0);
    }

    [TestMethod]
    public void LowPriorityMidpointInsertion()
    {
        BlockCache c = NewCache(5, 0, false, 0.40, 0.60);
        InsertChar(c, 'a');
        InsertChar(c, 'b');
        InsertChar(c, 'c');
        InsertChar(c, 'x', CachePriority.High);
        InsertChar(c, 'y', CachePriority.High);
        ValidateLruList(c, ["a", "b", "c", "x", "y"], 2, 3, 0);
        InsertChar(c, 'd');
        ValidateLruList(c, ["b", "c", "d", "x", "y"], 2, 3, 0);
        Assert.IsTrue(LookupChar(c, 'd'));
        ValidateLruList(c, ["b", "c", "x", "y", "d"], 2, 3, 0);
        InsertChar(c, 'z', CachePriority.High);
        ValidateLruList(c, ["c", "x", "y", "d", "z"], 2, 3, 0);
    }

    [TestMethod]
    public void BottomPriorityMidpointInsertion()
    {
        BlockCache c = NewCache(6, 0, false, 0.35, 0.35);
        InsertChar(c, 'a', CachePriority.Bottom);
        InsertChar(c, 'b', CachePriority.Bottom);
        InsertChar(c, 'i');
        InsertChar(c, 'j');
        InsertChar(c, 'x', CachePriority.High);
        InsertChar(c, 'y', CachePriority.High);
        ValidateLruList(c, ["a", "b", "i", "j", "x", "y"], 2, 2, 2);
        InsertChar(c, 'k');
        ValidateLruList(c, ["b", "i", "j", "k", "x", "y"], 2, 2, 2);
        Assert.IsTrue(LookupChar(c, 'k'));
        ValidateLruList(c, ["b", "i", "j", "x", "y", "k"], 2, 2, 2);
        InsertChar(c, 'z', CachePriority.High);
        ValidateLruList(c, ["i", "j", "x", "y", "k", "z"], 2, 2, 2);
        c.Erase(Key('x'));
        ValidateLruList(c, ["i", "j", "y", "k", "z"], 2, 1, 2);
        c.Erase(Key('y'));
        ValidateLruList(c, ["i", "j", "k", "z"], 2, 0, 2);
        InsertChar(c, 'c', CachePriority.Bottom);
        ValidateLruList(c, ["i", "j", "c", "k", "z"], 2, 0, 3);
        InsertChar(c, 'd', CachePriority.Bottom);
        ValidateLruList(c, ["i", "j", "c", "d", "k", "z"], 2, 0, 4);
        InsertChar(c, 'e', CachePriority.Bottom);
        ValidateLruList(c, ["j", "c", "d", "e", "k", "z"], 2, 0, 4);
        InsertChar(c, 'l');
        ValidateLruList(c, ["c", "d", "e", "l", "k", "z"], 2, 1, 3);
        InsertChar(c, 'm');
        ValidateLruList(c, ["d", "e", "l", "m", "k", "z"], 2, 2, 2);
        c.Erase(Key('k'));
        ValidateLruList(c, ["d", "e", "l", "m", "z"], 1, 2, 2);
        c.Erase(Key('z'));
        ValidateLruList(c, ["d", "e", "l", "m"], 0, 2, 2);
        InsertChar(c, 'f', CachePriority.Bottom);
        ValidateLruList(c, ["d", "e", "f", "l", "m"], 0, 2, 3);
        InsertChar(c, 'g', CachePriority.Bottom);
        ValidateLruList(c, ["d", "e", "f", "g", "l", "m"], 0, 2, 4);
        InsertChar(c, 'o', CachePriority.High);
        ValidateLruList(c, ["e", "f", "g", "l", "m", "o"], 1, 2, 3);
        InsertChar(c, 'p', CachePriority.High);
        ValidateLruList(c, ["f", "g", "l", "m", "o", "p"], 2, 2, 2);
    }

    [TestMethod]
    public void EntriesWithPriority()
    {
        BlockCache c = NewCache(6, 0, false, 0.35, 0.35);
        InsertChar(c, 'a');
        InsertChar(c, 'b');
        ValidateLruList(c, ["a", "b"], 0, 2, 0);
        InsertChar(c, 'c');
        ValidateLruList(c, ["a", "b", "c"], 0, 2, 1);
        InsertChar(c, 't');
        InsertChar(c, 'u');
        ValidateLruList(c, ["a", "b", "c", "t", "u"], 0, 2, 3);
        InsertChar(c, 'v');
        ValidateLruList(c, ["a", "b", "c", "t", "u", "v"], 0, 2, 4);
        InsertChar(c, 'w');
        ValidateLruList(c, ["b", "c", "t", "u", "v", "w"], 0, 2, 4);
        InsertChar(c, 'X', CachePriority.High);
        InsertChar(c, 'Y', CachePriority.High);
        ValidateLruList(c, ["t", "u", "v", "w", "X", "Y"], 2, 2, 2);
        InsertChar(c, 'Z', CachePriority.High);
        ValidateLruList(c, ["u", "v", "w", "X", "Y", "Z"], 2, 2, 2);
        InsertChar(c, 'a');
        ValidateLruList(c, ["v", "w", "X", "a", "Y", "Z"], 2, 2, 2);
    }

    [TestMethod]
    public void ConcurrentStress()
    {
        BlockCache c = BlockCache.CreateLru(new LruCacheOptions { Capacity = 64 * 1024, NumShardBits = 3 });
        const int threads = 4;
        var deadline = DateTime.UtcNow.AddSeconds(2);
        long ops = 0;
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
                            int k = rnd.Next(0, 2000);
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
                                byte[] buf = RocksArrayPool<byte>.Shared.Rent(64);
                                buf.AsSpan(0, 64).Fill((byte)k);
                                c.Insert(Key(k), CacheValue.FromPooledBuffer(buf, 64), 64, rnd.Next(2) == 0 ? CachePriority.High : CachePriority.Low);
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
                        Interlocked.Add(ref ops, 1000);
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
        Assert.IsTrue(c.Usage <= c.Capacity, "usage " + c.Usage);
        c.EraseUnreferencedEntries();
        Assert.AreEqual(0, c.Usage);
        Assert.AreEqual(0, c.Count);
        Assert.IsTrue(ops > 0);
    }
}
