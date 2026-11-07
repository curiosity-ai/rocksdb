using System.Buffers;
using RocksDb.Managed.Cache;
using RocksDb.Managed.Compression;

namespace RocksDb.Managed.Tests.Cache;

/// <summary>
/// Ports of cache/compressed_secondary_cache_test.cc, cache/secondary_cache_adapter_test.cc / lru_cache_test.cc
/// (DBSecondaryCacheTest-style adapter cases) and cache/tiered_secondary_cache_test.cc, plus concurrency checks.
/// </summary>
[TestClass]
public sealed class SecondaryCacheTests
{
    private static readonly BytesCacheItemHelper s_helper = new(CacheEntryRole.DataBlock);

    private static CacheKey Key(int i) => new(0x77, (ulong)i);

    private static byte[] KeyBytes(int i)
    {
        byte[] k = new byte[CacheKey.Size];
        Key(i).WriteTo(k);
        return k;
    }

    /// <summary>Compressible test payload of <paramref name="size"/> bytes derived from <paramref name="seed"/>.</summary>
    private static byte[] Payload(int seed, int size)
    {
        byte[] b = new byte[size];
        for (int i = 0; i < size; i++) b[i] = (byte)('a' + (seed + i / 16) % 7);
        return b;
    }

    private static CacheValue Value(byte[] payload)
    {
        byte[] buf = ArrayPool<byte>.Shared.Rent(payload.Length);
        payload.CopyTo(buf, 0);
        return CacheValue.FromPooledBuffer(buf, payload.Length).WithHelper(s_helper);
    }

    private static byte[] ReadBack(in CachedBlock h) => h.Span.ToArray();

    private static CompressedSecondaryCache NewCompressed(long capacity, CompressionType type = CompressionType.LZ4, bool splitMerge = false) =>
        new(new CompressedSecondaryCacheOptions
        {
            Capacity = capacity, NumShardBits = 0, MetadataChargePolicy = CacheMetadataChargePolicy.DontCharge, CompressionType = type,
            EnableCustomSplitMerge = splitMerge,
        });

    // ---- CompressedSecondaryCache --------------------------------------------------------------------------

    [TestMethod]
    [DataRow(CompressionType.None, false)]
    [DataRow(CompressionType.LZ4, false)]
    [DataRow(CompressionType.LZ4, true)]
    [DataRow(CompressionType.Zstd, false)]
    [DataRow(CompressionType.Snappy, true)]
    public void CompressedSecondaryCacheBasicInsertLookup(CompressionType type, bool splitMerge)
    {
        // BasicTestHelper: the first insert only records a dummy; the second (or a forced one) stores the value.
        CompressedSecondaryCache sec = NewCompressed(1 << 20, type, splitMerge);
        var stats = new Statistics();
        byte[] p1 = Payload(1, 1000);
        Assert.IsNull(sec.Lookup(KeyBytes(1), s_helper, null, true, false, stats, out bool kept));
        Assert.IsFalse(kept);

        sec.Insert(KeyBytes(1), Value(p1), s_helper, forceInsert: false);
        // Only a dummy so far.
        Assert.IsNull(sec.Lookup(KeyBytes(1), s_helper, null, true, false, stats, out _));
        Assert.AreEqual(1UL, stats.GetTickerCount(Ticker.CompressedSecondaryCacheDummyHits));

        sec.Insert(KeyBytes(1), Value(p1), s_helper, forceInsert: false);
        SecondaryCacheResultHandle? h = sec.Lookup(KeyBytes(1), s_helper, null, true, adviseErase: false, stats, out kept);
        Assert.IsNotNull(h);
        Assert.IsTrue(h.IsReady && h.HasValue);
        Assert.IsTrue(kept);
        CacheValue v = h.Value;
        CollectionAssert.AreEqual(p1, new ReadOnlySpan<byte>(v.Buffer, 0, v.Length).ToArray());
        Assert.AreEqual(p1.Length, h.Size);
        v.FreeForCache();
        Assert.AreEqual(1UL, stats.GetTickerCount(Ticker.CompressedSecondaryCacheHits));

        // Forced insert of another key stores it directly; advise_erase drops it and leaves a dummy behind.
        byte[] p2 = Payload(2, 3000);
        sec.Insert(KeyBytes(2), Value(p2), s_helper, forceInsert: true);
        h = sec.Lookup(KeyBytes(2), s_helper, null, true, adviseErase: true, stats, out kept);
        Assert.IsNotNull(h);
        Assert.IsFalse(kept);
        CollectionAssert.AreEqual(p2, new ReadOnlySpan<byte>(h.Value.Buffer, 0, h.Value.Length).ToArray());
        h.Value.FreeForCache();
        Assert.IsNull(sec.Lookup(KeyBytes(2), s_helper, null, true, false, stats, out _));
        Assert.AreEqual(2UL, stats.GetTickerCount(Ticker.CompressedSecondaryCacheDummyHits));

        if (type != CompressionType.None && !splitMerge)
        {
            // Compressible values are charged less than their size.
            sec.Insert(KeyBytes(3), Value(Payload(3, 8000)), s_helper, forceInsert: true);
            long charge = sec.GetChargeForTest(Key(3));
            Assert.IsTrue(charge > 0 && charge < 8000, "charge " + charge);
        }
    }

    [TestMethod]
    public void CompressedSecondaryCacheDoNotCompressRolesAndInsertSaved()
    {
        var sec = new CompressedSecondaryCache(new CompressedSecondaryCacheOptions
        {
            Capacity = 1 << 20, NumShardBits = 0, MetadataChargePolicy = CacheMetadataChargePolicy.DontCharge,
        });
        var filterHelper = new BytesCacheItemHelper(CacheEntryRole.FilterBlock);
        byte[] p = Payload(4, 4000);
        byte[] buf = ArrayPool<byte>.Shared.Rent(p.Length);
        p.CopyTo(buf, 0);
        sec.Insert(KeyBytes(4), CacheValue.FromPooledBuffer(buf, p.Length).WithHelper(filterHelper), filterHelper, forceInsert: true);
        // Filter blocks are stored uncompressed: tag (2 bytes) + data.
        Assert.AreEqual(p.Length + 2, sec.GetChargeForTest(Key(4)));
        CacheValue v = CacheValue.FromPooledBuffer(buf, p.Length);
        v.FreeForCache();

        // InsertSaved with compressed data (a non-volatile tier's promotion); uncompressed saved data is ignored.
        var compressed = new Managed.Util.PooledBufferWriter(256);
        Assert.IsTrue(BlockCompression.TryCompress(CompressionType.LZ4, p, 2, CompressionOptions.DefaultCompressionLevel, compressed));
        sec.InsertSaved(KeyBytes(5), compressed.WrittenSpan, CompressionType.LZ4, CacheTier.VolatileTier); // dummy first
        sec.InsertSaved(KeyBytes(5), compressed.WrittenSpan, CompressionType.LZ4, CacheTier.VolatileTier);
        sec.InsertSaved(KeyBytes(6), p, CompressionType.None, CacheTier.VolatileTier);
        SecondaryCacheResultHandle? h = sec.Lookup(KeyBytes(5), s_helper, null, true, false, null, out _);
        Assert.IsNotNull(h);
        CollectionAssert.AreEqual(p, new ReadOnlySpan<byte>(h.Value.Buffer, 0, h.Value.Length).ToArray());
        h.Value.FreeForCache();
        Assert.IsNull(sec.Lookup(KeyBytes(6), s_helper, null, true, false, null, out _));
        compressed.Dispose();
    }

    [TestMethod]
    public void CompressedSecondaryCacheSplitChunksFollowMallocBins()
    {
        // SplitValueIntoChunks with no compression: 8000 bytes + 2 tag bytes split at bin sizes (16-byte chunk headers).
        CompressedSecondaryCache sec = NewCompressed(1 << 20, CompressionType.None, splitMerge: true);
        sec.Insert(KeyBytes(7), Value(Payload(7, 8000)), s_helper, forceInsert: true);
        // upstream: predicted 8018 -> chunk 4096 (4080 bytes), remaining 3922 -> predicted 3938 -> 2048 (2032 bytes),
        // remaining 1890 -> predicted 1906 -> 1024 (1008), remaining 882 -> 898: bin 1024 - 898 = 126 < 128, no split.
        Assert.AreEqual(4096 + 2048 + 1024 + 898, sec.GetChargeForTest(Key(7)));
        SecondaryCacheResultHandle? h = sec.Lookup(KeyBytes(7), s_helper, null, true, false, null, out _);
        Assert.IsNotNull(h);
        CollectionAssert.AreEqual(Payload(7, 8000), new ReadOnlySpan<byte>(h.Value.Buffer, 0, h.Value.Length).ToArray());
        h.Value.FreeForCache();
    }

    [TestMethod]
    public void CompressedSecondaryCacheCapacityDeflateInflate()
    {
        CompressedSecondaryCache sec = NewCompressed(1 << 20);
        Assert.IsTrue(sec.TryGetCapacity(out long cap));
        Assert.AreEqual(1L << 20, cap);
        Assert.IsTrue(sec.Deflate(512 * 1024));
        Assert.AreEqual(512L * 1024, sec.Usage); // two 256 KiB placeholders
        Assert.IsTrue(sec.Inflate(512 * 1024));
        Assert.AreEqual(0, sec.Usage);
        Assert.IsTrue(sec.SetCapacity(0));
        sec.Insert(KeyBytes(8), Value(Payload(8, 100)), s_helper, forceInsert: true);
        Assert.IsNull(sec.Lookup(KeyBytes(8), s_helper, null, true, false, null, out _)); // disabled
    }

    // ---- adapter ---------------------------------------------------------------------------------------------

    private static BlockCache NewPrimaryWithSecondary(bool hyperClock, long primaryCapacity, SecondaryCache sec) => hyperClock
        ? BlockCache.CreateHyperClock(new HyperClockCacheOptions
        {
            Capacity = primaryCapacity, EstimatedEntryCharge = 1024, NumShardBits = 0, MetadataChargePolicy = CacheMetadataChargePolicy.DontCharge,
            SecondaryCache = sec,
        })
        : BlockCache.CreateLru(new LruCacheOptions
        {
            Capacity = primaryCapacity, NumShardBits = 0, MetadataChargePolicy = CacheMetadataChargePolicy.DontCharge, SecondaryCache = sec,
        });

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void AdapterDemotesEvictedEntriesAndPromotesOnSecondHit(bool hyperClock)
    {
        CompressedSecondaryCache sec = NewCompressed(1 << 20);
        BlockCache cache = NewPrimaryWithSecondary(hyperClock, 4 * 1024, sec);
        Assert.IsTrue(cache.ExtendedLookup);
        var stats = new Statistics();
        const int n = 40;
        // Fill the primary well past capacity twice: the first eviction leaves dummies, the second stores the values.
        for (int round = 0; round < 2; round++)
        {
            for (int i = 0; i < n; i++)
            {
                Assert.IsTrue(cache.TryInsert(Key(i), Value(Payload(i, 1000)), 1000, CachePriority.Low, out CachedBlock h));
                h.Dispose();
            }
        }
        Assert.IsTrue(sec.Count > 0);
        int secondaryHits = 0;
        for (int i = 0; i < n; i++)
        {
            if (!cache.TryLookup(Key(i), s_helper, null, CachePriority.Low, stats, out CachedBlock h)) continue;
            CollectionAssert.AreEqual(Payload(i, 1000), ReadBack(h), "key " + i);
            h.Dispose();
            secondaryHits++;
        }
        Assert.IsTrue(secondaryHits > n / 2, "hits " + secondaryHits);
        Assert.IsTrue(stats.GetTickerCount(Ticker.SecondaryCacheHits) > 0);
        Assert.AreEqual(stats.GetTickerCount(Ticker.SecondaryCacheHits), stats.GetTickerCount(Ticker.SecondaryCacheDataHits));

        // Plain lookups never return dummy entries.
        for (int i = 0; i < n; i++)
        {
            if (cache.TryLookup(Key(i), out CachedBlock h))
            {
                Assert.IsNotNull(h.Span.ToArray());
                h.Dispose();
            }
        }
        Assert.AreEqual(0, cache.PinnedUsage);
    }

    [TestMethod]
    public void AdapterFirstSecondaryHitIsStandaloneSecondPromotes()
    {
        CompressedSecondaryCache sec = NewCompressed(1 << 20);
        BlockCache cache = NewPrimaryWithSecondary(false, 64 * 1024, sec);
        // Put a value only into the secondary cache.
        sec.Insert(KeyBytes(1), Value(Payload(1, 2000)), s_helper, forceInsert: true);

        Assert.IsTrue(cache.TryLookup(Key(1), s_helper, null, CachePriority.Low, null, out CachedBlock h1));
        // A standalone handle plus a dummy in the primary: the value still lives in the secondary cache.
        Assert.AreEqual(2000, cache.PinnedUsage);
        h1.Dispose();
        Assert.AreEqual(0, cache.PinnedUsage);
        Assert.IsFalse(cache.TryLookup(Key(1), out _), "dummy entries are misses for plain lookups");

        // Second hit: the dummy is found, so the entry is promoted into the primary and erased from the secondary.
        Assert.IsTrue(cache.TryLookup(Key(1), s_helper, null, CachePriority.Low, null, out CachedBlock h2));
        h2.Dispose();
        Assert.IsTrue(cache.TryLookup(Key(1), out CachedBlock h3), "now a regular primary entry");
        CollectionAssert.AreEqual(Payload(1, 2000), ReadBack(h3));
        h3.Dispose();
        Assert.IsNull(sec.Lookup(KeyBytes(1), s_helper, null, true, false, null, out _));
    }

    [TestMethod]
    [DataRow(TieredAdmissionPolicy.AllowAll)]
    [DataRow(TieredAdmissionPolicy.AllowCacheHits)]
    public void TieredAdmissionPoliciesForceDemotion(TieredAdmissionPolicy policy)
    {
        BlockCache cache = BlockCache.CreateTiered(new TieredCacheOptions
        {
            TotalCapacity = 8 * 1024 * 1024, CompressedSecondaryRatio = 0.5, AdmissionPolicy = policy,
            LruCacheOptions = new LruCacheOptions { NumShardBits = 0, MetadataChargePolicy = CacheMetadataChargePolicy.DontCharge },
            CompressedSecondaryCacheOptions = new CompressedSecondaryCacheOptions { NumShardBits = 0, MetadataChargePolicy = CacheMetadataChargePolicy.DontCharge },
        })!;
        Assert.AreEqual("TieredCache", cache.Name);
        var adapter = (CacheWithSecondaryAdapter)cache;
        var sec = (CompressedSecondaryCache)adapter.Secondary;
        // Usable primary capacity is 4 MiB; insert 6 MiB of 64 KiB values, looking each up once (a hit).
        for (int i = 0; i < 96; i++)
        {
            cache.Insert(Key(i), Value(Payload(i, 64 * 1024)), 64 * 1024, CachePriority.Low);
            if (cache.TryLookup(Key(i), out CachedBlock h)) h.Dispose();
        }
        // Evicted entries were admitted on their first eviction (forced): real values, not just dummies.
        SecondaryCacheResultHandle? r = sec.Lookup(KeyBytes(0), s_helper, null, true, false, null, out _);
        Assert.IsNotNull(r);
        r.Value.FreeForCache();
    }

    [TestMethod]
    public void TieredCacheDistributesReservationsAndUpdates()
    {
        // NewTieredCache: total 100 MiB, 30% compressed. Placeholders reserved against the tiered cache are split 70/30.
        const long mib = 1 << 20;
        BlockCache cache = BlockCache.CreateTiered(new TieredCacheOptions
        {
            TotalCapacity = 100 * mib, CompressedSecondaryRatio = 0.3,
            LruCacheOptions = new LruCacheOptions { NumShardBits = 0, MetadataChargePolicy = CacheMetadataChargePolicy.DontCharge },
            CompressedSecondaryCacheOptions = new CompressedSecondaryCacheOptions { NumShardBits = 0, MetadataChargePolicy = CacheMetadataChargePolicy.DontCharge },
        })!;
        var adapter = (CacheWithSecondaryAdapter)cache;
        var sec = (CompressedSecondaryCache)adapter.Secondary;
        Assert.AreEqual(100 * mib, cache.Capacity);
        Assert.IsTrue(cache.TryGetSecondaryCacheCapacity(out long secCap));
        Assert.AreEqual(30 * mib, secCap);
        // The secondary budget is reserved in the primary.
        Assert.AreEqual(30 * mib, cache.Usage);
        Assert.AreEqual(0, cache.SecondaryCachePinnedUsage);

        var res = new CacheReservationManagerImpl(cache, CacheEntryRole.WriteBuffer, delayedDecrease: false);
        Assert.IsTrue(res.UpdateCacheReservation(10 * mib));
        // 10 MiB placeholders in the primary; 3 MiB of them are taken from the secondary (deflated) and credited back.
        Assert.AreEqual(3 * mib, cache.SecondaryCachePinnedUsage);
        Assert.AreEqual(3 * mib, sec.Usage);
        Assert.AreEqual(30 * mib - 3 * mib + 10 * mib, cache.Usage);
        Assert.IsTrue(res.UpdateCacheReservation(0));
        Assert.AreEqual(0, cache.SecondaryCachePinnedUsage);
        Assert.AreEqual(30 * mib, cache.Usage);

        // UpdateTieredCache: grow the total, then lower the ratio.
        BlockCache.UpdateTieredCache(cache, 200 * mib, -1, TieredAdmissionPolicy.Max);
        Assert.AreEqual(200 * mib, cache.Capacity);
        Assert.IsTrue(cache.TryGetSecondaryCacheCapacity(out secCap));
        Assert.AreEqual(60 * mib, secCap);
        BlockCache.UpdateTieredCache(cache, -1, 0.1, TieredAdmissionPolicy.AllowAll);
        Assert.IsTrue(cache.TryGetSecondaryCacheCapacity(out secCap));
        Assert.AreEqual(20 * mib, secCap);
        Assert.AreEqual(20 * mib, cache.Usage);
        Assert.AreEqual(TieredAdmissionPolicy.AllowAll, adapter.AdmissionPolicy);
        res.Dispose();

        var ex = Assert.ThrowsExactly<RocksDbException>(() => BlockCache.UpdateTieredCache(BlockCache.CreateLru(mib), 1, 0.5, TieredAdmissionPolicy.Max));
        Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);
        // Admission policy validation (NewTieredCache returns nullptr).
        Assert.IsNull(BlockCache.CreateTiered(new TieredCacheOptions { TotalCapacity = mib, AdmissionPolicy = TieredAdmissionPolicy.ThreeQueue }));
        Assert.IsNull(BlockCache.CreateTiered(new TieredCacheOptions
        {
            TotalCapacity = mib, AdmissionPolicy = TieredAdmissionPolicy.Placeholder, NvmSecondaryCache = new FakeNvmSecondaryCache(),
        }));
    }

    // ---- async lookups and the three-queue tier ----------------------------------------------------------------

    /// <summary>
    /// A non-volatile secondary cache stand-in: holds compressed values, lookups complete asynchronously (like
    /// TestSecondaryCache with ready = false in tiered_secondary_cache_test.cc).
    /// </summary>
    private sealed class FakeNvmSecondaryCache : SecondaryCache
    {
        private readonly Dictionary<CacheKey, byte[]> _data = new();
        public int Lookups;

        public override string Name => "FakeNvm";

        public void Put(int key, byte[] uncompressed)
        {
            var w = new Managed.Util.PooledBufferWriter(256);
            BlockCompression.TryCompress(CompressionType.LZ4, uncompressed, 2, CompressionOptions.DefaultCompressionLevel, w);
            lock (_data) _data[Key(key)] = w.WrittenSpan.ToArray();
            w.Dispose();
        }

        public override void Insert(ReadOnlySpan<byte> key, in CacheValue value, CacheItemHelper helper, bool forceInsert) { }

        public override void InsertSaved(ReadOnlySpan<byte> key, ReadOnlySpan<byte> saved, CompressionType type, CacheTier source)
        {
            lock (_data) _data[CacheKey.Read(key)] = saved.ToArray();
        }

        private sealed class Pending(byte[] data, CacheItemHelper helper, object? ctx) : SecondaryCacheResultHandle
        {
            private readonly TaskCompletionSource _done = new(TaskCreationOptions.RunContinuationsAsynchronously);
            private CacheValue _value;
            private long _size;
            private bool _has;

            public void Complete()
            {
                _has = helper.TryCreate(data, CompressionType.LZ4, CacheTier.VolatileTier, ctx, out _value, out _size);
                _done.TrySetResult();
            }

            public override bool IsReady => _done.Task.IsCompleted;

            public override void Wait()
            {
                if (!IsReady) Complete();
            }

            public override async ValueTask WaitAsync(CancellationToken cancellationToken)
            {
                _ = Task.Run(async () =>
                {
                    await Task.Delay(5, CancellationToken.None).ConfigureAwait(false);
                    Complete();
                }, CancellationToken.None);
                await _done.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            }

            public override bool HasValue => _has;

            public override CacheValue Value => _value;

            public override long Size => _size;
        }

        public override SecondaryCacheResultHandle? Lookup(ReadOnlySpan<byte> key, CacheItemHelper helper, object? createContext, bool wait,
            bool adviseErase, Statistics? stats, out bool keptInSecondaryCache)
        {
            Interlocked.Increment(ref Lookups);
            keptInSecondaryCache = true;
            byte[]? d;
            lock (_data)
            {
                if (!_data.TryGetValue(CacheKey.Read(key), out d)) return null;
            }
            var p = new Pending(d, helper, createContext);
            if (wait) p.Complete();
            return p;
        }

        public override bool SupportForceErase => false;

        public override void Erase(ReadOnlySpan<byte> key)
        {
            lock (_data) _data.Remove(CacheKey.Read(key));
        }
    }

    [TestMethod]
    public async Task ThreeQueueTierPromotesNvmHitsIntoCompressedTierAsynchronously()
    {
        var nvm = new FakeNvmSecondaryCache();
        BlockCache cache = BlockCache.CreateTiered(new TieredCacheOptions
        {
            TotalCapacity = 4 << 20, CompressedSecondaryRatio = 0.5, NvmSecondaryCache = nvm,
            LruCacheOptions = new LruCacheOptions { NumShardBits = 0, MetadataChargePolicy = CacheMetadataChargePolicy.DontCharge },
            CompressedSecondaryCacheOptions = new CompressedSecondaryCacheOptions { NumShardBits = 0, MetadataChargePolicy = CacheMetadataChargePolicy.DontCharge },
        })!;
        Assert.AreEqual(TieredAdmissionPolicy.ThreeQueue, ((CacheWithSecondaryAdapter)cache).AdmissionPolicy);
        var stats = new Statistics();
        for (int i = 0; i < 8; i++) nvm.Put(i, Payload(i, 4000));

        var handles = new AsyncLookupHandle[8];
        for (int i = 0; i < 8; i++)
        {
            handles[i] = new AsyncLookupHandle { Key = Key(i), Helper = s_helper, Statistics = stats };
            cache.StartAsyncLookup(handles[i]);
            Assert.IsTrue(handles[i].IsPending, "nvm lookups complete later");
        }
        await cache.WaitAllAsync(handles, handles.Length, CancellationToken.None);
        for (int i = 0; i < 8; i++)
        {
            Assert.IsFalse(handles[i].IsPending);
            CachedBlock h = handles[i].TakeResult();
            Assert.IsTrue(h.HasValue);
            CollectionAssert.AreEqual(Payload(i, 4000), ReadBack(h));
            h.Dispose();
        }
        // The NVM hits were offered to the compressed tier (first time: dummy only, MaybeInsertDummy).
        Assert.AreEqual(8UL, stats.GetTickerCount(Ticker.CompressedSecondaryCachePromotions));
        Assert.AreEqual(8UL, stats.GetTickerCount(Ticker.SecondaryCacheHits));

        // Synchronous WaitAll for a fresh key.
        nvm.Put(100, Payload(100, 500));
        var one = new AsyncLookupHandle { Key = Key(100), Helper = s_helper };
        cache.StartAsyncLookup(one);
        cache.WaitAll([one]);
        CachedBlock r = one.TakeResult();
        Assert.IsTrue(r.HasValue);
        r.Dispose();

        // Evictions are not demoted with three queues: the nvm tier is only fed with InsertSaved.
        var sec = (TieredSecondaryCache)((CacheWithSecondaryAdapter)cache).Secondary;
        Assert.AreEqual("TieredSecondaryCache", sec.Name);
    }

    [TestMethod]
    public void AsyncLookupOnPlainCacheCompletesImmediately()
    {
        BlockCache cache = BlockCache.CreateLru(1 << 20);
        cache.Insert(Key(1), Value(Payload(1, 10)), 10, CachePriority.Low);
        var h = new AsyncLookupHandle { Key = Key(1), Helper = s_helper };
        cache.StartAsyncLookup(h);
        Assert.IsFalse(h.IsPending);
        Assert.IsTrue(h.HasResult);
        CachedBlock b = h.TakeResult();
        b.Dispose();
        h.Reset();
        h.Key = Key(2);
        cache.StartAsyncLookup(h);
        Assert.IsFalse(h.HasResult);
    }

    [TestMethod]
    public void StandaloneHandlesChargeUsageUntilReleased()
    {
        BlockCache cache = BlockCache.CreateLru(new LruCacheOptions { Capacity = 1000, NumShardBits = 0, StrictCapacityLimit = true,
            MetadataChargePolicy = CacheMetadataChargePolicy.DontCharge });
        Assert.IsTrue(cache.TryCreateStandalone(Key(1), Value(Payload(1, 10)), 600, allowUncharged: false, out CachedBlock a));
        Assert.AreEqual(600, cache.Usage);
        Assert.AreEqual(600, cache.PinnedUsage);
        Assert.IsFalse(cache.TryLookup(Key(1), out _), "standalone entries are not in the table");
        // Full with a strict limit: refused, or created uncharged.
        Assert.IsFalse(cache.TryCreateStandalone(Key(2), default, 600, allowUncharged: false, out _));
        Assert.IsTrue(cache.TryCreateStandalone(Key(2), Value(Payload(2, 10)), 600, allowUncharged: true, out CachedBlock b));
        Assert.AreEqual(0, b.Charge);
        CachedBlock a2 = a.AddRef();
        a.Dispose();
        Assert.AreEqual(600, cache.Usage);
        a2.Dispose();
        b.Dispose();
        Assert.AreEqual(0, cache.Usage);
    }

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void ConcurrentLookupsAndInsertsThroughAdapter(bool hyperClock)
    {
        CompressedSecondaryCache sec = NewCompressed(4 << 20);
        BlockCache cache = NewPrimaryWithSecondary(hyperClock, 256 * 1024, sec);
        const int keys = 512, threads = 4, ops = 20_000;
        var failures = new System.Collections.Concurrent.ConcurrentQueue<string>();
        Parallel.For(0, threads, t =>
        {
            var rnd = new Random(t + 1);
            for (int i = 0; i < ops; i++)
            {
                int k = rnd.Next(keys);
                if (cache.TryLookup(Key(k), s_helper, null, CachePriority.Low, null, out CachedBlock h))
                {
                    byte[] got = h.Span.ToArray();
                    h.Dispose();
                    if (!got.AsSpan().SequenceEqual(Payload(k, 1000))) failures.Enqueue("key " + k);
                }
                else if (cache.TryInsert(Key(k), Value(Payload(k, 1000)), 1000, CachePriority.Low, out CachedBlock ins))
                {
                    ins.Dispose();
                }
            }
        });
        Assert.IsTrue(failures.IsEmpty, string.Join(", ", failures));
        Assert.AreEqual(0, cache.PinnedUsage);
        Assert.IsTrue(cache.Usage <= 256 * 1024 + 4096);
    }
}
