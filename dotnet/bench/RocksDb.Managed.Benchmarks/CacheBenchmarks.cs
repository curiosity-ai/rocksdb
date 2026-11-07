using System.Buffers;
using BenchmarkDotNet.Attributes;
using RocksDb.Managed.Cache;

namespace RocksDb.Managed.Benchmarks;

/// <summary>
/// Block cache micro-benchmarks (cf. cache/cache_bench_tool.cc): lookup hit + release, miss, insert with eviction,
/// and multi-threaded lookups. Hit paths must show no allocations.
/// </summary>
[MemoryDiagnoser]
public class CacheBenchmarks
{
    private const int NumKeys = 16 * 1024;
    private const int BlockSize = 4096;
    private const int OpsPerThread = 20_000;

    private BlockCache _cache = null!;
    private BlockCache _small = null!;
    private CacheKey[] _keys = null!;
    private int _next;

    private const int Threads = 4;

    /// <summary>Cache implementation under test (AutoHyperClock = HyperClockCache with estimated_entry_charge = 0).</summary>
    public enum Flavor
    {
        Lru,
        HyperClock,
        AutoHyperClock,
    }

    [Params(Flavor.Lru, Flavor.HyperClock, Flavor.AutoHyperClock)]
    public Flavor CacheType { get; set; } = Flavor.Lru;

    private BlockCache Create(long capacity) => CacheType switch
    {
        Flavor.Lru => BlockCache.CreateLru(capacity),
        Flavor.HyperClock => BlockCache.CreateHyperClock(new HyperClockCacheOptions { Capacity = capacity, EstimatedEntryCharge = BlockSize }),
        _ => BlockCache.CreateHyperClock(new HyperClockCacheOptions { Capacity = capacity, EstimatedEntryCharge = 0 }),
    };

    [GlobalSetup]
    public void Setup()
    {
        // 128 MiB: all keys resident.
        _cache = Create(128L << 20);
        var baseKey = new OffsetableCacheKey("db-id-1234", "ABCDEFGHIJ0123456789", 7);
        _keys = new CacheKey[NumKeys];
        for (int i = 0; i < NumKeys; i++)
        {
            _keys[i] = baseKey.WithOffsetForMinSizeRecord((ulong)i * BlockSize);
            byte[] buf = RocksArrayPool<byte>.Shared.Rent(BlockSize);
            _cache.Insert(_keys[i], CacheValue.FromPooledBuffer(buf, BlockSize), BlockSize, CachePriority.Low);
        }
        // 4 MiB: inserts constantly evict.
        _small = Create(4L << 20);
    }

    [Benchmark]
    public int LookupHit()
    {
        CacheKey k = _keys[_next++ & (NumKeys - 1)];
        if (!_cache.TryLookup(k, out CachedBlock h)) return -1;
        int len = h.Span.Length;
        h.Dispose();
        return len;
    }

    [Benchmark]
    public bool LookupMiss() => _cache.TryLookup(new CacheKey(1, (ulong)_next++), out _);

    [Benchmark]
    public bool InsertWithEviction()
    {
        CacheKey k = new(42, (ulong)_next++);
        byte[] buf = RocksArrayPool<byte>.Shared.Rent(BlockSize);
        return _small.Insert(k, CacheValue.FromPooledBuffer(buf, BlockSize), BlockSize, CachePriority.Low);
    }

    [Benchmark(OperationsPerInvoke = OpsPerThread * Threads)]
    public void LookupHitMultiThreaded()
    {
        Parallel.For(0, Threads, t =>
        {
            int idx = t * 7919;
            for (int i = 0; i < OpsPerThread; i++)
            {
                if (_cache.TryLookup(_keys[(idx + i) & (NumKeys - 1)], out CachedBlock h)) h.Dispose();
            }
        });
    }
}
