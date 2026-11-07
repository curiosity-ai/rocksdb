using BenchmarkDotNet.Attributes;
using RocksDb.Managed.Memory;
using RocksDb.Managed.Memtable;

namespace RocksDb.Managed.Benchmarks;

/// <summary>8-byte native-endian u64 keys (like memtable/memtablerep_bench.cc / inlineskiplist_test.cc).</summary>
public readonly unsafe struct U64SkipListComparer : ISkipListComparer
{
    public SkipListKey Decode(byte* encodedKey) => new(encodedKey, 8);
    public int Compare(byte* encodedKey, SkipListKey decoded) => (*(ulong*)encodedKey).CompareTo(*(ulong*)decoded.Ptr);
}

/// <summary>InlineSkipList insert / lookup throughput. Results are per operation (OperationsPerInvoke = N).</summary>
[MemoryDiagnoser]
public unsafe class SkipListBenchmarks
{
    private const int N = 200_000;
    private ulong[] _keys = [];
    private ConcurrentArena? _arena;
    private InlineSkipList<U64SkipListComparer>? _list;
    private InlineSkipList<U64SkipListComparer>? _filled;
    private ConcurrentArena? _filledArena;

    [GlobalSetup]
    public void Setup()
    {
        var rnd = new Random(42);
        _keys = new ulong[N];
        for (int i = 0; i < N; i++) _keys[i] = (ulong)rnd.NextInt64();
        _filledArena = new ConcurrentArena(1 << 20);
        _filled = new InlineSkipList<U64SkipListComparer>(default, _filledArena);
        foreach (ulong k in _keys) Insert(_filled, k, false);
    }

    [GlobalCleanup]
    public void Cleanup() => _filledArena?.Dispose();

    [IterationSetup(Targets = [nameof(InsertRandom), nameof(InsertRandomConcurrent4Threads), nameof(InsertSequential)])]
    public void NewList()
    {
        _arena = new ConcurrentArena(1 << 20);
        _list = new InlineSkipList<U64SkipListComparer>(default, _arena);
    }

    [IterationCleanup(Targets = [nameof(InsertRandom), nameof(InsertRandomConcurrent4Threads), nameof(InsertSequential)])]
    public void DisposeList() => _arena?.Dispose();

    private static void Insert(InlineSkipList<U64SkipListComparer> list, ulong k, bool concurrent)
    {
        byte* p = list.AllocateKey(8);
        *(ulong*)p = k;
        if (concurrent) list.InsertConcurrently(p);
        else list.Insert(p);
    }

    [Benchmark(OperationsPerInvoke = N)]
    public void InsertRandom()
    {
        InlineSkipList<U64SkipListComparer> list = _list!;
        ulong[] keys = _keys;
        for (int i = 0; i < keys.Length; i++) Insert(list, keys[i], false);
    }

    [Benchmark(OperationsPerInvoke = N)]
    public void InsertSequential()
    {
        InlineSkipList<U64SkipListComparer> list = _list!;
        for (int i = 0; i < N; i++) Insert(list, (ulong)i, false);
    }

    [Benchmark(OperationsPerInvoke = N)]
    public void InsertRandomConcurrent4Threads()
    {
        InlineSkipList<U64SkipListComparer> list = _list!;
        ulong[] keys = _keys;
        const int Threads = 4;
        var threads = new Thread[Threads];
        for (int t = 0; t < Threads; t++)
        {
            int id = t;
            threads[t] = new Thread(() =>
            {
                for (int i = id; i < keys.Length; i += Threads) Insert(list, keys[i], true);
            });
            threads[t].Start();
        }
        foreach (Thread th in threads) th.Join();
    }

    [Benchmark(OperationsPerInvoke = N)]
    public int ContainsRandom()
    {
        InlineSkipList<U64SkipListComparer> list = _filled!;
        int found = 0;
        ulong[] keys = _keys;
        for (int i = 0; i < keys.Length; i++)
        {
            ulong k = keys[i];
            if (list.Contains(new SkipListKey((byte*)&k, 8))) found++;
        }
        return found;
    }
}
