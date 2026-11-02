using BenchmarkDotNet.Attributes;
using RocksDb.Managed.Table.Filters;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Benchmarks;

/// <summary>Full filter query and construction (10 bits/key, 100k 16-byte keys). Queries must not allocate.</summary>
[MemoryDiagnoser]
[BenchmarkCategory("FiltersHashCompression")]
public class FilterBenchmarks
{
    private const int KeyCount = 100_000;
    private const int KeyLen = 16;

    /// <summary>Filter implementation under test.</summary>
    public enum FilterImpl
    {
        FastLocalBloom,
        Ribbon,
        LegacyBloom,
    }

    [Params(FilterImpl.FastLocalBloom, FilterImpl.Ribbon, FilterImpl.LegacyBloom)]
    public FilterImpl Impl { get; set; } = FilterImpl.FastLocalBloom;

    private byte[] _keys = [];
    private IFilterBitsReader _reader = null!;
    private FilterPolicy _policy = null!;
    private int _formatVersion;
    private readonly PooledBufferWriter _out = new(1 << 20);

    [GlobalSetup]
    public void Setup()
    {
        _keys = new byte[KeyCount * KeyLen];
        new Random(42).NextBytes(_keys);
        (_policy, _formatVersion) = Impl switch
        {
            FilterImpl.Ribbon => ((FilterPolicy)new RibbonFilterPolicy(10, -1), 5),
            FilterImpl.LegacyBloom => (new BloomFilterPolicy(10), 4),
            _ => (new BloomFilterPolicy(10), 5),
        };
        using IFilterBitsBuilder b = _policy.CreateBuilder(_formatVersion);
        for (int i = 0; i < KeyCount; i++) b.AddKey(_keys.AsSpan(i * KeyLen, KeyLen));
        var w = new PooledBufferWriter();
        b.Finish(w);
        _reader = _policy.CreateReader(w.WrittenMemory.ToArray());
    }

    /// <summary>1000 queries of present keys.</summary>
    [Benchmark(OperationsPerInvoke = 1000)]
    public int MayMatchHit()
    {
        int n = 0;
        ReadOnlySpan<byte> keys = _keys;
        for (int i = 0; i < 1000; i++)
        {
            if (_reader.MayMatch(keys.Slice(i * 97 % KeyCount * KeyLen, KeyLen))) n++;
        }
        return n;
    }

    /// <summary>1000 queries of absent keys (mostly negative).</summary>
    [Benchmark(OperationsPerInvoke = 1000)]
    public int MayMatchMiss()
    {
        int n = 0;
        Span<byte> key = stackalloc byte[KeyLen];
        for (int i = 0; i < 1000; i++)
        {
            Coding.EncodeFixed64(key, (ulong)i);
            if (_reader.MayMatch(key)) n++;
        }
        return n;
    }

    /// <summary>Build a whole 100k-key filter (AddKey x 100k + Finish), per key.</summary>
    [Benchmark(OperationsPerInvoke = KeyCount)]
    public int BuildFilter()
    {
        using IFilterBitsBuilder b = _policy.CreateBuilder(_formatVersion);
        ReadOnlySpan<byte> keys = _keys;
        for (int i = 0; i < KeyCount; i++) b.AddKey(keys.Slice(i * KeyLen, KeyLen));
        _out.Clear();
        b.Finish(_out);
        return _out.WrittenCount;
    }
}

[MemoryDiagnoser]
[BenchmarkCategory("FiltersHashCompression")]
public class HashBenchmarks
{
    private readonly byte[] _key16 = new byte[16];
    private readonly byte[] _key100 = new byte[100];
    private readonly byte[] _block4K = new byte[4096];

    [GlobalSetup]
    public void Setup()
    {
        var r = new Random(1);
        r.NextBytes(_key16);
        r.NextBytes(_key100);
        r.NextBytes(_block4K);
    }

    [Benchmark] public ulong Hash64_16B() => Hash.GetSliceHash64(_key16);
    [Benchmark] public ulong Hash64_100B() => Hash.GetSliceHash64(_key100);
    [Benchmark] public ulong Hash64_4K() => Hash.GetSliceHash64(_block4K);
    [Benchmark] public uint BloomHash_16B() => Hash.BloomHash(_key16);
    [Benchmark] public UInt128 Hash128_16B() => Hash.GetSliceHash128(_key16);
}
