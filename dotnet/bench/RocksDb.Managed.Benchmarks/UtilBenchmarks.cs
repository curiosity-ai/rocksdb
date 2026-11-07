using BenchmarkDotNet.Attributes;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Benchmarks;

[MemoryDiagnoser]
public class UtilBenchmarks
{
    private readonly byte[] _block = new byte[4096];
    private readonly byte[] _varints = new byte[1000 * 5];
    private int _varintBytes;

    [GlobalSetup]
    public void Setup()
    {
        new Random(42).NextBytes(_block);
        var rnd = new Random(1);
        int pos = 0;
        for (int i = 0; i < 1000; i++) pos += Coding.EncodeVarint32(_varints.AsSpan(pos), (uint)rnd.Next(0, 1 << 20));
        _varintBytes = pos;
    }

    // 24-byte internal-key-like keys sharing a 20-byte prefix (index/data block and memtable comparisons).
    private readonly byte[][] _cmpKeys = MakeCmpKeys();

    private static byte[][] MakeCmpKeys()
    {
        var keys = new byte[64][];
        for (int i = 0; i < keys.Length; i++) keys[i] = System.Text.Encoding.ASCII.GetBytes("user0000000000" + (i * 7919 % 1000000).ToString("D6") + "SEQ#");
        return keys;
    }

    [Benchmark(OperationsPerInvoke = 64)]
    public int CompareSequenceCompareTo24B()
    {
        byte[][] k = _cmpKeys;
        int s = 0;
        for (int i = 0; i < k.Length; i++) s += k[i].AsSpan().SequenceCompareTo(k[(i + 1) & 63]);
        return s;
    }

    [Benchmark(OperationsPerInvoke = 64)]
    public int CompareByteCompare24B()
    {
        byte[][] k = _cmpKeys;
        int s = 0;
        for (int i = 0; i < k.Length; i++) s += ByteCompare.Compare(k[i], k[(i + 1) & 63]);
        return s;
    }

    [Benchmark] public uint Crc32C4K() => Crc32C.Value(_block);

    [Benchmark]
    public uint DecodeVarint32x1000()
    {
        ReadOnlySpan<byte> s = _varints.AsSpan(0, _varintBytes);
        uint sum = 0;
        while (Coding.GetVarint32(ref s, out uint v)) sum += v;
        return sum;
    }
}
