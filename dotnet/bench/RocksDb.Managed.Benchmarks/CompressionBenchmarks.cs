using BenchmarkDotNet.Attributes;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Benchmarks;

/// <summary>Compress / decompress a compressible 4 KiB data-block-like payload with RocksDB framing.</summary>
[MemoryDiagnoser]
[BenchmarkCategory("FiltersHashCompression")]
public class CompressionBenchmarks
{
    [Params(CompressionType.Snappy, CompressionType.LZ4, CompressionType.LZ4HC, CompressionType.Zstd, CompressionType.Zlib)]
    public CompressionType Type { get; set; }

    private readonly byte[] _raw = new byte[4096];
    private byte[] _compressed = [];
    private readonly PooledBufferWriter _out = new(64 * 1024);

    [GlobalSetup]
    public void Setup()
    {
        var rnd = new Random(42);
        int pos = 0;
        int k = 0;
        while (pos < _raw.Length)
        {
            byte[] entry = System.Text.Encoding.ASCII.GetBytes("user_key_" + (k++).ToString("D8") + "value-" + rnd.Next(1000) + "-payload-payload;");
            int n = Math.Min(entry.Length, _raw.Length - pos);
            entry.AsSpan(0, n).CopyTo(_raw.AsSpan(pos));
            pos += n;
        }
        _out.Clear();
        if (!BlockCompression.TryCompress(Type, _raw, 5, BlockCompression.DefaultCompressionLevel, _out))
        {
            throw new InvalidOperationException("not compressible");
        }
        _compressed = _out.WrittenSpan.ToArray();
    }

    [Benchmark]
    public int Compress4K()
    {
        _out.Clear();
        BlockCompression.TryCompress(Type, _raw, 5, BlockCompression.DefaultCompressionLevel, _out);
        return _out.WrittenCount;
    }

    [Benchmark]
    public int Decompress4K()
    {
        _out.Clear();
        BlockCompression.Decompress(Type, _compressed, 5, _out);
        return _out.WrittenCount;
    }
}
