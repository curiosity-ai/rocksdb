using RocksDb.Managed.Compression;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Tests.Compression;

[TestClass]
public sealed class BlockCompressionTests
{
    private static readonly CompressionType[] Codecs =
        [CompressionType.Snappy, CompressionType.LZ4, CompressionType.LZ4HC, CompressionType.Zstd, CompressionType.Zlib];

    /// <summary>Compressible data resembling a data block (repeated key prefixes + text).</summary>
    private static byte[] Compressible(int size, int seed)
    {
        var rnd = new Random(seed);
        byte[] b = new byte[size];
        int pos = 0;
        while (pos < size)
        {
            ReadOnlySpan<byte> chunk = rnd.Next(3) switch
            {
                0 => "user_key_prefix_"u8,
                1 => "value-value-value"u8,
                _ => [(byte)rnd.Next(256), (byte)rnd.Next(256)],
            };
            int n = Math.Min(chunk.Length, size - pos);
            chunk[..n].CopyTo(b.AsSpan(pos));
            pos += n;
        }
        return b;
    }

    [TestMethod]
    public void RoundTripAcrossSizes()
    {
        int[] sizes = [1, 7, 64, 100, 1000, 4096, 4097, 16384, 16385, 65536, 1 << 20];
        foreach (CompressionType type in Codecs)
        {
            Assert.IsTrue(BlockCompression.IsSupported(type));
            foreach (int size in sizes)
            {
                byte[] raw = Compressible(size, size);
                using var compressed = new PooledBufferWriter();
                bool ok = BlockCompression.TryCompress(type, raw, 5, BlockCompression.DefaultCompressionLevel, compressed);
                if (!ok)
                {
                    Assert.AreEqual(0, compressed.WrittenCount, type + " wrote output despite rejecting");
                    // Only tiny inputs may be rejected for this data
                    Assert.IsTrue(size < 100, type + " rejected size " + size);
                    continue;
                }
                Assert.IsTrue(BlockCompression.IsGoodCompressionRatio(compressed.WrittenCount, raw.Length), type + " " + size);
                Assert.IsTrue(BlockCompression.TryGetUncompressedLength(type, compressed.WrittenSpan, 5, out int len));
                Assert.AreEqual(size, len);
                using var decompressed = new PooledBufferWriter();
                BlockCompression.Decompress(type, compressed.WrittenSpan, 5, decompressed);
                Assert.IsTrue(raw.AsSpan().SequenceEqual(decompressed.WrittenSpan), type + " size " + size);
            }
        }
    }

    [TestMethod]
    public void Format2SizePrefix()
    {
        byte[] raw = Compressible(5000, 1);
        foreach (CompressionType type in new[] { CompressionType.LZ4, CompressionType.Zstd, CompressionType.Zlib })
        {
            using var w = new PooledBufferWriter();
            Assert.IsTrue(BlockCompression.TryCompress(type, raw, 2, BlockCompression.DefaultCompressionLevel, w));
            ReadOnlySpan<byte> s = w.WrittenSpan;
            Assert.IsTrue(Coding.GetVarint32(ref s, out uint size));
            Assert.AreEqual(5000u, size);
        }
        // Snappy: raw snappy (its own varint length, same encoding)
        using var sw = new PooledBufferWriter();
        Assert.IsTrue(BlockCompression.TryCompress(CompressionType.Snappy, raw, 2, BlockCompression.DefaultCompressionLevel, sw));
        Assert.AreEqual(5000, Snappier.Snappy.GetUncompressedLength(sw.WrittenSpan));
    }

    [TestMethod]
    public void IncompressibleIsRejected()
    {
        byte[] raw = new byte[4096];
        new Random(7).NextBytes(raw);
        foreach (CompressionType type in Codecs)
        {
            using var w = new PooledBufferWriter();
            Assert.IsFalse(BlockCompression.TryCompress(type, raw, 5, BlockCompression.DefaultCompressionLevel, w), type.ToString());
            Assert.AreEqual(0, w.WrittenCount);
        }
        using var none = new PooledBufferWriter();
        Assert.IsFalse(BlockCompression.TryCompress(CompressionType.None, raw, 5, BlockCompression.DefaultCompressionLevel, none));
    }

    [TestMethod]
    public void RatioThreshold()
    {
        Assert.IsTrue(BlockCompression.IsGoodCompressionRatio(896, 1024));
        Assert.IsFalse(BlockCompression.IsGoodCompressionRatio(897, 1024));
        Assert.IsFalse(BlockCompression.IsGoodCompressionRatio(1, 1024, 0));
        // A threshold of 1024 is capped at 1023 like the table builder
        Assert.IsFalse(BlockCompression.IsGoodCompressionRatio(1024, 1024, 1024));
    }

    [TestMethod]
    public void LevelsRoundTrip()
    {
        byte[] raw = Compressible(8192, 3);
        foreach (int level in new[] { -5, -1, 0, 1, 3, 9, 12, 19 })
        {
            foreach (CompressionType type in new[] { CompressionType.LZ4, CompressionType.LZ4HC, CompressionType.Zstd, CompressionType.Zlib })
            {
                using var c = new PooledBufferWriter();
                bool ok = BlockCompression.TryCompress(type, raw, 5, level, c);
                if (type == CompressionType.Zlib && level == 0)
                {
                    // zlib level 0 = stored blocks: never beneficial
                    Assert.IsFalse(ok);
                    continue;
                }
                Assert.IsTrue(ok, type + " level " + level);
                using var d = new PooledBufferWriter();
                BlockCompression.Decompress(type, c.WrittenSpan, 5, d);
                Assert.IsTrue(raw.AsSpan().SequenceEqual(d.WrittenSpan));
            }
        }
    }

    [TestMethod]
    public void CorruptInputThrowsCorruption()
    {
        byte[] raw = Compressible(4096, 9);
        foreach (CompressionType type in Codecs)
        {
            using var c = new PooledBufferWriter();
            Assert.IsTrue(BlockCompression.TryCompress(type, raw, 5, BlockCompression.DefaultCompressionLevel, c));
            byte[] bad = c.WrittenSpan.ToArray();
            // Truncate the payload
            byte[] truncated = bad.AsSpan(0, bad.Length / 2).ToArray();
            using var d = new PooledBufferWriter();
            var ex = Assert.ThrowsExactly<RocksDbException>(() => BlockCompression.Decompress(type, truncated, 5, d), type.ToString());
            Assert.AreEqual(StatusCode.Corruption, ex.Code);
        }
        using var e = new PooledBufferWriter();
        var ex2 = Assert.ThrowsExactly<RocksDbException>(() => BlockCompression.Decompress(CompressionType.LZ4, [0x80], 5, e));
        Assert.AreEqual(StatusCode.Corruption, ex2.Code);
    }

    [TestMethod]
    public void UnsupportedTypes()
    {
        Assert.IsFalse(BlockCompression.IsSupported(CompressionType.BZip2));
        Assert.IsFalse(BlockCompression.IsSupported(CompressionType.Xpress));
        using var w = new PooledBufferWriter();
        Assert.IsFalse(BlockCompression.TryCompress(CompressionType.BZip2, new byte[1000], 5, BlockCompression.DefaultCompressionLevel, w));
        var ex = Assert.ThrowsExactly<RocksDbException>(() => BlockCompression.Decompress(CompressionType.Xpress, [1, 2, 3], 5, w));
        Assert.AreEqual(StatusCode.NotSupported, ex.Code);
        ex = Assert.ThrowsExactly<RocksDbException>(() => BlockCompression.Decompress(CompressionType.BZip2, [1, 2, 3], 5, w));
        Assert.AreEqual(StatusCode.NotSupported, ex.Code);
    }

    [TestMethod]
    public void NoneCopiesThrough()
    {
        using var w = new PooledBufferWriter();
        BlockCompression.Decompress(CompressionType.None, "abc"u8, 5, w);
        Assert.IsTrue(w.WrittenSpan.SequenceEqual("abc"u8));
    }
}
