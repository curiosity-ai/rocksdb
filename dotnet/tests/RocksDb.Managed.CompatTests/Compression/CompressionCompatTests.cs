using RocksDb.Managed.CompatTests.Filters;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.CompatTests.Compression;

/// <summary>
/// Data blocks compressed by native RocksDB decompress with the managed <see cref="BlockCompression"/> facade,
/// and their entries decode to the keys/values written. (The managed -> native direction is covered once the
/// managed table builder can produce SSTs for native ingestion.)
/// </summary>
[TestClass]
public sealed class CompressionCompatTests
{
    private const int KeyCount = 5000;

    private static void RunCase(RocksDbSharp.Compression nativeType, CompressionType expected, int formatVersion)
    {
        using var dir = new TempDir("compress");
        TestSstParser sst = NativeSst.Build(dir, IntPtr.Zero, formatVersion, nativeType, KeyCount);
        List<(ulong Offset, ulong Size)> handles = sst.DataBlockHandles();
        Assert.IsTrue(handles.Count > 1);
        int compressedBlocks = 0;
        int next = 0;
        foreach (var h in handles)
        {
            CompressionType type = sst.BlockCompressionType(h);
            if (type != CompressionType.None)
            {
                Assert.AreEqual(expected, type);
                compressedBlocks++;
                Assert.IsTrue(BlockCompression.TryGetUncompressedLength(type, sst.RawBlock(h), formatVersion, out int len));
                Assert.IsTrue(len > (int)h.Size);
            }
            byte[] block = sst.UncompressedBlock(h);
            foreach (var (internalKey, value) in TestSstParser.ParseBlockEntries(block, valueLengthPresent: true))
            {
                // internal key = user key + 8-byte (seq << 8 | type)
                CollectionAssert.AreEqual(NativeSst.Key(next), internalKey.AsSpan(0, internalKey.Length - 8).ToArray());
                CollectionAssert.AreEqual(NativeSst.Value(next), value);
                next++;
            }

            // Managed re-compression of the same block round-trips too.
            if (type != CompressionType.None)
            {
                using var c = new PooledBufferWriter();
                Assert.IsTrue(BlockCompression.TryCompress(type, block, formatVersion, BlockCompression.DefaultCompressionLevel, c));
                using var d = new PooledBufferWriter();
                BlockCompression.Decompress(type, c.WrittenSpan, formatVersion, d);
                CollectionAssert.AreEqual(block, d.WrittenSpan.ToArray());
            }
        }
        Assert.AreEqual(KeyCount, next);
        Assert.IsTrue(compressedBlocks > 0, "native did not compress any block");
    }

    [TestMethod] public void Snappy() => RunCase(RocksDbSharp.Compression.Snappy, CompressionType.Snappy, 5);
    [TestMethod] public void Lz4() => RunCase(RocksDbSharp.Compression.Lz4, CompressionType.LZ4, 5);
    [TestMethod] public void Lz4Hc() => RunCase(RocksDbSharp.Compression.Lz4hc, CompressionType.LZ4HC, 6);
    [TestMethod] public void Zstd() => RunCase(RocksDbSharp.Compression.Zstd, CompressionType.Zstd, 6);
    [TestMethod] public void ZstdFormatVersion2() => RunCase(RocksDbSharp.Compression.Zstd, CompressionType.Zstd, 2);
    [TestMethod] public void Zlib() => RunCase(RocksDbSharp.Compression.Zlib, CompressionType.Zlib, 5);
}
