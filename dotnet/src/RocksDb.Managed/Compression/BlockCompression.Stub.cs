using System.Buffers;

namespace RocksDb.Managed.Compression;

// TEMPORARY stub implementing the contract for CompressionType.None only.
// Stream C deletes this file and provides the real implementation.
public static partial class BlockCompression
{
    public static partial bool IsSupported(CompressionType type) => type == CompressionType.None;

    public static partial bool TryCompress(CompressionType type, ReadOnlySpan<byte> raw, int formatVersion, int level, IBufferWriter<byte> output) => false;

    public static partial void Decompress(CompressionType type, ReadOnlySpan<byte> compressed, int formatVersion, IBufferWriter<byte> output)
    {
        if (type != CompressionType.None) ThrowHelper.NotSupported("compression type " + type);
        output.Write(compressed);
    }
}
