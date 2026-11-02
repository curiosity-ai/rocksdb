using System.Buffers;

namespace RocksDb.Managed.Compression;

/// <summary>
/// CONTRACT (owned by stream C — hashing/filters/compression). Compresses/decompresses a block payload
/// using RocksDB's framing for the given table <c>format_version</c> (format_version >= 2 prefixes
/// LZ4/ZSTD/... payloads with a varint32 uncompressed size; Snappy carries its own length).
/// Port of util/compression.h (CompressData / UncompressData / *_Compress / *_Uncompress).
/// </summary>
public static partial class BlockCompression
{
    /// <summary>Whether this build can (de)compress the given type.</summary>
    public static partial bool IsSupported(CompressionType type);

    /// <summary>
    /// Appends the compressed representation of <paramref name="raw"/> to <paramref name="output"/>.
    /// Returns false if compression is unsupported or not beneficial (caller then stores raw, type None).
    /// </summary>
    public static partial bool TryCompress(CompressionType type, ReadOnlySpan<byte> raw, int formatVersion, int level, IBufferWriter<byte> output);

    /// <summary>Appends the decompressed block to <paramref name="output"/>. Throws Corruption on malformed input.</summary>
    public static partial void Decompress(CompressionType type, ReadOnlySpan<byte> compressed, int formatVersion, IBufferWriter<byte> output);
}
