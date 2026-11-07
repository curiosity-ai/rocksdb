using System.IO.Hashing;
using System.Runtime.CompilerServices;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Table;

/// <summary>
/// Block/footer checksums. Port of <c>ComputeBuiltinChecksum</c>, <c>ComputeBuiltinChecksumWithLastByte</c>
/// (table/format.cc), <c>ChecksumModifierForContext</c> (table/format.h) and <c>VerifyBlockChecksum</c>
/// (table/block_based/reader_common.cc).
/// </summary>
public static class BlockChecksum
{
    // Streaming hashers for the xxHash32/xxHash64 "with last byte" case; cached per thread to avoid
    // per-block allocation.
    [ThreadStatic] private static XxHash32? t_xxh32;
    [ThreadStatic] private static XxHash64? t_xxh64;

    /// <summary>
    /// Port of ChecksumModifierForContext: for format_version &gt;= 6 the stored checksum of a block at
    /// <paramref name="offset"/> is the raw checksum plus this modifier (0 when base context checksum is 0).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint ChecksumModifierForContext(uint baseContextChecksum, ulong offset)
    {
        uint allOrNothing = 0u - (baseContextChecksum != 0 ? 1u : 0u);
        uint modifier = baseContextChecksum ^ ((uint)offset + (uint)(offset >> 32));
        return modifier & allOrNothing;
    }

    /// <summary>Custom handling of the last byte for XXH3 (xor with a byte multiple of a random prime; its own inverse).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static uint ModifyChecksumForLastByte(uint checksum, byte lastByte) => checksum ^ (lastByte * 0x6b9083d9u);

    /// <summary>Port of ComputeBuiltinChecksum.</summary>
    public static uint ComputeBuiltinChecksum(ChecksumType type, ReadOnlySpan<byte> data)
    {
        switch (type)
        {
            case ChecksumType.Crc32c:
                return Crc32C.Mask(Crc32C.Value(data));
            case ChecksumType.XxHash:
                return XxHash32.HashToUInt32(data, 0);
            case ChecksumType.XxHash64:
                return (uint)XxHash64.HashToUInt64(data, 0);
            case ChecksumType.Xxh3:
                if (data.Length == 0) return 0; // special case, see format.cc
                return ModifyChecksumForLastByte((uint)XxHash3.HashToUInt64(data[..^1], 0), data[^1]);
            default:
                return 0;
        }
    }

    /// <summary>
    /// Port of ComputeBuiltinChecksumWithLastByte: checksum of <c>data || lastByte</c> (lastByte is the block's
    /// compression type in the trailer).
    /// </summary>
    public static uint ComputeBuiltinChecksumWithLastByte(ChecksumType type, ReadOnlySpan<byte> data, byte lastByte)
    {
        switch (type)
        {
            case ChecksumType.Crc32c:
            {
                uint crc = Crc32C.Value(data);
                crc = Crc32C.Extend(crc, new ReadOnlySpan<byte>(in lastByte));
                return Crc32C.Mask(crc);
            }
            case ChecksumType.XxHash:
            {
                XxHash32 h = t_xxh32 ??= new XxHash32(0);
                h.Reset();
                h.Append(data);
                h.Append(new ReadOnlySpan<byte>(in lastByte));
                return h.GetCurrentHashAsUInt32();
            }
            case ChecksumType.XxHash64:
            {
                XxHash64 h = t_xxh64 ??= new XxHash64(0);
                h.Reset();
                h.Append(data);
                h.Append(new ReadOnlySpan<byte>(in lastByte));
                return (uint)h.GetCurrentHashAsUInt64();
            }
            case ChecksumType.Xxh3:
                return ModifyChecksumForLastByte((uint)XxHash3.HashToUInt64(data, 0), lastByte);
            default:
                return 0;
        }
    }

    /// <summary>
    /// Port of VerifyBlockChecksum. <paramref name="blockWithTrailer"/> is the serialized block: payload of
    /// <c>Length - 5</c> bytes, compression type byte, stored checksum. Returns false on mismatch.
    /// </summary>
    public static bool Verify(ChecksumType type, uint baseContextChecksum, ReadOnlySpan<byte> blockWithTrailer, ulong offset)
    {
        if (type == ChecksumType.NoChecksum) return true;
        int len = blockWithTrailer.Length - 4; // payload + compression type
        uint stored = Coding.DecodeFixed32(blockWithTrailer[len..]);
        uint computed = ComputeBuiltinChecksum(type, blockWithTrailer[..len]);
        stored -= ChecksumModifierForContext(baseContextChecksum, offset);
        return stored == computed;
    }

    /// <summary>Computes the 4-byte trailer checksum for a block written at <paramref name="offset"/> (builder side).</summary>
    public static uint ComputeTrailerChecksum(ChecksumType type, uint baseContextChecksum, ReadOnlySpan<byte> payload, byte compressionType, ulong offset)
        => ComputeBuiltinChecksumWithLastByte(type, payload, compressionType) + ChecksumModifierForContext(baseContextChecksum, offset);
}
