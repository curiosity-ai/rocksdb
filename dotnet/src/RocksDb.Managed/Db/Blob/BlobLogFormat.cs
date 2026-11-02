using RocksDb.Managed.Compression;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Db.Blob;

/// <summary>
/// Constants and helpers of the blob log (blob file) format. Port of db/blob/blob_log_format.{h,cc}.
/// <code>
/// header  (30 bytes): magic(fixed32) version(fixed32) cf_id(fixed32) flags(1, bit0 = has_ttl) compression(1)
///                     expiration_range.first(fixed64) expiration_range.second(fixed64)
/// record  (32 bytes + key + value): key_size(fixed64) value_size(fixed64) expiration(fixed64)
///                     header_crc(fixed32, masked crc32c of the first 24 bytes) blob_crc(fixed32, masked crc32c of key+value)
/// footer  (32 bytes): magic(fixed32) blob_count(fixed64) expiration_range(2 x fixed64) crc(fixed32, masked crc32c of the first 28 bytes)
/// </code>
/// </summary>
internal static class BlobLogFormat
{
    /// <summary>kMagicNumber = 2395959 (0x00248f37).</summary>
    public const uint MagicNumber = 2395959;
    /// <summary>kVersion1.</summary>
    public const uint Version1 = 1;
    /// <summary>BlobLogHeader::kSize.</summary>
    public const int HeaderSize = 30;
    /// <summary>BlobLogFooter::kSize.</summary>
    public const int FooterSize = 32;
    /// <summary>BlobLogRecord::kHeaderSize.</summary>
    public const int RecordHeaderSize = 32;

    /// <summary>BlobLogRecord::CalculateAdjustmentForRecordHeader: distance from the record start to the blob value.</summary>
    public static ulong RecordHeaderAdjustment(ulong keySize) => keySize + RecordHeaderSize;

    /// <summary>BlobLogHeader::EncodeTo (no TTL: has_ttl = false, expiration range (0, 0)).</summary>
    public static void EncodeHeader(Span<byte> dst, uint columnFamilyId, CompressionType compression, bool hasTtl, ulong expirationFirst, ulong expirationSecond)
    {
        Coding.EncodeFixed32(dst, MagicNumber);
        Coding.EncodeFixed32(dst[4..], Version1);
        Coding.EncodeFixed32(dst[8..], columnFamilyId);
        dst[12] = (byte)(hasTtl ? 1 : 0);
        dst[13] = (byte)compression;
        Coding.EncodeFixed64(dst[14..], expirationFirst);
        Coding.EncodeFixed64(dst[22..], expirationSecond);
    }

    /// <summary>Decoded BlobLogHeader.</summary>
    public readonly record struct Header(uint Version, uint ColumnFamilyId, CompressionType Compression, bool HasTtl, ulong ExpirationFirst, ulong ExpirationSecond);

    /// <summary>BlobLogHeader::DecodeFrom. Throws Corruption.</summary>
    public static Header DecodeHeader(ReadOnlySpan<byte> src)
    {
        const string Msg = "Error while decoding blob log header: ";
        if (src.Length != HeaderSize) ThrowHelper.Corruption(Msg + "Unexpected blob file header size");
        uint magic = Coding.DecodeFixed32(src);
        uint version = Coding.DecodeFixed32(src[4..]);
        uint cf = Coding.DecodeFixed32(src[8..]);
        if (magic != MagicNumber) ThrowHelper.Corruption(Msg + "Magic number mismatch");
        if (version != Version1) ThrowHelper.Corruption(Msg + "Unknown header version");
        byte flags = src[12];
        var compression = (CompressionType)src[13];
        return new Header(version, cf, compression, (flags & 1) == 1, Coding.DecodeFixed64(src[14..]), Coding.DecodeFixed64(src[22..]));
    }

    /// <summary>BlobLogFooter::EncodeTo.</summary>
    public static void EncodeFooter(Span<byte> dst, ulong blobCount, ulong expirationFirst, ulong expirationSecond)
    {
        Coding.EncodeFixed32(dst, MagicNumber);
        Coding.EncodeFixed64(dst[4..], blobCount);
        Coding.EncodeFixed64(dst[12..], expirationFirst);
        Coding.EncodeFixed64(dst[20..], expirationSecond);
        uint crc = Crc32C.Mask(Crc32C.Value(dst[..28]));
        Coding.EncodeFixed32(dst[28..], crc);
    }

    /// <summary>Decoded BlobLogFooter.</summary>
    public readonly record struct Footer(ulong BlobCount, ulong ExpirationFirst, ulong ExpirationSecond);

    /// <summary>BlobLogFooter::DecodeFrom. Throws Corruption.</summary>
    public static Footer DecodeFooter(ReadOnlySpan<byte> src)
    {
        const string Msg = "Error while decoding blob log footer: ";
        if (src.Length != FooterSize) ThrowHelper.Corruption(Msg + "Unexpected blob file footer size");
        uint srcCrc = Crc32C.Mask(Crc32C.Value(src[..(FooterSize - sizeof(uint))]));
        uint magic = Coding.DecodeFixed32(src);
        if (magic != MagicNumber) ThrowHelper.Corruption(Msg + "Magic number mismatch");
        if (srcCrc != Coding.DecodeFixed32(src[28..])) ThrowHelper.Corruption(Msg + "CRC mismatch");
        return new Footer(Coding.DecodeFixed64(src[4..]), Coding.DecodeFixed64(src[12..]), Coding.DecodeFixed64(src[20..]));
    }

    /// <summary>BlobLogRecord::EncodeHeaderTo: the 32-byte record header for <paramref name="key"/> / <paramref name="value"/>.</summary>
    public static void EncodeRecordHeader(Span<byte> dst, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ulong expiration)
    {
        Coding.EncodeFixed64(dst, (ulong)key.Length);
        Coding.EncodeFixed64(dst[8..], (ulong)value.Length);
        Coding.EncodeFixed64(dst[16..], expiration);
        Coding.EncodeFixed32(dst[24..], Crc32C.Mask(Crc32C.Value(dst[..24])));
        uint blobCrc = Crc32C.Extend(Crc32C.Value(key), value);
        Coding.EncodeFixed32(dst[28..], Crc32C.Mask(blobCrc));
    }

    /// <summary>Decoded BlobLogRecord header.</summary>
    public readonly record struct RecordHeader(ulong KeySize, ulong ValueSize, ulong Expiration, uint HeaderCrc, uint BlobCrc);

    /// <summary>BlobLogRecord::DecodeHeaderFrom. Throws Corruption.</summary>
    public static RecordHeader DecodeRecordHeader(ReadOnlySpan<byte> src)
    {
        const string Msg = "Error while decoding blob record: ";
        if (src.Length != RecordHeaderSize) ThrowHelper.Corruption(Msg + "Unexpected blob record header size");
        uint srcCrc = Crc32C.Mask(Crc32C.Value(src[..(RecordHeaderSize - 8)]));
        var h = new RecordHeader(Coding.DecodeFixed64(src), Coding.DecodeFixed64(src[8..]), Coding.DecodeFixed64(src[16..]),
            Coding.DecodeFixed32(src[24..]), Coding.DecodeFixed32(src[28..]));
        if (srcCrc != h.HeaderCrc) ThrowHelper.Corruption(Msg + "Header CRC mismatch");
        return h;
    }

    /// <summary>BlobLogRecord::CheckBlobCRC.</summary>
    public static bool CheckBlobCrc(uint blobCrc, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) =>
        Crc32C.Mask(Crc32C.Extend(Crc32C.Value(key), value)) == blobCrc;

    /// <summary>IsValidBlobOffset (blob_log_format.h).</summary>
    public static bool IsValidBlobOffset(ulong valueOffset, ulong keySize, ulong valueSize, ulong fileSize)
    {
        const ulong MinPrefix = HeaderSize + RecordHeaderSize;
        if (valueOffset < MinPrefix) return false;
        if (valueOffset - MinPrefix < keySize) return false;
        if (fileSize < FooterSize) return false;
        ulong maxValueEnd = fileSize - FooterSize;
        if (valueSize > maxValueEnd) return false;
        if (valueOffset > maxValueEnd - valueSize) return false;
        return true;
    }
}
