using RocksDb.Managed.Compression;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Wal;

/// <summary>
/// Payload of a <see cref="LogRecordType.SetCompressionType"/> record: <c>fixed32 compression_type</c>.
/// Port of <c>CompressionTypeRecord</c> (util/compression.h).
/// </summary>
public static class CompressionTypeRecord
{
    public const int EncodedLength = 4;

    public static void EncodeTo(Span<byte> dst, CompressionType type) => Coding.EncodeFixed32(dst, (uint)type);

    /// <summary>Decodes the record. Returns false when truncated or when the type is not supported for streaming WAL
    /// compression (only None and ZSTD are, matching <c>StreamingCompressionTypeSupported</c>).</summary>
    public static bool TryDecode(ReadOnlySpan<byte> src, out CompressionType type)
    {
        if (!Coding.GetFixed32(ref src, out uint v) || (v != (uint)CompressionType.None && v != (uint)CompressionType.Zstd))
        {
            type = CompressionType.None;
            return false;
        }
        type = (CompressionType)v;
        return true;
    }
}

/// <summary>
/// Payload of a user-defined timestamp size record: a sequence of <c>fixed32 cf_id, fixed16 ts_sz</c> pairs.
/// Port of <c>UserDefinedTimestampSizeRecord</c> (util/udt_util.h).
/// </summary>
public static class UserDefinedTimestampSizeRecord
{
    /// <summary>kSizePerColumnFamily.</summary>
    public const int SizePerColumnFamily = 4 + 2;

    public static int EncodedLength(int entries) => entries * SizePerColumnFamily;

    public static void EncodeEntry(Span<byte> dst, uint columnFamilyId, ushort timestampSize)
    {
        Coding.EncodeFixed32(dst, columnFamilyId);
        Coding.EncodeFixed16(dst[4..], timestampSize);
    }

    /// <summary>Validates the payload length (must be a multiple of <see cref="SizePerColumnFamily"/>).</summary>
    public static bool IsValid(ReadOnlySpan<byte> src) => src.Length % SizePerColumnFamily == 0;

    public static int EntryCount(ReadOnlySpan<byte> src) => src.Length / SizePerColumnFamily;

    public static void DecodeEntry(ReadOnlySpan<byte> src, int index, out uint columnFamilyId, out ushort timestampSize)
    {
        ReadOnlySpan<byte> e = src.Slice(index * SizePerColumnFamily, SizePerColumnFamily);
        columnFamilyId = Coding.DecodeFixed32(e);
        timestampSize = Coding.DecodeFixed16(e[4..]);
    }
}

/// <summary>
/// Information about the predecessor WAL recorded at the start of a WAL when <c>track_and_verify_wals</c> is on:
/// <c>fixed64 log_number, fixed64 size_bytes, fixed64 last_seqno_recorded</c>. Port of <c>PredecessorWALInfo</c>
/// (db/dbformat.h).
/// </summary>
public readonly record struct PredecessorWalInfo(ulong LogNumber, ulong SizeBytes, ulong LastSequenceRecorded)
{
    public const int EncodedLength = 24;

    public void EncodeTo(Span<byte> dst)
    {
        Coding.EncodeFixed64(dst, LogNumber);
        Coding.EncodeFixed64(dst[8..], SizeBytes);
        Coding.EncodeFixed64(dst[16..], LastSequenceRecorded);
    }

    public static bool TryDecode(ReadOnlySpan<byte> src, out PredecessorWalInfo info)
    {
        if (Coding.GetFixed64(ref src, out ulong logNumber) &&
            Coding.GetFixed64(ref src, out ulong size) &&
            Coding.GetFixed64(ref src, out ulong lastSeq))
        {
            info = new PredecessorWalInfo(logNumber, size, lastSeq);
            return true;
        }
        info = default;
        return false;
    }
}
