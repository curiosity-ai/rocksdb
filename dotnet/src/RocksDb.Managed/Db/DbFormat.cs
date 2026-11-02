using System.Runtime.CompilerServices;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Db;

/// <summary>Mirrors rocksdb::ValueType (db/dbformat.h). Byte values are part of the on-disk format.</summary>
public enum ValueType : byte
{
    Deletion = 0x0,
    Value = 0x1,
    Merge = 0x2,
    LogData = 0x3,                       // WAL only.
    ColumnFamilyDeletion = 0x4,          // WAL only.
    ColumnFamilyValue = 0x5,             // WAL only.
    ColumnFamilyMerge = 0x6,             // WAL only.
    SingleDeletion = 0x7,
    ColumnFamilySingleDeletion = 0x8,    // WAL only.
    BeginPrepareXID = 0x9,               // WAL only.
    EndPrepareXID = 0xA,                 // WAL only.
    CommitXID = 0xB,                     // WAL only.
    RollbackXID = 0xC,                   // WAL only.
    Noop = 0xD,                          // WAL only.
    ColumnFamilyRangeDeletion = 0xE,     // WAL only.
    RangeDeletion = 0xF,                 // meta block
    ColumnFamilyBlobIndex = 0x10,        // Blob DB only
    BlobIndex = 0x11,                    // Blob DB only
    BeginPersistedPrepareXID = 0x12,     // WAL only.
    BeginUnprepareXID = 0x13,            // WAL only.
    DeletionWithTimestamp = 0x14,
    CommitXIDAndTimestamp = 0x15,        // WAL only
    WideColumnEntity = 0x16,
    ColumnFamilyWideColumnEntity = 0x17, // WAL only
    ValuePreferredSeqno = 0x18,
    ColumnFamilyValuePreferredSeqno = 0x19, // WAL only
    MaxValid = 0x1A,
    MaxValue = 0x7F,
}

/// <summary>
/// Internal key helpers (db/dbformat.h). An internal key is
/// <c>user_key | fixed64(PackSequenceAndType(seq, type))</c>.
/// </summary>
public static class DbFormat
{
    public const ulong MaxSequenceNumber = (1UL << 56) - 1;
    public const int InternalKeyFooterSize = 8;

    /// <summary>kValueTypeForSeek: the type with the highest byte value that may appear in a key, used when seeking.</summary>
    public const ValueType ValueTypeForSeek = ValueType.ValuePreferredSeqno;
    /// <summary>kValueTypeForSeekForPrev.</summary>
    public const ValueType ValueTypeForSeekForPrev = ValueType.Deletion;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong PackSequenceAndType(ulong seq, ValueType t) => (seq << 8) | (byte)t;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void UnpackSequenceAndType(ulong packed, out ulong seq, out ValueType t)
    {
        seq = packed >> 8;
        t = (ValueType)(byte)packed;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ReadOnlySpan<byte> ExtractUserKey(ReadOnlySpan<byte> internalKey) => internalKey[..^InternalKeyFooterSize];

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong ExtractFooter(ReadOnlySpan<byte> internalKey) => Coding.DecodeFixed64(internalKey[^InternalKeyFooterSize..]);

    /// <summary>Writes user_key + packed footer into dst; returns bytes written.</summary>
    public static int AppendInternalKey(Span<byte> dst, ReadOnlySpan<byte> userKey, ulong seq, ValueType t)
    {
        userKey.CopyTo(dst);
        Coding.EncodeFixed64(dst[userKey.Length..], PackSequenceAndType(seq, t));
        return userKey.Length + InternalKeyFooterSize;
    }

    public static bool TryParseInternalKey(ReadOnlySpan<byte> internalKey, out ParsedInternalKey result)
    {
        if (internalKey.Length < InternalKeyFooterSize)
        {
            result = default;
            return false;
        }
        UnpackSequenceAndType(ExtractFooter(internalKey), out ulong seq, out ValueType t);
        result = new ParsedInternalKey(seq, t, internalKey.Length - InternalKeyFooterSize);
        return (byte)t < (byte)ValueType.MaxValid;
    }
}

/// <summary>Parsed footer of an internal key. The user key is <c>internalKey[..UserKeyLength]</c>.</summary>
public readonly record struct ParsedInternalKey(ulong Sequence, ValueType Type, int UserKeyLength);
