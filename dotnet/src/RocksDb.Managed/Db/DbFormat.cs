using System.Runtime.CompilerServices;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Db;

/// <summary>Mirrors rocksdb::EntryType (db/dbformat.h). Byte values are part of the on-disk format.</summary>
public enum EntryType : byte
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
    public const EntryType ValueTypeForSeek = EntryType.ValuePreferredSeqno;
    /// <summary>kValueTypeForSeekForPrev.</summary>
    public const EntryType ValueTypeForSeekForPrev = EntryType.Deletion;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong PackSequenceAndType(ulong seq, EntryType t) => (seq << 8) | (byte)t;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void UnpackSequenceAndType(ulong packed, out ulong seq, out EntryType t)
    {
        seq = packed >> 8;
        t = (EntryType)(byte)packed;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ReadOnlySpan<byte> ExtractUserKey(ReadOnlySpan<byte> internalKey) => internalKey[..^InternalKeyFooterSize];

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong ExtractFooter(ReadOnlySpan<byte> internalKey) => Coding.DecodeFixed64(internalKey[^InternalKeyFooterSize..]);

    /// <summary>Writes user_key + packed footer into dst; returns bytes written.</summary>
    public static int AppendInternalKey(Span<byte> dst, ReadOnlySpan<byte> userKey, ulong seq, EntryType t)
    {
        userKey.CopyTo(dst);
        Coding.EncodeFixed64(dst[userKey.Length..], PackSequenceAndType(seq, t));
        return userKey.Length + InternalKeyFooterSize;
    }

    // ---- user-defined timestamps (db/dbformat.h) ------------------------------------------------------------

    /// <summary>Port of StripTimestampFromUserKey: the user key without its trailing <paramref name="timestampSize"/> bytes.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ReadOnlySpan<byte> StripTimestampFromUserKey(ReadOnlySpan<byte> userKey, int timestampSize) =>
        userKey[..^timestampSize];

    /// <summary>Port of ExtractTimestampFromUserKey: the trailing <paramref name="timestampSize"/> bytes of the user key.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ReadOnlySpan<byte> ExtractTimestampFromUserKey(ReadOnlySpan<byte> userKey, int timestampSize) =>
        userKey[^timestampSize..];

    /// <summary>Port of ExtractUserKeyAndStripTimestamp: the user key of an internal key without its timestamp.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ReadOnlySpan<byte> ExtractUserKeyAndStripTimestamp(ReadOnlySpan<byte> internalKey, int timestampSize) =>
        internalKey[..^(InternalKeyFooterSize + timestampSize)];

    /// <summary>Port of ExtractTimestampFromKey: the timestamp of an internal key.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ReadOnlySpan<byte> ExtractTimestampFromKey(ReadOnlySpan<byte> internalKey, int timestampSize) =>
        internalKey.Slice(internalKey.Length - InternalKeyFooterSize - timestampSize, timestampSize);

    /// <summary>
    /// Writes <c>userKey | timestamp | footer(seq, t)</c> into <paramref name="dst"/> (AppendInternalKeyWithDifferentTimestamp
    /// when <paramref name="userKey"/> is already stripped). Returns bytes written.
    /// </summary>
    public static int AppendInternalKey(Span<byte> dst, ReadOnlySpan<byte> userKey, ReadOnlySpan<byte> timestamp, ulong seq, EntryType t)
    {
        userKey.CopyTo(dst);
        timestamp.CopyTo(dst[userKey.Length..]);
        int n = userKey.Length + timestamp.Length;
        Coding.EncodeFixed64(dst[n..], PackSequenceAndType(seq, t));
        return n + InternalKeyFooterSize;
    }

    /// <summary>Port of AppendKeyWithMinTimestamp: <paramref name="key"/> followed by <paramref name="timestampSize"/> zero bytes.</summary>
    public static void AppendKeyWithMinTimestamp(System.Buffers.IBufferWriter<byte> dst, ReadOnlySpan<byte> key, int timestampSize)
    {
        Span<byte> d = dst.GetSpan(key.Length + timestampSize);
        key.CopyTo(d);
        d.Slice(key.Length, timestampSize).Clear();
        dst.Advance(key.Length + timestampSize);
    }

    /// <summary>Port of AppendKeyWithMaxTimestamp: <paramref name="key"/> followed by <paramref name="timestampSize"/> 0xff bytes.</summary>
    public static void AppendKeyWithMaxTimestamp(System.Buffers.IBufferWriter<byte> dst, ReadOnlySpan<byte> key, int timestampSize)
    {
        Span<byte> d = dst.GetSpan(key.Length + timestampSize);
        key.CopyTo(d);
        d.Slice(key.Length, timestampSize).Fill(0xff);
        dst.Advance(key.Length + timestampSize);
    }

    public static bool TryParseInternalKey(ReadOnlySpan<byte> internalKey, out ParsedInternalKey result)
    {
        if (internalKey.Length < InternalKeyFooterSize)
        {
            result = default;
            return false;
        }
        UnpackSequenceAndType(ExtractFooter(internalKey), out ulong seq, out EntryType t);
        result = new ParsedInternalKey(seq, t, internalKey.Length - InternalKeyFooterSize);
        // ParseInternalKey (db/dbformat.h) only accepts IsExtendedValueType: WAL-only types are corruption.
        return IsExtendedValueType(t);
    }

    /// <summary>Port of IsValueType (db/dbformat.h): the types that may appear in memtables and SST data blocks.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsValueType(EntryType t) =>
        t <= EntryType.Merge || t == EntryType.SingleDeletion || t == EntryType.BlobIndex || t == EntryType.DeletionWithTimestamp ||
        t == EntryType.WideColumnEntity || t == EntryType.ValuePreferredSeqno;

    /// <summary>
    /// Port of IsExtendedValueType (db/dbformat.h): <see cref="IsValueType"/> plus range deletions and
    /// <see cref="EntryType.MaxValid"/> (used by truncated range tombstone boundaries).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsExtendedValueType(EntryType t) => IsValueType(t) || t == EntryType.RangeDeletion || t == EntryType.MaxValid;
}

/// <summary>Parsed footer of an internal key. The user key is <c>internalKey[..UserKeyLength]</c>.</summary>
public readonly record struct ParsedInternalKey(ulong Sequence, EntryType Type, int UserKeyLength);
