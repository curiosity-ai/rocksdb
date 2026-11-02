using System.Runtime.CompilerServices;

namespace RocksDb.Managed.Wal;

/// <summary>
/// Physical record types of the log format. Port of <c>log::RecordType</c> (db/log_format.h). The byte values are
/// part of the on-disk format.
/// </summary>
public enum LogRecordType : byte
{
    /// <summary>Zero is reserved for preallocated files.</summary>
    Zero = 0,
    Full = 1,
    // For fragments
    First = 2,
    Middle = 3,
    Last = 4,
    // For recycled log files
    RecyclableFull = 5,
    RecyclableFirst = 6,
    RecyclableMiddle = 7,
    RecyclableLast = 8,
    /// <summary>Compression type used for WAL records (must be the first record of the log).</summary>
    SetCompressionType = 9,
    /// <summary>User-defined timestamp sizes that apply to all subsequent records.</summary>
    UserDefinedTimestampSize = 10,
    RecyclableUserDefinedTimestampSize = 11,
    /// <summary>Info about the predecessor WAL (track_and_verify_wals). Has the "safe to ignore" bit set.</summary>
    PredecessorWalInfo = 130,
    RecyclePredecessorWalInfo = 131,
}

/// <summary>
/// Log file layout constants. Port of db/log_format.h.
/// <para>
/// A log file is a sequence of 32KiB blocks. Each block holds a sequence of physical records; a block never ends
/// with fewer than <c>header size</c> bytes of free space (those bytes are zero-filled as a trailer). A logical
/// record is split into First/Middle/Last fragments when it does not fit in the current block.
/// </para>
/// <code>
/// Legacy record     := checksum: uint32 (masked crc32c of type and payload)
///                      length:   uint16 (little-endian)
///                      type:     uint8
///                      payload:  uint8[length]
/// Recyclable record := checksum: uint32 (masked crc32c of type, log number and payload)
///                      length:   uint16
///                      type:     uint8
///                      log number: uint32 (low 32 bits)
///                      payload:  uint8[length]
/// </code>
/// </summary>
public static class LogFormat
{
    /// <summary>kBlockSize.</summary>
    public const int BlockSize = 32768;

    /// <summary>kHeaderSize: checksum (4 bytes), length (2 bytes), type (1 byte).</summary>
    public const int HeaderSize = 4 + 2 + 1;

    /// <summary>kRecyclableHeaderSize: checksum (4), length (2), type (1), log number (4).</summary>
    public const int RecyclableHeaderSize = 4 + 2 + 1 + 4;

    /// <summary>kRecordTypeSafeIgnoreMask: unknown record types with this bit set can be safely ignored.</summary>
    public const byte RecordTypeSafeIgnoreMask = 1 << 7;

    /// <summary>kMaxRecordType.</summary>
    public const byte MaxRecordType = (byte)LogRecordType.RecyclePredecessorWalInfo;

    /// <summary>Whether a record of type <paramref name="type"/> uses the 11-byte recyclable header.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsRecyclableType(byte type) =>
        (type >= (byte)LogRecordType.RecyclableFull && type <= (byte)LogRecordType.RecyclableLast) ||
        type == (byte)LogRecordType.RecyclableUserDefinedTimestampSize ||
        type == (byte)LogRecordType.RecyclePredecessorWalInfo;

    /// <summary>Whether <paramref name="type"/> is a WAL metadata record (never compressed, never fragmented).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool IsMetadataType(byte type) =>
        type == (byte)LogRecordType.SetCompressionType ||
        type == (byte)LogRecordType.PredecessorWalInfo ||
        type == (byte)LogRecordType.RecyclePredecessorWalInfo ||
        type == (byte)LogRecordType.UserDefinedTimestampSize ||
        type == (byte)LogRecordType.RecyclableUserDefinedTimestampSize;
}

/// <summary>
/// How WAL corruption is handled during recovery. Port of <c>WALRecoveryMode</c> (include/rocksdb/options.h); the
/// numeric values match the C++ enum (and the OPTIONS file encoding).
/// </summary>
public enum WalRecoveryMode : byte
{
    /// <summary>Original LevelDB recovery: tolerate an incomplete record in the tail of any log.</summary>
    TolerateCorruptedTailRecords = 0,
    /// <summary>Recover from a clean shutdown: any corruption is reported.</summary>
    AbsoluteConsistency = 1,
    /// <summary>Recover to a point-in-time consistent state: stop replay at the first corruption (the default).</summary>
    PointInTimeRecovery = 2,
    /// <summary>Recover as much as possible, skipping corrupted records.</summary>
    SkipAnyCorruptedRecords = 3,
}
