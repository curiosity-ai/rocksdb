using System.Buffers;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Db;

/// <summary>
/// 128-bit SST unique id in its internal representation (table/unique_id_impl.h UniqueId64x2).
/// Persisted in the MANIFEST as two little-endian fixed64 values (EncodeUniqueIdBytes).
/// </summary>
public readonly record struct UniqueId64x2(ulong Lo, ulong Hi)
{
    public static readonly UniqueId64x2 Null = default;
    public bool IsNull => Lo == 0 && Hi == 0;
}

/// <summary>
/// Port of FileMetaData + FileDescriptor (db/version_edit.h): everything the MANIFEST records about one
/// SST file. In-memory-only members (refs, table reader pin, sampled stats, being_compacted, data-entry
/// stats) belong to the version set and are not modelled here.
/// </summary>
public sealed class FileMetaData
{
    /// <summary>kFileNumberMask: low 62 bits of the packed number/path-id word.</summary>
    public const ulong FileNumberMask = 0x3FFFFFFFFFFFFFFF;
    public const ulong UnknownOldestAncestorTime = 0;
    public const ulong UnknownFileCreationTime = 0;
    public const ulong UnknownEpochNumber = 0;
    /// <summary>kReservedEpochNumberForFileIngestedBehind.</summary>
    public const ulong ReservedEpochNumberForFileIngestedBehind = 1;
    /// <summary>kInvalidBlobFileNumber.</summary>
    public const ulong InvalidBlobFileNumber = 0;
    /// <summary>kUnknownFileChecksumFuncName (include/rocksdb/file_checksum.h).</summary>
    public const string UnknownFileChecksumFuncName = "Unknown";

    public ulong FileNumber { get; set; }
    /// <summary>Index into DbOptions.db_paths / cf_paths (0..3).</summary>
    public uint PathId { get; set; }
    public ulong FileSize { get; set; }
    /// <summary>Smallest internal key (user key + 8-byte footer).</summary>
    public byte[] Smallest { get; set; } = [];
    /// <summary>Largest internal key (user key + 8-byte footer).</summary>
    public byte[] Largest { get; set; } = [];
    public ulong SmallestSeqno { get; set; } = DbFormat.MaxSequenceNumber;
    public ulong LargestSeqno { get; set; }
    public bool MarkedForCompaction { get; set; }
    public Temperature Temperature { get; set; } = Temperature.Unknown;
    public ulong OldestBlobFileNumber { get; set; } = InvalidBlobFileNumber;
    public ulong OldestAncestorTime { get; set; } = UnknownOldestAncestorTime;
    public ulong FileCreationTime { get; set; } = UnknownFileCreationTime;
    public ulong EpochNumber { get; set; } = UnknownEpochNumber;
    /// <summary>Raw file checksum bytes (empty = unknown).</summary>
    public byte[] FileChecksum { get; set; } = [];
    public string FileChecksumFuncName { get; set; } = UnknownFileChecksumFuncName;
    public UniqueId64x2 UniqueId { get; set; }
    public ulong CompensatedRangeDeletionSize { get; set; }
    /// <summary>Bytes from the end of the last data block to the end of the file.</summary>
    public ulong TailSize { get; set; }
    public bool UserDefinedTimestampsPersisted { get; set; } = true;
    public byte[] MinTimestamp { get; set; } = [];
    public byte[] MaxTimestamp { get; set; } = [];
    /// <summary>Opaque file system metadata used to accelerate file opening (kFileOpenMetadata).</summary>
    public byte[] FileOpenMetadata { get; set; } = [];

    /// <summary>PackFileNumberAndPathId.</summary>
    public ulong PackedNumberAndPathId => PackFileNumberAndPathId(FileNumber, PathId);

    public static ulong PackFileNumberAndPathId(ulong number, ulong pathId) => number | (pathId * (FileNumberMask + 1));

    public FileMetaData Clone()
    {
        var c = (FileMetaData)MemberwiseClone();
        return c; // byte[] members are treated as immutable once assigned
    }

    public override string ToString() =>
        $"#{FileNumber} size={FileSize} seq=[{SmallestSeqno},{LargestSeqno}] epoch={EpochNumber}";
}

/// <summary>Port of db/blob/blob_file_addition.{h,cc}.</summary>
public sealed class BlobFileAddition
{
    private const uint EndMarker = 0;
    private const uint ForwardIncompatibleMask = 1u << 6;

    public ulong BlobFileNumber { get; set; }
    public ulong TotalBlobCount { get; set; }
    public ulong TotalBlobBytes { get; set; }
    public string ChecksumMethod { get; set; } = "";
    public byte[] ChecksumValue { get; set; } = [];

    public BlobFileAddition() { }

    public BlobFileAddition(ulong blobFileNumber, ulong totalBlobCount, ulong totalBlobBytes, string checksumMethod, byte[] checksumValue)
    {
        BlobFileNumber = blobFileNumber;
        TotalBlobCount = totalBlobCount;
        TotalBlobBytes = totalBlobBytes;
        ChecksumMethod = checksumMethod;
        ChecksumValue = checksumValue;
    }

    public void EncodeTo(IBufferWriter<byte> output)
    {
        Coding.PutVarint64(output, BlobFileNumber);
        Coding.PutVarint64(output, TotalBlobCount);
        Coding.PutVarint64(output, TotalBlobBytes);
        Coding.PutLengthPrefixedSlice(output, System.Text.Encoding.UTF8.GetBytes(ChecksumMethod));
        Coding.PutLengthPrefixedSlice(output, ChecksumValue);
        Coding.PutVarint32(output, EndMarker);
    }

    /// <summary>Returns null on success, else the corruption message.</summary>
    internal string? DecodeFrom(ref ReadOnlySpan<byte> input)
    {
        if (!Coding.GetVarint64(ref input, out ulong n)) return "Error decoding blob file number";
        BlobFileNumber = n;
        if (!Coding.GetVarint64(ref input, out n)) return "Error decoding total blob count";
        TotalBlobCount = n;
        if (!Coding.GetVarint64(ref input, out n)) return "Error decoding total blob bytes";
        TotalBlobBytes = n;
        if (!Coding.GetLengthPrefixedSlice(ref input, out ReadOnlySpan<byte> s)) return "Error decoding checksum method";
        ChecksumMethod = System.Text.Encoding.UTF8.GetString(s);
        if (!Coding.GetLengthPrefixedSlice(ref input, out s)) return "Error decoding checksum value";
        ChecksumValue = s.ToArray();
        while (true)
        {
            if (!Coding.GetVarint32(ref input, out uint tag)) return "Error decoding custom field tag";
            if (tag == EndMarker) break;
            if ((tag & ForwardIncompatibleMask) != 0) return "Forward incompatible custom field encountered";
            if (!Coding.GetLengthPrefixedSlice(ref input, out _)) return "Error decoding custom field value";
        }
        return null;
    }

    public override string ToString() =>
        $"blob_file_number: {BlobFileNumber} total_blob_count: {TotalBlobCount} total_blob_bytes: {TotalBlobBytes} checksum_method: {ChecksumMethod}";
}

/// <summary>Port of db/blob/blob_file_garbage.{h,cc}.</summary>
public sealed class BlobFileGarbage
{
    private const uint EndMarker = 0;
    private const uint ForwardIncompatibleMask = 1u << 6;

    public ulong BlobFileNumber { get; set; }
    public ulong GarbageBlobCount { get; set; }
    public ulong GarbageBlobBytes { get; set; }

    public BlobFileGarbage() { }

    public BlobFileGarbage(ulong blobFileNumber, ulong garbageBlobCount, ulong garbageBlobBytes)
    {
        BlobFileNumber = blobFileNumber;
        GarbageBlobCount = garbageBlobCount;
        GarbageBlobBytes = garbageBlobBytes;
    }

    public void EncodeTo(IBufferWriter<byte> output)
    {
        Coding.PutVarint64(output, BlobFileNumber);
        Coding.PutVarint64(output, GarbageBlobCount);
        Coding.PutVarint64(output, GarbageBlobBytes);
        Coding.PutVarint32(output, EndMarker);
    }

    internal string? DecodeFrom(ref ReadOnlySpan<byte> input)
    {
        if (!Coding.GetVarint64(ref input, out ulong n)) return "Error decoding blob file number";
        BlobFileNumber = n;
        if (!Coding.GetVarint64(ref input, out n)) return "Error decoding garbage blob count";
        GarbageBlobCount = n;
        if (!Coding.GetVarint64(ref input, out n)) return "Error decoding garbage blob bytes";
        GarbageBlobBytes = n;
        while (true)
        {
            if (!Coding.GetVarint32(ref input, out uint tag)) return "Error decoding custom field tag";
            if (tag == EndMarker) break;
            if ((tag & ForwardIncompatibleMask) != 0) return "Forward incompatible custom field encountered";
            if (!Coding.GetLengthPrefixedSlice(ref input, out _)) return "Error decoding custom field value";
        }
        return null;
    }
}

/// <summary>Port of WalAddition (db/wal_edit.{h,cc}): a WAL created or synced, with optional synced size.</summary>
public sealed class WalAddition
{
    private const uint TagTerminate = 1;
    private const uint TagSyncedSize = 2;

    public ulong LogNumber { get; set; }
    /// <summary>Synced size in bytes, or null when unknown (kUnknownWalSize).</summary>
    public ulong? SyncedSizeInBytes { get; set; }

    public WalAddition() { }

    public WalAddition(ulong logNumber, ulong? syncedSizeInBytes)
    {
        LogNumber = logNumber;
        SyncedSizeInBytes = syncedSizeInBytes;
    }

    public void EncodeTo(IBufferWriter<byte> dst)
    {
        Coding.PutVarint64(dst, LogNumber);
        if (SyncedSizeInBytes is ulong size)
        {
            Coding.PutVarint32(dst, TagSyncedSize);
            Coding.PutVarint64(dst, size);
        }
        Coding.PutVarint32(dst, TagTerminate);
    }

    internal string? DecodeFrom(ref ReadOnlySpan<byte> src)
    {
        if (!Coding.GetVarint64(ref src, out ulong n)) return "WalAddition: Error decoding WAL log number";
        LogNumber = n;
        while (true)
        {
            if (!Coding.GetVarint32(ref src, out uint tag)) return "WalAddition: Error decoding tag";
            switch (tag)
            {
                case TagSyncedSize:
                    if (!Coding.GetVarint64(ref src, out ulong size)) return "WalAddition: Error decoding WAL file size";
                    // kUnknownWalSize (uint64 max) means "no synced size".
                    SyncedSizeInBytes = size == ulong.MaxValue ? null : size;
                    break;
                case TagTerminate:
                    return null;
                default:
                    return "WalAddition: Unknown tag " + tag.ToString(System.Globalization.CultureInfo.InvariantCulture);
            }
        }
    }

    public override string ToString() => $"log_number: {LogNumber} synced_size_in_bytes: {SyncedSizeInBytes?.ToString() ?? ulong.MaxValue.ToString()}";
}
