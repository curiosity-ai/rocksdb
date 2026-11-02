using RocksDb.Managed.Util;

namespace RocksDb.Managed.Table;

/// <summary>Constants of the block-based table format (table/format.h, table/block_based/block_based_table_reader.h, meta_blocks.cc).</summary>
public static class BlockBasedTableConstants
{
    /// <summary>kBlockBasedTableMagicNumber (block_based_table_builder.cc).</summary>
    public const ulong BlockBasedTableMagicNumber = 0x88e241b785f4cff7UL;
    /// <summary>Legacy block-based magic (format_version &lt; 2) — only used for error reporting.</summary>
    public const ulong LegacyBlockBasedTableMagicNumber = 0xdb4775248b80fb57UL;
    /// <summary>BlockBasedTable::kBlockTrailerSize: 1-byte compression type + 4-byte checksum.</summary>
    public const int BlockTrailerSize = 5;
    public const int MagicNumberLengthByte = 8;

    public const int MinSupportedFormatVersionForRead = 2;
    public const int MinSupportedFormatVersionForWrite = 2;
    /// <summary>
    /// Highest format_version this port reads/writes. Upstream main supports 8 (common key prefix, value
    /// delta escape, metaindex gap) which is not ported yet.
    /// </summary>
    public const int LatestSupportedFormatVersion = 7;

    public const string PropertiesBlockName = "rocksdb.properties";
    /// <summary>Only used with format_version &gt;= 6 (index handle lives in the metaindex rather than the footer).</summary>
    public const string IndexBlockName = "rocksdb.index";
    public const string CompressionDictBlockName = "rocksdb.compression_dict";
    public const string RangeDelBlockName = "rocksdb.range_del";
    public const string FullFilterBlockPrefix = "fullfilter.";
    public const string PartitionedFilterBlockPrefix = "partitionedfilter.";
    /// <summary>Old (pre-6.x) block-based filter prefix, unsupported.</summary>
    public const string ObsoleteFilterBlockPrefix = "filter.";

    public static bool FormatVersionUsesContextChecksum(int v) => v >= 6;
    public static bool FormatVersionUsesIndexHandleInFooter(int v) => v < 6;
    public static bool FormatVersionUsesCompressionManagerName(int v) => v >= 7;
}

/// <summary>
/// The fixed-size footer at the end of every table file. Port of <c>Footer</c> / <c>FooterBuilder</c>
/// (table/format.h, table/format.cc). Layout for format_version &gt;= 1 (always 53 bytes):
/// <code>
///   Part1: checksum type (1 byte)
///   Part2 (40 bytes):
///     fv &lt;= 5: metaindex handle, index handle (varints), zero padding
///     fv &gt;= 6: extended magic 3e 00 7a 00, footer checksum (u32), base context checksum (u32),
///             metaindex block size (u32, metaindex immediately precedes the footer),
///             16 reserved zero bytes, incompatible feature flags (u64)
///   Part3: format_version (u32), magic number (u64)
/// </code>
/// </summary>
public readonly struct Footer
{
    /// <summary>kNewVersionsEncodedLength = 1 + 2 * BlockHandle::kMaxEncodedLength + 4 + 8 = 53.</summary>
    public const int EncodedLength = 1 + 2 * BlockHandle.MaxEncodedLength + 4 + BlockBasedTableConstants.MagicNumberLengthByte;
    /// <summary>kVersion0EncodedLength (legacy format_version 0) = 48, also the minimum.</summary>
    public const int MinEncodedLength = 2 * BlockHandle.MaxEncodedLength + BlockBasedTableConstants.MagicNumberLengthByte;
    private const int Part2Size = 2 * BlockHandle.MaxEncodedLength;
    /// <summary>kKnownFooterFeatures (currently only kFooterFeatureRequireUserDefinedIndex = 1, which this port can't honour).</summary>
    private const ulong KnownFooterFeatures = 0;

    public ulong TableMagicNumber { get; init; }
    public int FormatVersion { get; init; }
    public ChecksumType ChecksumType { get; init; }
    public uint BaseContextChecksum { get; init; }
    public ulong IncompatibleFeatures { get; init; }
    public BlockHandle MetaindexHandle { get; init; }
    /// <summary>Null for format_version &gt;= 6 (look up <see cref="BlockBasedTableConstants.IndexBlockName"/> in the metaindex).</summary>
    public BlockHandle IndexHandle { get; init; }

    private static ReadOnlySpan<byte> ExtendedMagic => [0x3e, 0x00, 0x7a, 0x00];

    /// <summary>
    /// Port of FooterBuilder::Build for the block-based magic number. Writes exactly <see cref="EncodedLength"/>
    /// bytes into <paramref name="dst"/>. <paramref name="footerOffset"/> is the file offset at which the footer
    /// will be written (needed for the context checksum).
    /// </summary>
    public static void Build(Span<byte> dst, int formatVersion, ulong footerOffset, ChecksumType checksumType,
        BlockHandle metaindexHandle, BlockHandle indexHandle, uint baseContextChecksum)
    {
        if (formatVersion < BlockBasedTableConstants.MinSupportedFormatVersionForWrite || formatVersion > BlockBasedTableConstants.LatestSupportedFormatVersion)
            ThrowHelper.InvalidArgument("unsupported format_version " + formatVersion);
        dst = dst[..EncodedLength];
        dst.Clear();
        // Part 1
        dst[0] = (byte)checksumType;
        // Part 3
        Span<byte> part3 = dst[(1 + Part2Size)..];
        Coding.EncodeFixed32(part3, (uint)formatVersion);
        Coding.EncodeFixed64(part3[4..], BlockBasedTableConstants.BlockBasedTableMagicNumber);

        if (formatVersion >= 6)
        {
            if (BlockChecksum.ChecksumModifierForContext(baseContextChecksum, 0) == 0)
                ThrowHelper.InvalidArgument("base context checksum required for format_version >= 6");
            Span<byte> cur = dst[1..];
            ExtendedMagic.CopyTo(cur);
            // footer checksum at [5..9) left zero for now
            Coding.EncodeFixed32(cur[8..], baseContextChecksum);
            ulong metaindexEnd = metaindexHandle.Offset + metaindexHandle.Size + BlockBasedTableConstants.BlockTrailerSize;
            if (metaindexHandle.Size > uint.MaxValue) ThrowHelper.NotSupported("Metaindex block size > 4GB");
            if (metaindexHandle.Size != 0 && metaindexEnd != footerOffset)
                ThrowHelper.InvalidArgument("metaindex block must immediately precede the footer");
            Coding.EncodeFixed32(cur[12..], (uint)metaindexHandle.Size);
            // 16 reserved zero bytes + incompatible features (0) already zero.
            uint checksum = BlockChecksum.ComputeBuiltinChecksum(checksumType, dst);
            checksum += BlockChecksum.ChecksumModifierForContext(baseContextChecksum, footerOffset);
            Coding.EncodeFixed32(dst[5..], checksum);
        }
        else
        {
            if (baseContextChecksum != 0) ThrowHelper.InvalidArgument("context checksum requires format_version >= 6");
            Span<byte> part2 = dst.Slice(1, Part2Size);
            int n = metaindexHandle.EncodeTo(part2);
            indexHandle.EncodeTo(part2[n..]);
        }
    }

    /// <summary>
    /// Port of Footer::DecodeFrom. <paramref name="input"/> is the tail of the file (at least
    /// <see cref="MinEncodedLength"/> bytes, ideally <see cref="EncodedLength"/>), starting at file offset
    /// <paramref name="inputOffset"/>.
    /// </summary>
    public static Footer Decode(ReadOnlySpan<byte> input, ulong inputOffset)
    {
        if (input.Length < MinEncodedLength) ThrowHelper.Corruption("input is too short to be an SST file");
        ulong magic = Coding.DecodeFixed64(input[^8..]);
        if (magic == BlockBasedTableConstants.LegacyBlockBasedTableMagicNumber)
            ThrowHelper.NotSupported("Unsupported legacy magic number for block-based SST format. Load with RocksDB >= 4.6.0 and < 11.0.0 and run full compaction to upgrade.");
        if (magic != BlockBasedTableConstants.BlockBasedTableMagicNumber)
            ThrowHelper.Corruption("Bad table magic number: expected " + BlockBasedTableConstants.BlockBasedTableMagicNumber + ", found " + magic);

        int formatVersion = (int)Coding.DecodeFixed32(input[^12..]);
        if (formatVersion < BlockBasedTableConstants.MinSupportedFormatVersionForRead || formatVersion > BlockBasedTableConstants.LatestSupportedFormatVersion)
            ThrowHelper.Corruption("Corrupt or unsupported format_version " + (uint)formatVersion + " for magic " + magic);
        if (input.Length < EncodedLength) ThrowHelper.Corruption("Input is too short to be an SST file");
        int adjustment = input.Length - EncodedLength;
        input = input[adjustment..];
        ulong footerOffset = inputOffset + (ulong)adjustment;

        byte checksumByte = input[0];
        if (checksumByte > (byte)ChecksumType.Xxh3) ThrowHelper.Corruption("Corrupt or unsupported checksum type: " + checksumByte);
        var checksumType = (ChecksumType)checksumByte;

        if (formatVersion >= 6)
        {
            uint computed = 0;
            if (checksumType != ChecksumType.NoChecksum)
            {
                Span<byte> copy = stackalloc byte[EncodedLength];
                input.CopyTo(copy);
                Coding.EncodeFixed32(copy[5..], 0);
                computed = BlockChecksum.ComputeBuiltinChecksum(checksumType, copy);
            }
            ReadOnlySpan<byte> p2 = input[1..];
            if (!p2[..4].SequenceEqual(ExtendedMagic)) ThrowHelper.Corruption("Bad extended magic number: 0x" + Convert.ToHexString(p2[..4]));
            uint stored = Coding.DecodeFixed32(p2[4..]);
            uint baseContext = Coding.DecodeFixed32(p2[8..]);
            if (BlockChecksum.ChecksumModifierForContext(baseContext, 0) == 0) ThrowHelper.Corruption("Invalid base context checksum");
            computed += BlockChecksum.ChecksumModifierForContext(baseContext, footerOffset);
            if (computed != stored) ThrowHelper.Corruption("Footer at " + footerOffset + " checksum mismatch");
            uint metaindexSize = Coding.DecodeFixed32(p2[12..]);
            ulong features = Coding.DecodeFixed64(p2[32..]);
            if ((features & ~KnownFooterFeatures) != 0)
                ThrowHelper.NotSupported("File uses unsupported incompatible footer features: " + (features & ~KnownFooterFeatures));
            ulong metaindexEnd = footerOffset - BlockBasedTableConstants.BlockTrailerSize;
            return new Footer
            {
                TableMagicNumber = magic,
                FormatVersion = formatVersion,
                ChecksumType = checksumType,
                BaseContextChecksum = baseContext,
                IncompatibleFeatures = features,
                MetaindexHandle = new BlockHandle(metaindexEnd - metaindexSize, metaindexSize),
                IndexHandle = BlockHandle.Null,
            };
        }
        else
        {
            ReadOnlySpan<byte> p2 = input[1..];
            BlockHandle index = default;
            if (!BlockHandle.TryDecode(ref p2, out BlockHandle meta) || !BlockHandle.TryDecode(ref p2, out index))
                ThrowHelper.Corruption("bad block handle");
            return new Footer
            {
                TableMagicNumber = magic,
                FormatVersion = formatVersion,
                ChecksumType = checksumType,
                MetaindexHandle = meta,
                IndexHandle = index,
            };
        }
    }
}
