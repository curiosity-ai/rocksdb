using System.Globalization;
using RocksDb.Managed.Table;
using RocksDb.Managed.Table.Filters;
using CompressionType = RocksDb.Managed.Compression.CompressionType;

namespace RocksDb.Managed.Db;

/// <summary>
/// Where a new table file goes, as needed to pick its compression and filter implementation. Mirrors the
/// level-related fields of TableBuilderOptions (table/table_builder.h): <c>level_at_creation</c>,
/// <c>is_bottommost</c>, <c>reason == kFlush</c>, plus the dynamic base level used for
/// <c>compression_per_level</c>.
/// </summary>
/// <param name="Level">Output level (0 for flushes); -1 when unknown.</param>
/// <param name="BaseLevel">VersionStorageInfo::base_level() (first non-L0 level receiving data).</param>
/// <param name="IsFlush">The file is written by a memtable flush.</param>
/// <param name="IsBottommost">The output is the bottommost non-empty level (selects bottommost compression).</param>
public readonly record struct TableFileLevel(int Level, int BaseLevel, bool IsFlush, bool IsBottommost)
{
    /// <summary>A memtable flush (level 0, not bottommost) as built by FlushJob::WriteLevel0Table.</summary>
    public static TableFileLevel Flush => new(0, 1, IsFlush: true, IsBottommost: false);
}

/// <summary>
/// Resolves the public options model (<see cref="BlockBasedTableOptions"/>, <see cref="ColumnFamilyOptions"/>) into the
/// runtime table settings (<see cref="TableFormatOptions"/>, <see cref="TableBuilderOptions"/>,
/// <see cref="TableReaderOptions"/>). Ports BlockBasedTableFactory::InitializeOptions
/// (table/block_based/block_based_table_factory.cc), the built-in FilterPolicy factories of
/// RegisterBuiltinFilterPolicies (table/block_based/filter_policy.cc), GetCompressionType / GetCompressionOptions
/// (db/compaction/compaction_picker.cc) and GetCompressionFlush (db/db_impl/db_impl.cc).
/// Cold path (DB open / per output file): allocations are fine here.
/// </summary>
public static class TableFactory
{
    /// <summary>
    /// Maps block-based table options to the runtime table format, sanitizing like
    /// BlockBasedTableFactory::InitializeOptions and resolving the filter policy id.
    /// </summary>
    public static TableFormatOptions CreateFormatOptions(BlockBasedTableOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (options.BlockSize == 0 || options.BlockSize > int.MaxValue) ThrowHelper.InvalidArgument("block_size out of range: " + options.BlockSize);
        if (options.MetadataBlockSize > int.MaxValue) ThrowHelper.InvalidArgument("metadata_block_size out of range: " + options.MetadataBlockSize);
        if (options.FormatVersion > int.MaxValue) ThrowHelper.InvalidArgument("Unsupported BlockBasedTable format_version " + options.FormatVersion);

        int deviation = options.BlockSizeDeviation is < 0 or > 100 ? 0 : options.BlockSizeDeviation;
        int restart = Math.Max(1, options.BlockRestartInterval);
        int indexRestart = Math.Max(1, options.IndexBlockRestartInterval);
        // Currently kHashSearch is incompatible with index_block_restart_interval > 1.
        if (options.IndexType == IndexType.HashSearch) indexRestart = 1;

        return new TableFormatOptions
        {
            BlockSize = (int)options.BlockSize,
            BlockSizeDeviation = deviation,
            BlockRestartInterval = restart,
            IndexBlockRestartInterval = indexRestart,
            MetadataBlockSize = (int)options.MetadataBlockSize,
            FormatVersion = (int)options.FormatVersion,
            Checksum = options.Checksum,
            IndexType = options.IndexType,
            IndexShortening = options.IndexShortening,
            DataBlockIndexType = options.DataBlockIndexType,
            DataBlockHashTableUtilRatio = options.DataBlockHashTableUtilRatio,
            FilterPolicy = CreateFilterPolicy(options.FilterPolicy),
            WholeKeyFiltering = options.WholeKeyFiltering,
            UseDeltaEncoding = options.UseDeltaEncoding,
            OptimizeKeyCommonPrefix = options.OptimizeKeyCommonPrefix,
            EnableIndexCompression = options.EnableIndexCompression,
        };
    }

    /// <summary>
    /// Creates a built-in filter policy from its OPTIONS-file id. Accepted forms (FilterPolicy::CreateFromString):
    /// null / "" / "nullptr" (no filter); "bloomfilter:B", "bloomfilter:B:false", "bloomfilter:B:true" (the
    /// obsolete block-based flag now maps to the full filter); "ribbonfilter:B" (bloom_before_level 0) and
    /// "ribbonfilter:B:L"; the nick names "rocksdb.BloomFilter" / "rocksdb.RibbonFilter" may replace the class
    /// names. Unknown ids throw <see cref="StatusCode.InvalidArgument"/>.
    /// </summary>
    public static FilterPolicy? CreateFilterPolicy(string? id)
    {
        if (string.IsNullOrEmpty(id) || id == "nullptr") return null;
        string[] parts = id.Split(':');
        string name = parts[0];
        bool bloom = name is BloomFilterPolicy.ClassName or BloomFilterPolicy.NickName;
        bool ribbon = name is RibbonFilterPolicy.ClassName or RibbonFilterPolicy.NickName;
        if ((bloom || ribbon) && parts.Length is 2 or 3
            && double.TryParse(parts[1], NumberStyles.Float, CultureInfo.InvariantCulture, out double bitsPerKey))
        {
            if (bloom)
            {
                // ":false" and ":true" (formerly block-based) both build full filters (NewBloomFilterPolicy).
                if (parts.Length == 2 || parts[2] is "false" or "true") return new BloomFilterPolicy(bitsPerKey);
            }
            else
            {
                if (parts.Length == 2) return new RibbonFilterPolicy(bitsPerKey, 0); // NewRibbonFilterPolicy default
                if (int.TryParse(parts[2], NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out int bloomBeforeLevel))
                    return new RibbonFilterPolicy(bitsPerKey, bloomBeforeLevel);
            }
        }
        throw RocksDbException.InvalidArgument("Unsupported filter policy: " + id);
    }

    /// <summary>Resolves a built-in comparator by its persisted name; custom comparators must be supplied by the caller.</summary>
    public static Comparator ResolveComparator(string name)
    {
        ArgumentNullException.ThrowIfNull(name);
        if (Comparator.TryGetBuiltin(name, out Comparator? c)) return c;
        throw RocksDbException.InvalidArgument("Unknown comparator '" + name + "' (only built-in comparators can be resolved by name)");
    }

    /// <summary>Port of GetCompressionFlush (db/db_impl/db_impl.cc), leveled/FIFO styles.</summary>
    public static CompressionType GetCompressionFlush(ColumnFamilyOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        // Upstream also disables flush compression for universal compaction with compression_size_percent >= 0;
        // compaction_options_universal is not modeled yet (default compression_size_percent = -1 compresses).
        return options.CompressionPerLevel.Count == 0 ? options.Compression : options.CompressionPerLevel[0];
    }

    /// <summary>Port of GetCompressionType (db/compaction/compaction_picker.cc) for compaction outputs.</summary>
    public static CompressionType GetCompressionType(ColumnFamilyOptions options, int level, int baseLevel, bool isBottommost, bool enableCompression)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (!enableCompression) return CompressionType.None;
        // If bottommost_compression is set and we are compacting to the bottommost level then we should use it.
        if (options.BottommostCompression != CompressionType.DisableCompressionOption && isBottommost) return options.BottommostCompression;
        // If the user has specified a different compression level for each level, then pick the compression for that level.
        if (options.CompressionPerLevel.Count > 0)
        {
            int idx = level == 0 ? 0 : level - baseLevel + 1;
            int n = options.CompressionPerLevel.Count - 1;
            // level may be -1 (unknown) -> level 0's compression; beyond the end -> the last value.
            return options.CompressionPerLevel[Math.Max(0, Math.Min(idx, n))];
        }
        return options.Compression;
    }

    /// <summary>Port of GetCompressionOptions (db/compaction/compaction_picker.cc).</summary>
    public static CompressionOptions GetCompressionOptions(ColumnFamilyOptions options, bool isBottommost, bool enableCompression)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (!enableCompression) return options.CompressionOpts;
        return isBottommost && options.BottommostCompressionOpts.Enabled ? options.BottommostCompressionOpts : options.CompressionOpts;
    }

    /// <summary>
    /// Builds the per-file options for a table in the column family described by <paramref name="options"/>: user
    /// comparator, merge operator / prefix extractor names, compression type and options for <paramref name="where"/>,
    /// and the level context used by level-aware filter policies. File identity (column family id/name, file number,
    /// DB/session ids, times) is left at defaults for the caller to fill in with a <c>with</c> expression.
    /// </summary>
    public static TableBuilderOptions CreateBuilderOptions(ColumnFamilyOptions options, TableFileLevel where)
    {
        ArgumentNullException.ThrowIfNull(options);
        CompressionType compression;
        CompressionOptions compressionOptions;
        if (where.IsFlush)
        {
            // FlushJob: GetCompressionFlush + mutable_cf_options.compression_opts.
            compression = GetCompressionFlush(options);
            compressionOptions = options.CompressionOpts;
        }
        else
        {
            compression = GetCompressionType(options, where.Level, where.BaseLevel, where.IsBottommost, enableCompression: true);
            compressionOptions = GetCompressionOptions(options, where.IsBottommost, enableCompression: true);
        }
        if (compression == CompressionType.DisableCompressionOption) compression = CompressionType.None;
        return new TableBuilderOptions
        {
            Comparator = ResolveComparator(options.ComparatorName),
            CompressionType = compression,
            CompressionOptions = compressionOptions,
            MergeOperatorName = options.MergeOperatorName ?? "nullptr",
            PrefixExtractorName = options.PrefixExtractor ?? "nullptr",
            LevelAtCreation = where.Level,
            IsFlush = where.IsFlush,
        };
    }

    /// <summary>
    /// Builder options for an external SST file (port of SstFileWriter::Open, table/sst_file_writer.cc): bottommost
    /// compression if set (with bottommost_compression_opts when enabled), else the last compression_per_level entry,
    /// else compression; unknown level (-1), the "SST Writer" DB id and the external-file properties.
    /// </summary>
    public static TableBuilderOptions CreateExternalSstFileBuilderOptions(ColumnFamilyOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        CompressionType compression;
        CompressionOptions compressionOptions = options.CompressionOpts;
        if (options.BottommostCompression != CompressionType.DisableCompressionOption)
        {
            compression = options.BottommostCompression;
            if (options.BottommostCompressionOpts.Enabled) compressionOptions = options.BottommostCompressionOpts;
        }
        else if (options.CompressionPerLevel.Count > 0)
        {
            // Use the compression of the last level if we have per level compression.
            compression = options.CompressionPerLevel[^1];
        }
        else
        {
            compression = options.Compression;
        }
        if (compression == CompressionType.DisableCompressionOption) compression = CompressionType.None;
        return new TableBuilderOptions
        {
            Comparator = ResolveComparator(options.ComparatorName),
            CompressionType = compression,
            CompressionOptions = compressionOptions,
            MergeOperatorName = options.MergeOperatorName ?? "nullptr",
            PrefixExtractorName = options.PrefixExtractor ?? "nullptr",
            LevelAtCreation = -1,
            IsFlush = false,
            DbId = "SST Writer",
            ExternalSstFile = true,
        };
    }

    /// <summary>Reader options for tables of the column family (comparator and the configured filter policy).</summary>
    public static TableReaderOptions CreateReaderOptions(ColumnFamilyOptions options, TableFormatOptions format)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(format);
        return new TableReaderOptions
        {
            Comparator = ResolveComparator(options.ComparatorName),
            FilterPolicy = format.FilterPolicy,
            VerifyChecksums = format.VerifyChecksums,
        };
    }
}
