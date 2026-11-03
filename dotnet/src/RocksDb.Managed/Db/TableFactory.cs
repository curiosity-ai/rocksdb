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
            // An unresolved OPTIONS-file id fails here like FilterPolicy::CreateFromString (or means "no filter" for
            // the bare class names written by the C API).
            FilterPolicy = options.FilterPolicy ?? CreateFilterPolicy(options.UnresolvedFilterPolicyId),
            WholeKeyFiltering = options.WholeKeyFiltering,
            UseDeltaEncoding = options.UseDeltaEncoding,
            OptimizeKeyCommonPrefix = options.OptimizeKeyCommonPrefix,
            EnableIndexCompression = options.EnableIndexCompression,
            UserDefinedIndexFactory = options.UserDefinedIndexFactory,
            IndexMode = options.IndexMode,
            // We do not support partitioned filters without partitioning indexes (InitializeOptions).
            PartitionFilters = options.PartitionFilters && options.IndexType == IndexType.TwoLevelIndexSearch,
            DecouplePartitionedFilters = options.DecouplePartitionedFilters,
            OptimizeFiltersForMemory = options.OptimizeFiltersForMemory,
            SeparateKeyValueInDataBlock = options.SeparateKeyValueInDataBlock,
            UniformCvThreshold = options.UniformCvThreshold,
            BlockAlign = options.BlockAlign,
            SuperBlockAlignmentSize = options.SuperBlockAlignmentSize > long.MaxValue ? long.MaxValue : (long)options.SuperBlockAlignmentSize,
            SuperBlockAlignmentSpaceOverheadRatio = options.SuperBlockAlignmentSpaceOverheadRatio > long.MaxValue
                ? long.MaxValue : (long)options.SuperBlockAlignmentSpaceOverheadRatio,
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
        if (id.Length >= 2 && id[0] == '{' && id[^1] == '}') return CreateFilterPolicyFromStruct(id);
        string[] parts = id.Split(':');
        string name = parts[0];
        bool bloom = name is BloomFilterPolicy.ClassName or BloomFilterPolicy.NickName;
        bool ribbon = name is RibbonFilterPolicy.ClassName or RibbonFilterPolicy.NickName;
        // A bare class name without bits-per-key is what native RocksDB writes to OPTIONS files for filter policies
        // created through the C API (rocksdb_filterpolicy_create_*), whose wrapper's id is just Name(). Upstream's
        // registry doesn't match it (the ":bits" part is mandatory, filter_policy.cc FilterPatternEntryWithBits) and
        // LoadLatestOptions' default ignore_unsupported_options turns that NotSupported into "no filter policy".
        if ((bloom || ribbon) && parts.Length == 1) return null;
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

    /// <summary>
    /// The struct form native RocksDB writes to OPTIONS files for a configured filter policy, e.g.
    /// <c>{id=ribbonfilter:10:0;bloom_before_level=0;}</c> (Customizable serialization: the id plus the object's
    /// options). Port of the GetOptionsMap + ConfigureNewObject steps of FilterPolicy::CreateFromString
    /// (table/block_based/filter_policy.cc); <c>bloom_before_level</c> is the only option of the built-in policies.
    /// </summary>
    private static FilterPolicy? CreateFilterPolicyFromStruct(string value)
    {
        string? id = null;
        int? bloomBeforeLevel = null;
        foreach (string part in value[1..^1].Split(';', StringSplitOptions.RemoveEmptyEntries))
        {
            int eq = part.IndexOf('=');
            string key = eq < 0 ? part : part[..eq], v = eq < 0 ? "" : part[(eq + 1)..];
            if (key == "id") id = v;
            else if (key == "bloom_before_level" && int.TryParse(v, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out int l)) bloomBeforeLevel = l;
            else throw RocksDbException.InvalidArgument("Unsupported filter policy option '" + part + "' in " + value);
        }
        if (string.IsNullOrEmpty(id) || id[0] == '{') throw RocksDbException.InvalidArgument("Unsupported filter policy: " + value);
        FilterPolicy? policy = CreateFilterPolicy(id);
        if (bloomBeforeLevel is int level && policy is RibbonFilterPolicy ribbon) ribbon.BloomBeforeLevel = level;
        return policy;
    }

    /// <summary>Resolves a built-in comparator by its persisted name; custom comparators must be supplied by the caller.</summary>
    public static Comparator ResolveComparator(string name)
    {
        ArgumentNullException.ThrowIfNull(name);
        if (Comparator.TryGetBuiltin(name, out Comparator? c)) return c;
        throw RocksDbException.InvalidArgument("Unknown comparator '" + name + "' (only built-in comparators can be resolved by name)");
    }

    /// <summary>
    /// The user comparator of a column family: <see cref="ColumnFamilyOptions.Comparator"/>; InvalidArgument when the
    /// options only name a comparator (parsed from an OPTIONS file) that is not built in.
    /// </summary>
    public static Comparator ResolveComparator(ColumnFamilyOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        return options.Comparator ?? ResolveComparator(options.ComparatorName);
    }

    /// <summary>Port of GetCompressionFlush (db/db_impl/db_impl.cc), leveled/FIFO styles.</summary>
    public static CompressionType GetCompressionFlush(ColumnFamilyOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        // Port of GetCompressionFlush (db/db_impl/db_impl.cc): compressing memtable flushes might not help unless the
        // sequential load optimization is used for leveled compaction (universal with compression_size_percent >= 0
        // compresses only the oldest data).
        if (options.CompactionStyle == CompactionStyle.Universal && options.CompactionOptionsUniversal.CompressionSizePercent >= 0)
        {
            return CompressionType.None;
        }
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
            Comparator = ResolveComparator(options),
            CompressionType = compression,
            CompressionOptions = compressionOptions,
            MergeOperatorName = options.MergeOperatorName ?? "nullptr",
            PrefixExtractor = options.PrefixExtractor,
            LevelAtCreation = where.Level,
            IsFlush = where.IsFlush,
            PersistUserDefinedTimestamps = options.PersistUserDefinedTimestamps,
            TablePropertiesCollectorFactories = options.TablePropertiesCollectorFactories.ToArray(),
            NumLevels = options.NumLevels,
            TableFactory = WriteFactory(options),
            PlainTableOptions = options.PlainTableOptions,
            CuckooTableOptions = options.CuckooTableOptions,
            BloomLocality = options.BloomLocality,
        };
    }

    /// <summary>The format new files of the column family are written in (AdaptiveTableFactory::NewTableBuilder: its write factory).</summary>
    public static TableFactoryType WriteFactory(ColumnFamilyOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        return options.TableFactory == TableFactoryType.AdaptiveTable ? options.AdaptiveTableWriteFactory : options.TableFactory;
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
            Comparator = ResolveComparator(options),
            CompressionType = compression,
            CompressionOptions = compressionOptions,
            MergeOperatorName = options.MergeOperatorName ?? "nullptr",
            PrefixExtractor = options.PrefixExtractor,
            LevelAtCreation = -1,
            IsFlush = false,
            DbId = "SST Writer",
            ExternalSstFile = true,
            PersistUserDefinedTimestamps = options.PersistUserDefinedTimestamps,
            TablePropertiesCollectorFactories = options.TablePropertiesCollectorFactories.ToArray(),
            NumLevels = options.NumLevels,
            TableFactory = WriteFactory(options),
            PlainTableOptions = options.PlainTableOptions,
            CuckooTableOptions = options.CuckooTableOptions,
            BloomLocality = options.BloomLocality,
        };
    }

    /// <summary>Reader options for tables of the column family (comparator and the configured filter policy).</summary>
    public static TableReaderOptions CreateReaderOptions(ColumnFamilyOptions options, TableFormatOptions format)
        => CreateReaderOptions(options, format, defaultBlockCache: null, dbSessionId: null);

    /// <summary>
    /// Reader options for a column family. The block cache is <c>TableOptions.BlockCache</c> if set, else
    /// <paramref name="defaultBlockCache"/> (unless <c>no_block_cache</c>). <paramref name="dbSessionId"/> seeds cache keys
    /// for files whose properties lack a session id (BlockBasedTable::SetupBaseCacheKey).
    /// </summary>
    public static TableReaderOptions CreateReaderOptions(ColumnFamilyOptions options, TableFormatOptions format,
        Cache.BlockCache? defaultBlockCache, string? dbSessionId)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(format);
        BlockBasedTableOptions t = options.TableOptions;
        Cache.BlockCache? cache = t.NoBlockCache ? null : (t.BlockCache ?? defaultBlockCache);
        TableReaderOptions r = new()
        {
            Comparator = ResolveComparator(options),
            FilterPolicy = format.FilterPolicy,
            VerifyChecksums = format.VerifyChecksums,
            CacheIndexAndFilterBlocks = cache is not null && t.CacheIndexAndFilterBlocks,
            CacheIndexAndFilterBlocksWithHighPriority = t.CacheIndexAndFilterBlocksWithHighPriority,
            PinTopLevelIndexAndFilter = t.PinTopLevelIndexAndFilter,
            PrefixExtractor = options.PrefixExtractor,
            DbSessionId = dbSessionId,
            BlockProtectionBytesPerKey = (int)options.BlockProtectionBytesPerKey,
            IndexBlockSearchType = t.IndexBlockSearchType,
            MaxAutoReadaheadSize = (long)Math.Min(t.MaxAutoReadaheadSize, int.MaxValue),
            InitialAutoReadaheadSize = (long)Math.Min(t.InitialAutoReadaheadSize, int.MaxValue),
            NumFileReadsForAutoReadahead = t.NumFileReadsForAutoReadahead,
            TableFactory = options.TableFactory,
            PlainTableOptions = options.PlainTableOptions,
            BloomLocality = options.BloomLocality,
            UserDefinedIndexFactory = t.UserDefinedIndexFactory,
            IndexMode = t.IndexMode,
        };
        if (cache is not null) r = r with { BlockCache = cache };
        return r;
    }
}
