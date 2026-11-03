using RocksDb.Managed.Compression;
using CompressionOptions = RocksDb.Managed.CompressionOptions;
using RocksDb.Managed.Table.Filters;

namespace RocksDb.Managed.Table;

// IndexType / IndexShorteningMode / DataBlockIndexType are defined once in Options/OptionEnums.cs.

/// <summary>
/// Block-based table configuration (the subset of rocksdb::TableFormatOptions, include/rocksdb/table.h, that
/// affects the table builder and reader in this port). Defaults match upstream.
/// </summary>
public sealed record TableFormatOptions
{
    /// <summary>Approximate size of user data packed per block (uncompressed). Default 4 KiB.</summary>
    public int BlockSize { get; init; } = 4 * 1024;
    /// <summary>Percentage: close a block early if the next entry would overflow it and it is at least (100 - x)% full. Default 10.</summary>
    public int BlockSizeDeviation { get; init; } = 10;
    public int BlockRestartInterval { get; init; } = 16;
    public int IndexBlockRestartInterval { get; init; } = 1;
    /// <summary>Target size of index partitions with <see cref="IndexType.TwoLevelIndexSearch"/>.</summary>
    public int MetadataBlockSize { get; init; } = 4096;
    /// <summary>Supported for writing: 2..8. Default 7 (upstream default).</summary>
    public int FormatVersion { get; init; } = 7;
    public ChecksumType Checksum { get; init; } = ChecksumType.Xxh3;
    public IndexType IndexType { get; init; } = IndexType.BinarySearch;
    public IndexShorteningMode IndexShortening { get; init; } = IndexShorteningMode.ShortenSeparators;
    public DataBlockIndexType DataBlockIndexType { get; init; } = DataBlockIndexType.BinarySearch;
    public double DataBlockHashTableUtilRatio { get; init; } = 0.75;
    /// <summary>Filter policy (null = no filter). The full filter is keyed "fullfilter.&lt;CompatibilityName&gt;".</summary>
    public FilterPolicy? FilterPolicy { get; init; }
    public bool WholeKeyFiltering { get; init; } = true;
    public bool UseDeltaEncoding { get; init; } = true;
    /// <summary>format_version &gt;= 8 common user-key prefix optimization for data and index blocks.</summary>
    public OptimizeKeyCommonPrefix OptimizeKeyCommonPrefix { get; init; } = OptimizeKeyCommonPrefix.IfFastSeek;
    public bool EnableIndexCompression { get; init; } = true;
    /// <summary>user_defined_index_factory: a custom index built with (or instead of) the standard index; null: none.</summary>
    public UserDefinedIndexFactory? UserDefinedIndexFactory { get; init; }
    /// <summary>index_mode (effective): how the custom index relates to the standard index.</summary>
    public BlockBasedIndexMode IndexMode { get; init; } = BlockBasedIndexMode.StandardDefault;
    /// <summary>
    /// partition_filters: write the filter as partitions with a top-level index ("partitionedfilter.&lt;name&gt;").
    /// Only effective with <see cref="IndexType.TwoLevelIndexSearch"/> (sanitized away otherwise, like
    /// BlockBasedTableFactory::InitializeOptions).
    /// </summary>
    public bool PartitionFilters { get; init; }
    /// <summary>decouple_partitioned_filters: cut filter partitions independently of index partitions (default true).</summary>
    public bool DecouplePartitionedFilters { get; init; } = true;
    /// <summary>optimize_filters_for_memory: round filter sizes to the allocator's usable size (default true, see <see cref="FilterAllocationModel"/>).</summary>
    public bool OptimizeFiltersForMemory { get; init; } = true;
    /// <summary>Verify block checksums when reading (ReadOptions::verify_checksums default true).</summary>
    public bool VerifyChecksums { get; init; } = true;
    /// <summary>
    /// separate_key_value_in_data_block: data blocks store all keys first and the values after them (a value-offset varint
    /// per restart interval). Readable by RocksDB 11.x with the feature (recorded in "rocksdb.separate.key.value.in.data.block").
    /// </summary>
    public bool SeparateKeyValueInDataBlock { get; init; }
    /// <summary>
    /// uniform_cv_threshold: index blocks whose restart-key gaps have a coefficient of variation below this get the
    /// "is_uniform" footer hint (counted in "rocksdb.num.uniform.blocks"). Negative (default -1) disables it.
    /// </summary>
    public double UniformCvThreshold { get; init; } = -1;
    /// <summary>block_align: pad every data block (+ trailer) to min(block_size, 4 KiB); block_size must be a power of 2 and compression disabled.</summary>
    public bool BlockAlign { get; init; }
    /// <summary>
    /// super_block_alignment_size: pad before a data block that would straddle a boundary of this power-of-two size, when the
    /// padding is below size / <see cref="SuperBlockAlignmentSpaceOverheadRatio"/>. 0 (default) disables it.
    /// </summary>
    public long SuperBlockAlignmentSize { get; init; }
    /// <summary>super_block_alignment_space_overhead_ratio (default 128; 0 disables super block alignment; else &gt;= 4).</summary>
    public long SuperBlockAlignmentSpaceOverheadRatio { get; init; } = 128;

    public static TableFormatOptions Default { get; } = new();

    /// <summary>kDefaultPageSize (table/block_based/block_based_table_builder.cc): the block_align upper bound.</summary>
    internal const int DefaultPageSize = 4 * 1024;

    internal void Validate()
    {
        if (FormatVersion < BlockBasedTableConstants.MinSupportedFormatVersionForWrite || FormatVersion > BlockBasedTableConstants.LatestSupportedFormatVersion)
            ThrowHelper.InvalidArgument("Unsupported BlockBasedTable format_version " + FormatVersion);
        if (BlockRestartInterval < 1 || IndexBlockRestartInterval < 1) ThrowHelper.InvalidArgument("restart interval must be >= 1");
        if (BlockSize <= 0) ThrowHelper.InvalidArgument("block_size must be > 0");
        if (MetadataBlockSize <= 0) ThrowHelper.InvalidArgument("metadata_block_size must be > 0");
        // BlockBasedTableFactory::ValidateOptions.
        if (BlockAlign && (BlockSize & (BlockSize - 1)) != 0) ThrowHelper.InvalidArgument("Block alignment requested but block size is not a power of 2");
        if (SuperBlockAlignmentSize < 0 || (SuperBlockAlignmentSize & (SuperBlockAlignmentSize - 1)) != 0)
            ThrowHelper.InvalidArgument("Super Block alignment requested but super block alignment size is not a power of 2");
        if (SuperBlockAlignmentSize > uint.MaxValue) ThrowHelper.InvalidArgument("Super block alignment size exceeds maximum number (4GiB) allowed");
        if (SuperBlockAlignmentSpaceOverheadRatio < 0 || (SuperBlockAlignmentSpaceOverheadRatio > 0 && SuperBlockAlignmentSpaceOverheadRatio < 4))
            ThrowHelper.InvalidArgument("Super block alignment space overhead is too high");
    }
}

/// <summary>
/// Per-file context for building one table. Port of the relevant fields of TableBuilderOptions
/// (table/table_builder.h) — column family, compression, identity and time properties.
/// </summary>
public sealed record TableBuilderOptions
{
    /// <summary>User key comparator. Persisted as "rocksdb.comparator".</summary>
    public Comparator Comparator { get; init; } = Comparator.Bytewise;
    public CompressionType CompressionType { get; init; } = CompressionType.None;
    /// <summary>
    /// TableBuilderOptions::compression_opts: level (kDefaultCompressionLevel = 32767 means library default) and
    /// max_compressed_bytes_per_kb are used for compression; all fields are persisted in "rocksdb.compression_options".
    /// max_dict_bytes / zstd_max_train_bytes / max_dict_buffer_bytes / use_zstd_dict_trainer enable dictionary compression.
    /// </summary>
    public CompressionOptions CompressionOptions { get; init; } = new();
    public ulong ColumnFamilyId { get; init; } = TableProperties.UnknownColumnFamily;
    public string ColumnFamilyName { get; init; } = "";
    /// <summary>Persisted as "rocksdb.merge.operator" ("nullptr" when none).</summary>
    public string MergeOperatorName { get; init; } = "nullptr";
    /// <summary>
    /// MutableCFOptions::prefix_extractor: prefixes of keys in its domain are added to the filter (in addition to whole
    /// keys with whole_key_filtering) and its id is persisted as "rocksdb.prefix.extractor.name" ("nullptr" when none).
    /// </summary>
    public SliceTransform? PrefixExtractor { get; init; }
    /// <summary>The "rocksdb.prefix.extractor.name" property value: <see cref="PrefixExtractor"/>'s id or "nullptr".</summary>
    public string PrefixExtractorName => PrefixExtractor?.AsString() ?? "nullptr";
    /// <summary>
    /// Level the file is created at (FilterBuildingContext::level_at_creation); -1 when unknown (treated as bottommost
    /// by level-aware filter policies such as Ribbon's bloom_before_level).
    /// </summary>
    public int LevelAtCreation { get; init; } = -1;
    /// <summary>
    /// TableBuilderOptions::target_file_size: with dictionary compression, data blocks are buffered (for sampling) up to
    /// min(target_file_size, max_dict_buffer_bytes), either being 0 meaning "no limit" from that source. 0 for flushes
    /// and external files.
    /// </summary>
    public ulong TargetFileSize { get; init; }
    /// <summary>True when the file is written by a memtable flush (TableFileCreationReason::kFlush; Ribbon treats it as level -1).</summary>
    public bool IsFlush { get; init; }
    public ulong OldestKeyTime { get; init; }
    public ulong NewestKeyTime { get; init; }
    public ulong FileCreationTime { get; init; }
    /// <summary>"rocksdb.creation.time" (oldest ancestor time).</summary>
    public ulong CreationTime { get; init; }
    public ulong FileNumber { get; init; }
    public string DbId { get; init; } = "";
    public string DbSessionId { get; init; } = "";
    public string DbHostId { get; init; } = "";
    /// <summary>
    /// Write the properties SstFileWriter adds for external files ("rocksdb.external_sst_file.version" = 2 and
    /// "rocksdb.external_sst_file.global_seqno" = 0), required by IngestExternalFile.
    /// </summary>
    public bool ExternalSstFile { get; init; }
    /// <summary>
    /// persist_user_defined_timestamps: false strips the user-defined timestamp from every persisted key (the table is
    /// read back with the minimum timestamp padded in; "rocksdb.user.defined.timestamps.persisted" = 0). Ignored by
    /// comparators without timestamps.
    /// </summary>
    public bool PersistUserDefinedTimestamps { get; init; } = true;
    /// <summary>
    /// ImmutableOptions::table_properties_collector_factories: user collectors run on this table; their names are
    /// recorded in "rocksdb.property.collectors" (written as "[]" when empty).
    /// </summary>
    public IReadOnlyList<TablePropertiesCollectorFactory> TablePropertiesCollectorFactories { get; init; } = [];
    /// <summary>ImmutableOptions::num_levels for TablePropertiesCollectorFactory::Context (-1 unknown).</summary>
    public int NumLevels { get; init; } = -1;
    /// <summary>TableBuilderOptions::last_level_inclusive_max_seqno_threshold (kMaxSequenceNumber: tiering disabled).</summary>
    public ulong LastLevelInclusiveMaxSeqnoThreshold { get; init; } = Db.DbFormat.MaxSequenceNumber;

    /// <summary>
    /// The SST format to write (the column family's table factory; for the adaptive factory its write factory). Read by
    /// <see cref="TableBuilder.Create"/>.
    /// </summary>
    public TableFactoryType TableFactory { get; init; } = TableFactoryType.BlockBasedTable;
    /// <summary>Plain table settings when <see cref="TableFactory"/> is <see cref="TableFactoryType.PlainTable"/> (null: defaults).</summary>
    public PlainTableOptions? PlainTableOptions { get; init; }
    /// <summary>Cuckoo table settings when <see cref="TableFactory"/> is <see cref="TableFactoryType.CuckooTable"/> (null: defaults).</summary>
    public CuckooTableOptions? CuckooTableOptions { get; init; }
    /// <summary>ImmutableOptions::bloom_locality (plain table bloom filters: 0 = no locality).</summary>
    public uint BloomLocality { get; init; }

    /// <summary>
    /// Test hook: a fixed non-zero base context checksum (format_version &gt;= 6) instead of a random one, so two builds of
    /// the same input produce identical bytes. 0 (default): random, like upstream.
    /// </summary>
    internal uint TestBaseContextChecksum { get; init; }
}
