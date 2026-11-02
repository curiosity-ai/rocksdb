using RocksDb.Managed.Compression;
using RocksDb.Managed.Options;

namespace RocksDb.Managed;

/// <summary>
/// Per column family options. Port of rocksdb::ColumnFamilyOptions + AdvancedColumnFamilyOptions
/// (include/rocksdb/options.h, advanced_options.h) with upstream defaults; OPTIONS-file names and string forms follow
/// options/cf_options.cc. Pluggable components (merge operator, compaction filter, prefix extractor, ...) are typed
/// objects; OPTIONS files reference them by name.
/// </summary>
public sealed class ColumnFamilyOptions : PersistedOptions<ColumnFamilyOptions>
{
    // ---- pluggable components ----------------------------------------------------------------
    // The objects are the API; their names are what OPTIONS files and table properties record. Built-in objects
    // named in a parsed OPTIONS file are recreated (like upstream's ObjectRegistry); other ids are preserved verbatim
    // and exposed as the *Name properties (see CustomizableOption).
    private CustomizableOption<MergeOperator> _mergeOperator;
    private CustomizableOption<CompactionFilter> _compactionFilter;
    private CustomizableOption<CompactionFilterFactory> _compactionFilterFactory;
    private CustomizableOption<SliceTransform> _prefixExtractor;
    private CustomizableOption<SliceTransform> _memtableInsertWithHintPrefixExtractor;

    /// <summary>Persisted name of the comparator (comparator option / MANIFEST).</summary>
    public string ComparatorName { get; set; } = "leveldb.BytewiseComparator";

    /// <summary>
    /// The merge operator (thread-safe; used by reads, flush and compaction). Built-ins: <see cref="UInt64AddOperator"/>,
    /// <see cref="StringAppendOperator"/> (also see <see cref="Managed.MergeOperator.CreateBuiltin"/>). Persisted as its
    /// <see cref="Managed.MergeOperator.Name"/>; when an OPTIONS file names a built-in operator it is recreated here.
    /// </summary>
    public MergeOperator? MergeOperator
    {
        get => _mergeOperator.Value;
        set => _mergeOperator.Set(value);
    }

    /// <summary>
    /// Name recorded for the merge operator: <see cref="MergeOperator"/>'s name, or the id of a merge operator named in
    /// a parsed OPTIONS file that has no managed implementation (such a column family cannot merge). Null when none.
    /// </summary>
    public string? MergeOperatorName => _mergeOperator.Name(MergeOperatorId);

    /// <summary>
    /// The compaction filter used by the DB (shared by all compactions, so it must be thread-safe). Persisted by
    /// <see cref="Managed.CompactionFilter.Name"/> (compaction_filter).
    /// </summary>
    public CompactionFilter? CompactionFilter
    {
        get => _compactionFilter.Value;
        set => _compactionFilter.Set(value);
    }

    /// <summary>Name recorded for the compaction filter (the object's, or an unresolved id from an OPTIONS file).</summary>
    public string? CompactionFilterName => _compactionFilter.Name(CompactionFilterId);

    /// <summary>
    /// Creates a compaction filter per compaction. Persisted by name (compaction_filter_factory). Ignored when
    /// <see cref="CompactionFilter"/> is set.
    /// </summary>
    public CompactionFilterFactory? CompactionFilterFactory
    {
        get => _compactionFilterFactory.Value;
        set => _compactionFilterFactory.Set(value);
    }

    /// <summary>Name recorded for the compaction filter factory (the object's, or an unresolved id from an OPTIONS file).</summary>
    public string? CompactionFilterFactoryName => _compactionFilterFactory.Name(CompactionFilterFactoryId);

    /// <summary>
    /// prefix_extractor: enables prefix Bloom filters in tables and prefix seek (see <see cref="SliceTransform"/>);
    /// null = none. Persisted by id (e.g. <c>rocksdb.FixedPrefix.4</c>); built-in transforms named in an OPTIONS file
    /// are recreated, other ids are preserved verbatim but leave this null.
    /// </summary>
    public SliceTransform? PrefixExtractor
    {
        get => _prefixExtractor.Value;
        set => _prefixExtractor.Set(value);
    }

    /// <summary>memtable_insert_with_hint_prefix_extractor (persisted only; the managed memtable takes no insert hints).</summary>
    public SliceTransform? MemtableInsertWithHintPrefixExtractor
    {
        get => _memtableInsertWithHintPrefixExtractor.Value;
        set => _memtableInsertWithHintPrefixExtractor.Set(value);
    }

    /// <summary>Persisted sst_partitioner_factory id (no managed SST partitioners exist; kept for round trips).</summary>
    public string? SstPartitionerFactoryName { get; set; }

    /// <summary>memtable_factory (the managed port implements the skip list only).</summary>
    public MemTableFactoryType MemtableFactory { get; set; } = MemTableFactoryType.SkipList;

    /// <summary>table_factory; the managed port reads and writes <see cref="TableFactoryType.BlockBasedTable"/> only.</summary>
    public TableFactoryType TableFactory { get; set; } = TableFactoryType.BlockBasedTable;

    /// <summary>Options of the block-based table factory (the [TableOptions/BlockBasedTable "cf"] section).</summary>
    public BlockBasedTableOptions TableOptions { get; set; } = new();
    /// <summary>Raw options of a non block-based table factory section, preserved for round trips.</summary>
    public Dictionary<string, string>? OtherTableFactoryOptions { get; set; }

    // ---- memtable ----------------------------------------------------------------------------
    public ulong WriteBufferSize { get; set; } = 64UL << 20;
    public int MaxWriteBufferNumber { get; set; } = 2;
    public int MinWriteBufferNumberToMerge { get; set; } = 1;
    public long MaxWriteBufferSizeToMaintain { get; set; }
    /// <summary>
    /// arena_block_size. 0 (the default, as in include/rocksdb/advanced_options.h) derives it from
    /// <see cref="WriteBufferSize"/> like SanitizeCfOptions: min(1 MiB, write_buffer_size / 8), 4 KiB aligned.
    /// </summary>
    public ulong ArenaBlockSize { get; set; }
    public bool InplaceUpdateSupport { get; set; }
    public ulong InplaceUpdateNumLocks { get; set; } = 10000;
    public double MemtablePrefixBloomSizeRatio { get; set; }
    public bool MemtableWholeKeyFiltering { get; set; }
    public ulong MemtableHugePageSize { get; set; }
    public uint BloomLocality { get; set; }
    public ulong MaxSuccessiveMerges { get; set; }
    public uint MemtableProtectionBytesPerKey { get; set; }
    public double ExperimentalMempurgeThreshold { get; set; }

    // ---- compression -------------------------------------------------------------------------
    /// <summary>Default kLZ4Compression (GetDefaultCompressionType: LZ4 when supported).</summary>
    public CompressionType Compression { get; set; } = CompressionType.LZ4;
    public CompressionType BottommostCompression { get; set; } = CompressionType.DisableCompressionOption;
    public CompressionOptions CompressionOpts { get; set; } = new();
    public CompressionOptions BottommostCompressionOpts { get; set; } = new();
    /// <summary>Per-level compression; empty means use <see cref="Compression"/> everywhere.</summary>
    public List<CompressionType> CompressionPerLevel { get; set; } = new();
    public ulong SampleForCompression { get; set; }

    // ---- LSM shape / compaction --------------------------------------------------------------
    public int NumLevels { get; set; } = 7;
    public int Level0FileNumCompactionTrigger { get; set; } = 4;
    public int Level0SlowdownWritesTrigger { get; set; } = 20;
    public int Level0StopWritesTrigger { get; set; } = 36;
    public ulong TargetFileSizeBase { get; set; } = 64UL * 1048576;
    public int TargetFileSizeMultiplier { get; set; } = 1;
    public ulong MaxBytesForLevelBase { get; set; } = 256UL * 1048576;
    public double MaxBytesForLevelMultiplier { get; set; } = 10;
    public List<int> MaxBytesForLevelMultiplierAdditional { get; set; } = new() { 1, 1, 1, 1, 1, 1, 1 };
    public bool LevelCompactionDynamicLevelBytes { get; set; } = true;
    public ulong MaxCompactionBytes { get; set; } = 64UL * 1048576 * 25;
    public ulong SoftPendingCompactionBytesLimit { get; set; } = 64UL * 1073741824;
    public ulong HardPendingCompactionBytesLimit { get; set; } = 256UL * 1073741824;
    public bool DisableAutoCompactions { get; set; }
    public CompactionStyle CompactionStyle { get; set; } = CompactionStyle.Level;
    public CompactionPri CompactionPri { get; set; } = CompactionPri.MinOverlappingRatio;
    /// <summary>FIFO compaction options (not persisted by the managed port yet).</summary>
    public CompactionOptionsFifo CompactionOptionsFifo { get; set; } = new();
    /// <summary>Universal compaction options (not persisted by the managed port yet).</summary>
    public CompactionOptionsUniversal CompactionOptionsUniversal { get; set; } = new();
    public ulong MaxSequentialSkipInIterations { get; set; } = 8;
    public bool OptimizeFiltersForHits { get; set; }
    public bool ParanoidFileChecks { get; set; }
    public bool ForceConsistencyChecks { get; set; } = true;
    public bool ReportBgIoStats { get; set; }
    /// <summary>Seconds; default 30 days (0xfffffffffffffffe "auto" resolves to this for leveled compaction).</summary>
    public ulong Ttl { get; set; } = 30UL * 24 * 60 * 60;
    public ulong PeriodicCompactionSeconds { get; set; }
    public ulong PrecludeLastLevelDataSeconds { get; set; }
    public ulong PreserveInternalTimeSeconds { get; set; }
    public Temperature LastLevelTemperature { get; set; } = Temperature.Unknown;
    public Temperature DefaultTemperature { get; set; } = Temperature.Unknown;
    public Temperature DefaultWriteTemperature { get; set; } = Temperature.Unknown;
    public uint BlockProtectionBytesPerKey { get; set; }
    public bool PersistUserDefinedTimestamps { get; set; } = true;
    public bool CfAllowIngestBehind { get; set; }

    // ---- blob files --------------------------------------------------------------------------
    public bool EnableBlobFiles { get; set; }
    public ulong MinBlobSize { get; set; }
    public ulong BlobFileSize { get; set; } = 1UL << 28;
    public CompressionType BlobCompressionType { get; set; } = CompressionType.None;
    public bool EnableBlobGarbageCollection { get; set; }
    public double BlobGarbageCollectionAgeCutoff { get; set; } = 0.25;
    public double BlobGarbageCollectionForceThreshold { get; set; } = 1.0;
    public ulong BlobCompactionReadaheadSize { get; set; }
    public int BlobFileStartingLevel { get; set; }
    public PrepopulateBlobCache PrepopulateBlobCache { get; set; } = PrepopulateBlobCache.Disable;

    private static readonly OptionTable<ColumnFamilyOptions> s_table = new OptionTable<ColumnFamilyOptions>()
        .Name("comparator", static o => o.ComparatorName, static (o, v) => o.ComparatorName = v ?? "leveldb.BytewiseComparator")
        .Add("merge_operator", static o => o._mergeOperator.Serialize(MergeOperatorId) ?? "nullptr",
            static (o, v) => o._mergeOperator.Parse(v, Managed.MergeOperator.CreateBuiltin, MergeOperatorId))
        .Add("compaction_filter", static o => o._compactionFilter.Serialize(CompactionFilterId) ?? "nullptr",
            static (o, v) => o._compactionFilter.Parse(v, static _ => null, CompactionFilterId))
        .Add("compaction_filter_factory", static o => o._compactionFilterFactory.Serialize(CompactionFilterFactoryId) ?? "nullptr",
            static (o, v) => o._compactionFilterFactory.Parse(v, static _ => null, CompactionFilterFactoryId))
        .Add("prefix_extractor", static o => o._prefixExtractor.Serialize(SliceTransformId) ?? "nullptr",
            static (o, v) => o._prefixExtractor.Parse(v, SliceTransform.CreateFromString, SliceTransformId))
        .Add("memtable_insert_with_hint_prefix_extractor", static o => o._memtableInsertWithHintPrefixExtractor.Serialize(SliceTransformId) ?? "nullptr",
            static (o, v) => o._memtableInsertWithHintPrefixExtractor.Parse(v, SliceTransform.CreateFromString, SliceTransformId))
        .Name("sst_partitioner_factory", static o => o.SstPartitionerFactoryName, static (o, v) => o.SstPartitionerFactoryName = v)
        .EnumId("memtable_factory", OptionEnumMaps.MemTableFactory, static o => o.MemtableFactory, static (o, v) => o.MemtableFactory = v)
        .EnumId("table_factory", OptionEnumMaps.TableFactory, static o => o.TableFactory, static (o, v) => o.TableFactory = v)
        .UInt64("write_buffer_size", static o => o.WriteBufferSize, static (o, v) => o.WriteBufferSize = v)
        .Int32("max_write_buffer_number", static o => o.MaxWriteBufferNumber, static (o, v) => o.MaxWriteBufferNumber = v)
        .Int32("min_write_buffer_number_to_merge", static o => o.MinWriteBufferNumberToMerge, static (o, v) => o.MinWriteBufferNumberToMerge = v)
        .Int64("max_write_buffer_size_to_maintain", static o => o.MaxWriteBufferSizeToMaintain, static (o, v) => o.MaxWriteBufferSizeToMaintain = v)
        .UInt64("arena_block_size", static o => o.ArenaBlockSize, static (o, v) => o.ArenaBlockSize = v)
        .Bool("inplace_update_support", static o => o.InplaceUpdateSupport, static (o, v) => o.InplaceUpdateSupport = v)
        .UInt64("inplace_update_num_locks", static o => o.InplaceUpdateNumLocks, static (o, v) => o.InplaceUpdateNumLocks = v)
        .Double("memtable_prefix_bloom_size_ratio", static o => o.MemtablePrefixBloomSizeRatio, static (o, v) => o.MemtablePrefixBloomSizeRatio = v)
        .Bool("memtable_whole_key_filtering", static o => o.MemtableWholeKeyFiltering, static (o, v) => o.MemtableWholeKeyFiltering = v)
        .UInt64("memtable_huge_page_size", static o => o.MemtableHugePageSize, static (o, v) => o.MemtableHugePageSize = v)
        .UInt32("bloom_locality", static o => o.BloomLocality, static (o, v) => o.BloomLocality = v)
        .UInt64("max_successive_merges", static o => o.MaxSuccessiveMerges, static (o, v) => o.MaxSuccessiveMerges = v)
        .UInt32("memtable_protection_bytes_per_key", static o => o.MemtableProtectionBytesPerKey, static (o, v) => o.MemtableProtectionBytesPerKey = v)
        .Double("experimental_mempurge_threshold", static o => o.ExperimentalMempurgeThreshold, static (o, v) => o.ExperimentalMempurgeThreshold = v)
        .Enum("compression", OptionEnumMaps.Compression, static o => o.Compression, static (o, v) => o.Compression = v)
        .Enum("bottommost_compression", OptionEnumMaps.Compression, static o => o.BottommostCompression, static (o, v) => o.BottommostCompression = v)
        .Add("compression_opts", static o => o.CompressionOpts.ToStructString(),
            static (o, v) => o.CompressionOpts = CompressionOptions.ParseStructString("compression_opts", v))
        .Add("bottommost_compression_opts", static o => o.BottommostCompressionOpts.ToStructString(),
            static (o, v) => o.BottommostCompressionOpts = CompressionOptions.ParseStructString("bottommost_compression_opts", v))
        .Add("compression_per_level", static o => FormatCompressionList(o.CompressionPerLevel),
            static (o, v) => o.CompressionPerLevel = ParseCompressionList(v))
        .UInt64("sample_for_compression", static o => o.SampleForCompression, static (o, v) => o.SampleForCompression = v)
        .Int32("num_levels", static o => o.NumLevels, static (o, v) => o.NumLevels = v)
        .Int32("level0_file_num_compaction_trigger", static o => o.Level0FileNumCompactionTrigger, static (o, v) => o.Level0FileNumCompactionTrigger = v)
        .Int32("level0_slowdown_writes_trigger", static o => o.Level0SlowdownWritesTrigger, static (o, v) => o.Level0SlowdownWritesTrigger = v)
        .Int32("level0_stop_writes_trigger", static o => o.Level0StopWritesTrigger, static (o, v) => o.Level0StopWritesTrigger = v)
        .UInt64("target_file_size_base", static o => o.TargetFileSizeBase, static (o, v) => o.TargetFileSizeBase = v)
        .Int32("target_file_size_multiplier", static o => o.TargetFileSizeMultiplier, static (o, v) => o.TargetFileSizeMultiplier = v)
        .UInt64("max_bytes_for_level_base", static o => o.MaxBytesForLevelBase, static (o, v) => o.MaxBytesForLevelBase = v)
        .Double("max_bytes_for_level_multiplier", static o => o.MaxBytesForLevelMultiplier, static (o, v) => o.MaxBytesForLevelMultiplier = v)
        .Add("max_bytes_for_level_multiplier_additional", static o => OptionStrings.FormatIntArray(o.MaxBytesForLevelMultiplierAdditional),
            static (o, v) => o.MaxBytesForLevelMultiplierAdditional = OptionStrings.ParseIntArray("max_bytes_for_level_multiplier_additional", v))
        .Bool("level_compaction_dynamic_level_bytes", static o => o.LevelCompactionDynamicLevelBytes, static (o, v) => o.LevelCompactionDynamicLevelBytes = v)
        .UInt64("max_compaction_bytes", static o => o.MaxCompactionBytes, static (o, v) => o.MaxCompactionBytes = v)
        .UInt64("soft_pending_compaction_bytes_limit", static o => o.SoftPendingCompactionBytesLimit, static (o, v) => o.SoftPendingCompactionBytesLimit = v)
        .UInt64("hard_pending_compaction_bytes_limit", static o => o.HardPendingCompactionBytesLimit, static (o, v) => o.HardPendingCompactionBytesLimit = v)
        .Bool("disable_auto_compactions", static o => o.DisableAutoCompactions, static (o, v) => o.DisableAutoCompactions = v)
        .Enum("compaction_style", OptionEnumMaps.CompactionStyle, static o => o.CompactionStyle, static (o, v) => o.CompactionStyle = v)
        .Enum("compaction_pri", OptionEnumMaps.CompactionPri, static o => o.CompactionPri, static (o, v) => o.CompactionPri = v)
        .UInt64("max_sequential_skip_in_iterations", static o => o.MaxSequentialSkipInIterations, static (o, v) => o.MaxSequentialSkipInIterations = v)
        .Bool("optimize_filters_for_hits", static o => o.OptimizeFiltersForHits, static (o, v) => o.OptimizeFiltersForHits = v)
        .Bool("paranoid_file_checks", static o => o.ParanoidFileChecks, static (o, v) => o.ParanoidFileChecks = v)
        .Bool("force_consistency_checks", static o => o.ForceConsistencyChecks, static (o, v) => o.ForceConsistencyChecks = v)
        .Bool("report_bg_io_stats", static o => o.ReportBgIoStats, static (o, v) => o.ReportBgIoStats = v)
        .UInt64("ttl", static o => o.Ttl, static (o, v) => o.Ttl = v)
        .UInt64("periodic_compaction_seconds", static o => o.PeriodicCompactionSeconds, static (o, v) => o.PeriodicCompactionSeconds = v)
        .UInt64("preclude_last_level_data_seconds", static o => o.PrecludeLastLevelDataSeconds, static (o, v) => o.PrecludeLastLevelDataSeconds = v)
        .UInt64("preserve_internal_time_seconds", static o => o.PreserveInternalTimeSeconds, static (o, v) => o.PreserveInternalTimeSeconds = v)
        .Enum("last_level_temperature", OptionEnumMaps.Temperature, static o => o.LastLevelTemperature, static (o, v) => o.LastLevelTemperature = v)
        .Enum("default_temperature", OptionEnumMaps.Temperature, static o => o.DefaultTemperature, static (o, v) => o.DefaultTemperature = v)
        .Enum("default_write_temperature", OptionEnumMaps.Temperature, static o => o.DefaultWriteTemperature, static (o, v) => o.DefaultWriteTemperature = v)
        .UInt32("block_protection_bytes_per_key", static o => o.BlockProtectionBytesPerKey, static (o, v) => o.BlockProtectionBytesPerKey = v)
        .Bool("persist_user_defined_timestamps", static o => o.PersistUserDefinedTimestamps, static (o, v) => o.PersistUserDefinedTimestamps = v)
        .Bool("cf_allow_ingest_behind", static o => o.CfAllowIngestBehind, static (o, v) => o.CfAllowIngestBehind = v)
        .Bool("enable_blob_files", static o => o.EnableBlobFiles, static (o, v) => o.EnableBlobFiles = v)
        .UInt64("min_blob_size", static o => o.MinBlobSize, static (o, v) => o.MinBlobSize = v)
        .UInt64("blob_file_size", static o => o.BlobFileSize, static (o, v) => o.BlobFileSize = v)
        .Enum("blob_compression_type", OptionEnumMaps.Compression, static o => o.BlobCompressionType, static (o, v) => o.BlobCompressionType = v)
        .Bool("enable_blob_garbage_collection", static o => o.EnableBlobGarbageCollection, static (o, v) => o.EnableBlobGarbageCollection = v)
        .Double("blob_garbage_collection_age_cutoff", static o => o.BlobGarbageCollectionAgeCutoff, static (o, v) => o.BlobGarbageCollectionAgeCutoff = v)
        .Double("blob_garbage_collection_force_threshold", static o => o.BlobGarbageCollectionForceThreshold,
            static (o, v) => o.BlobGarbageCollectionForceThreshold = v)
        .UInt64("blob_compaction_readahead_size", static o => o.BlobCompactionReadaheadSize, static (o, v) => o.BlobCompactionReadaheadSize = v)
        .Int32("blob_file_starting_level", static o => o.BlobFileStartingLevel, static (o, v) => o.BlobFileStartingLevel = v)
        .Enum("prepopulate_blob_cache", OptionEnumMaps.PrepopulateBlobCache, static o => o.PrepopulateBlobCache, static (o, v) => o.PrepopulateBlobCache = v);

    private static string MergeOperatorId(MergeOperator m) => m.Name;
    private static string CompactionFilterId(CompactionFilter f) => f.Name;
    private static string CompactionFilterFactoryId(CompactionFilterFactory f) => f.Name;
    private static string SliceTransformId(SliceTransform t) => t.AsString();

    internal override OptionTable<ColumnFamilyOptions> OptionTable => s_table;

    /// <summary>Deep copy (table options and compression options included).</summary>
    public ColumnFamilyOptions Clone()
    {
        var c = new ColumnFamilyOptions();
        c.CopyFrom(this);
        c._mergeOperator = _mergeOperator;
        c._prefixExtractor = _prefixExtractor;
        c._memtableInsertWithHintPrefixExtractor = _memtableInsertWithHintPrefixExtractor;
        c._compactionFilter = _compactionFilter;
        c._compactionFilterFactory = _compactionFilterFactory;
        c.CompactionOptionsFifo = CompactionOptionsFifo.Clone();
        c.CompactionOptionsUniversal = CompactionOptionsUniversal.Clone();
        c.TableOptions = TableOptions.Clone();
        c.OtherTableFactoryOptions = OtherTableFactoryOptions is null ? null : new Dictionary<string, string>(OtherTableFactoryOptions, StringComparer.Ordinal);
        return c;
    }

    private static string FormatCompressionList(List<CompressionType> list)
    {
        var parts = new string[list.Count];
        for (int i = 0; i < list.Count; i++) parts[i] = OptionEnumMaps.Compression.ToName(list[i]);
        return string.Join(':', parts);
    }

    private static List<CompressionType> ParseCompressionList(string v)
    {
        var list = new List<CompressionType>();
        if (v.Length == 0) return list;
        foreach (string p in v.Split(':')) list.Add(OptionEnumMaps.Compression.Parse("compression_per_level", p.Trim()));
        return list;
    }
}
