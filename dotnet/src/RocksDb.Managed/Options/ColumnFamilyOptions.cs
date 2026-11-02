using RocksDb.Managed.Compression;
using RocksDb.Managed.Options;

namespace RocksDb.Managed;

/// <summary>
/// Per column family options. Port of rocksdb::ColumnFamilyOptions + AdvancedColumnFamilyOptions
/// (include/rocksdb/options.h, advanced_options.h) with upstream defaults; OPTIONS-file names and string forms follow
/// options/cf_options.cc. Pluggable objects (comparator, merge operator, prefix extractor, ...) are referenced by
/// name here; the objects themselves are resolved by the DB at open time.
/// </summary>
public sealed class ColumnFamilyOptions : PersistedOptions<ColumnFamilyOptions>
{
    /// <summary>Name of the block-based table factory (TableFactory::Name()).</summary>
    public const string BlockBasedTableFactoryName = "BlockBasedTable";

    // ---- named pluggable components ----------------------------------------------------------
    /// <summary>
    /// Name of the user comparator (persisted in the MANIFEST, OPTIONS files and table properties). Setting
    /// <see cref="Comparator"/> sets this to its <see cref="Managed.Comparator.Name"/>; otherwise it must name a built-in
    /// comparator.
    /// </summary>
    public string ComparatorName { get; set; } = "leveldb.BytewiseComparator";

    /// <summary>
    /// The user key comparator (rocksdb::ColumnFamilyOptions::comparator); null = the built-in comparator named by
    /// <see cref="ComparatorName"/> (bytewise by default). A custom comparator must define a total order, be
    /// thread-safe, and keep its <see cref="Managed.Comparator.Name"/> and ordering stable for the life of the data: the
    /// name is recorded in the MANIFEST and opening an existing column family with a comparator of a different name fails
    /// with InvalidArgument. Setting it also sets <see cref="ComparatorName"/>.
    /// </summary>
    public Comparator? Comparator
    {
        get => _comparator;
        set
        {
            _comparator = value;
            if (value is not null) ComparatorName = value.Name;
        }
    }

    private Comparator? _comparator;
    public string? MergeOperatorName { get; set; }
    /// <summary>
    /// The merge operator implementation used by the DB (not persisted). When set, its <see cref="Managed.MergeOperator.Name"/>
    /// is what the DB records as <see cref="MergeOperatorName"/> in OPTIONS files and table properties.
    /// </summary>
    public MergeOperator? MergeOperator { get; set; }
    public string? CompactionFilterName { get; set; }
    public string? CompactionFilterFactoryName { get; set; }
    /// <summary>
    /// The compaction filter used by the DB (not persisted; shared by all compactions, so it must be thread-safe). When
    /// set, its <see cref="Managed.CompactionFilter.Name"/> is recorded as <see cref="CompactionFilterName"/>.
    /// </summary>
    public CompactionFilter? CompactionFilter { get; set; }
    /// <summary>
    /// Creates a compaction filter per compaction (not persisted). When set, its name is recorded as
    /// <see cref="CompactionFilterFactoryName"/>. Ignored when <see cref="CompactionFilter"/> is set.
    /// </summary>
    public CompactionFilterFactory? CompactionFilterFactory { get; set; }
    /// <summary>
    /// prefix_extractor: enables prefix Bloom filters in tables and prefix seek (see <see cref="SliceTransform"/>);
    /// null = none. Persisted by id (e.g. <c>rocksdb.FixedPrefix.4</c>); only built-in transforms can be recreated when
    /// an OPTIONS file is parsed (unknown ids parse as null, like upstream with ignore_unsupported_options).
    /// </summary>
    public SliceTransform? PrefixExtractor { get; set; }
    public string? MemtableInsertWithHintPrefixExtractor { get; set; }
    public string? SstPartitionerFactoryName { get; set; }
    public string MemtableFactoryName { get; set; } = "SkipListFactory";
    public string TableFactoryName { get; set; } = BlockBasedTableFactoryName;

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
        .Name("merge_operator", static o => o.MergeOperatorName, static (o, v) => o.MergeOperatorName = v)
        .Name("compaction_filter", static o => o.CompactionFilterName, static (o, v) => o.CompactionFilterName = v)
        .Name("compaction_filter_factory", static o => o.CompactionFilterFactoryName, static (o, v) => o.CompactionFilterFactoryName = v)
        .Name("prefix_extractor", static o => o.PrefixExtractor?.AsString(), static (o, v) => o.PrefixExtractor = SliceTransform.CreateFromString(v))
        .Name("memtable_insert_with_hint_prefix_extractor", static o => o.MemtableInsertWithHintPrefixExtractor,
            static (o, v) => o.MemtableInsertWithHintPrefixExtractor = v)
        .Name("sst_partitioner_factory", static o => o.SstPartitionerFactoryName, static (o, v) => o.SstPartitionerFactoryName = v)
        .Add("memtable_factory", static o => o.MemtableFactoryName, static (o, v) => o.MemtableFactoryName = UnwrapId(v))
        .Add("table_factory", static o => o.TableFactoryName, static (o, v) => o.TableFactoryName = UnwrapId(v))
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

    internal override OptionTable<ColumnFamilyOptions> OptionTable => s_table;

    /// <summary>Deep copy (table options and compression options included).</summary>
    public ColumnFamilyOptions Clone()
    {
        var c = new ColumnFamilyOptions();
        c.CopyFrom(this);
        c._comparator = _comparator;
        c.MergeOperator = MergeOperator;
        c.PrefixExtractor = PrefixExtractor;
        c.CompactionFilter = CompactionFilter;
        c.CompactionFilterFactory = CompactionFilterFactory;
        c.CompactionOptionsFifo = CompactionOptionsFifo.Clone();
        c.CompactionOptionsUniversal = CompactionOptionsUniversal.Clone();
        c.TableOptions = TableOptions.Clone();
        c.OtherTableFactoryOptions = OtherTableFactoryOptions is null ? null : new Dictionary<string, string>(OtherTableFactoryOptions, StringComparer.Ordinal);
        return c;
    }

    /// <summary>Customizable ids may be written as "{id=Name;...}"; keep the id only.</summary>
    private static string UnwrapId(string v)
    {
        if (v.Length > 0 && v[0] == '{')
        {
            foreach (KeyValuePair<string, string> kv in OptionStrings.ParseStruct("id", v))
            {
                if (kv.Key == "id") return kv.Value;
            }
        }
        return v;
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
