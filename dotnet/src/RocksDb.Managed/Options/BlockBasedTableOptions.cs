using RocksDb.Managed.Options;
using RocksDb.Managed.Table;
using RocksDb.Managed.Table.Filters;

namespace RocksDb.Managed;

/// <summary>
/// Port of rocksdb::BlockBasedTableOptions (include/rocksdb/table.h) with upstream defaults; OPTIONS-file names and
/// string forms follow table/block_based/block_based_table_factory.cc.
/// </summary>
public sealed class BlockBasedTableOptions : PersistedOptions<BlockBasedTableOptions>
{
    public ulong BlockSize { get; set; } = 4 * 1024;
    public int BlockSizeDeviation { get; set; } = 10;
    public int BlockRestartInterval { get; set; } = 16;
    public int IndexBlockRestartInterval { get; set; } = 1;
    public ulong MetadataBlockSize { get; set; } = 4096;
    public uint FormatVersion { get; set; } = 7;
    public ChecksumType Checksum { get; set; } = ChecksumType.Xxh3;
    public IndexType IndexType { get; set; } = IndexType.BinarySearch;
    /// <summary>
    /// index_block_search_type: the search algorithm for index blocks (read time only; default binary).
    /// <see cref="IndexBlockSearchType.Interpolation"/> requires the bytewise comparator;
    /// <see cref="IndexBlockSearchType.Auto"/> uses interpolation search only on index blocks marked "is_uniform" (see
    /// <see cref="UniformCvThreshold"/>). Partitioned-index top levels and hash indexes always use binary search, like upstream.
    /// </summary>
    public IndexBlockSearchType IndexBlockSearchType { get; set; } = IndexBlockSearchType.Binary;
    public DataBlockIndexType DataBlockIndexType { get; set; } = DataBlockIndexType.BinarySearch;
    private CustomizableOption<UserDefinedIndexFactory> _udiFactory;
    private BlockBasedIndexMode _indexMode = BlockBasedIndexMode.StandardDefault; // index_mode as set / parsed

    /// <summary>
    /// user_defined_index_factory (EXPERIMENTAL): a custom index built in addition to (or instead of) the standard index,
    /// see <see cref="IndexMode"/>. Persisted by name like upstream's Customizable (<c>user_defined_index_factory=trie_index</c>,
    /// <c>nullptr</c> when unset): the built-in <see cref="TrieIndexFactory"/> ("trie_index") is recreated when an
    /// OPTIONS file is read; other names are kept as <see cref="UnresolvedUserDefinedIndexFactoryName"/> and written back
    /// verbatim (attach the matching factory before opening a database whose files require it).
    /// </summary>
    public UserDefinedIndexFactory? UserDefinedIndexFactory
    {
        get => _udiFactory.Value;
        set => _udiFactory.Set(value);
    }

    /// <summary>The user_defined_index_factory name read from an OPTIONS file that is not a built-in factory (null otherwise).</summary>
    public string? UnresolvedUserDefinedIndexFactoryName => _udiFactory.UnresolvedText;

    /// <summary>
    /// index_mode (EXPERIMENTAL): how <see cref="UserDefinedIndexFactory"/>'s index interacts with the standard index
    /// (default <see cref="BlockBasedIndexMode.StandardDefault"/>). Reading returns the effective mode
    /// (BlockBasedTableOptions::GetEffectiveIndexMode): the index_mode option unless it is StandardDefault, else
    /// CustomDefault when <see cref="UseUdiAsPrimaryIndex"/>, else StandardRequired when <see cref="FailIfNoUdiOnOpen"/>.
    /// Setting it sets index_mode and the two legacy booleans to match (so that native releases which route only by the
    /// booleans, such as 11.12, read the same policy from the OPTIONS file).
    /// </summary>
    public BlockBasedIndexMode IndexMode
    {
        get => _indexMode != BlockBasedIndexMode.StandardDefault ? _indexMode
            : UseUdiAsPrimaryIndex ? BlockBasedIndexMode.CustomDefault
            : FailIfNoUdiOnOpen ? BlockBasedIndexMode.StandardRequired
            : BlockBasedIndexMode.StandardDefault;
        set
        {
            _indexMode = value;
            UseUdiAsPrimaryIndex = value == BlockBasedIndexMode.CustomDefault;
            FailIfNoUdiOnOpen = value == BlockBasedIndexMode.StandardRequired;
        }
    }

    /// <summary>
    /// use_udi_as_primary_index (EXPERIMENTAL, deprecated compatibility input; prefer <see cref="IndexMode"/>): with
    /// index_mode StandardDefault, routes reads through the custom index (<see cref="BlockBasedIndexMode.CustomDefault"/>).
    /// Persisted as written; native 11.12 routes by this flag only.
    /// </summary>
    public bool UseUdiAsPrimaryIndex { get; set; }

    /// <summary>
    /// fail_if_no_udi_on_open (EXPERIMENTAL, deprecated compatibility input; prefer <see cref="IndexMode"/>): with
    /// index_mode StandardDefault and <see cref="UseUdiAsPrimaryIndex"/> false, opening a file without the custom index
    /// fails (<see cref="BlockBasedIndexMode.StandardRequired"/>).
    /// </summary>
    public bool FailIfNoUdiOnOpen { get; set; }
    public double DataBlockHashTableUtilRatio { get; set; } = 0.75;
    private CustomizableOption<FilterPolicy> _filterPolicy;

    /// <summary>
    /// Filter policy (e.g. <c>new BloomFilterPolicy(10)</c>, <c>new RibbonFilterPolicy(10, 0)</c>), or null for no filter.
    /// Persisted as filter_policy by id (<c>bloomfilter:10:false</c>, <c>ribbonfilter:10:0</c>); built-in ids read from an
    /// OPTIONS file (including the struct form <c>{id=ribbonfilter:10:0;bloom_before_level=0;}</c>) are recreated as
    /// policy objects, and the parsed text is written back unchanged while the policy is not replaced or reconfigured.
    /// </summary>
    public FilterPolicy? FilterPolicy
    {
        get => _filterPolicy.Value;
        set => _filterPolicy.Set(value);
    }

    /// <summary>
    /// The filter_policy id read from an OPTIONS file that does not name a built-in policy (null otherwise). It is
    /// written back verbatim; opening a table with it fails like upstream's FilterPolicy::CreateFromString, except for
    /// the bare class names the C API writes, which mean "no filter".
    /// </summary>
    public string? UnresolvedFilterPolicyId => _filterPolicy.UnresolvedText;
    public bool WholeKeyFiltering { get; set; } = true;
    public bool PartitionFilters { get; set; }
    public bool DecouplePartitionedFilters { get; set; } = true;
    public bool OptimizeFiltersForMemory { get; set; } = true;
    public bool DetectFilterConstructCorruption { get; set; }
    public bool CacheIndexAndFilterBlocks { get; set; }
    public bool CacheIndexAndFilterBlocksWithHighPriority { get; set; } = true;
    public bool PinL0FilterAndIndexBlocksInCache { get; set; }
    public bool PinTopLevelIndexAndFilter { get; set; } = true;
    public bool NoBlockCache { get; set; }
    /// <summary>
    /// Block cache shared by readers using these options (port of <c>BlockBasedTableOptions::block_cache</c>; not persisted).
    /// When null and <see cref="NoBlockCache"/> is false, the database supplies a default 32 MiB HyperClock cache shared by
    /// all column families of that database.
    /// </summary>
    public Cache.BlockCache? BlockCache { get; set; }
    /// <summary>
    /// persistent_cache (legacy upstream; not persisted): a cache of blocks consulted before reading a block from its
    /// file, e.g. <see cref="Cache.PersistentCache.Create"/> (a file-based tier) or a <see cref="Cache.VolatileCacheTier"/>.
    /// Null (the default): none.
    /// </summary>
    public Cache.PersistentCache? PersistentCache { get; set; }
    /// <summary>block_align: pad data blocks to min(block_size, 4 KiB) (block_size must be a power of 2; requires no compression).</summary>
    public bool BlockAlign { get; set; }
    /// <summary>
    /// super_block_alignment_size: avoid a data block straddling a boundary of this power-of-two size by padding (at most
    /// size / <see cref="SuperBlockAlignmentSpaceOverheadRatio"/> bytes per block). 0 (default) disables it.
    /// </summary>
    public ulong SuperBlockAlignmentSize { get; set; }
    /// <summary>super_block_alignment_space_overhead_ratio (default 128; 0 disables super block alignment).</summary>
    public ulong SuperBlockAlignmentSpaceOverheadRatio { get; set; } = 128;
    /// <summary>
    /// separate_key_value_in_data_block: data blocks store the keys first and the values after them, which can improve
    /// read performance and compression at a cost of a varint per restart interval. Default false.
    /// </summary>
    public bool SeparateKeyValueInDataBlock { get; set; }
    /// <summary>
    /// uniform_cv_threshold: index blocks whose key gaps have a coefficient of variation below this get the "is_uniform"
    /// hint in their footer (used by readers with <see cref="IndexBlockSearchType"/> = Auto). -1 (default) disables it.
    /// </summary>
    public double UniformCvThreshold { get; set; } = -1;
    public bool EnableIndexCompression { get; set; } = true;
    public bool UseDeltaEncoding { get; set; } = true;
    public IndexShorteningMode IndexShortening { get; set; } = IndexShorteningMode.ShortenSeparators;
    /// <summary>format_version &gt;= 8 common user-key prefix optimization (needs <see cref="UseDeltaEncoding"/>).</summary>
    public OptimizeKeyCommonPrefix OptimizeKeyCommonPrefix { get; set; } = OptimizeKeyCommonPrefix.IfFastSeek;
    public uint ReadAmpBytesPerBit { get; set; }
    public bool VerifyCompression { get; set; }
    public PrepopulateBlockCache PrepopulateBlockCache { get; set; } = PrepopulateBlockCache.Disable;
    public ulong MaxAutoReadaheadSize { get; set; } = 256 * 1024;
    public ulong InitialAutoReadaheadSize { get; set; } = 8 * 1024;
    public ulong NumFileReadsForAutoReadahead { get; set; } = 2;
    /// <summary>flush_block_policy_factory (the managed table builder cuts blocks by size).</summary>
    public FlushBlockPolicyType FlushBlockPolicy { get; set; } = FlushBlockPolicyType.BySize;

    private static readonly OptionTable<BlockBasedTableOptions> s_table = new OptionTable<BlockBasedTableOptions>()
        .UInt64("block_size", static o => o.BlockSize, static (o, v) => o.BlockSize = v)
        .Int32("block_size_deviation", static o => o.BlockSizeDeviation, static (o, v) => o.BlockSizeDeviation = v)
        .Int32("block_restart_interval", static o => o.BlockRestartInterval, static (o, v) => o.BlockRestartInterval = v)
        .Int32("index_block_restart_interval", static o => o.IndexBlockRestartInterval, static (o, v) => o.IndexBlockRestartInterval = v)
        .UInt64("metadata_block_size", static o => o.MetadataBlockSize, static (o, v) => o.MetadataBlockSize = v)
        .UInt32("format_version", static o => o.FormatVersion, static (o, v) => o.FormatVersion = v)
        .Enum("checksum", OptionEnumMaps.Checksum, static o => o.Checksum, static (o, v) => o.Checksum = v)
        .Enum("index_type", OptionEnumMaps.IndexType, static o => o.IndexType, static (o, v) => o.IndexType = v)
        .Enum("index_block_search_type", OptionEnumMaps.IndexBlockSearchType, static o => o.IndexBlockSearchType,
            static (o, v) => o.IndexBlockSearchType = v)
        .Enum("data_block_index_type", OptionEnumMaps.DataBlockIndexType, static o => o.DataBlockIndexType, static (o, v) => o.DataBlockIndexType = v)
        .Double("data_block_hash_table_util_ratio", static o => o.DataBlockHashTableUtilRatio, static (o, v) => o.DataBlockHashTableUtilRatio = v)
        .Add("filter_policy", static o => o._filterPolicy.Serialize(FilterPolicyId) ?? "nullptr",
            static (o, v) => o._filterPolicy.Parse(v, Db.TableFactory.CreateFilterPolicy, FilterPolicyId))
        .Bool("whole_key_filtering", static o => o.WholeKeyFiltering, static (o, v) => o.WholeKeyFiltering = v)
        .Bool("partition_filters", static o => o.PartitionFilters, static (o, v) => o.PartitionFilters = v)
        .Bool("decouple_partitioned_filters", static o => o.DecouplePartitionedFilters, static (o, v) => o.DecouplePartitionedFilters = v)
        .Bool("optimize_filters_for_memory", static o => o.OptimizeFiltersForMemory, static (o, v) => o.OptimizeFiltersForMemory = v)
        .Bool("detect_filter_construct_corruption", static o => o.DetectFilterConstructCorruption, static (o, v) => o.DetectFilterConstructCorruption = v)
        .Bool("cache_index_and_filter_blocks", static o => o.CacheIndexAndFilterBlocks, static (o, v) => o.CacheIndexAndFilterBlocks = v)
        .Bool("cache_index_and_filter_blocks_with_high_priority", static o => o.CacheIndexAndFilterBlocksWithHighPriority,
            static (o, v) => o.CacheIndexAndFilterBlocksWithHighPriority = v)
        .Bool("pin_l0_filter_and_index_blocks_in_cache", static o => o.PinL0FilterAndIndexBlocksInCache, static (o, v) => o.PinL0FilterAndIndexBlocksInCache = v)
        .Bool("pin_top_level_index_and_filter", static o => o.PinTopLevelIndexAndFilter, static (o, v) => o.PinTopLevelIndexAndFilter = v)
        .Bool("no_block_cache", static o => o.NoBlockCache, static (o, v) => o.NoBlockCache = v)
        .Bool("block_align", static o => o.BlockAlign, static (o, v) => o.BlockAlign = v)
        .UInt64("super_block_alignment_size", static o => o.SuperBlockAlignmentSize, static (o, v) => o.SuperBlockAlignmentSize = v)
        .UInt64("super_block_alignment_space_overhead_ratio", static o => o.SuperBlockAlignmentSpaceOverheadRatio,
            static (o, v) => o.SuperBlockAlignmentSpaceOverheadRatio = v)
        .Bool("separate_key_value_in_data_block", static o => o.SeparateKeyValueInDataBlock, static (o, v) => o.SeparateKeyValueInDataBlock = v)
        .Double("uniform_cv_threshold", static o => o.UniformCvThreshold, static (o, v) => o.UniformCvThreshold = v)
        .Bool("enable_index_compression", static o => o.EnableIndexCompression, static (o, v) => o.EnableIndexCompression = v)
        .Bool("use_delta_encoding", static o => o.UseDeltaEncoding, static (o, v) => o.UseDeltaEncoding = v)
        .Enum("index_shortening", OptionEnumMaps.IndexShortening, static o => o.IndexShortening, static (o, v) => o.IndexShortening = v)
        .Enum("optimize_key_common_prefix", OptionEnumMaps.OptimizeKeyCommonPrefix, static o => o.OptimizeKeyCommonPrefix,
            static (o, v) => o.OptimizeKeyCommonPrefix = v)
        .UInt32("read_amp_bytes_per_bit", static o => o.ReadAmpBytesPerBit, static (o, v) => o.ReadAmpBytesPerBit = v)
        .Bool("verify_compression", static o => o.VerifyCompression, static (o, v) => o.VerifyCompression = v)
        .Enum("prepopulate_block_cache", OptionEnumMaps.PrepopulateBlockCache, static o => o.PrepopulateBlockCache, static (o, v) => o.PrepopulateBlockCache = v)
        .UInt64("max_auto_readahead_size", static o => o.MaxAutoReadaheadSize, static (o, v) => o.MaxAutoReadaheadSize = v)
        .UInt64("initial_auto_readahead_size", static o => o.InitialAutoReadaheadSize, static (o, v) => o.InitialAutoReadaheadSize = v)
        .UInt64("num_file_reads_for_auto_readahead", static o => o.NumFileReadsForAutoReadahead, static (o, v) => o.NumFileReadsForAutoReadahead = v)
        .EnumId("flush_block_policy_factory", OptionEnumMaps.FlushBlockPolicy, static o => o.FlushBlockPolicy, static (o, v) => o.FlushBlockPolicy = v)
        // The user-defined index entries native writes (block_based_table_type_info, table/block_based/
        // block_based_table_factory.cc): the factory by name and the raw index_mode / legacy booleans (11.12 serializes
        // the fields as set; the effective policy is resolved by IndexMode).
        .Add("user_defined_index_factory", static o => o._udiFactory.Serialize(static f => f.Name) ?? "nullptr",
            static (o, v) => o._udiFactory.Parse(v, CreateUserDefinedIndexFactory, static f => f.Name))
        .Enum("index_mode", OptionEnumMaps.IndexMode, static o => o._indexMode, static (o, v) => o._indexMode = v)
        .Bool("use_udi_as_primary_index", static o => o.UseUdiAsPrimaryIndex, static (o, v) => o.UseUdiAsPrimaryIndex = v)
        .Bool("fail_if_no_udi_on_open", static o => o.FailIfNoUdiOnOpen, static (o, v) => o.FailIfNoUdiOnOpen = v)
        .Struct("metadata_cache_options", static o => o.MetadataCacheOptions, static (o, v) => o.MetadataCacheOptions = v, null)
        // OptionVerificationType::kDeprecated entries of block_based_table_type_info: accepted and ignored.
        .Deprecated("hash_index_allow_collision", "index_per_partition", "reserve_table_builder_memory",
            "reserve_table_reader_memory", "skip_table_builder_flush", "block_cache_compressed");

    /// <summary>
    /// metadata_cache_options: per-tier pinning of metadata blocks in the block cache (MetadataCacheOptions,
    /// include/rocksdb/table.h). kFallback tiers follow <see cref="PinL0FilterAndIndexBlocksInCache"/> (unpartitioned
    /// blocks and partitions) and <see cref="PinTopLevelIndexAndFilter"/> (top-level index of partitioned metadata),
    /// as in BlockBasedTable::PrefetchIndexAndFilterBlocks.
    /// </summary>
    public MetadataCacheOptions MetadataCacheOptions { get; set; } = new();

    /// <summary>
    /// Resolves a user_defined_index_factory id (IndexFactory::CreateFromString with the built-in registry): "trie_index"
    /// (also as <c>{id=trie_index}</c>) is <see cref="TrieIndexFactory.Instance"/>; other ids are unknown (null).
    /// </summary>
    internal static UserDefinedIndexFactory? CreateUserDefinedIndexFactory(string id) =>
        OptionStrings.UnwrapId(id) == TrieIndexFactory.ClassName ? TrieIndexFactory.Instance : null;

    /// <summary>The persisted id of a filter policy object: the built-in policies' Id (FilterPolicy::GetId), else its name.</summary>
    private static string FilterPolicyId(FilterPolicy p) => p is BloomLikeFilterPolicy b ? b.Id : p.Name;

    internal override OptionTable<BlockBasedTableOptions> OptionTable => s_table;

    public BlockBasedTableOptions Clone()
    {
        var c = new BlockBasedTableOptions();
        c.CopyFrom(this);
        c.BlockCache = BlockCache;
        c.PersistentCache = PersistentCache;
        c._udiFactory = _udiFactory;
        c._filterPolicy = _filterPolicy;
        return c;
    }
}

/// <summary>
/// Port of rocksdb::MetadataCacheOptions (include/rocksdb/table.h); persisted as the struct
/// <c>metadata_cache_options={top_level_index_pinning=...;partition_pinning=...;unpartitioned_pinning=...;}</c>
/// (metadata_cache_options_type_info, table/block_based/block_based_table_factory.cc).
/// </summary>
public sealed class MetadataCacheOptions : PersistedOptions<MetadataCacheOptions>
{
    /// <summary>Tables whose top-level index into metadata partitions is pinned (needs cache_index_and_filter_blocks).</summary>
    public PinningTier TopLevelIndexPinning { get; set; } = PinningTier.Fallback;
    /// <summary>Tables whose metadata partitions are pinned.</summary>
    public PinningTier PartitionPinning { get; set; } = PinningTier.Fallback;
    /// <summary>Tables whose unpartitioned metadata blocks are pinned (needs cache_index_and_filter_blocks).</summary>
    public PinningTier UnpartitionedPinning { get; set; } = PinningTier.Fallback;

    // Registered in the order native 11.12 (libstdc++ unordered_map iteration) writes the members, so the struct value is
    // byte-identical to native's.
    private static readonly OptionTable<MetadataCacheOptions> s_table = new OptionTable<MetadataCacheOptions>()
        .Enum("unpartitioned_pinning", OptionEnumMaps.PinningTier, static o => o.UnpartitionedPinning, static (o, v) => o.UnpartitionedPinning = v)
        .Enum("partition_pinning", OptionEnumMaps.PinningTier, static o => o.PartitionPinning, static (o, v) => o.PartitionPinning = v)
        .Enum("top_level_index_pinning", OptionEnumMaps.PinningTier, static o => o.TopLevelIndexPinning, static (o, v) => o.TopLevelIndexPinning = v);

    internal override OptionTable<MetadataCacheOptions> OptionTable => s_table;
}
