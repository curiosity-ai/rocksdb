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
    /// <summary>
    /// user_defined_index_factory (EXPERIMENTAL): a custom index built in addition to (or instead of) the standard index,
    /// see <see cref="IndexMode"/>. Like upstream's factory pointer it is not persisted in the OPTIONS file (reattach it
    /// before opening a database whose files require it).
    /// </summary>
    public UserDefinedIndexFactory? UserDefinedIndexFactory { get; set; }
    /// <summary>
    /// index_mode (EXPERIMENTAL): how <see cref="UserDefinedIndexFactory"/>'s index interacts with the standard index
    /// (default <see cref="BlockBasedIndexMode.StandardDefault"/>). The OPTIONS-file entries (index_mode, the legacy
    /// use_udi_as_primary_index / fail_if_no_udi_on_open / skip_standard_index booleans) are preserved verbatim but not
    /// interpreted; set this property explicitly.
    /// </summary>
    public BlockBasedIndexMode IndexMode { get; set; } = BlockBasedIndexMode.StandardDefault;
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
        .EnumId("flush_block_policy_factory", OptionEnumMaps.FlushBlockPolicy, static o => o.FlushBlockPolicy, static (o, v) => o.FlushBlockPolicy = v);

    /// <summary>The persisted id of a filter policy object: the built-in policies' Id (FilterPolicy::GetId), else its name.</summary>
    private static string FilterPolicyId(FilterPolicy p) => p is BloomLikeFilterPolicy b ? b.Id : p.Name;

    internal override OptionTable<BlockBasedTableOptions> OptionTable => s_table;

    public BlockBasedTableOptions Clone()
    {
        var c = new BlockBasedTableOptions();
        c.CopyFrom(this);
        c.BlockCache = BlockCache;
        c.UserDefinedIndexFactory = UserDefinedIndexFactory;
        c.IndexMode = IndexMode;
        c._filterPolicy = _filterPolicy;
        return c;
    }
}
