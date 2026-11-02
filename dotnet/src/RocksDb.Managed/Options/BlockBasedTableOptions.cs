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
    public DataBlockIndexType DataBlockIndexType { get; set; } = DataBlockIndexType.BinarySearch;
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
    public bool BlockAlign { get; set; }
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
        c._filterPolicy = _filterPolicy;
        return c;
    }
}
