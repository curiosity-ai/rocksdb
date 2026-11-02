using RocksDb.Managed.Options;
using RocksDb.Managed.Table;

namespace RocksDb.Managed;

/// <summary>
/// Port of rocksdb::BlockBasedTableOptions (include/rocksdb/table.h) with upstream defaults; OPTIONS-file names and
/// string forms follow table/block_based/block_based_table_factory.cc. The filter policy is kept as its serialized id
/// (e.g. "bloomfilter:10:false", "ribbonfilter:10:-1"); the policy object is resolved by the table code.
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
    /// <summary>Serialized filter policy id, or null for no filter.</summary>
    public string? FilterPolicy { get; set; }
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
    public string FlushBlockPolicyFactory { get; set; } = "FlushBlockBySizePolicyFactory";

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
        .Name("filter_policy", static o => o.FilterPolicy, static (o, v) => o.FilterPolicy = v)
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
        .Add("flush_block_policy_factory", static o => o.FlushBlockPolicyFactory, static (o, v) => o.FlushBlockPolicyFactory = v);

    internal override OptionTable<BlockBasedTableOptions> OptionTable => s_table;

    public BlockBasedTableOptions Clone()
    {
        var c = new BlockBasedTableOptions();
        c.CopyFrom(this);
        c.BlockCache = BlockCache;
        return c;
    }
}
