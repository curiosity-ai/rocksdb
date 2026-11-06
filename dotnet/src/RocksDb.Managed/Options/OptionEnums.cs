namespace RocksDb.Managed;

/// <summary>Mirrors rocksdb::Temperature (include/rocksdb/types.h). Persisted in the MANIFEST (1 byte).</summary>
public enum Temperature : byte
{
    Unknown = 0,
    Hot = 0x04,
    Warm = 0x08,
    Cool = 0x0A,
    Cold = 0x0C,
    Ice = 0x10,
    /// <summary>Invalid sentinel just past the valid values (kLastTemperature).</summary>
    LastTemperature = 0x11,
}

/// <summary>Mirrors rocksdb::CompactionStyle (include/rocksdb/advanced_options.h).</summary>
public enum CompactionStyle : byte
{
    Level = 0x0,
    Universal = 0x1,
    Fifo = 0x2,
    None = 0x3,
}

/// <summary>Mirrors rocksdb::CompactionPri.</summary>
public enum CompactionPri : byte
{
    ByCompensatedSize = 0x0,
    OldestLargestSeqFirst = 0x1,
    OldestSmallestSeqFirst = 0x2,
    MinOverlappingRatio = 0x3,
    RoundRobin = 0x4,
}

/// <summary>Mirrors rocksdb::WALRecoveryMode (include/rocksdb/options.h).</summary>
public enum WalRecoveryMode : byte
{
    TolerateCorruptedTailRecords = 0x00,
    AbsoluteConsistency = 0x01,
    PointInTimeRecovery = 0x02,
    SkipAnyCorruptedRecords = 0x03,
}

/// <summary>Mirrors rocksdb::InfoLogLevel (include/rocksdb/env.h).</summary>
public enum InfoLogLevel : byte
{
    Debug = 0,
    Info,
    Warn,
    Error,
    Fatal,
    Header,
}

/// <summary>Mirrors rocksdb::PrepopulateBlobCache.</summary>
public enum PrepopulateBlobCache : byte
{
    Disable = 0x0,
    FlushOnly = 0x1,
}

/// <summary>Mirrors BlockBasedTableOptions::IndexType (include/rocksdb/table.h). Persisted in table properties.</summary>
public enum IndexType : byte
{
    BinarySearch = 0x00,
    HashSearch = 0x01,
    TwoLevelIndexSearch = 0x02,
    BinarySearchWithFirstKey = 0x03,
}

/// <summary>Mirrors BlockBasedTableOptions::DataBlockIndexType.</summary>
public enum DataBlockIndexType : byte
{
    BinarySearch = 0,
    BinaryAndHash = 1,
}

/// <summary>
/// Mirrors BlockBasedTableOptions::BlockSearchType (include/rocksdb/table.h): the search algorithm used when seeking in
/// index blocks. Read-time only; compatible with any block.
/// </summary>
public enum IndexBlockSearchType : byte
{
    /// <summary>Standard binary search (kBinary, default).</summary>
    Binary = 0x00,
    /// <summary>
    /// Interpolation search (kInterpolation), which may be faster for uniformly distributed keys. Requires the bytewise
    /// comparator.
    /// </summary>
    Interpolation = 0x01,
    /// <summary>
    /// kAuto: interpolation search for index blocks whose footer carries the "is_uniform" hint (written with
    /// <see cref="BlockBasedTableOptions.UniformCvThreshold"/> &gt;= 0) when the comparator is bytewise, else binary search.
    /// </summary>
    Auto = 0x02,
}

/// <summary>Mirrors BlockBasedTableOptions::IndexShorteningMode.</summary>
public enum IndexShorteningMode : byte
{
    NoShortening,
    ShortenSeparators,
    ShortenSeparatorsAndSuccessor,
}

/// <summary>
/// Mirrors BlockBasedTableOptions::OptimizeKeyCommonPrefix: when the format_version &gt;= 8 common user-key prefix
/// optimization is used for data and index blocks.
/// </summary>
public enum OptimizeKeyCommonPrefix : byte
{
    /// <summary>Never use the optimization.</summary>
    Disabled = 0,
    /// <summary>Only with a (reverse-)bytewise comparator (where it also enables the fast Seek). Default.</summary>
    IfFastSeek = 1,
    /// <summary>For all comparators.</summary>
    Enabled = 2,
}

/// <summary>Mirrors BlockBasedTableOptions::PrepopulateBlockCache.</summary>
public enum PrepopulateBlockCache : byte
{
    Disable,
    FlushOnly,
}

/// <summary>Mirrors rocksdb::ReadTier.</summary>
public enum ReadTier : byte
{
    ReadAllTier = 0x0,
    BlockCacheTier = 0x1,
    PersistedTier = 0x2,
    MemtableTier = 0x3,
}

/// <summary>Mirrors rocksdb::BlobGarbageCollectionPolicy (manual compaction override of blob garbage collection).</summary>
public enum BlobGarbageCollectionPolicy : byte
{
    /// <summary>Force blob file garbage collection.</summary>
    Force = 0,
    /// <summary>Skip blob file garbage collection.</summary>
    Disable = 1,
    /// <summary>Inherit the policy from the column family options.</summary>
    UseDefault = 2,
}

/// <summary>Mirrors rocksdb::BottommostLevelCompaction.</summary>
public enum BottommostLevelCompaction : byte
{
    Skip,
    IfHaveCompactionFilter,
    Force,
    ForceOptimized,
}

/// <summary>
/// The memtable representation factory (ColumnFamilyOptions::memtable_factory). OPTIONS-file names are the factories'
/// class names (include/rocksdb/memtablerep.h, memtable/hash_*_rep.cc). The managed port implements
/// <see cref="SkipList"/> only; the other values exist so OPTIONS files written by native RocksDB round-trip.
/// </summary>
public enum MemTableFactoryType : byte
{
    /// <summary>"SkipListFactory" (default).</summary>
    SkipList,
    /// <summary>"VectorRepFactory".</summary>
    Vector,
    /// <summary>"HashSkipListRepFactory".</summary>
    HashSkipList,
    /// <summary>"HashLinkListRepFactory".</summary>
    HashLinkList,
}

/// <summary>
/// The table format factory (ColumnFamilyOptions::table_factory); names are TableFactory::kXxxTableName()
/// (include/rocksdb/table.h) and AdaptiveTableFactory::Name() (table/adaptive/adaptive_table_factory.h).
/// </summary>
public enum TableFactoryType : byte
{
    /// <summary>"BlockBasedTable" (default).</summary>
    BlockBasedTable,
    /// <summary>"PlainTable".</summary>
    PlainTable,
    /// <summary>"CuckooTable".</summary>
    CuckooTable,
    /// <summary>
    /// "AdaptiveTableFactory" (NewAdaptiveTableFactory): reads block-based, plain and cuckoo tables by their magic number
    /// and writes <see cref="ColumnFamilyOptions.AdaptiveTableWriteFactory"/>. Upstream does not register it for
    /// OPTIONS-file loading.
    /// </summary>
    AdaptiveTable,
}

/// <summary>
/// When a block-based table data block is cut (BlockBasedTableOptions::flush_block_policy_factory;
/// include/rocksdb/flush_block_policy.h, table/block_based/flush_block_policy_impl.h).
/// </summary>
public enum FlushBlockPolicyType : byte
{
    /// <summary>"FlushBlockBySizePolicyFactory": cut by <see cref="BlockBasedTableOptions.BlockSize"/> (default).</summary>
    BySize,
    /// <summary>"FlushBlockEveryKeyPolicyFactory": one entry per data block.</summary>
    EveryKey,
}

/// <summary>
/// Port of rocksdb::VerifyOutputFlags (include/rocksdb/advanced_options.h): a bitmask of what to verify on compaction
/// output files (low bits) and for which compactions (bits 10+). Persisted as its uint32 value (verify_output_flags).
/// </summary>
[Flags]
public enum VerifyOutputFlags : uint
{
    /// <summary>kVerifyNone: no verification (default).</summary>
    None = 0x0,
    /// <summary>kVerifyBlockChecksum: verify block checksums.</summary>
    VerifyBlockChecksum = 1u << 0,
    /// <summary>kVerifyIteration: iterate the output and compare the full key/value hash with what was written.</summary>
    VerifyIteration = 1u << 1,
    /// <summary>kVerifyFileChecksum: verify the file-level checksum.</summary>
    VerifyFileChecksum = 1u << 2,
    /// <summary>kEnableForLocalCompaction: verify outputs of local compactions.</summary>
    EnableForLocalCompaction = 1u << 10,
    /// <summary>kEnableForRemoteCompaction: verify outputs of remote compactions.</summary>
    EnableForRemoteCompaction = 1u << 11,
    /// <summary>kVerifyAll.</summary>
    VerifyAll = 0xFFFFFFFF,
}

/// <summary>
/// Port of rocksdb::PinningTier (include/rocksdb/table.h): which block-based tables' metadata blocks are pinned in the
/// block cache (<see cref="MetadataCacheOptions"/>).
/// </summary>
public enum PinningTier : byte
{
    /// <summary>kFallback: use pin_l0_filter_and_index_blocks_in_cache / pin_top_level_index_and_filter (default).</summary>
    Fallback,
    /// <summary>kNone: no tables.</summary>
    None,
    /// <summary>kFlushedAndSimilar: L0 tables smaller than 1.5 times write_buffer_size.</summary>
    FlushedAndSimilar,
    /// <summary>kAll: all tables.</summary>
    All,
}
