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
/// (include/rocksdb/table.h). The managed port implements <see cref="BlockBasedTable"/> only; the options of the
/// other factories are preserved verbatim in <see cref="ColumnFamilyOptions.OtherTableFactoryOptions"/>.
/// </summary>
public enum TableFactoryType : byte
{
    /// <summary>"BlockBasedTable" (default).</summary>
    BlockBasedTable,
    /// <summary>"PlainTable".</summary>
    PlainTable,
    /// <summary>"CuckooTable".</summary>
    CuckooTable,
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
