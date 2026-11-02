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
    /// <summary>Supported for writing: 2..7. Default 7 (upstream default).</summary>
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
    public bool EnableIndexCompression { get; init; } = true;
    /// <summary>Verify block checksums when reading (ReadOptions::verify_checksums default true).</summary>
    public bool VerifyChecksums { get; init; } = true;

    public static TableFormatOptions Default { get; } = new();

    internal void Validate()
    {
        if (FormatVersion < BlockBasedTableConstants.MinSupportedFormatVersionForWrite || FormatVersion > BlockBasedTableConstants.LatestSupportedFormatVersion)
            ThrowHelper.InvalidArgument("Unsupported BlockBasedTable format_version " + FormatVersion);
        if (BlockRestartInterval < 1 || IndexBlockRestartInterval < 1) ThrowHelper.InvalidArgument("restart interval must be >= 1");
        if (BlockSize <= 0) ThrowHelper.InvalidArgument("block_size must be > 0");
        if (IndexType == IndexType.HashSearch) ThrowHelper.NotSupported("kHashSearch index is not supported for writing");
        if (IndexType == IndexType.TwoLevelIndexSearch) ThrowHelper.NotSupported("partitioned index (kTwoLevelIndexSearch) is not supported for writing yet");
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
    /// Dictionary compression is not implemented: max_dict_bytes etc. are only recorded, not applied.
    /// </summary>
    public CompressionOptions CompressionOptions { get; init; } = new();
    public ulong ColumnFamilyId { get; init; } = TableProperties.UnknownColumnFamily;
    public string ColumnFamilyName { get; init; } = "";
    /// <summary>Persisted as "rocksdb.merge.operator" ("nullptr" when none).</summary>
    public string MergeOperatorName { get; init; } = "nullptr";
    /// <summary>Persisted as "rocksdb.prefix.extractor.name" ("nullptr" when none). Prefix filtering itself is not implemented.</summary>
    public string PrefixExtractorName { get; init; } = "nullptr";
    /// <summary>
    /// Level the file is created at (FilterBuildingContext::level_at_creation); -1 when unknown (treated as bottommost
    /// by level-aware filter policies such as Ribbon's bloom_before_level).
    /// </summary>
    public int LevelAtCreation { get; init; } = -1;
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
}
