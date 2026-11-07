using RocksDb.Managed.Compression;
using RocksDb.Managed.Table;
using System.Text;
using System.Text.Json;

namespace RocksDb.Managed.RoundTrip.Protocol;

/// <summary>Merge operator configured on a column family. Both engines use the upstream semantics.</summary>
public enum MergeKind
{
    None,
    /// <summary>rocksdb::UInt64AddOperator: 8-byte little-endian sums; wrong-sized values count as 0.</summary>
    UInt64Add,
    /// <summary>rocksdb::StringAppendOperator with delimiter <see cref="CfSpec.MergeDelimiter"/> (default ',').</summary>
    StringAppend,
    /// <summary>rocksdb::MaxOperator ("max"): the bytewise largest of the existing value and the operands.</summary>
    Max,
    /// <summary>rocksdb::BytesXOROperator ("bytesxor"): byte-wise XOR, the longer input's tail is kept.</summary>
    BytesXor,
}

/// <summary>Filter policy configured on a column family (10 bits per key).</summary>
public enum FilterKind
{
    None,
    /// <summary>Full Bloom filter (NewBloomFilterPolicy(10)).</summary>
    Bloom,
    /// <summary>Ribbon filter (NewRibbonFilterPolicy(10), bloom_before_level 0).</summary>
    Ribbon,
}

/// <summary>Prefix extractor of a column family (prefix bloom filters, hash index, hash skip list memtable).</summary>
public enum PrefixKind
{
    None,
    /// <summary>NewFixedPrefixTransform(<see cref="CfSpec.PrefixLength"/>).</summary>
    Fixed,
    /// <summary>NewCappedPrefixTransform(<see cref="CfSpec.PrefixLength"/>); native through rocksdb_get_options_from_string
    /// (the C API has no capped-prefix constructor).</summary>
    Capped,
}

/// <summary>Memtable representation.</summary>
public enum MemtableKind
{
    SkipList,
    /// <summary>VectorRepFactory.</summary>
    Vector,
    /// <summary>NewHashSkipListRepFactory (needs a prefix extractor; upstream falls back to the skip list without one).</summary>
    HashSkipList,
}

/// <summary>
/// Options of one column family, expressed engine-neutrally so the parent can hand the same configuration to the
/// native child (rocksdb-sharp setters / C API) and to the managed engine (<see cref="ColumnFamilyOptions"/>).
/// The defaults are the upstream defaults (or the values the suite always used), so a default property is not set
/// explicitly on either side.
/// </summary>
public sealed class CfSpec
{
    public string Name { get; set; } = "default";
    /// <summary>None, Snappy, LZ4 or Zstd.</summary>
    public CompressionType Compression { get; set; } = CompressionType.LZ4;
    public int FormatVersion { get; set; } = 6;
    public FilterKind Filter { get; set; } = FilterKind.Bloom;
    public ulong BlockSize { get; set; } = 4096;
    public ulong WriteBufferSize { get; set; } = 64UL << 20;
    public bool DisableAutoCompactions { get; set; }
    public MergeKind Merge { get; set; } = MergeKind.StringAppend;
    /// <summary>Created when the DB is first created (others are only created by a createcf op).</summary>
    public bool Initial { get; set; } = true;
    /// <summary>
    /// User-defined timestamps: the CF uses "leveldb.BytewiseComparator.u64ts" (native: rocksdb_comparator_with_ts_create,
    /// managed: <see cref="Comparator.BytewiseWithU64Ts"/>); every write batch gets a fresh, increasing timestamp
    /// (<see cref="Engines.UdtClock"/>) and reads use the maximum timestamp, so the latest-value model still applies.
    /// Range deletions and SST ingestion are not used on such CFs.
    /// </summary>
    public bool UserTimestamps { get; set; }
    /// <summary>Integrated BlobDB: values of at least <see cref="MinBlobSize"/> bytes go to blob files.</summary>
    public bool EnableBlobFiles { get; set; }
    public ulong MinBlobSize { get; set; }
    public ulong BlobFileSize { get; set; } = 256UL << 20;
    /// <summary>None, Snappy, LZ4 or Zstd.</summary>
    public CompressionType BlobCompression { get; set; } = CompressionType.None;
    public bool EnableBlobGc { get; set; }
    public double BlobGcAgeCutoff { get; set; } = 0.25;

    // ---- merge operator details

    /// <summary>
    /// Delimiter of <see cref="MergeKind.StringAppend"/>. With ',' native uses the suite's callback operator named
    /// "StringAppendOperator"; with any other delimiter (and for <see cref="MergeKind.Max"/> /
    /// <see cref="MergeKind.BytesXor"/>) native uses the built-in C++ operator, configured through
    /// rocksdb_get_options_from_string.
    /// </summary>
    public string MergeDelimiter { get; set; } = ",";

    // ---- block-based table

    public IndexType Index { get; set; } = IndexType.BinarySearch;
    /// <summary>Partitioned filters (upstream silently drops them unless <see cref="IndexType.TwoLevelIndexSearch"/>).</summary>
    public bool PartitionFilters { get; set; }
    public ChecksumType Checksum { get; set; } = ChecksumType.Crc32c;
    /// <summary>kDataBlockBinaryAndHash (default util ratio 0.75).</summary>
    public bool DataBlockHashIndex { get; set; }
    public int BlockRestartInterval { get; set; } = 16;
    public int IndexBlockRestartInterval { get; set; } = 1;
    public bool CacheIndexAndFilterBlocks { get; set; }
    public bool WholeKeyFiltering { get; set; } = true;
    public PrefixKind Prefix { get; set; }
    public int PrefixLength { get; set; } = 3;
    public double MemtablePrefixBloomRatio { get; set; }
    public bool MemtableWholeKeyFiltering { get; set; }
    public bool OptimizeFiltersForHits { get; set; }

    // ---- compression

    /// <summary>compression_per_level (one entry per level); empty = <see cref="Compression"/> on every level.</summary>
    public List<CompressionType> CompressionPerLevel { get; set; } = [];
    /// <summary>bottommost_compression; null = kDisableCompressionOption (the regular compression applies).</summary>
    public CompressionType? BottommostCompression { get; set; }
    /// <summary>compression_opts.level (32767 = kDefaultCompressionLevel).</summary>
    public int CompressionLevel { get; set; } = 32767;
    public uint MaxDictBytes { get; set; }
    public uint ZstdMaxTrainBytes { get; set; }
    public uint ParallelThreads { get; set; } = 1;
    /// <summary>bottommost_compression_opts gets the same dictionary settings and is enabled.</summary>
    public bool BottommostDictionary { get; set; }

    // ---- LSM shape / compaction

    public CompactionStyle Style { get; set; } = CompactionStyle.Level;
    public bool DynamicLevelBytes { get; set; } = true;
    public int NumLevels { get; set; } = 7;
    public ulong TargetFileSizeBase { get; set; } = 64UL << 20;
    public ulong MaxBytesForLevelBase { get; set; } = 256UL << 20;
    public int Level0FileNumCompactionTrigger { get; set; } = 4;
    public CompactionPri Pri { get; set; } = CompactionPri.MinOverlappingRatio;
    public uint UniversalSizeRatio { get; set; } = 1;
    public uint UniversalMaxSizeAmplificationPercent { get; set; } = 200;
    public bool UniversalAllowTrivialMove { get; set; }
    /// <summary>FIFO max_table_files_size: 1 GiB, so FIFO never drops data the model expects.</summary>
    public ulong FifoMaxTableFilesSize { get; set; } = 1UL << 30;
    public bool FifoAllowCompaction { get; set; }
    /// <summary>ttl in seconds; 0 = the engine default.</summary>
    public ulong Ttl { get; set; }
    /// <summary>periodic_compaction_seconds; 0 = the engine default.</summary>
    public ulong PeriodicCompactionSeconds { get; set; }

    // ---- memtable

    public MemtableKind Memtable { get; set; }
    public bool InplaceUpdateSupport { get; set; }
    public int MaxWriteBufferNumber { get; set; } = 2;
    public int MinWriteBufferNumberToMerge { get; set; } = 1;

    public bool HasPrefix => Prefix != PrefixKind.None;

    /// <summary>The merge operator needs native's built-in C++ implementation (configured by name).</summary>
    public bool NativeBuiltinMerge => Merge is MergeKind.Max or MergeKind.BytesXor || (Merge == MergeKind.StringAppend && MergeDelimiter != ",");

    public CfSpec Clone()
    {
        var c = (CfSpec)MemberwiseClone();
        c.CompressionPerLevel = [.. CompressionPerLevel];
        return c;
    }

    public override string ToString()
    {
        var sb = new StringBuilder();
        sb.Append(Name).Append('[').Append(Compression).Append(",fv").Append(FormatVersion).Append(',').Append(Filter)
          .Append(",bs").Append(BlockSize).Append(",wb").Append(WriteBufferSize).Append(',').Append(DisableAutoCompactions ? "noauto" : "auto")
          .Append(',').Append(Merge);
        if (Merge == MergeKind.StringAppend && MergeDelimiter != ",") sb.Append('\'').Append(MergeDelimiter).Append('\'');
        if (EnableBlobFiles) sb.Append(",blob>=").Append(MinBlobSize).Append(',').Append(BlobCompression).Append(EnableBlobGc ? ",gc" + BlobGcAgeCutoff : "");
        if (UserTimestamps) sb.Append(",u64ts");
        if (Index != IndexType.BinarySearch) sb.Append(",idx=").Append(Index);
        if (PartitionFilters) sb.Append(",partfilters");
        if (Checksum != ChecksumType.Crc32c) sb.Append(',').Append(Checksum);
        if (DataBlockHashIndex) sb.Append(",datahash");
        if (BlockRestartInterval != 16 || IndexBlockRestartInterval != 1) sb.Append(",restart").Append(BlockRestartInterval).Append('/').Append(IndexBlockRestartInterval);
        if (CacheIndexAndFilterBlocks) sb.Append(",cacheidx");
        if (!WholeKeyFiltering) sb.Append(",nowkf");
        if (HasPrefix) sb.Append(",prefix=").Append(Prefix).Append(PrefixLength);
        if (MemtablePrefixBloomRatio > 0) sb.Append(",membloom").Append(MemtableWholeKeyFiltering ? "+wkf" : "");
        if (OptimizeFiltersForHits) sb.Append(",ofh");
        if (CompressionPerLevel.Count > 0) sb.Append(",perlevel=").Append(string.Join('/', CompressionPerLevel));
        if (BottommostCompression is { } bc) sb.Append(",bottom=").Append(bc);
        if (CompressionLevel != 32767) sb.Append(",clevel").Append(CompressionLevel);
        if (MaxDictBytes > 0) sb.Append(",dict").Append(MaxDictBytes).Append('/').Append(ZstdMaxTrainBytes).Append(BottommostDictionary ? "+bottom" : "");
        if (ParallelThreads > 1) sb.Append(",pthreads").Append(ParallelThreads);
        if (Style != CompactionStyle.Level) sb.Append(',').Append(Style);
        if (Style == CompactionStyle.Level)
        {
            if (!DynamicLevelBytes) sb.Append(",static-levels");
            if (Pri != CompactionPri.MinOverlappingRatio) sb.Append(',').Append(Pri);
        }
        if (Style == CompactionStyle.Universal)
            sb.Append(",ratio").Append(UniversalSizeRatio).Append(",amp").Append(UniversalMaxSizeAmplificationPercent).Append(UniversalAllowTrivialMove ? ",trivial" : "");
        if (Style == CompactionStyle.Fifo && FifoAllowCompaction) sb.Append(",fifo-compact");
        if (NumLevels != 7) sb.Append(",L").Append(NumLevels);
        if (TargetFileSizeBase != 64UL << 20) sb.Append(",tfs").Append(TargetFileSizeBase >> 10).Append('K');
        if (MaxBytesForLevelBase != 256UL << 20) sb.Append(",mbl").Append(MaxBytesForLevelBase >> 10).Append('K');
        if (Level0FileNumCompactionTrigger != 4) sb.Append(",l0t").Append(Level0FileNumCompactionTrigger);
        if (Ttl > 0) sb.Append(",ttl").Append(Ttl);
        if (PeriodicCompactionSeconds > 0) sb.Append(",periodic").Append(PeriodicCompactionSeconds);
        if (Memtable != MemtableKind.SkipList) sb.Append(',').Append(Memtable);
        if (InplaceUpdateSupport) sb.Append(",inplace");
        if (MaxWriteBufferNumber != 2 || MinWriteBufferNumberToMerge != 1) sb.Append(",mwb").Append(MaxWriteBufferNumber).Append('/').Append(MinWriteBufferNumberToMerge);
        return sb.Append(']').ToString();
    }
}

/// <summary>DB-wide settings (DBOptions) that affect the WAL, the write path and recovery.</summary>
public sealed class DbWideSpec
{
    public WalRecoveryMode WalRecoveryMode { get; set; } = WalRecoveryMode.PointInTimeRecovery;
    public ulong RecycleLogFileNum { get; set; }
    public bool ManualWalFlush { get; set; }
    /// <summary>None or Zstd.</summary>
    public CompressionType WalCompression { get; set; } = CompressionType.None;
    public bool TrackAndVerifyWals { get; set; }
    public bool TrackAndVerifyWalsInManifest { get; set; }
    public ulong MaxTotalWalSize { get; set; }
    public bool AvoidFlushDuringRecovery { get; set; }
    public bool AvoidFlushDuringShutdown { get; set; }
    public bool AtomicFlush { get; set; }
    public bool UnorderedWrite { get; set; }
    public bool EnablePipelinedWrite { get; set; }
    public bool AllowConcurrentMemtableWrite { get; set; } = true;
    public bool TwoWriteQueues { get; set; }
    /// <summary>The WAL lives in <see cref="WalDirFor"/> (wal_dir) instead of the DB directory.</summary>
    public bool SeparateWalDir { get; set; }
    public uint MaxSubcompactions { get; set; } = 1;
    public bool AllowIngestBehind { get; set; }

    public DbWideSpec Clone() => (DbWideSpec)MemberwiseClone();

    public static string WalDirFor(string dbPath) => dbPath.TrimEnd(Path.DirectorySeparatorChar) + ".wal";

    public override string ToString()
    {
        var parts = new List<string>();
        if (WalRecoveryMode != WalRecoveryMode.PointInTimeRecovery) parts.Add(WalRecoveryMode.ToString());
        if (RecycleLogFileNum > 0) parts.Add("recycle" + RecycleLogFileNum);
        if (ManualWalFlush) parts.Add("manual-wal-flush");
        if (WalCompression != CompressionType.None) parts.Add("wal-" + WalCompression);
        if (TrackAndVerifyWals) parts.Add("track-wals");
        if (TrackAndVerifyWalsInManifest) parts.Add("track-wals-in-manifest");
        if (MaxTotalWalSize > 0) parts.Add("max-wal" + (MaxTotalWalSize >> 10) + "K");
        if (AvoidFlushDuringRecovery) parts.Add("avoid-flush-recovery");
        if (AvoidFlushDuringShutdown) parts.Add("avoid-flush-shutdown");
        if (AtomicFlush) parts.Add("atomic-flush");
        if (UnorderedWrite) parts.Add("unordered");
        if (EnablePipelinedWrite) parts.Add("pipelined");
        if (!AllowConcurrentMemtableWrite) parts.Add("no-concurrent-memtable");
        if (TwoWriteQueues) parts.Add("two-queues");
        if (SeparateWalDir) parts.Add("wal-dir");
        if (MaxSubcompactions > 1) parts.Add("subcompactions" + MaxSubcompactions);
        if (AllowIngestBehind) parts.Add("allow-ingest-behind");
        return string.Join(",", parts);
    }
}

/// <summary>How a database is opened by either engine. Serialized as JSON for the child process.</summary>
public sealed class DbSpec
{
    /// <summary>Catalog of column families: every CF that exists (or may be created by a createcf op).</summary>
    public List<CfSpec> ColumnFamilies { get; set; } = [];

    /// <summary>When positive the DB is a TTL DB (native OpenWithTtl / managed TtlDatabase), default CF only.</summary>
    public int TtlSeconds { get; set; }

    /// <summary>
    /// Open with the options of the DB's latest OPTIONS file (native rocksdb_load_latest_options / managed
    /// <see cref="OptionsFile"/>) instead of building them from <see cref="ColumnFamilies"/>.
    /// </summary>
    public bool OptionsFromFile { get; set; }

    /// <summary>DB-wide options (WAL, write path, recovery).</summary>
    public DbWideSpec Db { get; set; } = new();

    /// <summary>
    /// Open as a pessimistic TransactionDB (WriteCommitted, allow_2pc): native rocksdb_transactiondb_open_column_families,
    /// managed <see cref="Utilities.Transactions.TransactionDatabase"/>. Enables the Txn* ops.
    /// </summary>
    public bool TransactionDb { get; set; }

    /// <summary>Open failures are reported ("OPENFAIL code message") instead of failing the run; see
    /// <see cref="Engines.ScriptRunner.OpenOrReport"/>.</summary>
    public bool ReportOpenFailure { get; set; }

    public CfSpec Cf(string name)
    {
        foreach (CfSpec cf in ColumnFamilies)
        {
            if (cf.Name == name) return cf;
        }
        throw new InvalidOperationException("column family not in spec catalog: " + name);
    }

    public DbSpec Clone()
    {
        var c = new DbSpec
        {
            TtlSeconds = TtlSeconds, OptionsFromFile = OptionsFromFile, Db = Db.Clone(), TransactionDb = TransactionDb,
            ReportOpenFailure = ReportOpenFailure,
        };
        foreach (CfSpec cf in ColumnFamilies) c.ColumnFamilies.Add(cf.Clone());
        return c;
    }

    public override string ToString()
    {
        string db = Db.ToString();
        return string.Join(" ", ColumnFamilies) + (TtlSeconds > 0 ? " ttl=" + TtlSeconds : "") + (OptionsFromFile ? " options-from-file" : "") +
            (db.Length > 0 ? " db[" + db + "]" : "") + (TransactionDb ? " txndb" : "");
    }

    private static readonly JsonSerializerOptions s_json = new() { WriteIndented = true };

    public string ToJson() => JsonSerializer.Serialize(this, s_json);

    public static DbSpec FromJson(string json) => JsonSerializer.Deserialize<DbSpec>(json, s_json)
        ?? throw new InvalidDataException("empty DbSpec");

    /// <summary>default + three CFs with deliberately different options and merge operators.</summary>
    public static DbSpec Standard(CompressionType compression, int formatVersion, FilterKind filter, ulong writeBufferSize, bool disableAutoCompactions)
    {
        var spec = new DbSpec();
        spec.ColumnFamilies.Add(new CfSpec
        {
            Name = "default", Compression = compression, FormatVersion = formatVersion, Filter = filter,
            WriteBufferSize = writeBufferSize, DisableAutoCompactions = disableAutoCompactions, Merge = MergeKind.StringAppend,
        });
        spec.ColumnFamilies.Add(new CfSpec
        {
            Name = "counters", Compression = CompressionType.Zstd, FormatVersion = 5, Filter = FilterKind.Ribbon, BlockSize = 1024,
            WriteBufferSize = writeBufferSize, DisableAutoCompactions = disableAutoCompactions, Merge = MergeKind.UInt64Add,
        });
        spec.ColumnFamilies.Add(new CfSpec
        {
            Name = "blobs", Compression = CompressionType.None, FormatVersion = 7, Filter = FilterKind.None, BlockSize = 16 * 1024,
            WriteBufferSize = Math.Max(writeBufferSize, 256 * 1024), DisableAutoCompactions = disableAutoCompactions, Merge = MergeKind.None,
        });
        spec.ColumnFamilies.Add(new CfSpec
        {
            Name = "snappy", Compression = CompressionType.Snappy, FormatVersion = 6, Filter = FilterKind.Bloom, BlockSize = 2048,
            WriteBufferSize = writeBufferSize, DisableAutoCompactions = disableAutoCompactions, Merge = MergeKind.StringAppend,
        });
        // Not created initially: exercised by createcf / dropcf ops.
        spec.ColumnFamilies.Add(new CfSpec
        {
            Name = "late", Compression = CompressionType.LZ4, FormatVersion = 7, Filter = FilterKind.Ribbon, WriteBufferSize = writeBufferSize,
            DisableAutoCompactions = disableAutoCompactions, Merge = MergeKind.StringAppend, Initial = false,
        });
        return spec;
    }
}
