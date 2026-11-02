namespace RocksDb.Managed;

/// <summary>
/// Options for read operations. Port of rocksdb::ReadOptions (include/rocksdb/options.h) with upstream defaults.
/// Not persisted.
/// </summary>
public sealed class ReadOptions
{
    /// <summary>Read as of this snapshot (null = the latest state). See <see cref="RocksDatabase.GetSnapshot"/>.</summary>
    public Snapshot? Snapshot { get; set; }
    /// <summary>Verify block checksums on reads (default true).</summary>
    public bool VerifyChecksums { get; set; } = true;
    /// <summary>Insert blocks read by this operation into the block cache (default true).</summary>
    public bool FillCache { get; set; } = true;
    /// <summary>Inclusive lower bound for iterators; null for none.</summary>
    public ReadOnlyMemory<byte>? IterateLowerBound { get; set; }
    /// <summary>Exclusive upper bound for iterators; null for none.</summary>
    public ReadOnlyMemory<byte>? IterateUpperBound { get; set; }
    /// <summary>
    /// Read timestamp for column families with user-defined timestamps (ReadOptions::timestamp): each key is read at its
    /// newest version whose timestamp is &lt;= this one. Required for such column families, rejected for others. Its
    /// length must equal the column family's timestamp size.
    /// </summary>
    public ReadOnlyMemory<byte>? Timestamp { get; set; }
    /// <summary>
    /// Lower timestamp bound for iterators (ReadOptions::iter_start_ts, inclusive): when set, the iterator returns every
    /// version with a timestamp in [IterStartTs, <see cref="Timestamp"/>], deletions included (see
    /// <see cref="Db.Iterators.Iterator.InternalKey"/>). Requires <see cref="Timestamp"/>.
    /// </summary>
    public ReadOnlyMemory<byte>? IterStartTs { get; set; }
    public ReadTier ReadTier { get; set; } = ReadTier.ReadAllTier;
    public bool IgnoreRangeDeletions { get; set; }
    public bool AsyncIo { get; set; }
    public bool OptimizeMultiGetForIo { get; set; } = true;
    /// <summary>Iterator readahead in bytes; 0 enables automatic readahead.</summary>
    public ulong ReadaheadSize { get; set; }
    public ulong MaxSkippableInternalKeys { get; set; }
    public bool Tailing { get; set; }
    public bool TotalOrderSeek { get; set; }
    public bool AutoPrefixMode { get; set; }
    public bool PrefixSameAsStart { get; set; }
    public bool PinData { get; set; }
    public bool AdaptiveReadahead { get; set; }
    public bool BackgroundPurgeOnIteratorCleanup { get; set; }
    public bool AutoReadaheadSize { get; set; } = true;
    public bool AllowUnpreparedValue { get; set; }
    /// <summary>Per-operation deadline; <see cref="TimeSpan.Zero"/> means none.</summary>
    public TimeSpan Deadline { get; set; }
    public TimeSpan IoTimeout { get; set; }
    public ulong ValueSizeSoftLimit { get; set; } = ulong.MaxValue;
}

/// <summary>Options for write operations. Port of rocksdb::WriteOptions with upstream defaults. Not persisted.</summary>
public sealed class WriteOptions
{
    /// <summary>fsync the WAL before acknowledging the write (default false).</summary>
    public bool Sync { get; set; }
    /// <summary>Skip the WAL (data may be lost on crash).</summary>
    public bool DisableWal { get; set; }
    public bool IgnoreMissingColumnFamilies { get; set; }
    /// <summary>Fail with Incomplete instead of waiting when writes are stalled.</summary>
    public bool NoSlowdown { get; set; }
    public bool LowPri { get; set; }
    public bool MemtableInsertHintPerBatch { get; set; }
    public ulong ProtectionBytesPerKey { get; set; }
}

/// <summary>Options for manual flush. Port of rocksdb::FlushOptions.</summary>
public sealed class FlushOptions
{
    /// <summary>Wait for the flush to finish (default true).</summary>
    public bool Wait { get; set; } = true;
    public bool AllowWriteStall { get; set; }
}

/// <summary>Options for manual compaction. Port of rocksdb::CompactRangeOptions.</summary>
public sealed class CompactRangeOptions
{
    public bool ExclusiveManualCompaction { get; set; }
    public bool ChangeLevel { get; set; }
    /// <summary>Target level when <see cref="ChangeLevel"/> is set; -1 = the minimum level that fits.</summary>
    public int TargetLevel { get; set; } = -1;
    public uint TargetPathId { get; set; }
    public BottommostLevelCompaction BottommostLevelCompaction { get; set; } = BottommostLevelCompaction.IfHaveCompactionFilter;
    public bool AllowWriteStall { get; set; }
    /// <summary>0 means use DbOptions.MaxSubcompactions.</summary>
    public uint MaxSubcompactions { get; set; }
    /// <summary>Overrides enable_blob_garbage_collection for this compaction (default: use the column family setting).</summary>
    public BlobGarbageCollectionPolicy BlobGarbageCollectionPolicy { get; set; } = BlobGarbageCollectionPolicy.UseDefault;
    /// <summary>Overrides blob_garbage_collection_age_cutoff; values outside [0, 1] (default -1) use the column family setting.</summary>
    public double BlobGarbageCollectionAgeCutoff { get; set; } = -1;
    /// <summary>
    /// When set, full_history_ts_low of the column family is first raised to this timestamp
    /// (CompactRangeOptions::full_history_ts_low); only allowed for a full-range compaction.
    /// </summary>
    public ReadOnlyMemory<byte>? FullHistoryTsLow { get; set; }
}
