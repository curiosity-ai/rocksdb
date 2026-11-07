namespace RocksDb.Managed;

/// <summary>
/// Options for read operations. Port of rocksdb::ReadOptions (include/rocksdb/options.h) with upstream defaults.
/// Not persisted.
/// </summary>
public sealed class ReadOptions
{
    /// <summary>Read as of this snapshot (null = the latest state). See <see cref="RocksDatabase.GetSnapshot"/>.</summary>
    public Snapshot? Snapshot { get; set; }

    /// <summary>A shallow copy (the snapshot and bounds are shared).</summary>
    internal ReadOptions Clone() => (ReadOptions)MemberwiseClone();
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
    /// <summary>
    /// ReadOptions::read_tier. <see cref="ReadTier.BlockCacheTier"/>: no file I/O, data that is not in the memtables or
    /// the block cache makes Get / MultiGet keys / iterator moves fail with <see cref="StatusCode.Incomplete"/>.
    /// <see cref="ReadTier.PersistedTier"/>: point lookups skip the memtables once data was written with
    /// <see cref="WriteOptions.DisableWal"/>; iterators are NotSupported. <see cref="ReadTier.MemtableTier"/>: iterators
    /// only see the memtables (point lookups are not restricted, like upstream).
    /// </summary>
    public ReadTier ReadTier { get; set; } = ReadTier.ReadAllTier;
    public bool IgnoreRangeDeletions { get; set; }
    public bool AsyncIo { get; set; }
    public bool OptimizeMultiGetForIo { get; set; } = true;
    /// <summary>Iterator readahead in bytes; 0 enables automatic readahead.</summary>
    public ulong ReadaheadSize { get; set; }
    public ulong MaxSkippableInternalKeys { get; set; }
    /// <summary>
    /// ReadOptions::tailing: the iterator sees data written after its creation (it reads at the latest sequence and
    /// renews its view after memtable switches, flushes and compactions on Seek / SeekToFirst / Next; the snapshot is
    /// ignored). SeekToLast, SeekForPrev and Prev are NotSupported, like upstream's ForwardIterator.
    /// </summary>
    public bool Tailing { get; set; }
    public bool TotalOrderSeek { get; set; }
    public bool AutoPrefixMode { get; set; }
    public bool PrefixSameAsStart { get; set; }
    /// <summary>
    /// read_index (EXPERIMENTAL): which index of block-based tables with a user-defined index serves the read
    /// (<see cref="Managed.ReadIndex"/>; see <see cref="BlockBasedTableOptions.UserDefinedIndexFactory"/>).
    /// </summary>
    public ReadIndex ReadIndex { get; set; }
    public bool PinData { get; set; }
    public bool AdaptiveReadahead { get; set; }
    public bool BackgroundPurgeOnIteratorCleanup { get; set; }
    public bool AutoReadaheadSize { get; set; } = true;
    public bool AllowUnpreparedValue { get; set; }
    /// <summary>
    /// ReadOptions::deadline: an absolute point in time, as the time elapsed since the Unix epoch (upstream compares it
    /// with SystemClock::NowMicros); <see cref="TimeSpan.Zero"/> (default) means none. Get, MultiGet and iterator moves
    /// that need a table file read once it has passed fail with <see cref="StatusCode.TimedOut"/> (reads served by the
    /// memtables or the block cache still succeed); MultiGet fails every key with TimedOut when it starts after the
    /// deadline. E.g. <c>DateTimeOffset.UtcNow.AddMilliseconds(5) - DateTimeOffset.UnixEpoch</c>.
    /// </summary>
    public TimeSpan Deadline { get; set; }
    /// <summary>
    /// ReadOptions::io_timeout: the timeout of each file read, passed to file systems that support it. Like upstream's
    /// PosixFileSystem, the built-in file systems do not enforce it.
    /// </summary>
    public TimeSpan IoTimeout { get; set; }
    /// <summary>
    /// ReadOptions::value_size_soft_limit: once the values a MultiGet has found exceed this many bytes, the keys not
    /// looked up yet fail with <see cref="StatusCode.Aborted"/> (the key whose value crosses the limit is returned).
    /// </summary>
    public ulong ValueSizeSoftLimit { get; set; } = ulong.MaxValue;
    /// <summary>
    /// rate_limiter_priority: charge the table block reads of this operation (Get, MultiGet, iterators) to
    /// <see cref="DbOptions.RateLimiter"/> at this priority. <see cref="Env.IoPriority.Total"/> (default) disables
    /// charging. Like upstream, the limiter only charges reads in <see cref="Env.RateLimiterMode.ReadsOnly"/> /
    /// <see cref="Env.RateLimiterMode.AllIo"/> mode, and small metadata reads (footer, properties at table open) are not charged.
    /// </summary>
    public Env.IoPriority RateLimiterPriority { get; set; } = Env.IoPriority.Total;
}

/// <summary>Options for write operations. Port of rocksdb::WriteOptions with upstream defaults. Not persisted.</summary>
public sealed class WriteOptions
{
    private bool _sync, _disableWal, _ignoreMissingColumnFamilies, _noSlowdown, _lowPri, _memtableInsertHintPerBatch;
    private ulong _protectionBytesPerKey;
    private int _memtableInsertParallelism = 1;
    private readonly bool _readOnly;

    public WriteOptions()
    {
    }

    private WriteOptions(bool sync)
    {
        _sync = sync;
        _readOnly = true;
    }

    /// <summary>
    /// Shared read-only options with <see cref="Sync"/> = true (the WAL is fsynced before the write is acknowledged),
    /// e.g. <c>db.Write(batch, WriteOptions.SyncWrites)</c>. Setting a property of this instance throws
    /// <see cref="InvalidOperationException"/>.
    /// </summary>
    public static WriteOptions SyncWrites { get; } = new(sync: true);

    /// <summary>True for the shared <see cref="SyncWrites"/> instance, whose properties cannot be changed.</summary>
    public bool IsReadOnly => _readOnly;

    /// <summary>fsync the WAL before acknowledging the write (default false).</summary>
    public bool Sync { get => _sync; set => _sync = Settable(value); }
    /// <summary>Skip the WAL (data may be lost on crash).</summary>
    public bool DisableWal { get => _disableWal; set => _disableWal = Settable(value); }
    public bool IgnoreMissingColumnFamilies { get => _ignoreMissingColumnFamilies; set => _ignoreMissingColumnFamilies = Settable(value); }
    /// <summary>Fail with Incomplete instead of waiting when writes are stalled.</summary>
    public bool NoSlowdown { get => _noSlowdown; set => _noSlowdown = Settable(value); }
    public bool LowPri { get => _lowPri; set => _lowPri = Settable(value); }
    public bool MemtableInsertHintPerBatch { get => _memtableInsertHintPerBatch; set => _memtableInsertHintPerBatch = Settable(value); }
    public ulong ProtectionBytesPerKey { get => _protectionBytesPerKey; set => _protectionBytesPerKey = Settable(value); }

    /// <summary>
    /// Managed extension (no upstream counterpart): the most threads that insert this write's batch into the memtable.
    /// Default 1, the upstream behaviour (one thread per batch). With a larger value a batch of at least 256 entries is
    /// cut at record boundaries into up to this many ranges of at least 128 entries, inserted concurrently by the
    /// writing thread and thread-pool threads. Sequence numbers, the WAL record and visibility are unchanged: readers
    /// see the whole batch only once every range is in the memtable. Useful for very large batches on a write path that
    /// has spare cores, e.g. a replica applying its primary's merged WAL batches.
    /// <para>The insert stays serial when <see cref="DbOptions.AllowConcurrentMemtableWrite"/> is off, with pipelined or
    /// unordered writes, transaction writes, per-key protection, blob direct write, a batch holding merge operands,
    /// blob indexes, timed puts or 2PC markers, and when the write is committed in one group with other writers (the
    /// writers of a group already insert their batches in parallel).</para>
    /// </summary>
    public int MemtableInsertParallelism
    {
        get => _memtableInsertParallelism;
        set
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(value, 1);
            _memtableInsertParallelism = Settable(value);
        }
    }

    private T Settable<T>(T value)
    {
        if (_readOnly) throw new InvalidOperationException("WriteOptions.SyncWrites is shared and read-only; create a new WriteOptions instead.");
        return value;
    }
}

/// <summary>Options for manual flush. Port of rocksdb::FlushOptions.</summary>
public sealed class FlushOptions
{
    /// <summary>Wait for the flush to finish (default true).</summary>
    public bool Wait { get; set; } = true;
    public bool AllowWriteStall { get; set; }
}

/// <summary>
/// Options for <see cref="ColumnFamily.CompactFiles(CompactFilesOptions, IEnumerable{string}, int)"/>. Port of
/// rocksdb::CompactionOptions plus the output path id argument of DB::CompactFiles.
/// </summary>
public sealed class CompactFilesOptions
{
    /// <summary>Size at which an output file is cut (output_file_size_limit). Default: unlimited, a single output file.</summary>
    public ulong OutputFileSizeLimit { get; set; } = ulong.MaxValue;

    /// <summary>
    /// Compression of the outputs (CompactionOptions::compression, deprecated upstream because the compression options
    /// still come from the column family). Null (default) = the column family's compression for the output level.
    /// </summary>
    public Compression.CompressionType? Compression { get; set; }

    /// <summary>Overrides DbOptions.MaxSubcompactions for this compaction when &gt; 0.</summary>
    public uint MaxSubcompactions { get; set; }

    /// <summary>Temperature of the output files; Unknown (default) = last_level_temperature / default_write_temperature.</summary>
    public Temperature OutputTemperatureOverride { get; set; } = Temperature.Unknown;

    /// <summary>
    /// Move the input files to the output level without rewriting them when that is possible (one input level, no
    /// overlap in the output level, matching compression). Default false: the files are always rewritten.
    /// </summary>
    public bool AllowTrivialMove { get; set; }

    /// <summary>Index into the DB paths for the outputs; -1 (default) = automatic, only supported with a single path.</summary>
    public int OutputPathId { get; set; } = -1;
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

    /// <summary>The trim_ts of CompactRangeInternal (DB::OpenAndTrimHistory only): data newer than it is removed.</summary>
    internal byte[]? TrimTs { get; init; }
    /// <summary>Overrides blob_garbage_collection_age_cutoff; values outside [0, 1] (default -1) use the column family setting.</summary>
    public double BlobGarbageCollectionAgeCutoff { get; set; } = -1;
    /// <summary>
    /// When set, full_history_ts_low of the column family is first raised to this timestamp
    /// (CompactRangeOptions::full_history_ts_low); only allowed for a full-range compaction.
    /// </summary>
    public ReadOnlyMemory<byte>? FullHistoryTsLow { get; set; }
}
