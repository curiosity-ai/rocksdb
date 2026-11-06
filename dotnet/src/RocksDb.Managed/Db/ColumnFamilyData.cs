using RocksDb.Managed.Table;

namespace RocksDb.Managed.Db;

/// <summary>
/// An immutable (switched-out) memtable waiting to be flushed. Port of the per-memtable flush bookkeeping of
/// MemTableList (db/memtable_list.h: flush_in_progress_, flush_completed_, next_log_number / GetNextLogNumber).
/// </summary>
internal sealed class ImmutableMemTable
{
    public ImmutableMemTable(MemTable mem, ulong nextLogNumber)
    {
        Mem = mem;
        NextLogNumber = nextLogNumber;
    }

    public MemTable Mem { get; }
    /// <summary>The WAL created when this memtable was switched out: once flushed, WALs below it hold no data of this CF.</summary>
    public ulong NextLogNumber { get; }
    /// <summary>Why the memtable was switched out (reported to listeners).</summary>
    public FlushReason FlushReason { get; init; }
    public bool FlushInProgress { get; set; }
    public bool FlushCompleted { get; set; }
    /// <summary>Blob files written by blob direct write while this memtable was mutable (registered by its flush).</summary>
    public Blob.BlobDirectWriteGeneration? BlobGeneration { get; init; }
}

/// <summary>
/// The read-side view of a column family: mutable memtable + immutable memtables + current version, all
/// referenced together so a reader sees a consistent set without holding the DB mutex. Port of SuperVersion
/// (db/column_family.h). Readers take a reference with <see cref="TryRef"/>; the last <see cref="Unref"/> must be
/// followed by <see cref="DbImpl.CleanupSuperVersion"/> (under the DB mutex).
/// </summary>
internal sealed class SuperVersion
{
    private int _refs = 1; // the column family's reference

    public SuperVersion(MemTable mem, MemTable[] imm, DbVersion current, ulong number, ColumnFamilyOptions mutableOptions)
    {
        Mem = mem;
        Imm = imm;
        Current = current;
        Number = number;
        MutableOptions = mutableOptions;
    }

    /// <summary>
    /// The column family's options when this super version was installed (SuperVersion::mutable_cf_options): an immutable
    /// snapshot, replaced (not modified) by SetOptions, so a reader sees one consistent set for its whole operation.
    /// </summary>
    public ColumnFamilyOptions MutableOptions { get; }

    public MemTable Mem { get; }
    /// <summary>Immutable memtables, newest first.</summary>
    public MemTable[] Imm { get; }
    public DbVersion Current { get; }
    public ulong Number { get; }

    /// <summary>Adds a reference unless the super version is already being cleaned up.</summary>
    public bool TryRef()
    {
        int r = Volatile.Read(ref _refs);
        while (r > 0)
        {
            int o = Interlocked.CompareExchange(ref _refs, r + 1, r);
            if (o == r) return true;
            r = o;
        }
        return false;
    }

    /// <summary>Returns true when the last reference was released (caller must clean up).</summary>
    public bool Unref() => Interlocked.Decrement(ref _refs) == 0;
}

/// <summary>
/// Per column family state. Port of ColumnFamilyData (db/column_family.h): options, comparators, memtables,
/// current version, super version, log number and flush scheduling flags. Mutable fields are guarded by the DB mutex
/// unless stated otherwise; <see cref="Mem"/> is additionally only replaced by the write-group leader.
/// </summary>
internal sealed class ColumnFamilyData
{
    public ColumnFamilyData(uint id, string name, ColumnFamilyOptions options, DbImpl db)
    {
        Id = id;
        Name = name;
        Options = options;
        Comparator ucmp = TableFactory.ResolveComparator(options);
        Icmp = ucmp.Kind == ComparatorKind.Bytewise ? InternalKeyComparator.Bytewise : new InternalKeyComparator(ucmp);
        TimestampSize = ucmp.TimestampSize;
        if (TimestampSize > 0 && !options.PersistUserDefinedTimestamps) ValidateMemtableOnlyTimestamps(ucmp, db.Options);
        // ColumnFamilyData::ValidateOptions / CheckConcurrentWritesSupported (db/column_family.cc).
        if (db.Options.AllowConcurrentMemtableWrite && options.InplaceUpdateSupport)
            ThrowHelper.InvalidArgument("In-place memtable updates (inplace_update_support) is not compatible with concurrent writes (allow_concurrent_memtable_write)");
        if (db.Options.UnorderedWrite && options.MaxSuccessiveMerges != 0)
            ThrowHelper.InvalidArgument("max_successive_merges > 0 is incompatible with unordered_write");
        // BlockBasedTableFactory::ValidateOptions (table/block_based/block_based_table_factory.cc).
        if (options.TableOptions.IndexBlockSearchType == IndexBlockSearchType.Interpolation && ucmp.Kind != ComparatorKind.Bytewise)
            ThrowHelper.InvalidArgument("Interpolation search requires BytewiseComparator");
        MergeOperator = options.MergeOperator;
        Format = TableFactory.CreateFormatOptions(options.TableOptions);
        OptionsSnapshot = options.Clone();
        _readerOptions = CreateReaderOptions(options, Format, db);
        NumLevels = Math.Max(1, options.NumLevels);
        MemOptions = new MemTableOptions
        {
            WriteBufferSize = (long)Math.Min(options.WriteBufferSize, (ulong)long.MaxValue),
            ArenaBlockSize = (int)Math.Min(options.ArenaBlockSize, int.MaxValue / 2),
            TrackNewestTimestampSize = options.PersistUserDefinedTimestamps ? 0 : TimestampSize,
            WriteBufferManager = db.WriteBufferManager,
        };
        MemOptions.ApplyColumnFamilyOptions(options);
        TableCache = new TableCache(db.SharedTableCache, db.FileSystem, db.DbPaths, ReaderOptions)
        {
            Statistics = db.Options.Statistics,
            VerifyUniqueIdInManifest = db.Options.VerifySstUniqueIdInManifest,
        };
        // Blob files live in cf_paths.front() (the first DB path); readers and the optional blob cache are per CF.
        Stats = new InternalStats(NumLevels);
        BlobSource = new Blob.BlobSource(db.FileSystem, db.DbPaths[0], id, options.BlobCache, db.Identity.DbId, db.Identity.SessionId, db.Options.Statistics,
            db.Options.MaxOpenFiles)
        {
            BlobFileReadHistogram = Stats.BlobFileReadHist,
        };
        CompactionState = new ColumnFamilyCompactionState(options, Icmp);
        TableCache.LevelStats = Stats;
        if (options.EnableBlobDirectWrite && options.EnableBlobFiles && !db.IsReadOnly)
        {
            // DBImpl::MaybeInitBlobDirectWriteColumnFamily.
            ValidateBlobDirectWrite(options, ucmp, db.Options);
            BlobDirectWrite = new Blob.BlobDirectWriteManager(db.FileSystem, db.DbPaths[0], db.NewPendingOutputFileNumber, options, db.Options.Statistics)
            {
                ColumnFamilyId = id,
                BlobFileSize = options.BlobFileSize,
                FileOptions = db.BlobFileWriteOptions(options),
                BlobFileReadHistogram = Stats.BlobFileReadHist,
            };
            BlobSource.DirectWrite = BlobDirectWrite;
        }
    }

    /// <summary>
    /// The blob direct write checks of ColumnFamilyData::ValidateOptions (db/column_family.cc); messages copied verbatim.
    /// </summary>
    private static void ValidateBlobDirectWrite(ColumnFamilyOptions options, Comparator ucmp, DbOptions dbOptions)
    {
        if (dbOptions.EnablePipelinedWrite) ThrowHelper.NotSupported("Blob direct write v1 does not support pipelined writes.");
        if (dbOptions.AllowConcurrentMemtableWrite) ThrowHelper.NotSupported("Blob direct write v1 does not support concurrent memtable writes.");
        if (dbOptions.UnorderedWrite) ThrowHelper.NotSupported("Blob direct write v1 does not support unordered writes.");
        if (dbOptions.TwoWriteQueues) ThrowHelper.NotSupported("Blob direct write v1 does not support two write queues.");
        if (options.ExperimentalMempurgeThreshold > 0.0) ThrowHelper.NotSupported("Blob direct write does not support MemPurge.");
        if (ucmp.TimestampSize > 0) ThrowHelper.NotSupported("Blob direct write does not support user-defined timestamps.");
    }

    /// <summary>
    /// Blob direct write of this column family (enable_blob_direct_write with enable_blob_files; null otherwise and for
    /// read-only DBs).
    /// </summary>
    public Blob.BlobDirectWriteManager? BlobDirectWrite { get; }

    /// <summary>
    /// The persist_user_defined_timestamps = false checks of ColumnFamilyData::ValidateOptions (db/column_family.cc):
    /// incompatible with atomic_flush and allow_concurrent_memtable_write, and only u64 timestamps (".u64ts"
    /// comparators) are supported. Messages are copied verbatim (including upstream's missing spaces).
    /// </summary>
    private static void ValidateMemtableOnlyTimestamps(Comparator ucmp, DbOptions dbOptions)
    {
        if (dbOptions.AtomicFlush)
        {
            ThrowHelper.NotSupported("Not persisting user-defined timestamps feature is not supportedin combination with atomic flush.");
        }
        if (dbOptions.AllowConcurrentMemtableWrite)
        {
            ThrowHelper.NotSupported("Not persisting user-defined timestamps feature is not supported in combination with concurrent memtable write.");
        }
        string name = ucmp.Name;
        if (name.Length <= BlockBasedTableReader.UdtComparatorSuffix.Length || !name.EndsWith(BlockBasedTableReader.UdtComparatorSuffix, StringComparison.Ordinal))
        {
            ThrowHelper.NotSupported("Not persisting user-defined timestampsfeature only support user-defined timestamps formatted as uint64_t.");
        }
    }

    /// <summary>persist_user_defined_timestamps = false with timestamps enabled (UDT in memtable only).</summary>
    public bool TimestampsInMemtableOnly => TimestampSize > 0 && !Options.PersistUserDefinedTimestamps;

    /// <summary>
    /// Set by a manual flush so the next flush of this CF is not postponed to retain timestamps
    /// (ColumnFamilyData::SetFlushSkipReschedule / GetAndClearFlushSkipReschedule). Guarded by the DB mutex.
    /// </summary>
    public bool FlushSkipReschedule { get; set; }

    /// <summary>
    /// Port of ColumnFamilyData::ShouldPostponeFlushToRetainUDT: with timestamps in memtable only and full_history_ts_low
    /// set, a flush of <paramref name="picked"/> is postponed while one of them holds a timestamp &gt;= full_history_ts_low.
    /// </summary>
    public bool ShouldPostponeFlushToRetainUdt(IReadOnlyList<MemTable> picked)
    {
        if (!TimestampsInMemtableOnly) return false;
        byte[]? low = FullHistoryTsLow;
        if (low is null) return false;
        foreach (MemTable m in picked)
        {
            byte[]? newest = m.GetNewestUdt();
            if (newest is null) continue;
            if (UserComparator.CompareTimestamp(newest, low) >= 0) return true;
        }
        return false;
    }

    /// <summary>Versions of this column family still referenced ("rocksdb.num-live-versions"). Guarded by the DB mutex.</summary>
    public int LiveVersions;

    /// <summary>Cumulative flush / compaction statistics ("rocksdb.cfstats").</summary>
    public InternalStats Stats { get; }

    public uint Id { get; }
    public string Name { get; }
    public ColumnFamilyOptions Options { get; }
    public InternalKeyComparator Icmp { get; }
    public Comparator UserComparator => Icmp.UserComparator;
    /// <summary>Size of the user-defined timestamp of every user key (0 = timestamps disabled). Fixed for the CF's lifetime.</summary>
    public int TimestampSize { get; }
    private byte[]? _fullHistoryTsLow;
    /// <summary>
    /// full_history_ts_low (ColumnFamilyData::GetFullHistoryTsLow): versions with a timestamp below it may be collapsed by
    /// compaction and reads below it are rejected. Null when unset. Only increases; written under the DB mutex (after the
    /// MANIFEST edit), read lock-free.
    /// </summary>
    public byte[]? FullHistoryTsLow
    {
        get => Volatile.Read(ref _fullHistoryTsLow);
        set => Volatile.Write(ref _fullHistoryTsLow, value);
    }
    public MergeOperator? MergeOperator { get; }
    /// <summary>Builder settings of the block-based table options; replaced (not modified) when SetOptions changes them.</summary>
    public TableFormatOptions Format { get => Volatile.Read(ref _format); private set => Volatile.Write(ref _format, value); }
    private TableFormatOptions _format = null!;

    /// <summary>Immutable snapshot of <see cref="Options"/> taken at creation and after each SetOptions (for super versions).</summary>
    public ColumnFamilyOptions OptionsSnapshot { get => Volatile.Read(ref _optionsSnapshot); private set => Volatile.Write(ref _optionsSnapshot, value); }
    private ColumnFamilyOptions _optionsSnapshot = null!;

    /// <summary>Refreshes the snapshot and the table builder settings after SetOptions changed <see cref="Options"/>. REQUIRES: DB mutex.</summary>
    /// <summary>
    /// Called (under the DB mutex) after SetOptions replaced the options: new tables are built with the new format and
    /// tables opened from now on use the new reader options (block cache, filter policy, cache/pin settings...); readers
    /// already in the table cache keep theirs, like upstream's readers keep the cloned table factory they were opened with.
    /// </summary>
    internal void OnOptionsChanged(DbImpl db)
    {
        Format = TableFactory.CreateFormatOptions(Options.TableOptions);
        OptionsSnapshot = Options.Clone();
        TableReaderOptions reader = CreateReaderOptions(Options, Format, db);
        Volatile.Write(ref _readerOptions, reader);
        TableCache.ReaderOptions = reader;
    }

    private static TableReaderOptions CreateReaderOptions(ColumnFamilyOptions options, TableFormatOptions format, DbImpl db)
    {
        TableReaderOptions r = TableFactory.CreateReaderOptions(options, format, db.GetDefaultBlockCache(options.TableOptions), db.Identity.SessionId);
        if (db.Options.Statistics is { } statistics) r = r with { Statistics = statistics.BlockCacheCounters };
        if (db.Options.RateLimiter is { } rateLimiter) r = r with { RateLimiter = rateLimiter };
        if (db.Options.LowestUsedCacheTier != Cache.CacheTier.NonVolatileBlockTier) r = r with { LowestUsedCacheTier = db.Options.LowestUsedCacheTier };
        r = r with { BlockCacheTracer = db.BlockCacheTracer, BlockRestartInterval = options.TableOptions.BlockRestartInterval };
        if (db.Options.AllowMmapReads) r = r with { AllowMmapReads = true };
        return r;
    }

    private TableReaderOptions _readerOptions;

    /// <summary>Reader options for tables opened now (replaced by SetOptions of reader-affecting table options).</summary>
    public TableReaderOptions ReaderOptions => Volatile.Read(ref _readerOptions);
    public MemTableOptions MemOptions { get; }
    public TableCache TableCache { get; }
    /// <summary>Blob file readers and the blob cache of this column family (BlobFileCache + BlobSource).</summary>
    public Blob.BlobSource BlobSource { get; }
    public int NumLevels { get; }
    /// <summary>Compaction picker, write-stall token and compaction queue flag (guarded by the DB mutex).</summary>
    public ColumnFamilyCompactionState CompactionState { get; }

    /// <summary>The public handle (created lazily).</summary>
    public ColumnFamily? Handle { get; set; }

    public MemTable Mem { get; set; } = null!;
    /// <summary>Immutable memtables, oldest first.</summary>
    public List<ImmutableMemTable> Imm { get; } = new();
    /// <summary>
    /// Flushed memtables kept for transaction conflict checking (MemTableList history,
    /// max_write_buffer_size_to_maintain), oldest first; each holds a reference. Guarded by the DB mutex.
    /// </summary>
    public List<MemTable> History { get; } = new();
    /// <summary>
    /// Flushed memtables that are still referenced (pinned by iterators or super versions): counted by
    /// "rocksdb.size-all-mem-tables" like upstream's MemTableList::current_memory_usage_. Guarded by the DB mutex.
    /// </summary>
    public HashSet<MemTable> FlushedPinnedMemTables { get; } = new(ReferenceEqualityComparer.Instance);
    public DbVersion Current { get; set; } = null!;
    private SuperVersion? _super;
    public SuperVersion Super => Volatile.Read(ref _super)!;
    public SuperVersion? SuperOrNull() => Volatile.Read(ref _super);
    public void SetSuper(SuperVersion sv) => Volatile.Write(ref _super, sv);
    /// <summary>Per-processor cached super version references for point reads (local_sv_).</summary>
    public SuperVersionCache LocalSuperVersions { get; } = new();

    /// <summary>WALs with numbers below this have been flushed for this CF (ColumnFamilyData::GetLogNumber).</summary>
    public ulong LogNumber { get; set; }
    public bool Dropped { get; set; }
    public bool FlushRunning { get; set; }
    public bool QueuedForFlush { get; set; }
    /// <summary>Next epoch number for L0 files (ColumnFamilyData::NewEpochNumber).</summary>
    public ulong NextEpochNumber { get; set; } = 1;

    public int NumNotFlushed
    {
        get
        {
            int n = 0;
            foreach (ImmutableMemTable m in Imm)
            {
                if (!m.FlushCompleted) n++;
            }
            return n;
        }
    }

    /// <summary>True if some immutable memtable is waiting for a flush to be picked (MemTableList::IsFlushPending).</summary>
    public bool HasPendingFlush
    {
        get
        {
            foreach (ImmutableMemTable m in Imm)
            {
                if (!m.FlushInProgress && !m.FlushCompleted) return true;
            }
            return false;
        }
    }

    public ulong NewEpochNumber() => NextEpochNumber++;

    /// <summary>Creates a fresh, referenced memtable (ColumnFamilyData::ConstructNewMemtable).</summary>
    public MemTable ConstructNewMemtable(ulong earliestSeq)
    {
        var m = new MemTable(Icmp, MemOptions, earliestSeq);
        m.Ref();
        return m;
    }

    /// <summary>Recomputes the next epoch number from the current files (recovery).</summary>
    public void RecoverEpochNumbers()
    {
        ulong max = 0;
        for (int l = 0; l < Current.Storage.NumLevels; l++)
        {
            foreach (FileMetaData f in Current.Storage.LevelFiles(l)) max = Math.Max(max, f.EpochNumber);
        }
        NextEpochNumber = max + 1;
    }
}
