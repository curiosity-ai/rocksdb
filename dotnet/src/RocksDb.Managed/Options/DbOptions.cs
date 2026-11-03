using RocksDb.Managed.Compression;
using RocksDb.Managed.Options;

namespace RocksDb.Managed;

/// <summary>
/// DB-wide options. Port of rocksdb::DBOptions (include/rocksdb/options.h) with upstream defaults; the OPTIONS-file
/// names and string forms follow options/db_options.cc. Options without a typed property are preserved in
/// <see cref="PersistedOptions{TSelf}.UnknownOptions"/> when read from a file.
/// </summary>
public sealed class DbOptions : PersistedOptions<DbOptions>
{
    // ---- not persisted in the OPTIONS file ----------------------------------------------------
    /// <summary>Directory for WAL files; empty means the DB directory. Not persisted.</summary>
    public string WalDir { get; set; } = "";
    /// <summary>Directory for info LOG files; empty means the DB directory. Not persisted.</summary>
    public string DbLogDir { get; set; } = "";
    /// <summary>File system the DB uses (rocksdb::DBOptions::env / file_system). Not persisted.</summary>
    public Env.IFileSystem FileSystem { get; set; } = Env.LocalFileSystem.Instance;
    /// <summary>Additional data paths (path, target size) for SST files. Not persisted.</summary>
    public List<(string Path, ulong TargetSize)> DbPaths { get; } = new();
    /// <summary>
    /// Statistics collector (rocksdb::DBOptions::statistics): tickers and histograms recorded by the DB; null (the default)
    /// disables statistics. One instance may be shared by several DBs. Not persisted.
    /// </summary>
    public Statistics? Statistics { get; set; }
    /// <summary>
    /// Receiver of the DB's informational log (rocksdb::DBOptions::info_log). Null (the default) makes the DB write its
    /// own <c>LOG</c> file in the DB directory (or <see cref="DbLogDir"/>), rolled per <see cref="MaxLogFileSize"/> /
    /// <see cref="LogFileTimeToRoll"/> and trimmed to <see cref="KeepLogFileNum"/> old files. A supplied logger is not
    /// disposed by the DB. Not persisted.
    /// </summary>
    public InfoLogger? InfoLog { get; set; }
    /// <summary>
    /// Event listeners notified of flushes, compactions, table file creation/deletion, write stalls and background
    /// errors (rocksdb::DBOptions::listeners). Not persisted.
    /// </summary>
    public List<EventListener> Listeners { get; } = new();
    /// <summary>
    /// Rate limiter for the DB's background writes (rocksdb::DBOptions::rate_limiter), e.g. a
    /// <see cref="Env.GenericRateLimiter"/>: flush outputs are charged at <see cref="Env.IoPriority.High"/>,
    /// compaction outputs at <see cref="Env.IoPriority.Low"/>; WAL and MANIFEST writes are not limited. May be shared
    /// by several DBs. Null (the default) means no limit. Only honored by file systems that apply
    /// <see cref="Env.WritableFileOptions.RateLimiter"/> (<see cref="Env.LocalFileSystem"/>). Not persisted.
    /// </summary>
    public Env.IRateLimiter? RateLimiter { get; set; }
    /// <summary>
    /// Tracks the DB's SST and blob files, enforces a space limit and rate limits file deletions
    /// (rocksdb::DBOptions::sst_file_manager; see <see cref="SstFileManager"/>). May be shared by several DBs. Null (the
    /// default) means no tracking. Not persisted.
    /// </summary>
    public SstFileManager? SstFileManager { get; set; }
    /// <summary>
    /// file_checksum_gen_factory: when set (e.g. <see cref="FileChecksumGenCrc32cFactory.Instance"/>), a whole-file
    /// checksum is computed for every table and blob file written by flushes and compactions and for ingested files, and
    /// recorded in the MANIFEST with its function name (native RocksDB reads and writes the same records); see
    /// <see cref="RocksDatabase.VerifyFileChecksums()"/> and <see cref="IngestExternalFileOptions.VerifyFileChecksum"/>.
    /// Null (the default) computes none. Persisted by name.
    /// </summary>
    public FileChecksumGenFactory? FileChecksumGenFactory { get; set; }
    /// <summary>
    /// Memtable memory budget shared by DBs and column families (rocksdb::DBOptions::write_buffer_manager): flushes are
    /// triggered when the budget is exceeded, writes may stall, and memtable memory may be charged to a block cache. Null
    /// (the default) gives the DB a private manager of <see cref="DbWriteBufferSize"/> bytes (no limit when 0), like
    /// SanitizeOptions. Not persisted.
    /// </summary>
    public WriteBufferManager? WriteBufferManager { get; set; }

    // ---- persisted ---------------------------------------------------------------------------
    public bool CreateIfMissing { get; set; }
    public bool CreateMissingColumnFamilies { get; set; }
    public bool ErrorIfExists { get; set; }
    public bool ParanoidChecks { get; set; } = true;
    public bool FlushVerifyMemtableCount { get; set; } = true;
    public bool CompactionVerifyRecordCount { get; set; } = true;
    public bool TrackAndVerifyWalsInManifest { get; set; }
    public bool VerifySstUniqueIdInManifest { get; set; } = true;
    /// <summary>-1 keeps all files open.</summary>
    public int MaxOpenFiles { get; set; } = -1;
    public int MaxFileOpeningThreads { get; set; } = 16;
    public ulong MaxTotalWalSize { get; set; }
    public bool UseFsync { get; set; }
    public ulong DeleteObsoleteFilesPeriodMicros { get; set; } = 6UL * 60 * 60 * 1000000;
    public int MaxBackgroundJobs { get; set; } = 2;
    public int MaxBackgroundCompactions { get; set; } = -1;
    public int MaxBackgroundFlushes { get; set; } = -1;
    public uint MaxSubcompactions { get; set; } = 1;
    public ulong MaxLogFileSize { get; set; }
    public ulong LogFileTimeToRoll { get; set; }
    public ulong KeepLogFileNum { get; set; } = 1000;
    public ulong RecycleLogFileNum { get; set; }
    public ulong MaxManifestFileSize { get; set; } = 1024UL * 1024 * 1024;
    public int TableCacheNumShardBits { get; set; } = 6;
    public ulong WalTtlSeconds { get; set; }
    public ulong WalSizeLimitMb { get; set; }
    public ulong ManifestPreallocationSize { get; set; } = 4 * 1024 * 1024;
    public bool AllowMmapReads { get; set; }
    public bool AllowMmapWrites { get; set; }
    public bool UseDirectReads { get; set; }
    public bool UseDirectIoForFlushAndCompaction { get; set; }
    public bool AllowFallocate { get; set; } = true;
    public bool IsFdCloseOnExec { get; set; } = true;
    public uint StatsDumpPeriodSec { get; set; } = 600;
    public uint StatsPersistPeriodSec { get; set; } = 600;

    /// <summary>
    /// max_compaction_trigger_wakeup_seconds: upper bound of the period of the background task that re-evaluates
    /// time-based and read-triggered compaction (TTL, periodic compaction, FIFO temperature changes,
    /// read_triggered_compaction_threshold) on DBs that receive no writes. The period is reduced to align with
    /// stats_dump_period_sec / stats_persist_period_sec and to a fifth of the smallest time-based compaction interval of
    /// any column family; it is at least 1 second (0 polls every second). Default 43200 (12 hours). Dynamically
    /// changeable through SetDBOptions.
    /// </summary>
    public ulong MaxCompactionTriggerWakeupSeconds { get; set; } = 43200;
    public bool PersistStatsToDisk { get; set; }
    public ulong StatsHistoryBufferSize { get; set; } = 1024 * 1024;
    public bool AdviseRandomOnOpen { get; set; } = true;
    /// <summary>
    /// db_write_buffer_size: total memtable memory of all column families of this DB before flushes are triggered, used for
    /// the DB's own <see cref="Managed.WriteBufferManager"/> when <see cref="WriteBufferManager"/> is null. 0 (default):
    /// no limit.
    /// </summary>
    public ulong DbWriteBufferSize { get; set; }
    /// <summary>
    /// lowest_used_cache_tier: <see cref="Cache.CacheTier.VolatileTier"/> keeps table blocks out of the block cache's
    /// secondary cache; the default uses every configured tier.
    /// </summary>
    public Cache.CacheTier LowestUsedCacheTier { get; set; } = Cache.CacheTier.NonVolatileBlockTier;
    public ulong CompactionReadaheadSize { get; set; } = 2 * 1024 * 1024;
    public ulong WritableFileMaxBufferSize { get; set; } = 1024 * 1024;
    public bool UseAdaptiveMutex { get; set; }
    public ulong BytesPerSync { get; set; }
    public ulong WalBytesPerSync { get; set; }
    public bool StrictBytesPerSync { get; set; }
    public bool EnableThreadTracking { get; set; }
    public ulong DelayedWriteRate { get; set; } = 16 * 1024 * 1024;
    public bool EnablePipelinedWrite { get; set; }
    /// <summary>
    /// unordered_write: trades higher write throughput for weaker snapshot guarantees. A write's sequence number becomes
    /// visible once its WAL write group finished, while its memtable insert (and the inserts of other writes with lower
    /// sequence numbers) may still be in flight. Writes issued after a snapshot stay invisible to it, and a writer reads
    /// its own write once the call returned, but a snapshot (and therefore Get, MultiGet and iterators at that snapshot)
    /// may change as pending writes with lower sequence numbers land. Requires <see cref="AllowConcurrentMemtableWrite"/>;
    /// incompatible with <see cref="EnablePipelinedWrite"/> and <c>max_successive_merges</c> &gt; 0. Use a WritePrepared
    /// TransactionDatabase with <see cref="TwoWriteQueues"/> to keep immutable snapshots.
    /// </summary>
    public bool UnorderedWrite { get; set; }
    public bool AllowConcurrentMemtableWrite { get; set; } = true;
    public bool EnableWriteThreadAdaptiveYield { get; set; } = true;
    public ulong MaxWriteBatchGroupSizeBytes { get; set; } = 1 << 20;
    public ulong WriteThreadMaxYieldUsec { get; set; } = 100;
    public ulong WriteThreadSlowYieldUsec { get; set; } = 3;
    public bool SkipStatsUpdateOnDbOpen { get; set; }
    public WalRecoveryMode WalRecoveryMode { get; set; } = WalRecoveryMode.PointInTimeRecovery;
    public bool Allow2Pc { get; set; }
    public bool DumpMallocStats { get; set; }
    public bool AvoidFlushDuringRecovery { get; set; }
    public bool AvoidFlushDuringShutdown { get; set; }
    public bool AllowIngestBehind { get; set; }
    /// <summary>
    /// two_write_queues: writes that only go to the WAL (2PC prepares of WriteCommitted transactions, commit and
    /// rollback markers of WritePrepared / WriteUnprepared transactions) use a second write queue instead of waiting
    /// behind memtable writes. WritePrepared / WriteUnprepared commits without prepare then take two writes (the data,
    /// then a WAL-less commit on the second queue). Default false.
    /// </summary>
    public bool TwoWriteQueues { get; set; }
    public bool ManualWalFlush { get; set; }
    public CompressionType WalCompression { get; set; } = CompressionType.None;
    public bool AtomicFlush { get; set; }
    public bool AvoidUnnecessaryBlockingIo { get; set; }
    public bool WriteDbIdToManifest { get; set; } = true;
    public bool WriteIdentityFile { get; set; } = true;
    public ulong LogReadaheadSize { get; set; }
    public bool BestEffortsRecovery { get; set; }
    /// <summary>
    /// Maximum number of automatic resume attempts after a retryable background I/O error (max_bgerror_resume_count);
    /// 0 or less disables automatic recovery (the DB then waits for <see cref="RocksDatabase.Resume"/>).
    /// </summary>
    public int MaxBgErrorResumeCount { get; set; } = int.MaxValue;
    /// <summary>Microseconds between automatic resume attempts (bgerror_resume_retry_interval).</summary>
    public ulong BgErrorResumeRetryInterval { get; set; } = 1000000;
    public bool AllowDataInErrors { get; set; }
    /// <summary>"__hostname__" is replaced by the host name when written to table properties.</summary>
    public string DbHostId { get; set; } = "__hostname__";
    public bool EnforceSingleDelContracts { get; set; } = true;
    public InfoLogLevel InfoLogLevel { get; set; } = InfoLogLevel.Info;

    private static readonly OptionTable<DbOptions> s_table = new OptionTable<DbOptions>()
        .Bool("create_if_missing", static o => o.CreateIfMissing, static (o, v) => o.CreateIfMissing = v)
        .Bool("create_missing_column_families", static o => o.CreateMissingColumnFamilies, static (o, v) => o.CreateMissingColumnFamilies = v)
        .Bool("error_if_exists", static o => o.ErrorIfExists, static (o, v) => o.ErrorIfExists = v)
        .Bool("paranoid_checks", static o => o.ParanoidChecks, static (o, v) => o.ParanoidChecks = v)
        .Bool("flush_verify_memtable_count", static o => o.FlushVerifyMemtableCount, static (o, v) => o.FlushVerifyMemtableCount = v)
        .Bool("compaction_verify_record_count", static o => o.CompactionVerifyRecordCount, static (o, v) => o.CompactionVerifyRecordCount = v)
        .Bool("track_and_verify_wals_in_manifest", static o => o.TrackAndVerifyWalsInManifest, static (o, v) => o.TrackAndVerifyWalsInManifest = v)
        .Bool("verify_sst_unique_id_in_manifest", static o => o.VerifySstUniqueIdInManifest, static (o, v) => o.VerifySstUniqueIdInManifest = v)
        .Int32("max_open_files", static o => o.MaxOpenFiles, static (o, v) => o.MaxOpenFiles = v)
        .Int32("max_file_opening_threads", static o => o.MaxFileOpeningThreads, static (o, v) => o.MaxFileOpeningThreads = v)
        .UInt64("max_total_wal_size", static o => o.MaxTotalWalSize, static (o, v) => o.MaxTotalWalSize = v)
        .Bool("use_fsync", static o => o.UseFsync, static (o, v) => o.UseFsync = v)
        .UInt64("delete_obsolete_files_period_micros", static o => o.DeleteObsoleteFilesPeriodMicros, static (o, v) => o.DeleteObsoleteFilesPeriodMicros = v)
        .Int32("max_background_jobs", static o => o.MaxBackgroundJobs, static (o, v) => o.MaxBackgroundJobs = v)
        .Int32("max_background_compactions", static o => o.MaxBackgroundCompactions, static (o, v) => o.MaxBackgroundCompactions = v)
        .Int32("max_background_flushes", static o => o.MaxBackgroundFlushes, static (o, v) => o.MaxBackgroundFlushes = v)
        .UInt32("max_subcompactions", static o => o.MaxSubcompactions, static (o, v) => o.MaxSubcompactions = v)
        .UInt64("max_log_file_size", static o => o.MaxLogFileSize, static (o, v) => o.MaxLogFileSize = v)
        .UInt64("log_file_time_to_roll", static o => o.LogFileTimeToRoll, static (o, v) => o.LogFileTimeToRoll = v)
        .UInt64("keep_log_file_num", static o => o.KeepLogFileNum, static (o, v) => o.KeepLogFileNum = v)
        .UInt64("recycle_log_file_num", static o => o.RecycleLogFileNum, static (o, v) => o.RecycleLogFileNum = v)
        .UInt64("max_manifest_file_size", static o => o.MaxManifestFileSize, static (o, v) => o.MaxManifestFileSize = v)
        .Int32("table_cache_numshardbits", static o => o.TableCacheNumShardBits, static (o, v) => o.TableCacheNumShardBits = v)
        .UInt64("WAL_ttl_seconds", static o => o.WalTtlSeconds, static (o, v) => o.WalTtlSeconds = v)
        .UInt64("WAL_size_limit_MB", static o => o.WalSizeLimitMb, static (o, v) => o.WalSizeLimitMb = v)
        .UInt64("manifest_preallocation_size", static o => o.ManifestPreallocationSize, static (o, v) => o.ManifestPreallocationSize = v)
        .Bool("allow_mmap_reads", static o => o.AllowMmapReads, static (o, v) => o.AllowMmapReads = v)
        .Bool("allow_mmap_writes", static o => o.AllowMmapWrites, static (o, v) => o.AllowMmapWrites = v)
        .Bool("use_direct_reads", static o => o.UseDirectReads, static (o, v) => o.UseDirectReads = v)
        .Bool("use_direct_io_for_flush_and_compaction", static o => o.UseDirectIoForFlushAndCompaction, static (o, v) => o.UseDirectIoForFlushAndCompaction = v)
        .Bool("allow_fallocate", static o => o.AllowFallocate, static (o, v) => o.AllowFallocate = v)
        .Bool("is_fd_close_on_exec", static o => o.IsFdCloseOnExec, static (o, v) => o.IsFdCloseOnExec = v)
        .UInt32("stats_dump_period_sec", static o => o.StatsDumpPeriodSec, static (o, v) => o.StatsDumpPeriodSec = v)
        .UInt32("stats_persist_period_sec", static o => o.StatsPersistPeriodSec, static (o, v) => o.StatsPersistPeriodSec = v)
        .UInt64("max_compaction_trigger_wakeup_seconds", static o => o.MaxCompactionTriggerWakeupSeconds,
            static (o, v) => o.MaxCompactionTriggerWakeupSeconds = v)
        .Bool("persist_stats_to_disk", static o => o.PersistStatsToDisk, static (o, v) => o.PersistStatsToDisk = v)
        .UInt64("stats_history_buffer_size", static o => o.StatsHistoryBufferSize, static (o, v) => o.StatsHistoryBufferSize = v)
        .Bool("advise_random_on_open", static o => o.AdviseRandomOnOpen, static (o, v) => o.AdviseRandomOnOpen = v)
        .UInt64("db_write_buffer_size", static o => o.DbWriteBufferSize, static (o, v) => o.DbWriteBufferSize = v)
        .UInt64("compaction_readahead_size", static o => o.CompactionReadaheadSize, static (o, v) => o.CompactionReadaheadSize = v)
        .UInt64("writable_file_max_buffer_size", static o => o.WritableFileMaxBufferSize, static (o, v) => o.WritableFileMaxBufferSize = v)
        .Bool("use_adaptive_mutex", static o => o.UseAdaptiveMutex, static (o, v) => o.UseAdaptiveMutex = v)
        .UInt64("bytes_per_sync", static o => o.BytesPerSync, static (o, v) => o.BytesPerSync = v)
        .UInt64("wal_bytes_per_sync", static o => o.WalBytesPerSync, static (o, v) => o.WalBytesPerSync = v)
        .Bool("strict_bytes_per_sync", static o => o.StrictBytesPerSync, static (o, v) => o.StrictBytesPerSync = v)
        .Bool("enable_thread_tracking", static o => o.EnableThreadTracking, static (o, v) => o.EnableThreadTracking = v)
        .UInt64("delayed_write_rate", static o => o.DelayedWriteRate, static (o, v) => o.DelayedWriteRate = v)
        .Bool("enable_pipelined_write", static o => o.EnablePipelinedWrite, static (o, v) => o.EnablePipelinedWrite = v)
        .Bool("unordered_write", static o => o.UnorderedWrite, static (o, v) => o.UnorderedWrite = v)
        .Bool("allow_concurrent_memtable_write", static o => o.AllowConcurrentMemtableWrite, static (o, v) => o.AllowConcurrentMemtableWrite = v)
        .Bool("enable_write_thread_adaptive_yield", static o => o.EnableWriteThreadAdaptiveYield, static (o, v) => o.EnableWriteThreadAdaptiveYield = v)
        .UInt64("max_write_batch_group_size_bytes", static o => o.MaxWriteBatchGroupSizeBytes, static (o, v) => o.MaxWriteBatchGroupSizeBytes = v)
        .UInt64("write_thread_max_yield_usec", static o => o.WriteThreadMaxYieldUsec, static (o, v) => o.WriteThreadMaxYieldUsec = v)
        .UInt64("write_thread_slow_yield_usec", static o => o.WriteThreadSlowYieldUsec, static (o, v) => o.WriteThreadSlowYieldUsec = v)
        .Bool("skip_stats_update_on_db_open", static o => o.SkipStatsUpdateOnDbOpen, static (o, v) => o.SkipStatsUpdateOnDbOpen = v)
        .Enum("wal_recovery_mode", OptionEnumMaps.WalRecoveryMode, static o => o.WalRecoveryMode, static (o, v) => o.WalRecoveryMode = v)
        .Bool("allow_2pc", static o => o.Allow2Pc, static (o, v) => o.Allow2Pc = v)
        .Bool("dump_malloc_stats", static o => o.DumpMallocStats, static (o, v) => o.DumpMallocStats = v)
        .Bool("avoid_flush_during_recovery", static o => o.AvoidFlushDuringRecovery, static (o, v) => o.AvoidFlushDuringRecovery = v)
        .Bool("avoid_flush_during_shutdown", static o => o.AvoidFlushDuringShutdown, static (o, v) => o.AvoidFlushDuringShutdown = v)
        .Bool("allow_ingest_behind", static o => o.AllowIngestBehind, static (o, v) => o.AllowIngestBehind = v)
        .Bool("two_write_queues", static o => o.TwoWriteQueues, static (o, v) => o.TwoWriteQueues = v)
        .Bool("manual_wal_flush", static o => o.ManualWalFlush, static (o, v) => o.ManualWalFlush = v)
        .Enum("wal_compression", OptionEnumMaps.Compression, static o => o.WalCompression, static (o, v) => o.WalCompression = v)
        .Bool("atomic_flush", static o => o.AtomicFlush, static (o, v) => o.AtomicFlush = v)
        .Bool("avoid_unnecessary_blocking_io", static o => o.AvoidUnnecessaryBlockingIo, static (o, v) => o.AvoidUnnecessaryBlockingIo = v)
        .Bool("write_dbid_to_manifest", static o => o.WriteDbIdToManifest, static (o, v) => o.WriteDbIdToManifest = v)
        .Bool("write_identity_file", static o => o.WriteIdentityFile, static (o, v) => o.WriteIdentityFile = v)
        .UInt64("log_readahead_size", static o => o.LogReadaheadSize, static (o, v) => o.LogReadaheadSize = v)
        .Bool("best_efforts_recovery", static o => o.BestEffortsRecovery, static (o, v) => o.BestEffortsRecovery = v)
        .Int32("max_bgerror_resume_count", static o => o.MaxBgErrorResumeCount, static (o, v) => o.MaxBgErrorResumeCount = v)
        .UInt64("bgerror_resume_retry_interval", static o => o.BgErrorResumeRetryInterval, static (o, v) => o.BgErrorResumeRetryInterval = v)
        .Bool("allow_data_in_errors", static o => o.AllowDataInErrors, static (o, v) => o.AllowDataInErrors = v)
        .String("db_host_id", static o => o.DbHostId, static (o, v) => o.DbHostId = v)
        // AsCustomSharedPtr<FileChecksumGenFactory> (kByNameAllowFromNull): the factory's name; only the built-in
        // FileChecksumGenCrc32cFactory is recreated from an OPTIONS file, other names load as null.
        .Add("file_checksum_gen_factory", static o => o.FileChecksumGenFactory?.Name ?? "",
            static (o, v) => o.FileChecksumGenFactory = OptionStrings.UnwrapId(v) == FileChecksumGenCrc32cFactory.ClassName ? FileChecksumGenCrc32cFactory.Instance : null)
        .Bool("enforce_single_del_contracts", static o => o.EnforceSingleDelContracts, static (o, v) => o.EnforceSingleDelContracts = v)
        .Enum("lowest_used_cache_tier", OptionEnumMaps.CacheTier, static o => o.LowestUsedCacheTier, static (o, v) => o.LowestUsedCacheTier = v)
        .Enum("info_log_level", OptionEnumMaps.InfoLogLevel, static o => o.InfoLogLevel, static (o, v) => o.InfoLogLevel = v);

    internal override OptionTable<DbOptions> OptionTable => s_table;

    /// <summary>
    /// DBImpl's seq_per_batch (not persisted): one sequence number per (sub-)batch instead of per key. Set by
    /// WritePrepared / WriteUnprepared transaction databases (TransactionDB::Open use_seq_per_batch).
    /// </summary>
    internal bool SeqPerBatch { get; set; }

    /// <summary>DBImpl's batch_per_txn (not persisted): false for WriteUnprepared (a transaction writes several batches).</summary>
    internal bool BatchPerTxn { get; set; } = true;

    /// <summary>
    /// Threads of the bottom-priority compaction pool (upstream <c>Env::SetBackgroundThreads(n, Env::Priority::BOTTOM)</c>;
    /// per DB here, not persisted). Default 0: no bottom pool. With a bottom pool, automatic compactions that output to
    /// the last (non-zero) level are forwarded to it, so compactions that relieve write stalls keep the regular
    /// threads. With <see cref="CompactionOptionsUniversal.ReduceFileLocking"/> (default true) only the newest input
    /// sorted run (the newest L0 file, or the input level's run) stays reserved while the compaction waits for a bottom
    /// thread, which then picks a fresh compaction to the last level; otherwise the whole compaction waits with all
    /// its input files reserved.
    /// </summary>
    public int BottomPriorityBackgroundThreads { get; set; }

    public DbOptions Clone()
    {
        var c = new DbOptions { WalDir = WalDir, DbLogDir = DbLogDir, FileSystem = FileSystem, Statistics = Statistics, InfoLog = InfoLog,
            RateLimiter = RateLimiter, SstFileManager = SstFileManager, FileChecksumGenFactory = FileChecksumGenFactory,
            WriteBufferManager = WriteBufferManager };
        c.DbPaths.AddRange(DbPaths);
        c.Listeners.AddRange(Listeners);
        c.CopyFrom(this);
        c.SeqPerBatch = SeqPerBatch;
        c.BatchPerTxn = BatchPerTxn;
        c.BottomPriorityBackgroundThreads = BottomPriorityBackgroundThreads;
        return c;
    }
}
