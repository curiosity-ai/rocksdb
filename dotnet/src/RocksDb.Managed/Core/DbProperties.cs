using System.Globalization;

namespace RocksDb.Managed;

/// <summary>
/// Names of the DB properties the managed port answers through <see cref="RocksDatabase.GetProperty(string)"/> /
/// <see cref="ColumnFamily.GetProperty(string)"/> (port of the DB::Properties constants, include/rocksdb/db.h and
/// db/internal_stats.cc). Property names are an open set upstream, so the getters keep taking strings; unknown names
/// return null.
/// </summary>
public static class DbProperties
{
    /// <summary>"rocksdb.num-files-at-level&lt;N&gt;" prefix (kNumFilesAtLevelPrefix); see <see cref="NumFilesAtLevel"/>.</summary>
    public const string NumFilesAtLevelPrefix = "rocksdb.num-files-at-level";
    /// <summary>Multi-line statistics (kStats): <see cref="DbStats"/> + <see cref="CfStats"/> + the managed compaction totals.</summary>
    public const string Stats = "rocksdb.stats";
    /// <summary>Column family statistics (kCFStats): the per-level compaction table and flush / compaction totals; map form via GetMapProperty.</summary>
    public const string CfStats = "rocksdb.cfstats";
    /// <summary>Files and bytes per level (kLevelStats).</summary>
    public const string LevelStats = "rocksdb.levelstats";
    /// <summary>Number of immutable memtables not yet flushed (kNumImmutableMemTable).</summary>
    public const string NumImmutableMemTable = "rocksdb.num-immutable-mem-table";
    /// <summary>1 if a compaction is pending, else 0 (kCompactionPending).</summary>
    public const string CompactionPending = "rocksdb.compaction-pending";
    /// <summary>Number of currently running compactions (kNumRunningCompactions).</summary>
    public const string NumRunningCompactions = "rocksdb.num-running-compactions";
    /// <summary>Approximate size of the active memtable in bytes (kCurSizeActiveMemTable).</summary>
    public const string CurSizeActiveMemTable = "rocksdb.cur-size-active-mem-table";
    /// <summary>Entries in the active memtable (kNumEntriesActiveMemTable).</summary>
    public const string NumEntriesActiveMemTable = "rocksdb.num-entries-active-mem-table";
    /// <summary>Entries in the unflushed immutable memtables (kNumEntriesImmMemTables).</summary>
    public const string NumEntriesImmMemTables = "rocksdb.num-entries-imm-mem-tables";
    /// <summary>Delete entries in the active memtable (kNumDeletesActiveMemTable).</summary>
    public const string NumDeletesActiveMemTable = "rocksdb.num-deletes-active-mem-table";
    /// <summary>Estimated number of keys (kEstimateNumKeys).</summary>
    public const string EstimateNumKeys = "rocksdb.estimate-num-keys";
    /// <summary>Number of unreleased snapshots (kNumSnapshots).</summary>
    public const string NumSnapshots = "rocksdb.num-snapshots";
    /// <summary>Total size of all SST files of all versions (kTotalSstFilesSize).</summary>
    public const string TotalSstFilesSize = "rocksdb.total-sst-files-size";
    /// <summary>Total size of the SST files of the current version (kLiveSstFilesSize).</summary>
    public const string LiveSstFilesSize = "rocksdb.live-sst-files-size";
    /// <summary>The level L0 data is compacted to (kBaseLevel).</summary>
    public const string BaseLevel = "rocksdb.base-level";
    /// <summary>Estimated bytes compaction needs to rewrite to bring all levels under target size (kEstimatePendingCompactionBytes).</summary>
    public const string EstimatePendingCompactionBytes = "rocksdb.estimate-pending-compaction-bytes";
    /// <summary>Current delayed write rate, 0 when writes are not delayed (kActualDelayedWriteRate).</summary>
    public const string ActualDelayedWriteRate = "rocksdb.actual-delayed-write-rate";
    /// <summary>1 if writes are stopped, else 0 (kIsWriteStopped).</summary>
    public const string IsWriteStopped = "rocksdb.is-write-stopped";

    /// <summary>
    /// kCFFileHistogram: the per-level table read latency histograms ("File Read Latency Histogram By Level"), recorded
    /// when the DB has statistics.
    /// </summary>
    public const string CfFileHistogram = "rocksdb.cf-file-histogram";
    /// <summary><see cref="CfStats"/> without the file read latency histograms (kCFStatsNoFileHistogram).</summary>
    public const string CfStatsNoFileHistogram = "rocksdb.cfstats-no-file-histogram";
    /// <summary>DB-wide write / WAL / stall statistics (kDBStats); map form via GetMapProperty.</summary>
    public const string DbStats = "rocksdb.dbstats";
    /// <summary>The files of every level with key ranges and sequence numbers (kSSTables, Version::DebugString).</summary>
    public const string SsTables = "rocksdb.sstables";
    /// <summary>Estimated size of the live data (kEstimateLiveDataSize).</summary>
    public const string EstimateLiveDataSize = "rocksdb.estimate-live-data-size";
    /// <summary>Approximate size of the active and unflushed immutable memtables (kCurSizeAllMemTables).</summary>
    public const string CurSizeAllMemTables = "rocksdb.cur-size-all-mem-tables";
    /// <summary>Approximate size of all memtables (kSizeAllMemTables).</summary>
    public const string SizeAllMemTables = "rocksdb.size-all-mem-tables";
    /// <summary>
    /// Memory used by open table readers outside the block cache, e.g. pinned index and filter blocks
    /// (kEstimateTableReadersMem).
    /// </summary>
    public const string EstimateTableReadersMem = "rocksdb.estimate-table-readers-mem";
    /// <summary>Block cache capacity (kBlockCacheCapacity).</summary>
    public const string BlockCacheCapacity = "rocksdb.block-cache-capacity";
    /// <summary>Block cache usage (kBlockCacheUsage).</summary>
    public const string BlockCacheUsage = "rocksdb.block-cache-usage";
    /// <summary>Pinned block cache usage (kBlockCachePinnedUsage).</summary>
    public const string BlockCachePinnedUsage = "rocksdb.block-cache-pinned-usage";
    /// <summary>Block cache capacity / usage map (kBlockCacheEntryStats, map property; subset).</summary>
    public const string BlockCacheEntryStats = "rocksdb.block-cache-entry-stats";
    /// <summary>Number of blob files in the current version (kNumBlobFiles).</summary>
    public const string NumBlobFiles = "rocksdb.num-blob-files";
    /// <summary>Blob file count, size, garbage and space amplification of the current version (kBlobStats).</summary>
    public const string BlobStats = "rocksdb.blob-stats";
    /// <summary>Total size of the blob files of all live versions (kTotalBlobFileSize).</summary>
    public const string TotalBlobFileSize = "rocksdb.total-blob-file-size";
    /// <summary>Total size of the blob files of the current version (kLiveBlobFileSize).</summary>
    public const string LiveBlobFileSize = "rocksdb.live-blob-file-size";
    /// <summary>Total garbage in the blob files of the current version (kLiveBlobFileGarbageSize).</summary>
    public const string LiveBlobFileGarbageSize = "rocksdb.live-blob-file-garbage-size";
    /// <summary>Blob cache capacity (kBlobCacheCapacity).</summary>
    public const string BlobCacheCapacity = "rocksdb.blob-cache-capacity";
    /// <summary>Blob cache usage (kBlobCacheUsage).</summary>
    public const string BlobCacheUsage = "rocksdb.blob-cache-usage";
    /// <summary>Pinned blob cache usage (kBlobCachePinnedUsage).</summary>
    public const string BlobCachePinnedUsage = "rocksdb.blob-cache-pinned-usage";
    /// <summary>Number of live versions (kNumLiveVersions).</summary>
    public const string NumLiveVersions = "rocksdb.num-live-versions";
    /// <summary>Number of the current super version (kCurrentSuperVersionNumber).</summary>
    public const string CurrentSuperVersionNumber = "rocksdb.current-super-version-number";
    /// <summary>Unix time of the oldest snapshot, 0 if none (kOldestSnapshotTime).</summary>
    public const string OldestSnapshotTime = "rocksdb.oldest-snapshot-time";
    /// <summary>Sequence number of the oldest snapshot, 0 if none (kOldestSnapshotSequence).</summary>
    public const string OldestSnapshotSequence = "rocksdb.oldest-snapshot-sequence";
    /// <summary>Number of running flushes (kNumRunningFlushes).</summary>
    public const string NumRunningFlushes = "rocksdb.num-running-flushes";
    /// <summary>1 if a memtable flush is pending (kMemTableFlushPending).</summary>
    public const string MemTableFlushPending = "rocksdb.mem-table-flush-pending";
    /// <summary>Delete entries in the unflushed immutable memtables (kNumDeletesImmMemTables).</summary>
    public const string NumDeletesImmMemTables = "rocksdb.num-deletes-imm-mem-tables";
    /// <summary>Number of background errors reported since open (kBackgroundErrors).</summary>
    public const string BackgroundErrors = "rocksdb.background-errors";
    /// <summary>Aggregated table properties of all files (kAggregatedTableProperties); map form via GetMapProperty.</summary>
    public const string AggregatedTableProperties = "rocksdb.aggregated-table-properties";
    /// <summary>Prefix of the per-level aggregated table properties (kAggregatedTablePropertiesAtLevel).</summary>
    public const string AggregatedTablePropertiesAtLevelPrefix = "rocksdb.aggregated-table-properties-at-level";
    /// <summary>The Statistics text, if statistics are enabled (kOptionsStatistics).</summary>
    public const string OptionsStatistics = "rocksdb.options-statistics";

    /// <summary>
    /// "rocksdb.compression-ratio-at-level&lt;N&gt;" prefix (kCompressionRatioAtLevelPrefix): uncompressed (raw key +
    /// value) bytes over file bytes of the files at level N whose table properties were loaded, -1 if none.
    /// </summary>
    public const string CompressionRatioAtLevelPrefix = "rocksdb.compression-ratio-at-level";
    /// <summary>Write stall counts of the column family by cause (kCFWriteStallStats); map form via GetMapProperty.</summary>
    public const string CfWriteStallStats = "rocksdb.cf-write-stall-stats";
    /// <summary>DB-wide write stall counts (WriteBufferManager stops; kDBWriteStallStats); map form via GetMapProperty.</summary>
    public const string DbWriteStallStats = "rocksdb.db-write-stall-stats";
    /// <summary>
    /// Like <see cref="BlockCacheEntryStats"/> but returns the last collected stats unless they are more than 3 minutes
    /// old (kFastBlockCacheEntryStats); <see cref="BlockCacheEntryStats"/> collects again when they are older than 10 s.
    /// </summary>
    public const string FastBlockCacheEntryStats = "rocksdb.fast-block-cache-entry-stats";
    /// <summary>Flushed memtables kept in memory as history (max_write_buffer_size_to_maintain; kNumImmutableMemTableFlushed).</summary>
    public const string NumImmutableMemTableFlushed = "rocksdb.num-immutable-mem-table-flushed";
    /// <summary>Running remote (CompactionService) compactions (kNumRunningRemoteCompactions).</summary>
    public const string NumRunningRemoteCompactions = "rocksdb.num-running-remote-compactions";
    /// <summary>Running compactions on the bottom-priority pool (kNumRunningBottomCompactions).</summary>
    public const string NumRunningBottomCompactions = "rocksdb.num-running-bottom-compactions";
    /// <summary>Sorted runs (L0 files and non-empty levels) read by the running compactions (kNumRunningCompactionSortedRuns).</summary>
    public const string NumRunningCompactionSortedRuns = "rocksdb.num-running-compaction-sorted-runs";
    /// <summary>Outstanding AbortAllCompactions calls (kCompactionAbortCount); always 0, AbortAllCompactions is not ported.</summary>
    public const string CompactionAbortCount = "rocksdb.compaction-abort-count";
    /// <summary>Column families queued for an automatic compaction that is not scheduled yet (kNumUnscheduledCompactions).</summary>
    public const string NumUnscheduledCompactions = "rocksdb.num-unscheduled-compactions";
    /// <summary>1 if obsolete files are deleted, 0 while DisableFileDeletions is in effect (kIsFileDeletionsEnabled).</summary>
    public const string IsFileDeletionsEnabled = "rocksdb.is-file-deletions-enabled";
    /// <summary>The minimum WAL number to keep, from the MANIFEST (kMinLogNumberToKeep).</summary>
    public const string MinLogNumberToKeep = "rocksdb.min-log-number-to-keep";
    /// <summary>The smallest table file number being written; 2^64-1 when none (kMinObsoleteSstNumberToKeep).</summary>
    public const string MinObsoleteSstNumberToKeep = "rocksdb.min-obsolete-sst-number-to-keep";
    /// <summary>Total size of table files no longer referenced by any version, not yet deleted (kObsoleteSstFilesSize).</summary>
    public const string ObsoleteSstFilesSize = "rocksdb.obsolete-sst-files-size";
    /// <summary>
    /// Estimated unix time of the oldest key (kEstimateOldestKeyTime); only for FIFO compaction without
    /// allow_compaction (null otherwise or when unknown).
    /// </summary>
    public const string EstimateOldestKeyTime = "rocksdb.estimate-oldest-key-time";
    /// <summary>
    /// "rocksdb.live-sst-files-size-at-temperature&lt;T&gt;" prefix (kLiveSstFilesSizeAtTemperature): total size of the
    /// current version's files with the numeric <see cref="Temperature"/> T.
    /// </summary>
    public const string LiveSstFilesSizeAtTemperaturePrefix = "rocksdb.live-sst-files-size-at-temperature";

    /// <summary>"rocksdb.num-files-at-level&lt;level&gt;": number of files at <paramref name="level"/>.</summary>
    public static string NumFilesAtLevel(int level)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(level);
        return NumFilesAtLevelPrefix + level.ToString(CultureInfo.InvariantCulture);
    }
}
