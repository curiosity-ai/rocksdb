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
    /// <summary>Multi-line LSM statistics (kStats; the managed port returns the level summary plus compaction totals).</summary>
    public const string Stats = "rocksdb.stats";
    /// <summary>Column family statistics (kCFStats; same text as <see cref="Stats"/> in the managed port).</summary>
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

    /// <summary>"rocksdb.num-files-at-level&lt;level&gt;": number of files at <paramref name="level"/>.</summary>
    public static string NumFilesAtLevel(int level)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(level);
        return NumFilesAtLevelPrefix + level.ToString(CultureInfo.InvariantCulture);
    }
}
