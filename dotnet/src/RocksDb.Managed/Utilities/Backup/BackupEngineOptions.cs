using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Logging;

namespace RocksDb.Managed.Utilities.Backup;

/// <summary>
/// Port of BackupEngineOptions::ShareFilesNaming (include/rocksdb/utilities/backup_engine.h): naming of table and blob
/// files in the <c>shared_checksum</c> directory. Values are kept identical.
/// </summary>
[Flags]
public enum ShareFilesNaming : uint
{
    /// <summary><c>&lt;number&gt;_&lt;crc32c&gt;_&lt;size&gt;.sst</c> (legacy; also used for blob files and SSTs without a DB session id).</summary>
    LegacyCrc32cAndFileSize = 1U,

    /// <summary><c>&lt;number&gt;_s&lt;db_session_id&gt;.sst</c>.</summary>
    UseDbSessionId = 2U,

    /// <summary>Mask of the naming scheme (without flags).</summary>
    MaskNoNamingFlags = 0xffffU,

    /// <summary>Insert <c>_&lt;size&gt;</c> before the extension when the scheme does not already contain it.</summary>
    FlagIncludeFileSize = 1U << 31,
}

/// <summary>Port of rocksdb::BackupEngineOptions, with upstream defaults.</summary>
public sealed class BackupEngineOptions
{
    public BackupEngineOptions(string backupDir)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupDir);
        BackupDir = backupDir;
    }

    /// <summary>Where to keep the backup files. Must differ from the DB directory (e.g. <c>dbname + "/backups"</c> is fine).</summary>
    public string BackupDir { get; }

    /// <summary>File system holding the backup directory (backup_env); null uses the engine's file system.</summary>
    public IFileSystem? BackupFileSystem { get; set; }

    /// <summary>Share table and blob files between backups (incremental backups). Default true.</summary>
    public bool ShareTableFiles { get; set; } = true;

    /// <summary>Sync files and directories so a backup or restore survives a machine crash. Default true.</summary>
    public bool Sync { get; set; } = true;

    /// <summary>Delete every existing backup when the engine opens. Default false.</summary>
    public bool DestroyOldData { get; set; }

    /// <summary>Back up the live WAL files. Default true.</summary>
    public bool BackupLogFiles { get; set; } = true;

    /// <summary>
    /// Max bytes per second written while creating a backup (backup_rate_limit); 0 is unlimited. Ignored when
    /// <see cref="BackupRateLimiter"/> is set.
    /// </summary>
    public ulong BackupRateLimit { get; set; }

    /// <summary>Rate limiter for backup copies (backup_rate_limiter). Default null.</summary>
    public BackupRateLimiter? BackupRateLimiter { get; set; }

    /// <summary>Max bytes per second written while restoring (restore_rate_limit); 0 is unlimited.</summary>
    public ulong RestoreRateLimit { get; set; }

    /// <summary>Rate limiter for restore copies (restore_rate_limiter). Default null.</summary>
    public BackupRateLimiter? RestoreRateLimiter { get; set; }

    /// <summary>
    /// Store shared table and blob files under names that include a checksum or DB session id (shared_checksum/), so
    /// files with the same number from different DB histories never collide. Only used with
    /// <see cref="ShareTableFiles"/>. Setting it to false is deprecated upstream. Default true.
    /// </summary>
    public bool ShareFilesWithChecksum { get; set; } = true;

    /// <summary>Progress callbacks fire every this many copied bytes (callback_trigger_interval_size). Default 4 MiB.</summary>
    public ulong CallbackTriggerIntervalSize { get; set; } = 4 * 1024 * 1024;

    /// <summary>Read-only engines open at most this many of the latest valid backups. Ignored by writable engines.</summary>
    public int MaxValidBackupsToOpen { get; set; } = int.MaxValue;

    /// <summary>Naming of shared_checksum files. Default <c>UseDbSessionId | FlagIncludeFileSize</c>.</summary>
    public ShareFilesNaming ShareFilesWithChecksumNaming { get; set; } = ShareFilesNaming.UseDbSessionId | ShareFilesNaming.FlagIncludeFileSize;

    /// <summary>Meta file schema version to write: 1 (default, readable by all versions) or 2.</summary>
    public int SchemaVersion { get; set; } = 1;

    /// <summary>Buffer size for file copies (io_buffer_size); 0 uses 5 MiB like upstream's default.</summary>
    public int IoBufferSize { get; set; }

    /// <summary>Informational logger (info_log). Default null.</summary>
    public InfoLogger? InfoLog { get; set; }

    internal BackupEngineOptions Clone() => (BackupEngineOptions)MemberwiseClone();
}

/// <summary>Port of rocksdb::CreateBackupOptions.</summary>
public sealed class CreateBackupOptions
{
    /// <summary>Flush the memtables before the backup (always done with 2PC); otherwise the live WALs are backed up.</summary>
    public bool FlushBeforeBackup { get; set; }

    /// <summary>
    /// Called every <see cref="BackupEngineOptions.CallbackTriggerIntervalSize"/> copied bytes. An exception thrown by the
    /// callback aborts the backup (Aborted).
    /// </summary>
    public Action? ProgressCallback { get; set; }
}

/// <summary>Port of RestoreOptions::Mode.</summary>
public enum RestoreMode : uint
{
    /// <summary>Keep table files of the DB directory whose DB session id and size match the backup.</summary>
    KeepLatestDbSessionIdFiles = 1U,

    /// <summary>Keep files of the DB directory whose crc32c checksum matches the backup.</summary>
    VerifyChecksum = 2U,

    /// <summary>Delete every file of the destination and restore all files (default).</summary>
    PurgeAllFiles = 0xffffU,
}

/// <summary>Port of rocksdb::RestoreOptions.</summary>
public sealed class RestoreOptions
{
    /// <summary>
    /// Keep the WAL files of the destination WAL directory (and move archived WALs back), e.g. for in-memory DBs backed
    /// up with <see cref="BackupEngineOptions.BackupLogFiles"/> = false. Default false.
    /// </summary>
    public bool KeepLogFiles { get; set; }

    /// <summary>Restore mode. Default <see cref="RestoreMode.PurgeAllFiles"/>.</summary>
    public RestoreMode Mode { get; set; } = RestoreMode.PurgeAllFiles;
}

/// <summary>A file of a backup (BackupFileInfo / FileStorageInfo).</summary>
public sealed class BackupFileInfo
{
    /// <summary>Path relative to the backup directory, e.g. <c>shared_checksum/000007_s..._1234.sst</c>.</summary>
    public required string RelativeFilename { get; init; }
    /// <summary>The backup's private directory.</summary>
    public required string Directory { get; init; }
    public ulong FileNumber { get; init; }
    public FileType FileType { get; init; }
    public ulong Size { get; init; }
}

/// <summary>Port of rocksdb::BackupInfo.</summary>
public sealed class BackupInfo
{
    public uint BackupId { get; init; }
    /// <summary>Creation time, seconds since the Unix epoch.</summary>
    public long Timestamp { get; init; }
    /// <summary>Total size of the backed up files (shared files counted in every backup using them).</summary>
    public ulong Size { get; init; }
    public uint NumberFiles { get; init; }
    /// <summary>User metadata stored with the backup (empty if none).</summary>
    public required string AppMetadata { get; init; }
    /// <summary>File details, with include_file_details only.</summary>
    public IReadOnlyList<BackupFileInfo>? FileDetails { get; init; }
    /// <summary>The backup's private directory (name_for_open), with include_file_details only.</summary>
    public string? NameForOpen { get; init; }
}

/// <summary>
/// Rate limiting hook for backup and restore copies (the rocksdb::RateLimiter passed as backup_rate_limiter /
/// restore_rate_limiter). <see cref="Create"/> returns a token bucket refilled every 100 ms.
/// </summary>
public abstract class BackupRateLimiter
{
    /// <summary>Waits until <paramref name="bytes"/> may be transferred.</summary>
    public abstract ValueTask RequestAsync(long bytes, CancellationToken cancellationToken);

    /// <summary>Synchronous variant of <see cref="RequestAsync"/>.</summary>
    public virtual void Request(long bytes) => RequestAsync(bytes, CancellationToken.None).AsTask().GetAwaiter().GetResult();

    /// <summary>A limiter allowing <paramref name="bytesPerSecond"/> on average (NewGenericRateLimiter).</summary>
    public static BackupRateLimiter Create(ulong bytesPerSecond)
    {
        ArgumentOutOfRangeException.ThrowIfZero(bytesPerSecond);
        return new TokenBucket(bytesPerSecond);
    }

    private sealed class TokenBucket(ulong bytesPerSecond) : BackupRateLimiter
    {
        private readonly Lock _lock = new();
        private readonly long _start = System.Diagnostics.Stopwatch.GetTimestamp();
        private double _granted; // bytes granted so far

        private TimeSpan Reserve(long bytes)
        {
            lock (_lock)
            {
                // The time at which all bytes granted so far (including these) are within the rate.
                _granted += bytes;
                double due = _granted / bytesPerSecond;
                double now = System.Diagnostics.Stopwatch.GetElapsedTime(_start).TotalSeconds;
                if (due < now - 0.1) _granted = (now - 0.1) * bytesPerSecond; // no unbounded burst credit after idling
                return due > now ? TimeSpan.FromSeconds(due - now) : TimeSpan.Zero;
            }
        }

        public override async ValueTask RequestAsync(long bytes, CancellationToken cancellationToken)
        {
            TimeSpan wait = Reserve(bytes);
            if (wait > TimeSpan.Zero) await Task.Delay(wait, cancellationToken).ConfigureAwait(false);
        }

        public override void Request(long bytes)
        {
            TimeSpan wait = Reserve(bytes);
            if (wait > TimeSpan.Zero) Thread.Sleep(wait);
        }
    }
}
