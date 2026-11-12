using System.Buffers;
using System.Globalization;
using System.Text;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Utilities.Backup;

/// <summary>
/// Read-only access to a backup directory: list, verify and restore backups. Port of rocksdb::BackupEngineReadOnly
/// (include/rocksdb/utilities/backup_engine.h, utilities/backup/backup_engine.cc). The directory layout and the meta
/// file format are identical to native RocksDB, so backups made by either side can be restored by the other.
/// <para>
/// Thread-safe: operations are serialized. Files are copied one at a time (max_background_operations is not ported).
/// </para>
/// </summary>
public class BackupEngineReadOnly : IDisposable
{
    internal const string PrivateDirName = "private";
    internal const string MetaDirName = "meta";
    internal const string SharedDirName = "shared";
    internal const string SharedChecksumDirName = "shared_checksum";
    private const int MaxAppMetaSize = 1024 * 1024; // 1MB
    private const int DefaultIoBufferSize = 5 * 1024 * 1024;

    private protected readonly BackupEngineOptions _options;
    private protected readonly IFileSystem _backupFs;
    private protected readonly IFileSystem _dbFs;
    private protected readonly SemaphoreSlim _lock = new(1, 1);
    private protected readonly bool _readOnly;

    // backups_, corrupt_backups_, backuped_file_infos_ (all guarded by _lock)
    private protected readonly SortedDictionary<uint, BackupMeta> _backups = new();
    private protected readonly SortedDictionary<uint, (RocksDbException Error, BackupMeta Meta)> _corruptBackups = new();
    private protected readonly Dictionary<string, BackupFileEntry> _backupedFileInfos = new(StringComparer.Ordinal);
    // What the meta file of each backup of _corruptBackups names (see FilesNamedByUnloadedBackup). Guarded by _lock.
    private protected readonly Dictionary<uint, HashSet<string>> _unloadedBackupFiles = new();
    private readonly HashSet<string> _reportedIgnoredFields = new(StringComparer.Ordinal);
    // Engines are locked in this order when a restore needs several (alternate_dirs).
    private static long s_nextLockOrder;
    private readonly long _lockOrder = Interlocked.Increment(ref s_nextLockOrder);
    private protected uint _latestBackupId;
    private protected uint _latestValidBackupId;
    private protected bool _mightNeedGarbageCollect;
    private protected volatile bool _stopBackup;
    private int _disposed;

    private protected BackupEngineReadOnly(BackupEngineOptions options, IFileSystem fileSystem, bool readOnly)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(fileSystem);
        _options = options.Clone();
        _dbFs = fileSystem;
        _backupFs = options.BackupFileSystem ?? fileSystem;
        _readOnly = readOnly;
        if (_options.BackupRateLimiter is null && _options.BackupRateLimit > 0) _options.BackupRateLimiter = BackupRateLimiter.Create(_options.BackupRateLimit);
        if (_options.RestoreRateLimiter is null && _options.RestoreRateLimit > 0) _options.RestoreRateLimiter = BackupRateLimiter.Create(_options.RestoreRateLimit);
    }

    /// <summary>Opens the backups in <paramref name="options"/>.BackupDir on the local file system (BackupEngineReadOnly::Open).</summary>
    public static BackupEngineReadOnly Open(BackupEngineOptions options) => Open(options, LocalFileSystem.Instance);

    /// <summary>
    /// Opens the backups read-only. <paramref name="fileSystem"/> is the file system restores write to (and holds the
    /// backup directory unless <see cref="BackupEngineOptions.BackupFileSystem"/> is set). Corrupt backups are skipped
    /// (see <see cref="GetCorruptedBackups"/>); a missing meta directory fails with NotFound.
    /// </summary>
    public static BackupEngineReadOnly Open(BackupEngineOptions options, IFileSystem fileSystem)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (options.DestroyOldData) ThrowHelper.InvalidArgument("Can't destroy old data with ReadOnly BackupEngine");
        var engine = new BackupEngineReadOnly(options, fileSystem, readOnly: true);
        engine.Initialize();
        return engine;
    }

    // -----------------------------------------------------------------------------------------------------
    // Paths

    private protected string BackupDir => _options.BackupDir.TrimEnd('/');

    private protected string GetAbsolutePath(string relativePath) => BackupDir + "/" + relativePath;

    private protected static string GetPrivateFileRel(uint backupId, bool tmp, string file) =>
        PrivateDirName + "/" + backupId.ToString(CultureInfo.InvariantCulture) + (tmp ? ".tmp" : "") + "/" + file;

    private protected static string GetSharedFileRel(string file, bool tmp) => SharedDirName + "/" + (tmp ? "." : "") + file + (tmp ? ".tmp" : "");

    private protected static string GetSharedFileWithChecksumRel(string file, bool tmp) =>
        SharedChecksumDirName + "/" + (tmp ? "." : "") + file + (tmp ? ".tmp" : "");

    private protected string GetBackupMetaFile(uint backupId, bool tmp) =>
        GetAbsolutePath(MetaDirName) + "/" + (tmp ? "." : "") + backupId.ToString(CultureInfo.InvariantCulture) + (tmp ? ".tmp" : "");

    private protected string PrivateDir(uint backupId) => GetAbsolutePath(PrivateDirName + "/" + backupId.ToString(CultureInfo.InvariantCulture));

    /// <summary>GetFileFromChecksumFile: <c>&lt;number&gt;_&lt;anything&gt;.&lt;ext&gt;</c> -> <c>&lt;number&gt;.&lt;ext&gt;</c>.</summary>
    internal static string GetFileFromChecksumFile(string file)
    {
        int firstUnderscore = file.IndexOf('_');
        if (firstUnderscore < 0) return file;
        int lastDot = file.LastIndexOf('.');
        // No extension after the '_' (upstream erases with a wrapped-around length): not a shared_checksum name, and
        // not a DB file name either, which is what the callers report.
        if (lastDot < firstUnderscore) return file;
        return file.Remove(firstUnderscore, lastDot - firstUnderscore);
    }

    /// <summary>ChecksumInt32ToHex: the big-endian hex form used in messages.</summary>
    internal static string ChecksumToHex(uint crc) => crc.ToString("x8", CultureInfo.InvariantCulture);

    // -----------------------------------------------------------------------------------------------------
    // Initialize

    /// <summary>Port of BackupEngineImpl::Initialize: creates the directories (writable engines) and loads the meta files.</summary>
    private protected void Initialize()
    {
        string metaPath = GetAbsolutePath(MetaDirName);
        if (!_readOnly)
        {
            // We might need to clean up from previous crash or I/O errors.
            _mightNeedGarbageCollect = true;
            _options.MaxValidBackupsToOpen = int.MaxValue;
            var dirs = new List<string> { BackupDir };
            if (_options.ShareTableFiles) dirs.Add(GetAbsolutePath(_options.ShareFilesWithChecksum ? SharedChecksumDirName : SharedDirName));
            dirs.Add(GetAbsolutePath(PrivateDirName));
            dirs.Add(metaPath);
            foreach (string d in dirs) _backupFs.CreateDirectoryIfMissing(d);
        }
        List<string>? metaFiles = TryGetChildren(_backupFs, metaPath);
        if (metaFiles is null) throw new RocksDbException(StatusCode.NotFound, "NotFound: " + metaPath + " is missing");
        foreach (string file in metaFiles)
        {
            if (!uint.TryParse(file, NumberStyles.None, CultureInfo.InvariantCulture, out uint id) || id == 0 ||
                file != id.ToString(CultureInfo.InvariantCulture))
            {
                // Invalid file name, will be deleted with auto-GC when user initiates an append or write operation.
                LogInfo("Skipping unrecognized meta file " + file);
                continue;
            }
            _backups[id] = new BackupMeta(GetBackupMetaFile(id, false), GetBackupMetaFile(id, true), _backupedFileInfos);
        }
        _latestBackupId = 0;
        _latestValidBackupId = 0;
        if (_options.DestroyOldData)
        {
            LogInfo("Backup Engine started with destroy_old_data == true, deleting all backups");
            PurgeOldBackupsLocked(0);
            GarbageCollectLocked();
            return;
        }
        var absPathToSize = new Dictionary<string, ulong>(StringComparer.Ordinal);
        ReadChildFileCurrentSizes(GetAbsolutePath(SharedDirName), absPathToSize);
        ReadChildFileCurrentSizes(GetAbsolutePath(SharedChecksumDirName), absPathToSize);
        int validToOpen = _options.MaxValidBackupsToOpen;
        var ids = new List<uint>(_backups.Keys);
        ids.Reverse();
        foreach (uint id in ids)
        {
            if (_latestBackupId == 0) _latestBackupId = id;
            if (validToOpen == 0) break;
            BackupMeta meta = _backups[id];
            try
            {
                ReadChildFileCurrentSizes(PrivateDir(id), absPathToSize);
                meta.LoadFromFile(_backupFs, BackupDir, absPathToSize, _reportedIgnoredFields, LogWarn);
                if (_latestValidBackupId == 0) _latestValidBackupId = id;
                validToOpen--;
            }
            catch (RocksDbException e) when (e.Code is StatusCode.Corruption or StatusCode.NotSupported)
            {
                LogInfo("Backup " + id + " corrupted -- " + e.Message);
                _corruptBackups[id] = (e, meta);
                _unloadedBackupFiles[id] = BackupMeta.FilesNamedByUnloadedBackup(_backupFs, meta.MetaFilename);
                _backups.Remove(id);
            }
        }
        // Erase the backups before max_valid_backups_to_open.
        if (_options.MaxValidBackupsToOpen > 0)
        {
            while (_backups.Count > _options.MaxValidBackupsToOpen)
            {
                uint first = FirstKey(_backups);
                _backups.Remove(first);
            }
        }
        LogInfo("Latest backup is " + _latestBackupId);
        LogInfo("Latest valid backup is " + _latestValidBackupId);
    }

    private static uint FirstKey<T>(SortedDictionary<uint, T> d)
    {
        foreach (uint k in d.Keys) return k;
        return 0;
    }

    private protected static List<string>? TryGetChildren(IFileSystem fs, string dir)
    {
        try
        {
            if (!fs.DirectoryExistsSafe(dir)) return null;
            return new List<string>(fs.GetChildren(dir));
        }
        catch (RocksDbException e) when (e.Code == StatusCode.NotFound || e.SubCode == StatusSubCode.PathNotFound)
        {
            return null;
        }
        catch (DirectoryNotFoundException)
        {
            return null;
        }
    }

    /// <summary>ReadChildFileCurrentSizes: absolute path -> size of every file of <paramref name="dir"/> (missing dir: nothing).</summary>
    private protected void ReadChildFileCurrentSizes(string dir, Dictionary<string, ulong> result)
    {
        List<string>? children = TryGetChildren(_backupFs, dir);
        if (children is null) return;
        foreach (string c in children)
        {
            string p = dir + "/" + c;
            if (_backupFs.DirectoryExistsSafe(p)) continue;
            result[p] = (ulong)_backupFs.GetFileSize(p);
        }
    }

    private protected void LogInfo(string message) => _options.InfoLog?.Log(InfoLogLevel.Info, message);

    private protected void LogWarn(string message) => _options.InfoLog?.Log(InfoLogLevel.Warn, message);

    private protected void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

    // -----------------------------------------------------------------------------------------------------
    // Info

    /// <summary>The valid backups, oldest first (BackupEngine::GetBackupInfo).</summary>
    public List<BackupInfo> GetBackupInfo() => GetBackupInfo(includeFileDetails: false);

    /// <summary>The valid backups, oldest first; with <paramref name="includeFileDetails"/> also their files.</summary>
    public List<BackupInfo> GetBackupInfo(bool includeFileDetails)
    {
        ThrowIfDisposed();
        _lock.Wait();
        try
        {
            var list = new List<BackupInfo>(_backups.Count);
            foreach (KeyValuePair<uint, BackupMeta> b in _backups)
            {
                if (!b.Value.Empty) list.Add(ToBackupInfo(b.Key, b.Value, includeFileDetails));
            }
            return list;
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>One backup's info (latest valid one for id 0); throws NotFound / Corruption.</summary>
    public BackupInfo GetBackupInfo(uint backupId, bool includeFileDetails)
    {
        ThrowIfDisposed();
        _lock.Wait();
        try
        {
            if (backupId == 0) backupId = _latestValidBackupId; // kLatestBackupIDMarker
            // GetBackupInfo reports a corrupt backup as Corruption whatever made it unusable (Status::Corruption of
            // the stored status' text); VerifyBackup and RestoreDBFromBackup return the stored status itself.
            if (_corruptBackups.TryGetValue(backupId, out (RocksDbException Error, BackupMeta Meta) corrupt) && corrupt.Error.Code != StatusCode.Corruption)
            {
                throw RocksDbException.Corruption(corrupt.Error.Message);
            }
            return ToBackupInfo(backupId, FindValidBackup(backupId), includeFileDetails);
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>Ids of the backups whose meta file is corrupt (GetCorruptedBackups).</summary>
    public List<uint> GetCorruptedBackups()
    {
        ThrowIfDisposed();
        _lock.Wait();
        try
        {
            return new List<uint>(_corruptBackups.Keys);
        }
        finally
        {
            _lock.Release();
        }
    }

    private BackupInfo ToBackupInfo(uint id, BackupMeta meta, bool includeFileDetails)
    {
        List<BackupFileInfo>? details = null;
        string dir = PrivateDir(id);
        if (includeFileDetails)
        {
            details = new List<BackupFileInfo>(meta.Files.Count);
            foreach (BackupFileEntry f in meta.Files)
            {
                FileNames.TryParseFileName(f.GetDbFileName(), out ulong number, out FileType type);
                details.Add(new BackupFileInfo { RelativeFilename = f.Filename, Directory = dir, FileNumber = number, FileType = type, Size = f.Size });
            }
        }
        return new BackupInfo
        {
            BackupId = id,
            Timestamp = meta.Timestamp,
            Size = meta.Size,
            NumberFiles = meta.NumberFiles,
            AppMetadata = Encoding.UTF8.GetString(meta.AppMetadata),
            FileDetails = details,
            ExcludedFiles = includeFileDetails ? meta.ExcludedFiles.ToArray() : null,
            NameForOpen = includeFileDetails ? dir : null,
        };
    }

    /// <summary>The loaded, non-empty backup <paramref name="backupId"/>. Corrupt backups throw their error. REQUIRES: _lock.</summary>
    private protected BackupMeta FindValidBackup(uint backupId)
    {
        if (_corruptBackups.TryGetValue(backupId, out (RocksDbException Error, BackupMeta Meta) corrupt)) throw corrupt.Error.Rethrowable();
        if (!_backups.TryGetValue(backupId, out BackupMeta? meta) || meta.Empty) throw new RocksDbException(StatusCode.NotFound, "NotFound: Backup not found");
        return meta;
    }

    // -----------------------------------------------------------------------------------------------------
    // Verify

    /// <summary>
    /// Port of BackupEngine::VerifyBackup: every file of the backup exists with the recorded size and, with
    /// <paramref name="verifyWithChecksum"/>, the recorded crc32c. Throws NotFound / Corruption.
    /// </summary>
    public void VerifyBackup(uint backupId, bool verifyWithChecksum) =>
        VerifyBackupCoreAsync(backupId, verifyWithChecksum, synchronous: true, CancellationToken.None).GetAwaiter().GetResult();

    public async ValueTask VerifyBackupAsync(uint backupId, bool verifyWithChecksum, CancellationToken cancellationToken = default) =>
        await VerifyBackupCoreAsync(backupId, verifyWithChecksum, synchronous: false, cancellationToken).ConfigureAwait(false);

    private async ValueTask VerifyBackupCoreAsync(uint backupId, bool verifyWithChecksum, bool synchronous, CancellationToken ct)
    {
        ThrowIfDisposed();
        await AcquireAsync(synchronous, ct).ConfigureAwait(false);
        try
        {
            BackupMeta meta = FindValidBackup(backupId);
            LogInfo("Verifying backup id " + backupId);
            var current = new Dictionary<string, ulong>(StringComparer.Ordinal);
            ReadChildFileCurrentSizes(PrivateDir(backupId), current);
            ReadChildFileCurrentSizes(GetAbsolutePath(SharedDirName), current);
            ReadChildFileCurrentSizes(GetAbsolutePath(SharedChecksumDirName), current);
            foreach (BackupFileEntry f in meta.Files)
            {
                string abs = GetAbsolutePath(f.Filename);
                if (!current.TryGetValue(abs, out ulong size)) throw new RocksDbException(StatusCode.NotFound, "NotFound: File missing: " + abs);
                if (size != f.Size)
                {
                    ThrowHelper.Corruption("File corrupted: File size mismatch for " + abs + ": Expected file size is " +
                        f.Size.ToString(CultureInfo.InvariantCulture) + " while found file size is " + size.ToString(CultureInfo.InvariantCulture));
                }
            }
            if (!verifyWithChecksum) return;
            foreach (BackupFileEntry f in meta.Files)
            {
                if (f.Crc32c is not uint expected) continue;
                string abs = GetAbsolutePath(f.Filename);
                uint actual = await ComputeChecksumAsync(_backupFs, abs, 0, _options.BackupRateLimiter, synchronous, ct).ConfigureAwait(false);
                if (actual != expected)
                {
                    ThrowHelper.Corruption("File corruption! Checksum mismatch for " + abs + ". Expected: " + ChecksumToHex(expected) + ", got: " + ChecksumToHex(actual));
                }
            }
        }
        finally
        {
            _lock.Release();
        }
    }

    private protected async ValueTask AcquireAsync(bool synchronous, CancellationToken ct)
    {
        if (synchronous) _lock.Wait(ct);
        else await _lock.WaitAsync(ct).ConfigureAwait(false);
    }

    // -----------------------------------------------------------------------------------------------------
    // Restore

    /// <summary>Restores the latest valid backup into <paramref name="dbDir"/> (WALs into <paramref name="walDir"/>).</summary>
    public void RestoreDbFromLatestBackup(string dbDir, string walDir, RestoreOptions options) => RestoreDbFromBackup(0, dbDir, walDir, options);

    public ValueTask RestoreDbFromLatestBackupAsync(string dbDir, string walDir, RestoreOptions options, CancellationToken cancellationToken = default) =>
        RestoreDbFromBackupAsync(0, dbDir, walDir, options, cancellationToken);

    /// <summary>
    /// Port of BackupEngine::RestoreDBFromBackup: deletes the files of the destination (keeping WALs with
    /// <see cref="RestoreOptions.KeepLogFiles"/>, and matching table files in the incremental modes), copies the backup's
    /// files with checksum verification and installs CURRENT last (atomic rename). Backup id 0 is the latest.
    /// </summary>
    public void RestoreDbFromBackup(uint backupId, string dbDir, string walDir, RestoreOptions options) =>
        RestoreCoreAsync(backupId, dbDir, walDir, options, synchronous: true, CancellationToken.None).GetAwaiter().GetResult();

    public async ValueTask RestoreDbFromBackupAsync(uint backupId, string dbDir, string walDir, RestoreOptions options,
        CancellationToken cancellationToken = default) =>
        await RestoreCoreAsync(backupId, dbDir, walDir, options, synchronous: false, cancellationToken).ConfigureAwait(false);

    private async ValueTask RestoreCoreAsync(uint backupId, string dbDir, string walDir, RestoreOptions options, bool synchronous, CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrEmpty(dbDir);
        ArgumentNullException.ThrowIfNull(options);
        ThrowIfDisposed();
        dbDir = dbDir.TrimEnd('/');
        walDir = string.IsNullOrEmpty(walDir) ? dbDir : walDir.TrimEnd('/');
        // BackupEngineImplThreadSafe::RestoreDBFromBackup: this engine and those of options.alternate_dirs are locked,
        // in a consistent order, for the whole restore (their file tables are read and their files copied).
        var engines = new List<BackupEngineReadOnly> { this };
        foreach (BackupEngineReadOnly be in options.AlternateDirs)
        {
            ArgumentNullException.ThrowIfNull(be);
            if (!engines.Contains(be)) engines.Add(be);
        }
        engines.Sort(static (a, b) => a._lockOrder.CompareTo(b._lockOrder));
        int locked = 0;
        try
        {
            for (; locked < engines.Count; locked++)
            {
                engines[locked].ThrowIfDisposed();
                await engines[locked].AcquireAsync(synchronous, ct).ConfigureAwait(false);
            }
            if (backupId == 0) backupId = _latestValidBackupId;
            BackupMeta backup = FindValidBackup(backupId);
            LogInfo("Restoring backup id " + backupId);
            LogInfo("keep_log_files: " + (options.KeepLogFiles ? 1 : 0));
            // just in case. Ignore errors
            TryIgnore(() => _dbFs.CreateDirectoryIfMissing(dbDir));
            TryIgnore(() => _dbFs.CreateDirectoryIfMissing(walDir));

            // Files to restore, and from where (taking into account excluded files): an excluded file comes from the
            // first of options.alternate_dirs that knows it.
            var restoreFiles = new List<(BackupEngineReadOnly From, BackupFileEntry File)>(backup.Files.Count + backup.ExcludedFiles.Count);
            var unownedBackups = new HashSet<string>(StringComparer.Ordinal);
            foreach (string excluded in backup.ExcludedFiles)
            {
                bool found = false;
                foreach (BackupEngineReadOnly be in options.AlternateDirs)
                {
                    if (be._backupedFileInfos.TryGetValue(excluded, out BackupFileEntry? info))
                    {
                        restoreFiles.Add((be, info));
                        found = true;
                        break;
                    }
                }
                if (!found)
                {
                    // In kKeepLatestDbSessionIdFiles mode the backup file need not be available as long as the DB file
                    // already in the destination has the db session id and size of this unowned backup file.
                    if (options.Mode == RestoreMode.KeepLatestDbSessionIdFiles)
                    {
                        unownedBackups.Add(excluded);
                        continue;
                    }
                    ThrowHelper.InvalidArgument("Excluded file " + excluded + " not found in any of " + options.AlternateDirs.Count + " backup directories!");
                }
            }
            foreach (BackupFileEntry f in backup.Files) restoreFiles.Add((this, f));

            HashSet<ulong> filesToKeep = await InferDbFilesToRetainAsync(restoreFiles, unownedBackups, dbDir, options.Mode, synchronous, ct).ConfigureAwait(false);
            if (unownedBackups.Count > 0)
            {
                string first = "";
                foreach (string u in unownedBackups)
                {
                    if (first.Length == 0 || string.CompareOrdinal(u, first) < 0) first = u;
                }
                ThrowHelper.InvalidArgument("Excluded file " + first + " (one amongst " + unownedBackups.Count + ") not found in any of " +
                    options.AlternateDirs.Count + " backup directories!");
            }

            // Where each file goes. Upstream parses the names in its copy loop, after the destination was emptied; a
            // backup naming something that is not a DB file is refused here, before anything is deleted.
            string? temporaryCurrent = null, finalCurrent = null;
            bool syncDbDir = false, syncWalDir = false;
            var copies = new List<(BackupEngineReadOnly From, BackupFileEntry File, string Dst)>(restoreFiles.Count);
            foreach ((BackupEngineReadOnly from, BackupFileEntry f) in restoreFiles)
            {
                string dst = f.GetDbFileName();
                if (!FileNames.TryParseFileName(dst, out ulong number, out FileType type)) ThrowHelper.Corruption("Backup corrupted: Fail to parse filename " + dst);
                if (IsKept(filesToKeep, number, type)) continue; // already in the destination directory
                if (type == FileType.WalFile)
                {
                    dst = walDir + "/" + dst;
                    syncWalDir = _options.Sync;
                }
                else
                {
                    dst = dbDir + "/" + dst;
                    syncDbDir = _options.Sync;
                }
                // For atomicity, initially restore CURRENT file to a temporary name.
                if (type == FileType.CurrentFile)
                {
                    finalCurrent = dst;
                    dst = temporaryCurrent = dst + ".tmp";
                }
                copies.Add((from, f, dst));
            }

            if (options.KeepLogFiles)
            {
                // Delete non-matching files in db_dir, but keep all the log files.
                DeleteChildren(dbDir, filesToKeep, 1 << (int)FileType.WalFile);
                // Move all the files from archive dir to wal_dir.
                string archive = walDir + "/" + FileNames.ArchivalDirName;
                foreach (string f in TryGetChildren(_dbFs, archive) ?? [])
                {
                    if (FileNames.TryParseFileName(f, out _, out FileType t) && t == FileType.WalFile)
                    {
                        LogInfo("Moving log file from archive/ to wal_dir: " + f);
                        _dbFs.RenameFile(archive + "/" + f, walDir + "/" + f);
                    }
                }
            }
            else
            {
                DeleteChildren(walDir, filesToKeep, 0);
                DeleteChildren(walDir + "/" + FileNames.ArchivalDirName, filesToKeep, 0);
                DeleteChildren(dbDir, filesToKeep, 0);
            }

            // The copies run on up to max_background_operations workers (RestoreFileWorkItems on the copy engine).
            await RunParallelAsync(copies.Count, async (i, sync, token) =>
            {
                (BackupEngineReadOnly from, BackupFileEntry f, string dst) = copies[i];
                LogInfo("Restoring " + f.Filename + " to " + dst);
                uint crc = await CopyFileAsync(from._backupFs, from.GetAbsolutePath(f.Filename), _dbFs, dst, f.Size, _options.Sync, _options.RestoreRateLimiter, null,
                    sync, token).ConfigureAwait(false);
                if (f.Crc32c is uint expected && expected != crc)
                {
                    ThrowHelper.Corruption("While restoring " + f.Filename + " -> " + dst + ": expected checksum is " + ChecksumToHex(expected) +
                        " while computed checksum is " + ChecksumToHex(crc));
                }
            }, synchronous, ct).ConfigureAwait(false);
            // The first fsync makes sure all files are persisted before renaming CURRENT.tmp.
            if (syncDbDir) _dbFs.SyncDirectory(dbDir);
            if (syncWalDir && walDir != dbDir) _dbFs.SyncDirectory(walDir);
            if (temporaryCurrent is not null)
            {
                LogInfo("Restore: atomic rename CURRENT.tmp");
                _dbFs.RenameFile(temporaryCurrent, finalCurrent!);
                if (syncDbDir) _dbFs.SyncDirectory(dbDir);
            }
            LogInfo("Restoring done");
        }
        finally
        {
            while (locked > 0) engines[--locked]._lock.Release();
        }
    }

    private static void TryIgnore(Action a)
    {
        try { a(); }
        catch (Exception e) when (e is IOException or RocksDbException or UnauthorizedAccessException) { }
    }

    /// <summary>
    /// files_to_keep holds the numbers of the destination's table / blob files that match the backup. Upstream tests
    /// the number alone, which also keeps (and does not restore over) any other file of the destination carrying that
    /// number, e.g. a stale 000012.log next to a kept 000012.sst, which the restored DB would replay.
    /// </summary>
    private static bool IsKept(HashSet<ulong> filesToKeep, ulong number, FileType type) =>
        type is FileType.TableFile or FileType.BlobFile && filesToKeep.Contains(number);

    /// <summary>Port of DeleteChildren: deletes the files of <paramref name="dir"/> except kept table / blob files and filtered types.</summary>
    private void DeleteChildren(string dir, HashSet<ulong> filesToKeep, int fileTypeFilter)
    {
        foreach (string f in TryGetChildren(_dbFs, dir) ?? [])
        {
            bool ok = FileNames.TryParseFileName(f, out ulong number, out FileType type);
            if (ok && IsKept(filesToKeep, number, type)) continue;
            if (ok && (fileTypeFilter & (1 << (int)type)) != 0) continue;
            string p = dir + "/" + f;
            if (_dbFs.DirectoryExistsSafe(p)) continue; // DeleteFile fails on directories upstream
            TryIgnore(() => _dbFs.DeleteFile(p));
        }
    }

    /// <summary>
    /// Port of InferDBFilesToRetainInRestore: in the incremental restore modes, table files (and blob files with
    /// kVerifyChecksum) of the destination identical to the backup's are kept.
    /// </summary>
    private async ValueTask<HashSet<ulong>> InferDbFilesToRetainAsync(List<(BackupEngineReadOnly From, BackupFileEntry File)> files,
        HashSet<string> unownedBackups, string dbDir, RestoreMode mode, bool synchronous, CancellationToken ct)
    {
        var keep = new HashSet<ulong>();
        if (mode == RestoreMode.PurgeAllFiles) return keep;
        var byNumber = new Dictionary<ulong, (BackupEngineReadOnly From, BackupFileEntry File)>();
        foreach ((BackupEngineReadOnly from, BackupFileEntry f) in files)
        {
            if (!FileNames.TryParseFileName(f.GetDbFileName(), out ulong number, out FileType type)) continue;
            if (type == FileType.TableFile || (type == FileType.BlobFile && mode == RestoreMode.VerifyChecksum)) byNumber[number] = (from, f);
        }
        foreach (string child in TryGetChildren(_dbFs, dbDir) ?? [])
        {
            if (!FileNames.TryParseFileName(child, out ulong number, out FileType type)) continue;
            if (type != FileType.TableFile && type != FileType.BlobFile) continue;
            if (type == FileType.BlobFile && mode != RestoreMode.VerifyChecksum) continue;
            bool inBackup = byNumber.TryGetValue(number, out (BackupEngineReadOnly From, BackupFileEntry File) entry);
            if (!inBackup && (mode != RestoreMode.KeepLatestDbSessionIdFiles || unownedBackups.Count == 0)) continue;
            BackupFileEntry? bf = inBackup ? entry.File : null;
            string path = dbDir + "/" + child;
            ulong size;
            try { size = (ulong)_dbFs.GetFileSize(path); }
            catch (Exception e) when (e is IOException or RocksDbException) { continue; }
            if (mode == RestoreMode.KeepLatestDbSessionIdFiles)
            {
                // The existing file is kept only when the backup file has exactly the shared_checksum name the existing
                // file would get (GenerateSharedFileWithDbSessionIdAndSize: <number>_s<session id>_<size>.sst).
                if (!TryGetFileDbIdentities(_dbFs, path, out _, out string sid)) continue;
                int dot = child.LastIndexOf('.');
                string shared = "shared_checksum/" + child[..dot] + "_s" + sid + "_" + size.ToString(System.Globalization.CultureInfo.InvariantCulture) + child[dot..];
                if (bf is not null && bf.Filename == shared) keep.Add(number);
                // The DB file is the excluded backup file that no backup directory holds.
                else if (type == FileType.TableFile && unownedBackups.Remove(shared)) keep.Add(number);
            }
            else
            {
                // "Existing file is not present in the backup": same number under another name (000012.ldb, or a blob
                // file against a table file).
                if (bf is null || bf.GetDbFileName() != child) continue;
                if (size != bf.Size) continue;
                uint expected = bf.Crc32c ?? await ComputeChecksumAsync(entry.From._backupFs, entry.From.GetAbsolutePath(bf.Filename), 0,
                    _options.RestoreRateLimiter, synchronous, ct).ConfigureAwait(false);
                uint actual = await ComputeChecksumAsync(_dbFs, path, 0, _options.RestoreRateLimiter, synchronous, ct).ConfigureAwait(false);
                if (actual == expected) keep.Add(number);
            }
        }
        return keep;
    }

    // -----------------------------------------------------------------------------------------------------
    // File helpers

    /// <summary>
    /// GetFileDbIdentities: the db id and DB session id from a table file's properties. False when the file has none
    /// (old files) or cannot be read as a table.
    /// </summary>
    private protected static bool TryGetFileDbIdentities(IFileSystem fs, string path, out string dbId, out string dbSessionId)
    {
        dbId = "";
        dbSessionId = "";
        try
        {
            // SstFileDumper reads any table format (picked by the footer's magic number).
            using TableReader reader = TableReaderFactory.Open(fs.OpenRandomAccess(path), new TableReaderOptions
            {
                VerifyChecksums = true,
                TableFactory = TableFactoryType.AdaptiveTable,
                AllowMmapReads = true,
                PlainTableOptions = new PlainTableOptions { FullScanMode = true },
            });
            dbId = reader.Properties.DbId;
            dbSessionId = reader.Properties.DbSessionId;
            return dbSessionId.Length > 0;
        }
        catch (Exception e) when (e is IOException or RocksDbException or InvalidOperationException or ArgumentException)
        {
            return false;
        }
    }

    /// <summary>
    /// GetFileDbIdentities as used by AddBackupFileWorkItem: the DB session id of a table file, "" when its properties
    /// have none (NotFound upstream, fine for old files). A file that cannot be read as a table fails the backup.
    /// </summary>
    private protected static string GetFileDbSessionId(IFileSystem fs, string path)
    {
        try
        {
            using TableReader reader = TableReaderFactory.Open(fs.OpenRandomAccess(path), new TableReaderOptions
            {
                VerifyChecksums = true,
                TableFactory = TableFactoryType.AdaptiveTable,
                AllowMmapReads = true,
                PlainTableOptions = new PlainTableOptions { FullScanMode = true },
            });
            return reader.Properties.DbSessionId;
        }
        catch (Exception e) when (e is IOException or InvalidOperationException or ArgumentException)
        {
            throw RocksDbException.FromException(e).Rethrowable();
        }
    }

    // bytes_toward_next_callback of the copy engine's worker (carried over from file to file and from one operation
    // to the next). One counter for all workers here.
    private ulong _bytesTowardNextCallback;
    private readonly Lock _progressLock = new();

    private int IoBufferSize => _options.IoBufferSize > 0 ? _options.IoBufferSize : DefaultIoBufferSize;

    /// <summary>ReadFileAndComputeChecksum: crc32c of the first <paramref name="sizeLimit"/> bytes (0 = all).</summary>
    private protected async ValueTask<uint> ComputeChecksumAsync(IFileSystem fs, string path, ulong sizeLimit, BackupRateLimiter? limiter, bool synchronous,
        CancellationToken ct)
    {
        if (sizeLimit == 0) sizeLimit = ulong.MaxValue;
        byte[] buffer = RocksArrayPool<byte>.Shared.Rent(Math.Min(IoBufferSize, 1 << 20));
        try
        {
            using ISequentialFile src = fs.OpenSequential(path);
            uint crc = 0;
            while (sizeLimit > 0)
            {
                ct.ThrowIfCancellationRequested();
                int want = (int)Math.Min((ulong)buffer.Length, sizeLimit);
                int n = synchronous ? src.Read(buffer.AsSpan(0, want)) : await src.ReadAsync(buffer.AsMemory(0, want), ct).ConfigureAwait(false);
                if (n <= 0) break;
                if (limiter is not null)
                {
                    if (synchronous) limiter.Request(n);
                    else await limiter.RequestAsync(n, ct).ConfigureAwait(false);
                }
                crc = Crc32C.Extend(crc, buffer.AsSpan(0, n));
                sizeLimit -= (ulong)n;
            }
            return crc;
        }
        finally
        {
            RocksArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <summary>
    /// Port of BackupEngineImpl::CopyOrCreateFile (copy part): copies up to <paramref name="sizeLimit"/> bytes (0 = all),
    /// rate limited, reporting progress, computing the crc32c of what was copied; syncs the destination with
    /// <paramref name="sync"/>. Returns the checksum.
    /// </summary>
    private protected async ValueTask<uint> CopyFileAsync(IFileSystem srcFs, string src, IFileSystem dstFs, string dst, ulong sizeLimit, bool sync,
        BackupRateLimiter? limiter, Action? progress, bool synchronous, CancellationToken ct)
    {
        if (sizeLimit == 0) sizeLimit = ulong.MaxValue;
        byte[] buffer = RocksArrayPool<byte>.Shared.Rent(IoBufferSize);
        IWritableFile? dstFile = null;
        try
        {
            using ISequentialFile srcFile = srcFs.OpenSequential(src);
            dstFile = dstFs.CreateWritable(dst, truncate: true);
            uint crc = 0;
            ulong interval = Math.Max(1, _options.CallbackTriggerIntervalSize);
            while (sizeLimit > 0)
            {
                if (_stopBackup && !_readOnly && progress is not null) throw new RocksDbException(StatusCode.Incomplete, "Incomplete: Backup stopped");
                ct.ThrowIfCancellationRequested();
                int want = (int)Math.Min((ulong)buffer.Length, sizeLimit);
                int n = synchronous ? srcFile.Read(buffer.AsSpan(0, want)) : await srcFile.ReadAsync(buffer.AsMemory(0, want), ct).ConfigureAwait(false);
                if (n <= 0) break;
                if (limiter is not null)
                {
                    if (synchronous) limiter.Request(n);
                    else await limiter.RequestAsync(n, ct).ConfigureAwait(false);
                }
                crc = Crc32C.Extend(crc, buffer.AsSpan(0, n));
                if (synchronous) dstFile.Append(buffer.AsSpan(0, n));
                else await dstFile.AppendAsync(buffer.AsMemory(0, n), ct).ConfigureAwait(false);
                sizeLimit -= (ulong)n;
                // CopyOrCreateFile: one callback per callback_trigger_interval_size bytes read, counted across files.
                lock (_progressLock)
                {
                    _bytesTowardNextCallback += (ulong)n;
                    while (_bytesTowardNextCallback >= interval)
                    {
                        _bytesTowardNextCallback -= interval;
                        if (progress is not null) InvokeProgress(progress);
                    }
                }
            }
            if (sync)
            {
                if (synchronous) dstFile.Sync();
                else await dstFile.SyncAsync(ct).ConfigureAwait(false);
            }
            return crc;
        }
        finally
        {
            RocksArrayPool<byte>.Shared.Return(buffer);
            if (dstFile is not null)
            {
                if (synchronous) dstFile.Dispose();
                else await dstFile.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Runs <paramref name="work"/> for 0 .. <paramref name="count"/> - 1 on up to
    /// <see cref="BackupEngineOptions.MaxBackgroundOperations"/> concurrent workers (the copy engine's threads,
    /// utilities/backup/backup_engine.cc). The first failure is rethrown once the started items finished; items not
    /// started yet are skipped.
    /// </summary>
    private protected async ValueTask RunParallelAsync(int count, Func<int, bool, CancellationToken, ValueTask> work, bool synchronous, CancellationToken ct)
    {
        int workers = Math.Min(Math.Max(1, _options.MaxBackgroundOperations), count);
        if (workers <= 1)
        {
            for (int i = 0; i < count; i++) await work(i, synchronous, ct).ConfigureAwait(false);
            return;
        }
        int next = -1;
        Exception? failure = null;
        // Synchronous callers block on thread-pool workers doing synchronous I/O; async callers get async workers.
        var tasks = new Task[workers];
        for (int w = 0; w < workers; w++)
        {
            tasks[w] = Task.Run(async () =>
            {
                while (Volatile.Read(ref failure) is null)
                {
                    int i = Interlocked.Increment(ref next);
                    if (i >= count) return;
                    try
                    {
                        await work(i, synchronous, ct).ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        Interlocked.CompareExchange(ref failure, e, null);
                    }
                }
            }, CancellationToken.None);
        }
        if (synchronous) Task.WaitAll(tasks, CancellationToken.None);
        else await Task.WhenAll(tasks).ConfigureAwait(false);
        if (failure is not null) System.Runtime.ExceptionServices.ExceptionDispatchInfo.Throw(failure);
    }

    private static void InvokeProgress(Action progress)
    {
        try
        {
            progress();
        }
        catch (Exception e) when (e is not OutOfMemoryException)
        {
            throw new RocksDbException(StatusCode.Aborted, "Operation aborted: Exception in progress_callback: " + e.Message, StatusSubCode.None, e);
        }
    }

    // -----------------------------------------------------------------------------------------------------

    protected virtual void Dispose(bool disposing)
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        if (disposing) _lock.Dispose();
    }

    public void Dispose()
    {
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }

    // ---- writable engine internals shared with BackupEngine (REQUIRE: _lock) ------------------------------

    /// <summary>Port of BackupEngineImpl::PurgeOldBackups (without the lock).</summary>
    private protected void PurgeOldBackupsLocked(uint numBackupsToKeep)
    {
        LogInfo("Purging old backups, keeping " + numBackupsToKeep);
        Exception? overall = null;
        var toDelete = new List<uint>();
        foreach (uint id in _backups.Keys)
        {
            if (_backups.Count - toDelete.Count <= numBackupsToKeep) break;
            toDelete.Add(id);
        }
        foreach (uint id in toDelete)
        {
            try { DeleteBackupNoGcLocked(id); }
            catch (Exception e) when (e is IOException or RocksDbException) { overall ??= e; }
        }
        // Clean up after any incomplete backup deletion, potentially from earlier session.
        if (_mightNeedGarbageCollect)
        {
            try { GarbageCollectLocked(); }
            catch (Exception e) when (e is IOException or RocksDbException) { overall ??= e; }
        }
        if (overall is not null) throw RocksDbException.FromException(overall).Rethrowable();
    }

    /// <summary>Port of BackupEngineImpl::DeleteBackupNoGC.</summary>
    private protected void DeleteBackupNoGcLocked(uint backupId)
    {
        LogInfo("Deleting backup " + backupId);
        if (_backups.TryGetValue(backupId, out BackupMeta? meta))
        {
            meta.Delete(_backupFs, deleteMeta: true);
            _backups.Remove(backupId);
            _latestValidBackupId = 0;
            foreach (uint k in _backups.Keys) _latestValidBackupId = k;
        }
        else if (_corruptBackups.TryGetValue(backupId, out (RocksDbException Error, BackupMeta Meta) corrupt))
        {
            corrupt.Meta.Delete(_backupFs, deleteMeta: true);
            _corruptBackups.Remove(backupId);
            // Its files were never reference counted: only a garbage collection finds them.
            _unloadedBackupFiles.Remove(backupId);
            _mightNeedGarbageCollect = true;
        }
        else
        {
            throw new RocksDbException(StatusCode.NotFound, "NotFound: Backup not found");
        }
        // After removing meta file, best effort deletion even with errors.
        var unreferenced = new List<string>();
        foreach (KeyValuePair<string, BackupFileEntry> kv in _backupedFileInfos)
        {
            if (kv.Value.Refs != 0) continue;
            unreferenced.Add(kv.Key);
            if (IsNamedByUnloadedBackup(kv.Key)) continue;
            try { _backupFs.DeleteFile(GetAbsolutePath(kv.Key)); }
            catch (Exception e) when (e is IOException or RocksDbException) { _mightNeedGarbageCollect = true; }
        }
        foreach (string k in unreferenced) _backupedFileInfos.Remove(k);
        // Take care of private dirs -- GarbageCollect() will take care of them if they are not empty.
        try { _backupFs.DeleteDirectory(PrivateDir(backupId)); }
        catch (Exception e) when (e is IOException or RocksDbException) { _mightNeedGarbageCollect = true; }
    }

    /// <summary>
    /// Whether a backup that could not be loaded (corrupt_backups_: a damaged meta file, or one of a schema this version
    /// does not support) names <paramref name="relativeFile"/>. Such a backup holds no reference counts, so upstream's
    /// DeleteBackupNoGC and GarbageCollect delete its shared files and its private directory although its meta file
    /// stays; a backup written by a newer version, or one whose meta file can be repaired, is destroyed that way. Here
    /// what its meta file names is kept until the backup is deleted by id.
    /// </summary>
    private protected bool IsNamedByUnloadedBackup(string relativeFile)
    {
        foreach (HashSet<string> files in _unloadedBackupFiles.Values)
        {
            if (files.Contains(relativeFile)) return true;
        }
        return false;
    }

    /// <summary>
    /// Port of BackupEngineImpl::GarbageCollect: deletes unreferenced shared files and private dirs of dead backups.
    /// Beyond upstream: temporary meta files (<c>meta/.&lt;id&gt;.tmp</c>, left by a StoreToFile that failed; Initialize's
    /// comment promises their collection, GarbageCollect never looks into meta/) are deleted, and the files of backups
    /// that could not be loaded are kept (<see cref="IsNamedByUnloadedBackup"/>).
    /// </summary>
    private protected void GarbageCollectLocked()
    {
        _mightNeedGarbageCollect = false;
        LogInfo("Starting garbage collection");
        foreach (bool withChecksum in (ReadOnlySpan<bool>)[false, true])
        {
            string sharedPath = GetAbsolutePath(withChecksum ? SharedChecksumDirName : SharedDirName);
            foreach (string child in TryGetChildren(_backupFs, sharedPath) ?? [])
            {
                string rel = withChecksum ? GetSharedFileWithChecksumRel(child, false) : GetSharedFileRel(child, false);
                if (_backupedFileInfos.TryGetValue(rel, out BackupFileEntry? e) && e.Refs != 0) continue;
                if (IsNamedByUnloadedBackup(rel)) continue;
                try { _backupFs.DeleteFile(GetAbsolutePath(rel)); }
                catch (Exception ex) when (ex is IOException or RocksDbException) { _mightNeedGarbageCollect = true; }
                _backupedFileInfos.Remove(rel);
            }
        }
        foreach (string child in TryGetChildren(_backupFs, GetAbsolutePath(PrivateDirName)) ?? [])
        {
            bool tmpDir = child.Contains(".tmp", StringComparison.Ordinal);
            uint.TryParse(LeadingDigits(child), NumberStyles.None, CultureInfo.InvariantCulture, out uint id);
            if (!tmpDir && (id == 0 || _backups.ContainsKey(id))) continue; // not a number, or still alive
            if (!tmpDir && _corruptBackups.ContainsKey(id) && child == id.ToString(CultureInfo.InvariantCulture)) continue; // not loaded, not deleted
            string full = GetAbsolutePath(PrivateDirName + "/" + child);
            foreach (string sub in TryGetChildren(_backupFs, full) ?? [])
            {
                try { _backupFs.DeleteFile(full + "/" + sub); }
                catch (Exception ex) when (ex is IOException or RocksDbException) { _mightNeedGarbageCollect = true; }
            }
            try { _backupFs.DeleteDirectory(full); }
            catch (Exception ex) when (ex is IOException or RocksDbException) { _mightNeedGarbageCollect = true; }
        }
        DeleteTemporaryMetaFiles();
    }

    /// <summary>Deletes <c>meta/.&lt;id&gt;.tmp</c> files (BackupMeta::StoreToFile's temporary name). REQUIRES: _lock.</summary>
    private void DeleteTemporaryMetaFiles()
    {
        string metaPath = GetAbsolutePath(MetaDirName);
        foreach (string child in TryGetChildren(_backupFs, metaPath) ?? [])
        {
            if (child.Length < 6 || child[0] != '.' || !child.EndsWith(".tmp", StringComparison.Ordinal)) continue;
            string digits = child[1..^4];
            if (LeadingDigits(digits).Length != digits.Length) continue;
            try { _backupFs.DeleteFile(metaPath + "/" + child); }
            catch (Exception ex) when (ex is IOException or RocksDbException) { _mightNeedGarbageCollect = true; }
        }
    }

    private static string LeadingDigits(string s)
    {
        int i = 0;
        while (i < s.Length && s[i] >= '0' && s[i] <= '9') i++;
        return s[..i];
    }

    internal static int CheckAppMetadata(string appMetadata)
    {
        int n = Encoding.UTF8.GetByteCount(appMetadata);
        if (n > MaxAppMetaSize) ThrowHelper.InvalidArgument("App metadata too large");
        return n;
    }
}
