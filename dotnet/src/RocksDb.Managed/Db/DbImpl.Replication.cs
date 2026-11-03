using RocksDb.Managed.Wal;

namespace RocksDb.Managed.Db;

/// <summary>
/// Replication / operational support of DBImpl:
/// <list type="bullet">
/// <item>file deletion control (DisableFileDeletions / EnableFileDeletions, db/db_impl/db_impl_files.cc),</item>
/// <item>WAL archival and archive purging (db/wal_manager.cc: ArchiveWALFile, PurgeObsoleteWALFiles),</item>
/// <item>sorted WAL listing with first-record sequence numbers (WalManager::GetSortedWalFiles / ReadFirstRecord),</item>
/// <item>GetUpdatesSince (DBImpl::GetUpdatesSince + WalManager::GetUpdatesSince / RetainProbableWalFiles),</item>
/// <item>a write-path signal so WAL tailers can await new writes instead of polling,</item>
/// <item>GetLiveFiles, GetCurrentWalFile, FlushWAL, SyncWAL (db/db_filesnapshot.cc, db_impl.cc).</item>
/// </list>
/// </summary>
internal sealed partial class DbImpl
{
    // DisableFileDeletions nesting count (disable_delete_obsolete_files_). Guarded by mutex.
    private int _disableDeleteObsoleteFiles;
    // WalManager::read_first_record_cache_ (only non-zero first sequences are cached). Guarded by itself.
    private readonly Dictionary<ulong, ulong> _readFirstRecordCache = new();
    // WalManager::purge_wal_files_last_run_ (unix seconds). Guarded by mutex.
    private long _purgeWalFilesLastRun;
    // Woken (and replaced) whenever new sequence numbers are published; see WaitForSequenceAsync.
    private TaskCompletionSource? _sequenceWaiters;

    /// <summary>kDefaultIntervalToDeleteObsoleteWAL (db/wal_manager.h): 600 seconds.</summary>
    private const long DefaultIntervalToDeleteObsoleteWal = 600;

    private bool WalArchivalEnabled => Options.WalTtlSeconds > 0 || Options.WalSizeLimitMb > 0;

    // ---- file deletions ---------------------------------------------------------------------------------

    /// <summary>True while DisableFileDeletions is in effect. REQUIRES: mutex.</summary>
    private bool FileDeletionsDisabled => _disableDeleteObsoleteFiles > 0;

    /// <summary>Port of DBImpl::DisableFileDeletions: nested; obsolete files are kept until re-enabled.</summary>
    public int DisableFileDeletions()
    {
        ThrowIfDisposed();
        lock (_mutex) return ++_disableDeleteObsoleteFiles;
    }

    /// <summary>
    /// Port of DBImpl::EnableFileDeletions (the legacy <c>force</c> flag resets the nesting count). When the count
    /// reaches zero, obsolete files are deleted right away.
    /// </summary>
    public int EnableFileDeletions(bool force)
    {
        ThrowIfDisposed();
        int remaining;
        lock (_mutex)
        {
            if (force) _disableDeleteObsoleteFiles = 0;
            else if (_disableDeleteObsoleteFiles > 0) _disableDeleteObsoleteFiles--;
            remaining = _disableDeleteObsoleteFiles;
        }
        if (remaining == 0 && !_readOnly)
        {
            try
            {
                PurgeObsoleteFiles();
            }
            catch (Exception e) when (e is IOException or RocksDbException)
            {
                // Best effort, like background purges.
            }
        }
        return remaining;
    }

    public bool IsFileDeletionsEnabled
    {
        get
        {
            lock (_mutex) return _disableDeleteObsoleteFiles == 0;
        }
    }

    // ---- WAL archival ----------------------------------------------------------------------------------

    /// <summary>
    /// Deletes an obsolete WAL, or moves it to the archive directory when WAL_ttl_seconds / WAL_size_limit_MB is set
    /// (PurgeObsoleteFiles: "if (type == kWalFile && (WAL_ttl_seconds > 0 || WAL_size_limit_MB > 0)) ArchiveWALFile").
    /// </summary>
    internal void DeleteOrArchiveWal(ulong number)
    {
        string fname = FileNames.LogFileName(WalDir, number);
        if (!WalArchivalEnabled)
        {
            TryDeleteFile(fname);
            return;
        }
        // WalManager::ArchiveWALFile.
        try
        {
            FileSystem.CreateDirectoryIfMissing(FileNames.ArchivalDirectory(WalDir));
            FileSystem.RenameFile(fname, FileNames.ArchivedLogFileName(WalDir, number));
        }
        catch (Exception e) when (e is IOException or RocksDbException or UnauthorizedAccessException)
        {
            // Like upstream, a failed archive leaves the file in place (it is retried by the next full scan).
        }
    }

    /// <summary>
    /// Port of WalManager::PurgeObsoleteWALFiles: deletes archived WALs older than WAL_ttl_seconds and, with
    /// WAL_size_limit_MB, the oldest archived WALs beyond the size budget. Runs at most every
    /// min(600s, ttl/2) unless <paramref name="force"/>.
    /// </summary>
    internal void PurgeObsoleteWalFiles(bool force)
    {
        bool ttlEnabled = Options.WalTtlSeconds > 0;
        bool sizeLimitEnabled = Options.WalSizeLimitMb > 0;
        if (!ttlEnabled && !sizeLimitEnabled) return;
        long now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        long timeToCheck = ttlEnabled ? (long)Math.Min((ulong)DefaultIntervalToDeleteObsoleteWal, Options.WalTtlSeconds / 2) : DefaultIntervalToDeleteObsoleteWal;
        lock (_mutex)
        {
            if (FileDeletionsDisabled) return;
            if (!force && _purgeWalFilesLastRun + timeToCheck > now) return;
            _purgeWalFilesLastRun = now;
        }
        string archivalDir = FileNames.ArchivalDirectory(WalDir);
        if (!FileSystem.DirectoryExistsSafe(archivalDir)) return;
        IEnumerable<string> files;
        try { files = FileSystem.GetChildren(archivalDir); }
        catch (RocksDbException) { return; }
        int logFilesNum = 0;
        ulong logFileSize = 0;
        foreach (string f in files)
        {
            if (!FileNames.TryParseFileName(f, out ulong number, out FileType type) || type != FileType.WalFile) continue;
            string path = archivalDir + "/" + f;
            if (ttlEnabled)
            {
                long mtime;
                try { mtime = FileSystem.GetFileModificationTime(path).ToUnixTimeSeconds(); }
                catch (RocksDbException) { continue; }
                if (now - mtime > (long)Math.Min(Options.WalTtlSeconds, long.MaxValue))
                {
                    TryDeleteFile(path);
                    lock (_readFirstRecordCache) _readFirstRecordCache.Remove(number);
                    continue;
                }
            }
            if (sizeLimitEnabled)
            {
                ulong size;
                try { size = (ulong)FileSystem.GetFileSize(path); }
                catch (RocksDbException) { return; }
                if (size > 0)
                {
                    logFileSize = Math.Max(logFileSize, size);
                    logFilesNum++;
                }
                else
                {
                    TryDeleteFile(path);
                    lock (_readFirstRecordCache) _readFirstRecordCache.Remove(number);
                }
            }
        }
        if (logFilesNum == 0 || !sizeLimitEnabled) return;
        ulong filesKeepNum = Options.WalSizeLimitMb * 1024 * 1024 / logFileSize;
        if ((ulong)logFilesNum <= filesKeepNum) return;
        int filesDelNum = logFilesNum - (int)filesKeepNum;
        List<WalFileInfo> archived = GetSortedWalsOfType(archivalDir, WalFileType.ArchivedLogFile);
        if (filesDelNum > archived.Count) filesDelNum = archived.Count;
        for (int i = 0; i < filesDelNum; i++)
        {
            TryDeleteFile(FileNames.ArchivedLogFileName(WalDir, archived[i].LogNumber));
            lock (_readFirstRecordCache) _readFirstRecordCache.Remove(archived[i].LogNumber);
        }
    }

    /// <summary>
    /// Port of the archived-WAL branch of DBImpl::DeleteFile + WalManager::DeleteFile: deletes the archived WAL
    /// <paramref name="number"/> and forgets its cached first sequence. Live WALs cannot be deleted
    /// (upstream: "Delete only supported for archived logs").
    /// </summary>
    public void DeleteArchivedWalFile(ulong number)
    {
        ThrowIfDisposed();
        if (_readOnly) ThrowHelper.NotSupported("DeleteFile is not supported in read-only mode");
        string path = FileNames.ArchivedLogFileName(WalDir, number);
        if (!FileSystem.FileExists(path))
        {
            if (FileSystem.FileExists(FileNames.LogFileName(WalDir, number))) ThrowHelper.NotSupported("Delete only supported for archived logs");
            throw new RocksDbException(StatusCode.NotFound, "NotFound: " + path);
        }
        FileSystem.DeleteFile(path);
        lock (_readFirstRecordCache) _readFirstRecordCache.Remove(number);
    }

    // ---- sorted WAL files ------------------------------------------------------------------------------

    /// <summary>
    /// Port of WalManager::GetSortedWalFiles: the alive WALs of wal_dir and (optionally) the archived ones, sorted
    /// by log number, each with the sequence number of its first record. Empty WALs are left out. A WAL seen in both
    /// places (archived while listing) is reported once, as archived.
    /// </summary>
    public List<WalFileInfo> GetSortedWalFiles(bool includeArchived)
    {
        // First get sorted files in db dir, then get sorted files from archived dir, to avoid a race condition where
        // a log file is moved to archived dir in between.
        List<WalFileInfo> logs = GetSortedWalsOfType(WalDir, WalFileType.AliveLogFile);
        if (!includeArchived) return logs;
        string archivedir = FileNames.ArchivalDirectory(WalDir);
        List<WalFileInfo> files = FileSystem.DirectoryExistsSafe(archivedir)
            ? GetSortedWalsOfType(archivedir, WalFileType.ArchivedLogFile)
            : new List<WalFileInfo>();
        ulong latestArchived = files.Count > 0 ? files[^1].LogNumber : 0;
        foreach (WalFileInfo log in logs)
        {
            // When the race condition happens, we could see the same log in both db dir and archived dir. Simply
            // ignore the one in db dir.
            if (log.LogNumber > latestArchived) files.Add(log);
        }
        return files;
    }

    /// <summary>Port of WalManager::GetSortedWalsOfType.</summary>
    private List<WalFileInfo> GetSortedWalsOfType(string path, WalFileType type)
    {
        var result = new List<WalFileInfo>();
        foreach (string f in FileSystem.GetChildren(path))
        {
            if (!FileNames.TryParseFileName(f, out ulong number, out FileType ft) || ft != FileType.WalFile) continue;
            ulong sequence = ReadFirstRecord(type, number);
            if (sequence == 0) continue; // empty file
            long size;
            try
            {
                size = FileSystem.GetFileSize(FileNames.LogFileName(path, number));
            }
            catch (RocksDbException) when (type == WalFileType.AliveLogFile)
            {
                // Re-try in case the alive log file has been moved to the archive.
                string archived = FileNames.ArchivedLogFileName(WalDir, number);
                if (!FileSystem.FileExists(archived)) continue;
                try { size = FileSystem.GetFileSize(archived); }
                catch (RocksDbException) { continue; }
            }
            catch (RocksDbException)
            {
                continue; // just deleted from the archive
            }
            result.Add(new WalFileInfo(number, type, sequence, (ulong)size));
        }
        result.Sort(static (a, b) => a.LogNumber.CompareTo(b.LogNumber));
        return result;
    }

    /// <summary>Port of WalManager::ReadFirstRecord: the first sequence number of a WAL, or 0 when it has no record.</summary>
    private ulong ReadFirstRecord(WalFileType type, ulong number)
    {
        lock (_readFirstRecordCache)
        {
            if (_readFirstRecordCache.TryGetValue(number, out ulong cached)) return cached;
        }
        ulong sequence = 0;
        if (type == WalFileType.AliveLogFile)
        {
            if (!TryReadFirstLine(FileNames.LogFileName(WalDir, number), number, out sequence))
            {
                // Check if the file got moved to the archive.
                if (!TryReadFirstLine(FileNames.ArchivedLogFileName(WalDir, number), number, out sequence)) sequence = 0;
            }
        }
        else if (!TryReadFirstLine(FileNames.ArchivedLogFileName(WalDir, number), number, out sequence))
        {
            sequence = 0;
        }
        if (sequence != 0)
        {
            lock (_readFirstRecordCache) _readFirstRecordCache[number] = sequence;
        }
        return sequence;
    }

    /// <summary>Port of WalManager::ReadFirstLine. Returns false if the file does not exist.</summary>
    private bool TryReadFirstLine(string fname, ulong number, out ulong sequence)
    {
        sequence = 0;
        Env.ISequentialFile file;
        try
        {
            file = FileSystem.OpenSequential(fname);
        }
        catch (RocksDbException e) when (e.SubCode == StatusSubCode.PathNotFound || !FileSystem.FileExists(fname))
        {
            return false;
        }
        using var reader = new LogReader(file, null, checksum: true, number) { RecoveryMode = WalRecoveryMode.PointInTimeRecovery };
        while (reader.TryReadRecord(out ReadOnlySpan<byte> record))
        {
            if (record.Length < WriteBatch.HeaderSize) continue; // "very small log record"
            sequence = Util.Coding.DecodeFixed64(record);
            return true;
        }
        return true;
    }

    /// <summary>Port of DBImpl::GetCurrentWalFile (WalManager::GetLiveWalFile): the WAL currently written to.</summary>
    public WalFileInfo GetCurrentWalFile()
    {
        ThrowIfDisposed();
        ulong number;
        lock (_mutex) number = _logNumber;
        if (number == 0) ThrowHelper.NotSupported("No WAL is written by this DB instance");
        long size = FileSystem.FileExists(FileNames.LogFileName(WalDir, number)) ? FileSystem.GetFileSize(FileNames.LogFileName(WalDir, number)) : 0;
        return new WalFileInfo(number, WalFileType.AliveLogFile, 0, (ulong)size);
    }

    // ---- GetUpdatesSince -------------------------------------------------------------------------------

    /// <summary>
    /// Port of DBImpl::GetUpdatesSince + WalManager::GetUpdatesSince: lists the WALs (live + archived), keeps those
    /// that may contain <paramref name="seq"/> (RetainProbableWalFiles) and positions an iterator on the batch
    /// containing it. Throws NotFound if <paramref name="seq"/> was not written yet.
    /// </summary>
    public TransactionLogIterator GetUpdatesSince(ulong seq, bool verifyChecksums)
    {
        ThrowIfDisposed();
        if (seq > Versions.LastSequence) throw new RocksDbException(StatusCode.NotFound, "NotFound: Requested sequence not yet written in the db");
        List<WalFileInfo> files = GetSortedWalFiles(includeArchived: true);
        RetainProbableWalFiles(files, seq);
        return new TransactionLogIterator(this, seq, files, verifyChecksums);
    }

    /// <summary>Port of WalManager::RetainProbableWalFiles: drops the files that end before <paramref name="target"/>.</summary>
    internal static void RetainProbableWalFiles(List<WalFileInfo> allLogs, ulong target)
    {
        long start = 0;
        long end = allLogs.Count - 1;
        // Binary Search. avoid opening all files.
        while (end >= start)
        {
            long mid = start + (end - start) / 2;
            ulong current = allLogs[(int)mid].StartSequence;
            if (current == target)
            {
                end = mid;
                break;
            }
            if (current < target) start = mid + 1;
            else end = mid - 1;
        }
        // end could be -ve. The last wal file is always included.
        int startIndex = (int)Math.Max(0, end);
        allLogs.RemoveRange(0, startIndex);
    }

    /// <summary>Opens a WAL for sequential reading; alive WALs fall back to the archive (TransactionLogIteratorImpl::OpenLogFile).</summary>
    internal Env.ISequentialFile OpenWalForRead(WalFileInfo file)
    {
        if (file.Type == WalFileType.ArchivedLogFile) return FileSystem.OpenSequential(FileNames.ArchivedLogFileName(WalDir, file.LogNumber));
        try
        {
            return FileSystem.OpenSequential(FileNames.LogFileName(WalDir, file.LogNumber));
        }
        catch (RocksDbException)
        {
            // If cannot open file in DB directory, try the archive dir, as it could have moved in the meanwhile.
            return FileSystem.OpenSequential(FileNames.ArchivedLogFileName(WalDir, file.LogNumber));
        }
    }

    // ---- write-path signal ---------------------------------------------------------------------------------

    /// <summary>
    /// Wakes tailers waiting for new sequence numbers. Called by the write leader right after publishing a group
    /// (and by read-only/secondary catch-up). One interlocked exchange per write group, which also orders the
    /// published sequence before the waiter check (see <see cref="WaitForSequenceAsync"/>).
    /// </summary>
    internal void NotifySequenceWaiters()
    {
        TaskCompletionSource? waiters = Interlocked.Exchange(ref _sequenceWaiters, null);
        waiters?.TrySetResult();
    }

    /// <summary>
    /// Completes when the DB has published sequence number <paramref name="sequence"/> (LastSequence &gt;=
    /// sequence) or when the DB is closed (returns false then). Allocation-free when already satisfied.
    /// </summary>
    public async ValueTask<bool> WaitForSequenceAsync(ulong sequence, CancellationToken cancellationToken)
    {
        while (Versions.LastSequence < sequence)
        {
            if (IsDisposed) return false;
            TaskCompletionSource? tcs = Volatile.Read(ref _sequenceWaiters);
            if (tcs is null)
            {
                var created = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                tcs = Interlocked.CompareExchange(ref _sequenceWaiters, created, null) ?? created;
            }
            // Re-check after registering (the CompareExchange / writer's Exchange are full fences).
            Interlocked.MemoryBarrier();
            if (Versions.LastSequence >= sequence) break;
            if (IsDisposed) return false;
            await tcs.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        return true;
    }

    // ---- live files ------------------------------------------------------------------------------------

    /// <summary>
    /// Port of DBImpl::GetLiveFiles: optionally flushes all column families, then returns the names (relative to the
    /// DB directory, starting with '/') of the live table and blob files, CURRENT, the MANIFEST and the latest OPTIONS file,
    /// plus the MANIFEST size consistent with the table set. Callers that copy the files should disable file
    /// deletions first.
    /// </summary>
    public async ValueTask<(List<string> Files, ulong ManifestFileSize)> GetLiveFilesAsync(bool flushMemtable, bool synchronous, CancellationToken ct)
    {
        ThrowIfDisposed();
        if (flushMemtable && !_readOnly)
        {
            foreach (ColumnFamilyData cfd in GetColumnFamilies())
            {
                if (cfd.Dropped) continue;
                try
                {
                    await FlushColumnFamilyAsync(cfd, wait: true, FlushReason.GetLiveFiles, synchronous, ct).ConfigureAwait(false);
                }
                catch (RocksDbException e) when (e.Code == StatusCode.ColumnFamilyDropped)
                {
                }
            }
        }
        var ret = new List<string>();
        ulong manifestNumber, manifestSize;
        lock (_mutex)
        {
            var seen = new HashSet<ulong>();
            foreach (ColumnFamilyData cfd in Versions.ColumnFamilies)
            {
                if (cfd.Dropped) continue;
                VersionStorageInfo s = cfd.Current.Storage;
                for (int l = 0; l < s.NumLevels; l++)
                {
                    foreach (FileMetaData f in s.LevelFiles(l))
                    {
                        if (seen.Add(f.FileNumber)) ret.Add("/" + FileNames.MakeTableFileName(f.FileNumber));
                    }
                }
                // Blob files of the current version (GetLiveFiles: BlobFileName("", number)).
                foreach (Blob.BlobFileMetaData b in s.BlobFiles)
                {
                    if (seen.Add(b.BlobFileNumber)) ret.Add(FileNames.BlobFileName("", b.BlobFileNumber));
                }
            }
            (manifestNumber, manifestSize) = Versions.InstalledManifest;
        }
        ret.Add("/" + FileNames.CurrentFileName);
        ret.Add("/" + FileNames.DescriptorFileName(manifestNumber));
        ulong options = LatestOptionsFileNumber();
        if (options != 0) ret.Add("/" + FileNames.OptionsFileName(options));
        return (ret, manifestSize);
    }

    /// <summary>Number of the newest OPTIONS file in the DB directory (0 if none).</summary>
    internal ulong LatestOptionsFileNumber()
    {
        ulong best = 0;
        foreach (string child in FileSystem.GetChildren(DbName))
        {
            if (FileNames.TryParseFileName(child, out ulong n, out FileType t) && t == FileType.OptionsFile && n > best) best = n;
        }
        return best;
    }

    // ---- WAL flush / sync ------------------------------------------------------------------------------

    /// <summary>Port of DBImpl::FlushWAL: pushes buffered WAL data to the OS (manual_wal_flush) and optionally syncs it.</summary>
    public async ValueTask FlushWalAsync(bool sync, bool synchronous, CancellationToken ct)
    {
        ThrowIfDisposed();
        if (_readOnly) ThrowHelper.NotSupported("FlushWAL is not supported in read-only mode");
        // While LockWAL holds a write stop no write can be inside the WAL stage (a stalled leader waits before it), so the
        // WAL is flushed without entering the write thread: a stalled leader holds it until UnlockWAL (upstream's
        // FlushWAL only takes log_write_mutex_).
        WriteThread.Writer? w = IsWalLocked ? null : await EnterUnbatchedAsync(synchronous, ct).ConfigureAwait(false);
        try
        {
            LogWriter? log = _log;
            if (log is null) return;
            if (synchronous)
            {
                log.WriteBuffer();
                if (sync)
                {
                    // SyncWAL: every WAL in logs_, oldest first.
                    if (_closedWalsToSync.Count > 0) SyncClosedWalsAsync(synchronous: true).GetAwaiter().GetResult();
                    log.Sync();
                    SyncWalDirIfNeeded();
                }
            }
            else
            {
                await log.WriteBufferAsync(CancellationToken.None).ConfigureAwait(false);
                if (sync)
                {
                    if (_closedWalsToSync.Count > 0) await SyncClosedWalsAsync(synchronous: false).ConfigureAwait(false);
                    await log.SyncAsync(CancellationToken.None).ConfigureAwait(false);
                    SyncWalDirIfNeeded();
                }
            }
        }
        catch (Exception e) when (e is not OperationCanceledException && e is not RocksDbException { Code: StatusCode.NotSupported })
        {
            SetBackgroundError(e, BackgroundErrorReason.WriteCallback, walRelated: true);
            throw;
        }
        finally
        {
            if (w is not null) ExitUnbatched(w, synchronous);
        }
    }
}

internal static class FileSystemReplicationExtensions
{
    /// <summary>DirectoryExists for file systems that may not implement it (falls back to listing).</summary>
    public static bool DirectoryExistsSafe(this Env.IFileSystem fs, string path)
    {
        try
        {
            return fs.DirectoryExists(path);
        }
        catch (RocksDbException e) when (e.Code == StatusCode.NotSupported)
        {
            try
            {
                fs.GetChildren(path);
                return true;
            }
            catch (RocksDbException)
            {
                return false;
            }
        }
    }
}
