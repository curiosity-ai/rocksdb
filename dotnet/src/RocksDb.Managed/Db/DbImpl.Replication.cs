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
    // Waiters for newly published sequence numbers (WAL tailers, replicas); see WaitForSequenceAsync.
    private readonly Util.AsyncBroadcastSignal _sequenceSignal = new();

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
    /// time_to_check of WalManager::PurgeObsoleteWALFiles: min(kDefaultIntervalToDeleteObsoleteWAL, max(1, ttl / 2))
    /// seconds with WAL_ttl_seconds, else kDefaultIntervalToDeleteObsoleteWAL (600).
    /// </summary>
    internal static long WalPurgeIntervalSeconds(ulong walTtlSeconds) =>
        walTtlSeconds > 0 ? (long)Math.Min((ulong)DefaultIntervalToDeleteObsoleteWal, Math.Max(1UL, walTtlSeconds / 2)) : DefaultIntervalToDeleteObsoleteWal;

    /// <summary>
    /// Port of WalManager::PurgeObsoleteWALFiles: deletes archived WALs older than WAL_ttl_seconds and, with
    /// WAL_size_limit_MB, the oldest archived WALs beyond the size budget. Runs at most every
    /// min(600s, max(1s, ttl/2)) unless <paramref name="force"/>.
    /// </summary>
    internal void PurgeObsoleteWalFiles(bool force)
    {
        bool ttlEnabled = Options.WalTtlSeconds > 0;
        bool sizeLimitEnabled = Options.WalSizeLimitMb > 0;
        if (!ttlEnabled && !sizeLimitEnabled) return;
        long now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        long timeToCheck = WalPurgeIntervalSeconds(Options.WalTtlSeconds);
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
        // need_seqno = false: every archived WAL is a candidate, also one without a readable first record.
        List<WalFileInfo> archived;
        try { archived = GetSortedWalsOfType(archivalDir, WalFileType.ArchivedLogFile, needSeqnos: false); }
        catch (RocksDbException) { return; }
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
    /// Port of WalManager::GetSortedWalFiles (need_seqnos = true): the alive WALs of wal_dir and (optionally) the
    /// archived ones, sorted by log number, each with the sequence number of its first record. Empty WALs are left out.
    /// A WAL seen in both places (archived while listing) is reported once, as archived.
    /// </summary>
    public List<WalFileInfo> GetSortedWalFiles(bool includeArchived) => GetSortedWalFiles(includeArchived, needSeqnos: true);

    /// <summary>
    /// Port of WalManager::GetSortedWalFiles. With <paramref name="needSeqnos"/> false (GetLiveFilesStorageInfo) the
    /// first records are not read: every WAL is listed, empty ones included, with a first sequence number of 0.
    /// </summary>
    public List<WalFileInfo> GetSortedWalFiles(bool includeArchived, bool needSeqnos)
    {
        // First get sorted files in db dir, then get sorted files from archived dir, to avoid a race condition where
        // a log file is moved to archived dir in between.
        List<WalFileInfo> logs = GetSortedWalsOfType(WalDir, WalFileType.AliveLogFile, needSeqnos);
        if (!includeArchived) return logs;
        string archivedir = FileNames.ArchivalDirectory(WalDir);
        List<WalFileInfo> files = FileSystem.DirectoryExistsSafe(archivedir)
            ? GetSortedWalsOfType(archivedir, WalFileType.ArchivedLogFile, needSeqnos)
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
    private List<WalFileInfo> GetSortedWalsOfType(string path, WalFileType type, bool needSeqnos)
    {
        var result = new List<WalFileInfo>();
        foreach (string f in FileSystem.GetChildren(path))
        {
            if (!FileNames.TryParseFileName(f, out ulong number, out FileType ft) || ft != FileType.WalFile) continue;
            ulong sequence = 0;
            if (needSeqnos)
            {
                sequence = ReadFirstRecord(type, number);
                if (sequence == 0) continue; // empty file
            }
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

    /// <summary>
    /// Port of WalManager::ReadFirstRecord: the first sequence number of a WAL, or 0 when it has no record (or was
    /// deleted from the archive meanwhile). An error reading a file that exists is thrown.
    /// </summary>
    private ulong ReadFirstRecord(WalFileType type, ulong number)
    {
        lock (_readFirstRecordCache)
        {
            if (_readFirstRecordCache.TryGetValue(number, out ulong cached)) return cached;
        }
        ulong sequence = 0;
        RocksDbException? status = null;
        if (type == WalFileType.AliveLogFile)
        {
            string fname = FileNames.LogFileName(WalDir, number);
            status = ReadFirstLine(fname, number, out sequence);
            // Return any error that is not caused by non-existing file.
            if (status is not null && FileSystem.FileExists(fname)) throw status.Rethrowable();
        }
        if (type == WalFileType.ArchivedLogFile || status is not null)
        {
            // Check if the file got moved to the archive.
            string archived = FileNames.ArchivedLogFileName(WalDir, number);
            status = ReadFirstLine(archived, number, out sequence);
            // Maybe the file was deleted from the archive dir: the caller identifies this as an empty file.
            if (status is not null && !FileSystem.FileExists(archived)) return 0;
        }
        if (status is not null) throw status.Rethrowable();
        if (sequence != 0)
        {
            lock (_readFirstRecordCache) _readFirstRecordCache[number] = sequence;
        }
        return sequence;
    }

    /// <summary>The LogReporter of WalManager::ReadFirstLine: keeps the first error only.</summary>
    private sealed class FirstLineReporter : LogReporter
    {
        public RocksDbException? Status;

        public override void Corruption(long bytes, StatusCode code, string reason, ulong logNumber) =>
            Status ??= code == StatusCode.Corruption ? RocksDbException.Corruption(reason) : new RocksDbException(code, "IO error: " + reason);
    }

    /// <summary>
    /// Port of WalManager::ReadFirstLine: reads the first record only. Returns null with the sequence number of that
    /// record, null with 0 for a file without a record (1 for an empty compressed WAL, which holds only the
    /// kSetCompressionType record and must still be listed), or the error: the file cannot be opened, corruption was
    /// reported before the first record (ignored without paranoid_checks) or the first record is too small to be a
    /// write batch.
    /// </summary>
    private RocksDbException? ReadFirstLine(string fname, ulong number, out ulong sequence)
    {
        sequence = 0;
        Env.ISequentialFile file;
        try
        {
            file = FileSystem.OpenSequential(fname);
        }
        catch (RocksDbException e)
        {
            return e;
        }
        catch (Exception e) when (e is IOException or UnauthorizedAccessException)
        {
            return RocksDbException.FromException(e);
        }
        var reporter = new FirstLineReporter();
        using var reader = new LogReader(file, reporter, checksum: true, number);
        if (reader.TryReadRecord(out ReadOnlySpan<byte> record) && (reporter.Status is null || !Options.ParanoidChecks))
        {
            if (record.Length < WriteBatch.HeaderSize)
            {
                reporter.Corruption(record.Length, StatusCode.Corruption, "log record too small", number);
            }
            else
            {
                sequence = Util.Coding.DecodeFixed64(record);
                return null;
            }
        }
        // ReadRecord might have returned false on EOF, which means that the log file is empty. Or, a failure may have
        // occurred while processing the first entry.
        if (reporter.Status is null && reader.IsCompressedAndEmptyFile) sequence = 1;
        return reporter.Status;
    }

    /// <summary>The number of the WAL currently written to (0 if none); no file system access.</summary>
    public ulong CurrentWalNumber
    {
        get
        {
            lock (_mutex) return _logNumber;
        }
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
        // DBImpl::GetUpdatesSince: the iterator assumes one sequence number per update (batch sequence + count - 1).
        if (_seqPerBatch) ThrowHelper.NotSupported("This API is not yet compatible with write-prepared/write-unprepared transactions");
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
    /// (and by read-only/secondary catch-up and close). A full fence and one read when nobody waits; the fence also
    /// orders the published sequence before the waiter check (see <see cref="WaitForSequenceAsync"/>).
    /// </summary>
    internal void NotifySequenceWaiters() => _sequenceSignal.SignalAll();

    /// <summary>
    /// Completes when the DB has published sequence number <paramref name="sequence"/> (LastSequence &gt;=
    /// sequence) or when the DB is closed (returns false then). Allocation-free: synchronous when already satisfied, and
    /// a wait uses a pooled waiter node and a pooled state machine (cancellation included).
    /// </summary>
    public ValueTask<bool> WaitForSequenceAsync(ulong sequence, CancellationToken cancellationToken)
    {
        if (Versions.LastSequence >= sequence) return new ValueTask<bool>(true);
        return WaitForSequenceSlowAsync(sequence, cancellationToken);
    }

    [System.Runtime.CompilerServices.AsyncMethodBuilder(typeof(System.Runtime.CompilerServices.PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<bool> WaitForSequenceSlowAsync(ulong sequence, CancellationToken cancellationToken)
    {
        while (Versions.LastSequence < sequence)
        {
            if (IsDisposed) return false;
            Util.AsyncBroadcastSignal.Waiter waiter = _sequenceSignal.Register();
            // Re-check after registering (Register's fence pairs with the one in NotifySequenceWaiters; close sets
            // _disposed with an interlocked exchange before notifying).
            if (Versions.LastSequence >= sequence || IsDisposed)
            {
                _sequenceSignal.Unregister(waiter);
                continue;
            }
            await _sequenceSignal.WaitAsync(waiter, cancellationToken).ConfigureAwait(false);
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
            // DBImpl::FlushWAL / SyncWAL: WALIOStatusCheck.
            if (WalErrorStopsWrites(e)) SetBackgroundError(e, BackgroundErrorReason.WriteCallback, walRelated: true);
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
