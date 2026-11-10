using System.Globalization;
using System.Text;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Utilities.Backup;

/// <summary>
/// Creates, deletes and restores backups of a DB. Port of rocksdb::BackupEngine (utilities/backup/backup_engine.cc,
/// include/rocksdb/utilities/backup_engine.h).
/// <para>
/// Layout of the backup directory (identical to native RocksDB, which can read and extend it):
/// <c>meta/&lt;id&gt;</c> (text meta files with crc32c checksums), <c>private/&lt;id&gt;/</c> (MANIFEST, CURRENT, OPTIONS,
/// WALs), <c>shared_checksum/</c> (table and blob files shared between backups, named
/// <c>&lt;number&gt;_s&lt;db_session_id&gt;_&lt;size&gt;.sst</c> or <c>&lt;number&gt;_&lt;crc32c&gt;_&lt;size&gt;.blob</c>) or <c>shared/</c>.
/// Table and blob files are recognized by their file type, so any file the DB reports as live is backed up.
/// </para>
/// </summary>
public sealed class BackupEngine : BackupEngineReadOnly
{
    private BackupEngine(BackupEngineOptions options, IFileSystem fileSystem)
        : base(options, fileSystem, readOnly: false)
    {
    }

    /// <summary>Opens (creating the directories if needed) the backup directory on the local file system (BackupEngine::Open).</summary>
    public static new BackupEngine Open(BackupEngineOptions options) => Open(options, LocalFileSystem.Instance);

    /// <summary>
    /// Opens the backup directory. <paramref name="fileSystem"/> is the file system restores write to (and holds the
    /// backup directory unless <see cref="BackupEngineOptions.BackupFileSystem"/> is set). Files of the backed up DB are
    /// read through the DB's own file system.
    /// </summary>
    public static new BackupEngine Open(BackupEngineOptions options, IFileSystem fileSystem)
    {
        var engine = new BackupEngine(options, fileSystem);
        engine.Initialize();
        return engine;
    }

    // -----------------------------------------------------------------------------------------------------
    // Create

    /// <summary>Creates a backup of <paramref name="db"/> and returns its id (BackupEngine::CreateNewBackup).</summary>
    public uint CreateNewBackup(RocksDatabase db, bool flushBeforeBackup) =>
        CreateNewBackup(db, new CreateBackupOptions { FlushBeforeBackup = flushBeforeBackup });

    public uint CreateNewBackup(RocksDatabase db, CreateBackupOptions options) => CreateNewBackupWithMetadata(db, "", options);

    /// <summary>Creates a backup carrying <paramref name="appMetadata"/> (at most 1 MiB, UTF-8).</summary>
    public uint CreateNewBackupWithMetadata(RocksDatabase db, string appMetadata, CreateBackupOptions options) =>
        CreateCoreAsync(db, appMetadata, options, synchronous: true, CancellationToken.None).GetAwaiter().GetResult();

    public ValueTask<uint> CreateNewBackupAsync(RocksDatabase db, bool flushBeforeBackup, CancellationToken cancellationToken = default) =>
        CreateNewBackupAsync(db, new CreateBackupOptions { FlushBeforeBackup = flushBeforeBackup }, cancellationToken);

    public ValueTask<uint> CreateNewBackupAsync(RocksDatabase db, CreateBackupOptions options, CancellationToken cancellationToken = default) =>
        CreateNewBackupWithMetadataAsync(db, "", options, cancellationToken);

    public async ValueTask<uint> CreateNewBackupWithMetadataAsync(RocksDatabase db, string appMetadata, CreateBackupOptions options,
        CancellationToken cancellationToken = default) =>
        await CreateCoreAsync(db, appMetadata, options, synchronous: false, cancellationToken).ConfigureAwait(false);

    /// <summary>
    /// Makes a running <see cref="CreateNewBackup(RocksDatabase, bool)"/> fail with Incomplete as soon as possible, and
    /// every later one too (BackupEngine::StopBackup).
    /// </summary>
    public void StopBackup() => _stopBackup = true;

    /// <summary>Port of BackupEngineImpl::CreateNewBackupWithMetadata.</summary>
    private async ValueTask<uint> CreateCoreAsync(RocksDatabase database, string appMetadata, CreateBackupOptions options, bool synchronous, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentNullException.ThrowIfNull(appMetadata);
        ArgumentNullException.ThrowIfNull(options);
        ThrowIfDisposed();
        if (_stopBackup) throw new RocksDbException(StatusCode.Incomplete, "Incomplete: Backup stopped");
        CheckAppMetadata(appMetadata);
        if (options.ExcludeFilesCallback is not null && _options.SchemaVersion < 2)
        {
            ThrowHelper.InvalidArgument("exclude_files_callback requires schema_version >= 2");
        }
        DbImpl db = database.Impl;
        db.ThrowIfDisposed();
        await AcquireAsync(synchronous, ct).ConfigureAwait(false);
        uint newId = _latestBackupId + 1;
        try
        {
            string privateDir = PrivateDir(newId);
            if (_backupFs.DirectoryExistsSafe(privateDir))
            {
                // Maybe last backup failed and left partial state behind, clean it up.
                GarbageCollectLocked();
            }
            var meta = new BackupMeta(GetBackupMetaFile(newId, false), GetBackupMetaFile(newId, true), _backupedFileInfos);
            _backups[newId] = meta;
            meta.RecordTimestamp();
            meta.AppMetadata = Encoding.UTF8.GetBytes(appMetadata);
            long start = System.Diagnostics.Stopwatch.GetTimestamp();
            LogInfo("Started the backup process -- creating backup " + newId);
            if (_options.ShareTableFiles && !_options.ShareFilesWithChecksum)
            {
                LogWarn("BackupEngineOptions::share_files_with_checksum=false is DEPRECATED and could lead to data loss.");
            }
            try
            {
                _backupFs.CreateDirectoryIfMissing(privateDir);
                var liveDstPaths = new HashSet<string>(StringComparer.Ordinal);
                db.DisableFileDeletions();
                try
                {
                    // CheckpointImpl::CreateCustomCheckpoint: the sequence number is read before the live files.
                    ulong sequenceNumber = db.Versions.LastSequence;
                    ulong walSizeForFlush = options.FlushBeforeBackup || db.Options.Allow2Pc ? 0 : ulong.MaxValue;
                    // compare_checksum: with the DB's crc32c checksum factory the live table / blob file checksums (crc32c
                    // of the whole file, the backup engine's own checksum) come from the MANIFEST instead of being
                    // computed, and the copies are verified against them.
                    bool compareChecksum = db.Options.FileChecksumGenFactory?.Name == FileChecksumGenCrc32cFactory.ClassName;
                    List<LiveFileStorageInfo> infos = await db.GetLiveFilesStorageInfoAsync(walSizeForFlush, compareChecksum, synchronous, ct)
                        .ConfigureAwait(false);
                    string? dir = null;
                    foreach (LiveFileStorageInfo info in infos)
                    {
                        if (info.FileType == FileType.WalFile) continue;
                        if (dir is null) dir = info.Directory;
                        else if (dir != info.Directory) ThrowHelper.NotSupported("db_paths / cf_paths not supported for Checkpoint nor BackupEngine");
                    }
                    // Decide every file's destination first (sequential: the reuse checks read the backup directory
                    // state); then the copies run on max_background_operations workers.
                    var items = new List<BackupWorkItem>(infos.Count);
                    var excludable = new List<BackupWorkItem>();
                    foreach (LiveFileStorageInfo info in infos)
                    {
                        if (info.FileType == FileType.WalFile && !_options.BackupLogFiles) continue;
                        LogInfo("add file for backup " + info.RelativeFileName);
                        if (info.ReplacementContents is not null)
                        {
                            if (info.Size != (ulong)info.ReplacementContents.Length) ThrowHelper.Corruption("Inconsistent size metadata for " + info.RelativeFileName);
                            BackupFileEntry created = await CreatePrivateFileAsync(newId, info.RelativeFileName, info.ReplacementContents, liveDstPaths,
                                synchronous, ct).ConfigureAwait(false);
                            items.Add(new BackupWorkItem { Done = created });
                            continue;
                        }
                        bool sharable = info.FileType is FileType.TableFile or FileType.BlobFile;
                        ulong sizeBytes = sharable ? (ulong)db.FileSystem.GetFileSize(info.Directory + "/" + info.RelativeFileName) : info.Size;
                        bool sharedChecksum = _options.ShareFilesWithChecksum && sharable;
                        BackupWorkItem item = await PrepareBackupFileAsync(newId, liveDstPaths, _options.ShareTableFiles && sharable, info.Directory,
                            info.RelativeFileName, db.FileSystem, info.FileType, sizeBytes, info.Size, sharedChecksum, info.FileChecksumFuncName,
                            info.FileChecksum, synchronous, ct).ConfigureAwait(false);
                        // AddBackupFileWorkItem: with an exclude_files_callback, shared_checksum files that need a copy are
                        // excludable and only dispatched after the callback.
                        if (options.ExcludeFilesCallback is not null && item.Shared && sharedChecksum && item.NeedToCopy) excludable.Add(item);
                        else items.Add(item);
                    }
                    if (options.ExcludeFilesCallback is { } exclude && excludable.Count > 0)
                    {
                        var maybe = new MaybeExcludeBackupFile[excludable.Count];
                        for (int i = 0; i < maybe.Length; i++) maybe[i] = new MaybeExcludeBackupFile(excludable[i].DstRelative);
                        try
                        {
                            exclude(maybe);
                        }
                        catch (Exception e) when (e is not OutOfMemoryException)
                        {
                            throw new RocksDbException(StatusCode.Aborted, "Operation aborted: Exception in exclude_files_callback: " + e.Message, StatusSubCode.None, e);
                        }
                        for (int i = 0; i < maybe.Length; i++)
                        {
                            if (maybe[i].ExcludeDecision) meta.AddExcludedFile(excludable[i].DstRelative);
                            else items.Add(excludable[i]);
                        }
                    }
                    Action progress = options.ProgressCallback ?? NoProgress;
                    await RunParallelAsync(items.Count, (i, sync, token) => CopyBackupFileAsync(items[i], progress, sync, token), synchronous, ct)
                        .ConfigureAwait(false);
                    // BackupAfterCopyOrCreateWorkItem, in dispatch order: install shared copies and record the files.
                    foreach (BackupWorkItem item in items)
                    {
                        if (item.Done is null)
                        {
                            if (item.Shared && item.NeedToCopy) _backupFs.RenameFile(item.CopyDestPath, item.FinalDestPath);
                            item.Done = new BackupFileEntry(item.DstRelative, item.Size, item.Crc, item.DbSessionId);
                        }
                        meta.AddFile(item.Done);
                    }
                    meta.SequenceNumber = sequenceNumber;
                }
                finally
                {
                    // We copied all the files, enable file deletions.
                    db.EnableFileDeletions(force: false);
                }
                // Persist the backup metadata on the disk.
                await meta.StoreToFileAsync(_backupFs, _options.Sync, _options.SchemaVersion, synchronous, ct).ConfigureAwait(false);
                if (_options.Sync)
                {
                    _backupFs.SyncDirectory(privateDir);
                    _backupFs.SyncDirectory(GetAbsolutePath(PrivateDirName));
                    _backupFs.SyncDirectory(GetAbsolutePath(MetaDirName));
                    if (_options.ShareTableFiles) _backupFs.SyncDirectory(GetAbsolutePath(_options.ShareFilesWithChecksum ? SharedChecksumDirName : SharedDirName));
                    _backupFs.SyncDirectory(BackupDir);
                }
            }
            catch (Exception e) when (e is not OutOfMemoryException)
            {
                LogInfo("Backup failed -- " + e.Message);
                // Delete files that we might have already written.
                _mightNeedGarbageCollect = true;
                try
                {
                    DeleteBackupNoGcLocked(newId);
                    GarbageCollectLocked();
                }
                catch (Exception cleanup) when (cleanup is IOException or RocksDbException)
                {
                }
                if (e is RocksDbException or OperationCanceledException or ObjectDisposedException) throw;
                throw RocksDbException.FromException(e).Rethrowable();
            }
            // Here we know that we succeeded and installed the new backup.
            _latestBackupId = newId;
            _latestValidBackupId = newId;
            LogInfo("Backup DONE. All is good");
            LogInfo("Backup number of files: " + meta.NumberFiles);
            LogInfo("Backup size: " + meta.Size + " bytes");
            LogInfo("Backup time: " + (long)System.Diagnostics.Stopwatch.GetElapsedTime(start).TotalMicroseconds + " microseconds");
            return newId;
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>The create_file_cb of the backup (CURRENT): written to the private directory from its contents.</summary>
    private async ValueTask<BackupFileEntry> CreatePrivateFileAsync(uint backupId, string fname, string contents, HashSet<string> liveDstPaths,
        bool synchronous, CancellationToken ct)
    {
        string rel = GetPrivateFileRel(backupId, false, fname);
        string path = GetAbsolutePath(rel);
        liveDstPaths.Add(path);
        byte[] bytes = Encoding.ASCII.GetBytes(contents);
        IWritableFile f = _backupFs.CreateWritable(path, truncate: true);
        try
        {
            if (synchronous)
            {
                f.Append(bytes);
                if (_options.Sync) f.Sync();
            }
            else
            {
                await f.AppendAsync(bytes, ct).ConfigureAwait(false);
                if (_options.Sync) await f.SyncAsync(ct).ConfigureAwait(false);
            }
        }
        finally
        {
            if (synchronous) f.Dispose();
            else await f.DisposeAsync().ConfigureAwait(false);
        }
        return new BackupFileEntry(rel, (ulong)bytes.Length, Crc32C.Value(bytes), "");
    }

    /// <summary>One file of a backup being created (CopyOrCreateWorkItem + BackupAfterCopyOrCreateWorkItem).</summary>
    private sealed class BackupWorkItem
    {
        public string SrcPath = "";
        public IFileSystem? SrcFs;
        public string DstRelative = "";
        public string CopyDestPath = "";
        public string FinalDestPath = "";
        public ulong SizeLimit;
        public ulong Size;
        public uint? Crc;
        public string DbSessionId = "";
        public bool Shared;
        public bool NeedToCopy;
        /// <summary>Set for items that need no copy step (created files).</summary>
        public BackupFileEntry? Done;
    }

    private ShareFilesNaming NamingNoFlags => _options.ShareFilesWithChecksumNaming & ShareFilesNaming.MaskNoNamingFlags;

    /// <summary>
    /// GetSharedFileWithChecksum: <c>&lt;number&gt;_&lt;crc32c&gt;_&lt;size&gt;.&lt;ext&gt;</c> (legacy naming, or no session id), otherwise
    /// <c>&lt;number&gt;_s&lt;session&gt;[_&lt;size&gt;].&lt;ext&gt;</c>.
    /// </summary>
    private string GetSharedFileWithChecksum(string file, uint? crc, ulong fileSize, string dbSessionId)
    {
        int dot = file.LastIndexOf('.');
        if (NamingNoFlags == ShareFilesNaming.LegacyCrc32cAndFileSize || dbSessionId.Length == 0)
        {
            return file.Insert(dot, "_" + crc!.Value.ToString(CultureInfo.InvariantCulture) + "_" + fileSize.ToString(CultureInfo.InvariantCulture));
        }
        string s = file.Insert(dot, "_s" + dbSessionId);
        if ((_options.ShareFilesWithChecksumNaming & ShareFilesNaming.FlagIncludeFileSize) != 0)
        {
            s = s.Insert(s.LastIndexOf('.'), "_" + fileSize.ToString(CultureInfo.InvariantCulture));
        }
        return s;
    }

    /// <summary>
    /// Port of BackupEngineImpl::AddBackupFileWorkItem (decision part): picks the destination (shared_checksum, shared or
    /// private) and skips files the backup directory already holds (reusing their known checksum). The copy runs later
    /// (<see cref="CopyBackupFileAsync"/>), shared ones through a temporary name renamed into place afterwards. A source
    /// checksum from the DB's MANIFEST (<paramref name="srcChecksumFuncName"/> = kDbFileChecksumFuncName,
    /// "FileChecksumCrc32c") is used as the file's crc32c: no extra read for the legacy shared_checksum name, and the
    /// copy is verified against it.
    /// </summary>
    private async ValueTask<BackupWorkItem> PrepareBackupFileAsync(uint backupId, HashSet<string> liveDstPaths, bool shared, string srcDir, string fname,
        IFileSystem srcFs, FileType fileType, ulong sizeBytes, ulong sizeLimit, bool sharedChecksum, string srcChecksumFuncName, byte[] srcChecksum,
        bool synchronous, CancellationToken ct)
    {
        string srcPath = srcDir + "/" + fname;
        uint? crc = null;
        // Step 0: Check if default checksum function name is passed in.
        if (srcChecksumFuncName == FileChecksumGenCrc32cFactory.Crc32cFuncName)
        {
            if (srcChecksum.Length == 0) throw new RocksDbException(StatusCode.Aborted, "Operation aborted: Unknown checksum value for " + fname);
            if (srcChecksum.Length != 4) ThrowHelper.Corruption("Invalid crc32c file checksum of " + fname);
            // ChecksumStrToHex / ChecksumHexToInt32: the factory stores the crc32c big endian.
            crc = System.Buffers.Binary.BinaryPrimitives.ReadUInt32BigEndian(srcChecksum);
        }
        string dbSessionId = "";
        string dstRelative, dstRelativeTmp = "";
        if (shared && sharedChecksum)
        {
            if (NamingNoFlags != ShareFilesNaming.LegacyCrc32cAndFileSize && fileType != FileType.BlobFile)
            {
                // Prepare db_session_id to add to the file name (empty for old files). Any other failure to read the
                // table (GetFileDbIdentities: not NotFound) fails the backup.
                dbSessionId = GetFileDbSessionId(srcFs, srcPath);
            }
            // Calculate the checksum when no db session id is available (the name then contains it).
            if (crc is null && dbSessionId.Length == 0) crc = await ComputeChecksumAsync(srcFs, srcPath, sizeLimit, _options.BackupRateLimiter, synchronous, ct).ConfigureAwait(false);
            string name = GetSharedFileWithChecksum(fname, crc, sizeBytes, dbSessionId);
            dstRelativeTmp = GetSharedFileWithChecksumRel(name, true);
            dstRelative = GetSharedFileWithChecksumRel(name, false);
        }
        else if (shared)
        {
            dstRelativeTmp = GetSharedFileRel(fname, true);
            dstRelative = GetSharedFileRel(fname, false);
        }
        else
        {
            dstRelative = GetPrivateFileRel(backupId, false, fname);
        }
        string finalDestPath = GetAbsolutePath(dstRelative);
        string copyDestPath = dstRelativeTmp.Length > 0 ? GetAbsolutePath(dstRelativeTmp) : finalDestPath;

        // If it's shared, we also need to check if it exists -- if it does, no need to copy it again.
        bool needToCopy = true;
        bool samePath = liveDstPaths.Contains(finalDestPath);
        bool fileExists = shared && !samePath && _backupFs.FileExists(finalDestPath);
        if (shared && (samePath || fileExists))
        {
            needToCopy = false;
            bool known = _backupedFileInfos.TryGetValue(dstRelative, out BackupFileEntry? existing);
            if (!known && !samePath)
            {
                LogInfo(fname + " already present, but not referenced by any backup. We will overwrite the file.");
                needToCopy = true;
                try { _backupFs.DeleteFile(finalDestPath); }
                catch (Exception e) when (e is IOException or RocksDbException) { }
            }
            else if (crc is null)
            {
                // Copy the prior known checksum instead of reading the file again (fast incremental backups).
                crc = !samePath && existing?.Crc32c is uint known2
                    ? known2
                    : await ComputeChecksumAsync(srcFs, srcPath, sizeLimit, _options.BackupRateLimiter, synchronous, ct).ConfigureAwait(false);
                LogInfo(fname + " already present, with checksum " + BackupEngineReadOnly.ChecksumToHex(crc.Value) + " and size " + sizeBytes);
            }
        }
        liveDstPaths.Add(finalDestPath);
        return new BackupWorkItem
        {
            SrcPath = srcPath,
            SrcFs = srcFs,
            DstRelative = dstRelative,
            CopyDestPath = copyDestPath,
            FinalDestPath = finalDestPath,
            SizeLimit = sizeLimit,
            Size = sizeBytes,
            Crc = crc,
            DbSessionId = dbSessionId,
            Shared = shared,
            NeedToCopy = needToCopy,
        };
    }

    /// <summary>The copy step of a work item (CopyOrCreateFile), run by one of the copy workers.</summary>
    private async ValueTask CopyBackupFileAsync(BackupWorkItem item, Action progress, bool synchronous, CancellationToken ct)
    {
        if (item.Done is not null || !item.NeedToCopy) return;
        LogInfo("Copying " + item.SrcPath + " to " + item.CopyDestPath);
        uint copiedCrc = await CopyFileAsync(item.SrcFs!, item.SrcPath, _backupFs, item.CopyDestPath, item.SizeLimit, _options.Sync, _options.BackupRateLimiter,
            progress, synchronous, ct).ConfigureAwait(false);
        if (item.Crc is uint expected && expected != copiedCrc)
        {
            // CopyEngine: the checksum known before the copy (computed, or the DB's crc32c file checksum) must match.
            ThrowHelper.Corruption("Checksum mismatch after copying to " + item.CopyDestPath + ": Expected checksum is " +
                BackupEngineReadOnly.ChecksumToHex(expected) + " while computed checksum is " + BackupEngineReadOnly.ChecksumToHex(copiedCrc));
        }
        item.Crc = copiedCrc;
        item.Size = (ulong)_backupFs.GetFileSize(item.CopyDestPath);
    }

    private static readonly Action NoProgress = static () => { };

    // -----------------------------------------------------------------------------------------------------
    // Delete

    /// <summary>Deletes all but the newest <paramref name="numBackupsToKeep"/> backups and their unreferenced files.</summary>
    public void PurgeOldBackups(uint numBackupsToKeep)
    {
        ThrowIfDisposed();
        _lock.Wait();
        try
        {
            PurgeOldBackupsLocked(numBackupsToKeep);
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>Deletes backup <paramref name="backupId"/> (valid or corrupt) and the files only it referenced.</summary>
    public void DeleteBackup(uint backupId)
    {
        ThrowIfDisposed();
        _lock.Wait();
        try
        {
            Exception? first = null;
            try { DeleteBackupNoGcLocked(backupId); }
            catch (RocksDbException e) { first = e; }
            // Clean up after any incomplete backup deletion, potentially from earlier session.
            if (_mightNeedGarbageCollect)
            {
                try { GarbageCollectLocked(); }
                catch (Exception e) when (e is IOException or RocksDbException) { first ??= e; }
            }
            if (first is not null) throw RocksDbException.FromException(first).Rethrowable();
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>Deletes files of the backup directory no backup references (left by failed or interrupted operations).</summary>
    public void GarbageCollect()
    {
        ThrowIfDisposed();
        _lock.Wait();
        try
        {
            GarbageCollectLocked();
        }
        finally
        {
            _lock.Release();
        }
    }
}
