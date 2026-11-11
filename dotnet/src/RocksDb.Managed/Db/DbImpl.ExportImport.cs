namespace RocksDb.Managed.Db;

/// <summary>
/// Column family export and import. Ports of CheckpointImpl::ExportColumnFamily / ExportFilesInMetaData
/// (utilities/checkpoint/checkpoint_impl.cc) and DBImpl::CreateColumnFamilyWithImport (db/db_impl/db_impl.cc) with
/// <see cref="ImportColumnFamilyJob"/>.
/// </summary>
internal sealed partial class DbImpl
{
    /// <summary>
    /// CheckpointImpl::ExportColumnFamily: flushes the column family, hard-links (or, across file systems, copies) its
    /// live table files into "<paramref name="exportDir"/>.tmp" with file deletions disabled, renames that to
    /// <paramref name="exportDir"/> and returns the files' metadata (with <see cref="LiveFileMetadata.Directory"/> =
    /// <paramref name="exportDir"/>). Blob files are not exported (like upstream).
    /// </summary>
    public async ValueTask<ExportImportFilesMetadata> ExportColumnFamilyAsync(ColumnFamilyData cfd, string exportDir, bool synchronous, CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrEmpty(exportDir);
        ThrowIfDisposed();
        if (FileSystem.DirectoryExistsSafe(exportDir) || FileSystem.FileExists(exportDir)) ThrowHelper.InvalidArgument("Specified export_dir exists");
        string trimmed = exportDir.TrimEnd('/');
        if (trimmed.Length == 0) ThrowHelper.InvalidArgument("Specified export_dir invalid");
        string tmpExportDir = trimmed + ".tmp";
        bool movedToUserDir = false;
        try
        {
            // Env::CreateDir fails on an existing directory: a staging directory left behind by an interrupted export
            // fails this export, and the failure path below removes it (so the next attempt succeeds).
            if (FileSystem.DirectoryExistsSafe(tmpExportDir)) throw RocksDbException.IOError("While mkdir: " + tmpExportDir + ": File exists");
            FileSystem.CreateDirectoryIfMissing(tmpExportDir);
            // FIXME upstream: should respect atomic_flush and flush all CFs if needed.
            if (!_readOnly) await FlushColumnFamilyAsync(cfd, wait: true, FlushReason.ManualFlush, synchronous, ct).ConfigureAwait(false);
            var files = new List<(int Level, FileMetaData File)>();
            DisableFileDeletions();
            try
            {
                SuperVersion sv = GetReferencedSuperVersion(cfd);
                try
                {
                    VersionStorageInfo vs = sv.Current.Storage;
                    for (int l = 0; l < vs.NumLevels; l++)
                    {
                        foreach (FileMetaData f in vs.LevelFiles(l)) files.Add((l, f));
                    }
                }
                finally
                {
                    ReturnSuperVersion(sv);
                }
                // ExportFilesInMetaData: hard link, falling back to copies when the first link is not supported.
                bool hardlink = true;
                foreach ((int _, FileMetaData f) in files)
                {
                    ct.ThrowIfCancellationRequested();
                    string fname = "/" + FileNames.MakeTableFileName(f.FileNumber);
                    string src = DbPaths[(int)Math.Min(f.PathId, (uint)(DbPaths.Count - 1))] + fname;
                    if (hardlink)
                    {
                        try
                        {
                            FileSystem.LinkFile(src, tmpExportDir + fname);
                            continue;
                        }
                        catch (RocksDbException e) when (e.Code == StatusCode.NotSupported)
                        {
                            hardlink = false;
                        }
                    }
                    await CopyFileAsync(src, tmpExportDir + fname, 0, synchronous, ct).ConfigureAwait(false);
                }
            }
            finally
            {
                EnableFileDeletions(force: false);
            }
            // Move temporary export directory to the actual export directory, then fsync it.
            FileSystem.RenameFile(tmpExportDir, trimmed);
            movedToUserDir = true;
            FileSystem.SyncDirectory(trimmed);

            var result = new List<LiveFileMetadata>(files.Count);
            foreach ((int level, FileMetaData f) in files)
            {
                result.Add(new LiveFileMetadata("/" + FileNames.MakeTableFileName(f.FileNumber), trimmed, f.FileNumber, cfd.Name, level, f.FileSize,
                    DbFormat.ExtractUserKey(f.Smallest).ToArray(), DbFormat.ExtractUserKey(f.Largest).ToArray(), f.SmallestSeqno, f.LargestSeqno,
                    f.EpochNumber)
                {
                    SmallestInternalKey = f.Smallest,
                    LargestInternalKey = f.Largest,
                    OldestBlobFileNumber = f.OldestBlobFileNumber,
                    Temperature = f.Temperature,
                    OldestAncestorTime = f.OldestAncestorTime,
                    FileCreationTime = f.FileCreationTime,
                    FileChecksum = f.FileChecksum,
                    FileChecksumFuncName = f.FileChecksumFuncName,
                });
            }
            return new ExportImportFilesMetadata(cfd.UserComparator.Name, result);
        }
        catch
        {
            // Failure: Clean up all the files/directories created.
            CleanupDirectory(movedToUserDir ? trimmed : tmpExportDir);
            throw;
        }
    }

    /// <summary>
    /// DBImpl::CreateColumnFamilyWithImport: checks the comparator names, creates the column family, reserves file
    /// numbers (persisted by a dummy edit so a crash after linking can't reuse them), prepares the files, then with writes
    /// stopped installs the import edit. On failure the column family is dropped again.
    /// </summary>
    public async ValueTask<ColumnFamilyData> CreateColumnFamilyWithImportAsync(string name, ColumnFamilyOptions options,
        ImportColumnFamilyOptions importOptions, IReadOnlyList<ExportImportFilesMetadata> metadatas, bool synchronous, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(importOptions);
        ArgumentNullException.ThrowIfNull(metadatas);
        ThrowIfDisposed();
        if (_readOnly) ThrowReadOnly();
        string cfComparatorName = options.ComparatorName;
        foreach (ExportImportFilesMetadata m in metadatas)
        {
            ArgumentNullException.ThrowIfNull(m);
            if (cfComparatorName != m.DbComparatorName) ThrowHelper.InvalidArgument("Comparator name mismatch");
        }
        ColumnFamilyData cfd = await CreateColumnFamilyAsync(name, options, synchronous, ct).ConfigureAwait(false);
        var job = new ImportColumnFamilyJob(this, cfd, importOptions, metadatas);
        bool ok = false;
        bool keepFiles = false;
        try
        {
            lock (_mutex)
            {
                // Don't import files when there is a bg_error.
                if (IsDbStopped && ErrorHandler.BgError is { } bg) ThrowBackgroundError(bg);
            }
            await job.PrepareAsync(ReserveFileNumberForIngestion, synchronous, ct).ConfigureAwait(false);
            // If a crash happens after a hard link was established, recovery must not reuse the file numbers already
            // assigned to the internal files: persist the next file number with a dummy edit.
            var dummy = new VersionEdit { ColumnFamily = cfd.Id };
            dummy.MarkForegroundOperation(); // dummy_edit.MarkForegroundOperation()
            await Versions.LogAndApplyAsync(cfd, dummy, synchronous, ct).ConfigureAwait(false);

            // Stop writes to the DB by entering the write thread(s).
            WriteThread.Writer w = await EnterUnbatchedAsync(synchronous, ct).ConfigureAwait(false);
            BeginIngestion();
            try
            {
                if (cfd.Dropped) ThrowDropped(cfd);
                VersionEdit edit;
                ulong largestSeqno;
                lock (_mutex) edit = job.Run(out largestSeqno);
                TestHooks.Invoke("DBImpl::CreateColumnFamilyWithImport:BeforeLogAndApply");
                try
                {
                    await Versions.LogAndApplyAsync(cfd, edit, synchronous, ct).ConfigureAwait(false);
                }
                catch
                {
                    // After a MANIFEST I/O error (VersionSet::io_status_) the import record may be durable in the
                    // MANIFEST that CURRENT names: the imported files must stay (see IngestExternalFilesAsync).
                    keepFiles = Versions.IoStatus is not null;
                    throw;
                }
                lock (_mutex)
                {
                    // Imported files keep their sequence numbers: the DB's last sequence must cover them.
                    if (largestSeqno > Versions.LastSequence)
                    {
                        Versions.SetLastSequence(largestSeqno);
                        Versions.SetLastPublishedSequence(largestSeqno);
                    }
                    InstallSuperVersion(cfd);
                }
                ok = true;
            }
            finally
            {
                EndIngestion();
                ExitUnbatched(w, synchronous);
            }
        }
        finally
        {
            lock (_mutex)
            {
                foreach (ulong n in job.ReservedFileNumbers) _pendingOutputs.Remove(n);
            }
            if (ok || !keepFiles) job.Cleanup(ok);
            if (!ok)
            {
                try
                {
                    await DropColumnFamilyAsync(cfd, synchronous, CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception e) when (e is RocksDbException or IOException or ObjectDisposedException)
                {
                    // DropColumnFamily failed: upstream only logs it.
                }
            }
        }
        if (ok) NotifySequenceWaiters();
        return cfd;
    }
}
