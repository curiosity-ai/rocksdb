namespace RocksDb.Managed.Db;

/// <summary>
/// SstFileManager integration of the DB: the default manager of SanitizeOptions, TrackExistingDataFiles and
/// ReserveDiskBuffer at open, trash cleanup (DeleteScheduler::CleanupDirectory), DeleteDBFile (file/file_util.cc) and
/// the "Max allowed space was reached" checks after flushes and compactions (db/db_impl/db_impl_open.cc,
/// db/db_impl/db_impl_compaction_flush.cc). Files added by edits are reported by <c>VersionSet</c> when they are installed.
/// </summary>
internal sealed partial class DbImpl
{
    private bool _ownsSstFileManager;

    /// <summary>SanitizeOptions: every DB has an SstFileManager (a private one with immediate deletion by default).</summary>
    private void SetupSstFileManager()
    {
        if (Options.SstFileManager is not null) return;
        Options.SstFileManager = new SstFileManager(FileSystem, 0, 0.25, 64UL << 20);
        _ownsSstFileManager = true;
    }

    private void DisposeOwnedSstFileManager()
    {
        if (!_ownsSstFileManager) return;
        _ownsSstFileManager = false;
        Options.SstFileManager?.Dispose();
    }

    /// <summary>
    /// After open: reports the live table and blob files (TrackExistingDataFiles), reserves the memtable budget as the
    /// free space needed to clear a hard out-of-space error (ReserveDiskBuffer) and schedules the deletion of trash left
    /// by an earlier process (CleanupDirectory).
    /// </summary>
    private void StartSstFileManager()
    {
        if (Options.SstFileManager is not { } sfm) return;
        ulong maxWriteBufferSize = 0;
        lock (_mutex)
        {
            foreach (ColumnFamilyData cfd in Versions.ColumnFamilies)
            {
                maxWriteBufferSize = Math.Max(maxWriteBufferSize, cfd.Options.WriteBufferSize);
                VersionStorageInfo s = cfd.Current.Storage;
                for (int l = 0; l < s.NumLevels; l++)
                {
                    foreach (FileMetaData f in s.LevelFiles(l)) sfm.OnAddFile(FileNames.TableFileName(DbPaths, f.FileNumber, f.PathId), f.FileSize);
                }
                foreach (Blob.BlobFileMetaData b in s.BlobFiles)
                {
                    sfm.OnAddFile(FileNames.BlobFileName(DbPaths[0], b.BlobFileNumber),
                        b.TotalBlobBytes + Blob.BlobLogFormat.HeaderSize + Blob.BlobLogFormat.FooterSize);
                }
            }
        }
        sfm.ReserveDiskBuffer(maxWriteBufferSize, DbPaths[0]);
        foreach (string path in DbPaths) sfm.CleanupTrash(path);
    }

    /// <summary>VersionSet hook: a table or blob file became part of a version.</summary>
    internal void OnDataFileAdded(string path, ulong size) => Options.SstFileManager?.OnAddFile(path, size);

    /// <summary>
    /// FlushMemTableToOutputFile / CompactionJob::FinishCompactionOutputFile: with a space limit that the tracked files
    /// reached, the DB stops with SpaceLimit("Max allowed space was reached").
    /// </summary>
    internal void MaybeReportMaxAllowedSpaceReached(BackgroundErrorReason reason)
    {
        if (Options.SstFileManager is not { IsMaxAllowedSpaceReached: true }) return;
        SetBackgroundError(RocksDbException.SpaceLimit("Max allowed space was reached"), reason, walRelated: false);
    }

    /// <summary>DeleteDBFile: table and blob files go through the SstFileManager (rate limited trash deletion).</summary>
    private Exception? DeleteDbFile(string path, string directory)
    {
        try
        {
            if (Options.SstFileManager is { } sfm) sfm.ScheduleFileDeletion(path, directory, forceBackground: false);
            else FileSystem.DeleteFile(path);
            return null;
        }
        catch (Exception e) when (e is IOException or UnauthorizedAccessException or RocksDbException)
        {
            return e;
        }
    }
}
