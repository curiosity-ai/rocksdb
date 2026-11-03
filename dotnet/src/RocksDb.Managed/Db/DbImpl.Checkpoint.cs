using System.Buffers;
using System.Text;

namespace RocksDb.Managed.Db;

/// <summary>
/// A live file to be captured by a checkpoint/backup. Port of LiveFileStorageInfo (include/rocksdb/metadata.h).
/// </summary>
internal sealed record LiveFileStorageInfo(string Directory, string RelativeFileName, ulong FileNumber, FileType FileType, ulong Size,
    bool TrimToSize, string? ReplacementContents)
{
    /// <summary>
    /// file_checksum_func_name with include_checksum_info: the MANIFEST's function name of a table / blob file, or
    /// kUnknownFileChecksumFuncName ("Unknown"); empty when checksum info was not requested.
    /// </summary>
    public string FileChecksumFuncName { get; init; } = "";

    /// <summary>file_checksum with include_checksum_info (kUnknownFileChecksum, empty, when unknown).</summary>
    public byte[] FileChecksum { get; init; } = [];
}

/// <summary>
/// Checkpoints. Ports DBImpl::GetLiveFilesStorageInfo (db/db_filesnapshot.cc) and CheckpointImpl::CreateCheckpoint /
/// CreateCustomCheckpoint (utilities/checkpoint/checkpoint_impl.cc): with file deletions disabled, optionally flush
/// the memtables, hard link the table files and OPTIONS (falling back to copies across file systems), copy the
/// MANIFEST truncated to the size matching the captured versions, write CURRENT, and copy/link the live WALs (the
/// last one copied up to its current size), into "&lt;dir&gt;.tmp" which is then renamed to the checkpoint directory.
/// </summary>
internal sealed partial class DbImpl
{
    /// <summary>
    /// Port of CheckpointImpl::CreateCheckpoint. Returns the DB's latest sequence number at the start of the
    /// checkpoint (all writes up to it are in the checkpoint). <paramref name="logSizeForFlush"/>: 0 always flushes
    /// the memtables first; ulong.MaxValue never flushes (WALs are copied instead); otherwise flushes only when the
    /// live WALs total at least that many bytes.
    /// </summary>
    public async ValueTask<ulong> CreateCheckpointAsync(string checkpointDir, ulong logSizeForFlush, bool synchronous, CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrEmpty(checkpointDir);
        ThrowIfDisposed();
        if (FileSystem.DirectoryExistsSafe(checkpointDir) || FileSystem.FileExists(checkpointDir)) ThrowHelper.InvalidArgument("Directory exists");
        string trimmed = checkpointDir.TrimEnd('/');
        if (trimmed.Length == 0) ThrowHelper.InvalidArgument("invalid checkpoint directory name");
        string fullPrivatePath = trimmed + ".tmp";
        if (FileSystem.DirectoryExistsSafe(fullPrivatePath))
        {
            throw RocksDbException.IOError("While mkdir: " + fullPrivatePath + ": File exists");
        }
        FileSystem.CreateDirectoryIfMissing(fullPrivatePath);
        ulong sequenceNumber;
        try
        {
            DisableFileDeletions();
            try
            {
                sequenceNumber = await CreateCustomCheckpointAsync(fullPrivatePath, logSizeForFlush, synchronous, ct).ConfigureAwait(false);
            }
            finally
            {
                // We copied all the files, enable file deletions.
                EnableFileDeletions(force: false);
            }
            // Move tmp private backup to real snapshot directory.
            FileSystem.RenameFile(fullPrivatePath, trimmed);
            FileSystem.SyncDirectory(trimmed);
            string? parent = System.IO.Path.GetDirectoryName(trimmed);
            if (!string.IsNullOrEmpty(parent)) FileSystem.SyncDirectory(parent);
        }
        catch
        {
            // Clean all the files we might have created.
            CleanupDirectory(fullPrivatePath);
            throw;
        }
        return sequenceNumber;
    }

    private void CleanupDirectory(string dir)
    {
        try
        {
            foreach (string child in FileSystem.GetChildren(dir)) TryDeleteFile(dir + "/" + child);
            FileSystem.DeleteDirectory(dir);
        }
        catch (Exception e) when (e is IOException or RocksDbException or UnauthorizedAccessException)
        {
        }
    }

    /// <summary>Port of CheckpointImpl::CreateCustomCheckpoint with the default link / copy / create callbacks.</summary>
    private async ValueTask<ulong> CreateCustomCheckpointAsync(string dest, ulong logSizeForFlush, bool synchronous, CancellationToken ct)
    {
        ulong sequenceNumber = Versions.LastSequence;
        // CheckpointImpl::CreateCheckpoint: get_live_table_checksum = false (the copies are not verified).
        List<LiveFileStorageInfo> infos = await GetLiveFilesStorageInfoAsync(logSizeForFlush, includeChecksumInfo: false, synchronous, ct)
            .ConfigureAwait(false);
        // Verify that everything except WAL files are in same directory (db_paths / cf_paths not supported).
        string? dir = null;
        foreach (LiveFileStorageInfo info in infos)
        {
            if (info.FileType == FileType.WalFile) continue;
            if (dir is null) dir = info.Directory;
            else if (dir != info.Directory) ThrowHelper.NotSupported("db_paths / cf_paths not supported for Checkpoint nor BackupEngine");
        }
        bool sameFs = true;
        foreach (LiveFileStorageInfo info in infos)
        {
            ct.ThrowIfCancellationRequested();
            string target = dest + "/" + info.RelativeFileName;
            if (info.ReplacementContents is not null)
            {
                // Currently should only be used for CURRENT file.
                if (info.Size != (ulong)info.ReplacementContents.Length) ThrowHelper.Corruption("Inconsistent size metadata for " + info.RelativeFileName);
                await CreateFileAsync(target, info.ReplacementContents, synchronous, ct).ConfigureAwait(false);
                continue;
            }
            if (sameFs && !info.TrimToSize)
            {
                try
                {
                    FileSystem.LinkFile(info.Directory + "/" + info.RelativeFileName, target);
                    continue;
                }
                catch (RocksDbException e) when (e.Code == StatusCode.NotSupported)
                {
                    sameFs = false;
                }
            }
            await CopyFileAsync(info.Directory + "/" + info.RelativeFileName, target, info.Size, synchronous, ct).ConfigureAwait(false);
        }
        return sequenceNumber;
    }

    /// <summary>
    /// Port of DBImpl::GetLiveFilesStorageInfo: table and blob files, MANIFEST (trimmed to the size matching the
    /// captured versions), CURRENT (replacement contents), OPTIONS and the live WALs (the last one trimmed to its
    /// current size). With <paramref name="includeChecksumInfo"/> (LiveFilesStorageInfoOptions::include_checksum_info)
    /// table and blob files carry their MANIFEST file checksum and function name, every other file the unknown ones.
    /// </summary>
    internal async ValueTask<List<LiveFileStorageInfo>> GetLiveFilesStorageInfoAsync(ulong walSizeForFlush, bool includeChecksumInfo, bool synchronous,
        CancellationToken ct)
    {
        var results = new List<LiveFileStorageInfo>();
        bool flushMemtable = !_readOnly;
        if (walSizeForFlush == ulong.MaxValue)
        {
            flushMemtable = false;
        }
        else if (walSizeForFlush > 0 && flushMemtable)
        {
            // If the outstanding WAL files are small, we skip the flush.
            ulong total = 0;
            foreach (WalFileInfo w in GetSortedWalFiles(includeArchived: false)) total += w.SizeBytes;
            if (total < walSizeForFlush) flushMemtable = false;
        }
        if (flushMemtable)
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

        ulong manifestNumber, manifestSize, minLogNum, maxLogNum;
        lock (_mutex)
        {
            var seen = new HashSet<ulong>();
            foreach (ColumnFamilyData cfd in Versions.ColumnFamilies)
            {
                if (cfd.Dropped) continue;
                VersionStorageInfo vsi = cfd.Current.Storage;
                for (int level = 0; level < vsi.NumLevels; level++)
                {
                    foreach (FileMetaData meta in vsi.LevelFiles(level))
                    {
                        if (!seen.Add(meta.FileNumber)) continue;
                        string directory = DbPaths[(int)Math.Min(meta.PathId, (uint)(DbPaths.Count - 1))];
                        var info = new LiveFileStorageInfo(directory, FileNames.MakeTableFileName(meta.FileNumber), meta.FileNumber, FileType.TableFile,
                            meta.FileSize, TrimToSize: false, null);
                        results.Add(includeChecksumInfo ? WithChecksum(info, meta.FileChecksumFuncName, meta.FileChecksum) : info);
                    }
                }
                // Blob files live in cf_paths.front() (GetLiveFilesStorageInfo: blob_files of the current version).
                foreach (Blob.BlobFileMetaData b in vsi.BlobFiles)
                {
                    if (!seen.Add(b.BlobFileNumber)) continue;
                    var info = new LiveFileStorageInfo(DbPaths[0], FileNames.BlobFileName(b.BlobFileNumber), b.BlobFileNumber, FileType.BlobFile,
                        b.TotalBlobBytes + Blob.BlobLogFormat.HeaderSize + Blob.BlobLogFormat.FooterSize, TrimToSize: false, null);
                    results.Add(includeChecksumInfo ? WithChecksum(info, b.Addition.ChecksumMethod, b.Addition.ChecksumValue) : info);
                }
            }
            (manifestNumber, manifestSize) = Versions.InstalledManifest;
            if (manifestNumber == 0)
            {
                // Read-only / secondary instances never wrote a MANIFEST: capture the current one in full.
                manifestNumber = Versions.ManifestFileNumber;
                manifestSize = (ulong)FileSystem.GetFileSize(FileNames.DescriptorFileName(DbName, manifestNumber));
            }
            minLogNum = MinLogNumberToKeepLive();
            maxLogNum = _logNumber == 0 ? ulong.MaxValue : _logNumber;
        }

        string manifestFname = FileNames.DescriptorFileName(manifestNumber);
        results.Add(new LiveFileStorageInfo(DbName, manifestFname, manifestNumber, FileType.DescriptorFile, manifestSize, TrimToSize: true, null));
        // CURRENT could be replaced so we have to record the contents as needed.
        string current = manifestFname + "\n";
        results.Add(new LiveFileStorageInfo(DbName, FileNames.CurrentFileName, 0, FileType.CurrentFile, (ulong)current.Length, TrimToSize: false, current));
        ulong options = LatestOptionsFileNumber();
        if (options != 0)
        {
            results.Add(new LiveFileStorageInfo(DbName, FileNames.OptionsFileName(options), options, FileType.OptionsFile,
                (ulong)FileSystem.GetFileSize(FileNames.OptionsFileName(DbName, options)), TrimToSize: false, null));
        }

        // FlushWAL is required to ensure we can physically copy everything logically written to the WAL.
        if (!_readOnly) await FlushWalAsync(sync: false, synchronous, ct).ConfigureAwait(false);

        List<WalFileInfo> liveWalFiles = GetSortedWalFiles(includeArchived: false);
        // Link WAL files. Copy exact size of last one because it is the only one that has changes after the last flush.
        for (int i = 0; i < liveWalFiles.Count; i++)
        {
            WalFileInfo w = liveWalFiles[i];
            if (w.Type == WalFileType.AliveLogFile && (!flushMemtable || w.LogNumber >= minLogNum) && w.LogNumber <= maxLogNum)
            {
                bool trim = i + 1 == liveWalFiles.Count;
                results.Add(new LiveFileStorageInfo(WalDir, FileNames.LogFileName(w.LogNumber), w.LogNumber, FileType.WalFile, w.SizeBytes, trim, null));
            }
        }
        if (includeChecksumInfo)
        {
            // MANIFEST, CURRENT, OPTIONS and WAL files: kUnknownFileChecksumFuncName / kUnknownFileChecksum.
            for (int i = 0; i < results.Count; i++)
            {
                if (results[i].FileChecksumFuncName.Length == 0) results[i] = WithChecksum(results[i], "", []);
            }
        }
        return results;
    }

    /// <summary>The checksum info of a live file; an empty function name becomes kUnknownFileChecksumFuncName.</summary>
    private static LiveFileStorageInfo WithChecksum(LiveFileStorageInfo info, string funcName, byte[] checksum) =>
        funcName.Length == 0
            ? info with { FileChecksumFuncName = FileMetaData.UnknownFileChecksumFuncName, FileChecksum = [] }
            : info with { FileChecksumFuncName = funcName, FileChecksum = checksum };

    /// <summary>Port of CreateFile (file/file_util.cc): writes <paramref name="contents"/> and syncs.</summary>
    private async ValueTask CreateFileAsync(string path, string contents, bool synchronous, CancellationToken ct)
    {
        byte[] bytes = Encoding.ASCII.GetBytes(contents);
        Env.IWritableFile f = FileSystem.CreateWritable(path, truncate: true);
        try
        {
            if (synchronous)
            {
                f.Append(bytes);
                f.Sync();
            }
            else
            {
                await f.AppendAsync(bytes, ct).ConfigureAwait(false);
                await f.SyncAsync(ct).ConfigureAwait(false);
            }
        }
        finally
        {
            if (synchronous) f.Dispose();
            else await f.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Port of CopyFile (file/file_util.cc): copies the first <paramref name="size"/> bytes (0 = the whole file) and
    /// syncs the copy. Fails if the source is shorter.
    /// </summary>
    internal async ValueTask CopyFileAsync(string source, string destination, ulong size, bool synchronous, CancellationToken ct)
    {
        if (size == 0) size = (ulong)FileSystem.GetFileSize(source);
        using Env.ISequentialFile src = FileSystem.OpenSequential(source);
        Env.IWritableFile dst = FileSystem.CreateWritable(destination, truncate: true);
        byte[] buffer = ArrayPool<byte>.Shared.Rent(64 * 1024);
        try
        {
            ulong remaining = size;
            while (remaining > 0)
            {
                int want = (int)Math.Min((ulong)buffer.Length, remaining);
                int n = synchronous ? src.Read(buffer.AsSpan(0, want)) : await src.ReadAsync(buffer.AsMemory(0, want), ct).ConfigureAwait(false);
                if (n <= 0) ThrowHelper.Corruption("file too small: " + source);
                if (synchronous) dst.Append(buffer.AsSpan(0, n));
                else await dst.AppendAsync(buffer.AsMemory(0, n), ct).ConfigureAwait(false);
                remaining -= (ulong)n;
            }
            if (synchronous) dst.Sync();
            else await dst.SyncAsync(ct).ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
            if (synchronous) dst.Dispose();
            else await dst.DisposeAsync().ConfigureAwait(false);
        }
    }
}
