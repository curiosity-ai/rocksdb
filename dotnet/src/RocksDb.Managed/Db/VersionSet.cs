using RocksDb.Managed.Env;

namespace RocksDb.Managed.Db;

/// <summary>A file that no live version references any more (VersionSet obsolete_files_ / ObsoleteFileInfo).</summary>
internal readonly record struct ObsoleteTableFile(FileMetaData Meta, ColumnFamilyData Cfd);

/// <summary>A blob file that no live version references any more (VersionSet obsolete_blob_files_ / ObsoleteBlobFileInfo).</summary>
internal readonly record struct ObsoleteBlobFile(ulong BlobFileNumber, ColumnFamilyData Cfd);

/// <summary>
/// The set of column families with their current versions, plus the DB-wide counters persisted in the MANIFEST
/// (next file number, last sequence, min log number to keep) and the MANIFEST writer. Port of rocksdb::VersionSet
/// (db/version_set.cc: LogAndApply / ProcessManifestWrites / WriteCurrentStateToManifest / NewFileNumber /
/// MarkFileNumberUsed / SetLastSequence) and ColumnFamilySet (db/column_family.cc).
/// <para>
/// <b>Synchronization.</b> Column family membership, versions and file references are guarded by the DB mutex
/// (<see cref="DbImpl.Mutex"/>). MANIFEST writes are serialized by <see cref="_manifestLock"/>, which is held across
/// the I/O (the C++ manifest_writers_ queue); the DB mutex is never held during I/O. Sequence numbers and the file
/// number counter are atomics.
/// </para>
/// </summary>
internal sealed class VersionSet : IAsyncDisposable
{
    private readonly DbImpl _db;
    private readonly IFileSystem _fs;
    private readonly string _dbname;
    private readonly SemaphoreSlim _manifestLock = new(1, 1);
    private readonly Dictionary<uint, ColumnFamilyData> _cfs = new();
    private readonly Dictionary<ulong, int> _fileRefs = new();
    private readonly List<ObsoleteTableFile> _obsoleteFiles = new();
    // Blob files: references from live versions (the shared_ptr<SharedBlobFileMetaData> counts upstream).
    private readonly Dictionary<ulong, (int Refs, ulong FileSize, ColumnFamilyData Cfd)> _blobFileRefs = new();
    private readonly List<ObsoleteBlobFile> _obsoleteBlobFiles = new();

    private ulong _nextFileNumber = 2;
    private ulong _lastSequence;          // last published sequence (visible to readers)
    private ulong _lastAllocatedSequence; // last sequence handed out to a write group
    private ulong _versionNumber;
    private ulong _descriptorLastSequence; // last_sequence recorded in the MANIFEST so far (descriptor_last_sequence_), guarded by _manifestLock
    private ManifestLogWriter? _manifestLog;
    private ManifestWriter? _manifest;

    public VersionSet(DbImpl db, IFileSystem fs, string dbname)
    {
        _db = db;
        _fs = fs;
        _dbname = dbname;
    }

    public string? DbId { get; set; }
    public uint MaxColumnFamily { get; set; }
    public ulong MinLogNumberToKeep { get; private set; }
    public ulong ManifestFileNumber { get; private set; }
    public ulong PrevLogNumber { get; set; }
    /// <summary>
    /// MANIFEST number and size as of the last installed edit (manifest_file_number_ / manifest_file_size_), consistent
    /// with the installed versions. REQUIRES: DB mutex.
    /// </summary>
    public (ulong Number, ulong Size) InstalledManifest { get; set; }

    public IReadOnlyCollection<ColumnFamilyData> ColumnFamilies => _cfs.Values;

    // ---- counters -------------------------------------------------------------------------------

    /// <summary>VersionSet::NewFileNumber.</summary>
    public ulong NewFileNumber() => Interlocked.Increment(ref _nextFileNumber) - 1;

    public ulong CurrentNextFileNumber => Volatile.Read(ref _nextFileNumber);

    /// <summary>VersionSet::MarkFileNumberUsed: ensures future file numbers are &gt; <paramref name="number"/>.</summary>
    public void MarkFileNumberUsed(ulong number)
    {
        ulong cur = Volatile.Read(ref _nextFileNumber);
        while (cur <= number)
        {
            ulong o = Interlocked.CompareExchange(ref _nextFileNumber, number + 1, cur);
            if (o == cur) return;
            cur = o;
        }
    }

    public void SetNextFileNumber(ulong value) => Volatile.Write(ref _nextFileNumber, value);

    /// <summary>The last sequence number visible to readers (LastSequence / LastPublishedSequence).</summary>
    public ulong LastSequence => Volatile.Read(ref _lastSequence);

    /// <summary>Publishes <paramref name="s"/> to readers (SetLastSequence). Only the write leader calls this.</summary>
    public void SetLastSequence(ulong s)
    {
        Volatile.Write(ref _lastSequence, s);
        if (s > _lastAllocatedSequence) _lastAllocatedSequence = s;
    }

    /// <summary>Allocates <paramref name="count"/> sequence numbers; returns the first. Write leader only.</summary>
    public ulong AllocateSequence(ulong count)
    {
        ulong first = _lastAllocatedSequence + 1;
        _lastAllocatedSequence += count;
        return first;
    }

    public ulong LastAllocatedSequence => _lastAllocatedSequence;

    // ---- column families ------------------------------------------------------------------------

    public ColumnFamilyData? GetColumnFamily(uint id) => _cfs.GetValueOrDefault(id);

    public ColumnFamilyData? GetColumnFamily(string name)
    {
        foreach (ColumnFamilyData c in _cfs.Values)
        {
            if (c.Name == name) return c;
        }
        return null;
    }

    /// <summary>Registers a column family with an initial version built from <paramref name="files"/>. REQUIRES: DB mutex.</summary>
    public void AddColumnFamily(ColumnFamilyData cfd, FileMetaData[][]? files) => AddColumnFamily(cfd, files, null);

    /// <summary>
    /// Registers a column family with an initial version built from <paramref name="files"/> and the recovered
    /// <paramref name="blobFiles"/> (null = none). REQUIRES: DB mutex.
    /// </summary>
    public void AddColumnFamily(ColumnFamilyData cfd, FileMetaData[][]? files, IEnumerable<BlobFileState>? blobFiles)
    {
        _cfs[cfd.Id] = cfd;
        if (cfd.Id > MaxColumnFamily) MaxColumnFamily = cfd.Id;
        Blob.BlobFileMetaData[] blobs = [];
        if (blobFiles is not null)
        {
            var baseList = new List<Blob.BlobFileMetaData>();
            foreach (BlobFileState b in blobFiles) baseList.Add(new Blob.BlobFileMetaData(b.Addition, b.GarbageBlobCount, b.GarbageBlobBytes, []));
            blobs = Blob.BlobFileMetaData.Build(baseList, null, files ?? []);
        }
        var storage = new VersionStorageInfo(cfd.Icmp, Math.Max(cfd.NumLevels, files?.Length ?? 0), cfd.Options, files, blobs);
        var v = new DbVersion(cfd, storage, ++_versionNumber);
        InstallVersion(cfd, v);
    }

    /// <summary>Makes <paramref name="v"/> the current version of <paramref name="cfd"/>. REQUIRES: DB mutex.</summary>
    private void InstallVersion(ColumnFamilyData cfd, DbVersion v)
    {
        RefFiles(v);
        v.Ref();
        cfd.LiveVersions++;
        DbVersion? old = cfd.Current;
        cfd.Current = v;
        if (old is not null) UnrefVersion(old);
    }

    private void RefFiles(DbVersion v)
    {
        VersionStorageInfo s = v.Storage;
        for (int l = 0; l < s.NumLevels; l++)
        {
            foreach (FileMetaData f in s.LevelFiles(l))
            {
                _fileRefs[f.FileNumber] = _fileRefs.GetValueOrDefault(f.FileNumber) + 1;
            }
        }
        foreach (Blob.BlobFileMetaData b in s.BlobFiles)
        {
            int refs = _blobFileRefs.TryGetValue(b.BlobFileNumber, out var e) ? e.Refs : 0;
            _blobFileRefs[b.BlobFileNumber] = (refs + 1, b.TotalBlobBytes + Blob.BlobLogFormat.HeaderSize + Blob.BlobLogFormat.FooterSize, v.Cfd);
        }
    }

    /// <summary>Drops a version reference; files no longer referenced become obsolete. REQUIRES: DB mutex.</summary>
    public void UnrefVersion(DbVersion v)
    {
        if (!v.Unref()) return;
        v.Cfd.LiveVersions--;
        VersionStorageInfo s = v.Storage;
        for (int l = 0; l < s.NumLevels; l++)
        {
            foreach (FileMetaData f in s.LevelFiles(l))
            {
                int r = _fileRefs[f.FileNumber] - 1;
                if (r == 0)
                {
                    _fileRefs.Remove(f.FileNumber);
                    _obsoleteFiles.Add(new ObsoleteTableFile(f, v.Cfd));
                }
                else
                {
                    _fileRefs[f.FileNumber] = r;
                }
            }
        }
        foreach (Blob.BlobFileMetaData b in s.BlobFiles)
        {
            var e = _blobFileRefs[b.BlobFileNumber];
            if (e.Refs == 1)
            {
                _blobFileRefs.Remove(b.BlobFileNumber);
                _obsoleteBlobFiles.Add(new ObsoleteBlobFile(b.BlobFileNumber, v.Cfd));
            }
            else
            {
                _blobFileRefs[b.BlobFileNumber] = (e.Refs - 1, e.FileSize, e.Cfd);
            }
        }
    }

    /// <summary>
    /// ColumnFamilyData::GetTotalBlobFileSize: total size of the blob files referenced by any live version of
    /// <paramref name="cfd"/>. REQUIRES: DB mutex.
    /// </summary>
    public ulong TotalBlobFileSize(ColumnFamilyData cfd)
    {
        ulong total = 0;
        foreach (var e in _blobFileRefs.Values)
        {
            if (e.Cfd == cfd) total += e.FileSize;
        }
        return total;
    }

    /// <summary>True if some live version references the blob file. REQUIRES: DB mutex.</summary>
    public bool IsLiveBlobFile(ulong blobFileNumber) => _blobFileRefs.ContainsKey(blobFileNumber);

    /// <summary>Moves the obsolete blob files to <paramref name="into"/>. REQUIRES: DB mutex.</summary>
    public void TakeObsoleteBlobFiles(List<ObsoleteBlobFile> into)
    {
        into.AddRange(_obsoleteBlobFiles);
        _obsoleteBlobFiles.Clear();
    }

    /// <summary>True if some live version references the table file. REQUIRES: DB mutex.</summary>
    public bool IsLiveFile(ulong fileNumber) => _fileRefs.ContainsKey(fileNumber);

    /// <summary>Moves the obsolete table files to <paramref name="into"/>. REQUIRES: DB mutex.</summary>
    public void TakeObsoleteFiles(List<ObsoleteTableFile> into)
    {
        into.AddRange(_obsoleteFiles);
        _obsoleteFiles.Clear();
    }

    /// <summary>Minimum log number with unflushed data over live column families (PreComputeMinLogNumberWithUnflushedData).</summary>
    public ulong MinLogNumberWithUnflushedData(ColumnFamilyData? exclude)
    {
        ulong min = ulong.MaxValue;
        foreach (ColumnFamilyData c in _cfs.Values)
        {
            if (c == exclude || c.Dropped) continue;
            min = Math.Min(min, c.LogNumber);
        }
        return min;
    }

    // ---- MANIFEST -------------------------------------------------------------------------------

    private RocksDbException? _ioError;

    /// <summary>
    /// The error of the last failed MANIFEST (or CURRENT) write, cleared by the next successful one (VersionSet::io_status).
    /// While set, the next edit starts a new MANIFEST.
    /// </summary>
    public RocksDbException? IoStatus => Volatile.Read(ref _ioError);

    /// <summary>
    /// Applies <paramref name="edit"/> to <paramref name="cfd"/> (null for DB-wide edits), records it in the MANIFEST
    /// (rolling to a new MANIFEST with a full snapshot when there is none yet or it exceeds max_manifest_file_size) and
    /// installs the resulting version. Port of VersionSet::LogAndApply / ProcessManifestWrites. With
    /// <paramref name="synchronous"/> all I/O is blocking and the returned task is already complete.
    /// For column family creation the caller passes the new <paramref name="cfd"/>; it is registered after the
    /// MANIFEST write succeeds.
    /// </summary>
    public async ValueTask LogAndApplyAsync(ColumnFamilyData? cfd, VersionEdit edit, bool synchronous, CancellationToken cancellationToken)
    {
        if (synchronous) _manifestLock.Wait(cancellationToken);
        else await _manifestLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            RecoveredVersionState? snapshot = null;
            FileMetaData[][]? newFiles = null;
            Blob.BlobFileMetaData[]? newBlobFiles = null;
            bool newManifest;
            lock (_db.Mutex)
            {
                // ProcessManifestWrites: edits of a column family dropped meanwhile are not written (a later edit for a
                // dropped CF id would make the MANIFEST unrecoverable: "MANIFEST record referencing unknown column family").
                // The drop itself is applied under _manifestLock, so this check cannot race with it.
                if (cfd is not null && cfd.Dropped && !edit.IsColumnFamilyAdd)
                {
                    throw new RocksDbException(StatusCode.ColumnFamilyDropped, "Column family drop: column family " + cfd.Name + " was dropped");
                }
                if (edit.IsColumnFamilyAdd)
                {
                    edit.MaxColumnFamily = Math.Max(MaxColumnFamily, edit.ColumnFamily);
                }
                else if (cfd is not null && !edit.IsColumnFamilyDrop && (edit.NewFiles.Count > 0 || edit.DeletedFiles.Count > 0 ||
                                                                         edit.BlobFileAdditions.Count > 0 || edit.BlobFileGarbages.Count > 0))
                {
                    VersionStorageInfo cur = cfd.Current.Storage;
                    newFiles = cur.ApplyEdit(edit, cfd.NumLevels);
                    newBlobFiles = Blob.BlobFileMetaData.Build(cur.BlobFiles, edit, newFiles);
                }
                newManifest = _manifest is null || (ulong)_manifestLog!.Log.File!.Size >= _db.Options.MaxManifestFileSize;
                if (newManifest)
                {
                    snapshot = BuildSnapshotState();
                }
                // Like ProcessManifestWrites / LogAndApplyHelper: the edit carries next_file_number and a last_sequence that
                // never goes backwards and covers the largest seqno of the files it adds (ingested files are assigned a
                // seqno above the published last sequence, which is only advanced after this edit is durable).
                ulong lastSeq = Math.Max(LastSequence, _descriptorLastSequence);
                if (edit.LastSequence is ulong own && own > lastSeq) lastSeq = own;
                edit.LastSequence = lastSeq;
                _descriptorLastSequence = lastSeq;
            }

            TestHooks.Invoke("VersionSet::LogAndApply:BeforeManifestWrite");
            ulong manifestNumber = 0;
            ManifestLogWriter? newLog = null;
            bool newManifestWritten = false;
            try
            {
                if (newManifest)
                {
                    manifestNumber = NewFileNumber();
                    snapshot!.NextFileNumber = CurrentNextFileNumber;
                    newLog = ManifestLogWriter.Create(_fs, FileNames.DescriptorFileName(_dbname, manifestNumber));
                    var writer = new ManifestWriter(newLog);
                    // WriteCurrentStateToManifest (synced at the end).
                    await Await(writer.WriteSnapshotAsync(snapshot, static _ => 0, cancellationToken), synchronous).ConfigureAwait(false);
                    edit.NextFileNumber = CurrentNextFileNumber;
                    await Await(writer.AddEditAsync(edit, 0, sync: true, cancellationToken), synchronous).ConfigureAwait(false);
                    newManifestWritten = true;
                    await Await(FileNames.SetCurrentFileAsync(_fs, _dbname, manifestNumber, syncDirectory: true, cancellationToken), synchronous)
                        .ConfigureAwait(false);
                    ManifestLogWriter? old = _manifestLog;
                    _manifestLog = newLog;
                    _manifest = writer;
                    newLog = null;
                    ulong oldNumber = ManifestFileNumber;
                    ManifestFileNumber = manifestNumber;
                    if (oldNumber != 0) _db.ScheduleDeleteObsoleteManifest(oldNumber);
                    try { old?.Dispose(); }
                    catch (Exception e) when (e is IOException or RocksDbException) { }
                }
                else
                {
                    edit.NextFileNumber = CurrentNextFileNumber;
                    await Await(_manifest!.AddEditAsync(edit, 0, sync: true, cancellationToken), synchronous).ConfigureAwait(false);
                }
            }
            catch (Exception writeError)
            {
                // VersionSet::io_status_: remembered so the error handler can classify the failure as a MANIFEST write
                // error and recovery forces a new MANIFEST.
                if (writeError is not OperationCanceledException) Volatile.Write(ref _ioError, RocksDbException.FromException(writeError));
                // If the MANIFEST append failed for whatever reason, the file could be corrupted (torn record), and after a
                // failed CURRENT update it is unknown which MANIFEST CURRENT names. Force the next version update to start a
                // new MANIFEST and point CURRENT at it (ProcessManifestWrites: descriptor_log_.reset()).
                if (_manifestLog is not null)
                {
                    try { _manifestLog.Dispose(); }
                    catch (Exception e) when (e is IOException or RocksDbException) { }
                    _manifestLog = null;
                    _manifest = null;
                }
                if (newLog is not null)
                {
                    try { newLog.Dispose(); }
                    catch (Exception e) when (e is IOException or RocksDbException) { }
                    // Only delete the new MANIFEST if writing it failed: if only the CURRENT update failed, CURRENT may
                    // already name it (e.g. rename done, directory fsync failed), so it must stay.
                    if (!newManifestWritten)
                    {
                        try { _fs.DeleteFile(FileNames.DescriptorFileName(_dbname, manifestNumber)); }
                        catch (IOException) { }
                        catch (RocksDbException) { }
                    }
                }
                throw;
            }

            Volatile.Write(ref _ioError, null);
            lock (_db.Mutex)
            {
                InstalledManifest = (ManifestFileNumber, (ulong)_manifestLog!.Log.File!.Size);
                if (edit.MinLogNumberToKeep is ulong minLog && minLog > MinLogNumberToKeep) MinLogNumberToKeep = minLog;
                if (edit.IsColumnFamilyAdd)
                {
                    if (cfd is not null)
                    {
                        if (edit.LogNumber is ulong ln) cfd.LogNumber = ln;
                        AddColumnFamily(cfd, null);
                    }
                }
                else if (edit.IsColumnFamilyDrop)
                {
                    if (cfd is not null)
                    {
                        cfd.Dropped = true;
                        _cfs.Remove(cfd.Id);
                    }
                }
                else if (cfd is not null)
                {
                    if (newFiles is not null)
                    {
                        var storage = new VersionStorageInfo(cfd.Icmp, newFiles.Length, cfd.Options, newFiles, newBlobFiles ?? []);
                        InstallVersion(cfd, new DbVersion(cfd, storage, ++_versionNumber));
                    }
                    if (edit.LogNumber is ulong ln && ln > cfd.LogNumber) cfd.LogNumber = ln;
                    if (edit.FullHistoryTsLow is { Length: > 0 } tsLow && (cfd.FullHistoryTsLow is not { } cur
                        || cfd.UserComparator.CompareTimestamp(tsLow, cur) > 0))
                    {
                        // ColumnFamilyData::SetFullHistoryTsLow (only increases).
                        cfd.FullHistoryTsLow = tsLow;
                    }
                }
            }
        }
        finally
        {
            _manifestLock.Release();
        }
    }

    private static ValueTask Await(ValueTask t, bool synchronous)
    {
        // Synchronous callers block here (cold path: MANIFEST I/O) so the enclosing async method completes synchronously.
        if (synchronous && !t.IsCompleted)
        {
            t.AsTask().GetAwaiter().GetResult();
            return default;
        }
        return t;
    }

    /// <summary>Builds the state written at the start of a new MANIFEST (WriteCurrentStateToManifest). REQUIRES: DB mutex.</summary>
    private RecoveredVersionState BuildSnapshotState()
    {
        var state = new RecoveredVersionState
        {
            DbId = _db.Options.WriteDbIdToManifest ? DbId : null,
            LastSequence = LastSequence,
            MaxColumnFamily = MaxColumnFamily,
            MinLogNumberToKeep = MinLogNumberToKeep,
            PrevLogNumber = PrevLogNumber,
        };
        state.ColumnFamilies.Clear();
        var ids = new List<uint>(_cfs.Keys);
        ids.Sort();
        foreach (uint id in ids)
        {
            ColumnFamilyData c = _cfs[id];
            if (c.Dropped) continue;
            var cv = new ColumnFamilyVersion
            {
                Id = c.Id,
                Name = c.Name,
                ComparatorName = c.UserComparator.Name,
                PersistUserDefinedTimestamps = c.Options.PersistUserDefinedTimestamps,
                LogNumber = c.LogNumber,
                FullHistoryTsLow = c.FullHistoryTsLow,
            };
            VersionStorageInfo s = c.Current.Storage;
            for (int l = 0; l < s.NumLevels; l++)
            {
                List<FileMetaData> level = cv.MutableLevel(l);
                foreach (FileMetaData f in s.LevelFiles(l)) level.Add(f);
            }
            foreach (Blob.BlobFileMetaData b in s.BlobFiles)
            {
                cv.BlobFiles[b.BlobFileNumber] = new BlobFileState
                {
                    Addition = b.Addition,
                    GarbageBlobCount = b.GarbageBlobCount,
                    GarbageBlobBytes = b.GarbageBlobBytes,
                };
            }
            state.ColumnFamilies[c.Id] = cv;
        }
        return state;
    }

    /// <summary>Recovers counters from a replayed MANIFEST (VersionEditHandler::CheckIterationResult).</summary>
    public void RecoverFrom(RecoveredVersionState state, ulong manifestFileNumber)
    {
        DbId = state.DbId;
        MaxColumnFamily = state.MaxColumnFamily;
        MinLogNumberToKeep = state.MinLogNumberToKeep;
        PrevLogNumber = state.PrevLogNumber;
        // next_file_number_ = recorded next file + 1 (version_edit_handler.cc).
        SetNextFileNumber(state.NextFileNumber + 1);
        MarkFileNumberUsed(state.PrevLogNumber);
        MarkFileNumberUsed(state.LogNumber);
        MarkFileNumberUsed(manifestFileNumber);
        _lastSequence = state.LastSequence;
        _lastAllocatedSequence = state.LastSequence;
        _descriptorLastSequence = state.LastSequence;
        ManifestFileNumber = manifestFileNumber;
    }

    public async ValueTask DisposeAsync()
    {
        await _manifestLock.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_manifestLog is not null) await _manifestLog.DisposeAsync().ConfigureAwait(false);
            _manifestLog = null;
            _manifest = null;
        }
        finally
        {
            _manifestLock.Release();
        }
    }
}
