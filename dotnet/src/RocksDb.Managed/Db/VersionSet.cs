using RocksDb.Managed.Env;

namespace RocksDb.Managed.Db;

/// <summary>A file that no live version references any more (VersionSet obsolete_files_ / ObsoleteFileInfo).</summary>
internal readonly record struct ObsoleteTableFile(FileMetaData Meta, ColumnFamilyData Cfd);

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

    private ulong _nextFileNumber = 2;
    private ulong _lastSequence;          // last published sequence (visible to readers)
    private ulong _lastAllocatedSequence; // last sequence handed out to a write group
    private ulong _versionNumber;
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
    public void AddColumnFamily(ColumnFamilyData cfd, FileMetaData[][]? files)
    {
        _cfs[cfd.Id] = cfd;
        if (cfd.Id > MaxColumnFamily) MaxColumnFamily = cfd.Id;
        var storage = new VersionStorageInfo(cfd.Icmp, Math.Max(cfd.NumLevels, files?.Length ?? 0), cfd.Options, files);
        var v = new DbVersion(cfd, storage, ++_versionNumber);
        InstallVersion(cfd, v);
    }

    /// <summary>Makes <paramref name="v"/> the current version of <paramref name="cfd"/>. REQUIRES: DB mutex.</summary>
    private void InstallVersion(ColumnFamilyData cfd, DbVersion v)
    {
        RefFiles(v);
        v.Ref();
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
    }

    /// <summary>Drops a version reference; files no longer referenced become obsolete. REQUIRES: DB mutex.</summary>
    public void UnrefVersion(DbVersion v)
    {
        if (!v.Unref()) return;
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
            bool newManifest;
            lock (_db.Mutex)
            {
                if (edit.IsColumnFamilyAdd)
                {
                    edit.MaxColumnFamily = Math.Max(MaxColumnFamily, edit.ColumnFamily);
                }
                else if (cfd is not null && !edit.IsColumnFamilyDrop && (edit.NewFiles.Count > 0 || edit.DeletedFiles.Count > 0))
                {
                    newFiles = cfd.Current.Storage.ApplyEdit(edit, cfd.NumLevels);
                }
                newManifest = _manifest is null || (ulong)_manifestLog!.Log.File!.Size >= _db.Options.MaxManifestFileSize;
                if (newManifest)
                {
                    snapshot = BuildSnapshotState();
                }
                // Like ProcessManifestWrites: the last edit of a batch carries next_file_number and last_sequence.
                edit.LastSequence = LastSequence;
            }

            ulong manifestNumber = 0;
            ManifestLogWriter? newLog = null;
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
                    await Await(FileNames.SetCurrentFileAsync(_fs, _dbname, manifestNumber, syncDirectory: true, cancellationToken), synchronous)
                        .ConfigureAwait(false);
                    ManifestLogWriter? old = _manifestLog;
                    _manifestLog = newLog;
                    _manifest = writer;
                    newLog = null;
                    ulong oldNumber = ManifestFileNumber;
                    ManifestFileNumber = manifestNumber;
                    old?.Dispose();
                    if (oldNumber != 0) _db.ScheduleDeleteObsoleteManifest(oldNumber);
                }
                else
                {
                    edit.NextFileNumber = CurrentNextFileNumber;
                    await Await(_manifest!.AddEditAsync(edit, 0, sync: true, cancellationToken), synchronous).ConfigureAwait(false);
                }
            }
            catch
            {
                if (newLog is not null)
                {
                    newLog.Dispose();
                    try { _fs.DeleteFile(FileNames.DescriptorFileName(_dbname, manifestNumber)); }
                    catch (IOException) { }
                    catch (RocksDbException) { }
                }
                throw;
            }

            lock (_db.Mutex)
            {
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
                        var storage = new VersionStorageInfo(cfd.Icmp, newFiles.Length, cfd.Options, newFiles);
                        InstallVersion(cfd, new DbVersion(cfd, storage, ++_versionNumber));
                    }
                    if (edit.LogNumber is ulong ln && ln > cfd.LogNumber) cfd.LogNumber = ln;
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
            };
            VersionStorageInfo s = c.Current.Storage;
            for (int l = 0; l < s.NumLevels; l++)
            {
                List<FileMetaData> level = cv.MutableLevel(l);
                foreach (FileMetaData f in s.LevelFiles(l)) level.Add(f);
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
