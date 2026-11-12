using RocksDb.Managed.Env;

namespace RocksDb.Managed.Db;

/// <summary>A file that no live version references any more (VersionSet obsolete_files_ / ObsoleteFileInfo).</summary>
internal readonly record struct ObsoleteTableFile(FileMetaData Meta, ColumnFamilyData Cfd);

/// <summary>A blob file that no live version references any more (VersionSet obsolete_blob_files_ / ObsoleteBlobFileInfo).</summary>
internal readonly record struct ObsoleteBlobFile(ulong BlobFileNumber, ColumnFamilyData Cfd);

/// <summary>One edit of a VersionSet::LogAndApply batch and the column family it applies to (null for DB-wide edits).</summary>
internal readonly record struct ManifestEditEntry(ColumnFamilyData? Cfd, VersionEdit Edit);

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
    // last_sequence_ <= last_published_sequence_ <= last_allocated_sequence_ (db/version_set.h). Without two write
    // queues (or without seq_per_batch) the last sequence is also the last published one.
    private ulong _lastSequence;          // last sequence in the memtables (maintained by the main write queue)
    private ulong _lastAllocatedSequence; // last sequence handed out to a write group (atomic with two write queues)
    private ulong _lastPublishedSequence; // last sequence visible to readers when it differs from _lastSequence
    private ulong _versionNumber;
    private ulong _descriptorLastSequence; // last_sequence recorded in the MANIFEST so far (descriptor_last_sequence_), guarded by _manifestLock
    private ManifestLogWriter? _manifestLog;
    // VersionSet::wals_ (WalSet): WALs tracked in the MANIFEST with track_and_verify_wals_in_manifest. Guarded by the DB mutex.
    private readonly SortedDictionary<ulong, ulong?> _wals = new();
    private ulong _minWalNumberToKeep;
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

    /// <summary>WalSet::GetMinWalNumberToKeep. REQUIRES: DB mutex.</summary>
    public ulong MinWalNumberToKeep => _minWalNumberToKeep;

    /// <summary>The WALs tracked in the MANIFEST (WalSet::GetWals). REQUIRES: DB mutex.</summary>
    public IReadOnlyDictionary<ulong, ulong?> TrackedWals => _wals;
    public ulong ManifestFileNumber { get; private set; }
    public ulong PrevLogNumber { get; set; }
    /// <summary>
    /// MANIFEST number and size as of the last installed edit (manifest_file_number_ / manifest_file_size_), consistent
    /// with the installed versions. REQUIRES: DB mutex.
    /// </summary>
    public (ulong Number, ulong Size) InstalledManifest { get; set; }

    public IReadOnlyCollection<ColumnFamilyData> ColumnFamilies => _cfs.Values;

    /// <summary>
    /// Incremented whenever a column family is added or removed, so readers can cache a snapshot of
    /// <see cref="ColumnFamilies"/> (DbImpl.ColumnFamiliesSnapshot). REQUIRES: DB mutex (like the set itself).
    /// </summary>
    public int ColumnFamilySetVersion { get; private set; }

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

    /// <summary>
    /// The last sequence number written to the memtables (VersionSet::LastSequence). It is also the last sequence
    /// visible to readers unless the DB has two write queues with seq_per_batch (see
    /// <see cref="LastPublishedSequence"/>).
    /// </summary>
    public ulong LastSequence => Volatile.Read(ref _lastSequence);

    /// <summary>
    /// Sets the last sequence number (SetLastSequence). Only the main write queue's leader (or a writer that blocked both
    /// queues) calls this. Also raises the last allocated sequence when it is behind (recovery, ingestion).
    /// </summary>
    public void SetLastSequence(ulong s)
    {
        Volatile.Write(ref _lastSequence, s);
        ulong cur = Volatile.Read(ref _lastAllocatedSequence);
        while (cur < s)
        {
            ulong o = Interlocked.CompareExchange(ref _lastAllocatedSequence, s, cur);
            if (o == cur) break;
            cur = o;
        }
    }

    /// <summary>
    /// Allocates <paramref name="count"/> sequence numbers; returns the first. Thread-safe (FetchAddLastAllocatedSequence
    /// + 1): with two write queues both leaders allocate concurrently.
    /// </summary>
    public ulong AllocateSequence(ulong count) => Interlocked.Add(ref _lastAllocatedSequence, count) - count + 1;

    /// <summary>LastAllocatedSequence: the last sequence handed out (&gt;= <see cref="LastSequence"/>).</summary>
    public ulong LastAllocatedSequence => Volatile.Read(ref _lastAllocatedSequence);

    /// <summary>
    /// LastPublishedSequence: the last sequence visible to readers when the DB has two write queues and seq_per_batch
    /// (WritePrepared / WriteUnprepared), published by the commit callbacks of the second queue.
    /// </summary>
    public ulong LastPublishedSequence => Volatile.Read(ref _lastPublishedSequence);

    /// <summary>
    /// SetLastPublishedSequence. Callers publish in increasing order (the WAL-only queue publishes); a smaller value
    /// (a callback run at open time from the main queue) never moves the published sequence back.
    /// </summary>
    public void SetLastPublishedSequence(ulong s)
    {
        ulong cur = Volatile.Read(ref _lastPublishedSequence);
        while (cur < s)
        {
            ulong o = Interlocked.CompareExchange(ref _lastPublishedSequence, s, cur);
            if (o == cur) return;
            cur = o;
        }
    }

    /// <summary>
    /// SyncLastSequenceWithAllocated: raises the last sequence to the last allocated one. REQUIRES: no write in
    /// progress on either queue (sequence numbers allocated by WAL-only writes of the second queue are accounted for in
    /// new memtables / MANIFEST state).
    /// </summary>
    public void SyncLastSequenceWithAllocated()
    {
        ulong alloc = LastAllocatedSequence;
        if (alloc > LastSequence) Volatile.Write(ref _lastSequence, alloc);
    }

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
    public void AddColumnFamily(ColumnFamilyData cfd, FileMetaData[][]? files, IEnumerable<BlobFileState>? blobFiles) =>
        AddColumnFamily(cfd, files, blobFiles, null, updateStats: false);

    /// <summary>
    /// Registers a recovered column family (VersionEditHandler: the version built from the MANIFEST), with its
    /// round-robin <paramref name="compactCursors"/> and, with <paramref name="updateStats"/> (not
    /// skip_stats_update_on_db_open), the entry stats of its files loaded (Version::PrepareAppend(update_stats)).
    /// REQUIRES: DB mutex (only taken during DB open, when the table reads under it block nobody).
    /// </summary>
    public void AddColumnFamily(ColumnFamilyData cfd, FileMetaData[][]? files, IEnumerable<BlobFileState>? blobFiles,
        IReadOnlyList<CompactCursor>? compactCursors, bool updateStats)
    {
        _cfs[cfd.Id] = cfd;
        ColumnFamilySetVersion++;
        if (cfd.Id > MaxColumnFamily) MaxColumnFamily = cfd.Id;
        Blob.BlobFileMetaData[] blobs = [];
        if (blobFiles is not null)
        {
            var baseList = new List<Blob.BlobFileMetaData>();
            foreach (BlobFileState b in blobFiles) baseList.Add(new Blob.BlobFileMetaData(b.Addition, b.GarbageBlobCount, b.GarbageBlobBytes, []));
            blobs = Blob.BlobFileMetaData.Build(baseList, null, files ?? []);
        }
        AccumulatedFileStats? stats = null;
        if (updateStats && files is not null)
        {
            // DB open: upstream has opened these tables already (VersionEditHandler::LoadTables with
            // prefetch_index_and_filter_in_cache = false), so UpdateAccumulatedStats finds them open. Here the stats
            // load is what opens them first, with the same setting.
            cfd.TableCache.InitialLoad = true;
            try
            {
                stats = LoadFileStats(cfd, files, default);
            }
            finally
            {
                cfd.TableCache.InitialLoad = false;
            }
        }
        var context = new VersionBuildContext
        {
            CompactCursors = compactCursors,
            AccumulatedStats = stats,
            AllowIngestBehind = AllowIngestBehind(cfd),
            FullHistoryTsLow = cfd.FullHistoryTsLow,
        };
        // VersionBuilder::SaveTo -> CheckConsistency of the recovered version (MANIFEST replay).
        if (files is not null) VersionStorageInfo.CheckConsistency(cfd.Icmp, files, blobs, cfd.Options.ForceConsistencyChecks);
        var storage = new VersionStorageInfo(cfd.Icmp, Math.Max(cfd.NumLevels, files?.Length ?? 0), cfd.Options, files, blobs, context);
        var v = new DbVersion(cfd, storage, ++_versionNumber);
        InstallVersion(cfd, v);
    }

    private bool AllowIngestBehind(ColumnFamilyData cfd) => _db.Options.AllowIngestBehind || cfd.Options.CfAllowIngestBehind;

    /// <summary>
    /// Version::UpdateAccumulatedStats for a version about to be built from <paramref name="files"/>: loads the entry
    /// stats of new files through the table cache (no I/O when the table is open or the stats came from the builder).
    /// </summary>
    private AccumulatedFileStats LoadFileStats(ColumnFamilyData cfd, FileMetaData[][] files, AccumulatedFileStats accumulated)
    {
        // The readers opened here record their reads in the histogram of the file's level (file_read_hist).
        for (int l = 0; l < files.Length; l++)
        {
            foreach (FileMetaData f in files[l]) f.LevelHint = l;
        }
        return VersionStorageInfo.UpdateAccumulatedStats(files, accumulated, _db.Options.MaxOpenFiles == -1, f => ReadTableProperties(cfd, f));
    }

    private Table.TableProperties? ReadTableProperties(ColumnFamilyData cfd, FileMetaData f)
    {
        try
        {
            using TableReaderLease lease = cfd.TableCache.GetReader(f);
            return lease.Reader.Properties;
        }
        catch (Exception e) when (e is RocksDbException or IOException)
        {
            _db.LogError("Unable to load table properties for file " + f.FileNumber.ToString(System.Globalization.CultureInfo.InvariantCulture) + " --- " + e.Message);
            return null;
        }
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
                int refs = _fileRefs.GetValueOrDefault(f.FileNumber);
                if (refs == 0) v.Cfd.AllVersionsSstFilesSize += f.FileSize; // GetTotalSstFilesSize: each file once
                _fileRefs[f.FileNumber] = refs + 1;
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
                    v.Cfd.AllVersionsSstFilesSize -= f.FileSize;
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

    /// <summary>
    /// VersionSet::GetObsoleteSstFilesSize: total size of the table files no longer referenced by any version that were
    /// not yet taken for deletion ("rocksdb.obsolete-sst-files-size"). REQUIRES: DB mutex.
    /// </summary>
    public ulong ObsoleteSstFilesSize
    {
        get
        {
            ulong total = 0;
            foreach (ObsoleteTableFile f in _obsoleteFiles) total += f.Meta.FileSize;
            return total;
        }
    }

    /// <summary>
    /// Port of the check of VersionBuilder::ApplyFileAddition (db/version_builder.cc): a table file the edit adds must
    /// not be in the version already, unless the same edit deletes it (deletions are applied first: a trivial move).
    /// Only a file some live version references can be in the current one, so the usual case is one dictionary lookup
    /// per added file. REQUIRES: DB mutex.
    /// </summary>
    private void CheckFileAdditions(VersionStorageInfo current, VersionEdit edit)
    {
        foreach (NewFileEntry nf in edit.NewFiles)
        {
            ulong number = nf.Meta.FileNumber;
            if (!_fileRefs.ContainsKey(number)) continue;
            for (int level = 0; level < current.NumLevels; level++)
            {
                bool found = false;
                foreach (FileMetaData f in current.LevelFiles(level))
                {
                    if (f.FileNumber == number)
                    {
                        found = true;
                        break;
                    }
                }
                if (!found) continue;
                bool deleted = false;
                foreach ((int deletedLevel, ulong deletedNumber) in edit.DeletedFiles)
                {
                    if (deletedLevel == level && deletedNumber == number) deleted = true;
                }
                if (!deleted)
                {
                    ThrowHelper.Corruption("VersionBuilder: Cannot add table file #" + number.ToString(System.Globalization.CultureInfo.InvariantCulture) +
                        " to level " + nf.Level.ToString(System.Globalization.CultureInfo.InvariantCulture) + " since it is already in the LSM tree on level " +
                        level.ToString(System.Globalization.CultureInfo.InvariantCulture));
                }
                break;
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
    public ValueTask LogAndApplyAsync(ColumnFamilyData? cfd, VersionEdit edit, bool synchronous, CancellationToken cancellationToken) =>
        LogAndApplyAsync([new ManifestEditEntry(cfd, edit)], atomicGroup: false, synchronous, null, cancellationToken);

    /// <summary>
    /// <see cref="LogAndApplyAsync(ColumnFamilyData?, VersionEdit, bool, CancellationToken)"/> that also runs
    /// <paramref name="installedUnderMutex"/> (if not null) in the DB-mutex section that installs the new version.
    /// </summary>
    public ValueTask LogAndApplyAsync(ColumnFamilyData? cfd, VersionEdit edit, bool synchronous, Action? installedUnderMutex,
        CancellationToken cancellationToken) =>
        LogAndApplyAsync([new ManifestEditEntry(cfd, edit)], atomicGroup: false, synchronous, installedUnderMutex, cancellationToken);

    /// <summary>The multi-edit form without a callback (see the overload with <c>installedUnderMutex</c>).</summary>
    public ValueTask LogAndApplyAsync(IReadOnlyList<ManifestEditEntry> edits, bool atomicGroup, bool synchronous, CancellationToken cancellationToken) =>
        LogAndApplyAsync(edits, atomicGroup, synchronous, null, cancellationToken);

    /// <summary>
    /// Port of the multi-edit form of VersionSet::LogAndApply (one ProcessManifestWrites batch): writes all
    /// <paramref name="edits"/> to the MANIFEST with a single sync and installs them together. With
    /// <paramref name="atomicGroup"/> (and more than one edit) they are written as an atomic group, so recovery applies
    /// all of them or none (used by atomic flushes, e.g. WAL write error recovery). Column family additions and drops
    /// cannot be batched. At most one edit per column family may change files.
    /// <para>
    /// <paramref name="installedUnderMutex"/>, if not null, runs while the DB mutex is still held after the new versions
    /// were installed (and only if they were). Upstream's LogAndApply returns with the mutex held, and its callers install
    /// the super version (InstallSuperVersionAndScheduleWork, which recalculates the write stall) before releasing it.
    /// Installing it in a later critical section lets other threads see the new version with the previous write-stall
    /// state in between (e.g. an empty L0 while writes are still delayed).
    /// </para>
    /// </summary>
    public async ValueTask LogAndApplyAsync(IReadOnlyList<ManifestEditEntry> edits, bool atomicGroup, bool synchronous, Action? installedUnderMutex,
        CancellationToken cancellationToken)
    {
        int n = edits.Count;
        if (n == 0) return;
        if (n > 1)
        {
            foreach (ManifestEditEntry e in edits)
            {
                if (e.Edit.IsColumnFamilyAdd || e.Edit.IsColumnFamilyDrop) ThrowHelper.InvalidArgument("column family add/drop cannot be batched");
            }
        }
        if (synchronous) _manifestLock.Wait(cancellationToken);
        else await _manifestLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            RecoveredVersionState? snapshot = null;
            var newFiles = new FileMetaData[n][][];
            var newBlobFiles = new Blob.BlobFileMetaData[n][];
            var editList = new VersionEdit[n];
            var tsSizes = new int[n];
            var baseStats = new AccumulatedFileStats[n];
            bool newManifest;
            lock (_db.Mutex)
            {
                // Deviation from ProcessManifestWrites ("max_last_sequence = descriptor_last_sequence_", the largest value
                // recorded so far): the port starts from the DB's current last sequence, so every edit records it. The
                // sequence reservation of a WAL write error recovery relies on that (the failed write's sequence range
                // is published, then persisted by the next edit); a larger last_sequence is harmless to native.
                ulong lastSeq = Math.Max(LastSequence, _descriptorLastSequence);
                for (int i = 0; i < n; i++)
                {
                    ColumnFamilyData? cfd = edits[i].Cfd;
                    VersionEdit edit = edits[i].Edit;
                    editList[i] = edit;
                    // ProcessManifestWrites: edits of a column family dropped meanwhile are not written (a later edit for
                    // a dropped CF id would make the MANIFEST unrecoverable: "MANIFEST record referencing unknown column
                    // family"). The drop itself is applied under _manifestLock, so this check cannot race with it.
                    if (cfd is not null && cfd.Dropped && !edit.IsColumnFamilyAdd)
                    {
                        throw new RocksDbException(StatusCode.ColumnFamilyDropped, "Column family drop: column family " + cfd.Name + " was dropped");
                    }
                    if (edit.IsColumnFamilyDrop)
                    {
                        // LogAndApplyCFHelper: "if we drop column family, we have to make sure to save max column family,
                        // so that we don't reuse existing ID". An add records none (recovery raises the maximum to the
                        // id of every column family it creates).
                        edit.MaxColumnFamily = MaxColumnFamily;
                    }
                    else if (cfd is not null && !edit.IsColumnFamilyDrop && (edit.NewFiles.Count > 0 || edit.DeletedFiles.Count > 0 ||
                                                                             edit.BlobFileAdditions.Count > 0 || edit.BlobFileGarbages.Count > 0 ||
                                                                             edit.CompactCursors.Count > 0))
                    {
                        for (int j = 0; j < i; j++)
                        {
                            if (edits[j].Cfd == cfd && newFiles[j] is not null) ThrowHelper.InvalidArgument("two file edits of one column family in a batch");
                        }
                        VersionStorageInfo cur = cfd.Current.Storage;
                        CheckFileAdditions(cur, edit);
                        newFiles[i] = cur.ApplyEdit(edit, cfd.NumLevels);
                        newBlobFiles[i] = Blob.BlobFileMetaData.Build(cur.BlobFiles, edit, newFiles[i]);
                        // VersionBuilder::SaveTo -> CheckConsistency, before the edit is written.
                        VersionStorageInfo.CheckConsistency(cfd.Icmp, newFiles[i], newBlobFiles[i], cfd.Options.ForceConsistencyChecks);
                        baseStats[i] = cur.AccumulatedStats;
                    }
                    // WalSet::AddWal fails for a WAL created twice: check before anything is written.
                    foreach (WalAddition w in edit.WalAdditions)
                    {
                        if (w.SyncedSizeInBytes is null && w.LogNumber >= _minWalNumberToKeep && _wals.ContainsKey(w.LogNumber))
                        {
                            ThrowHelper.Corruption("WalSet::AddWal: WAL " + w.LogNumber + " is created more than once");
                        }
                    }
                    // Like LogAndApplyHelper: last_sequence never goes backwards and covers the largest seqno of the files
                    // an edit adds (ingested files are assigned a seqno above the published last sequence, which is only
                    // advanced after the edit is durable).
                    if (edit.IsColumnFamilyManipulation)
                    {
                        // LogAndApplyCFHelper: next_file (below) and last_sequence only.
                        edit.LastSequence = lastSeq;
                    }
                    else
                    {
                        // LogAndApplyHelper: prev_log_number on every edit that has none, next_file (below), and the
                        // running maximum of last_sequence (an edit keeps its own when that is larger).
                        edit.PrevLogNumber ??= PrevLogNumber;
                        if (edit.LastSequence is ulong own && own > lastSeq) lastSeq = own;
                        else edit.LastSequence = lastSeq;
                    }
                    tsSizes[i] = TimestampSizeOfEdit(edit);
                }
                newManifest = _manifest is null || (ulong)_manifestLog!.Log.File!.Size >= EnforcedManifestLimit(editList);
                if (newManifest)
                {
                    snapshot = BuildSnapshotState();
                    // ProcessManifestWrites: "if we are writing out new snapshot make sure to persist max column family".
                    if (MaxColumnFamily > 0) editList[0].MaxColumnFamily = MaxColumnFamily;
                }
                _descriptorLastSequence = lastSeq;
            }

            // Version::PrepareAppend(update_stats): load the entry stats of the new files before the versions are built
            // (outside the DB mutex; the new FileMetaData objects are not visible to anyone else yet).
            var newStats = new AccumulatedFileStats?[n];
            for (int i = 0; i < n; i++)
            {
                if (newFiles[i] is not null) newStats[i] = LoadFileStats(edits[i].Cfd!, newFiles[i], baseStats[i]);
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
                    Wal.LogWriter sizeOf = newLog.Log;
                    var writer = new ManifestWriter(newLog) { FileSizeProvider = () => (ulong)sizeOf.File!.Size };
                    // WriteCurrentStateToManifest (synced at the end).
                    await Await(writer.WriteCurrentStateAsync(snapshot, TimestampSizeOfColumnFamily, cancellationToken), synchronous).ConfigureAwait(false);
                    ulong next = CurrentNextFileNumber;
                    foreach (VersionEdit e in editList) e.NextFileNumber = next;
                    await Await(writer.AddEditsAsync(editList, tsSizes, atomicGroup, cancellationToken), synchronous).ConfigureAwait(false);
                    newManifestWritten = true;
                    // The new MANIFEST (snapshot and edits) plus CURRENT ("MANIFEST-nnnnnn\n").
                    editList[0].ManifestBytesWritten = (ulong)newLog.Log.File!.Size + (ulong)FileNames.DescriptorFileName(manifestNumber).Length + 1;
                    await Await(FileNames.SetCurrentFileAsync(_fs, _dbname, manifestNumber, syncDirectory: true, cancellationToken), synchronous)
                        .ConfigureAwait(false);
                    ManifestLogWriter? old = _manifestLog;
                    // ProcessManifestWrites: last_compacted_manifest_file_size_ = new_manifest_file_size (after the
                    // batch's edits), then TuneMaxManifestFileSize().
                    _lastCompactedManifestFileSize = (ulong)newLog.Log.File!.Size;
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
                    ulong next = CurrentNextFileNumber;
                    foreach (VersionEdit e in editList) e.NextFileNumber = next;
                    long sizeBefore = _manifestLog!.Log.File!.Size;
                    await Await(_manifest!.AddEditsAsync(editList, tsSizes, atomicGroup, cancellationToken), synchronous).ConfigureAwait(false);
                    editList[0].ManifestBytesWritten = (ulong)(_manifestLog!.Log.File!.Size - sizeBefore);
                }
            }
            catch (Exception writeError)
            {
                // VersionSet::io_status_: remembered so the error handler can classify the failure as a MANIFEST write
                // error and recovery forces a new MANIFEST.
                if (writeError is not OperationCanceledException) Volatile.Write(ref _ioError, RocksDbException.FromException(writeError));
                // ProcessManifestWrites: error_handler_->AddFilesToQuarantine(files_to_quarantine_if_commit_fail). The record
                // may have reached a MANIFEST that CURRENT names, so the files it adds must survive obsolete-file scans
                // until a later MANIFEST write succeeds.
                _db.QuarantineFiles(editList);
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
                _db.ClearQuarantinedFiles(); // ProcessManifestWrites: error_handler_->ClearFilesToQuarantine()
                InstalledManifest = (ManifestFileNumber, (ulong)_manifestLog!.Log.File!.Size);
                for (int i = 0; i < n; i++) InstallEdit(edits[i].Cfd, editList[i], newFiles[i], newBlobFiles[i], newStats[i]);
                installedUnderMutex?.Invoke();
            }
        }
        finally
        {
            _manifestLock.Release();
        }
    }

    /// <summary>Installs one written edit (the in-memory part of ProcessManifestWrites). REQUIRES: DB mutex.</summary>
    private void InstallEdit(ColumnFamilyData? cfd, VersionEdit edit, FileMetaData[][]? newFiles, Blob.BlobFileMetaData[]? newBlobFiles,
        AccumulatedFileStats? newStats)
    {
        if (edit.MinLogNumberToKeep is ulong minLog && minLog > MinLogNumberToKeep) MinLogNumberToKeep = minLog;
        // SstFileManagerImpl::OnAddFile for the table and blob files the edit adds (flush, compaction, ingestion outputs).
        foreach (NewFileEntry nf in edit.NewFiles) _db.OnDataFileAdded(FileNames.TableFileName(_db.DbPaths, nf.Meta.FileNumber, nf.Meta.PathId), nf.Meta.FileSize);
        foreach (BlobFileAddition b in edit.BlobFileAdditions)
        {
            _db.OnDataFileAdded(FileNames.BlobFileName(_db.DbPaths[0], b.BlobFileNumber), b.TotalBlobBytes + Blob.BlobLogFormat.HeaderSize + Blob.BlobLogFormat.FooterSize);
        }
        // VersionSet::wals_ (ProcessManifestWrites: AddWals / DeleteWalsBefore after the write succeeded).
        foreach (WalAddition w in edit.WalAdditions) WalSet.AddWal(_wals, _minWalNumberToKeep, w);
        if (edit.IsWalDeletion) WalSet.DeleteWalsBefore(_wals, ref _minWalNumberToKeep, edit.WalDeletionBefore);
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
                ColumnFamilySetVersion++;
            }
        }
        else if (cfd is not null)
        {
            if (newFiles is not null)
            {
                var context = new VersionBuildContext
                {
                    RefStorage = cfd.Current.Storage,
                    CompactCursors = edit.CompactCursors,
                    AccumulatedStats = newStats,
                    AllowIngestBehind = AllowIngestBehind(cfd),
                    FullHistoryTsLow = cfd.FullHistoryTsLow,
                };
                var storage = new VersionStorageInfo(cfd.Icmp, newFiles.Length, cfd.Options, newFiles, newBlobFiles ?? [], context);
                InstallVersion(cfd, new DbVersion(cfd, storage, ++_versionNumber));
            }
            if (edit.LogNumber is ulong ln && ln > cfd.LogNumber) cfd.LogNumber = ln;
            if (edit.FullHistoryTsLow is { Length: > 0 } tsLow && (cfd.FullHistoryTsLow is not { } cur
                || cfd.UserComparator.CompareTimestamp(tsLow, cur) > 0))
            {
                // ColumnFamilyData::SetFullHistoryTsLow (only increases).
                cfd.FullHistoryTsLow = tsLow;
                cfd.Current.Storage.FullHistoryTsLow = tsLow;
            }
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

    /// <summary>
    /// The running timestamp size of a column family, used to encode file boundaries (VersionEdit::EncodeTo(dst, ts_sz)):
    /// files that do not persist timestamps (persist_user_defined_timestamps = false) store their boundaries without them.
    /// </summary>
    private int TimestampSizeOfColumnFamily(ColumnFamilyVersion cf) => _cfs.TryGetValue(cf.Id, out ColumnFamilyData? c) ? c.TimestampSize : 0;

    private int TimestampSizeOfEdit(VersionEdit edit) =>
        edit.NewFiles.Count > 0 && _cfs.TryGetValue(edit.ColumnFamily, out ColumnFamilyData? c) ? c.TimestampSize : 0;

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
            MinWalNumberToKeep = _minWalNumberToKeep,
        };
        foreach (KeyValuePair<ulong, ulong?> w in _wals) state.Wals[w.Key] = w.Value;
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
            // WriteCurrentStateToManifest: edit.SetCompactCursors(vstorage->GetCompactCursors()).
            foreach (CompactCursor cursor in s.ValidCompactCursors()) cv.CompactCursors[cursor.Level] = cursor.InternalKey;
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

    // last_compacted_manifest_file_size_: the size of the last newly written (compacted) MANIFEST, or the value of its
    // kLastCompactedManifestFileSize record when recovered. Guarded by _manifestLock (read under the DB mutex too).
    private ulong _lastCompactedManifestFileSize;

    /// <summary>
    /// VersionSet::TuneMaxManifestFileSize: max(max_manifest_file_size, last compacted size * (100 +
    /// max_manifest_space_amp_pct) / 100) (computed from the live, mutable DB options).
    /// </summary>
    internal ulong TunedMaxManifestFileSize
    {
        get
        {
            ulong pct = (ulong)Math.Max(_db.Options.MaxManifestSpaceAmpPct, 0);
            ulong last = _lastCompactedManifestFileSize;
            ulong tuned = last > ulong.MaxValue / (100 + pct) ? ulong.MaxValue : last * (100 + pct) / 100;
            return Math.Max(_db.Options.MaxManifestFileSize, tuned);
        }
    }

    /// <summary>
    /// ProcessManifestWrites: for MANIFEST write batches with any foreground operation (external file ingestion/import,
    /// DeleteFilesInRange(s), column family create / drop), relax the size limit by 25% to reduce the likelihood of a
    /// user operation blocking on MANIFEST rotation. Background-only batches rotate at the normal threshold.
    /// </summary>
    private ulong EnforcedManifestLimit(VersionEdit[] batch)
    {
        ulong limit = TunedMaxManifestFileSize;
        foreach (VersionEdit e in batch)
        {
            if (e.IsForegroundOperation)
            {
                ulong relaxed = limit + limit / 4;
                // don't keep in case of overflow
                return Math.Max(limit, relaxed);
            }
        }
        return limit;
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
        _lastPublishedSequence = state.LastSequence;
        _descriptorLastSequence = state.LastSequence;
        ManifestFileNumber = manifestFileNumber;
        _wals.Clear();
        foreach (KeyValuePair<ulong, ulong?> w in state.Wals) _wals[w.Key] = w.Value;
        _minWalNumberToKeep = state.MinWalNumberToKeep;
        // VersionEditHandler: edit.HasLastCompactedManifestFileSize() -> TuneMaxManifestFileSize().
        _lastCompactedManifestFileSize = state.LastCompactedManifestFileSize ?? 0;
    }

    /// <summary>
    /// The content validation of VersionSet::Close (verify_manifest_content_on_close): reads the current MANIFEST back,
    /// checking every record's CRC and VersionEdit encoding. A corrupt MANIFEST is counted
    /// (MANIFEST_VALIDATION_FAILURE_COUNT) and rewritten once (a new compacted MANIFEST via an empty edit), then checked
    /// again; a second failure throws Corruption. Called by close before <see cref="DisposeAsync"/>.
    /// </summary>
    internal async ValueTask VerifyManifestContentOnCloseAsync(ColumnFamilyData defaultCfd)
    {
        // Loop up to 2 checks with 1 rewrite attempt in between, so we also verify the rewritten manifest is healthy.
        const int MaxContentChecks = 2;
        for (int check = 0; check < MaxContentChecks; check++)
        {
            string path;
            await _manifestLock.WaitAsync().ConfigureAwait(false);
            try
            {
                if (_manifestLog is null) return;
                // Re-read the manifest file name in case it was rotated by a rewrite.
                path = FileNames.DescriptorFileName(_dbname, ManifestFileNumber);
            }
            finally
            {
                _manifestLock.Release();
            }
            bool corrupt = false;
            try
            {
                using ManifestLogReader reader = ManifestLogReader.Open(_fs, path);
                while (reader.TryReadRecord(out ReadOnlySpan<byte> record)) VersionEdit.DecodeFrom(record);
            }
            catch (RocksDbException e) when (e.Code == StatusCode.Corruption)
            {
                corrupt = true;
            }
            if (!corrupt) return; // Manifest is healthy, no need to check again
            _db.Stats?.RecordTick(Ticker.ManifestValidationFailureCount);
            if (check == 0)
            {
                // First check failed -- rewrite and verify again.
                _db.LogError("MANIFEST content verification on Close failed, filename " + path + ", rewriting manifest");
                await _manifestLock.WaitAsync().ConfigureAwait(false);
                try
                {
                    if (_manifestLog is not null)
                    {
                        try { await _manifestLog.DisposeAsync().ConfigureAwait(false); }
                        catch (Exception e) when (e is IOException or RocksDbException) { }
                    }
                    // The next LogAndApply writes a new MANIFEST (ProcessManifestWrites without descriptor_log_).
                    _manifestLog = null;
                    _manifest = null;
                }
                finally
                {
                    _manifestLock.Release();
                }
                await LogAndApplyAsync(defaultCfd, new VersionEdit(), synchronous: false, CancellationToken.None).ConfigureAwait(false);
            }
            else
            {
                // Rewritten manifest is also corrupt -- likely a recurring filesystem issue.
                _db.LogError("MANIFEST content verification on Close failed again after rewrite, filename " + path);
                ThrowHelper.Corruption("MANIFEST content verification failed after rewrite: " + path);
            }
        }
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
