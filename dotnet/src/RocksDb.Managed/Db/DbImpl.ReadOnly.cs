using RocksDb.Managed.Wal;

namespace RocksDb.Managed.Db;

/// <summary>
/// Read-only and secondary instances. Ports DBImplReadOnly (db/db_impl/db_impl_readonly.cc) and DBImplSecondary
/// (db/db_impl/db_impl_secondary.cc):
/// <list type="bullet">
/// <item>Read-only: no LOCK file, nothing is written (no new WAL, MANIFEST, OPTIONS or table files, no obsolete
/// file deletion); the WALs are replayed into memtables, which simply grow; writes, flushes and column family
/// changes fail with NotSupported. A subset of the column families may be opened.</item>
/// <item>Secondary: a read-only instance that follows a live primary. <see cref="TryCatchUpWithPrimaryAsync"/>
/// tails the primary's MANIFEST (applying only the VersionEdits appended since the previous catch-up and installing
/// new versions for the column families whose files changed), then tails the WALs from the oldest one holding
/// unflushed data. When a column family's log number advanced (the primary flushed), memtables are rebuilt from the
/// remaining WALs; otherwise only records appended since the previous catch-up are applied (incremental tailing with
/// <see cref="FragmentBufferedLogReader"/>, like DBImplSecondary::RecoverLogFiles).</item>
/// </list>
/// MANIFEST tailing ports ReactiveVersionSet::ReadAndApply / MaybeSwitchManifest (db/version_set.cc) and ManifestTailer
/// (db/version_edit_handler.h): the reader keeps its position in the MANIFEST and a <see cref="ManifestReplayer"/> keeps
/// the replayed state; only when CURRENT names a new MANIFEST (the primary rolled it over) is the new one replayed from
/// its start (it begins with a full snapshot). Like ManifestTailer (a VersionEditHandlerPointInTime, VerifyFile), the
/// table and blob files a new version adds are verified (present, MANIFEST size, and with verify_sst_unique_id_in_manifest
/// opened, which also keeps them readable) before it is installed; when one is missing (the primary already deleted it
/// after edits not yet visible) the MANIFEST is read on (the edit that dropped the file precedes its deletion) and the
/// newer state installed; after a few such rounds the column family keeps its current version until a later catch-up.
/// Upstream instead installs the latest intermediate point in time whose files all exist (VersionEditHandlerPointInTime)
/// and reaches the newer state at the next catch-up; the managed replayer only keeps the latest state. Files deleted by the primary while still referenced by an old secondary version are only readable if
/// their table reader is open (same caveat as upstream).
/// </summary>
internal sealed partial class DbImpl
{
    private bool _readOnly;
    private bool _secondary;
    // A secondary opened by OpenAndCompact skips WAL replay (OpenAsSecondaryImpl with recover_wal = false).
    private bool _secondaryRecoverWal = true;
    private string? _secondaryPath;
    private readonly SemaphoreSlim _catchUpLock = new(1, 1);
    // Secondary: open WAL readers by log number (incremental tailing). Guarded by _catchUpLock.
    private readonly SortedDictionary<ulong, FragmentBufferedLogReader> _secondaryWalReaders = new();
    // Secondary: position in the primary's current MANIFEST + replayed state. Guarded by _catchUpLock.
    private SecondaryManifestTailer? _manifestTailer;
    private int _secondaryManifestOpens;
    // Secondary: a column family's new version was not installed because a file was missing (retried by the next
    // catch-up even when the MANIFEST did not change). Guarded by _catchUpLock.
    private bool _secondaryInstallDeferred;
    private long _secondaryManifestRecords;

    /// <summary>Secondary diagnostics: MANIFESTs replayed from their start, and MANIFEST records applied in total.</summary>
    internal (int ManifestOpens, long ManifestRecords) SecondaryManifestTailStats =>
        (Volatile.Read(ref _secondaryManifestOpens), Interlocked.Read(ref _secondaryManifestRecords));

    public bool IsReadOnly => _readOnly;
    public bool IsSecondary => _secondary;

    private static void ThrowReadOnly() => throw RocksDbException.NotSupported("Not supported operation in read only mode.");

    /// <summary>Opens a read-only (or, with <paramref name="secondaryPath"/>, secondary) instance.</summary>
    public static ValueTask<DbImpl> OpenReadOnlyAsync(string path, DbOptions options, IReadOnlyList<ColumnFamilyDescriptor> columnFamilies,
        bool errorIfWalFileExists, string? secondaryPath, bool synchronous, CancellationToken cancellationToken) =>
        OpenReadOnlyAsync(path, options, columnFamilies, errorIfWalFileExists, secondaryPath, recoverWal: true, synchronous, cancellationToken);

    /// <summary>Read-only (<paramref name="secondaryPath"/> null) or secondary open; a secondary without <paramref name="recoverWal"/> ignores the WALs.</summary>
    public static async ValueTask<DbImpl> OpenReadOnlyAsync(string path, DbOptions options, IReadOnlyList<ColumnFamilyDescriptor> columnFamilies,
        bool errorIfWalFileExists, string? secondaryPath, bool recoverWal, bool synchronous, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(columnFamilies);
        string dbname = path.Length > 1 ? path.TrimEnd('/') : path;
        var db = new DbImpl(dbname, options.Clone())
        {
            _readOnly = true, _secondary = secondaryPath is not null, _secondaryPath = secondaryPath, _secondaryRecoverWal = recoverWal,
        };
        try
        {
            await db.OpenReadOnlyCoreAsync(columnFamilies, errorIfWalFileExists, synchronous, cancellationToken).ConfigureAwait(false);
            return db;
        }
        catch
        {
            db.AbortOpen();
            db.DisposeSecondaryReaders();
            throw;
        }
    }

    private async ValueTask OpenReadOnlyCoreAsync(IReadOnlyList<ColumnFamilyDescriptor> columnFamilies, bool errorIfWalFileExists, bool synchronous,
        CancellationToken ct)
    {
        var descriptors = new Dictionary<string, ColumnFamilyOptions>(StringComparer.Ordinal);
        foreach (ColumnFamilyDescriptor d in columnFamilies)
        {
            if (!descriptors.TryAdd(d.Name, d.Options)) ThrowHelper.InvalidArgument("Duplicate column family name: " + d.Name);
        }
        if (!descriptors.ContainsKey(DefaultColumnFamilyName)) descriptors[DefaultColumnFamilyName] = new ColumnFamilyOptions();
        if (_secondaryPath is not null) FileSystem.CreateDirectoryIfMissing(_secondaryPath);
        _infoLog = Options.InfoLog; // read-only and secondary instances don't create a LOG file
        SetComparatorResolver(descriptors);

        if (!FileSystem.FileExists(FileNames.CurrentFilePath(DbName)))
        {
            throw new RocksDbException(StatusCode.InvalidArgument, "Invalid argument: " + DbName + ": does not exist (read-only open)");
        }
        ulong manifestNumber;
        RecoveredVersionState state;
        if (_secondary)
        {
            state = TailManifestWithRetry(out _);
            manifestNumber = _manifestTailer!.ManifestNumber;
        }
        else
        {
            state = ManifestReader.RecoverFromDirectory(FileSystem, DbName, _comparatorResolver, out manifestNumber);
        }
        Versions.RecoverFrom(state, manifestNumber);
        foreach (string name in descriptors.Keys)
        {
            if (state.FindColumnFamily(name) is null) ThrowHelper.InvalidArgument("Column family not found: " + name);
        }

        string? dbId = state.DbId;
        if (dbId is null && FileSystem.FileExists(FileNames.IdentityFileName(DbName))) dbId = FileNames.ReadIdentityFile(FileSystem, DbName);
        dbId ??= "";
        Versions.DbId = dbId;
        Identity = Identity with { DbId = dbId };

        lock (_mutex)
        {
            foreach (ColumnFamilyVersion cf in state.ColumnFamilies.Values)
            {
                // Read-only and secondary instances may open a subset of the column families.
                if (!descriptors.TryGetValue(cf.Name, out ColumnFamilyOptions? cfOptions)) continue;
                ColumnFamilyOptions opts = PrepareCfOptions(cfOptions);
                ValidateRecoveredColumnFamily(cf, opts);
                var cfd = new ColumnFamilyData(cf.Id, cf.Name, opts, this) { LogNumber = cf.LogNumber, FullHistoryTsLow = cf.FullHistoryTsLow };
                Versions.AddColumnFamily(cfd, ToFileArrays(cf), cf.BlobFiles.Values);
                cfd.RecoverEpochNumbers();
                cfd.Mem = cfd.ConstructNewMemtable(state.LastSequence);
                RegisterCfForWrite(cfd);
            }
        }

        if (_secondary)
        {
            await CatchUpCoreAsync(state, manifestChanged: true, synchronous, ct).ConfigureAwait(false);
        }
        else
        {
            ulong minLog;
            lock (_mutex) minLog = Math.Max(Versions.MinLogNumberWithUnflushedData(null), Versions.MinLogNumberToKeep);
            var wals = ListWalNumbers(minLog);
            if (wals.Count > 0 && errorIfWalFileExists)
            {
                ThrowHelper.InvalidArgument("Write Ahead Log file in " + WalDir + " exists, but error_if_wal_file_exists is true");
            }
            // DBImpl::Recover(read_only = true): the same WAL checks and replay as the primary (ProcessLogFiles).
            CheckWalFilesExist(isNewDb: false, ListWalNumbers(0).Count);
            ReplayProgress progress = wals.Count == 0 || Options.BestEffortsRecovery
                ? new ReplayProgress { LastSequence = Versions.LastSequence }
                : await ProcessLogFilesAsync(wals, minLog, readOnly: true, edits: null, synchronous, ct).ConfigureAwait(false);
            ulong last = progress.LastSequence;
            Versions.SetLastSequence(last);
            lock (_mutex)
            {
                foreach (ColumnFamilyData cfd in Versions.ColumnFamilies) InstallSuperVersion(cfd);
            }
        }
    }

    private static FileMetaData[][] ToFileArrays(ColumnFamilyVersion cf)
    {
        var files = new FileMetaData[cf.NumLevels][];
        for (int l = 0; l < cf.NumLevels; l++) files[l] = new List<FileMetaData>(cf.Levels[l]).ToArray();
        return files;
    }

    private List<ulong> ListWalNumbers(ulong minLog)
    {
        var wals = new List<ulong>();
        foreach (string child in FileSystem.GetChildren(WalDir))
        {
            if (FileNames.TryParseFileName(child, out ulong number, out FileType type) && type == FileType.WalFile && number >= minLog) wals.Add(number);
        }
        wals.Sort();
        return wals;
    }

    // ---- secondary MANIFEST tailing ----------------------------------------------------------------------

    /// <summary>Open MANIFEST reader + the state replayed from it so far (ReactiveVersionSet's manifest_reader_ + ManifestTailer).</summary>
    private sealed class SecondaryManifestTailer(ulong manifestNumber, FragmentBufferedLogReader reader, WalRecoveryReporter reporter,
        ManifestReplayer replayer) : IDisposable
    {
        public readonly ulong ManifestNumber = manifestNumber;
        public readonly FragmentBufferedLogReader Reader = reader;
        public readonly WalRecoveryReporter Reporter = reporter;
        public readonly ManifestReplayer Replayer = replayer;
        /// <summary>Validated state as of the last applied edit (null until the first <see cref="ManifestReplayer.CurrentState"/>).</summary>
        public RecoveredVersionState? State;

        public void Dispose() => Reader.Dispose();
    }

    /// <summary>
    /// <see cref="TailManifest"/>, starting over from CURRENT when it fails: the primary may have switched to a new MANIFEST
    /// and deleted the old one between reading CURRENT and opening it (Status::TryAgain in MaybeSwitchManifest), or the
    /// tail did not decode. Caller serializes (open / _catchUpLock).
    /// </summary>
    private RecoveredVersionState TailManifestWithRetry(out bool changed)
    {
        for (int attempt = 0; ; attempt++)
        {
            try
            {
                return TailManifest(out changed);
            }
            catch (RocksDbException) when (attempt < 5)
            {
                DisposeManifestTailer();
                Thread.Sleep(1 << attempt);
            }
        }
    }

    /// <summary>
    /// Port of ReactiveVersionSet::ReadAndApply: switches to the MANIFEST named by CURRENT if it changed
    /// (MaybeSwitchManifest -- the new one is replayed from its start), then applies every complete record appended since
    /// the previous call. A torn trailing record stays buffered in the reader; an incomplete trailing atomic group stays
    /// buffered in the replayer. <paramref name="changed"/> is false when no new edit was applied (nothing to install).
    /// The returned state is live (owned by the tailer).
    /// </summary>
    private RecoveredVersionState TailManifest(out bool changed)
    {
        changed = false;
        string path = FileNames.GetCurrentManifestPath(FileSystem, DbName, out ulong manifestNumber);
        SecondaryManifestTailer? tailer = _manifestTailer;
        if (tailer is null || tailer.ManifestNumber != manifestNumber)
        {
            // Like C++, the rest of the old MANIFEST is not read: the new one starts with a snapshot of the full state.
            DisposeManifestTailer();
            var reporter = new WalRecoveryReporter { FileName = path };
            // log::FragmentBufferedReader(..., checksum=true, log_number=0), as in MaybeSwitchManifest.
            var reader = new FragmentBufferedLogReader(FileSystem.OpenSequential(path), reporter, checksum: true, logNumber: 0);
            tailer = new SecondaryManifestTailer(manifestNumber, reader, reporter, new ManifestReplayer(_comparatorResolver));
            _manifestTailer = tailer;
            Interlocked.Increment(ref _secondaryManifestOpens);
            changed = true;
        }
        long applied = 0;
        while (tailer.Reader.TryReadRecord(out ReadOnlySpan<byte> record))
        {
            if (tailer.Reporter.Error is not null) break;
            tailer.Replayer.Apply(VersionEdit.DecodeFrom(record));
            applied++;
        }
        if (tailer.Reporter.Error is { } error) throw error;
        if (applied > 0)
        {
            Interlocked.Add(ref _secondaryManifestRecords, applied);
            changed = true;
        }
        // CurrentState validates and re-sorts the levels: only needed after new edits.
        if (changed) tailer.State = tailer.Replayer.CurrentState();
        return tailer.State!;
    }

    private void DisposeManifestTailer()
    {
        _manifestTailer?.Dispose();
        _manifestTailer = null;
    }

    // ---- secondary catch-up ----------------------------------------------------------------------------

    /// <summary>Port of DBImplSecondary::TryCatchUpWithPrimary.</summary>
    public async ValueTask TryCatchUpWithPrimaryAsync(bool synchronous, CancellationToken ct)
    {
        ThrowIfDisposed();
        if (!_secondary) ThrowHelper.NotSupported("TryCatchUpWithPrimary is only supported by secondary instances");
        if (synchronous) _catchUpLock.Wait(ct);
        else await _catchUpLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            ThrowIfDisposed();
            RecoveredVersionState state = TailManifestWithRetry(out bool manifestChanged);
            TestHooks.Invoke("DBImplSecondary::TryCatchUpWithPrimary:AfterManifestTail");
            await CatchUpCoreAsync(state, manifestChanged, synchronous, ct).ConfigureAwait(false);
        }
        finally
        {
            _catchUpLock.Release();
        }
    }

    /// <summary>
    /// Installs the primary's current versions (when <paramref name="manifestChanged"/>) and applies its WALs. Caller
    /// serializes (open / _catchUpLock).
    /// </summary>
    private ValueTask CatchUpCoreAsync(RecoveredVersionState state, bool manifestChanged, bool synchronous, CancellationToken ct)
    {
        _ = synchronous;
        ct.ThrowIfCancellationRequested();
        for (int attempt = 0; manifestChanged || _secondaryInstallDeferred; attempt++)
        {
            var obsolete = new List<ObsoleteTableFile>();
            var obsoleteBlobs = new List<ObsoleteBlobFile>();
            bool resetMemtables = InstallManifestState(state, obsolete, obsoleteBlobs);
            foreach (ObsoleteTableFile t in obsolete) t.Cfd.TableCache.Evict(t.Meta.FileNumber);
            foreach (ObsoleteBlobFile b in obsoleteBlobs) b.Cfd.BlobSource.Evict(b.BlobFileNumber);
            if (resetMemtables) DisposeSecondaryWalReaders();
            if (!_secondaryInstallDeferred || attempt == MaxDeferredInstallRetries) break;
            // A file of the replayed state is gone: the primary deletes a table / blob file only after the MANIFEST edit
            // that dropped it from its live versions was written (LogAndApply, then PurgeObsoleteFiles), so that edit was
            // appended after this catch-up read the MANIFEST. Read on and install the newer state, which no longer
            // references the file (upstream's point-in-time handler would install an older consistent version and only
            // reach this state at the next catch-up).
            state = TailManifestWithRetry(out manifestChanged);
            if (!manifestChanged) break;
        }
        if (_secondaryRecoverWal) ApplyWalTail(state);
        else
        {
            lock (_mutex)
            {
                foreach (ColumnFamilyData cfd in Versions.ColumnFamilies) InstallSuperVersion(cfd);
            }
        }
        return default;
    }

    /// <summary>Re-reads of the MANIFEST within one catch-up when the replayed state references a deleted file.</summary>
    private const int MaxDeferredInstallRetries = 3;

    /// <summary>
    /// Installs new versions for the column families whose live files changed (ReactiveVersionSet's cfds_changed).
    /// Returns true when a log number advanced and the memtables were reset (WALs must be re-read).
    /// </summary>
    private bool InstallManifestState(RecoveredVersionState state, List<ObsoleteTableFile> obsolete, List<ObsoleteBlobFile> obsoleteBlobs)
    {
        bool resetMemtables = false;
        _secondaryInstallDeferred = false;
        lock (_mutex)
        {
            foreach (ColumnFamilyData cfd in Versions.ColumnFamilies)
            {
                ColumnFamilyVersion? cf = null;
                foreach (ColumnFamilyVersion c in state.ColumnFamilies.Values)
                {
                    if (c.Id == cfd.Id) cf = c;
                }
                if (cf is null) continue; // dropped by the primary: keep serving the last state
                if (!SameFiles(cfd.Current.Storage, cf))
                {
                    // ManifestTailer::VerifyFile: install only a version whose new files all exist.
                    if (!NewFilesPresent(cfd, cf))
                    {
                        // The log number stays with the installed version: the WAL data that the new version's table
                        // files hold is replayed into the memtables until that version can be installed.
                        _secondaryInstallDeferred = true;
                        continue;
                    }
                    Versions.AddColumnFamily(cfd, ToFileArrays(cf), cf.BlobFiles.Values); // installs a new current version
                }
                if (cf.LogNumber != cfd.LogNumber)
                {
                    cfd.LogNumber = cf.LogNumber;
                    resetMemtables = true;
                }
            }
            // Never delete the primary's files: just drop cached readers of files no version uses any more.
            Versions.TakeObsoleteFiles(obsolete);
            Versions.TakeObsoleteBlobFiles(obsoleteBlobs);
            if (resetMemtables)
            {
                foreach (ColumnFamilyData cfd in Versions.ColumnFamilies)
                {
                    MemTable old = cfd.Mem;
                    cfd.Mem = cfd.ConstructNewMemtable(state.LastSequence);
                    if (old is not null && old.Unref()) old.Dispose();
                }
            }
        }
        return resetMemtables;
    }

    /// <summary>Applies WAL records appended since the previous catch-up (DBImplSecondary::RecoverLogFiles).</summary>
    private void ApplyWalTail(RecoveredVersionState state)
    {
        ulong minLog;
        lock (_mutex) minLog = Versions.MinLogNumberWithUnflushedData(null);
        // Readers of WALs whose data is now in table files are no longer needed.
        var stale = new List<ulong>();
        foreach (ulong n in _secondaryWalReaders.Keys)
        {
            if (n < minLog) stale.Add(n);
        }
        foreach (ulong n in stale)
        {
            _secondaryWalReaders[n].Dispose();
            _secondaryWalReaders.Remove(n);
        }

        ulong last = Math.Max(Versions.LastSequence, state.LastSequence);
        var reporter = new WalRecoveryReporter();
        foreach (ulong walNumber in ListWalNumbers(minLog))
        {
            if (!_secondaryWalReaders.TryGetValue(walNumber, out FragmentBufferedLogReader? reader))
            {
                Env.ISequentialFile file;
                try
                {
                    file = FileSystem.OpenSequential(FileNames.LogFileName(WalDir, walNumber));
                }
                catch (RocksDbException)
                {
                    continue; // deleted / archived by the primary meanwhile
                }
                reader = new FragmentBufferedLogReader(file, reporter, checksum: true, walNumber);
                _secondaryWalReaders[walNumber] = reader;
            }
            else if (reader.AtStaleTail && !TryRepositionSecondaryWalReader(reader, walNumber))
            {
                continue;
            }
            while (reader.TryReadRecord(out ReadOnlySpan<byte> record))
            {
                if (record.Length < WriteBatch.HeaderSize) continue;
                ulong seq = Util.Coding.DecodeFixed64(record);
                uint count = Util.Coding.DecodeFixed32(record[8..]);
                var inserter = new MemTableInserter(this, walNumber, ignoreMissingColumnFamilies: true) { Sequence = seq };
                inserter.Run(record);
                _flushScheduler.Clear();
                // The inserter's next sequence (not the header count): prepared sections consume no sequence numbers.
                if (count > 0 && inserter.Sequence > seq) last = Math.Max(last, inserter.Sequence - 1);
            }
        }
        Versions.SetLastSequence(last);
        lock (_mutex)
        {
            foreach (ColumnFamilyData cfd in Versions.ColumnFamilies) InstallSuperVersion(cfd);
        }
        NotifySequenceWaiters();
    }

    /// <summary>
    /// Managed fix for secondaries following a primary with recycle_log_file_num &gt; 0. The previous catch-up stopped
    /// the reader of a reused WAL at bytes of the file's previous use (an old record, or a record cut by the new data),
    /// which it had already buffered or dropped; the primary overwrites them in place. Re-read the WAL from the end of
    /// the last complete record on a fresh handle. Upstream's secondary keeps its reader (and spins on the old record,
    /// see FragmentBufferedLogReader). Returns false when the WAL is gone (deleted by the primary meanwhile).
    /// </summary>
    private bool TryRepositionSecondaryWalReader(FragmentBufferedLogReader reader, ulong walNumber)
    {
        Env.ISequentialFile file;
        try
        {
            file = FileSystem.OpenSequential(FileNames.LogFileName(WalDir, walNumber));
        }
        catch (RocksDbException)
        {
            return false;
        }
        reader.Reposition(file);
        return true;
    }

    /// <summary>
    /// Port of ManifestTailer::VerifyFile / VersionSet::VerifyFileMetadata for the files <paramref name="cf"/> adds to the
    /// column family's current version: each table file must exist with the size recorded in the MANIFEST and, with
    /// verify_sst_unique_id_in_manifest, open through the table cache (unique id checked; the open reader stays cached);
    /// each new blob file must exist. Missing files are logged.
    /// </summary>
    private bool NewFilesPresent(ColumnFamilyData cfd, ColumnFamilyVersion cf)
    {
        VersionStorageInfo current = cfd.Current.Storage;
        var known = new HashSet<ulong>();
        for (int l = 0; l < current.NumLevels; l++)
        {
            foreach (FileMetaData f in current.LevelFiles(l)) known.Add(f.FileNumber);
        }
        for (int l = 0; l < cf.NumLevels; l++)
        {
            foreach (FileMetaData f in cf.Levels[l])
            {
                if (known.Contains(f.FileNumber)) continue;
                string path = FileNames.TableFileName(DbPaths, f.FileNumber, f.PathId);
                try
                {
                    if (!FileSystem.FileExists(path) || (ulong)FileSystem.GetFileSize(path) != f.FileSize)
                    {
                        LogInfo("Secondary: table file " + path + " is missing or has a different size; keeping the current version of [" + cfd.Name + "]");
                        return false;
                    }
                    if (Options.VerifySstUniqueIdInManifest)
                    {
                        f.LevelHint = l;
                        using TableReaderLease lease = cfd.TableCache.GetReader(f);
                    }
                }
                catch (Exception e) when (e is RocksDbException or IOException)
                {
                    LogInfo("Secondary: table file " + path + " is not usable (" + e.Message + "); keeping the current version of [" + cfd.Name + "]");
                    return false;
                }
            }
        }
        var knownBlobs = new HashSet<ulong>();
        foreach (Blob.BlobFileMetaData b in current.BlobFiles) knownBlobs.Add(b.BlobFileNumber);
        foreach (ulong blobNumber in cf.BlobFiles.Keys)
        {
            if (knownBlobs.Contains(blobNumber)) continue;
            if (!FileSystem.FileExists(FileNames.BlobFileName(DbPaths[0], blobNumber)))
            {
                LogInfo("Secondary: blob file " + blobNumber + " is missing; keeping the current version of [" + cfd.Name + "]");
                return false;
            }
        }
        return true;
    }

    private static bool SameFiles(VersionStorageInfo current, ColumnFamilyVersion cf)
    {
        int levels = Math.Max(current.NumLevels, cf.NumLevels);
        for (int l = 0; l < levels; l++)
        {
            ReadOnlySpan<FileMetaData> a = l < current.NumLevels ? current.LevelFiles(l) : default;
            int bCount = l < cf.NumLevels ? cf.Levels[l].Count : 0;
            if (a.Length != bCount) return false;
            if (bCount == 0) continue;
            var numbers = new HashSet<ulong>();
            foreach (FileMetaData f in a) numbers.Add(f.FileNumber);
            foreach (FileMetaData f in cf.Levels[l])
            {
                if (!numbers.Contains(f.FileNumber)) return false;
            }
        }
        return true;
    }

    /// <summary>Disposes the MANIFEST tailer and the WAL readers (close / failed open).</summary>
    private void DisposeSecondaryReaders()
    {
        DisposeManifestTailer();
        DisposeSecondaryWalReaders();
    }

    private void DisposeSecondaryWalReaders()
    {
        foreach (FragmentBufferedLogReader r in _secondaryWalReaders.Values) r.Dispose();
        _secondaryWalReaders.Clear();
    }
}
