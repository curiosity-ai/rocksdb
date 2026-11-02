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
/// re-reads the primary's MANIFEST (installing new versions), then tails the WALs from the oldest one holding
/// unflushed data. When a column family's log number advanced (the primary flushed), memtables are rebuilt from the
/// remaining WALs; otherwise only records appended since the previous catch-up are applied (incremental tailing with
/// <see cref="FragmentBufferedLogReader"/>, like DBImplSecondary::RecoverLogFiles).</item>
/// </list>
/// The managed secondary re-reads the whole MANIFEST on every catch-up instead of tailing it with a
/// ReactiveVersionSet; files deleted by the primary while still referenced by an old secondary version are only
/// readable if their table reader was already open (same caveat as upstream).
/// </summary>
internal sealed partial class DbImpl
{
    private bool _readOnly;
    private bool _secondary;
    private string? _secondaryPath;
    private readonly SemaphoreSlim _catchUpLock = new(1, 1);
    // Secondary: open WAL readers by log number (incremental tailing). Guarded by _catchUpLock.
    private readonly SortedDictionary<ulong, FragmentBufferedLogReader> _secondaryWalReaders = new();

    public bool IsReadOnly => _readOnly;
    public bool IsSecondary => _secondary;

    private static void ThrowReadOnly() => throw RocksDbException.NotSupported("Not supported operation in read only mode.");

    /// <summary>Opens a read-only (or, with <paramref name="secondaryPath"/>, secondary) instance.</summary>
    public static async ValueTask<DbImpl> OpenReadOnlyAsync(string path, DbOptions options, IReadOnlyList<ColumnFamilyDescriptor> columnFamilies,
        bool errorIfWalFileExists, string? secondaryPath, bool synchronous, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(columnFamilies);
        string dbname = path.Length > 1 ? path.TrimEnd('/') : path;
        var db = new DbImpl(dbname, options.Clone()) { _readOnly = true, _secondary = secondaryPath is not null, _secondaryPath = secondaryPath };
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
        RecoveredVersionState state = RecoverManifestWithRetry(out ulong manifestNumber);
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
                var cfd = new ColumnFamilyData(cf.Id, cf.Name, opts, this) { LogNumber = cf.LogNumber };
                Versions.AddColumnFamily(cfd, ToFileArrays(cf));
                cfd.RecoverEpochNumbers();
                cfd.Mem = cfd.ConstructNewMemtable(state.LastSequence);
                RegisterCfForWrite(cfd);
            }
        }

        if (_secondary)
        {
            await CatchUpCoreAsync(state, synchronous, ct).ConfigureAwait(false);
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
            var progress = new ReplayProgress { LastSequence = Versions.LastSequence };
            foreach (ulong walNumber in wals)
            {
                string fname = FileNames.LogFileName(WalDir, walNumber);
                var reporter = new WalRecoveryReporter { FileName = fname };
                using var reader = new LogReader(FileSystem.OpenSequential(fname), reporter, checksum: Options.ParanoidChecks, walNumber)
                {
                    RecoveryMode = Options.WalRecoveryMode,
                };
                while (true)
                {
                    bool got = ReplayNextRecord(reader, reporter, walNumber, progress);
                    _flushScheduler.Clear(); // read-only memtables just grow
                    if (reporter.Error is { } err)
                    {
                        if (Options.WalRecoveryMode == WalRecoveryMode.SkipAnyCorruptedRecords) reporter.Error = null;
                        else if (Options.WalRecoveryMode == WalRecoveryMode.PointInTimeRecovery && err.Code != StatusCode.IOError)
                        {
                            // Like the primary: drop records until the sequence continues (MaybeReviseStopReplayForCorruption).
                            reporter.Error = null;
                            progress.StopReplay = true;
                        }
                        else throw err;
                    }
                    if (!got) break;
                }
            }
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

    /// <summary>Reads CURRENT + MANIFEST, retrying when the primary switches MANIFEST concurrently.</summary>
    private RecoveredVersionState RecoverManifestWithRetry(out ulong manifestNumber)
    {
        for (int attempt = 0; ; attempt++)
        {
            try
            {
                return ManifestReader.RecoverFromDirectory(FileSystem, DbName, _comparatorResolver, out manifestNumber);
            }
            catch (RocksDbException) when (_secondary && attempt < 5)
            {
                Thread.Sleep(1 << attempt);
            }
        }
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
            RecoveredVersionState state = RecoverManifestWithRetry(out _);
            await CatchUpCoreAsync(state, synchronous, ct).ConfigureAwait(false);
        }
        finally
        {
            _catchUpLock.Release();
        }
    }

    /// <summary>Installs the primary's current versions and applies its WALs. Caller serializes (open / _catchUpLock).</summary>
    private ValueTask CatchUpCoreAsync(RecoveredVersionState state, bool synchronous, CancellationToken ct)
    {
        _ = synchronous;
        ct.ThrowIfCancellationRequested();
        bool resetMemtables = false;
        var obsolete = new List<ObsoleteTableFile>();
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
                    Versions.AddColumnFamily(cfd, ToFileArrays(cf)); // installs a new current version
                }
                if (cf.LogNumber != cfd.LogNumber)
                {
                    cfd.LogNumber = cf.LogNumber;
                    resetMemtables = true;
                }
            }
            // Never delete the primary's files: just drop cached readers of files no version uses any more.
            Versions.TakeObsoleteFiles(obsolete);
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
        foreach (ObsoleteTableFile t in obsolete) t.Cfd.TableCache.Evict(t.Meta.FileNumber);
        if (resetMemtables) DisposeSecondaryReaders();

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
            while (reader.TryReadRecord(out ReadOnlySpan<byte> record))
            {
                if (record.Length < WriteBatch.HeaderSize) continue;
                ulong seq = Util.Coding.DecodeFixed64(record);
                uint count = Util.Coding.DecodeFixed32(record[8..]);
                var inserter = new MemTableInserter(this, walNumber, ignoreMissingColumnFamilies: true) { Sequence = seq };
                WriteBatch.Iterate(record, ref inserter);
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
        return default;
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

    private void DisposeSecondaryReaders()
    {
        foreach (FragmentBufferedLogReader r in _secondaryWalReaders.Values) r.Dispose();
        _secondaryWalReaders.Clear();
    }
}
