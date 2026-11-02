using System.Globalization;
using RocksDb.Managed.Wal;

namespace RocksDb.Managed.Db;

internal sealed partial class DbImpl
{
    /// <summary>
    /// Opens (creating if allowed) the DB at <paramref name="path"/>. Port of DB::Open / DBImpl::Open / DBImpl::Recover
    /// (db/db_impl/db_impl_open.cc): lock, create (NewDB) or read CURRENT + MANIFEST, open the column families, replay
    /// WALs into memtables, flush the recovered memtables to L0 (unless avoid_flush_during_recovery), start a new WAL,
    /// write a new MANIFEST, create missing column families, delete obsolete files and write an OPTIONS file.
    /// With <paramref name="synchronous"/> all I/O is blocking and the returned task is complete.
    /// </summary>
    public static async ValueTask<DbImpl> OpenAsync(string path, DbOptions options, IReadOnlyList<ColumnFamilyDescriptor> columnFamilies,
        bool synchronous, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(columnFamilies);
        string dbname = path.Length > 1 ? path.TrimEnd('/') : path;
        var db = new DbImpl(dbname, options.Clone());
        try
        {
            await db.OpenCoreAsync(columnFamilies, synchronous, cancellationToken).ConfigureAwait(false);
            return db;
        }
        catch
        {
            db.AbortOpen();
            throw;
        }
    }

    private void AbortOpen()
    {
        Volatile.Write(ref _disposed, 1);
        try
        {
            _log?.Dispose();
            Versions.DisposeAsync().AsTask().GetAwaiter().GetResult();
            lock (_mutex)
            {
                foreach (ColumnFamilyData cfd in Versions.ColumnFamilies)
                {
                    if (cfd.Current is not null) ReleaseColumnFamilyResources(cfd);
                }
            }
        }
        catch (Exception e) when (e is IOException or RocksDbException or ObjectDisposedException)
        {
        }
        _groupBatch.Dispose();
        _dbLock?.Dispose();
        _dbLock = null;
    }

    private async ValueTask OpenCoreAsync(IReadOnlyList<ColumnFamilyDescriptor> columnFamilies, bool synchronous, CancellationToken ct)
    {
        // ---- descriptors (DB::Open requires the default CF; we add it with default options when omitted) ----
        var descriptors = new Dictionary<string, ColumnFamilyOptions>(StringComparer.Ordinal);
        foreach (ColumnFamilyDescriptor d in columnFamilies)
        {
            if (!descriptors.TryAdd(d.Name, d.Options)) ThrowHelper.InvalidArgument("Duplicate column family name: " + d.Name);
        }
        if (!descriptors.ContainsKey(DefaultColumnFamilyName)) descriptors[DefaultColumnFamilyName] = new ColumnFamilyOptions();

        // ---- directories + LOCK (DBImpl::Recover) ----
        if (Options.CreateIfMissing)
        {
            FileSystem.CreateDirectoryIfMissing(DbName);
            if (WalDir != DbName) FileSystem.CreateDirectoryIfMissing(WalDir);
            for (int i = 1; i < DbPaths.Count; i++) FileSystem.CreateDirectoryIfMissing(DbPaths[i]);
        }
        try
        {
            _dbLock = FileSystem.LockFile(FileNames.LockFileName(DbName));
        }
        catch (RocksDbException e) when (e.SubCode == StatusSubCode.PathNotFound && !Options.CreateIfMissing)
        {
            throw RocksDbException.InvalidArgument(DbName + ": does not exist (create_if_missing is false)");
        }

        bool exists = FileSystem.FileExists(FileNames.CurrentFilePath(DbName));
        if (!exists)
        {
            if (!Options.CreateIfMissing) ThrowHelper.InvalidArgument(DbName + ": does not exist (create_if_missing is false)");
            await NewDbAsync(descriptors[DefaultColumnFamilyName], synchronous, ct).ConfigureAwait(false);
        }
        else if (Options.ErrorIfExists)
        {
            ThrowHelper.InvalidArgument(DbName + ": exists (error_if_exists is true)");
        }

        // ---- MANIFEST (VersionSet::Recover) ----
        RecoveredVersionState state = ManifestReader.RecoverFromDirectory(FileSystem, DbName,
            static name => Comparator.TryGetBuiltin(name, out Comparator? c) ? c : null, out ulong manifestNumber);
        Versions.RecoverFrom(state, manifestNumber);

        // Missing descriptors for existing CFs ("Column families not opened").
        var notOpened = new List<string>();
        foreach (ColumnFamilyVersion cf in state.ColumnFamilies.Values)
        {
            if (!descriptors.ContainsKey(cf.Name)) notOpened.Add(cf.Name);
        }
        if (notOpened.Count > 0) ThrowHelper.InvalidArgument("Column families not opened: " + string.Join(", ", notOpened));
        var missing = new List<string>();
        foreach (string name in descriptors.Keys)
        {
            if (state.FindColumnFamily(name) is null) missing.Add(name);
        }
        if (missing.Count > 0 && !Options.CreateMissingColumnFamilies) ThrowHelper.InvalidArgument("Column family not found: " + missing[0]);

        // ---- DB identity (DBImpl::SetupDBId) ----
        string? dbId = state.DbId;
        if (dbId is null && FileSystem.FileExists(FileNames.IdentityFileName(DbName))) dbId = FileNames.ReadIdentityFile(FileSystem, DbName);
        dbId ??= FileNames.GenerateUniqueId();
        if (Options.WriteIdentityFile && !FileSystem.FileExists(FileNames.IdentityFileName(DbName)))
        {
            await AwaitIo2(FileNames.SetIdentityFileAsync(FileSystem, DbName, dbId, ct), synchronous).ConfigureAwait(false);
        }
        Versions.DbId = dbId;
        Identity = Identity with { DbId = dbId };

        // ---- column families from the MANIFEST ----
        lock (_mutex)
        {
            foreach (ColumnFamilyVersion cf in state.ColumnFamilies.Values)
            {
                ColumnFamilyOptions opts = PrepareCfOptions(descriptors[cf.Name]);
                if (cf.ComparatorName is not null && cf.ComparatorName != opts.ComparatorName)
                {
                    ThrowHelper.InvalidArgument(opts.ComparatorName + " does not match existing comparator " + cf.ComparatorName);
                }
                var cfd = new ColumnFamilyData(cf.Id, cf.Name, opts, this) { LogNumber = cf.LogNumber };
                var files = new FileMetaData[cf.NumLevels][];
                for (int l = 0; l < cf.NumLevels; l++)
                {
                    var list = new List<FileMetaData>(cf.Levels[l]);
                    files[l] = list.ToArray();
                }
                Versions.AddColumnFamily(cfd, files);
                cfd.RecoverEpochNumbers();
                cfd.Mem = cfd.ConstructNewMemtable(state.LastSequence);
                InstallSuperVersion(cfd);
                RegisterCfForWrite(cfd);
            }
        }
        // LoadTables: every live table file must exist.
        foreach (ColumnFamilyData cfd in GetColumnFamilies())
        {
            VersionStorageInfo s = cfd.Current.Storage;
            for (int l = 0; l < s.NumLevels; l++)
            {
                foreach (FileMetaData f in s.LevelFiles(l))
                {
                    string p = FileNames.TableFileName(DbPaths, f.FileNumber, f.PathId);
                    if (!FileSystem.FileExists(p)) ThrowHelper.Corruption("IO error: No such file or directory: While open a file for random read: " + p);
                }
            }
        }

        // ---- WAL replay (DBImpl::RecoverLogFiles) ----
        Dictionary<uint, VersionEdit> recoveryEdits = await RecoverLogFilesAsync(synchronous, ct).ConfigureAwait(false);

        // ---- new WAL + recovery edits (first LogAndApply writes the new MANIFEST) ----
        ulong newLogNumber = Versions.NewFileNumber();
        _log = CreateWal(newLogNumber);
        _logEmpty = true;
        lock (_mutex)
        {
            _logNumber = newLogNumber;
            _aliveWals.Add(newLogNumber);
        }
        foreach (ColumnFamilyData cfd in GetColumnFamilies())
        {
            if (!recoveryEdits.TryGetValue(cfd.Id, out VersionEdit? edit))
            {
                edit = new VersionEdit { ColumnFamily = cfd.Id };
            }
            // All data of this CF in the replayed WALs is now in table files (or there was none).
            if (cfd.Mem.IsEmpty && cfd.Imm.Count == 0) edit.LogNumber = Math.Max(cfd.LogNumber, newLogNumber);
            await Versions.LogAndApplyAsync(cfd, edit, synchronous, ct).ConfigureAwait(false);
        }
        lock (_mutex)
        {
            foreach (ColumnFamilyData cfd in Versions.ColumnFamilies) InstallSuperVersion(cfd);
            _pendingOutputs.Clear();
        }

        // ---- create missing column families ----
        foreach (string name in missing)
        {
            await CreateColumnFamilyCoreAsync(name, descriptors[name], writeOptionsFile: false, synchronous, ct).ConfigureAwait(false);
        }

        DeleteObsoleteFilesFullScan();
        await WriteOptionsFileAsync(synchronous, ct).ConfigureAwait(false);
        DeleteObsoleteFilesFullScan();
    }

    /// <summary>Resolves the pluggable objects of a CF's options for this DB (merge operator name). Returns a copy.</summary>
    private static ColumnFamilyOptions PrepareCfOptions(ColumnFamilyOptions options)
    {
        ColumnFamilyOptions o = options.Clone();
        if (o.MergeOperator is not null) o.MergeOperatorName = o.MergeOperator.Name;
        return o;
    }

    private static ValueTask AwaitIo(ValueTask t, bool synchronous)
    {
        if (synchronous && !t.IsCompleted)
        {
            t.AsTask().GetAwaiter().GetResult();
            return default;
        }
        return t;
    }

    /// <summary>Port of DBImpl::NewDB: writes MANIFEST-000001 describing an empty DB and points CURRENT at it.</summary>
    private async ValueTask NewDbAsync(ColumnFamilyOptions defaultCf, bool synchronous, CancellationToken ct)
    {
        string? dbId = null;
        if (Options.WriteIdentityFile)
        {
            dbId = await AwaitIo2(FileNames.SetIdentityFileAsync(FileSystem, DbName, null, ct), synchronous).ConfigureAwait(false);
        }
        else
        {
            dbId = FileNames.GenerateUniqueId();
        }
        var edit = new VersionEdit
        {
            LogNumber = 0,
            NextFileNumber = 2,
            LastSequence = 0,
            ComparatorName = defaultCf.ComparatorName,
            PersistUserDefinedTimestamps = defaultCf.PersistUserDefinedTimestamps,
        };
        if (Options.WriteDbIdToManifest) edit.DbId = dbId;
        const ulong manifestNumber = 1;
        string manifest = FileNames.DescriptorFileName(DbName, manifestNumber);
        try
        {
            using (ManifestLogWriter log = ManifestLogWriter.Create(FileSystem, manifest))
            {
                var writer = new ManifestWriter(log);
                await AwaitIo(writer.AddEditAsync(edit, 0, sync: true, ct), synchronous).ConfigureAwait(false);
            }
            await AwaitIo(FileNames.SetCurrentFileAsync(FileSystem, DbName, manifestNumber, syncDirectory: true, ct), synchronous).ConfigureAwait(false);
        }
        catch
        {
            TryDeleteFile(manifest);
            throw;
        }
    }

    private static ValueTask<T> AwaitIo2<T>(ValueTask<T> t, bool synchronous)
    {
        if (synchronous && !t.IsCompleted) return new ValueTask<T>(t.AsTask().GetAwaiter().GetResult());
        return t;
    }

    /// <summary>LogReporter for WAL recovery (DBImpl::RecoverLogFiles LogReporter): remembers the first error.</summary>
    private sealed class WalRecoveryReporter : LogReporter
    {
        public RocksDbException? Error;
        public string FileName = "";

        public override void Corruption(long bytes, StatusCode code, string reason, ulong logNumber)
        {
            Error ??= new RocksDbException(code == StatusCode.IOError ? StatusCode.IOError : StatusCode.Corruption,
                (code == StatusCode.IOError ? "IO error: " : "Corruption: ") + FileName + ": dropping " + bytes.ToString(CultureInfo.InvariantCulture) +
                " bytes; " + reason);
        }
    }

    /// <summary>
    /// Port of DBImpl::RecoverLogFiles / ProcessLogFiles: replays every WAL with a number &gt;= the minimum log number
    /// with unflushed data, in order, into the memtables (skipping CFs that already flushed that WAL), honoring
    /// wal_recovery_mode. Recovered memtables are flushed to L0 (WriteLevel0TableForRecovery) when they fill up and,
    /// unless avoid_flush_during_recovery, at the end. Returns the per-CF edits to apply.
    /// </summary>
    private async ValueTask<Dictionary<uint, VersionEdit>> RecoverLogFilesAsync(bool synchronous, CancellationToken ct)
    {
        var edits = new Dictionary<uint, VersionEdit>();
        ulong minLog;
        lock (_mutex) minLog = Versions.MinLogNumberWithUnflushedData(null);
        minLog = Math.Max(minLog, Versions.MinLogNumberToKeep);

        var wals = new List<ulong>();
        foreach (string child in FileSystem.GetChildren(WalDir))
        {
            if (FileNames.TryParseFileName(child, out ulong number, out FileType type) && type == FileType.WalFile)
            {
                Versions.MarkFileNumberUsed(number);
                if (number >= minLog) wals.Add(number);
            }
        }
        wals.Sort();
        if (wals.Count == 0) return edits;

        var progress = new ReplayProgress { LastSequence = Versions.LastSequence };
        bool stopReplay = false;
        WalRecoveryMode mode = Options.WalRecoveryMode;
        foreach (ulong walNumber in wals)
        {
            if (stopReplay) break;
            string fname = FileNames.LogFileName(WalDir, walNumber);
            var reporter = new WalRecoveryReporter { FileName = fname };
            using var reader = new LogReader(FileSystem.OpenSequential(fname), reporter, checksum: Options.ParanoidChecks, walNumber)
            {
                RecoveryMode = mode,
            };
            while (true)
            {
                bool got = ReplayNextRecord(reader, reporter, walNumber, progress);
                // Memtables that filled up during recovery are written out right away.
                if (_flushScheduler.Count > 0)
                {
                    foreach (ColumnFamilyData cfd in _flushScheduler)
                    {
                        await FlushRecoveredMemTableAsync(cfd, edits, progress.LastSequence, synchronous, ct).ConfigureAwait(false);
                    }
                    _flushScheduler.Clear();
                }
                if (reporter.Error is { } err)
                {
                    switch (mode)
                    {
                        case WalRecoveryMode.SkipAnyCorruptedRecords:
                            reporter.Error = null;
                            break;
                        case WalRecoveryMode.PointInTimeRecovery:
                            // We should ignore the error but not continue replaying.
                            stopReplay = true;
                            break;
                        default:
                            throw err;
                    }
                }
                if (!got || stopReplay) break;
            }
        }
        ulong lastSequence = progress.LastSequence;
        bool anyData = progress.AnyData;

        Versions.SetLastSequence(lastSequence);
        if (anyData)
        {
            Versions.MarkFileNumberUsed(wals[^1] + 1);
            foreach (ColumnFamilyData cfd in GetColumnFamilies())
            {
                if (cfd.Mem.IsEmpty) continue;
                if (Options.AvoidFlushDuringRecovery)
                {
                    // Keep the data in the memtable; the WALs stay alive until it is flushed.
                    continue;
                }
                await FlushRecoveredMemTableAsync(cfd, edits, lastSequence, synchronous, ct).ConfigureAwait(false);
            }
        }
        lock (_mutex)
        {
            foreach (ulong n in wals) _aliveWals.Add(n);
        }
        return edits;
    }

    private sealed class ReplayProgress
    {
        public ulong LastSequence;
        public bool AnyData;
    }

    /// <summary>Reads one WAL record and inserts it into the memtables. Returns false at the end of the WAL.</summary>
    private bool ReplayNextRecord(LogReader reader, WalRecoveryReporter reporter, ulong walNumber, ReplayProgress progress)
    {
        if (!reader.TryReadRecord(out ReadOnlySpan<byte> record)) return false;
        if (record.Length < WriteBatch.HeaderSize)
        {
            reporter.Corruption(record.Length, StatusCode.Corruption, "log record too small", walNumber);
            return true;
        }
        ulong seq = Util.Coding.DecodeFixed64(record);
        uint count = Util.Coding.DecodeFixed32(record[8..]);
        var inserter = new MemTableInserter(this, walNumber, ignoreMissingColumnFamilies: true) { Sequence = seq };
        WriteBatch.Iterate(record, ref inserter);
        if (count > 0)
        {
            progress.AnyData = true;
            ulong last = seq + count - 1;
            if (last > progress.LastSequence) progress.LastSequence = last;
        }
        return true;
    }

    /// <summary>Port of WriteLevel0TableForRecovery: writes the CF's current memtable to an L0 file and replaces it.</summary>
    private async ValueTask FlushRecoveredMemTableAsync(ColumnFamilyData cfd, Dictionary<uint, VersionEdit> edits, ulong lastSequence, bool synchronous,
        CancellationToken ct)
    {
        MemTable mem = cfd.Mem;
        if (mem.IsEmpty) return;
        ulong fileNumber = Versions.NewFileNumber();
        string path = FileNames.TableFileName(DbPaths, fileNumber, 0);
        FileMetaData? meta = await FlushJob.WriteLevel0TableAsync(FileSystem, path, cfd, [mem], fileNumber, cfd.NewEpochNumber(), Identity, synchronous, ct)
            .ConfigureAwait(false);
        if (!edits.TryGetValue(cfd.Id, out VersionEdit? edit))
        {
            edit = new VersionEdit { ColumnFamily = cfd.Id };
            edits[cfd.Id] = edit;
        }
        if (meta is not null)
        {
            FileSystem.SyncDirectory(DbPaths[0]);
            edit.AddFile(0, meta);
            // Keep the obsolete-file scan from deleting the new (not yet installed) file.
            lock (_mutex) _pendingOutputs.Add(fileNumber);
        }
        lock (_mutex)
        {
            cfd.Mem = cfd.ConstructNewMemtable(lastSequence);
            InstallSuperVersion(cfd);
            if (mem.Unref()) mem.Dispose();
        }
    }
}
