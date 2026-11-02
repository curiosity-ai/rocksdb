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
        try { ShutdownMonitoring(); } catch (Exception e) when (e is IOException or RocksDbException) { }
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

        InitInfoLog();
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
        SetComparatorResolver(descriptors);
        RecoveredVersionState state = ManifestReader.RecoverFromDirectory(FileSystem, DbName, _comparatorResolver, out ulong manifestNumber);
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
                ValidateRecoveredColumnFamily(cf, opts);
                var cfd = new ColumnFamilyData(cf.Id, cf.Name, opts, this) { LogNumber = cf.LogNumber, FullHistoryTsLow = cf.FullHistoryTsLow };
                var files = new FileMetaData[cf.NumLevels][];
                for (int l = 0; l < cf.NumLevels; l++)
                {
                    var list = new List<FileMetaData>(cf.Levels[l]);
                    files[l] = list.ToArray();
                }
                Versions.AddColumnFamily(cfd, files, cf.BlobFiles.Values);
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
        LogOpenSummary();
        LogInfo("DB pointer " + GetHashCode().ToString("x", CultureInfo.InvariantCulture) + " opened " + DbName);
        StartStatsDump();
        EnableBackgroundCompactions();
    }

    /// <summary>
    /// Copies a CF's options for this DB and sanitizes them
    /// (<see cref="SanitizeCfOptions"/>). Returns a copy; the caller's object is not modified.
    /// </summary>
    private ColumnFamilyOptions PrepareCfOptions(ColumnFamilyOptions options)
    {
        ColumnFamilyOptions o = options.Clone();
        SanitizeCfOptions(o, Options.AllowIngestBehind, DbPaths.Count);
        return o;
    }

    /// <summary>
    /// Port of SanitizeCfOptions (db/column_family.cc), in place: clamps the options the engine relies on to their valid
    /// ranges exactly like upstream, so that the same (possibly out-of-range) user options behave the same way in both
    /// implementations and the OPTIONS file records the sanitized values. Not ported: memtable factory / prefix bloom
    /// adjustments for unsupported memtables, ttl / periodic compaction defaults (the managed defaults are already the
    /// sanitized values).
    /// </summary>
    internal static void SanitizeCfOptions(ColumnFamilyOptions result, bool allowIngestBehind, int numDbPaths)
    {
        // ClipToRange(&write_buffer_size, 64 KiB, 64 GiB)
        result.WriteBufferSize = Math.Clamp(result.WriteBufferSize, 64UL << 10, 64UL << 30);
        // arena_block_size <= 0 is derived from write_buffer_size by MemTableOptions.GetEffectiveArenaBlockSize.
        result.MinWriteBufferNumberToMerge = Math.Min(result.MinWriteBufferNumberToMerge, result.MaxWriteBufferNumber - 1);
        if (result.MinWriteBufferNumberToMerge < 1) result.MinWriteBufferNumberToMerge = 1;
        if (result.NumLevels < 1) result.NumLevels = 1;
        if (result.CompactionStyle == CompactionStyle.Level && result.NumLevels < 2) result.NumLevels = 2;
        if (result.CompactionStyle == CompactionStyle.Universal && (allowIngestBehind || result.CfAllowIngestBehind) && result.NumLevels < 3)
        {
            result.NumLevels = 3;
        }
        if (result.MaxWriteBufferNumber < 2) result.MaxWriteBufferNumber = 2;
        if (result.MaxWriteBufferSizeToMaintain < 0)
        {
            result.MaxWriteBufferSizeToMaintain = result.MaxWriteBufferNumber * (long)result.WriteBufferSize;
        }
        // bloom filter size shouldn't exceed 1/4 of memtable size.
        if (result.MemtablePrefixBloomSizeRatio > 0.25) result.MemtablePrefixBloomSizeRatio = 0.25;
        else if (result.MemtablePrefixBloomSizeRatio < 0) result.MemtablePrefixBloomSizeRatio = 0;
        if (result.CompactionStyle == CompactionStyle.Fifo)
        {
            // since we delete level0 files in FIFO compaction when there are too many of them, these options don't
            // really mean anything
            result.Level0SlowdownWritesTrigger = int.MaxValue;
            result.Level0StopWritesTrigger = int.MaxValue;
        }
        if (result.MaxBytesForLevelMultiplier <= 0) result.MaxBytesForLevelMultiplier = 1;
        if (result.Level0FileNumCompactionTrigger == 0) result.Level0FileNumCompactionTrigger = 1;
        if (result.Level0StopWritesTrigger < result.Level0SlowdownWritesTrigger
            || result.Level0SlowdownWritesTrigger < result.Level0FileNumCompactionTrigger)
        {
            if (result.Level0SlowdownWritesTrigger < result.Level0FileNumCompactionTrigger)
            {
                result.Level0SlowdownWritesTrigger = result.Level0FileNumCompactionTrigger;
            }
            if (result.Level0StopWritesTrigger < result.Level0SlowdownWritesTrigger)
            {
                result.Level0StopWritesTrigger = result.Level0SlowdownWritesTrigger;
            }
        }
        if (result.SoftPendingCompactionBytesLimit == 0)
        {
            result.SoftPendingCompactionBytesLimit = result.HardPendingCompactionBytesLimit;
        }
        else if (result.HardPendingCompactionBytesLimit > 0 && result.SoftPendingCompactionBytesLimit > result.HardPendingCompactionBytesLimit)
        {
            result.SoftPendingCompactionBytesLimit = result.HardPendingCompactionBytesLimit;
        }
        if (result.LevelCompactionDynamicLevelBytes)
        {
            // only makes sense for level-based compaction; can't be used with multiple db_paths (cf_paths default to them)
            if (result.CompactionStyle != CompactionStyle.Level || numDbPaths > 1) result.LevelCompactionDynamicLevelBytes = false;
        }
        if (result.MaxCompactionBytes == 0)
        {
            result.MaxCompactionBytes = result.TargetFileSizeBase * 25;
        }
        // "enable_blob_direct_write requires enable_blob_files=true. Disabling blob direct write."
        if (result.EnableBlobDirectWrite && !result.EnableBlobFiles) result.EnableBlobDirectWrite = false;
        if (result.BlobDirectWritePartitions == 0) result.BlobDirectWritePartitions = 1;
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
        if (Options.Allow2Pc)
        {
            // In 2PC mode WALs below the CFs' log numbers may still hold prepared sections: start at the persisted
            // min_log_number_to_keep (DBImpl::SetupLogFilesRecovery).
            minLog = Versions.MinLogNumberToKeep;
        }
        else
        {
            lock (_mutex) minLog = Versions.MinLogNumberWithUnflushedData(null);
            minLog = Math.Max(minLog, Versions.MinLogNumberToKeep);
        }

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
        WalRecoveryMode mode = Options.WalRecoveryMode;
        ulong corruptedWal = 0;
        foreach (ulong walNumber in wals)
        {
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
                            if (err.Code == StatusCode.IOError) throw err; // likely loss of synced WAL: recovery fails
                            // We should ignore the error but not continue replaying (HandleNonOkStatusOrOldLogRecord).
                            // Records are still read: replay resumes if a later record continues the sequence
                            // (MaybeReviseStopReplayForCorruption), e.g. writes made after an earlier PIT recovery.
                            reporter.Error = null;
                            if (!progress.StopReplay) corruptedWal = walNumber;
                            progress.StopReplay = true;
                            break;
                        default:
                            throw err;
                    }
                }
                if (!got) break;
            }
        }
        if (progress.StopReplay && mode is WalRecoveryMode.PointInTimeRecovery or WalRecoveryMode.TolerateCorruptedTailRecords)
        {
            // MaybeHandleStopReplayForCorruptionForInconsistency: a CF that already flushed data beyond the point of
            // corruption cannot be made consistent with the others.
            foreach (ColumnFamilyData cfd in GetColumnFamilies())
            {
                ulong liveBytes = 0;
                VersionStorageInfo vs = cfd.Current.Storage;
                for (int l = 0; l < vs.NumLevels; l++) liveBytes += vs.NumLevelBytes(l);
                if (cfd.LogNumber > corruptedWal && liveBytes > 0)
                {
                    ThrowHelper.Corruption("Column family inconsistency: SST file contains data beyond the point of corruption in CF " + cfd.Name);
                }
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
                // Force flush on DB open if 2PC is enabled, since with 2PC we have no guarantee that consecutive log
                // files have consecutive sequence id, which make recovery complicated (SanitizeOptions).
                if (Options.AvoidFlushDuringRecovery && !Options.Allow2Pc)
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
        MarkRecoveredPrepSections();
        return edits;
    }

    private sealed class ReplayProgress
    {
        public ulong LastSequence;
        public bool AnyData;
        /// <summary>Sequence the next record must start with to resume after a corruption (next_sequence), or MaxValue.</summary>
        public ulong NextSequence = ulong.MaxValue;
        /// <summary>Point-in-time recovery hit a corruption: records are dropped until the sequence continues.</summary>
        public bool StopReplay;
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
        if (progress.StopReplay)
        {
            // MaybeReviseStopReplayForCorruption: in point-in-time recovery mode, if sequence ids are consecutive we
            // continue recovery despite the corruption.
            if (Options.WalRecoveryMode == WalRecoveryMode.PointInTimeRecovery && seq == progress.NextSequence) progress.StopReplay = false;
            else return true; // dropped
        }
        IReadOnlyDictionary<uint, int> recordedTs = reader.RecordedTimestampSizes;
        byte[]? reconciled = null;
        if (_cfTimestampSizes is not null || recordedTs.Count > 0) reconciled = HandleRecoveredBatchTimestampSizes(record, recordedTs);
        if (reconciled is not null) record = reconciled;
        var inserter = new MemTableInserter(this, walNumber, ignoreMissingColumnFamilies: true) { Sequence = seq };
        inserter.Run(record);
        // next_sequence comes from the inserter, not the record header: a prepared section counts its entries but
        // consumes no sequence numbers, and a commit marker consumes those of the prepared batch it applies.
        ulong next = inserter.Sequence;
        progress.NextSequence = next;
        if (count > 0 || next > seq) progress.AnyData = true;
        if (next > seq)
        {
            ulong last = next - 1;
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
        // WriteLevel0TableForRecovery writes blob files too (BlobFileCreationReason::kRecovery). The blob files stay
        // registered as pending outputs, like the table file, until the recovery edits are installed.
        Blob.BlobFileBuilder? blobBuilder = CreateBlobFileBuilder(cfd, outputLevel: 0, Blob.BlobFileCreationReason.Recovery);
        FileMetaData? meta;
        try
        {
            meta = await FlushJob.WriteLevel0TableAsync(FileSystem, path, cfd, [mem], fileNumber, cfd.NewEpochNumber(), Identity, [], CurrentSnapshotChecker, blobBuilder, null, synchronous, ct)
                .ConfigureAwait(false);
        }
        catch
        {
            if (blobBuilder is not null) ReleaseBlobOutputs(blobBuilder);
            throw;
        }
        finally
        {
            blobBuilder?.Dispose();
        }
        if (!edits.TryGetValue(cfd.Id, out VersionEdit? edit))
        {
            edit = new VersionEdit { ColumnFamily = cfd.Id };
            edits[cfd.Id] = edit;
        }
        if (meta is not null)
        {
            FileSystem.SyncDirectory(DbPaths[0]);
            edit.AddFile(0, meta);
            if (blobBuilder is not null) edit.BlobFileAdditions.AddRange(blobBuilder.Additions);
            // Keep the obsolete-file scan from deleting the new (not yet installed) file.
            lock (_mutex) _pendingOutputs.Add(fileNumber);
            // WriteLevel0TableForRecovery: with UDT in memtable only, move the cutoff timestamp up whenever a flush happens.
            if (GetFullHistoryTsLowAboveCutoffUdt(cfd, [mem]) is { } low
                && (edit.FullHistoryTsLow is not { } prev || cfd.UserComparator.CompareTimestamp(low, prev) > 0))
            {
                edit.FullHistoryTsLow = low;
            }
        }
        lock (_mutex)
        {
            cfd.Mem = cfd.ConstructNewMemtable(lastSequence);
            InstallSuperVersion(cfd);
            if (mem.Unref()) mem.Dispose();
        }
    }
}
