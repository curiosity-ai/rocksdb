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
        DbOptions sanitized = options.Clone();
        ValidateDbOptions(sanitized);
        SanitizeDbOptions(sanitized);
        SanitizeWalOptions(sanitized);
        var db = new DbImpl(dbname, sanitized);
        try
        {
            await db.OpenCoreAsync(columnFamilies, synchronous, cancellationToken).ConfigureAwait(false);
            db.RegisterWithWriteBufferManager();
            return db;
        }
        catch
        {
            db.AbortOpen();
            throw;
        }
    }

    /// <summary>recovered_seq of DBImpl::Open: the next sequence after a point-in-time recovery found corruption, else MaxValue.</summary>
    private ulong _recoveredSeq = ulong.MaxValue;

    /// <summary>
    /// DBImpl::Open after a point-in-time recovery that found a corrupted WAL: an empty batch with the expected next
    /// sequence is written to the new WAL and synced. The corrupted WAL may be kept (2PC keeps WALs down to the persisted
    /// min_log_number_to_keep; WritePrepared also has sequence gaps); the dummy record tells the next recovery that the
    /// new WAL continues the sequence, so it resumes after the corrupted WAL (MaybeReviseStopReplayForCorruption) instead
    /// of reporting the column families flushed past it as inconsistent.
    /// </summary>
    private void WriteRecoveredSequenceRecord(ulong sequence)
    {
        Span<byte> batch = stackalloc byte[WriteBatch.HeaderSize];
        batch.Clear();
        Util.Coding.EncodeFixed64(batch, sequence);
        LogWriter log = _log!;
        log.AddRecord(batch, sequence);
        log.WriteBuffer();
        log.Sync();
        lock (_mutex)
        {
            _logEmpty = false;
            _currentWalSize += batch.Length;
        }
    }

    /// <summary>
    /// The WAL part of SanitizeOptions (db/db_impl/db_impl_open.cc), in place: WAL archival (WAL_ttl_seconds /
    /// WAL_size_limit_MB) disables WAL recycling, and so do kTolerateCorruptedTailRecords (it cannot tell the end of the
    /// new data in a recycled WAL from a real corruption) and kAbsoluteConsistency (upstream: temporarily, because of a
    /// hole in the recovered data, PR 7252).
    /// </summary>
    internal static void SanitizeWalOptions(DbOptions result)
    {
        if (result.WalTtlSeconds > 0 || result.WalSizeLimitMb > 0) result.RecycleLogFileNum = 0;
        if (result.RecycleLogFileNum != 0
            && result.WalRecoveryMode is WalRecoveryMode.TolerateCorruptedTailRecords or WalRecoveryMode.AbsoluteConsistency)
        {
            result.RecycleLogFileNum = 0;
        }
    }

    private void AbortOpen()
    {
        DeregisterFromWriteBufferManager();
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
        DisposeOwnedSstFileManager();
        SharedTableCache.Dispose();
        _dbLock?.Dispose();
        _dbLock = null;
    }

    /// <summary>
    /// The write-mode checks of DBImpl::ValidateOptions (db/db_impl/db_impl_open.cc); messages copied verbatim.
    /// Upstream checks the column families first: a column family with blob direct write reports its own NotSupported for these write
    /// modes (<see cref="ColumnFamilyData"/>), so the DB-level checks are left to it then.
    /// </summary>
    private static void ValidateWriteModeOptions(DbOptions o, IReadOnlyList<ColumnFamilyDescriptor> columnFamilies)
    {
        foreach (ColumnFamilyDescriptor d in columnFamilies)
        {
            if (d.Options is { EnableBlobDirectWrite: true, EnableBlobFiles: true }) return;
        }
        if (o.UnorderedWrite && !o.AllowConcurrentMemtableWrite)
            ThrowHelper.InvalidArgument("unordered_write is incompatible with !allow_concurrent_memtable_write");
        if (o.UnorderedWrite && o.EnablePipelinedWrite)
            ThrowHelper.InvalidArgument("unordered_write is incompatible with enable_pipelined_write");
        if (o.AtomicFlush && o.EnablePipelinedWrite)
            ThrowHelper.InvalidArgument("atomic_flush is incompatible with enable_pipelined_write");
    }

    private async ValueTask OpenCoreAsync(IReadOnlyList<ColumnFamilyDescriptor> columnFamilies, bool synchronous, CancellationToken ct)
    {
        // ---- descriptors (DB::Open requires the default CF; we add it with default options when omitted) ----
        ValidateWriteModeOptions(Options, columnFamilies);
        var descriptors = new Dictionary<string, ColumnFamilyOptions>(StringComparer.Ordinal);
        foreach (ColumnFamilyDescriptor d in columnFamilies)
        {
            if (!descriptors.TryAdd(d.Name, d.Options)) ThrowHelper.InvalidArgument("Duplicate column family name: " + d.Name);
        }
        if (!descriptors.ContainsKey(DefaultColumnFamilyName)) descriptors[DefaultColumnFamilyName] = new ColumnFamilyOptions();
        SetupSstFileManager();

        // ---- directories + LOCK (DBImpl::Recover) ----
        if (Options.CreateIfMissing)
        {
            FileSystem.CreateDirectoryIfMissing(DbName);
            if (WalDir != DbName) FileSystem.CreateDirectoryIfMissing(WalDir);
            foreach (string path in DbPaths)
            {
                if (path != DbName) FileSystem.CreateDirectoryIfMissing(path);
            }
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
        // Best-efforts recovery ignores CURRENT: any non-empty MANIFEST means the DB exists.
        bool exists = Options.BestEffortsRecovery ? HasNonEmptyManifest() : FileSystem.FileExists(FileNames.CurrentFilePath(DbName));
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
        RecoveredVersionState state;
        ulong manifestNumber;
        if (Options.BestEffortsRecovery)
        {
            state = ManifestReader.TryRecoverPointInTime(FileSystem, DbName, _comparatorResolver, new RecoveryFileVerifier(this, descriptors), out manifestNumber,
                out bool hasMissingFiles);
            if (hasMissingFiles) LogWarn("Best-efforts recovery: some table or blob files are missing or corrupted; recovered to the latest consistent point in time");
        }
        else
        {
            state = ManifestReader.RecoverFromDirectory(FileSystem, DbName, _comparatorResolver, out manifestNumber);
        }
        Versions.RecoverFrom(state, manifestNumber);
        AddImplicitStatsColumnFamily(descriptors, state.FindColumnFamily(PersistentStatsColumnFamilyName) is not null);

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
        // VersionSet::Recover reads with Env::IOActivity::kDBOpen (table properties for the version stats, LoadTables):
        // the reads are timed in FILE_READ_DB_OPEN_MICROS.
        int openActivity = Monitoring.FileReadActivity.Enter(Histogram.FileReadDbOpenMicros);
        try
        {
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
                    var cursors = new List<CompactCursor>(cf.CompactCursors.Count);
                    foreach (KeyValuePair<int, byte[]> c in cf.CompactCursors) cursors.Add(new CompactCursor(c.Key, c.Value));
                    Versions.AddColumnFamily(cfd, files, cf.BlobFiles.Values, cursors, updateStats: !Options.SkipStatsUpdateOnDbOpen);
                    cfd.RecoverEpochNumbers();
                    cfd.Mem = cfd.ConstructNewMemtable(state.LastSequence);
                    InstallSuperVersion(cfd);
                    RegisterCfForWrite(cfd);
                }
            }
            // LoadTables (before anything is written: a failure leaves the MANIFEST untouched).
            if (!Options.BestEffortsRecovery) LoadTableHandlers(FileNames.DescriptorFileName(DbName, manifestNumber));
            foreach (ColumnFamilyData cfd in GetColumnFamilies())
            {
                VersionStorageInfo s = cfd.Current.Storage;
                // The FIFO TTL picker needs the newest key time of every L0 file (upstream: max_open_files = -1 pins every
                // reader at open, LoadTableHandlers); opening the readers here records it in FileMetaData.NewestKeyTime.
                if (cfd.Options.CompactionStyle == CompactionStyle.Fifo && cfd.Options.Ttl > 0)
                {
                    foreach (FileMetaData f in s.LevelFiles(0))
                    {
                        using TableReaderLease lease = cfd.TableCache.GetReader(f);
                    }
                }
            }
        }
        finally
        {
            Monitoring.FileReadActivity.Exit(openActivity);
        }

        // ---- WALs tracked in the MANIFEST (DBImpl::Recover: WalSet::CheckWals) ----
        VersionEdit? untrackWals = VerifyTrackedWalsAtOpen();

        // ---- WAL replay (DBImpl::RecoverLogFiles) ----
        Dictionary<uint, VersionEdit> recoveryEdits = await RecoverLogFilesAsync(isNewDb: !exists, synchronous, ct).ConfigureAwait(false);

        // ---- new WAL + recovery edits (first LogAndApply writes the new MANIFEST) ----
        ulong newLogNumber = Versions.NewFileNumber();
        // Like upstream (a TODO there), the first WAL of an open records no predecessor, even when recovered WALs are kept
        // (avoid_flush_during_recovery).
        _log = CreateWal(newLogNumber, predecessor: null, recycleLogNumber: 0);
        _logEmpty = true;
        lock (_mutex)
        {
            // Prevent WALs created by a previous instance from being recycled: they might be in alive_wal_files_ and
            // get recycled otherwise (min_wal_number_to_recycle_).
            _minWalNumberToRecycle = newLogNumber;
            _logNumber = newLogNumber;
            _walTrackingStart = newLogNumber;
            _aliveWals.Add(newLogNumber);
        }
        if (_recoveredSeq != ulong.MaxValue) WriteRecoveredSequenceRecord(_recoveredSeq);
        if (untrackWals is not null) await Versions.LogAndApplyAsync(null, untrackWals, synchronous, ct).ConfigureAwait(false);
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
        // After reaching the post-recovery seqno but before creating SuperVersions ensure seqno to time mapping is
        // pre-populated as needed (a new DB with preserve / preclude set on any column family it is opened with).
        if (!exists)
        {
            var preserveInfo = new MinAndMaxPreserveSeconds();
            foreach (ColumnFamilyOptions o in descriptors.Values) preserveInfo.Combine(o);
            if (preserveInfo.IsEnabled) await PrepopulateSeqnoToTimeMappingAsync(preserveInfo, synchronous, ct).ConfigureAwait(false);
        }
        lock (_mutex)
        {
            foreach (ColumnFamilyData cfd in Versions.ColumnFamilies)
            {
                // InstallSuperVersionForConfigChange
                EnsureSeqnoToTimeMapping(cfd.Options);
                InstallSuperVersion(cfd);
            }
            _pendingOutputs.Clear();
        }

        // ---- create missing column families ----
        foreach (string name in missing)
        {
            await CreateColumnFamilyCoreAsync(name, descriptors[name], writeOptionsFile: false, synchronous, ct).ConfigureAwait(false);
        }
        if (Options.PersistStatsToDisk) await InitPersistStatsColumnFamilyAsync(synchronous, ct).ConfigureAwait(false);

        StartSstFileManager();
        DeleteObsoleteFilesFullScan(atOpen: true);
        await WriteOptionsFileAsync(synchronous, ct).ConfigureAwait(false);
        DeleteObsoleteFilesFullScan(atOpen: true);
        lock (_mutex) _deleteObsoleteFilesLastRun = DateTime.UtcNow.Ticks / 10;
        LogOpenSummary();
        LogInfo("DB pointer " + GetHashCode().ToString("x", CultureInfo.InvariantCulture) + " opened " + DbName);
        StartStatsDump();
        StartStatsPersist();
        EnableBackgroundCompactions();
        RegisterRecordSeqnoTimeWorker();
    }

    /// <summary>
    /// Port of VersionEditHandler::LoadTables / VersionBuilder::LoadTableHandlers for DB::Open: opens the table readers of
    /// the recovered versions into the table cache: all of them with an unbounded table cache (max_open_files = -1, the
    /// default), else at most min(16, capacity / 4) (kInitialLoadLimit). Opening a table checks that the file exists,
    /// that its size matches the MANIFEST, its footer and meta blocks, and (verify_sst_unique_id_in_manifest) its unique
    /// id, so a missing or truncated table fails the open here with paranoid_checks (the default), before the recovery
    /// writes a new MANIFEST; without paranoid_checks errors are ignored (and surface when the file is read). Tables not
    /// loaded at open are checked the same way when first opened. Errors are reported like VersionEditHandler: a missing
    /// file (PathNotFound) becomes Corruption, and Corruption messages name the MANIFEST.
    /// </summary>
    private void LoadTableHandlers(string manifestPath)
    {
        TableCacheStore store = SharedTableCache;
        bool alwaysLoad = store.Capacity == int.MaxValue;
        // kInitialLoadLimit = 16, capped at a quarter of the table cache.
        int loadLimit = alwaysLoad ? int.MaxValue : Math.Min(16, store.Capacity / 4);
        foreach (ColumnFamilyData cfd in GetColumnFamilies())
        {
            VersionStorageInfo s = cfd.Current.Storage;
            for (int l = 0; l < s.NumLevels; l++)
            {
                foreach (FileMetaData f in s.LevelFiles(l))
                {
                    if (!alwaysLoad && store.Count >= loadLimit) return;
                    try
                    {
                        using TableReaderLease lease = cfd.TableCache.GetReader(f);
                    }
                    catch (Exception e) when (e is RocksDbException or IOException or InvalidDataException)
                    {
                        if (Options.ParanoidChecks)
                        {
                            if (e is not RocksDbException re) throw;
                            // VersionEditHandler::CheckIterationResult: PathNotFound marks the DB as corrupted.
                            if (re.SubCode == StatusSubCode.PathNotFound)
                                throw new RocksDbException(StatusCode.Corruption, "Corruption: " + re.Message + "  The file " + manifestPath + " may be corrupted.", StatusSubCode.None, re);
                            // VersionEditHandlerBase::Iterate appends the MANIFEST name to Corruption messages.
                            if (re.Code == StatusCode.Corruption)
                                throw new RocksDbException(StatusCode.Corruption, re.Message + "  The file " + manifestPath + " may be corrupted.", StatusSubCode.None, re);
                            throw;
                        }
                        LogWarn("LoadTableHandlers: ignoring table file " + f.FileNumber.ToString(CultureInfo.InvariantCulture)
                            + " that failed to open (paranoid_checks = false): " + e.Message);
                    }
                }
            }
        }
    }

    /// <summary>
    /// Copies a CF's options for this DB and sanitizes them
    /// (<see cref="SanitizeCfOptions"/>). Returns a copy; the caller's object is not modified.
    /// </summary>
    private ColumnFamilyOptions PrepareCfOptions(ColumnFamilyOptions options)
    {
        ColumnFamilyOptions o = options.Clone();
        SanitizeCfOptions(o, Options.AllowIngestBehind, DbPaths.Count);
        // ColumnFamilyData::ValidateOptions (DB::Open / CreateColumnFamily).
        ValidateCfOptions(Options, o);
        o.DbOffpeakTime = OffpeakTime;
        return o;
    }

    /// <summary>
    /// Port of SanitizeCfOptions (db/column_family.cc), in place: clamps the options the engine relies on to their valid
    /// ranges exactly like upstream, so that the same (possibly out-of-range) user options behave the same way in both
    /// implementations and the OPTIONS file records the sanitized values. Not ported: memtable factory / prefix bloom
    /// adjustments for unsupported memtables; the ttl default is already the sanitized value (30 days).
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
        result.DbAllowIngestBehind = allowIngestBehind;
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
            // For FIFO with use_kv_ratio_compaction, leave max_compaction_bytes as 0 to signal "auto-calculate target
            // from capacity and SST/blob ratio".
            if (result.CompactionStyle != CompactionStyle.Fifo || !result.CompactionOptionsFifo.UseKvRatioCompaction)
            {
                result.MaxCompactionBytes = result.TargetFileSizeBase * 25;
            }
        }
        // periodic_compaction_seconds defaults (the managed port only has block-based tables).
        const ulong adjustedPeriodicCompSecs = 30UL * 24 * 60 * 60;
        if (result.CompactionStyle == CompactionStyle.Level)
        {
            if ((result.CompactionFilter is not null || result.CompactionFilterFactory is not null)
                && result.PeriodicCompactionSeconds == ColumnFamilyOptions.DefaultPeriodicCompactionSeconds)
            {
                result.PeriodicCompactionSeconds = adjustedPeriodicCompSecs;
            }
        }
        else if (result.CompactionStyle == CompactionStyle.Universal)
        {
            if (result.PeriodicCompactionSeconds == ColumnFamilyOptions.DefaultPeriodicCompactionSeconds)
            {
                result.PeriodicCompactionSeconds = adjustedPeriodicCompSecs;
            }
        }
        else if (result.CompactionStyle == CompactionStyle.Fifo)
        {
            // "last_level_temperature is ignored with FIFO compaction. Consider
            // CompactionOptionsFIFO::file_temperature_age_thresholds."
            result.LastLevelTemperature = Temperature.Unknown;
        }
        // For universal compaction, `ttl` and `periodic_compaction_seconds` mean the same thing, take the stricter value.
        if (result.CompactionStyle == CompactionStyle.Universal)
        {
            if (result.PeriodicCompactionSeconds == 0) result.PeriodicCompactionSeconds = result.Ttl;
            else if (result.Ttl != 0) result.PeriodicCompactionSeconds = Math.Min(result.Ttl, result.PeriodicCompactionSeconds);
        }
        if (result.PeriodicCompactionSeconds == ColumnFamilyOptions.DefaultPeriodicCompactionSeconds) result.PeriodicCompactionSeconds = 0;
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

    /// <summary>
    /// LogReporter for WAL recovery (DBOpenLogRecordReadReporter). <see cref="Error"/> is the per-WAL status of
    /// DBImpl::ProcessLogFile: reports set it (first one wins) only while <see cref="RecordErrors"/> (paranoid_checks and
    /// not kSkipAnyCorruptedRecords; otherwise upstream's reporter has no status), but the WAL filter sets it directly.
    /// </summary>
    private sealed class WalRecoveryReporter : LogReporter
    {
        public RocksDbException? Error;
        public string FileName = "";
        public bool RecordErrors = true;
        /// <summary>Log number named by the recorded corruption (GetCorruptedLogNumber), or UnspecifiedLogNumber.</summary>
        public ulong CorruptedLogNumber = UnspecifiedLogNumber;
        /// <summary>
        /// The reader met a record from a previous use of a recycled WAL (DBOpenLogRecordReadReporter::OldLogRecord,
        /// reported in point-in-time mode only). Set whatever paranoid_checks says, like upstream's old_log_record.
        /// </summary>
        public bool OldLogRecordSeen;

        public override void OldLogRecord(long bytes)
        {
            OldLogRecordSeen = true;
            Log?.Log(InfoLogLevel.Warn, FileName + ": dropping " + bytes.ToString(CultureInfo.InvariantCulture) + " bytes; possibly recycled");
        }

        /// <summary>The info LOG receiving the dropped-bytes warnings, or null.</summary>
        public InfoLogger? Log;

        /// <summary>
        /// DBOpenLogRecordReadReporter::Corruption: the dropped bytes are logged ("(ignoring error) " when the status is
        /// not recorded), and the first reported status becomes the WAL's status, as reported (without the file name).
        /// </summary>
        public override void Corruption(long bytes, StatusCode code, string reason, ulong logNumber)
        {
            string status = (code == StatusCode.IOError ? "IO error: " : "Corruption: ") + reason;
            Log?.Log(InfoLogLevel.Warn, (RecordErrors ? "" : "(ignoring error) ") + FileName + ": dropping " +
                bytes.ToString(CultureInfo.InvariantCulture) + " bytes; " + status);
            if (!RecordErrors || Error is not null) return;
            Error = new RocksDbException(code == StatusCode.IOError ? StatusCode.IOError : StatusCode.Corruption, status);
            CorruptedLogNumber = logNumber;
        }
    }

    /// <summary>
    /// Port of DBImpl::RecoverLogFiles: replays every WAL with a number &gt;= the minimum log number with unflushed data
    /// (<see cref="ProcessLogFilesAsync"/>). Recovered memtables are flushed to L0 (WriteLevel0TableForRecovery) when they
    /// fill up and, unless avoid_flush_during_recovery, at the end. Returns the per-CF edits to apply.
    /// </summary>
    private async ValueTask<Dictionary<uint, VersionEdit>> RecoverLogFilesAsync(bool isNewDb, bool synchronous, CancellationToken ct)
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
        int walFiles = 0;
        foreach (string child in FileSystem.GetChildren(WalDir))
        {
            if (FileNames.TryParseFileName(child, out ulong number, out FileType type) && type == FileType.WalFile)
            {
                walFiles++;
                Versions.MarkFileNumberUsed(number);
                if (number >= minLog) wals.Add(number);
            }
        }
        wals.Sort();
        CheckWalFilesExist(isNewDb, walFiles);
        // Best-efforts recovery does not replay WALs (DBImpl::Recover does not list the WAL directory); their numbers are
        // still marked used, and they become obsolete once the recovery edits advance every column family.
        if (wals.Count == 0 || Options.BestEffortsRecovery) return edits;

        ReplayProgress progress = await ProcessLogFilesAsync(wals, minLog, readOnly: false, edits, synchronous, ct).ConfigureAwait(false);
        // DBImpl::Recover: a point-in-time recovery that stopped at a corrupted WAL hands next_sequence to Open
        // (recovered_seq), which writes it to the new WAL as a dummy record.
        if (progress.CorruptedWalFound) _recoveredSeq = progress.NextSequence;
        ulong lastSequence = progress.LastSequence;
        bool anyData = progress.AnyData;

        Versions.SetLastSequence(lastSequence);
        // RecoverLogFiles: last_allocated_sequence_ and last_published_sequence_ follow the recovered last sequence.
        Versions.SetLastPublishedSequence(lastSequence);
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
            foreach (ulong n in wals)
            {
                _aliveWals.Add(n);
                // alive_wal_files_ entries of the recovered WALs carry their file sizes (wals_total_size_).
                long size;
                try { size = FileSystem.GetFileSize(FileNames.LogFileName(WalDir, n)); }
                catch (Exception e) when (e is IOException or RocksDbException) { size = 0; }
                _aliveWalSizes[n] = size;
                Interlocked.Add(ref _walsTotalSize, size);
            }
        }
        MarkRecoveredPrepSections();
        return edits;
    }

    /// <summary>
    /// DBImpl::Recover with track_and_verify_wals: opening an existing DB whose WAL directory holds no WAL file at all is
    /// corruption (every open leaves at least the WAL it created). Not checked with best-efforts recovery.
    /// </summary>
    private void CheckWalFilesExist(bool isNewDb, int walFiles)
    {
        if (Options.TrackAndVerifyWals && !isNewDb && !Options.BestEffortsRecovery && walFiles == 0)
        {
            ThrowHelper.Corruption("Opening an existing DB with no WAL files");
        }
    }

    /// <summary>
    /// Port of DBImpl::ProcessLogFiles / ProcessLogFile (shared by primary and read-only opens, like upstream): replays
    /// <paramref name="wals"/> (all &gt;= <paramref name="minLog"/>, ascending) through the WAL filter into the memtables,
    /// verifies the predecessor-WAL-info records (track_and_verify_wals), handles a non-OK WAL status according to
    /// wal_recovery_mode (HandleNonOkStatusOrOldLogRecord) and, after a point-in-time stop, checks that no column family
    /// flushed data beyond the point of corruption (MaybeHandleStopReplayForCorruptionForInconsistency). Read-only opens
    /// never flush (their memtables just grow).
    /// </summary>
    private async ValueTask<ReplayProgress> ProcessLogFilesAsync(List<ulong> wals, ulong minLog, bool readOnly, Dictionary<uint, VersionEdit>? edits,
        bool synchronous, CancellationToken ct)
    {
        // No-op for wal_filter == null.
        InvokeWalFilterOnColumnFamilyToWalNumberMap();
        var progress = new ReplayProgress { LastSequence = Versions.LastSequence };
        WalRecoveryMode mode = Options.WalRecoveryMode;
        ulong corruptedWal = DbFormat.MaxSequenceNumber;
        foreach (ulong walNumber in wals)
        {
            string fname = FileNames.LogFileName(WalDir, walNumber);
            if (progress.StopReplayByWalFilter)
            {
                LogWalFileDropped(fname);
                continue;
            }
            var reporter = new WalRecoveryReporter
            {
                FileName = fname,
                RecordErrors = Options.ParanoidChecks && mode != WalRecoveryMode.SkipAnyCorruptedRecords,
                Log = _infoLog,
            };
            progress.LastSeqnoObserved = 0;
            // InitializeLogReader: checksums are always verified, even without paranoid_checks.
            using (var reader = new LogReader(FileSystem.OpenSequential(fname), reporter, checksum: true, walNumber)
            {
                RecoveryMode = mode,
                TrackAndVerifyWals = Options.TrackAndVerifyWals,
                StopReplayForCorruption = progress.StopReplay,
                MinWalNumberToKeep = minLog,
                ObservedPredecessorWalInfo = progress.Predecessor,
                FileName = fname,
            })
            {
                while (!progress.StopReplayByWalFilter)
                {
                    bool processed = ReplayNextRecord(reader, reporter, walNumber, fname, progress);
                    // Memtables that filled up during recovery are written out right away (MaybeWriteLevel0TableForRecovery).
                    if (_flushScheduler.Count > 0)
                    {
                        if (!readOnly)
                        {
                            foreach (ColumnFamilyData cfd in _flushScheduler)
                            {
                                await FlushRecoveredMemTableAsync(cfd, edits!, progress.LastSequence, synchronous, ct).ConfigureAwait(false);
                            }
                        }
                        _flushScheduler.Clear();
                    }
                    if (!processed || progress.StopReplay) break;
                }
            }
            if (reporter.Error is null)
            {
                UpdatePredecessorWalInfo(walNumber, fname, progress);
                if (!reporter.OldLogRecordSeen) continue;
            }
            // HandleNonOkStatusOrOldLogRecord: a non-OK status, or (status OK) the end of the new data of a recycled WAL
            // found by a record from its previous use.
            RocksDbException? err = reporter.Error;
            // NotSupported is not a corruption (e.g. a WAL filter returning more records than the original batch).
            if (err is { Code: StatusCode.NotSupported }) throw err;
            switch (mode)
            {
                case WalRecoveryMode.SkipAnyCorruptedRecords:
                    break; // ignore all errors unconditionally
                case WalRecoveryMode.PointInTimeRecovery:
                    if (err is { Code: StatusCode.IOError })
                    {
                        LogError("IOError during point-in-time reading log #" + walNumber + ". " + err.Message +
                            ". This likely mean loss of synced WAL, thus recovery fails.");
                        throw err;
                    }
                    // We should ignore the error but not continue replaying. Later WALs are still read: replay resumes
                    // if one continues the sequence (MaybeReviseStopReplayForCorruption), e.g. writes made after an
                    // earlier point-in-time recovery.
                    progress.StopReplay = true;
                    progress.CorruptedWalFound = true;
                    corruptedWal = reporter.CorruptedLogNumber != LogReporter.UnspecifiedLogNumber ? reporter.CorruptedLogNumber : walNumber;
                    break;
                default:
                    // The reader reports old records in point-in-time mode only, so err is set here.
                    if (err is not null) throw err;
                    break;
            }
        }
        if (progress.StopReplay && mode is WalRecoveryMode.PointInTimeRecovery or WalRecoveryMode.TolerateCorruptedTailRecords)
        {
            foreach (ColumnFamilyData cfd in GetColumnFamilies())
            {
                ulong liveBytes = 0;
                VersionStorageInfo vs = cfd.Current.Storage;
                for (int l = 0; l < vs.NumLevels; l++) liveBytes += vs.NumLevelBytes(l);
                if (cfd.LogNumber > corruptedWal && liveBytes > 0)
                {
                    LogError("Column family inconsistency: SST file contains data beyond the point of corruption.");
                    ThrowHelper.Corruption("Column family inconsistency: SST file contains data beyond the point of corruption in CF " + cfd.Name +
                        ". WAL recovery stopped at corruption point, but SST files contain newer data.");
                }
            }
        }
        return progress;
    }

    private void LogWalFileDropped(string fname)
    {
        long bytes;
        try { bytes = FileSystem.GetFileSize(fname); }
        catch (Exception e) when (e is IOException or RocksDbException) { return; }
        LogWarn(fname + ": dropping " + bytes.ToString(CultureInfo.InvariantCulture) + " bytes");
    }

    /// <summary>
    /// Port of DBImpl::UpdatePredecessorWALInfo: a WAL replayed without error becomes the observed predecessor of the
    /// next one (its number, its size on disk and the sequence of its last record). DEBUG builds pass
    /// [fixed64 wal number, fixed64 mock seqno = kMaxSequenceNumber] to the "DBImpl::UpdatePredecessorWALInfo" span hook;
    /// a test may overwrite the seqno.
    /// </summary>
    private void UpdatePredecessorWalInfo(ulong walNumber, string fname, ReplayProgress progress)
    {
        ulong bytes = (ulong)FileSystem.GetFileSize(fname);
        ulong seqno = progress.LastSeqnoObserved;
#if DEBUG
        Span<byte> pair = stackalloc byte[16];
        Util.Coding.EncodeFixed64(pair, walNumber);
        Util.Coding.EncodeFixed64(pair[8..], DbFormat.MaxSequenceNumber);
        TestHooks.InvokeSpan("DBImpl::UpdatePredecessorWALInfo", pair);
        ulong mock = Util.Coding.DecodeFixed64(pair[8..]);
        if (mock != DbFormat.MaxSequenceNumber) seqno = mock;
#endif
        progress.Predecessor = new PredecessorWalInfo(walNumber, bytes, seqno);
    }

    private sealed class ReplayProgress
    {
        public ulong LastSequence;
        public bool AnyData;
        /// <summary>Sequence the next record must start with to resume after a corruption (next_sequence), or MaxValue.</summary>
        public ulong NextSequence = ulong.MaxValue;
        /// <summary>Point-in-time recovery hit a corruption: WALs are dropped until one continues the sequence.</summary>
        public bool StopReplay;
        /// <summary>Point-in-time recovery ignored a WAL error or an old record (corrupted_wal_found).</summary>
        public bool CorruptedWalFound;
        /// <summary>The WAL filter stopped the replay: the remaining records and WALs are dropped (stop_replay_by_wal_filter).</summary>
        public bool StopReplayByWalFilter;
        /// <summary>Sequence of the last record read from the current WAL (last_seqno_observed), 0 if none.</summary>
        public ulong LastSeqnoObserved;
        /// <summary>The last WAL replayed without error (predecessor_wal_info), null before the first one.</summary>
        public PredecessorWalInfo? Predecessor;
        /// <summary>Record and replacement batches handed to the WAL filter (reused for every record).</summary>
        public WriteBatch? FilterBatch;
        public WriteBatch? FilterNewBatch;
    }

    /// <summary>
    /// Reads one WAL record and processes it (DBImpl::ProcessLogRecord): WAL filter, then insert into the memtables.
    /// Returns false at the end of the WAL or once the WAL's status is not OK (the caller then stops reading the WAL).
    /// </summary>
    private bool ReplayNextRecord(LogReader reader, WalRecoveryReporter reporter, ulong walNumber, string fname, ReplayProgress progress)
    {
        // Reading changes reporter.Error when a corruption is encountered.
        if (!reader.TryReadRecord(out ReadOnlySpan<byte> record) || reporter.Error is not null) return false;
        if (record.Length < WriteBatch.HeaderSize)
        {
            reporter.Corruption(record.Length, StatusCode.Corruption, "log record too small", LogReporter.UnspecifiedLogNumber);
            return true;
        }
        IReadOnlyDictionary<uint, int> recordedTs = reader.RecordedTimestampSizes;
        byte[]? reconciled = null;
        if (_cfTimestampSizes is not null || recordedTs.Count > 0) reconciled = HandleRecoveredBatchTimestampSizes(record, recordedTs);
        if (reconciled is not null) record = reconciled;
        ulong seq = Util.Coding.DecodeFixed64(record);
        progress.LastSeqnoObserved = seq;
        if (progress.StopReplay)
        {
            // MaybeReviseStopReplayForCorruption: in point-in-time recovery mode, if sequence ids are consecutive we
            // continue recovery despite the corruption. Otherwise the rest of this WAL is dropped.
            if (Options.WalRecoveryMode == WalRecoveryMode.PointInTimeRecovery && seq == progress.NextSequence)
            {
                progress.StopReplay = false;
            }
            else
            {
                LogWalFileDropped(fname);
                return true;
            }
        }
        // For the default case of wal_filter == null, always performs no-op.
        if (Options.WalFilter is { } filter && !InvokeWalFilterOnWalRecord(filter, walNumber, fname, reporter, progress, ref record))
        {
            return true;
        }
        uint count = Util.Coding.DecodeFixed32(record[8..]);
        var inserter = new MemTableInserter(this, walNumber, ignoreMissingColumnFamilies: true) { Sequence = seq, FailOnDuplicateKeySeq = true };
        try
        {
            inserter.Run(record);
        }
        catch (DuplicateKeySeqException e)
        {
            // InsertLogRecordToMemtable failed: MaybeIgnoreError, else the failure becomes the WAL's status (reported
            // like a read error). next_sequence is the inserter's sequence at the failing entry.
            if (Options.ParanoidChecks) reporter.Corruption(record.Length, StatusCode.Corruption, e.Message, LogReporter.UnspecifiedLogNumber);
            else LogWarn("Ignoring error Corruption: " + e.Message);
            CheckSeqnoNotSetBackDuringRecovery(progress.NextSequence, inserter.Sequence);
            progress.NextSequence = inserter.Sequence;
            return true;
        }
        // next_sequence comes from the inserter, not the record header: a prepared section counts its entries but
        // consumes no sequence numbers, and a commit marker consumes those of the prepared batch it applies.
        ulong next = inserter.Sequence;
        CheckSeqnoNotSetBackDuringRecovery(progress.NextSequence, next);
        progress.NextSequence = next;
        if (count > 0 || next > seq) progress.AnyData = true;
        if (next > seq)
        {
            ulong last = next - 1;
            if (last > progress.LastSequence) progress.LastSequence = last;
        }
        return true;
    }

    /// <summary>
    /// Port of DBImpl::CheckSeqnoNotSetBackDuringRecovery: the next sequence never goes back while WAL records are
    /// replayed (e.g. a stale copy of an older WAL under a newer number); a serious bug or corruption, the open fails in
    /// every recovery mode. <paramref name="prevNextSeqno"/> is ulong.MaxValue (kMaxSequenceNumber) before the first record.
    /// </summary>
    private static void CheckSeqnoNotSetBackDuringRecovery(ulong prevNextSeqno, ulong currentNextSeqno)
    {
        if (prevNextSeqno == ulong.MaxValue || prevNextSeqno <= currentNextSeqno) return;
        ThrowHelper.Corruption("Sequence number is being set backwards during recovery, this is likely a software bug or a data corruption. Prev next seqno: " +
            prevNextSeqno.ToString(CultureInfo.InvariantCulture) + " , current next seqno: " + currentNextSeqno.ToString(CultureInfo.InvariantCulture));
    }

    /// <summary>Port of DBImpl::InvokeWalFilterIfNeededOnColumnFamilyToWalNumberMap.</summary>
    private void InvokeWalFilterOnColumnFamilyToWalNumberMap()
    {
        if (Options.WalFilter is not { } filter) return;
        var logNumbers = new SortedDictionary<uint, ulong>();
        var nameIds = new SortedDictionary<string, uint>(StringComparer.Ordinal);
        foreach (ColumnFamilyData cfd in GetColumnFamilies())
        {
            nameIds.TryAdd(cfd.Name, cfd.Id);
            logNumbers.TryAdd(cfd.Id, cfd.LogNumber);
        }
        filter.ColumnFamilyLogNumberMap(logNumbers, nameIds);
    }

    /// <summary>
    /// Port of DBImpl::InvokeWalFilterIfNeededOnWalRecord: returns false when the record must not be replayed (ignored,
    /// replay stopped, reported corrupted, or an invalid replacement), with the WAL's status set where upstream sets it.
    /// A replacement batch (batch_changed) gets the original record's sequence and replaces <paramref name="record"/>.
    /// </summary>
    private bool InvokeWalFilterOnWalRecord(WalFilter filter, ulong walNumber, string fname, WalRecoveryReporter reporter, ReplayProgress progress,
        ref ReadOnlySpan<byte> record)
    {
        WriteBatch batch = progress.FilterBatch ??= new WriteBatch();
        WriteBatch newBatch = progress.FilterNewBatch ??= new WriteBatch();
        batch.SetContents(record);
        newBatch.Clear();
        WalProcessingOption option = filter.LogRecordFound(walNumber, fname, batch, newBatch, out bool batchChanged);
        switch (option)
        {
            case WalProcessingOption.ContinueProcessing:
                break; // do nothing, proceed normally
            case WalProcessingOption.IgnoreCurrentRecord:
                return false; // skip current record
            case WalProcessingOption.StopReplay:
                // skip current record and stop replay
                progress.StopReplayByWalFilter = true;
                return false;
            case WalProcessingOption.CorruptedRecord:
                // MaybeIgnoreError: without paranoid_checks the report is ignored and the record is processed.
                if (Options.ParanoidChecks)
                {
                    reporter.Error ??= RocksDbException.Corruption("Corruption reported by Wal Filter : " + filter.Name);
                    return false;
                }
                LogWarn("Ignoring error Corruption: Corruption reported by Wal Filter " + filter.Name);
                break;
            default:
                // Not a WalProcessingOption: with paranoid_checks the replay stops and the open fails; otherwise the error
                // is ignored and the record processed.
                if (Options.ParanoidChecks)
                {
                    reporter.Error ??= RocksDbException.NotSupported("Unknown WalProcessingOption returned by Wal Filter : " + filter.Name);
                    progress.StopReplayByWalFilter = true;
                    return false;
                }
                break;
        }
        if (batchChanged)
        {
            // Make sure that the count in the new batch is within the original count.
            int newCount = newBatch.Count;
            int originalCount = batch.Count;
            if (newCount > originalCount)
            {
                _infoLog?.Log(InfoLogLevel.Fatal, "Recovering log #" + walNumber + " mode " + (int)Options.WalRecoveryMode + " log filter " + filter.Name +
                    " returned more records (" + newCount + ") than original (" + originalCount + ") which is not allowed. Aborting recovery.");
                reporter.Error ??= RocksDbException.NotSupported("More than original # of records returned by Wal Filter : " + filter.Name);
                return false;
            }
            // Set the same sequence number in the new_batch as the original batch.
            newBatch.Sequence = batch.Sequence;
            record = newBatch.Data;
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
            SeqnoToTimeMapping? seqnoToTime;
            lock (_mutex) seqnoToTime = CopySeqnoToTimeMapping(cfd);
            meta = await FlushJob.WriteLevel0TableAsync(FileSystem, path, TableFileWriteOptions(Env.IoPriority.High), cfd, [mem], fileNumber, cfd.NewEpochNumber(), Identity, [],
                    CurrentSnapshotChecker, blobBuilder, null, seqnoToTime, synchronous, ct)
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
