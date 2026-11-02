using System.Globalization;
using RocksDb.Managed.Cache;
using RocksDb.Managed.Env;
using RocksDb.Managed.Wal;

namespace RocksDb.Managed.Db;

/// <summary>
/// The DB implementation behind <see cref="RocksDatabase"/>. Port of rocksdb::DBImpl (db/db_impl/db_impl.cc,
/// db_impl_open.cc, db_impl_write.cc, db_impl_compaction_flush.cc, db_impl_files.cc), split into partial files:
/// <list type="bullet">
/// <item><c>DbImpl.cs</c>: state, snapshots, properties, obsolete file deletion, shutdown.</item>
/// <item><c>DbImpl.Open.cs</c>: create / recover (MANIFEST + WAL replay).</item>
/// <item><c>DbImpl.Write.cs</c>: group commit write path, memtable switch, write stalls.</item>
/// <item><c>DbImpl.Read.cs</c>: point lookups.</item>
/// <item><c>DbImpl.Flush.cs</c>: flush scheduling and background flush jobs.</item>
/// <item><c>DbImpl.ColumnFamilies.cs</c>: create / drop column families, OPTIONS file.</item>
/// </list>
/// <para>
/// <b>Synchronization</b> (mirrors DBImpl::mutex_): <see cref="Mutex"/> guards column family state (memtable lists,
/// super versions, flush queue, snapshots, alive WALs, background error). The WAL writer and the mutable memtables
/// are only written by the current write-group leader (<see cref="WriteThread"/>); operations that replace them
/// (memtable switch, column family creation) run as an "unbatched" leader. No I/O is done while holding the mutex.
/// </para>
/// </summary>
internal sealed partial class DbImpl : IAsyncDisposable
{
    private const string DefaultColumnFamilyName = ColumnFamilyDescriptor.DefaultColumnFamilyName;

    private readonly Lock _mutex = new();

    /// <summary>The DB mutex, for tests that inspect state guarded by it.</summary>
    internal Lock MutexForTests => _mutex;

    private readonly SnapshotList _snapshots = new();
    private readonly List<ulong> _aliveWals = new();          // WAL numbers that may still hold unflushed data (guarded by mutex)
    private readonly List<ulong> _obsoleteManifests = new();  // guarded by mutex
    private readonly HashSet<ulong> _pendingOutputs = new();  // table files being written (guarded by mutex)
    private readonly List<ColumnFamilyData> _droppedColumnFamilies = new(); // table caches closed at shutdown (guarded by mutex)
    private IDisposable? _dbLock;
    private int _disposed;

    private DbImpl(string dbname, DbOptions options)
    {
        DbName = dbname;
        Options = options;
        FileSystem = options.FileSystem;
        WalDir = string.IsNullOrEmpty(options.WalDir) ? dbname : options.WalDir.TrimEnd('/');
        var paths = new List<string> { dbname };
        foreach ((string p, _) in options.DbPaths)
        {
            if (paths.Count == 1 && p.TrimEnd('/') == dbname) continue;
            paths.Add(p.TrimEnd('/'));
        }
        DbPaths = paths;
        Versions = new VersionSet(this, FileSystem, dbname);
        ErrorHandler = new ErrorHandler(this);
        _writeThread = new WriteThread(options.MaxWriteBatchGroupSizeBytes);
        // GetBGJobLimits (db/db_impl/db_impl_compaction_flush.cc).
        int maxFlushes = options.MaxBackgroundFlushes;
        if (maxFlushes == -1 && options.MaxBackgroundCompactions == -1) maxFlushes = Math.Max(1, options.MaxBackgroundJobs / 4);
        _maxBackgroundFlushes = Math.Max(1, maxFlushes);
        string host = options.DbHostId == "__hostname__" ? Environment.MachineName : options.DbHostId;
        Identity = new DbIdentity("", GenerateDbSessionId(), host);
        _seqPerBatch = options.SeqPerBatch;
        _batchPerTxn = options.BatchPerTxn;
        _twoWriteQueues = options.TwoWriteQueues;
        // last_seq_same_as_publish_seq_ (db_impl.cc): only the WAL-only writes of a seq_per_batch DB consume sequence
        // numbers on the second queue, so only then is the published sequence maintained separately.
        _lastSeqSameAsPublishSeq = !(_seqPerBatch && _twoWriteQueues);
        if (_twoWriteQueues)
        {
            _nonmemWriteThread = new WriteThread(options.MaxWriteBatchGroupSizeBytes);
            _walWriteLock = new SemaphoreSlim(1, 1);
        }
    }

    // ---- WritePrepared / WriteUnprepared support (seq_per_batch_, batch_per_txn_, use_custom_gc_) ---------

    /// <summary>seq_per_batch_: one sequence number per sub-batch (WritePrepared / WriteUnprepared).</summary>
    private readonly bool _seqPerBatch;
    /// <summary>batch_per_txn_: false for WriteUnprepared (write_before_prepare).</summary>
    private readonly bool _batchPerTxn;
    /// <summary>Hooks of the sequence-per-batch transaction DB (null otherwise). Set once after open.</summary>
    private SeqPerBatchHooks? _txnHooks;

    internal bool SeqPerBatch => _seqPerBatch;
    internal bool BatchPerTxn => _batchPerTxn;
    internal SeqPerBatchHooks? TxnHooks => _txnHooks;

    /// <summary>
    /// DBImpl::SetSnapshotChecker + the WritePreparedTxnDB read / write overrides. Called once by the transaction DB
    /// after open (before it is handed to the application).
    /// </summary>
    internal void SetSeqPerBatchHooks(SeqPerBatchHooks hooks)
    {
        lock (_mutex)
        {
            if (_txnHooks is not null) throw new InvalidOperationException("snapshot checker already set");
            _txnHooks = hooks;
        }
    }

    /// <summary>
    /// The snapshot checker for flush / compaction (InitSnapshotContext): the transaction DB's, or
    /// <see cref="DisableGcSnapshotChecker"/> for a sequence-per-batch DB before it is installed (use_custom_gc_).
    /// </summary>
    internal SnapshotChecker? CurrentSnapshotChecker => _txnHooks?.SnapshotChecker ?? (_seqPerBatch ? DisableGcSnapshotChecker.Instance : null);

    public string DbName { get; }
    public DbOptions Options { get; }
    public IFileSystem FileSystem { get; }
    public string WalDir { get; }
    public IReadOnlyList<string> DbPaths { get; }
    public VersionSet Versions { get; }
    private Cache.BlockCache? _defaultBlockCache;
    private readonly Lock _defaultBlockCacheLock = new();

    /// <summary>Default block cache capacity when <c>BlockBasedTableOptions.BlockCache</c> is not set (upstream: 32 MiB).</summary>
    internal const long DefaultBlockCacheCapacity = 32L << 20;

    /// <summary>
    /// The DB-wide default block cache (HyperClock, 32 MiB) used by column families that don't configure their own;
    /// created lazily and shared so that all column families compete for one budget.
    /// </summary>
    internal Cache.BlockCache? GetDefaultBlockCache(BlockBasedTableOptions tableOptions)
    {
        if (tableOptions.NoBlockCache || tableOptions.BlockCache is not null) return null;
        lock (_defaultBlockCacheLock)
        {
            return _defaultBlockCache ??= Cache.BlockCache.CreateDefault(DefaultBlockCacheCapacity, (int)Math.Min(tableOptions.BlockSize, int.MaxValue));
        }
    }

    public DbIdentity Identity { get; private set; }
    public Lock Mutex => _mutex;

    internal bool IsDisposed => Volatile.Read(ref _disposed) != 0;

    internal void ThrowIfDisposed()
    {
        if (Volatile.Read(ref _disposed) != 0) ThrowDisposed();
    }

    private static void ThrowDisposed() => throw new ObjectDisposedException(nameof(RocksDatabase));

    /// <summary>DBImpl::GenerateDbSessionId: 20 base-36 characters from 128 random bits (never a zero lower half).</summary>
    private static string GenerateDbSessionId()
    {
        Span<byte> b = stackalloc byte[16];
        System.Security.Cryptography.RandomNumberGenerator.Fill(b);
        ulong upper = BitConverter.ToUInt64(b);
        ulong lower = BitConverter.ToUInt64(b[8..]) | 1;
        return OffsetableCacheKey.EncodeSessionId(upper, lower);
    }

    // ---- column family lookup ---------------------------------------------------------------------------

    public ColumnFamilyData GetColumnFamilyData(string name)
    {
        lock (_mutex)
        {
            return Versions.GetColumnFamily(name) ?? throw RocksDbException.InvalidArgument("Column family not found: " + name);
        }
    }

    public ColumnFamilyData? TryGetColumnFamilyData(string name)
    {
        lock (_mutex) return Versions.GetColumnFamily(name);
    }

    public List<ColumnFamilyData> GetColumnFamilies()
    {
        lock (_mutex)
        {
            var list = new List<ColumnFamilyData>(Versions.ColumnFamilies);
            list.Sort(static (a, b) => a.Id.CompareTo(b.Id));
            return list;
        }
    }

    // ---- super versions ---------------------------------------------------------------------------------

    /// <summary>Port of ColumnFamilyData::GetReferencedSuperVersion (without thread-local caching).</summary>
    internal static SuperVersion GetReferencedSuperVersion(ColumnFamilyData cfd)
    {
        while (true)
        {
            SuperVersion sv = cfd.Super;
            if (sv.TryRef()) return sv;
            if (cfd.Dropped) ThrowDropped(cfd);
            Thread.SpinWait(1);
        }
    }

    private static void ThrowDropped(ColumnFamilyData cfd) =>
        throw new RocksDbException(StatusCode.ColumnFamilyDropped, "Column family drop: column family " + cfd.Name + " was dropped");

    /// <summary>Releases a reader reference (ReturnAndCleanupSuperVersion).</summary>
    internal void ReturnSuperVersion(SuperVersion sv)
    {
        if (sv.Unref())
        {
            lock (_mutex) CleanupSuperVersion(sv);
        }
    }

    /// <summary>Releases what a dead super version referenced (SuperVersion::Cleanup). REQUIRES: mutex.</summary>
    internal void CleanupSuperVersion(SuperVersion sv)
    {
        if (sv.Mem.Unref()) sv.Mem.Dispose();
        foreach (MemTable m in sv.Imm)
        {
            if (m.Unref()) m.Dispose();
        }
        Versions.UnrefVersion(sv.Current);
    }

    private ulong _superVersionNumber;

    /// <summary>Installs a new super version for the CF's current memtables and version (InstallSuperVersion). REQUIRES: mutex.</summary>
    internal void InstallSuperVersion(ColumnFamilyData cfd)
    {
        var imm = new MemTable[cfd.Imm.Count];
        for (int i = 0; i < imm.Length; i++)
        {
            MemTable m = cfd.Imm[cfd.Imm.Count - 1 - i].Mem; // newest first
            m.Ref();
            imm[i] = m;
        }
        cfd.Mem.Ref();
        cfd.Current.Ref();
        var sv = new SuperVersion(cfd.Mem, imm, cfd.Current, ++_superVersionNumber);
        SuperVersion? old = cfd.SuperOrNull();
        cfd.SetSuper(sv);
        if (old is not null && old.Unref()) CleanupSuperVersion(old);
        OnSuperVersionInstalled(cfd);
    }

    // ---- snapshots --------------------------------------------------------------------------------------

    public Snapshot GetSnapshot()
    {
        if (_txnHooks is { } hooks) return hooks.GetSnapshot();
        return GetSnapshotImpl();
    }

    /// <summary>DBImpl::GetSnapshotImpl: a snapshot at the last sequence (without transaction-DB enhancements).</summary>
    internal Snapshot GetSnapshotImpl()
    {
        ThrowIfDisposed();
        lock (_mutex)
        {
            var s = new Snapshot(this, LastPublishedSequence, DateTimeOffset.UtcNow.ToUnixTimeSeconds());
            _snapshots.Add(s);
            return s;
        }
    }

    internal void ReleaseSnapshot(Snapshot s)
    {
        lock (_mutex) _snapshots.Remove(s);
    }

    /// <summary>The live snapshots with a sequence number &lt;= <paramref name="max"/>, ascending (LoadSnapshots).</summary>
    internal void GetSnapshotSequences(List<ulong> into, ulong max)
    {
        lock (_mutex) _snapshots.GetAll(into, max);
    }

    /// <summary>Sequence numbers of live snapshots, oldest first (for compaction / diagnostics).</summary>
    public List<ulong> GetSnapshotSequences()
    {
        var list = new List<ulong>();
        lock (_mutex) _snapshots.GetAll(list, ulong.MaxValue);
        return list;
    }

    public int SnapshotCount
    {
        get
        {
            lock (_mutex) return _snapshots.Count;
        }
    }

    // ---- properties -------------------------------------------------------------------------------------

    /// <summary>Port of the supported subset of DB::GetProperty (db/internal_stats.cc).</summary>
    public string? GetProperty(ColumnFamilyData cfd, string property)
    {
        SuperVersion sv = GetReferencedSuperVersion(cfd);
        try
        {
            if (TryGetExtendedProperty(cfd, sv, property, out string? extendedValue)) return extendedValue;
            if (TryGetCompactionProperty(cfd, sv, property, out string? compactionValue)) return compactionValue;
            switch (property)
            {
                case DbProperties.NumEntriesActiveMemTable:
                    return sv.Mem.NumEntries.ToString(CultureInfo.InvariantCulture);
                case DbProperties.NumEntriesImmMemTables:
                    {
                        long n = 0;
                        foreach (MemTable m in sv.Imm) n += m.NumEntries;
                        return n.ToString(CultureInfo.InvariantCulture);
                    }
                case DbProperties.NumDeletesActiveMemTable:
                    return sv.Mem.NumDeletes.ToString(CultureInfo.InvariantCulture);
                case DbProperties.NumImmutableMemTable:
                    return sv.Imm.Length.ToString(CultureInfo.InvariantCulture);
                case DbProperties.CurSizeActiveMemTable:
                    return sv.Mem.ApproximateMemoryUsage.ToString(CultureInfo.InvariantCulture);
                case DbProperties.EstimateNumKeys:
                    return EstimateNumKeys(cfd, sv).ToString(CultureInfo.InvariantCulture);
                case DbProperties.TotalSstFilesSize:
                case DbProperties.LiveSstFilesSize:
                    {
                        ulong n = 0;
                        for (int l = 0; l < sv.Current.Storage.NumLevels; l++) n += sv.Current.Storage.NumLevelBytes(l);
                        return n.ToString(CultureInfo.InvariantCulture);
                    }
                case DbProperties.LevelStats:
                    return sv.Current.Storage.LevelSummary();
                case DbProperties.NumSnapshots:
                    return SnapshotCount.ToString(CultureInfo.InvariantCulture);
                case DbProperties.IsWriteStopped:
                    lock (_mutex) return IsWriteStalled() ? "1" : "0";
            }
            if (property.StartsWith(DbProperties.NumFilesAtLevelPrefix, StringComparison.Ordinal)
                && int.TryParse(property.AsSpan(DbProperties.NumFilesAtLevelPrefix.Length), NumberStyles.None, CultureInfo.InvariantCulture, out int level))
            {
                return level < sv.Current.Storage.NumLevels ? sv.Current.Storage.NumLevelFiles(level).ToString(CultureInfo.InvariantCulture) : "0";
            }
            return null;
        }
        finally
        {
            ReturnSuperVersion(sv);
        }
    }

    /// <summary>Port of "rocksdb.estimate-num-keys": memtable entries minus 2x deletes plus table-property estimates.</summary>
    private static ulong EstimateNumKeys(ColumnFamilyData cfd, SuperVersion sv)
    {
        long mem = sv.Mem.NumEntries - 2 * sv.Mem.NumDeletes;
        foreach (MemTable m in sv.Imm) mem += m.NumEntries - 2 * m.NumDeletes;
        long files = 0;
        VersionStorageInfo s = sv.Current.Storage;
        for (int l = 0; l < s.NumLevels; l++)
        {
            foreach (FileMetaData f in s.LevelFiles(l))
            {
                using TableReaderLease lease = cfd.TableCache.GetReader(f);
                Table.TableProperties p = lease.Reader.Properties;
                files += (long)p.NumEntries - 2 * (long)p.NumDeletions;
            }
        }
        long total = Math.Max(0, mem) + Math.Max(0, files);
        return (ulong)total;
    }

    /// <summary>Port of DB::GetLiveFilesMetaData.</summary>
    public List<LiveFileMetadata> GetLiveFilesMetadata()
    {
        var result = new List<LiveFileMetadata>();
        foreach (ColumnFamilyData cfd in GetColumnFamilies())
        {
            SuperVersion sv = GetReferencedSuperVersion(cfd);
            try
            {
                VersionStorageInfo s = sv.Current.Storage;
                for (int l = 0; l < s.NumLevels; l++)
                {
                    foreach (FileMetaData f in s.LevelFiles(l))
                    {
                        string dir = DbPaths[(int)Math.Min(f.PathId, (uint)(DbPaths.Count - 1))];
                        result.Add(new LiveFileMetadata(
                            "/" + FileNames.MakeTableFileName(f.FileNumber),
                            dir,
                            f.FileNumber,
                            cfd.Name,
                            l,
                            f.FileSize,
                            DbFormat.ExtractUserKey(f.Smallest).ToArray(),
                            DbFormat.ExtractUserKey(f.Largest).ToArray(),
                            f.SmallestSeqno,
                            f.LargestSeqno,
                            f.EpochNumber)
                        {
                            Temperature = f.Temperature,
                            OldestAncestorTime = f.OldestAncestorTime,
                            FileCreationTime = f.FileCreationTime,
                            NumReadsSampled = f.NumReadsSampled,
                        });
                    }
                }
            }
            finally
            {
                ReturnSuperVersion(sv);
            }
        }
        return result;
    }

    // ---- background error ----------------------------------------------------------------------------------

    private ulong _failedWalNumber; // newest WAL whose write/sync failed (guarded by mutex)

    /// <summary>The background error handler (error_handler_).</summary>
    internal ErrorHandler ErrorHandler { get; }

    /// <summary>
    /// Records a background error for <paramref name="reason"/> (ErrorHandler::SetBGError): depending on its severity
    /// writes stop, background work pauses and automatic recovery starts. Called without the mutex.
    /// </summary>
    internal void SetBackgroundError(Exception e, BackgroundErrorReason reason, bool walRelated)
    {
        if (walRelated)
        {
            lock (_mutex) _failedWalNumber = Math.Max(_failedWalNumber, _logNumber);
        }
        ErrorHandler.SetBgError(e, reason, walRelated);
        lock (_mutex) SignalBackgroundWork();
    }

    /// <summary>The current background error (with its severity), or null.</summary>
    internal RocksDbException? BackgroundError
    {
        get
        {
            lock (_mutex) return ErrorHandler.BgError;
        }
    }

    /// <summary>Writes are stopped by a background error (hard or worse).</summary>
    internal bool IsDbStopped => ErrorHandler.IsDbStopped;

    /// <summary>Background flushes and compactions are stopped by a background error. REQUIRES: mutex.</summary>
    private bool IsBgWorkStopped => ErrorHandler.IsBgWorkStopped;

    private static void ThrowBackgroundError(RocksDbException e) => throw e.Rethrowable();

    /// <summary>Throws the background error if writes are stopped (PreprocessWrite's IsDBStopped check). Lock-free.</summary>
    private void ThrowIfDbStopped()
    {
        if (ErrorHandler.IsDbStopped)
        {
            RocksDbException? bg = BackgroundError;
            if (bg is not null) ThrowBackgroundError(bg);
        }
    }

    // ---- obsolete files ---------------------------------------------------------------------------------

    internal void ScheduleDeleteObsoleteManifest(ulong number)
    {
        lock (_mutex) _obsoleteManifests.Add(number);
    }

    /// <summary>
    /// Port of FindObsoleteFiles(force = false) + PurgeObsoleteFiles (db/db_impl/db_impl_files.cc): deletes table files
    /// no version references, WALs below the minimum log number with unflushed data, and old MANIFESTs.
    /// </summary>
    internal void PurgeObsoleteFiles()
    {
        var tables = new List<ObsoleteTableFile>();
        var blobs = new List<ObsoleteBlobFile>();
        var wals = new List<ulong>();
        var manifests = new List<ulong>();
        List<(ColumnFamilyData Cfd, ulong Number)>? releasedBlobs = null;
        TestHooks.Invoke("DBImpl::PurgeObsoleteFiles:Start");
        lock (_mutex)
        {
            // DisableFileDeletions: keep everything queued until deletions are re-enabled (FindObsoleteFiles).
            if (FileDeletionsDisabled || _readOnly) return;
            Versions.TakeObsoleteFiles(tables);
            Versions.TakeObsoleteBlobFiles(blobs);
            if (_anyBlobDirectWrite)
            {
                // Blob direct write files referenced by a live memtable are deleted once it is released.
                blobs.RemoveAll(b => _pendingOutputs.Contains(b.BlobFileNumber));
                releasedBlobs = TakeReleasedBlobDirectWriteFiles();
            }
            ulong minLog = MinLogNumberToKeepLive();
            for (int i = 0; i < _aliveWals.Count;)
            {
                ulong n = _aliveWals[i];
                if (n < minLog && n != _logNumber)
                {
                    wals.Add(n);
                    _aliveWals.RemoveAt(i);
                    ForgetAliveWalSize(n);
                }
                else i++;
            }
            manifests.AddRange(_obsoleteManifests);
            _obsoleteManifests.Clear();
        }
        foreach (ObsoleteTableFile t in tables)
        {
            t.Cfd.TableCache.Evict(t.Meta.FileNumber);
            DeleteObsoleteTableFile(FileNames.TableFileName(DbPaths, t.Meta.FileNumber, t.Meta.PathId));
        }
        foreach (ObsoleteBlobFile b in blobs)
        {
            // The SharedBlobFileMetaData deleter: evict the reader, then delete the file (AddObsoleteBlobFile).
            b.Cfd.BlobSource.Evict(b.BlobFileNumber);
            b.Cfd.BlobDirectWrite?.Forget(b.BlobFileNumber);
            TryDeleteFile(FileNames.BlobFileName(DbPaths[0], b.BlobFileNumber));
        }
        if (releasedBlobs is not null)
        {
            foreach ((ColumnFamilyData cfd, ulong n) in releasedBlobs)
            {
                cfd.BlobSource.Evict(n);
                cfd.BlobDirectWrite?.Forget(n);
                TryDeleteFile(FileNames.BlobFileName(DbPaths[0], n));
            }
        }
        foreach (ulong n in wals) DeleteOrArchiveWal(n);
        foreach (ulong n in manifests) TryDeleteFile(FileNames.DescriptorFileName(DbName, n));
        PurgeObsoleteWalFiles(force: false);
    }

    /// <summary>
    /// Full scan variant used at open (FindObsoleteFiles(force = true)): any table file that is not live, old
    /// MANIFESTs, temp files and WALs that hold no unflushed data are deleted; only the two newest OPTIONS files are kept.
    /// </summary>
    private void DeleteObsoleteFilesFullScan()
    {
        var toDelete = new List<string>();
        var options = new List<(ulong Number, string Path)>();
        var walsToDelete = new List<ulong>();
        lock (_mutex)
        {
            if (FileDeletionsDisabled || _readOnly) return;
            ulong minLog = MinLogNumberToKeepLive();
            var dirs = new List<string>(DbPaths);
            if (!dirs.Contains(WalDir)) dirs.Add(WalDir);
            foreach (string dir in dirs)
            {
                IEnumerable<string> children;
                try { children = FileSystem.GetChildren(dir); }
                catch (Exception e) when (e is IOException or RocksDbException) { continue; }
                foreach (string child in children)
                {
                    if (!FileNames.TryParseFileName(child, out ulong number, out FileType type)) continue;
                    string full = dir + "/" + child;
                    switch (type)
                    {
                        case FileType.TableFile:
                            if (!Versions.IsLiveFile(number) && !_pendingOutputs.Contains(number)) toDelete.Add(full);
                            break;
                        case FileType.BlobFile:
                            if (!Versions.IsLiveBlobFile(number) && !_pendingOutputs.Contains(number)) toDelete.Add(full);
                            break;
                        case FileType.DescriptorFile:
                            if (dir == DbName && number != Versions.ManifestFileNumber) toDelete.Add(full);
                            break;
                        case FileType.TempFile:
                            if (dir == DbName) toDelete.Add(full);
                            break;
                        case FileType.WalFile:
                            if (dir == WalDir && number < minLog && number != _logNumber)
                            {
                                walsToDelete.Add(number);
                                _aliveWals.Remove(number);
                                ForgetAliveWalSize(number);
                            }
                            break;
                        case FileType.OptionsFile:
                            if (dir == DbName) options.Add((number, full));
                            break;
                    }
                }
            }
        }
        options.Sort(static (a, b) => b.Number.CompareTo(a.Number));
        for (int i = 2; i < options.Count; i++) toDelete.Add(options[i].Path);
        foreach (string p in toDelete) TryDeleteFile(p);
        foreach (ulong n in walsToDelete) DeleteOrArchiveWal(n);
        PurgeObsoleteWalFiles(force: true);
    }

    /// <summary>A WAL left alive_wal_files_: its size leaves wals_total_size_. REQUIRES: mutex.</summary>
    private void ForgetAliveWalSize(ulong number)
    {
        if (_aliveWalSizes.Remove(number, out long size)) Interlocked.Add(ref _walsTotalSize, -size);
    }

    private void TryDeleteFile(string path)
    {
        try { FileSystem.DeleteFile(path); }
        catch (IOException) { }
        catch (UnauthorizedAccessException) { }
        catch (RocksDbException) { }
    }

    // ---- shutdown ---------------------------------------------------------------------------------------

    /// <summary>
    /// Port of DBImpl::CloseHelper: waits for in-flight writes and background flushes, flushes memtables holding
    /// data that is not in the WAL (unless avoid_flush_during_shutdown), closes the WAL and MANIFEST and releases the
    /// LOCK file. The WAL is flushed to the OS but not fsynced (like upstream).
    /// </summary>
    public async ValueTask CloseAsync(bool synchronous)
    {
        if (Volatile.Read(ref _disposed) != 0) return;
        // CancelErrorRecoveryForShutDown: stop automatic recovery first (it needs the write thread and background work).
        await ErrorHandler.EndAutoRecoveryAsync(synchronous).ConfigureAwait(false);
        // force_flush_for_blob_direct_write: blob direct write files are only registered by flushes (WAL recovery of
        // unregistered files is out of scope, like upstream v1), so they are flushed even with avoid_flush_during_shutdown.
        if (((!Options.AvoidFlushDuringShutdown && Volatile.Read(ref _hasUnpersistedData)) || HasInFlightBlobDirectWriteFiles()) && BackgroundError is null)
        {
            try
            {
                foreach (ColumnFamilyData cfd in GetColumnFamilies())
                {
                    await FlushColumnFamilyAsync(cfd, wait: true, FlushReason.ShutDown, synchronous, CancellationToken.None).ConfigureAwait(false);
                }
            }
            catch (RocksDbException)
            {
                // Best effort, like upstream's shutdown flush.
            }
        }
        // Become the (last) write leader so no write is in flight.
        WriteThread.Writer w = await EnterUnbatchedCoreAsync(synchronous).ConfigureAwait(false);
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            ExitUnbatchedQueues();
            return;
        }
        NotifySequenceWaiters(); // wake WAL tailers so they observe the close
        try
        {
            LogInfo("Shutdown: canceling all background work");
            lock (_mutex)
            {
                _shuttingDown = true;
                SignalBackgroundWork(); // wake FlushAsync waiters whose queued flush will never be scheduled now
            }
            CancelBackgroundCompactions();
            await WaitForBackgroundWorkAsync(synchronous).ConfigureAwait(false);
            // logs_.clear() (ClearWriter): closed WALs not synced yet are closed without a sync, like the current one.
            foreach (LogWriter closed in _closedWalsToSync) CloseQuietly(closed);
            _closedWalsToSync.Clear();
            LogWriter? log = _log;
            _log = null;
            if (log is not null)
            {
                if (synchronous) log.Dispose();
                else await log.DisposeAsync().ConfigureAwait(false);
            }
            // ~ColumnFamilyHandleImpl of dropped column families (their handles live as long as the DB): release their
            // memtables and current version; their table files become obsolete and are deleted now.
            bool releasedDropped = false;
            lock (_mutex)
            {
                foreach (ColumnFamilyData cfd in _droppedColumnFamilies)
                {
                    if (cfd.Current is not { } v) continue;
                    ReleaseColumnFamilyMemTables(cfd);
                    cfd.Current = null!;
                    Versions.UnrefVersion(v);
                    releasedDropped = true;
                }
            }
            if (releasedDropped)
            {
                try { PurgeObsoleteFiles(); }
                catch (Exception e) when (e is IOException or RocksDbException) { }
            }
            await Versions.DisposeAsync().ConfigureAwait(false);
            lock (_mutex)
            {
                foreach (ColumnFamilyData cfd in Versions.ColumnFamilies) ReleaseColumnFamilyResources(cfd);
                // Dropped column families keep their table cache open until close: readers and background jobs that
                // still hold one of their versions read through it (their files are evicted as they become obsolete).
                foreach (ColumnFamilyData cfd in _droppedColumnFamilies)
                {
                    cfd.TableCache.Dispose();
                    cfd.BlobSource.Dispose();
                    cfd.BlobDirectWrite?.Dispose();
                }
                _droppedColumnFamilies.Clear();
            }
        }
        finally
        {
            ExitUnbatchedQueues();
            WriteThread.Return(w, synchronous);
            _groupBatch.Dispose();
            _nonmemGroupBatch.Dispose();
            try { ShutdownMonitoring(); } catch (Exception e) when (e is IOException or RocksDbException) { }
            _dbLock?.Dispose();
            _dbLock = null;
            if (_secondary) DisposeSecondaryReaders();
        }
    }

    /// <summary>Drops the CF's references on its super version, memtables and version and closes its table cache. REQUIRES: mutex.</summary>
    private void ReleaseColumnFamilyResources(ColumnFamilyData cfd)
    {
        ReleaseColumnFamilyMemTables(cfd);
        cfd.TableCache.Dispose();
        cfd.BlobSource.Dispose();
        cfd.BlobDirectWrite?.Dispose();
    }

    /// <summary>
    /// Drops the CF's references on its super version and memtables (memtables still referenced by readers or a running
    /// flush stay alive until those release them). REQUIRES: mutex.
    /// </summary>
    private void ReleaseColumnFamilyMemTables(ColumnFamilyData cfd)
    {
        SuperVersion? sv = cfd.SuperOrNull();
        if (sv is not null && sv.Unref()) CleanupSuperVersion(sv);
        if (cfd.Mem is not null && cfd.Mem.Unref()) cfd.Mem.Dispose();
        foreach (ImmutableMemTable m in cfd.Imm)
        {
            if (m.Mem.Unref()) m.Mem.Dispose();
        }
        cfd.Imm.Clear();
        ReleaseMemTableHistory(cfd);
    }

    public ValueTask DisposeAsync() => CloseAsync(synchronous: false);
}

