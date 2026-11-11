namespace RocksDb.Managed.Db;

internal sealed partial class DbImpl
{
    private readonly Queue<ColumnFamilyData> _flushQueue = new(); // guarded by mutex
    private int _maxBackgroundFlushes; // changed by SetDBOptions under the mutex
    private int _runningFlushes;   // guarded by mutex
    private int _backgroundJobs;   // guarded by mutex
    private bool _shuttingDown;    // guarded by mutex
    // ---- atomic_flush (guarded by mutex) ----
    private bool _atomicFlushRunning;   // a scheduled atomic flush (all its members) is running
    private ulong _atomicCutNext;       // last memtable switch id handed out
    private ulong _atomicCutCompleted;  // newest switch whose memtables are all sealed (flushable)
    private ulong _atomicSwitchCut;     // the multi column family switch in progress, or 0
    // Completed (and replaced) whenever background work finishes or fails; waiters re-check their condition.
    private TaskCompletionSource _bgSignal = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Wakes everyone waiting for background progress. REQUIRES: mutex.</summary>
    private void SignalBackgroundWork()
    {
        TaskCompletionSource old = _bgSignal;
        _bgSignal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        old.TrySetResult();
    }

    /// <summary>Port of SchedulePendingFlush. REQUIRES: mutex.</summary>
    private void EnqueueFlush(ColumnFamilyData cfd)
    {
        // atomic_flush: there is no per column family request; MaybeScheduleFlush starts one flush for every column
        // family with sealed memtables.
        if (Options.AtomicFlush) return;
        if (cfd.QueuedForFlush || cfd.Dropped || !cfd.HasPendingFlush) return;
        cfd.QueuedForFlush = true;
        _flushQueue.Enqueue(cfd);
    }

    /// <summary>
    /// Port of MaybeScheduleFlushOrCompaction (flush part): starts background flush jobs up to the flush limit
    /// (GetBGJobLimits). Flushes of one column family are serialized; different CFs flush in parallel. REQUIRES: mutex.
    /// </summary>
    private void MaybeScheduleFlush()
    {
        if (_shuttingDown || IsBgWorkStopped) return;
        if (Options.AtomicFlush)
        {
            MaybeScheduleAtomicFlush();
            return;
        }
        int attempts = _flushQueue.Count;
        while (_runningFlushes < _maxBackgroundFlushes && attempts-- > 0 && _flushQueue.TryDequeue(out ColumnFamilyData? cfd))
        {
            cfd.QueuedForFlush = false;
            if (cfd.Dropped || !cfd.HasPendingFlush) continue;
            if (cfd.FlushRunning) continue; // re-enqueued when the running flush finishes
            cfd.FlushRunning = true;
            _runningFlushes++;
            _backgroundJobs++;
            BackgroundThreadPool.High.Schedule(new FlushRun(this, cfd));
        }
    }

    /// <summary>
    /// Starts a switch of the memtables of several column families that must be flushed together (DBImpl::ScheduleFlushes /
    /// AtomicFlushMemTables: every selected column family is switched, then one FlushRequest with their max memtable ids
    /// is generated). Until <see cref="EndAtomicSwitch"/> the memtables sealed by it are not picked by an atomic flush.
    /// REQUIRES: write leader.
    /// </summary>
    internal void BeginAtomicSwitch()
    {
        lock (_mutex) _atomicSwitchCut = ++_atomicCutNext;
    }

    /// <summary>Ends <see cref="BeginAtomicSwitch"/>: the sealed memtables become flushable and the flush is scheduled.</summary>
    internal void EndAtomicSwitch()
    {
        lock (_mutex)
        {
            _atomicCutCompleted = _atomicSwitchCut;
            _atomicSwitchCut = 0;
            MaybeScheduleFlush();
        }
    }

    /// <summary>
    /// Switches the memtables of <paramref name="cfds"/> that hold data, for a flush with <paramref name="reason"/>. With
    /// atomic_flush the memtables of every column family holding data are switched instead
    /// (DBImpl::SelectColumnFamiliesForAtomicFlush) and flushed as one atomic flush. REQUIRES: write leader.
    /// </summary>
    private async ValueTask SwitchMemtablesForFlushAsync(IReadOnlyList<ColumnFamilyData> cfds, FlushReason reason, bool synchronous,
        CancellationToken cancellationToken)
    {
        if (Options.AtomicFlush)
        {
            var all = new List<ColumnFamilyData>();
            lock (_mutex)
            {
                foreach (ColumnFamilyData c in Versions.ColumnFamilies)
                {
                    if (!c.Dropped && !c.Mem.IsEmpty) all.Add(c);
                }
            }
            cfds = all;
        }
        if (cfds.Count == 0) return;
        BeginAtomicSwitch();
        try
        {
            for (int i = 0; i < cfds.Count; i++)
            {
                ColumnFamilyData cfd = cfds[i];
                if (cfd.Dropped || cfd.Mem.IsEmpty) continue;
                await SwitchMemtableAsync(cfd, reason, synchronous, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            EndAtomicSwitch();
        }
    }

    /// <summary>True if an atomic flush is running or could be started (sealed memtables of a completed switch). REQUIRES: mutex.</summary>
    private bool AtomicFlushPending
    {
        get
        {
            if (!Options.AtomicFlush) return false;
            if (_atomicFlushRunning) return true;
            foreach (ColumnFamilyData c in Versions.ColumnFamilies)
            {
                if (!c.Dropped && c.HasPendingFlushUpTo(_atomicCutCompleted)) return true;
            }
            return false;
        }
    }

    /// <summary>
    /// atomic_flush part of MaybeScheduleFlushOrCompaction / BackgroundFlush: one flush job covers every column family
    /// with sealed memtables up to the newest completed switch (the FlushRequest of DBImpl::GenerateFlushRequest). Atomic
    /// flushes run one at a time. REQUIRES: mutex.
    /// </summary>
    private void MaybeScheduleAtomicFlush()
    {
        if (_atomicFlushRunning || _runningFlushes >= _maxBackgroundFlushes) return;
        ulong cut = _atomicCutCompleted;
        List<ColumnFamilyData>? members = null;
        foreach (ColumnFamilyData c in Versions.ColumnFamilies)
        {
            if (c.Dropped || !c.HasPendingFlushUpTo(cut)) continue;
            if (c.FlushRunning) return; // an error recovery flush owns it; rescheduled when recovery ends
            (members ??= new List<ColumnFamilyData>()).Add(c);
        }
        if (members is null) return;
        foreach (ColumnFamilyData c in members)
        {
            c.FlushRunning = true;
            _runningFlushes++;
            _backgroundJobs++;
        }
        _atomicFlushRunning = true;
        BackgroundThreadPool.High.Schedule(new AtomicFlushRun(this, members, cut));
    }

    /// <summary>A scheduled atomic flush (DBImpl::AtomicFlushMemTablesToOutputFiles) on the HIGH pool.</summary>
    private sealed class AtomicFlushRun(DbImpl db, List<ColumnFamilyData> members, ulong cut) : IThreadPoolWorkItem
    {
        public void Execute() => db.RunAtomicFlushAsync(members, cut).GetAwaiter().GetResult();
    }

    /// <summary>
    /// Port of DBImpl::AtomicFlushMemTablesToOutputFiles: every member writes its L0 file, then one MANIFEST atomic group
    /// installs all results (MemTableList::InstallMemtableAtomicFlushResults). If a member fails nothing is installed
    /// and the memtables of all members stay to be flushed again (RollbackMemtableFlush).
    /// </summary>
    private async Task RunAtomicFlushAsync(List<ColumnFamilyData> members, ulong cut)
    {
        try
        {
            var group = new AtomicFlushGroup(this, members);
            var tasks = new Task[members.Count];
            for (int i = 0; i < members.Count; i++) tasks[i] = BackgroundFlushAsync(members[i], group, synchronous: true, cut);
            await Task.WhenAll(tasks).ConfigureAwait(false);
        }
        finally
        {
            lock (_mutex)
            {
                _atomicFlushRunning = false;
                MaybeScheduleFlush();
                SignalBackgroundWork();
            }
        }
    }

    /// <summary>
    /// Port of BackgroundFlush + FlushMemTableToOutputFile + MemTableList::TryInstallMemtableFlushResults: flushes all
    /// immutable memtables of the CF that are not being flushed into one L0 file, records it in the MANIFEST together
    /// with the CF's new log number, then drops the memtables and deletes obsolete WALs.
    /// </summary>
    private Task BackgroundFlushAsync(ColumnFamilyData cfd) => BackgroundFlushAsync(cfd, null, synchronous: false, ulong.MaxValue);


    /// <summary>
    /// A scheduled flush (BGWorkFlush on the HIGH pool): runs the flush synchronously on a <see cref="BackgroundThreadPool.High"/>
    /// thread, like upstream's flush threads (table building is CPU-bound; it does not belong on the .NET thread pool).
    /// </summary>
    private sealed class FlushRun(DbImpl db, ColumnFamilyData cfd) : IThreadPoolWorkItem
    {
        public void Execute() => db.BackgroundFlushAsync(cfd, null, synchronous: true, ulong.MaxValue).GetAwaiter().GetResult();
    }

    /// <summary>
    /// <see cref="BackgroundFlushAsync(ColumnFamilyData)"/> as a member of <paramref name="group"/> (an atomic flush:
    /// the flush result is committed to the MANIFEST together with the other members' results), or alone when null.
    /// With <paramref name="synchronous"/> the file and MANIFEST I/O is synchronous (on a background pool thread).
    /// Only memtables sealed by a switch up to <paramref name="maxCut"/> are picked (the max memtable id of
    /// MemTableList::PickMemtablesToFlush).
    /// </summary>
    private async Task BackgroundFlushAsync(ColumnFamilyData cfd, AtomicFlushGroup? group, bool synchronous, ulong maxCut)
    {
        Exception? failure = null;
        bool cfDropped = false;
        var picked = new List<ImmutableMemTable>();
        ulong fileNumber;
        ulong epoch;
        lock (_mutex)
        {
            if (ShouldRescheduleFlushToRetainUdt(cfd))
            {
                RescheduleFlushToRetainUdt(cfd);
                return;
            }
            foreach (ImmutableMemTable m in cfd.Imm)
            {
                if (m.FlushInProgress || m.FlushCompleted) continue;
                if (m.AtomicCut > maxCut) break;
                m.FlushInProgress = true;
                // The flush reads the memtable without the mutex: keep it alive even if the CF is dropped meanwhile
                // (DropColumnFamily releases the CF's memtable references).
                m.Mem.Ref();
                picked.Add(m);
            }
            fileNumber = Versions.NewFileNumber();
            epoch = cfd.NewEpochNumber();
            _pendingOutputs.Add(fileNumber);
        }
        try
        {
            // SyncClosedWals + ApplyWALToManifest (FlushMemTableToOutputFile): record the synced sizes of closed WALs.
            await TrackClosedWalsAsync(synchronous).ConfigureAwait(false);
            if (picked.Count > 0) await FlushMemTablesAsync(cfd, picked, fileNumber, epoch, synchronous, group).ConfigureAwait(false);
        }
        catch (RocksDbException e) when (e.Code == StatusCode.ColumnFamilyDropped || cfd.Dropped)
        {
            failure = e;
            cfDropped = true;
            lock (_mutex)
            {
                foreach (ImmutableMemTable m in picked) m.FlushInProgress = false;
            }
            TryDeleteFile(FileNames.TableFileName(DbPaths, fileNumber, 0));
        }
        catch (Exception e)
        {
            failure = e;
            lock (_mutex)
            {
                foreach (ImmutableMemTable m in picked) m.FlushInProgress = false;
            }
            // The output is left for the full scan at the next open: after a failed MANIFEST write it may already be
            // referenced by a MANIFEST that CURRENT names (upstream quarantines such files). An atomic flush that
            // failed before its MANIFEST write (another member's job failed) installed nothing: its output is deleted
            // (AtomicFlushMemTablesToOutputFiles deletes the outputs of a failed atomic flush).
            if (group is not null && !group.ManifestWriteAttempted) TryDeleteFile(FileNames.TableFileName(DbPaths, fileNumber, 0));
            // FlushMemTableToOutputFile: the WAL is synced at memtable switch, so a failed MANIFEST write maps to
            // kManifestWriteNoWAL and any other failure (table file write) to kFlushNoWAL.
            SetBackgroundError(e, Versions.IoStatus is not null ? BackgroundErrorReason.ManifestWriteNoWal : BackgroundErrorReason.FlushNoWal,
                walRelated: false);
        }
        finally
        {
            // A member that failed or had nothing to flush leaves the atomic flush (a failure fails the whole group; a
            // dropped column family is skipped, AtomicFlushMemTablesToOutputFiles: "if (cfds[i]->IsDropped()) continue").
            group?.Leave(cfd, cfDropped ? null : failure);
            lock (_mutex)
            {
                foreach (ImmutableMemTable m in picked)
                {
                    if (m.Mem.Unref())
                    {
                        m.Mem.Dispose();
                    }
                    else if (m.Mem.FlushedPinnedSet is null)
                    {
                        // Still referenced (an iterator's super version, the history list or a failed flush that left
                        // it in the list): keep counting it in size-all-mem-tables until it is released.
                        cfd.FlushedPinnedMemTables.Add(m.Mem);
                        m.Mem.FlushedPinnedSet = cfd.FlushedPinnedMemTables;
                    }
                }
                _pendingOutputs.Remove(fileNumber);
                // The flush itself is done: let the next flush of this CF start before the (slow) obsolete-file purge.
                // Waiters (FlushAsync, write stalls) are signalled after the purge, like upstream's WaitForFlushMemTables
                // woken by the bg_cv_ signal at the end of BackgroundCallFlush: a Flush(wait) that returns has the WAL
                // it made obsolete archived or deleted, and the obsolete table files deleted.
                cfd.FlushRunning = false;
                _runningFlushes--;
                EnqueueFlush(cfd);
                MaybeScheduleFlush();
            }
            try
            {
                // Before the job is accounted as finished (like BackgroundCallFlush: bg_flush_scheduled_-- after
                // PurgeObsoleteFiles): CloseAsync waits for _backgroundJobs to drain, so a purge running after close
                // could delete files of a new DB created in the same directory.
                PurgeObsoleteFiles();
            }
            catch (Exception e) when (e is IOException or RocksDbException)
            {
                // Deleting obsolete files is best effort.
            }
            lock (_mutex)
            {
                _backgroundJobs--;
                SignalBackgroundWork();
            }
        }
    }

    /// <summary>Builds the L0 file for <paramref name="picked"/> and installs the result (FlushJob::Run + install).</summary>
    private async ValueTask FlushMemTablesAsync(ColumnFamilyData cfd, List<ImmutableMemTable> picked, ulong fileNumber, ulong epoch,
        bool synchronous, AtomicFlushGroup? group)
    {
        TestHooks.Invoke("DBImpl::FlushMemTables:Start");
        long flushStart = System.Diagnostics.Stopwatch.GetTimestamp();
        int jobId = NewJobId();
        var mems = new List<MemTable>(picked.Count);
        foreach (ImmutableMemTable m in picked) mems.Add(m.Mem);
        FlushReason reason = picked[^1].FlushReason;
        OnFlushStarted(cfd, jobId, mems, reason);
        string path = FileNames.TableFileName(DbPaths, fileNumber, 0);
        NotifyOnFlushBegin(cfd, jobId, fileNumber, path, mems, reason);
        using Blob.BlobFileBuilder? blobBuilder = CreateBlobFileBuilder(cfd, outputLevel: 0, Blob.BlobFileCreationReason.Flush);
        try
        {
            await FlushAndInstallAsync(cfd, picked, fileNumber, epoch, synchronous, jobId, flushStart, mems, path, blobBuilder, reason, group)
                .ConfigureAwait(false);
        }
        finally
        {
            if (blobBuilder is not null) ReleaseBlobOutputs(blobBuilder);
        }
    }

    private async ValueTask FlushAndInstallAsync(ColumnFamilyData cfd, List<ImmutableMemTable> picked, ulong fileNumber, ulong epoch,
        bool synchronous, int jobId, long flushStart, List<MemTable> mems, string path, Blob.BlobFileBuilder? blobBuilder,
        FlushReason reason, AtomicFlushGroup? group)
    {
        ulong[] snapshots;
        SeqnoToTimeMapping? seqnoToTime;
        DbVersion baseVersion;
        lock (_mutex)
        {
            snapshots = SnapshotsForCompaction();
            seqnoToTime = CopySeqnoToTimeMapping(cfd);
            // FlushJob::PickMemTable: base_ = cfd_->current(); base_->Ref(). BuildTable estimates the data covered by
            // the flushed range tombstones in it (compensated_range_deletion_size).
            baseVersion = cfd.Current;
            baseVersion.Ref();
        }
        // PrepareFlushAdditions: the blob direct write files of the flushed memtables are sealed and registered with the
        // table file; their blobs the output does not reference (hidden versions, failed writes) are garbage.
        (List<BlobFileAddition> Additions, Blob.BlobGarbageMeter Meter)? directWrite = PrepareBlobDirectWriteFlush(cfd, picked);
        FileMetaData? meta;
        try
        {
            meta = await FlushJob.WriteLevel0TableAsync(FileSystem, path, TableFileWriteOptions(Env.IoPriority.High), cfd, mems, fileNumber, epoch, Identity, snapshots, CurrentSnapshotChecker, blobBuilder,
                    directWrite?.Meter, seqnoToTime, baseVersion.Storage, synchronous, CancellationToken.None)
                .ConfigureAwait(false);
        }
        catch (Exception e) when (HasListeners)
        {
            // BuildTable: LogAndNotifyTableFileCreationFinished also reports a failed file, with its status.
            NotifyOnTableFileCreated(cfd, jobId, path, 0, TableFileCreationReason.Flush, e, null);
            throw;
        }
        finally
        {
            lock (_mutex) Versions.UnrefVersion(baseVersion);
        }
        if (meta is null) NotifyOnEmptyTableFile(cfd, jobId, TableFileCreationReason.Flush);
        if (meta is not null)
        {
            // BuildTable: "Verify that the table is usable" (table_cache->NewIterator): the new table is opened and stays
            // in the table cache, so lookups that must not do file I/O (KeyMayExist, kBlockCacheTier) can use it.
            // (ReadOptions with Env::IOActivity::kFlush: the reads are timed in FILE_READ_FLUSH_MICROS.)
            int activity = Monitoring.FileReadActivity.Enter(Histogram.FileReadFlushMicros);
            try
            {
                using (cfd.TableCache.GetReader(meta))
                {
                }
            }
            finally
            {
                Monitoring.FileReadActivity.Exit(activity);
            }
            FileSystem.SyncDirectory(DbPaths[0]);
            // BuildTable: the file creation event is raised when the table is finished, before the result is installed.
            if (HasListeners) NotifyOnTableFileCreated(cfd, jobId, path, meta.FileSize, TableFileCreationReason.Flush, null, TryGetTableProperties(cfd, meta));
            RecordFlushBytes(meta.FileSize + BlobBytesWritten(blobBuilder));
            // FlushJob::WriteLevel0Table logs this before the result is installed (and compactions are scheduled).
            LogLevel0FlushTable(cfd, jobId, meta);
        }

        var edit = new VersionEdit
        {
            ColumnFamily = cfd.Id,
            PrevLogNumber = 0,
            // The CF's data in WALs below the next log number of the newest flushed memtable is now persisted.
            LogNumber = picked[^1].NextLogNumber,
        };
        if (meta is not null)
        {
            edit.AddFile(0, meta);
            if (blobBuilder is not null) edit.BlobFileAdditions.AddRange(blobBuilder.Additions);
        }
        if (directWrite is { } dw)
        {
            edit.BlobFileAdditions.AddRange(dw.Additions);
            dw.Meter.AddGarbageTo(edit.BlobFileGarbages);
        }
        // FlushJob::MaybeIncreaseFullHistoryTsLowToAboveCutoffUDT (written in the flush's own edit instead of a separate
        // MANIFEST record before it; recovery applies both the same way).
        if (GetFullHistoryTsLowAboveCutoffUdt(cfd, mems) is { } newTsLow) edit.FullHistoryTsLow = newTsLow;
        VersionEdit? walDeletion = null;
        lock (_mutex)
        {
            if (cfd.Dropped) ThrowDropped(cfd);
            ulong minWalToKeep;
            if (Options.Allow2Pc)
            {
                // In 2PC mode the WALs holding outstanding prepared sections (or prepared sections of committed data
                // still in memtables) must survive (PrecomputeMinLogNumberToKeep2PC).
                minWalToKeep = PrecomputeMinLogNumberToKeep2PC(cfd, edit.LogNumber.Value, picked);
                edit.MinLogNumberToKeep = minWalToKeep;
            }
            else
            {
                // PrecomputeMinLogNumberToKeepNon2PC: minimum of the other CFs' log numbers and this edit's (an atomic
                // flush computes it over all its members when it commits).
                minWalToKeep = Math.Min(Versions.MinLogNumberWithUnflushedData(cfd), edit.LogNumber.Value);
                // GetDBRecoveryEditForObsoletingMemTables records it for every flush (also with a single column family),
                // which keeps versions_->min_log_number_to_keep ("rocksdb.min-log-number-to-keep") current.
                if (group is null) edit.MinLogNumberToKeep = minWalToKeep;
            }
            // GetDBRecoveryEditForObsoletingMemTables: with track_and_verify_wals_in_manifest the WALs the flush makes
            // obsolete are recorded as deleted in a separate edit of the same MANIFEST write.
            if (group is null && Options.TrackAndVerifyWalsInManifest && minWalToKeep > Versions.MinWalNumberToKeep)
            {
                walDeletion = new VersionEdit { WalDeletionBefore = minWalToKeep };
            }
        }
        TestHooks.Invoke("DBImpl::FlushMemTables:BeforeLogAndApply");
        // From here on a failure may still have made the edit durable: leave the blob files to the obsolete-file scan.
        if (blobBuilder is not null) blobBuilder.Committed = true;
        // Removes the flushed memtables and installs the super version (which recalculates the write stall); runs in the
        // critical section that installs the new version, like upstream's TryInstallMemtableFlushResults followed by
        // InstallSuperVersionAndScheduleWork under one mutex hold. REQUIRES: mutex.
        bool dropped = false;
        void InstallFlushResults()
        {
            foreach (ImmutableMemTable m in picked)
            {
                m.FlushCompleted = true;
                // A drop that committed right after our MANIFEST write already released the list's references.
                if (cfd.Imm.Remove(m)) RetireFlushedMemTable(cfd, m.Mem);
            }
            if (cfd.Dropped)
            {
                dropped = true;
                return;
            }
            InstallSuperVersion(cfd);
            _maybeStalled = IsWriteStalled();
        }
        if (group is not null)
        {
            await group.CommitAsync(cfd, edit).ConfigureAwait(false);
            lock (_mutex) InstallFlushResults();
        }
        else if (walDeletion is not null)
        {
            await Versions.LogAndApplyAsync([new ManifestEditEntry(cfd, edit), new ManifestEditEntry(null, walDeletion)], atomicGroup: false, synchronous,
                InstallFlushResults, CancellationToken.None).ConfigureAwait(false);
        }
        else
        {
            await Versions.LogAndApplyAsync(cfd, edit, synchronous, InstallFlushResults, CancellationToken.None).ConfigureAwait(false);
        }
        if (meta is not null) MaybeReportMaxAllowedSpaceReached(BackgroundErrorReason.Flush);
        if (dropped) return;
        if (blobBuilder is not null && meta is not null) LogBlobFileCreationEvents(cfd, jobId, blobBuilder.Additions);
        OnFlushCompleted(cfd, jobId, meta, BlobBytesWritten(blobBuilder), flushStart);
        NotifyOnFlushCompleted(cfd, jobId, fileNumber, meta, path, mems, reason);
    }

    /// <summary>
    /// Manual flush (DBImpl::FlushMemTable): switches the CF's memtable if it holds data, schedules the flush and, with
    /// <paramref name="wait"/>, waits until every memtable that existed at the call has been flushed.
    /// </summary>
    internal async ValueTask FlushColumnFamilyAsync(ColumnFamilyData cfd, bool wait, FlushReason reason, bool synchronous, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        if (_readOnly) ThrowReadOnly();
        WriteThread.Writer w = await EnterUnbatchedAsync(synchronous, cancellationToken).ConfigureAwait(false);
        // The newest sealed memtable of every column family to wait for (flush_memtable_ids): the requested one, or with
        // atomic_flush every column family (DBImpl::AtomicFlushMemTables).
        var targets = new List<(ColumnFamilyData Cfd, ImmutableMemTable Mem)>(1);
        try
        {
            lock (_mutex)
            {
                if (IsBgWorkStopped && ErrorHandler.BgError is { } bg) ThrowBackgroundError(bg);
            }
            if (cfd.Dropped) ThrowDropped(cfd);
            await SwitchMemtablesForFlushAsync([cfd], reason, synchronous, cancellationToken).ConfigureAwait(false);
            lock (_mutex)
            {
                if (Options.AtomicFlush)
                {
                    foreach (ColumnFamilyData c in Versions.ColumnFamilies)
                    {
                        if (!c.Dropped && c.Imm.Count > 0) targets.Add((c, c.Imm[^1]));
                    }
                }
                else if (cfd.Imm.Count > 0)
                {
                    targets.Add((cfd, cfd.Imm[^1]));
                }
                // DBImpl::FlushMemTable: a manual flush is never postponed to retain user-defined timestamps.
                if (cfd.TimestampsInMemtableOnly && cfd.HasPendingFlush) cfd.FlushSkipReschedule = true;
                EnqueueFlush(cfd);
                MaybeScheduleFlush();
            }
        }
        finally
        {
            ExitUnbatched(w, synchronous);
        }
        if (!wait || targets.Count == 0) return;
        while (true)
        {
            Task signal;
            lock (_mutex)
            {
                bool done = true;
                foreach ((ColumnFamilyData c, ImmutableMemTable target) in targets)
                {
                    if (!target.FlushCompleted && !c.Dropped && c.Imm.Contains(target)) done = false;
                }
                if (done)
                {
                    // WaitForFlushMemTables: an error that stopped the DB (e.g. SpaceLimit after the flush) is reported.
                    if (ErrorHandler.IsDbStopped && ErrorHandler.BgError is { } stopped) ThrowBackgroundError(stopped);
                    return;
                }
                if (IsBgWorkStopped && ErrorHandler.BgError is { } e) ThrowBackgroundError(e);
                if (cfd.Dropped) ThrowDropped(cfd);
                // Close stops scheduling flushes: a flush that was queued but never started will not complete, so
                // waiters must not sleep past shutdown (CloseAsync signals after setting _shuttingDown).
                if (_shuttingDown) ThrowDisposed();
                signal = _bgSignal.Task;
            }
            if (synchronous) signal.Wait(cancellationToken);
            else await signal.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    // ---- user-defined timestamps in memtable only (persist_user_defined_timestamps = false) ----------------

    /// <summary>
    /// Port of DBImpl::ShouldRescheduleFlushRequestToRetainUDT: an automatic flush of a column family with timestamps
    /// in memtable only is postponed while a memtable to flush holds timestamps not below full_history_ts_low, unless
    /// postponing would stall writes (the memtable criteria of GetWriteStallConditionAndCause) or a manual flush asked
    /// for it. REQUIRES: mutex.
    /// </summary>
    private bool ShouldRescheduleFlushToRetainUdt(ColumnFamilyData cfd)
    {
        if (!cfd.TimestampsInMemtableOnly) return false;
        if (cfd.FlushSkipReschedule)
        {
            cfd.FlushSkipReschedule = false;
            return false;
        }
        if (cfd.Dropped) return false;
        var mems = new List<MemTable>();
        foreach (ImmutableMemTable m in cfd.Imm)
        {
            if (!m.FlushInProgress && !m.FlushCompleted) mems.Add(m.Mem);
        }
        if (!cfd.ShouldPostponeFlushToRetainUdt(mems)) return false;
        int unflushed = (cfd.Mem.IsEmpty ? 0 : 1) + cfd.NumNotFlushed;
        ColumnFamilyOptions o = cfd.Options;
        bool stall = unflushed >= o.MaxWriteBufferNumber
            || (o.MaxWriteBufferNumber > 3 && unflushed >= o.MaxWriteBufferNumber - 1 && unflushed - 1 >= o.MinWriteBufferNumberToMerge);
        return !stall;
    }

    /// <summary>
    /// Re-queues a postponed flush after 100 ms (BGWorkFlush: "prevent hot loop"). The flush runs once
    /// full_history_ts_low has passed the memtables' newest timestamps. REQUIRES: mutex.
    /// </summary>
    private void RescheduleFlushToRetainUdt(ColumnFamilyData cfd)
    {
        // Release the flush slot now; the delayed retry stays a background job (shutdown waits for it).
        cfd.FlushRunning = false;
        _runningFlushes--;
        _ = RetryAsync(cfd);

        async Task RetryAsync(ColumnFamilyData c)
        {
            try
            {
                TestHooks.Invoke("DBImpl::AfterRetainUDTReschedule:cb");
                await Task.Delay(100).ConfigureAwait(false);
            }
            finally
            {
                lock (_mutex)
                {
                    _backgroundJobs--;
                    EnqueueFlush(c);
                    MaybeScheduleFlush();
                    SignalBackgroundWork();
                }
            }
        }
    }

    /// <summary>
    /// FlushJob::GetEffectiveCutoffUDTForPickedMemTables + MaybeIncreaseFullHistoryTsLowToAboveCutoffUDT: the newest
    /// timestamp of the flushed memtables plus one (GetFullHistoryTsLowFromU64CutoffTs) when that raises the column
    /// family's full_history_ts_low, else null.
    /// </summary>
    private static byte[]? GetFullHistoryTsLowAboveCutoffUdt(ColumnFamilyData cfd, List<MemTable> mems)
    {
        if (!cfd.TimestampsInMemtableOnly) return null;
        Comparator ucmp = cfd.UserComparator;
        byte[]? cutoff = null;
        foreach (MemTable m in mems)
        {
            byte[]? newest = m.GetNewestUdt();
            if (newest is null) continue;
            if (cutoff is null || ucmp.CompareTimestamp(newest, cutoff) > 0) cutoff = newest;
        }
        if (cutoff is null || cutoff.Length != sizeof(ulong)) return null;
        byte[]? low = cfd.FullHistoryTsLow;
        if (low is not null && ucmp.CompareTimestamp(cutoff, low) < 0) return null;
        byte[] next = new byte[sizeof(ulong)];
        Util.Coding.EncodeFixed64(next, Util.Coding.DecodeFixed64(cutoff) + 1);
        return next;
    }

    /// <summary>Test helper (TEST_WaitForBackgroundWork): waits until no background job (flush and its purge, compaction) runs.</summary>
    internal void WaitForBackgroundWorkForTest() => WaitForBackgroundWorkAsync(synchronous: true).GetAwaiter().GetResult();

    /// <summary>Waits until no background job is running (used at shutdown).</summary>
    private async ValueTask WaitForBackgroundWorkAsync(bool synchronous)
    {
        while (true)
        {
            Task signal;
            lock (_mutex)
            {
                if (_backgroundJobs == 0) return;
                signal = _bgSignal.Task;
            }
            if (synchronous) signal.Wait();
            else await signal.ConfigureAwait(false);
        }
    }

    // ---- blob files (shared by flush, recovery flush and compaction) -----------------------------------

    /// <summary>
    /// A blob file builder for output to <paramref name="outputLevel"/> when the column family writes blob files there
    /// (enable_blob_files and output level &gt;= blob_file_starting_level), else null. Its file numbers are registered as
    /// pending outputs until <see cref="ReleaseBlobOutputs"/>.
    /// </summary>
    internal Blob.BlobFileBuilder? CreateBlobFileBuilder(ColumnFamilyData cfd, int outputLevel, Blob.BlobFileCreationReason reason)
    {
        ColumnFamilyOptions o = cfd.Options;
        if (!o.EnableBlobFiles || outputLevel < o.BlobFileStartingLevel) return null;
        // Blob files of flushes are rate limited like their table files (IO_HIGH), those of compactions at IO_LOW.
        Env.IoPriority priority = reason == Blob.BlobFileCreationReason.Compaction ? Env.IoPriority.Low : Env.IoPriority.High;
        return new Blob.BlobFileBuilder(FileSystem, DbPaths[0], NewPendingOutputFileNumber, o, cfd.Id, reason, cfd.BlobSource, Stats,
            BlobFileWriteOptions(o, priority));
    }

    /// <summary>
    /// Write options of table files written by flushes (<see cref="Env.IoPriority.High"/>) and compactions
    /// (<see cref="Env.IoPriority.Low"/>): bytes_per_sync, writable_file_max_buffer_size and the DB's rate limiter
    /// (WritableFileWriter charges DBOptions::rate_limiter at the job's I/O priority).
    /// </summary>
    internal Env.WritableFileOptions TableFileWriteOptions(Env.IoPriority priority) => new()
    {
        BytesPerSync = (long)Math.Min(Options.BytesPerSync, long.MaxValue),
        MaxBufferSize = (int)Math.Clamp(Options.WritableFileMaxBufferSize, 64 * 1024, int.MaxValue),
        RateLimiter = Options.RateLimiter,
        IoPriority = priority,
        Statistics = Options.Statistics,
        FileChecksumGenFactory = Options.FileChecksumGenFactory,
    };

    /// <summary>
    /// Write options of blob files: bytes_per_sync and the max buffer size (blob_file_writable_file_max_buffer_size when
    /// set, else writable_file_max_buffer_size).
    /// </summary>
    internal Env.WritableFileOptions BlobFileWriteOptions(ColumnFamilyOptions o) => BlobFileWriteOptions(o, Env.IoPriority.Total);

    /// <summary><see cref="BlobFileWriteOptions(ColumnFamilyOptions)"/> charged to the rate limiter at <paramref name="priority"/>.</summary>
    internal Env.WritableFileOptions BlobFileWriteOptions(ColumnFamilyOptions o, Env.IoPriority priority) => new()
    {
        RateLimiter = priority == Env.IoPriority.Total ? null : Options.RateLimiter,
        IoPriority = priority,
        Statistics = Options.Statistics,
        FileChecksumGenFactory = Options.FileChecksumGenFactory,
        BytesPerSync = (long)Math.Min(Options.BytesPerSync, long.MaxValue),
        MaxBufferSize = (int)Math.Clamp(o.BlobFileWritableFileMaxBufferSize != 0 ? o.BlobFileWritableFileMaxBufferSize : Options.WritableFileMaxBufferSize,
            64 * 1024, int.MaxValue),
    };

    /// <summary>Total size of the blob files a builder finished (bytes_written_blob).</summary>
    internal static ulong BlobBytesWritten(Blob.BlobFileBuilder? builder)
    {
        ulong n = 0;
        if (builder is null) return 0;
        foreach (BlobFileAddition a in builder.Additions) n += a.TotalBlobBytes + Blob.BlobLogFormat.HeaderSize + Blob.BlobLogFormat.FooterSize;
        return n;
    }

    /// <summary>
    /// Unregisters the builder's pending blob outputs; deletes its files unless the job's version edit was committed
    /// (<see cref="Blob.BlobFileBuilder.Committed"/>).
    /// </summary>
    internal void ReleaseBlobOutputs(Blob.BlobFileBuilder builder)
    {
        if (!builder.Committed) builder.DeleteCreatedFiles();
        lock (_mutex)
        {
            foreach (ulong n in builder.CreatedFileNumbers) _pendingOutputs.Remove(n);
        }
    }
}
