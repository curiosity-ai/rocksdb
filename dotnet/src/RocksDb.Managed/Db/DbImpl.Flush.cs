namespace RocksDb.Managed.Db;

internal sealed partial class DbImpl
{
    private readonly Queue<ColumnFamilyData> _flushQueue = new(); // guarded by mutex
    private int _maxBackgroundFlushes; // changed by SetDBOptions under the mutex
    private int _runningFlushes;   // guarded by mutex
    private int _backgroundJobs;   // guarded by mutex
    private bool _shuttingDown;    // guarded by mutex
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
        int attempts = _flushQueue.Count;
        while (_runningFlushes < _maxBackgroundFlushes && attempts-- > 0 && _flushQueue.TryDequeue(out ColumnFamilyData? cfd))
        {
            cfd.QueuedForFlush = false;
            if (cfd.Dropped || !cfd.HasPendingFlush) continue;
            if (cfd.FlushRunning) continue; // re-enqueued when the running flush finishes
            cfd.FlushRunning = true;
            _runningFlushes++;
            _backgroundJobs++;
            _ = Task.Run(() => BackgroundFlushAsync(cfd));
        }
    }

    /// <summary>
    /// Port of BackgroundFlush + FlushMemTableToOutputFile + MemTableList::TryInstallMemtableFlushResults: flushes all
    /// immutable memtables of the CF that are not being flushed into one L0 file, records it in the MANIFEST together
    /// with the CF's new log number, then drops the memtables and deletes obsolete WALs.
    /// </summary>
    private Task BackgroundFlushAsync(ColumnFamilyData cfd) => BackgroundFlushAsync(cfd, null);

    /// <summary>
    /// <see cref="BackgroundFlushAsync(ColumnFamilyData)"/> as a member of <paramref name="group"/> (an atomic flush:
    /// the flush result is committed to the MANIFEST together with the other members' results), or alone when null.
    /// </summary>
    private async Task BackgroundFlushAsync(ColumnFamilyData cfd, AtomicFlushGroup? group)
    {
        Exception? failure = null;
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
            await TrackClosedWalsAsync().ConfigureAwait(false);
            if (picked.Count > 0) await FlushMemTablesAsync(cfd, picked, fileNumber, epoch, synchronous: false, group).ConfigureAwait(false);
        }
        catch (RocksDbException e) when (e.Code == StatusCode.ColumnFamilyDropped || cfd.Dropped)
        {
            failure = e;
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
            // referenced by a MANIFEST that CURRENT names (upstream quarantines such files).
            // FlushMemTableToOutputFile: the WAL is synced at memtable switch, so a failed MANIFEST write maps to
            // kManifestWriteNoWAL and any other failure (table file write) to kFlushNoWAL.
            SetBackgroundError(e, Versions.IoStatus is not null ? BackgroundErrorReason.ManifestWriteNoWal : BackgroundErrorReason.FlushNoWal,
                walRelated: false);
        }
        finally
        {
            // A member that failed or had nothing to flush leaves the atomic flush (a failure fails the whole group).
            group?.Leave(cfd, failure);
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
                // The flush itself is done: release FlushAsync waiters and let the next flush of this CF start before
                // the (slow) obsolete-file purge, like upstream's bg_cv_ signal after the install.
                cfd.FlushRunning = false;
                _runningFlushes--;
                EnqueueFlush(cfd);
                MaybeScheduleFlush();
                SignalBackgroundWork();
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
        OnFlushStarted(cfd, jobId, mems);
        string path = FileNames.TableFileName(DbPaths, fileNumber, 0);
        FlushReason reason = picked[^1].FlushReason;
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
        lock (_mutex)
        {
            snapshots = SnapshotsForCompaction();
            seqnoToTime = CopySeqnoToTimeMapping(cfd);
        }
        // PrepareFlushAdditions: the blob direct write files of the flushed memtables are sealed and registered with the
        // table file; their blobs the output does not reference (hidden versions, failed writes) are garbage.
        (List<BlobFileAddition> Additions, Blob.BlobGarbageMeter Meter)? directWrite = PrepareBlobDirectWriteFlush(cfd, picked);
        FileMetaData? meta = await FlushJob.WriteLevel0TableAsync(FileSystem, path, TableFileWriteOptions(Env.IoPriority.High), cfd, mems, fileNumber, epoch, Identity, snapshots, CurrentSnapshotChecker, blobBuilder,
                directWrite?.Meter, seqnoToTime, synchronous, CancellationToken.None)
            .ConfigureAwait(false);
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
            RecordFlushBytes(meta.FileSize + BlobBytesWritten(blobBuilder));
            // FlushJob::WriteLevel0Table logs this before the result is installed (and compactions are scheduled).
            LogInfo("[" + cfd.Name + "] [JOB " + jobId + "] Level-0 flush table #" + meta.FileNumber + ": " + meta.FileSize + " bytes OK");
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
                if (Versions.ColumnFamilies.Count > 1 && group is null) edit.MinLogNumberToKeep = minWalToKeep;
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
        OnFlushCompleted(cfd, jobId, meta, BlobBytesWritten(blobBuilder), flushStart);
        NotifyOnFlushCompleted(cfd, jobId, meta, path, mems, reason);
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
        ImmutableMemTable? target = null;
        try
        {
            lock (_mutex)
            {
                if (IsBgWorkStopped && ErrorHandler.BgError is { } bg) ThrowBackgroundError(bg);
            }
            if (cfd.Dropped) ThrowDropped(cfd);
            if (!cfd.Mem.IsEmpty)
            {
                await SwitchMemtableAsync(cfd, reason, synchronous, cancellationToken).ConfigureAwait(false);
            }
            lock (_mutex)
            {
                if (cfd.Imm.Count > 0) target = cfd.Imm[^1];
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
        if (!wait || target is null) return;
        while (true)
        {
            Task signal;
            lock (_mutex)
            {
                if (target.FlushCompleted || !cfd.Imm.Contains(target))
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
