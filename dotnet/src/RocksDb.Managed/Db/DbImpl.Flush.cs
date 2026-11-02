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
        if (_shuttingDown || _bgError is not null) return;
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
    private async Task BackgroundFlushAsync(ColumnFamilyData cfd)
    {
        var picked = new List<ImmutableMemTable>();
        ulong fileNumber;
        ulong epoch;
        lock (_mutex)
        {
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
            if (picked.Count > 0) await FlushMemTablesAsync(cfd, picked, fileNumber, epoch, synchronous: false).ConfigureAwait(false);
        }
        catch (RocksDbException e) when (e.Code == StatusCode.ColumnFamilyDropped || cfd.Dropped)
        {
            lock (_mutex)
            {
                foreach (ImmutableMemTable m in picked) m.FlushInProgress = false;
            }
            TryDeleteFile(FileNames.TableFileName(DbPaths, fileNumber, 0));
        }
        catch (Exception e)
        {
            lock (_mutex)
            {
                foreach (ImmutableMemTable m in picked) m.FlushInProgress = false;
            }
            SetBackgroundError(e);
        }
        finally
        {
            lock (_mutex)
            {
                foreach (ImmutableMemTable m in picked)
                {
                    if (m.Mem.Unref()) m.Mem.Dispose();
                }
                _pendingOutputs.Remove(fileNumber);
                cfd.FlushRunning = false;
                _runningFlushes--;
                _backgroundJobs--;
                EnqueueFlush(cfd);
                MaybeScheduleFlush();
                SignalBackgroundWork();
            }
        }
        try
        {
            PurgeObsoleteFiles();
        }
        catch (Exception e) when (e is IOException or RocksDbException)
        {
            // Deleting obsolete files is best effort.
        }
    }

    /// <summary>Builds the L0 file for <paramref name="picked"/> and installs the result (FlushJob::Run + install).</summary>
    private async ValueTask FlushMemTablesAsync(ColumnFamilyData cfd, List<ImmutableMemTable> picked, ulong fileNumber, ulong epoch, bool synchronous)
    {
        TestHooks.Invoke("DBImpl::FlushMemTables:Start");
        long flushStart = System.Diagnostics.Stopwatch.GetTimestamp();
        int jobId = NewJobId();
        var mems = new List<MemTable>(picked.Count);
        foreach (ImmutableMemTable m in picked) mems.Add(m.Mem);
        OnFlushStarted(cfd, jobId, mems);
        string path = FileNames.TableFileName(DbPaths, fileNumber, 0);
        FileMetaData? meta = await FlushJob.WriteLevel0TableAsync(FileSystem, path, cfd, mems, fileNumber, epoch, Identity, synchronous, CancellationToken.None)
            .ConfigureAwait(false);
        if (meta is not null)
        {
            FileSystem.SyncDirectory(DbPaths[0]);
            RecordFlushBytes(meta.FileSize);
        }

        var edit = new VersionEdit
        {
            ColumnFamily = cfd.Id,
            PrevLogNumber = 0,
            // The CF's data in WALs below the next log number of the newest flushed memtable is now persisted.
            LogNumber = picked[^1].NextLogNumber,
        };
        if (meta is not null) edit.AddFile(0, meta);
        lock (_mutex)
        {
            if (cfd.Dropped) ThrowDropped(cfd);
            if (Versions.ColumnFamilies.Count > 1)
            {
                // PrecomputeMinLogNumberToKeepNon2PC: minimum of the other CFs' log numbers and this edit's.
                edit.MinLogNumberToKeep = Math.Min(Versions.MinLogNumberWithUnflushedData(cfd), edit.LogNumber.Value);
            }
        }
        TestHooks.Invoke("DBImpl::FlushMemTables:BeforeLogAndApply");
        await Versions.LogAndApplyAsync(cfd, edit, synchronous, CancellationToken.None).ConfigureAwait(false);

        lock (_mutex)
        {
            foreach (ImmutableMemTable m in picked)
            {
                m.FlushCompleted = true;
                // A drop that committed right after our MANIFEST write already released the list's references.
                if (cfd.Imm.Remove(m) && m.Mem.Unref()) m.Mem.Dispose();
            }
            if (cfd.Dropped) return;
            InstallSuperVersion(cfd);
            _maybeStalled = IsWriteStalled();
        }
        OnFlushCompleted(cfd, jobId, meta, flushStart);
    }

    /// <summary>
    /// Manual flush (DBImpl::FlushMemTable): switches the CF's memtable if it holds data, schedules the flush and, with
    /// <paramref name="wait"/>, waits until every memtable that existed at the call has been flushed.
    /// </summary>
    internal async ValueTask FlushColumnFamilyAsync(ColumnFamilyData cfd, bool wait, bool synchronous, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        if (_readOnly) ThrowReadOnly();
        WriteThread.Writer w = await EnterUnbatchedAsync(synchronous, cancellationToken).ConfigureAwait(false);
        ImmutableMemTable? target = null;
        try
        {
            RocksDbException? bg = BackgroundError;
            if (bg is not null) ThrowBackgroundError(bg);
            if (cfd.Dropped) ThrowDropped(cfd);
            if (!cfd.Mem.IsEmpty)
            {
                await SwitchMemtableAsync(cfd, synchronous, cancellationToken).ConfigureAwait(false);
            }
            lock (_mutex)
            {
                if (cfd.Imm.Count > 0) target = cfd.Imm[^1];
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
                if (target.FlushCompleted || !cfd.Imm.Contains(target)) return;
                if (_bgError is { } e) ThrowBackgroundError(e);
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
}
