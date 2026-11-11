using System.Collections.Concurrent;
using RocksDb.Managed.Table;

namespace RocksDb.Managed.Db;

/// <summary>
/// Listener notifications of the DB: ports of DBImpl::NotifyOnFlushBegin / NotifyOnFlushCompleted /
/// NotifyOnCompactionBegin / NotifyOnCompactionCompleted / NotifyOnMemTableSealed / NotifyOnExternalFileIngested,
/// EventHelpers::NotifyTableFileCreationStarted / LogAndNotifyTableFileCreationFinished / LogAndNotifyTableFileDeletion
/// and the write stall notifications of SuperVersionContext (InstallSuperVersionAndScheduleWork).
/// <para>
/// Flush, compaction, table file and ingestion events are delivered synchronously on the background job's thread
/// without the DB mutex (as upstream). Write stall and memtable-sealed events are raised under the mutex (by writers and
/// super version installs), so they are queued and delivered in order by a notification work item, keeping the write
/// path free of user callbacks. Everything here costs a null/count check when no listener is registered.
/// </para>
/// </summary>
internal sealed partial class DbImpl
{
    private readonly ConcurrentQueue<object> _pendingNotifications = new();
    private int _notificationsDraining;

    /// <summary>The public DB object, set once open completed (listeners receive it); null during open.</summary>
    internal RocksDatabase? Owner { get; set; }

    internal bool HasListeners => Options.Listeners.Count > 0;

    private void InvokeListeners<TArg>(TArg arg, Action<EventListener, RocksDatabase?, TArg> callback, string name)
    {
        RocksDatabase? owner = Owner;
        foreach (EventListener l in Options.Listeners)
        {
            try
            {
                callback(l, owner, arg);
            }
            catch (Exception e) when (e is not OutOfMemoryException)
            {
                LogWarn("EventListener." + name + " threw: " + e.Message);
            }
        }
    }

    // ---- flush ------------------------------------------------------------------------------------------

    private FlushJobInfo? NewFlushJobInfo(ColumnFamilyData cfd, int jobId, ulong fileNumber, string path, IReadOnlyList<MemTable> mems, FlushReason reason,
        FileMetaData? meta, TableProperties? props, bool completed)
    {
        ulong smallest = ulong.MaxValue, largest = 0;
        if (meta is not null)
        {
            smallest = meta.SmallestSeqno;
            largest = meta.LargestSeqno;
        }
        else if (completed)
        {
            // FlushJob::GetFlushJobInfo for a flush without an output file: meta_.fd keeps the allocated file number and
            // the bounds of an empty FileDescriptor (smallest_seqno = kMaxSequenceNumber, largest_seqno = 0).
            smallest = DbFormat.MaxSequenceNumber;
        }
        else
        {
            // Before the flush: the memtables' bounds (the largest is the newest memtable's last write or less).
            foreach (MemTable m in mems)
            {
                if (m.EarliestSequenceNumber < smallest) smallest = m.EarliestSequenceNumber;
            }
            lock (_mutex) largest = Versions.LastSequence;
        }
        bool slowdown, stop;
        lock (_mutex)
        {
            // NotifyOnFlushBegin / NotifyOnFlushCompleted: "if (shutting_down_.load()) return".
            if (_shuttingDown) return null;
            // NotifyOnFlushBegin / NotifyOnFlushCompleted: the current version's L0 file count against the triggers
            // (not the column family's write stall condition, which also has other causes and ignores L0 while auto
            // compactions are disabled).
            int l0 = cfd.Current.Storage.NumLevelFiles(0);
            ColumnFamilyOptions o = cfd.Options;
            slowdown = l0 >= o.Level0SlowdownWritesTrigger;
            stop = l0 >= o.Level0StopWritesTrigger;
        }
        return new FlushJobInfo
        {
            ColumnFamilyId = cfd.Id,
            ColumnFamilyName = cfd.Name,
            FilePath = path,
            FileNumber = fileNumber,
            ThreadId = Environment.CurrentManagedThreadId,
            JobId = jobId,
            TriggeredWritesSlowdown = slowdown,
            TriggeredWritesStop = stop,
            SmallestSeqno = smallest == ulong.MaxValue ? 0 : smallest,
            LargestSeqno = largest,
            TableProperties = props,
            FlushReason = reason,
        };
    }

    /// <summary>DBImpl::NotifyOnFlushBegin. Called without the mutex.</summary>
    internal void NotifyOnFlushBegin(ColumnFamilyData cfd, int jobId, ulong fileNumber, string path, IReadOnlyList<MemTable> mems, FlushReason reason)
    {
        if (!HasListeners) return;
        FlushJobInfo? info = NewFlushJobInfo(cfd, jobId, fileNumber, path, mems, reason, null, null, completed: false);
        if (info is null) return;
        InvokeListeners(info, static (l, db, i) => l.OnFlushBegin(db, i), nameof(EventListener.OnFlushBegin));
    }

    /// <summary>
    /// DBImpl::NotifyOnFlushCompleted with the FlushJobInfo of FlushJob::GetFlushJobInfo: the file number and path are
    /// the ones allocated for the flush, also when the flush kept no file. Called without the mutex, after the flush
    /// result was installed.
    /// </summary>
    internal void NotifyOnFlushCompleted(ColumnFamilyData cfd, int jobId, ulong fileNumber, FileMetaData? meta, string path, IReadOnlyList<MemTable> mems,
        FlushReason reason)
    {
        if (!HasListeners) return;
        lock (_mutex)
        {
            if (_shuttingDown) return;
        }
        TableProperties? props = meta is null ? null : TryGetTableProperties(cfd, meta);
        FlushJobInfo? info = NewFlushJobInfo(cfd, jobId, fileNumber, path, mems, reason, meta, props, completed: true);
        if (info is null) return;
        InvokeListeners(info, static (l, db, i) => l.OnFlushCompleted(db, i), nameof(EventListener.OnFlushCompleted));
    }

    /// <summary>
    /// BuildTable / CompactionJob::FinishCompactionOutputFile for an output that kept no file: the creation event names
    /// "(nil)" with Status::Aborted("Empty SST file not kept") and the file number allocated for the output.
    /// </summary>
    internal void NotifyOnEmptyTableFile(ColumnFamilyData cfd, int jobId, ulong fileNumber, TableFileCreationReason reason)
    {
        // db/builder.cc:590-602: the path is "(nil)" but the event is logged with meta->fd, whose number is the one
        // allocated for the output.
        NotifyOnTableFileNotKept(cfd, jobId, "(nil)", fileNumber, reason, new RocksDbException(StatusCode.Aborted, "Operation aborted: Empty SST file not kept"));
    }

    private static TableProperties? TryGetTableProperties(ColumnFamilyData cfd, FileMetaData meta)
    {
        try
        {
            using TableReaderLease lease = cfd.TableCache.GetReader(meta);
            return lease.Reader.Properties;
        }
        catch (Exception e) when (e is IOException or RocksDbException or ObjectDisposedException)
        {
            return null;
        }
    }

    // ---- table files ------------------------------------------------------------------------------------

    /// <summary>EventHelpers::LogAndNotifyTableFileCreationFinished. Called without the mutex.</summary>
    internal void NotifyOnTableFileCreated(ColumnFamilyData cfd, int jobId, string path, ulong size, TableFileCreationReason reason,
        Exception? failure, TableProperties? props)
    {
        if (failure is not null)
        {
            // The file number is the one in the path ("(nil)" for an empty output, whose number the callers do not pass).
            ulong fileNumber = 0;
            if (FileNames.TryParseFileName(Path.GetFileName(path), out ulong parsed, out FileType type) && type == FileType.TableFile) fileNumber = parsed;
            NotifyOnTableFileNotKept(cfd, jobId, path, fileNumber, reason, failure);
            return;
        }
        if (!HasListeners) return;
        var info = new TableFileCreationInfo
        {
            DbName = DbName,
            ColumnFamilyName = cfd.Name,
            FilePath = path,
            FileSize = size,
            JobId = jobId,
            Reason = reason,
            TableProperties = props,
        };
        InvokeListeners(info, static (l, _, i) => l.OnTableFileCreated(i), nameof(EventListener.OnTableFileCreated));
    }

    /// <summary>
    /// EventHelpers::LogAndNotifyTableFileCreationFinished for a table that was not kept (a failed or an empty output):
    /// the EVENT_LOG line carries the failure (the events of kept tables are written by OnFlushCompleted /
    /// OnCompactionCompleted), then the listeners are told. <paramref name="fileNumber"/>: the number of the file
    /// descriptor upstream logs (0 for an output that never got one). Called without the mutex.
    /// </summary>
    internal void NotifyOnTableFileNotKept(ColumnFamilyData cfd, int jobId, string path, ulong fileNumber, TableFileCreationReason reason, Exception failure)
    {
        RocksDbException status = RocksDbException.FromException(failure);
        LogTableFileCreationFailedEvent(cfd, jobId, fileNumber, status.Message);
        if (!HasListeners) return;
        var info = new TableFileCreationInfo
        {
            DbName = DbName,
            ColumnFamilyName = cfd.Name,
            FilePath = path,
            FileSize = 0,
            JobId = jobId,
            Reason = reason,
            Status = status,
        };
        InvokeListeners(info, static (l, _, i) => l.OnTableFileCreated(i), nameof(EventListener.OnTableFileCreated));
    }

    /// <summary>EventHelpers::LogAndNotifyTableFileDeletion. Called without the mutex.</summary>
    private void NotifyOnTableFileDeleted(string path, int jobId, Exception? failure)
    {
        if (!HasListeners) return;
        var info = new TableFileDeletionInfo
        {
            DbName = DbName,
            FilePath = path,
            JobId = jobId,
            Status = failure is null ? null : RocksDbException.FromException(failure),
        };
        InvokeListeners(info, static (l, _, i) => l.OnTableFileDeleted(i), nameof(EventListener.OnTableFileDeleted));
    }

    /// <summary>Deletes an obsolete table file and reports it to the listeners (DeleteObsoleteFileImpl).</summary>
    private void DeleteObsoleteTableFile(string path, ulong fileNumber)
    {
        // DeleteDBFile: through the SstFileManager (trash + rate limit) when there is one.
        Exception? failure = DeleteDbFile(path, Path.GetDirectoryName(path) ?? DbName);
        LogFileDeletionEvent("table_file_deletion", 0, fileNumber, failure);
        if (HasListeners) NotifyOnTableFileDeleted(path, 0, failure);
    }

    // ---- compaction -------------------------------------------------------------------------------------

    private CompactionJobInfo NewCompactionJobInfo(ColumnFamilyData cfd, Compaction c, CompactionJob? job, int jobId, Exception? failure)
    {
        var inputs = new List<string>();
        ulong inputBytes = 0;
        int baseLevel = int.MaxValue;
        for (int i = 0; i < c.NumInputLevels; i++)
        {
            if (c.Inputs(i).Files.Count > 0) baseLevel = Math.Min(baseLevel, c.Level(i));
            foreach (FileMetaData f in c.Inputs(i).Files)
            {
                inputs.Add(FileNames.TableFileName(DbPaths, f.FileNumber, f.PathId));
                inputBytes += f.FileSize;
            }
        }
        var outputs = new List<string>();
        ulong outputBytes = 0;
        if (job is not null && failure is null)
        {
            foreach (FileMetaData f in job.AllOutputs)
            {
                outputs.Add(FileNames.TableFileName(DbPaths, f.FileNumber, f.PathId));
                outputBytes += f.FileSize;
            }
        }
        else if (failure is null && !c.IsDeletionCompaction)
        {
            // Trivial move: the inputs are the outputs.
            for (int i = 0; i < c.NumInputLevels; i++)
            {
                if (c.Level(i) == c.OutputLevel) continue;
                foreach (FileMetaData f in c.Inputs(i).Files) outputs.Add(FileNames.TableFileName(DbPaths, f.FileNumber, f.PathId));
            }
        }
        return new CompactionJobInfo
        {
            ColumnFamilyId = cfd.Id,
            ColumnFamilyName = cfd.Name,
            Status = failure is null ? null : RocksDbException.FromException(failure),
            ThreadId = Environment.CurrentManagedThreadId,
            JobId = jobId,
            BaseInputLevel = baseLevel == int.MaxValue ? c.StartLevel : baseLevel,
            OutputLevel = c.OutputLevel,
            InputFiles = inputs,
            OutputFiles = outputs,
            CompactionReason = c.Reason,
            TotalInputBytes = inputBytes,
            TotalOutputBytes = outputBytes,
            NumInputRecords = job?.Stats.NumInputRecords ?? 0,
            NumOutputRecords = job?.Stats.NumOutputRecords ?? 0,
            IsTrivialMove = job is null && !c.IsDeletionCompaction,
        };
    }

    /// <summary>
    /// DBImpl::NotifyOnCompactionBegin. Called without the mutex. Skipped while shutting down and, for a manual
    /// compaction, while manual compactions are paused; OnCompactionCompleted is only raised for a compaction whose
    /// begin was (Compaction::SetNotifyOnCompactionCompleted).
    /// </summary>
    private void NotifyOnCompactionBegin(ColumnFamilyData cfd, Compaction c, int jobId)
    {
        if (!HasListeners) return;
        lock (_mutex)
        {
            if (_shuttingDown) return;
            if (c.IsManual && _manualCompactionPaused > 0) return;
        }
        c.NotifyOnCompactionCompleted = true;
        CompactionJobInfo info = WithoutOutputs(NewCompactionJobInfo(cfd, c, null, jobId, null));
        InvokeListeners(info, static (l, db, i) => l.OnCompactionBegin(db, i), nameof(EventListener.OnCompactionBegin));
    }

    private static CompactionJobInfo WithoutOutputs(CompactionJobInfo i) => new()
    {
        ColumnFamilyId = i.ColumnFamilyId,
        ColumnFamilyName = i.ColumnFamilyName,
        ThreadId = i.ThreadId,
        JobId = i.JobId,
        BaseInputLevel = i.BaseInputLevel,
        OutputLevel = i.OutputLevel,
        InputFiles = i.InputFiles,
        OutputFiles = [],
        CompactionReason = i.CompactionReason,
        TotalInputBytes = i.TotalInputBytes,
        IsTrivialMove = i.IsTrivialMove,
    };

    /// <summary>
    /// The table file creation events of a finished compaction job's outputs (CompactionJob::FinishCompactionOutputFile
    /// -> LogAndNotifyTableFileCreationFinished). Called without the mutex, before the result is installed.
    /// </summary>
    private void NotifyOnCompactionOutputsCreated(ColumnFamilyData cfd, CompactionJob job, int jobId)
    {
        if (!HasListeners) return;
        foreach (FileMetaData f in job.AllOutputs)
        {
            NotifyOnTableFileCreated(cfd, jobId, FileNames.TableFileName(DbPaths, f.FileNumber, f.PathId), f.FileSize,
                TableFileCreationReason.Compaction, null, TryGetTableProperties(cfd, f));
        }
    }

    /// <summary>
    /// DBImpl::NotifyOnCompactionCompleted. Called without the mutex, after the result was installed (or the compaction
    /// failed). Skipped while shutting down and when OnCompactionBegin was not raised for the compaction.
    /// </summary>
    private void NotifyOnCompactionCompleted(ColumnFamilyData cfd, Compaction c, CompactionJob? job, int jobId, Exception? failure)
    {
        if (!HasListeners) return;
        lock (_mutex)
        {
            if (_shuttingDown) return;
        }
        if (!c.NotifyOnCompactionCompleted) return;
        CompactionJobInfo info = NewCompactionJobInfo(cfd, c, job, jobId, failure);
        InvokeListeners(info, static (l, db, i) => l.OnCompactionCompleted(db, i), nameof(EventListener.OnCompactionCompleted));
    }

    // ---- ingestion / column families ---------------------------------------------------------------------

    /// <summary>DBImpl::NotifyOnExternalFileIngested. Called without the mutex.</summary>
    internal void NotifyOnExternalFileIngested(ColumnFamilyData cfd, ExternalSstFileIngestionJob job)
    {
        if (!HasListeners) return;
        foreach (IngestedFileInfo f in job.Files)
        {
            var meta = new FileMetaData { FileNumber = f.FileNumber, FileSize = f.FileSize };
            var info = new ExternalFileIngestionInfo
            {
                ColumnFamilyName = cfd.Name,
                ExternalFilePath = f.ExternalFilePath,
                InternalFilePath = f.InternalFilePath,
                GlobalSeqno = f.AssignedSeqno,
                TableProperties = TryGetTableProperties(cfd, meta),
            };
            InvokeListeners(info, static (l, db, i) => l.OnExternalFileIngested(db, i), nameof(EventListener.OnExternalFileIngested));
        }
    }

    /// <summary>EventListener::OnColumnFamilyHandleDeletionStarted for <paramref name="handle"/>.</summary>
    internal void NotifyOnColumnFamilyHandleDeletionStarted(ColumnFamily handle)
    {
        if (!HasListeners) return;
        InvokeListeners(handle, static (l, _, h) => l.OnColumnFamilyHandleDeletionStarted(h), nameof(EventListener.OnColumnFamilyHandleDeletionStarted));
    }

    // ---- queued events (raised under the mutex) ----------------------------------------------------------

    /// <summary>Queues OnMemTableSealed for <paramref name="mem"/> (DBImpl::NotifyOnMemTableSealed). REQUIRES: mutex.</summary>
    private void EnqueueMemTableSealed(ColumnFamilyData cfd, MemTable mem)
    {
        // NotifyOnMemTableSealed: "if (shutting_down_.load(std::memory_order_acquire)) return".
        if (_shuttingDown) return;
        EnqueueNotification(new MemTableInfo
        {
            ColumnFamilyName = cfd.Name,
            FirstSeqno = mem.FirstSequenceNumber,
            EarliestSeqno = mem.EarliestSequenceNumber,
            NumEntries = (ulong)mem.NumEntries,
            NumDeletes = (ulong)mem.NumDeletes,
        });
    }

    /// <summary>Queues OnStallConditionsChanged when the condition changed (SuperVersionContext::PushWriteStallNotification).</summary>
    private void MaybeEnqueueStallNotification(ColumnFamilyData cfd, WriteStallCondition previous, WriteStallCondition current)
    {
        if (previous == current || !HasListeners) return;
        EnqueueNotification(new WriteStallInfo { ColumnFamilyName = cfd.Name, Previous = previous, Current = current });
    }

    private void EnqueueNotification(object info)
    {
        _pendingNotifications.Enqueue(info);
        if (Interlocked.CompareExchange(ref _notificationsDraining, 1, 0) == 0)
        {
            ThreadPool.UnsafeQueueUserWorkItem(static db => db.DrainNotifications(), this, preferLocal: false);
        }
    }

    private void DrainNotifications()
    {
        while (true)
        {
            while (_pendingNotifications.TryDequeue(out object? info))
            {
                switch (info)
                {
                    case WriteStallInfo w:
                        InvokeListeners(w, static (l, _, i) => l.OnStallConditionsChanged(i), nameof(EventListener.OnStallConditionsChanged));
                        break;
                    case MemTableInfo m:
                        InvokeListeners(m, static (l, _, i) => l.OnMemTableSealed(i), nameof(EventListener.OnMemTableSealed));
                        break;
                }
            }
            Volatile.Write(ref _notificationsDraining, 0);
            if (_pendingNotifications.IsEmpty || Interlocked.CompareExchange(ref _notificationsDraining, 1, 0) != 0) return;
        }
    }

    /// <summary>True while queued write stall / memtable notifications are not yet delivered (tests).</summary>
    internal bool HasPendingNotifications => !_pendingNotifications.IsEmpty || Volatile.Read(ref _notificationsDraining) != 0;
}
