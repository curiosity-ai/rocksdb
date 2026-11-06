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

    private FlushJobInfo NewFlushJobInfo(ColumnFamilyData cfd, int jobId, ulong fileNumber, string path, IReadOnlyList<MemTable> mems, FlushReason reason,
        FileMetaData? meta, TableProperties? props)
    {
        ulong smallest = ulong.MaxValue, largest = 0;
        if (meta is not null)
        {
            smallest = meta.SmallestSeqno;
            largest = meta.LargestSeqno;
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
            WriteStallCondition c = cfd.CompactionState.Stall.Condition;
            slowdown = c == WriteStallCondition.Delayed;
            stop = c == WriteStallCondition.Stopped;
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
        FlushJobInfo info = NewFlushJobInfo(cfd, jobId, fileNumber, path, mems, reason, null, null);
        InvokeListeners(info, static (l, db, i) => l.OnFlushBegin(db, i), nameof(EventListener.OnFlushBegin));
    }

    /// <summary>
    /// DBImpl::NotifyOnFlushCompleted, preceded by the table file creation event of the flush output. Called without
    /// the mutex, after the flush result was installed.
    /// </summary>
    internal void NotifyOnFlushCompleted(ColumnFamilyData cfd, int jobId, FileMetaData? meta, string path, IReadOnlyList<MemTable> mems, FlushReason reason)
    {
        if (!HasListeners) return;
        TableProperties? props = meta is null ? null : TryGetTableProperties(cfd, meta);
        if (meta is not null)
        {
            NotifyOnTableFileCreated(cfd, jobId, path, meta.FileSize, TableFileCreationReason.Flush, null, props);
        }
        FlushJobInfo info = NewFlushJobInfo(cfd, jobId, meta?.FileNumber ?? 0, meta is null ? "" : path, mems, reason, meta, props);
        InvokeListeners(info, static (l, db, i) => l.OnFlushCompleted(db, i), nameof(EventListener.OnFlushCompleted));
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
        if (!HasListeners) return;
        var info = new TableFileCreationInfo
        {
            DbName = DbName,
            ColumnFamilyName = cfd.Name,
            FilePath = path,
            FileSize = failure is null ? size : 0,
            JobId = jobId,
            Reason = reason,
            Status = failure is null ? null : RocksDbException.FromException(failure),
            TableProperties = props,
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

    /// <summary>DBImpl::NotifyOnCompactionBegin. Called without the mutex.</summary>
    private void NotifyOnCompactionBegin(ColumnFamilyData cfd, Compaction c, int jobId)
    {
        if (!HasListeners) return;
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
    /// DBImpl::NotifyOnCompactionCompleted, preceded by the table file creation events of the outputs. Called without
    /// the mutex, after the result was installed (or the compaction failed).
    /// </summary>
    private void NotifyOnCompactionCompleted(ColumnFamilyData cfd, Compaction c, CompactionJob? job, int jobId, Exception? failure)
    {
        if (!HasListeners) return;
        if (job is not null && failure is null)
        {
            foreach (FileMetaData f in job.AllOutputs)
            {
                NotifyOnTableFileCreated(cfd, jobId, FileNames.TableFileName(DbPaths, f.FileNumber, f.PathId), f.FileSize,
                    TableFileCreationReason.Compaction, null, TryGetTableProperties(cfd, f));
            }
        }
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
