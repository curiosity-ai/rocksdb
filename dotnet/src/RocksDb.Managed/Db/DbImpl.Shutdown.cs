namespace RocksDb.Managed.Db;

/// <summary>
/// Ports of DBImpl::CancelAllBackgroundWork and DBImpl::NotifyOnDBShutdownBegin (db/db_impl/db_impl.cc).
/// </summary>
internal sealed partial class DbImpl
{
    private bool _shutdownNotificationSent; // guarded by mutex

    /// <summary>
    /// DBImpl::NotifyOnDBShutdownBegin: EventListener.OnDBShutdownBegin, once per DB instance (CancelAllBackgroundWork,
    /// Close and the cleanup of a failed open all reach it), before the shutdown state is published. Called without the
    /// mutex (upstream releases it while notifying).
    /// </summary>
    private void NotifyOnDBShutdownBegin()
    {
        lock (_mutex)
        {
            if (_shutdownNotificationSent || !HasListeners) return;
            _shutdownNotificationSent = true;
        }
        InvokeListeners(0, static (l, db, _) => l.OnDBShutdownBegin(db), nameof(EventListener.OnDBShutdownBegin));
    }

    /// <summary>
    /// Port of DBImpl::CancelAllBackgroundWork: unless a shutdown already began, flushes the memtables holding data that
    /// is not in the WAL (or blob direct write files that only a flush registers; skipped with
    /// avoid_flush_during_shutdown), cancels awaited remote compactions, notifies OnDBShutdownBegin, then publishes the
    /// shutdown state: no new flush or compaction is scheduled and running compactions are canceled. With
    /// <paramref name="wait"/>, returns once the running background jobs finished. The DB stays open: reads and writes
    /// keep working (writes are no longer flushed), and Close completes the shutdown without flushing again.
    /// </summary>
    public async ValueTask CancelAllBackgroundWorkAsync(bool wait, bool synchronous)
    {
        if (IsDisposed) return;
        LogInfo("Shutdown: canceling all background work");
        bool alreadyShuttingDown;
        lock (_mutex) alreadyShuttingDown = _shuttingDown;
        if (!alreadyShuttingDown)
        {
            await ShutdownFlushAsync(synchronous).ConfigureAwait(false);
            NotifyOnDBShutdownBegin();
        }
        lock (_mutex)
        {
            _shuttingDown = true;
            SignalBackgroundWork(); // wake FlushAsync waiters whose queued flush will never be scheduled now
        }
        CancelBackgroundCompactions(); // incl. CompactionService.CancelAwaitingJobs
        if (!wait) return;
        await WaitForBackgroundWorkAsync(synchronous).ConfigureAwait(false);
    }

    /// <summary>
    /// The shutdown flush of CancelAllBackgroundWork: has_unpersisted_data_ and !avoid_flush_during_shutdown, or
    /// force_flush_for_blob_direct_write (blob direct write files are only registered by flushes; WAL recovery of
    /// unregistered files is out of scope, like upstream v1). Best effort: a failure is logged, not thrown.
    /// </summary>
    private async ValueTask ShutdownFlushAsync(bool synchronous)
    {
        bool flushForUnpersistedData = !Options.AvoidFlushDuringShutdown && Volatile.Read(ref _hasUnpersistedData);
        if ((!flushForUnpersistedData && !HasInFlightBlobDirectWriteFiles()) || BackgroundError is not null) return;
        try
        {
            foreach (ColumnFamilyData cfd in GetColumnFamilies())
            {
                await FlushColumnFamilyAsync(cfd, wait: true, FlushReason.ShutDown, synchronous, CancellationToken.None).ConfigureAwait(false);
            }
        }
        catch (RocksDbException e)
        {
            LogWarn("Shutdown flush failed: " + e.Message);
        }
    }

    private bool IsShuttingDown
    {
        get
        {
            lock (_mutex) return _shuttingDown;
        }
    }
}
