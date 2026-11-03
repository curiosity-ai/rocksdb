namespace RocksDb.Managed.Db;

/// <summary>
/// LockWAL / UnlockWAL (db/db_impl/db_impl.cc): freezes the logical state of the DB by stopping writes through the write
/// controller, and flushes the WAL buffer. Upstream counts the locks per thread id and requires UnlockWAL on the
/// locking thread; the managed port counts locks per <see cref="RocksDatabase.WalLock"/> handle instead (async code has
/// no thread affinity), so a lock may be released from any thread exactly once.
/// </summary>
internal sealed partial class DbImpl
{
    private int _lockWalCount; // outstanding LockWAL() calls (lock_wal_owner_thread_id_counts_ total). Guarded by mutex.

    /// <summary>True while at least one LockWAL is held. REQUIRES: mutex (or a racy read for diagnostics).</summary>
    internal bool IsWalLocked => Volatile.Read(ref _lockWalCount) > 0;

    /// <summary>DBImpl::LockWAL: the first lock enters the write thread(s) so no write is in progress, then takes a write-stop token.</summary>
    public async ValueTask LockWalAsync(bool synchronous, CancellationToken ct)
    {
        ThrowIfDisposed();
        bool first;
        lock (_mutex)
        {
            first = _lockWalCount == 0;
            if (!first) _lockWalCount++;
        }
        if (first)
        {
            // NOTE (upstream): this will "unnecessarily" wait for other non-LockWAL() write stalls to clear before
            // LockWAL returns.
            WriteThread.Writer w = await EnterUnbatchedAsync(synchronous, ct).ConfigureAwait(false);
            try
            {
                lock (_mutex)
                {
                    // Releasing the mutex while entering the write thread might mean another LockWAL got there first.
                    if (_lockWalCount++ == 0)
                    {
                        WriteControllerInstance.AcquireStop();
                        _maybeStalled = true;
                    }
                }
            }
            finally
            {
                ExitUnbatched(w, synchronous);
            }
        }
        try
        {
            // NOTE: avoid I/O holding DB mutex
            if (!_readOnly) await FlushWalAsync(sync: false, synchronous, ct).ConfigureAwait(false);
        }
        catch
        {
            // Undo this LockWAL().
            UnlockWal();
            throw;
        }
    }

    /// <summary>DBImpl::UnlockWAL: the last unlock releases the write stop and wakes the stalled writers.</summary>
    public void UnlockWal()
    {
        lock (_mutex)
        {
            if (_lockWalCount == 0) throw new RocksDbException(StatusCode.Aborted, "No LockWAL() held by current thread");
            if (--_lockWalCount == 0)
            {
                WriteControllerInstance.ReleaseStop();
                _maybeStalled = IsWriteStalled();
                SignalBackgroundWork();
            }
        }
    }
}
