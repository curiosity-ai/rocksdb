using RocksDb.Managed.Util;

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
            // Leads synchronously once the DB has synchronous writers (see LeadsSynchronously); an asynchronous call
            // moves to a write wake-up thread first, so that it never blocks its caller.
            bool exclusive = LeadsSynchronously(synchronous);
            if (exclusive && !synchronous && !WriteThread.OnWakeupThread) await WriteThread.WakeupThreads.SwitchTo();
            WriteThread.Writer w = await EnterUnbatchedIgnoringWalLockAsync(exclusive, ct).ConfigureAwait(false);
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
                ExitUnbatched(w, exclusive);
                // Back to the thread pool before the caller resumes.
                if (!synchronous && WriteThread.OnWakeupThread) await WriteThread.ResumeOnThreadPoolAsync();
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
            UnlockWal(0);
            throw;
        }
        if (synchronous) NoteWalLockOwner(Environment.CurrentManagedThreadId, 1);
    }

    // The threads holding a LockWAL taken through the synchronous API (lock_wal_owner_thread_id_counts_), for the
    // deadlock check of a synchronous manual flush. Locks taken through the asynchronous API have no owner thread.
    // Guarded by mutex.
    private Dictionary<int, int>? _walLockOwnerThreads;

    private void NoteWalLockOwner(int threadId, int delta)
    {
        if (threadId == 0) return;
        lock (_mutex)
        {
            _walLockOwnerThreads ??= new Dictionary<int, int>();
            _walLockOwnerThreads.TryGetValue(threadId, out int n);
            n += delta;
            if (n <= 0) _walLockOwnerThreads.Remove(threadId);
            else _walLockOwnerThreads[threadId] = n;
        }
    }

    /// <summary>True when the calling thread took a LockWAL through the synchronous API and still holds it.</summary>
    private bool CurrentThreadHoldsWalLock()
    {
        lock (_mutex) return _walLockOwnerThreads is { } owners && owners.ContainsKey(Environment.CurrentManagedThreadId);
    }

    /// <summary>
    /// The tail of DBImpl::WaitForPendingWrites: "Wait for any LockWAL to clear" (while
    /// (!lock_wal_owner_thread_id_counts_.empty()) bg_cv_.Wait()). Called by a write leader (batched or unbatched)
    /// before it switches a memtable or the WAL, or changes the logical state: LockWAL promises that neither happens
    /// until UnlockWAL, and a flush of a switched memtable would let the WAL be archived, recycled or deleted under
    /// the lock. Ends when the DB closes (the close then leads the queue; nothing it does needs the lock). With
    /// <paramref name="synchronous"/> the wait blocks the thread and needs no thread-pool work (UnlockWAL completes
    /// the signal directly).
    /// </summary>
    private ValueTask WaitForWalUnlockAsync(bool synchronous, CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref _lockWalCount) == 0) return default;
        return WaitForWalUnlockSlowAsync(synchronous, cancellationToken);
    }

    /// <summary>
    /// <see cref="WaitForWalUnlockAsync"/> for the leader of a write group (PreprocessWrite, before it switches a
    /// memtable or the WAL). The wait is a write stall like the DelayWrite that would follow it: a no_slowdown write
    /// fails with Incomplete instead of waiting, and the stall barrier is up meanwhile, so queued and later
    /// no_slowdown writers fail too instead of waiting behind this leader (upstream waits here without either; the
    /// writes are stopped by the lock in any case).
    /// </summary>
    private ValueTask WaitForWalUnlockAsLeaderAsync(bool noSlowdown, bool synchronous, CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref _lockWalCount) == 0) return default;
        return WaitForWalUnlockAsLeaderSlowAsync(noSlowdown, synchronous, cancellationToken);
    }

    private async ValueTask WaitForWalUnlockAsLeaderSlowAsync(bool noSlowdown, bool synchronous, CancellationToken cancellationToken)
    {
        if (noSlowdown) throw new RocksDbException(StatusCode.Incomplete, "Incomplete: Write stall");
        _writeThread.BeginWriteStall();
        try
        {
            await WaitForWalUnlockSlowAsync(synchronous, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _writeThread.EndWriteStall();
        }
    }

    private async ValueTask WaitForWalUnlockSlowAsync(bool synchronous, CancellationToken cancellationToken)
    {
        while (true)
        {
            Task wait;
            lock (_mutex)
            {
                if (_lockWalCount == 0 || _closing || _shuttingDown || IsDisposed) return;
                wait = _bgSignal.Task;
            }
            TestHooks.Invoke("DBImpl::WaitForPendingWrites:LockWal");
            if (synchronous) wait.Wait(cancellationToken);
            else await wait.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>DBImpl::UnlockWAL: the last unlock releases the write stop and wakes the stalled writers.</summary>
    /// <param name="ownerThreadId">The thread that took this lock through the synchronous API, 0 otherwise.</param>
    public void UnlockWal(int ownerThreadId)
    {
        long stallBegun = 0, nonmemStallBegun = 0;
        // A FlushWAL / SyncWAL called while the WAL is locked uses the WAL outside the write thread, under the WAL I/O
        // lock: the write stop is released only once none is in flight (the stalled writers append to the same WAL).
        SyncValueTask.GetResult(AcquireWalIoLockAsync(synchronous: true));
        try
        {
            lock (_mutex)
            {
                if (_lockWalCount == 0) throw new RocksDbException(StatusCode.Aborted, "No LockWAL() held by current thread");
                if (--_lockWalCount == 0)
                {
                    WriteControllerInstance.ReleaseStop();
                    _maybeStalled = IsWriteStalled();
                    // "For the last UnlockWAL, we don't want to return from UnlockWAL() until the thread(s) that called
                    // BeginWriteStall() have had a chance to call EndWriteStall(), so that no_slowdown writes after
                    // UnlockWAL() are guaranteed to succeed if there's no other source of stall."
                    stallBegun = _writeThread.GetBegunCountOfOutstandingStall();
                    if (_twoWriteQueues) nonmemStallBegun = _nonmemWriteThread!.GetBegunCountOfOutstandingStall();
                    SignalBackgroundWork();
                }
            }
        }
        finally
        {
            ReleaseWalIoLock();
        }
        NoteWalLockOwner(ownerThreadId, -1);
        // Ensure stalls have cleared. Outside the WAL I/O lock and the mutex: the stalled leader needs neither to end
        // its stall, but it needs a thread (an asynchronous leader's continuation runs on the thread pool).
        if (stallBegun != 0) _writeThread.WaitForStallEndedCount(stallBegun);
        if (nonmemStallBegun != 0) _nonmemWriteThread!.WaitForStallEndedCount(nonmemStallBegun);
    }
}
