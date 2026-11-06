using System.Diagnostics;

namespace RocksDb.Managed.Db;

/// <summary>
/// Write buffer manager integration of the write path. Ports of DBImpl::PreprocessWrite (the write_buffer_manager_
/// parts), HandleWriteBufferManagerFlush, CollectFlushableCFs and WriteBufferManagerStallWrites
/// (db/db_impl/db_impl_write.cc, db/db_impl/db_impl_compaction_flush.cc), and WBMStallInterface (db/db_impl/db_impl.h).
/// </summary>
internal sealed partial class DbImpl : IWriteBufferManagerFlushInitiator
{
    /// <summary>The DB's write buffer manager: DBOptions::write_buffer_manager, else a private one of db_write_buffer_size (SanitizeOptions).</summary>
    internal WriteBufferManager WriteBufferManager { get; private set; } = null!;

    private WbmStall? _wbmStall;
    private int _wbmRegistered;

    /// <summary>WBMStallInterface: a stalled DB waits for the manager's signal.</summary>
    private sealed class WbmStall : WriteBufferManagerStall
    {
        private TaskCompletionSource _signal = NewSignal();

        private static TaskCompletionSource NewSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>SetState(BLOCKED): a fresh signal for the next stall.</summary>
        public Task Block()
        {
            TaskCompletionSource tcs = NewSignal();
            Volatile.Write(ref _signal, tcs);
            return tcs.Task;
        }

        public override void Signal() => Volatile.Read(ref _signal).TrySetResult();
    }

    private void InitializeWriteBufferManager()
    {
        WriteBufferManager = Options.WriteBufferManager ?? new WriteBufferManager((long)Math.Min(Options.DbWriteBufferSize, long.MaxValue), null, false);
    }

    /// <summary>Registers the DB for cross-DB flush selection once it is open for writes (RegisterFlushInitiator).</summary>
    private void RegisterWithWriteBufferManager()
    {
        if (Interlocked.Exchange(ref _wbmRegistered, 1) == 0) WriteBufferManager.RegisterFlushInitiator(this);
    }

    private void DeregisterFromWriteBufferManager()
    {
        if (Interlocked.Exchange(ref _wbmRegistered, 0) == 1) WriteBufferManager.DeregisterFlushInitiator(this);
        if (_wbmStall is { } s)
        {
            WriteBufferManager.RemoveDbFromQueue(s);
            s.Signal();
        }
    }

    /// <summary>
    /// The write buffer manager steps of PreprocessWrite. REQUIRES: write leader. Cheap when the manager is disabled
    /// (one volatile read each).
    /// </summary>
    private async ValueTask PreprocessWriteBufferManagerAsync(bool noSlowdown, bool synchronous, CancellationToken cancellationToken)
    {
        WriteBufferManager wbm = WriteBufferManager;
        if (wbm.ShouldFlush() && wbm.TryAcquireLocalFlush(null))
        {
            // Before a new memtable is added in SwitchMemtable, ShouldFlush keeps returning true; another DB sharing the
            // manager may flush too ("suboptimal but still correct").
            if (_pipelined) await WaitForMemTableWritersAsync(synchronous).ConfigureAwait(false);
            await WaitForPendingWritesAsync(synchronous).ConfigureAwait(false);
            await HandleWriteBufferManagerFlushAsync(synchronous, cancellationToken).ConfigureAwait(false);
        }
        // Writers of all DBs using the manager stall once memory usage exceeds the limit; the current write too.
        if (wbm.ShouldStall())
        {
            Interlocked.Increment(ref _writeStats.WriteBufferManagerLimitStops); // kIntStatsWriteBufferManagerLimitStopsCounts
            if (noSlowdown) throw new RocksDbException(StatusCode.Incomplete, "Incomplete: Write stall");
            await WriteBufferManagerStallWritesAsync(synchronous, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// HandleWriteBufferManagerFlush: switches the memtable of the column family picked by the flush policy (all non-empty
    /// ones with atomic_flush) and schedules its flush with FlushReason kWriteBufferManager. REQUIRES: write leader.
    /// </summary>
    private async ValueTask HandleWriteBufferManagerFlushAsync(bool synchronous, CancellationToken cancellationToken)
    {
        var cfds = new List<ColumnFamilyData>();
        lock (_mutex)
        {
            if (Options.AtomicFlush)
            {
                foreach (ColumnFamilyData c in Versions.ColumnFamilies)
                {
                    if (!c.Dropped && !c.Mem.IsEmpty) cfds.Add(c);
                }
            }
            else
            {
                WriteBufferFlushPolicy policy = WriteBufferManager.FlushPolicy;
                // Cross-DB selection still flushes the largest CF within the chosen DB.
                bool largest = policy != WriteBufferFlushPolicy.FlushOldest;
                ColumnFamilyData? picked = CollectFlushableColumnFamilies(includeWaitingImmutable: policy == WriteBufferFlushPolicy.FlushLargestAcrossDbs,
                    out ColumnFamilyData? oldest, out _);
                picked = largest ? picked : oldest;
                if (picked is not null) cfds.Add(picked);
            }
        }
        if (cfds.Count == 0) return;
        WriteBufferManager wbm = WriteBufferManager;
        LogInfo("Flushing triggered to alleviate write buffer memory usage. Write buffer is using " + wbm.MemoryUsage +
                " bytes out of a total of " + wbm.BufferSize + ".");
        foreach (ColumnFamilyData cfd in cfds)
        {
            if (cfd.Dropped || cfd.Mem.IsEmpty) continue;
            await SwitchMemtableAsync(cfd, FlushReason.WriteBufferManager, synchronous, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// CollectFlushableCFs: the column family with the largest flushable memory (mutable memtable, plus immutable ones not
    /// yet flushing with <paramref name="includeWaitingImmutable"/>) and the one with the oldest mutable memtable, among
    /// those not already flushing. REQUIRES: mutex.
    /// </summary>
    private ColumnFamilyData? CollectFlushableColumnFamilies(bool includeWaitingImmutable, out ColumnFamilyData? oldest, out long largestMem)
    {
        ColumnFamilyData? largest = null;
        oldest = null;
        largestMem = 0;
        ulong oldestSeq = ulong.MaxValue;
        foreach (ColumnFamilyData cfd in Versions.ColumnFamilies)
        {
            if (cfd.Dropped || cfd.Mem is null) continue;
            long mutableMem = cfd.Mem.IsEmpty ? 0 : MemUsageForWbm(cfd.Mem);
            bool flushPendingOrRunning = false;
            long waiting = 0;
            foreach (ImmutableMemTable m in cfd.Imm)
            {
                if (m.FlushInProgress) flushPendingOrRunning = true;
                else if (m.FlushCompleted) continue;
                else waiting += MemUsageForWbm(m.Mem);
            }
            // Non-atomic flushes skip CFs already flushing.
            if (flushPendingOrRunning) continue;
            long mem = mutableMem + (includeWaitingImmutable ? waiting : 0);
            if (mem == 0) continue;
            if (largest is null || mem > largestMem)
            {
                largest = cfd;
                largestMem = mem;
            }
            ulong seq = cfd.Mem.CreationSequence;
            if (oldest is null || seq < oldestSeq)
            {
                oldest = cfd;
                oldestSeq = seq;
            }
        }
        return largest;
    }

    // WBMTrackedMemoryUsage, falling back to the arena usage when the manager does not track memtables.
    private static long MemUsageForWbm(MemTable m)
    {
        long tracked = m.WriteBufferManagerTrackedMemory;
        return tracked > 0 ? tracked : m.ApproximateMemoryUsageFast;
    }

    /// <summary>
    /// WriteBufferManagerStallWrites: the leader blocks (later writers queue behind it) until the manager signals that
    /// memory dropped below the limit. With the cross-DB policy, a DB at the front of the stall queue retries a local
    /// flush every work cycle.
    /// </summary>
    private async ValueTask WriteBufferManagerStallWritesAsync(bool synchronous, CancellationToken cancellationToken)
    {
        WriteBufferManager wbm = WriteBufferManager;
        long start = Stopwatch.GetTimestamp();
        while (true)
        {
            WbmStall stall = _wbmStall ??= new WbmStall();
            Task blocked = stall.Block();
            wbm.BeginWriteStall(stall);
            bool retryLocalFlush = false;
            if (wbm.FlushPolicy == WriteBufferFlushPolicy.FlushLargestAcrossDbs)
            {
                while (true)
                {
                    if (synchronous) blocked.Wait(20, cancellationToken);
                    else await Task.WhenAny(blocked, Task.Delay(20, cancellationToken)).ConfigureAwait(false);
                    if (blocked.IsCompleted) break;
                    if (wbm.TryAcquireLocalFlush(stall))
                    {
                        wbm.RemoveDbFromQueue(stall);
                        retryLocalFlush = true;
                        break;
                    }
                    if (IsDisposed || _shuttingDown) break;
                }
            }
            else
            {
                // Wake up periodically to notice a shutdown.
                while (!blocked.IsCompleted && !IsDisposed && !_shuttingDown)
                {
                    if (synchronous) blocked.Wait(100, cancellationToken);
                    else await Task.WhenAny(blocked, Task.Delay(100, cancellationToken)).ConfigureAwait(false);
                }
            }
            if (IsDisposed || _shuttingDown) throw new RocksDbException(StatusCode.ShutdownInProgress, "Shutdown in progress");
            if (!retryLocalFlush || !wbm.ShouldStall()) break;
            if (_pipelined) await WaitForMemTableWritersAsync(synchronous).ConfigureAwait(false);
            await WaitForPendingWritesAsync(synchronous).ConfigureAwait(false);
            await HandleWriteBufferManagerFlushAsync(synchronous, cancellationToken).ConfigureAwait(false);
        }
        RecordWriteStall(start);
    }

    // ---- FlushInitiator (FlushLargestAcrossDbs) --------------------------------------------------------------

    long IWriteBufferManagerFlushInitiator.FlushableMemory
    {
        get
        {
            if (IsDisposed || _readOnly) return 0;
            lock (_mutex)
            {
                CollectFlushableColumnFamilies(includeWaitingImmutable: true, out _, out long mem);
                return mem;
            }
        }
    }

    /// <summary>A flush requested by the manager's coordinator: enters the write thread and switches the largest column family.</summary>
    async ValueTask IWriteBufferManagerFlushInitiator.FlushForWriteBufferManagerAsync()
    {
        if (IsDisposed || _readOnly) return;
        WriteThread.Writer w = await EnterUnbatchedAsync(synchronous: false, CancellationToken.None).ConfigureAwait(false);
        try
        {
            ColumnFamilyData? cfd;
            lock (_mutex) cfd = CollectFlushableColumnFamilies(includeWaitingImmutable: true, out _, out _);
            if (cfd is not null && !cfd.Dropped && !cfd.Mem.IsEmpty)
                await SwitchMemtableAsync(cfd, FlushReason.WriteBufferManager, synchronous: false, CancellationToken.None).ConfigureAwait(false);
        }
        finally
        {
            ExitUnbatched(w, synchronous: false);
        }
    }
}
