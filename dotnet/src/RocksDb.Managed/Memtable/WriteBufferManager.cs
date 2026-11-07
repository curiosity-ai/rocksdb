using RocksDb.Managed.Cache;

namespace RocksDb.Managed;

/// <summary>Which memtable a write-buffer-manager flush picks (rocksdb::WriteBufferFlushPolicy).</summary>
public enum WriteBufferFlushPolicy : byte
{
    /// <summary>Flush the oldest mutable memtable of the writing DB (the historical default).</summary>
    FlushOldest = 0,
    /// <summary>Flush the largest mutable memtable of the writing DB.</summary>
    FlushLargest = 1,
    /// <summary>Flush the DB that would reclaim the most memory among all sharing this manager (its largest column family).</summary>
    FlushLargestAcrossDbs = 2,
}

/// <summary>A DB blocked by a write buffer manager stall (StallInterface).</summary>
internal abstract class WriteBufferManagerStall
{
    /// <summary>Unblocks the DB.</summary>
    public abstract void Signal();
}

/// <summary>A DB sharing a manager, for <see cref="WriteBufferFlushPolicy.FlushLargestAcrossDbs"/> (FlushInitiator).</summary>
internal interface IWriteBufferManagerFlushInitiator
{
    /// <summary>Memory a flush of this DB would reclaim (its largest flushable column family); 0 if nothing to flush.</summary>
    long FlushableMemory { get; }

    /// <summary>Switches the memtable to flush and schedules the flush (FlushReason kWriteBufferManager).</summary>
    ValueTask FlushForWriteBufferManagerAsync();
}

/// <summary>
/// Controls the total memtable memory of any number of DBs and column families. Port of WriteBufferManager
/// (include/rocksdb/write_buffer_manager.h, memtable/write_buffer_manager.cc):
/// <list type="bullet">
/// <item><b>buffer_size</b> (0 = no limit): memtables report their arena blocks (<c>ReserveMem</c>); once the mutable
/// memtables exceed 7/8 of it, or all memtables exceed it while at least half is still mutable, the next write of a DB
/// switches a memtable and flushes it (FlushReason <see cref="FlushReason.WriteBufferManager"/>), picked by
/// <see cref="FlushPolicy"/>.</item>
/// <item><b>allow_stall</b>: writes of every DB using the manager stall while the total exceeds buffer_size, until flushes
/// free memory (no_slowdown writes fail with Incomplete "Write stall").</item>
/// <item><b>cost to cache</b>: with a block cache, memtable memory is charged to it with 256 KiB placeholder entries
/// (CacheReservationManager, role <see cref="CacheEntryRole.WriteBuffer"/>, delayed decrease), so block cache and
/// memtables share one memory budget. With a tiered cache the charge is split between the tiers.</item>
/// </list>
/// Pass an instance in <see cref="DbOptions.WriteBufferManager"/> to share it; otherwise each DB gets its own of
/// <see cref="DbOptions.DbWriteBufferSize"/> bytes. Thread-safe.
/// </summary>
public sealed class WriteBufferManager
{
    // A soft-limit flush work cycle (kFlushWorkCycleMicros).
    private const long FlushWorkCycleMs = 20;

    private long _bufferSize;
    private long _mutableLimit;
    private long _memoryUsed;
    // Memory that hasn't been scheduled to free.
    private long _memoryActive;
    private readonly CacheReservationManagerImpl? _cacheResMgr;
    // Protects _cacheResMgr.
    private readonly Lock _cacheResMgrMutex = new();
    private readonly List<WriteBufferManagerStall> _queue = new();
    // Protects _queue and _stallActive changes.
    private readonly Lock _mu = new();
    private volatile bool _allowStall;
    private volatile bool _stallActive;
    private volatile WriteBufferFlushPolicy _flushPolicy;
    private readonly List<IWriteBufferManagerFlushInitiator> _initiators = new();
    private long _localFlushDeadline;
    private int _coordinatorRunning;

    /// <param name="bufferSize">Memtable memory limit in bytes; 0 = no limit (memory usage is then only tracked for cache charging).</param>
    /// <param name="cache">If set, memtable memory is charged to this cache (works with <paramref name="bufferSize"/> = 0 too).</param>
    /// <param name="allowStall">Stall writes while memory usage exceeds <paramref name="bufferSize"/>.</param>
    public WriteBufferManager(long bufferSize, BlockCache? cache, bool allowStall)
        : this(bufferSize, cache, allowStall, WriteBufferFlushPolicy.FlushOldest) { }

    /// <param name="bufferSize">Memtable memory limit in bytes; 0 = no limit.</param>
    /// <param name="cache">If set, memtable memory is charged to this cache.</param>
    /// <param name="allowStall">Stall writes while memory usage exceeds <paramref name="bufferSize"/>.</param>
    /// <param name="flushPolicy">Which memtable a manager-triggered flush picks.</param>
    public WriteBufferManager(long bufferSize, BlockCache? cache, bool allowStall, WriteBufferFlushPolicy flushPolicy)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(bufferSize);
        _bufferSize = bufferSize;
        _mutableLimit = bufferSize * 7 / 8;
        _allowStall = allowStall;
        _flushPolicy = flushPolicy;
        // Memtable memory usage fluctuates frequently, so delayed_decrease saves dummy entry insertions on increases
        // right after decreases.
        if (cache is not null) _cacheResMgr = new CacheReservationManagerImpl(cache, CacheEntryRole.WriteBuffer, delayedDecrease: true);
    }

    /// <summary>True if a buffer size limit (&gt; 0) is set (enabled()).</summary>
    public bool Enabled => BufferSize > 0;

    /// <summary>True if memtable memory is charged to a cache (cost_to_cache()).</summary>
    public bool CostToCache => _cacheResMgr is not null;

    /// <summary>Total memory used by memtables (memory_usage()); only tracked when enabled or charging a cache.</summary>
    public long MemoryUsage => Volatile.Read(ref _memoryUsed);

    /// <summary>Memory used by mutable (active) memtables (mutable_memtable_memory_usage()).</summary>
    public long MutableMemtableMemoryUsage => Volatile.Read(ref _memoryActive);

    /// <summary>Memory reserved in the cache by placeholder entries (dummy_entries_in_cache_usage()).</summary>
    public long DummyEntriesInCacheUsage => _cacheResMgr?.TotalReservedCacheSize ?? 0;

    /// <summary>The limit (buffer_size()).</summary>
    public long BufferSize => Volatile.Read(ref _bufferSize);

    /// <summary>Whether writes stall when the limit is exceeded.</summary>
    public bool AllowStall => _allowStall;

    /// <summary>The policy of manager-triggered flushes (flush_policy()).</summary>
    public WriteBufferFlushPolicy FlushPolicy => _flushPolicy;

    /// <summary>Changes the limit (SetBufferSize). REQUIRED: <paramref name="newSize"/> &gt; 0.</summary>
    public void SetBufferSize(long newSize)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(newSize);
        Volatile.Write(ref _bufferSize, newSize);
        Volatile.Write(ref _mutableLimit, newSize * 7 / 8);
        // Check if stall is active and can be ended.
        MaybeEndWriteStall();
        RequestCoordinatedFlush();
    }

    /// <summary>Enables or disables write stalls (SetAllowStall).</summary>
    public void SetAllowStall(bool newAllowStall)
    {
        _allowStall = newAllowStall;
        MaybeEndWriteStall();
    }

    /// <summary>Changes the flush policy (SetFlushPolicy).</summary>
    public void SetFlushPolicy(WriteBufferFlushPolicy newFlushPolicy)
    {
        _flushPolicy = newFlushPolicy;
        RequestCoordinatedFlush();
    }

    // ---- internal API (called by RocksDB) ----------------------------------------------------------

    /// <summary>
    /// ShouldFlush: the mutable memtables exceed 7/8 of the limit, or the total exceeds it while at least half is still
    /// mutable (if more than half is already being flushed, more flushes may not help).
    /// </summary>
    internal bool ShouldFlush()
    {
        long size = BufferSize;
        if (size <= 0) return false;
        long active = MutableMemtableMemoryUsage;
        if (active > Volatile.Read(ref _mutableLimit)) return true;
        return MemoryUsage >= size && active >= size / 2;
    }

    /// <summary>ShouldStall: stalls are allowed and active, or the total reached the limit.</summary>
    internal bool ShouldStall()
    {
        if (!_allowStall || !Enabled) return false;
        return IsStallActive || IsStallThresholdExceeded;
    }

    internal bool IsStallActive => _stallActive;

    internal bool IsStallThresholdExceeded => MemoryUsage >= BufferSize;

    internal void ReserveMem(long mem)
    {
        bool enabled = Enabled;
        long previousTotal = 0;
        if (_cacheResMgr is not null) previousTotal = ReserveMemWithCache(mem);
        else if (enabled) previousTotal = Interlocked.Add(ref _memoryUsed, mem) - mem;
        if (!enabled) return;
        long previousActive = Interlocked.Add(ref _memoryActive, mem) - mem;
        if (_flushPolicy == WriteBufferFlushPolicy.FlushLargestAcrossDbs)
        {
            long mutableLimit = Volatile.Read(ref _mutableLimit), total = BufferSize;
            bool crossedMutable = previousActive <= mutableLimit && mem > mutableLimit - previousActive;
            bool crossedTotal = previousTotal < total && mem >= total - previousTotal;
            if (crossedMutable || crossedTotal) RequestCoordinatedFlush();
        }
    }

    private long ReserveMemWithCache(long mem)
    {
        lock (_cacheResMgrMutex)
        {
            long previous = Volatile.Read(ref _memoryUsed);
            long newUsed = previous + mem;
            Volatile.Write(ref _memoryUsed, newUsed);
            // The error is absorbed: the manager cannot handle a failed charge (strict-capacity caches).
            _cacheResMgr!.UpdateCacheReservation(newUsed);
            return previous;
        }
    }

    /// <summary>The memory is being freed (immutable memtable): not counted as mutable any more (ScheduleFreeMem).</summary>
    internal void ScheduleFreeMem(long mem)
    {
        if (Enabled) Interlocked.Add(ref _memoryActive, -mem);
    }

    internal void FreeMem(long mem)
    {
        if (_cacheResMgr is not null)
        {
            lock (_cacheResMgrMutex)
            {
                long newUsed = Volatile.Read(ref _memoryUsed) - mem;
                Volatile.Write(ref _memoryUsed, newUsed);
                _cacheResMgr.UpdateCacheReservation(Math.Max(0, newUsed));
            }
        }
        else if (Enabled)
        {
            Interlocked.Add(ref _memoryUsed, -mem);
        }
        // Check if stall is active and can be ended.
        MaybeEndWriteStall();
    }

    /// <summary>Adds the DB to the stall queue if the stall conditions still hold, else signals it right away (BeginWriteStall).</summary>
    internal void BeginWriteStall(WriteBufferManagerStall stall)
    {
        bool queued = false;
        lock (_mu)
        {
            // Verify if the stall conditions are still active.
            if (ShouldStall())
            {
                _stallActive = true;
                _queue.Add(stall);
                queued = true;
            }
        }
        // If the node was not consumed, the stall has ended already and we can signal the caller.
        if (!queued) stall.Signal();
    }

    /// <summary>Called when memory is freed or the limit changed: unblocks all stalled DBs once the condition resolved.</summary>
    internal void MaybeEndWriteStall()
    {
        // Stall conditions have not been resolved.
        if (_allowStall && IsStallThresholdExceeded) return;
        WriteBufferManagerStall[] toSignal;
        lock (_mu)
        {
            if (!_stallActive) return; // nothing to do
            // Unblock new writers.
            _stallActive = false;
            toSignal = _queue.ToArray();
            _queue.Clear();
        }
        foreach (WriteBufferManagerStall s in toSignal) s.Signal();
    }

    internal void RemoveDbFromQueue(WriteBufferManagerStall stall)
    {
        if (!Enabled || !_allowStall) return;
        lock (_mu) _queue.Remove(stall);
    }

    /// <summary>
    /// TryAcquireLocalFlush: with <see cref="WriteBufferFlushPolicy.FlushLargestAcrossDbs"/> soft-limit flushes belong to the
    /// coordinator; at the hard limit at most one local flush per work cycle is granted manager-wide (a stalled DB only
    /// when at the front of the stall queue). Other policies always flush locally.
    /// </summary>
    internal bool TryAcquireLocalFlush(WriteBufferManagerStall? stalledDb)
    {
        if (_flushPolicy != WriteBufferFlushPolicy.FlushLargestAcrossDbs) return true;
        if (!IsStallThresholdExceeded)
        {
            RequestCoordinatedFlush();
            return false;
        }
        if (stalledDb is not null)
        {
            lock (_mu)
            {
                if (_queue.Count == 0 || !ReferenceEquals(_queue[0], stalledDb)) return false;
            }
        }
        long now = Environment.TickCount64;
        long deadline = Volatile.Read(ref _localFlushDeadline);
        while (deadline <= now)
        {
            long seen = Interlocked.CompareExchange(ref _localFlushDeadline, now + FlushWorkCycleMs, deadline);
            if (seen == deadline) return true;
            deadline = seen;
        }
        return false;
    }

    internal void RegisterFlushInitiator(IWriteBufferManagerFlushInitiator initiator)
    {
        lock (_initiators) _initiators.Add(initiator);
    }

    internal void DeregisterFlushInitiator(IWriteBufferManagerFlushInitiator initiator)
    {
        lock (_initiators) _initiators.Remove(initiator);
    }

    /// <summary>Number of DBs registered with the manager.</summary>
    internal int RegisteredDbCount
    {
        get
        {
            lock (_initiators) return _initiators.Count;
        }
    }

    /// <summary>
    /// The cross-DB flush coordinator (a simplification of upstream's sorter thread and flush hand-off): while the soft
    /// limit is exceeded it asks the registered DB with the most flushable memory to flush its largest column family,
    /// one DB at a time.
    /// </summary>
    private void RequestCoordinatedFlush()
    {
        if (_flushPolicy != WriteBufferFlushPolicy.FlushLargestAcrossDbs || !ShouldFlush()) return;
        if (Interlocked.CompareExchange(ref _coordinatorRunning, 1, 0) != 0) return;
        _ = Task.Run(RunCoordinatorAsync);
    }

    private async Task RunCoordinatorAsync()
    {
        try
        {
            for (int rounds = 0; rounds < 1024 && _flushPolicy == WriteBufferFlushPolicy.FlushLargestAcrossDbs && ShouldFlush(); rounds++)
            {
                IWriteBufferManagerFlushInitiator? best = null;
                long bestMem = 0;
                lock (_initiators)
                {
                    foreach (IWriteBufferManagerFlushInitiator i in _initiators)
                    {
                        long m = i.FlushableMemory;
                        if (m > bestMem)
                        {
                            bestMem = m;
                            best = i;
                        }
                    }
                }
                if (best is null) break;
                try
                {
                    await best.FlushForWriteBufferManagerAsync().ConfigureAwait(false);
                }
                catch (Exception e) when (e is RocksDbException or ObjectDisposedException or IOException)
                {
                    break;
                }
            }
        }
        finally
        {
            Volatile.Write(ref _coordinatorRunning, 0);
        }
    }
}
