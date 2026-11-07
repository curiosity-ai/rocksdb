namespace RocksDb.Managed.Memory;

/// <summary>
/// Reports the arena blocks of one memtable to a <see cref="WriteBufferManager"/>. Port of AllocTracker
/// (memory/allocator.h, memtable/alloc_tracker.cc): every block the arena allocates is reserved
/// (<see cref="Allocate"/>); when the memtable becomes immutable its memory is scheduled to be freed
/// (<see cref="DoneAllocating"/>, no longer counted as mutable); when it is destroyed the memory is freed
/// (<see cref="FreeMem"/>). Only created when the manager is enabled or charges a cache, so memtables of DBs without a
/// budget pay nothing. <see cref="Allocate"/> may be called concurrently (the arena's block allocation is under its
/// lock); the other calls happen once each.
/// </summary>
internal sealed class AllocTracker(WriteBufferManager writeBufferManager)
{
    private readonly WriteBufferManager _wbm = writeBufferManager;
    private long _bytesAllocated;
    private int _doneAllocating;
    private int _freed;

    /// <summary>Bytes reported so far (WBMTrackedMemoryUsage of the memtable).</summary>
    public long BytesAllocated => Volatile.Read(ref _bytesAllocated);

    public void Allocate(long bytes)
    {
        if (_wbm.Enabled || _wbm.CostToCache)
        {
            Interlocked.Add(ref _bytesAllocated, bytes);
            _wbm.ReserveMem(bytes);
        }
    }

    public void DoneAllocating()
    {
        if (Interlocked.Exchange(ref _doneAllocating, 1) != 0) return;
        if (_wbm.Enabled || _wbm.CostToCache) _wbm.ScheduleFreeMem(Volatile.Read(ref _bytesAllocated));
    }

    public void FreeMem()
    {
        DoneAllocating();
        if (Interlocked.Exchange(ref _freed, 1) != 0) return;
        if (_wbm.Enabled || _wbm.CostToCache) _wbm.FreeMem(Volatile.Read(ref _bytesAllocated));
    }
}
