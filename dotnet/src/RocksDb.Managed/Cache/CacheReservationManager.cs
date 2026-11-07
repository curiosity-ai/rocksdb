namespace RocksDb.Managed.Cache;

/// <summary>
/// Reserves memory against a block cache by inserting pinned placeholder ("dummy") entries of
/// <see cref="DummyEntrySize"/> bytes, so that memory used outside the cache (memtables, table readers, filter
/// construction...) is charged to it. Port of cache/cache_reservation_manager.{h,cc} (CacheReservationManager).
/// Results map Status to bool: false means the cache refused a placeholder (Status::MemoryLimit with a strict
/// capacity limit); the reservation then stays at what could be inserted.
/// </summary>
public abstract class CacheReservationManager
{
    /// <summary>kSizeDummyEntry: 256 KiB per placeholder.</summary>
    public const long DummyEntrySize = 256 * 1024;

    /// <summary>Reserves (or releases) placeholders so that the reservation covers <paramref name="newMemoryUsed"/>.</summary>
    public abstract bool UpdateCacheReservation(long newMemoryUsed);

    /// <summary>Changes the memory used by <paramref name="memoryUsedDelta"/> (increase or decrease, clamped at 0).</summary>
    public virtual bool UpdateCacheReservation(long memoryUsedDelta, bool increase)
    {
        if (memoryUsedDelta == 0) return true;
        long total = TotalMemoryUsed;
        if (increase) return UpdateCacheReservation(total + memoryUsedDelta);
        return UpdateCacheReservation(total - Math.Min(memoryUsedDelta, total));
    }

    /// <summary>
    /// Adds <paramref name="incrementalMemoryUsed"/> to the reservation; disposing <paramref name="handle"/> releases it
    /// (MakeCacheReservation). The handle is returned even when the cache refused a placeholder.
    /// </summary>
    public bool MakeCacheReservation(long incrementalMemoryUsed, out CacheReservationHandle handle)
    {
        bool ok = UpdateCacheReservation(TotalMemoryUsed + incrementalMemoryUsed);
        handle = new CacheReservationHandle(this, incrementalMemoryUsed);
        return ok;
    }

    /// <summary>Size of the reservation in the cache (a multiple of <see cref="DummyEntrySize"/>).</summary>
    public abstract long TotalReservedCacheSize { get; }

    /// <summary>The memory used that was last reported.</summary>
    public abstract long TotalMemoryUsed { get; }

    internal void ReleaseCacheReservation(long incrementalMemoryUsed) =>
        UpdateCacheReservation(TotalMemoryUsed - Math.Min(incrementalMemoryUsed, TotalMemoryUsed));
}

/// <summary>A reservation made by <see cref="CacheReservationManager.MakeCacheReservation"/>; dispose to release it.</summary>
public sealed class CacheReservationHandle : IDisposable
{
    private CacheReservationManager? _manager;
    private readonly long _incrementalMemoryUsed;

    internal CacheReservationHandle(CacheReservationManager manager, long incrementalMemoryUsed)
    {
        _manager = manager;
        _incrementalMemoryUsed = incrementalMemoryUsed;
    }

    public void Dispose() => Interlocked.Exchange(ref _manager, null)?.ReleaseCacheReservation(_incrementalMemoryUsed);
}

/// <summary>
/// CacheReservationManagerImpl&lt;R&gt;: placeholders of role <c>R</c>. Not thread-safe (wrap it in
/// <see cref="ConcurrentCacheReservationManager"/>), except <see cref="TotalReservedCacheSize"/>.
/// </summary>
public sealed class CacheReservationManagerImpl : CacheReservationManager, IDisposable
{
    private readonly BlockCache _cache;
    private readonly bool _delayedDecrease;
    private readonly CacheValue _placeholder;
    private readonly List<CachedBlock> _dummyHandles = new();
    private long _cacheAllocatedSize;
    private long _memoryUsed;

    /// <param name="cache">The cache to reserve memory in.</param>
    /// <param name="role">Role of the placeholder entries.</param>
    /// <param name="delayedDecrease">
    /// Keep placeholders until the memory used drops below 3/4 of the reservation (saves re-inserting them when memory
    /// fluctuates, e.g. memtables).
    /// </param>
    public CacheReservationManagerImpl(BlockCache cache, CacheEntryRole role, bool delayedDecrease)
    {
        _cache = cache ?? throw new ArgumentNullException(nameof(cache));
        _delayedDecrease = delayedDecrease;
        _placeholder = default(CacheValue).WithHelper(CacheItemHelper.Basic(role));
    }

    public BlockCache Cache => _cache;

    public override long TotalReservedCacheSize => Volatile.Read(ref _cacheAllocatedSize);

    public override long TotalMemoryUsed => _memoryUsed;

    public override bool UpdateCacheReservation(long newMemoryUsed)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(newMemoryUsed);
        _memoryUsed = newMemoryUsed;
        long cur = _cacheAllocatedSize;
        if (newMemoryUsed == cur) return true;
        if (newMemoryUsed > cur) return IncreaseCacheReservation(newMemoryUsed);
        // In delayed decrease mode, we don't decrease the reservation until the memory usage is less than 3/4 of what
        // we reserve in the cache.
        if (_delayedDecrease && newMemoryUsed >= cur / 4 * 3) return true;
        DecreaseCacheReservation(newMemoryUsed);
        return true;
    }

    private bool IncreaseCacheReservation(long newMemoryUsed)
    {
        while (newMemoryUsed > _cacheAllocatedSize)
        {
            if (!_cache.TryInsert(CacheKey.CreateUniqueForCacheLifetime(_cache), _placeholder, DummyEntrySize, CachePriority.Low, out CachedBlock h))
                return false; // MemoryLimit
            _dummyHandles.Add(h);
            Volatile.Write(ref _cacheAllocatedSize, _cacheAllocatedSize + DummyEntrySize);
        }
        return true;
    }

    private void DecreaseCacheReservation(long newMemoryUsed)
    {
        // Decrease to the smallest multiple of kSizeDummyEntry that is >= new_mem_used.
        while (newMemoryUsed + DummyEntrySize <= _cacheAllocatedSize)
        {
            CachedBlock h = _dummyHandles[^1];
            _dummyHandles.RemoveAt(_dummyHandles.Count - 1);
            _cache.Release(ref h, eraseIfLastRef: true);
            Volatile.Write(ref _cacheAllocatedSize, _cacheAllocatedSize - DummyEntrySize);
        }
    }

    /// <summary>Releases every placeholder (the destructor).</summary>
    public void Dispose()
    {
        for (int i = _dummyHandles.Count - 1; i >= 0; i--)
        {
            CachedBlock h = _dummyHandles[i];
            _cache.Release(ref h, eraseIfLastRef: true);
        }
        _dummyHandles.Clear();
        Volatile.Write(ref _cacheAllocatedSize, 0);
        _memoryUsed = 0;
    }
}

/// <summary>A thread-safe wrapper of a <see cref="CacheReservationManager"/> (ConcurrentCacheReservationManager).</summary>
public sealed class ConcurrentCacheReservationManager(CacheReservationManager inner) : CacheReservationManager
{
    private readonly CacheReservationManager _inner = inner ?? throw new ArgumentNullException(nameof(inner));
    private readonly Lock _lock = new();

    public override bool UpdateCacheReservation(long newMemoryUsed)
    {
        lock (_lock) return _inner.UpdateCacheReservation(newMemoryUsed);
    }

    public override bool UpdateCacheReservation(long memoryUsedDelta, bool increase)
    {
        lock (_lock) return _inner.UpdateCacheReservation(memoryUsedDelta, increase);
    }

    public override long TotalReservedCacheSize => _inner.TotalReservedCacheSize;

    public override long TotalMemoryUsed
    {
        get
        {
            lock (_lock) return _inner.TotalMemoryUsed;
        }
    }
}
