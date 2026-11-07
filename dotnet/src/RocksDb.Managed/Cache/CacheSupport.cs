namespace RocksDb.Managed.Cache;

/// <summary>
/// Callback of <see cref="BlockCache.ApplyToAllEntries{TState}(TState, CacheEntryVisitor{TState})"/>: the entry's key,
/// value (valid only during the call; not owned by the callee), charge and helper (null: none).
/// </summary>
public delegate void CacheEntryVisitor<TState>(TState state, in CacheKey key, in CacheValue value, long charge, CacheItemHelper? helper);

/// <summary>
/// Receives entries evicted from a cache (the Cache::EvictionCallback, include/rocksdb/advanced_cache.h). Never takes
/// ownership of the value (the callback always returns false upstream for the secondary cache adapter).
/// </summary>
internal abstract class CacheEvictionHandler
{
    /// <summary>
    /// <paramref name="entry"/> was evicted for capacity (not erased); its value is valid during the call.
    /// <paramref name="wasHit"/>: the entry was looked up after it was inserted.
    /// </summary>
    internal abstract void OnEvicted(in CacheKey key, CacheEntry entry, bool wasHit);
}

/// <summary>
/// A standalone (detached) entry (Cache::CreateStandalone): visible only through its handles, freed by the last release.
/// </summary>
internal sealed class StandaloneEntry : CacheEntry
{
    private readonly BlockCache _owner;
    private int _refs = 1;

    internal StandaloneEntry(BlockCache owner) => _owner = owner;

    internal override void AddRef(int generation)
    {
        while (true)
        {
            int r = Volatile.Read(ref _refs);
            if (r <= 0 || generation != Generation) CachedBlock.ThrowStale();
            if (Interlocked.CompareExchange(ref _refs, r + 1, r) == r) return;
        }
    }

    internal override void Release(int generation, bool eraseIfLastRef)
    {
        if (generation != Generation) CachedBlock.ThrowStale();
        int r = Interlocked.Decrement(ref _refs);
        if (r < 0) CachedBlock.ThrowStale();
        if (r == 0)
        {
            Generation++;
            _owner.ReleaseStandaloneCharge(TotalCharge);
            FreeValue();
        }
    }
}

/// <summary>
/// State of a lookup started with <see cref="BlockCache.StartAsyncLookup"/> (Cache::AsyncLookupHandle). Set the inputs,
/// start the lookup, then (if <see cref="IsPending"/>) wait with <see cref="BlockCache.WaitAll"/> or
/// <see cref="BlockCache.WaitAllAsync"/>; <see cref="TakeResult"/> then returns the pinned handle (empty on a miss),
/// owned by the caller. A handle object may be reused after <see cref="Reset"/>.
/// </summary>
public sealed class AsyncLookupHandle
{
    public CacheKey Key { get; set; }
    public CacheItemHelper? Helper { get; set; }
    public object? CreateContext { get; set; }
    public CachePriority Priority { get; set; } = CachePriority.Low;
    public Statistics? Statistics { get; set; }

    private CachedBlock _result;
    internal SecondaryCacheResultHandle? PendingHandle;
    internal SecondaryCache? PendingCache;
    internal bool FoundDummyEntry;
    internal bool KeptInSecCache;

    /// <summary>True while a secondary-cache lookup is outstanding.</summary>
    public bool IsPending => PendingHandle is not null;

    /// <summary>True when the lookup completed with a hit.</summary>
    public bool HasResult => !IsPending && _result.HasValue;

    /// <summary>Takes the result (empty on a miss or while pending), leaving the handle empty. The caller releases it.</summary>
    public CachedBlock TakeResult()
    {
        if (IsPending) return default;
        CachedBlock r = _result;
        _result = default;
        return r;
    }

    internal ref CachedBlock ResultRef => ref _result;

    internal void SetResult(CachedBlock block) => _result = block;

    /// <summary>Clears all state (releases nothing: take the result first).</summary>
    public void Reset()
    {
        Key = default;
        Helper = null;
        CreateContext = null;
        Priority = CachePriority.Low;
        Statistics = null;
        _result = default;
        PendingHandle = null;
        PendingCache = null;
        FoundDummyEntry = false;
        KeptInSecCache = false;
    }
}
