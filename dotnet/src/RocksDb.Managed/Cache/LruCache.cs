using System.Runtime.CompilerServices;

namespace RocksDb.Managed.Cache;

/// <summary>Options for <see cref="BlockCache.CreateLru(LruCacheOptions)"/> (LRUCacheOptions in include/rocksdb/cache.h).</summary>
public sealed class LruCacheOptions
{
    /// <summary>Total capacity in bytes of charge.</summary>
    public long Capacity { get; init; }

    /// <summary>log2 of the shard count; -1 picks <see cref="BlockCache.GetDefaultCacheShardBits"/> with 512 KiB min shard size.</summary>
    public int NumShardBits { get; init; } = -1;

    /// <summary>Fail inserts (with handle) instead of exceeding capacity when everything is pinned.</summary>
    public bool StrictCapacityLimit { get; init; }

    /// <summary>Fraction of capacity reserved for high-priority entries (default 0.5).</summary>
    public double HighPriPoolRatio { get; init; } = 0.5;

    /// <summary>Fraction of capacity reserved for low-priority entries (default 0.0).</summary>
    public double LowPriPoolRatio { get; init; }

    /// <summary>Whether <see cref="LruCache.EntryMetadataCharge"/> is added to each entry's charge (default full).</summary>
    public CacheMetadataChargePolicy MetadataChargePolicy { get; init; } = CacheMetadataChargePolicy.FullCharge;
}

/// <summary>
/// Sharded LRU cache. Port of cache/lru_cache.{h,cc} (LRUCacheShard, LRUHandleTable) and cache/sharded_cache.{h,cc}.
/// Each shard has its own <see cref="Lock"/>, an intrusive chained hash table and a circular LRU list with
/// high/low/bottom priority pools (midpoint insertion). The shard is chosen by the low bits of
/// <see cref="CacheKey.Hash32"/>, the table bucket by the high bits.
/// <para>
/// As in C++, values of evicted/erased entries are freed after the shard lock is released (so a value's Dispose may
/// re-enter the cache). Difference: freed entry objects are recycled through a per-shard free list (costing a
/// second short lock acquisition on eviction) so steady-state inserts do not allocate.
/// </para>
/// </summary>
public sealed class LruCache : BlockCache
{
    /// <summary>Metadata charge per entry under <see cref="CacheMetadataChargePolicy.FullCharge"/> (approx. object size).</summary>
    public const int EntryMetadataCharge = 112;

    private const long MinShardSize = 512 * 1024;

    private readonly LruCacheShard[] _shards;
    private readonly uint _shardMask;
    private long _capacity;
    private bool _strict;

    public LruCache(LruCacheOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentOutOfRangeException.ThrowIfNegative(options.Capacity);
        if (options.HighPriPoolRatio < 0 || options.LowPriPoolRatio < 0 || options.HighPriPoolRatio + options.LowPriPoolRatio > 1.0)
            ThrowHelper.InvalidArgument("invalid high/low priority pool ratio");
        int bits = options.NumShardBits >= 0 ? options.NumShardBits : GetDefaultCacheShardBits(options.Capacity, MinShardSize);
        if (bits > 19) ThrowHelper.InvalidArgument("num_shard_bits too large");
        int numShards = 1 << bits;
        _shardMask = (uint)(numShards - 1);
        _capacity = options.Capacity;
        _strict = options.StrictCapacityLimit;
        _shards = new LruCacheShard[numShards];
        long perShard = PerShardCapacity(options.Capacity, numShards);
        for (int i = 0; i < numShards; i++)
        {
            _shards[i] = new LruCacheShard(perShard, options.StrictCapacityLimit, options.HighPriPoolRatio, options.LowPriPoolRatio,
                options.MetadataChargePolicy, 32 - bits);
        }
    }

    public override string Name => "LRUCache";

    public int NumShards => _shards.Length;

    public override long Capacity => Volatile.Read(ref _capacity);

    public override bool StrictCapacityLimit => Volatile.Read(ref _strict);

    // ShardedCacheBase::ComputePerShardCapacity: ceil(capacity / num_shards).
    private static long PerShardCapacity(long capacity, int numShards) => (capacity + (numShards - 1)) / numShards;

    public override void SetCapacity(long capacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(capacity);
        Volatile.Write(ref _capacity, capacity);
        long perShard = PerShardCapacity(capacity, _shards.Length);
        foreach (LruCacheShard s in _shards) s.SetCapacity(perShard);
    }

    public override void SetStrictCapacityLimit(bool strictCapacityLimit)
    {
        Volatile.Write(ref _strict, strictCapacityLimit);
        foreach (LruCacheShard s in _shards) s.SetStrictCapacityLimit(strictCapacityLimit);
    }

    public void SetHighPriorityPoolRatio(double ratio)
    {
        foreach (LruCacheShard s in _shards) s.SetHighPriorityPoolRatio(ratio);
    }

    public void SetLowPriorityPoolRatio(double ratio)
    {
        foreach (LruCacheShard s in _shards) s.SetLowPriorityPoolRatio(ratio);
    }

    public override long Usage
    {
        get
        {
            long sum = 0;
            foreach (LruCacheShard s in _shards) sum += s.Usage;
            return sum;
        }
    }

    public override long PinnedUsage
    {
        get
        {
            long sum = 0;
            foreach (LruCacheShard s in _shards) sum += s.PinnedUsage;
            return sum;
        }
    }

    public override int Count
    {
        get
        {
            int sum = 0;
            foreach (LruCacheShard s in _shards) sum += s.Count;
            return sum;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private LruCacheShard ShardFor(uint hash) => _shards[hash & _shardMask];

    public override bool TryLookup(in CacheKey key, out CachedBlock block)
    {
        uint hash = key.Hash32();
        LruEntry? e = ShardFor(hash).Lookup(key, hash);
        if (e is null)
        {
            block = default;
            return false;
        }
        block = new CachedBlock(e);
        return true;
    }

    public override bool TryInsert(in CacheKey key, in CacheValue value, long charge, CachePriority priority, out CachedBlock block)
    {
        uint hash = key.Hash32();
        LruEntry? e = ShardFor(hash).Insert(key, hash, value, charge, priority, wantHandle: true);
        if (e is null)
        {
            block = default;
            return false;
        }
        block = new CachedBlock(e);
        return true;
    }

    public override bool Insert(in CacheKey key, in CacheValue value, long charge, CachePriority priority)
    {
        uint hash = key.Hash32();
        return ShardFor(hash).Insert(key, hash, value, charge, priority, wantHandle: false) is not null;
    }

    public override bool Erase(in CacheKey key)
    {
        uint hash = key.Hash32();
        return ShardFor(hash).Erase(key, hash);
    }

    public override void EraseUnreferencedEntries()
    {
        foreach (LruCacheShard s in _shards) s.EraseUnRefEntries();
    }

    /// <summary>Test hook: the shard holding <paramref name="key"/>.</summary>
    internal LruCacheShard GetShardForTest(in CacheKey key) => ShardFor(key.Hash32());
}

/// <summary>LRUHandle: entry in the shard's hash table and (when unreferenced) LRU list.</summary>
internal sealed class LruEntry : CacheEntry
{
    // m_flags / im_flags of LRUHandle.
    internal const byte FlagInCache = 1;
    internal const byte FlagIsHighPri = 2;
    internal const byte FlagIsLowPri = 4;
    internal const byte FlagInHighPriPool = 8;
    internal const byte FlagInLowPriPool = 16;
    internal const byte FlagHasHit = 32;

    internal readonly LruCacheShard? Shard;
    internal LruEntry? Next;
    internal LruEntry? Prev;
    internal LruEntry? NextHash;
    internal LruEntry? NextFree;
    internal uint Hash;
    internal int Refs;
    internal byte Flags;

    internal LruEntry(LruCacheShard? shard) => Shard = shard;

    internal bool InCache => (Flags & FlagInCache) != 0;
    internal bool IsHighPri => (Flags & FlagIsHighPri) != 0;
    internal bool IsLowPri => (Flags & FlagIsLowPri) != 0;
    internal bool InHighPriPool => (Flags & FlagInHighPriPool) != 0;
    internal bool InLowPriPool => (Flags & FlagInLowPriPool) != 0;
    internal bool HasHit => (Flags & FlagHasHit) != 0;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void SetFlag(byte flag, bool on) => Flags = on ? (byte)(Flags | flag) : (byte)(Flags & ~flag);

    internal override void Release(int generation, bool eraseIfLastRef) => Shard!.Release(this, generation, eraseIfLastRef);

    internal override void AddRef(int generation) => Shard!.Ref(this, generation);
}

/// <summary>Port of LRUHandleTable: chained hash table indexed by the upper hash bits, resized when load ≥ 1.</summary>
internal sealed class LruHandleTable
{
    private LruEntry?[] _list;
    private int _lengthBits;
    private int _elems;
    private readonly int _maxLengthBits;

    public LruHandleTable(int maxUpperHashBits)
    {
        _lengthBits = 4;
        _list = new LruEntry?[1 << _lengthBits];
        _maxLengthBits = maxUpperHashBits;
    }

    public int Count => _elems;

    public int LengthBits => _lengthBits;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public LruEntry? Lookup(in CacheKey key, uint hash)
    {
        LruEntry? e = _list[(int)(hash >> (32 - _lengthBits))];
        while (e is not null && (e.Hash != hash || !e.Key.Equals(key))) e = e.NextHash;
        return e;
    }

    /// <summary>Inserts <paramref name="h"/>, returning the entry with the same key that it replaced (or null).</summary>
    public LruEntry? Insert(LruEntry h)
    {
        int bucket = (int)(h.Hash >> (32 - _lengthBits));
        LruEntry? prev = null;
        LruEntry? cur = _list[bucket];
        while (cur is not null && (cur.Hash != h.Hash || !cur.Key.Equals(h.Key)))
        {
            prev = cur;
            cur = cur.NextHash;
        }
        h.NextHash = cur?.NextHash;
        if (prev is null) _list[bucket] = h;
        else prev.NextHash = h;
        if (cur is null)
        {
            ++_elems;
            if ((_elems >> _lengthBits) > 0) Resize();
        }
        else
        {
            cur.NextHash = null;
        }
        return cur;
    }

    public LruEntry? Remove(in CacheKey key, uint hash)
    {
        int bucket = (int)(hash >> (32 - _lengthBits));
        LruEntry? prev = null;
        LruEntry? cur = _list[bucket];
        while (cur is not null && (cur.Hash != hash || !cur.Key.Equals(key)))
        {
            prev = cur;
            cur = cur.NextHash;
        }
        if (cur is not null)
        {
            if (prev is null) _list[bucket] = cur.NextHash;
            else prev.NextHash = cur.NextHash;
            cur.NextHash = null;
            --_elems;
        }
        return cur;
    }

    private void Resize()
    {
        if (_lengthBits >= _maxLengthBits || _lengthBits >= 30) return;
        int newBits = _lengthBits + 1;
        var newList = new LruEntry?[1 << newBits];
        foreach (LruEntry? head in _list)
        {
            LruEntry? h = head;
            while (h is not null)
            {
                LruEntry? next = h.NextHash;
                int b = (int)(h.Hash >> (32 - newBits));
                h.NextHash = newList[b];
                newList[b] = h;
                h = next;
            }
        }
        _list = newList;
        _lengthBits = newBits;
    }
}

/// <summary>Port of LRUCacheShard (cache/lru_cache.cc). All state is guarded by <see cref="_mutex"/>.</summary>
internal sealed class LruCacheShard
{
    private const int MaxFreeEntries = 1024;

    private readonly Lock _mutex = new();
    private readonly LruHandleTable _table;
    private readonly CacheMetadataChargePolicy _metadataChargePolicy;
    // Dummy head of the circular LRU list. lru_.prev is the newest entry, lru_.next the oldest.
    private readonly LruEntry _lru = new(null);
    private LruEntry _lruLowPri;
    private LruEntry _lruBottomPri;
    private long _capacity;
    private long _highPriPoolUsage;
    private long _lowPriPoolUsage;
    private bool _strictCapacityLimit;
    private double _highPriPoolRatio;
    private double _highPriPoolCapacity;
    private double _lowPriPoolRatio;
    private double _lowPriPoolCapacity;
    private long _usage;
    private long _lruUsage;
    private LruEntry? _freeList;
    private LruEntry? _pendingFree;
    private int _freeCount;

    public LruCacheShard(long capacity, bool strictCapacityLimit, double highPriPoolRatio, double lowPriPoolRatio,
        CacheMetadataChargePolicy metadataChargePolicy, int maxUpperHashBits)
    {
        _strictCapacityLimit = strictCapacityLimit;
        _highPriPoolRatio = highPriPoolRatio;
        _lowPriPoolRatio = lowPriPoolRatio;
        _metadataChargePolicy = metadataChargePolicy;
        _table = new LruHandleTable(maxUpperHashBits);
        _lru.Next = _lru;
        _lru.Prev = _lru;
        _lruLowPri = _lru;
        _lruBottomPri = _lru;
        SetCapacity(capacity);
    }

    public long Usage => Volatile.Read(ref _usage);

    public long PinnedUsage
    {
        get
        {
            lock (_mutex) return _usage - _lruUsage;
        }
    }

    public int Count
    {
        get
        {
            lock (_mutex) return _table.Count;
        }
    }

    public void SetCapacity(long capacity)
    {
        LruEntry? pending;
        lock (_mutex)
        {
            _capacity = capacity;
            _highPriPoolCapacity = _capacity * _highPriPoolRatio;
            _lowPriPoolCapacity = _capacity * _lowPriPoolRatio;
            EvictFromLru(0);
            pending = TakePendingFree();
        }
        FreePending(pending);
    }

    public void SetStrictCapacityLimit(bool strict)
    {
        lock (_mutex) _strictCapacityLimit = strict;
    }

    public void SetHighPriorityPoolRatio(double ratio)
    {
        lock (_mutex)
        {
            _highPriPoolRatio = ratio;
            _highPriPoolCapacity = _capacity * ratio;
            MaintainPoolSize();
        }
    }

    public void SetLowPriorityPoolRatio(double ratio)
    {
        lock (_mutex)
        {
            _lowPriPoolRatio = ratio;
            _lowPriPoolCapacity = _capacity * ratio;
            MaintainPoolSize();
        }
    }

    public LruEntry? Lookup(in CacheKey key, uint hash)
    {
        lock (_mutex)
        {
            LruEntry? e = _table.Lookup(key, hash);
            if (e is not null)
            {
                if (e.Refs == 0) LruRemove(e); // in LRU because it is in the table with no external refs
                e.Refs++;
                e.Flags |= LruEntry.FlagHasHit;
            }
            return e;
        }
    }

    public void Ref(LruEntry e, int generation)
    {
        lock (_mutex)
        {
            if (e.Generation != generation || e.Refs <= 0) CachedBlock.ThrowStale();
            e.Refs++;
        }
    }

    public LruEntry? Insert(in CacheKey key, uint hash, in CacheValue value, long charge, CachePriority priority, bool wantHandle)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(charge);
        LruEntry? result, pending;
        lock (_mutex)
        {
            LruEntry e = NewEntry();
            e.SetValue(value);
            e.Key = key;
            e.Hash = hash;
            e.Refs = 0;
            e.Flags = priority switch
            {
                CachePriority.High => LruEntry.FlagIsHighPri,
                CachePriority.Low => LruEntry.FlagIsLowPri,
                _ => 0,
            };
            e.TotalCharge = charge + (_metadataChargePolicy == CacheMetadataChargePolicy.FullCharge ? LruCache.EntryMetadataCharge : 0);
            e.Flags |= LruEntry.FlagInCache;
            result = InsertItem(e, wantHandle);
            pending = TakePendingFree();
        }
        FreePending(pending);
        return result;
    }

    // LRUCacheShard::InsertItem. Returns the entry (referenced) when wantHandle, else the entry if retained.
    private LruEntry? InsertItem(LruEntry e, bool wantHandle)
    {
        EvictFromLru(e.TotalCharge);
        if (_usage + e.TotalCharge > _capacity && (_strictCapacityLimit || !wantHandle))
        {
            e.SetFlag(LruEntry.FlagInCache, false);
            if (!wantHandle)
            {
                // As if inserted and evicted immediately: the cache owns and frees the value.
                FreeEntry(e);
            }
            else
            {
                // Status::MemoryLimit: caller keeps ownership of the value.
                e.Buffer = null;
                e.Object = null;
                e.ReturnBufferToPool = false;
                RecycleEntry(e);
            }
            return null;
        }
        // Insert into the cache. The cache might get larger than its capacity if not enough space was freed.
        LruEntry? old = _table.Insert(e);
        _usage += e.TotalCharge;
        if (old is not null)
        {
            old.SetFlag(LruEntry.FlagInCache, false);
            if (old.Refs == 0)
            {
                LruRemove(old);
                _usage -= old.TotalCharge;
                FreeEntry(old);
            }
        }
        if (!wantHandle)
        {
            LruInsert(e);
        }
        else
        {
            e.Refs++;
        }
        return e;
    }

    public void Release(LruEntry e, int generation, bool eraseIfLastRef)
    {
        bool mustFree;
        lock (_mutex)
        {
            if (e.Generation != generation || e.Refs <= 0) CachedBlock.ThrowStale();
            mustFree = --e.Refs == 0;
            if (mustFree && e.InCache)
            {
                // The item is still in cache, and nobody else holds a reference to it.
                if (_usage > _capacity || eraseIfLastRef)
                {
                    _table.Remove(e.Key, e.Hash);
                    e.SetFlag(LruEntry.FlagInCache, false);
                }
                else
                {
                    LruInsert(e);
                    mustFree = false;
                }
            }
            if (mustFree) _usage -= e.TotalCharge;
        }
        // Free the entry here outside of mutex for performance reasons (and so that a value's Dispose may
        // re-enter the cache).
        if (mustFree) FreePending(e);
    }

    public bool Erase(in CacheKey key, uint hash)
    {
        LruEntry? e;
        bool mustFree = false;
        lock (_mutex)
        {
            e = _table.Remove(key, hash);
            if (e is null) return false;
            e.SetFlag(LruEntry.FlagInCache, false);
            if (e.Refs == 0)
            {
                // The entry is in LRU since it's in hash and has no external references.
                LruRemove(e);
                _usage -= e.TotalCharge;
                mustFree = true;
            }
        }
        if (mustFree) FreePending(e);
        return true;
    }

    public void EraseUnRefEntries()
    {
        LruEntry? pending;
        lock (_mutex)
        {
            while (_lru.Next != _lru)
            {
                LruEntry old = _lru.Next!;
                LruRemove(old);
                _table.Remove(old.Key, old.Hash);
                old.SetFlag(LruEntry.FlagInCache, false);
                _usage -= old.TotalCharge;
                FreeEntry(old);
            }
            pending = TakePendingFree();
        }
        FreePending(pending);
    }

    private LruEntry NewEntry()
    {
        LruEntry? e = _freeList;
        if (e is not null)
        {
            _freeList = e.NextFree;
            e.NextFree = null;
            _freeCount--;
            return e;
        }
        return new LruEntry(this);
    }

    // Queues an entry that left the cache (not in table, no refs) to be freed after the mutex is released
    // (the autovector<LRUHandle*> last_reference_list of the C++ code, kept intrusive to avoid allocation).
    private void FreeEntry(LruEntry e)
    {
        e.NextFree = _pendingFree;
        _pendingFree = e;
    }

    private LruEntry? TakePendingFree()
    {
        LruEntry? p = _pendingFree;
        _pendingFree = null;
        return p;
    }

    /// <summary>Frees values of a chain of entries outside the mutex, then recycles the entry objects.</summary>
    private void FreePending(LruEntry? chain)
    {
        if (chain is null) return;
        for (LruEntry? e = chain; e is not null; e = e.NextFree) e.FreeValue();
        lock (_mutex)
        {
            LruEntry? e = chain;
            while (e is not null)
            {
                LruEntry? next = e.NextFree;
                e.NextFree = null;
                RecycleEntry(e);
                e = next;
            }
        }
    }

    private void RecycleEntry(LruEntry e)
    {
        e.Generation++;
        e.Next = e.Prev = e.NextHash = null;
        e.Flags = 0;
        if (_freeCount < MaxFreeEntries)
        {
            e.NextFree = _freeList;
            _freeList = e;
            _freeCount++;
        }
    }

    private void LruRemove(LruEntry e)
    {
        if (_lruLowPri == e) _lruLowPri = e.Prev!;
        if (_lruBottomPri == e) _lruBottomPri = e.Prev!;
        e.Next!.Prev = e.Prev;
        e.Prev!.Next = e.Next;
        e.Prev = e.Next = null;
        _lruUsage -= e.TotalCharge;
        if (e.InHighPriPool) _highPriPoolUsage -= e.TotalCharge;
        else if (e.InLowPriPool) _lowPriPoolUsage -= e.TotalCharge;
    }

    private void LruInsert(LruEntry e)
    {
        if (_highPriPoolRatio > 0 && (e.IsHighPri || e.HasHit))
        {
            // Insert e to head of LRU list.
            e.Next = _lru;
            e.Prev = _lru.Prev;
            e.Prev!.Next = e;
            e.Next.Prev = e;
            e.SetFlag(LruEntry.FlagInHighPriPool, true);
            e.SetFlag(LruEntry.FlagInLowPriPool, false);
            _highPriPoolUsage += e.TotalCharge;
            MaintainPoolSize();
        }
        else if (_lowPriPoolRatio > 0 && (e.IsHighPri || e.IsLowPri || e.HasHit))
        {
            // Insert e to the head of low-pri pool.
            e.Next = _lruLowPri.Next;
            e.Prev = _lruLowPri;
            e.Prev.Next = e;
            e.Next!.Prev = e;
            e.SetFlag(LruEntry.FlagInHighPriPool, false);
            e.SetFlag(LruEntry.FlagInLowPriPool, true);
            _lowPriPoolUsage += e.TotalCharge;
            _lruLowPri = e;
            MaintainPoolSize();
        }
        else
        {
            // Insert e to the head of bottom-pri pool.
            e.Next = _lruBottomPri.Next;
            e.Prev = _lruBottomPri;
            e.Prev.Next = e;
            e.Next!.Prev = e;
            e.SetFlag(LruEntry.FlagInHighPriPool, false);
            e.SetFlag(LruEntry.FlagInLowPriPool, false);
            // If the low-pri pool is empty, lru_low_pri_ also needs to be updated.
            if (_lruBottomPri == _lruLowPri) _lruLowPri = e;
            _lruBottomPri = e;
        }
        _lruUsage += e.TotalCharge;
    }

    private void MaintainPoolSize()
    {
        while (_highPriPoolUsage > _highPriPoolCapacity)
        {
            // Overflow last entry in high-pri pool to low-pri pool.
            _lruLowPri = _lruLowPri.Next!;
            _lruLowPri.SetFlag(LruEntry.FlagInHighPriPool, false);
            _lruLowPri.SetFlag(LruEntry.FlagInLowPriPool, true);
            _highPriPoolUsage -= _lruLowPri.TotalCharge;
            _lowPriPoolUsage += _lruLowPri.TotalCharge;
        }
        while (_lowPriPoolUsage > _lowPriPoolCapacity)
        {
            // Overflow last entry in low-pri pool to bottom-pri pool.
            _lruBottomPri = _lruBottomPri.Next!;
            _lruBottomPri.SetFlag(LruEntry.FlagInHighPriPool, false);
            _lruBottomPri.SetFlag(LruEntry.FlagInLowPriPool, false);
            _lowPriPoolUsage -= _lruBottomPri.TotalCharge;
        }
    }

    private void EvictFromLru(long charge)
    {
        while (_usage + charge > _capacity && _lru.Next != _lru)
        {
            LruEntry old = _lru.Next!;
            LruRemove(old);
            _table.Remove(old.Key, old.Hash);
            old.SetFlag(LruEntry.FlagInCache, false);
            _usage -= old.TotalCharge;
            FreeEntry(old);
        }
    }

    /// <summary>
    /// Test hook (TEST_GetLRUList + ValidateLRUList): entries from oldest to newest with pool membership
    /// (0 = bottom, 1 = low, 2 = high).
    /// </summary>
    internal List<(CacheKey Key, int Pool)> GetLruListForTest()
    {
        var result = new List<(CacheKey, int)>();
        lock (_mutex)
        {
            for (LruEntry e = _lru.Next!; e != _lru; e = e.Next!)
                result.Add((e.Key, e.InHighPriPool ? 2 : e.InLowPriPool ? 1 : 0));
        }
        return result;
    }
}
