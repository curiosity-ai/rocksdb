using System.Numerics;
using System.Runtime.CompilerServices;

namespace RocksDb.Managed.Cache;

/// <summary>Options for <see cref="HyperClockCache"/> (HyperClockCacheOptions in include/rocksdb/cache.h).</summary>
public sealed class HyperClockCacheOptions
{
    /// <summary>Total capacity in bytes of charge.</summary>
    public long Capacity { get; init; }

    /// <summary>
    /// Expected average charge of an entry (e.g. the block size). Required: the fixed-size table is sized as
    /// capacity / (estimated_entry_charge * 0.7) slots (FixedHyperClockTable::CalcHashBits).
    /// </summary>
    public long EstimatedEntryCharge { get; init; }

    /// <summary>log2 of the shard count; -1 picks the default with a 32 MiB minimum shard size.</summary>
    public int NumShardBits { get; init; } = -1;

    public bool StrictCapacityLimit { get; init; }

    public CacheMetadataChargePolicy MetadataChargePolicy { get; init; } = CacheMetadataChargePolicy.FullCharge;
}

/// <summary>
/// Lock-free clock cache. Port of the fixed-size variant of HyperClockCache (cache/clock_cache.{h,cc}:
/// FixedHyperClockTable, ClockCacheShard, BaseClockTable). Each shard is an open-addressed table of slots with
/// double hashing; each slot's state and reference counts live in one atomic 64-bit "meta" word:
/// <code>
/// | acquire counter (30) | release counter (30) | hit (1) | occupied (1) | shareable (1) | visible (1) |
/// </code>
/// Lookups take a reference optimistically with one atomic add (no locks); eviction is a CLOCK sweep with a
/// per-priority countdown (high 3, low 2, bottom 1). Unlike <see cref="LruCache"/>, inserting an existing key does
/// not replace the visible entry (as in C++).
/// </summary>
public sealed class HyperClockCache : BlockCache
{
    private const long MinShardSize = 32L * 1024 * 1024;

    private readonly ClockShard[] _shards;
    private readonly uint _shardMask;
    private long _capacity;
    private bool _strict;

    public HyperClockCache(HyperClockCacheOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentOutOfRangeException.ThrowIfNegative(options.Capacity);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(options.EstimatedEntryCharge, 0);
        int bits = options.NumShardBits >= 0 ? options.NumShardBits : GetDefaultCacheShardBits(options.Capacity, MinShardSize);
        if (bits > 19) ThrowHelper.InvalidArgument("num_shard_bits too large");
        int numShards = 1 << bits;
        _shardMask = (uint)(numShards - 1);
        _capacity = options.Capacity;
        _strict = options.StrictCapacityLimit;
        long perShard = (options.Capacity + numShards - 1) / numShards;
        _shards = new ClockShard[numShards];
        for (int i = 0; i < numShards; i++)
            _shards[i] = new ClockShard(perShard, options.EstimatedEntryCharge, options.StrictCapacityLimit, options.MetadataChargePolicy);
    }

    public override string Name => "HyperClockCache";

    public int NumShards => _shards.Length;

    /// <summary>Slots per shard (table length).</summary>
    public int TableSize => _shards[0].TableSize;

    public override long Capacity => Volatile.Read(ref _capacity);

    public override bool StrictCapacityLimit => Volatile.Read(ref _strict);

    public override void SetCapacity(long capacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(capacity);
        Volatile.Write(ref _capacity, capacity);
        long perShard = (capacity + _shards.Length - 1) / _shards.Length;
        foreach (ClockShard s in _shards) s.SetCapacity(perShard);
    }

    public override void SetStrictCapacityLimit(bool strictCapacityLimit)
    {
        Volatile.Write(ref _strict, strictCapacityLimit);
        foreach (ClockShard s in _shards) s.SetStrictCapacityLimit(strictCapacityLimit);
    }

    public override long Usage
    {
        get
        {
            long sum = 0;
            foreach (ClockShard s in _shards) sum += s.Usage;
            return sum;
        }
    }

    public override long PinnedUsage
    {
        get
        {
            long sum = 0;
            foreach (ClockShard s in _shards) sum += s.ComputePinnedUsage();
            return sum;
        }
    }

    public override int Count
    {
        get
        {
            long sum = 0;
            foreach (ClockShard s in _shards) sum += s.Occupancy;
            return (int)sum;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void HashKey(in CacheKey key, out ulong h1, out ulong h2)
    {
        h1 = Mix(key.OffsetEtc64 ^ BitOperations.RotateLeft(key.FileNumEtc64 * 0x9E3779B97F4A7C15UL, 29));
        h2 = Mix(key.FileNumEtc64 ^ BitOperations.RotateLeft(key.OffsetEtc64 * 0xC2B2AE3D27D4EB4FUL, 31) ^ 0x165667B19E3779F9UL);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong Mix(ulong x)
    {
        x ^= x >> 32;
        x *= 0xBF58476D1CE4E5B9UL;
        x ^= x >> 29;
        x *= 0x94D049BB133111EBUL;
        x ^= x >> 32;
        return x;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ClockShard ShardFor(ulong h1) => _shards[(uint)h1 & _shardMask];

    public override bool TryLookup(in CacheKey key, out CachedBlock block)
    {
        HashKey(key, out ulong h1, out ulong h2);
        ClockSlot? s = ShardFor(h1).Lookup(key, h1, h2);
        if (s is null)
        {
            block = default;
            return false;
        }
        block = new CachedBlock(s);
        return true;
    }

    public override bool TryInsert(in CacheKey key, in CacheValue value, long charge, CachePriority priority, out CachedBlock block)
    {
        HashKey(key, out ulong h1, out ulong h2);
        ClockSlot? s = ShardFor(h1).Insert(key, h1, h2, value, charge, priority, wantHandle: true);
        if (s is null)
        {
            block = default;
            return false;
        }
        block = new CachedBlock(s);
        return true;
    }

    public override bool Insert(in CacheKey key, in CacheValue value, long charge, CachePriority priority)
    {
        HashKey(key, out ulong h1, out ulong h2);
        return ShardFor(h1).Insert(key, h1, h2, value, charge, priority, wantHandle: false) is not null;
    }

    public override bool Erase(in CacheKey key)
    {
        HashKey(key, out ulong h1, out ulong h2);
        return ShardFor(h1).Erase(key, h1, h2);
    }

    public override void EraseUnreferencedEntries()
    {
        foreach (ClockShard s in _shards) s.EraseUnRefEntries();
    }
}

/// <summary>A table slot (ClockHandle) or a standalone (detached) entry.</summary>
internal sealed class ClockSlot : CacheEntry
{
    // ---- meta word layout (ClockHandle::SlotMeta) ----
    internal const int CounterNumBits = 30;
    internal const ulong CounterMask = (1UL << CounterNumBits) - 1;
    internal const int AcquireCounterShift = 0;
    internal const int ReleaseCounterShift = CounterNumBits;
    internal const ulong AcquireIncrement = 1UL << AcquireCounterShift;
    internal const ulong ReleaseIncrement = 1UL << ReleaseCounterShift;
    internal const ulong HitBit = 1UL << 60;
    internal const int StateShift = 61;
    internal const ulong StateEmpty = 0;
    internal const ulong StateConstruction = 1;  // occupied
    internal const ulong StateInvisible = 3;     // occupied | shareable
    internal const ulong StateVisible = 7;       // occupied | shareable | visible
    internal const ulong StateOccupiedBit = 1;
    internal const ulong StateShareableBit = 2;
    internal const ulong StateVisibleBit = 4;

    // Countdown clock initial values by priority.
    internal const ulong HighCountdown = 3;
    internal const ulong LowCountdown = 2;
    internal const ulong BottomCountdown = 1;
    internal const ulong MaxCountdown = HighCountdown;

    internal long Meta;            // accessed with Interlocked/Volatile only
    internal int Displacements;    // number of probe sequences passing over this slot
    internal ulong H1, H2;         // hashed key (probe base / increment)
    internal readonly ClockShard Shard;
    internal readonly bool Standalone;

    internal ClockSlot(ClockShard shard, bool standalone)
    {
        Shard = shard;
        Standalone = standalone;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static ulong State(ulong meta) => meta >> StateShift;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static ulong Refcount(ulong meta) =>
        (((meta >> AcquireCounterShift) & CounterMask) - ((meta >> ReleaseCounterShift) & CounterMask)) & CounterMask;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal ulong FetchAdd(ulong delta) => unchecked((ulong)Interlocked.Add(ref Meta, (long)delta) - delta);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal ulong FetchSub(ulong delta) => unchecked((ulong)Interlocked.Add(ref Meta, -(long)delta) + delta);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool Cas(ulong expected, ulong desired) =>
        Interlocked.CompareExchange(ref Meta, unchecked((long)desired), unchecked((long)expected)) == unchecked((long)expected);

    internal ulong LoadMeta() => unchecked((ulong)Volatile.Read(ref Meta));

    internal override void Release(int generation, bool eraseIfLastRef) => Shard.Release(this, generation, eraseIfLastRef);

    internal override void AddRef(int generation)
    {
        if (Generation != generation || Refcount(LoadMeta()) == 0) CachedBlock.ThrowStale();
        FetchAdd(AcquireIncrement);
    }
}

/// <summary>Port of ClockCacheShard&lt;FixedHyperClockTable&gt;.</summary>
internal sealed class ClockShard
{
    private const double LoadFactor = 0.7;
    private const double StrictLoadFactor = 0.84;

    private readonly ClockSlot[] _array;
    private readonly int _lengthBits;
    private readonly int _lengthMask;
    private readonly long _occupancyLimit;
    private readonly CacheMetadataChargePolicy _metadataChargePolicy;
    private long _capacity;
    private bool _strict;
    private long _usage;
    private long _standaloneUsage;
    private long _occupancy;
    private long _clockPointer;

    public ClockShard(long capacity, long estimatedEntryCharge, bool strict, CacheMetadataChargePolicy policy)
    {
        _capacity = capacity;
        _strict = strict;
        _metadataChargePolicy = policy;
        _lengthBits = CalcHashBits(capacity, estimatedEntryCharge, policy);
        _array = new ClockSlot[1 << _lengthBits];
        for (int i = 0; i < _array.Length; i++) _array[i] = new ClockSlot(this, standalone: false);
        _lengthMask = _array.Length - 1;
        _occupancyLimit = (long)(_array.Length * StrictLoadFactor);
    }

    public int TableSize => _array.Length;
    public long Usage => Volatile.Read(ref _usage);
    public long Occupancy => Volatile.Read(ref _occupancy);

    // FixedHyperClockTable::CalcHashBits
    private static int CalcHashBits(long capacity, long estimatedEntryCharge, CacheMetadataChargePolicy policy)
    {
        double averageSlotCharge = estimatedEntryCharge * LoadFactor;
        if (policy == CacheMetadataChargePolicy.FullCharge) averageSlotCharge += LruCache.EntryMetadataCharge;
        ulong numSlots = (ulong)(capacity / averageSlotCharge + 0.999999);
        int hashBits = numSlots <= 1 ? 0 : BitOperations.Log2((numSlots << 1) - 1);
        return Math.Clamp(hashBits, 1, 28);
    }

    public void SetCapacity(long capacity) => Volatile.Write(ref _capacity, capacity);

    public void SetStrictCapacityLimit(bool strict) => Volatile.Write(ref _strict, strict);

    private long MetadataCharge => _metadataChargePolicy == CacheMetadataChargePolicy.FullCharge ? LruCache.EntryMetadataCharge : 0;

    // ---- probing ----

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int Base(ulong h1) => (int)(h1 >> 32) & _lengthMask;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int Increment(ulong h2) => ((int)h2 & _lengthMask) | 1;

    /// <summary>Undo displacement increments on the probe path from the base up to (excluding) <paramref name="stop"/>.</summary>
    private void Rollback(ulong h1, ulong h2, ClockSlot stop)
    {
        int current = Base(h1), inc = Increment(h2);
        while (_array[current] != stop)
        {
            Interlocked.Decrement(ref _array[current].Displacements);
            current = (current + inc) & _lengthMask;
        }
    }

    private void RollbackCount(ulong h1, ulong h2, int count)
    {
        int current = Base(h1), inc = Increment(h2);
        for (int i = 0; i < count; i++)
        {
            Interlocked.Decrement(ref _array[current].Displacements);
            current = (current + inc) & _lengthMask;
        }
    }

    // CorrectNearOverflow: clear the top counter bits once the release counter gets large.
    private static void CorrectNearOverflow(ulong oldMeta, ClockSlot h)
    {
        const ulong topBit = 1UL << (ClockSlot.CounterNumBits - 1);
        const ulong threshold = topBit + ClockSlot.MaxCountdown;
        if (((oldMeta >> ClockSlot.ReleaseCounterShift) & ClockSlot.CounterMask) > threshold)
        {
            ulong clear = ~((topBit << ClockSlot.AcquireCounterShift) | (topBit << ClockSlot.ReleaseCounterShift));
            Interlocked.And(ref h.Meta, unchecked((long)clear));
        }
    }

    // ---- lookup ----

    public ClockSlot? Lookup(in CacheKey key, ulong h1, ulong h2)
    {
        int current = Base(h1), inc = Increment(h2);
        for (int probe = 0; probe <= _lengthMask; probe++)
        {
            ClockSlot h = _array[current];
            // (Optimistically) increment acquire counter
            ulong oldMeta = h.FetchAdd(ClockSlot.AcquireIncrement);
            ulong state = ClockSlot.State(oldMeta);
            if (state == ClockSlot.StateVisible)
            {
                // Acquired a read reference
                if (h.H1 == h1 && h.H2 == h2 && h.Key.Equals(key)) return h;
                h.FetchSub(ClockSlot.AcquireIncrement); // mismatch: pretend we never took the reference
            }
            else if (state == ClockSlot.StateInvisible)
            {
                h.FetchSub(ClockSlot.AcquireIncrement);
            }
            // For other states, incrementing the acquire counter has no effect (it is overwritten when the slot
            // becomes shareable), so we must not undo it.
            if (Volatile.Read(ref h.Displacements) == 0) return null;
            current = (current + inc) & _lengthMask;
        }
        return null;
    }

    // ---- insert ----

    public ClockSlot? Insert(in CacheKey key, ulong h1, ulong h2, in CacheValue value, long charge, CachePriority priority, bool wantHandle)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(charge);
        long totalCharge = charge + MetadataCharge;
        ulong initialCountdown = priority switch
        {
            CachePriority.High => ClockSlot.HighCountdown,
            CachePriority.Low => ClockSlot.LowCountdown,
            _ => ClockSlot.BottomCountdown,
        };

        // Optimistically assume available occupancy.
        long oldOccupancy = Interlocked.Increment(ref _occupancy) - 1;
        bool needEvictForOccupancy = oldOccupancy + 1 > _occupancyLimit;
        bool useStandalone = false;
        if (Volatile.Read(ref _strict))
        {
            if (!ChargeUsageMaybeEvictStrict(totalCharge, needEvictForOccupancy))
            {
                Interlocked.Decrement(ref _occupancy);
                if (!wantHandle) value.FreeForCache();
                return null; // MemoryLimit: with a handle the caller keeps ownership
            }
        }
        else if (!ChargeUsageMaybeEvictNonStrict(totalCharge, needEvictForOccupancy))
        {
            // Can't meet occupancy requirement.
            Interlocked.Decrement(ref _occupancy);
            if (!wantHandle)
            {
                value.FreeForCache();
                return null;
            }
            Interlocked.Add(ref _usage, totalCharge);
            useStandalone = true;
        }

        if (!useStandalone)
        {
            ClockSlot? e = DoInsert(key, h1, h2, value, totalCharge, initialCountdown, wantHandle);
            if (e is not null) return e;
            // Not inserted
            Interlocked.Decrement(ref _occupancy);
            if (!wantHandle)
            {
                // As if inserted and evicted immediately.
                Interlocked.Add(ref _usage, -totalCharge);
                value.FreeForCache();
                return null;
            }
        }

        // Standalone (detached) entry: visible only through the returned handle.
        var s = new ClockSlot(this, standalone: true);
        s.SetValue(value);
        s.Key = key;
        s.H1 = h1;
        s.H2 = h2;
        s.TotalCharge = totalCharge;
        Volatile.Write(ref s.Meta, unchecked((long)((ClockSlot.StateInvisible << ClockSlot.StateShift) | ClockSlot.AcquireIncrement)));
        Interlocked.Add(ref _standaloneUsage, totalCharge);
        return s;
    }

    private ClockSlot? DoInsert(in CacheKey key, ulong h1, ulong h2, in CacheValue value, long totalCharge, ulong initialCountdown, bool takeRef)
    {
        int current = Base(h1), inc = Increment(h2);
        int probe = 0;
        bool alreadyMatches = false;
        for (; probe <= _lengthMask; probe++)
        {
            ClockSlot h = _array[current];
            if (TryInsert(h, key, h1, h2, value, totalCharge, initialCountdown, takeRef, ref alreadyMatches)) return h;
            if (alreadyMatches) break;
            Interlocked.Increment(ref h.Displacements);
            current = (current + inc) & _lengthMask;
        }
        RollbackCount(h1, h2, probe);
        return null;
    }

    private static bool TryInsert(ClockSlot h, in CacheKey key, ulong h1, ulong h2, in CacheValue value, long totalCharge,
        ulong initialCountdown, bool takeRef, ref bool alreadyMatches)
    {
        // Optimistically transition the slot from "empty" to "under construction" (no effect on other states).
        ulong oldMeta = unchecked((ulong)Interlocked.Or(ref h.Meta, unchecked((long)(ClockSlot.StateOccupiedBit << ClockSlot.StateShift))));
        ulong oldState = ClockSlot.State(oldMeta);
        if (oldState == ClockSlot.StateEmpty)
        {
            // Took ownership of an available slot: save data fields, then publish (MarkVisible).
            h.SetValue(value);
            h.Key = key;
            h.H1 = h1;
            h.H2 = h2;
            h.TotalCharge = totalCharge;
            ulong meta = (ClockSlot.StateVisible << ClockSlot.StateShift)
                | ((initialCountdown + (takeRef ? 1UL : 0)) << ClockSlot.AcquireCounterShift)
                | (initialCountdown << ClockSlot.ReleaseCounterShift);
            Volatile.Write(ref h.Meta, unchecked((long)meta));
            return true;
        }
        if (oldState != ClockSlot.StateVisible) return false; // not usable now

        // Existing visible entry which might be a match. Acquire initialCountdown refs to read it (boosting the
        // clock state if it matches).
        oldMeta = h.FetchAdd(ClockSlot.AcquireIncrement * initialCountdown);
        ulong state = ClockSlot.State(oldMeta);
        if (state == ClockSlot.StateVisible)
        {
            if (h.H1 == h1 && h.H2 == h2 && h.Key.Equals(key))
            {
                // Match. Release in a way that boosts the clock state.
                oldMeta = h.FetchAdd(ClockSlot.ReleaseIncrement * initialCountdown);
                CorrectNearOverflow(oldMeta, h);
                alreadyMatches = true;
                return false;
            }
            h.FetchSub(ClockSlot.AcquireIncrement * initialCountdown);
        }
        else if (state == ClockSlot.StateInvisible)
        {
            h.FetchSub(ClockSlot.AcquireIncrement * initialCountdown);
        }
        return false;
    }

    private bool ChargeUsageMaybeEvictStrict(long totalCharge, bool needEvictForOccupancy)
    {
        long capacity = Volatile.Read(ref _capacity);
        if (totalCharge > capacity) return false;
        // Grab any available capacity, and free up any more required.
        long oldUsage = Volatile.Read(ref _usage);
        long newUsage;
        while (true)
        {
            newUsage = Math.Min(capacity, oldUsage + totalCharge);
            if (newUsage == oldUsage) break;
            long seen = Interlocked.CompareExchange(ref _usage, newUsage, oldUsage);
            if (seen == oldUsage) break;
            oldUsage = seen;
        }
        long needEvictCharge = oldUsage + totalCharge - newUsage;
        long requestEvictCharge = needEvictCharge;
        if (needEvictForOccupancy && requestEvictCharge == 0) requestEvictCharge = 1;
        if (requestEvictCharge > 0)
        {
            Evict(requestEvictCharge, out long freedCharge, out long freedCount);
            Interlocked.Add(ref _occupancy, -freedCount);
            if (freedCharge > needEvictCharge)
            {
                Interlocked.Add(ref _usage, -(freedCharge - needEvictCharge));
            }
            else if (freedCharge < needEvictCharge || (needEvictForOccupancy && freedCount == 0))
            {
                // Roll back to old usage minus evicted.
                Interlocked.Add(ref _usage, -(freedCharge + (newUsage - oldUsage)));
                return false;
            }
        }
        return true;
    }

    private bool ChargeUsageMaybeEvictNonStrict(long totalCharge, bool needEvictForOccupancy)
    {
        long capacity = Volatile.Read(ref _capacity);
        long oldUsage = Volatile.Read(ref _usage);
        long needEvictCharge;
        if (oldUsage + totalCharge <= capacity || totalCharge > oldUsage)
        {
            needEvictCharge = 0;
        }
        else
        {
            needEvictCharge = totalCharge;
            if (oldUsage > capacity) needEvictCharge += Math.Min(capacity / 1024, totalCharge) + 1;
        }
        long freedCharge = 0;
        if (needEvictCharge > 0 || needEvictForOccupancy)
        {
            Evict(Math.Max(needEvictCharge, 1), out freedCharge, out long freedCount);
            if (needEvictForOccupancy && freedCount == 0) return false;
            Interlocked.Add(ref _occupancy, -freedCount);
        }
        // Track new usage even if we weren't able to evict enough.
        Interlocked.Add(ref _usage, totalCharge - freedCharge);
        return true;
    }

    // FixedHyperClockTable::Evict
    private void Evict(long requestedCharge, out long freedCharge, out long freedCount)
    {
        const int stepSize = 4;
        freedCharge = 0;
        freedCount = 0;
        ulong oldClockPointer = unchecked((ulong)Interlocked.Add(ref _clockPointer, stepSize) - stepSize);
        ulong maxClockPointer = oldClockPointer + (ClockSlot.MaxCountdown << _lengthBits);
        while (true)
        {
            for (int i = 0; i < stepSize; i++)
            {
                ClockSlot h = _array[(int)((oldClockPointer + (ulong)i) & (ulong)_lengthMask)];
                if (ClockUpdate(h))
                {
                    freedCharge += h.TotalCharge;
                    freedCount++;
                    Rollback(h.H1, h.H2, h);
                    FreeDataMarkEmpty(h);
                }
            }
            if (freedCharge >= requestedCharge) return;
            if (oldClockPointer >= maxClockPointer) return;
            oldClockPointer = unchecked((ulong)Interlocked.Add(ref _clockPointer, stepSize) - stepSize);
        }
    }

    // ClockUpdate: age unreferenced visible entries; take ownership of expired or invisible unreferenced ones.
    private static bool ClockUpdate(ClockSlot h)
    {
        ulong meta = h.LoadMeta();
        if (((meta >> ClockSlot.StateShift) & ClockSlot.StateShareableBit) == 0) return false;
        ulong acquireCount = (meta >> ClockSlot.AcquireCounterShift) & ClockSlot.CounterMask;
        ulong releaseCount = (meta >> ClockSlot.ReleaseCounterShift) & ClockSlot.CounterMask;
        if (acquireCount != releaseCount) return false; // referenced
        if (ClockSlot.State(meta) == ClockSlot.StateVisible && acquireCount > 0)
        {
            // Decrement clock (not aggressively: a failed CAS just means the entry was used).
            ulong newCount = Math.Min(acquireCount - 1, ClockSlot.MaxCountdown - 1);
            ulong newMeta = (ClockSlot.StateVisible << ClockSlot.StateShift) | (meta & ClockSlot.HitBit)
                | (newCount << ClockSlot.ReleaseCounterShift) | (newCount << ClockSlot.AcquireCounterShift);
            h.Cas(meta, newMeta);
            return false;
        }
        // Remove (unreferenced invisible, or unreferenced expired visible).
        return h.Cas(meta, (ClockSlot.StateConstruction << ClockSlot.StateShift) | (meta & ClockSlot.HitBit));
    }

    // Caller owns the slot (Construction state).
    private static void FreeDataMarkEmpty(ClockSlot h)
    {
        h.FreeValue();
        h.Generation++;
        Volatile.Write(ref h.Meta, 0);
    }

    // ---- release / erase ----

    public void Release(ClockSlot h, int generation, bool eraseIfLastRef)
    {
        if (h.Generation != generation || ClockSlot.Refcount(h.LoadMeta()) == 0) CachedBlock.ThrowStale();
        ulong oldMeta = h.FetchAdd(ClockSlot.ReleaseIncrement);
        if (eraseIfLastRef || ClockSlot.State(oldMeta) == ClockSlot.StateInvisible)
        {
            oldMeta += ClockSlot.ReleaseIncrement;
            // Take ownership if no refs.
            while (true)
            {
                if (ClockSlot.Refcount(oldMeta) != 0) return; // not last ref
                if ((ClockSlot.State(oldMeta) & ClockSlot.StateShareableBit) == 0) return; // someone else took ownership
                ulong seen = unchecked((ulong)Interlocked.CompareExchange(ref h.Meta,
                    unchecked((long)(ClockSlot.StateConstruction << ClockSlot.StateShift)), unchecked((long)oldMeta)));
                if (seen == oldMeta) break;
                oldMeta = seen;
            }
            long totalCharge = h.TotalCharge;
            if (h.Standalone)
            {
                h.FreeValue();
                h.Generation++;
                Interlocked.Add(ref _standaloneUsage, -totalCharge);
                Interlocked.Add(ref _usage, -totalCharge);
                return;
            }
            Rollback(h.H1, h.H2, h);
            FreeDataMarkEmpty(h);
            Interlocked.Add(ref _usage, -totalCharge);
            Interlocked.Decrement(ref _occupancy);
            return;
        }
        CorrectNearOverflow(oldMeta, h);
    }

    public bool Erase(in CacheKey key, ulong h1, ulong h2)
    {
        bool found = false;
        int current = Base(h1), inc = Increment(h2);
        for (int probe = 0; probe <= _lengthMask; probe++)
        {
            ClockSlot h = _array[current];
            ulong oldMeta = h.FetchAdd(ClockSlot.AcquireIncrement);
            ulong state = ClockSlot.State(oldMeta);
            if (state == ClockSlot.StateVisible)
            {
                if (h.H1 == h1 && h.H2 == h2 && h.Key.Equals(key))
                {
                    found = true;
                    // Match. Set invisible.
                    const ulong visibleMask = ClockSlot.StateVisibleBit << ClockSlot.StateShift;
                    oldMeta = unchecked((ulong)Interlocked.And(ref h.Meta, unchecked((long)~visibleMask))) & ~visibleMask;
                    while (true)
                    {
                        ulong refcount = ClockSlot.Refcount(oldMeta);
                        if (refcount > 1)
                        {
                            // Not last ref: pretend we never took the reference; the last Release frees it.
                            h.FetchSub(ClockSlot.AcquireIncrement);
                            break;
                        }
                        ulong seen = unchecked((ulong)Interlocked.CompareExchange(ref h.Meta,
                            unchecked((long)(ClockSlot.StateConstruction << ClockSlot.StateShift)), unchecked((long)oldMeta)));
                        if (seen == oldMeta)
                        {
                            long totalCharge = h.TotalCharge;
                            Rollback(h1, h2, h);
                            FreeDataMarkEmpty(h);
                            Interlocked.Add(ref _usage, -totalCharge);
                            Interlocked.Decrement(ref _occupancy);
                            break;
                        }
                        oldMeta = seen;
                    }
                }
                else
                {
                    h.FetchSub(ClockSlot.AcquireIncrement);
                }
            }
            else if (state == ClockSlot.StateInvisible)
            {
                h.FetchSub(ClockSlot.AcquireIncrement);
            }
            if (Volatile.Read(ref h.Displacements) == 0) break;
            current = (current + inc) & _lengthMask;
        }
        return found;
    }

    // EraseUnRefEntries: take ownership of every unreferenced shareable slot.
    public void EraseUnRefEntries()
    {
        foreach (ClockSlot h in _array)
        {
            ulong meta = h.LoadMeta();
            if ((ClockSlot.State(meta) & ClockSlot.StateShareableBit) != 0 && ClockSlot.Refcount(meta) == 0
                && h.Cas(meta, ClockSlot.StateConstruction << ClockSlot.StateShift))
            {
                long totalCharge = h.TotalCharge;
                Rollback(h.H1, h.H2, h);
                FreeDataMarkEmpty(h);
                Interlocked.Add(ref _usage, -totalCharge);
                Interlocked.Decrement(ref _occupancy);
            }
        }
    }

    public long ComputePinnedUsage()
    {
        long pinned = 0;
        foreach (ClockSlot h in _array)
        {
            ulong meta = h.LoadMeta();
            if ((ClockSlot.State(meta) & ClockSlot.StateShareableBit) != 0 && ClockSlot.Refcount(meta) > 0)
                pinned += h.TotalCharge;
        }
        return pinned + Volatile.Read(ref _standaloneUsage);
    }
}
