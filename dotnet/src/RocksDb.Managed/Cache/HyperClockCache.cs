using System.Numerics;
using System.Runtime.CompilerServices;

namespace RocksDb.Managed.Cache;

/// <summary>Options for <see cref="HyperClockCache"/> (HyperClockCacheOptions in include/rocksdb/cache.h).</summary>
public sealed class HyperClockCacheOptions
{
    /// <summary>Total capacity in bytes of charge.</summary>
    public long Capacity { get; init; }

    /// <summary>
    /// Expected average charge of an entry (e.g. the block size): the fixed-size table is sized as
    /// capacity / (estimated_entry_charge * 0.7) slots (FixedHyperClockTable::CalcHashBits). 0 (the upstream default)
    /// selects the automatically resizing table (AutoHyperClockTable), which starts small and grows with the number of
    /// entries up to capacity / <see cref="MinAvgEntryCharge"/> slots.
    /// </summary>
    public long EstimatedEntryCharge { get; init; }

    /// <summary>
    /// min_avg_entry_charge of the automatically resizing table (<see cref="EstimatedEntryCharge"/> = 0): the smallest
    /// average entry charge the table can grow to accommodate. Default 450 (upstream).
    /// </summary>
    public long MinAvgEntryCharge { get; init; } = 450;

    /// <summary>log2 of the shard count; -1 picks the default with a 32 MiB minimum shard size.</summary>
    public int NumShardBits { get; init; } = -1;

    public bool StrictCapacityLimit { get; init; }

    /// <summary>
    /// Metadata charging: with <see cref="CacheMetadataChargePolicy.FullCharge"/> the slot table itself
    /// (<see cref="HyperClockCache.SlotMetadataCharge"/> bytes per slot, the size of upstream's cache-line sized
    /// HandleImpl) is charged to the usage when the cache is created, and entries are charged their own charge only.
    /// </summary>
    public CacheMetadataChargePolicy MetadataChargePolicy { get; init; } = CacheMetadataChargePolicy.FullCharge;

    /// <summary>
    /// eviction_effort_cap: when nearly all entries are pinned (referenced), an eviction gives up once it has seen this
    /// many pinned entries per entry it could free (plus a start-up allowance), and the cache may exceed its capacity
    /// instead of burning CPU (non-strict limit only; a strict capacity limit implies no cap). Values below 1 are
    /// treated as 1. Default 30.
    /// </summary>
    public int EvictionEffortCap { get; init; } = 30;

    /// <summary>
    /// Secondary cache consulted on misses and fed with evicted entries (ShardedCacheOptions::secondary_cache); honored by
    /// <see cref="BlockCache.CreateHyperClock"/>, which wraps the cache in a secondary cache adapter.
    /// </summary>
    public SecondaryCache? SecondaryCache { get; init; }
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

    /// <summary>sizeof(FixedHyperClockTable::HandleImpl): the per-slot metadata charge (static_assert'ed to 64 upstream).</summary>
    public const int SlotMetadataCharge = 64;

    private readonly ClockShard[] _shards;
    private readonly uint _shardMask;
    private long _capacity;
    private bool _strict;

    public HyperClockCache(HyperClockCacheOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentOutOfRangeException.ThrowIfNegative(options.Capacity);
        ArgumentOutOfRangeException.ThrowIfNegative(options.EstimatedEntryCharge);
        if (options.EstimatedEntryCharge == 0) ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(options.MinAvgEntryCharge, 0);
        int bits = options.NumShardBits >= 0 ? options.NumShardBits : GetDefaultCacheShardBits(options.Capacity, MinShardSize);
        if (bits > 19) ThrowHelper.InvalidArgument("num_shard_bits too large");
        int numShards = 1 << bits;
        _shardMask = (uint)(numShards - 1);
        _capacity = options.Capacity;
        _strict = options.StrictCapacityLimit;
        long perShard = (options.Capacity + numShards - 1) / numShards;
        _shards = new ClockShard[numShards];
        for (int i = 0; i < numShards; i++)
            _shards[i] = new ClockShard(this, perShard, options.EstimatedEntryCharge, options.MinAvgEntryCharge, options.StrictCapacityLimit,
                options.MetadataChargePolicy, options.EvictionEffortCap);
    }

    public override string Name => "HyperClockCache";

    // Hit bits are only recorded while an eviction callback is installed (a plain field, read on every hit).
    private bool _trackHits;

    internal override void SetEvictionHandler(CacheEvictionHandler? handler)
    {
        base.SetEvictionHandler(handler);
        Volatile.Write(ref _trackHits, handler is not null);
    }

    /// <summary>The tables resize automatically (estimated_entry_charge = 0).</summary>
    public bool IsAutoResizing => _shards[0].IsAuto;

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
            return sum + StandaloneUsage;
        }
    }

    public override long PinnedUsage
    {
        get
        {
            long sum = 0;
            foreach (ClockShard s in _shards) sum += s.ComputePinnedUsage();
            return sum + StandaloneUsage;
        }
    }

    /// <summary>Number of evictions that stopped because the eviction effort cap was exceeded (all shards).</summary>
    public long EvictionEffortExceededCount
    {
        get
        {
            long sum = 0;
            foreach (ClockShard s in _shards) sum += s.EvictionEffortExceededCount;
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
        // Record the hit for the eviction callback (was_hit), only when one is installed (as upstream).
        if (_trackHits && (s.LoadMeta() & ClockSlot.HitBit) == 0) Interlocked.Or(ref s.Meta, unchecked((long)ClockSlot.HitBit));
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

    /// <summary>ShardedCache::ApplyToAllEntries: a slice of each shard per round, rotating between shards.</summary>
    public override void ApplyToAllEntries<TState>(TState state, CacheEntryVisitor<TState> callback, int averageEntriesPerLock)
    {
        ArgumentNullException.ThrowIfNull(callback);
        int aepl = Math.Max(averageEntriesPerLock, 1);
        Span<long> states = _shards.Length <= 64 ? stackalloc long[_shards.Length] : new long[_shards.Length];
        states.Clear();
        bool remainingWork;
        do
        {
            remainingWork = false;
            for (int i = 0; i < _shards.Length; i++)
            {
                if (states[i] == long.MaxValue) continue;
                _shards[i].ApplyToSomeEntries(state, callback, aepl, ref states[i]);
                remainingWork |= states[i] != long.MaxValue;
            }
        } while (remainingWork);
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

    /// <summary>The slot array with its derived sizes; replaced as a whole when an automatic table grows.</summary>
    /// <para>
    /// A table's slot objects are created by the first insert into it (<see cref="ClockShard.MaterializeAll"/>), all at
    /// once so that they lie together in memory like the slots of upstream's array. Until then (and for positions an
    /// insert reaches while another thread is still filling the table) an element is null: an empty slot that no probe
    /// sequence passes over (an insert materializes every slot it probes before changing it), so lookups treat it as the
    /// end of a probe and scans skip it.
    /// </para>
    internal sealed class Table
    {
        public readonly ClockSlot?[] Array;
        public readonly int LengthBits;
        public readonly int Mask;
        /// <summary>Occupancy above which an insert must evict (kStrictLoadFactor).</summary>
        public readonly long OccupancyLimit;
        /// <summary>Occupancy above which an automatic table grows (kLoadFactor); long.MaxValue when it cannot grow.</summary>
        public readonly long GrowLimit;
        /// <summary>1 once every element holds a slot object (see <see cref="ClockShard.MaterializeAll"/>).</summary>
        public int Materialized;

        public Table(ClockSlot?[] array, int lengthBits, bool canGrow)
        {
            Array = array;
            LengthBits = lengthBits;
            Mask = array.Length - 1;
            OccupancyLimit = (long)(array.Length * StrictLoadFactor);
            GrowLimit = canGrow ? (long)(array.Length * LoadFactor) : long.MaxValue;
        }
    }

    private Table _t;
    // Automatic resizing (AutoHyperClockTable): the table doubles while it is fuller than LoadFactor and has not reached
    // its maximum size. Growing moves the slot objects to a new array; operations that change the probe structure
    // (insert, evict, erase, freeing on release) hold a mutator count, and the grower waits for it to drain.
    private readonly bool _auto;
    private readonly int _maxLengthBits;
    private readonly CacheMetadataChargePolicy _policy;
    private int _mutators;
    private int _resizing;
    private readonly uint _evictionEffortCap;
    private long _evictionEffortExceededCount;
    private long _capacity;
    private bool _strict;
    private long _usage;
    private long _standaloneUsage;
    private long _occupancy;
    private long _clockPointer;

    /// <summary>Initial size of an automatically resizing table (log2 slots).</summary>
    private const int AutoInitialLengthBits = 6;

    private readonly HyperClockCache? _owner;

    public ClockShard(HyperClockCache? owner, long capacity, long estimatedEntryCharge, long minAvgEntryCharge, bool strict,
        CacheMetadataChargePolicy policy, int evictionEffortCap)
    {
        _owner = owner;
        _capacity = capacity;
        _strict = strict;
        _policy = policy;
        // SanitizeEvictionEffortCap
        _evictionEffortCap = (uint)Math.Max(1, evictionEffortCap);
        int lengthBits;
        if (estimatedEntryCharge == 0)
        {
            // AutoHyperClockTable: the maximum size is what the minimum average entry charge would need.
            _auto = true;
            _maxLengthBits = CalcHashBits(capacity, minAvgEntryCharge, policy);
            lengthBits = Math.Min(AutoInitialLengthBits, _maxLengthBits);
        }
        else
        {
            lengthBits = CalcHashBits(capacity, estimatedEntryCharge, policy);
            _maxLengthBits = lengthBits;
        }
        // Slot objects are created by the first insert (MaterializeAll): a cache that is never used (e.g. the default
        // cache of a DB that is opened and closed without reads) does not pay for one slot object per table position.
        var array = new ClockSlot?[1 << lengthBits];
        _t = new Table(array, lengthBits, _auto && lengthBits < _maxLengthBits);
        // FixedHyperClockTable::FixedHyperClockTable: the table's metadata is charged up front (an automatic table charges
        // the slots it has grown to).
        if (policy == CacheMetadataChargePolicy.FullCharge) _usage = (long)array.Length * HyperClockCache.SlotMetadataCharge;
    }

    public bool IsAuto => _auto;

    // ---- automatic resizing ----

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void EnterMutation()
    {
        if (!_auto) return;
        while (true)
        {
            Interlocked.Increment(ref _mutators);
            if (Volatile.Read(ref _resizing) == 0) return;
            Interlocked.Decrement(ref _mutators);
            var spin = new SpinWait();
            while (Volatile.Read(ref _resizing) != 0) spin.SpinOnce();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ExitMutation()
    {
        if (_auto) Interlocked.Decrement(ref _mutators);
    }

    /// <summary>
    /// Doubles an automatic table (AutoHyperClockTable::Grow, simplified): waits for in-flight mutators, moves every
    /// occupied slot object to its probe position in a twice as large array (recomputing displacements) and publishes
    /// the new array. Lookups keep running on the old array meanwhile (they may miss an entry, never find a wrong one).
    /// REQUIRES: the caller does not hold a mutator count.
    /// </summary>
    private void Grow(Table expected)
    {
        if (Interlocked.CompareExchange(ref _resizing, 1, 0) != 0)
        {
            // Another thread grows the table: wait for it (the caller re-checks).
            var wait = new SpinWait();
            while (Volatile.Read(ref _resizing) != 0) wait.SpinOnce();
            return;
        }
        try
        {
            var spin = new SpinWait();
            while (Volatile.Read(ref _mutators) != 0) spin.SpinOnce();
            Table old = _t;
            if (old != expected || old.LengthBits >= _maxLengthBits) return;
            int newBits = old.LengthBits + 1;
            var array = new ClockSlot?[1 << newBits];
            int mask = array.Length - 1;
            // Occupied slots move to their probe position; the positions they pass over hold occupied slots (whose
            // displacements are recomputed); every other position stays unmaterialized (empty slot objects are dropped).
            foreach (ClockSlot? h in old.Array)
            {
                if (h is not null) Volatile.Write(ref h.Displacements, 0);
            }
            foreach (ClockSlot? h in old.Array)
            {
                if (h is null || ClockSlot.State(h.LoadMeta()) == ClockSlot.StateEmpty) continue;
                int current = (int)(h.H1 >> 32) & mask, inc = ((int)h.H2 & mask) | 1;
                while (array[current] is ClockSlot passed)
                {
                    passed.Displacements++;
                    current = (current + inc) & mask;
                }
                array[current] = h;
            }
            if (_policy == CacheMetadataChargePolicy.FullCharge)
            {
                Interlocked.Add(ref _usage, (long)(array.Length - old.Array.Length) * HyperClockCache.SlotMetadataCharge);
            }
            Volatile.Write(ref _t, new Table(array, newBits, newBits < _maxLengthBits));
        }
        finally
        {
            Volatile.Write(ref _resizing, 0);
        }
    }

    public long EvictionEffortExceededCount => Volatile.Read(ref _evictionEffortExceededCount);

    public int TableSize => Volatile.Read(ref _t).Array.Length;
    public long Usage => Volatile.Read(ref _usage);
    public long Occupancy => Volatile.Read(ref _occupancy);

    /// <summary>
    /// Port of FixedHyperClockTable::CalcHashBits. With full metadata charging, for very small estimated value sizes
    /// the table could overshoot the capacity: the table is shrunk until its metadata fits. The managed port also caps
    /// the table at 2^28 slots (array size).
    /// </summary>
    internal static int CalcHashBits(long capacity, long estimatedEntryCharge, CacheMetadataChargePolicy policy)
    {
        double averageSlotCharge = estimatedEntryCharge * LoadFactor;
        if (policy == CacheMetadataChargePolicy.FullCharge) averageSlotCharge += HyperClockCache.SlotMetadataCharge;
        ulong numSlots = (ulong)(capacity / averageSlotCharge + 0.999999);
        int hashBits = numSlots == 0 ? 0 : BitOperations.Log2((numSlots << 1) - 1);
        if (policy == CacheMetadataChargePolicy.FullCharge)
        {
            // For very small estimated value sizes, it's possible to overshoot.
            while (hashBits > 0 && ((ulong)HyperClockCache.SlotMetadataCharge << hashBits) > (ulong)capacity) hashBits--;
        }
        return Math.Min(hashBits, 28);
    }

    public void SetCapacity(long capacity) => Volatile.Write(ref _capacity, capacity);

    public void SetStrictCapacityLimit(bool strict) => Volatile.Write(ref _strict, strict);

    // ---- probing ----

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int Base(Table t, ulong h1) => (int)(h1 >> 32) & t.Mask;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int Increment(Table t, ulong h2) => ((int)h2 & t.Mask) | 1;

    /// <summary>Undo displacement increments on the probe path from the base up to (excluding) <paramref name="stop"/>.</summary>
    private static void Rollback(Table t, ulong h1, ulong h2, ClockSlot stop)
    {
        int current = Base(t, h1), inc = Increment(t, h2);
        // Every slot on an inserted entry's probe path was materialized by that insert.
        while (t.Array[current] != stop)
        {
            Interlocked.Decrement(ref t.Array[current]!.Displacements);
            current = (current + inc) & t.Mask;
        }
    }

    private static void RollbackCount(Table t, ulong h1, ulong h2, int count)
    {
        int current = Base(t, h1), inc = Increment(t, h2);
        for (int i = 0; i < count; i++)
        {
            Interlocked.Decrement(ref t.Array[current]!.Displacements);
            current = (current + inc) & t.Mask;
        }
    }

    /// <summary>
    /// Creates the slot objects of every still empty position of <paramref name="t"/> in one pass (first insert into the
    /// table), so they are allocated together and stay close in memory, as eagerly created slots were. Elements another
    /// thread materialized meanwhile (<see cref="Materialize"/>) are kept.
    /// </summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    internal void MaterializeAll(Table t)
    {
        lock (t)
        {
            if (t.Materialized != 0) return;
            ClockSlot?[] array = t.Array;
            for (int i = 0; i < array.Length; i++)
            {
                if (array[i] is null) Interlocked.CompareExchange(ref array[i], new ClockSlot(this, standalone: false), null);
            }
            Volatile.Write(ref t.Materialized, 1);
        }
    }

    /// <summary>
    /// Creates the slot object of table position <paramref name="index"/> if an insert probes it before the table is
    /// fully materialized (a fallback: inserts materialize the whole table first). Racing inserts agree on one object
    /// (CAS); the loser's object is dropped.
    /// </summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private ClockSlot Materialize(ClockSlot?[] array, int index)
    {
        var s = new ClockSlot(this, standalone: false);
        return Interlocked.CompareExchange(ref array[index], s, null) ?? s;
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
        Table t = Volatile.Read(ref _t);
        int current = Base(t, h1), inc = Increment(t, h2);
        for (int probe = 0; probe <= t.Mask; probe++)
        {
            ClockSlot? h = t.Array[current];
            if (h is null) return null; // never materialized: empty, no probe passes over it
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
            current = (current + inc) & t.Mask;
        }
        return null;
    }

    // ---- insert ----

    public ClockSlot? Insert(in CacheKey key, ulong h1, ulong h2, in CacheValue value, long charge, CachePriority priority, bool wantHandle)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(charge);
        if (_auto)
        {
            // Grow instead of evicting while entries are smaller than the table was sized for (within capacity).
            while (true)
            {
                Table t0 = Volatile.Read(ref _t);
                if (Volatile.Read(ref _occupancy) + 1 <= t0.GrowLimit || Volatile.Read(ref _usage) + charge > Volatile.Read(ref _capacity)) break;
                Grow(t0);
            }
        }
        EnterMutation();
        try
        {
            return InsertCore(key, h1, h2, value, charge, priority, wantHandle);
        }
        finally
        {
            ExitMutation();
        }
    }

    private ClockSlot? InsertCore(in CacheKey key, ulong h1, ulong h2, in CacheValue value, long charge, CachePriority priority, bool wantHandle)
    {
        // ClockCacheShard::Insert: total_charge = charge (the slot metadata is charged with the table).
        long totalCharge = charge;
        ulong initialCountdown = priority switch
        {
            CachePriority.High => ClockSlot.HighCountdown,
            CachePriority.Low => ClockSlot.LowCountdown,
            _ => ClockSlot.BottomCountdown,
        };

        // Optimistically assume available occupancy.
        long oldOccupancy = Interlocked.Increment(ref _occupancy) - 1;
        bool needEvictForOccupancy = oldOccupancy + 1 > Volatile.Read(ref _t).OccupancyLimit;
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
        Table t = Volatile.Read(ref _t);
        if (Volatile.Read(ref t.Materialized) == 0) MaterializeAll(t);
        int current = Base(t, h1), inc = Increment(t, h2);
        int probe = 0;
        bool alreadyMatches = false;
        for (; probe <= t.Mask; probe++)
        {
            ClockSlot h = t.Array[current] ?? Materialize(t.Array, current);
            if (TryInsert(h, key, h1, h2, value, totalCharge, initialCountdown, takeRef, ref alreadyMatches)) return h;
            if (alreadyMatches) break;
            Interlocked.Increment(ref h.Displacements);
            current = (current + inc) & t.Mask;
        }
        RollbackCount(t, h1, h2, probe);
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
            // Took ownership of an available slot: save data fields, then publish (FinishSlotInsert: the acquire counter
            // is the initial countdown and a kept reference lowers the release counter, so releasing the insert's handle
            // leaves the countdown at its initial value instead of counting as a use).
            h.SetValue(value);
            h.Key = key;
            h.H1 = h1;
            h.H2 = h2;
            h.TotalCharge = totalCharge;
            ulong meta = (ClockSlot.StateVisible << ClockSlot.StateShift)
                | (initialCountdown << ClockSlot.AcquireCounterShift)
                | ((initialCountdown - (takeRef ? 1UL : 0)) << ClockSlot.ReleaseCounterShift);
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
        ulong seenPinnedCount = 0;
        ulong oldClockPointer = unchecked((ulong)Interlocked.Add(ref _clockPointer, stepSize) - stepSize);
        // Cap the eviction effort at this thread (along with those operating in parallel) circling through the whole
        // structure kMaxCountdown times.
        Table t = Volatile.Read(ref _t);
        ulong maxClockPointer = oldClockPointer + (ClockSlot.MaxCountdown << t.LengthBits);
        // EecAndScl::GetEffectiveEvictionEffortCap: strict_capacity_limit (the top bit) implies an infinite cap.
        ulong effectiveCap = Volatile.Read(ref _strict) ? _evictionEffortCap | 0x80000000u : _evictionEffortCap;
        CacheEvictionHandler? handler = _owner?.EvictionHandler;
        while (true)
        {
            for (int i = 0; i < stepSize; i++)
            {
                ClockSlot? h = t.Array[(int)((oldClockPointer + (ulong)i) & (ulong)t.Mask)];
                if (h is not null && ClockUpdate(h, ref seenPinnedCount))
                {
                    freedCharge += h.TotalCharge;
                    freedCount++;
                    // eviction_callback_(key, handle, was_hit) while this thread owns the slot, before the value is freed.
                    if (handler is not null) handler.OnEvicted(h.Key, h, (h.LoadMeta() & ClockSlot.HitBit) != 0);
                    Rollback(t, h.H1, h.H2, h);
                    FreeDataMarkEmpty(h);
                }
            }
            if (freedCharge >= requestedCharge) return;
            if (oldClockPointer >= maxClockPointer) return;
            // BaseClockTable::IsEvictionEffortExceeded: the ratio of useful effort to wasted effort is too low, with a
            // start-up allowance for wasted effort before any useful effort.
            if (((ulong)freedCount + 1) * effectiveCap <= seenPinnedCount)
            {
                Interlocked.Increment(ref _evictionEffortExceededCount);
                return;
            }
            oldClockPointer = unchecked((ulong)Interlocked.Add(ref _clockPointer, stepSize) - stepSize);
        }
    }

    // ClockUpdate: age unreferenced visible entries; take ownership of expired or invisible unreferenced ones.
    // Referenced (pinned) entries are counted in seenPinnedCount (EvictionData::seen_pinned_count).
    private static bool ClockUpdate(ClockSlot h, ref ulong seenPinnedCount)
    {
        ulong meta = h.LoadMeta();
        if (((meta >> ClockSlot.StateShift) & ClockSlot.StateShareableBit) == 0) return false;
        ulong acquireCount = (meta >> ClockSlot.AcquireCounterShift) & ClockSlot.CounterMask;
        ulong releaseCount = (meta >> ClockSlot.ReleaseCounterShift) & ClockSlot.CounterMask;
        if (acquireCount != releaseCount)
        {
            // Only clock update entries with no outstanding refs.
            seenPinnedCount++;
            return false;
        }
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
            EnterMutation();
            try
            {
                ReleaseErase(h, oldMeta);
            }
            finally
            {
                ExitMutation();
            }
            return;
        }
        CorrectNearOverflow(oldMeta, h);
    }

    // The erase part of Release: free the entry if this was the last reference. REQUIRES: mutator count.
    private void ReleaseErase(ClockSlot h, ulong oldMeta)
    {
        {
            oldMeta += ClockSlot.ReleaseIncrement;
            // Take ownership if no refs.
            while (true)
            {
                if (ClockSlot.Refcount(oldMeta) != 0)
                {
                    // Not last ref at some point during this Release call: correct for possible (but rare) overflow.
                    CorrectNearOverflow(oldMeta, h);
                    return;
                }
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
            Rollback(Volatile.Read(ref _t), h.H1, h.H2, h);
            FreeDataMarkEmpty(h);
            Interlocked.Add(ref _usage, -totalCharge);
            Interlocked.Decrement(ref _occupancy);
        }
    }

    public bool Erase(in CacheKey key, ulong h1, ulong h2)
    {
        EnterMutation();
        try
        {
            return EraseCore(key, h1, h2);
        }
        finally
        {
            ExitMutation();
        }
    }

    private bool EraseCore(in CacheKey key, ulong h1, ulong h2)
    {
        bool found = false;
        Table t = Volatile.Read(ref _t);
        int current = Base(t, h1), inc = Increment(t, h2);
        for (int probe = 0; probe <= t.Mask; probe++)
        {
            ClockSlot? h = t.Array[current];
            if (h is null) break; // never materialized: empty, no probe passes over it
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
                            Rollback(t, h1, h2, h);
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
            current = (current + inc) & t.Mask;
        }
        return found;
    }

    /// <summary>
    /// ClockCacheShard::ApplyToSomeEntries + ConstApplyToEntriesRange: the state is a slot index (long.MaxValue when done).
    /// Each visible slot is pinned with an optimistic acquire for the duration of the callback, then released like a
    /// handle (so an entry erased meanwhile is freed by this release, as by any last reference).
    /// </summary>
    public void ApplyToSomeEntries<TState>(TState state, CacheEntryVisitor<TState> callback, int averageEntriesPerLock, ref long position)
    {
        Table t = Volatile.Read(ref _t);
        long length = t.Array.Length;
        long indexBegin = position;
        long indexEnd = indexBegin + averageEntriesPerLock;
        if (indexEnd >= length)
        {
            // Going to end.
            indexEnd = length;
            position = long.MaxValue;
        }
        else
        {
            position = indexEnd;
        }
        for (long i = indexBegin; i < indexEnd; i++)
        {
            ClockSlot? h = t.Array[i];
            if (h is null) continue;
            ulong oldMeta = h.LoadMeta();
            // Check if it's an entry visible to lookups.
            if (ClockSlot.State(oldMeta) != ClockSlot.StateVisible) continue;
            // Increment acquire counter; check whether we actually acquired a reference.
            oldMeta = h.FetchAdd(ClockSlot.AcquireIncrement);
            ulong st = ClockSlot.State(oldMeta);
            if ((st & ClockSlot.StateShareableBit) == 0) continue; // other states: the increment has no effect
            var handle = new CachedBlock(h);
            try
            {
                if (st == ClockSlot.StateVisible)
                {
                    CacheValue v = h.GetValue();
                    callback(state, h.Key, v, h.TotalCharge, h.Helper);
                }
            }
            finally
            {
                handle.Release(false);
            }
        }
    }

    // EraseUnRefEntries: take ownership of every unreferenced shareable slot.
    public void EraseUnRefEntries()
    {
        EnterMutation();
        try
        {
            EraseUnRefEntriesCore();
        }
        finally
        {
            ExitMutation();
        }
    }

    private void EraseUnRefEntriesCore()
    {
        Table t = Volatile.Read(ref _t);
        foreach (ClockSlot? h in t.Array)
        {
            if (h is null) continue;
            ulong meta = h.LoadMeta();
            if ((ClockSlot.State(meta) & ClockSlot.StateShareableBit) != 0 && ClockSlot.Refcount(meta) == 0
                && h.Cas(meta, ClockSlot.StateConstruction << ClockSlot.StateShift))
            {
                long totalCharge = h.TotalCharge;
                Rollback(t, h.H1, h.H2, h);
                FreeDataMarkEmpty(h);
                Interlocked.Add(ref _usage, -totalCharge);
                Interlocked.Decrement(ref _occupancy);
            }
        }
    }

    public long ComputePinnedUsage()
    {
        long pinned = 0;
        foreach (ClockSlot? h in Volatile.Read(ref _t).Array)
        {
            if (h is null) continue;
            ulong meta = h.LoadMeta();
            if ((ClockSlot.State(meta) & ClockSlot.StateShareableBit) != 0 && ClockSlot.Refcount(meta) > 0)
            {
                pinned += h.TotalCharge;
                // ClockCacheShard::GetPinnedUsage: with full metadata charging a pinned entry also pins its slot.
                if (_policy == CacheMetadataChargePolicy.FullCharge) pinned += HyperClockCache.SlotMetadataCharge;
            }
        }
        return pinned + Volatile.Read(ref _standaloneUsage);
    }
}
