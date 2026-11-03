using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace RocksDb.Managed.Cache;

/// <summary>Insertion priority. Values mirror rocksdb::Cache::Priority (include/rocksdb/advanced_cache.h).</summary>
public enum CachePriority : byte
{
    /// <summary>Inserted at the head of the high-priority pool (index/filter blocks when cache_index_and_filter_blocks_with_high_priority).</summary>
    High = 0,
    /// <summary>Inserted at the head of the low-priority pool (data blocks; the default).</summary>
    Low = 1,
    /// <summary>Inserted at the head of the bottom-priority pool (e.g. blob values).</summary>
    Bottom = 2,
}

/// <summary>Whether per-entry bookkeeping overhead counts against capacity (CacheMetadataChargePolicy).</summary>
public enum CacheMetadataChargePolicy : byte
{
    DontCharge = 0,
    FullCharge = 1,
}

/// <summary>
/// A value handed to the cache. Ownership passes to the cache on successful insertion (and always for
/// <see cref="BlockCache.Insert"/>); when the entry is freed (evicted/erased and unreferenced) the cache returns
/// <see cref="Buffer"/> to <see cref="ArrayPool{T}.Shared"/> if <see cref="ReturnBufferToPool"/> is set and disposes
/// <see cref="Object"/> if it implements <see cref="IDisposable"/> (the CacheItemHelper::del_cb equivalent).
/// <para>
/// Typical use by the table reader: <c>CacheValue.FromPooledBuffer(rented, blockLength)</c> for raw block bytes, or
/// <c>new CacheValue(rented, len, parsedBlock, true)</c> to attach a parsed representation (restart array, filter
/// reader...) that should live exactly as long as the bytes.
/// </para>
/// </summary>
public readonly struct CacheValue
{
    public readonly byte[]? Buffer;
    public readonly int Length;
    public readonly object? Object;
    public readonly bool ReturnBufferToPool;
    /// <summary>
    /// The entry's role and secondary-cache callbacks (Cache::CacheItemHelper); null: role <see cref="CacheEntryRole.Misc"/>,
    /// never saved to a secondary cache.
    /// </summary>
    public readonly CacheItemHelper? Helper;

    public CacheValue(byte[]? buffer, int length, object? obj, bool returnBufferToPool)
    {
        if (buffer is null ? length != 0 : (uint)length > (uint)buffer.Length) ThrowHelper.InvalidArgument("length out of range");
        Buffer = buffer;
        Length = length;
        Object = obj;
        ReturnBufferToPool = returnBufferToPool && buffer is not null;
        Helper = null;
    }

    private CacheValue(in CacheValue v, CacheItemHelper? helper)
    {
        Buffer = v.Buffer;
        Length = v.Length;
        Object = v.Object;
        ReturnBufferToPool = v.ReturnBufferToPool;
        Helper = helper;
    }

    /// <summary>This value with <paramref name="helper"/> attached (see <see cref="Helper"/>).</summary>
    public CacheValue WithHelper(CacheItemHelper? helper) => new(this, helper);

    /// <summary>A placeholder value (no bytes, no object): a memory reservation (CacheReservationManager dummy entry).</summary>
    public bool IsPlaceholder => Buffer is null && Object is null;

    /// <summary>Bytes rented from <see cref="ArrayPool{T}.Shared"/>; returned to the pool when the entry is freed.</summary>
    public static CacheValue FromPooledBuffer(byte[] buffer, int length) => new(buffer, length, null, true);

    /// <summary>Bytes the cache must not return to any pool.</summary>
    public static CacheValue FromBuffer(byte[] buffer, int length) => new(buffer, length, null, false);

    /// <summary>An arbitrary object (disposed on free if <see cref="IDisposable"/>).</summary>
    public static CacheValue FromObject(object value) => new(null, 0, value, false);

    /// <summary>Frees a value the cache took ownership of but did not retain.</summary>
    internal void FreeForCache()
    {
        if (ReturnBufferToPool && Buffer is not null) ArrayPool<byte>.Shared.Return(Buffer);
        (Object as IDisposable)?.Dispose();
    }
}

/// <summary>
/// Base class of cache entries (the LRUHandle / ClockHandle equivalents). All mutable state is owned by the cache
/// implementation; consumers only see entries through <see cref="CachedBlock"/>.
/// </summary>
internal abstract class CacheEntry
{
    internal byte[]? Buffer;
    internal int Length;
    internal object? Object;
    internal bool ReturnBufferToPool;
    internal CacheItemHelper? Helper;
    internal CacheKey Key;
    /// <summary>Charge including metadata (total_charge).</summary>
    internal long TotalCharge;
    /// <summary>Incremented whenever the entry is freed/recycled; handles carry the value they were issued with.</summary>
    internal int Generation;

    internal void SetValue(in CacheValue value)
    {
        Buffer = value.Buffer;
        Length = value.Length;
        Object = value.Object;
        ReturnBufferToPool = value.ReturnBufferToPool;
        Helper = value.Helper;
    }

    /// <summary>The value as handed to the cache (for <see cref="CacheItemHelper"/> callbacks).</summary>
    internal CacheValue GetValue() => new CacheValue(Buffer, Length, Object, false).WithHelper(Helper);

    /// <summary>Releases the value (del_cb) and clears references so the entry object can be recycled.</summary>
    internal void FreeValue()
    {
        byte[]? buffer = Buffer;
        object? obj = Object;
        bool toPool = ReturnBufferToPool;
        Buffer = null;
        Object = null;
        Length = 0;
        ReturnBufferToPool = false;
        Helper = null;
        if (toPool && buffer is not null) ArrayPool<byte>.Shared.Return(buffer);
        (obj as IDisposable)?.Dispose();
    }

    internal abstract void Release(int generation, bool eraseIfLastRef);

    internal abstract void AddRef(int generation);
}

/// <summary>
/// A pinned reference to a cache entry (the Cache::Handle* equivalent). While held, the entry's memory stays valid
/// and is not evicted. Must be released exactly once: <see cref="Dispose"/> (idempotent on the same variable — it
/// clears itself) or <see cref="Release(bool)"/>. Copies of the struct share one reference; releasing through more
/// than one copy is a bug and is detected (InvalidOperationException) when it would underflow or touch a recycled
/// entry. Use <see cref="AddRef"/> to obtain an independent reference (Cache::Ref).
/// </summary>
public struct CachedBlock : IDisposable
{
    private CacheEntry? _entry;
    private int _generation;

    internal CachedBlock(CacheEntry entry)
    {
        _entry = entry;
        _generation = entry.Generation;
    }

    /// <summary>True if this handle references an entry (lookup hit / successful insert).</summary>
    public readonly bool HasValue => _entry is not null;

    public readonly ReadOnlySpan<byte> Span
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            CacheEntry e = EntryOrThrow();
            return new ReadOnlySpan<byte>(e.Buffer, 0, e.Length);
        }
    }

    public readonly ReadOnlyMemory<byte> Memory
    {
        get
        {
            CacheEntry e = EntryOrThrow();
            return new ReadOnlyMemory<byte>(e.Buffer, 0, e.Length);
        }
    }

    public readonly int Length => EntryOrThrow().Length;

    /// <summary>The attached object (e.g. a parsed block), if any.</summary>
    public readonly object? Value => EntryOrThrow().Object;

    /// <summary>Charge of the entry including metadata, if charged.</summary>
    public readonly long Charge => EntryOrThrow().TotalCharge;

    public readonly CacheKey Key => EntryOrThrow().Key;

    /// <summary>The entry's helper (GetCacheItemHelper), or null.</summary>
    public readonly CacheItemHelper? Helper => EntryOrThrow().Helper;

    /// <summary>True for a placeholder entry (no bytes, no object).</summary>
    public readonly bool IsPlaceholder
    {
        get
        {
            CacheEntry e = EntryOrThrow();
            return e.Buffer is null && e.Object is null;
        }
    }

    /// <summary>The value as handed to the cache (helper callbacks, secondary-cache demotion).</summary>
    internal readonly CacheValue GetValue() => EntryOrThrow().GetValue();

    internal readonly CacheEntry? Entry => _entry;

    /// <summary>Takes an additional, independent reference (Cache::Ref). The returned handle must be released too.</summary>
    public readonly CachedBlock AddRef()
    {
        CacheEntry e = EntryOrThrow();
        e.AddRef(_generation);
        return new CachedBlock(e) { _generation = _generation };
    }

    /// <summary>Releases the reference (Cache::Release(handle, erase_if_last_ref)).</summary>
    public void Release(bool eraseIfLastRef)
    {
        CacheEntry? e = _entry;
        if (e is null) return;
        _entry = null;
        e.Release(_generation, eraseIfLastRef);
    }

    public void Dispose() => Release(false);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private readonly CacheEntry EntryOrThrow()
    {
        CacheEntry? e = _entry;
        if (e is null) ThrowEmpty();
        return e;
    }

    [DoesNotReturn]
    private static void ThrowEmpty() => throw new InvalidOperationException("CachedBlock is empty or already released.");

    [DoesNotReturn]
    internal static void ThrowStale() => throw new InvalidOperationException("CachedBlock released more times than it was acquired (double release).");
}

/// <summary>
/// Block cache API (port of the rocksdb::Cache interface, include/rocksdb/advanced_cache.h, restricted to what
/// the managed port needs). Keys are 16-byte <see cref="CacheKey"/>s; values are <see cref="CacheValue"/>s.
/// All members are thread-safe. Hit paths (<see cref="TryLookup"/> + release) do not allocate.
/// <para>
/// A <see cref="BlockCache"/> can be assigned directly to <see cref="Table.TableReaderOptions.BlockCache"/>: the table
/// reader detects it and uses it natively (per-file <see cref="OffsetableCacheKey"/>s, handles, priorities). The
/// explicit <see cref="Table.IBlockCache"/> implementation only exists for that assignment (and other legacy callers).
/// </para>
/// </summary>
public abstract class BlockCache : Table.IBlockCache
{
    private long _lastId;
    private long _standaloneUsage;
    private CacheEvictionHandler? _evictionHandler;

    /// <summary>
    /// True for caches whose lookups need the full context (the TryLookup overload with helper / create context /
    /// statistics): caches with a secondary cache and simulation caches. Readers check this field once per lookup so a
    /// plain cache pays nothing for the secondary-cache layers.
    /// </summary>
    internal readonly bool ExtendedLookup;

    protected BlockCache() { }

    /// <summary>For wrappers whose lookups need the helper / statistics (see <see cref="ExtendedLookup"/>).</summary>
    private protected BlockCache(bool extendedLookup) => ExtendedLookup = extendedLookup;

    // Legacy seam keys: (per-reader table id, offset) tagged in the top bit so they cannot meet OffsetableCacheKey keys
    // of the same process in practice.
    private static CacheKey LegacyKey(in Table.BlockCacheKey key) => new(key.TableId | (1UL << 63), key.Offset);

    bool Table.IBlockCache.TryGet(in Table.BlockCacheKey key, [NotNullWhen(true)] out Table.Block? block)
    {
        if (TryLookup(LegacyKey(key), out CachedBlock h))
        {
            if (h.Value is Table.Block b)
            {
                b.AddRef();
                h.Dispose();
                block = b;
                return true;
            }
            h.Dispose();
        }
        block = null;
        return false;
    }

    void Table.IBlockCache.Insert(in Table.BlockCacheKey key, Table.Block block)
    {
        block.AddRef();
        Insert(LegacyKey(key), CacheValue.FromObject(block), block.Charge, CachePriority.Low);
    }

    /// <summary>Implementation name ("LRUCache", "HyperClockCache").</summary>
    public abstract string Name { get; }

    public abstract long Capacity { get; }

    /// <summary>Sets the capacity, evicting unreferenced entries as needed.</summary>
    public abstract void SetCapacity(long capacity);

    public abstract bool StrictCapacityLimit { get; }

    public abstract void SetStrictCapacityLimit(bool strictCapacityLimit);

    /// <summary>Total charge of all entries resident in the cache (including pinned ones).</summary>
    public abstract long Usage { get; }

    /// <summary>Total charge of entries currently referenced by handles.</summary>
    public abstract long PinnedUsage { get; }

    /// <summary>Number of entries resident in the cache.</summary>
    public abstract int Count { get; }

    /// <summary>Looks up <paramref name="key"/>; on hit returns a pinned handle that must be released.</summary>
    public abstract bool TryLookup(in CacheKey key, out CachedBlock block);

    /// <summary>
    /// Inserts and returns a pinned handle (Insert with handle != nullptr). An existing entry with the same key is
    /// replaced (it stays valid for current holders). Returns false only when <see cref="StrictCapacityLimit"/> is
    /// set and the cache is full of pinned entries (Status::MemoryLimit); the caller then keeps ownership of
    /// <paramref name="value"/>.
    /// </summary>
    public abstract bool TryInsert(in CacheKey key, in CacheValue value, long charge, CachePriority priority, out CachedBlock block);

    /// <summary>
    /// Inserts without a handle (as if released immediately). The cache always takes ownership of
    /// <paramref name="value"/>; returns false if the entry could not be retained (it was freed immediately).
    /// </summary>
    public abstract bool Insert(in CacheKey key, in CacheValue value, long charge, CachePriority priority);

    /// <summary>Removes the mapping; the entry is freed once its last handle is released. Returns true if present.</summary>
    public abstract bool Erase(in CacheKey key);

    /// <summary>Frees every entry that is not currently referenced (EraseUnRefEntries).</summary>
    public abstract void EraseUnreferencedEntries();

    /// <summary>Returns a new id unique for this cache's lifetime (Cache::NewId).</summary>
    public virtual long NewId() => Interlocked.Increment(ref _lastId);

    // ---- secondary-cache support (advanced_cache.h) ------------------------------------------------

    /// <summary>
    /// Lookup with the entry's helper and create context (Cache::Lookup(key, helper, create_context, priority, stats)).
    /// A plain cache ignores the extra arguments; a cache with a secondary cache looks the key up there on a primary
    /// miss and creates the value with <paramref name="helper"/> (<see cref="CacheItemHelper.TryCreate"/>).
    /// </summary>
    public virtual bool TryLookup(in CacheKey key, CacheItemHelper? helper, object? createContext, CachePriority priority,
        Statistics? stats, out CachedBlock block) => TryLookup(key, out block);

    /// <summary>
    /// Insert that also offers the value's compressed (serialized) form to the secondary tiers (the compressed_value /
    /// type arguments of Cache::Insert; only used with <see cref="TieredAdmissionPolicy.ThreeQueue"/>).
    /// </summary>
    public virtual bool TryInsert(in CacheKey key, in CacheValue value, long charge, CachePriority priority,
        ReadOnlySpan<byte> compressedValue, Compression.CompressionType compressionType, out CachedBlock block) =>
        TryInsert(key, value, charge, priority, out block);

    /// <summary>
    /// Starts a lookup that may complete later (Cache::StartAsyncLookup): the primary cache is probed immediately; on a
    /// miss a secondary-cache lookup may stay pending until <see cref="WaitAll"/> / <see cref="WaitAllAsync"/>.
    /// </summary>
    public virtual void StartAsyncLookup(AsyncLookupHandle handle)
    {
        ArgumentNullException.ThrowIfNull(handle);
        handle.SetResult(TryLookup(handle.Key, handle.Helper, handle.CreateContext, handle.Priority, handle.Statistics, out CachedBlock b)
            ? b : default);
    }

    /// <summary>
    /// Releases <paramref name="handle"/> through the cache (Cache::Release(handle, erase_if_last_ref)). Equivalent to
    /// <see cref="CachedBlock.Release(bool)"/> except for wrappers that account for releases (placeholder reservations
    /// of a tiered cache); memory reservations should be released this way.
    /// </summary>
    public virtual void Release(ref CachedBlock handle, bool eraseIfLastRef) => handle.Release(eraseIfLastRef);

    /// <summary>Completes the pending lookups among <paramref name="handles"/> (Cache::WaitAll).</summary>
    public virtual void WaitAll(ReadOnlySpan<AsyncLookupHandle> handles) { }

    /// <summary>Async <see cref="WaitAll"/>: awaits pending secondary-cache lookups instead of blocking a thread.</summary>
    public virtual ValueTask WaitAllAsync(AsyncLookupHandle[] handles, int count, CancellationToken cancellationToken)
    {
        WaitAll(handles.AsSpan(0, count));
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Creates an entry that is not in the cache's table and is freed when its handle is released (Cache::CreateStandalone).
    /// Its charge counts toward <see cref="Usage"/> while referenced. With a strict capacity limit and no room, the entry
    /// is created uncharged if <paramref name="allowUncharged"/>, else nothing is created (false; the caller keeps
    /// ownership of <paramref name="value"/>).
    /// </summary>
    public bool TryCreateStandalone(in CacheKey key, in CacheValue value, long charge, bool allowUncharged, out CachedBlock block)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(charge);
        if (StrictCapacityLimit && Usage + charge > Capacity)
        {
            if (!allowUncharged)
            {
                block = default;
                return false;
            }
            charge = 0;
        }
        var e = new StandaloneEntry(this);
        e.SetValue(value);
        e.Key = key;
        e.TotalCharge = charge;
        Interlocked.Add(ref _standaloneUsage, charge);
        block = new CachedBlock(e);
        return true;
    }

    /// <summary>Total charge of live standalone entries (included in <see cref="Usage"/> and <see cref="PinnedUsage"/>).</summary>
    protected long StandaloneUsage => Volatile.Read(ref _standaloneUsage);

    internal void ReleaseStandaloneCharge(long charge) => Interlocked.Add(ref _standaloneUsage, -charge);

    /// <summary>
    /// Called for entries evicted for capacity (not erased), outside internal locks, before the value is freed
    /// (Cache::SetEvictionCallback). Used by the secondary cache adapter to demote evicted blocks.
    /// </summary>
    internal CacheEvictionHandler? EvictionHandler => Volatile.Read(ref _evictionHandler);

    internal virtual void SetEvictionHandler(CacheEvictionHandler? handler) => Volatile.Write(ref _evictionHandler, handler);

    /// <summary>Capacity of the secondary cache, if any (Cache::GetSecondaryCacheCapacity).</summary>
    public virtual bool TryGetSecondaryCacheCapacity(out long capacity)
    {
        capacity = 0;
        return false;
    }

    /// <summary>Secondary cache memory taken by distributed reservations (Cache::GetSecondaryCachePinnedUsage).</summary>
    public virtual long SecondaryCachePinnedUsage => 0;

    /// <summary>
    /// Creates a primary + compressed secondary tiered cache whose total memory budget is split between the tiers and whose
    /// memory reservations (e.g. a <see cref="WriteBufferManager"/> charging memtables to it) are distributed
    /// proportionally (NewTieredCache, cache/secondary_cache_adapter.cc). Returns null for an invalid admission policy.
    /// </summary>
    public static BlockCache? CreateTiered(TieredCacheOptions options) => CacheWithSecondaryAdapter.NewTieredCache(options);

    /// <summary>
    /// Dynamically updates a cache created by <see cref="CreateTiered"/> (UpdateTieredCache): <paramref name="totalCapacity"/>
    /// &gt; 0 sets the total budget, a ratio in [0, 1] sets the compressed secondary share, a policy other than
    /// <see cref="TieredAdmissionPolicy.Max"/> sets the admission policy. Throws InvalidArgument for other caches.
    /// Once the ratio is lowered to 0 the secondary cache cannot be re-enabled (as upstream).
    /// </summary>
    public static void UpdateTieredCache(BlockCache cache, long totalCapacity, double compressedSecondaryRatio, TieredAdmissionPolicy admissionPolicy) =>
        CacheWithSecondaryAdapter.UpdateTiered(cache, totalCapacity, compressedSecondaryRatio, admissionPolicy);

    /// <summary>
    /// Wraps <paramref name="cache"/> with a key-only shadow LRU cache of <paramref name="simCapacity"/> bytes that reports
    /// the hit rate the cache would have at that capacity (NewSimCache, utilities/simulator_cache/sim_cache.cc).
    /// </summary>
    public static SimCache CreateSimCache(BlockCache cache, long simCapacity, int numShardBits) =>
        SimCache.Create(cache, simCapacity, numShardBits);

    /// <summary>Wraps <paramref name="cache"/> with a secondary cache (what LRUCacheOptions::secondary_cache does).</summary>
    public static BlockCache WithSecondaryCache(BlockCache cache, SecondaryCache secondaryCache) =>
        new CacheWithSecondaryAdapter(cache, secondaryCache, TieredAdmissionPolicy.Auto, distributeCacheReservations: false);

    /// <summary>Creates a sharded LRU cache (NewLRUCache).</summary>
    public static BlockCache CreateLru(long capacity) => new LruCache(new LruCacheOptions { Capacity = capacity });

    /// <summary>Creates a sharded LRU cache (NewLRUCache).</summary>
    /// <remarks>With <see cref="LruCacheOptions.SecondaryCache"/> the cache is wrapped in a secondary cache adapter.</remarks>
    public static BlockCache CreateLru(LruCacheOptions options)
    {
        BlockCache c = new LruCache(options);
        return options.SecondaryCache is { } sec ? WithSecondaryCache(c, sec) : c;
    }

    /// <summary>
    /// Creates a lock-free HyperClockCache (HyperClockCacheOptions::MakeSharedCache); with
    /// <see cref="HyperClockCacheOptions.SecondaryCache"/> it is wrapped in a secondary cache adapter.
    /// </summary>
    public static BlockCache CreateHyperClock(HyperClockCacheOptions options)
    {
        BlockCache c = new HyperClockCache(options);
        return options.SecondaryCache is { } sec ? WithSecondaryCache(c, sec) : c;
    }

    /// <summary>
    /// The recommended block cache for a DB: a <see cref="HyperClockCache"/> of <paramref name="capacity"/> bytes whose
    /// table is sized for blocks of the column families' <c>block_size</c> (see <see cref="EstimateBlockCharge"/>).
    /// HyperClock lookups are lock-free and scale with concurrent readers, unlike the mutex-per-shard LRU.
    /// </summary>
    public static BlockCache CreateDefault(long capacity, int blockSize) =>
        new HyperClockCache(new HyperClockCacheOptions { Capacity = capacity, EstimatedEntryCharge = EstimateBlockCharge(blockSize) });

    /// <summary>
    /// Typical charge of one cached data block for a table <c>block_size</c>: the table reader charges the pooled
    /// buffer holding the block (payload, which usually ends slightly past block_size, plus the 5-byte trailer,
    /// rounded up to the buffer pool's size class), i.e. the per-entry charge to pass as
    /// <see cref="HyperClockCacheOptions.EstimatedEntryCharge"/>.
    /// </summary>
    public static long EstimateBlockCharge(int blockSize)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(blockSize);
        return RocksDb.Managed.Memory.BlockBufferPool.RoundUp((int)Math.Min(int.MaxValue / 2, (long)blockSize + 6));
    }

    /// <summary>
    /// Number of shard bits for a capacity (sharded_cache.cc GetDefaultCacheShardBits): one shard per
    /// <paramref name="minShardSize"/> bytes, at most 2^6 shards.
    /// </summary>
    public static int GetDefaultCacheShardBits(long capacity, long minShardSize)
    {
        int numShardBits = 0;
        long numShards = capacity / minShardSize;
        while ((numShards >>= 1) != 0)
        {
            if (++numShardBits >= 6) return numShardBits;
        }
        return numShardBits;
    }
}
