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

    public CacheValue(byte[]? buffer, int length, object? obj, bool returnBufferToPool)
    {
        if (buffer is null ? length != 0 : (uint)length > (uint)buffer.Length) ThrowHelper.InvalidArgument("length out of range");
        Buffer = buffer;
        Length = length;
        Object = obj;
        ReturnBufferToPool = returnBufferToPool && buffer is not null;
    }

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
    }

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
/// </summary>
public abstract class BlockCache
{
    private long _lastId;

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
    public long NewId() => Interlocked.Increment(ref _lastId);

    /// <summary>Creates a sharded LRU cache (NewLRUCache).</summary>
    public static BlockCache CreateLru(long capacity) => new LruCache(new LruCacheOptions { Capacity = capacity });

    /// <summary>Creates a sharded LRU cache (NewLRUCache).</summary>
    public static BlockCache CreateLru(LruCacheOptions options) => new LruCache(options);

    /// <summary>Creates a lock-free HyperClockCache (HyperClockCacheOptions::MakeSharedCache).</summary>
    public static BlockCache CreateHyperClock(HyperClockCacheOptions options) => new HyperClockCache(options);

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
