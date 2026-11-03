using RocksDb.Managed.Compression;

namespace RocksDb.Managed.Cache;

/// <summary>
/// Result of a <see cref="SecondaryCache.Lookup"/> (rocksdb::SecondaryCacheResultHandle, include/rocksdb/secondary_cache.h).
/// A lookup may complete later (non-volatile tiers): <see cref="IsReady"/> / <see cref="Wait"/> / <see cref="WaitAsync"/>.
/// Once ready, <see cref="Value"/> is the created value (owned by whoever takes it: the adapter inserts it into the
/// primary cache) or default on a miss, and <see cref="Size"/> its charge.
/// </summary>
public abstract class SecondaryCacheResultHandle
{
    /// <summary>Whether the handle is ready.</summary>
    public abstract bool IsReady { get; }

    /// <summary>Blocks until the handle is ready.</summary>
    public abstract void Wait();

    /// <summary>Waits asynchronously until the handle is ready (default: <see cref="Wait"/>).</summary>
    public virtual ValueTask WaitAsync(CancellationToken cancellationToken)
    {
        Wait();
        return ValueTask.CompletedTask;
    }

    /// <summary>True when the lookup found something (Value() != nullptr).</summary>
    public abstract bool HasValue { get; }

    /// <summary>The created cache value (meaningful when <see cref="HasValue"/>).</summary>
    public abstract CacheValue Value { get; }

    /// <summary>The out_charge of the helper's create callback (Size(): really the charge).</summary>
    public abstract long Size { get; }
}

/// <summary>A result handle that is ready on creation.</summary>
public sealed class ReadySecondaryCacheResultHandle : SecondaryCacheResultHandle
{
    private readonly CacheValue _value;
    private readonly long _size;

    public ReadySecondaryCacheResultHandle(in CacheValue value, long size)
    {
        _value = value;
        _size = size;
    }

    public override bool IsReady => true;

    public override void Wait() { }

    public override bool HasValue => true;

    public override CacheValue Value => _value;

    public override long Size => _size;
}

/// <summary>
/// A cache tier behind the primary block cache (rocksdb::SecondaryCache, include/rocksdb/secondary_cache.h). Entries reach
/// it in their persistable form (<see cref="CacheItemHelper.SaveTo"/>) and come back through
/// <see cref="CacheItemHelper.TryCreate"/>. Status results map to: Insert / InsertSaved return nothing (OK, the insertion
/// may be skipped by the admission policy), the capacity calls return false for NotSupported. Thread-safe.
/// </summary>
public abstract class SecondaryCache
{
    /// <summary>Implementation name (Customizable::Name).</summary>
    public abstract string Name { get; }

    /// <summary>
    /// Suggests inserting an entry (Insert). <paramref name="value"/> stays owned by the caller; when admitted its saved
    /// form is copied. <paramref name="forceInsert"/> bypasses the admission policy (e.g. the dummy-first rule).
    /// </summary>
    public abstract void Insert(ReadOnlySpan<byte> key, in CacheValue value, CacheItemHelper helper, bool forceInsert);

    /// <summary>
    /// Inserts a value from its saved form (InsertSaved), possibly compressed with <paramref name="type"/>; used to warm
    /// the cache. May be ignored by the admission policy.
    /// </summary>
    public abstract void InsertSaved(ReadOnlySpan<byte> key, ReadOnlySpan<byte> saved, CompressionType type, CacheTier source);

    /// <summary>
    /// Looks up <paramref name="key"/>; <paramref name="helper"/>'s create callback builds the value. With
    /// <paramref name="wait"/> the returned handle is ready. <paramref name="adviseErase"/>: the primary cache will keep
    /// the entry, so this tier may drop it (only honored when <see cref="SupportForceErase"/>).
    /// <paramref name="keptInSecondaryCache"/> reports whether the entry stays here after the lookup.
    /// </summary>
    public abstract SecondaryCacheResultHandle? Lookup(ReadOnlySpan<byte> key, CacheItemHelper helper, object? createContext, bool wait,
        bool adviseErase, Statistics? stats, out bool keptInSecondaryCache);

    /// <summary>Whether <see cref="Erase"/> / advise_erase are supported (SupportForceErase).</summary>
    public abstract bool SupportForceErase { get; }

    /// <summary>At the discretion of the implementation, erases the data associated with <paramref name="key"/>.</summary>
    public abstract void Erase(ReadOnlySpan<byte> key);

    /// <summary>Waits for a collection of handles to become ready (WaitAll).</summary>
    public virtual void WaitAll(ReadOnlySpan<SecondaryCacheResultHandle> handles)
    {
        foreach (SecondaryCacheResultHandle h in handles) h.Wait();
    }

    /// <summary>Sets the capacity (SetCapacity); false: NotSupported.</summary>
    public virtual bool SetCapacity(long capacity) => false;

    /// <summary>Gets the capacity (GetCapacity); false: NotSupported.</summary>
    public virtual bool TryGetCapacity(out long capacity)
    {
        capacity = 0;
        return false;
    }

    /// <summary>Temporarily decreases the capacity by <paramref name="decrease"/> (Deflate); false: NotSupported.</summary>
    public virtual bool Deflate(long decrease) => false;

    /// <summary>Restores capacity taken by <see cref="Deflate"/> (Inflate); false: NotSupported.</summary>
    public virtual bool Inflate(long increase) => false;

    /// <summary>Printable description (GetPrintableOptions).</summary>
    public virtual string GetPrintableOptions() => "";
}

/// <summary>Forwards every call to a target secondary cache (rocksdb::SecondaryCacheWrapper).</summary>
public abstract class SecondaryCacheWrapper(SecondaryCache target) : SecondaryCache
{
    protected SecondaryCache Target { get; } = target ?? throw new ArgumentNullException(nameof(target));

    public override void Insert(ReadOnlySpan<byte> key, in CacheValue value, CacheItemHelper helper, bool forceInsert) =>
        Target.Insert(key, value, helper, forceInsert);

    public override void InsertSaved(ReadOnlySpan<byte> key, ReadOnlySpan<byte> saved, CompressionType type, CacheTier source) =>
        Target.InsertSaved(key, saved, type, source);

    public override SecondaryCacheResultHandle? Lookup(ReadOnlySpan<byte> key, CacheItemHelper helper, object? createContext, bool wait,
        bool adviseErase, Statistics? stats, out bool keptInSecondaryCache) =>
        Target.Lookup(key, helper, createContext, wait, adviseErase, stats, out keptInSecondaryCache);

    public override bool SupportForceErase => Target.SupportForceErase;

    public override void Erase(ReadOnlySpan<byte> key) => Target.Erase(key);

    public override void WaitAll(ReadOnlySpan<SecondaryCacheResultHandle> handles) => Target.WaitAll(handles);

    public override bool SetCapacity(long capacity) => Target.SetCapacity(capacity);

    public override bool TryGetCapacity(out long capacity) => Target.TryGetCapacity(out capacity);

    public override bool Deflate(long decrease) => Target.Deflate(decrease);

    public override bool Inflate(long increase) => Target.Inflate(increase);

    public override string GetPrintableOptions() => Target.GetPrintableOptions();
}
