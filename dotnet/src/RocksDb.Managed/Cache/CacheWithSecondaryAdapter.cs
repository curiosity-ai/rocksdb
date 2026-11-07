using System.Runtime.CompilerServices;
using RocksDb.Managed.Compression;

namespace RocksDb.Managed.Cache;

/// <summary>
/// A primary block cache combined with a <see cref="SecondaryCache"/>. Port of CacheWithSecondaryAdapter
/// (cache/secondary_cache_adapter.{h,cc}) and NewTieredCache / UpdateTieredCache:
/// <list type="bullet">
/// <item>Lookups that miss the primary cache (or find a "dummy" entry recording a recent secondary hit) go to the secondary
/// cache; a hit is promoted: with a secondary cache supporting erase (CompressedSecondaryCache), the first hit returns a
/// standalone handle and inserts a dummy into the primary cache, the second (dummy found) inserts the real entry and lets
/// the secondary drop it (insert on second hit).</item>
/// <item>Entries evicted from the primary cache are offered to the secondary cache (except with
/// <see cref="TieredAdmissionPolicy.ThreeQueue"/>), forced for <see cref="TieredAdmissionPolicy.AllowAll"/> and for hit
/// entries with <see cref="TieredAdmissionPolicy.AllowCacheHits"/>.</item>
/// <item>With distribute_cache_res (tiered caches) the primary is sized to the total budget, the secondary budget is
/// reserved against it, and placeholder reservations inserted by a <see cref="CacheReservationManager"/> are split
/// between the tiers (Deflate / Inflate of the secondary in 1 MiB chunks).</item>
/// </list>
/// </summary>
internal sealed class CacheWithSecondaryAdapter : BlockCache
{
    private const long ReservationChunkSize = 1 << 20;
    private const string TieredCacheName = "TieredCache";

    /// <summary>kDummyObj: the value of dummy entries in the primary cache.</summary>
    internal static readonly object DummyObject = new DummyMarker();

    private sealed class DummyMarker
    {
        public override string ToString() => "kDummy";
    }

    private static readonly CacheValue s_dummyValue = CacheValue.FromObject(DummyObject).WithHelper(CacheItemHelper.Basic(CacheEntryRole.Misc));

    private readonly BlockCache _target;
    private readonly SecondaryCache _secondary;
    private volatile TieredAdmissionPolicy _admPolicy;
    private readonly bool _distributeCacheRes;
    private readonly ConcurrentCacheReservationManager? _priCacheRes;
    private double _secCacheResRatio;
    private readonly Lock _cacheResMutex = new();
    private long _placeholderUsage;
    private long _reservedUsage;
    private long _secReserved;

    public CacheWithSecondaryAdapter(BlockCache target, SecondaryCache secondaryCache, TieredAdmissionPolicy admissionPolicy,
        bool distributeCacheReservations) : base(extendedLookup: true)
    {
        _target = target ?? throw new ArgumentNullException(nameof(target));
        _secondary = secondaryCache ?? throw new ArgumentNullException(nameof(secondaryCache));
        _admPolicy = admissionPolicy;
        _distributeCacheRes = distributeCacheReservations;
        _target.SetEvictionHandler(new Evictions(this));
        if (distributeCacheReservations)
        {
            _priCacheRes = new ConcurrentCacheReservationManager(new CacheReservationManagerImpl(_target, CacheEntryRole.Misc, delayedDecrease: false));
            _secondary.TryGetCapacity(out long secCapacity);
            // Initially, the primary cache is sized to uncompressed cache budget plus compressed secondary cache budget.
            // The secondary cache budget is then taken away from the primary cache through cache reservations.
            _priCacheRes.UpdateCacheReservation(secCapacity);
            long cap = _target.Capacity;
            _secCacheResRatio = cap == 0 ? 0 : (double)secCapacity / cap;
        }
    }

    internal BlockCache Primary => _target;

    internal SecondaryCache Secondary => _secondary;

    public TieredAdmissionPolicy AdmissionPolicy => _admPolicy;

    // The compressed value is only used with kAdmPolicyThreeQueue (Insert below).
    internal override bool AdmitsCompressedBlocks => _admPolicy == TieredAdmissionPolicy.ThreeQueue;

    private sealed class Evictions(CacheWithSecondaryAdapter owner) : CacheEvictionHandler
    {
        internal override void OnEvicted(in CacheKey key, CacheEntry entry, bool wasHit) => owner.EvictionHandlerImpl(key, entry, wasHit);
    }

    // EvictionHandler: spill into the secondary cache; never takes ownership.
    private void EvictionHandlerImpl(in CacheKey key, CacheEntry entry, bool wasHit)
    {
        CacheItemHelper? helper = entry.Helper;
        TieredAdmissionPolicy policy = _admPolicy;
        if (helper is null || !helper.IsSecondaryCacheCompatible || policy == TieredAdmissionPolicy.ThreeQueue) return;
        if (ReferenceEquals(entry.Object, DummyObject) || (entry.Object is null && entry.Buffer is null)) return;
        bool force = policy switch
        {
            TieredAdmissionPolicy.AllowCacheHits => wasHit,
            TieredAdmissionPolicy.AllowAll => true,
            _ => false,
        };
        Span<byte> k = stackalloc byte[CacheKey.Size];
        key.WriteTo(k);
        _secondary.Insert(k, entry.GetValue(), helper, force);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsDummy(in CachedBlock h) => ReferenceEquals(h.Entry!.Object, DummyObject);

    // ProcessDummyResult: a dummy hit is released (and erased if the caller will look further) and reported as a miss.
    private bool ProcessDummyResult(ref CachedBlock handle, bool erase)
    {
        if (handle.HasValue && IsDummy(handle))
        {
            handle.Release(erase);
            return true;
        }
        return false;
    }

    private bool Promote(SecondaryCacheResultHandle secondaryHandle, in CacheKey key, CacheItemHelper helper, CachePriority priority,
        Statistics? stats, bool foundDummyEntry, bool keptInSecCache, out CachedBlock result)
    {
        result = default;
        if (!secondaryHandle.HasValue) return false;
        switch (helper.Role)
        {
            case CacheEntryRole.FilterBlock: stats?.RecordTick(Ticker.SecondaryCacheFilterHits); break;
            case CacheEntryRole.IndexBlock: stats?.RecordTick(Ticker.SecondaryCacheIndexHits); break;
            case CacheEntryRole.DataBlock: stats?.RecordTick(Ticker.SecondaryCacheDataHits); break;
        }
        stats?.RecordTick(Ticker.SecondaryCacheHits);
        CacheValue obj = secondaryHandle.Value;
        long charge = secondaryHandle.Size;
        if (_secondary.SupportForceErase && !foundDummyEntry)
        {
            // Create standalone and insert dummy. Allow standalone to be created even if cache is full, to avoid reading
            // the entry from storage.
            _target.TryCreateStandalone(key, obj, charge, allowUncharged: true, out result);
            // Insert dummy to record recent use.
            _target.Insert(key, s_dummyValue, 0, priority);
            return true;
        }
        // Insert regular entry into primary cache. Don't allow it to spill into secondary cache again if it was kept there.
        CacheValue v = obj.WithHelper(keptInSecCache ? helper.WithoutSecondaryCompat : helper);
        if (TryInsert(key, v, charge, priority, out result)) return true;
        _target.TryCreateStandalone(key, obj, charge, allowUncharged: true, out result);
        return true;
    }

    // ---- lookup ------------------------------------------------------------------------------------

    public override bool TryLookup(in CacheKey key, out CachedBlock block)
    {
        if (!_target.TryLookup(key, out block)) return false;
        return !ProcessDummyResult(ref block, erase: false);
    }

    public override bool TryLookup(in CacheKey key, CacheItemHelper? helper, object? createContext, CachePriority priority, Statistics? stats,
        out CachedBlock block)
    {
        bool hit = _target.ExtendedLookup
            ? _target.TryLookup(key, helper, createContext, priority, stats, out block)
            : _target.TryLookup(key, out block);
        bool secondaryCompatible = helper is not null && helper.IsSecondaryCacheCompatible;
        if (hit && !IsDummy(block)) return true;
        bool foundDummyEntry = hit && ProcessDummyResult(ref block, erase: secondaryCompatible);
        if (!secondaryCompatible) return false;
        Span<byte> k = stackalloc byte[CacheKey.Size];
        key.WriteTo(k);
        SecondaryCacheResultHandle? sh = _secondary.Lookup(k, helper!, createContext, wait: true, adviseErase: foundDummyEntry, stats,
            out bool kept);
        return sh is not null && Promote(sh, key, helper!, priority, stats, foundDummyEntry, kept, out block);
    }

    public override void StartAsyncLookup(AsyncLookupHandle handle)
    {
        ArgumentNullException.ThrowIfNull(handle);
        _target.StartAsyncLookup(handle);
        if (handle.IsPending) return;
        bool secondaryCompatible = handle.Helper is not null && handle.Helper.IsSecondaryCacheCompatible;
        handle.FoundDummyEntry |= ProcessDummyResult(ref handle.ResultRef, erase: secondaryCompatible);
        if (!handle.ResultRef.HasValue && secondaryCompatible) StartAsyncLookupOnMySecondary(handle);
    }

    private void StartAsyncLookupOnMySecondary(AsyncLookupHandle handle)
    {
        Span<byte> k = stackalloc byte[CacheKey.Size];
        handle.Key.WriteTo(k);
        SecondaryCacheResultHandle? sh = _secondary.Lookup(k, handle.Helper!, handle.CreateContext, wait: false, handle.FoundDummyEntry,
            handle.Statistics, out bool kept);
        handle.KeptInSecCache = kept;
        if (sh is not null)
        {
            handle.PendingHandle = sh;
            handle.PendingCache = _secondary;
        }
    }

    // WaitAll / WaitAllAsync collect the handles pending on this adapter's secondary cache into arrays rented from
    // ArrayPool (no allocation per batch); a batch with nothing pending returns without collecting.
    public override void WaitAll(ReadOnlySpan<AsyncLookupHandle> handles)
    {
        if (!AnyPending(handles)) return;
        AsyncLookupHandle[] mine = System.Buffers.ArrayPool<AsyncLookupHandle>.Shared.Rent(handles.Length);
        SecondaryCacheResultHandle[]? sec = null;
        int n = 0;
        try
        {
            n = CollectPending(handles, mine, out bool innerPending);
            if (innerPending)
            {
                _target.WaitAll(handles);
                n = ConvertInner(handles, mine, n);
            }
            sec = System.Buffers.ArrayPool<SecondaryCacheResultHandle>.Shared.Rent(Math.Max(1, n));
            for (int i = 0; i < n; i++) sec[i] = mine[i].PendingHandle!;
            _secondary.WaitAll(sec.AsSpan(0, n));
            for (int i = 0; i < n; i++) Complete(mine[i]);
        }
        finally
        {
            if (sec is not null) System.Buffers.ArrayPool<SecondaryCacheResultHandle>.Shared.Return(sec, clearArray: true);
            System.Buffers.ArrayPool<AsyncLookupHandle>.Shared.Return(mine, clearArray: true);
        }
    }

    public override ValueTask WaitAllAsync(AsyncLookupHandle[] handles, int count, CancellationToken cancellationToken)
    {
        if (count == 0 || !AnyPending(handles.AsSpan(0, count))) return ValueTask.CompletedTask;
        return WaitAllSlowAsync(handles, count, cancellationToken);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask WaitAllSlowAsync(AsyncLookupHandle[] handles, int count, CancellationToken cancellationToken)
    {
        AsyncLookupHandle[] mine = System.Buffers.ArrayPool<AsyncLookupHandle>.Shared.Rent(count);
        try
        {
            int n = CollectPending(handles.AsSpan(0, count), mine, out bool innerPending);
            if (innerPending)
            {
                await _target.WaitAllAsync(handles, count, cancellationToken).ConfigureAwait(false);
                n = ConvertInner(handles.AsSpan(0, count), mine, n);
            }
            for (int i = 0; i < n; i++)
            {
                SecondaryCacheResultHandle sh = mine[i].PendingHandle!;
                if (!sh.IsReady) await sh.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            for (int i = 0; i < n; i++) Complete(mine[i]);
        }
        finally
        {
            System.Buffers.ArrayPool<AsyncLookupHandle>.Shared.Return(mine, clearArray: true);
        }
    }

    private static bool AnyPending(ReadOnlySpan<AsyncLookupHandle> handles)
    {
        foreach (AsyncLookupHandle h in handles)
        {
            if (h.PendingCache is not null) return true;
        }
        return false;
    }

    // Requests pending on *my* secondary (marked as handled by this caller) go to `mine`; returns their number and
    // whether any are pending on an inner one.
    private int CollectPending(ReadOnlySpan<AsyncLookupHandle> handles, AsyncLookupHandle[] mine, out bool innerPending)
    {
        int n = 0;
        innerPending = false;
        foreach (AsyncLookupHandle cur in handles)
        {
            if (cur.PendingCache is null) continue;
            if (ReferenceEquals(cur.PendingCache, _secondary))
            {
                mine[n++] = cur;
                cur.PendingCache = null;
            }
            else
            {
                innerPending = true;
            }
        }
        return n;
    }

    // For those that failed to find something in an inner cache, convert to a lookup in my secondary cache.
    private int ConvertInner(ReadOnlySpan<AsyncLookupHandle> handles, AsyncLookupHandle[] mine, int n)
    {
        foreach (AsyncLookupHandle cur in handles)
        {
            if (cur.IsPending || cur.ResultRef.HasValue || Array.IndexOf(mine, cur, 0, n) >= 0) continue;
            if (cur.Helper is null || !cur.Helper.IsSecondaryCacheCompatible) continue;
            StartAsyncLookupOnMySecondary(cur);
            if (cur.IsPending)
            {
                mine[n++] = cur;
                cur.PendingCache = null;
            }
        }
        return n;
    }

    private void Complete(AsyncLookupHandle cur)
    {
        SecondaryCacheResultHandle sh = cur.PendingHandle!;
        cur.PendingHandle = null;
        cur.PendingCache = null;
        Promote(sh, cur.Key, cur.Helper!, cur.Priority, cur.Statistics, cur.FoundDummyEntry, cur.KeptInSecCache, out CachedBlock r);
        cur.SetResult(r);
    }

    // ---- insert / release --------------------------------------------------------------------------

    public override bool TryInsert(in CacheKey key, in CacheValue value, long charge, CachePriority priority, out CachedBlock block)
    {
        if (!_target.TryInsert(key, value, charge, priority, out block)) return false;
        if (value.IsPlaceholder && _distributeCacheRes) OnPlaceholderInserted(block.Charge);
        return true;
    }

    public override bool TryInsert(in CacheKey key, in CacheValue value, long charge, CachePriority priority, ReadOnlySpan<byte> compressedValue,
        CompressionType compressionType, out CachedBlock block)
    {
        bool ok = TryInsert(key, value, charge, priority, out block);
        // Warm up the secondary cache with the compressed block. The secondary cache may ignore it (admission policy).
        if (!value.IsPlaceholder && !compressedValue.IsEmpty && _admPolicy == TieredAdmissionPolicy.ThreeQueue
            && value.Helper is { IsSecondaryCacheCompatible: true })
        {
            Span<byte> k = stackalloc byte[CacheKey.Size];
            key.WriteTo(k);
            _secondary.InsertSaved(k, compressedValue, compressionType, CacheTier.VolatileTier);
        }
        return ok;
    }

    public override bool Insert(in CacheKey key, in CacheValue value, long charge, CachePriority priority) =>
        _target.Insert(key, value, charge, priority);

    private void OnPlaceholderInserted(long charge)
    {
        lock (_cacheResMutex)
        {
            _placeholderUsage += charge;
            // Don't charge the secondary beyond its capacity, and only adjust in kReservationChunkSize steps.
            if (_placeholderUsage <= _target.Capacity && _placeholderUsage - _reservedUsage >= ReservationChunkSize)
            {
                _reservedUsage = _placeholderUsage & ~(ReservationChunkSize - 1);
                long newSecReserved = (long)(_reservedUsage * _secCacheResRatio);
                long secCharge = newSecReserved - _secReserved;
                _secondary.Deflate(secCharge);
                _priCacheRes!.UpdateCacheReservation(secCharge, increase: false);
                _secReserved += secCharge;
            }
        }
    }

    public override void Release(ref CachedBlock handle, bool eraseIfLastRef)
    {
        if (eraseIfLastRef && _distributeCacheRes && handle.HasValue && handle.IsPlaceholder)
        {
            long charge = handle.Charge;
            lock (_cacheResMutex)
            {
                _placeholderUsage -= charge;
                if (_placeholderUsage <= _target.Capacity && _placeholderUsage < _reservedUsage)
                {
                    // Adjust reserved_usage_ in chunks of kReservationChunkSize, so we don't hit this slow path too often.
                    _reservedUsage = _placeholderUsage & ~(ReservationChunkSize - 1);
                    long newSecReserved = (long)(_reservedUsage * _secCacheResRatio);
                    long secCharge = _secReserved - newSecReserved;
                    _secondary.Inflate(secCharge);
                    _priCacheRes!.UpdateCacheReservation(secCharge, increase: true);
                    _secReserved -= secCharge;
                }
            }
        }
        handle.Release(eraseIfLastRef);
    }

    public override bool Erase(in CacheKey key) => _target.Erase(key);

    public override void EraseUnreferencedEntries() => _target.EraseUnreferencedEntries();

    // CacheWrapper::ApplyToAllEntries. Dummy entries carry a helper without secondary-cache support.
    public override void ApplyToAllEntries<TState>(TState state, CacheEntryVisitor<TState> callback, int averageEntriesPerLock) =>
        _target.ApplyToAllEntries(state, callback, averageEntriesPerLock);

    // ---- capacity / properties ---------------------------------------------------------------------

    public override string Name => _distributeCacheRes ? TieredCacheName : _target.Name;

    public override long Capacity => _target.Capacity;

    public override bool StrictCapacityLimit => _target.StrictCapacityLimit;

    public override void SetStrictCapacityLimit(bool strictCapacityLimit) => _target.SetStrictCapacityLimit(strictCapacityLimit);

    public override long Usage => _target.Usage;

    public override long PinnedUsage => _target.PinnedUsage;

    public override int Count => _target.Count;

    public override long NewId() => _target.NewId();

    public override bool TryGetSecondaryCacheCapacity(out long capacity) => _secondary.TryGetCapacity(out capacity);

    public override long SecondaryCachePinnedUsage
    {
        get
        {
            if (!_distributeCacheRes) return 0;
            lock (_cacheResMutex)
            {
                return _secondary.TryGetCapacity(out long capacity) ? capacity - _priCacheRes!.TotalMemoryUsed : 0;
            }
        }
    }

    /// <summary>
    /// Updates the total capacity; with distributed reservations the secondary share and the reservation against the
    /// primary are updated too, in an order that avoids temporary over-use or unnecessary evictions.
    /// </summary>
    public override void SetCapacity(long capacity)
    {
        if (!_distributeCacheRes)
        {
            _target.SetCapacity(capacity);
            return;
        }
        lock (_cacheResMutex)
        {
            long secCapacity = (long)(capacity * _secCacheResRatio);
            if (!_secondary.TryGetCapacity(out long oldSecCapacity)) return;
            if (oldSecCapacity > secCapacity)
            {
                // Shrinking: 1. lower the secondary capacity, 2. credit the primary, 3. lower the primary capacity.
                if (_secondary.SetCapacity(secCapacity))
                {
                    if (_placeholderUsage > capacity) _reservedUsage = capacity & ~(ReservationChunkSize - 1);
                    long newSecReserved = (long)(_reservedUsage * _secCacheResRatio);
                    _priCacheRes!.UpdateCacheReservation(oldSecCapacity - secCapacity - (_secReserved - newSecReserved), increase: false);
                    _secReserved = newSecReserved;
                    _target.SetCapacity(capacity);
                }
            }
            else
            {
                // Expanding: 1. raise the primary capacity, 2. reserve for the secondary, 3. raise the secondary capacity.
                _target.SetCapacity(capacity);
                _priCacheRes!.UpdateCacheReservation(secCapacity - oldSecCapacity, increase: true);
                _secondary.SetCapacity(secCapacity);
            }
        }
    }

    /// <summary>UpdateCacheReservationRatio: changes the secondary share of the total budget (NotSupported without distribution).</summary>
    internal void UpdateCacheReservationRatio(double ratio)
    {
        if (!_distributeCacheRes) ThrowHelper.NotSupported("not a tiered cache");
        lock (_cacheResMutex)
        {
            long priCapacity = _target.Capacity;
            long secCapacity = (long)(priCapacity * ratio);
            if (!_secondary.TryGetCapacity(out long oldSecCapacity)) ThrowHelper.NotSupported("secondary cache capacity");
            _secCacheResRatio = ratio;
            long newSecReserved = (long)(_reservedUsage * _secCacheResRatio);
            if (secCapacity > oldSecCapacity)
            {
                // A higher ratio gives the secondary a higher share of the reservations: deflate it first, then reserve the
                // capacity increase net of that share, then raise its capacity.
                _secondary.Deflate(newSecReserved - _secReserved);
                _priCacheRes!.UpdateCacheReservation(secCapacity - oldSecCapacity - (newSecReserved - _secReserved), increase: true);
                _secReserved = newSecReserved;
                _secondary.SetCapacity(secCapacity);
            }
            else if (_secondary.SetCapacity(secCapacity))
            {
                // Shrinking the ratio: lower the capacity, release the reservation net of the share change, inflate.
                _priCacheRes!.UpdateCacheReservation(oldSecCapacity - secCapacity - (_secReserved - newSecReserved), increase: false);
                _secondary.Inflate(_secReserved - newSecReserved);
                _secReserved = newSecReserved;
            }
        }
    }

    internal void UpdateAdmissionPolicy(TieredAdmissionPolicy policy) => _admPolicy = policy;

    // ---- NewTieredCache / UpdateTieredCache --------------------------------------------------------

    internal static BlockCache? NewTieredCache(TieredCacheOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        TieredAdmissionPolicy policy = options.AdmissionPolicy;
        switch (policy)
        {
            case TieredAdmissionPolicy.Auto:
                // Select an appropriate default policy.
                policy = options.NvmSecondaryCache is not null ? TieredAdmissionPolicy.ThreeQueue : TieredAdmissionPolicy.Placeholder;
                break;
            case TieredAdmissionPolicy.Placeholder:
            case TieredAdmissionPolicy.AllowCacheHits:
            case TieredAdmissionPolicy.AllowAll:
                if (options.NvmSecondaryCache is not null) return null;
                break;
            case TieredAdmissionPolicy.ThreeQueue:
                if (options.NvmSecondaryCache is null) return null;
                break;
            default:
                return null;
        }

        long total = options.TotalCapacity;
        BlockCache primary;
        if (options.CacheType == PrimaryCacheType.Lru)
        {
            LruCacheOptions o = options.LruCacheOptions ?? new LruCacheOptions();
            primary = new LruCache(new LruCacheOptions
            {
                Capacity = total, NumShardBits = o.NumShardBits, StrictCapacityLimit = o.StrictCapacityLimit, HighPriPoolRatio = o.HighPriPoolRatio,
                LowPriPoolRatio = o.LowPriPoolRatio, MetadataChargePolicy = o.MetadataChargePolicy,
            });
        }
        else if (options.CacheType == PrimaryCacheType.HyperClock)
        {
            HyperClockCacheOptions o = options.HyperClockCacheOptions ?? new HyperClockCacheOptions();
            primary = new HyperClockCache(new HyperClockCacheOptions
            {
                Capacity = total, EstimatedEntryCharge = o.EstimatedEntryCharge, MinAvgEntryCharge = o.MinAvgEntryCharge, NumShardBits = o.NumShardBits,
                StrictCapacityLimit = o.StrictCapacityLimit, MetadataChargePolicy = o.MetadataChargePolicy, EvictionEffortCap = o.EvictionEffortCap,
            });
        }
        else
        {
            return null;
        }

        CompressedSecondaryCacheOptions c = options.CompressedSecondaryCacheOptions ?? new CompressedSecondaryCacheOptions();
        SecondaryCache sec = new CompressedSecondaryCache(c.WithCapacity((long)(total * options.CompressedSecondaryRatio)));
        if (options.NvmSecondaryCache is { } nvm)
        {
            if (policy != TieredAdmissionPolicy.ThreeQueue) return null;
            sec = new TieredSecondaryCache(sec, nvm);
        }
        return new CacheWithSecondaryAdapter(primary, sec, policy, distributeCacheReservations: true);
    }

    internal static void UpdateTiered(BlockCache cache, long totalCapacity, double compressedSecondaryRatio, TieredAdmissionPolicy policy)
    {
        if (cache is not CacheWithSecondaryAdapter { Name: TieredCacheName } tiered)
        {
            ThrowHelper.InvalidArgument("not a tiered cache");
            return;
        }
        if (totalCapacity > 0) tiered.SetCapacity(totalCapacity);
        if (compressedSecondaryRatio >= 0.0 && compressedSecondaryRatio <= 1.0) tiered.UpdateCacheReservationRatio(compressedSecondaryRatio);
        if (policy < TieredAdmissionPolicy.Max) tiered.UpdateAdmissionPolicy(policy);
    }
}

/// <summary>
/// Options of <see cref="BlockCache.CreateTiered"/> (rocksdb::TieredCacheOptions, include/rocksdb/cache.h). EXPERIMENTAL
/// upstream. Capacities in the primary / compressed options are ignored: <see cref="TotalCapacity"/> is the budget,
/// split by <see cref="CompressedSecondaryRatio"/>.
/// </summary>
public sealed class TieredCacheOptions
{
    /// <summary>Primary cache type (cache_type).</summary>
    public PrimaryCacheType CacheType { get; init; } = PrimaryCacheType.Lru;

    /// <summary>Primary LRU options when <see cref="CacheType"/> is LRU (cache_opts); capacity is ignored.</summary>
    public LruCacheOptions? LruCacheOptions { get; init; }

    /// <summary>Primary HyperClock options when <see cref="CacheType"/> is HyperClock (cache_opts); capacity is ignored.</summary>
    public HyperClockCacheOptions? HyperClockCacheOptions { get; init; }

    /// <summary>Admission policy (adm_policy).</summary>
    public TieredAdmissionPolicy AdmissionPolicy { get; init; } = TieredAdmissionPolicy.Auto;

    /// <summary>Compressed secondary cache options (comp_cache_opts); capacity is ignored.</summary>
    public CompressedSecondaryCacheOptions? CompressedSecondaryCacheOptions { get; init; }

    /// <summary>The total memory budget divided between the primary and the compressed secondary cache (total_capacity).</summary>
    public long TotalCapacity { get; init; }

    /// <summary>Fraction of <see cref="TotalCapacity"/> given to the compressed secondary cache (compressed_secondary_ratio).</summary>
    public double CompressedSecondaryRatio { get; init; }

    /// <summary>Optional non-volatile secondary cache tier (nvm_sec_cache); requires <see cref="TieredAdmissionPolicy.ThreeQueue"/>.</summary>
    public SecondaryCache? NvmSecondaryCache { get; init; }
}
