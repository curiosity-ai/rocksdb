using RocksDb.Managed.Compression;

namespace RocksDb.Managed.Cache;

/// <summary>
/// The compressed secondary cache stacked on a non-volatile secondary cache for
/// <see cref="TieredAdmissionPolicy.ThreeQueue"/>. Port of cache/tiered_secondary_cache.{h,cc}: lookups try the compressed
/// tier, then the non-volatile tier, whose hits (still compressed) are promoted into the compressed tier unless the
/// primary will keep them (advise_erase). Demotion of primary evictions is a no-op; <see cref="InsertSaved"/> warms the
/// non-volatile tier directly.
/// </summary>
internal sealed class TieredSecondaryCache : SecondaryCacheWrapper
{
    private readonly SecondaryCache _nvm;

    public TieredSecondaryCache(SecondaryCache compressedSecondaryCache, SecondaryCache nvmSecondaryCache) : base(compressedSecondaryCache) =>
        _nvm = nvmSecondaryCache ?? throw new ArgumentNullException(nameof(nvmSecondaryCache));

    public override string Name => "TieredSecondaryCache";

    // This is a no-op as we currently don't allow demotion (i.e. insertion by the upper layer) of evicted blocks.
    public override void Insert(ReadOnlySpan<byte> key, in CacheValue value, CacheItemHelper helper, bool forceInsert) { }

    // Warm up the nvm tier directly.
    public override void InsertSaved(ReadOnlySpan<byte> key, ReadOnlySpan<byte> saved, CompressionType type, CacheTier source) =>
        _nvm.InsertSaved(key, saved, type, source);

    private sealed class CreateContext
    {
        public CacheKey Key;
        public bool AdviseErase;
        public CacheItemHelper Helper = null!;
        public object? InnerContext;
        public SecondaryCache CompressedSecondaryCache = null!;
        public Statistics? Stats;
    }

    /// <summary>MaybeInsertAndCreate: offers the non-volatile tier's compressed data to the compressed tier, then creates the value.</summary>
    private sealed class MaybeInsertAndCreateHelper() : CacheItemHelper(CacheEntryRole.Misc)
    {
        public static readonly MaybeInsertAndCreateHelper Instance = new();

        public override int GetSaveSize(in CacheValue value) => throw new InvalidOperationException();

        public override void SaveTo(in CacheValue value, Span<byte> destination) => throw new InvalidOperationException();

        public override bool TryCreate(ReadOnlySpan<byte> data, CompressionType type, CacheTier source, object? createContext,
            out CacheValue value, out long charge)
        {
            var context = (CreateContext)createContext!;
            if (!context.AdviseErase && type != CompressionType.None)
            {
                // Attempt to insert into compressed secondary cache.
                Span<byte> k = stackalloc byte[CacheKey.Size];
                context.Key.WriteTo(k);
                context.CompressedSecondaryCache.InsertSaved(k, data, type, source);
                context.Stats?.RecordTick(Ticker.CompressedSecondaryCachePromotions);
            }
            else
            {
                context.Stats?.RecordTick(Ticker.CompressedSecondaryCachePromotionSkips);
            }
            // Primary cache will accept the object, so call its helper to create the object.
            return context.Helper.TryCreate(data, type, source, context.InnerContext, out value, out charge);
        }
    }

    private sealed class ResultHandle(SecondaryCacheResultHandle inner) : SecondaryCacheResultHandle
    {
        private SecondaryCacheResultHandle? _inner = inner;
        private bool _ready;
        private bool _hasValue;
        private CacheValue _value;
        private long _size;

        public SecondaryCacheResultHandle? Inner => _inner;

        public override bool IsReady
        {
            get
            {
                if (_inner is { IsReady: true }) Complete();
                return _ready;
            }
        }

        public override void Wait()
        {
            _inner?.Wait();
            Complete();
        }

        public override async ValueTask WaitAsync(CancellationToken cancellationToken)
        {
            if (_inner is { } i) await i.WaitAsync(cancellationToken).ConfigureAwait(false);
            Complete();
        }

        public void Complete()
        {
            if (_ready) return;
            SecondaryCacheResultHandle i = _inner!;
            _hasValue = i.HasValue;
            if (_hasValue)
            {
                _value = i.Value;
                _size = i.Size;
            }
            _inner = null;
            _ready = true;
        }

        public override bool HasValue => _hasValue;

        public override CacheValue Value => _value;

        public override long Size => _size;
    }

    public override SecondaryCacheResultHandle? Lookup(ReadOnlySpan<byte> key, CacheItemHelper helper, object? createContext, bool wait,
        bool adviseErase, Statistics? stats, out bool keptInSecondaryCache)
    {
        SecondaryCacheResultHandle? result = Target.Lookup(key, helper, createContext, wait, adviseErase, stats, out _);
        // We never want the item to spill back into the secondary cache.
        keptInSecondaryCache = true;
        if (result is not null) return result;
        var ctx = new CreateContext
        {
            Key = CacheKey.Read(key), AdviseErase = adviseErase, Helper = helper, InnerContext = createContext,
            CompressedSecondaryCache = Target, Stats = stats,
        };
        SecondaryCacheResultHandle? inner = _nvm.Lookup(key, MaybeInsertAndCreateHelper.Instance, ctx, wait, adviseErase, stats, out keptInSecondaryCache);
        if (inner is null) return null;
        if (wait) return inner;
        return new ResultHandle(inner);
    }

    public override void WaitAll(ReadOnlySpan<SecondaryCacheResultHandle> handles)
    {
        var nvmHandles = new List<SecondaryCacheResultHandle>();
        var mine = new List<ResultHandle>();
        foreach (SecondaryCacheResultHandle h in handles)
        {
            // The handle could belong to the compressed secondary cache (already ready).
            if (h.IsReady) continue;
            var r = (ResultHandle)h;
            nvmHandles.Add(r.Inner!);
            mine.Add(r);
        }
        _nvm.WaitAll(nvmHandles.ToArray());
        foreach (ResultHandle r in mine) r.Complete();
    }
}
