using System.Buffers;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Cache;

/// <summary>
/// Options of <see cref="CompressedSecondaryCache"/> (rocksdb::CompressedSecondaryCacheOptions, include/rocksdb/cache.h):
/// the LRU options of the cache that holds the compressed entries plus the compression settings.
/// </summary>
public sealed class CompressedSecondaryCacheOptions
{
    public long Capacity { get; init; }
    /// <summary>log2 of the shard count; -1 picks the default for the capacity.</summary>
    public int NumShardBits { get; init; } = -1;
    public bool StrictCapacityLimit { get; init; }
    public double HighPriPoolRatio { get; init; } = 0.5;
    public double LowPriPoolRatio { get; init; }
    public CacheMetadataChargePolicy MetadataChargePolicy { get; init; } = CacheMetadataChargePolicy.FullCharge;

    /// <summary>The compression method (if any) used to compress entries (default LZ4; None stores them uncompressed).</summary>
    public CompressionType CompressionType { get; init; } = CompressionType.LZ4;

    /// <summary>Compression level (CompressionOptions::level); the default picks the library default.</summary>
    public int CompressionLevel { get; init; } = RocksDb.Managed.CompressionOptions.DefaultCompressionLevel;

    /// <summary>
    /// Split each saved value into chunks fitting malloc bin sizes (enable_custom_split_merge), to reduce internal
    /// fragmentation of allocators like jemalloc. Charges follow the chunk sizes.
    /// </summary>
    public bool EnableCustomSplitMerge { get; init; }

    /// <summary>Roles whose entries are stored uncompressed (do_not_compress_roles; default: filter blocks).</summary>
    public CacheEntryRoleSet DoNotCompressRoles { get; init; } = CacheEntryRoleSet.Of(CacheEntryRole.FilterBlock);

    /// <summary>MakeSharedSecondaryCache / NewCompressedSecondaryCache.</summary>
    public CompressedSecondaryCache CreateSecondaryCache() => new(this);

    internal CompressedSecondaryCacheOptions WithCapacity(long capacity) => new()
    {
        Capacity = capacity, NumShardBits = NumShardBits, StrictCapacityLimit = StrictCapacityLimit, HighPriPoolRatio = HighPriPoolRatio,
        LowPriPoolRatio = LowPriPoolRatio, MetadataChargePolicy = MetadataChargePolicy, CompressionType = CompressionType,
        CompressionLevel = CompressionLevel, EnableCustomSplitMerge = EnableCustomSplitMerge, DoNotCompressRoles = DoNotCompressRoles,
    };
}

/// <summary>
/// An in-memory secondary cache holding entries in compressed form. Port of cache/compressed_secondary_cache.{h,cc}.
/// <para>
/// When a block is found by <see cref="Lookup"/> and the primary cache has a dummy entry for it (adviseErase), it is
/// erased here and inserted into the primary cache; otherwise a dummy is inserted into the primary cache and the block
/// stays here (a standalone handle is returned to the caller). When a block is evicted from the primary cache it is
/// inserted here only if a dummy for it exists already (second eviction), else just a dummy is recorded.
/// </para>
/// <para>
/// Value format (in-memory only, never persisted): a 2-byte tag (source <see cref="CacheTier"/>, compression type) then
/// the saved or compressed data, either in one pooled array or, with <c>enable_custom_split_merge</c>, in a chain of
/// chunks sized to malloc bins. Compressed data uses RocksDB block framing of format_version 2 (varint32 size prefix).
/// </para>
/// </summary>
public sealed class CompressedSecondaryCache : SecondaryCache
{
    private const int TagSize = 2;
    // malloc_bin_sizes_ and sizeof(CacheValueChunk) - 1 (next pointer + size + 1-byte data array, minus that byte).
    private static readonly int[] s_mallocBinSizes = [128, 256, 512, 1024, 2048, 4096, 8192, 16384];
    private const int ChunkHeaderSize = 16;

    private readonly LruCache _cache;
    private readonly Lock _capacityMutex = new();
    private readonly ConcurrentCacheReservationManager _cacheResMgr;
    private CompressedSecondaryCacheOptions _options;
    private volatile bool _disableCache;
    private static readonly CacheValue s_dummy = default(CacheValue).WithHelper(CacheItemHelper.Basic(CacheEntryRole.Misc));
    private static readonly BytesCacheItemHelper s_sliceHelper = new(CacheEntryRole.Misc);

    public CompressedSecondaryCache(CompressedSecondaryCacheOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options;
        _cache = new LruCache(new LruCacheOptions
        {
            Capacity = options.Capacity, NumShardBits = options.NumShardBits, StrictCapacityLimit = options.StrictCapacityLimit,
            HighPriPoolRatio = options.HighPriPoolRatio, LowPriPoolRatio = options.LowPriPoolRatio, MetadataChargePolicy = options.MetadataChargePolicy,
        });
        _cacheResMgr = new ConcurrentCacheReservationManager(new CacheReservationManagerImpl(_cache, CacheEntryRole.Misc, delayedDecrease: false));
        _disableCache = options.Capacity == 0;
    }

    public override string Name => "CompressedSecondaryCache";

    /// <summary>Usage of the cache holding the compressed entries (TEST_GetUsage).</summary>
    public long Usage => _cache.Usage;

    /// <summary>Number of entries (including dummies).</summary>
    public int Count => _cache.Count;

    public override bool SupportForceErase => true;

    /// <summary>Value of an entry stored in chunks (CacheValueChunk chain).</summary>
    private sealed class ValueChunks(byte[][] chunks, int[] sizes, int total)
    {
        public readonly byte[][] Chunks = chunks;
        public readonly int[] Sizes = sizes;
        public readonly int Total = total;
    }

    public override SecondaryCacheResultHandle? Lookup(ReadOnlySpan<byte> key, CacheItemHelper helper, object? createContext, bool wait,
        bool adviseErase, Statistics? stats, out bool keptInSecondaryCache)
    {
        ArgumentNullException.ThrowIfNull(helper);
        keptInSecondaryCache = false;
        if (_disableCache) return null;
        CacheKey k = CacheKey.Read(key);
        if (!_cache.TryLookup(k, out CachedBlock h)) return null;
        CacheEntry e = h.Entry!;
        if (e.Buffer is null && e.Object is null)
        {
            h.Release(false);
            stats?.RecordTick(Ticker.CompressedSecondaryCacheDummyHits);
            return null;
        }

        byte[]? merged = null;
        byte[]? uncompressed = null;
        try
        {
            ReadOnlySpan<byte> tagged;
            if (e.Object is ValueChunks chunks)
            {
                merged = RocksArrayPool<byte>.Shared.Rent(chunks.Total);
                int off = 0;
                for (int i = 0; i < chunks.Chunks.Length; i++)
                {
                    chunks.Chunks[i].AsSpan(0, chunks.Sizes[i]).CopyTo(merged.AsSpan(off));
                    off += chunks.Sizes[i];
                }
                tagged = merged.AsSpan(0, chunks.Total);
            }
            else
            {
                tagged = new ReadOnlySpan<byte>(e.Buffer, 0, e.Length);
            }
            var source = (CacheTier)tagged[0];
            var type = (CompressionType)tagged[1];
            ReadOnlySpan<byte> saved = tagged[TagSize..];
            if (source == CacheTier.VolatileCompressedTier)
            {
                if (type != CompressionType.None)
                {
                    var w = new PooledBufferWriter(Math.Max(256, saved.Length * 2));
                    try
                    {
                        BlockCompression.Decompress(type, saved, 2, w);
                        uncompressed = RocksArrayPool<byte>.Shared.Rent(Math.Max(1, w.WrittenCount));
                        w.WrittenSpan.CopyTo(uncompressed);
                        saved = uncompressed.AsSpan(0, w.WrittenCount);
                    }
                    catch (RocksDbException)
                    {
                        h.Release(true);
                        return null;
                    }
                    finally
                    {
                        w.Dispose();
                    }
                    type = CompressionType.None;
                }
                // Reduced as if it came from primary cache.
                source = CacheTier.VolatileTier;
            }

            if (!helper.TryCreate(saved, type, source, createContext, out CacheValue value, out long charge))
            {
                h.Release(true);
                return null;
            }
            if (adviseErase)
            {
                h.Release(true);
                // Insert a dummy handle.
                _cache.Insert(k, s_dummy, 0, CachePriority.Low);
            }
            else
            {
                keptInSecondaryCache = true;
                h.Release(false);
            }
            stats?.RecordTick(Ticker.CompressedSecondaryCacheHits);
            return new ReadySecondaryCacheResultHandle(value, charge);
        }
        finally
        {
            if (merged is not null) RocksArrayPool<byte>.Shared.Return(merged);
            if (uncompressed is not null) RocksArrayPool<byte>.Shared.Return(uncompressed);
        }
    }

    // MaybeInsertDummy: the first time a key is offered only a dummy is recorded.
    private bool MaybeInsertDummy(in CacheKey key)
    {
        if (!_cache.TryLookup(key, out CachedBlock h))
        {
            _cache.Insert(key, s_dummy, 0, CachePriority.Low);
            return true;
        }
        h.Release(false);
        return false;
    }

    public override void Insert(ReadOnlySpan<byte> key, in CacheValue value, CacheItemHelper helper, bool forceInsert)
    {
        ArgumentNullException.ThrowIfNull(helper);
        if (value.IsPlaceholder) ThrowHelper.InvalidArgument("null value");
        CacheKey k = CacheKey.Read(key);
        if (!forceInsert && MaybeInsertDummy(k)) return;
        int size = helper.GetSaveSize(value);
        byte[] data = RocksArrayPool<byte>.Shared.Rent(Math.Max(1, size));
        try
        {
            helper.SaveTo(value, data.AsSpan(0, size));
            InsertInternal(k, data.AsSpan(0, size), helper.Role, CompressionType.None, CacheTier.VolatileCompressedTier);
        }
        finally
        {
            RocksArrayPool<byte>.Shared.Return(data);
        }
    }

    public override void InsertSaved(ReadOnlySpan<byte> key, ReadOnlySpan<byte> saved, CompressionType type, CacheTier source)
    {
        // Unexpected source (would violate InsertInternal preconditions); uncompressed and split/merge are not supported upstream.
        if (source == CacheTier.VolatileCompressedTier || type == CompressionType.None || _options.EnableCustomSplitMerge) return;
        CacheKey k = CacheKey.Read(key);
        if (MaybeInsertDummy(k)) return;
        InsertInternal(k, saved, s_sliceHelper.Role, type, source);
    }

    private void InsertInternal(in CacheKey key, ReadOnlySpan<byte> data, CacheEntryRole role, CompressionType fromType, CacheTier source)
    {
        CompressedSecondaryCacheOptions o = Volatile.Read(ref _options);
        CompressionType toType = CompressionType.None;
        PooledBufferWriter? compressed = null;
        try
        {
            if (o.CompressionType != CompressionType.None && fromType == CompressionType.None && !o.DoNotCompressRoles.Contains(role))
            {
                compressed = new PooledBufferWriter(Math.Max(64, data.Length));
                // Accept only output smaller than the input (data_size_original - 1 bound).
                if (BlockCompression.TryCompress(o.CompressionType, data, 2, o.CompressionLevel, 1023, compressed)
                    && compressed.WrittenCount < data.Length)
                {
                    toType = o.CompressionType;
                    data = compressed.WrittenSpan;
                }
            }
            byte tagSource = (byte)source;
            byte tagType = (byte)(source == CacheTier.VolatileCompressedTier ? toType : fromType);
            int taggedLength = data.Length + TagSize;
            if (o.EnableCustomSplitMerge)
            {
                ValueChunks chunks = SplitValueIntoChunks(tagSource, tagType, data, out long charge);
                _cache.Insert(key, CacheValue.FromObject(chunks), charge, CachePriority.Low);
            }
            else
            {
                byte[] buf = RocksArrayPool<byte>.Shared.Rent(taggedLength);
                buf[0] = tagSource;
                buf[1] = tagType;
                data.CopyTo(buf.AsSpan(TagSize));
                _cache.Insert(key, CacheValue.FromPooledBuffer(buf, taggedLength), taggedLength, CachePriority.Low);
            }
        }
        finally
        {
            compressed?.Dispose();
        }
    }

    // SplitValueIntoChunks: chunk sizes follow malloc bins so allocations waste little; charge is the sum of chunk sizes.
    private static ValueChunks SplitValueIntoChunks(byte tagSource, byte tagType, ReadOnlySpan<byte> data, out long charge)
    {
        int srcSize = data.Length + TagSize;
        var chunks = new List<byte[]>();
        var sizes = new List<int>();
        charge = 0;
        int srcOff = 0;
        while (srcSize > 0)
        {
            int predicted = ChunkHeaderSize + srcSize;
            int upper = UpperBound(predicted);
            int tmpSize;
            // Do not split when the value is too small, too large, or close to a bin size.
            if (upper == 0 || upper == s_mallocBinSizes.Length || s_mallocBinSizes[upper] - predicted < s_mallocBinSizes[0])
                tmpSize = predicted;
            else
                tmpSize = s_mallocBinSizes[upper - 1];
            int actual = tmpSize - ChunkHeaderSize;
            byte[] chunk = new byte[actual];
            int i = 0;
            if (srcOff == 0)
            {
                chunk[0] = tagSource;
                if (actual > 1) chunk[1] = tagType;
                i = Math.Min(actual, TagSize);
            }
            else if (srcOff == 1)
            {
                chunk[0] = tagType;
                i = 1;
            }
            int dataOff = srcOff + i - TagSize;
            data.Slice(dataOff, actual - i).CopyTo(chunk.AsSpan(i));
            srcOff += actual;
            chunks.Add(chunk);
            sizes.Add(actual);
            srcSize -= actual;
            charge += tmpSize;
        }
        return new ValueChunks(chunks.ToArray(), sizes.ToArray(), data.Length + TagSize);
    }

    private static int UpperBound(int value)
    {
        int i = 0;
        while (i < s_mallocBinSizes.Length && s_mallocBinSizes[i] <= value) i++;
        return i;
    }

    public override void Erase(ReadOnlySpan<byte> key) => _cache.Erase(CacheKey.Read(key));

    public override bool SetCapacity(long capacity)
    {
        lock (_capacityMutex)
        {
            _options = _options.WithCapacity(capacity);
            _cache.SetCapacity(capacity);
            _disableCache = capacity == 0;
        }
        return true;
    }

    public override bool TryGetCapacity(out long capacity)
    {
        lock (_capacityMutex) capacity = _options.Capacity;
        return true;
    }

    /// <summary>Takes <paramref name="decrease"/> bytes of capacity away through a reservation in the inner cache.</summary>
    public override bool Deflate(long decrease) => _cacheResMgr.UpdateCacheReservation(decrease, increase: true);

    public override bool Inflate(long increase) => _cacheResMgr.UpdateCacheReservation(increase, increase: false);

    /// <summary>Charge of the entry for <paramref name="key"/> (TEST_GetCharge); 0 if absent.</summary>
    internal long GetChargeForTest(in CacheKey key)
    {
        if (!_cache.TryLookup(key, out CachedBlock h)) return 0;
        long c = h.Charge;
        h.Release(false);
        return c;
    }

    public override string GetPrintableOptions()
    {
        CompressedSecondaryCacheOptions o = _options;
        return "    capacity : " + o.Capacity + "\n    compression_type : " + o.CompressionType + "\n";
    }
}
