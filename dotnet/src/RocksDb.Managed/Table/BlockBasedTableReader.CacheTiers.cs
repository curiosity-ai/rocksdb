using System.Buffers;
using System.Runtime.CompilerServices;
using RocksDb.Managed.Cache;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Memory;

namespace RocksDb.Managed.Table;

/// <summary>
/// Secondary cache and persistent cache support of the block-based table reader:
/// <list type="bullet">
/// <item>Cache item helpers (the BlockCreateContext / FullTypedCacheHelper callbacks of table/block_based/block_cache.h):
/// blocks are saved as their uncompressed contents and re-created (decompressing data a tier kept compressed) with the
/// reader as create context; filters are saved as their raw payload and re-created with the table's filter policy.</item>
/// <item>Lookups on caches with <see cref="BlockCache.ExtendedLookup"/> pass the helper, the reader and the DB statistics
/// (Cache::Lookup with helper); async reads start a lookup that a non-volatile tier may complete asynchronously
/// (StartAsyncLookup / WaitAll).</item>
/// <item>Block reads go through <see cref="TableReaderOptions.PersistentCache"/> when set (table/persistent_cache_helper.cc,
/// the persistent cache paths of BlockFetcher).</item>
/// </list>
/// </summary>
public sealed partial class BlockBasedTableReader
{
    /// <summary>Helper for cached <see cref="Block"/> values (data blocks, index blocks / partitions, filter partition indexes).</summary>
    private sealed class BlockItemHelper(CacheEntryRole role, BlockKind kind) : CacheItemHelper(role)
    {
        public override int GetSaveSize(in CacheValue value) => Unsafe.As<Block>(value.Object)!.Length;

        public override void SaveTo(in CacheValue value, Span<byte> destination) => Unsafe.As<Block>(value.Object)!.Data.CopyTo(destination);

        public override bool TryCreate(ReadOnlySpan<byte> data, CompressionType type, CacheTier source, object? createContext,
            out CacheValue value, out long charge)
        {
            if (createContext is not BlockBasedTableReader reader)
            {
                value = default;
                charge = 0;
                return false;
            }
            Block b = reader.CreateBlockFromSaved(data, type, kind);
            value = CacheValue.FromObject(b).WithHelper(this);
            charge = b.Charge;
            return true;
        }
    }

    /// <summary>Helper for cached filter readers (raw filter payload + <see cref="Filters.IFilterBitsReader"/>).</summary>
    private sealed class FilterItemHelper() : CacheItemHelper(CacheEntryRole.FilterBlock)
    {
        public override int GetSaveSize(in CacheValue value) => value.Length;

        public override void SaveTo(in CacheValue value, Span<byte> destination) =>
            new ReadOnlySpan<byte>(value.Buffer, 0, value.Length).CopyTo(destination);

        public override bool TryCreate(ReadOnlySpan<byte> data, CompressionType type, CacheTier source, object? createContext,
            out CacheValue value, out long charge)
        {
            value = default;
            charge = 0;
            if (createContext is not BlockBasedTableReader { _options.FilterPolicy: { } policy } reader) return false;
            byte[] buf;
            int len;
            if (type != CompressionType.None)
            {
                var w = new RentedBufferWriter(data.Length * 2, BlockBufferSource.Shared);
                try
                {
                    BlockCompression.Decompress(type, data, reader._footer.FormatVersion, w);
                    buf = w.Detach(out len);
                }
                finally
                {
                    w.Dispose();
                }
            }
            else
            {
                len = data.Length;
                buf = ArrayPool<byte>.Shared.Rent(Math.Max(1, len));
                data.CopyTo(buf);
            }
            Filters.IFilterBitsReader filter;
            try
            {
                filter = CreateFilterReader(policy, buf, len);
            }
            catch
            {
                ArrayPool<byte>.Shared.Return(buf);
                throw;
            }
            value = new CacheValue(buf, len, filter, true).WithHelper(this);
            charge = buf.Length;
            return true;
        }
    }

    // Indexed by BlockKind (Data, Index, Filter = partitioned filter index block).
    private static readonly CacheItemHelper[] s_compatHelpers =
    [
        new BlockItemHelper(CacheEntryRole.DataBlock, BlockKind.Data),
        new BlockItemHelper(CacheEntryRole.IndexBlock, BlockKind.Index),
        new BlockItemHelper(CacheEntryRole.FilterMetaBlock, BlockKind.Filter),
    ];

    private static readonly CacheItemHelper[] s_basicHelpers =
    [
        s_compatHelpers[0].WithoutSecondaryCompat,
        s_compatHelpers[1].WithoutSecondaryCompat,
        s_compatHelpers[2].WithoutSecondaryCompat,
    ];

    private static readonly FilterItemHelper s_filterHelper = new();

    /// <summary>The helpers of a reader: secondary-cache compatible unless lowest_used_cache_tier is kVolatileTier.</summary>
    private static CacheItemHelper[] HelpersFor(TableReaderOptions options) =>
        options.LowestUsedCacheTier > CacheTier.VolatileTier ? s_compatHelpers : s_basicHelpers;

    private static CacheItemHelper FilterHelperFor(TableReaderOptions options) =>
        options.LowestUsedCacheTier > CacheTier.VolatileTier ? s_filterHelper : s_filterHelper.WithoutSecondaryCompat;

    /// <summary>Primary lookup; caches with a secondary tier (or a simulator) get the helper, the reader and the statistics.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool LookupCache(BlockCache cache, in CacheKey key, BlockKind kind, out CachedBlock h) =>
        cache.ExtendedLookup ? LookupExtended(cache, key, kind, out h) : cache.TryLookup(key, out h);

    [MethodImpl(MethodImplOptions.NoInlining)]
    private bool LookupExtended(BlockCache cache, in CacheKey key, BlockKind kind, out CachedBlock h) =>
        cache.TryLookup(key, _helpers[(int)kind], this, kind == BlockKind.Data ? CachePriority.Low : _metaPriority, _stats?.Owner, out h);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool LookupFilterCache(BlockCache cache, in CacheKey key, out CachedBlock h) =>
        cache.ExtendedLookup
            ? cache.TryLookup(key, FilterHelperFor(_options), this, _metaPriority, _stats?.Owner, out h)
            : cache.TryLookup(key, out h);

    /// <summary>
    /// Async block lookup through a cache with a secondary tier: StartAsyncLookup, then WaitAllAsync when the secondary
    /// lookup is pending (a non-volatile tier doing I/O), else read the block from the file.
    /// </summary>
    private async ValueTask<BlockRef> GetBlockRefExtendedAsync(BlockCache cache, CacheKey key, BlockHandle handle, BlockKind kind,
        TableReadFlags flags, CancellationToken ct)
    {
        var lookup = new AsyncLookupHandle
        {
            Key = key, Helper = _helpers[(int)kind], CreateContext = this,
            Priority = kind == BlockKind.Data ? CachePriority.Low : _metaPriority, Statistics = _stats?.Owner,
        };
        cache.StartAsyncLookup(lookup);
        if (lookup.IsPending) await cache.WaitAllAsync([lookup], 1, ct).ConfigureAwait(false);
        CachedBlock h = lookup.TakeResult();
        if (h.HasValue)
        {
            _stats?.Record(HitCounter(kind));
            if (_bcTracer is { IsEnabled: true } t) TraceAccess(t, key, kind, Unsafe.As<Block>(h.Value), hit: true, flags);
            return BlockRef.FromCache(Unsafe.As<Block>(h.Value)!, h);
        }
        _stats?.Record(MissCounter(kind));
        return await ReadAndInsertAsync(cache, key, handle, kind, flags, ct).ConfigureAwait(false);
    }

    /// <summary>Re-creates a block from its saved form (uncompressed contents, or a compressed payload kept by a tier).</summary>
    private Block CreateBlockFromSaved(ReadOnlySpan<byte> data, CompressionType type, BlockKind kind)
    {
        if (type != CompressionType.None) return Protect(BlockFetcher.Decompress(type, data, _footer.FormatVersion, DictFor(kind)), kind);
        byte[] buf = BlockBufferPool.Rent(Math.Max(data.Length, 1));
        data.CopyTo(buf);
        return Protect(Block.Create(buf, data.Length, rentedFromSharedPool: true), kind);
    }

    /// <summary>
    /// Reads a block from the file, through the persistent cache if configured (BlockFetcher with
    /// PersistentCacheHelper): a serialized (compressed) cache stores the payload + trailer and is verified like a file
    /// read; an uncompressed cache stores the block contents.
    /// </summary>
    private Block ReadBlockFromFile(BlockHandle handle, BlockKind kind, TableReadFlags flags)
    {
        PersistentCache? pc = _persistentCache;
        if (pc is null) return BlockFetcher.ReadBlock(_file, _footer, handle, Verify(flags), DictFor(kind));
        return ReadBlockThroughPersistentCache(pc, handle, kind);
    }

    /// <summary>
    /// Async <see cref="ReadBlockThroughPersistentCache"/>: the persistent cache lookup (<see cref="PersistentCache.LookupAsync"/>)
    /// and the file read on a miss use async I/O.
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<Block> ReadBlockThroughPersistentCacheAsync(PersistentCache pc, BlockHandle handle, BlockKind kind, CancellationToken ct)
    {
        byte[] key = ArrayPool<byte>.Shared.Rent(CacheKey.Size);
        try
        {
            _baseCacheKey.WithOffsetForMinSizeRecord(handle.Offset).WriteTo(key);
            ReadOnlyMemory<byte> k = key.AsMemory(0, CacheKey.Size);
            Statistics? stats = _stats?.Owner;
            int serializedSize = checked((int)handle.Size + BlockBasedTableConstants.BlockTrailerSize);
            PersistentCacheLookupResult hit = await pc.LookupAsync(k, ct).ConfigureAwait(false);
            if (pc.IsCompressed)
            {
                if (hit.Found)
                {
                    try
                    {
                        if (hit.Length == serializedSize)
                        {
                            stats?.RecordTick(Ticker.PersistentCacheHit);
                            return BlockFetcher.ProcessSerializedCopy(hit.Data.AsSpan(0, hit.Length), _footer, handle, _options.VerifyChecksums, DictFor(kind));
                        }
                    }
                    finally
                    {
                        ArrayPool<byte>.Shared.Return(hit.Data);
                    }
                }
                stats?.RecordTick(Ticker.PersistentCacheMiss);
                byte[] raw = ArrayPool<byte>.Shared.Rent(serializedSize);
                try
                {
                    int read = 0;
                    while (read < serializedSize)
                    {
                        int r = await _file.ReadAsync((long)handle.Offset + read, raw.AsMemory(read, serializedSize - read), ct).ConfigureAwait(false);
                        if (r <= 0) break;
                        read += r;
                    }
                    if (read != serializedSize) ThrowHelper.Corruption("truncated block read from file at offset " + handle.Offset);
                    Block b = BlockFetcher.ProcessSerializedCopy(raw.AsSpan(0, serializedSize), _footer, handle, _options.VerifyChecksums, DictFor(kind));
                    // Only verified blocks are cached (InsertSerialized after the checksum check).
                    pc.Insert(k.Span, raw.AsSpan(0, serializedSize));
                    return b;
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(raw);
                }
            }
            if (hit.Found)
            {
                try
                {
                    stats?.RecordTick(Ticker.PersistentCacheHit);
                    byte[] copy = BlockBufferPool.Rent(Math.Max(hit.Length, 1));
                    hit.Data.AsSpan(0, hit.Length).CopyTo(copy);
                    return Block.Create(copy, hit.Length, rentedFromSharedPool: true);
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(hit.Data);
                }
            }
            stats?.RecordTick(Ticker.PersistentCacheMiss);
            Block block = await BlockFetcher.ReadBlockAsync(_file, _footer, handle, _options.VerifyChecksums, DictFor(kind), ct).ConfigureAwait(false);
            pc.Insert(k.Span, block.Data);
            return block;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(key);
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private Block ReadBlockThroughPersistentCache(PersistentCache pc, BlockHandle handle, BlockKind kind)
    {
        Span<byte> key = stackalloc byte[CacheKey.Size];
        _baseCacheKey.WithOffsetForMinSizeRecord(handle.Offset).WriteTo(key);
        Statistics? stats = _stats?.Owner;
        int serializedSize = checked((int)handle.Size + BlockBasedTableConstants.BlockTrailerSize);
        if (pc.IsCompressed)
        {
            if (pc.TryLookup(key, out byte[] data, out int length))
            {
                try
                {
                    if (length == serializedSize)
                    {
                        stats?.RecordTick(Ticker.PersistentCacheHit);
                        return BlockFetcher.ProcessSerializedCopy(data.AsSpan(0, length), _footer, handle, _options.VerifyChecksums, DictFor(kind));
                    }
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(data);
                }
            }
            stats?.RecordTick(Ticker.PersistentCacheMiss);
            byte[] raw = ArrayPool<byte>.Shared.Rent(serializedSize);
            try
            {
                int read = BlockFetcher.ReadFully(_file, (long)handle.Offset, raw.AsSpan(0, serializedSize));
                if (read != serializedSize) ThrowHelper.Corruption("truncated block read from file at offset " + handle.Offset);
                Block b = BlockFetcher.ProcessSerializedCopy(raw.AsSpan(0, serializedSize), _footer, handle, _options.VerifyChecksums, DictFor(kind));
                // Only verified blocks are cached (InsertSerialized after the checksum check).
                pc.Insert(key, raw.AsSpan(0, serializedSize));
                return b;
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(raw);
            }
        }
        if (pc.TryLookup(key, out byte[] contents, out int contentLength))
        {
            try
            {
                stats?.RecordTick(Ticker.PersistentCacheHit);
                byte[] copy = BlockBufferPool.Rent(Math.Max(contentLength, 1));
                contents.AsSpan(0, contentLength).CopyTo(copy);
                return Block.Create(copy, contentLength, rentedFromSharedPool: true);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(contents);
            }
        }
        stats?.RecordTick(Ticker.PersistentCacheMiss);
        Block block = BlockFetcher.ReadBlock(_file, _footer, handle, _options.VerifyChecksums, DictFor(kind));
        pc.Insert(key, block.Data);
        return block;
    }
}
