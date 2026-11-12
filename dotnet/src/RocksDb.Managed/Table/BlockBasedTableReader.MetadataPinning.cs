using System.Runtime.CompilerServices;
using RocksDb.Managed.Cache;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table.Filters;

namespace RocksDb.Managed.Table;

/// <summary>
/// Which metadata blocks of a table the reader pins in the block cache. Port of the is_pinned / pin_top_level_index /
/// pin_partition / pin_unpartitioned computation of BlockBasedTable::PrefetchIndexAndFilterBlocks
/// (table/block_based/block_based_table_reader.cc).
/// </summary>
internal readonly record struct MetadataPins(bool TopLevelIndex, bool Partition, bool Unpartitioned)
{
    /// <summary>
    /// metadata_cache_options with the kFallback tiers resolved through the legacy booleans:
    /// top_level_index_pinning falls back to kAll under pin_top_level_index_and_filter, partition_pinning and
    /// unpartitioned_pinning to kFlushedAndSimilar under pin_l0_filter_and_index_blocks_in_cache, kNone otherwise.
    /// kFlushedAndSimilar pins level-0 files of at most max_file_size_for_l0_meta_pin bytes (maybe_flushed).
    /// </summary>
    public static MetadataPins Compute(TableReaderOptions options, long fileSize)
    {
        bool maybeFlushed = options.Level == 0 && fileSize <= options.MaxFileSizeForL0MetaPin;
        PinningTier l0Fallback = options.PinL0FilterAndIndexBlocksInCache ? PinningTier.FlushedAndSimilar : PinningTier.None;
        return new MetadataPins(
            IsPinned(options.TopLevelIndexPinning, options.PinTopLevelIndexAndFilter ? PinningTier.All : PinningTier.None, maybeFlushed),
            IsPinned(options.PartitionPinning, l0Fallback, maybeFlushed),
            IsPinned(options.UnpartitionedPinning, l0Fallback, maybeFlushed));
    }

    private static bool IsPinned(PinningTier tier, PinningTier fallback, bool maybeFlushed)
    {
        if (tier == PinningTier.Fallback) tier = fallback;
        return tier switch
        {
            PinningTier.All => true,
            PinningTier.FlushedAndSimilar => maybeFlushed,
            _ => false,
        };
    }
}

/// <summary>
/// Pinning of the index and filter blocks in the block cache. Port of the index / filter part of
/// BlockBasedTable::PrefetchIndexAndFilterBlocks and of the readers it creates:
/// BinarySearchIndexReader / HashIndexReader / PartitionIndexReader::Create and IndexReaderCommon::GetOrReadIndexBlock
/// (index_reader_common.cc), FullFilterBlockReader / PartitionedFilterBlockReader::Create and
/// FilterBlockReaderCommon::GetOrReadFilterBlock (filter_block_reader_common.cc), PartitionIndexReader::CacheDependencies
/// (partitioned_index_reader.cc) and PartitionedFilterBlockReader::CacheDependencies (partitioned_filter_block.cc).
/// <list type="bullet">
/// <item>With cache_index_and_filter_blocks (use_cache) the (top-level) index and the full filter / top-level filter
/// index are block cache entries. At open (prefetch = prefetch_all || pin) each is looked up, and read and inserted on
/// a miss. A pinned one keeps its cache handle for the reader's lifetime and is used without a lookup; an unpinned one
/// is released again and looked up on every use (re-read after eviction; Incomplete for the index and "may match" for
/// the filter when the read is restricted to the block cache).</item>
/// <item>Index and filter partitions always live in the block cache. At open (prefetch_all || pin_partition) all of
/// them are loaded into it (CacheDependencies); with pin_partition the reader keeps their handles and uses them
/// without a lookup.</item>
/// </list>
/// The pinned blocks are exposed through the fields the reader-owned blocks use (<see cref="_indexBlock"/>,
/// <see cref="_filter"/>, <see cref="_filterIndexBlock"/>), so reads of a table that owns or pins its metadata take
/// the same path, with no block cache lookup.
/// </summary>
public sealed partial class BlockBasedTableReader
{
    /// <summary>The reference behind a pinned <see cref="_indexBlock"/> (a block cache handle, or ownership after a rejected insert); empty for an index read at open.</summary>
    private BlockRef _indexRef;

    /// <summary>The reference behind a pinned <see cref="_filterIndexBlock"/>; empty for a top-level filter index read at open.</summary>
    private BlockRef _filterIndexRef;

    /// <summary>The block cache handle of a pinned full filter (<see cref="_filter"/>).</summary>
    private CachedBlock _filterLease;

    /// <summary>Pool the buffer of an owned full filter (<see cref="_filterBuffer"/>) goes back to.</summary>
    private BlockBufferSource _filterBufferSource = BlockBufferSource.TableBuffers;

    /// <summary>
    /// The pinned index partitions by block offset (partition_map_ of PartitionIndexReader); null unless every
    /// partition was pinned. Immutable once the reader is published.
    /// </summary>
    private Dictionary<ulong, Block>? _pinnedIndexPartitions;

    private BlockRef[]? _pinnedIndexPartitionRefs;

    /// <summary>Number of index partitions the reader pins (tests).</summary>
    internal int PinnedIndexPartitionCount => _pinnedIndexPartitions?.Count ?? 0;

    /// <summary>Number of filter partitions the reader pins or owns (tests).</summary>
    internal int PinnedFilterPartitionCount => _pinnedFilterPartitions?.Count ?? 0;

    /// <summary>
    /// The index and filter part of BlockBasedTable::PrefetchIndexAndFilterBlocks for a reader with a block cache, in
    /// upstream's order: index reader, index partitions, filter reader, filter partitions, compression dictionary.
    /// <paramref name="useCache"/> is cache_index_and_filter_blocks. Blocks are read through <paramref name="file"/>.
    /// Errors fail the open, except those of the filter prefetch (the filter readers' Create then returns no reader).
    /// </summary>
    private void PrefetchMetadataIntoCache(BlockCache cache, bool useCache, IRandomAccessFile file, MetadataPins pins, bool prefetchAll)
    {
        bool partitionedIndex = IsPartitionedIndex;
        if (useCache)
        {
            // pin the first level of index; prefetch the first level of index
            bool pinIndex = partitionedIndex ? pins.TopLevelIndex : pins.Unpartitioned;
            if (prefetchAll || pinIndex)
            {
                BlockRef index = LookupOrReadMetaBlock(cache, file, IndexHandle, BlockKind.Index);
                if (pinIndex)
                {
                    _indexRef = index;
                    _indexBlock = index.Block;
                }
                else
                {
                    index.Release();
                }
            }
        }
        // The partitions of partitioned index are always stored in cache. They hence follow the configuration for pin and
        // prefetch regardless of the value of cache_index_and_filter_blocks.
        if (partitionedIndex && (prefetchAll || pins.Partition)) CacheIndexPartitions(cache, file, pins.Partition);

        if (_filterKind == FilterKind.Full)
        {
            if (useCache && (prefetchAll || pins.Unpartitioned))
            {
                try
                {
                    PrefetchFilterIntoCache(cache, file, pins.Unpartitioned);
                }
                catch (Exception e) when (e is RocksDbException or IOException)
                {
                    // FullFilterBlockReader::Create: a filter block that cannot be read is ignored
                    // (IGNORE_STATUS_IF_ERROR): no filter reader is created and the table is opened without a filter.
                    DropFilter();
                }
            }
        }
        else if (_filterKind == FilterKind.Partitioned)
        {
            if (useCache && (prefetchAll || pins.TopLevelIndex))
            {
                try
                {
                    BlockRef top = LookupOrReadMetaBlock(cache, file, _filterHandle, BlockKind.Filter);
                    if (pins.TopLevelIndex)
                    {
                        _filterIndexRef = top;
                        _filterIndexBlock = top.Block;
                    }
                    else
                    {
                        top.Release();
                    }
                }
                catch (Exception e) when (e is RocksDbException or IOException)
                {
                    // PartitionedFilterBlockReader::Create: ignored like the full filter's, so CacheDependencies is not
                    // reached either.
                    DropFilter();
                }
            }
            if (_filterKind == FilterKind.Partitioned && (prefetchAll || pins.Partition)) CacheFilterPartitions(cache, file, pins.Partition);
        }

        // UncompressionDictReader::Create with use_cache: looked up (read and inserted on a miss) at open and kept only
        // when pinned. Unlike the filter, a dictionary that cannot be read fails the open.
        if (useCache && _dictHandle.Size > 0) PrefetchDictionaryIntoCache(file, prefetchAll || pins.Unpartitioned, pins.Unpartitioned);
    }

    /// <summary>The table is read without its filter from now on (rep_-&gt;filter stays null). Only before the reader is published.</summary>
    private void DropFilter()
    {
        _filterKind = FilterKind.None;
        _hasFilter = false;
    }

    /// <summary>
    /// BlockBasedTable::RetrieveBlock at open for a block held as a <see cref="Block"/> (index block, index partition,
    /// top-level filter index): a block cache lookup (with the secondary tiers), and on a miss a read through
    /// <paramref name="file"/> and an insert. Hits, misses and adds are recorded like any lookup of the block kind.
    /// The caller releases the reference, or keeps it to pin the block.
    /// </summary>
    private BlockRef LookupOrReadMetaBlock(BlockCache cache, IRandomAccessFile file, BlockHandle handle, BlockKind kind)
    {
        CacheKey key = _baseCacheKey.WithOffsetForMinSizeRecord(handle.Offset);
        if (LookupCache(cache, key, kind, out CachedBlock h))
        {
            _stats?.RecordHit(HitCounter(kind), h.Charge);
            return BlockRef.FromCache(Unsafe.As<Block>(h.Value)!, h);
        }
        _stats?.Record(MissCounter(kind));
        Block b = Protect(BlockFetcher.ReadBlock(file, _footer, handle, VerifyRead(TableReadFlags.None), null, BlockBufferSource.BlockBufferPool), kind);
        return InsertIntoCache(cache, key, b, kind);
    }

    /// <summary>
    /// Port of PartitionIndexReader::CacheDependencies: loads every index partition into the block cache (a lookup,
    /// then a read and insert on a miss), and with <paramref name="pin"/> keeps them. The partitions are only used
    /// pinned when all of them could be kept (map_in_progress.size() == partition_count).
    /// </summary>
    private void CacheIndexPartitions(BlockCache cache, IRandomAccessFile file, bool pin)
    {
        var pinned = pin ? new List<(ulong Offset, BlockRef Ref)>() : null;
        var seen = pin ? new Dictionary<ulong, Block>() : null;
        int partitionCount = 0;
        // GetOrReadIndexBlock: the pinned top level, or a block cache lookup.
        BlockRef top = GetIndexBlockRef(TableReadFlags.None);
        var it = new BlockIter();
        try
        {
            InitIndexIter(it, top.Block!, isPartition: false);
            for (it.SeekToFirst(); it.Valid; it.Next())
            {
                BlockHandle handle = it.BlockHandle;
                partitionCount++;
                BlockRef partition = LookupOrReadMetaBlock(cache, file, handle, BlockKind.Index);
                if (seen is not null && seen.TryAdd(handle.Offset, partition.Block!)) pinned!.Add((handle.Offset, partition));
                else partition.Release();
            }
        }
        catch
        {
            if (pinned is not null)
            {
                foreach ((ulong _, BlockRef r) in pinned)
                {
                    BlockRef copy = r;
                    copy.Release();
                }
            }
            throw;
        }
        finally
        {
            it.Dispose();
            top.Release();
        }
        if (pinned is null) return;
        var refs = new BlockRef[pinned.Count];
        for (int i = 0; i < refs.Length; i++) refs[i] = pinned[i].Ref;
        if (pinned.Count != partitionCount)
        {
            // All or nothing: map_in_progress is dropped, releasing what it held.
            for (int i = 0; i < refs.Length; i++) refs[i].Release();
            return;
        }
        _pinnedIndexPartitionRefs = refs;
        _pinnedIndexPartitions = seen;
    }

    private void ReleasePinnedIndexPartitions()
    {
        BlockRef[]? refs = _pinnedIndexPartitionRefs;
        if (refs is null) return;
        _pinnedIndexPartitionRefs = null;
        for (int i = 0; i < refs.Length; i++) refs[i].Release();
    }

    /// <summary>
    /// An index partition for an index iterator (PartitionIndexReader::NewIterator): the pinned partition when the
    /// reader pins them (partition_map_), else a block cache lookup, reading and inserting the partition on a miss.
    /// </summary>
    internal BlockRef GetIndexPartitionRef(BlockHandle handle, TableReadFlags flags) =>
        _pinnedIndexPartitions is { } pinned && pinned.TryGetValue(handle.Offset, out Block? b)
            ? BlockRef.FromPinned(b)
            : GetBlockRef(handle, BlockKind.Index, flags);

    /// <summary>Async <see cref="GetIndexPartitionRef"/>; completes synchronously for a pinned or cached partition.</summary>
    internal ValueTask<BlockRef> GetIndexPartitionRefAsync(BlockHandle handle, TableReadFlags flags, CancellationToken ct) =>
        _pinnedIndexPartitions is { } pinned && pinned.TryGetValue(handle.Offset, out Block? b)
            ? new ValueTask<BlockRef>(BlockRef.FromPinned(b))
            : GetBlockRefAsync(handle, BlockKind.Index, flags, ct);

    /// <summary>
    /// A filter block (the full filter or a filter partition) through the block cache at open: a lookup, and on a miss
    /// a read through <paramref name="file"/> and an insert. Returns the cache handle; when the cache rejected the
    /// insert (strict capacity limit) the handle is empty and the caller owns <paramref name="ownedBuffer"/> (from
    /// <see cref="RocksArrayPool{T}.Shared"/>) and <paramref name="ownedFilter"/>.
    /// </summary>
    private CachedBlock LookupOrReadFilterBlock(BlockCache cache, IRandomAccessFile file, BlockHandle handle, out byte[]? ownedBuffer,
        out IFilterBitsReader? ownedFilter)
    {
        ownedBuffer = null;
        ownedFilter = null;
        CacheKey key = _baseCacheKey.WithOffsetForMinSizeRecord(handle.Offset);
        if (LookupFilterCache(cache, key, out CachedBlock h))
        {
            _stats?.RecordHit(BlockCacheCounter.FilterHit, h.Charge);
            return h;
        }
        _stats?.Record(BlockCacheCounter.FilterMiss);
        // A cached filter's buffer is returned to RocksArrayPool.Shared by the cache entry (CacheValue).
        byte[] buf = BlockFetcher.ReadRawPayload(file, _footer, handle, VerifyRead(TableReadFlags.None), BlockBufferSource.Shared, out int len);
        IFilterBitsReader filter;
        try
        {
            filter = CreateFilterReader(_options.FilterPolicy!, buf, len);
        }
        catch
        {
            RocksArrayPool<byte>.Shared.Return(buf);
            throw;
        }
        if (cache.TryInsert(key, new CacheValue(buf, len, filter, true).WithHelper(FilterHelperFor(_options)), buf.Length, _metaPriority, out h))
        {
            RecordAdd(_stats, BlockKind.Filter, buf.Length, h.InsertedRedundantly);
            return h;
        }
        _stats?.Record(BlockCacheCounter.AddFailure);
        ownedBuffer = buf;
        ownedFilter = filter;
        return default;
    }

    /// <summary>
    /// Port of FullFilterBlockReader::Create with use_cache and prefetch: the filter is looked up in the block cache,
    /// and read and inserted on a miss. With <paramref name="pin"/> the reader keeps the entry (or the filter itself
    /// when the cache rejected it); otherwise it is released and every filter probe looks it up.
    /// </summary>
    private void PrefetchFilterIntoCache(BlockCache cache, IRandomAccessFile file, bool pin)
    {
        CachedBlock h = LookupOrReadFilterBlock(cache, file, _filterHandle, out byte[]? ownedBuffer, out IFilterBitsReader? ownedFilter);
        if (h.HasValue)
        {
            if (pin)
            {
                _filterLease = h;
                _filter = Unsafe.As<IFilterBitsReader>(h.Value);
            }
            else
            {
                h.Dispose();
            }
        }
        else if (pin)
        {
            _filterBufferSource = BlockBufferSource.Shared;
            _filterBuffer = ownedBuffer;
            _filter = ownedFilter;
        }
        else
        {
            RocksArrayPool<byte>.Shared.Return(ownedBuffer!);
        }
    }

    /// <summary>
    /// Port of PartitionedFilterBlockReader::CacheDependencies: loads every filter partition into the block cache (a
    /// lookup, then a read and insert on a miss), and with <paramref name="pin"/> keeps the cached ones (filter_map_).
    /// A partition that is not pinned is looked up for each probe.
    /// </summary>
    private void CacheFilterPartitions(BlockCache cache, IRandomAccessFile file, bool pin)
    {
        // GetOrReadFilterBlock: the owned or pinned top level, or a block cache lookup.
        BlockRef top = _filterIndexBlock is not null ? BlockRef.FromPinned(_filterIndexBlock) : GetBlockRef(_filterHandle, BlockKind.Filter, TableReadFlags.None);
        PinnedFilterPartitions? pinned = pin ? new PinnedFilterPartitions() : null;
        var it = new BlockIter();
        try
        {
            InitFilterIndexIter(it, top.Block!);
            for (it.SeekToFirst(); it.Valid; it.Next())
            {
                BlockHandle handle = it.BlockHandle;
                if (handle.Size == 0) continue;
                CachedBlock h = LookupOrReadFilterBlock(cache, file, handle, out byte[]? ownedBuffer, out _);
                if (!h.HasValue)
                {
                    RocksArrayPool<byte>.Shared.Return(ownedBuffer!);
                    continue;
                }
                if (pinned is null || !pinned.TryAddCached(handle.Offset, h)) h.Dispose();
            }
        }
        catch
        {
            pinned?.Dispose();
            throw;
        }
        finally
        {
            it.Dispose();
            top.Release();
        }
        _pinnedFilterPartitions = pinned;
    }
}
