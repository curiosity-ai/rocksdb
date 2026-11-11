using System.Runtime.CompilerServices;
using RocksDb.Managed.Cache;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Env;
using RocksDb.Managed.Memory;

namespace RocksDb.Managed.Table;

/// <summary>
/// The compression dictionary block ("rocksdb.compression_dict") of the block-based table reader. Port of
/// table/block_based/uncompression_dict_reader.cc and of its call sites in block_based_table_reader.cc /
/// block_based_table_reader_sync_and_async.h:
/// <list type="bullet">
/// <item>Without cache_index_and_filter_blocks (use_cache = false) the reader owns the dictionary: it is read at open,
/// counted in <see cref="ApproximateMemoryUsage"/> and never touches the block cache.</item>
/// <item>With it, the dictionary is a block cache entry (role <see cref="CacheEntryRole.OtherBlock"/>, the priority of
/// the other metadata blocks, charged its <see cref="CompressionDictionary.ApproximateMemoryUsage"/>). At open it is
/// looked up, and read and inserted on a miss (UncompressionDictReader::Create with prefetch). A pinned dictionary
/// (<see cref="PinUnpartitioned"/>) keeps its cache handle for the reader's lifetime, so reads use it directly. An
/// unpinned one is looked up again for every data block retrieval (GetOrReadUncompressionDictionary in
/// NewDataBlockIterator, once per file in MultiGet) and re-read after eviction.</item>
/// </list>
/// Lookups record BLOCK_CACHE_COMPRESSION_DICT_{HIT,MISS,ADD,BYTES_INSERT} and the totals BLOCK_CACHE_{HIT,MISS,ADD,
/// BYTES_READ,BYTES_WRITE} (BlockBasedTable::UpdateCacheHitMetrics / UpdateCacheMissMetrics / UpdateCacheInsertionMetrics).
/// </summary>
public sealed partial class BlockBasedTableReader
{
    /// <summary>Handle of the compression dictionary block; Size 0 when the table has none.</summary>
    private BlockHandle _dictHandle;

    /// <summary>
    /// True when the dictionary lives in the block cache without being pinned by the reader: data block retrievals then
    /// look it up first. False for tables without a dictionary and for an owned or pinned one (<see cref="_compressionDict"/>),
    /// so those reads pay one predictable branch and no lookup. Set before the reader is published.
    /// </summary>
    private bool _dictLookupPerRead;

    /// <summary>The reader's own reference: the owned dictionary, or the block cache handle of a pinned one.</summary>
    private DictLease _dictLease;

    /// <summary>
    /// A reference to a dictionary (CachableEntry&lt;DecompressorDict&gt;): a block cache handle, or ownership of a
    /// dictionary that is not in the cache (fill_cache = false, or an insert the cache rejected).
    /// </summary>
    private struct DictLease
    {
        public CompressionDictionary? Dict;
        private CachedBlock _handle;
        private bool _owned;

        public static DictLease Owned(CompressionDictionary dict) => new() { Dict = dict, _owned = true };

        public static DictLease Cached(CachedBlock handle) =>
            new() { Dict = Unsafe.As<CompressionDictionary>(handle.Value), _handle = handle };

        /// <summary>CachableEntry::GetOwnValue.</summary>
        public readonly bool IsOwned => _owned;

        public void Release()
        {
            if (_handle.HasValue) _handle.Dispose();
            else if (_owned) Dict?.Dispose();
            this = default;
        }
    }

    /// <summary>
    /// Helper of cached dictionaries (the DecompressorDict instantiation of BlockCreateContext::Create,
    /// table/block_based/block_cache.cc): saved as the raw dictionary bytes and re-created from them.
    /// </summary>
    private sealed class DictItemHelper() : CacheItemHelper(CacheEntryRole.OtherBlock)
    {
        public override int GetSaveSize(in CacheValue value) => Unsafe.As<CompressionDictionary>(value.Object)!.Length;

        public override void SaveTo(in CacheValue value, Span<byte> destination) =>
            Unsafe.As<CompressionDictionary>(value.Object)!.Data.CopyTo(destination);

        public override bool TryCreate(ReadOnlySpan<byte> data, CompressionType type, CacheTier source, object? createContext,
            out CacheValue value, out long charge)
        {
            value = default;
            charge = 0;
            if (createContext is not BlockBasedTableReader reader) return false;
            byte[] raw;
            if (type != CompressionType.None)
            {
                var w = new RentedBufferWriter(data.Length * 2, BlockBufferSource.Shared);
                try
                {
                    BlockCompression.Decompress(type, data, reader._footer.FormatVersion, w);
                    byte[] rented = w.Detach(out int length);
                    raw = rented.AsSpan(0, length).ToArray();
                    RocksArrayPool<byte>.Shared.Return(rented);
                }
                finally
                {
                    w.Dispose();
                }
            }
            else
            {
                raw = data.ToArray();
            }
            var dict = new CompressionDictionary(raw);
            if (reader.DigestsDictionary) dict.DigestForDecompression();
            value = CacheValue.FromObject(dict).WithHelper(this);
            charge = dict.ApproximateMemoryUsage;
            return true;
        }
    }

    private static readonly DictItemHelper s_dictHelper = new();

    private CacheItemHelper DictHelper =>
        _options.LowestUsedCacheTier > CacheTier.VolatileTier ? s_dictHelper : s_dictHelper.WithoutSecondaryCompat;

    /// <summary>
    /// A cached dictionary of a ZSTD table is digested when it is created (DecompressorDict::Populate builds the
    /// dictionary-aware decompressor up front), so that its cache charge includes the digested form.
    /// </summary>
    private bool DigestsDictionary => Properties.CompressionName == "ZSTD";

    /// <summary>
    /// Port of pin_unpartitioned in BlockBasedTable::PrefetchIndexAndFilterBlocks: metadata_cache_options.unpartitioned_pinning,
    /// with kFallback meaning kFlushedAndSimilar under pin_l0_filter_and_index_blocks_in_cache and kNone otherwise.
    /// kFlushedAndSimilar pins level-0 files of at most max_file_size_for_l0_meta_pin bytes (maybe_flushed).
    /// </summary>
    internal static bool PinUnpartitioned(TableReaderOptions options, long fileSize)
    {
        bool maybeFlushed = options.Level == 0 && fileSize <= options.MaxFileSizeForL0MetaPin;
        return options.UnpartitionedPinning switch
        {
            PinningTier.All => true,
            PinningTier.FlushedAndSimilar => maybeFlushed,
            PinningTier.None => false,
            _ => options.PinL0FilterAndIndexBlocksInCache && maybeFlushed,
        };
    }

    /// <summary>
    /// Reads the dictionary block from the file (the read of RetrieveBlock for BlockType::kCompressionDictionary):
    /// checksum mismatches and short reads surface as Corruption, I/O errors as thrown by the file.
    /// </summary>
    private static CompressionDictionary ReadDictionaryBlock(IRandomAccessFile file, in Footer footer, BlockHandle handle,
        BlockFetcher.ChecksumVerification verify, bool digest)
    {
        byte[] raw = BlockFetcher.ReadRawPayload(file, footer, handle, verify, out int length);
        CompressionDictionary dict;
        try
        {
            dict = new CompressionDictionary(raw.AsSpan(0, length).ToArray());
        }
        finally
        {
            RocksArrayPool<byte>.Shared.Return(raw);
        }
        // BlockFetcher::ReadBlockContents: PERF_COUNTER_ADD(compression_dict_block_read_count, 1).
        PerfContext.Add(PerfMetric.CompressionDictBlockReadCount, 1);
        if (digest) dict.DigestForDecompression();
        return dict;
    }

    /// <summary>
    /// Port of UncompressionDictReader::Create with use_cache and prefetch: the dictionary is looked up in the block
    /// cache, and read and inserted on a miss. With <paramref name="pin"/> the reader keeps the entry; otherwise the
    /// entry is released again and every data block retrieval looks it up. Read errors fail the open.
    /// </summary>
    private void PrefetchDictionaryIntoCache(IRandomAccessFile file, bool pin)
    {
        DictLease lease = GetOrReadDictionary(TableReadFlags.None, file);
        if (pin)
        {
            _compressionDict = lease.Dict;
            _dictLease = lease;
        }
        else
        {
            lease.Release();
            _dictLookupPerRead = true;
        }
    }

    /// <summary>
    /// Port of UncompressionDictReader::ReadUncompressionDictionary with use_cache (BlockBasedTable::RetrieveBlock /
    /// MaybeReadBlockAndLoadToCache for the dictionary block): a block cache lookup; on a miss the block is read from
    /// <paramref name="file"/> and inserted, unless the read is restricted to the block cache
    /// (<see cref="TableReadFlags.BlockCacheTier"/>: Incomplete) or does not fill it
    /// (<see cref="TableReadFlags.DontFillCache"/>: the caller owns the dictionary it read). The caller releases the lease.
    /// </summary>
    private DictLease GetOrReadDictionary(TableReadFlags flags, IRandomAccessFile file)
    {
        BlockCache cache = _cache!;
        CacheKey key = _baseCacheKey.WithOffsetForMinSizeRecord(_dictHandle.Offset);
        bool found = cache.ExtendedLookup
            ? cache.TryLookup(key, DictHelper, this, _metaPriority, _stats?.Owner, out CachedBlock h)
            : cache.TryLookup(key, out h);
        if (found)
        {
            // BlockBasedTable::UpdateCacheHitMetrics, BlockType::kCompressionDictionary.
            long usage = h.Charge;
            _stats?.RecordHit(BlockCacheCounter.CompressionDictHit, usage);
            PerfContext.Add(PerfMetric.BlockCacheHitCount, 1);
            PerfContext.Add(PerfMetric.BlockCacheCompressionDictReadByte, (ulong)usage);
            if (_bcTracer is { IsEnabled: true } t) TraceDictionaryAccess(t, key, (ulong)usage, hit: true, flags);
            return DictLease.Cached(h);
        }
        _stats?.Record(BlockCacheCounter.CompressionDictMiss); // UpdateCacheMissMetrics
        ThrowIfNoIo(flags);
        CompressionDictionary dict = ReadDictionaryBlock(file, _footer, _dictHandle, VerifyRead(flags), DigestsDictionary);
        long charge = dict.ApproximateMemoryUsage;
        if (_bcTracer is { IsEnabled: true } tr) TraceDictionaryAccess(tr, key, (ulong)charge, hit: false, flags);
        if ((flags & TableReadFlags.DontFillCache) != 0) return DictLease.Owned(dict);
        if (cache.TryInsert(key, CacheValue.FromObject(dict).WithHelper(DictHelper), charge, _metaPriority, out h))
        {
            // UpdateCacheInsertionMetrics, BlockType::kCompressionDictionary.
            BlockCacheStatistics? stats = _stats;
            if (stats is not null)
            {
                stats.Record(BlockCacheCounter.CompressionDictAdd);
                stats.Add(BlockCacheCounter.BytesInserted, charge);
                stats.Add(BlockCacheCounter.CompressionDictBytesInsert, charge);
            }
            return DictLease.Cached(h);
        }
        // Strict capacity limit reached: the caller uses the dictionary uncached.
        _stats?.Record(BlockCacheCounter.AddFailure);
        return DictLease.Owned(dict);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void TraceDictionaryAccess(Tracing.BlockCacheTracer t, in CacheKey key, ulong usage, bool hit, TableReadFlags flags)
    {
        bool noInsert = (flags & (TableReadFlags.BlockCacheTier | TableReadFlags.DontFillCache)) != 0;
        t.TraceBlockAccess(key, Tracing.TraceType.BlockUncompressionDictBlock, usage, 0, hit, noInsert, Properties.ColumnFamilyId,
            Properties.ColumnFamilyName, _options.Level >= 0 ? (uint)_options.Level : uint.MaxValue,
            _options.FileNumber != 0 ? _options.FileNumber : ulong.MaxValue);
    }

    /// <summary>
    /// A data block retrieval of a table whose dictionary is in the block cache unpinned. Port of the dictionary step
    /// of BlockBasedTable::NewDataBlockIterator: GetOrReadUncompressionDictionary runs before the data block lookup,
    /// whether or not the data block is cached, and its failure (Incomplete with no I/O, a read error) fails the retrieval.
    /// </summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private BlockRef GetDataBlockRefWithCachedDictionary(BlockHandle handle, TableReadFlags flags, AsyncPrefetchBuffer? prefetch)
    {
        DictLease lease = GetOrReadDictionary(flags, _file);
        try
        {
            return GetDataBlockRefWithDictionary(handle, flags, prefetch, lease.Dict!);
        }
        finally
        {
            lease.Release();
        }
    }

    /// <summary><see cref="GetBlockRef(BlockHandle, BlockKind, TableReadFlags)"/> for a data block, decompressing with <paramref name="dict"/>.</summary>
    private BlockRef GetDataBlockRefWithDictionary(BlockHandle handle, TableReadFlags flags, AsyncPrefetchBuffer? prefetch, CompressionDictionary dict)
    {
        BlockCache cache = _cache!;
        CacheKey key = _baseCacheKey.WithOffsetForMinSizeRecord(handle.Offset);
        if (LookupCache(cache, key, BlockKind.Data, out CachedBlock h))
        {
            _stats?.RecordHit(BlockCacheCounter.DataHit, h.Charge);
            PerfContext.Add(PerfMetric.BlockCacheHitCount, 1);
            if (_bcTracer is { IsEnabled: true } t) TraceAccess(t, key, BlockKind.Data, Unsafe.As<Block>(h.Value), hit: true, flags);
            return BlockRef.FromCache(Unsafe.As<Block>(h.Value)!, h);
        }
        _stats?.Record(BlockCacheCounter.DataMiss);
        ThrowIfNoIo(flags);
        ChargeRateLimiter(handle, flags);
        Block b = Protect(prefetch is null
            ? ReadBlockFromFile(handle, BlockKind.Data, flags, dict)
            : BlockFetcher.ReadBlock(prefetch, _footer, handle, VerifyRead(flags), dict), BlockKind.Data);
        if (_bcTracer is { IsEnabled: true } tr) TraceAccess(tr, key, BlockKind.Data, b, hit: false, flags);
        return (flags & TableReadFlags.DontFillCache) != 0 ? BlockRef.FromOwned(b) : InsertIntoCache(cache, key, b, BlockKind.Data);
    }

    /// <summary>Async <see cref="GetDataBlockRefWithCachedDictionary"/>; the dictionary itself is read synchronously, as upstream.</summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private ValueTask<BlockRef> GetDataBlockRefWithCachedDictionaryAsync(BlockHandle handle, TableReadFlags flags, CancellationToken ct)
    {
        DictLease lease = GetOrReadDictionary(flags, _file);
        ValueTask<BlockRef> pending;
        try
        {
            pending = GetDataBlockRefWithDictionaryAsync(handle, flags, lease.Dict!, ct);
        }
        catch
        {
            lease.Release();
            throw;
        }
        if (pending.IsCompletedSuccessfully)
        {
            lease.Release();
            return pending;
        }
        return ReleaseWhenRead(pending, lease);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private static async ValueTask<BlockRef> ReleaseWhenRead(ValueTask<BlockRef> pending, DictLease lease)
    {
        try
        {
            return await pending.ConfigureAwait(false);
        }
        finally
        {
            lease.Release();
        }
    }

    /// <summary>Async <see cref="GetDataBlockRefWithDictionary"/>; completes synchronously on a cache hit.</summary>
    private ValueTask<BlockRef> GetDataBlockRefWithDictionaryAsync(BlockHandle handle, TableReadFlags flags, CompressionDictionary dict, CancellationToken ct)
    {
        BlockCache cache = _cache!;
        CacheKey key = _baseCacheKey.WithOffsetForMinSizeRecord(handle.Offset);
        if (LookupCache(cache, key, BlockKind.Data, out CachedBlock h))
        {
            _stats?.RecordHit(BlockCacheCounter.DataHit, h.Charge);
            if (_bcTracer is { IsEnabled: true } t) TraceAccess(t, key, BlockKind.Data, Unsafe.As<Block>(h.Value), hit: true, flags);
            return new ValueTask<BlockRef>(BlockRef.FromCache(Unsafe.As<Block>(h.Value)!, h));
        }
        _stats?.Record(BlockCacheCounter.DataMiss);
        return ReadDataBlockWithDictionaryAsync(cache, key, handle, flags, dict, ct);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<BlockRef> ReadDataBlockWithDictionaryAsync(BlockCache cache, CacheKey key, BlockHandle handle, TableReadFlags flags,
        CompressionDictionary dict, CancellationToken ct)
    {
        ThrowIfNoIo(flags);
        await ChargeRateLimiterAsync(handle, flags, ct).ConfigureAwait(false);
        Block b = Protect(_persistentCache is { } pc
            ? await ReadBlockThroughPersistentCacheAsync(pc, handle, BlockKind.Data, flags, dict, ct).ConfigureAwait(false)
            : await BlockFetcher.ReadBlockAsync(_file, _footer, handle, VerifyRead(flags), dict, ct).ConfigureAwait(false), BlockKind.Data);
        if (_bcTracer is { IsEnabled: true } t) TraceAccess(t, key, BlockKind.Data, b, hit: false, flags);
        return (flags & TableReadFlags.DontFillCache) != 0 ? BlockRef.FromOwned(b) : InsertIntoCache(cache, key, b, BlockKind.Data);
    }

    /// <summary>
    /// The dictionary of one MultiGet batch over this table (the dict / dict_inited / dict_status locals of
    /// BlockBasedTable::MultiGet): looked up once, by the first key that reaches a data block, and shared by the rest
    /// of the batch; a failed lookup fails every later key the same way without another lookup. Pooled per reader.
    /// </summary>
    private sealed class BatchDictionary
    {
        public DictLease Lease;
        public bool Inited;
        public RocksDbException? Error;
    }

    private BatchDictionary? _spareBatchDictionary;

    private BatchDictionary RentBatchDictionary() => Interlocked.Exchange(ref _spareBatchDictionary, null) ?? new BatchDictionary();

    private void ReturnBatchDictionary(BatchDictionary batch)
    {
        batch.Lease.Release();
        batch.Inited = false;
        batch.Error = null;
        Volatile.Write(ref _spareBatchDictionary, batch);
    }

    /// <summary>A data block of a MultiGet batch over a table whose dictionary is in the block cache unpinned.</summary>
    private ValueTask<BlockRef> GetDataBlockRefForBatch(BatchDictionary batch, BlockHandle handle, TableReadFlags flags, bool synchronous,
        CancellationToken ct)
    {
        if (!batch.Inited)
        {
            batch.Inited = true;
            try
            {
                batch.Lease = GetOrReadDictionary(flags, _file);
            }
            catch (RocksDbException e)
            {
                batch.Error = e;
            }
        }
        if (batch.Error is { } error) throw new RocksDbException(error.Code, error.Message, error.SubCode, error);
        return synchronous
            ? new ValueTask<BlockRef>(GetDataBlockRefWithDictionary(handle, flags, null, batch.Lease.Dict!))
            : GetDataBlockRefWithDictionaryAsync(handle, flags, batch.Lease.Dict!, ct);
    }

    /// <summary>
    /// A data block a cache tier kept compressed, for a table whose dictionary is in the block cache unpinned: the
    /// dictionary is read from the file for this one block. Not reached by blocks this reader inserted (see
    /// <see cref="OffersCompressed"/>); it covers entries another reader of the same file, with a pinned dictionary, offered.
    /// The block cache is not consulted because this runs inside a cache lookup.
    /// </summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private Block CreateDataBlockFromSavedWithDictionaryRead(ReadOnlySpan<byte> data, CompressionType type)
    {
        using CompressionDictionary dict = ReadDictionaryBlock(_file, _footer, _dictHandle, VerifyRead(TableReadFlags.None), digest: false);
        return Protect(BlockFetcher.Decompress(type, data, _footer.FormatVersion, dict), BlockKind.Data);
    }
}
