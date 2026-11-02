using System.Buffers;
using System.Runtime.CompilerServices;
using System.Text;
using RocksDb.Managed.Cache;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table.Filters;
using RocksDb.Managed.Util;
using EntryType = RocksDb.Managed.Db.EntryType;

namespace RocksDb.Managed.Table;

/// <summary>Options for opening a <see cref="BlockBasedTableReader"/>.</summary>
public sealed record TableReaderOptions
{
    /// <summary>User comparator the table was built with.</summary>
    public Comparator Comparator { get; init; } = Comparator.Bytewise;
    /// <summary>Filter policy used to interpret the full filter block ("fullfilter.&lt;name&gt;"); null ignores filters.</summary>
    public FilterPolicy? FilterPolicy { get; init; }
    public bool VerifyChecksums { get; init; } = true;
    /// <summary>
    /// Block cache for data blocks and index partitions (and the index/filter blocks with
    /// <see cref="CacheIndexAndFilterBlocks"/>). Default: none. Pass a <see cref="Cache.BlockCache"/>
    /// (<c>BlockCache.CreateHyperClock(...)</c> recommended, or <c>BlockCache.CreateLru(...)</c>), which the reader uses
    /// natively: per-file <see cref="OffsetableCacheKey"/> keys, pinned <see cref="CachedBlock"/> handles and insertion
    /// priorities. Any other <see cref="IBlockCache"/> is used through the legacy seam (per-reader keys).
    /// One cache instance is normally shared by all readers of a DB.
    /// </summary>
    public IBlockCache BlockCache { get; init; } = NoBlockCache.Instance;
    /// <summary>
    /// cache_index_and_filter_blocks: with a <see cref="Cache.BlockCache"/>, the top-level index block and the full
    /// filter are kept in the block cache (inserted at open, looked up on each access and re-read after eviction)
    /// instead of being pinned by the reader. Ignored without a <see cref="Cache.BlockCache"/> (as upstream, where
    /// the table reader then owns them).
    /// </summary>
    public bool CacheIndexAndFilterBlocks { get; init; }
    /// <summary>
    /// cache_index_and_filter_blocks_with_high_priority: insert index blocks, index partitions and filter blocks with
    /// <see cref="CachePriority.High"/> (data blocks always use <see cref="CachePriority.Low"/>). Default true.
    /// </summary>
    public bool CacheIndexAndFilterBlocksWithHighPriority { get; init; } = true;
    /// <summary>
    /// Current DB session id, used for block cache keys when the file's table properties lack db_session_id /
    /// orig_file_number (the "old SST file" case of BlockBasedTable::SetupBaseCacheKey). When neither is available,
    /// a base key unique for the process lifetime is used.
    /// </summary>
    public string? DbSessionId { get; init; }
    /// <summary>Current file number of the table (see <see cref="DbSessionId"/>); 0 when unknown.</summary>
    public ulong FileNumber { get; init; }
    /// <summary>Optional block cache hit/miss counters (may be shared by many readers); null disables recording.</summary>
    public BlockCacheStatistics? Statistics { get; init; }
    /// <summary>
    /// Largest sequence number of the file as recorded in the MANIFEST, used to resolve the global sequence number
    /// of ingested external files; <see cref="DbFormat.MaxSequenceNumber"/> when unknown.
    /// </summary>
    public ulong LargestSeqno { get; init; } = DbFormat.MaxSequenceNumber;

    public static TableReaderOptions Default { get; } = new();
}

/// <summary>Result of a point lookup.</summary>
public readonly record struct TableGetResult(bool Found, ulong Sequence, EntryType Type);

/// <summary>
/// Visitor for <see cref="BlockBasedTableReader.Get{TState}(ReadOnlySpan{byte}, ref TState, TableEntryVisitor{TState})"/>:
/// called for each entry whose user key equals the lookup key, starting at the lookup position. Return false to stop.
/// </summary>
public delegate bool TableEntryVisitor<TState>(ref TState state, ReadOnlySpan<byte> internalKey, ReadOnlySpan<byte> value)
    where TState : allows ref struct;

/// <summary>Kind of block requested from the cache (BlockType subset): selects the priority and statistics counters.</summary>
internal enum BlockKind : byte
{
    Data,
    Index,
    Filter,
}

/// <summary>
/// A reference to a block used for the duration of one access: either a pinned block cache entry
/// (<see cref="CachedBlock"/> handle), a block reference owned by the caller, or a block pinned by the reader (no
/// release needed). Port of the role of CachableEntry&lt;Block&gt;. Release exactly once.
/// </summary>
internal struct BlockRef
{
    public Block? Block;
    private CachedBlock _handle;
    private bool _owned;

    public static BlockRef FromCache(Block block, CachedBlock handle) => new() { Block = block, _handle = handle };
    public static BlockRef FromOwned(Block block) => new() { Block = block, _owned = true };
    public static BlockRef FromPinned(Block block) => new() { Block = block };

    public readonly bool IsEmpty => Block is null;

    public void Release()
    {
        Block? b = Block;
        Block = null;
        if (_handle.HasValue)
        {
            _handle.Dispose();
        }
        else if (_owned)
        {
            _owned = false;
            b!.Release();
        }
    }

    /// <summary>Converts to a block reference owned by the caller (releases the cache handle).</summary>
    public Block ToOwnedBlock()
    {
        Block b = Block!;
        if (_owned)
        {
            _owned = false;
            Block = null;
            return b;
        }
        b.AddRef();
        Release();
        return b;
    }
}

/// <summary>
/// Reader for block-based tables. Port of BlockBasedTable (table/block_based/block_based_table_reader.cc),
/// BinarySearchIndexReader / PartitionedIndexReader (read side), meta_blocks.cc (ReadTableProperties,
/// FindMetaBlock) and GetGlobalSequenceNumber.
/// <para>
/// The footer, metaindex, properties, (top-level) index, full filter and range-deletion blocks are read at open and
/// pinned for the lifetime of the reader (the index and filter go to the block cache instead with
/// <see cref="TableReaderOptions.CacheIndexAndFilterBlocks"/>). Data blocks (and index partitions) are fetched on demand
/// through the block cache. Thread-safe for concurrent lookups and iterators.
/// </para>
/// <para>
/// Block cache (port of BlockBasedTable::RetrieveBlock / MaybeReadBlockAndLoadToCache / GetCacheKey): with a
/// <see cref="Cache.BlockCache"/>, keys are <c>BaseCacheKey.WithOffsetForMinSizeRecord(handle.Offset)</c> with the base
/// key from <see cref="SetupBaseCacheKey"/>; entries hold the parsed, checksum-verified, decompressed <see cref="Block"/>
/// (or the filter reader), so a hit does no verification, decompression or parsing. A hit pins the entry with a
/// <see cref="CachedBlock"/> handle only for the duration of the access.
/// </para>
/// </summary>
[SkipLocalsInit]
public sealed class BlockBasedTableReader : IDisposable
{
    private static long s_nextTableId;

    private readonly IRandomAccessFile _file;
    private readonly TableReaderOptions _options;
    private readonly InternalKeyComparator _icmp;
    private readonly Footer _footer;
    private readonly Block? _indexBlock;          // null when the index lives in the block cache
    private readonly Block? _rangeDelBlock;
    private readonly IFilterBitsReader? _filter;  // pinned filter (null when absent or cached)
    private byte[]? _filterBuffer;
    private readonly bool _hasFilter;
    private readonly BlockHandle _filterHandle;   // used when the filter lives in the block cache
    private readonly ulong _tableId;
    private readonly BlockCache? _cache;          // native block cache (null: none or legacy seam)
    private readonly IBlockCache? _legacyCache;   // legacy IBlockCache seam (null: none or native)
    private readonly OffsetableCacheKey _baseCacheKey;
    private readonly CachePriority _metaPriority;
    private readonly BlockCacheStatistics? _stats;
    private int _disposed;

    public Footer Footer => _footer;
    public TableProperties Properties { get; }
    public IReadOnlyDictionary<string, BlockHandle> MetaBlocks { get; }
    /// <summary>Handle of the (top-level) index block (footer for format_version &lt; 6, metaindex otherwise).</summary>
    public BlockHandle IndexHandle { get; }
    public IndexType IndexType { get; }
    public bool IndexKeyIncludesSeq { get; }
    public bool IndexValueIsFull { get; }
    public bool IndexHasFirstKey => IndexType == IndexType.BinarySearchWithFirstKey;
    public bool WholeKeyFiltering { get; }
    public bool HasFilter => _hasFilter;
    /// <summary>Base block cache key of this file (see <see cref="SetupBaseCacheKey"/>).</summary>
    public OffsetableCacheKey BaseCacheKey => _baseCacheKey;
    /// <summary>True when the top-level index and the filter live in the block cache rather than being pinned by the reader.</summary>
    public bool IndexAndFilterInCache => _indexBlock is null;
    /// <summary>Effective global sequence number (external files), or <see cref="BlockIter.DisableGlobalSequenceNumber"/>.</summary>
    public ulong GlobalSequenceNumber { get; }
    public long FileSize { get; }
    internal InternalKeyComparator InternalComparator => _icmp;
    internal IRandomAccessFile File => _file;

    private BlockBasedTableReader(IRandomAccessFile file, TableReaderOptions options, Footer footer, Dictionary<string, BlockHandle> metaBlocks,
        TableProperties props, BlockHandle indexHandle, Block? indexBlock, Block? rangeDel, IFilterBitsReader? filter, byte[]? filterBuffer,
        bool hasFilter, BlockHandle filterHandle, in OffsetableCacheKey baseCacheKey, long fileSize)
    {
        IndexHandle = indexHandle;
        _file = file;
        _options = options;
        _icmp = options.Comparator.Kind == ComparatorKind.Bytewise ? InternalKeyComparator.Bytewise : new InternalKeyComparator(options.Comparator);
        _footer = footer;
        MetaBlocks = metaBlocks;
        Properties = props;
        _indexBlock = indexBlock;
        _rangeDelBlock = rangeDel;
        _filter = filter;
        _filterBuffer = filterBuffer;
        _hasFilter = hasFilter;
        _filterHandle = filterHandle;
        FileSize = fileSize;
        _tableId = (ulong)Interlocked.Increment(ref s_nextTableId);
        _cache = options.BlockCache as BlockCache;
        _legacyCache = _cache is null && options.BlockCache is not NoBlockCache ? options.BlockCache : null;
        _baseCacheKey = baseCacheKey;
        _metaPriority = MetaPriority(options);
        _stats = options.Statistics;

        IndexType = props.UserCollectedProperties.TryGetValue(TablePropertiesNames.BlockBasedIndexType, out byte[]? it) && it.Length >= 4
            ? (IndexType)Coding.DecodeFixed32(it)
            : IndexType.BinarySearch;
        if (IndexType > IndexType.BinarySearchWithFirstKey) ThrowHelper.Corruption("unknown index type " + (uint)IndexType);
        IndexKeyIncludesSeq = props.IndexKeyIsUserKey == 0;
        IndexValueIsFull = props.IndexValueIsDeltaEncoded == 0;
        WholeKeyFiltering = !props.UserCollectedProperties.TryGetValue(TablePropertiesNames.BlockBasedWholeKeyFiltering, out byte[]? wkf)
            || wkf.AsSpan().SequenceEqual("1"u8);
        GlobalSequenceNumber = ResolveGlobalSeqno(props, options.LargestSeqno);
    }

    // ---------------------------------------------------------------------------------------------
    // Open

    /// <summary>Opens a table. The reader takes ownership of <paramref name="file"/> (disposed with the reader).</summary>
    public static BlockBasedTableReader Open(IRandomAccessFile file, TableReaderOptions options)
    {
        ArgumentNullException.ThrowIfNull(file);
        ArgumentNullException.ThrowIfNull(options);
        long fileSize = file.Length;
        Footer footer = ReadFooter(file, fileSize);
        return OpenWithFooter(file, options, footer, fileSize);
    }

    /// <summary>Opens a table asynchronously (footer and tail meta blocks are read with async I/O).</summary>
    public static async ValueTask<BlockBasedTableReader> OpenAsync(IRandomAccessFile file, TableReaderOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(file);
        ArgumentNullException.ThrowIfNull(options);
        long fileSize = file.Length;
        if (fileSize < Footer.MinEncodedLength) ThrowHelper.Corruption("file is too short (" + fileSize + " bytes) to be an sstable");
        int n = (int)Math.Min(fileSize, Footer.EncodedLength);
        byte[] buf = new byte[n];
        int total = 0;
        while (total < n)
        {
            int r = await file.ReadAsync(fileSize - n + total, buf.AsMemory(total), cancellationToken).ConfigureAwait(false);
            if (r <= 0) break;
            total += r;
        }
        if (total != n) ThrowHelper.Corruption("truncated footer read");
        Footer footer = Footer.Decode(buf, (ulong)(fileSize - n));
        // Prefetch the tail (meta blocks) asynchronously so the remaining open work is served from memory.
        var tail = new TailPrefetchFile(file);
        await tail.PrefetchAsync(fileSize, footer, cancellationToken).ConfigureAwait(false);
        try
        {
            return OpenWithFooter(tail, options, footer, fileSize, file);
        }
        finally
        {
            tail.ReleaseBuffer();
        }
    }

    private static Footer ReadFooter(IRandomAccessFile file, long fileSize)
    {
        if (fileSize < Footer.MinEncodedLength) ThrowHelper.Corruption("file is too short (" + fileSize + " bytes) to be an sstable");
        int n = (int)Math.Min(fileSize, Footer.EncodedLength);
        Span<byte> buf = stackalloc byte[Footer.EncodedLength];
        int read = BlockFetcher.ReadFully(file, fileSize - n, buf[..n]);
        if (read != n) ThrowHelper.Corruption("truncated footer read");
        return Footer.Decode(buf[..n], (ulong)(fileSize - n));
    }

    private static BlockBasedTableReader OpenWithFooter(IRandomAccessFile file, TableReaderOptions options, Footer footer, long fileSize, IRandomAccessFile? ownedFile = null)
    {
        bool verify = options.VerifyChecksums;
        var metaBlocks = new Dictionary<string, BlockHandle>(StringComparer.Ordinal);
        Block metaindex = BlockFetcher.ReadBlock(file, footer, footer.MetaindexHandle, verify);
        try
        {
            using var it = new BlockIter();
            it.InitializeMeta(metaindex);
            for (it.SeekToFirst(); it.Valid; it.Next())
                metaBlocks[Encoding.UTF8.GetString(it.Key)] = BlockHandle.Decode(it.Value);
        }
        finally
        {
            metaindex.Release();
        }

        TableProperties props;
        if (metaBlocks.TryGetValue(BlockBasedTableConstants.PropertiesBlockName, out BlockHandle propsHandle))
        {
            Block pb = BlockFetcher.ReadBlock(file, footer, propsHandle, verify);
            try { props = TableProperties.Parse(pb, propsHandle.Offset); }
            finally { pb.Release(); }
        }
        else
        {
            props = new TableProperties();
        }

        BlockHandle indexHandle = footer.IndexHandle;
        if (!BlockBasedTableConstants.FormatVersionUsesIndexHandleInFooter(footer.FormatVersion))
        {
            if (!metaBlocks.TryGetValue(BlockBasedTableConstants.IndexBlockName, out indexHandle))
                ThrowHelper.Corruption("Index block handle not found in metaindex");
        }

        OffsetableCacheKey baseCacheKey = SetupBaseCacheKey(props, options.DbSessionId, options.FileNumber);
        // The index and filter go to the block cache only with a native cache (upstream: without a block cache the
        // table reader always owns them).
        BlockCache? metaCache = options.CacheIndexAndFilterBlocks ? options.BlockCache as BlockCache : null;
        CachePriority metaPriority = MetaPriority(options);

        Block? indexBlock = BlockFetcher.ReadBlock(file, footer, indexHandle, verify);
        Block? rangeDel = null;
        byte[]? filterBuf = null;
        try
        {
            if (metaCache is not null)
            {
                // PrefetchIndexAndFilterBlocks with prefetch_index_and_filter_in_cache: warm the cache, don't pin.
                Block b = indexBlock;
                indexBlock = null;
                if (metaCache.Insert(baseCacheKey.WithOffsetForMinSizeRecord(indexHandle.Offset), CacheValue.FromObject(b), b.Charge, metaPriority))
                    RecordAdd(options.Statistics, BlockKind.Index, b.Charge);
            }

            if (metaBlocks.TryGetValue(BlockBasedTableConstants.RangeDelBlockName, out BlockHandle rdHandle))
                rangeDel = BlockFetcher.ReadBlock(file, footer, rdHandle, verify);

            // Port of the filter lookup in BlockBasedTable::PrefetchIndexAndFilterBlocks: the full filter is keyed by the
            // configured policy's CompatibilityName (no policy -> filters ignored). Partitioned filters are not read yet,
            // which is safe (no filtering).
            IFilterBitsReader? filter = null;
            bool hasFilter = false;
            BlockHandle fh = default;
            if (options.FilterPolicy is { } policy
                && metaBlocks.TryGetValue(BlockBasedTableConstants.FullFilterBlockPrefix + policy.CompatibilityName, out fh))
            {
                hasFilter = true;
                filterBuf = BlockFetcher.ReadRawPayload(file, footer, fh, verify, out int flen);
                filter = CreateFilterReader(policy, filterBuf, flen);
                if (metaCache is not null)
                {
                    byte[] fb = filterBuf;
                    filterBuf = null;
                    if (metaCache.Insert(baseCacheKey.WithOffsetForMinSizeRecord(fh.Offset), new CacheValue(fb, flen, filter, true), fb.Length, metaPriority))
                        RecordAdd(options.Statistics, BlockKind.Filter, fb.Length);
                    filter = null;
                }
            }
            return new BlockBasedTableReader(ownedFile ?? file, options, footer, metaBlocks, props, indexHandle, indexBlock, rangeDel, filter, filterBuf,
                hasFilter, fh, baseCacheKey, fileSize);
        }
        catch
        {
            indexBlock?.Release();
            rangeDel?.Release();
            if (filterBuf is not null) ArrayPool<byte>.Shared.Return(filterBuf);
            throw;
        }
    }

    // Any built-in policy reads any built-in filter format (BuiltinFilterPolicy::GetBuiltinFilterBitsReader).
    private static IFilterBitsReader CreateFilterReader(FilterPolicy policy, byte[] buffer, int length) =>
        policy.CompatibilityName == BloomLikeFilterPolicy.BuiltinFilterMetadataName
            ? BuiltinFilterBitsReader.Create(buffer.AsMemory(0, length))
            : policy.CreateReader(buffer.AsMemory(0, length));

    private static CachePriority MetaPriority(TableReaderOptions options) =>
        options.CacheIndexAndFilterBlocksWithHighPriority ? CachePriority.High : CachePriority.Low;

    private static long s_uniqueBaseKeyCounter;
    private static readonly ulong s_processNonce = (ulong)Random.Shared.NextInt64() | 1;

    /// <summary>
    /// Port of BlockBasedTable::SetupBaseCacheKey: a stable key from the table properties (db_id, db_session_id,
    /// orig_file_number) when both the session id and the original file number are recorded (newer SST files);
    /// otherwise from the current session id and file number with db id "unknown". When no session id is known at all
    /// (a table opened outside a DB) a base key unique for the process lifetime is returned, so unrelated files can
    /// never share cache entries.
    /// </summary>
    public static OffsetableCacheKey SetupBaseCacheKey(TableProperties properties, string? currentDbSessionId, ulong currentFileNumber)
    {
        ArgumentNullException.ThrowIfNull(properties);
        if (properties.DbSessionId.Length > 0 && properties.OriginalFileNumber > 0)
        {
            // (Newer SST file case) Both properties are needed for a stable unique id because CreateColumnFamilyWithImport
            // or IngestExternalFiles can change the file numbers on a file.
            return new OffsetableCacheKey(properties.DbId, properties.DbSessionId, properties.OriginalFileNumber);
        }
        if (!string.IsNullOrEmpty(currentDbSessionId))
        {
            // (Old SST file case) Unique keys based on current identifiers, stable across reopen within the session.
            return new OffsetableCacheKey("unknown", currentDbSessionId, currentFileNumber);
        }
        return OffsetableCacheKey.FromInternalUniqueId(s_processNonce, (ulong)Interlocked.Increment(ref s_uniqueBaseKeyCounter));
    }

    /// <summary>Port of GetGlobalSequenceNumber (table/meta_blocks.cc).</summary>
    private static ulong ResolveGlobalSeqno(TableProperties props, ulong largestSeqno)
    {
        const ulong disabled = BlockIter.DisableGlobalSequenceNumber;
        if (largestSeqno == disabled) return disabled;
        bool hasVersion = props.UserCollectedProperties.TryGetValue(TablePropertiesNames.ExternalSstFileVersion, out byte[]? versionBytes);
        bool hasSeqno = props.UserCollectedProperties.TryGetValue(TablePropertiesNames.ExternalSstFileGlobalSeqno, out byte[]? seqnoBytes);
        if (!hasVersion)
        {
            if (hasSeqno) ThrowHelper.Corruption("A non-external sst file have global seqno property");
            return disabled;
        }
        if (versionBytes!.Length < 4) ThrowHelper.Corruption("bad external sst file version");
        uint version = Coding.DecodeFixed32(versionBytes);
        if (version != 2)
        {
            if (version != 1) ThrowHelper.Corruption("An external sst file has corrupted version " + version + ".");
            if (hasSeqno) ThrowHelper.Corruption("An external sst file with version 1 has global seqno property");
            return disabled;
        }
        ulong resolved = 0;
        if (hasSeqno && seqnoBytes!.Length >= 8) resolved = Coding.DecodeFixed64(seqnoBytes);
        if (largestSeqno < DbFormat.MaxSequenceNumber)
        {
            if (resolved == 0) resolved = largestSeqno;
            if (resolved != largestSeqno) ThrowHelper.Corruption("An external sst file has a global seqno inconsistent with its largest seqno");
        }
        if (resolved > DbFormat.MaxSequenceNumber) ThrowHelper.Corruption("global seqno greater than kMaxSequenceNumber");
        // Keys of external files are written with seqno 0, so a zero global seqno needs no rewriting.
        return resolved == 0 ? disabled : resolved;
    }

    // ---------------------------------------------------------------------------------------------
    // Blocks

    /// <summary>Gets a data block or index partition (through the cache). The returned block carries a reference owned by the caller.</summary>
    internal Block GetBlock(BlockHandle handle) => GetBlockRef(handle, BlockKind.Data, TableReadFlags.None).ToOwnedBlock();

    /// <summary>
    /// Port of BlockBasedTable::RetrieveBlock: returns the block at <paramref name="handle"/> from the block cache, or
    /// reads (verifies, decompresses, parses) it and inserts it unless <see cref="TableReadFlags.DontFillCache"/>.
    /// The caller must <see cref="BlockRef.Release"/> the result.
    /// </summary>
    internal BlockRef GetBlockRef(BlockHandle handle, BlockKind kind, TableReadFlags flags)
    {
        BlockCache? cache = _cache;
        if (cache is not null)
        {
            CacheKey key = _baseCacheKey.WithOffsetForMinSizeRecord(handle.Offset);
            if (cache.TryLookup(key, out CachedBlock h))
            {
                _stats?.Record(HitCounter(kind));
                return BlockRef.FromCache(Unsafe.As<Block>(h.Value)!, h);
            }
            return ReadAndInsert(cache, key, handle, kind, flags);
        }
        return GetBlockRefLegacy(handle, kind, flags);
    }

    private BlockRef ReadAndInsert(BlockCache cache, in CacheKey key, BlockHandle handle, BlockKind kind, TableReadFlags flags)
    {
        _stats?.Record(MissCounter(kind));
        Block b = BlockFetcher.ReadBlock(_file, _footer, handle, _options.VerifyChecksums);
        return (flags & TableReadFlags.DontFillCache) != 0 ? BlockRef.FromOwned(b) : InsertIntoCache(cache, key, b, kind);
    }

    private BlockRef GetBlockRefLegacy(BlockHandle handle, BlockKind kind, TableReadFlags flags)
    {
        IBlockCache? legacy = _legacyCache;
        if (legacy is null) return BlockRef.FromOwned(BlockFetcher.ReadBlock(_file, _footer, handle, _options.VerifyChecksums));
        var key = new BlockCacheKey(_tableId, handle.Offset);
        if (legacy.TryGet(key, out Block? b))
        {
            _stats?.Record(HitCounter(kind));
            return BlockRef.FromOwned(b);
        }
        _stats?.Record(MissCounter(kind));
        b = BlockFetcher.ReadBlock(_file, _footer, handle, _options.VerifyChecksums);
        if ((flags & TableReadFlags.DontFillCache) == 0) legacy.Insert(key, b);
        return BlockRef.FromOwned(b);
    }

    private BlockRef InsertIntoCache(BlockCache cache, in CacheKey key, Block b, BlockKind kind)
    {
        CachePriority priority = kind == BlockKind.Data ? CachePriority.Low : _metaPriority;
        // On success the cache takes over our reference (released through Block's IDisposable when the entry is freed).
        if (cache.TryInsert(key, CacheValue.FromObject(b), b.Charge, priority, out CachedBlock h))
        {
            RecordAdd(_stats, kind, b.Charge);
            return BlockRef.FromCache(b, h);
        }
        // Strict capacity limit reached: we keep ownership and use the block uncached.
        _stats?.Record(BlockCacheCounter.AddFailure);
        return BlockRef.FromOwned(b);
    }

    /// <summary>Async <see cref="GetBlockRef"/>; completes synchronously on a cache hit.</summary>
    internal ValueTask<BlockRef> GetBlockRefAsync(BlockHandle handle, BlockKind kind, TableReadFlags flags, CancellationToken ct)
    {
        BlockCache? cache = _cache;
        if (cache is not null)
        {
            CacheKey key = _baseCacheKey.WithOffsetForMinSizeRecord(handle.Offset);
            if (cache.TryLookup(key, out CachedBlock h))
            {
                _stats?.Record(HitCounter(kind));
                return new ValueTask<BlockRef>(BlockRef.FromCache(Unsafe.As<Block>(h.Value)!, h));
            }
            _stats?.Record(MissCounter(kind));
            return ReadAndInsertAsync(cache, key, handle, kind, flags, ct);
        }
        IBlockCache? legacy = _legacyCache;
        if (legacy is not null)
        {
            if (legacy.TryGet(new BlockCacheKey(_tableId, handle.Offset), out Block? b))
            {
                _stats?.Record(HitCounter(kind));
                return new ValueTask<BlockRef>(BlockRef.FromOwned(b));
            }
            _stats?.Record(MissCounter(kind));
        }
        return ReadAndInsertLegacyAsync(handle, flags, ct);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<BlockRef> ReadAndInsertAsync(BlockCache cache, CacheKey key, BlockHandle handle, BlockKind kind, TableReadFlags flags, CancellationToken ct)
    {
        Block b = await BlockFetcher.ReadBlockAsync(_file, _footer, handle, _options.VerifyChecksums, ct).ConfigureAwait(false);
        return (flags & TableReadFlags.DontFillCache) != 0 ? BlockRef.FromOwned(b) : InsertIntoCache(cache, key, b, kind);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<BlockRef> ReadAndInsertLegacyAsync(BlockHandle handle, TableReadFlags flags, CancellationToken ct)
    {
        Block b = await BlockFetcher.ReadBlockAsync(_file, _footer, handle, _options.VerifyChecksums, ct).ConfigureAwait(false);
        if (_legacyCache is { } legacy && (flags & TableReadFlags.DontFillCache) == 0) legacy.Insert(new BlockCacheKey(_tableId, handle.Offset), b);
        return BlockRef.FromOwned(b);
    }

    /// <summary>The top-level index block: pinned by the reader, or looked up in (and if evicted re-read into) the block cache.</summary>
    internal BlockRef GetIndexBlockRef(TableReadFlags flags) =>
        _indexBlock is not null ? BlockRef.FromPinned(_indexBlock) : GetBlockRef(IndexHandle, BlockKind.Index, flags);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static BlockCacheCounter HitCounter(BlockKind kind) => kind switch
    {
        BlockKind.Data => BlockCacheCounter.DataHit,
        BlockKind.Index => BlockCacheCounter.IndexHit,
        _ => BlockCacheCounter.FilterHit,
    };

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static BlockCacheCounter MissCounter(BlockKind kind) => kind switch
    {
        BlockKind.Data => BlockCacheCounter.DataMiss,
        BlockKind.Index => BlockCacheCounter.IndexMiss,
        _ => BlockCacheCounter.FilterMiss,
    };

    private static void RecordAdd(BlockCacheStatistics? stats, BlockKind kind, long charge)
    {
        if (stats is null) return;
        stats.Record(kind switch
        {
            BlockKind.Data => BlockCacheCounter.DataAdd,
            BlockKind.Index => BlockCacheCounter.IndexAdd,
            _ => BlockCacheCounter.FilterAdd,
        });
        stats.Add(BlockCacheCounter.BytesInserted, charge);
    }

    internal void InitIndexIter(BlockIter it, Block block) =>
        it.InitializeIndex(block, _icmp, IndexKeyIncludesSeq, IndexValueIsFull, IndexHasFirstKey,
            BlockBasedTableConstants.FormatVersionUsesValueDeltaEscape(_footer.FormatVersion), GlobalSequenceNumber);

    /// <summary>The pinned top-level index block (not available when <see cref="IndexAndFilterInCache"/>).</summary>
    internal Block IndexBlock => _indexBlock ?? throw new InvalidOperationException("The index block is held by the block cache (CacheIndexAndFilterBlocks).");
    internal bool IsPartitionedIndex => IndexType == IndexType.TwoLevelIndexSearch;

    // ---------------------------------------------------------------------------------------------
    // Point lookups

    /// <summary>
    /// Whether the filter (if any) says <paramref name="userKey"/> may be present. Port of
    /// BlockBasedTable::FullFilterKeyMayMatch (whole key filtering only).
    /// </summary>
    public bool KeyMayMatch(ReadOnlySpan<byte> userKey) => KeyMayMatch(userKey, TableReadFlags.None);

    /// <inheritdoc cref="KeyMayMatch(ReadOnlySpan{byte})"/>
    public bool KeyMayMatch(ReadOnlySpan<byte> userKey, TableReadFlags flags)
    {
        if (!WholeKeyFiltering) return true;
        IFilterBitsReader? f = _filter;
        if (f is not null) return f.MayMatch(userKey);
        return !_hasFilter || CachedFilterMayMatch(userKey, flags);
    }

    /// <summary>Filter lookup through the block cache (cache_index_and_filter_blocks). A filter miss is read synchronously.</summary>
    private bool CachedFilterMayMatch(ReadOnlySpan<byte> userKey, TableReadFlags flags)
    {
        BlockCache cache = _cache!;
        CacheKey key = _baseCacheKey.WithOffsetForMinSizeRecord(_filterHandle.Offset);
        if (cache.TryLookup(key, out CachedBlock h))
        {
            _stats?.Record(BlockCacheCounter.FilterHit);
            bool r = Unsafe.As<IFilterBitsReader>(h.Value)!.MayMatch(userKey);
            h.Dispose();
            return r;
        }
        _stats?.Record(BlockCacheCounter.FilterMiss);
        byte[] buf = BlockFetcher.ReadRawPayload(_file, _footer, _filterHandle, _options.VerifyChecksums, out int len);
        IFilterBitsReader reader;
        try
        {
            reader = CreateFilterReader(_options.FilterPolicy!, buf, len);
        }
        catch
        {
            ArrayPool<byte>.Shared.Return(buf);
            throw;
        }
        bool result = reader.MayMatch(userKey);
        if ((flags & TableReadFlags.DontFillCache) != 0)
        {
            ArrayPool<byte>.Shared.Return(buf);
        }
        else if (cache.Insert(key, new CacheValue(buf, len, reader, true), buf.Length, _metaPriority))
        {
            RecordAdd(_stats, BlockKind.Filter, buf.Length);
        }
        return result;
    }

    /// <summary>
    /// Point lookup. <paramref name="internalKey"/> is a lookup key (user key + packed (snapshot seq, kValueTypeForSeek)).
    /// Calls <paramref name="visitor"/> for each entry with the same user key at or after the lookup key, in order,
    /// until it returns false. Port of BlockBasedTable::Get.
    /// </summary>
    public void Get<TState>(ReadOnlySpan<byte> internalKey, ref TState state, TableEntryVisitor<TState> visitor)
        where TState : allows ref struct => Get(internalKey, ref state, visitor, TableReadFlags.None);

    /// <inheritdoc cref="Get{TState}(ReadOnlySpan{byte}, ref TState, TableEntryVisitor{TState})"/>
    public void Get<TState>(ReadOnlySpan<byte> internalKey, ref TState state, TableEntryVisitor<TState> visitor, TableReadFlags flags)
        where TState : allows ref struct
    {
        ReadOnlySpan<byte> userKey = DbFormat.ExtractUserKey(internalKey);
        if (!KeyMayMatch(userKey, flags)) return;
        IndexCursor idx = IndexCursor.Rent(this, flags);
        BlockIter data = IterPool.Rent();
        try
        {
            idx.Seek(internalKey);
            bool first = true;
            while (idx.Valid)
            {
                BlockRef block = GetBlockRef(idx.Handle, BlockKind.Data, flags);
                try
                {
                    data.InitializeData(block.Block!, _icmp, GlobalSequenceNumber);
                    if (first)
                    {
                        if (!data.SeekForGet(internalKey)) return;
                        first = false;
                    }
                    else
                    {
                        data.SeekToFirst();
                    }
                    for (; data.Valid; data.Next())
                    {
                        ReadOnlySpan<byte> k = data.Key;
                        if (!UserKeyEquals(DbFormat.ExtractUserKey(k), userKey)) return;
                        if (!visitor(ref state, k, data.Value)) return;
                    }
                }
                finally
                {
                    data.Clear();
                    block.Release();
                }
                idx.Next();
            }
        }
        finally
        {
            IterPool.Return(data);
            idx.Return();
        }
    }

    /// <summary>
    /// Finds the first entry for the user key of <paramref name="internalKey"/> visible at the lookup sequence number.
    /// Copies its value into <paramref name="value"/> (if non-null). Returns <c>Found=false</c> if no entry exists.
    /// The caller interprets the entry type (deletion, merge, ...).
    /// </summary>
    public TableGetResult Get(ReadOnlySpan<byte> internalKey, IBufferWriter<byte>? value) => Get(internalKey, value, TableReadFlags.None);

    /// <inheritdoc cref="Get(ReadOnlySpan{byte}, IBufferWriter{byte}?)"/>
    public TableGetResult Get(ReadOnlySpan<byte> internalKey, IBufferWriter<byte>? value, TableReadFlags flags)
    {
        if (!KeyMayMatch(DbFormat.ExtractUserKey(internalKey), flags)) return default;
        IndexCursor idx = IndexCursor.Rent(this, flags);
        BlockIter data = IterPool.Rent();
        try
        {
            idx.Seek(internalKey);
            bool first = true;
            while (idx.Valid)
            {
                BlockRef block = GetBlockRef(idx.Handle, BlockKind.Data, flags);
                try
                {
                    int r = ScanBlockForGet(data, block.Block!, internalKey, first, value, out TableGetResult result);
                    if (r != 0) return result;
                    first = false;
                }
                finally
                {
                    data.Clear();
                    block.Release();
                }
                idx.Next();
            }
            return default;
        }
        finally
        {
            IterPool.Return(data);
            idx.Return();
        }
    }

    /// <summary>Async <see cref="Get(ReadOnlySpan{byte}, IBufferWriter{byte}?)"/>: only data block reads are asynchronous; completes synchronously on cache hits.</summary>
    public ValueTask<TableGetResult> GetAsync(ReadOnlyMemory<byte> internalKey, IBufferWriter<byte>? value, CancellationToken cancellationToken = default) =>
        GetAsync(internalKey, value, TableReadFlags.None, cancellationToken);

    /// <inheritdoc cref="GetAsync(ReadOnlyMemory{byte}, IBufferWriter{byte}?, CancellationToken)"/>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<TableGetResult> GetAsync(ReadOnlyMemory<byte> internalKey, IBufferWriter<byte>? value, TableReadFlags flags, CancellationToken cancellationToken)
    {
        if (!KeyMayMatch(DbFormat.ExtractUserKey(internalKey.Span), flags)) return default;
        IndexCursor idx = IndexCursor.Rent(this, flags);
        BlockIter data = IterPool.Rent();
        try
        {
            idx.Seek(internalKey.Span);
            bool first = true;
            while (idx.Valid)
            {
                ValueTask<BlockRef> pending = GetBlockRefAsync(idx.Handle, BlockKind.Data, flags, cancellationToken);
                BlockRef block = pending.IsCompletedSuccessfully ? pending.Result : await pending.ConfigureAwait(false);
                try
                {
                    int r = ScanBlockForGet(data, block.Block!, internalKey.Span, first, value, out TableGetResult result);
                    if (r != 0) return result;
                    first = false;
                }
                finally
                {
                    data.Clear();
                    block.Release();
                }
                idx.Next();
            }
            return default;
        }
        finally
        {
            IterPool.Return(data);
            idx.Return();
        }
    }

    /// <summary>Returns 1 = found, -1 = definitely absent, 0 = continue with the next block.</summary>
    private int ScanBlockForGet(BlockIter data, Block block, ReadOnlySpan<byte> internalKey, bool first, IBufferWriter<byte>? value, out TableGetResult result)
    {
        result = default;
        data.InitializeData(block, _icmp, GlobalSequenceNumber);
        if (first)
        {
            if (!data.SeekForGet(internalKey)) return -1;
        }
        else
        {
            data.SeekToFirst();
        }
        if (!data.Valid) return 0;
        ReadOnlySpan<byte> k = data.Key;
        if (!UserKeyEquals(DbFormat.ExtractUserKey(k), DbFormat.ExtractUserKey(internalKey))) return -1;
        DbFormat.UnpackSequenceAndType(DbFormat.ExtractFooter(k), out ulong seq, out EntryType t);
        value?.Write(data.Value);
        result = new TableGetResult(true, seq, t);
        return 1;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool UserKeyEquals(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => _icmp.EqualUserKey(a, b);

    // ---------------------------------------------------------------------------------------------
    // Iterators

    /// <summary>Creates a two-level (index → data) iterator over the table's point entries. Dispose when done.</summary>
    public BlockBasedTableIterator NewIterator() => new(this, TableReadFlags.None);

    /// <summary>Creates an iterator with explicit read flags (e.g. <see cref="TableReadFlags.DontFillCache"/> for scans/compaction).</summary>
    public BlockBasedTableIterator NewIterator(TableReadFlags flags) => new(this, flags);

    public bool HasRangeTombstones => _rangeDelBlock is not null;

    /// <summary>
    /// Iterator over the range tombstones (internal start key → exclusive end user key), or null if none.
    /// The returned iterator is valid while the reader is alive; dispose it when done.
    /// </summary>
    public BlockIter? NewRangeTombstoneIterator()
    {
        if (_rangeDelBlock is null) return null;
        var it = new BlockIter();
        it.InitializeData(_rangeDelBlock, _icmp, GlobalSequenceNumber);
        return it;
    }

    /// <summary>
    /// Releases the reader's pinned blocks and the file. Entries this file put into a shared block cache stay there
    /// until evicted (keys are not reused by other files); erase them explicitly when the file is deleted if desired.
    /// </summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        _indexBlock?.Release();
        _rangeDelBlock?.Release();
        byte[]? fb = Interlocked.Exchange(ref _filterBuffer, null);
        if (fb is not null) ArrayPool<byte>.Shared.Return(fb);
        _file.Dispose();
    }

    /// <summary>Wraps a file and serves reads within an asynchronously prefetched tail region from memory (port of tail prefetching in BlockBasedTable::PrefetchTail).</summary>
    private sealed class TailPrefetchFile : IRandomAccessFile
    {
        private readonly IRandomAccessFile _inner;
        private byte[]? _tail;
        private long _tailOffset;
        private int _tailLength;

        public TailPrefetchFile(IRandomAccessFile inner) => _inner = inner;
        public long Length => _inner.Length;

        public async ValueTask PrefetchAsync(long fileSize, Footer footer, CancellationToken ct)
        {
            // The tail start isn't known before the properties are read; like upstream's default tail prefetch,
            // read the last 512 KiB (at least through the metaindex). Reads outside fall back to the inner file.
            long start = Math.Max(0, fileSize - (512 << 10));
            if (footer.IndexHandle.Size > 0) start = Math.Min(start, (long)footer.IndexHandle.Offset);
            start = Math.Min(start, (long)footer.MetaindexHandle.Offset);
            long len = fileSize - start;
            if (len <= 0 || len > 64L << 20) return;
            byte[] buf = ArrayPool<byte>.Shared.Rent((int)len);
            int total = 0;
            while (total < len)
            {
                int r = await _inner.ReadAsync(start + total, buf.AsMemory(total, (int)len - total), ct).ConfigureAwait(false);
                if (r <= 0) break;
                total += r;
            }
            _tail = buf;
            _tailOffset = start;
            _tailLength = total;
        }

        public int Read(long offset, Span<byte> destination)
        {
            if (_tail is not null && offset >= _tailOffset && offset + destination.Length <= _tailOffset + _tailLength)
            {
                _tail.AsSpan((int)(offset - _tailOffset), destination.Length).CopyTo(destination);
                return destination.Length;
            }
            return _inner.Read(offset, destination);
        }

        public ValueTask<int> ReadAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken = default) =>
            new(Read(offset, destination.Span));

        public void ReleaseBuffer()
        {
            if (_tail is not null) ArrayPool<byte>.Shared.Return(_tail);
            _tail = null;
        }

        public void Dispose() => ReleaseBuffer();
    }
}

/// <summary>Small per-thread pool of <see cref="BlockIter"/> instances for point lookups.</summary>
internal static class IterPool
{
    [ThreadStatic] private static BlockIter?[]? t_items;
    [ThreadStatic] private static int t_count;

    public static BlockIter Rent()
    {
        BlockIter?[]? items = t_items;
        if (items is not null && t_count > 0)
        {
            BlockIter it = items[--t_count]!;
            items[t_count] = null;
            return it;
        }
        return new BlockIter();
    }

    public static void Return(BlockIter it)
    {
        it.Clear();
        BlockIter?[] items = t_items ??= new BlockIter?[8];
        if (t_count < items.Length) items[t_count++] = it;
        else it.Dispose();
    }
}
