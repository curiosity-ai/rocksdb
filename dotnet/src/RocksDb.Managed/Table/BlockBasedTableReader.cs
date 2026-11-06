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

/// <summary>Options for opening a <see cref="TableReader"/> (<see cref="TableReaderFactory.Open"/>).</summary>
public sealed record TableReaderOptions
{
    /// <summary>
    /// The column family's table factory: the format files are read as (a file of another format fails with Corruption
    /// "Bad table magic number"), or <see cref="TableFactoryType.AdaptiveTable"/> to pick the reader by magic number.
    /// </summary>
    public TableFactoryType TableFactory { get; init; } = TableFactoryType.BlockBasedTable;
    /// <summary>Plain table reader settings (bloom_bits_per_key, hash_table_ratio, index_sparseness, full_scan_mode); null: defaults.</summary>
    public PlainTableOptions? PlainTableOptions { get; init; }
    /// <summary>DBOptions::allow_mmap_reads: cuckoo tables can only be read memory-mapped (InvalidArgument otherwise).</summary>
    public bool AllowMmapReads { get; init; }
    /// <summary>ImmutableOptions::bloom_locality: locality of the bloom filters plain tables build at open.</summary>
    public uint BloomLocality { get; init; }
    /// <summary>BlockBasedTableOptions::user_defined_index_factory: reader of the tables' custom index; null: none.</summary>
    public UserDefinedIndexFactory? UserDefinedIndexFactory { get; init; }
    /// <summary>BlockBasedTableOptions::index_mode (effective).</summary>
    public BlockBasedIndexMode IndexMode { get; init; } = BlockBasedIndexMode.StandardDefault;

    /// <summary>User comparator the table was built with.</summary>
    public Comparator Comparator { get; init; } = Comparator.Bytewise;
    /// <summary>Filter policy used to interpret the full filter block ("fullfilter.&lt;name&gt;"); null ignores filters.</summary>
    public FilterPolicy? FilterPolicy { get; init; }
    public bool VerifyChecksums { get; init; } = true;
    /// <summary>
    /// block_protection_bytes_per_key (0, 1, 2, 4 or 8): per key-value checksums of the data, index and metaindex blocks
    /// in memory, verified for every entry an iterator positions on (Corruption on mismatch). Default 0 (off).
    /// </summary>
    public int BlockProtectionBytesPerKey { get; init; }
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
    /// <summary>
    /// pin_top_level_index_and_filter: with <see cref="CacheIndexAndFilterBlocks"/>, the top-level block of a partitioned
    /// index or filter is inserted into the block cache and also pinned by the reader for its lifetime. Default true.
    /// </summary>
    public bool PinTopLevelIndexAndFilter { get; init; } = true;
    /// <summary>
    /// The column family's current prefix extractor (MutableCFOptions::prefix_extractor). Point lookups use a table's
    /// prefix filter (whole_key_filtering off) only when this extractor has the id recorded in the table; prefix seeks
    /// use the table's own extractor (recreated from its id when it differs, like BlockBasedTable::Open).
    /// </summary>
    public SliceTransform? PrefixExtractor { get; init; }
    /// <summary>Current file number of the table (see <see cref="DbSessionId"/>); 0 when unknown.</summary>
    public ulong FileNumber { get; init; }
    /// <summary>Optional block cache hit/miss counters (may be shared by many readers); null disables recording.</summary>
    public BlockCacheStatistics? Statistics { get; init; }
    /// <summary>
    /// DBOptions::lowest_used_cache_tier: <see cref="Cache.CacheTier.VolatileTier"/> keeps blocks out of any secondary
    /// cache of <see cref="BlockCache"/> (they are inserted without secondary-cache callbacks). Default: all tiers.
    /// </summary>
    public Cache.CacheTier LowestUsedCacheTier { get; init; } = Cache.CacheTier.NonVolatileBlockTier;
    /// <summary>
    /// BlockBasedTableOptions::persistent_cache: consulted before reading a block from the file, filled after a read
    /// (table/persistent_cache_helper.cc). Null: none.
    /// </summary>
    public Cache.PersistentCache? PersistentCache { get; init; }
    /// <summary>
    /// Largest sequence number of the file as recorded in the MANIFEST, used to resolve the global sequence number
    /// of ingested external files; <see cref="DbFormat.MaxSequenceNumber"/> when unknown.
    /// </summary>
    public ulong LargestSeqno { get; init; } = DbFormat.MaxSequenceNumber;
    /// <summary>
    /// index_block_search_type: search algorithm for index blocks (binary-search indexes and index partitions; the top
    /// level of a partitioned index and hash indexes always use binary search, like upstream's index readers).
    /// </summary>
    public IndexBlockSearchType IndexBlockSearchType { get; init; } = IndexBlockSearchType.Binary;
    /// <summary>
    /// DBOptions::rate_limiter for block reads whose <see cref="TableReadFlags"/> carry a rate limiter priority
    /// (RandomAccessFileReader::ChargeRateLimiterForRead). Null: reads are never charged.
    /// </summary>
    public Env.IRateLimiter? RateLimiter { get; init; }
    /// <summary>
    /// max_auto_readahead_size: iterators read sequential data blocks with automatic readahead growing up to this many
    /// bytes (0 disables it). Default 256 KiB.
    /// </summary>
    public long MaxAutoReadaheadSize { get; init; } = 256 * 1024;
    /// <summary>initial_auto_readahead_size: the first automatic readahead (0 disables it). Default 8 KiB.</summary>
    public long InitialAutoReadaheadSize { get; init; } = 8 * 1024;
    /// <summary>num_file_reads_for_auto_readahead: sequential data block reads before automatic readahead starts. Default 2.</summary>
    public ulong NumFileReadsForAutoReadahead { get; init; } = 2;

    /// <summary>The DB's block cache tracer (BlockBasedTable::block_cache_tracer_), or null.</summary>
    internal Tracing.BlockCacheTracer? BlockCacheTracer { get; init; }

    /// <summary>LSM level of the file (TableReaderOptions::level, -1 unknown); only used for tracing.</summary>
    internal int Level { get; init; } = -1;

    /// <summary>block_restart_interval of the table options (estimates the keys of a traced block).</summary>
    internal int BlockRestartInterval { get; init; } = 16;

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

/// <summary>
/// Per-iterator prefix seek settings derived from <see cref="ReadOptions"/> and the column family's prefix extractor
/// (the check_filter / need_upper_bound_check / prefix_extractor arguments of BlockBasedTable::NewIterator).
/// </summary>
public readonly struct TableIteratorPrefixOptions
{
    /// <summary>The column family's current prefix extractor.</summary>
    public SliceTransform? PrefixExtractor { get; init; }
    /// <summary>check_filter: consult the prefix filter on Seek / SeekForPrev.</summary>
    public bool CheckFilter { get; init; }
    /// <summary>need_upper_bound_check: only filter when [seek key, upper bound) is within one prefix (auto_prefix_mode, changed extractor).</summary>
    public bool NeedUpperBoundCheck { get; init; }
    /// <summary>ReadOptions::iterate_upper_bound.</summary>
    public ReadOnlyMemory<byte>? UpperBound { get; init; }
}

/// <summary>Kind of filter block in a table.</summary>
internal enum FilterKind : byte
{
    None,
    /// <summary>"fullfilter.&lt;name&gt;": one filter for the whole table.</summary>
    Full,
    /// <summary>"partitionedfilter.&lt;name&gt;": a top-level index over filter partitions.</summary>
    Partitioned,
}

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
public sealed partial class BlockBasedTableReader : TableReader
{
    private static long s_nextTableId;

    private readonly IRandomAccessFile _file;
    private readonly TableReaderOptions _options;
    private readonly InternalKeyComparator _icmp;
    /// <summary>User-defined timestamp size of the comparator (0 = none): filters are keyed by the user key without it.</summary>
    private readonly int _tsSize;
    private readonly Footer _footer;
    private readonly Block? _indexBlock;          // null when the index lives in the block cache
    private readonly Block? _rangeDelBlock;
    private readonly IFilterBitsReader? _filter;  // pinned full filter (null when absent or cached)
    private byte[]? _filterBuffer;
    private readonly bool _hasFilter;
    private readonly FilterKind _filterKind;
    private readonly BlockHandle _filterHandle;   // full filter / top-level partition index handle (for the block cache)
    private readonly Block? _filterIndexBlock;    // pinned top-level filter partition index (null when absent or cached)
    private readonly PinnedFilterPartitions? _pinnedFilterPartitions;
    private readonly SliceTransform? _tablePrefixExtractor;
    private readonly bool _prefixExtractorChanged; // relative to TableReaderOptions.PrefixExtractor
    private readonly Compression.CompressionDictionary? _compressionDict; // "rocksdb.compression_dict" (data blocks)
    private readonly ulong _tableId;
    private readonly BlockCache? _cache;          // native block cache (null: none or legacy seam)
    private readonly IBlockCache? _legacyCache;   // legacy IBlockCache seam (null: none or native)
    private readonly OffsetableCacheKey _baseCacheKey;
    private readonly CachePriority _metaPriority;
    private readonly BlockCacheStatistics? _stats;
    private readonly Cache.CacheItemHelper[] _helpers;      // per BlockKind (secondary-cache callbacks)
    private readonly Cache.PersistentCache? _persistentCache;
    private readonly Tracing.BlockCacheTracer? _bcTracer;
    private int _disposed;

    public Footer Footer => _footer;
    public override TableProperties Properties { get; }
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
    public override ulong GlobalSequenceNumber { get; }
    /// <summary>
    /// Size of the minimum timestamp padded into every key read from this file (pad_min_timestamp_), 0 when the keys
    /// are stored with their timestamps. Non-zero for files written with persist_user_defined_timestamps = false
    /// ("rocksdb.user.defined.timestamps.persisted" = 0) and for files written without timestamps by the comparator
    /// this column family's timestamp comparator extends (UDT enabled on an existing column family, ".u64ts" suffix).
    /// Deviation: upstream takes this from FileMetaData::user_defined_timestamps_persisted; the file's own properties
    /// give the same answer for every file the toggling rules (ValidateUserDefinedTimestampsOptions) accept.
    /// </summary>
    public override int PadTimestampSize { get; }
    public override long FileSize { get; }
    internal override InternalKeyComparator InternalComparator => _icmp;
    internal IRandomAccessFile File => _file;

    private BlockBasedTableReader(IRandomAccessFile file, TableReaderOptions options, Footer footer, Dictionary<string, BlockHandle> metaBlocks,
        TableProperties props, BlockHandle indexHandle, Block? indexBlock, Block? rangeDel, IFilterBitsReader? filter, byte[]? filterBuffer,
        FilterKind filterKind, BlockHandle filterHandle, Block? filterIndexBlock, PinnedFilterPartitions? pinnedFilterPartitions,
        Compression.CompressionDictionary? compressionDict, in OffsetableCacheKey baseCacheKey, long fileSize)
    {
        _compressionDict = compressionDict;
        bool hasFilter = filterKind != FilterKind.None;
        _filterKind = filterKind;
        _filterIndexBlock = filterIndexBlock;
        _pinnedFilterPartitions = pinnedFilterPartitions;
        _tablePrefixExtractor = ResolveTablePrefixExtractor(props, options.PrefixExtractor, out _prefixExtractorChanged);
        IndexHandle = indexHandle;
        _file = file;
        _options = options;
        _icmp = options.Comparator.Kind == ComparatorKind.Bytewise ? InternalKeyComparator.Bytewise : new InternalKeyComparator(options.Comparator);
        _tsSize = options.Comparator.TimestampSize;
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
        _helpers = HelpersFor(options);
        _persistentCache = options.PersistentCache;
        _bcTracer = options.BlockCacheTracer;

        IndexType = ReadIndexType(props);
        IndexKeyIncludesSeq = props.IndexKeyIsUserKey == 0;
        IndexValueIsFull = props.IndexValueIsDeltaEncoded == 0;
        WholeKeyFiltering = !props.UserCollectedProperties.TryGetValue(TablePropertiesNames.BlockBasedWholeKeyFiltering, out byte[]? wkf)
            || wkf.AsSpan().SequenceEqual("1"u8);
        GlobalSequenceNumber = ResolveGlobalSeqno(props, options.LargestSeqno);
        PadTimestampSize = TimestampsStripped(props, options.Comparator) ? _tsSize : 0;
        if (IndexType == IndexType.HashSearch && _tablePrefixExtractor is not null) _prefixIndex = TryCreatePrefixIndex(file, footer, metaBlocks, options.VerifyChecksums);
    }

    /// <summary>
    /// Port of HashIndexReader::Create (table/block_based/hash_index_reader.cc): builds the <see cref="BlockPrefixIndex"/>
    /// from the hash index meta blocks. Failure to create the prefix hash index is not a hard error; the index is then
    /// searched with binary search (null).
    /// </summary>
    private static BlockPrefixIndex? TryCreatePrefixIndex(IRandomAccessFile file, in Footer footer, Dictionary<string, BlockHandle> metaBlocks, bool verify)
    {
        if (!metaBlocks.TryGetValue(BlockBasedTableConstants.HashIndexPrefixesBlockName, out BlockHandle prefixesHandle)
            || !metaBlocks.TryGetValue(BlockBasedTableConstants.HashIndexPrefixesMetadataBlockName, out BlockHandle metaHandle))
            return null;
        byte[]? prefixes = null, meta = null;
        try
        {
            prefixes = BlockFetcher.ReadRawPayload(file, footer, prefixesHandle, verify, out int prefixesLength);
            meta = BlockFetcher.ReadRawPayload(file, footer, metaHandle, verify, out int metaLength);
            return BlockPrefixIndex.Create(prefixes.AsSpan(0, prefixesLength), meta.AsSpan(0, metaLength));
        }
        catch (RocksDbException)
        {
            return null;
        }
        finally
        {
            if (prefixes is { Length: > 0 }) ArrayPool<byte>.Shared.Return(prefixes);
            if (meta is { Length: > 0 }) ArrayPool<byte>.Shared.Return(meta);
        }
    }

    private readonly BlockPrefixIndex? _prefixIndex;

    /// <summary>The kHashSearch prefix index (null unless the table has a hash index and its prefix extractor is known).</summary>
    public BlockPrefixIndex? PrefixIndex => _prefixIndex;

    /// <summary>
    /// The candidate data blocks for the prefix of <paramref name="internalKey"/>'s user key when point lookups can use the
    /// hash index (prefix extractor unchanged and the key in its domain); false to use binary search.
    /// </summary>
    internal bool TryGetPrefixBlocks(ReadOnlySpan<byte> internalKey, out ReadOnlySpan<uint> blocks)
    {
        blocks = default;
        BlockPrefixIndex? pi = _prefixIndex;
        if (pi is null || _prefixExtractorChanged) return false;
        ReadOnlySpan<byte> userKey = DbFormat.ExtractUserKey(internalKey);
        SliceTransform pe = _tablePrefixExtractor!;
        if (!pe.InDomain(userKey)) return false;
        blocks = pi.GetBlocks(pe.Transform(userKey));
        return true;
    }

    /// <summary>True when the keys of a table with <paramref name="props"/> are stored without the timestamps of <paramref name="ucmp"/>.</summary>
    internal static bool TimestampsStripped(TableProperties props, Comparator ucmp)
    {
        if (ucmp.TimestampSize == 0) return false;
        if (props.UserDefinedTimestampsPersisted == 0) return true;
        // CompareComparator (util/udt_util.cc) == kEnableUDT: written by "<name>", read by "<name>.u64ts".
        string running = ucmp.Name;
        string written = props.ComparatorName;
        return written.Length > 0 && running.Length == written.Length + UdtComparatorSuffix.Length
            && running.StartsWith(written, StringComparison.Ordinal) && running.EndsWith(UdtComparatorSuffix, StringComparison.Ordinal);
    }

    /// <summary>kUDTSuffix of util/udt_util.cc.</summary>
    internal const string UdtComparatorSuffix = ".u64ts";

    private static IndexType ReadIndexType(TableProperties props)
    {
        IndexType t = props.UserCollectedProperties.TryGetValue(TablePropertiesNames.BlockBasedIndexType, out byte[]? it) && it.Length >= 4
            ? (IndexType)Coding.DecodeFixed32(it)
            : IndexType.BinarySearch;
        if (t > IndexType.BinarySearchWithFirstKey) ThrowHelper.Corruption("unknown index type " + (uint)t);
        return t;
    }

    /// <summary>
    /// Port of the prefix extractor setup in BlockBasedTable::Open: the configured extractor when its id matches the
    /// table's "rocksdb.prefix.extractor.name" (PrefixExtractorChangedHelper), else the built-in extractor recreated from
    /// the recorded id (null when the table has none or it is not a built-in).
    /// </summary>
    private static SliceTransform? ResolveTablePrefixExtractor(TableProperties props, SliceTransform? configured, out bool changed)
    {
        string name = props.PrefixExtractorName;
        if (configured is not null && name.Length > 0 && name == configured.AsString())
        {
            // Establish fast path for unchanged prefix_extractor
            changed = false;
            return configured;
        }
        changed = true;
        return name.Length > 0 ? SliceTransform.CreateFromString(name) : null;
    }

    /// <summary>
    /// Port of BlockBasedTable::PrefixExtractorChanged: true unless <paramref name="prefixExtractor"/> has the id the
    /// table was built with.
    /// </summary>
    public bool PrefixExtractorChanged(SliceTransform? prefixExtractor)
    {
        if (prefixExtractor is null) return true;
        if (ReferenceEquals(prefixExtractor, _options.PrefixExtractor)) return _prefixExtractorChanged;
        if (ReferenceEquals(prefixExtractor, _tablePrefixExtractor)) return false;
        string name = Properties.PrefixExtractorName;
        return name.Length == 0 || name != prefixExtractor.AsString();
    }

    /// <summary>The prefix extractor the table was built with (null when none or not recreatable).</summary>
    public SliceTransform? TablePrefixExtractor => _tablePrefixExtractor;

    /// <summary>The dictionary data blocks were compressed with (UncompressionDict), or null.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private Compression.CompressionDictionary? DictFor(BlockKind kind) => kind == BlockKind.Data ? _compressionDict : null;

    /// <summary>The table's compression dictionary ("rocksdb.compression_dict"), empty when none.</summary>
    public ReadOnlySpan<byte> CompressionDictionary => _compressionDict is null ? default : _compressionDict.Data;

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
        Block metaindex = Protect(BlockFetcher.ReadBlock(file, footer, footer.MetaindexHandle, verify), options);
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
            // ReadTablePropertiesHelper: read and parse without checksum verification, then verify (an ingested file
            // whose global seqno was rewritten by the legacy write_global_seqno has a stale properties checksum).
            Block pb = BlockFetcher.ReadBlock(file, footer, propsHandle, verifyChecksums: false);
            try { props = TableProperties.Parse(pb, propsHandle.Offset); }
            finally { pb.Release(); }
            if (verify) VerifyPropertiesBlockChecksum(file, footer, propsHandle, props.ExternalSstFileGlobalSeqnoOffset);
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

        // A pinned index and filter (not in the block cache) live as long as the reader: their buffers come from the table
        // buffer pool, where the table builder has just returned its index and filter buffers (the same sizes) and where
        // the builders of later flushes / compactions find them again once this reader is closed. Blocks that go to the
        // block cache keep the block buffer pool's finer size classes (the cache is charged the buffer length).
        BlockBufferSource pinnedSource = metaCache is null ? BlockBufferSource.TableBuffers : BlockBufferSource.BlockBufferPool;
        // pin_top_level_index_and_filter (partitioned index / filter only; unpartitioned ones are not pinned when cached).
        bool pinTopLevel = options.PinTopLevelIndexAndFilter;
        // A cached index that is not pinned is looked up in the block cache (and its secondary tiers) once the reader
        // exists, and read only on a miss (RetrieveBlock in IndexReaderCommon::ReadIndexBlock with prefetch).
        bool prefetchIndexIntoCache = metaCache is not null && !(pinTopLevel && ReadIndexType(props) == IndexType.TwoLevelIndexSearch);
        Block? indexBlock = prefetchIndexIntoCache ? null : Protect(BlockFetcher.ReadBlock(file, footer, indexHandle, verify, null, pinnedSource), options);
        bool prefetchFilterIntoCache = false;
        BlockHandle prefetchFilterHandle = default;
        Block? rangeDel = null;
        byte[]? filterBuf = null;
        BlockBufferSource filterSource = BlockBufferSource.NotRented;
        Block? filterIndex = null;
        PinnedFilterPartitions? pinnedParts = null;
        Compression.CompressionDictionary? compressionDict = null;
        BlockBasedTableReader reader;
        try
        {
            if (metaCache is not null && indexBlock is not null)
            {
                // PrefetchIndexAndFilterBlocks with prefetch_index_and_filter_in_cache and pin_top_level_index_and_filter:
                // the pinned top level of a partitioned index is cached too.
                Block b = indexBlock;
                b.AddRef();
                if (metaCache.Insert(baseCacheKey.WithOffsetForMinSizeRecord(indexHandle.Offset), CacheValue.FromObject(b).WithHelper(HelpersFor(options)[(int)BlockKind.Index]), b.Charge, metaPriority))
                    RecordAdd(options.Statistics, BlockKind.Index, b.Charge);
            }

            if (metaBlocks.TryGetValue(BlockBasedTableConstants.RangeDelBlockName, out BlockHandle rdHandle))
                rangeDel = Protect(BlockFetcher.ReadBlock(file, footer, rdHandle, verify), options);

            // Port of the compression dictionary setup (ReadCompressionDictBlock / UncompressionDictReader): pinned by
            // the reader for its lifetime and used to decompress data blocks.
            if (metaBlocks.TryGetValue(BlockBasedTableConstants.CompressionDictBlockName, out BlockHandle dictHandle) && dictHandle.Size > 0)
            {
                byte[] raw = BlockFetcher.ReadRawPayload(file, footer, dictHandle, verify, out int dictLength);
                try { compressionDict = new Compression.CompressionDictionary(raw.AsSpan(0, dictLength).ToArray()); }
                finally { ArrayPool<byte>.Shared.Return(raw); }
            }

            // Port of the filter lookup in BlockBasedTable::PrefetchIndexAndFilterBlocks: the filter is keyed by the
            // configured policy's CompatibilityName (no policy -> filters ignored), as a full or a partitioned filter.
            IFilterBitsReader? filter = null;
            FilterKind filterKind = FilterKind.None;
            BlockHandle fh = default;
            if (options.FilterPolicy is { } ppolicy
                && metaBlocks.TryGetValue(BlockBasedTableConstants.PartitionedFilterBlockPrefix + ppolicy.CompatibilityName, out fh)
                && fh.Size > 0)
            {
                filterKind = FilterKind.Partitioned;
                Block top = BlockFetcher.ReadBlock(file, footer, fh, verify);
                filterIndex = top;
                if (!options.CacheIndexAndFilterBlocks || metaCache is null)
                {
                    // Partition pinning (MetadataCacheOptions kFallback without cache_index_and_filter_blocks):
                    // PartitionedFilterBlockReader::CacheDependencies loads all partitions up front and keeps them.
                    pinnedParts = PinnedFilterPartitions.Load(file, footer, verify, ppolicy, top, props, options.Comparator);
                }
                if (metaCache is not null)
                {
                    if (pinTopLevel) top.AddRef();
                    else filterIndex = null;
                    if (metaCache.Insert(baseCacheKey.WithOffsetForMinSizeRecord(fh.Offset), CacheValue.FromObject(top).WithHelper(HelpersFor(options)[(int)BlockKind.Filter]), top.Charge, metaPriority))
                        RecordAdd(options.Statistics, BlockKind.Filter, top.Charge);
                }
            }
            else if (options.FilterPolicy is { } policy
                && metaBlocks.TryGetValue(BlockBasedTableConstants.FullFilterBlockPrefix + policy.CompatibilityName, out fh))
            {
                filterKind = FilterKind.Full;
                if (metaCache is not null)
                {
                    // Looked up in the block cache (and its secondary tiers) once the reader exists; read on a miss.
                    prefetchFilterIntoCache = true;
                    prefetchFilterHandle = fh;
                }
                else
                {
                    filterSource = BlockBufferSource.TableBuffers;
                    filterBuf = BlockFetcher.ReadRawPayload(file, footer, fh, verify, filterSource, out int flen);
                    filter = CreateFilterReader(policy, filterBuf, flen);
                }
            }
            UserDefinedIndexReader? udi = OpenUserDefinedIndex(file, options, footer, metaBlocks, props, verify, out bool udiStub);
            reader = new BlockBasedTableReader(ownedFile ?? file, options, footer, metaBlocks, props, indexHandle, indexBlock, rangeDel, filter, filterBuf,
                filterKind, fh, filterIndex, pinnedParts, compressionDict, baseCacheKey, fileSize);
            reader._udiReader = udi;
            reader._udiStub = udiStub;
            reader._udiPrimary = options.IndexMode is BlockBasedIndexMode.CustomDefault or BlockBasedIndexMode.CustomOnly;
        }
        catch
        {
            indexBlock?.Release();
            rangeDel?.Release();
            filterIndex?.Release();
            pinnedParts?.Dispose();
            compressionDict?.Dispose();
            if (filterBuf is not null) TableBuilderBuffers.Return(filterSource, filterBuf);
            throw;
        }
        if (prefetchIndexIntoCache || prefetchFilterIntoCache)
        {
            try
            {
                // PrefetchIndexAndFilterBlocks with prefetch_index_and_filter_in_cache: warm the block cache, reading through
                // `file` (the tail prefetch buffer of an async open).
                if (prefetchIndexIntoCache) reader.PrefetchIndexIntoCache(metaCache!, file);
                if (prefetchFilterIntoCache) reader.PrefetchFilterIntoCache(metaCache!, file, prefetchFilterHandle);
            }
            catch
            {
                reader.Dispose();
                throw;
            }
        }
        return reader;
    }

    /// <summary>
    /// Looks the index block up in the block cache (with the secondary tiers, as RetrieveBlock does) and reads and inserts
    /// it on a miss. Hits and misses are recorded like any index block lookup.
    /// </summary>
    private void PrefetchIndexIntoCache(BlockCache cache, IRandomAccessFile file)
    {
        CacheKey key = _baseCacheKey.WithOffsetForMinSizeRecord(IndexHandle.Offset);
        if (LookupCache(cache, key, BlockKind.Index, out CachedBlock h))
        {
            _stats?.Record(HitCounter(BlockKind.Index));
            h.Dispose();
            return;
        }
        _stats?.Record(MissCounter(BlockKind.Index));
        Block b = Protect(BlockFetcher.ReadBlock(file, _footer, IndexHandle, _options.VerifyChecksums, null, BlockBufferSource.BlockBufferPool), BlockKind.Index);
        InsertIntoCache(cache, key, b, BlockKind.Index).Release();
    }

    /// <summary>The full filter counterpart of <see cref="PrefetchIndexIntoCache"/>.</summary>
    private void PrefetchFilterIntoCache(BlockCache cache, IRandomAccessFile file, BlockHandle handle)
    {
        CacheKey key = _baseCacheKey.WithOffsetForMinSizeRecord(handle.Offset);
        if (LookupFilterCache(cache, key, out CachedBlock h))
        {
            _stats?.Record(BlockCacheCounter.FilterHit);
            h.Dispose();
            return;
        }
        _stats?.Record(BlockCacheCounter.FilterMiss);
        // A cached filter's buffer is returned to ArrayPool.Shared by the cache entry (CacheValue).
        byte[] buf = BlockFetcher.ReadRawPayload(file, _footer, handle, _options.VerifyChecksums, BlockBufferSource.Shared, out int len);
        IFilterBitsReader filter;
        try
        {
            filter = CreateFilterReader(_options.FilterPolicy!, buf, len);
        }
        catch
        {
            ArrayPool<byte>.Shared.Return(buf);
            throw;
        }
        if (cache.Insert(key, new CacheValue(buf, len, filter, true).WithHelper(FilterHelperFor(_options)), buf.Length, _metaPriority))
            RecordAdd(_stats, BlockKind.Filter, buf.Length);
    }

    private UserDefinedIndexReader? _udiReader;

    /// <summary>
    /// Port of BlockBasedTable::ApproximateMemoryUsage: the blocks pinned by this reader outside the block cache (index,
    /// full filter or filter partition index and partitions, range tombstones) and the user-defined index reader.
    /// Blocks held by the block cache are not counted (they are part of the cache's usage). Unlike upstream,
    /// sizeof(Rep) and the index/filter reader objects themselves are not included.
    /// </summary>
    public override ulong ApproximateMemoryUsage
    {
        get
        {
            ulong usage = 0;
            if (_indexBlock is not null) usage += (ulong)_indexBlock.Charge;
            if (_rangeDelBlock is not null) usage += (ulong)_rangeDelBlock.Charge;
            if (_filter is not null && _filterBuffer is not null) usage += (ulong)_filterBuffer.Length;
            if (_filterIndexBlock is not null) usage += (ulong)_filterIndexBlock.Charge;
            if (_pinnedFilterPartitions is not null) usage += _pinnedFilterPartitions.MemoryUsage;
            if (_udiReader is not null) usage += _udiReader.ApproximateMemoryUsage;
            return usage;
        }
    }
    private bool _udiStub;
    private bool _udiPrimary;

    /// <summary>The table's user-defined index reader (null: none loaded).</summary>
    public UserDefinedIndexReader? UserDefinedIndex => _udiReader;

    /// <summary>
    /// Port of the user-defined index setup of BlockBasedTable::Open / PrefetchIndexAndFilterBlocks
    /// (block_based_table_reader.cc): validates index_mode against the file (footer kFooterFeatureRequireUserDefinedIndex,
    /// "rocksdb.standard.index.is.stub") and loads the "rocksdb.user_defined_index.&lt;name&gt;" block through the configured
    /// factory. In kStandardDefault a missing or unusable custom index falls back to the standard index.
    /// </summary>
    private static UserDefinedIndexReader? OpenUserDefinedIndex(IRandomAccessFile file, TableReaderOptions options, in Footer footer,
        Dictionary<string, BlockHandle> metaBlocks, TableProperties props, bool verify, out bool requiresCustomReader)
    {
        BlockBasedIndexMode mode = options.IndexMode;
        UserDefinedIndexFactory? factory = options.UserDefinedIndexFactory;
        if (mode is BlockBasedIndexMode.CustomDefault or BlockBasedIndexMode.CustomOnly && factory is null)
            ThrowHelper.InvalidArgument("index_mode kCustomDefault/kCustomOnly requires user_defined_index_factory");
        requiresCustomReader = (footer.IncompatibleFeatures & Footer.FeatureRequireUserDefinedIndex) != 0 || props.StandardIndexIsStub != 0;
        if (requiresCustomReader && (mode == BlockBasedIndexMode.StandardOnly || factory is null))
            ThrowHelper.InvalidArgument("SST has only a stub standard index; configure the matching user_defined_index_factory to read it");
        if (mode == BlockBasedIndexMode.StandardOnly || factory is null) return null;
        // kStandardDefault is the only mode that treats the custom index as optional.
        bool customIndexRequired = mode != BlockBasedIndexMode.StandardDefault || requiresCustomReader;
        string name = factory.Name;
        UserDefinedIndexReader? reader = null;
        if (!metaBlocks.TryGetValue(UserDefinedIndexFactory.MetaBlockPrefix + name, out BlockHandle h))
        {
            if (customIndexRequired) ThrowHelper.Corruption("Not found: Cannot find the meta block " + UserDefinedIndexFactory.MetaBlockPrefix + name);
        }
        else if (h.Size == 0)
        {
            // A zero-size UDI block means no user-defined index for this SST (legacy builds).
            if (requiresCustomReader) ThrowHelper.Corruption("UDI block " + name + " has size 0 -- cannot serve reads in the configured index_mode");
        }
        else
        {
            try
            {
                byte[] raw = BlockFetcher.ReadRawPayload(file, footer, h, verify, out int length);
                byte[] contents;
                try { contents = raw.AsSpan(0, length).ToArray(); }
                finally { if (raw.Length > 0) ArrayPool<byte>.Shared.Return(raw); }
                reader = factory.NewReader(new UserDefinedIndexOptions { Comparator = options.Comparator }, contents)
                    ?? throw RocksDbException.Corruption("Failed to create UDI reader for " + name);
            }
            catch (RocksDbException e) when (!customIndexRequired && e.Code is StatusCode.Corruption or StatusCode.NotSupported or StatusCode.InvalidArgument)
            {
                // The file is still fully indexed by the standard index: keep it readable.
                reader = null;
            }
        }
        if (requiresCustomReader && reader is null) ThrowHelper.Corruption("SST requires a matching user_defined_index_factory reader");
        return reader;
    }

    /// <summary>
    /// IndexFactoryReaderWrapper::NewIterator: the custom index iterator when the read routes through it (kPreferCustom,
    /// or kDefault with a primary custom index / a stub standard index), else null for the standard index.
    /// </summary>
    internal UserDefinedIndexIterator? NewUserDefinedIndexIterator(TableReadFlags flags)
    {
        UserDefinedIndexReader? udi = _udiReader;
        if (udi is null) return null;
        bool useUdi = (flags & TableReadFlags.BuiltinIndex) != 0 ? false
            : (flags & TableReadFlags.PreferCustomIndex) != 0 || _udiPrimary || _udiStub;
        if (!useUdi)
        {
            // Only reachable via an explicit read_index=kBuiltin request.
            if (_udiStub) ThrowHelper.InvalidArgument("Cannot read via the standard index because this SST was written with only a custom index");
            return null;
        }
        return udi.NewIterator(null) ?? throw RocksDbException.Corruption("Could not create UDI iterator");
    }

    // Any built-in policy reads any built-in filter format (BuiltinFilterPolicy::GetBuiltinFilterBitsReader).
    internal static IFilterBitsReader CreateFilterReader(FilterPolicy policy, byte[] buffer, int length) =>
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

    /// <summary>
    /// The stable base cache key of a table (the "newer SST file" case of BlockBasedTable::SetupBaseCacheKey, where
    /// out_is_stable is true): false when the properties lack the session id or the original file number, so the keys
    /// of the file depend on the current DB session.
    /// </summary>
    public static bool TryGetStableBaseCacheKey(TableProperties properties, out OffsetableCacheKey baseKey)
    {
        ArgumentNullException.ThrowIfNull(properties);
        if (properties.DbSessionId.Length > 0 && properties.OriginalFileNumber > 0)
        {
            baseKey = new OffsetableCacheKey(properties.DbId, properties.DbSessionId, properties.OriginalFileNumber);
            return true;
        }
        baseKey = default;
        return false;
    }

    /// <summary>
    /// Properties block checksum check of ReadTablePropertiesHelper (table/meta_blocks.cc): on a mismatch, retry with
    /// the rocksdb.external_sst_file.global_seqno value restored to its original zero placeholder, since older
    /// ingestion (write_global_seqno) rewrote it without updating the block checksum. Other mismatches are Corruption.
    /// </summary>
    private static void VerifyPropertiesBlockChecksum(IRandomAccessFile file, in Footer footer, BlockHandle handle, ulong globalSeqnoFileOffset)
    {
        if (handle.Size + BlockBasedTableConstants.BlockTrailerSize > int.MaxValue) ThrowHelper.Corruption("block too large: " + handle);
        int n = (int)(handle.Size + BlockBasedTableConstants.BlockTrailerSize);
        byte[] buf = System.Buffers.ArrayPool<byte>.Shared.Rent(n);
        try
        {
            Span<byte> block = buf.AsSpan(0, n);
            if (BlockFetcher.ReadFully(file, (long)handle.Offset, block) != n) ThrowHelper.Corruption("truncated block read at offset " + handle.Offset);
            if (BlockChecksum.Verify(footer.ChecksumType, footer.BaseContextChecksum, block, handle.Offset)) return;
            ulong blockSize = handle.Size;
            if (globalSeqnoFileOffset > handle.Offset && blockSize >= 8 && globalSeqnoFileOffset - handle.Offset <= blockSize - 8)
            {
                // Older ingestion could rewrite this value without updating the block checksum. Verify against the
                // original zero placeholder.
                Coding.EncodeFixed64(block[(int)(globalSeqnoFileOffset - handle.Offset)..], 0);
                if (BlockChecksum.Verify(footer.ChecksumType, footer.BaseContextChecksum, block, handle.Offset)) return;
            }
            ThrowHelper.Corruption("block checksum mismatch at offset " + handle.Offset + " size " + handle.Size + " (properties block)");
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(buf);
        }
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
            if (LookupCache(cache, key, kind, out CachedBlock h))
            {
                _stats?.Record(HitCounter(kind));
                PerfContext.Add(PerfMetric.BlockCacheHitCount, 1); // BlockBasedTable::UpdateCacheHitMetrics
                if (_bcTracer is { IsEnabled: true } t) TraceAccess(t, key, kind, Unsafe.As<Block>(h.Value), hit: true, flags);
                return BlockRef.FromCache(Unsafe.As<Block>(h.Value)!, h);
            }
            return ReadAndInsert(cache, key, handle, kind, flags);
        }
        return GetBlockRefLegacy(handle, kind, flags);
    }

    /// <summary>
    /// <see cref="GetBlockRef(BlockHandle, BlockKind, TableReadFlags)"/> reading a block that is not cached through
    /// <paramref name="prefetch"/> (sequential readahead, e.g. compaction_readahead_size for compaction inputs).
    /// </summary>
    internal BlockRef GetBlockRef(BlockHandle handle, BlockKind kind, TableReadFlags flags, AsyncPrefetchBuffer? prefetch)
    {
        if (prefetch is null) return GetBlockRef(handle, kind, flags);
        BlockCache? cache = _cache;
        CacheKey key = default;
        if (cache is not null)
        {
            key = _baseCacheKey.WithOffsetForMinSizeRecord(handle.Offset);
            if (LookupCache(cache, key, kind, out CachedBlock h))
            {
                _stats?.Record(HitCounter(kind));
                if (_bcTracer is { IsEnabled: true } t) TraceAccess(t, key, kind, Unsafe.As<Block>(h.Value), hit: true, flags);
                return BlockRef.FromCache(Unsafe.As<Block>(h.Value)!, h);
            }
        }
        else if (_legacyCache is not null)
        {
            return GetBlockRefLegacy(handle, kind, flags);
        }
        _stats?.Record(MissCounter(kind));
        // Charged per block consumed from the readahead buffer (upstream charges the prefetch reads themselves).
        ChargeRateLimiter(handle, flags);
        Block b = Protect(BlockFetcher.ReadBlock(prefetch, _footer, handle, _options.VerifyChecksums, DictFor(kind)), kind);
        if (cache is not null && _bcTracer is { IsEnabled: true } tr) TraceAccess(tr, key, kind, b, hit: false, flags);
        return cache is null || (flags & TableReadFlags.DontFillCache) != 0 ? BlockRef.FromOwned(b) : InsertIntoCache(cache, key, b, kind);
    }

    /// <summary>
    /// Port of RandomAccessFileReader::ChargeRateLimiterForRead: charges the bytes of a block read from the file
    /// (block + trailer) to the rate limiter at the priority in <paramref name="flags"/>, in chunks of at most the
    /// single burst size. Like upstream, no statistics are recorded for these read charges.
    /// </summary>
    private void ChargeRateLimiter(BlockHandle handle, TableReadFlags flags)
    {
        Env.IRateLimiter? limiter = _options.RateLimiter;
        if (limiter is null || (flags & TableReadFlags.RateLimiterPriorityMask) == 0) return;
        ChargeRateLimiterSlow(limiter, handle, flags.RateLimiterPriority());
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ChargeRateLimiterSlow(Env.IRateLimiter limiter, BlockHandle handle, Env.IoPriority priority)
    {
        if (!limiter.IsRateLimited(Env.RateLimiterOpType.Read)) return;
        long remaining = (long)handle.Size + BlockBasedTableConstants.BlockTrailerSize;
        while (remaining > 0)
        {
            long request = Math.Min(Math.Max(1, limiter.SingleBurstBytes), remaining);
            limiter.Request(request, priority, null, Env.RateLimiterOpType.Read);
            remaining -= request;
        }
    }

    /// <summary>Async <see cref="ChargeRateLimiter"/>.</summary>
    private ValueTask ChargeRateLimiterAsync(BlockHandle handle, TableReadFlags flags, CancellationToken ct)
    {
        Env.IRateLimiter? limiter = _options.RateLimiter;
        if (limiter is null || (flags & TableReadFlags.RateLimiterPriorityMask) == 0 || !limiter.IsRateLimited(Env.RateLimiterOpType.Read))
            return ValueTask.CompletedTask;
        return ChargeRateLimiterSlowAsync(limiter, handle, flags.RateLimiterPriority(), ct);
    }

    private static async ValueTask ChargeRateLimiterSlowAsync(Env.IRateLimiter limiter, BlockHandle handle, Env.IoPriority priority, CancellationToken ct)
    {
        long remaining = (long)handle.Size + BlockBasedTableConstants.BlockTrailerSize;
        while (remaining > 0)
        {
            long request = Math.Min(Math.Max(1, limiter.SingleBurstBytes), remaining);
            await limiter.RequestAsync(request, priority, null, Env.RateLimiterOpType.Read, ct).ConfigureAwait(false);
            remaining -= request;
        }
    }

    private BlockRef ReadAndInsert(BlockCache cache, in CacheKey key, BlockHandle handle, BlockKind kind, TableReadFlags flags)
    {
        _stats?.Record(MissCounter(kind));
        ChargeRateLimiter(handle, flags);
        if (cache.AdmitsCompressedBlocks && OffersCompressed(kind, flags)) return ReadAndInsertOfferingCompressed(cache, key, handle, kind, flags);
        Block b = Protect(ReadBlockFromFile(handle, kind), kind);
        if (_bcTracer is { IsEnabled: true } t) TraceAccess(t, key, kind, b, hit: false, flags);
        return (flags & TableReadFlags.DontFillCache) != 0 ? BlockRef.FromOwned(b) : InsertIntoCache(cache, key, b, kind);
    }

    private BlockRef GetBlockRefLegacy(BlockHandle handle, BlockKind kind, TableReadFlags flags)
    {
        IBlockCache? legacy = _legacyCache;
        if (legacy is null)
        {
            ChargeRateLimiter(handle, flags);
            return BlockRef.FromOwned(Protect(ReadBlockFromFile(handle, kind), kind));
        }
        var key = new BlockCacheKey(_tableId, handle.Offset);
        if (legacy.TryGet(key, out Block? b))
        {
            _stats?.Record(HitCounter(kind));
            return BlockRef.FromOwned(b);
        }
        _stats?.Record(MissCounter(kind));
        ChargeRateLimiter(handle, flags);
        b = Protect(ReadBlockFromFile(handle, kind), kind);
        if ((flags & TableReadFlags.DontFillCache) == 0) legacy.Insert(key, b);
        return BlockRef.FromOwned(b);
    }

    /// <summary>
    /// Whether a block read from the file is offered compressed to the cache's secondary tiers: secondary-cache compatible
    /// helpers (lowest_used_cache_tier), the cache is filled, and the block comes from the file rather than a persistent
    /// cache (whose compressed copy is not kept).
    /// </summary>
    private bool OffersCompressed(BlockKind kind, TableReadFlags flags) =>
        (flags & TableReadFlags.DontFillCache) == 0 && _persistentCache is null && _helpers[(int)kind].IsSecondaryCacheCompatible;

    /// <summary>
    /// MaybeReadBlockAndLoadToCache + PutDataBlockToCache with a cache using <see cref="TieredAdmissionPolicy.ThreeQueue"/>:
    /// the uncompressed block goes to the primary cache and the compressed payload (when the block is stored compressed)
    /// is passed along as Cache::Insert's compressed_value, which the adapter hands to the secondary tiers (InsertSaved).
    /// </summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private BlockRef ReadAndInsertOfferingCompressed(BlockCache cache, in CacheKey key, BlockHandle handle, BlockKind kind, TableReadFlags flags)
    {
        Block b = Protect(BlockFetcher.ReadBlockKeepingCompressed(_file, _footer, handle, _options.VerifyChecksums, DictFor(kind), out byte[]? serialized), kind);
        try
        {
            if (_bcTracer is { IsEnabled: true } t) TraceAccess(t, key, kind, b, hit: false, flags);
            return InsertIntoCache(cache, key, b, kind, serialized, (int)handle.Size);
        }
        finally
        {
            if (serialized is not null) Memory.BlockBufferPool.Return(serialized);
        }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<BlockRef> ReadAndInsertOfferingCompressedAsync(BlockCache cache, CacheKey key, BlockHandle handle, BlockKind kind,
        TableReadFlags flags, CancellationToken ct)
    {
        BlockFetcher.CompressedRead read = await BlockFetcher.ReadBlockKeepingCompressedAsync(_file, _footer, handle, _options.VerifyChecksums,
            DictFor(kind), ct).ConfigureAwait(false);
        try
        {
            Block b = Protect(read.Block, kind);
            if (_bcTracer is { IsEnabled: true } t) TraceAccess(t, key, kind, b, hit: false, flags);
            return InsertIntoCache(cache, key, b, kind, read.Serialized, (int)handle.Size);
        }
        finally
        {
            if (read.Serialized is not null) Memory.BlockBufferPool.Return(read.Serialized);
        }
    }

    // InsertIntoCache with the serialized block: its payload ([0, size), compression type at [size]) is the compressed value.
    private BlockRef InsertIntoCache(BlockCache cache, in CacheKey key, Block b, BlockKind kind, byte[]? serialized, int size)
    {
        if (serialized is null) return InsertIntoCache(cache, key, b, kind);
        CachePriority priority = kind == BlockKind.Data ? CachePriority.Low : _metaPriority;
        if (cache.TryInsert(key, CacheValue.FromObject(b).WithHelper(_helpers[(int)kind]), b.Charge, priority, serialized.AsSpan(0, size),
                (Compression.CompressionType)serialized[size], out CachedBlock h))
        {
            RecordAdd(_stats, kind, b.Charge);
            return BlockRef.FromCache(b, h);
        }
        _stats?.Record(BlockCacheCounter.AddFailure);
        return BlockRef.FromOwned(b);
    }

    private BlockRef InsertIntoCache(BlockCache cache, in CacheKey key, Block b, BlockKind kind)
    {
        CachePriority priority = kind == BlockKind.Data ? CachePriority.Low : _metaPriority;
        // On success the cache takes over our reference (released through Block's IDisposable when the entry is freed).
        if (cache.TryInsert(key, CacheValue.FromObject(b).WithHelper(_helpers[(int)kind]), b.Charge, priority, out CachedBlock h))
        {
            RecordAdd(_stats, kind, b.Charge);
            return BlockRef.FromCache(b, h);
        }
        // Strict capacity limit reached: we keep ownership and use the block uncached.
        _stats?.Record(BlockCacheCounter.AddFailure);
        return BlockRef.FromOwned(b);
    }

    /// <summary>
    /// BlockBasedTable::SaveLookupContextOrTraceRecord + FinishTraceRecord: records one block cache lookup while a block
    /// cache trace runs. The number of keys is estimated from the restart points as upstream.
    /// </summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private void TraceAccess(Tracing.BlockCacheTracer t, in CacheKey key, BlockKind kind, Block? block, bool hit, TableReadFlags flags, ulong sizeIfNoBlock = 0)
    {
        ulong nkeys = 0;
        ulong usage = sizeIfNoBlock;
        if (block is not null)
        {
            int interval = _options.BlockRestartInterval;
            nkeys = (ulong)interval * (ulong)block.NumRestarts;
            if (nkeys > 0) nkeys -= (ulong)(interval / 2); // ~ half-utilized last restart interval
            usage = (ulong)block.Charge;
        }
        Tracing.TraceType type = kind switch
        {
            BlockKind.Data => Tracing.TraceType.BlockDataBlock,
            BlockKind.Index => Tracing.TraceType.BlockIndexBlock,
            _ => Tracing.TraceType.BlockFilterBlock,
        };
        bool noInsert = (flags & (TableReadFlags.BlockCacheTier | TableReadFlags.DontFillCache)) != 0;
        ulong cfId = Properties.ColumnFamilyId;
        string cfName = Properties.ColumnFamilyName;
        t.TraceBlockAccess(key, type, usage, nkeys, hit, noInsert, cfId, cfName, _options.Level >= 0 ? (uint)_options.Level : uint.MaxValue,
            _options.FileNumber != 0 ? _options.FileNumber : ulong.MaxValue);
    }

    /// <summary>Async <see cref="GetBlockRef"/>; completes synchronously on a cache hit.</summary>
    internal ValueTask<BlockRef> GetBlockRefAsync(BlockHandle handle, BlockKind kind, TableReadFlags flags, CancellationToken ct)
    {
        BlockCache? cache = _cache;
        if (cache is not null)
        {
            CacheKey key = _baseCacheKey.WithOffsetForMinSizeRecord(handle.Offset);
            if (cache.ExtendedLookup) return GetBlockRefExtendedAsync(cache, key, handle, kind, flags, ct);
            if (cache.TryLookup(key, out CachedBlock h))
            {
                _stats?.Record(HitCounter(kind));
                if (_bcTracer is { IsEnabled: true } t) TraceAccess(t, key, kind, Unsafe.As<Block>(h.Value), hit: true, flags);
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
        return ReadAndInsertLegacyAsync(handle, kind, flags, ct);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<BlockRef> ReadAndInsertAsync(BlockCache cache, CacheKey key, BlockHandle handle, BlockKind kind, TableReadFlags flags, CancellationToken ct)
    {
        await ChargeRateLimiterAsync(handle, flags, ct).ConfigureAwait(false);
        if (cache.AdmitsCompressedBlocks && OffersCompressed(kind, flags))
            return await ReadAndInsertOfferingCompressedAsync(cache, key, handle, kind, flags, ct).ConfigureAwait(false);
        Block b = Protect(_persistentCache is { } pc ? await ReadBlockThroughPersistentCacheAsync(pc, handle, kind, ct).ConfigureAwait(false)
            : await BlockFetcher.ReadBlockAsync(_file, _footer, handle, _options.VerifyChecksums, DictFor(kind), ct).ConfigureAwait(false), kind);
        if (_bcTracer is { IsEnabled: true } t) TraceAccess(t, key, kind, b, hit: false, flags);
        return (flags & TableReadFlags.DontFillCache) != 0 ? BlockRef.FromOwned(b) : InsertIntoCache(cache, key, b, kind);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<BlockRef> ReadAndInsertLegacyAsync(BlockHandle handle, BlockKind kind, TableReadFlags flags, CancellationToken ct)
    {
        await ChargeRateLimiterAsync(handle, flags, ct).ConfigureAwait(false);
        Block b = Protect(_persistentCache is { } pc ? await ReadBlockThroughPersistentCacheAsync(pc, handle, kind, ct).ConfigureAwait(false)
            : await BlockFetcher.ReadBlockAsync(_file, _footer, handle, _options.VerifyChecksums, DictFor(kind), ct).ConfigureAwait(false), kind);
        if (_legacyCache is { } legacy && (flags & TableReadFlags.DontFillCache) == 0) legacy.Insert(new BlockCacheKey(_tableId, handle.Offset), b);
        return BlockRef.FromOwned(b);
    }

    /// <summary>
    /// block_protection_bytes_per_key: requests per key-value checksums for a key/value block (data, index, metaindex,
    /// range deletion) just loaded (Block::Initialize*ProtectionInfo in BlockCreateContext::Create). Filter blocks are
    /// not key/value blocks and stay unprotected.
    /// </summary>
    private Block Protect(Block b, BlockKind kind)
    {
        if (_options.BlockProtectionBytesPerKey != 0 && kind != BlockKind.Filter) b.RequestKvProtection(_options.BlockProtectionBytesPerKey);
        return b;
    }

    private static Block Protect(Block b, TableReaderOptions options)
    {
        if (options.BlockProtectionBytesPerKey != 0) b.RequestKvProtection(options.BlockProtectionBytesPerKey);
        return b;
    }

    /// <summary>The top-level index block: pinned by the reader, or looked up in (and if evicted re-read into) the block cache.</summary>
    /// <summary>The options the reader was opened with.</summary>
    internal TableReaderOptions Options => _options;

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

    /// <summary>
    /// Initializes an index iterator over <paramref name="block"/> (the top-level index block, or an index partition when
    /// <paramref name="isPartition"/>). Port of BlockBasedTable::InitBlockIterator&lt;IndexBlockIter&gt; /
    /// BinarySearchIndexReader::NewIterator / PartitionIndexReader::NewIterator: index_block_search_type applies to
    /// binary-search index blocks and index partitions, while partitioned-index top levels and hash indexes use kBinary.
    /// </summary>
    internal void InitIndexIter(BlockIter it, Block block, bool isPartition)
    {
        it.InitializeIndex(block, _icmp, IndexKeyIncludesSeq, IndexValueIsFull, IndexHasFirstKey,
            BlockBasedTableConstants.FormatVersionUsesValueDeltaEscape(_footer.FormatVersion), GlobalSequenceNumber, PadTimestampSize);
        IndexBlockSearchType searchType = _options.IndexBlockSearchType;
        if (searchType == IndexBlockSearchType.Binary) return;
        if (!isPartition && (IsPartitionedIndex || IndexType == IndexType.HashSearch)) return;
        // Block::NewIndexIterator: kAuto resolves to interpolation search only for blocks marked "is_uniform" with the
        // bytewise comparator (kInterpolation itself is rejected for other comparators by option validation).
        bool interpolation = searchType == IndexBlockSearchType.Interpolation
            ? _icmp.IsBytewise
            : block.IsUniform && _icmp.IsBytewise;
        if (interpolation) it.UseInterpolationSearch();
    }

    /// <summary>Iterator over the top-level filter partition index (NewFilterPartitionIndexIterator: no global seqno, no first key).</summary>
    private void InitFilterIndexIter(BlockIter it, Block block) =>
        it.InitializeIndex(block, _icmp, IndexKeyIncludesSeq, IndexValueIsFull, false,
            BlockBasedTableConstants.FormatVersionUsesValueDeltaEscape(_footer.FormatVersion), BlockIter.DisableGlobalSequenceNumber, PadTimestampSize);

    /// <summary>The pinned top-level index block (not available when <see cref="IndexAndFilterInCache"/>).</summary>
    internal Block IndexBlock => _indexBlock ?? throw new InvalidOperationException("The index block is held by the block cache (CacheIndexAndFilterBlocks).");

    /// <summary>The buffer of the pinned full filter (null when absent or held by the block cache). Tests only.</summary>
    internal byte[]? PinnedFilterBuffer => Volatile.Read(ref _filterBuffer);
    internal bool IsPartitionedIndex => IndexType == IndexType.TwoLevelIndexSearch;

    // ---------------------------------------------------------------------------------------------
    // Point lookups

    /// <summary>
    /// Port of BlockBasedTable::ApproximateOffsetOf: approximate file offset of the data for <paramref name="internalKey"/>
    /// (the offset of the data block the index points to, pro-rated over the whole file to account for metadata).
    /// </summary>
    public override ulong ApproximateOffsetOf(ReadOnlySpan<byte> internalKey)
    {
        ulong dataSize = Properties.DataSize;
        // Hmm. Let's just split in half to avoid skewing one way or another, since we don't know whether we're
        // operating on lower bound or upper bound.
        if (dataSize == 0) return (ulong)FileSize / 2;
        ulong offset = ApproximateDataOffsetOf(internalKey, dataSize);
        // Pro-rate file metadata (incl filters) size-proportionally across data blocks.
        double sizeRatio = (double)offset / dataSize;
        return (ulong)(sizeRatio * FileSize);
    }

    /// <summary>Port of BlockBasedTable::ApproximateSize: approximate bytes of the data between two internal keys.</summary>
    public override ulong ApproximateSize(ReadOnlySpan<byte> startInternalKey, ReadOnlySpan<byte> endInternalKey)
    {
        ulong dataSize = Properties.DataSize;
        // Hmm. Assume whole file is involved, since we have lower and upper bound.
        if (dataSize == 0) return (ulong)FileSize;
        ulong startOffset = ApproximateDataOffsetOf(startInternalKey, dataSize);
        ulong endOffset = ApproximateDataOffsetOf(endInternalKey, dataSize);
        double sizeRatio = (double)(endOffset - Math.Min(startOffset, endOffset)) / dataSize;
        return (ulong)(sizeRatio * FileSize);
    }

    /// <summary>
    /// Port of BlockBasedTable::ApproximateKeyAnchors: walks the index and returns up to ~128 anchors (user key, bytes of
    /// the data blocks since the previous anchor) that partition the file evenly; used to plan subcompactions.
    /// </summary>
    public override List<(byte[] UserKey, ulong RangeSize)> ApproximateKeyAnchors()
    {
        var anchors = new List<(byte[], ulong)>();
        // If needed the threshold could be more adaptive (e.g. based on size).
        const ulong MaxNumAnchors = 128;
        ulong numBlocksPerAnchor = Math.Max(1UL, Properties.NumDataBlocks / MaxNumAnchors);
        ulong count = 0, rangeSize = 0, prevOffset = 0;
        byte[] lastKey = [];
        IndexCursor cursor = IndexCursor.Rent(this, TableReadFlags.DontFillCache);
        try
        {
            for (cursor.SeekToFirst(); cursor.Valid; cursor.Next())
            {
                BlockHandle bh = cursor.Handle;
                rangeSize += bh.Offset + bh.Size - prevOffset;
                prevOffset = bh.Offset + bh.Size;
                ReadOnlySpan<byte> userKey = IndexKeyIncludesSeq ? DbFormat.ExtractUserKey(cursor.Key) : cursor.Key;
                if (++count % numBlocksPerAnchor == 0)
                {
                    count = 0;
                    anchors.Add((userKey.ToArray(), rangeSize));
                    rangeSize = 0;
                }
                else
                {
                    lastKey = userKey.ToArray();
                }
            }
        }
        finally
        {
            cursor.Return();
        }
        if (count != 0) anchors.Add((lastKey, rangeSize));
        return anchors;
    }

    /// <summary>ApproximateDataOffsetOf: the offset of the data block holding the key, or the data size past the end.</summary>
    /// <summary>
    /// The file span of the data blocks holding [<paramref name="startInternalKey"/>, <paramref name="endInternalKey"/>]
    /// (null end = to the last data block), found through the index (the block-handle lookup of
    /// BlockBasedTableIterator::Prepare for MultiScan). False if the range starts after the last block.
    /// </summary>
    internal bool TryGetDataBlockSpan(ReadOnlySpan<byte> startInternalKey, ReadOnlySpan<byte> endInternalKey, bool hasEnd, out long offset, out long length)
    {
        offset = length = 0;
        IndexCursor cursor = IndexCursor.Rent(this, TableReadFlags.DontFillCache);
        try
        {
            cursor.Seek(startInternalKey);
            if (!cursor.Valid) return false;
            offset = (long)cursor.Handle.Offset;
            long end = (long)Properties.DataSize;
            if (hasEnd)
            {
                cursor.Seek(endInternalKey);
                if (cursor.Valid) end = (long)(cursor.Handle.Offset + cursor.Handle.Size) + BlockBasedTableConstants.BlockTrailerSize;
            }
            length = Math.Max(0, end - offset);
            return length > 0;
        }
        finally
        {
            cursor.Return();
        }
    }

    private ulong ApproximateDataOffsetOf(ReadOnlySpan<byte> internalKey, ulong dataSize)
    {
        IndexCursor cursor = IndexCursor.Rent(this, TableReadFlags.DontFillCache);
        try
        {
            cursor.Seek(internalKey);
            return cursor.Valid ? cursor.Handle.Offset : dataSize;
        }
        finally
        {
            cursor.Return();
        }
    }

    /// <summary>
    /// Whether the filter (if any) says <paramref name="userKey"/> may be present (looked up as of the latest sequence
    /// number). See <see cref="InternalKeyMayMatch(ReadOnlySpan{byte}, TableReadFlags)"/>.
    /// </summary>
    public bool KeyMayMatch(ReadOnlySpan<byte> userKey) => KeyMayMatch(userKey, TableReadFlags.None);

    /// <inheritdoc cref="KeyMayMatch(ReadOnlySpan{byte})"/>
    [SkipLocalsInit]
    public bool KeyMayMatch(ReadOnlySpan<byte> userKey, TableReadFlags flags)
    {
        if (_filterKind == FilterKind.None) return true;
        int n = userKey.Length + DbFormat.InternalKeyFooterSize;
        byte[]? rented = null;
        Span<byte> ikey = n <= 256 ? stackalloc byte[256] : (rented = ArrayPool<byte>.Shared.Rent(n));
        try
        {
            DbFormat.AppendInternalKey(ikey, userKey, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
            return InternalKeyMayMatch(ikey[..n], flags);
        }
        finally
        {
            if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// Port of BlockBasedTable::FullFilterKeyMayMatch: with whole_key_filtering, whether the user key of
    /// <paramref name="internalKey"/> may be present; otherwise, when the configured prefix extractor matches the
    /// table's, whether its prefix may be present. The internal key selects the partition of a partitioned filter.
    /// </summary>
    public override bool InternalKeyMayMatch(ReadOnlySpan<byte> internalKey, TableReadFlags flags)
    {
        if (_filterKind == FilterKind.None) return true;
        // FullFilterKeyMayMatch: filters hold user keys without the timestamp.
        ReadOnlySpan<byte> userKey = DbFormat.ExtractUserKeyAndStripTimestamp(internalKey, _tsSize);
        if (WholeKeyFiltering)
        {
            bool mayMatch = FilterMayMatch(userKey, internalKey, flags);
            _stats?.Record(mayMatch ? BlockCacheCounter.BloomFilterFullPositive : BlockCacheCounter.BloomFilterUseful);
            PerfContext.Add(mayMatch ? PerfMetric.BloomSstHitCount : PerfMetric.BloomSstMissCount, 1);
            return mayMatch;
        }
        SliceTransform? pe = _options.PrefixExtractor;
        if (!_prefixExtractorChanged && pe!.InDomain(userKey))
        {
            // FIXME (upstream): there should be no reason for Get() to depend on current prefix_extractor at all.
            bool mayMatch = FilterMayMatch(pe.Transform(userKey), internalKey, flags);
            if (_stats is { } s)
            {
                s.Record(BlockCacheCounter.BloomFilterPrefixChecked);
                if (!mayMatch) s.Record(BlockCacheCounter.BloomFilterPrefixUseful);
            }
            return mayMatch;
        }
        return true;
    }

    /// <summary>
    /// Port of BlockBasedTable::PrefixRangeMayMatch + FilterBlockReaderCommon::RangeMayExist / IsFilterCompatible:
    /// whether keys with the prefix of the seek key <paramref name="internalKey"/> may exist in the table. With
    /// <see cref="TableIteratorPrefixOptions.NeedUpperBoundCheck"/>, the filter is only consulted when every key in
    /// [seek key, upper bound) has the seek key's prefix.
    /// </summary>
    internal bool PrefixRangeMayMatch(ReadOnlySpan<byte> internalKey, in TableIteratorPrefixOptions o, TableReadFlags flags)
    {
        if (_filterKind == FilterKind.None) return true;
        SliceTransform? prefixExtractor;
        if (_tablePrefixExtractor is null)
        {
            if (o.NeedUpperBoundCheck) return true;
            prefixExtractor = o.PrefixExtractor;
        }
        else
        {
            prefixExtractor = _tablePrefixExtractor;
        }
        ReadOnlySpan<byte> userKey = DbFormat.ExtractUserKeyAndStripTimestamp(internalKey, _tsSize);
        if (prefixExtractor is null || !prefixExtractor.InDomain(userKey)) return true;
        ReadOnlySpan<byte> prefix = prefixExtractor.Transform(userKey);
        if (o.NeedUpperBoundCheck && !IsFilterCompatible(o.UpperBound, prefix)) return true;
        bool mayMatch = FilterMayMatch(prefix, internalKey, flags);
        if (!mayMatch) _stats?.Record(BlockCacheCounter.BloomFilterPrefixUseful);
        return mayMatch;
    }

    /// <summary>
    /// Port of FilterBlockReaderCommon::IsFilterCompatible: the table's prefix filter can serve a seek whose prefix
    /// differs from the configured one if [seek key, upper bound) all share <paramref name="prefix"/>.
    /// </summary>
    private bool IsFilterCompatible(ReadOnlyMemory<byte>? iterateUpperBound, ReadOnlySpan<byte> prefix)
    {
        SliceTransform? pe = _tablePrefixExtractor;
        if (iterateUpperBound is not ReadOnlyMemory<byte> ubm || pe is null) return false;
        ReadOnlySpan<byte> upperBound = ubm.Span;
        if (!pe.InDomain(upperBound)) return false;
        ReadOnlySpan<byte> upperBoundXform = pe.Transform(upperBound);
        Comparator ucmp = _icmp.UserComparator;
        // first check if user_key and upper_bound all share the same prefix
        if (ucmp.Compare(prefix, upperBoundXform) != 0)
        {
            // second check if user_key's prefix is the immediate predecessor of upper_bound and have the same length. If
            // so, we know for sure all keys in the range [user_key, upper_bound) share the same prefix. Also need to make
            // sure upper_bound are full length to ensure correctness
            if (!pe.FullLengthEnabled(out int fullLength) || upperBound.Length != fullLength || !ucmp.IsSameLengthImmediateSuccessor(prefix, upperBound))
                return false;
        }
        return true;
    }

    internal void RecordSeekFiltered() => _stats?.Record(BlockCacheCounter.SeekFiltered);

    /// <summary>MayMatch of the full filter, or of the partition of a partitioned filter covering <paramref name="internalKey"/>.</summary>
    private bool FilterMayMatch(ReadOnlySpan<byte> entry, ReadOnlySpan<byte> internalKey, TableReadFlags flags)
    {
        if (_filterKind == FilterKind.Full)
        {
            IFilterBitsReader? f = _filter;
            return f is not null ? f.MayMatch(entry) : FilterBlockMayMatch(_filterHandle, entry, flags);
        }
        return PartitionedFilterMayMatch(entry, internalKey, flags);
    }

    /// <summary>
    /// Port of PartitionedFilterBlockReader::MayMatch: seeks the top-level partition index to the partition covering
    /// <paramref name="internalKey"/> (the last one if the key is past all partitions) and queries it.
    /// </summary>
    private bool PartitionedFilterMayMatch(ReadOnlySpan<byte> entry, ReadOnlySpan<byte> internalKey, TableReadFlags flags)
    {
        // kBlockCacheTier: the partition index would have to be read (conservatively "may match").
        if (_filterIndexBlock is null && (flags & TableReadFlags.BlockCacheTier) != 0) return true;
        BlockRef top = _filterIndexBlock is not null ? BlockRef.FromPinned(_filterIndexBlock) : GetBlockRef(_filterHandle, BlockKind.Filter, flags);
        BlockHandle partition;
        BlockIter it = IterPool.Rent();
        try
        {
            InitFilterIndexIter(it, top.Block!);
            it.Seek(internalKey);
            if (!it.Valid)
            {
                // entry is larger than all the keys. However its prefix might still be present in the last partition.
                it.SeekToLast();
            }
            if (!it.Valid) return true;
            partition = it.BlockHandle;
        }
        finally
        {
            it.Clear();
            IterPool.Return(it);
            top.Release();
        }
        if (partition.Size == 0) return false; // key is out of range
        if (_pinnedFilterPartitions is { } pinned && pinned.TryGet(partition.Offset, out IFilterBitsReader? r)) return r.MayMatch(entry);
        return FilterBlockMayMatch(partition, entry, flags);
    }

    /// <summary>
    /// Filter lookup through the block cache (cache_index_and_filter_blocks, filter partitions). A miss is read
    /// synchronously; without a block cache the filter is read and discarded.
    /// </summary>
    private bool FilterBlockMayMatch(BlockHandle handle, ReadOnlySpan<byte> entry, TableReadFlags flags)
    {
        BlockCache? cache = _cache;
        CacheKey key = default;
        if (cache is not null)
        {
            key = _baseCacheKey.WithOffsetForMinSizeRecord(handle.Offset);
            if (LookupFilterCache(cache, key, out CachedBlock h))
            {
                _stats?.Record(BlockCacheCounter.FilterHit);
                if (_bcTracer is { IsEnabled: true } t) TraceAccess(t, key, BlockKind.Filter, null, hit: true, flags, (ulong)handle.Size);
                bool r = Unsafe.As<IFilterBitsReader>(h.Value)!.MayMatch(entry);
                h.Dispose();
                return r;
            }
        }
        _stats?.Record(BlockCacheCounter.FilterMiss);
        if ((flags & TableReadFlags.BlockCacheTier) != 0) return true; // no I/O allowed: Incomplete -> may match
        byte[] buf = BlockFetcher.ReadRawPayload(_file, _footer, handle, _options.VerifyChecksums, out int len);
        if (cache is not null && _bcTracer is { IsEnabled: true } tm) TraceAccess(tm, key, BlockKind.Filter, null, hit: false, flags, (ulong)len);
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
        bool result = reader.MayMatch(entry);
        if (cache is null || (flags & TableReadFlags.DontFillCache) != 0)
        {
            ArrayPool<byte>.Shared.Return(buf);
        }
        else if (cache.Insert(key, new CacheValue(buf, len, reader, true).WithHelper(FilterHelperFor(_options)), buf.Length, _metaPriority))
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
    public override void Get<TState>(ReadOnlySpan<byte> internalKey, ref TState state, TableEntryVisitor<TState> visitor, TableReadFlags flags)
    {
        ReadOnlySpan<byte> userKey = DbFormat.ExtractUserKey(internalKey);
        if (!InternalKeyMayMatch(internalKey, flags)) return;
        IndexCursor idx = IndexCursor.Rent(this, flags);
        BlockIter data = IterPool.Rent();
        try
        {
            idx.SeekForGet(internalKey);
            bool first = true;
            while (idx.Valid)
            {
                BlockRef block = GetBlockRef(idx.Handle, BlockKind.Data, flags);
                try
                {
                    data.InitializeData(block.Block!, _icmp, GlobalSequenceNumber, PadTimestampSize);
                    if (first)
                    {
                        if (!SeekForGet(data, internalKey)) return;
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
    public override TableGetResult Get(ReadOnlySpan<byte> internalKey, IBufferWriter<byte>? value, TableReadFlags flags)
    {
        if (!InternalKeyMayMatch(internalKey, flags)) return default;
        IndexCursor idx = IndexCursor.Rent(this, flags);
        BlockIter data = IterPool.Rent();
        try
        {
            idx.SeekForGet(internalKey);
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

    /// <summary>Async <see cref="Get(ReadOnlySpan{byte}, IBufferWriter{byte}?)"/>: data blocks and index partitions are read asynchronously; completes synchronously on cache hits.</summary>
    public ValueTask<TableGetResult> GetAsync(ReadOnlyMemory<byte> internalKey, IBufferWriter<byte>? value, CancellationToken cancellationToken = default) =>
        GetAsync(internalKey, value, TableReadFlags.None, cancellationToken);

    /// <inheritdoc cref="GetAsync(ReadOnlyMemory{byte}, IBufferWriter{byte}?, CancellationToken)"/>
    public override ValueTask<TableGetResult> GetAsync(ReadOnlyMemory<byte> internalKey, IBufferWriter<byte>? value, TableReadFlags flags, CancellationToken cancellationToken)
    {
        // A filter miss (the common case of a lookup in a table without the key) completes without an async state machine.
        if (!InternalKeyMayMatch(internalKey.Span, flags)) return default;
        return GetAfterFilterAsync(internalKey, value, flags, cancellationToken);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<TableGetResult> GetAfterFilterAsync(ReadOnlyMemory<byte> internalKey, IBufferWriter<byte>? value, TableReadFlags flags,
        CancellationToken cancellationToken)
    {
        IndexCursor idx = IndexCursor.Rent(this, flags);
        BlockIter data = IterPool.Rent();
        try
        {
            // Index partitions that are not cached are read with async I/O too.
            ValueTask seek = idx.SeekForGetAsync(internalKey, cancellationToken);
            if (!seek.IsCompletedSuccessfully) await seek.ConfigureAwait(false);
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
                ValueTask next = idx.NextAsync(cancellationToken);
                if (!next.IsCompletedSuccessfully) await next.ConfigureAwait(false);
            }
            return default;
        }
        finally
        {
            IterPool.Return(data);
            idx.Return();
        }
    }

    /// <summary>
    /// Async <see cref="Get{TState}(ReadOnlySpan{byte}, ref TState, TableEntryVisitor{TState}, TableReadFlags)"/>: the
    /// visitor sees every entry of the lookup key's user key in order (for user-defined timestamps, every version)
    /// while data blocks are read with async I/O; completes synchronously on cache hits. <paramref name="state"/> is a
    /// reference type, so the visitor's updates survive the awaits.
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public override async ValueTask GetAsync<TState>(ReadOnlyMemory<byte> internalKey, TState state, TableEntryVisitor<TState> visitor, TableReadFlags flags,
        CancellationToken cancellationToken)
    {
        if (!InternalKeyMayMatch(internalKey.Span, flags)) return;
        IndexCursor idx = IndexCursor.Rent(this, flags);
        BlockIter data = IterPool.Rent();
        try
        {
            ValueTask seek = idx.SeekForGetAsync(internalKey, cancellationToken);
            if (!seek.IsCompletedSuccessfully) await seek.ConfigureAwait(false);
            bool first = true;
            while (idx.Valid)
            {
                ValueTask<BlockRef> pending = GetBlockRefAsync(idx.Handle, BlockKind.Data, flags, cancellationToken);
                BlockRef block = pending.IsCompletedSuccessfully ? pending.Result : await pending.ConfigureAwait(false);
                try
                {
                    if (!VisitBlockForGet(data, block.Block!, internalKey.Span, first, state, visitor)) return;
                    first = false;
                }
                finally
                {
                    data.Clear();
                    block.Release();
                }
                ValueTask next = idx.NextAsync(cancellationToken);
                if (!next.IsCompletedSuccessfully) await next.ConfigureAwait(false);
            }
        }
        finally
        {
            IterPool.Return(data);
            idx.Return();
        }
    }

    /// <summary>The per-block part of the visitor Get; returns false when the lookup is done.</summary>
    private bool VisitBlockForGet<TState>(BlockIter data, Block block, ReadOnlySpan<byte> internalKey, bool first, TState state,
        TableEntryVisitor<TState> visitor)
    {
        ReadOnlySpan<byte> userKey = DbFormat.ExtractUserKey(internalKey);
        data.InitializeData(block, _icmp, GlobalSequenceNumber, PadTimestampSize);
        if (first)
        {
            if (!SeekForGet(data, internalKey)) return false;
        }
        else
        {
            data.SeekToFirst();
        }
        for (; data.Valid; data.Next())
        {
            ReadOnlySpan<byte> k = data.Key;
            if (!UserKeyEquals(DbFormat.ExtractUserKey(k), userKey)) return false;
            if (!visitor(ref state, k, data.Value)) return false;
        }
        return true;
    }

    /// <summary>Returns 1 = found, -1 = definitely absent, 0 = continue with the next block.</summary>
    private int ScanBlockForGet(BlockIter data, Block block, ReadOnlySpan<byte> internalKey, bool first, IBufferWriter<byte>? value, out TableGetResult result)
    {
        result = default;
        data.InitializeData(block, _icmp, GlobalSequenceNumber, PadTimestampSize);
        if (first)
        {
            if (!SeekForGet(data, internalKey)) return -1;
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

    /// <summary>
    /// User keys of the same key (GetContext::SaveValue): with user-defined timestamps, versions with different
    /// timestamps belong to the same key (EqualWithoutTimestamp).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool UserKeyEquals(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) =>
        _tsSize == 0 ? _icmp.EqualUserKey(a, b) : _icmp.UserComparator.EqualWithoutTimestamp(a, b);

    /// <summary>
    /// DataBlockIter::SeekForGet. With user-defined timestamps the hash index (keyed by the user key with its timestamp)
    /// cannot find older versions, so a plain binary-search Seek is used (upstream does not end the search on a hash
    /// miss in that case).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool SeekForGet(BlockIter data, ReadOnlySpan<byte> internalKey)
    {
        if (_tsSize == 0) return data.SeekForGet(internalKey);
        data.Seek(internalKey);
        return true;
    }

    // ---------------------------------------------------------------------------------------------
    // Iterators

    /// <summary>Creates a two-level (index → data) iterator over the table's point entries. Dispose when done.</summary>
    public BlockBasedTableIterator NewIterator() => new(this, TableReadFlags.None, default);

    /// <summary>Creates an iterator with explicit read flags (e.g. <see cref="TableReadFlags.DontFillCache"/> for scans/compaction).</summary>
    public BlockBasedTableIterator NewIterator(TableReadFlags flags) => new(this, flags, default);

    /// <summary>
    /// Creates an iterator that honours prefix seek (port of BlockBasedTable::NewIterator with a prefix extractor):
    /// with <see cref="TableIteratorPrefixOptions.CheckFilter"/>, Seek/SeekForPrev to a key whose prefix the filter
    /// excludes leaves the iterator invalid without reading data blocks.
    /// </summary>
    public BlockBasedTableIterator NewIterator(TableReadFlags flags, in TableIteratorPrefixOptions prefixOptions) => new(this, flags, prefixOptions);

    /// <inheritdoc/>
    public override Db.Iterators.InternalIterator NewInternalIterator(TableReadFlags flags, ReadOptions? readOptions, SliceTransform? prefixExtractor) =>
        new Db.Iterators.TableInternalIterator(new TableReaderLease(this, null, null), flags, readOptions, prefixExtractor);

    /// <summary>
    /// Prefix seek settings for an iterator over this table (port of the need_upper_bound_check / check_filter
    /// computation in BlockBasedTable::NewIterator).
    /// </summary>
    public TableIteratorPrefixOptions GetPrefixOptions(ReadOptions? readOptions, SliceTransform? prefixExtractor)
    {
        if (prefixExtractor is null || _filterKind == FilterKind.None) return default;
        bool autoPrefixMode = readOptions?.AutoPrefixMode == true;
        bool totalOrderSeek = readOptions?.TotalOrderSeek == true;
        bool prefixSameAsStart = readOptions?.PrefixSameAsStart == true;
        return new TableIteratorPrefixOptions
        {
            PrefixExtractor = prefixExtractor,
            NeedUpperBoundCheck = autoPrefixMode || PrefixExtractorChanged(prefixExtractor),
            CheckFilter = !totalOrderSeek || autoPrefixMode || prefixSameAsStart,
            UpperBound = readOptions?.IterateUpperBound,
        };
    }

    public override bool HasRangeTombstones => _rangeDelBlock is not null;

    /// <summary>
    /// Iterator over the range tombstones (internal start key → exclusive end user key), or null if none.
    /// The returned iterator is valid while the reader is alive; dispose it when done.
    /// </summary>
    public override BlockIter? NewRangeTombstoneIterator()
    {
        if (_rangeDelBlock is null) return null;
        var it = new BlockIter();
        it.InitializeData(_rangeDelBlock, _icmp, GlobalSequenceNumber, PadTimestampSize);
        return it;
    }

    /// <summary>
    /// Releases the reader's pinned blocks and the file. Entries this file put into a shared block cache stay there
    /// until evicted (keys are not reused by other files); erase them explicitly when the file is deleted if desired.
    /// </summary>
    public override void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        _indexBlock?.Release();
        _rangeDelBlock?.Release();
        _filterIndexBlock?.Release();
        _pinnedFilterPartitions?.Dispose();
        _compressionDict?.Dispose();
        _udiReader?.Dispose();


        byte[]? fb = Interlocked.Exchange(ref _filterBuffer, null);
        // The pinned full filter (never set when the filter is held by the block cache) is rented from the table buffers.
        if (fb is not null) TableBuilderBuffers.Bytes.Return(fb);
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

        public bool Prefetch(long offset, long length) => _inner.Prefetch(offset, length);

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
