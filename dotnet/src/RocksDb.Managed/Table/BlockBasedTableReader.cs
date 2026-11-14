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
    /// cache_index_and_filter_blocks: with a <see cref="Cache.BlockCache"/>, the (top-level) index block, the full
    /// filter (or the top-level index of a partitioned filter) and the compression dictionary are block cache entries
    /// instead of memory owned by the reader. Each is either pinned (the reader holds the cache entry for its
    /// lifetime and uses it without a lookup) or not (looked up in the block cache on each use and re-read after
    /// eviction), as decided by <see cref="UnpartitionedPinning"/> / <see cref="TopLevelIndexPinning"/> and their
    /// fallbacks. Ignored without a <see cref="Cache.BlockCache"/> (as upstream, where the table reader then owns them).
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
    /// pin_top_level_index_and_filter: the fallback of <see cref="TopLevelIndexPinning"/> (kAll when set, kNone
    /// otherwise). Default true.
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

    /// <summary>
    /// pin_l0_filter_and_index_blocks_in_cache: the fallback of <see cref="UnpartitionedPinning"/> and
    /// <see cref="PartitionPinning"/> (kFlushedAndSimilar when set, kNone otherwise; BlockBasedTable::PrefetchIndexAndFilterBlocks).
    /// </summary>
    public bool PinL0FilterAndIndexBlocksInCache { get; init; }
    /// <summary>
    /// metadata_cache_options.unpartitioned_pinning: which tables keep their unpartitioned metadata blocks (the index
    /// of a binary-search / hash index, the full filter, the compression dictionary) pinned while those live in the
    /// block cache (<see cref="CacheIndexAndFilterBlocks"/>). A pinned block is looked up once at open and held for
    /// the reader's lifetime; an unpinned one is looked up in the block cache on each use (the index per Get / Seek,
    /// the filter per filter probe, the dictionary per data block retrieval) and re-read after eviction.
    /// </summary>
    public PinningTier UnpartitionedPinning { get; init; } = PinningTier.Fallback;
    /// <summary>
    /// metadata_cache_options.top_level_index_pinning: which tables keep the top-level block of a partitioned index
    /// or partitioned filter pinned while it lives in the block cache (<see cref="CacheIndexAndFilterBlocks"/>).
    /// kFallback follows <see cref="PinTopLevelIndexAndFilter"/>.
    /// </summary>
    public PinningTier TopLevelIndexPinning { get; init; } = PinningTier.Fallback;
    /// <summary>
    /// metadata_cache_options.partition_pinning: which tables keep their index and filter partitions pinned. The
    /// partitions always live in the block cache (whatever <see cref="CacheIndexAndFilterBlocks"/> says); pinned ones
    /// are held for the reader's lifetime and used without a lookup. kFallback follows
    /// <see cref="PinL0FilterAndIndexBlocksInCache"/>.
    /// </summary>
    public PinningTier PartitionPinning { get; init; } = PinningTier.Fallback;
    /// <summary>
    /// prefetch_index_and_filter_in_cache of BlockBasedTable::Open (prefetch_all): at open, the metadata blocks that
    /// live in the block cache are looked up there and read and inserted on a miss, and the index / filter partitions
    /// are loaded into the block cache. When false only the pinned ones are, except for a level-0 table, which always
    /// prefetches (prefetch_all = prefetch_index_and_filter_in_cache || level == 0). Default true.
    /// </summary>
    public bool PrefetchIndexAndFilterInCache { get; init; } = true;
    /// <summary>
    /// TableReaderOptions::max_file_size_for_l0_meta_pin (MaxFileSizeForL0MetaPin: 1.5 times write_buffer_size): the
    /// largest level-0 file <see cref="PinningTier.FlushedAndSimilar"/> pins. Default 0, as upstream.
    /// </summary>
    public long MaxFileSizeForL0MetaPin { get; init; }

    /// <summary>The DB's block cache tracer (BlockBasedTable::block_cache_tracer_), or null.</summary>
    internal Tracing.BlockCacheTracer? BlockCacheTracer { get; init; }

    /// <summary>
    /// LSM level of the file when it was opened (TableReaderOptions::level, -1 unknown): block cache tracing and
    /// BlockBasedTable::IsLastLevel (the {NON_,}LAST_LEVEL_SEEK_* tickers).
    /// </summary>
    internal int Level { get; init; } = -1;

    /// <summary>ImmutableOptions::num_levels of the column family (-1 unknown): with <see cref="Level"/>, IsLastLevel.</summary>
    internal int NumLevels { get; init; } = -1;

    /// <summary>
    /// read_amp_bytes_per_bit: data blocks read from the file track the bytes read (READ_AMP_* tickers of the DB
    /// statistics the <see cref="Statistics"/> belong to). 0 (default): off.
    /// </summary>
    public uint ReadAmpBytesPerBit { get; init; }

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

    /// <summary>True for a block cache entry (CachableEntry::IsCached); false for an owned or reader-pinned block.</summary>
    public readonly bool IsCacheHandle => _handle.HasValue;

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
/// owned by the reader for its lifetime (the index, filter and compression dictionary go to the block cache instead
/// with <see cref="TableReaderOptions.CacheIndexAndFilterBlocks"/>, pinned there or not: see
/// BlockBasedTableReader.MetadataPinning.cs). Data blocks, index partitions and filter partitions are fetched on
/// demand through the block cache. Thread-safe for concurrent lookups and iterators.
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
    // The fields below that describe pinned metadata blocks are set before the reader is published and never changed
    // afterwards (some after the constructor, by the prefetch of BlockBasedTableReader.MetadataPinning.cs).
    private Block? _indexBlock;                   // owned or pinned (top-level) index; null when looked up in the block cache per use
    private readonly Block? _rangeDelBlock;
    private IFilterBitsReader? _filter;           // owned or pinned full filter (null when absent or looked up per use)
    private byte[]? _filterBuffer;                // buffer of an owned full filter
    private bool _hasFilter;
    private FilterKind _filterKind;
    private readonly BlockHandle _filterHandle;   // full filter / top-level partition index handle (for the block cache)
    private Block? _filterIndexBlock;             // owned or pinned top-level filter partition index (null when absent or looked up per use)
    private PinnedFilterPartitions? _pinnedFilterPartitions;
    private readonly SliceTransform? _tablePrefixExtractor;
    private readonly bool _prefixExtractorChanged; // relative to TableReaderOptions.PrefixExtractor
    // "rocksdb.compression_dict" (data blocks): the dictionary the reader owns or pins; null when the table has none or
    // when it lives in the block cache unpinned (see BlockBasedTableReader.CompressionDict.cs). Set before the reader
    // is published and never changed afterwards.
    private Compression.CompressionDictionary? _compressionDict;
    private readonly ulong _tableId;
    private readonly BlockCache? _cache;          // native block cache (null: none or legacy seam)
    private readonly IBlockCache? _legacyCache;   // legacy IBlockCache seam (null: none or native)
    private readonly OffsetableCacheKey _baseCacheKey;
    private readonly CachePriority _metaPriority;
    private readonly BlockCacheStatistics? _stats;
    private readonly bool _hasEmbeddedBlobs;
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
    /// <summary>
    /// True when the top-level index, the filter and the compression dictionary are block cache entries (use_cache of
    /// BlockBasedTable::PrefetchIndexAndFilterBlocks), pinned by the reader or not, rather than memory it owns.
    /// </summary>
    public bool IndexAndFilterInCache => _cache is not null && _options.CacheIndexAndFilterBlocks;
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
        if (compressionDict is not null) _dictLease = DictLease.Owned(compressionDict);
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
        // The presence of the embedded blob stats property signals same-file ("embedded") blob records.
        _hasEmbeddedBlobs = props.UserCollectedProperties.ContainsKey(TablePropertiesNames.EmbeddedBlobSstStats);
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
    /// from the hash index meta blocks. Missing meta blocks, an unreadable prefixes metadata block or a failure to create the
    /// prefix hash index are not hard errors; the index is then searched with binary search (null).
    /// </summary>
    private static BlockPrefixIndex? TryCreatePrefixIndex(IRandomAccessFile file, in Footer footer, Dictionary<string, BlockHandle> metaBlocks, BlockFetcher.ChecksumVerification verify)
    {
        if (!metaBlocks.TryGetValue(BlockBasedTableConstants.HashIndexPrefixesBlockName, out BlockHandle prefixesHandle)
            || !metaBlocks.TryGetValue(BlockBasedTableConstants.HashIndexPrefixesMetadataBlockName, out BlockHandle metaHandle))
            return null;
        // A prefixes block that cannot be read fails the open ("return s" after prefixes_block_fetcher.ReadBlockContents);
        // only a bad prefixes metadata block or BlockPrefixIndex::Create failure falls back to binary search.
        byte[]? prefixes = BlockFetcher.ReadRawPayload(file, footer, prefixesHandle, verify, out int prefixesLength);
        byte[]? meta = null;
        try
        {
            meta = BlockFetcher.ReadRawPayload(file, footer, metaHandle, verify, out int metaLength);
            return BlockPrefixIndex.Create(prefixes.AsSpan(0, prefixesLength), meta.AsSpan(0, metaLength));
        }
        catch (RocksDbException)
        {
            return null;
        }
        finally
        {
            if (prefixes is { Length: > 0 }) RocksArrayPool<byte>.Shared.Return(prefixes);
            if (meta is { Length: > 0 }) RocksArrayPool<byte>.Shared.Return(meta);
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

    /// <summary>
    /// The table's compression dictionary ("rocksdb.compression_dict"), empty when none. A dictionary that lives in the
    /// block cache unpinned is looked up there (and read on a miss), like the dictionary dump of BlockBasedTable::DumpTable.
    /// </summary>
    public ReadOnlySpan<byte> CompressionDictionary
    {
        get
        {
            if (_compressionDict is not null) return _compressionDict.Data;
            if (!_dictLookupPerRead) return default;
            DictLease lease = GetOrReadDictionary(TableReadFlags.None, _file);
            // The bytes are a managed array that outlives the lease (only the digested dictionary is freed with it).
            ReadOnlySpan<byte> data = lease.Dict!.Data;
            lease.Release();
            return data;
        }
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
        var verify = new BlockFetcher.ChecksumVerification(options.VerifyChecksums, options.Statistics?.Owner);
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
            // The block is read from the file once (upstream's single BlockFetcher read); parsing and the checksum
            // verification below both work on those bytes.
            IRandomAccessFile propsFile = file;
            byte[]? propsRaw = null;
            ulong propsLength = propsHandle.Size + BlockBasedTableConstants.BlockTrailerSize;
            if (verify.Enabled && propsLength <= 1 << 26)
            {
                propsRaw = RocksArrayPool<byte>.Shared.Rent((int)propsLength);
                if (BlockFetcher.ReadFully(file, (long)propsHandle.Offset, propsRaw.AsSpan(0, (int)propsLength)) == (int)propsLength)
                {
                    propsFile = new RangeCachedFile(file, (long)propsHandle.Offset, propsRaw, (int)propsLength);
                }
            }
            try
            {
                Block pb = BlockFetcher.ReadBlock(propsFile, footer, propsHandle, verifyChecksums: false);
                try { props = TableProperties.Parse(pb, propsHandle.Offset); }
                finally { pb.Release(); }
                if (verify.Enabled) VerifyPropertiesBlockChecksum(propsFile, footer, propsHandle, props.ExternalSstFileGlobalSeqnoOffset);
            }
            finally
            {
                if (propsRaw is not null) RocksArrayPool<byte>.Shared.Return(propsRaw);
            }
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
        // table reader always owns them). Index and filter partitions always go through the block cache.
        BlockCache? blockCache = options.BlockCache as BlockCache;
        BlockCache? metaCache = options.CacheIndexAndFilterBlocks ? blockCache : null;
        MetadataPins pins = MetadataPins.Compute(options, fileSize);
        // BlockBasedTable::Open: prefetch_all = prefetch_index_and_filter_in_cache || level == 0.
        bool prefetchAll = options.PrefetchIndexAndFilterInCache || options.Level == 0;

        // An index and filter the reader owns (not in the block cache) live as long as the reader: their buffers come from
        // the table buffer pool, where the table builder has just returned its index and filter buffers (the same sizes)
        // and where the builders of later flushes / compactions find them again once this reader is closed. Blocks that go
        // to the block cache keep the block buffer pool's finer size classes (the cache is charged the buffer length).
        // An index in the block cache is looked up there (and its secondary tiers) once the reader exists, and read only
        // on a miss (BinarySearchIndexReader / PartitionIndexReader::Create with use_cache).
        Block? indexBlock = metaCache is not null ? null : Protect(BlockFetcher.ReadBlock(file, footer, indexHandle, verify, null, BlockBufferSource.TableBuffers), options);
        Block? rangeDel = null;
        byte[]? filterBuf = null;
        BlockBufferSource filterSource = BlockBufferSource.NotRented;
        Block? filterIndex = null;
        PinnedFilterPartitions? pinnedParts = null;
        Compression.CompressionDictionary? compressionDict = null;
        bool hasDict = false;
        BlockHandle dictHandle = default;
        BlockBasedTableReader reader;
        try
        {
            if (metaBlocks.TryGetValue(BlockBasedTableConstants.RangeDelBlockName, out BlockHandle rdHandle))
                rangeDel = Protect(BlockFetcher.ReadBlock(file, footer, rdHandle, verify), options);

            // Port of the compression dictionary setup of BlockBasedTable::PrefetchIndexAndFilterBlocks
            // (UncompressionDictReader::Create): without cache_index_and_filter_blocks (use_cache = false) the reader
            // owns the dictionary; with it the dictionary goes through the block cache once the reader exists.
            hasDict = metaBlocks.TryGetValue(BlockBasedTableConstants.CompressionDictBlockName, out dictHandle) && dictHandle.Size > 0;
            if (hasDict && metaCache is null)
                compressionDict = ReadDictionaryBlock(file, footer, dictHandle, verify, digest: false);

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
                // With cache_index_and_filter_blocks the top-level filter index is looked up in the block cache once the
                // reader exists (PartitionedFilterBlockReader::Create with use_cache).
                if (metaCache is null)
                {
                    Block? top = null;
                    try
                    {
                        top = BlockFetcher.ReadBlock(file, footer, fh, verify);
                    }
                    catch (Exception e) when (e is RocksDbException or IOException)
                    {
                        // PartitionedFilterBlockReader::Create (partitioned_filter_block.cc): a top-level filter index that
                        // cannot be read is ignored (IGNORE_STATUS_IF_ERROR) and the table is opened without a filter.
                        filterKind = FilterKind.None;
                    }
                    filterIndex = top;
                    if (top is not null && blockCache is null)
                    {
                        // Deviation: without a native block cache the reader loads all partitions up front and keeps
                        // them. C++ loads nothing at open then (MaybeReadBlockAndLoadToCache is a no-op) and reads the
                        // partition for every lookup. Read errors fail the open, except without any block cache, where a
                        // lookup ignores an unreadable partition.
                        pinnedParts = PinnedFilterPartitions.Load(file, footer, verify, ppolicy, top, props, options.Comparator,
                            ignoreUnreadablePartitions: options.BlockCache is NoBlockCache);
                    }
                }
            }
            else if (options.FilterPolicy is { } policy
                && metaBlocks.TryGetValue(BlockBasedTableConstants.FullFilterBlockPrefix + policy.CompatibilityName, out fh))
            {
                filterKind = FilterKind.Full;
                // With cache_index_and_filter_blocks the filter is looked up in the block cache (and its secondary tiers)
                // once the reader exists, and read on a miss (FullFilterBlockReader::Create with use_cache).
                if (metaCache is null)
                {
                    filterSource = BlockBufferSource.TableBuffers;
                    try
                    {
                        filterBuf = BlockFetcher.ReadRawPayload(file, footer, fh, verify, filterSource, out int flen);
                        filter = CreateFilterReader(policy, filterBuf, flen);
                    }
                    catch (Exception e) when (e is RocksDbException or IOException)
                    {
                        // FullFilterBlockReader::Create (full_filter_block.cc): a filter block that cannot be read is
                        // ignored (IGNORE_STATUS_IF_ERROR) and the table is opened without a filter.
                        if (filterBuf is not null) TableBuilderBuffers.Return(filterSource, filterBuf);
                        filterBuf = null;
                        filter = null;
                        filterKind = FilterKind.None;
                    }
                }
            }
            UserDefinedIndexReader? udi = OpenUserDefinedIndex(file, options, footer, metaBlocks, props, verify, out bool udiStub);
            reader = new BlockBasedTableReader(ownedFile ?? file, options, footer, metaBlocks, props, indexHandle, indexBlock, rangeDel, filter, filterBuf,
                filterKind, fh, filterIndex, pinnedParts, compressionDict, baseCacheKey, fileSize);
            if (hasDict) reader._dictHandle = dictHandle;
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
        if (blockCache is not null)
        {
            try
            {
                // The rest of BlockBasedTable::PrefetchIndexAndFilterBlocks, through the block cache and reading through
                // `file` (the tail prefetch buffer of an async open).
                reader.PrefetchMetadataIntoCache(blockCache, metaCache is not null, file, pins, prefetchAll);
            }
            catch
            {
                reader.Dispose();
                throw;
            }
        }
        return reader;
    }

    private UserDefinedIndexReader? _udiReader;

    // sizeof of the native objects BlockBasedTable::ApproximateMemoryUsage charges (x86-64 / libstdc++, main 11.14,
    // measured with g++ against the headers): the managed reader keeps the same state in differently shaped objects,
    // so the native sizes stand in for them, as LruCache.EntryMetadataCharge does for cache handles.
    private const int NativeRepSize = 944;                          // BlockBasedTable::Rep (block_based_table_reader.h)
    private const int NativePartitionIndexReaderSize = 104;         // PartitionIndexReader (partitioned_index_reader.h)
    private const int NativeBinarySearchIndexReaderSize = 48;       // BinarySearchIndexReader (binary_search_index_reader.h)
    private const int NativeHashIndexReaderSize = 56;               // HashIndexReader (hash_index_reader.h)
    private const int NativeFullFilterBlockReaderSize = 64;         // FullFilterBlockReader (full_filter_block.h)
    private const int NativePartitionedFilterBlockReaderSize = 120; // PartitionedFilterBlockReader (partitioned_filter_block.h)
    private const int NativeUncompressionDictReaderSize = 40;       // UncompressionDictReader (uncompression_dict_reader.h)

    /// <summary>
    /// Port of BlockBasedTable::ApproximateMemoryUsage (table/block_based/block_based_table_reader.cc): Rep, the filter
    /// reader, the index reader and the uncompression dictionary reader (each the object, malloc_usable_size or sizeof
    /// without ROCKSDB_MALLOC_USABLE_SIZE, see <see cref="FilterAllocationModel"/>, plus the block it owns outside the
    /// block cache), the table properties (TableProperties::ApproximateMemoryUsage) and the user-defined index. Blocks
    /// held by the block cache, pinned or not, are part of the cache's usage. As upstream, the range tombstones
    /// (Rep::fragmented_range_dels) and pinned index partitions (partition_map_) are not counted; the filter partitions
    /// the port owns without a block cache (a deviation: upstream keeps none, see PinnedFilterPartitions) are.
    /// </summary>
    public override ulong ApproximateMemoryUsage
    {
        get
        {
            // Rep::ApproximateMemoryUsage.
            ulong usage = FilterAllocationModel.UsableSize(NativeRepSize);
            // FullFilterBlockReader / PartitionedFilterBlockReader::ApproximateMemoryUsage: the reader object and the
            // filter block it owns (FilterBlockReaderCommon::ApproximateFilterBlockMemoryUsage, GetOwnValue only).
            switch (_filterKind)
            {
                case FilterKind.Full:
                    usage += FilterAllocationModel.UsableSize(NativeFullFilterBlockReaderSize);
                    if (_filter is not null && _filterBuffer is not null) usage += (ulong)_filterBuffer.Length;
                    break;
                case FilterKind.Partitioned:
                    usage += FilterAllocationModel.UsableSize(NativePartitionedFilterBlockReaderSize);
                    if (_filterIndexBlock is not null && !_filterIndexRef.IsCacheHandle) usage += (ulong)_filterIndexBlock.Charge;
                    if (_pinnedFilterPartitions is not null) usage += _pinnedFilterPartitions.MemoryUsage;
                    break;
            }
            // The index reader of BlockBasedTable::CreateIndexReader (a hash index without the table's prefix extractor
            // falls back to binary search) and IndexReaderCommon::ApproximateIndexBlockMemoryUsage.
            usage += IndexType switch
            {
                IndexType.TwoLevelIndexSearch => FilterAllocationModel.UsableSize(NativePartitionIndexReaderSize),
                IndexType.HashSearch when _tablePrefixExtractor is not null => HashIndexReaderSize(),
                _ => FilterAllocationModel.UsableSize(NativeBinarySearchIndexReaderSize),
            };
            if (_indexBlock is not null && !_indexRef.IsCacheHandle) usage += (ulong)_indexBlock.Charge;
            // UncompressionDictReader::ApproximateMemoryUsage: the reader exists when the table has a dictionary block;
            // only a dictionary it owns (GetOwnValue) is counted.
            if (_dictHandle.Size > 0)
            {
                usage += FilterAllocationModel.UsableSize(NativeUncompressionDictReaderSize);
                if (_compressionDict is not null && _dictLease.IsOwned) usage += (ulong)_compressionDict.ApproximateMemoryUsage;
            }
            usage += Properties.ApproximateMemoryUsage;
            if (_udiReader is not null) usage += _udiReader.ApproximateMemoryUsage;
            return usage;
        }
    }

    /// <summary>
    /// HashIndexReader::ApproximateMemoryUsage: with ROCKSDB_MALLOC_USABLE_SIZE the object only, without it sizeof plus
    /// BlockPrefixIndex::ApproximateMemoryUsage (upstream counts the prefix index only in that branch).
    /// </summary>
    private ulong HashIndexReaderSize() => FilterAllocationModel.IsActive
        ? FilterAllocationModel.UsableSize(NativeHashIndexReaderSize)
        : NativeHashIndexReaderSize + (_prefixIndex?.ApproximateMemoryUsage ?? 0);

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
        Dictionary<string, BlockHandle> metaBlocks, TableProperties props, BlockFetcher.ChecksumVerification verify, out bool requiresCustomReader)
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
                finally { if (raw.Length > 0) RocksArrayPool<byte>.Shared.Return(raw); }
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
    /// Port of BlockBasedTable::VerifyChecksum (+ VerifyChecksumInMetaBlocks / VerifyChecksumInBlocks): reads every meta
    /// block (the properties block with the global seqno special case; the index block only when it was not verified
    /// at open) and every data block listed by the index, raw, and verifies its checksum. Corruption on a mismatch.
    /// Like upstream, index and filter partitions are not checked.
    /// </summary>
    public void VerifyChecksum()
    {
        foreach (KeyValuePair<string, BlockHandle> kv in MetaBlocks)
        {
            if (kv.Key == BlockBasedTableConstants.PropertiesBlockName)
            {
                VerifyPropertiesBlockChecksum(_file, _footer, kv.Value, Properties.ExternalSstFileGlobalSeqnoOffset);
                continue;
            }
            // WART (upstream): the index block checksum is skipped when it was checked on open.
            if (kv.Key == BlockBasedTableConstants.IndexBlockName && _options.VerifyChecksums) continue;
            RocksArrayPool<byte>.Shared.Return(BlockFetcher.ReadRawPayload(_file, _footer, kv.Value, verifyChecksums: true, out _));
        }
        IndexCursor idx = IndexCursor.Rent(this, TableReadFlags.DontFillCache);
        try
        {
            for (idx.SeekToFirst(); idx.Valid; idx.Next())
            {
                RocksArrayPool<byte>.Shared.Return(BlockFetcher.ReadRawPayload(_file, _footer, idx.Handle, verifyChecksums: true, out _));
            }
        }
        finally
        {
            idx.Return();
        }
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
        byte[] buf = RocksArrayPool<byte>.Shared.Rent(n);
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
            RocksArrayPool<byte>.Shared.Return(buf);
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
        if (version < 2)
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
        if (_dictLookupPerRead && kind == BlockKind.Data) return GetDataBlockRefWithCachedDictionary(handle, flags, null);
        BlockCache? cache = _cache;
        if (cache is not null)
        {
            CacheKey key = _baseCacheKey.WithOffsetForMinSizeRecord(handle.Offset);
            if (LookupCache(cache, key, kind, out CachedBlock h))
            {
                _stats?.RecordHit(HitCounter(kind), h.Charge);
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
        if (_dictLookupPerRead && kind == BlockKind.Data) return GetDataBlockRefWithCachedDictionary(handle, flags, prefetch);
        BlockCache? cache = _cache;
        CacheKey key = default;
        if (cache is not null)
        {
            key = _baseCacheKey.WithOffsetForMinSizeRecord(handle.Offset);
            if (LookupCache(cache, key, kind, out CachedBlock h))
            {
                _stats?.RecordHit(HitCounter(kind), h.Charge);
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
        ThrowIfNoIo(flags);
        Block b = Protect(BlockFetcher.ReadBlock(prefetch, _footer, handle, VerifyRead(flags), DictFor(kind)), kind);
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

    /// <summary>
    /// Whether blocks this read loads from the file are checksum-verified: the reader's setting, unless the read opted
    /// out (ReadOptions::verify_checksums = false, <see cref="TableReadFlags.SkipChecksumVerification"/>).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool Verify(TableReadFlags flags) => _options.VerifyChecksums && (flags & TableReadFlags.SkipChecksumVerification) == 0;

    /// <summary><see cref="Verify"/> for a block read, with the statistics that count checksum verifications.</summary>
    private BlockFetcher.ChecksumVerification VerifyRead(TableReadFlags flags) => new(Verify(flags), _stats?.Owner);

    /// <summary>
    /// BlockBasedTable::RetrieveBlock with no_io (ReadOptions::read_tier = kBlockCacheTier): a block that is not cached
    /// fails the read with Incomplete("no blocking io") instead of being read from the file. Also enforces the
    /// read deadline (<see cref="ReadDeadline"/>) before the file is read.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void ThrowIfNoIo(TableReadFlags flags)
    {
        if ((flags & TableReadFlags.BlockCacheTier) != 0) ThrowNoIo();
        // ReadOptions::deadline (PrepareIOFromReadOptions): no file read once it has passed.
        ReadDeadline.CheckBeforeIo();
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    [System.Diagnostics.CodeAnalysis.DoesNotReturn]
    internal static void ThrowNoIo() => throw new RocksDbException(StatusCode.Incomplete, "Result incomplete: no blocking io");

    private BlockRef ReadAndInsert(BlockCache cache, in CacheKey key, BlockHandle handle, BlockKind kind, TableReadFlags flags)
    {
        _stats?.Record(MissCounter(kind));
        ThrowIfNoIo(flags);
        ChargeRateLimiter(handle, flags);
        if (cache.AdmitsCompressedBlocks && OffersCompressed(kind, flags)) return ReadAndInsertOfferingCompressed(cache, key, handle, kind, flags);
        Block b = Protect(ReadBlockFromFile(handle, kind, flags, DictFor(kind)), kind);
        if (_bcTracer is { IsEnabled: true } t) TraceAccess(t, key, kind, b, hit: false, flags);
        return (flags & TableReadFlags.DontFillCache) != 0 ? BlockRef.FromOwned(b) : InsertIntoCache(cache, key, b, kind);
    }

    private BlockRef GetBlockRefLegacy(BlockHandle handle, BlockKind kind, TableReadFlags flags)
    {
        IBlockCache? legacy = _legacyCache;
        if (legacy is null)
        {
            ThrowIfNoIo(flags);
            ChargeRateLimiter(handle, flags);
            return BlockRef.FromOwned(Protect(ReadBlockFromFile(handle, kind, flags, DictFor(kind)), kind));
        }
        var key = new BlockCacheKey(_tableId, handle.Offset);
        if (legacy.TryGet(key, out Block? b))
        {
            _stats?.RecordHit(HitCounter(kind), b.Charge);
            return BlockRef.FromOwned(b);
        }
        _stats?.Record(MissCounter(kind));
        ThrowIfNoIo(flags);
        ChargeRateLimiter(handle, flags);
        b = Protect(ReadBlockFromFile(handle, kind, flags, DictFor(kind)), kind);
        if ((flags & TableReadFlags.DontFillCache) == 0) legacy.Insert(key, b);
        return BlockRef.FromOwned(b);
    }

    /// <summary>
    /// Whether a block read from the file is offered compressed to the cache's secondary tiers: secondary-cache compatible
    /// helpers (lowest_used_cache_tier), the cache is filled, and the block comes from the file rather than a persistent
    /// cache (whose compressed copy is not kept).
    /// </summary>
    private bool OffersCompressed(BlockKind kind, TableReadFlags flags) =>
        (flags & TableReadFlags.DontFillCache) == 0 && _persistentCache is null && _helpers[(int)kind].IsSecondaryCacheCompatible
        // Deviation: data blocks of a table whose dictionary is in the block cache unpinned are not offered compressed (a
        // tier re-creating one would need the dictionary, which may have been evicted by then).
        && !(_dictLookupPerRead && kind == BlockKind.Data);

    /// <summary>
    /// MaybeReadBlockAndLoadToCache + PutDataBlockToCache with a cache using <see cref="TieredAdmissionPolicy.ThreeQueue"/>:
    /// the uncompressed block goes to the primary cache and the compressed payload (when the block is stored compressed)
    /// is passed along as Cache::Insert's compressed_value, which the adapter hands to the secondary tiers (InsertSaved).
    /// </summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private BlockRef ReadAndInsertOfferingCompressed(BlockCache cache, in CacheKey key, BlockHandle handle, BlockKind kind, TableReadFlags flags)
    {
        Block b = Protect(BlockFetcher.ReadBlockKeepingCompressed(_file, _footer, handle, VerifyRead(flags), DictFor(kind), out byte[]? serialized), kind);
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
        BlockFetcher.CompressedRead read = await BlockFetcher.ReadBlockKeepingCompressedAsync(_file, _footer, handle, VerifyRead(flags),
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
            RecordAdd(_stats, kind, b.Charge, h.InsertedRedundantly);
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
            RecordAdd(_stats, kind, b.Charge, h.InsertedRedundantly);
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
        if (_dictLookupPerRead && kind == BlockKind.Data) return GetDataBlockRefWithCachedDictionaryAsync(handle, flags, ct);
        BlockCache? cache = _cache;
        if (cache is not null)
        {
            CacheKey key = _baseCacheKey.WithOffsetForMinSizeRecord(handle.Offset);
            if (cache.ExtendedLookup) return GetBlockRefExtendedAsync(cache, key, handle, kind, flags, ct);
            if (cache.TryLookup(key, out CachedBlock h))
            {
                _stats?.RecordHit(HitCounter(kind), h.Charge);
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
                _stats?.RecordHit(HitCounter(kind), b.Charge);
                return new ValueTask<BlockRef>(BlockRef.FromOwned(b));
            }
            _stats?.Record(MissCounter(kind));
        }
        return ReadAndInsertLegacyAsync(handle, kind, flags, ct);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<BlockRef> ReadAndInsertAsync(BlockCache cache, CacheKey key, BlockHandle handle, BlockKind kind, TableReadFlags flags, CancellationToken ct)
    {
        ThrowIfNoIo(flags);
        await ChargeRateLimiterAsync(handle, flags, ct).ConfigureAwait(false);
        if (cache.AdmitsCompressedBlocks && OffersCompressed(kind, flags))
            return await ReadAndInsertOfferingCompressedAsync(cache, key, handle, kind, flags, ct).ConfigureAwait(false);
        Block b = Protect(_persistentCache is { } pc ? await ReadBlockThroughPersistentCacheAsync(pc, handle, kind, flags, DictFor(kind), ct).ConfigureAwait(false)
            : await BlockFetcher.ReadBlockAsync(_file, _footer, handle, VerifyRead(flags), DictFor(kind), ct).ConfigureAwait(false), kind);
        if (_bcTracer is { IsEnabled: true } t) TraceAccess(t, key, kind, b, hit: false, flags);
        return (flags & TableReadFlags.DontFillCache) != 0 ? BlockRef.FromOwned(b) : InsertIntoCache(cache, key, b, kind);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<BlockRef> ReadAndInsertLegacyAsync(BlockHandle handle, BlockKind kind, TableReadFlags flags, CancellationToken ct)
    {
        ThrowIfNoIo(flags);
        await ChargeRateLimiterAsync(handle, flags, ct).ConfigureAwait(false);
        Block b = Protect(_persistentCache is { } pc ? await ReadBlockThroughPersistentCacheAsync(pc, handle, kind, flags, DictFor(kind), ct).ConfigureAwait(false)
            : await BlockFetcher.ReadBlockAsync(_file, _footer, handle, VerifyRead(flags), DictFor(kind), ct).ConfigureAwait(false), kind);
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
        // Block::Block with read_amp_bytes_per_bit and statistics: data blocks read from the file get a read-amp bitmap.
        if (_options.ReadAmpBytesPerBit != 0 && kind == BlockKind.Data) AttachReadAmpBitmap(b);
        return b;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void AttachReadAmpBitmap(Block b)
    {
        if (_stats?.Owner is { } st && b.RestartOffset > 0) b.ReadAmp = new BlockReadAmpBitmap(b.RestartOffset, _options.ReadAmpBytesPerBit, st);
    }

    private static Block Protect(Block b, TableReaderOptions options)
    {
        if (options.BlockProtectionBytesPerKey != 0) b.RequestKvProtection(options.BlockProtectionBytesPerKey);
        return b;
    }

    /// <summary>The options the reader was opened with.</summary>
    internal TableReaderOptions Options => _options;

    /// <summary>
    /// Port of IndexReaderCommon::GetOrReadIndexBlock: the (top-level) index block the reader owns or pins, else a
    /// block cache lookup (reading and inserting the block on a miss; Incomplete with <see cref="TableReadFlags.BlockCacheTier"/>).
    /// </summary>
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

    /// <summary>
    /// Port of BlockBasedTable::UpdateCacheInsertionMetrics: BLOCK_CACHE_{,kind_}ADD and the bytes inserted, plus
    /// BLOCK_CACHE_{,kind_}ADD_REDUNDANT when the cache reported the insert as redundant (Status::OkOverwritten, see
    /// <see cref="CachedBlock.InsertedRedundantly"/>).
    /// </summary>
    private static void RecordAdd(BlockCacheStatistics? stats, BlockKind kind, long charge, bool redundant)
    {
        if (stats is null) return;
        if (redundant)
        {
            stats.Record(kind switch
            {
                BlockKind.Data => BlockCacheCounter.DataAddRedundant,
                BlockKind.Index => BlockCacheCounter.IndexAddRedundant,
                _ => BlockCacheCounter.FilterAddRedundant,
            });
        }
        stats.Record(kind switch
        {
            BlockKind.Data => BlockCacheCounter.DataAdd,
            BlockKind.Index => BlockCacheCounter.IndexAdd,
            _ => BlockCacheCounter.FilterAdd,
        });
        stats.Add(BlockCacheCounter.BytesInserted, charge);
        stats.Add(kind switch
        {
            BlockKind.Data => BlockCacheCounter.DataBytesInsert,
            BlockKind.Index => BlockCacheCounter.IndexBytesInsert,
            _ => BlockCacheCounter.FilterBytesInsert,
        }, charge);
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

    /// <summary>The top-level index block the reader owns or pins (not available when it is looked up in the block cache per use).</summary>
    internal Block IndexBlock => _indexBlock ?? throw new InvalidOperationException("The index block is held by the block cache (CacheIndexAndFilterBlocks).");

    /// <summary>The buffer of the full filter the reader owns (null when absent or held by the block cache). Tests only.</summary>
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
        // An index iterator error: split in half as well.
        if (!TryApproximateDataOffsetOf(internalKey, dataSize, out ulong offset)) return (ulong)FileSize / 2;
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
        // An index iterator error: assume the file is involved from the start / until the end.
        if (!TryApproximateDataOffsetOf(startInternalKey, dataSize, out ulong startOffset)) startOffset = 0;
        if (!TryApproximateDataOffsetOf(endInternalKey, dataSize, out ulong endOffset)) endOffset = dataSize;
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

    // False when the index seek failed (index_iter->status() not ok upstream: an unreadable index partition).
    private bool TryApproximateDataOffsetOf(ReadOnlySpan<byte> internalKey, ulong dataSize, out ulong offset)
    {
        IndexCursor cursor = IndexCursor.Rent(this, TableReadFlags.DontFillCache);
        try
        {
            cursor.Seek(internalKey);
            offset = cursor.Valid ? cursor.Handle.Offset : dataSize;
            return true;
        }
        catch (RocksDbException)
        {
            offset = 0;
            return false;
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
        Span<byte> ikey = n <= 256 ? stackalloc byte[256] : (rented = RocksArrayPool<byte>.Shared.Rent(n));
        try
        {
            DbFormat.AppendInternalKey(ikey, userKey, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
            return InternalKeyMayMatch(ikey[..n], flags);
        }
        finally
        {
            if (rented is not null) RocksArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// Port of BlockBasedTable::FullFilterKeyMayMatch: with whole_key_filtering, whether the user key of
    /// <paramref name="internalKey"/> may be present; otherwise, when the configured prefix extractor matches the
    /// table's, whether its prefix may be present. The internal key selects the partition of a partitioned filter.
    /// </summary>
    public override bool InternalKeyMayMatch(ReadOnlySpan<byte> internalKey, TableReadFlags flags)
    {
        if (_filterKind == FilterKind.None || (flags & TableReadFlags.SkipFilters) != 0) return true;
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
            // FullFilterBlockReader::PrefixMayMatch -> MayMatch counts the probe like a whole key probe.
            PerfContext.Add(mayMatch ? PerfMetric.BloomSstHitCount : PerfMetric.BloomSstMissCount, 1);
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
    internal bool PrefixRangeMayMatch(ReadOnlySpan<byte> internalKey, in TableIteratorPrefixOptions o, TableReadFlags flags, out bool filterChecked)
    {
        filterChecked = false;
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
        // FullFilterBlockReader::PrefixMayMatch records no BLOOM_FILTER_* ticker: seeks are counted by the iterator
        // (SEEK_FILTERED / SEEK_FILTER_MATCH); BLOOM_FILTER_PREFIX_USEFUL is for point lookups only.
        filterChecked = true;
        bool prefixMayMatch = FilterMayMatch(prefix, internalKey, flags);
        // FilterBlockReaderCommon::RangeMayExist -> PrefixMayMatch -> FullFilterBlockReader::MayMatch.
        PerfContext.Add(prefixMayMatch ? PerfMetric.BloomSstHitCount : PerfMetric.BloomSstMissCount, 1);
        return prefixMayMatch;
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
        if (ucmp.CompareWithoutTimestamp(prefix, false, upperBoundXform, false) != 0)
        {
            // second check if user_key's prefix is the immediate predecessor of upper_bound and have the same length. If
            // so, we know for sure all keys in the range [user_key, upper_bound) share the same prefix. Also need to make
            // sure upper_bound are full length to ensure correctness
            if (!pe.FullLengthEnabled(out int fullLength) || upperBound.Length != fullLength || !ucmp.IsSameLengthImmediateSuccessor(prefix, upperBound))
                return false;
        }
        return true;
    }

    /// <summary>
    /// BlockBasedTableIterator::SeekImpl / SeekForPrev statistics (block_based_table_iterator.cc): a seek the prefix
    /// filter ruled out records {NON_,}LAST_LEVEL_SEEK_FILTERED, one it let through after consulting the filter
    /// {NON_,}LAST_LEVEL_SEEK_FILTER_MATCH, by whether the file is in the last level (BlockBasedTable::IsLastLevel).
    /// </summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    internal void RecordSeekFilterResult(bool filtered)
    {
        if (filtered) _stats?.Record(BlockCacheCounter.SeekFiltered);
        if (_stats?.Owner is { } st)
        {
            bool last = IsLastLevel;
            st.RecordTick(filtered
                ? last ? Ticker.LastLevelSeekFiltered : Ticker.NonLastLevelSeekFiltered
                : last ? Ticker.LastLevelSeekFilterMatch : Ticker.NonLastLevelSeekFilterMatch);
        }
    }

    /// <summary>
    /// BlockBasedTableIterator::InitDataBlock: {NON_,}LAST_LEVEL_SEEK_DATA for the first data block a seek loads. False
    /// (nothing recorded) without a statistics object.
    /// </summary>
    internal bool RecordSeekData()
    {
        if (_stats?.Owner is not { } st) return false;
        st.RecordTick(IsLastLevel ? Ticker.LastLevelSeekData : Ticker.NonLastLevelSeekData);
        return true;
    }

    /// <summary>BlockBasedTableIterator::value(): {NON_,}LAST_LEVEL_SEEK_DATA_USEFUL_{FILTER_MATCH,NO_FILTER}.</summary>
    internal void RecordSeekDataUseful(bool filterUsed)
    {
        if (_stats?.Owner is not { } st) return;
        bool last = IsLastLevel;
        st.RecordTick(filterUsed
            ? last ? Ticker.LastLevelSeekDataUsefulFilterMatch : Ticker.NonLastLevelSeekDataUsefulFilterMatch
            : last ? Ticker.LastLevelSeekDataUsefulNoFilter : Ticker.NonLastLevelSeekDataUsefulNoFilter);
    }

    /// <summary>BlockBasedTable::IsLastLevel: the level the file was opened at is the last level of the LSM tree.</summary>
    internal bool IsLastLevel => _options.Level >= 0 && _options.Level == _options.NumLevels - 1;

    /// <summary>
    /// BLOOM_FILTER_FULL_TRUE_POSITIVE / BLOOM_FILTER_PREFIX_TRUE_POSITIVE (BlockBasedTable::Get / MultiGet): a point
    /// lookup that the table's filter let through found an entry with the user key.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void RecordFilterTruePositive(TableReadFlags flags)
    {
        if (_stats is not null && _filterKind != FilterKind.None && (flags & TableReadFlags.SkipFilters) == 0) RecordFilterTruePositive();
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void RecordFilterTruePositive()
    {
        if (_stats?.Owner is { } st) st.RecordTick(WholeKeyFiltering ? Ticker.BloomFilterFullTruePositive : Ticker.BloomFilterPrefixTruePositive);
    }

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
        BlockRef top;
        if (_filterIndexBlock is not null) top = BlockRef.FromPinned(_filterIndexBlock);
        else if (!TryGetFilterIndexRef(flags, out top)) return true;
        BlockHandle partition;
        try
        {
            if (!TryFindFilterPartition(top.Block!, internalKey, out partition)) return true;
        }
        finally
        {
            top.Release();
        }
        if (partition.Size == 0) return false; // key is out of range
        if (_pinnedFilterPartitions is { } pinned && pinned.TryGet(partition.Offset, out IFilterBitsReader? r)) return r.MayMatch(entry);
        return FilterBlockMayMatch(partition, entry, flags);
    }

    /// <summary>
    /// Port of FilterBlockReaderCommon::GetOrReadFilterBlock for a top-level filter index the reader does not hold: a
    /// block cache lookup, and on a miss a read and an insert. False when the probe must answer "may match"
    /// (IGNORE_STATUS_IF_ERROR): the block is not cached and the read is restricted to the block cache
    /// (<see cref="TableReadFlags.BlockCacheTier"/>), or it cannot be read.
    /// </summary>
    private bool TryGetFilterIndexRef(TableReadFlags flags, out BlockRef top)
    {
        top = default;
        if ((flags & TableReadFlags.BlockCacheTier) != 0)
        {
            // no_io: only the lookup, without the exception a miss would cost through GetBlockRef.
            BlockCache? cache = _cache;
            if (cache is null) return false;
            CacheKey key = _baseCacheKey.WithOffsetForMinSizeRecord(_filterHandle.Offset);
            if (!LookupCache(cache, key, BlockKind.Filter, out CachedBlock h))
            {
                _stats?.Record(BlockCacheCounter.FilterMiss);
                return false;
            }
            _stats?.RecordHit(BlockCacheCounter.FilterHit, h.Charge);
            top = BlockRef.FromCache(Unsafe.As<Block>(h.Value)!, h);
            return true;
        }
        try
        {
            top = GetBlockRef(_filterHandle, BlockKind.Filter, flags);
            return true;
        }
        catch (Exception e) when (e is RocksDbException or IOException)
        {
            return false;
        }
    }

    /// <summary>
    /// PartitionedFilterBlockReader::GetFilterPartitionHandle: the handle of the partition covering
    /// <paramref name="internalKey"/> in the top-level filter index <paramref name="top"/> (the last partition if the
    /// key is past all of them). False when the index has no entry (the key may match).
    /// </summary>
    private bool TryFindFilterPartition(Block top, ReadOnlySpan<byte> internalKey, out BlockHandle partition)
    {
        partition = default;
        BlockIter it = IterPool.Rent();
        try
        {
            InitFilterIndexIter(it, top);
            it.Seek(internalKey);
            if (!it.Valid)
            {
                // entry is larger than all the keys. However its prefix might still be present in the last partition.
                it.SeekToLast();
            }
            if (!it.Valid) return false;
            partition = it.BlockHandle;
            return true;
        }
        finally
        {
            it.Clear();
            IterPool.Return(it);
        }
    }

    /// <summary>
    /// A filter block held for one or more probes: a block cache handle, or a buffer the holder owns (a block read
    /// with fill_cache = false, without a block cache, or that the cache rejected). Release once.
    /// </summary>
    private struct FilterBlockLease
    {
        public CachedBlock Handle;
        public byte[]? OwnedBuffer;

        public void Release()
        {
            if (Handle.HasValue) Handle.Dispose();
            byte[]? buffer = OwnedBuffer;
            if (buffer is not null)
            {
                OwnedBuffer = null;
                RocksArrayPool<byte>.Shared.Return(buffer);
            }
        }
    }

    /// <summary>
    /// Filter lookup through the block cache (cache_index_and_filter_blocks, filter partitions). A miss is read
    /// synchronously; without a block cache the filter is read and discarded.
    /// </summary>
    private bool FilterBlockMayMatch(BlockHandle handle, ReadOnlySpan<byte> entry, TableReadFlags flags)
    {
        FilterBlockLease lease = default;
        IFilterBitsReader? reader = AcquireFilterBlock(handle, flags, ref lease);
        bool result = reader is null || reader.MayMatch(entry);
        lease.Release();
        return result;
    }

    /// <summary>
    /// Port of FilterBlockReaderCommon::GetOrReadFilterBlock / PartitionedFilterBlockReader::GetFilterPartitionBlock
    /// for a filter that is not pinned: a block cache lookup, and on a miss a read and an insert (not with
    /// <see cref="TableReadFlags.DontFillCache"/>). Returns the filter, kept alive by <paramref name="lease"/> until it
    /// is released; null when the probe must answer "may match": the filter is not cached and the read is restricted to
    /// the block cache, or the block cannot be read (IGNORE_STATUS_IF_ERROR).
    /// </summary>
    private IFilterBitsReader? AcquireFilterBlock(BlockHandle handle, TableReadFlags flags, ref FilterBlockLease lease)
    {
        BlockCache? cache = _cache;
        CacheKey key = default;
        if (cache is not null)
        {
            key = _baseCacheKey.WithOffsetForMinSizeRecord(handle.Offset);
            if (LookupFilterCache(cache, key, out CachedBlock h))
            {
                _stats?.RecordHit(BlockCacheCounter.FilterHit, h.Charge);
                if (_bcTracer is { IsEnabled: true } t) TraceAccess(t, key, BlockKind.Filter, null, hit: true, flags, (ulong)handle.Size);
                lease.Handle = h;
                return Unsafe.As<IFilterBitsReader>(h.Value);
            }
        }
        _stats?.Record(BlockCacheCounter.FilterMiss);
        if ((flags & TableReadFlags.BlockCacheTier) != 0) return null; // no I/O allowed: Incomplete -> may match
        // FullFilterBlockReader::MayMatch / PartitionedFilterBlockReader::MayMatch: a filter block (or partition) that
        // cannot be read is ignored (IGNORE_STATUS_IF_ERROR) and the key may match.
        byte[] buf;
        int len;
        try
        {
            buf = BlockFetcher.ReadRawPayload(_file, _footer, handle, VerifyRead(flags), out len);
        }
        catch (Exception e) when (e is RocksDbException or IOException)
        {
            return null;
        }
        if (cache is not null && _bcTracer is { IsEnabled: true } tm) TraceAccess(tm, key, BlockKind.Filter, null, hit: false, flags, (ulong)len);
        IFilterBitsReader reader;
        try
        {
            reader = CreateFilterReader(_options.FilterPolicy!, buf, len);
        }
        catch (RocksDbException)
        {
            RocksArrayPool<byte>.Shared.Return(buf);
            return null;
        }
        catch
        {
            RocksArrayPool<byte>.Shared.Return(buf);
            throw;
        }
        if (cache is null || (flags & TableReadFlags.DontFillCache) != 0)
        {
            lease.OwnedBuffer = buf;
        }
        else if (cache.TryInsert(key, new CacheValue(buf, len, reader, true).WithHelper(FilterHelperFor(_options)), buf.Length, _metaPriority, out CachedBlock inserted))
        {
            RecordAdd(_stats, BlockKind.Filter, buf.Length, inserted.InsertedRedundantly);
            lease.Handle = inserted;
        }
        else
        {
            // Strict capacity limit reached: the filter is used uncached.
            _stats?.Record(BlockCacheCounter.AddFailure);
            lease.OwnedBuffer = buf;
        }
        return reader;
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
            bool matched = false;
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
                        if (!matched)
                        {
                            matched = true;
                            RecordFilterTruePositive(flags);
                        }
                        if (!(_hasEmbeddedBlobs && IsBlobIndexEntry(k) ? VisitEmbedded(ref state, visitor, k, data.Value, flags) : visitor(ref state, k, data.Value))) return;
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
                    int r = ScanBlockForGet(data, block.Block!, internalKey, first, value, flags, out TableGetResult result);
                    if (r != 0)
                    {
                        if (r > 0) RecordFilterTruePositive(flags);
                        return result;
                    }
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
                    int r = ScanBlockForGet(data, block.Block!, internalKey.Span, first, value, flags, out TableGetResult result);
                    if (r != 0)
                    {
                        if (r > 0) RecordFilterTruePositive(flags);
                        return result;
                    }
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
            bool matched = false;
            while (idx.Valid)
            {
                ValueTask<BlockRef> pending = GetBlockRefAsync(idx.Handle, BlockKind.Data, flags, cancellationToken);
                BlockRef block = pending.IsCompletedSuccessfully ? pending.Result : await pending.ConfigureAwait(false);
                try
                {
                    if (!VisitBlockForGet(data, block.Block!, internalKey.Span, first, state, visitor, flags, ref matched)) return;
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
        TableEntryVisitor<TState> visitor, TableReadFlags flags, ref bool matched)
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
            if (!matched)
            {
                matched = true;
                RecordFilterTruePositive(flags);
            }
            if (!(_hasEmbeddedBlobs && IsBlobIndexEntry(k) ? VisitEmbedded(ref state, visitor, k, data.Value, flags) : visitor(ref state, k, data.Value))) return false;
        }
        return true;
    }

    /// <summary>Returns 1 = found, -1 = definitely absent, 0 = continue with the next block.</summary>
    private int ScanBlockForGet(BlockIter data, Block block, ReadOnlySpan<byte> internalKey, bool first, IBufferWriter<byte>? value, TableReadFlags flags,
        out TableGetResult result)
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
        if (_hasEmbeddedBlobs && t == EntryType.BlobIndex && IsSameFileBlobIndex(data.Value))
        {
            // A same-file blob index reads as the value it references (BlockBasedTable::Get with embedded blobs).
            if (value is not null) ResolveEmbeddedBlob(data.Value, value, flags);
            t = EntryType.Value;
        }
        else
        {
            value?.Write(data.Value);
        }
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
        // A block pinned in the block cache (or owned after a rejected insert) is released through its reference.
        if (!_indexRef.IsEmpty) _indexRef.Release();
        else _indexBlock?.Release();
        _rangeDelBlock?.Release();
        if (!_filterIndexRef.IsEmpty) _filterIndexRef.Release();
        else _filterIndexBlock?.Release();
        _pinnedFilterPartitions?.Dispose();
        ReleasePinnedIndexPartitions();
        _filterLease.Dispose();
        _dictLease.Release(); // the owned dictionary, or the block cache handle of a pinned one
        _udiReader?.Dispose();


        byte[]? fb = Interlocked.Exchange(ref _filterBuffer, null);
        // The owned full filter (never set when the filter is held by the block cache) is rented from the pinned table
        // buffers, or from the shared pool when it is a filter the block cache rejected.
        if (fb is not null) TableBuilderBuffers.Return(_filterBufferSource, fb);
        _file.Dispose();
    }

    /// <summary>Serves reads inside one byte range already read from <paramref name="inner"/> from memory; other reads go to the file.</summary>
    private sealed class RangeCachedFile(IRandomAccessFile inner, long offset, byte[] buffer, int length) : IRandomAccessFile
    {
        public long Length => inner.Length;

        public int Read(long position, Span<byte> destination)
        {
            if (position >= offset && position + destination.Length <= offset + length)
            {
                buffer.AsSpan((int)(position - offset), destination.Length).CopyTo(destination);
                return destination.Length;
            }
            return inner.Read(position, destination);
        }

        public ValueTask<int> ReadAsync(long position, Memory<byte> destination, CancellationToken cancellationToken = default) =>
            new(Read(position, destination.Span));

        public bool Prefetch(long position, long count) => inner.Prefetch(position, count);

        public void Dispose()
        {
        }
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
            byte[] buf = RocksArrayPool<byte>.Shared.Rent((int)len);
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
            if (_tail is not null) RocksArrayPool<byte>.Shared.Return(_tail);
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
