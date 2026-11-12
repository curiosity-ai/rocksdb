using System.Runtime.CompilerServices;

namespace RocksDb.Managed.Table;

/// <summary>Block cache access counters recorded by <see cref="BlockBasedTableReader"/> (BLOCK_CACHE_* tickers).</summary>
public enum BlockCacheCounter
{
    /// <summary>BLOCK_CACHE_DATA_HIT.</summary>
    DataHit = 0,
    /// <summary>BLOCK_CACHE_DATA_MISS.</summary>
    DataMiss = 1,
    /// <summary>BLOCK_CACHE_DATA_ADD.</summary>
    DataAdd = 2,
    /// <summary>BLOCK_CACHE_INDEX_HIT (top-level index block when cached, and index partitions).</summary>
    IndexHit = 3,
    /// <summary>BLOCK_CACHE_INDEX_MISS.</summary>
    IndexMiss = 4,
    /// <summary>BLOCK_CACHE_INDEX_ADD.</summary>
    IndexAdd = 5,
    /// <summary>BLOCK_CACHE_FILTER_HIT.</summary>
    FilterHit = 6,
    /// <summary>BLOCK_CACHE_FILTER_MISS.</summary>
    FilterMiss = 7,
    /// <summary>BLOCK_CACHE_FILTER_ADD.</summary>
    FilterAdd = 8,
    /// <summary>BLOCK_CACHE_ADD_FAILURES (insert rejected by a strict capacity limit).</summary>
    AddFailure = 9,
    /// <summary>BLOCK_CACHE_BYTES_WRITE: charge of blocks inserted.</summary>
    BytesInserted = 10,
    /// <summary>BLOOM_FILTER_USEFUL: point lookups the whole-key filter answered negatively (no data block read).</summary>
    BloomFilterUseful = 11,
    /// <summary>BLOOM_FILTER_FULL_POSITIVE: point lookups the whole-key filter answered positively.</summary>
    BloomFilterFullPositive = 12,
    /// <summary>BLOOM_FILTER_PREFIX_CHECKED: point lookups checked against the prefix filter (whole_key_filtering off).</summary>
    BloomFilterPrefixChecked = 13,
    /// <summary>BLOOM_FILTER_PREFIX_USEFUL: point lookups and seeks the prefix filter answered negatively.</summary>
    BloomFilterPrefixUseful = 14,
    /// <summary>NON_LAST_LEVEL_SEEK_FILTERED + LAST_LEVEL_SEEK_FILTERED: table seeks skipped by the prefix filter.</summary>
    SeekFiltered = 15,
    /// <summary>BLOCK_CACHE_BYTES_READ: charge of the blocks found in the cache (BlockBasedTable::UpdateCacheHitMetrics).</summary>
    BytesRead = 16,
    /// <summary>BLOCK_CACHE_DATA_BYTES_INSERT (BlockBasedTable::UpdateCacheInsertionMetrics).</summary>
    DataBytesInsert = 17,
    /// <summary>BLOCK_CACHE_INDEX_BYTES_INSERT.</summary>
    IndexBytesInsert = 18,
    /// <summary>BLOCK_CACHE_FILTER_BYTES_INSERT.</summary>
    FilterBytesInsert = 19,
    /// <summary>BLOCK_CACHE_COMPRESSION_DICT_HIT (the compression dictionary block, with cache_index_and_filter_blocks).</summary>
    CompressionDictHit = 20,
    /// <summary>BLOCK_CACHE_COMPRESSION_DICT_MISS.</summary>
    CompressionDictMiss = 21,
    /// <summary>BLOCK_CACHE_COMPRESSION_DICT_ADD.</summary>
    CompressionDictAdd = 22,
    /// <summary>BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT.</summary>
    CompressionDictBytesInsert = 23,
    /// <summary>
    /// BLOCK_CACHE_DATA_ADD_REDUNDANT: data block inserts the cache reported as redundant (Cache::Insert returning
    /// Status::OkOverwritten: the key was already present; see <see cref="Cache.CacheEntry.InsertedRedundantly"/>).
    /// Each is also counted in the block kind's ADD counter.
    /// </summary>
    DataAddRedundant = 24,
    /// <summary>BLOCK_CACHE_INDEX_ADD_REDUNDANT.</summary>
    IndexAddRedundant = 25,
    /// <summary>BLOCK_CACHE_FILTER_ADD_REDUNDANT.</summary>
    FilterAddRedundant = 26,
    /// <summary>BLOCK_CACHE_COMPRESSION_DICT_ADD_REDUNDANT.</summary>
    CompressionDictAddRedundant = 27,
}

/// <summary>
/// Thread-safe block cache counters shared by any number of table readers (pass the same instance in
/// <see cref="TableReaderOptions.Statistics"/>). Counters are striped by processor so concurrent lookups on different
/// cores do not contend on one cache line (like upstream's per-core <c>StatisticsImpl</c>). Reads sum the stripes and
/// are not atomic snapshots across counters.
/// </summary>
public sealed class BlockCacheStatistics
{
    private const int NumCounters = 32;     // 32 * 8 bytes = 256 bytes per stripe (four cache lines, no false sharing)
    private const int NumStripes = 16;       // power of two
    private readonly long[] _counters = new long[NumStripes * NumCounters];

    /// <summary>
    /// The DB statistics these counters belong to (set by <see cref="Managed.Statistics"/>): secondary / persistent /
    /// simulation cache tickers are recorded there.
    /// </summary>
    internal Statistics? Owner { get; init; }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void Record(BlockCacheCounter counter) => Add(counter, 1);

    /// <summary>A cache hit of <paramref name="counter"/>'s block kind and the charge of the block found (BLOCK_CACHE_BYTES_READ).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void RecordHit(BlockCacheCounter counter, long charge)
    {
        int stripe = Thread.GetCurrentProcessorId() & (NumStripes - 1);
        Interlocked.Increment(ref _counters[stripe * NumCounters + (int)counter]);
        Interlocked.Add(ref _counters[stripe * NumCounters + (int)BlockCacheCounter.BytesRead], charge);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void Add(BlockCacheCounter counter, long delta)
    {
        int stripe = Thread.GetCurrentProcessorId() & (NumStripes - 1);
        Interlocked.Add(ref _counters[stripe * NumCounters + (int)counter], delta);
    }

    /// <summary>Current value of <paramref name="counter"/> (sum over all stripes).</summary>
    public long Get(BlockCacheCounter counter)
    {
        long sum = 0;
        for (int s = 0; s < NumStripes; s++) sum += Volatile.Read(ref _counters[s * NumCounters + (int)counter]);
        return sum;
    }

    public long DataHits => Get(BlockCacheCounter.DataHit);
    public long DataMisses => Get(BlockCacheCounter.DataMiss);
    public long IndexHits => Get(BlockCacheCounter.IndexHit);
    public long IndexMisses => Get(BlockCacheCounter.IndexMiss);
    public long FilterHits => Get(BlockCacheCounter.FilterHit);
    public long FilterMisses => Get(BlockCacheCounter.FilterMiss);

    /// <summary>Total hits over all block kinds (BLOCK_CACHE_HIT).</summary>
    public long Hits => DataHits + IndexHits + FilterHits + Get(BlockCacheCounter.CompressionDictHit);

    /// <summary>Total misses over all block kinds (BLOCK_CACHE_MISS).</summary>
    public long Misses => DataMisses + IndexMisses + FilterMisses + Get(BlockCacheCounter.CompressionDictMiss);

    /// <summary>Total redundant inserts over all block kinds (BLOCK_CACHE_ADD_REDUNDANT).</summary>
    public long RedundantAdds => Get(BlockCacheCounter.DataAddRedundant) + Get(BlockCacheCounter.IndexAddRedundant)
        + Get(BlockCacheCounter.FilterAddRedundant) + Get(BlockCacheCounter.CompressionDictAddRedundant);

    /// <summary>Resets all counters to zero (not atomic with respect to concurrent updates).</summary>
    public void Reset()
    {
        for (int i = 0; i < _counters.Length; i++) Volatile.Write(ref _counters[i], 0);
    }
}

/// <summary>Per-call flags for table reads (the table-level subset of ReadOptions).</summary>
[Flags]
public enum TableReadFlags : ushort
{
    /// <summary>Default: blocks read from the file are inserted into the block cache (ReadOptions::fill_cache = true).</summary>
    None = 0,
    /// <summary>ReadOptions::fill_cache = false: look blocks up in the cache, but do not insert blocks read on a miss.</summary>
    DontFillCache = 1,
    /// <summary>
    /// ReadOptions::read_tier = kBlockCacheTier (no_io): no file I/O. A data block, index block or index partition that
    /// is neither pinned nor in the block cache fails the read with <see cref="StatusCode.Incomplete"/> ("no blocking
    /// io", BlockBasedTable::RetrieveBlock); a filter that is not cached answers "may match" (FullFilterBlockReader).
    /// </summary>
    BlockCacheTier = 2,
    /// <summary>
    /// Bits holding the rate limiter priority the block reads are charged at (ReadOptions::rate_limiter_priority, stored
    /// as priority + 1; 0 = IO_TOTAL, not charged). Use <see cref="TableReadFlagsExtensions"/> to read and set it.
    /// </summary>
    RateLimiterPriorityMask = 0b11100,
    /// <summary>ReadOptions::read_index = kBuiltin: read through the standard index.</summary>
    BuiltinIndex = 0b100000,
    /// <summary>ReadOptions::read_index = kPreferCustom: read through the user-defined index when the table has one.</summary>
    PreferCustomIndex = 0b1000000,
    /// <summary>
    /// ReadOptions::verify_checksums = false: block checksums are not verified for blocks this read loads from the file
    /// (BlockFetcher::ReadBlockContents checks read_options.verify_checksums). When clear, blocks are verified unless
    /// the reader was opened with <see cref="TableReaderOptions.VerifyChecksums"/> = false.
    /// </summary>
    SkipChecksumVerification = 0b10000000,
    /// <summary>
    /// Version::IsFilterSkipped (optimize_filters_for_hits, last non-empty level): point lookups and seeks do not consult
    /// the table's filter (the skip_filters argument of TableCache::Get / MultiGet / NewIterator).
    /// </summary>
    SkipFilters = 0b1_00000000,
}

/// <summary>Helpers for the packed fields of <see cref="TableReadFlags"/>.</summary>
public static class TableReadFlagsExtensions
{
    private const int PriorityShift = 2;

    /// <summary>The flags with block reads charged to the rate limiter at <paramref name="priority"/> (Total: not charged).</summary>
    public static TableReadFlags WithRateLimiterPriority(this TableReadFlags flags, Env.IoPriority priority)
    {
        flags &= ~TableReadFlags.RateLimiterPriorityMask;
        return priority >= Env.IoPriority.Total ? flags : flags | (TableReadFlags)(((int)priority + 1) << PriorityShift);
    }

    /// <summary>The rate limiter priority of block reads (<see cref="Env.IoPriority.Total"/>: not charged).</summary>
    public static Env.IoPriority RateLimiterPriority(this TableReadFlags flags)
    {
        int v = ((int)(flags & TableReadFlags.RateLimiterPriorityMask)) >> PriorityShift;
        return v == 0 ? Env.IoPriority.Total : (Env.IoPriority)(v - 1);
    }

    /// <summary>
    /// The table-level flags of user read options: fill_cache, read_tier = kBlockCacheTier (no_io), verify_checksums,
    /// read_index and rate_limiter_priority (null options: the defaults).
    /// </summary>
    public static TableReadFlags FromReadOptions(ReadOptions? options)
    {
        if (options is null) return TableReadFlags.None;
        TableReadFlags f = options.FillCache ? TableReadFlags.None : TableReadFlags.DontFillCache;
        if (options.ReadTier == ReadTier.BlockCacheTier) f |= TableReadFlags.BlockCacheTier;
        if (!options.VerifyChecksums) f |= TableReadFlags.SkipChecksumVerification;
        f |= options.ReadIndex switch
        {
            ReadIndex.Builtin => TableReadFlags.BuiltinIndex,
            ReadIndex.PreferCustom => TableReadFlags.PreferCustomIndex,
            _ => TableReadFlags.None,
        };
        return f.WithRateLimiterPriority(options.RateLimiterPriority);
    }
}
