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
}

/// <summary>
/// Thread-safe block cache counters shared by any number of table readers (pass the same instance in
/// <see cref="TableReaderOptions.Statistics"/>). Counters are striped by processor so concurrent lookups on different
/// cores do not contend on one cache line (like upstream's per-core <c>StatisticsImpl</c>). Reads sum the stripes and
/// are not atomic snapshots across counters.
/// </summary>
public sealed class BlockCacheStatistics
{
    private const int NumCounters = 16;     // 16 * 8 bytes = 128 bytes per stripe (two cache lines, no false sharing)
    private const int NumStripes = 16;       // power of two
    private readonly long[] _counters = new long[NumStripes * NumCounters];

    /// <summary>
    /// The DB statistics these counters belong to (set by <see cref="Managed.Statistics"/>): secondary / persistent /
    /// simulation cache tickers are recorded there.
    /// </summary>
    internal Statistics? Owner { get; init; }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void Record(BlockCacheCounter counter) => Add(counter, 1);

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
    public long Hits => DataHits + IndexHits + FilterHits;

    /// <summary>Total misses over all block kinds (BLOCK_CACHE_MISS).</summary>
    public long Misses => DataMisses + IndexMisses + FilterMisses;

    /// <summary>Resets all counters to zero (not atomic with respect to concurrent updates).</summary>
    public void Reset()
    {
        for (int i = 0; i < _counters.Length; i++) Volatile.Write(ref _counters[i], 0);
    }
}

/// <summary>Per-call flags for table reads (the table-level subset of ReadOptions).</summary>
[Flags]
public enum TableReadFlags : byte
{
    /// <summary>Default: blocks read from the file are inserted into the block cache (ReadOptions::fill_cache = true).</summary>
    None = 0,
    /// <summary>ReadOptions::fill_cache = false: look blocks up in the cache, but do not insert blocks read on a miss.</summary>
    DontFillCache = 1,
    /// <summary>
    /// ReadOptions::read_tier = kBlockCacheTier for filter checks: no file I/O; a filter (or partitioned filter index)
    /// that is neither pinned nor in the block cache answers "may match" (upstream returns Incomplete).
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
    /// The table-level flags of user read options: fill_cache and rate_limiter_priority (null options: the defaults).
    /// </summary>
    public static TableReadFlags FromReadOptions(ReadOptions? options)
    {
        if (options is null) return TableReadFlags.None;
        TableReadFlags f = options.FillCache ? TableReadFlags.None : TableReadFlags.DontFillCache;
        f |= options.ReadIndex switch
        {
            ReadIndex.Builtin => TableReadFlags.BuiltinIndex,
            ReadIndex.PreferCustom => TableReadFlags.PreferCustomIndex,
            _ => TableReadFlags.None,
        };
        return f.WithRateLimiterPriority(options.RateLimiterPriority);
    }
}
