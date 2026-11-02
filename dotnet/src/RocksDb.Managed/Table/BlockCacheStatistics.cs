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
}
