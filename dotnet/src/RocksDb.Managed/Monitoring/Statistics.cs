using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using RocksDb.Managed.Table;

namespace RocksDb.Managed;

/// <summary>
/// How much the DB records into <see cref="Statistics"/>. Port of rocksdb::StatsLevel (include/rocksdb/statistics.h);
/// numeric values match upstream.
/// </summary>
public enum StatsLevel : byte
{
    /// <summary>Disable all metrics.</summary>
    DisableAll = 0,
    /// <summary>Disable tickers.</summary>
    ExceptTickers = DisableAll,
    /// <summary>Disable timer stats and skip histogram stats.</summary>
    ExceptHistogramOrTimers = 1,
    /// <summary>Skip timer stats.</summary>
    ExceptTimers = 2,
    /// <summary>Collect all stats except time inside mutex lock and time spent on compression (the default).</summary>
    ExceptDetailedTimers = 3,
    /// <summary>Collect all stats except the counters requiring to get time inside the mutex lock.</summary>
    ExceptTimeForMutex = 4,
    /// <summary>Collect all stats, including measuring the duration of mutex operations.</summary>
    All = 5,
}

/// <summary>Summary of one histogram (rocksdb::HistogramData).</summary>
public readonly record struct HistogramData(
    double Median, double Percentile95, double Percentile99, double Average, double StandardDeviation,
    double Max, ulong Count, ulong Sum, double Min);

/// <summary>
/// DB statistics: tickers (monotonic counters) and histograms. Port of rocksdb::Statistics / StatisticsImpl
/// (include/rocksdb/statistics.h, monitoring/statistics.cc, monitoring/histogram.cc). Assign one instance to
/// <see cref="DbOptions.Statistics"/> (it may be shared by several DBs); statistics are off when it is null (the
/// default).
/// <para>
/// Recording is cheap and allocation-free: tickers and histogram buckets are <c>long</c> counters striped by processor
/// (like upstream's per-core storage) and updated with <see cref="Interlocked"/>; reads sum the stripes, so a read
/// is not an atomic snapshot across counters. Thread-safe. <see cref="ToString"/> produces the same text format as
/// native <c>Statistics::ToString</c> (<c>rocksdb_options_statistics_get_string</c>), listing every upstream ticker
/// and histogram name; tickers the managed port does not record stay 0.
/// </para>
/// </summary>
public sealed class Statistics
{
    private const int NumStripes = 16; // power of two
    // Ticker stride per stripe, rounded to whole cache lines (8 longs) so that stripes never share a line.
    private const int TickerStride = (StatisticsNames.TickerCount + 7) & ~7;

    private readonly long[] _tickers = new long[NumStripes * TickerStride];
    private readonly HistogramImpl[] _histograms;

    /// <summary>Block cache / filter counters fed by the table readers of DBs using this instance.</summary>
    internal readonly BlockCacheStatistics BlockCacheCounters = new();

    public Statistics()
    {
        _histograms = new HistogramImpl[StatisticsNames.HistogramCount];
        for (int i = 0; i < _histograms.Length; i++) _histograms[i] = new HistogramImpl();
    }

    /// <summary>What is recorded (default <see cref="StatsLevel.ExceptDetailedTimers"/>). May be changed at any time.</summary>
    public StatsLevel StatsLevel { get; set; } = StatsLevel.ExceptDetailedTimers;

    /// <summary>Whether histograms (and timers) are recorded at the current level (Statistics::HistEnabledForType).</summary>
    public bool HistogramsEnabled
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => StatsLevel > StatsLevel.ExceptHistogramOrTimers;
    }

    /// <summary>Whether tickers are recorded at the current level.</summary>
    public bool TickersEnabled
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => StatsLevel > StatsLevel.ExceptTickers;
    }

    // ---- tickers ------------------------------------------------------------------------------------

    /// <summary>Adds 1 to <paramref name="ticker"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void RecordTick(Ticker ticker) => RecordTick(ticker, 1);

    /// <summary>Adds <paramref name="count"/> to <paramref name="ticker"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void RecordTick(Ticker ticker, ulong count)
    {
        if (StatsLevel <= StatsLevel.ExceptTickers || (uint)ticker >= StatisticsNames.TickerCount) return;
        int stripe = Thread.GetCurrentProcessorId() & (NumStripes - 1);
        Interlocked.Add(ref _tickers[stripe * TickerStride + (int)ticker], (long)count);
    }

    /// <summary>Sets <paramref name="ticker"/> to <paramref name="count"/> (Statistics::setTickerCount).</summary>
    public void SetTickerCount(Ticker ticker, ulong count)
    {
        ValidateTicker(ticker);
        for (int s = 0; s < NumStripes; s++) Volatile.Write(ref _tickers[s * TickerStride + (int)ticker], 0);
        Volatile.Write(ref _tickers[(int)ticker], (long)count);
    }

    /// <summary>Current value of <paramref name="ticker"/> (sum over all stripes).</summary>
    public ulong GetTickerCount(Ticker ticker)
    {
        ValidateTicker(ticker);
        long sum = 0;
        for (int s = 0; s < NumStripes; s++) sum += Volatile.Read(ref _tickers[s * TickerStride + (int)ticker]);
        sum += BlockCacheTicker(ticker);
        return (ulong)sum;
    }

    /// <summary>
    /// Returns the value of <paramref name="ticker"/> and resets it to 0 (Statistics::getAndResetTickerCount). The block
    /// cache and filter tickers are shared table-reader counters that are only reset by <see cref="Reset"/>.
    /// </summary>
    public ulong GetAndResetTickerCount(Ticker ticker)
    {
        ValidateTicker(ticker);
        long sum = 0;
        for (int s = 0; s < NumStripes; s++) sum += Interlocked.Exchange(ref _tickers[s * TickerStride + (int)ticker], 0);
        sum += BlockCacheTicker(ticker);
        return (ulong)sum;
    }

    private static void ValidateTicker(Ticker ticker)
    {
        if ((uint)ticker >= StatisticsNames.TickerCount) throw new ArgumentOutOfRangeException(nameof(ticker));
    }

    /// <summary>The table-reader counters behind the BLOCK_CACHE_* / BLOOM_FILTER_* tickers.</summary>
    private long BlockCacheTicker(Ticker ticker)
    {
        BlockCacheStatistics b = BlockCacheCounters;
        return ticker switch
        {
            Ticker.BlockCacheMiss => b.Misses,
            Ticker.BlockCacheHit => b.Hits,
            Ticker.BlockCacheAdd => b.Get(BlockCacheCounter.DataAdd) + b.Get(BlockCacheCounter.IndexAdd) + b.Get(BlockCacheCounter.FilterAdd),
            Ticker.BlockCacheAddFailures => b.Get(BlockCacheCounter.AddFailure),
            Ticker.BlockCacheIndexMiss => b.IndexMisses,
            Ticker.BlockCacheIndexHit => b.IndexHits,
            Ticker.BlockCacheIndexAdd => b.Get(BlockCacheCounter.IndexAdd),
            Ticker.BlockCacheFilterMiss => b.FilterMisses,
            Ticker.BlockCacheFilterHit => b.FilterHits,
            Ticker.BlockCacheFilterAdd => b.Get(BlockCacheCounter.FilterAdd),
            Ticker.BlockCacheDataMiss => b.DataMisses,
            Ticker.BlockCacheDataHit => b.DataHits,
            Ticker.BlockCacheDataAdd => b.Get(BlockCacheCounter.DataAdd),
            Ticker.BlockCacheBytesWrite => b.Get(BlockCacheCounter.BytesInserted),
            Ticker.BloomFilterUseful => b.Get(BlockCacheCounter.BloomFilterUseful),
            Ticker.BloomFilterFullPositive => b.Get(BlockCacheCounter.BloomFilterFullPositive),
            Ticker.BloomFilterPrefixChecked => b.Get(BlockCacheCounter.BloomFilterPrefixChecked),
            Ticker.BloomFilterPrefixUseful => b.Get(BlockCacheCounter.BloomFilterPrefixUseful),
            _ => 0,
        };
    }

    /// <summary>All tickers by upstream name (Statistics::getTickerMap).</summary>
    public Dictionary<string, ulong> GetTickerMap()
    {
        var map = new Dictionary<string, ulong>(StatisticsNames.TickerCount, StringComparer.Ordinal);
        for (int t = 0; t < StatisticsNames.TickerCount; t++) map[StatisticsNames.Tickers[t]] = GetTickerCount((Ticker)t);
        return map;
    }

    /// <summary>The upstream name of a ticker, e.g. "rocksdb.block.cache.miss".</summary>
    public static string GetTickerName(Ticker ticker)
    {
        ValidateTicker(ticker);
        return StatisticsNames.Tickers[(int)ticker];
    }

    /// <summary>The upstream name of a histogram, e.g. "rocksdb.db.get.micros".</summary>
    public static string GetHistogramName(Histogram histogram)
    {
        if ((uint)histogram >= StatisticsNames.HistogramCount) throw new ArgumentOutOfRangeException(nameof(histogram));
        return StatisticsNames.Histograms[(int)histogram];
    }

    // ---- histograms ---------------------------------------------------------------------------------

    /// <summary>Records <paramref name="value"/> into <paramref name="histogram"/> (Statistics::recordInHistogram).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void RecordInHistogram(Histogram histogram, ulong value)
    {
        if (StatsLevel <= StatsLevel.ExceptHistogramOrTimers || (uint)histogram >= StatisticsNames.HistogramCount) return;
        _histograms[(int)histogram].Add(value);
    }

    /// <summary>Records the time elapsed since <paramref name="startTimestamp"/> (<see cref="System.Diagnostics.Stopwatch.GetTimestamp"/>), in microseconds.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void RecordElapsedMicros(Histogram histogram, long startTimestamp)
    {
        if (startTimestamp == 0) return;
        RecordInHistogram(histogram, (ulong)System.Diagnostics.Stopwatch.GetElapsedTime(startTimestamp).Ticks / 10);
    }

    /// <summary>A start timestamp for <see cref="RecordElapsedMicros"/>, or 0 when timers are not recorded.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal long StartTimer() => StatsLevel > StatsLevel.ExceptTimers ? System.Diagnostics.Stopwatch.GetTimestamp() : 0;

    /// <summary>Summary of <paramref name="histogram"/> (Statistics::histogramData).</summary>
    public HistogramData GetHistogramData(Histogram histogram)
    {
        if ((uint)histogram >= StatisticsNames.HistogramCount) throw new ArgumentOutOfRangeException(nameof(histogram));
        return _histograms[(int)histogram].Data();
    }

    /// <summary>HistogramImpl::ToString for one histogram.</summary>
    public string GetHistogramString(Histogram histogram)
    {
        if ((uint)histogram >= StatisticsNames.HistogramCount) throw new ArgumentOutOfRangeException(nameof(histogram));
        return _histograms[(int)histogram].ToString();
    }

    /// <summary>Resets every ticker and histogram (Statistics::Reset).</summary>
    public void Reset()
    {
        for (int i = 0; i < _tickers.Length; i++) Volatile.Write(ref _tickers[i], 0);
        foreach (HistogramImpl h in _histograms) h.Clear();
        BlockCacheCounters.Reset();
    }

    /// <summary>
    /// All tickers ("name COUNT : n") and histograms ("name P50 : x P95 : x P99 : x P100 : x COUNT : n SUM : n"), one
    /// per line, in upstream order. Port of StatisticsImpl::ToString.
    /// </summary>
    public override string ToString()
    {
        var sb = new StringBuilder(20000);
        for (int t = 0; t < StatisticsNames.TickerCount; t++)
        {
            sb.Append(StatisticsNames.Tickers[t]).Append(" COUNT : ").Append(GetTickerCount((Ticker)t).ToString(CultureInfo.InvariantCulture)).Append('\n');
        }
        for (int h = 0; h < StatisticsNames.HistogramCount; h++)
        {
            HistogramData d = _histograms[h].Data();
            sb.Append(StatisticsNames.Histograms[h])
                .Append(" P50 : ").Append(Fixed6(d.Median))
                .Append(" P95 : ").Append(Fixed6(d.Percentile95))
                .Append(" P99 : ").Append(Fixed6(d.Percentile99))
                .Append(" P100 : ").Append(Fixed6(d.Max))
                .Append(" COUNT : ").Append(d.Count.ToString(CultureInfo.InvariantCulture))
                .Append(" SUM : ").Append(d.Sum.ToString(CultureInfo.InvariantCulture))
                .Append('\n');
        }
        return sb.ToString();
    }

    /// <summary>printf("%f").</summary>
    internal static string Fixed6(double v) => v.ToString("F6", CultureInfo.InvariantCulture);
}

/// <summary>
/// Bucket boundaries of every histogram. Port of HistogramBucketMapper (monitoring/histogram.cc): 1, 2, then x1.5 rounded
/// down to two significant digits, up to the largest value representable in a uint64.
/// </summary>
internal static class HistogramBuckets
{
    public static readonly ulong[] Limits = Build();
    public static int Count => Limits.Length;

    private static ulong[] Build()
    {
        var values = new List<ulong> { 1, 2 };
        double bucketVal = values[^1];
        while ((bucketVal = 1.5 * bucketVal) <= ulong.MaxValue)
        {
            ulong v = (ulong)bucketVal;
            // Extracts two most significant digits to make histogram buckets more human-readable. E.g., 172 becomes 170.
            ulong powOfTen = 1;
            while (v / 10 > 10)
            {
                v /= 10;
                powOfTen *= 10;
            }
            values.Add(v * powOfTen);
        }
        return values.ToArray();
    }

    /// <summary>HistogramBucketMapper::IndexForValue (lower_bound).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int IndexForValue(ulong value)
    {
        ulong[] l = Limits;
        if (value >= l[^1]) return l.Length - 1;
        int lo = 0, hi = l.Length - 1;
        while (lo < hi)
        {
            int mid = (lo + hi) >>> 1;
            if (l[mid] < value) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }
}

/// <summary>
/// One histogram: count, sum, sum of squares, min, max and bucket counts, striped by processor. Port of
/// HistogramStat / HistogramImpl (monitoring/histogram.cc).
/// </summary>
internal sealed class HistogramImpl
{
    private const int NumStripes = 8; // power of two
    private static readonly int s_stride = (HistogramBuckets.Count + 4 + 7) & ~7; // [num, sum, min, max, buckets...]

    private readonly long[] _data = new long[NumStripes * s_stride];
    private readonly double[] _sumSquares = new double[NumStripes * 8];

    public HistogramImpl() => Clear();

    public void Clear()
    {
        for (int s = 0; s < NumStripes; s++)
        {
            int b = s * s_stride;
            Array.Clear(_data, b, s_stride);
            Volatile.Write(ref _data[b + 2], long.MaxValue); // min (as ulong max)
            Volatile.Write(ref _sumSquares[s * 8], 0);
        }
    }

    public void Add(ulong value)
    {
        int stripe = Thread.GetCurrentProcessorId() & (NumStripes - 1);
        int b = stripe * s_stride;
        Interlocked.Increment(ref _data[b + 4 + HistogramBuckets.IndexForValue(value)]);
        long v = (long)Math.Min(value, (ulong)long.MaxValue);
        // min / max: CAS loops (rarely retried).
        long cur = Volatile.Read(ref _data[b + 2]);
        while (v < cur)
        {
            long o = Interlocked.CompareExchange(ref _data[b + 2], v, cur);
            if (o == cur) break;
            cur = o;
        }
        cur = Volatile.Read(ref _data[b + 3]);
        while (v > cur)
        {
            long o = Interlocked.CompareExchange(ref _data[b + 3], v, cur);
            if (o == cur) break;
            cur = o;
        }
        Interlocked.Add(ref _data[b + 1], v);
        double sq = (double)v * v;
        ref double ss = ref _sumSquares[stripe * 8];
        double old = Volatile.Read(ref ss);
        while (true)
        {
            double o = Interlocked.CompareExchange(ref ss, old + sq, old);
            if (o.Equals(old)) break;
            old = o;
        }
        Interlocked.Increment(ref _data[b]); // num last: a reader seeing num sees its bucket
    }

    private void Merge(out ulong num, out ulong sum, out double sumSquares, out ulong min, out ulong max, ulong[] buckets)
    {
        num = 0;
        sum = 0;
        sumSquares = 0;
        min = ulong.MaxValue;
        max = 0;
        for (int s = 0; s < NumStripes; s++)
        {
            int b = s * s_stride;
            num += (ulong)Volatile.Read(ref _data[b]);
            sum += (ulong)Volatile.Read(ref _data[b + 1]);
            min = Math.Min(min, (ulong)Volatile.Read(ref _data[b + 2]));
            max = Math.Max(max, (ulong)Volatile.Read(ref _data[b + 3]));
            sumSquares += Volatile.Read(ref _sumSquares[s * 8]);
            for (int i = 0; i < buckets.Length; i++) buckets[i] += (ulong)Volatile.Read(ref _data[b + 4 + i]);
        }
        if (min == (ulong)long.MaxValue) min = HistogramBuckets.Limits[^1]; // HistogramStat::Clear: min_ = bucketMapper.LastValue()
    }

    /// <summary>HistogramStat::Percentile.</summary>
    private static double Percentile(double p, ulong num, ulong min, ulong max, ulong[] buckets)
    {
        double threshold = num * (p / 100.0);
        ulong cumulativeSum = 0;
        for (int b = 0; b < buckets.Length; b++)
        {
            ulong bucketValue = buckets[b];
            cumulativeSum += bucketValue;
            if (cumulativeSum >= threshold)
            {
                // Scale linearly within this bucket
                ulong leftPoint = b == 0 ? 0 : HistogramBuckets.Limits[b - 1];
                ulong rightPoint = HistogramBuckets.Limits[b];
                ulong leftSum = cumulativeSum - bucketValue;
                ulong rightSum = cumulativeSum;
                double pos = 0;
                ulong rightLeftDiff = rightSum - leftSum;
                if (rightLeftDiff != 0) pos = (threshold - leftSum) / rightLeftDiff;
                double r = leftPoint + (rightPoint - leftPoint) * pos;
                if (r < min) r = min;
                if (r > max) r = max;
                return r;
            }
        }
        return max;
    }

    /// <summary>HistogramStat::Data.</summary>
    public HistogramData Data()
    {
        var buckets = new ulong[HistogramBuckets.Count];
        Merge(out ulong num, out ulong sum, out double sumSquares, out ulong min, out ulong max, buckets);
        double avg = num == 0 ? 0 : (double)sum / num;
        double stdDev = 0;
        if (num != 0)
        {
            double variance = (sumSquares * num - (double)sum * sum) / ((double)num * num);
            stdDev = Math.Sqrt(Math.Max(0, variance));
        }
        return new HistogramData(
            Percentile(50, num, min, max, buckets), Percentile(95, num, min, max, buckets), Percentile(99, num, min, max, buckets),
            avg, stdDev, max, num, sum, num == 0 ? 0 : min);
    }

    /// <summary>HistogramStat::ToString.</summary>
    public override string ToString()
    {
        var buckets = new ulong[HistogramBuckets.Count];
        Merge(out ulong num, out ulong sum, out _, out ulong min, out ulong max, buckets);
        HistogramData d = Data();
        var sb = new StringBuilder();
        sb.Append(string.Format(CultureInfo.InvariantCulture, "Count: {0} Average: {1:F4}  StdDev: {2:F2}\n", num, d.Average, d.StandardDeviation));
        sb.Append(string.Format(CultureInfo.InvariantCulture, "Min: {0}  Median: {1:F4}  Max: {2}\n", num == 0 ? 0 : min, d.Median, num == 0 ? 0 : max));
        sb.Append(string.Format(CultureInfo.InvariantCulture, "Percentiles: P50: {0:F2} P75: {1:F2} P99: {2:F2} P99.9: {3:F2} P99.99: {4:F2}\n",
            d.Median, Percentile(75, num, min, max, buckets), d.Percentile99, Percentile(99.9, num, min, max, buckets),
            Percentile(99.99, num, min, max, buckets)));
        sb.Append("------------------------------------------------------\n");
        if (num == 0) return sb.ToString();
        double mult = 100.0 / num;
        ulong cumulative = 0;
        for (int b = 0; b < buckets.Length; b++)
        {
            ulong bucketValue = buckets[b];
            if (bucketValue == 0) continue;
            cumulative += bucketValue;
            sb.Append(string.Format(CultureInfo.InvariantCulture, "{0} {1,7} , {2,7} ] {3,8} {4,7:F3}% {5,7:F3}% ",
                b == 0 ? '[' : '(', b == 0 ? 0 : HistogramBuckets.Limits[b - 1], HistogramBuckets.Limits[b], bucketValue,
                mult * bucketValue, mult * cumulative));
            int marks = (int)(20 * ((double)bucketValue / num) + 0.5);
            sb.Append('#', marks).Append('\n');
        }
        return sb.ToString();
    }
}
