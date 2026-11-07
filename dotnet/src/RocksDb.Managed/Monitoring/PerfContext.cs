using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;

namespace RocksDb.Managed;

/// <summary>Port of rocksdb::PerfLevel (include/rocksdb/perf_level.h): how much a thread's <see cref="PerfContext"/> measures.</summary>
public enum PerfLevel : byte
{
    /// <summary>Unknown setting (kUninitialized); not accepted by <see cref="PerfContext.Level"/>.</summary>
    Uninitialized = 0,
    /// <summary>No perf stats (kDisable). The managed default (upstream's default is <see cref="EnableCount"/>).</summary>
    Disable = 1,
    /// <summary>Count metrics: names with "count" or "byte" (kEnableCount).</summary>
    EnableCount = 2,
    /// <summary>Plus the time user threads wait for RocksDB itself: "_[wait|delay]_*_[time|nanos]" (kEnableWait).</summary>
    EnableWait = 3,
    /// <summary>Plus the end-to-end time metrics, "time" / "nanos", except mutex times (kEnableTimeExceptForMutex).</summary>
    EnableTimeExceptForMutex = 4,
    /// <summary>Plus the CPU time metrics, "_cpu_*_[time|nanos]" (kEnableTimeAndCPUTimeExceptForMutex).</summary>
    EnableTimeAndCpuTimeExceptForMutex = 5,
    /// <summary>Plus mutex wait times, "_[mutex|condition]_*_[time|nanos]" (kEnableTime).</summary>
    EnableTime = 6,
}

/// <summary>
/// Port of rocksdb::PerfContext (include/rocksdb/perf_context.h, monitoring/perf_context.cc,
/// monitoring/perf_context_imp.h, monitoring/perf_step_timer.h): per-thread counters and timers of the operations the
/// current thread runs (Get, MultiGet, iteration, writes, table and block reads). Use it to explain the cost of single
/// operations: set <see cref="Level"/> on the thread, <see cref="Reset"/>, run the operation, then read the counters
/// (<see cref="this[PerfMetric]"/>, <see cref="ToString(bool)"/>).
/// <para>
/// <b>Threading</b>: like upstream the context and its level are thread-local (<see cref="Current"/>). An async
/// operation is measured on the threads its parts run on: use the synchronous APIs (or a single-threaded
/// synchronization context) for exact per-operation numbers. Background flushes and compactions are not attributed to
/// the caller. Reading another thread's context while it is updated gives approximate values.
/// </para>
/// <para>
/// <b>Cost</b>: with every thread at <see cref="PerfLevel.Disable"/> (the default) each instrumentation point is one
/// read of a static field; enabled, counters cost a thread-local lookup and timers a <see cref="Stopwatch"/> read.
/// </para>
/// <para>
/// Managed differences: the default level is <see cref="PerfLevel.Disable"/> (upstream: kEnableCount); the per-level
/// counters (EnablePerLevelPerfContext, level_to_perf_context) are not ported; db_mutex_lock_nanos and
/// db_condition_wait_nanos are not measured (the DB mutex is a <see cref="Lock"/>); the CPU-time metrics stay 0. The
/// metrics a managed code path updates are listed in docs/validation.md ("PerfContext / IOStatsContext"); the others
/// stay 0.
/// </para>
/// </summary>
public sealed partial class PerfContext
{
    [ThreadStatic] private static PerfContext? t_current;
    [ThreadStatic] private static PerfLevel t_level;

    // Number of threads whose level is above Disable: while it is 0 every instrumentation point is a single static read.
    internal static int s_enabledThreads;

    internal readonly ulong[] _values = new ulong[MetricCount];

    private PerfContext()
    {
    }

    /// <summary>The calling thread's context (get_perf_context()).</summary>
    public static PerfContext Current => t_current ??= new PerfContext();

    /// <summary>
    /// The calling thread's perf level (GetPerfLevel / SetPerfLevel). Setting <see cref="PerfLevel.Uninitialized"/> or an
    /// undefined value throws <see cref="ArgumentOutOfRangeException"/>.
    /// </summary>
    public static PerfLevel Level
    {
        get => t_level == PerfLevel.Uninitialized ? PerfLevel.Disable : t_level;
        set
        {
            if (value is <= PerfLevel.Uninitialized or > PerfLevel.EnableTime) throw new ArgumentOutOfRangeException(nameof(value));
            bool was = t_level > PerfLevel.Disable, now = value > PerfLevel.Disable;
            t_level = value;
            if (was != now)
            {
                if (now) Interlocked.Increment(ref s_enabledThreads);
                else Interlocked.Decrement(ref s_enabledThreads);
            }
        }
    }

    /// <summary>The value of <paramref name="metric"/> (nanoseconds for the time metrics).</summary>
    public ulong this[PerfMetric metric] => _values[(int)metric];

    /// <summary>PerfContext::Reset: every counter back to 0.</summary>
    public void Reset() => Array.Clear(_values);

    /// <summary>
    /// PerfContext::ToString: <c>"user_key_comparison_count = 3, block_cache_hit_count = 0, ..."</c> in upstream order,
    /// leaving out zero counters when <paramref name="excludeZeroCounters"/>.
    /// </summary>
    public string ToString(bool excludeZeroCounters)
    {
        var sb = new StringBuilder();
        for (int i = 0; i < MetricCount; i++)
        {
            ulong v = _values[i];
            if (excludeZeroCounters && v == 0) continue;
            if (sb.Length > 0) sb.Append(", ");
            sb.Append(s_names[i]).Append(" = ").Append(v.ToString(CultureInfo.InvariantCulture));
        }
        return sb.ToString();
    }

    /// <inheritdoc cref="ToString(bool)"/>
    public override string ToString() => ToString(excludeZeroCounters: false);

    // ---- instrumentation (perf_context_imp.h) --------------------------------------------------------------

    /// <summary>True when the calling thread counts (perf_level &gt;= kEnableCount). One static read when nothing is enabled.</summary>
    internal static bool CountEnabled
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => Volatile.Read(ref s_enabledThreads) != 0 && t_level >= PerfLevel.EnableCount;
    }

    /// <summary>PERF_COUNTER_ADD(metric, value).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void Add(PerfMetric metric, ulong value)
    {
        if (Volatile.Read(ref s_enabledThreads) != 0) AddSlow(metric, value);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void AddSlow(PerfMetric metric, ulong value)
    {
        if (t_level >= PerfLevel.EnableCount) Current._values[(int)metric] += value;
    }

    /// <summary>
    /// PERF_TIMER_GUARD start: a <see cref="Stopwatch"/> timestamp when the calling thread measures times at
    /// <paramref name="enableLevel"/> (default kEnableTimeExceptForMutex), else 0. Pair with <see cref="StopTimer"/>.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static long StartTimer(PerfLevel enableLevel = PerfLevel.EnableTimeExceptForMutex) =>
        Volatile.Read(ref s_enabledThreads) != 0 ? StartTimerSlow(enableLevel) : 0;

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static long StartTimerSlow(PerfLevel enableLevel) => t_level >= enableLevel ? Stopwatch.GetTimestamp() : 0;

    /// <summary>PerfStepTimer::Stop: adds the nanoseconds since <paramref name="start"/> (no-op for 0).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void StopTimer(PerfMetric metric, long start)
    {
        if (start != 0) StopTimerSlow(metric, start);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void StopTimerSlow(PerfMetric metric, long start) =>
        Current._values[(int)metric] += ElapsedNanos(start);

    /// <summary>
    /// BlockFetcher::ReadBlockContents: block_read_count, block_read_byte (block + trailer) and, from
    /// <paramref name="start"/> (<see cref="StartTimer"/>), block_read_time.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void RecordBlockRead(int bytesWithTrailer, long start)
    {
        if (Volatile.Read(ref s_enabledThreads) != 0) RecordBlockReadSlow(bytesWithTrailer, start);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void RecordBlockReadSlow(int bytesWithTrailer, long start)
    {
        if (t_level < PerfLevel.EnableCount) return;
        ulong[] v = Current._values;
        v[(int)PerfMetric.BlockReadCount]++;
        v[(int)PerfMetric.BlockReadByte] += (ulong)bytesWithTrailer;
        if (start != 0) v[(int)PerfMetric.BlockReadTime] += ElapsedNanos(start);
    }

    /// <summary>SystemClock::NowNanos() - start, from <see cref="Stopwatch"/> timestamps.</summary>
    internal static ulong ElapsedNanos(long start)
    {
        long d = Stopwatch.GetTimestamp() - start;
        if (d <= 0) return 0;
        return Stopwatch.Frequency == 1_000_000_000 ? (ulong)d : (ulong)(d * (1_000_000_000.0 / Stopwatch.Frequency));
    }
}

/// <summary>
/// Port of rocksdb::IOStatsContext (include/rocksdb/iostats_context.h, monitoring/iostats_context.cc): the calling
/// thread's file I/O bytes and times. Enabled by the thread's <see cref="PerfContext.Level"/>.
/// <para>
/// Managed differences: bytes are counted only while the thread's perf level is at least
/// <see cref="PerfLevel.EnableCount"/> (upstream counts them always), the times from
/// <see cref="PerfLevel.EnableTimeExceptForMutex"/> as upstream; thread_pool_id and the per-temperature counters are not
/// ported; allocate_nanos, range_sync_nanos, logger_nanos and the CPU nanos stay 0.
/// </para>
/// </summary>
public sealed partial class IOStatsContext
{
    [ThreadStatic] private static IOStatsContext? t_current;

    internal readonly ulong[] _values = new ulong[MetricCount];

    private IOStatsContext()
    {
    }

    /// <summary>The calling thread's context (get_iostats_context()).</summary>
    public static IOStatsContext Current => t_current ??= new IOStatsContext();

    /// <summary>The value of <paramref name="metric"/> (bytes, or nanoseconds for the time metrics).</summary>
    public ulong this[IOStatsMetric metric] => _values[(int)metric];

    /// <summary>IOStatsContext::Reset.</summary>
    public void Reset() => Array.Clear(_values);

    /// <summary>IOStatsContext::ToString (without thread_pool_id and the per-temperature counters).</summary>
    public string ToString(bool excludeZeroCounters)
    {
        var sb = new StringBuilder();
        // Upstream's output order: bytes_read before bytes_written, then the struct order.
        foreach (IOStatsMetric m in s_outputOrder)
        {
            ulong v = _values[(int)m];
            if (excludeZeroCounters && v == 0) continue;
            if (sb.Length > 0) sb.Append(", ");
            sb.Append(s_names[(int)m]).Append(" = ").Append(v.ToString(CultureInfo.InvariantCulture));
        }
        return sb.ToString();
    }

    /// <inheritdoc cref="ToString(bool)"/>
    public override string ToString() => ToString(excludeZeroCounters: false);

    private static readonly IOStatsMetric[] s_outputOrder =
    [
        IOStatsMetric.BytesRead, IOStatsMetric.BytesWritten, IOStatsMetric.OpenNanos, IOStatsMetric.AllocateNanos, IOStatsMetric.WriteNanos,
        IOStatsMetric.ReadNanos, IOStatsMetric.RangeSyncNanos, IOStatsMetric.FsyncNanos, IOStatsMetric.PrepareWriteNanos, IOStatsMetric.LoggerNanos,
        IOStatsMetric.CpuWriteNanos, IOStatsMetric.CpuReadNanos,
    ];

    /// <summary>IOSTATS_ADD(metric, value).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void Add(IOStatsMetric metric, ulong value)
    {
        if (Volatile.Read(ref PerfContext.s_enabledThreads) != 0) AddSlow(metric, value);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void AddSlow(IOStatsMetric metric, ulong value)
    {
        if (PerfContext.CountEnabled) Current._values[(int)metric] += value;
    }

    /// <summary>IOSTATS_TIMER_GUARD stop: adds the nanoseconds since <paramref name="start"/> (from <see cref="PerfContext.StartTimer"/>).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void StopTimer(IOStatsMetric metric, long start)
    {
        if (start != 0) StopTimerSlow(metric, start);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void StopTimerSlow(IOStatsMetric metric, long start) =>
        Current._values[(int)metric] += PerfContext.ElapsedNanos(start);
}
