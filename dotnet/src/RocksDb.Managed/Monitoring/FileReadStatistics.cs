using System.Diagnostics;
using System.Runtime.CompilerServices;
using RocksDb.Managed.Env;

namespace RocksDb.Managed.Monitoring;

/// <summary>
/// The I/O activity of the current thread (Env::IOActivity carried by ReadOptions::io_activity upstream), used to pick
/// the per-activity file read histogram (GetFileReadHistograms, file/random_access_file_reader.cc). Set around
/// synchronous Get / MultiGet / iterator positioning (when statistics are enabled) and compaction jobs; reads issued
/// from other contexts (e.g. async continuations) count as unknown and only record SST_READ_MICROS, like upstream's
/// kUnknown activity.
/// </summary>
internal static class FileReadActivity
{
    /// <summary>Histogram + 1 of the current activity; 0 = unknown.</summary>
    [ThreadStatic] private static int t_current;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int Enter(Histogram histogram)
    {
        int previous = t_current;
        t_current = (int)histogram + 1;
        return previous;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Exit(int previous) => t_current = previous;

    public static int Current => t_current;
}

/// <summary>
/// Timing decorator of a file read through RandomAccessFileReader (its StopWatch in Read / ReadAsync,
/// file/random_access_file_reader.cc): every read records the reader's hist_type_ (<paramref name="readHistogram"/>:
/// SST_READ_MICROS for table files, BLOB_DB_BLOB_FILE_READ_MICROS for blob files) and the histogram of the current
/// <see cref="FileReadActivity"/> (FILE_READ_GET_MICROS, FILE_READ_MULTIGET_MICROS, FILE_READ_DB_ITERATOR_MICROS,
/// FILE_READ_COMPACTION_MICROS, FILE_READ_FLUSH_MICROS, FILE_READ_DB_OPEN_MICROS, FILE_READ_VERIFY_FILE_CHECKSUMS_MICROS),
/// plus the reader's file_read_hist_ (<paramref name="fileHistogram"/>: the per-level table histogram or the blob file
/// histogram of InternalStats, may be null).
/// Only installed when the DB has statistics; records nothing when timers are disabled by the stats level.
/// </summary>
internal sealed class StatisticsRandomAccessFile(IRandomAccessFile inner, Statistics statistics, HistogramImpl? fileHistogram, Histogram readHistogram)
    : IRandomAccessFile
{
    /// <summary>A table file reader (SST_READ_MICROS).</summary>
    public StatisticsRandomAccessFile(IRandomAccessFile inner, Statistics statistics) : this(inner, statistics, null, Histogram.SstReadMicros)
    {
    }

    /// <summary>A table file reader (SST_READ_MICROS) with its per-level histogram.</summary>
    public StatisticsRandomAccessFile(IRandomAccessFile inner, Statistics statistics, HistogramImpl? levelHistogram)
        : this(inner, statistics, levelHistogram, Histogram.SstReadMicros)
    {
    }

    /// <summary>
    /// The file is a table file of the last level (RandomAccessFileReader's is_last_level, "level == num_levels - 1"
    /// when TableCache::GetTableReader opened it): its reads count in LAST_LEVEL_READ_BYTES / _COUNT, those of every
    /// other file in NON_LAST_LEVEL_READ_BYTES / _COUNT (RecordIOStats, file/random_access_file_reader.cc).
    /// </summary>
    public bool IsLastLevel { get; init; }

    public long Length => inner.Length;

    private void RecordIoStats(int bytes)
    {
        statistics.RecordTick(IsLastLevel ? Ticker.LastLevelReadBytes : Ticker.NonLastLevelReadBytes, (ulong)Math.Max(bytes, 0));
        statistics.RecordTick(IsLastLevel ? Ticker.LastLevelReadCount : Ticker.NonLastLevelReadCount);
    }

    public int Read(long offset, Span<byte> destination)
    {
        long t0 = statistics.StartTimer();
        int activity = FileReadActivity.Current;
        int n = inner.Read(offset, destination);
        RecordIoStats(n);
        if (t0 != 0) Record(t0, activity);
        return n;
    }

    public ValueTask<int> ReadAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        long t0 = statistics.StartTimer();
        int activity = FileReadActivity.Current;
        ValueTask<int> t = inner.ReadAsync(offset, destination, cancellationToken);
        if (t.IsCompletedSuccessfully)
        {
            int n = t.Result;
            RecordIoStats(n);
            if (t0 != 0) Record(t0, activity);
            return new ValueTask<int>(n);
        }
        return Await(t, t0, activity);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<int> Await(ValueTask<int> t, long t0, int activity)
    {
        int n = await t.ConfigureAwait(false);
        RecordIoStats(n);
        if (t0 != 0) Record(t0, activity);
        return n;
    }

    private void Record(long t0, int activity)
    {
        ulong micros = (ulong)Stopwatch.GetElapsedTime(t0).Ticks / 10;
        statistics.RecordInHistogram(readHistogram, micros);
        if (activity != 0) statistics.RecordInHistogram((Histogram)(activity - 1), micros);
        fileHistogram?.Add(micros);
    }

    public bool Prefetch(long offset, long length) => inner.Prefetch(offset, length);

    public void Dispose() => inner.Dispose();
}
