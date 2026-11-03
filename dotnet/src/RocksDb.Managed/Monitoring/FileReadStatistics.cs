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
/// Timing decorator of a table file (RandomAccessFileReader::Read with its StopWatch, file/random_access_file_reader.cc):
/// every read records SST_READ_MICROS and the histogram of the current <see cref="FileReadActivity"/>
/// (FILE_READ_GET_MICROS, FILE_READ_MULTIGET_MICROS, FILE_READ_DB_ITERATOR_MICROS, FILE_READ_COMPACTION_MICROS,
/// FILE_READ_FLUSH_MICROS, FILE_READ_DB_OPEN_MICROS, FILE_READ_VERIFY_FILE_CHECKSUMS_MICROS), plus the per-level
/// file_read_hist_ of the reader (<paramref name="levelHistogram"/>, may be null).
/// Only installed when the DB has statistics; records nothing when timers are disabled by the stats level.
/// </summary>
internal sealed class StatisticsRandomAccessFile(IRandomAccessFile inner, Statistics statistics, HistogramImpl? levelHistogram) : IRandomAccessFile
{
    public StatisticsRandomAccessFile(IRandomAccessFile inner, Statistics statistics) : this(inner, statistics, null)
    {
    }

    public long Length => inner.Length;

    public int Read(long offset, Span<byte> destination)
    {
        long t0 = statistics.StartTimer();
        int activity = FileReadActivity.Current;
        int n = inner.Read(offset, destination);
        if (t0 != 0) Record(t0, activity);
        return n;
    }

    public ValueTask<int> ReadAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        long t0 = statistics.StartTimer();
        int activity = FileReadActivity.Current;
        ValueTask<int> t = inner.ReadAsync(offset, destination, cancellationToken);
        if (t0 == 0) return t;
        if (t.IsCompletedSuccessfully)
        {
            Record(t0, activity);
            return t;
        }
        return Await(t, t0, activity);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<int> Await(ValueTask<int> t, long t0, int activity)
    {
        int n = await t.ConfigureAwait(false);
        Record(t0, activity);
        return n;
    }

    private void Record(long t0, int activity)
    {
        ulong micros = (ulong)Stopwatch.GetElapsedTime(t0).Ticks / 10;
        statistics.RecordInHistogram(Histogram.SstReadMicros, micros);
        if (activity != 0) statistics.RecordInHistogram((Histogram)(activity - 1), micros);
        levelHistogram?.Add(micros);
    }

    public void Dispose() => inner.Dispose();
}
