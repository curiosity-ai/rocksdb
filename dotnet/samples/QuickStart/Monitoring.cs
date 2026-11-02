using RocksDb.Managed;
using RocksDb.Managed.Table;
using static QuickStart.Show;

namespace QuickStart;

/// <summary>Statistics, event listeners and the info LOG.</summary>
internal static class Monitoring
{
    // <snippet:listener-class>
    /// <summary>Listener callbacks run on background threads: keep them short and thread-safe.</summary>
    private sealed class PrintingListener : EventListener
    {
        public int Flushes;

        public override void OnFlushCompleted(RocksDatabase? db, FlushJobInfo info)
        {
            Interlocked.Increment(ref Flushes);
            Console.WriteLine($"flushed {info.ColumnFamilyName} -> {Path.GetFileName(info.FilePath)} ({info.FlushReason})");
        }

        public override void OnCompactionCompleted(RocksDatabase? db, CompactionJobInfo info) =>
            Console.WriteLine($"compacted L{info.BaseInputLevel} -> L{info.OutputLevel}: {info.InputFiles.Count} -> {info.OutputFiles.Count} files");

        public override void OnBackgroundError(BackgroundErrorEventArgs e) =>
            Console.WriteLine($"background error ({e.Reason}): {e.Error?.Message}");
    }

    /// <summary>Routes the DB's info log to the console instead of the LOG file.</summary>
    private sealed class ConsoleInfoLogger() : InfoLogger(InfoLogLevel.Warn)
    {
        protected override void LogCore(InfoLogLevel level, string message) => Console.WriteLine($"[{level}] {message}");
    }
    // </snippet:listener-class>

    public static Task StatisticsListenersLog(string path)
    {
        // <snippet:monitoring>
        var statistics = new Statistics();                  // all upstream tickers and histograms
        var listener = new PrintingListener();
        var options = new DbOptions { CreateIfMissing = true, Statistics = statistics };
        options.Listeners.Add(listener);
        // InfoLog = null (default) writes a native-style LOG file in the DB directory (rolled per MaxLogFileSize /
        // LogFileTimeToRoll, KeepLogFileNum old files kept); set InfoLog = new ConsoleInfoLogger() to redirect it.

        using RocksDatabase db = RocksDatabase.Open(path, options);
        for (int i = 0; i < 100; i++) db.Put(BitConverter.GetBytes(i), "value"u8);
        db.Get(BitConverter.GetBytes(7));
        db.Flush();

        Console.WriteLine(statistics.GetTickerCount(Ticker.NumberKeysWritten));          // 100
        HistogramData gets = statistics.GetHistogramData(Histogram.DbGet);                 // micros per Get
        Console.WriteLine($"get p99 {gets.Percentile99} us over {gets.Count} calls");
        string dump = statistics.ToString();                 // same text as native Statistics::ToString()

        Console.WriteLine(db.GetIntProperty(DbProperties.EstimateNumKeys));               // 100
        Dictionary<string, TableProperties> tables = db.GetPropertiesOfAllTables();
        // </snippet:monitoring>
        Line($"keys written {statistics.GetTickerCount(Ticker.NumberKeysWritten)}, flushes seen {listener.Flushes}, " +
             $"tables {tables.Count}, LOG exists {File.Exists(Path.Combine(path, "LOG"))}, stats text {dump.Length} chars");
        Check(statistics.GetTickerCount(Ticker.NumberKeysWritten) == 100 && listener.Flushes == 1, "statistics + listener");
        _ = new ConsoleInfoLogger();
        return Task.CompletedTask;
    }
}
