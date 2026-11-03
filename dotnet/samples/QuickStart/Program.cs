using System.Diagnostics;

namespace QuickStart;

/// <summary>
/// Runs every example of dotnet/README.md against a fresh database directory under the system temp directory and
/// prints what each one observed. Exits with code 1 if an example throws. Pass example names (e.g. <c>Basics
/// Transactions</c>) to run a subset.
/// </summary>
internal static class Program
{
    private static readonly (string Name, Func<string, Task> Run)[] s_examples =
    [
        ("OpenSync", Basics.OpenSync),
        ("OpenAsync", Basics.OpenAsync),
        ("TypedOptions", Basics.TypedOptions),
        ("ColumnFamilies", Basics.ColumnFamilies),
        ("ReadVariants", Basics.ReadVariants),
        ("WriteBatch", Batches.WriteBatches),
        ("MultiGetBatch", Batches.MultiGet),
        ("Iterators", Reading.Iterators),
        ("Snapshots", Reading.Snapshots),
        ("MergeOperators", Reading.MergeOperators),
        ("PrefixSeek", Reading.PrefixSeek),
        ("TableFormats", Reading.TableFormats),
        ("Compaction", Maintenance.Compaction),
        ("OptimisticTransactions", Transactions.Optimistic),
        ("PessimisticTransactions", Transactions.Pessimistic),
        ("RangeLocks", Transactions.RangeLocks),
        ("TransactionEntities", Transactions.TransactionEntities),
        ("TwoPhaseCommit", Transactions.TwoPhaseCommit),
        ("WriteBatchWithIndex", Transactions.WriteBatchWithIndexExample),
        ("WideColumns", DataModels.WideColumns),
        ("AttributeGroups", DataModels.AttributeGroups),
        ("UserDefinedTimestamps", DataModels.UserDefinedTimestamps),
        ("BlobFiles", DataModels.BlobFiles),
        ("Ttl", DataModels.Ttl),
        ("CheckpointBackupRestore", Maintenance.CheckpointBackupRestore),
        ("SstFileIngest", Maintenance.SstFileIngest),
        ("ExportImport", DbOperations.ExportImport),
        ("RemoteCompaction", DbOperations.RemoteCompaction),
        ("MergeOperandsAndScans", DbOperations.MergeOperandsAndScans),
        ("FileLevelOperations", DbOperations.FileLevelOperations),
        ("WalTailing", Replication.WalTailing),
        ("ReadOnlyAndSecondary", Replication.ReadOnlyAndSecondary),
        ("Monitoring", Monitoring.StatisticsListenersLog),
        ("ErrorHandling", Errors.ErrorHandling),
        ("Tracing", Tracing.TraceAndReplay),
    ];

    public static async Task<int> Main(string[] args)
    {
        string root = Path.Combine(Path.GetTempPath(), "rocksdb-managed-quickstart-" + Environment.ProcessId);
        int failures = 0, ran = 0;
        var total = Stopwatch.StartNew();
        try
        {
            foreach ((string name, Func<string, Task> run) in s_examples)
            {
                if (args.Length > 0 && Array.IndexOf(args, name) < 0) continue;
                string dir = Path.Combine(root, name);
                Directory.CreateDirectory(dir);
                Console.WriteLine($"== {name}");
                var sw = Stopwatch.StartNew();
                try
                {
                    await run(dir);
                    Console.WriteLine($"   ok ({sw.ElapsedMilliseconds} ms)");
                }
                catch (Exception e)
                {
                    failures++;
                    Console.WriteLine($"   FAILED: {e}");
                }
                ran++;
            }
        }
        finally
        {
            try { Directory.Delete(root, recursive: true); } catch (IOException) { } catch (UnauthorizedAccessException) { }
        }
        Console.WriteLine($"{ran - failures}/{ran} examples passed in {total.ElapsedMilliseconds} ms");
        return failures == 0 ? 0 : 1;
    }
}

/// <summary>Small helpers shared by the examples.</summary>
internal static class Show
{
    public static string Utf8(ReadOnlySpan<byte> bytes) => System.Text.Encoding.UTF8.GetString(bytes);

    public static string Utf8(byte[]? bytes) => bytes is null ? "<null>" : System.Text.Encoding.UTF8.GetString(bytes);

    public static void Line(string text) => Console.WriteLine("   " + text);

    /// <summary>Throws if <paramref name="condition"/> is false, so a regression makes the sample exit non-zero.</summary>
    public static void Check(bool condition, string what)
    {
        if (!condition) throw new InvalidOperationException("check failed: " + what);
    }
}
