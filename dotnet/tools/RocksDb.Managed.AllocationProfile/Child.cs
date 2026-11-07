using System.Diagnostics;
using System.Text.Json;

namespace RocksDb.Managed.AllocationProfile;

/// <summary>Exact numbers of one scenario's measured phase (child side).</summary>
internal sealed class ExactResult
{
    public long Ops { get; set; }
    public double Seconds { get; set; }
    public double ProcessBytesPerOp { get; set; }
    public double ThreadBytesPerOp { get; set; }
    public long ProcessBytes { get; set; }
    public double NsPerOp { get; set; }
    public int Gen0 { get; set; }
    public int Gen1 { get; set; }
    public int Gen2 { get; set; }
    public long WarmupOps { get; set; }
    public double SetupSeconds { get; set; }
}

/// <summary>Allocation total while the parent traced (child side).</summary>
internal sealed class WindowResult
{
    public long Ops { get; set; }
    public long ProcessBytes { get; set; }
    public double ProcessBytesPerOp { get; set; }
}

/// <summary>
/// Child process: setup, warm-up, exact measured phase, then (when traced) a steady-state loop until the parent
/// writes <c>STOP</c> to stdin. Protocol lines on stdout start with <c>@@</c>; everything else is a log line.
/// </summary>
internal static class Child
{
    public const string ResultTag = "@@RESULT ";
    public const string ReadyTag = "@@READY";
    public const string WindowTag = "@@WINDOW ";
    public const string ErrorTag = "@@ERROR ";

    private static volatile bool s_stop;

    public static async Task<int> RunAsync(Scenario scenario, string workDirectory, double warmupSeconds, double measureSeconds, bool traced)
    {
        var ctx = new ScenarioContext(workDirectory);
        try
        {
            var setupWatch = Stopwatch.StartNew();
            Runner runner = await scenario.Setup(ctx).ConfigureAwait(false);
            double setupSeconds = setupWatch.Elapsed.TotalSeconds;
            int chunk = scenario.ChunkOps;

            // Warm-up: at least two chunks and warmupSeconds (tiering, pools, caches, memtable rotations).
            long warmOps = 0;
            var sw = Stopwatch.StartNew();
            do
            {
                await runner.Run(chunk).ConfigureAwait(false);
                warmOps += chunk;
            } while (sw.Elapsed.TotalSeconds < warmupSeconds || warmOps < 2L * chunk);

            // Measured phase. No console output or other allocations by the harness inside.
            int g0 = GC.CollectionCount(0), g1 = GC.CollectionCount(1), g2 = GC.CollectionCount(2);
            long thread0 = GC.GetAllocatedBytesForCurrentThread();
            long proc0 = GC.GetTotalAllocatedBytes(precise: true);
            long ops = 0;
            sw.Restart();
            do
            {
                await runner.Run(chunk).ConfigureAwait(false);
                ops += chunk;
            } while (sw.Elapsed.TotalSeconds < measureSeconds || ops < chunk);
            double seconds = sw.Elapsed.TotalSeconds;
            long proc1 = GC.GetTotalAllocatedBytes(precise: true);
            long thread1 = GC.GetAllocatedBytesForCurrentThread();
            var result = new ExactResult
            {
                Ops = ops,
                Seconds = seconds,
                ProcessBytes = proc1 - proc0,
                ProcessBytesPerOp = (proc1 - proc0) / (double)ops,
                ThreadBytesPerOp = (thread1 - thread0) / (double)ops,
                NsPerOp = seconds * 1e9 / ops,
                Gen0 = GC.CollectionCount(0) - g0,
                Gen1 = GC.CollectionCount(1) - g1,
                Gen2 = GC.CollectionCount(2) - g2,
                WarmupOps = warmOps,
                SetupSeconds = setupSeconds,
            };
            Console.Out.WriteLine(ResultTag + JsonSerializer.Serialize(result));
            Console.Out.Flush();

            if (traced)
            {
                var stdin = new Thread(() =>
                {
                    try
                    {
                        while (Console.In.ReadLine() is { } line)
                        {
                            if (line == "STOP") break;
                        }
                    }
                    catch (IOException)
                    {
                    }
                    s_stop = true;
                })
                { IsBackground = true, Name = "stdin" };
                stdin.Start();
                Console.Out.WriteLine(ReadyTag);
                Console.Out.Flush();
                long wOps = 0;
                long w0 = GC.GetTotalAllocatedBytes(precise: true);
                var guard = Stopwatch.StartNew();
                while (!s_stop && guard.Elapsed.TotalMinutes < 5)
                {
                    await runner.Run(chunk).ConfigureAwait(false);
                    wOps += chunk;
                }
                long w1 = GC.GetTotalAllocatedBytes(precise: true);
                var window = new WindowResult { Ops = wOps, ProcessBytes = w1 - w0, ProcessBytesPerOp = wOps == 0 ? 0 : (w1 - w0) / (double)wOps };
                Console.Out.WriteLine(WindowTag + JsonSerializer.Serialize(window));
                Console.Out.Flush();
            }
            return 0;
        }
        catch (Exception e)
        {
            Console.Out.WriteLine(ErrorTag + JsonSerializer.Serialize(e.ToString()));
            Console.Out.Flush();
            return 1;
        }
        finally
        {
            await ctx.DisposeAsync().ConfigureAwait(false);
        }
    }
}
