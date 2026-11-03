// Replication benchmarks (see bench/README.md, docs/benchmarks.md "Replication"):
//
//   dotnet run -c Release --project bench/RocksDb.Managed.Replication.Benchmarks -f net10.0 -- all
//   ... -- stream --transport inmem|grpc|both --num 200000 --batch 1 --value_size 100 --threads 4 --rate 0 --chunk_bytes 262144
//   ... -- switchover --transport inmem|grpc|both --handoffs 10 --writers 4 --interval_ms 300
//
// Every measured run is preceded by a warm-up run of the same scenario (fresh databases), so the measured numbers are
// for tiered-up code; the "JIT" column counts methods compiled during the measured run (should be ~0).
using System.Globalization;
using RocksDb.Managed.Replication.Benchmarks;

string mode = args.Length > 0 ? args[0] : "all";
var flags = new Dictionary<string, string>(StringComparer.Ordinal);
for (int i = 1; i + 1 < args.Length; i += 2) flags[args[i].TrimStart('-')] = args[i + 1];
int Int(string name, int dflt) => flags.TryGetValue(name, out string? v) ? int.Parse(v, CultureInfo.InvariantCulture) : dflt;
string[] Transports() => (flags.TryGetValue("transport", out string? t) ? t : "both") == "both" ? ["inmem", "grpc"] : [flags["transport"]];

Console.WriteLine("# replication benchmarks: " + Environment.ProcessorCount + " CPUs, " + System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription +
                  ", load average " + LoadAverage());
switch (mode)
{
    case "stream":
    {
        var configs = new List<StreamConfig>();
        foreach (string t in Transports())
        {
            configs.Add(new StreamConfig(t, Int("num", 200_000), Int("batch", 1), Int("value_size", 100), Int("threads", 4), Int("rate", 0),
                Int("chunk_bytes", 256 * 1024)));
        }
        await RunStreamsAsync(configs, Int("repeat", 1));
        break;
    }
    case "switchover":
    {
        var configs = new List<SwitchoverConfig>();
        foreach (string t in Transports()) configs.Add(new SwitchoverConfig(t, Int("handoffs", 10), Int("writers", 4), Int("interval_ms", 300)));
        await RunSwitchoversAsync(configs);
        break;
    }
    case "all":
    {
        var configs = new List<StreamConfig>();
        foreach (string t in new[] { "inmem", "grpc" })
        {
            configs.Add(new StreamConfig(t, 200_000, 1, 100, 1, 0, 256 * 1024));
            configs.Add(new StreamConfig(t, 400_000, 1, 100, 4, 0, 256 * 1024));
            configs.Add(new StreamConfig(t, 1_000_000, 100, 100, 4, 0, 256 * 1024));
            configs.Add(new StreamConfig(t, 100_000, 1, 100, 4, 20_000, 256 * 1024));
        }
        await RunStreamsAsync(configs, Int("repeat", 1));
        await RunSwitchoversAsync([new SwitchoverConfig("inmem", 10, 4, 300), new SwitchoverConfig("grpc", 10, 4, 300)]);
        break;
    }
    default:
        Console.Error.WriteLine("usage: (all | stream | switchover) [--flag value]...");
        return 1;
}
return 0;

static async Task RunStreamsAsync(List<StreamConfig> configs, int repeat)
{
    Console.WriteLine();
    Console.WriteLine(StreamBenchmark.Header);
    foreach (StreamConfig c in configs)
    {
        // Warm-up: the same pipeline (writers, tailer, transport, consumer, flushes) on throwaway databases.
        await StreamBenchmark.RunAsync(c with { Rate = 0 });
        for (int r = 0; r < repeat; r++) Console.WriteLine(StreamBenchmark.Row(await StreamBenchmark.RunAsync(c)));
    }
}

static async Task RunSwitchoversAsync(List<SwitchoverConfig> configs)
{
    Console.WriteLine();
    Console.WriteLine(SwitchoverBenchmark.Header);
    foreach (SwitchoverConfig c in configs)
    {
        List<SwitchoverRun> runs = await SwitchoverBenchmark.RunAsync(c);
        Console.WriteLine(SwitchoverBenchmark.Summary(c, runs));
        foreach (SwitchoverRun r in runs)
        {
            Console.Error.WriteLine(string.Create(CultureInfo.InvariantCulture,
                $"  {c.Transport} handoff {r.Index} from {r.From}: {r.Outcome}, held {r.HeldMs:F1} ms, call {r.TotalMs:F1} ms, client gap {r.ClientGapMs:F1} ms"));
        }
    }
}

static string LoadAverage()
{
    try
    {
        return File.ReadAllText("/proc/loadavg").Split(' ')[0];
    }
    catch (IOException)
    {
        return "n/a";
    }
    catch (UnauthorizedAccessException)
    {
        return "n/a";
    }
}
