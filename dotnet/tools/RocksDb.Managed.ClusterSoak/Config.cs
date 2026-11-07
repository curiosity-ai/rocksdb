using System.Globalization;
using System.Text.Json;

namespace RocksDb.Managed.ClusterSoak;

/// <summary>Fault (nemesis) kinds; the <c>--faults</c> names.</summary>
internal enum FaultKind
{
    PartitionHalves,
    IsolateLeader,
    IsolateNode,
    Bridge,
    Ring,
    OneWay,
    Slow,
    Kill,
    KillLeader,
    Pause,
    PauseLeader,
    PowerLoss,
    PowerLossLeader,
    KillDuringCatchup,
    ClockSkew,
    RollingRestart,
    ClientCut,
}

/// <summary>Run configuration (run.json), fixed for the whole run.</summary>
internal sealed class RunConfig
{
    public ulong Seed { get; set; }
    public int Nodes { get; set; } = 3;
    public int Clients { get; set; } = 4;
    public int KeysPerClient { get; set; } = 200;
    public int Accounts { get; set; } = 16;
    public int Counters { get; set; } = 16;
    /// <summary>Seconds of RUN between VERIFY phases.</summary>
    public double VerifyEverySeconds { get; set; } = 300;
    /// <summary>Mean seconds between nemesis actions (exponential).</summary>
    public double MeanFaultIntervalSeconds { get; set; } = 6;
    /// <summary>Faults that may be active at the same time.</summary>
    public int MaxConcurrentFaults { get; set; } = 2;
    public int NativeEvery { get; set; } = 3;
    public int KeepHistory { get; set; } = 6;
    public double ConvergeTimeoutSeconds { get; set; } = 180;
    public int ElectionMinMs { get; set; } = 800;
    public int ElectionMaxMs { get; set; } = 1600;
    public int HeartbeatMs { get; set; } = 150;
    public int WriteTimeoutMs { get; set; } = 2000;
    public ulong WriteBufferSize { get; set; } = 512 * 1024;
    public ulong WalTtlSeconds { get; set; } = 20;
    public Dictionary<string, int> Faults { get; set; } = DefaultFaults();
    public string StartedUtc { get; set; } = "";
    public bool Smoke { get; set; }

    public static Dictionary<string, int> DefaultFaults() => new(StringComparer.Ordinal)
    {
        [nameof(FaultKind.PartitionHalves)] = 10,
        [nameof(FaultKind.IsolateLeader)] = 10,
        [nameof(FaultKind.IsolateNode)] = 6,
        [nameof(FaultKind.Bridge)] = 4,
        [nameof(FaultKind.Ring)] = 4,
        [nameof(FaultKind.OneWay)] = 6,
        [nameof(FaultKind.Slow)] = 4,
        [nameof(FaultKind.Kill)] = 6,
        [nameof(FaultKind.KillLeader)] = 8,
        [nameof(FaultKind.Pause)] = 4,
        [nameof(FaultKind.PauseLeader)] = 6,
        [nameof(FaultKind.PowerLoss)] = 6,
        [nameof(FaultKind.PowerLossLeader)] = 8,
        [nameof(FaultKind.KillDuringCatchup)] = 5,
        [nameof(FaultKind.ClockSkew)] = 4,
        [nameof(FaultKind.RollingRestart)] = 2,
        [nameof(FaultKind.ClientCut)] = 3,
    };

    public int RegisterKeys => Clients * KeysPerClient;

    public static RunConfig Load(string path) => JsonSerializer.Deserialize<RunConfig>(File.ReadAllText(path))!;

    public void Save(string path) => Json.WriteAtomic(path, this);
}

/// <summary>Layout of a run directory.</summary>
internal sealed class RunPaths(string root)
{
    public string Root { get; } = Path.GetFullPath(root);
    public string RunJson => Path.Combine(Root, "run.json");
    public string ModelJson => Path.Combine(Root, "model.json");
    public string StatusJson => Path.Combine(Root, "status.json");
    public string Log => Path.Combine(Root, "soak.log");
    public string Nemesis => Path.Combine(Root, "nemesis.log");
    public string Nodes => Path.Combine(Root, "nodes");
    public string Events => Path.Combine(Root, "events");
    public string Logs => Path.Combine(Root, "logs");
    public string History => Path.Combine(Root, "history");
    public string Verify => Path.Combine(Root, "verify");
    public string Failures => Path.Combine(Root, "failures");
    public string Scratch => Path.Combine(Root, "scratch");

    public string NodeRoot(string id) => Path.Combine(Nodes, id);
    public string EventFile(string id) => Path.Combine(Events, id + ".log");
    public string NodeLog(string id) => Path.Combine(Logs, id + ".log");
    public string HistoryFile(int epoch) => Path.Combine(History, "epoch-" + epoch.ToString(CultureInfo.InvariantCulture) + ".tsv");
    public string VerifyFile(int epoch) => Path.Combine(Verify, "epoch-" + epoch.ToString(CultureInfo.InvariantCulture) + ".json");

    public void Create()
    {
        foreach (string d in new[] { Root, Nodes, Events, Logs, History, Verify, Scratch }) Directory.CreateDirectory(d);
    }
}

internal static class Json
{
    public static readonly JsonSerializerOptions Indented = new() { WriteIndented = true };

    public static void WriteAtomic<T>(string path, T value)
    {
        string tmp = path + ".tmp";
        File.WriteAllText(tmp, JsonSerializer.Serialize(value, Indented));
        File.Move(tmp, path, overwrite: true);
    }
}

internal static class Cli
{
    public static Dictionary<string, string> ParseOptions(string[] args, int start)
    {
        var o = new Dictionary<string, string>(StringComparer.Ordinal);
        for (int i = start; i < args.Length; i++)
        {
            if (!args[i].StartsWith("--", StringComparison.Ordinal)) continue;
            string k = args[i][2..];
            if (i + 1 < args.Length && !args[i + 1].StartsWith("--", StringComparison.Ordinal)) o[k] = args[++i];
            else o[k] = "true";
        }
        return o;
    }

    public static TimeSpan ParseDuration(string s)
    {
        double n = double.Parse(s.AsSpan(0, s.Length - 1), CultureInfo.InvariantCulture);
        return s[^1] switch
        {
            's' => TimeSpan.FromSeconds(n),
            'm' => TimeSpan.FromMinutes(n),
            'h' => TimeSpan.FromHours(n),
            'd' => TimeSpan.FromDays(n),
            _ => TimeSpan.FromSeconds(double.Parse(s, CultureInfo.InvariantCulture)),
        };
    }

    public static Dictionary<string, int> ParseFaults(string spec, Dictionary<string, int> into)
    {
        foreach (string part in spec.Split(',', StringSplitOptions.RemoveEmptyEntries))
        {
            string[] kv = part.Split('=');
            if (!Enum.TryParse(kv[0], out FaultKind _)) throw new ArgumentException("unknown fault " + kv[0]);
            into[kv[0]] = kv.Length > 1 ? int.Parse(kv[1], CultureInfo.InvariantCulture) : 1;
        }
        return into;
    }
}

/// <summary>Deterministic per-purpose random streams derived from the run seed.</summary>
internal static class Seeds
{
    public static int Derive(ulong seed, string purpose, long n)
    {
        ulong h = seed ^ 0x9E3779B97F4A7C15UL;
        foreach (char c in purpose) h = (h ^ c) * 0x100000001B3UL;
        h ^= (ulong)n * 0xBF58476D1CE4E5B9UL;
        h ^= h >> 31;
        return (int)(h & 0x7FFFFFFF);
    }
}
