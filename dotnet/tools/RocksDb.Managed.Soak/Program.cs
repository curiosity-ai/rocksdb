using System.Globalization;
using System.Text;
using System.Text.Json;
using RocksDb.Managed.Env;

namespace RocksDb.Managed.Soak;

/// <summary>
/// Jepsen-style soak test for RocksDb.Managed (see docs/soak.md).
/// <code>
/// run --dir DIR [--duration 72h] [--seed N] [--smoke] [options]   supervisor: RUN / FAULT / VERIFY cycles
/// report --dir DIR                                                summary of a run directory
/// export-history --run-dir DIR [--cycle N|all] --format edn --out OUTDIR   Jepsen EDN histories (register, bank)
/// selftest [--dir DIR]                                            inject bugs, expect every check to catch them
/// worker | verify | init | corrupt-check | native-digest          child processes (internal)
/// </code>
/// </summary>
internal static class Program
{
    private const string Usage = """
        usage: RocksDb.Managed.Soak <command> [options]
          run --dir DIR [--duration 72h] [--seed N] [--smoke]
              [--mean-run S] [--min-run S] [--max-run S] [--native-every N] [--corrupt-every N] [--keep-history N]
              [--faults Kill=30,Graceful=8,PowerLoss=12,TornOp=7,TornSeq=7,FsyncTrigger=8,RecoveryKill=8,Pause=6,IoError=8]
              [--writers N] [--keys-per-writer N]
              [--lazyfs-bin PATH | --lazyfs-fifo PATH [--lazyfs-fifo-completed PATH]]
              [--lazyfs-cache 1GB] [--lazyfs-eviction] [--unsafe-skip-fsync]
          report --dir DIR
          export-history --run-dir DIR [--cycle N|all] --format edn --out OUTDIR
          selftest [--dir DIR]
        durations: 90s, 45m, 72h, 3d
        """;

    public static int Main(string[] args)
    {
        if (args.Length == 0 || args[0] is "-h" or "--help" or "help")
        {
            Console.WriteLine(Usage);
            return args.Length == 0 ? 2 : 0;
        }
        var o = ParseOptions(args, 1);
        try
        {
            switch (args[0])
            {
                case "run": return RunCommand(o);
                case "worker":
                    return new Worker(new RunPaths(o["run-dir"]), int.Parse(o.GetValueOrDefault("part", "0"), CultureInfo.InvariantCulture))
                        .RunAsync().GetAwaiter().GetResult();
                case "verify": return new Verifier(new RunPaths(o["run-dir"])).RunCycle();
                case "init": return Init(new RunPaths(o["run-dir"]));
                case "corrupt-check":
                    return CorruptionNemesis.Check(new RunPaths(o["run-dir"]), o["db"], Enum.Parse<DbFileKind>(o["target"]), o["out"]);
                case "native-digest": return NativeDigest.Run(args[1], args[2]);
                case "report": return Report.Print(new RunPaths(o["dir"]));
                case "export-history": return Edn.Export(new RunPaths(o["run-dir"]), o.GetValueOrDefault("cycle", "all"), o.GetValueOrDefault("format", "edn"), o["out"]);
                case "selftest": return SelfTest.Run(o.GetValueOrDefault("dir"));
                default:
                    Console.Error.WriteLine("unknown command " + args[0] + "\n" + Usage);
                    return 2;
            }
        }
        catch (KeyNotFoundException e)
        {
            Console.Error.WriteLine("missing option: " + e.Message + "\n" + Usage);
            return 2;
        }
    }

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

    private static int RunCommand(Dictionary<string, string> o)
    {
        var paths = new RunPaths(o["dir"]);
        bool smoke = o.ContainsKey("smoke");
        TimeSpan duration = ParseDuration(o.GetValueOrDefault("duration", smoke ? "2m" : "72h"));
        RunConfig cfg;
        if (File.Exists(paths.RunJson))
        {
            cfg = RunConfig.Load(paths.RunJson);
            Console.WriteLine($"resuming the run in {paths.Root} (seed {cfg.Seed}); only --duration applies");
        }
        else
        {
            ulong seed = o.TryGetValue("seed", out string? s) ? ulong.Parse(s, CultureInfo.InvariantCulture) : (ulong)Random.Shared.NextInt64(1, long.MaxValue);
            cfg = new RunConfig { Seed = seed, Db = DbChoices.FromSeed(seed), StartedUtc = DateTime.UtcNow.ToString("O") };
            if (smoke)
            {
                cfg.MeanRunSeconds = 4;
                cfg.MinRunSeconds = 1.5;
                cfg.MaxRunSeconds = 10;
                cfg.NativeEvery = 4;
                cfg.CorruptEvery = 2;
                cfg.Layout = new Layout { Writers = 3, KeysPerWriter = 300, Accounts = 12, Counters = 8, RdelKeys = 256 };
            }
            if (o.TryGetValue("mean-run", out s)) cfg.MeanRunSeconds = double.Parse(s, CultureInfo.InvariantCulture);
            if (o.TryGetValue("min-run", out s)) cfg.MinRunSeconds = double.Parse(s, CultureInfo.InvariantCulture);
            if (o.TryGetValue("max-run", out s)) cfg.MaxRunSeconds = double.Parse(s, CultureInfo.InvariantCulture);
            if (o.TryGetValue("native-every", out s)) cfg.NativeEvery = int.Parse(s, CultureInfo.InvariantCulture);
            if (o.TryGetValue("corrupt-every", out s)) cfg.CorruptEvery = int.Parse(s, CultureInfo.InvariantCulture);
            if (o.TryGetValue("keep-history", out s)) cfg.KeepHistory = int.Parse(s, CultureInfo.InvariantCulture);
            if (o.TryGetValue("writers", out s)) cfg.Layout.Writers = int.Parse(s, CultureInfo.InvariantCulture);
            if (o.TryGetValue("keys-per-writer", out s)) cfg.Layout.KeysPerWriter = int.Parse(s, CultureInfo.InvariantCulture);
            if (o.TryGetValue("faults", out s))
            {
                var w = new Dictionary<string, int>();
                foreach (string kv in s.Split(',', StringSplitOptions.RemoveEmptyEntries))
                {
                    string[] p = kv.Split('=');
                    w[Enum.Parse<FaultKind>(p[0], ignoreCase: true).ToString()] = int.Parse(p[1], CultureInfo.InvariantCulture);
                }
                cfg.FaultWeights = w;
            }
            cfg.LazyFsBinary = o.GetValueOrDefault("lazyfs-bin");
            cfg.LazyFsFifo = o.GetValueOrDefault("lazyfs-fifo");
            cfg.LazyFsFifoCompleted = o.GetValueOrDefault("lazyfs-fifo-completed");
            cfg.UnsafeSkipFsync = o.ContainsKey("unsafe-skip-fsync");
            if (o.TryGetValue("lazyfs-cache", out s)) cfg.LazyFsCache = s;
            cfg.LazyFsEviction = o.ContainsKey("lazyfs-eviction");
            if (cfg.UnsafeSkipFsync && !cfg.UsesLazyFs)
            {
                Console.Error.WriteLine("--unsafe-skip-fsync needs --lazyfs-bin or --lazyfs-fifo");
                return 2;
            }
            if (cfg.UsesLazyFs && cfg.FaultWeights.GetValueOrDefault(nameof(FaultKind.LazyFsClear)) == 0)
                cfg.FaultWeights[nameof(FaultKind.LazyFsClear)] = 30;
            paths.CreateDirectories();
            cfg.Save(paths.RunJson);
        }
        paths.CreateDirectories();
        Console.WriteLine($"seed {cfg.Seed}");
        return new Supervisor(paths, cfg, duration).Run();
    }

    /// <summary>Creates the database: column families and the bank accounts (synced), and the initial model.</summary>
    private static int Init(RunPaths paths)
    {
        RunConfig cfg = RunConfig.Load(paths.RunJson);
        Layout l = cfg.Layout;
        var c = CycleOptions.FromSeed(cfg.Seed);
        var cache = Cache.BlockCache.CreateLru(1 << 20);
        using (RocksDatabase db = RocksDatabase.Open(paths.Db, DbSetup.DbOptions(c, LocalFileSystem.Instance),
                   DbSetup.Descriptors(paths.Db, LocalFileSystem.Instance, cfg.Db, c, cache)))
        {
            ColumnFamily bank = db.GetColumnFamily(Keys.Bank);
            using Db.WriteBatch b = db.CreateWriteBatch();
            for (int a = 0; a < l.Accounts; a++) b.Put(bank.Id, Keys.Account(a), Keys.U64(l.InitialBalance));
            db.Write(new WriteOptions { Sync = true }, b);
            db.Flush();
        }
        Model m = Model.Empty(l);
        m.Cycle = 1;
        m.BankInitialized = true;
        Array.Fill(m.Balances, l.InitialBalance);
        m.Save(paths.ModelFile);
        Console.WriteLine($"initialised {paths.Db}: {cfg.Db}");
        return 0;
    }
}

/// <summary>The report command: a summary of a run directory.</summary>
internal static class Report
{
    public static int Print(RunPaths paths)
    {
        Status? s = Status.Load(paths.Status);
        if (s is null)
        {
            Console.Error.WriteLine("no status.json in " + paths.Root);
            return 2;
        }
        RunConfig? cfg = File.Exists(paths.RunJson) ? RunConfig.Load(paths.RunJson) : null;
        var sb = new StringBuilder();
        sb.AppendLine($"run {paths.Root}");
        sb.AppendLine($"  result {s.Result}, phase {s.Phase}, cycle {s.Cycle}, seed {s.Seed}");
        sb.AppendLine($"  elapsed {TimeSpan.FromSeconds(s.ElapsedSeconds):d\\.hh\\:mm\\:ss} of {TimeSpan.FromSeconds(s.DurationSeconds):d\\.hh\\:mm\\:ss}, {s.CyclesCompleted} cycles verified, {s.NativeChecks} native cross-checks");
        if (cfg is not null) sb.AppendLine($"  db: {cfg.Db}");
        sb.AppendLine("faults (cycles ended by each):");
        foreach (KeyValuePair<string, long> kv in s.Faults) sb.AppendLine($"  {kv.Key,-16} {kv.Value,8}");
        sb.AppendLine("notes:");
        foreach (KeyValuePair<string, long> kv in s.Notes) sb.AppendLine($"  {kv.Key,-32} {kv.Value,8}");
        // Ops: Type -> ok / fail / info
        var types = new SortedDictionary<string, long[]>();
        foreach (KeyValuePair<string, long> kv in s.Ops)
        {
            string[] p = kv.Key.Split('.');
            if (!types.TryGetValue(p[0], out long[]? c)) types[p[0]] = c = new long[3];
            c[p[1] switch { "ok" => 0, "fail" => 1, _ => 2 }] += kv.Value;
        }
        sb.AppendLine("operations (history):        ok       fail       info");
        long total = 0;
        foreach (KeyValuePair<string, long[]> kv in types)
        {
            sb.AppendLine($"  {kv.Key,-12} {kv.Value[0],12} {kv.Value[1],10} {kv.Value[2],10}");
            total += kv.Value[0] + kv.Value[1] + kv.Value[2];
        }
        sb.AppendLine($"  total {total}");
        if (s.Corruption.Count > 0)
        {
            sb.AppendLine("file corruption trials (target/mode/outcome):");
            foreach (KeyValuePair<string, long> kv in s.Corruption) sb.AppendLine($"  {kv.Key,-48} {kv.Value,6}");
        }
        // Verify step timings across the kept reports.
        var steps = new SortedDictionary<string, (int N, int Failed, double Sum, double Max)>();
        if (Directory.Exists(paths.Verify))
        {
            foreach (string f in Directory.GetFiles(paths.Verify, "cycle-*.json"))
            {
                VerifyReport? r = JsonSerializer.Deserialize<VerifyReport>(File.ReadAllText(f), RunConfig.Json);
                if (r is null) continue;
                foreach (StepResult st in r.Steps)
                {
                    (int n, int failed, double sum, double max) = steps.GetValueOrDefault(st.Name);
                    steps[st.Name] = (n + 1, failed + (st.Pass ? 0 : 1), sum + st.Ms, Math.Max(max, st.Ms));
                }
            }
        }
        sb.AppendLine("verify steps:            runs  failed   avg ms   max ms");
        foreach (KeyValuePair<string, (int N, int Failed, double Sum, double Max)> kv in steps)
            sb.AppendLine($"  {kv.Key,-22} {kv.Value.N,6} {kv.Value.Failed,7} {kv.Value.Sum / kv.Value.N,8:F0} {kv.Value.Max,8:F0}");
        if (s.LastVerify is not null)
        {
            sb.AppendLine($"last verify: cycle {s.LastVerify.Cycle} ({s.LastVerify.Fault}) {(s.LastVerify.Pass ? "passed" : "FAILED")}");
            foreach (string st in s.LastVerify.Steps) sb.AppendLine("  " + st);
        }
        sb.AppendLine(s.Failures.Count == 0 ? "failures: none" : "failures:");
        foreach (string f in s.Failures) sb.AppendLine("  " + f);
        Console.Write(sb.ToString());
        return s.Failures.Count == 0 ? 0 : 1;
    }
}
