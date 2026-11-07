using System.Globalization;
using System.Text;
using System.Text.Json;

namespace RocksDb.Managed.ClusterSoak;

/// <summary>
/// Jepsen-style soak test of the Raft cluster mode (see docs/cluster-soak.md).
/// <code>
/// run --dir DIR [--nodes 3] [--duration 72h] [--seed N] [--smoke] [options]   supervisor
/// report --dir DIR                                                            summary of a run directory
/// export-history --dir DIR [--epoch N|all] --format edn --out OUTDIR          Jepsen EDN histories
/// selftest [--dir DIR]                                                        inject bugs, expect each check to catch them
/// node --config FILE | native-digest DIR OUT                                  child processes (internal)
/// </code>
/// </summary>
internal static class Program
{
    private const string Usage = """
        usage: RocksDb.Managed.ClusterSoak <command> [options]
          run --dir DIR [--nodes 3|5] [--duration 72h] [--seed N] [--smoke]
              [--clients N] [--keys-per-client N] [--verify-every 5m] [--mean-fault 6s] [--max-faults 2]
              [--native-every N] [--keep-history N] [--converge-timeout 180s]
              [--faults PartitionHalves=10,IsolateLeader=10,...]   (weights; 0 disables a fault)
          report --dir DIR
          export-history --dir DIR [--epoch N|all] --format edn --out OUTDIR
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
        Dictionary<string, string> o = Cli.ParseOptions(args, 1);
        try
        {
            switch (args[0])
            {
                case "run": return RunCommand(o);
                case "node": return NodeMain.RunAsync(o["config"]).GetAwaiter().GetResult();
                case "native-digest": return NativeDigest.Run(args[1], args[2]);
                case "report": return Report.Print(new RunPaths(o["dir"]));
                case "export-history": return Edn.Export(new RunPaths(o["dir"]), o.GetValueOrDefault("epoch", "all"), o.GetValueOrDefault("format", "edn"), o["out"]);
                case "selftest": return SelfTest.RunAsync(o.GetValueOrDefault("dir")).GetAwaiter().GetResult();
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

    private static int RunCommand(Dictionary<string, string> o)
    {
        var paths = new RunPaths(o["dir"]);
        bool smoke = o.ContainsKey("smoke");
        TimeSpan duration = Cli.ParseDuration(o.GetValueOrDefault("duration", smoke ? "150s" : "72h"));
        RunConfig cfg;
        if (File.Exists(paths.RunJson))
        {
            cfg = RunConfig.Load(paths.RunJson);
            Console.WriteLine("resuming the run in " + paths.Root + " (seed " + cfg.Seed + "); only --duration applies");
        }
        else
        {
            cfg = BuildConfig(o, smoke);
            paths.Create();
            cfg.Save(paths.RunJson);
        }
        paths.Create();
        return new Supervisor(cfg, paths, duration).RunAsync().GetAwaiter().GetResult();
    }

    public static RunConfig BuildConfig(Dictionary<string, string> o, bool smoke)
    {
        ulong seed = o.TryGetValue("seed", out string? s) ? ulong.Parse(s, CultureInfo.InvariantCulture) : (ulong)Random.Shared.NextInt64(1, long.MaxValue);
        var cfg = new RunConfig { Seed = seed, StartedUtc = DateTime.UtcNow.ToString("O", CultureInfo.InvariantCulture), Smoke = smoke };
        if (o.TryGetValue("nodes", out s)) cfg.Nodes = int.Parse(s, CultureInfo.InvariantCulture);
        if (smoke)
        {
            cfg.Clients = 3;
            cfg.KeysPerClient = 60;
            cfg.VerifyEverySeconds = 40;
            cfg.MeanFaultIntervalSeconds = 3;
            cfg.NativeEvery = 2;
            cfg.ConvergeTimeoutSeconds = 90;
        }
        // Per-run engine choices from the seed: memtable size (flush / WAL rotation rate) and WAL retention (how often a
        // lagging follower needs a snapshot instead of the WAL stream).
        var rng = new Random(Seeds.Derive(seed, "options", 0));
        ulong[] wbs = [128 * 1024, 512 * 1024, 2 * 1024 * 1024];
        ulong[] ttl = [0, 5, 30];
        cfg.WriteBufferSize = wbs[rng.Next(wbs.Length)];
        cfg.WalTtlSeconds = ttl[rng.Next(ttl.Length)];
        if (o.TryGetValue("clients", out s)) cfg.Clients = int.Parse(s, CultureInfo.InvariantCulture);
        if (o.TryGetValue("keys-per-client", out s)) cfg.KeysPerClient = int.Parse(s, CultureInfo.InvariantCulture);
        if (o.TryGetValue("verify-every", out s)) cfg.VerifyEverySeconds = Cli.ParseDuration(s).TotalSeconds;
        if (o.TryGetValue("mean-fault", out s)) cfg.MeanFaultIntervalSeconds = Cli.ParseDuration(s).TotalSeconds;
        if (o.TryGetValue("max-faults", out s)) cfg.MaxConcurrentFaults = int.Parse(s, CultureInfo.InvariantCulture);
        if (o.TryGetValue("native-every", out s)) cfg.NativeEvery = int.Parse(s, CultureInfo.InvariantCulture);
        if (o.TryGetValue("keep-history", out s)) cfg.KeepHistory = int.Parse(s, CultureInfo.InvariantCulture);
        if (o.TryGetValue("converge-timeout", out s)) cfg.ConvergeTimeoutSeconds = Cli.ParseDuration(s).TotalSeconds;
        if (o.TryGetValue("wal-ttl", out s)) cfg.WalTtlSeconds = ulong.Parse(s, CultureInfo.InvariantCulture);
        if (o.TryGetValue("faults", out s)) Cli.ParseFaults(s, cfg.Faults);
        return cfg;
    }
}

/// <summary>Native (rocksdb-sharp) scan of a checkpoint: the same digest as <c>DatabaseDigest</c> (child process).</summary>
internal static class NativeDigest
{
    public static int Run(string dir, string outFile)
    {
        var r = new Dictionary<string, string>(StringComparer.Ordinal);
        try
        {
            var dbo = new RocksDbSharp.DbOptions();
            var cfo = new RocksDbSharp.ColumnFamilyOptions();
            cfo.SetUint64addMergeOperator();
            var cfs = new RocksDbSharp.ColumnFamilies(cfo);
            using RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.OpenReadOnly(dbo, dir, cfs, false);
            const ulong FnvOffset = 14695981039346656037UL, FnvPrime = 1099511628211UL;
            ulong hash = FnvOffset;
            long count = 0;
            using RocksDbSharp.Iterator it = db.NewIterator();
            for (it.SeekToFirst(); it.Valid(); it.Next())
            {
                foreach (byte b in it.Key())
                {
                    hash ^= b;
                    hash *= FnvPrime;
                }
                hash ^= 0xFF;
                hash *= FnvPrime;
                foreach (byte b in it.Value())
                {
                    hash ^= b;
                    hash *= FnvPrime;
                }
                hash ^= 0xFE;
                hash *= FnvPrime;
                count++;
            }
            r["hash"] = hash.ToString("x16", CultureInfo.InvariantCulture);
            r["count"] = count.ToString(CultureInfo.InvariantCulture);
        }
        catch (Exception e)
        {
            r["error"] = e.GetType().Name + ": " + e.Message;
        }
        File.WriteAllText(outFile, JsonSerializer.Serialize(r));
        return 0;
    }
}

/// <summary>The <c>report</c> command.</summary>
internal static class Report
{
    public static int Print(RunPaths p)
    {
        if (!File.Exists(p.StatusJson))
        {
            Console.Error.WriteLine("no status.json in " + p.Root);
            return 2;
        }
        Status s = JsonSerializer.Deserialize<Status>(File.ReadAllText(p.StatusJson))!;
        var sb = new StringBuilder();
        sb.AppendLine("cluster soak " + p.Root);
        sb.AppendLine("  result: " + s.Result + " (phase " + s.Phase + ", epoch " + s.Epoch + "), seed " + s.Seed + ", " + s.Nodes + " nodes");
        sb.AppendLine("  elapsed: " + TimeSpan.FromSeconds(s.ElapsedSeconds).ToString(@"d\.hh\:mm\:ss", CultureInfo.InvariantCulture) + " of " +
                      TimeSpan.FromSeconds(s.DurationSeconds).ToString(@"d\.hh\:mm\:ss", CultureInfo.InvariantCulture) + "; " + s.Verifies + " VERIFY phases, " + s.NativeChecks + " native cross-checks");
        sb.AppendLine("  faults:");
        foreach (KeyValuePair<string, long> kv in Sorted(s.Faults)) sb.AppendLine("    " + kv.Key.PadRight(28) + kv.Value);
        sb.AppendLine("  operations (ok / fail / info):");
        foreach (string f in new[] { "write", "transfer", "add", "read", "bankread" })
            sb.AppendLine("    " + f.PadRight(12) + s.Ops.GetValueOrDefault(f + ".ok") + " / " + s.Ops.GetValueOrDefault(f + ".fail") + " / " + s.Ops.GetValueOrDefault(f + ".info"));
        sb.AppendLine("  reads (statistics; no ReadIndex, see the docs):");
        foreach (KeyValuePair<string, long> kv in Sorted(s.Stats))
            if (kv.Key.StartsWith("read", StringComparison.Ordinal) || kv.Key.StartsWith("bank", StringComparison.Ordinal)) sb.AppendLine("    " + kv.Key.PadRight(32) + kv.Value);
        sb.AppendLine("  availability: " + s.TermsWithLeader + " terms with a leader; leaderless " + s.LeaderlessSeconds.ToString("F1", CultureInfo.InvariantCulture) + " s of " +
                      s.ObservedSeconds.ToString("F1", CultureInfo.InvariantCulture) + " s in " + s.LeaderlessIntervals + " intervals (longest " +
                      s.LongestLeaderlessSeconds.ToString("F1", CultureInfo.InvariantCulture) + " s; " + Fmt(s.LeaderlessIntervalSeconds) + " s)");
        sb.AppendLine("  write latency (ok, ms): " + Fmt(s.WriteLatencyMs));
        sb.AppendLine("  ops by fault phase (top 25):");
        var phases = new List<KeyValuePair<string, long>>(s.OpsByFaultPhase);
        phases.Sort(static (a, b) => b.Value.CompareTo(a.Value));
        for (int i = 0; i < phases.Count && i < 25; i++) sb.AppendLine("    " + phases[i].Key.PadRight(48) + phases[i].Value);
        sb.AppendLine("  verify steps (runs, failures, avg ms, max ms):");
        foreach (KeyValuePair<string, StepTotal> kv in s.StepTotals)
            sb.AppendLine("    " + kv.Key.PadRight(20) + kv.Value.Runs.ToString(CultureInfo.InvariantCulture).PadLeft(6) + kv.Value.Failures.ToString(CultureInfo.InvariantCulture).PadLeft(6) +
                          (kv.Value.TotalMs / Math.Max(1, kv.Value.Runs)).ToString("F0", CultureInfo.InvariantCulture).PadLeft(9) + kv.Value.MaxMs.ToString("F0", CultureInfo.InvariantCulture).PadLeft(9));
        if (s.LastVerify is { } lv)
        {
            sb.AppendLine("  last verify: epoch " + lv.Epoch + " " + (lv.Pass ? "PASS" : "FAIL"));
            foreach (StepResult st in lv.Steps) sb.AppendLine("    " + (st.Pass ? "PASS " : "FAIL ") + st.Name.PadRight(20) + st.Detail);
        }
        foreach (string f in s.Failures) sb.AppendLine("  FAILURE: " + f);
        Console.Write(sb.ToString());
        return s.Result is "failed" ? 1 : 0;
    }

    private static List<KeyValuePair<string, long>> Sorted(Dictionary<string, long> d)
    {
        var l = new List<KeyValuePair<string, long>>(d);
        l.Sort(static (a, b) => string.CompareOrdinal(a.Key, b.Key));
        return l;
    }

    private static string Fmt(Dictionary<string, double> p) =>
        p.Count == 0 ? "-" : "p50 " + p["p50"].ToString("F1", CultureInfo.InvariantCulture) + ", p95 " + p["p95"].ToString("F1", CultureInfo.InvariantCulture) +
                             ", p99 " + p["p99"].ToString("F1", CultureInfo.InvariantCulture) + ", max " + p["max"].ToString("F1", CultureInfo.InvariantCulture) +
                             " (n=" + p["count"].ToString("F0", CultureInfo.InvariantCulture) + ")";
}

/// <summary>
/// <c>export-history --format edn</c>: Jepsen histories for Elle / Knossos. register.edn: rw-register transactions
/// (<c>[[:w k v]]</c>, <c>[[:r k v] ...]</c>); bank.edn: transfers and reads. Every op still open at the end of an
/// epoch gets an :info completion; process ids never repeat after :info (the workers already renumber).
/// </summary>
internal static class Edn
{
    public static int Export(RunPaths p, string epoch, string format, string outDir)
    {
        if (format != "edn") throw new ArgumentException("only --format edn is supported");
        Directory.CreateDirectory(outDir);
        var files = new List<(int, string)>();
        foreach (string f in Directory.GetFiles(p.History, "epoch-*.tsv"))
        {
            int n = int.Parse(Path.GetFileNameWithoutExtension(f)[6..], CultureInfo.InvariantCulture);
            if (epoch == "all" || epoch == n.ToString(CultureInfo.InvariantCulture)) files.Add((n, f));
        }
        files.Sort();
        using var reg = new StreamWriter(Path.Combine(outDir, "register.edn"));
        using var bank = new StreamWriter(Path.Combine(outDir, "bank.edn"));
        long ri = 0, bi = 0, timeBase = 0;
        foreach ((int n, string f) in files)
        {
            List<Op> ops = History.Read(f);
            var open = new Dictionary<int, Op>();
            long lastTime = 0;
            foreach (Op op in ops)
            {
                lastTime = op.TimeUs;
                if (op.F == OpF.Nemesis) continue;
                if (op.Type == OpType.Invoke) open[op.Process] = op;
                else open.Remove(op.Process);
                Emit(op, op.Type, n, timeBase, reg, bank, ref ri, ref bi);
            }
            foreach (Op op in open.Values) Emit(op, OpType.Info, n, timeBase, reg, bank, ref ri, ref bi);
            timeBase += lastTime + 1000;
        }
        Console.WriteLine("wrote " + ri + " register and " + bi + " bank ops to " + outDir);
        return 0;
    }

    private static void Emit(Op op, OpType type, int epoch, long timeBase, StreamWriter reg, StreamWriter bank, ref long ri, ref long bi)
    {
        string t = ":" + type.ToString().ToLowerInvariant();
        long proc = epoch * 1_000_000L + op.Process;
        long time = (timeBase + op.TimeUs) * 1000; // ns
        string head = ", :type " + t + ", :process " + proc.ToString(CultureInfo.InvariantCulture) + ", :time " + time.ToString(CultureInfo.InvariantCulture);
        switch (op.F)
        {
            case OpF.Write:
                reg.WriteLine("{:index " + ri++ + head + ", :f :txn, :value [[:w " + op.Keys[0] + " " + op.Vals[0] + "]]}");
                break;
            case OpF.Read:
            {
                var sb = new StringBuilder();
                for (int i = 0; i < op.Keys.Length; i++)
                {
                    string v = type == OpType.Ok && i < op.Vals.Length && op.Vals[i] >= 0 ? op.Vals[i].ToString(CultureInfo.InvariantCulture) : "nil";
                    sb.Append("[:r ").Append(op.Keys[i].ToString(CultureInfo.InvariantCulture)).Append(' ').Append(v).Append(']');
                }
                reg.WriteLine("{:index " + ri++ + head + ", :f :txn, :value [" + sb + "]}");
                break;
            }
            case OpF.Transfer:
                bank.WriteLine("{:index " + bi++ + head + ", :f :transfer, :value {:from " + op.Keys[0] + ", :to " + op.Keys[1] + ", :amount " +
                               (op.Vals.Length > 0 ? op.Vals[0] : 0) + "}}");
                break;
            case OpF.BankRead:
            {
                var sb = new StringBuilder();
                if (type == OpType.Ok)
                    for (int i = 0; i < op.Vals.Length; i++) sb.Append(i == 0 ? "" : ", ").Append(i).Append(' ').Append(op.Vals[i].ToString(CultureInfo.InvariantCulture));
                bank.WriteLine("{:index " + bi++ + head + ", :f :read, :value " + (type == OpType.Ok ? "{" + sb + "}" : "nil") + "}");
                break;
            }
        }
    }
}
