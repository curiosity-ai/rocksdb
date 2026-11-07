using System.Diagnostics;
using System.Text.Json;
using RocksDb.Managed.Env;

namespace RocksDb.Managed.Soak;

/// <summary>
/// selftest: proves the checks bite. A short real cycle (worker killed with SIGKILL) gives a database, a history and a
/// model; each case copies that state, injects one bug behind the model's back, runs the verifier child, and expects
/// the intended step to fail. Positive controls (no bug) must pass. Two cases run real workers on the fault file
/// system: a power loss must pass, and the same power loss with fsync disabled (SkipSync) must fail durability.
/// </summary>
internal static class SelfTest
{
    private sealed record Result(string Name, bool ExpectPass, string[] Expected, bool Ok, string Detail);

    public static int Run(string? dir)
    {
        string root = Path.GetFullPath(dir ?? Path.Combine(Path.GetTempPath(), "rocksdb-soak-selftest-" + Environment.ProcessId));
        if (Directory.Exists(root)) Directory.Delete(root, true);
        var sw = Stopwatch.StartNew();
        var basePaths = new RunPaths(Path.Combine(root, "baseline"));
        basePaths.CreateDirectories();
        var cfg = new RunConfig
        {
            Seed = 20261107,
            Layout = new Layout { Writers = 2, KeysPerWriter = 200, Accounts = 8, Counters = 4, RdelKeys = 128 },
            StartedUtc = DateTime.UtcNow.ToString("O"),
        };
        cfg.Db = DbChoices.FromSeed(cfg.Seed);
        cfg.Save(basePaths.RunJson);
        Console.WriteLine($"selftest in {root}; db {cfg.Db}");
        Check(Child.Run(["init", "--run-dir", basePaths.Root], TimeSpan.FromMinutes(2), Path.Combine(basePaths.Logs, "init.log")), "init");
        var plan = new CyclePlan { Cycle = 1, CycleSeed = 11, Fault = FaultKind.Kill, RunSeconds = 6, Options = CycleOptions.FromSeed(11) };
        plan.Options.WriteBufferSize = 64 << 10; // many flushes: SST files for the corruption case
        plan.Save(basePaths.CycleJson);
        RunWorker(basePaths, plan, killAfter: TimeSpan.FromSeconds(6), crashCommand: null);

        var results = new List<Result>();
        results.Add(Case(root, basePaths, "control: unmodified state", true, [], static (_, _, _, _) => "no change"));
        results.Add(Case(root, basePaths, "acknowledged register deleted behind the model's back", false, ["durability"], DeleteAcked));
        results.Add(Case(root, basePaths, "byte flipped in an SST file", false, ["integrity", "scans", "registers", "open"], CorruptSst));
        results.Add(Case(root, basePaths, "bank total broken", false, ["bank"], BreakBank));
        results.Add(Case(root, basePaths, "counter rolled back", false, ["counters"], RollBackCounter));
        results.Add(Case(root, basePaths, "history claims an extra ack", false, ["durability"], ExtraAck));
        results.Add(Case(root, basePaths, "phantom register version", false, ["registers"], Phantom));
        results.Add(Case(root, basePaths, "silently corrupted register value", false, ["registers", "scans"], SilentValue));
        results.Add(Case(root, basePaths, "range-delete key space changed", false, ["range-deletes", "durability"], BreakRdel));
        results.Add(Case(root, basePaths, "mishandled torn-seq image (hole: older write lost, newer survived)", false, ["registers"], Hole));
        results.Add(CorruptCheckCase(root, basePaths));
        results.Add(FaultFsCase(root, basePaths, "control: power loss (clear-cache) on the fault file system", skipSync: false, "clear"));
        results.Add(FaultFsCase(root, basePaths, "control: torn-seq on the fault file system", skipSync: false, "tornseq"));
        results.Add(FaultFsCase(root, basePaths, "missing fsync (SkipSync) + power loss", skipSync: true, "clear"));

        Console.WriteLine();
        Console.WriteLine("selftest results:");
        bool all = true;
        foreach (Result r in results)
        {
            all &= r.Ok;
            Console.WriteLine($"  {(r.Ok ? "OK  " : "BAD ")} {r.Name}: {(r.ExpectPass ? "expected to pass" : "expected failure of " + string.Join("|", r.Expected))} -> {r.Detail}");
        }
        Console.WriteLine($"{(all ? "all injected faults were caught" : "SOME CHECKS DID NOT BITE")} ({sw.Elapsed.TotalSeconds:F0} s)");
        if (all) Directory.Delete(root, true);
        else Console.WriteLine("state kept in " + root);
        return all ? 0 : 1;
    }

    private static void Check((int Code, string Tail) r, string what)
    {
        if (r.Code != 0) throw new InvalidOperationException($"{what} failed ({Child.Describe(r.Code)}):\n{r.Tail}");
    }

    private static void RunWorker(RunPaths p, CyclePlan plan, TimeSpan killAfter, string? crashCommand)
    {
        using ChildProcess w = ChildProcess.Start(["worker", "--run-dir", p.Root, "--part", "0"], p.LogFile(plan.Cycle, "worker"));
        if (!w.Opened.Wait(TimeSpan.FromMinutes(2))) throw new InvalidOperationException("worker did not open:\n" + w.Tail);
        Thread.Sleep(killAfter);
        if (w.HasExited) throw new InvalidOperationException("worker exited early:\n" + w.Tail);
        if (crashCommand is null) w.Kill();
        else w.Send(crashCommand);
        int? code = w.WaitAsync(TimeSpan.FromMinutes(2)).Result;
        if (crashCommand is not null && code != Worker.ExitCrash) throw new InvalidOperationException($"worker crash returned {code}:\n{w.Tail}");
    }

    private delegate string Mutation(RunPaths p, RunConfig cfg, Model m, List<Op> ops);

    private static Result Case(string root, RunPaths basePaths, string name, bool expectPass, string[] expected, Mutation mutate)
    {
        var p = new RunPaths(Path.Combine(root, "case-" + Math.Abs(name.GetHashCode(StringComparison.Ordinal))));
        Supervisor.CopyDirectory(basePaths.Root, p.Root);
        RunConfig cfg = RunConfig.Load(p.RunJson);
        Model m = Model.Load(p.ModelFile);
        List<Op> ops = Op.Pair(HistoryReader.ReadParts(p.HistoryParts(1)));
        string what;
        try
        {
            what = mutate(p, cfg, m, ops);
        }
        catch (Exception e)
        {
            return new Result(name, expectPass, expected, false, "mutation failed: " + e.Message);
        }
        return Judge(p, name, expectPass, expected, what, 1);
    }

    private static Result Judge(RunPaths p, string name, bool expectPass, string[] expected, string what, int cycle)
    {
        (int code, string tail) = Child.Run(["verify", "--run-dir", p.Root], TimeSpan.FromMinutes(5), p.LogFile(cycle, "verify"));
        if (!File.Exists(p.VerifyJson(cycle))) return new Result(name, expectPass, expected, false, $"no verify report ({Child.Describe(code)}): {tail}");
        VerifyReport rep = JsonSerializer.Deserialize<VerifyReport>(File.ReadAllText(p.VerifyJson(cycle)), RunConfig.Json)!;
        var failed = rep.Steps.FindAll(static s => !s.Pass).ConvertAll(static s => s.Name);
        string first = rep.Steps.Find(static s => !s.Pass)?.Errors.FirstOrDefault() ?? rep.Exception ?? "";
        if (first.Length > 160) first = first[..160] + "...";
        bool ok = expectPass ? rep.Pass : failed.Exists(f => Array.IndexOf(expected, f) >= 0);
        string detail = rep.Pass ? $"{what}; all steps passed" : $"{what}; failed steps: {string.Join(",", failed)}: {first}";
        if (ok) Directory.Delete(p.Root, true);
        return new Result(name, expectPass, expected, ok, detail);
    }

    private static RocksDatabase OpenDb(RunPaths p, RunConfig cfg) => Verifier.Open(p.Db, cfg, LocalFileSystem.Instance);

    /// <summary>The last op of each register key in the history (by version), when it is an acknowledged put.</summary>
    private static Op? LastAckedPut(List<Op> ops)
    {
        var last = new Dictionary<(int, int), Op>();
        foreach (Op op in ops)
        {
            if (op.Invoke.Type is not (OpType.RegPut or OpType.RegDel)) continue;
            (int, int) k = (op.Invoke.Process, op.Invoke.A);
            if (!last.TryGetValue(k, out Op? prev) || prev.Invoke.V < op.Invoke.V) last[k] = op;
        }
        foreach (Op op in last.Values)
            if (op.IsOk && op.Invoke.Type == OpType.RegPut) return op;
        return null;
    }

    private static string DeleteAcked(RunPaths p, RunConfig cfg, Model m, List<Op> ops)
    {
        Op op = LastAckedPut(ops) ?? throw new InvalidOperationException("no acknowledged put");
        using RocksDatabase db = OpenDb(p, cfg);
        db.GetColumnFamily(Keys.RegisterCf(op.Invoke.A)).Delete(Keys.Register(op.Invoke.Process, op.Invoke.A));
        return $"deleted w{op.Invoke.Process} key {op.Invoke.A} (acked v{op.Invoke.V})";
    }

    private static string CorruptSst(RunPaths p, RunConfig cfg, Model m, List<Op> ops)
    {
        string? largest = null;
        foreach (string f in Directory.GetFiles(p.Db, "*.sst"))
            if (largest is null || new FileInfo(f).Length > new FileInfo(largest).Length) largest = f;
        if (largest is null) throw new InvalidOperationException("no SST file");
        byte[] data = File.ReadAllBytes(largest);
        int off = data.Length / 3;
        data[off] ^= 0x10;
        File.WriteAllBytes(largest, data);
        return $"flipped a bit at offset {off} of {Path.GetFileName(largest)} ({data.Length} bytes)";
    }

    private static string BreakBank(RunPaths p, RunConfig cfg, Model m, List<Op> ops)
    {
        using RocksDatabase db = OpenDb(p, cfg);
        ColumnFamily bank = db.GetColumnFamily(Keys.Bank);
        Keys.TryU64(bank.Get(Keys.Account(0)), out ulong b);
        bank.Put(Keys.Account(0), Keys.U64(b + 1));
        return $"account 0: {b} -> {b + 1}";
    }

    private static string RollBackCounter(RunPaths p, RunConfig cfg, Model m, List<Op> ops)
    {
        var lower = (ulong[])m.Counters.Clone();
        foreach (Op op in ops)
            if (op.Invoke.Type == OpType.CtrAdd && op.IsOk) lower[op.Invoke.A] += (ulong)op.Invoke.V;
        int c = Array.FindIndex(lower, static v => v > 0);
        if (c < 0) throw new InvalidOperationException("no acknowledged increment");
        using RocksDatabase db = OpenDb(p, cfg);
        db.GetColumnFamily(Keys.Ctr).Put(Keys.Counter(c), Keys.U64(lower[c] - 1));
        return $"counter {c} set to {lower[c] - 1}, below its acknowledged {lower[c]}";
    }

    private static long MaxVersion(List<Op> ops, Model m, int w)
    {
        long v = m.NextVersion[w];
        foreach (Op op in ops)
            if (op.Invoke.Type is OpType.RegPut or OpType.RegDel && op.Invoke.Process == w) v = Math.Max(v, op.Invoke.V);
        return v;
    }

    private static string ExtraAck(RunPaths p, RunConfig cfg, Model m, List<Op> ops)
    {
        long v = MaxVersion(ops, m, 0) + 1000;
        using var h = new HistoryWriter(p.History(1, 0), long.MaxValue / 2);
        Keys.MakeValue((byte)'V', 0, 3, v, 10, out uint crc);
        h.Append(Phase.Invoke, OpType.RegPut, 0, 0, 1, 3, 0, v, crc, 10);
        h.Append(Phase.Ok, OpType.RegPut, 0, 0, 1, 3, 0, v, crc, 10);
        return $"history: put w0 key 3 v{v} acknowledged, never written";
    }

    private static string Phantom(RunPaths p, RunConfig cfg, Model m, List<Op> ops)
    {
        long v = MaxVersion(ops, m, 1) + 5000;
        using RocksDatabase db = OpenDb(p, cfg);
        db.GetColumnFamily(Keys.RegisterCf(7)).Put(Keys.Register(1, 7), Keys.MakeValue((byte)'V', 1, 7, v, 33, out _));
        return $"w1 key 7 = v{v} (never written by anyone)";
    }

    private static string SilentValue(RunPaths p, RunConfig cfg, Model m, List<Op> ops)
    {
        Op op = LastAckedPut(ops) ?? throw new InvalidOperationException("no acknowledged put");
        using RocksDatabase db = OpenDb(p, cfg);
        ColumnFamily cf = db.GetColumnFamily(Keys.RegisterCf(op.Invoke.A));
        byte[] key = Keys.Register(op.Invoke.Process, op.Invoke.A);
        byte[] v = cf.Get(key) ?? throw new InvalidOperationException("key absent");
        v[^1] ^= 0x01;
        cf.Put(key, v);
        return $"w{op.Invoke.Process} key {op.Invoke.A}: last payload byte changed, header kept";
    }

    private static string BreakRdel(RunPaths p, RunConfig cfg, Model m, List<Op> ops)
    {
        using RocksDatabase db = OpenDb(p, cfg);
        ColumnFamily cf = db.GetColumnFamily(Keys.Rdel);
        for (int k = 0; k < cfg.Layout.RdelKeys; k++)
        {
            if (cf.Get(Keys.RdelKey(k)) is not null)
            {
                cf.Delete(Keys.RdelKey(k));
                return $"rdel key {k} deleted";
            }
        }
        throw new InvalidOperationException("no rdel key present");
    }

    private static string Hole(RunPaths p, RunConfig cfg, Model m, List<Op> ops)
    {
        // A power-loss cycle: three unsynced acknowledged writes of writer 0; the image keeps the 1st and 3rd only,
        // as an engine that replays WAL records after a torn-seq hole would.
        CyclePlan plan = CyclePlan.Load(p.CycleJson);
        plan.Fault = FaultKind.PowerLoss;
        plan.Save(p.CycleJson);
        long v = MaxVersion(ops, m, 0) + 1;
        int[] keys = [11, 12, 13];
        using (var h = new HistoryWriter(p.History(1, 0), long.MaxValue / 2))
        {
            for (int i = 0; i < 3; i++)
            {
                Keys.MakeValue((byte)'V', 0, keys[i], v + i, 20, out uint crc);
                h.Append(Phase.Invoke, OpType.RegPut, 0, 0, 10 + i, keys[i], 0, v + i, crc, 20);
                h.Append(Phase.Ok, OpType.RegPut, 0, 0, 10 + i, keys[i], 0, v + i, crc, 20);
            }
        }
        using RocksDatabase db = OpenDb(p, cfg);
        foreach (int i in new[] { 0, 2 })
            db.GetColumnFamily(Keys.RegisterCf(keys[i])).Put(Keys.Register(0, keys[i]), Keys.MakeValue((byte)'V', 0, keys[i], v + i, 20, out _));
        return $"w0 v{v} and v{v + 2} survived, v{v + 1} (in between) lost";
    }

    /// <summary>
    /// The corruption trial's classifier: a copy whose register holds an older, perfectly valid version (a lost
    /// write that no checksum can see) must be classified silent-wrong, and an unmodified copy correct.
    /// </summary>
    private static Result CorruptCheckCase(string root, RunPaths basePaths)
    {
        const string name = "corruption trial: a silently wrong (older, valid) value is classified silent-wrong";
        var p = new RunPaths(Path.Combine(root, "case-corrupt-check"));
        Supervisor.CopyDirectory(basePaths.Root, p.Root);
        try
        {
            Check(Child.Run(["verify", "--run-dir", p.Root], TimeSpan.FromMinutes(5), p.LogFile(1, "verify")), "baseline verify");
            RunConfig cfg = RunConfig.Load(p.RunJson);
            List<Op> ops = Op.Pair(HistoryReader.ReadParts(p.HistoryParts(1)));
            string copy = Path.Combine(p.Root, "copy");
            Supervisor.CopyDirectory(p.Db, copy);
            string outFile = Path.Combine(p.Root, "check.json");
            Check(Child.Run(["corrupt-check", "--run-dir", p.Root, "--db", copy, "--target", "Sst", "--out", outFile], TimeSpan.FromMinutes(3), null), "corrupt-check");
            string control = JsonSerializer.Deserialize<CorruptCheckResult>(File.ReadAllText(outFile), RunConfig.Json)!.Outcome;
            // Roll one register back to an earlier acknowledged version of the same key.
            Op? older = null;
            Op? last = LastAckedPut(ops);
            foreach (Op op in ops)
            {
                if (last is not null && op.IsOk && op.Invoke.Type == OpType.RegPut && op.Invoke.Process == last.Invoke.Process &&
                    op.Invoke.A == last.Invoke.A && op.Invoke.V < last.Invoke.V) older = op;
            }
            if (older is null) return new Result(name, false, ["silent-wrong"], false, "no key with two acknowledged puts");
            using (RocksDatabase db = Verifier.Open(copy, cfg, LocalFileSystem.Instance))
            {
                HistoryRecord o = older.Invoke;
                db.GetColumnFamily(Keys.RegisterCf(o.A)).Put(Keys.Register(o.Process, o.A), Keys.MakeValue((byte)'V', o.Process, o.A, o.V, o.Len, out _));
            }
            Check(Child.Run(["corrupt-check", "--run-dir", p.Root, "--db", copy, "--target", "Sst", "--out", outFile], TimeSpan.FromMinutes(3), null), "corrupt-check");
            string outcome = JsonSerializer.Deserialize<CorruptCheckResult>(File.ReadAllText(outFile), RunConfig.Json)!.Outcome;
            bool ok = control == "correct" && outcome == "silent-wrong";
            if (ok) Directory.Delete(p.Root, true);
            return new Result(name, false, ["silent-wrong"], ok, $"unmodified copy -> {control}; w{older.Invoke.Process} key {older.Invoke.A} rolled back to v{older.Invoke.V} -> {outcome}");
        }
        catch (Exception e)
        {
            return new Result(name, false, ["silent-wrong"], false, "setup failed: " + e.Message);
        }
    }

    /// <summary>Verifies and folds the baseline cycle, then runs a cycle on the fault file system ending with a crash command.</summary>
    private static Result FaultFsCase(string root, RunPaths basePaths, string name, bool skipSync, string mode)
    {
        var p = new RunPaths(Path.Combine(root, "case-" + Math.Abs(name.GetHashCode(StringComparison.Ordinal))));
        Supervisor.CopyDirectory(basePaths.Root, p.Root);
        try
        {
            Check(Child.Run(["verify", "--run-dir", p.Root], TimeSpan.FromMinutes(5), p.LogFile(1, "verify")), "baseline verify");
            File.Move(p.NextModelFile, p.ModelFile, overwrite: true);
            var plan = new CyclePlan
            {
                Cycle = 2, CycleSeed = 22, Fault = FaultKind.PowerLoss, RunSeconds = 6, FaultFs = true, SkipSync = skipSync,
                Options = CycleOptions.FromSeed(22),
            };
            plan.Options.RecycleLogFileNum = 0;
            plan.Save(p.CycleJson);
            RunWorker(p, plan, TimeSpan.FromSeconds(6), $"crash {mode} 5");
        }
        catch (Exception e)
        {
            return new Result(name, !skipSync, ["durability"], false, "setup failed: " + e.Message);
        }
        return Judge(p, name, !skipSync, ["durability"], skipSync ? "fsync ignored, then clear-cache" : mode, 2);
    }
}
