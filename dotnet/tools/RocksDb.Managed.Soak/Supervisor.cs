using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text.Json;

namespace RocksDb.Managed.Soak;

/// <summary>status.json: the machine-readable state of a run, rewritten after every phase.</summary>
internal sealed class Status
{
    public string Phase { get; set; } = "init";
    public int Cycle { get; set; }
    public string Fault { get; set; } = "";
    public ulong Seed { get; set; }
    public string StartedUtc { get; set; } = "";
    public string UpdatedUtc { get; set; } = "";
    public double ElapsedSeconds { get; set; }
    public double DurationSeconds { get; set; }
    public int CyclesCompleted { get; set; }
    public SortedDictionary<string, long> Faults { get; set; } = [];
    public SortedDictionary<string, long> Ops { get; set; } = [];
    public SortedDictionary<string, long> Notes { get; set; } = [];
    /// <summary>File-corruption trials: "target/outcome" -> count.</summary>
    public SortedDictionary<string, long> Corruption { get; set; } = [];
    public int NativeChecks { get; set; }
    public LastVerify? LastVerify { get; set; }
    public List<string> Failures { get; set; } = [];
    public string Result { get; set; } = "running";

    public void Save(string path)
    {
        UpdatedUtc = DateTime.UtcNow.ToString("O");
        string tmp = path + ".tmp";
        File.WriteAllText(tmp, JsonSerializer.Serialize(this, RunConfig.Json));
        File.Move(tmp, path, overwrite: true);
    }

    public static Status? Load(string path) =>
        File.Exists(path) ? JsonSerializer.Deserialize<Status>(File.ReadAllText(path), RunConfig.Json) : null;
}

internal sealed class LastVerify
{
    public int Cycle { get; set; }
    public bool Pass { get; set; }
    public string Fault { get; set; } = "";
    public List<string> Steps { get; set; } = [];
}

/// <summary>The long-lived supervisor (never crashed on purpose): RUN, FAULT, VERIFY cycles until the duration ends.</summary>
internal sealed class Supervisor
{
    private readonly RunPaths _paths;
    private readonly RunConfig _cfg;
    private readonly Status _status;
    private readonly Stopwatch _clock = Stopwatch.StartNew();
    private readonly double _elapsedBefore;
    private readonly TimeSpan _duration;
    private volatile bool _stopRequested;
    private readonly CancellationTokenSource _stopCts = new();
    private double _lastOpenMs = 300;
    private readonly LazyFsMount? _lazyfs;
    private Thread? _watchdog;
    /// <summary>Set by the watchdog when the LazyFS mount stopped answering (an infrastructure failure).</summary>
    private volatile string? _lazyfsHung;

    public Supervisor(RunPaths paths, RunConfig cfg, TimeSpan duration)
    {
        _paths = paths;
        _cfg = cfg;
        _duration = duration;
        _status = Status.Load(paths.Status) ?? new Status { Seed = cfg.Seed, StartedUtc = cfg.StartedUtc };
        _elapsedBefore = _status.ElapsedSeconds;
        _status.DurationSeconds = _elapsedBefore + duration.TotalSeconds;
        _status.Result = "running";
        if (cfg.LazyFsBinary is not null) _lazyfs = new LazyFsMount(cfg.LazyFsBinary, paths, cfg.LazyFsCache, cfg.LazyFsEviction, Log);
    }

    private void Log(string s)
    {
        string line = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss") + " " + s;
        Console.WriteLine(line);
        File.AppendAllText(_paths.Log, line + Environment.NewLine);
    }

    private void SetPhase(string phase, int cycle, string fault)
    {
        _status.Phase = phase;
        _status.Cycle = cycle;
        _status.Fault = fault;
        _status.ElapsedSeconds = _elapsedBefore + _clock.Elapsed.TotalSeconds;
        _status.Save(_paths.Status);
    }

    /// <summary>
    /// Probes the LazyFS mount every 15 s with an external <c>stat</c> (a stuck FUSE request would also block a
    /// thread of this process). After 3 consecutive probes without an answer the mount is declared hung: the FUSE
    /// connection is aborted (every blocked request fails with ENOTCONN, so the current phase ends) and the cycle is
    /// reported as an infrastructure failure, not as a failure of the port.
    /// </summary>
    private void StartWatchdog()
    {
        if (!_cfg.UsesLazyFs) return;
        _watchdog = new Thread(LazyFsWatchdog) { IsBackground = true, Name = "lazyfs-watchdog" };
        _watchdog.Start();
    }

    /// <summary>
    /// After a LazyFS hang (the daemon was killed and the connection aborted): remount LazyFS and verify the cycle
    /// again with power-loss expectations. Restarting LazyFS loses its unsynced cache, which is exactly a power loss;
    /// writes acknowledged as synced were on the backing disk before LazyFS acknowledged the fsync, so the
    /// power-loss checks apply unchanged. Returns 0 when the re-verification passed (the cycle is folded).
    /// </summary>
    private int RecoverFromLazyFsHang(CyclePlan plan)
    {
        Bump(_status.Notes, "lazyfs-hang-restarts");
        Log($"cycle {plan.Cycle}: restarting LazyFS after the hang (its unsynced cache is lost: a power loss); " +
            "verifying the cycle again with power-loss expectations");
        try
        {
            _lazyfs!.Unmount();
            _lazyfs.Mount();
            _lazyfsHung = null;
            StartWatchdog();
            plan.Fault = FaultKind.LazyFsClear;
            plan.CorruptTrial = false;
            plan.Save(_paths.CycleJson);
            Bump(_status.Faults, "LazyFsHangRestart");
            int v = Verify(plan);
            if (v != 0) return v;
            Fold(plan);
            return 0;
        }
        catch (IOException e) when (_lazyfsHung is not null)
        {
            return LazyFsHungFailure(plan, e.Message);
        }
        catch (InvalidOperationException e)
        {
            Log("could not restart LazyFS: " + e.Message);
            return 3;
        }
    }

    private void LazyFsWatchdog()
    {
        int misses = 0;
        while (_lazyfsHung is null)
        {
            Thread.Sleep(15_000);
            if (_status.Result is "passed" or "failed") return;
            if (LazyFsMount.Probe(_paths.Db, TimeSpan.FromSeconds(20)))
            {
                misses = 0;
                continue;
            }
            if (++misses < 3) continue;
            _lazyfsHung = $"the LazyFS mount {_paths.Db} did not answer `ls -f` for 3 probes (~2 min)";
            Log("LAZYFS HUNG: " + _lazyfsHung + "; aborting the FUSE connection");
            if (_lazyfs is not null) _lazyfs.Abort();
            else LazyFsMount.AbortConnection(_paths.Db, Log);
            return;
        }
    }

    /// <summary>
    /// The LazyFS daemon stopped answering: keep the cycle's logs and the LazyFS log, return 3. The run loop restarts
    /// LazyFS when the supervisor owns the mount (<c>--lazyfs-bin</c>), otherwise it stops with exit code 3.
    /// </summary>
    private int LazyFsHungFailure(CyclePlan plan, string reason)
    {
        string dir = Path.Combine(_paths.Failures, $"cycle-{plan.Cycle:D6}-lazyfs-hung");
        Directory.CreateDirectory(dir);
        try
        {
            Directory.CreateDirectory(Path.Combine(dir, "logs"));
            foreach (string f in Directory.GetFiles(_paths.Logs, $"cycle-{plan.Cycle:D6}-*")) File.Copy(f, Path.Combine(dir, "logs", Path.GetFileName(f)), true);
            if (_lazyfs is not null && File.Exists(_lazyfs.LogFile)) File.Copy(_lazyfs.LogFile, Path.Combine(dir, "lazyfs.log"), true);
        }
        catch (Exception e)
        {
            Log("could not preserve everything: " + e.Message);
        }
        string summary =
            $"INFRASTRUCTURE FAILURE in cycle {plan.Cycle}: {_lazyfsHung}\n" +
            $"This is not a check failure of the port: LazyFS stopped answering FUSE requests (a lock-order deadlock " +
            $"inside LazyFS; see docs/soak.md, " +
            $"\"Real LazyFS mode\"). The phase then ended with: {reason.Split('\n')[0]}\n" +
            $"seed {_cfg.Seed}, cycle seed {plan.CycleSeed}, fault {plan.Fault}; LazyFS cache {_cfg.LazyFsCache}, eviction {_cfg.LazyFsEviction}\n";
        File.WriteAllText(Path.Combine(dir, "summary.txt"), summary);
        Bump(_status.Notes, "lazyfs-hangs");
        Log(summary);
        Log("preserved in " + dir);
        return 3;
    }

    private static void Bump(SortedDictionary<string, long> d, string k, long n = 1) => d[k] = d.GetValueOrDefault(k) + n;

    public int Run()
    {
        using PosixSignalRegistration sigint = PosixSignalRegistration.Create(PosixSignal.SIGINT, OnSignal);
        using PosixSignalRegistration sigterm = PosixSignalRegistration.Create(PosixSignal.SIGTERM, OnSignal);
        try
        {
            _lazyfs?.Mount();
            StartWatchdog();
            if (!File.Exists(_paths.ModelFile))
            {
                SetPhase("init", 0, "");
                Log($"initialising {_paths.Db} (seed {_cfg.Seed}; {_cfg.Db})");
                (int code, string tail) = Child.Run(["init", "--run-dir", _paths.Root], TimeSpan.FromMinutes(5), Path.Combine(_paths.Logs, "init.log"));
                if (code != 0)
                {
                    Log("init failed: " + tail);
                    return 2;
                }
            }
            Log($"run {_paths.Root}: seed {_cfg.Seed}, duration {_duration}, db: {_cfg.Db}");
            bool lastNative = false;
            while (!_stopRequested && _clock.Elapsed < _duration)
            {
                Model model = Model.Load(_paths.ModelFile);
                CyclePlan plan = Plan(model.Cycle);
                int r;
                try { r = RunCycle(plan); }
                catch (IOException e) when (_lazyfsHung is not null) { r = LazyFsHungFailure(plan, e.Message); }
                for (int restarts = 0; r == 3 && _lazyfs is not null && restarts < 3; restarts++)
                    r = RecoverFromLazyFsHang(plan);
                if (r == 3)
                {
                    _status.Result = "infrastructure-failure";
                    SetPhase("failed", plan.Cycle, plan.Fault.ToString());
                }
                if (r != 0) return r;
                lastNative = plan.NativeCheck;
            }
            if (!lastNative)
            {
                // A final verification with the native cross-check (no RUN phase).
                Model model = Model.Load(_paths.ModelFile);
                CyclePlan plan = Plan(model.Cycle);
                plan.Fault = FaultKind.Graceful;
                plan.RunSeconds = 0;
                plan.NativeCheck = true;
                plan.CorruptTrial = false;
                plan.FaultFs = false;
                plan.Save(_paths.CycleJson);
                Log($"cycle {plan.Cycle}: final VERIFY with native cross-check");
                int r = Verify(plan);
                if (r != 0) return r;
                Fold(plan);
            }
            _status.Result = "passed";
            SetPhase("done", _status.Cycle, "");
            Log($"done: {_status.CyclesCompleted} cycles, no failures. Faults: {string.Join(", ", Fmt(_status.Faults))}");
            return 0;
        }
        finally
        {
            _lazyfs?.Unmount();
        }
    }

    private static IEnumerable<string> Fmt(SortedDictionary<string, long> d)
    {
        foreach (KeyValuePair<string, long> kv in d) yield return kv.Key + "=" + kv.Value;
    }

    private void OnSignal(PosixSignalContext ctx)
    {
        ctx.Cancel = true;
        if (!_stopRequested) Log("stop requested: finishing the current phase");
        _stopRequested = true;
        _stopCts.Cancel();
    }

    public CyclePlan Plan(int cycle)
    {
        ulong cycleSeed = Keys.Mix(_cfg.Seed * 0x9E3779B97F4A7C15UL + (ulong)cycle);
        var r = new Random((int)(cycleSeed & 0x7FFFFFFF));
        int total = 0;
        foreach (KeyValuePair<string, int> kv in _cfg.FaultWeights) total += kv.Value;
        int x = r.Next(Math.Max(1, total));
        FaultKind fault = FaultKind.Kill;
        foreach (KeyValuePair<string, int> kv in _cfg.FaultWeights)
        {
            if (x < kv.Value)
            {
                fault = Enum.Parse<FaultKind>(kv.Key);
                break;
            }
            x -= kv.Value;
        }
        if (fault == FaultKind.LazyFsClear && !_cfg.UsesLazyFs) fault = FaultKind.Kill;
        if (_cfg.UsesLazyFs && fault is FaultKind.PowerLoss or FaultKind.TornOp or FaultKind.TornSeq or FaultKind.FsyncTrigger or FaultKind.IoError)
            fault = FaultKind.LazyFsClear; // the DB is on a LazyFS mount: its page cache is the fault
        double run = -Math.Log(1 - r.NextDouble()) * _cfg.MeanRunSeconds;
        run = Math.Clamp(run, _cfg.MinRunSeconds, _cfg.MaxRunSeconds);
        return new CyclePlan
        {
            Cycle = cycle,
            CycleSeed = cycleSeed,
            Fault = fault,
            RunSeconds = Math.Round(run, 1),
            FaultFs = _cfg.UnsafeSkipFsync || fault is FaultKind.PowerLoss or FaultKind.TornOp or FaultKind.TornSeq or FaultKind.FsyncTrigger or FaultKind.IoError,
            SkipSync = _cfg.UnsafeSkipFsync,
            SkipRealSync = _cfg.UnsafeSkipFsync,
            Options = CycleOptions.FromSeed(cycleSeed),
            NativeCheck = _cfg.NativeEvery > 0 && cycle % _cfg.NativeEvery == 0,
            CorruptTrial = _cfg.CorruptEvery > 0 && cycle % _cfg.CorruptEvery == 0,
        };
    }

    private int RunCycle(CyclePlan plan)
    {
        plan.Save(_paths.CycleJson);
        Log($"cycle {plan.Cycle}: RUN {plan.RunSeconds:F1} s, fault {plan.Fault}{(plan.FaultFs ? " (fault-injecting fs)" : "")}; {plan.Options}");
        SetPhase("run", plan.Cycle, plan.Fault.ToString());
        string? failure = RunPhase(plan);
        if (failure is not null) return Fail(plan, "RUN: " + failure);
        Bump(_status.Faults, plan.Fault.ToString());
        int v = Verify(plan);
        if (v != 0) return v;
        if (plan.CorruptTrial)
        {
            SetPhase("corrupt", plan.Cycle, plan.Fault.ToString());
            string? cf = new CorruptionNemesis(_paths, Log).Trial(plan, _status.Corruption);
            if (cf is not null) return Fail(plan, "CORRUPTION TRIAL: " + cf);
        }
        Fold(plan);
        return 0;
    }

    private int Verify(CyclePlan plan)
    {
        SetPhase("verify", plan.Cycle, plan.Fault.ToString());
        var sw = Stopwatch.StartNew();
        (int code, string tail) = Child.Run(["verify", "--run-dir", _paths.Root], TimeSpan.FromMinutes(20), _paths.LogFile(plan.Cycle, "verify"));
        VerifyReport? report = File.Exists(_paths.VerifyJson(plan.Cycle))
            ? JsonSerializer.Deserialize<VerifyReport>(File.ReadAllText(_paths.VerifyJson(plan.Cycle)), RunConfig.Json)
            : null;
        if (report is not null)
        {
            foreach (KeyValuePair<string, long> kv in report.Ops) Bump(_status.Ops, kv.Key, kv.Value);
            _status.LastVerify = new LastVerify
            {
                Cycle = plan.Cycle, Pass = report.Pass, Fault = report.Fault,
                Steps = report.Steps.ConvertAll(static s => $"{(s.Pass ? "PASS" : "FAIL")} {s.Name} ({s.Ms:F0} ms) {s.Detail}"),
            };
            if (report.Steps.Exists(static s => s.Name == "native-cross-check" && s.Pass)) _status.NativeChecks++;
            long ops = 0;
            foreach (KeyValuePair<string, long> kv in report.Ops) ops += kv.Value;
            Log($"cycle {plan.Cycle}: VERIFY {(report.Pass ? "passed" : "FAILED")} in {sw.Elapsed.TotalSeconds:F1} s ({ops} ops in the history)");
            foreach (StepResult s in report.Steps) Log("    " + s.ToString().Replace("\n", "\n    ", StringComparison.Ordinal));
        }
        if (code != 0) return Fail(plan, $"VERIFY: verifier {Child.Describe(code)}" + (report is null ? "\n" + tail : ""));
        return 0;
    }

    private void Fold(CyclePlan plan)
    {
        File.Move(_paths.NextModelFile, _paths.ModelFile, overwrite: true);
        // Keep the histories of the last KeepHistory cycles.
        int old = plan.Cycle - _cfg.KeepHistory;
        if (old >= 0)
        {
            foreach (string p in _paths.HistoryParts(old)) File.Delete(p);
            foreach (string f in Directory.GetFiles(_paths.Logs, $"cycle-{old:D6}-*")) File.Delete(f);
        }
        _status.CyclesCompleted++;
        SetPhase("folded", plan.Cycle, plan.Fault.ToString());
    }

    /// <summary>Preserves everything about a failed cycle (DB first) and stops.</summary>
    private int Fail(CyclePlan plan, string reason)
    {
        // A phase can give up on a blocked child just before the watchdog decides; probe once more before blaming
        // the port.
        if (_lazyfsHung is null && _cfg.UsesLazyFs && !LazyFsMount.Probe(_paths.Db, TimeSpan.FromSeconds(30)))
        {
            _lazyfsHung = $"the LazyFS mount {_paths.Db} did not answer `ls -f` within 30 s after: {reason.Split('\n')[0]}";
            Log("LAZYFS HUNG: " + _lazyfsHung + "; aborting the FUSE connection");
            if (_lazyfs is not null) _lazyfs.Abort();
            else LazyFsMount.AbortConnection(_paths.Db, Log);
        }
        if (_lazyfsHung is not null) return LazyFsHungFailure(plan, reason);
        string dir = Path.Combine(_paths.Failures, $"cycle-{plan.Cycle:D6}");
        Directory.CreateDirectory(dir);
        try
        {
            CopyDirectory(_paths.Db, Path.Combine(dir, "db"));
            Directory.CreateDirectory(Path.Combine(dir, "history"));
            foreach (string p in _paths.HistoryParts(plan.Cycle)) File.Copy(p, Path.Combine(dir, "history", Path.GetFileName(p)), true);
            foreach (string f in new[] { _paths.ModelFile, _paths.RunJson, _paths.CycleJson })
                if (File.Exists(f)) File.Copy(f, Path.Combine(dir, Path.GetFileName(f)), true);
            Directory.CreateDirectory(Path.Combine(dir, "verify"));
            if (File.Exists(_paths.VerifyJson(plan.Cycle))) File.Copy(_paths.VerifyJson(plan.Cycle), Path.Combine(dir, "verify", Path.GetFileName(_paths.VerifyJson(plan.Cycle))), true);
            Directory.CreateDirectory(Path.Combine(dir, "logs"));
            foreach (string f in Directory.GetFiles(_paths.Logs, $"cycle-{plan.Cycle:D6}-*")) File.Copy(f, Path.Combine(dir, "logs", Path.GetFileName(f)), true);
        }
        catch (Exception e)
        {
            Log("could not preserve everything: " + e.Message);
        }
        string summary =
            $"FAILURE in cycle {plan.Cycle}\n{reason}\n\nseed {_cfg.Seed}, cycle seed {plan.CycleSeed}, fault {plan.Fault}, run {plan.RunSeconds} s\n" +
            $"options: {plan.Options}\ndb: {_cfg.Db}\n\n" +
            $"Re-run the verifier on the preserved state:\n  RocksDb.Managed.Soak verify --run-dir {dir}\n" +
            $"Export the cycle's history:\n  RocksDb.Managed.Soak export-history --run-dir {dir} --cycle {plan.Cycle} --format edn --out {dir}/history.edn\n" +
            $"Replay the same plan (same faults and options; timing differs):\n  tools/soak.sh --dir <new dir> --seed {_cfg.Seed} --duration ...\n";
        File.WriteAllText(Path.Combine(dir, "summary.txt"), summary);
        _status.Failures.Add($"cycle {plan.Cycle} ({plan.Fault}): {reason.Split('\n')[0]}");
        _status.Result = "failed";
        SetPhase("failed", plan.Cycle, plan.Fault.ToString());
        Log(summary);
        Log("preserved in " + dir);
        return 1;
    }

    public static void CopyDirectory(string from, string to)
    {
        Directory.CreateDirectory(to);
        foreach (string f in Directory.GetFiles(from)) File.Copy(f, Path.Combine(to, Path.GetFileName(f)), true);
        foreach (string d in Directory.GetDirectories(from)) CopyDirectory(d, Path.Combine(to, Path.GetFileName(d)));
    }

    // ---- the RUN phase and its fault ----

    private bool Wait(TimeSpan t, ChildProcess w)
    {
        // Returns false if the worker exited by itself while waiting.
        var sw = Stopwatch.StartNew();
        while (sw.Elapsed < t && !_stopRequested)
        {
            if (w.HasExited) return false;
            Thread.Sleep(50);
        }
        return !w.HasExited;
    }

    private string Unexpected(ChildProcess w, int? code) =>
        code == Worker.ExitViolation
            ? "online check violation in the worker:\n" + w.Tail
            : $"worker ended unexpectedly with {(code is null ? "no exit" : Child.Describe(code.Value))}:\n{w.Tail}";

    /// <summary>Runs the worker and ends it with the cycle's fault. Returns a failure description or null.</summary>
    private string? RunPhase(CyclePlan plan)
    {
        var r = new Random((int)(Keys.Mix(plan.CycleSeed ^ 0xFA) & 0x7FFFFFFF));
        using ChildProcess w = ChildProcess.Start(["worker", "--run-dir", _paths.Root, "--part", "0"], _paths.LogFile(plan.Cycle, "worker"));
        if (!w.Opened.Wait(TimeSpan.FromMinutes(5)))
        {
            w.Kill();
            return w.HasExited ? Unexpected(w, w.WaitAsync(TimeSpan.FromSeconds(10)).Result) : "worker open hung (5 min)\n" + w.Tail;
        }
        _lastOpenMs = w.Opened.Result;
        TimeSpan run = TimeSpan.FromSeconds(plan.RunSeconds);
        switch (plan.Fault)
        {
            case FaultKind.Kill:
            case FaultKind.LazyFsClear:
            {
                if (!Wait(run, w)) return Unexpected(w, w.WaitAsync(TimeSpan.FromSeconds(10)).Result);
                w.Kill();
                int? c = w.WaitAsync(TimeSpan.FromSeconds(60)).Result;
                if (c is null) return "worker did not die after SIGKILL";
                if (plan.Fault == FaultKind.LazyFsClear) return LazyFsClearCache();
                return null;
            }
            case FaultKind.Graceful:
                return Stop(w, run, allowCloseFailure: false);
            case FaultKind.PowerLoss:
            case FaultKind.TornOp:
            case FaultKind.TornSeq:
            {
                if (!Wait(run, w)) return Unexpected(w, w.WaitAsync(TimeSpan.FromSeconds(10)).Result);
                string mode = plan.Fault switch { FaultKind.TornOp => "tornop", FaultKind.TornSeq => "tornseq", _ => "clear" };
                w.Send($"crash {mode} {r.Next()}");
                return ExpectCrash(w);
            }
            case FaultKind.FsyncTrigger:
            {
                string[] kinds = ["wal", "manifest", "sst"];
                string kind = kinds[r.Next(kinds.Length)];
                int n = kind == "wal" ? r.Next(1, 40) : r.Next(1, 12);
                w.Send($"arm {kind} {n}");
                if (Wait(run, w))
                {
                    Bump(_status.Notes, "fsync-trigger-not-reached");
                    w.Send($"crash clear {r.Next()}");
                }
                else Bump(_status.Notes, "fsync-trigger-" + kind);
                return ExpectCrash(w);
            }
            case FaultKind.RecoveryKill:
            {
                if (!Wait(run, w)) return Unexpected(w, w.WaitAsync(TimeSpan.FromSeconds(10)).Result);
                w.Kill();
                w.WaitAsync(TimeSpan.FromSeconds(60)).Wait();
                double delay = r.NextDouble() * (_lastOpenMs * 1.5 + 100);
                using ChildProcess w2 = ChildProcess.Start(["worker", "--run-dir", _paths.Root, "--part", "1"], _paths.LogFile(plan.Cycle, "worker-recovery"));
                Thread.Sleep(TimeSpan.FromMilliseconds(delay));
                bool opened = w2.Opened.IsCompleted;
                if (w2.HasExited) return "the recovering worker exited by itself:\n" + w2.Tail;
                w2.Kill();
                w2.WaitAsync(TimeSpan.FromSeconds(60)).Wait();
                Bump(_status.Notes, opened ? "recovery-kill-after-open" : "recovery-kill-during-open");
                Log($"cycle {plan.Cycle}: second open killed after {delay:F0} ms ({(opened ? "it had opened" : "during recovery")})");
                return null;
            }
            case FaultKind.Pause:
            {
                var sw = Stopwatch.StartNew();
                int pauses = 0;
                while (sw.Elapsed < run && !_stopRequested)
                {
                    if (!Wait(TimeSpan.FromMilliseconds(r.Next(300, 3000)), w)) return Unexpected(w, w.WaitAsync(TimeSpan.FromSeconds(10)).Result);
                    w.Signal(ChildProcess.SigStop);
                    Thread.Sleep(r.Next(100, 5000));
                    w.Signal(ChildProcess.SigCont);
                    pauses++;
                }
                Bump(_status.Notes, "pauses", pauses);
                if (r.Next(2) == 0) return Stop(w, TimeSpan.Zero, allowCloseFailure: false);
                w.Kill();
                w.WaitAsync(TimeSpan.FromSeconds(60)).Wait();
                return null;
            }
            case FaultKind.IoError:
            {
                if (!Wait(run / 3, w)) return Unexpected(w, w.WaitAsync(TimeSpan.FromSeconds(10)).Result);
                int[] rates = [200, 1000, 5000];
                // Plain IOErrors in flush / WAL writes are fatal background errors upstream (Resume refuses them until a
                // reopen); retryable ones and NoSpace are recoverable (auto-recovery or Resume).
                string[] kinds = ["plain", "retryable", "nospace"];
                string kind = kinds[r.Next(kinds.Length)];
                Bump(_status.Notes, "io-error-kind-" + kind);
                w.Send($"ioerr {rates[r.Next(3)]} {rates[r.Next(3)]} {rates[r.Next(3)] / 2} {kind}");
                if (!Wait(run / 3, w)) return Unexpected(w, w.WaitAsync(TimeSpan.FromSeconds(10)).Result);
                w.Send("ioerr-off");
                if (!Wait(run / 3 + TimeSpan.FromSeconds(1), w)) return Unexpected(w, w.WaitAsync(TimeSpan.FromSeconds(10)).Result);
                string? result;
                if (r.Next(2) == 0) result = Stop(w, TimeSpan.Zero, allowCloseFailure: true);
                else
                {
                    w.Kill();
                    w.WaitAsync(TimeSpan.FromSeconds(60)).Wait();
                    result = null;
                }
                CountIoErrorNotes(plan);
                return result;
            }
            default:
                return "unknown fault " + plan.Fault;
        }
    }

    /// <summary>Background errors and Resume after an I/O error window, from the worker log.</summary>
    private void CountIoErrorNotes(CyclePlan plan)
    {
        string log = _paths.LogFile(plan.Cycle, "worker");
        if (!File.Exists(log)) return;
        long injected = 0;
        int resumeOk = 0, resumeFailed = 0;
        foreach (string line in File.ReadLines(log))
        {
            int i = line.IndexOf("I/O errors off (", StringComparison.Ordinal);
            if (i >= 0)
            {
                string n = line[(i + 16)..];
                n = n[..n.IndexOf(' ')];
                injected += long.Parse(n, System.Globalization.CultureInfo.InvariantCulture);
            }
            if (line.Contains("RESUME ok", StringComparison.Ordinal)) resumeOk++;
            if (line.Contains("RESUME failed", StringComparison.Ordinal)) resumeFailed++;
        }
        Bump(_status.Notes, "io-errors-injected", injected);
        Bump(_status.Notes, "resume-ok", resumeOk);
        if (resumeFailed > 0) Bump(_status.Notes, "resume-attempts-failed", resumeFailed);
        Log($"cycle {plan.Cycle}: {injected} I/O errors injected; Resume ok {resumeOk}, failed attempts {resumeFailed}");
    }

    private string? Stop(ChildProcess w, TimeSpan after, bool allowCloseFailure)
    {
        if (!Wait(after, w)) return Unexpected(w, w.WaitAsync(TimeSpan.FromSeconds(10)).Result);
        w.Send("stop");
        int? c = w.WaitAsync(TimeSpan.FromMinutes(5)).Result;
        if (c is null)
        {
            w.Kill();
            return "graceful shutdown hung (5 min)\n" + w.Tail;
        }
        if (c == 0) return null;
        if (c == Worker.ExitCloseFailed && allowCloseFailure)
        {
            Bump(_status.Notes, "close-failed-after-io-errors");
            return null;
        }
        return Unexpected(w, c);
    }

    private string? ExpectCrash(ChildProcess w)
    {
        int? c = w.WaitAsync(TimeSpan.FromMinutes(3)).Result;
        if (c is null)
        {
            w.Kill();
            return "the simulated crash did not end the worker (3 min)\n" + w.Tail;
        }
        foreach (string line in w.Tail.Split('\n'))
        {
            if (line.Contains(" CRASH ", StringComparison.Ordinal) || line.Contains("fsync trigger", StringComparison.Ordinal))
                Log("    worker: " + line[(line.IndexOf(' ') + 1)..]);
            if (line.Contains("torn-seq on", StringComparison.Ordinal)) Bump(_status.Notes, "torn-seq-applied");
            else if (line.Contains("torn-op on", StringComparison.Ordinal)) Bump(_status.Notes, "torn-op-applied");
            else if (line.Contains("no torn candidate", StringComparison.Ordinal)) Bump(_status.Notes, "torn-no-candidate");
        }
        return c == Worker.ExitCrash ? null : Unexpected(w, c);
    }

    private string? LazyFsClearCache()
    {
        string? fifo = _lazyfs?.Fifo ?? _cfg.LazyFsFifo;
        string? done = _lazyfs?.FifoCompleted ?? _cfg.LazyFsFifoCompleted;
        if (fifo is null) return "LazyFsClear without a LazyFS FIFO";
        return LazyFsMount.ClearCache(fifo, done, Log);
    }
}

/// <summary>
/// A LazyFS mount for the DB directory, as Jepsen's jepsen.lazyfs does it: the backing root on the real disk
/// (lazyfs/data), the FIFOs, a config file, and the daemon started with <c>-o allow_other -o modules=subdir
/// -o subdir=root</c>. Requires the lazyfs binary, FUSE 3 and <c>user_allow_other</c> in /etc/fuse.conf.
/// </summary>
internal sealed class LazyFsMount(string binary, RunPaths paths, string cacheSize, bool eviction, Action<string> log)
{
    private Process? _daemon;
    private string Dir => Path.Combine(paths.Root, "lazyfs");
    public string Data => Path.Combine(Dir, "data");
    public string LogFile => Path.Combine(Dir, "lazyfs.log");
    public string Fifo => Path.Combine(Dir, "fifo");
    public string FifoCompleted => Path.Combine(Dir, "fifo-completed");
    private string ConfigFile => Path.Combine(Dir, "config.toml");

    public void Mount()
    {
        Directory.CreateDirectory(Data);
        Directory.CreateDirectory(paths.Db);
        foreach (string f in new[] { Fifo, FifoCompleted })
            if (!File.Exists(f)) Exec("mkfifo", f);
        File.WriteAllText(ConfigFile,
            $"[faults]\nfifo_path=\"{Fifo}\"\nfifo_path_completed=\"{FifoCompleted}\"\n\n[cache]\napply_eviction={(eviction ? "true" : "false")}\n\n" +
            $"[cache.simple]\ncustom_size=\"{cacheSize}\"\nblocks_per_page=1\n\n[filesystem]\nlogfile=\"{Path.Combine(Dir, "lazyfs.log")}\"\nlog_all_operations=false\n");
        var psi = new ProcessStartInfo(binary) { UseShellExecute = false, RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (string a in new[] { paths.Db, "--config-path", ConfigFile, "-o", "allow_other", "-o", "modules=subdir", "-o", "subdir=" + Data, "-f" })
            psi.ArgumentList.Add(a);
        _daemon = Process.Start(psi) ?? throw new InvalidOperationException("could not start lazyfs");
        _daemon.OutputDataReceived += (_, _) => { };
        _daemon.ErrorDataReceived += (_, _) => { };
        _daemon.BeginOutputReadLine();
        _daemon.BeginErrorReadLine();
        var sw = Stopwatch.StartNew();
        while (Exec("mountpoint", "-q", paths.Db) != 0)
        {
            if (sw.Elapsed > TimeSpan.FromSeconds(30) || _daemon.HasExited) throw new InvalidOperationException("lazyfs did not mount " + paths.Db);
            Thread.Sleep(200);
        }
        log($"lazyfs mounted at {paths.Db} (backing root {Data}, fifo {Fifo})");
    }

    public void Unmount()
    {
        try
        {
            if (Exec("mountpoint", "-q", paths.Db) == 0)
            {
                // Like Jepsen: drop the cache first, so the unmount does not flush it.
                try { ClearCache(Fifo, FifoCompleted, log); }
                catch (Exception) { /* best effort */ }
            }
            // Also when mountpoint fails: an aborted connection leaves a stale mount ("Transport endpoint is not
            // connected") that only the lazy unmount removes.
            Exec("fusermount3", "-uz", paths.Db);
            if (_daemon is { HasExited: false })
            {
                if (!_daemon.WaitForExit(10_000)) _daemon.Kill();
            }
            log("lazyfs unmounted");
        }
        catch (Exception e)
        {
            log("lazyfs unmount failed: " + e.Message);
        }
    }

    /// <summary>Kills the daemon; the kernel then aborts the FUSE connection (pending requests fail with ENOTCONN).</summary>
    public void Abort()
    {
        try
        {
            if (_daemon is { HasExited: false }) _daemon.Kill();
        }
        catch (InvalidOperationException) { }
        AbortConnection(paths.Db, log);
    }

    /// <summary>Aborts the FUSE connection of a mount through fusectl (<c>/sys/fs/fuse/connections/N/abort</c>).</summary>
    public static void AbortConnection(string mountPoint, Action<string> log)
    {
        try
        {
            string mp = Path.TrimEndingDirectorySeparator(Path.GetFullPath(mountPoint));
            foreach (string line in File.ReadLines("/proc/self/mountinfo"))
            {
                string[] f = line.Split(' ');
                if (f.Length < 5 || f[4] != mp) continue;
                string minor = f[2][(f[2].IndexOf(':') + 1)..];
                string abort = $"/sys/fs/fuse/connections/{minor}/abort";
                if (File.Exists(abort)) File.WriteAllText(abort, "1");
                log("FUSE connection " + minor + " aborted");
                return;
            }
        }
        catch (Exception e)
        {
            log("could not abort the FUSE connection: " + e.Message);
        }
    }

    /// <summary>True when <c>ls -f</c> of the mount point answers within the timeout (run as a separate process).</summary>
    public static bool Probe(string mountPoint, TimeSpan timeout)
    {
        // ls -f: a readdir without per-file getattrs. A stat of the mount point alone is not enough (LazyFS can
        // deadlock its per-file locks against its global inode lock while still answering the root's getattr), and
        // getattrs of every file would add load on exactly the LazyFS path that deadlocks.
        var psi = new ProcessStartInfo("ls") { UseShellExecute = false, RedirectStandardOutput = true, RedirectStandardError = true };
        psi.ArgumentList.Add("-f");
        psi.ArgumentList.Add(mountPoint);
        using Process p = Process.Start(psi)!;
        if (p.WaitForExit(timeout)) return true; // any answer, even an error, means the daemon replied
        try { p.Kill(); }
        catch (InvalidOperationException) { }
        return false;
    }

    /// <summary>Writes lazyfs::clear-cache to the FIFO and waits for finished::clear-cache on the completion FIFO.</summary>
    public static string? ClearCache(string fifo, string? completed, Action<string> log)
    {
        Task<string?>? wait = null;
        if (completed is not null)
        {
            wait = Task.Run(() =>
            {
                using var r = new StreamReader(completed);
                return r.ReadLine();
            });
        }
        var write = Task.Run(() => File.WriteAllText(fifo, "lazyfs::clear-cache\n"));
        if (!write.Wait(TimeSpan.FromSeconds(30))) return "writing lazyfs::clear-cache to " + fifo + " timed out";
        if (wait is not null)
        {
            if (!wait.Wait(TimeSpan.FromSeconds(60))) return "no finished::clear-cache on " + completed;
            if (wait.Result?.Trim() != "finished::clear-cache") return "unexpected completion message: " + wait.Result;
        }
        log("lazyfs: clear-cache done");
        return null;
    }

    private static int Exec(string file, params string[] args)
    {
        var psi = new ProcessStartInfo(file) { UseShellExecute = false, RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (string a in args) psi.ArgumentList.Add(a);
        using Process p = Process.Start(psi)!;
        p.WaitForExit();
        return p.ExitCode;
    }
}
