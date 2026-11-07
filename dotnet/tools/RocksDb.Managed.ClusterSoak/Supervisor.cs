using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text.Json;

namespace RocksDb.Managed.ClusterSoak;

internal sealed class StepTotal
{
    public long Runs { get; set; }
    public long Failures { get; set; }
    public double TotalMs { get; set; }
    public double MaxMs { get; set; }
}

/// <summary>status.json: rewritten after every phase and every 10 s while running.</summary>
internal sealed class Status
{
    public string Phase { get; set; } = "init";
    public int Epoch { get; set; }
    public ulong Seed { get; set; }
    public int Nodes { get; set; }
    public double ElapsedSeconds { get; set; }
    public double DurationSeconds { get; set; }
    public int Verifies { get; set; }
    public Dictionary<string, long> Faults { get; set; } = [];
    public Dictionary<string, long> Ops { get; set; } = [];
    public Dictionary<string, long> Stats { get; set; } = [];
    public Dictionary<string, long> OpsByFaultPhase { get; set; } = [];
    public long TermsWithLeader { get; set; }
    public double ObservedSeconds { get; set; }
    public double LeaderlessSeconds { get; set; }
    public long LeaderlessIntervals { get; set; }
    public double LongestLeaderlessSeconds { get; set; }
    public Dictionary<string, double> LeaderlessIntervalSeconds { get; set; } = [];
    public Dictionary<string, double> WriteLatencyMs { get; set; } = [];
    public int NativeChecks { get; set; }
    public Dictionary<string, StepTotal> StepTotals { get; set; } = [];
    public VerifyReport? LastVerify { get; set; }
    public List<string> Failures { get; set; } = [];
    public string Result { get; set; } = "running";
}

/// <summary>The control node: RUN epochs with clients + nemesis, VERIFY phases, failure preservation.</summary>
internal sealed class Supervisor
{
    private readonly RunConfig _cfg;
    private readonly RunPaths _p;
    private readonly TimeSpan _duration;
    private readonly StreamWriter _logFile;
    private readonly Stopwatch _clock = Stopwatch.StartNew();
    private readonly ConcurrentDictionary<string, long> _faultCounts = new(StringComparer.Ordinal);
    private Status _status = new();
    private volatile bool _stopRequested;
    private readonly List<double> _allLatencies = [];

    public Supervisor(RunConfig cfg, RunPaths paths, TimeSpan duration)
    {
        _cfg = cfg;
        _p = paths;
        _duration = duration;
        _logFile = new StreamWriter(new FileStream(paths.Log, FileMode.Append, FileAccess.Write, FileShare.Read)) { AutoFlush = true };
        if (File.Exists(paths.StatusJson))
        {
            try
            {
                _status = JsonSerializer.Deserialize<Status>(File.ReadAllText(paths.StatusJson)) ?? new Status();
                foreach (KeyValuePair<string, long> kv in _status.Faults) _faultCounts[kv.Key] = kv.Value;
            }
            catch (JsonException)
            {
                _status = new Status();
            }
        }
        _status.Seed = cfg.Seed;
        _status.Nodes = cfg.Nodes;
        _status.DurationSeconds = duration.TotalSeconds;
        _status.Result = "running";
        _status.Failures.Clear();
    }

    public void Log(string m)
    {
        string line = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture) + " " + m;
        Console.WriteLine(line);
        lock (_logFile) _logFile.WriteLine(line);
    }

    private void SaveStatus(string phase)
    {
        _status.Phase = phase;
        _status.ElapsedSeconds = _clock.Elapsed.TotalSeconds;
        _status.Faults = new Dictionary<string, long>(_faultCounts, StringComparer.Ordinal);
        Json.WriteAtomic(_p.StatusJson, _status);
    }

    private static void Merge(Dictionary<string, long> into, IEnumerable<KeyValuePair<string, long>> add)
    {
        foreach (KeyValuePair<string, long> kv in add) into[kv.Key] = into.GetValueOrDefault(kv.Key) + kv.Value;
    }

    public static Dictionary<string, double> Percentiles(List<double> xs)
    {
        var d = new Dictionary<string, double>(StringComparer.Ordinal);
        if (xs.Count == 0) return d;
        var s = new List<double>(xs);
        s.Sort();
        d["count"] = s.Count;
        d["p50"] = s[(int)(0.50 * (s.Count - 1))];
        d["p95"] = s[(int)(0.95 * (s.Count - 1))];
        d["p99"] = s[(int)(0.99 * (s.Count - 1))];
        d["max"] = s[^1];
        return d;
    }

    public async Task<int> RunAsync()
    {
        using var sigterm = PosixSignalRegistration.Create(PosixSignal.SIGTERM, c =>
        {
            c.Cancel = true;
            _stopRequested = true;
            Log("SIGTERM: finishing the current phase");
        });
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            _stopRequested = true;
            Log("SIGINT: finishing the current phase");
        };
        Log("cluster soak: " + _cfg.Nodes + " nodes, " + _cfg.Clients + " clients, seed " + _cfg.Seed + ", duration " + _duration + ", dir " + _p.Root);
        await using var cluster = new ClusterControl(_cfg, _p, Log, null);
        using var schedule = new StreamWriter(new FileStream(_p.Nemesis, FileMode.Append, FileAccess.Write, FileShare.Read));
        bool fresh = !File.Exists(_p.ModelJson);
        Model model = fresh ? Model.Fresh(_cfg) : JsonSerializer.Deserialize<Model>(File.ReadAllText(_p.ModelJson))!;
        SaveStatus("init");

        string? err = await BringUpAsync(cluster, model, fresh).ConfigureAwait(false);
        if (err != null) return await FailAsync(cluster, model.Epoch + 1, "cluster start: " + err, null).ConfigureAwait(false);

        while (true)
        {
            int epoch = model.Epoch + 1;
            int result = await EpochAsync(cluster, model, epoch, schedule, null).ConfigureAwait(false);
            if (result != 0) return result;
            if (_stopRequested || _clock.Elapsed >= _duration) break;
        }
        Log("stopping the nodes");
        SaveStatus("stopping");
        foreach (NodeHandle h in cluster.Nodes.Values)
        {
            if (!await h.StopAsync(TimeSpan.FromSeconds(30)).ConfigureAwait(false))
            {
                _status.Failures.Add("graceful stop of " + h.Id + " hung at the end of the run");
            }
        }
        foreach (NodeHandle h in cluster.Nodes.Values)
            if (h.UnexpectedExit is { } u) _status.Failures.Add(u);
        _status.Result = _status.Failures.Count == 0 ? (_stopRequested && _clock.Elapsed < _duration ? "stopped" : "passed") : "failed";
        SaveStatus("done");
        Log("result: " + _status.Result + " after " + _status.Verifies + " VERIFY phases");
        return _status.Failures.Count == 0 ? 0 : 1;
    }

    /// <summary>Starts every node (n1, the bootstrap primary, first), waits for Cluster mode, and initialises the bank on a fresh run.</summary>
    public async Task<string?> BringUpAsync(ClusterControl cluster, Model model, bool fresh)
    {
        foreach (string id in cluster.Ids)
        {
            if (!await cluster.StartNodeAsync(id, TimeSpan.FromSeconds(120)).ConfigureAwait(false))
                return "node " + id + " did not start: " + (cluster.Nodes[id].UnexpectedExit ?? "not ready");
        }
        cluster.StartMonitor();
        string? conv;
        if (fresh || !model.BankInitialized)
        {
            // The bootstrap primary leads without an election; the cluster switches to Cluster mode once a quorum
            // replicated something (an empty log never counts as in sync), so the bank initialisation comes first.
            var req = new WriteReq { Kind = WriteKind.PutMany, TimeoutMs = 5000 };
            for (int i = 0; i < _cfg.Accounts; i++)
            {
                req.Keys.Add(Keys.Account(i));
                req.Values.Add(Keys.U64(1000));
            }
            if (!await cluster.WriteIdempotentAsync(req, TimeSpan.FromSeconds(60)).ConfigureAwait(false)) return "bank initialisation not committed";
            conv = await cluster.WaitConvergedAsync(TimeSpan.FromSeconds(_cfg.ConvergeTimeoutSeconds)).ConfigureAwait(false);
            if (conv != null) return conv;
            Dictionary<string, DumpDto> d = await cluster.DumpAllAsync().ConfigureAwait(false);
            Checker.Fold(model, _cfg, d[cluster.Ids[0]], null);
            model.BankInitialized = true;
            Json.WriteAtomic(_p.ModelJson, model);
            Log("cluster up: bank initialised, seq " + model.LatestSeq);
            return null;
        }
        conv = await cluster.WaitConvergedAsync(TimeSpan.FromSeconds(_cfg.ConvergeTimeoutSeconds)).ConfigureAwait(false);
        return conv;
    }

    /// <summary>Hooks for the self-test (inject bugs between the VERIFY steps).</summary>
    public sealed class Hooks
    {
        public Func<ClusterControl, Task>? BeforeConverge;
        public Func<ClusterControl, Task>? AfterConverge;
        /// <summary>Runs after the checks (e.g. repairs an injected bug before the next epoch's clients start).</summary>
        public Func<ClusterControl, Task>? AfterVerify;
        public Action<Dictionary<string, DumpDto>>? OnDumps;
        public Func<List<Op>, List<Op>>? OnHistory;
        public Func<Model, Model>? OnModel;
        public string? EventsDir;
        public TimeSpan? RunTime;
        public bool NoNemesis;
        public Func<ClusterControl, CancellationToken, Task>? DuringRun;
    }

    public VerifyReport? LastReport { get; private set; }

    /// <summary>One RUN phase and its VERIFY. 0 = passed, otherwise the exit code (failure preserved).</summary>
    public async Task<int> EpochAsync(ClusterControl cluster, Model model, int epoch, StreamWriter schedule, Hooks? hooks)
    {
        Log("==== epoch " + epoch + ": RUN");
        _status.Epoch = epoch;
        SaveStatus("run");
        using var history = new History(_p.HistoryFile(epoch));
        var state = new WorkloadState(_cfg, model);
        var nemesis = new Nemesis(cluster, _cfg, history, Seeds.Derive(_cfg.Seed, "nemesis", epoch), Log, schedule, _faultCounts, epoch);
        state.FaultLabel = nemesis.Label;
        using var stopClients = new CancellationTokenSource();
        using var stopNemesis = new CancellationTokenSource();
        var workers = new List<ClientWorker>();
        var tasks = new List<Task>();
        for (int w = 0; w < _cfg.Clients; w++)
        {
            var cw = new ClientWorker(w, _cfg, history, state, cluster.Ids, cluster.Net, Seeds.Derive(_cfg.Seed, "client" + w, epoch));
            workers.Add(cw);
            tasks.Add(Task.Run(() => cw.RunAsync(stopClients.Token)));
        }
        Task nem = hooks?.NoNemesis == true ? Task.CompletedTask : Task.Run(() => nemesis.RunAsync(stopNemesis.Token));
        Task during = hooks?.DuringRun is { } dr ? Task.Run(() => dr(cluster, stopNemesis.Token)) : Task.CompletedTask;

        TimeSpan runFor = hooks?.RunTime ?? TimeSpan.FromSeconds(_cfg.VerifyEverySeconds);
        if (hooks is null && _clock.Elapsed + runFor > _duration) runFor = _duration - _clock.Elapsed;
        if (runFor < TimeSpan.FromSeconds(5)) runFor = TimeSpan.FromSeconds(5);
        var sw = Stopwatch.StartNew();
        string? early = null;
        long lastStatus = 0;
        while (sw.Elapsed < runFor && !_stopRequested)
        {
            await Task.Delay(200).ConfigureAwait(false);
            early = NodeFailure(cluster) ?? nemesis.GracefulHang;
            if (early is null && !state.Violations.IsEmpty && state.Violations.TryPeek(out string? v)) early = "online check: " + v;
            if (early != null) break;
            if (sw.ElapsedMilliseconds - lastStatus > 10_000)
            {
                lastStatus = sw.ElapsedMilliseconds;
                UpdateLiveStats(cluster, state);
                SaveStatus("run");
            }
        }
        await stopNemesis.CancelAsync().ConfigureAwait(false);
        await nem.ConfigureAwait(false);
        await during.ConfigureAwait(false);
        await stopClients.CancelAsync().ConfigureAwait(false);
        await Task.WhenAll(tasks).ConfigureAwait(false);
        foreach (ClientWorker cw in workers) cw.Dispose();
        history.Flush();
        if (early != null)
        {
            Log("FAILURE during RUN: " + early);
            return await FailAsync(cluster, epoch, early, null).ConfigureAwait(false);
        }

        // ---------------- VERIFY ----------------
        Log("==== epoch " + epoch + ": VERIFY");
        SaveStatus("verify");
        var rep = new VerifyReport { Epoch = epoch };
        string? convErr = null;
        rep.Run("heal-and-converge", s =>
        {
            var t = Stopwatch.StartNew();
            Task heal = nemesis.HealAllAsync();
            if (!heal.Wait(TimeSpan.FromSeconds(180))) s.Fail("healing (undo of every fault, restarts) did not finish in 180 s");
            if (hooks?.BeforeConverge is { } bc) bc(cluster).GetAwaiter().GetResult();
            convErr = cluster.WaitConvergedAsync(TimeSpan.FromSeconds(_cfg.ConvergeTimeoutSeconds)).GetAwaiter().GetResult();
            if (NodeFailure(cluster) is { } nf) s.Fail(nf);
            if (convErr != null) s.Fail(convErr);
            s.Detail = "healed, restarted and converged in " + t.Elapsed.TotalSeconds.ToString("F1", CultureInfo.InvariantCulture) + " s";
        });
        if (hooks?.AfterConverge is { } ac) await ac(cluster).ConfigureAwait(false);
        Dictionary<string, DumpDto> dumps = await cluster.DumpAllAsync().ConfigureAwait(false);
        hooks?.OnDumps?.Invoke(dumps);
        List<Op> ops = hooks?.OnHistory is { } oh ? oh(history.Ops) : history.Ops;
        Model checkModel = hooks?.OnModel is { } om ? om(model) : model;
        Checker.CheckState(rep, _cfg, checkModel, ops, dumps, state.Violations, hooks?.EventsDir ?? _p.Events);

        // native cross-check of a checkpoint of one node
        model.Verifies++;
        if (_cfg.NativeEvery > 0 && model.Verifies % _cfg.NativeEvery == 0 && rep.Find("convergence")?.Pass == true)
        {
            string node = cluster.Ids[Seeds.Derive(_cfg.Seed, "native", epoch) % cluster.Ids.Count];
            rep.Run("native-cross-check", s => NativeCheck(cluster, node, dumps[node], epoch, s));
            _status.NativeChecks++;
        }

        // availability statistics
        rep.Run("availability", s =>
        {
            UpdateLiveStats(cluster, state);
            s.Detail = "leaderless " + cluster.LeaderlessSeconds.ToString("F1", CultureInfo.InvariantCulture) + " of " +
                       cluster.ObservedSeconds.ToString("F1", CultureInfo.InvariantCulture) + " s observed; " + rep.Stats.GetValueOrDefault("terms-with-leader") + " terms with a leader";
        });
        foreach (KeyValuePair<string, long> kv in state.Stats)
            if (!kv.Key.StartsWith("phase:", StringComparison.Ordinal)) rep.Stats[kv.Key] = kv.Value;

        _status.Verifies++;
        Merge(_status.Ops, rep.Ops);
        foreach (KeyValuePair<string, long> kv in state.Stats)
        {
            if (kv.Key.StartsWith("phase:", StringComparison.Ordinal)) _status.OpsByFaultPhase[kv.Key[6..]] = _status.OpsByFaultPhase.GetValueOrDefault(kv.Key[6..]) + kv.Value;
            else _status.Stats[kv.Key] = _status.Stats.GetValueOrDefault(kv.Key) + kv.Value;
        }
        _status.TermsWithLeader = rep.Stats.GetValueOrDefault("terms-with-leader");
        foreach (StepResult st in rep.Steps)
        {
            if (!_status.StepTotals.TryGetValue(st.Name, out StepTotal? t)) _status.StepTotals[st.Name] = t = new StepTotal();
            t.Runs++;
            if (!st.Pass) t.Failures++;
            t.TotalMs += st.Ms;
            t.MaxMs = Math.Max(t.MaxMs, st.Ms);
        }
        _status.LastVerify = rep;
        LastReport = rep;
        Json.WriteAtomic(_p.VerifyFile(epoch), rep);
        foreach (StepResult st in rep.Steps)
        {
            Log("  " + (st.Pass ? "PASS " : "FAIL ") + st.Name.PadRight(20) + " " + st.Ms.ToString("F0", CultureInfo.InvariantCulture).PadLeft(7) + " ms  " + st.Detail);
            foreach (string pr in st.Problems) Log("       " + pr);
        }
        if (hooks?.AfterVerify is { } av) await av(cluster).ConfigureAwait(false);
        if (!rep.Pass && hooks is null)
            return await FailAsync(cluster, epoch, "VERIFY failed: " + string.Join(", ", rep.Steps.FindAll(x => !x.Pass).ConvertAll(x => x.Name)), rep).ConfigureAwait(false);

        if (dumps.TryGetValue(cluster.Ids[0], out DumpDto? d0) && d0.Available)
        {
            Checker.Fold(model, _cfg, d0, state);
            model.Epoch = epoch;
            Json.WriteAtomic(_p.ModelJson, model);
        }
        // keep the last --keep-history histories
        int drop = epoch - _cfg.KeepHistory;
        if (drop > 0 && File.Exists(_p.HistoryFile(drop))) File.Delete(_p.HistoryFile(drop));
        SaveStatus("verified");
        return 0;
    }

    private void UpdateLiveStats(ClusterControl cluster, WorkloadState state)
    {
        _status.ObservedSeconds = cluster.ObservedSeconds;
        _status.LeaderlessSeconds = cluster.LeaderlessSeconds;
        _status.LeaderlessIntervals = cluster.LeaderlessIntervals;
        _status.LongestLeaderlessSeconds = cluster.LongestLeaderlessSeconds;
        lock (cluster.LeaderlessDurations) _status.LeaderlessIntervalSeconds = Percentiles(cluster.LeaderlessDurations);
        lock (state.WriteLatencyMs)
        {
            lock (_allLatencies)
            {
                // Sample the epoch's latencies into the run-wide reservoir (bounded).
                foreach (double x in state.WriteLatencyMs)
                    if (_allLatencies.Count < 2_000_000) _allLatencies.Add(x);
                state.WriteLatencyMs.Clear();
                _status.WriteLatencyMs = Percentiles(_allLatencies);
            }
        }
    }

    private static string? NodeFailure(ClusterControl cluster)
    {
        foreach (NodeHandle h in cluster.Nodes.Values)
            if (h.UnexpectedExit is { } u) return u;
        return null;
    }

    private void NativeCheck(ClusterControl cluster, string node, DumpDto dump, int epoch, StepResult s)
    {
        string dir = Path.Combine(_p.Scratch, "native-epoch-" + epoch.ToString(CultureInfo.InvariantCulture));
        if (Directory.Exists(dir)) Directory.Delete(dir, true);
        try
        {
            string e = cluster.Nodes[node].Control!.WithDeadline(DateTime.UtcNow.AddSeconds(60)).CheckpointAsync(dir).GetAwaiter().GetResult();
            if (e.Length > 0)
            {
                s.Fail("checkpoint of " + node + " failed: " + e);
                return;
            }
            string outFile = dir + ".json";
            var psi = new ProcessStartInfo(Environment.ProcessPath!) { UseShellExecute = false, RedirectStandardOutput = true, RedirectStandardError = true };
            if (Path.GetFileNameWithoutExtension(Environment.ProcessPath) == "dotnet") psi.ArgumentList.Add(typeof(Supervisor).Assembly.Location);
            psi.ArgumentList.Add("native-digest");
            psi.ArgumentList.Add(dir);
            psi.ArgumentList.Add(outFile);
            using Process p = Process.Start(psi)!;
            string stderr = p.StandardError.ReadToEnd();
            p.StandardOutput.ReadToEnd();
            if (!p.WaitForExit(120_000))
            {
                p.Kill();
                s.Fail("native digest timed out");
                return;
            }
            if (p.ExitCode != 0 || !File.Exists(outFile))
            {
                s.Fail("native digest exited with " + p.ExitCode + ": " + stderr);
                return;
            }
            Dictionary<string, string> r = JsonSerializer.Deserialize<Dictionary<string, string>>(File.ReadAllText(outFile))!;
            File.Delete(outFile);
            if (r.TryGetValue("error", out string? err))
            {
                s.Fail("native rocksdb could not read the checkpoint of " + node + ": " + err);
                return;
            }
            ulong hash = ulong.Parse(r["hash"], NumberStyles.HexNumber, CultureInfo.InvariantCulture);
            long count = long.Parse(r["count"], CultureInfo.InvariantCulture);
            s.Count = count;
            if (hash != dump.DigestHash || count != dump.DigestKeys)
                s.Fail("native scan of " + node + "'s checkpoint: " + count + " keys, digest " + r["hash"] + "; managed: " + dump.DigestKeys + " keys, digest " +
                       dump.DigestHash.ToString("x16", CultureInfo.InvariantCulture));
            s.Detail = "native librocksdb read a checkpoint of " + node + ": " + count + " keys, same digest";
        }
        finally
        {
            try
            {
                if (Directory.Exists(dir)) Directory.Delete(dir, true);
            }
            catch (IOException)
            {
            }
        }
    }

    /// <summary>Stops everything, preserves the evidence in failures/&lt;n&gt;/ and returns exit code 1.</summary>
    public async Task<int> FailAsync(ClusterControl cluster, int epoch, string reason, VerifyReport? rep)
    {
        Log("FAILURE (epoch " + epoch + "): " + reason);
        _status.Failures.Add("epoch " + epoch + ": " + reason);
        _status.Result = "failed";
        SaveStatus("failed");
        // Freeze the nodes first (SIGKILL keeps exactly what is on disk), then copy.
        foreach (NodeHandle h in cluster.Nodes.Values)
        {
            h.Resume();
            await h.KillAsync().ConfigureAwait(false);
        }
        int n = 1;
        while (Directory.Exists(Path.Combine(_p.Failures, n.ToString(CultureInfo.InvariantCulture)))) n++;
        string dst = Path.Combine(_p.Failures, n.ToString(CultureInfo.InvariantCulture));
        Directory.CreateDirectory(dst);
        CopyDir(_p.Nodes, Path.Combine(dst, "nodes"));
        CopyDir(_p.Events, Path.Combine(dst, "events"));
        CopyDir(_p.Logs, Path.Combine(dst, "logs"));
        CopyDir(_p.History, Path.Combine(dst, "history"));
        CopyDir(_p.Verify, Path.Combine(dst, "verify"));
        foreach (string f in new[] { _p.RunJson, _p.ModelJson, _p.Nemesis, _p.Log, _p.StatusJson })
            if (File.Exists(f)) File.Copy(f, Path.Combine(dst, Path.GetFileName(f)), true);
        File.WriteAllText(Path.Combine(dst, "summary.txt"),
            "reason: " + reason + "\nepoch: " + epoch + "\nseed: " + _cfg.Seed + "\nnodes: " + _cfg.Nodes + "\n" +
            (rep is null ? "" : JsonSerializer.Serialize(rep, Json.Indented)) + "\n");
        Log("failure preserved in " + dst);
        return 1;
    }

    private static void CopyDir(string src, string dst)
    {
        if (!Directory.Exists(src)) return;
        Directory.CreateDirectory(dst);
        foreach (string d in Directory.GetDirectories(src, "*", SearchOption.AllDirectories))
            Directory.CreateDirectory(Path.Combine(dst, Path.GetRelativePath(src, d)));
        foreach (string f in Directory.GetFiles(src, "*", SearchOption.AllDirectories))
        {
            try
            {
                File.Copy(f, Path.Combine(dst, Path.GetRelativePath(src, f)), true);
            }
            catch (IOException)
            {
            }
        }
    }
}
