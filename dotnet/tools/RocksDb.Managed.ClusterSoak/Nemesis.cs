using System.Collections.Concurrent;
using System.Globalization;

namespace RocksDb.Managed.ClusterSoak;

/// <summary>
/// The nemesis of the RUN phase: every exp(mean) seconds it starts a seeded, weighted fault (partition shapes, one-way
/// links, latency, client cuts, SIGKILL, SIGSTOP, power loss, kill during catch-up, clock skew, rolling restart) and
/// undoes it after a hold time. Up to <see cref="RunConfig.MaxConcurrentFaults"/> faults overlap (one network fault at
/// a time; each node is under at most one process fault), so a quorum can be unavailable for a while.
/// </summary>
internal sealed class Nemesis
{
    private readonly ClusterControl _c;
    private readonly RunConfig _cfg;
    private readonly History _h;
    private readonly Random _rng;
    private readonly Action<string> _log;
    private readonly StreamWriter _schedule;
    private readonly Lock _gate = new();
    private readonly List<Active> _active = [];
    private readonly List<Task> _undos = [];
    private readonly ConcurrentDictionary<string, long> _counts;
    private readonly int _epoch;

    private sealed class Active
    {
        public FaultKind Kind;
        public string Desc = "";
        public bool Network;
        public HashSet<string> Nodes = [];
        public DateTime End;
        public Func<Task> Undo = static () => Task.CompletedTask;
        public bool Undone;
    }

    public Nemesis(ClusterControl c, RunConfig cfg, History h, int seed, Action<string> log, StreamWriter schedule, ConcurrentDictionary<string, long> counts, int epoch)
    {
        _c = c;
        _cfg = cfg;
        _h = h;
        _rng = new Random(seed);
        _log = log;
        _schedule = schedule;
        _counts = counts;
        _epoch = epoch;
    }

    /// <summary>The active faults, e.g. "Kill+PartitionHalves" or "none" (the per-fault-phase op statistics).</summary>
    public string Label()
    {
        lock (_gate)
        {
            if (_active.Count == 0) return "none";
            var k = new List<string>();
            foreach (Active a in _active)
                if (!k.Contains(a.Kind.ToString())) k.Add(a.Kind.ToString());
            k.Sort(StringComparer.Ordinal);
            return string.Join("+", k);
        }
    }

    private void Record(string what)
    {
        string line = DateTime.UtcNow.ToString("O", CultureInfo.InvariantCulture) + " epoch " + _epoch + " " + what;
        lock (_schedule)
        {
            _schedule.WriteLine(line);
            _schedule.Flush();
        }
        _h.Add(new Op { Type = OpType.Info, Process = -1, F = OpF.Nemesis, Note = what });
        _log("nemesis: " + what);
    }

    public async Task RunAsync(CancellationToken stop)
    {
        var weights = new List<(FaultKind, int)>();
        foreach (KeyValuePair<string, int> kv in _cfg.Faults)
            if (kv.Value > 0 && Enum.TryParse(kv.Key, out FaultKind k)) weights.Add((k, kv.Value));
        if (weights.Count == 0) return;
        DateTime next = DateTime.UtcNow + TimeSpan.FromSeconds(Exp(_cfg.MeanFaultIntervalSeconds));
        while (!stop.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(100, stop).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            ExpireDue();
            if (DateTime.UtcNow < next) continue;
            next = DateTime.UtcNow + TimeSpan.FromSeconds(Exp(_cfg.MeanFaultIntervalSeconds));
            int active;
            lock (_gate) active = _active.Count;
            if (active >= _cfg.MaxConcurrentFaults) continue;
            FaultKind kind = Pick(weights);
            try
            {
                await StartAsync(kind).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                _log("nemesis: " + kind + " failed to start: " + e.Message);
            }
        }
    }

    private double Exp(double mean) => -mean * Math.Log(1 - _rng.NextDouble());

    private FaultKind Pick(List<(FaultKind Kind, int W)> weights)
    {
        int total = 0;
        foreach ((FaultKind _, int w) in weights) total += w;
        int r = _rng.Next(total);
        foreach ((FaultKind k, int w) in weights)
        {
            if (r < w) return k;
            r -= w;
        }
        return weights[0].Kind;
    }

    private double Hold(double min, double max) => min + _rng.NextDouble() * (max - min);

    private void ExpireDue()
    {
        List<Active> due = [];
        lock (_gate)
        {
            foreach (Active a in _active)
                if (!a.Undone && DateTime.UtcNow >= a.End)
                {
                    a.Undone = true;
                    due.Add(a);
                }
        }
        foreach (Active a in due) StartUndo(a);
    }

    private void StartUndo(Active a)
    {
        Task t = Task.Run(async () =>
        {
            try
            {
                await a.Undo().ConfigureAwait(false);
            }
            catch (Exception e)
            {
                _log("nemesis: undo of " + a.Kind + " failed: " + e.Message);
            }
            Record("end " + a.Kind + " " + a.Desc);
            lock (_gate) _active.Remove(a);
        });
        lock (_gate) _undos.Add(t);
    }

    private bool NetworkBusy()
    {
        lock (_gate)
            foreach (Active a in _active)
                if (a.Network) return true;
        return false;
    }

    private HashSet<string> BusyNodes()
    {
        var s = new HashSet<string>(StringComparer.Ordinal);
        lock (_gate)
            foreach (Active a in _active) s.UnionWith(a.Nodes);
        return s;
    }

    private List<string> Shuffled()
    {
        var l = new List<string>(_c.Ids);
        for (int i = l.Count - 1; i > 0; i--)
        {
            int j = _rng.Next(i + 1);
            (l[i], l[j]) = (l[j], l[i]);
        }
        return l;
    }

    /// <summary>The monitor's leader if its process is still running (the monitor polls every 250 ms).</summary>
    private string? LiveLeader => _c.Leader is { } l && _c.Nodes[l].State == NodeState.Running ? l : null;

    private string? FreeNode(bool leader)
    {
        HashSet<string> busy = BusyNodes();
        if (leader && LiveLeader is { } l && !busy.Contains(l)) return l;
        foreach (string n in Shuffled())
            if (!busy.Contains(n) && _c.Nodes[n].State == NodeState.Running) return n;
        return null;
    }

    private void Add(Active a)
    {
        lock (_gate) _active.Add(a);
        _counts.AddOrUpdate(a.Kind.ToString(), 1, static (_, v) => v + 1);
        Record("start " + a.Kind + " " + a.Desc);
    }

    private Task HealNodeLinks()
    {
        foreach (LinkProxy l in _c.Net.All)
            if (!l.From.StartsWith('c')) l.Heal();
        return Task.CompletedTask;
    }

    private async Task StartAsync(FaultKind kind)
    {
        Network net = _c.Net;
        int n = _c.Ids.Count;
        switch (kind)
        {
            case FaultKind.PartitionHalves or FaultKind.IsolateLeader or FaultKind.IsolateNode or FaultKind.Bridge or FaultKind.Ring or FaultKind.OneWay or FaultKind.Slow:
            {
                if (NetworkBusy()) return;
                List<string> order = Shuffled();
                string desc;
                if (kind == FaultKind.PartitionHalves || (kind == FaultKind.Ring && n < 5) || (kind == FaultKind.Bridge && n < 3))
                {
                    int majority = n / 2 + 1;
                    List<string> a = order.GetRange(0, majority), b = order.GetRange(majority, n - majority);
                    // Sometimes the leader ends up on the minority side on purpose.
                    if (LiveLeader is { } l && _rng.Next(2) == 0 && a.Contains(l) && b.Count > 0)
                    {
                        a.Remove(l);
                        a.Add(b[0]);
                        b[0] = l;
                    }
                    net.CutBetween(a, b);
                    desc = "majority [" + string.Join(",", a) + "] | minority [" + string.Join(",", b) + "]";
                    kind = FaultKind.PartitionHalves;
                }
                else if (kind is FaultKind.IsolateLeader or FaultKind.IsolateNode)
                {
                    string victim = kind == FaultKind.IsolateLeader && LiveLeader is { } l ? l : order[0];
                    net.CutBetween([victim], _c.Ids);
                    desc = victim + (kind == FaultKind.IsolateLeader ? " (leader)" : "") + " isolated; clients still reach it";
                }
                else if (kind == FaultKind.Bridge)
                {
                    string bridge = order[0];
                    int half = (n - 1) / 2;
                    List<string> a = order.GetRange(1, half), b = order.GetRange(1 + half, n - 1 - half);
                    net.CutBetween(a, b);
                    desc = "bridge " + bridge + " between [" + string.Join(",", a) + "] and [" + string.Join(",", b) + "]";
                }
                else if (kind == FaultKind.Ring)
                {
                    // Majorities ring: each node sees its two ring neighbours only (a majority of 5), no two the same.
                    for (int i = 0; i < n; i++)
                        for (int j = 0; j < n; j++)
                        {
                            int d = Math.Abs(i - j);
                            d = Math.Min(d, n - d);
                            if (d >= 2) net[order[i], order[j]].Cut();
                        }
                    desc = "majorities ring " + string.Join("-", order);
                }
                else if (kind == FaultKind.OneWay)
                {
                    string a = LiveLeader is { } l && _rng.Next(2) == 0 ? l : order[0];
                    int mode = _rng.Next(3);
                    if (mode == 0)
                    {
                        string b = order.Find(x => x != a)!;
                        net[a, b].Cut();
                        desc = a + " -> " + b + " cut (RPCs from " + a + " to " + b + " fail; " + b + " -> " + a + " works)";
                    }
                    else if (mode == 1)
                    {
                        foreach (string b in _c.Ids)
                            if (b != a) net[a, b].Cut();
                        desc = "every outbound link of " + a + " cut (it receives RPCs but cannot send any)";
                    }
                    else
                    {
                        foreach (string b in _c.Ids)
                            if (b != a) net[b, a].Cut();
                        desc = "every inbound link of " + a + " cut (it sends RPCs but receives none)";
                    }
                }
                else
                {
                    var slow = new List<string>();
                    foreach (LinkProxy l in net.All)
                    {
                        if (l.From.StartsWith('c') || _rng.Next(2) == 0) continue;
                        l.DelayMs = 20 + _rng.Next(300);
                        slow.Add(l.From + ">" + l.To + "=" + l.DelayMs + "ms");
                    }
                    desc = "latency on " + string.Join(" ", slow);
                }
                Add(new Active { Kind = kind, Desc = desc, Network = true, End = DateTime.UtcNow + TimeSpan.FromSeconds(Hold(2, 15)), Undo = HealNodeLinks });
                return;
            }
            case FaultKind.ClientCut:
            {
                if (NetworkBusy()) return;
                string target = _c.Leader ?? Shuffled()[0];
                var cut = new List<LinkProxy>();
                foreach (string cl in net.Clients)
                    if (_rng.Next(2) == 0 || cut.Count == 0)
                    {
                        cut.Add(net[cl, target]);
                        net[cl, target].Cut();
                    }
                var names = new List<string>();
                foreach (LinkProxy l in cut) names.Add(l.From);
                Add(new Active
                {
                    Kind = kind,
                    Desc = "clients " + string.Join(",", names) + " cut from " + target,
                    Network = true,
                    End = DateTime.UtcNow + TimeSpan.FromSeconds(Hold(2, 8)),
                    Undo = () =>
                    {
                        foreach (LinkProxy l in cut) l.Heal();
                        return Task.CompletedTask;
                    },
                });
                return;
            }
            case FaultKind.Kill or FaultKind.KillLeader:
            {
                string? v = FreeNode(kind == FaultKind.KillLeader);
                if (v is null) return;
                var a = new Active { Kind = kind, Desc = "SIGKILL " + v, Nodes = [v], End = DateTime.UtcNow + TimeSpan.FromSeconds(Hold(1, 12)) };
                a.Undo = () => RestartAsync(v);
                Add(a);
                await _c.Nodes[v].KillAsync().ConfigureAwait(false);
                return;
            }
            case FaultKind.Pause or FaultKind.PauseLeader:
            {
                string? v = FreeNode(kind == FaultKind.PauseLeader);
                if (v is null || !_c.Nodes[v].Pause()) return;
                double hold = Hold(0.5, 6);
                Add(new Active
                {
                    Kind = kind,
                    Desc = "SIGSTOP " + v + " for " + hold.ToString("F1", CultureInfo.InvariantCulture) + " s",
                    Nodes = [v],
                    End = DateTime.UtcNow + TimeSpan.FromSeconds(hold),
                    Undo = () =>
                    {
                        _c.Nodes[v].Resume();
                        return Task.CompletedTask;
                    },
                });
                return;
            }
            case FaultKind.PowerLoss or FaultKind.PowerLossLeader:
            {
                string? v = FreeNode(kind == FaultKind.PowerLossLeader);
                if (v is null) return;
                var a = new Active { Kind = kind, Desc = "power loss on " + v, Nodes = [v], End = DateTime.UtcNow + TimeSpan.FromSeconds(Hold(1, 10)) };
                a.Undo = () => RestartAsync(v);
                Add(a);
                await _c.Nodes[v].PowerLossAsync().ConfigureAwait(false);
                return;
            }
            case FaultKind.KillDuringCatchup:
            {
                string? v = FreeNode(false);
                if (v is null) return;
                double lag = Hold(3, 12);
                var a = new Active { Kind = kind, Desc = v + ": SIGKILL, lag " + lag.ToString("F1", CultureInfo.InvariantCulture) + " s, restart, SIGKILL again during catch-up", Nodes = [v] };
                a.End = DateTime.MaxValue;
                Add(a);
                Task seq = CatchupSequenceAsync(v, lag);
                a.Undo = async () =>
                {
                    await seq.ConfigureAwait(false);
                    await RestartAsync(v).ConfigureAwait(false);
                };
                _ = seq.ContinueWith(_ =>
                {
                    lock (_gate)
                        if (!a.Undone)
                        {
                            a.Undone = true;
                            StartUndo(a);
                        }
                }, TaskScheduler.Default);
                return;
            }
            case FaultKind.ClockSkew:
            {
                string? v = FreeNode(_rng.Next(2) == 0);
                if (v is null) return;
                double[] rates = [0.25, 0.5, 2, 4];
                double rate = rates[_rng.Next(rates.Length)];
                if (!_c.Nodes[v].Send("clock " + rate.ToString(CultureInfo.InvariantCulture))) return;
                Add(new Active
                {
                    Kind = kind,
                    Desc = v + " clock runs at " + rate.ToString(CultureInfo.InvariantCulture) + "x (election timer and heartbeats)",
                    Nodes = [v],
                    End = DateTime.UtcNow + TimeSpan.FromSeconds(Hold(3, 15)),
                    Undo = () =>
                    {
                        _c.Nodes[v].Send("clock 1");
                        return Task.CompletedTask;
                    },
                });
                return;
            }
            case FaultKind.RollingRestart:
            {
                if (BusyNodes().Count > 0) return;
                var a = new Active { Kind = kind, Desc = "graceful restart of every node, one at a time", Nodes = [.. _c.Ids], End = DateTime.MaxValue };
                Add(a);
                Task seq = RollingAsync();
                a.Undo = () => seq;
                _ = seq.ContinueWith(_ =>
                {
                    lock (_gate)
                        if (!a.Undone)
                        {
                            a.Undone = true;
                            StartUndo(a);
                        }
                }, TaskScheduler.Default);
                return;
            }
        }
    }

    private async Task RestartAsync(string id)
    {
        NodeHandle h = _c.Nodes[id];
        if (h.State != NodeState.Down) return;
        if (!await _c.StartNodeAsync(id, TimeSpan.FromSeconds(90)).ConfigureAwait(false)) _log("nemesis: restart of " + id + " did not complete");
    }

    private async Task CatchupSequenceAsync(string v, double lag)
    {
        NodeHandle h = _c.Nodes[v];
        await h.KillAsync().ConfigureAwait(false);
        await Task.Delay(TimeSpan.FromSeconds(lag)).ConfigureAwait(false);
        var seen = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        void OnLine(string node, string line)
        {
            if (node != v) return;
            if (line.Contains("snapshot from", StringComparison.Ordinal) || line.Contains("resync", StringComparison.Ordinal) ||
                line.Contains("following ", StringComparison.Ordinal) || line.Contains("snapshot", StringComparison.Ordinal))
                seen.TrySetResult(line);
        }
        _c.NodeLine += OnLine;
        try
        {
            Task<bool> start = h.StartAsync();
            Task done = await Task.WhenAny(seen.Task, Task.Delay(15000)).ConfigureAwait(false);
            string what = done == seen.Task ? await seen.Task.ConfigureAwait(false) : "(no catch-up line within 15 s)";
            await Task.Delay(_rng.Next(600)).ConfigureAwait(false);
            string kind = what.Contains("snapshot", StringComparison.Ordinal) || what.Contains("resync", StringComparison.Ordinal) ? "kill-during-snapshot" : "kill-during-stream-catchup";
            _counts.AddOrUpdate(kind, 1, static (_, x) => x + 1);
            Record(v + ": SIGKILL during catch-up after \"" + (what.Length > 120 ? what[..120] : what) + "\"");
            await h.KillAsync().ConfigureAwait(false);
            await start.ConfigureAwait(false);
            await Task.Delay(TimeSpan.FromSeconds(Hold(0.5, 4))).ConfigureAwait(false);
        }
        finally
        {
            _c.NodeLine -= OnLine;
        }
    }

    private async Task RollingAsync()
    {
        foreach (string id in Shuffled())
        {
            NodeHandle h = _c.Nodes[id];
            Record("rolling restart: stopping " + id);
            if (!await h.StopAsync(TimeSpan.FromSeconds(30)).ConfigureAwait(false))
            {
                _counts.AddOrUpdate("graceful-stop-hung", 1, static (_, x) => x + 1);
                _log("FAILURE: graceful stop of " + id + " hung for 30 s");
                GracefulHang = "graceful stop of " + id + " did not finish within 30 s";
            }
            await RestartAsync(id).ConfigureAwait(false);
            await Task.Delay(TimeSpan.FromSeconds(Hold(1, 4))).ConfigureAwait(false);
        }
    }

    /// <summary>Set when a graceful stop hung (a failure).</summary>
    public volatile string? GracefulHang;

    /// <summary>Ends every active fault: undoes them, heals every link, resumes paused nodes, resets clocks, restarts down nodes.</summary>
    public async Task HealAllAsync()
    {
        List<Active> rest = [];
        lock (_gate)
        {
            foreach (Active a in _active)
                if (!a.Undone)
                {
                    a.Undone = true;
                    rest.Add(a);
                }
        }
        foreach (Active a in rest) StartUndo(a);
        Task[] undos;
        lock (_gate) undos = [.. _undos];
        await Task.WhenAll(undos).ConfigureAwait(false);
        _c.Net.HealAll();
        foreach (NodeHandle h in _c.Nodes.Values)
        {
            h.Resume();
            if (h.State == NodeState.Running) h.Send("clock 1");
        }
        foreach (string id in _c.Ids)
            if (_c.Nodes[id].State == NodeState.Down) await RestartAsync(id).ConfigureAwait(false);
    }
}
