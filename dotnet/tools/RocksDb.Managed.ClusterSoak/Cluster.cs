using System.Diagnostics;
using System.Globalization;
using Grpc.Core;
using RocksDb.Managed.Cluster;

namespace RocksDb.Managed.ClusterSoak;

/// <summary>
/// The supervisor's view of the cluster: node processes, the proxy network, a status monitor (leader, availability),
/// and the heal / converge / dump steps of VERIFY.
/// </summary>
internal sealed class ClusterControl : IAsyncDisposable
{
    private readonly RunConfig _cfg;
    private readonly RunPaths _paths;
    private readonly Action<string> _log;
    private readonly CancellationTokenSource _monitorCts = new();
    private Task _monitor = Task.CompletedTask;
    private readonly Lock _statusGate = new();
    private readonly Dictionary<string, NodeStatusDto?> _status = new(StringComparer.Ordinal);

    public ClusterControl(RunConfig cfg, RunPaths paths, Action<string> log, Func<string, NodeConfig, NodeConfig>? tweak)
    {
        _cfg = cfg;
        _paths = paths;
        _log = log;
        var ids = new List<string>();
        for (int i = 1; i <= cfg.Nodes; i++) ids.Add("n" + i.ToString(CultureInfo.InvariantCulture));
        Ids = ids;
        var clients = new List<string>();
        for (int c = 0; c < cfg.Clients; c++) clients.Add(ClientWorker.Name(c));
        Nodes = new Dictionary<string, NodeHandle>(StringComparer.Ordinal);
        foreach (string id in ids)
        {
            string nid = id;
            Nodes[id] = new NodeHandle(id, paths, () => tweak is null ? BuildConfig(nid) : tweak(nid, BuildConfig(nid)), log);
            Nodes[id].Line += OnNodeLine;
        }
        Net = new Network(ids, clients, id => Nodes[id].Port);
    }

    public IReadOnlyList<string> Ids { get; }
    public Dictionary<string, NodeHandle> Nodes { get; }
    public Network Net { get; }

    /// <summary>Per-node unsafe flags for the self-test (applied at the next start).</summary>
    public readonly Dictionary<string, bool> SkipRaftFsync = new(StringComparer.Ordinal);

    // availability statistics
    public double LeaderlessSeconds;
    public double ObservedSeconds;
    public int LeaderlessIntervals;
    public double LongestLeaderlessSeconds;
    public readonly List<double> LeaderlessDurations = [];
    private double _currentLeaderless;

    /// <summary>Raised for node output lines (catch-up detection of the nemesis).</summary>
    public event Action<string, string>? NodeLine;

    private void OnNodeLine(NodeHandle h, string line)
    {
        if (line.Contains("address already in use", StringComparison.OrdinalIgnoreCase)) h.PortConflict = true;
        NodeLine?.Invoke(h.Id, line);
    }

    private NodeConfig BuildConfig(string id)
    {
        var peers = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (string o in Ids)
            if (o != id) peers[o] = Net[id, o].Port;
        return new NodeConfig
        {
            NodeId = id,
            Root = _paths.NodeRoot(id),
            PeerPorts = peers,
            Members = [.. Ids],
            BootstrapId = Ids[0],
            EventsPath = _paths.EventFile(id),
            FaultFs = true,
            ElectionMinMs = _cfg.ElectionMinMs,
            ElectionMaxMs = _cfg.ElectionMaxMs,
            HeartbeatMs = _cfg.HeartbeatMs,
            RpcTimeoutMs = Math.Max(200, _cfg.HeartbeatMs * 3),
            WriteBufferSize = _cfg.WriteBufferSize,
            WalTtlSeconds = _cfg.WalTtlSeconds,
            UnsafeSkipRaftFsync = SkipRaftFsync.GetValueOrDefault(id),
        };
    }

    /// <summary>Starts a node and waits until it is ready (retrying on a port conflict). False if it exited.</summary>
    public async Task<bool> StartNodeAsync(string id, TimeSpan timeout)
    {
        NodeHandle h = Nodes[id];
        for (int attempt = 0; attempt < 5; attempt++)
        {
            h.PortConflict = false;
            Task<bool> ready = h.StartAsync();
            Task done = await Task.WhenAny(ready, Task.Delay(timeout)).ConfigureAwait(false);
            if (done == ready && await ready.ConfigureAwait(false)) return true;
            if (done != ready)
            {
                _log("node " + id + " not ready after " + timeout.TotalSeconds + " s (still starting)");
                return false;
            }
            if (!h.PortConflict) return false;
            _log("node " + id + ": port " + h.Port + " in use; picking another");
            h.UnexpectedExit = null;
            h.NewPort();
            Net.SetTargetPort(id, h.Port);
        }
        return false;
    }

    public void StartMonitor()
    {
        _monitor = Task.Run(() => MonitorLoopAsync(_monitorCts.Token));
    }

    public string? Leader { get; private set; }
    public long LeaderTerm { get; private set; }

    public NodeStatusDto? LastStatus(string id)
    {
        lock (_statusGate) return _status.GetValueOrDefault(id);
    }

    private async Task MonitorLoopAsync(CancellationToken ct)
    {
        var sw = Stopwatch.StartNew();
        double last = 0;
        while (!ct.IsCancellationRequested)
        {
            var tasks = new Dictionary<string, Task<NodeStatusDto?>>(StringComparer.Ordinal);
            foreach (string id in Ids) tasks[id] = Nodes[id].StatusAsync(TimeSpan.FromMilliseconds(400));
            string? leader = null;
            long best = -1;
            var polled = new List<NodeStatusDto>();
            foreach (KeyValuePair<string, Task<NodeStatusDto?>> kv in tasks)
            {
                NodeStatusDto? s = await kv.Value.ConfigureAwait(false);
                lock (_statusGate) _status[kv.Key] = s;
                if (s is null) continue;
                polled.Add(s);
                if (s.Role == (int)RaftRole.Leader && !s.Resyncing && s.Term > best)
                {
                    best = s.Term;
                    leader = kv.Key;
                }
            }
            Leader = leader;
            LeaderTerm = best;
            double now = sw.Elapsed.TotalSeconds, dt = now - last;
            last = now;
            // Available = a leader that a quorum (itself included) currently follows in its term.
            int followers = 0;
            foreach (NodeStatusDto s in polled)
                if (leader != null && s.Term == best && (s.NodeId == leader || s.LeaderId == leader)) followers++;
            bool hasLeader = leader != null && followers >= Ids.Count / 2 + 1;
            ObservedSeconds += dt;
            if (!hasLeader)
            {
                LeaderlessSeconds += dt;
                if (_currentLeaderless == 0) LeaderlessIntervals++;
                _currentLeaderless += dt;
            }
            else if (_currentLeaderless > 0)
            {
                LongestLeaderlessSeconds = Math.Max(LongestLeaderlessSeconds, _currentLeaderless);
                lock (LeaderlessDurations) LeaderlessDurations.Add(_currentLeaderless);
                _currentLeaderless = 0;
            }
            try
            {
                await Task.Delay(250, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    /// <summary>
    /// Waits until every node runs, exactly one leader is followed by all in the same term, and every node holds the
    /// leader's latest sequence as its commit index, stable over two polls. Returns null or a description of what did
    /// not converge.
    /// </summary>
    public async Task<string?> WaitConvergedAsync(TimeSpan timeout)
    {
        var sw = Stopwatch.StartNew();
        string why = "";
        ulong stableSeq = ulong.MaxValue;
        int stable = 0;
        while (sw.Elapsed < timeout)
        {
            var st = new Dictionary<string, NodeStatusDto?>(StringComparer.Ordinal);
            foreach (string id in Ids) st[id] = await Nodes[id].StatusAsync(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
            why = Describe(st, out ulong seq);
            if (why.Length == 0)
            {
                if (seq == stableSeq && ++stable >= 2) return null;
                if (seq != stableSeq)
                {
                    stableSeq = seq;
                    stable = 0;
                }
            }
            else stable = 0;
            await Task.Delay(300).ConfigureAwait(false);
        }
        return "not converged after " + timeout.TotalSeconds + " s: " + why;
    }

    private string Describe(Dictionary<string, NodeStatusDto?> st, out ulong seq)
    {
        seq = 0;
        NodeStatusDto? leader = null;
        foreach (NodeStatusDto? s in st.Values)
        {
            if (s is { Role: (int)RaftRole.Leader })
            {
                if (leader != null && s.Term == leader.Term) return "two leaders in term " + s.Term;
                if (leader == null || s.Term > leader.Term) leader = s;
            }
        }
        var parts = new List<string>();
        foreach (KeyValuePair<string, NodeStatusDto?> kv in st)
        {
            NodeStatusDto? s = kv.Value;
            if (s is null)
            {
                parts.Add(kv.Key + " unreachable (" + Nodes[kv.Key].State + ")");
                continue;
            }
            parts.Add(kv.Key + " " + (RaftRole)s.Role + " t" + s.Term + " seq " + s.LatestSeq + " commit " + s.CommitSeq + (s.Resyncing ? " resyncing" : "") +
                      " leader " + (s.LeaderId ?? "-"));
        }
        string all = string.Join("; ", parts);
        if (leader is null) return "no leader: " + all;
        foreach (NodeStatusDto? s in st.Values)
        {
            if (s is null || s.Resyncing || s.Term != leader.Term || (s.NodeId != leader.NodeId && s.LeaderId != leader.NodeId) ||
                s.LatestSeq != leader.LatestSeq || s.CommitSeq != leader.LatestSeq || s.Mode != (int)ClusterMode.Cluster)
                return all;
        }
        seq = leader.LatestSeq;
        return "";
    }

    public async Task<Dictionary<string, DumpDto>> DumpAllAsync()
    {
        var d = new Dictionary<string, DumpDto>(StringComparer.Ordinal);
        foreach (string id in Ids)
        {
            try
            {
                d[id] = await Nodes[id].Control!.WithDeadline(DateTime.UtcNow.AddSeconds(30)).DumpAsync();
            }
            catch (RpcException e)
            {
                d[id] = new DumpDto { Available = false, Terms = "", Keys = [], Values = [] };
                _log("dump of " + id + " failed: " + e.Status.Detail);
            }
        }
        return d;
    }

    /// <summary>Writes through whichever node leads, retrying until committed (idempotent puts only).</summary>
    public async Task<bool> WriteIdempotentAsync(WriteReq req, TimeSpan timeout)
    {
        var sw = Stopwatch.StartNew();
        while (sw.Elapsed < timeout)
        {
            foreach (string id in Ids)
            {
                try
                {
                    WriteResp r = await Nodes[id].Control!.WithDeadline(DateTime.UtcNow.AddSeconds(10)).WriteAsync(req);
                    if (r.Status == WriteOutcome.Committed) return true;
                }
                catch (RpcException)
                {
                }
            }
            await Task.Delay(200).ConfigureAwait(false);
        }
        return false;
    }

    public async Task StopMonitorAsync()
    {
        await _monitorCts.CancelAsync().ConfigureAwait(false);
        await _monitor.ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        await StopMonitorAsync().ConfigureAwait(false);
        foreach (NodeHandle h in Nodes.Values) await h.DisposeAsync().ConfigureAwait(false);
        await Net.DisposeAsync().ConfigureAwait(false);
    }
}
