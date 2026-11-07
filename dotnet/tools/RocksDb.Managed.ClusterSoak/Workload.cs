using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using Grpc.Core;
using Grpc.Net.Client;
using MagicOnion.Client;

namespace RocksDb.Managed.ClusterSoak;

/// <summary>State shared by the client workers of one epoch and read by the checker.</summary>
internal sealed class WorkloadState
{
    public WorkloadState(RunConfig cfg, Model model)
    {
        int n = cfg.RegisterKeys;
        AckedMax = new long[n];
        MaxInvoked = new long[n];
        NextVersion = new long[n];
        for (int i = 0; i < n; i++)
        {
            AckedMax[i] = model.RegVersion[i];
            MaxInvoked[i] = model.RegVersion[i];
            NextVersion[i] = model.NextVersion[i];
        }
        BankTotal = (ulong)cfg.Accounts * 1000;
    }

    /// <summary>Highest acknowledged version per register (the converged model counts as acknowledged).</summary>
    public readonly long[] AckedMax;
    /// <summary>Highest invoked version per register (a read of a higher one is a phantom).</summary>
    public readonly long[] MaxInvoked;
    /// <summary>Next version per register (each register has exactly one writer).</summary>
    public readonly long[] NextVersion;
    public readonly ulong BankTotal;
    public readonly ConcurrentDictionary<string, long> Stats = new(StringComparer.Ordinal);
    public readonly ConcurrentQueue<string> Violations = new();
    public readonly List<double> WriteLatencyMs = [];
    public Func<string> FaultLabel = static () => "none";

    public void Count(string key, long n = 1) => Stats.AddOrUpdate(key, n, (_, v) => v + n);

    public void Violation(string what)
    {
        if (Violations.Count < 1000) Violations.Enqueue(what);
    }

    public static void Max(long[] a, int i, long v)
    {
        long cur = Volatile.Read(ref a[i]);
        while (v > cur)
        {
            long prev = Interlocked.CompareExchange(ref a[i], v, cur);
            if (prev == cur) return;
            cur = prev;
        }
    }
}

/// <summary>
/// One client worker (a Jepsen process: sequential ops; after an :info op it continues as a new process id). It
/// sends writes to the node it believes leads (following NotLeader hints) and reads from leaders and followers, all
/// through its own proxies (links "cK -> node").
/// </summary>
internal sealed class ClientWorker : IDisposable
{
    private readonly int _w;
    private readonly RunConfig _cfg;
    private readonly History _h;
    private readonly WorkloadState _s;
    private readonly IReadOnlyList<string> _nodes;
    private readonly Dictionary<string, ISoakService> _svc = new(StringComparer.Ordinal);
    private readonly List<GrpcChannel> _channels = [];
    private readonly Dictionary<string, long[]> _lastSeen = new(StringComparer.Ordinal);
    private readonly Random _rng;
    private int _process;
    private string _leader;

    public ClientWorker(int w, RunConfig cfg, History h, WorkloadState s, IReadOnlyList<string> nodes, Network net, int seed)
    {
        _w = w;
        _cfg = cfg;
        _h = h;
        _s = s;
        _nodes = nodes;
        _rng = new Random(seed);
        _process = w;
        _leader = nodes[0];
        foreach (string n in nodes)
        {
            GrpcChannel ch = GrpcChannel.ForAddress("http://127.0.0.1:" + net[Name(w), n].Port.ToString(CultureInfo.InvariantCulture), new GrpcChannelOptions
            {
                InitialReconnectBackoff = TimeSpan.FromMilliseconds(100),
                MaxReconnectBackoff = TimeSpan.FromMilliseconds(500),
            });
            _channels.Add(ch);
            _svc[n] = MagicOnionClient.Create<ISoakService>(ch);
            _lastSeen[n] = new long[cfg.RegisterKeys];
            Array.Fill(_lastSeen[n], -2);
        }
    }

    public static string Name(int w) => "c" + w.ToString(CultureInfo.InvariantCulture);

    public async Task RunAsync(CancellationToken stop)
    {
        while (!stop.IsCancellationRequested)
        {
            int r = _rng.Next(100);
            try
            {
                if (r < 50) await WriteAsync().ConfigureAwait(false);
                else if (r < 68) await ReadAsync().ConfigureAwait(false);
                else if (r < 80) await TransferAsync().ConfigureAwait(false);
                else if (r < 92) await AddAsync().ConfigureAwait(false);
                else await BankReadAsync().ConfigureAwait(false);
            }
            catch (Exception e)
            {
                _s.Violation("client " + _w + " internal error: " + e);
            }
        }
    }

    private Op Invoke(OpF f, string node, int[] keys, long[] vals) =>
        _h.Add(new Op { Type = OpType.Invoke, Process = _process, F = f, Node = node, Keys = keys, Vals = vals });

    private void Complete(Op inv, OpType type, long[] vals, string note, string label)
    {
        _h.Add(new Op { Type = type, Process = inv.Process, F = inv.F, Node = inv.Node, Keys = inv.Keys, Vals = vals, Note = note });
        string t = type.ToString().ToLowerInvariant();
        _s.Count(inv.F.ToString().ToLowerInvariant() + "." + t);
        _s.Count("phase:" + label + ":" + inv.F.ToString().ToLowerInvariant() + "." + t);
        // Jepsen: a process whose op is indeterminate may still be running it; continue as a fresh process.
        if (type == OpType.Info) _process += _cfg.Clients;
    }

    private string RandomNode() => _nodes[_rng.Next(_nodes.Count)];

    private async Task<(OpType Type, string Note)> SendWriteAsync(string node, WriteReq req)
    {
        try
        {
            WriteResp resp = await _svc[node].WithDeadline(DateTime.UtcNow.AddMilliseconds(_cfg.WriteTimeoutMs + 2000)).WriteAsync(req);
            if (resp.LeaderId is { Length: > 0 } hint && hint != node) _leader = hint;
            else if (resp.Status == WriteOutcome.NotLeader)
            {
                _leader = RandomNode();
                await Task.Delay(10 + _rng.Next(40)).ConfigureAwait(false);
            }
            return resp.Status switch
            {
                WriteOutcome.Committed => (OpType.Ok, "term " + resp.Term),
                WriteOutcome.NotLeader => (OpType.Fail, "not-leader"),
                _ => (OpType.Info, resp.Status.ToString() + (resp.Error is null ? "" : ": " + resp.Error)),
            };
        }
        catch (RpcException e)
        {
            _leader = RandomNode();
            // The node is down or the link is cut: back off a little instead of spinning on connection errors.
            await Task.Delay(20 + _rng.Next(80)).ConfigureAwait(false);
            return (OpType.Info, "rpc " + e.StatusCode);
        }
    }

    private async Task WriteAsync()
    {
        int key = _w * _cfg.KeysPerClient + _rng.Next(_cfg.KeysPerClient);
        long version = _s.NextVersion[key]++;
        WorkloadState.Max(_s.MaxInvoked, key, version);
        string node = _leader;
        string label = _s.FaultLabel();
        Op inv = Invoke(OpF.Write, node, [key], [version]);
        var req = new WriteReq { Kind = WriteKind.Put, Keys = [Keys.Reg(key)], Values = [Keys.RegValue(key, (int)version)], TimeoutMs = _cfg.WriteTimeoutMs };
        long t0 = Stopwatch.GetTimestamp();
        (OpType type, string note) = await SendWriteAsync(node, req).ConfigureAwait(false);
        if (type == OpType.Ok)
        {
            WorkloadState.Max(_s.AckedMax, key, version);
            double ms = Stopwatch.GetElapsedTime(t0).TotalMilliseconds;
            lock (_s.WriteLatencyMs)
                if (_s.WriteLatencyMs.Count < 500_000) _s.WriteLatencyMs.Add(ms);
        }
        Complete(inv, type, [version], note, label);
    }

    private async Task TransferAsync()
    {
        int from = _rng.Next(_cfg.Accounts), to = _rng.Next(_cfg.Accounts - 1);
        if (to >= from) to++;
        long amount = 1 + _rng.Next(20);
        string node = _leader, label = _s.FaultLabel();
        Op inv = Invoke(OpF.Transfer, node, [from, to], [amount]);
        var req = new WriteReq
        {
            Kind = WriteKind.Merge,
            Keys = [Keys.Account(from), Keys.Account(to)],
            Values = [Keys.U64(unchecked((ulong)-amount)), Keys.U64((ulong)amount)],
            TimeoutMs = _cfg.WriteTimeoutMs,
        };
        (OpType type, string note) = await SendWriteAsync(node, req).ConfigureAwait(false);
        Complete(inv, type, [amount], note, label);
    }

    private async Task AddAsync()
    {
        int ctr = _rng.Next(_cfg.Counters);
        long delta = 1 + _rng.Next(9);
        string node = _leader, label = _s.FaultLabel();
        Op inv = Invoke(OpF.Add, node, [ctr], [delta]);
        var req = new WriteReq { Kind = WriteKind.Merge, Keys = [Keys.Counter(ctr)], Values = [Keys.U64((ulong)delta)], TimeoutMs = _cfg.WriteTimeoutMs };
        (OpType type, string note) = await SendWriteAsync(node, req).ConfigureAwait(false);
        Complete(inv, type, [delta], note, label);
    }

    private async Task<ReadResp?> SendReadAsync(string node, List<string> keys)
    {
        try
        {
            return await _svc[node].WithDeadline(DateTime.UtcNow.AddMilliseconds(1500)).ReadAsync(keys);
        }
        catch (RpcException)
        {
            await Task.Delay(20 + _rng.Next(80)).ConfigureAwait(false);
            return null;
        }
    }

    private async Task ReadAsync()
    {
        int n = 1 + _rng.Next(4);
        int[] keys = new int[n];
        for (int i = 0; i < n; i++) keys[i] = _rng.Next(_cfg.RegisterKeys);
        string node = _rng.Next(2) == 0 ? _leader : RandomNode();
        string label = _s.FaultLabel();
        long[] ackedAtInvoke = new long[n];
        for (int i = 0; i < n; i++) ackedAtInvoke[i] = Volatile.Read(ref _s.AckedMax[keys[i]]);
        Op inv = Invoke(OpF.Read, node, keys, []);
        var names = new List<string>(n);
        foreach (int k in keys) names.Add(Keys.Reg(k));
        ReadResp? resp = await SendReadAsync(node, names).ConfigureAwait(false);
        if (resp is null || !resp.Available || resp.Values.Count != n)
        {
            Complete(inv, OpType.Fail, [], resp is null ? "rpc" : "unavailable", label);
            return;
        }
        string role = resp.IsLeader ? "leader" : "follower";
        long[] vals = new long[n];
        long[] seen = _lastSeen[node];
        for (int i = 0; i < n; i++)
        {
            int k = keys[i];
            byte[]? v = resp.Values[i];
            long version = -1;
            if (v != null)
            {
                if (!Keys.TryDecodeReg(k, v, out int ver, out string? err))
                {
                    _s.Violation("read of register " + k + " on " + node + ": " + err);
                    version = -3;
                }
                else version = ver;
            }
            vals[i] = version;
            if (version > Volatile.Read(ref _s.MaxInvoked[k])) _s.Violation("phantom read: register " + k + " version " + version + " on " + node + " was never written");
            if (version < ackedAtInvoke[i]) _s.Count("read.stale." + role);
            if (version < seen[k]) _s.Count("read.session-backwards." + role);
            seen[k] = Math.Max(seen[k], version);
        }
        _s.Count("read.keys." + role, n);
        Complete(inv, OpType.Ok, vals, role, label);
    }

    private async Task BankReadAsync()
    {
        string node = _rng.Next(2) == 0 ? _leader : RandomNode();
        string label = _s.FaultLabel();
        Op inv = Invoke(OpF.BankRead, node, [], []);
        var names = new List<string>(_cfg.Accounts);
        for (int i = 0; i < _cfg.Accounts; i++) names.Add(Keys.Account(i));
        ReadResp? resp = await SendReadAsync(node, names).ConfigureAwait(false);
        if (resp is null || !resp.Available || resp.Values.Count != _cfg.Accounts)
        {
            Complete(inv, OpType.Fail, [], resp is null ? "rpc" : "unavailable", label);
            return;
        }
        int present = 0;
        ulong sum = 0;
        long[] vals = new long[_cfg.Accounts];
        for (int i = 0; i < _cfg.Accounts; i++)
        {
            if (Keys.TryU64(resp.Values[i], out ulong b))
            {
                present++;
                sum += b;
                vals[i] = unchecked((long)b);
            }
            else if (resp.Values[i] != null) _s.Violation("bank account " + i + " on " + node + " is not a uint64");
        }
        string role = resp.IsLeader ? "leader" : "follower";
        if (present == 0)
        {
            _s.Count("bankread.empty");
            Complete(inv, OpType.Fail, [], "empty", label);
            return;
        }
        if (present != _cfg.Accounts) _s.Violation("bank read on " + node + " (" + role + ", seq " + resp.LatestSeq + "): " + present + " of " + _cfg.Accounts + " accounts");
        else if (sum != _s.BankTotal) _s.Violation("bank read on " + node + " (" + role + ", seq " + resp.LatestSeq + "): total " + unchecked((long)sum) + " != " + _s.BankTotal);
        Complete(inv, OpType.Ok, vals, role, label);
    }

    public void Dispose()
    {
        foreach (GrpcChannel c in _channels) c.Dispose();
    }
}
