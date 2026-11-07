using System.Net;
using System.Net.Sockets;

namespace RocksDb.Managed.ClusterSoak;

/// <summary>
/// A user-space TCP proxy for one directed link (from -> to): the "from" side connects to <see cref="Port"/>, the proxy
/// connects on to the target's listening port and pumps bytes both ways. The nemesis controls it:
/// <list type="bullet">
/// <item><see cref="Cut"/>: every open connection is closed (reset) and new connections are accepted and closed at once
/// (connection refused for the RPC layer). Healing (<see cref="Heal"/>) lets new connections through again.</item>
/// <item><see cref="DelayMs"/>: every forwarded read is held for that long before it is written on (latency; order is
/// kept because each direction is one pump).</item>
/// </list>
/// A link carries the RPCs initiated by "from" and their answers, so cutting only A->B is an RPC-level one-way
/// partition: A cannot call B while B can still call A.
/// </summary>
internal sealed class LinkProxy : IAsyncDisposable
{
    private readonly TcpListener _listener;
    private readonly Lock _gate = new();
    private readonly HashSet<Connection> _open = [];
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _accept;
    private volatile bool _cut;
    private volatile int _targetPort;

    public LinkProxy(string from, string to, int targetPort)
    {
        From = from;
        To = to;
        _targetPort = targetPort;
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start(512);
        Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
        _accept = Task.Run(AcceptLoopAsync);
    }

    public string From { get; }
    public string To { get; }
    public int Port { get; }
    public bool IsCut => _cut;
    public long Connections;
    public long Refused;

    /// <summary>Per-chunk forwarding delay in ms (0 = none).</summary>
    public volatile int DelayMs;

    public int TargetPort
    {
        get => _targetPort;
        set => _targetPort = value;
    }

    public void Cut()
    {
        _cut = true;
        Connection[] open;
        lock (_gate) open = [.. _open];
        foreach (Connection c in open) c.Close();
    }

    public void Heal()
    {
        _cut = false;
        DelayMs = 0;
    }

    /// <summary>Closes the open connections without refusing new ones (a transient network blip).</summary>
    public void ResetConnections()
    {
        Connection[] open;
        lock (_gate) open = [.. _open];
        foreach (Connection c in open) c.Close();
    }

    private async Task AcceptLoopAsync()
    {
        while (!_cts.IsCancellationRequested)
        {
            Socket client;
            try
            {
                client = await _listener.AcceptSocketAsync(_cts.Token).ConfigureAwait(false);
            }
            catch (Exception) when (_cts.IsCancellationRequested)
            {
                return;
            }
            catch (SocketException)
            {
                continue;
            }
            if (_cut)
            {
                Interlocked.Increment(ref Refused);
                Abort(client);
                continue;
            }
            _ = Task.Run(() => HandleAsync(client));
        }
    }

    private static void Abort(Socket s)
    {
        try
        {
            s.LingerState = new LingerOption(true, 0); // RST, like a dropped link
            s.Close();
        }
        catch (Exception)
        {
        }
    }

    private async Task HandleAsync(Socket client)
    {
        var server = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
        client.NoDelay = true;
        try
        {
            using var connectTimeout = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
            connectTimeout.CancelAfter(2000);
            await server.ConnectAsync(new IPEndPoint(IPAddress.Loopback, _targetPort), connectTimeout.Token).ConfigureAwait(false);
        }
        catch (Exception)
        {
            Abort(client);
            Abort(server);
            return;
        }
        var c = new Connection(client, server);
        lock (_gate)
        {
            if (_cut)
            {
                c.Close();
                return;
            }
            _open.Add(c);
        }
        Interlocked.Increment(ref Connections);
        try
        {
            await Task.WhenAll(PumpAsync(client, server, c), PumpAsync(server, client, c)).ConfigureAwait(false);
        }
        finally
        {
            c.Close();
            lock (_gate) _open.Remove(c);
        }
    }

    private async Task PumpAsync(Socket src, Socket dst, Connection c)
    {
        byte[] buffer = new byte[64 * 1024];
        try
        {
            while (!c.Closed)
            {
                int n = await src.ReceiveAsync(buffer, SocketFlags.None, _cts.Token).ConfigureAwait(false);
                if (n == 0) break;
                int delay = DelayMs;
                if (delay > 0) await Task.Delay(delay, _cts.Token).ConfigureAwait(false);
                if (c.Closed) break;
                int sent = 0;
                while (sent < n) sent += await dst.SendAsync(buffer.AsMemory(sent, n - sent), SocketFlags.None, _cts.Token).ConfigureAwait(false);
            }
        }
        catch (Exception)
        {
        }
        c.Close();
    }

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync().ConfigureAwait(false);
        _listener.Stop();
        Cut();
        try
        {
            await _accept.ConfigureAwait(false);
        }
        catch (Exception)
        {
        }
    }

    private sealed class Connection(Socket a, Socket b)
    {
        private int _closed;
        public bool Closed => Volatile.Read(ref _closed) == 1;

        public void Close()
        {
            if (Interlocked.Exchange(ref _closed, 1) == 1) return;
            Abort(a);
            Abort(b);
        }
    }
}

/// <summary>All links of a run: node->node and client->node, plus the topology helpers the nemesis uses.</summary>
internal sealed class Network : IAsyncDisposable
{
    private readonly Dictionary<(string, string), LinkProxy> _links = new();

    public Network(IReadOnlyList<string> nodes, IReadOnlyList<string> clients, Func<string, int> nodePort)
    {
        Nodes = nodes;
        Clients = clients;
        foreach (string a in nodes)
            foreach (string b in nodes)
                if (a != b) _links[(a, b)] = new LinkProxy(a, b, nodePort(b));
        foreach (string c in clients)
            foreach (string b in nodes)
                _links[(c, b)] = new LinkProxy(c, b, nodePort(b));
    }

    public IReadOnlyList<string> Nodes { get; }
    public IReadOnlyList<string> Clients { get; }

    public LinkProxy this[string from, string to] => _links[(from, to)];

    public IEnumerable<LinkProxy> All => _links.Values;

    public void SetTargetPort(string node, int port)
    {
        foreach (LinkProxy l in _links.Values)
            if (l.To == node) l.TargetPort = port;
    }

    public void HealAll()
    {
        foreach (LinkProxy l in _links.Values) l.Heal();
    }

    /// <summary>Cuts both directions between every node of <paramref name="a"/> and every node of <paramref name="b"/>.</summary>
    public void CutBetween(IEnumerable<string> a, IEnumerable<string> b)
    {
        foreach (string x in a)
            foreach (string y in b)
            {
                if (x == y) continue;
                _links[(x, y)].Cut();
                _links[(y, x)].Cut();
            }
    }

    /// <summary>A textual description of the cut node links (for the log and the nemesis schedule).</summary>
    public string DescribeCuts()
    {
        var cut = new List<string>();
        foreach (LinkProxy l in _links.Values)
            if (l.IsCut) cut.Add(l.From + ">" + l.To);
        foreach (LinkProxy l in _links.Values)
            if (l.DelayMs > 0) cut.Add(l.From + "~" + l.To + "(" + l.DelayMs + "ms)");
        cut.Sort(StringComparer.Ordinal);
        return cut.Count == 0 ? "none" : string.Join(" ", cut);
    }

    public async ValueTask DisposeAsync()
    {
        foreach (LinkProxy l in _links.Values) await l.DisposeAsync().ConfigureAwait(false);
    }
}
