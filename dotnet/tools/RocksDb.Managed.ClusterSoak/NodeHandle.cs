using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text.Json;
using Grpc.Core;
using Grpc.Net.Client;
using MagicOnion.Client;

namespace RocksDb.Managed.ClusterSoak;

internal enum NodeState
{
    Down,
    Starting,
    Running,
    Paused,
}

/// <summary>
/// Supervisor-side handle of one node process: start / SIGKILL / SIGSTOP / SIGCONT / power loss / graceful stop, its
/// output (logs/&lt;id&gt;.log) and a direct (unproxied) control client. An exit the supervisor did not ask for is a
/// failure (<see cref="UnexpectedExit"/>).
/// </summary>
internal sealed class NodeHandle : IAsyncDisposable
{
    private readonly RunPaths _paths;
    private readonly Func<NodeConfig> _config;
    private readonly Action<string> _log;
    private readonly Lock _gate = new();
    private readonly StreamWriter _out;
    private Process? _proc;
    private TaskCompletionSource<bool>? _ready;
    private GrpcChannel? _channel;
    private int _expectedExit = -1;
    private int _generation;

    public NodeHandle(string id, RunPaths paths, Func<NodeConfig> config, Action<string> log)
    {
        Id = id;
        _paths = paths;
        _config = config;
        _log = log;
        Directory.CreateDirectory(paths.NodeRoot(id));
        _out = new StreamWriter(new FileStream(paths.NodeLog(id), FileMode.Append, FileAccess.Write, FileShare.ReadWrite)) { AutoFlush = true };
        Port = FindFreePort();
    }

    public string Id { get; }
    public int Port { get; private set; }
    public volatile NodeState State = NodeState.Down;
    /// <summary>Set when the process exited without the supervisor asking for it (crash, unhandled exception).</summary>
    public volatile string? UnexpectedExit;
    /// <summary>The last start failed because the listen port was taken.</summary>
    public volatile bool PortConflict;
    public int Starts;

    /// <summary>Raised for every output line of the node (from a reader thread).</summary>
    public event Action<NodeHandle, string>? Line;

    public ISoakService? Control { get; private set; }

    public static int FindFreePort()
    {
        using var l = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        l.Start();
        int port = ((System.Net.IPEndPoint)l.LocalEndpoint).Port;
        l.Stop();
        return port;
    }

    public void NewPort() => Port = FindFreePort();

    /// <summary>Starts the process; completes when it printed READY (true) or exited first (false).</summary>
    public Task<bool> StartAsync()
    {
        lock (_gate)
        {
            if (State != NodeState.Down) return _ready?.Task ?? Task.FromResult(true);
            NodeConfig cfg = _config();
            cfg.ListenPort = Port;
            string cfgPath = Path.Combine(_paths.NodeRoot(Id), "node.json");
            File.WriteAllText(cfgPath, JsonSerializer.Serialize(cfg, Json.Indented));
            var psi = new ProcessStartInfo(Environment.ProcessPath!)
            {
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                RedirectStandardInput = true,
            };
            if (Path.GetFileNameWithoutExtension(Environment.ProcessPath) == "dotnet") psi.ArgumentList.Add(typeof(NodeHandle).Assembly.Location);
            psi.ArgumentList.Add("node");
            psi.ArgumentList.Add("--config");
            psi.ArgumentList.Add(cfgPath);
            psi.Environment["DOTNET_gcServer"] = "0";
            psi.Environment["DOTNET_GCHeapHardLimit"] = "0x30000000";
            _ready = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            _expectedExit = -1;
            int gen = ++_generation;
            Starts++;
            _out.WriteLine("==== start #" + Starts + " " + DateTime.UtcNow.ToString("O", CultureInfo.InvariantCulture) + " port " + Port);
            Process p = Process.Start(psi)!;
            _proc = p;
            State = NodeState.Starting;
            TaskCompletionSource<bool> ready = _ready;
            p.OutputDataReceived += (_, e) => OnOutput(e.Data, ready);
            p.ErrorDataReceived += (_, e) => OnOutput(e.Data, null);
            p.BeginOutputReadLine();
            p.BeginErrorReadLine();
            _ = Task.Run(async () =>
            {
                await p.WaitForExitAsync().ConfigureAwait(false);
                OnExited(p, gen);
            });
            _channel?.Dispose();
            _channel = GrpcChannel.ForAddress("http://127.0.0.1:" + Port.ToString(CultureInfo.InvariantCulture), new GrpcChannelOptions
            {
                InitialReconnectBackoff = TimeSpan.FromMilliseconds(100),
                MaxReconnectBackoff = TimeSpan.FromMilliseconds(500),
                MaxReceiveMessageSize = 64 << 20,
            });
            Control = MagicOnionClient.Create<ISoakService>(_channel);
            return ready.Task;
        }
    }

    private void OnOutput(string? line, TaskCompletionSource<bool>? ready)
    {
        if (line is null) return;
        lock (_out) _out.WriteLine(line);
        if (ready != null && line.Contains("READY ", StringComparison.Ordinal))
        {
            lock (_gate)
            {
                if (State == NodeState.Starting) State = NodeState.Running;
            }
            ready.TrySetResult(true);
        }
        Line?.Invoke(this, line);
    }

    private void OnExited(Process p, int gen)
    {
        int code;
        try
        {
            code = p.ExitCode;
        }
        catch (InvalidOperationException)
        {
            code = -1;
        }
        lock (_gate)
        {
            if (gen != _generation) return;
            bool expected = _expectedExit != -1 && (code == _expectedExit || code == 137 || code == -1 || (_expectedExit == 0 && code == 0));
            lock (_out) _out.WriteLine("==== exit " + code + (expected ? "" : " (UNEXPECTED)"));
            if (!expected)
            {
                UnexpectedExit = "node " + Id + " exited with code " + code + " (start #" + Starts + ")";
                _log("FAILURE: " + UnexpectedExit);
            }
            State = NodeState.Down;
            _ready?.TrySetResult(false);
        }
    }

    [DllImport("libc", SetLastError = true, EntryPoint = "kill")]
    private static extern int SysKill(int pid, int sig);

    private const int SIGKILL = 9, SIGSTOP = 19, SIGCONT = 18;

    private bool Signal(int sig)
    {
        Process? p = _proc;
        if (p is null) return false;
        try
        {
            if (p.HasExited) return false;
            return SysKill(p.Id, sig) == 0;
        }
        catch (InvalidOperationException)
        {
            return false;
        }
    }

    /// <summary>SIGKILL (also of a paused process) and wait for the exit.</summary>
    public async Task KillAsync()
    {
        Process? p;
        lock (_gate)
        {
            p = _proc;
            if (State == NodeState.Down || p is null) return;
            _expectedExit = 137;
        }
        Signal(SIGKILL);
        await WaitExitAsync(p, TimeSpan.FromSeconds(30)).ConfigureAwait(false);
    }

    public bool Pause()
    {
        lock (_gate)
        {
            if (State != NodeState.Running) return false;
            if (!Signal(SIGSTOP)) return false;
            State = NodeState.Paused;
            return true;
        }
    }

    public void Resume()
    {
        lock (_gate)
        {
            if (State != NodeState.Paused) return;
            Signal(SIGCONT);
            State = NodeState.Running;
        }
    }

    /// <summary>Sends a command line to the node's stdin.</summary>
    public bool Send(string command)
    {
        Process? p = _proc;
        if (p is null || State is NodeState.Down) return false;
        try
        {
            p.StandardInput.WriteLine(command);
            p.StandardInput.Flush();
            return true;
        }
        catch (Exception e) when (e is IOException or InvalidOperationException)
        {
            return false;
        }
    }

    /// <summary>Simulated power loss (the node drops its unsynced data and exits with 77); SIGKILL if it does not.</summary>
    public async Task<bool> PowerLossAsync()
    {
        Process? p;
        lock (_gate)
        {
            p = _proc;
            if (State != NodeState.Running || p is null) return false;
            _expectedExit = 77;
        }
        if (!Send("crash")) return false;
        if (!await WaitExitAsync(p, TimeSpan.FromSeconds(40)).ConfigureAwait(false))
        {
            _log("node " + Id + " did not exit after the power-loss command; SIGKILL");
            Signal(SIGKILL);
            await WaitExitAsync(p, TimeSpan.FromSeconds(10)).ConfigureAwait(false);
        }
        return true;
    }

    /// <summary>Graceful stop (stdin "stop"): the node closes the host and the database and exits 0. False on a hang.</summary>
    public async Task<bool> StopAsync(TimeSpan timeout)
    {
        Process? p;
        lock (_gate)
        {
            p = _proc;
            if (State == NodeState.Down || p is null) return true;
            if (State == NodeState.Paused)
            {
                Signal(SIGCONT);
                State = NodeState.Running;
            }
            _expectedExit = 0;
        }
        Send("stop");
        if (await WaitExitAsync(p, timeout).ConfigureAwait(false)) return true;
        lock (_gate) _expectedExit = 137;
        Signal(SIGKILL);
        await WaitExitAsync(p, TimeSpan.FromSeconds(10)).ConfigureAwait(false);
        return false;
    }

    private static async Task<bool> WaitExitAsync(Process p, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        try
        {
            await p.WaitForExitAsync(cts.Token).ConfigureAwait(false);
            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }

    public async Task<NodeStatusDto?> StatusAsync(TimeSpan timeout)
    {
        ISoakService? c = Control;
        if (c is null || State is NodeState.Down or NodeState.Paused or NodeState.Starting) return null;
        try
        {
            return await c.WithDeadline(DateTime.UtcNow + timeout).StatusAsync();
        }
        catch (RpcException)
        {
            return null;
        }
    }

    public async ValueTask DisposeAsync()
    {
        await KillAsync().ConfigureAwait(false);
        _channel?.Dispose();
        lock (_out) _out.Dispose();
    }
}
