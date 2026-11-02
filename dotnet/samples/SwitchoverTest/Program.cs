// A primary/replica pair with planned switchover over gRPC, as separate processes: the managed port of rocksdb-sharp's
// Tests/SwitchoverTest ("upgrade" scenario; the other scenarios run in-process in tests/RocksDb.Managed.Replication.Tests).
//
//   SwitchoverTest [coordinator]
//   SwitchoverTest node <id> <port> <dbPath> <primary|replica> <peerId> <peerPort>
//
// The coordinator starts A (primary) and B (replica), runs client writers that follow primary hints, then performs a
// rolling upgrade: hand off A -> B, kill and restart A (rejoins from the WAL), hand off B -> A, kill and restart B. At the
// end every acknowledged write must be on both nodes and their digests must match.
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using Grpc.Net.Client;
using MagicOnion.Client;
using RocksDb.Managed;
using RocksDb.Managed.Replication.Hosting;
using RocksDb.Managed.Replication.MagicOnion;
using RocksDb.Managed.Replication.Switchover;

if (args.Length > 0 && args[0] == "node")
{
    return await RunNodeAsync(args[1], int.Parse(args[2], CultureInfo.InvariantCulture), args[3], args[4] == "primary" ? ReplicationRole.Primary : ReplicationRole.Replica,
        args[5], int.Parse(args[6], CultureInfo.InvariantCulture));
}
return await RunCoordinatorAsync();

static async Task<int> RunNodeAsync(string id, int port, string dbPath, ReplicationRole initialRole, string peerId, int peerPort)
{
    using var peer = new GrpcPairClient(peerId, "http://127.0.0.1:" + peerPort, TimeSpan.FromSeconds(2));
    await using var host = new PairNodeHost(new PairNodeHostOptions
    {
        NodeId = id,
        PeerId = peerId,
        DbPath = dbPath,
        InitialRole = initialRole,
        // The WAL archive is what lets a node that was down for an upgrade rejoin without a snapshot.
        DbOptionsFactory = static () => new DbOptions { WalTtlSeconds = 600 },
        Log = static m => Console.WriteLine(DateTime.UtcNow.ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture) + " " + m),
    }, peer);
    await using GrpcNodeServer server = await GrpcNodeServer.StartAsync<PairService, PairNodeHost>(host, System.Net.IPAddress.Loopback, port, CancellationToken.None);
    await host.StartAsync(CancellationToken.None);
    Console.WriteLine("READY " + id);
    // Run until the coordinator kills us (or closes stdin).
    await Task.Run(Console.In.ReadToEnd);
    return 0;
}

static async Task<int> RunCoordinatorAsync()
{
    string root = Path.Combine(Path.GetTempPath(), "rocksdb-managed-switchover-sample-" + Guid.NewGuid().ToString("N"));
    int portA = GrpcNodeServer.FindFreePort(), portB = GrpcNodeServer.FindFreePort();
    var ports = new Dictionary<string, int> { ["A"] = portA, ["B"] = portB };
    var procs = new Dictionary<string, Process>();
    Process Start(string id) =>
        procs[id] = Spawn("node", id, ports[id].ToString(CultureInfo.InvariantCulture), Path.Combine(root, id), id == "A" ? "primary" : "replica",
            id == "A" ? "B" : "A", ports[id == "A" ? "B" : "A"].ToString(CultureInfo.InvariantCulture));
    using var chA = GrpcChannel.ForAddress("http://127.0.0.1:" + portA);
    using var chB = GrpcChannel.ForAddress("http://127.0.0.1:" + portB);
    var clients = new Dictionary<string, IPairService> { ["A"] = MagicOnionClient.Create<IPairService>(chA), ["B"] = MagicOnionClient.Create<IPairService>(chB) };
    var acked = new ConcurrentDictionary<string, string>();
    using var stop = new CancellationTokenSource();

    await WaitReadyAsync(Start("A"));
    await WaitReadyAsync(Start("B"));
    Task[] writers = new Task[4];
    for (int w = 0; w < writers.Length; w++)
    {
        int writer = w;
        writers[w] = Task.Run(async () =>
        {
            string target = "A";
            for (int i = 0; !stop.IsCancellationRequested;)
            {
                string key = "w" + writer + "-" + i.ToString("D7", CultureInfo.InvariantCulture);
                try
                {
                    PairWriteResultDto r = await clients[target].WriteAsync(key, "v" + i);
                    if (r.Applied)
                    {
                        acked[key] = "v" + i;
                        i++;
                    }
                    else
                    {
                        target = r.PrimaryHint ?? (target == "A" ? "B" : "A");
                    }
                }
                catch (Grpc.Core.RpcException)
                {
                    target = target == "A" ? "B" : "A";
                    await Task.Delay(20);
                }
            }
        });
    }

    async Task<bool> HandOffAsync(string from)
    {
        await Task.Delay(2000);
        SwitchoverResult r = (await clients[from].HandOffAsync()).ToResult();
        Console.WriteLine("Coordinator: handoff from " + from + ": " + r.Outcome + " epoch " + r.Epoch + ", writes held " + r.WritesHeld.TotalMilliseconds + "ms " + r.Reason);
        return r.Outcome == SwitchoverOutcome.Completed;
    }

    async Task RestartAsync(string id)
    {
        procs[id].Kill(entireProcessTree: true);
        await procs[id].WaitForExitAsync();
        procs[id].Dispose();
        await Task.Delay(1000);
        await WaitReadyAsync(Start(id));
    }

    bool ok = await HandOffAsync("A");
    await RestartAsync("A");
    ok &= await HandOffAsync("B");
    await RestartAsync("B");
    await Task.Delay(2000);
    await stop.CancelAsync();
    await Task.WhenAll(writers);

    // Converge, then validate.
    DigestDto a = new(), b = new();
    for (int attempt = 0; attempt < 100; attempt++)
    {
        a = await clients["A"].ChecksumAsync();
        b = await clients["B"].ChecksumAsync();
        if (a.Available && b.Available && a.LatestSeq == b.LatestSeq) break;
        await Task.Delay(100);
    }
    ok &= a.Hash == b.Hash && a.KeyCount == b.KeyCount && a.LatestSeq == b.LatestSeq;
    ok &= a.KeyCount >= acked.Count;
    Console.WriteLine("Coordinator: " + acked.Count + " acknowledged writes; A keys=" + a.KeyCount + " seq=" + a.LatestSeq + ", B keys=" + b.KeyCount + " seq=" + b.LatestSeq);
    Console.WriteLine(ok ? "Coordinator: OK" : "Coordinator: FAILED");
    foreach (Process p in procs.Values)
    {
        p.Kill(entireProcessTree: true);
        p.Dispose();
    }
    try { Directory.Delete(root, true); } catch (IOException) { }
    return ok ? 0 : 1;
}

static Process Spawn(params string[] args)
{
    var psi = new ProcessStartInfo(Environment.ProcessPath!) { UseShellExecute = false, RedirectStandardOutput = true, RedirectStandardInput = true };
    // Run through the same host: "dotnet Sample.dll" re-launches "dotnet Sample.dll <args>".
    if (Path.GetFileNameWithoutExtension(Environment.ProcessPath) == "dotnet") psi.ArgumentList.Add(System.Reflection.Assembly.GetEntryAssembly()!.Location);
    foreach (string a in args) psi.ArgumentList.Add(a);
    return Process.Start(psi)!;
}

static async Task WaitReadyAsync(Process p)
{
    var ready = new TaskCompletionSource();
    p.OutputDataReceived += (_, e) =>
    {
        if (e.Data == null) return;
        Console.WriteLine(e.Data);
        if (e.Data.StartsWith("READY", StringComparison.Ordinal)) ready.TrySetResult();
    };
    p.BeginOutputReadLine();
    await ready.Task.WaitAsync(TimeSpan.FromSeconds(60));
}
