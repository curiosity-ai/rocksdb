// A 3-node Raft cluster over gRPC, as separate processes: the managed port of rocksdb-sharp's Tests/ClusterTest
// ("crash" scenario; election, re-election, quorum commit, far-behind catch-up and term persistence also run in-process
// in tests/RocksDb.Managed.Replication.Tests).
//
//   ClusterTest [coordinator]
//   ClusterTest node <id> <dbPath> <bootstrapId> <id1>=<port1> <id2>=<port2> ...
//
// The coordinator starts n1 (bootstrap primary) and n2, n3, waits for Cluster mode, runs a committed-write load against
// whoever leads, kills the leader, waits for a new one, restarts the old leader, and finally checks that every
// acknowledged (committed) write survived and that all three digests match.
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using Grpc.Net.Client;
using MagicOnion.Client;
using RocksDb.Managed;
using RocksDb.Managed.Cluster;
using RocksDb.Managed.Replication.Hosting;
using RocksDb.Managed.Replication.MagicOnion;

if (args.Length > 0 && args[0] == "node") return await RunNodeAsync(args);
return await RunCoordinatorAsync();

static async Task<int> RunNodeAsync(string[] args)
{
    string id = args[1], dbPath = args[2], bootstrap = args[3];
    var members = new List<ClusterMember>();
    for (int i = 4; i < args.Length; i++)
    {
        string[] kv = args[i].Split('=');
        members.Add(new ClusterMember(kv[0], "http://127.0.0.1:" + kv[1]));
    }
    var clients = new Dictionary<string, GrpcClusterClient>();
    foreach (ClusterMember m in members) clients[m.NodeId] = new GrpcClusterClient(m, TimeSpan.FromSeconds(1));
    ClusterMember self = members.Find(m => m.NodeId == id)!;
    await using var host = new ClusterNodeHost(new ClusterNodeHostOptions
    {
        NodeId = id,
        Endpoint = self.Endpoint,
        DbPath = dbPath,
        Members = members,
        BootstrapPrimaryId = bootstrap,
        DbOptionsFactory = static () => new DbOptions { WalTtlSeconds = 120 },
        ElectionTimeoutMinMs = 1500,
        ElectionTimeoutMaxMs = 3000,
        HeartbeatIntervalMs = 300,
        Log = static m => Console.WriteLine(DateTime.UtcNow.ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture) + " " + m),
    }, m => clients[m.NodeId]);
    await using GrpcNodeServer server = await GrpcNodeServer.StartAsync<ClusterService, ClusterNodeHost>(host, System.Net.IPAddress.Loopback,
        new Uri(self.Endpoint).Port, CancellationToken.None);
    await host.StartAsync(CancellationToken.None);
    Console.WriteLine("READY " + id);
    await Task.Run(Console.In.ReadToEnd);
    foreach (GrpcClusterClient c in clients.Values) c.Dispose();
    return 0;
}

static async Task<int> RunCoordinatorAsync()
{
    string root = Path.Combine(Path.GetTempPath(), "rocksdb-managed-cluster-sample-" + Guid.NewGuid().ToString("N"));
    string[] ids = ["n1", "n2", "n3"];
    var ports = new Dictionary<string, int>();
    foreach (string id in ids) ports[id] = GrpcNodeServer.FindFreePort();
    var memberArgs = new List<string>();
    foreach (string id in ids) memberArgs.Add(id + "=" + ports[id].ToString(CultureInfo.InvariantCulture));
    var clients = new Dictionary<string, IClusterService>();
    var channels = new List<GrpcChannel>();
    foreach (string id in ids)
    {
        GrpcChannel ch = GrpcChannel.ForAddress("http://127.0.0.1:" + ports[id]);
        channels.Add(ch);
        clients[id] = MagicOnionClient.Create<IClusterService>(ch);
    }
    var procs = new Dictionary<string, Process>();
    async Task StartAsync(string id)
    {
        var a = new List<string> { "node", id, Path.Combine(root, id), "n1" };
        a.AddRange(memberArgs);
        procs[id] = Spawn([.. a]);
        await WaitReadyAsync(procs[id]);
    }

    async Task<string?> LeaderAsync()
    {
        foreach (string id in ids)
        {
            try
            {
                ClusterStatusDto s = await clients[id].WithDeadline(DateTime.UtcNow.AddSeconds(1)).GetStatusAsync();
                if ((RaftRole)s.Role == RaftRole.Leader && !s.Resyncing) return id;
            }
            catch (Grpc.Core.RpcException)
            {
            }
        }
        return null;
    }

    foreach (string id in ids) await StartAsync(id);

    var acked = new ConcurrentDictionary<string, string>();
    using var stop = new CancellationTokenSource();
    Task writer = Task.Run(async () =>
    {
        for (int i = 0; !stop.IsCancellationRequested;)
        {
            string? leader = await LeaderAsync();
            if (leader == null)
            {
                await Task.Delay(100);
                continue;
            }
            string key = "k" + i.ToString("D7", CultureInfo.InvariantCulture);
            try
            {
                if (await clients[leader].WriteAsync(key, "v" + i))
                {
                    acked[key] = "v" + i;
                    i++;
                }
            }
            catch (Grpc.Core.RpcException)
            {
                await Task.Delay(100);
            }
        }
    });

    // The bootstrap primary serves writes right away; followers count as in-sync once they applied some of them.
    bool clusterMode = false;
    for (int i = 0; i < 300 && !clusterMode; i++)
    {
        await Task.Delay(100);
        clusterMode = (await clients["n1"].GetStatusAsync()).Mode == (int)ClusterMode.Cluster;
    }
    Console.WriteLine(clusterMode ? "Coordinator: cluster mode reached" : "Coordinator: cluster mode NOT reached");
    await Task.Delay(2000);
    string? old = await LeaderAsync();
    Console.WriteLine("Coordinator: killing leader " + old);
    procs[old!].Kill(entireProcessTree: true);
    await procs[old!].WaitForExitAsync();
    string? next = null;
    for (int i = 0; i < 200 && (next == null || next == old); i++)
    {
        await Task.Delay(100);
        next = await LeaderAsync();
    }
    Console.WriteLine("Coordinator: new leader " + next);
    await StartAsync(old!);
    await Task.Delay(3000);
    await stop.CancelAsync();
    await writer;

    var digests = new List<DigestDto>();
    for (int attempt = 0; attempt < 100; attempt++)
    {
        digests.Clear();
        foreach (string id in ids) digests.Add(await clients[id].ChecksumAsync());
        if (digests.TrueForAll(d => d.Available && d.LatestSeq == digests[0].LatestSeq)) break;
        await Task.Delay(100);
    }
    bool ok = clusterMode && next != null && next != old && digests.TrueForAll(d => d.Available && d.Hash == digests[0].Hash && d.LatestSeq == digests[0].LatestSeq);
    ok &= digests[0].KeyCount >= acked.Count;
    Console.WriteLine("Coordinator: " + acked.Count + " committed writes, " + digests[0].KeyCount + " keys, seq " + digests[0].LatestSeq);
    Console.WriteLine(ok ? "Coordinator: OK" : "Coordinator: FAILED");
    foreach (Process p in procs.Values)
    {
        if (!p.HasExited) p.Kill(entireProcessTree: true);
        p.Dispose();
    }
    foreach (GrpcChannel ch in channels) ch.Dispose();
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
