using RocksDb.Managed.Cluster;
using RocksDb.Managed.Replication.Hosting;
using RocksDb.Managed.Replication.MagicOnion;
using RocksDb.Managed.Replication.Switchover;

namespace RocksDb.Managed.Replication.Tests;

/// <summary>
/// The MagicOnion/gRPC hosting over real HTTP/2 on localhost (free ports): a 2-node replication, a switchover pair and a
/// 3-node Raft cluster with a leader failover.
/// </summary>
[TestClass]
public sealed class GrpcTests
{
    [TestMethod]
    public async Task TwoNodeReplicationOverGrpc()
    {
        using var dir = new TempDir("grpc-repl");
        DbOptions o = DbOpts(null);
        o.WalTtlSeconds = 600;
        await using RocksDatabase source = await RocksDatabase.OpenAsync(dir.Sub("primary"), o);
        await WriteWorkloadAsync(source, 0, 500, 0);
        var primary = new ReplicationPrimary(source, dir.Sub("checkpoints"), new AdaptiveCommitDelayController(1)) { MaxChunkBytes = 16 * 1024 };
        await using GrpcNodeServer server = await GrpcNodeServer.StartAsync<ReplicationService, ReplicationPrimary>(primary, CancellationToken.None);

        using var client = new GrpcReplicationClient(server.Endpoint);
        SnapshotRestoreResult r = await ReplicaFollower.SyncInitialStateAsync(client, LocalFileSystem.Instance, dir.Sub("replica"), [], CancellationToken.None);
        Assert.IsTrue(r.TransferredFiles > 0);
        await using RocksDatabase replica = await RocksDatabase.OpenAsync(dir.Sub("replica"), DbOpts(null));
        AssertReplicaEquals(source, replica, "after the initial state");

        using var cts = new CancellationTokenSource();
        Task<long> follow = ReplicaFollower.FollowAsync(client, replica, 0, cts.Token);
        await WriteWorkloadAsync(source, 500, 1500, 1);
        await source.FlushAsync();
        await WriteWorkloadAsync(source, 1500, 2000, 2);
        await WaitForSequenceAsync(replica, source.GetLatestSequenceNumber(), "gRPC stream");
        AssertReplicaEquals(source, replica, "after streaming over gRPC");
        await WaitUntilAsync(() => primary.Source.SlowestReplicaSequence == source.GetLatestSequenceNumber(), "progress reports");
        await cts.CancelAsync();
        try
        {
            Assert.IsTrue(await follow > 0);
        }
        catch (OperationCanceledException)
        {
        }
        catch (Grpc.Core.RpcException e) when (e.StatusCode == Grpc.Core.StatusCode.Cancelled)
        {
        }
    }

    [TestMethod]
    public async Task SwitchoverPairOverGrpc()
    {
        using var dir = new TempDir("grpc-pair");
        int portA = GrpcNodeServer.FindFreePort(), portB = GrpcNodeServer.FindFreePort();
        var toB = new GrpcPairClient("B", "http://127.0.0.1:" + portB, TimeSpan.FromSeconds(2));
        var toA = new GrpcPairClient("A", "http://127.0.0.1:" + portA, TimeSpan.FromSeconds(2));
        PairNodeHost Host(string id, IPairNodeClient peer) => new(new PairNodeHostOptions
        {
            NodeId = id,
            PeerId = id == "A" ? "B" : "A",
            DbPath = dir.Sub(id),
            InitialRole = id == "A" ? ReplicationRole.Primary : ReplicationRole.Replica,
            DbOptionsFactory = static () => new DbOptions { WalTtlSeconds = 600 },
            RetryDelay = TimeSpan.FromMilliseconds(50),
        }, peer);
        await using PairNodeHost a = Host("A", toB);
        await using PairNodeHost b = Host("B", toA);
        await using GrpcNodeServer serverA = await GrpcNodeServer.StartAsync<PairService, PairNodeHost>(a, System.Net.IPAddress.Loopback, portA, CancellationToken.None);
        await using GrpcNodeServer serverB = await GrpcNodeServer.StartAsync<PairService, PairNodeHost>(b, System.Net.IPAddress.Loopback, portB, CancellationToken.None);
        await a.StartAsync(CancellationToken.None);
        await b.StartAsync(CancellationToken.None);

        for (int i = 0; i < 300; i++) Assert.IsTrue((await toA.Service.WriteAsync(K(i), V(i, 0))).Applied);
        SwitchoverResult result = (await toA.Service.HandOffAsync()).ToResult();
        Assert.AreEqual(SwitchoverOutcome.Completed, result.Outcome, result.Reason);
        PairWriteResultDto refused = await toA.Service.WriteAsync("x", "y");
        Assert.IsFalse(refused.Applied);
        Assert.AreEqual("B", refused.PrimaryHint);
        for (int i = 300; i < 400; i++) Assert.IsTrue((await toB.Service.WriteAsync(K(i), V(i, 0))).Applied);
        await WaitUntilAsync(() => a.Db.GetLatestSequenceNumber() == b.Db.GetLatestSequenceNumber(), "A follows B");
        Assert.AreEqual((await toA.Service.ChecksumAsync()).ToDigest(), (await toB.Service.ChecksumAsync()).ToDigest());
        toA.Dispose();
        toB.Dispose();
    }

    [TestMethod]
    public async Task ThreeNodeClusterOverGrpc()
    {
        using var dir = new TempDir("grpc-cluster");
        var members = new List<ClusterMember>();
        for (int i = 1; i <= 3; i++) members.Add(new ClusterMember("n" + i, "http://127.0.0.1:" + GrpcNodeServer.FindFreePort()));
        var clients = new Dictionary<string, GrpcClusterClient>();
        foreach (ClusterMember m in members) clients[m.NodeId] = new GrpcClusterClient(m, TimeSpan.FromSeconds(1));
        var hosts = new Dictionary<string, ClusterNodeHost>();
        var servers = new Dictionary<string, GrpcNodeServer>();
        try
        {
            foreach (ClusterMember m in members)
            {
                var host = new ClusterNodeHost(new ClusterNodeHostOptions
                {
                    NodeId = m.NodeId,
                    Endpoint = m.Endpoint,
                    DbPath = dir.Sub(m.NodeId),
                    Members = members,
                    BootstrapPrimaryId = "n1",
                    DbOptionsFactory = static () => new DbOptions { WalTtlSeconds = 600 },
                    ElectionTimeoutMinMs = 400,
                    ElectionTimeoutMaxMs = 800,
                    HeartbeatIntervalMs = 60,
                    RetryDelay = TimeSpan.FromMilliseconds(50),
                }, peer => clients[peer.NodeId]);
                hosts[m.NodeId] = host;
                servers[m.NodeId] = await GrpcNodeServer.StartAsync<ClusterService, ClusterNodeHost>(host, System.Net.IPAddress.Loopback,
                    new Uri(m.Endpoint).Port, CancellationToken.None);
            }
            await hosts["n1"].StartAsync(CancellationToken.None);
            for (int i = 0; i < 100; i++) Assert.IsTrue(await hosts["n1"].TryWriteAsLeaderAsync(U(K(i)), U(V(i, 0)), CancellationToken.None));
            await hosts["n2"].StartAsync(CancellationToken.None);
            await hosts["n3"].StartAsync(CancellationToken.None);
            await WaitUntilAsync(() => hosts.Values.All(h => h.Node.Mode == ClusterMode.Cluster), "Cluster mode over gRPC");
            for (int i = 100; i < 200; i++) Assert.IsTrue(await clients["n1"].Service.WriteAsync(K(i), V(i, 0)), "committed write " + i);

            // Leader failover.
            await servers["n1"].DisposeAsync();
            servers.Remove("n1");
            await hosts["n1"].DisposeAsync();
            hosts.Remove("n1");
            await WaitUntilAsync(() => hosts.Values.Any(h => h.Node.Role == RaftRole.Leader), TimeSpan.FromSeconds(15), "a new leader over gRPC");
            ClusterNodeHost leader = hosts.Values.First(h => h.Node.Role == RaftRole.Leader);
            Assert.IsTrue(leader.Node.CurrentTerm >= 2);
            for (int i = 200; i < 250; i++) Assert.IsTrue(await clients[leader.NodeId].Service.WriteAsync(K(i), V(i, 0)), "committed write " + i);
            await WaitUntilAsync(() => hosts.Values.All(h => h.Db.GetLatestSequenceNumber() == leader.Db.GetLatestSequenceNumber()), "followers converge");
            DigestDto d1 = await clients["n2"].Service.ChecksumAsync(), d2 = await clients["n3"].Service.ChecksumAsync();
            Assert.AreEqual(d1.ToDigest(), d2.ToDigest());
            Assert.AreEqual(250 + 1 /* leader no-op key */, d1.KeyCount);
        }
        finally
        {
            foreach (GrpcNodeServer s in servers.Values) await s.DisposeAsync();
            foreach (ClusterNodeHost h in hosts.Values) await h.DisposeAsync();
            foreach (GrpcClusterClient c in clients.Values) c.Dispose();
        }
    }
}
