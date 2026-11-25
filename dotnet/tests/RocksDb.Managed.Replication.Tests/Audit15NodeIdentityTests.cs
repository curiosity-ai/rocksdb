using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Grpc.Core;
using GrpcStatus = Grpc.Core.StatusCode;
using RocksDb.Managed.Cluster;
using RocksDb.Managed.Replication.Hosting;
using RocksDb.Managed.Replication.MagicOnion;
using RocksDb.Managed.Replication.Switchover;

namespace RocksDb.Managed.Replication.Tests;

/// <summary>
/// Audit #15, per-node identities: a participant proves in the handshake who it is (a node by its node id, a client, an
/// operator), the hosts authorize every endpoint from that (<see cref="ReplicationAuthorizer"/>), and running without
/// authentication is an explicit choice. Before this, every holder of the shared secret could call every endpoint as
/// any node.
/// </summary>
[TestClass]
public sealed class Audit15NodeIdentityTests
{
    private static byte[] Secret(byte seed)
    {
        byte[] s = new byte[32];
        Array.Fill(s, seed);
        return s;
    }

    private static async Task<GrpcStatus?> CodeOfAsync(Func<Task> call)
    {
        try
        {
            await call();
            return null;
        }
        catch (RpcException e)
        {
            return e.StatusCode;
        }
    }

    // A failed handshake surfaces as Unavailable or Internal, depending on which side ended it; never as a reply.
    private static void AssertHandshakeRefused(GrpcStatus? code, string what) =>
        Assert.IsTrue(code is GrpcStatus.Unavailable or GrpcStatus.Internal, what + ": the call must fail in the transport, got " + (code?.ToString() ?? "a reply"));

    // ---- the certificate ------------------------------------------------------------------------------------

    [TestMethod]
    public void TheCertificateProvesTheIdentityItStates()
    {
        using var node = new SharedSecretTls(Secret(1), ReplicationCaller.Node("n1"));
        using var other = new SharedSecretTls(Secret(1), ReplicationCaller.Node("n2"));
        using var op = new SharedSecretTls(Secret(1), ReplicationCaller.Operator("ops"));
        using var client = new SharedSecretTls(Secret(1), ReplicationCaller.Client("app"));

        Assert.IsTrue(node.TryAuthenticate(other.Certificate, out ReplicationCaller who));
        Assert.AreEqual(ReplicationCaller.Node("n2"), who);
        Assert.IsTrue(node.TryAuthenticate(op.Certificate, out who));
        Assert.AreEqual(ReplicationCaller.Operator("ops"), who);
        Assert.IsTrue(node.TryAuthenticate(client.Certificate, out who));
        Assert.AreEqual(ReplicationCaller.Client("app"), who);

        // A client checks the server it reaches: a node, and the node it meant.
        Assert.IsTrue(client.IsExpectedServer(node.Certificate, "n1"));
        Assert.IsTrue(client.IsExpectedServer(node.Certificate, null));
        Assert.IsFalse(client.IsExpectedServer(node.Certificate, "n2"), "another node than the one expected");
        Assert.IsFalse(client.IsExpectedServer(op.Certificate, null), "an operator is not a server");
        Assert.IsFalse(node.IsExpectedServer(client.Certificate, null), "a client is not a server");

        // The identity is covered by the proof: n2's proof on n2's key with the identity rewritten to n1 is refused.
        using X509Certificate2 forged = Reissue(other.Certificate, identity: [(byte)ReplicationCallerKind.Node, (byte)'n', (byte)'1']);
        Assert.IsFalse(node.TryAuthenticate(forged, out _), "a rewritten identity");
        // And a certificate without an identity (the previous format) is refused.
        using X509Certificate2 anonymous = Reissue(other.Certificate, identity: null);
        Assert.IsFalse(node.TryAuthenticate(anonymous, out _), "no identity");

        Assert.ThrowsExactly<ArgumentException>(() => new SharedSecretTls(Secret(1), default));
        Assert.ThrowsExactly<ArgumentException>(() => new SharedSecretTls(Secret(1), ReplicationCaller.Unauthenticated));
        Assert.ThrowsExactly<ArgumentException>(() => new SharedSecretTls(Secret(1), ReplicationCaller.Node(new string('x', SharedSecretTls.MaxNameBytes + 1))));
    }

    /// <summary>A certificate on a new key that copies the proofs of <paramref name="source"/> and states <paramref name="identity"/>.</summary>
    private static X509Certificate2 Reissue(X509Certificate2 source, byte[]? identity)
    {
        using ECDsa key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var request = new CertificateRequest("CN=someone", source.GetECDsaPublicKey() is null ? key : key, HashAlgorithmName.SHA256);
        foreach (X509Extension e in source.Extensions)
        {
            if (e.Oid!.Value == SharedSecretTls.IdentityExtensionOid) continue;
            if (e.Oid.Value is SharedSecretTls.NodeProofExtensionOid or SharedSecretTls.ClientProofExtensionOid)
            {
                request.CertificateExtensions.Add(new X509Extension(e.Oid, e.RawData, critical: false));
            }
        }
        if (identity is not null)
        {
            var writer = new System.Formats.Asn1.AsnWriter(System.Formats.Asn1.AsnEncodingRules.DER);
            writer.WriteOctetString(identity);
            request.CertificateExtensions.Add(new X509Extension(SharedSecretTls.IdentityExtensionOid, writer.Encode(), critical: false));
        }
        return request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
    }

    [TestMethod]
    public void AClientSecretProvesAClientAndNothingElse()
    {
        byte[] nodes = Secret(1), clients = Secret(2);
        using var node = new SharedSecretTls(nodes, clients, ReplicationCaller.Node("n1"));
        using var client = SharedSecretTls.ForClient(clients, "app");

        Assert.IsTrue(node.TryAuthenticate(client.Certificate, out ReplicationCaller who));
        Assert.AreEqual(ReplicationCaller.Client("app"), who);
        Assert.IsTrue(client.IsExpectedServer(node.Certificate, "n1"), "the node also proves that it holds the client secret");

        // Whoever holds only the client secret cannot be a node or an operator to a node, whatever it states: using the
        // client secret as if it were the nodes' secret gives proofs under the wrong key.
        using var asNode = new SharedSecretTls(clients, ReplicationCaller.Node("n2"));
        using var asOperator = new SharedSecretTls(clients, clients, ReplicationCaller.Operator("ops"));
        Assert.IsFalse(node.TryAuthenticate(asNode.Certificate, out _), "the client secret used as the nodes' secret");
        Assert.IsFalse(node.TryAuthenticate(asOperator.Certificate, out _), "an operator that only holds the client secret");

        // A node of a deployment without a client secret does not accept client-secret holders at all.
        using var strict = new SharedSecretTls(nodes, ReplicationCaller.Node("n3"));
        Assert.IsFalse(strict.TryAuthenticate(client.Certificate, out _));
    }

    // ---- the authorizer ---------------------------------------------------------------------------------------

    [TestMethod]
    public void TheAuthorizerKeepsTheClassesApart()
    {
        var auth = new ReplicationAuthorizer("n1", ["n1", "n2", "n3"]);
        ReplicationCaller n2 = ReplicationCaller.Node("n2"), stranger = ReplicationCaller.Node("n9"), self = ReplicationCaller.Node("n1");
        ReplicationCaller client = ReplicationCaller.Client("app"), op = ReplicationCaller.Operator("ops");

        auth.RequirePeer(n2, "n2", "vote");
        auth.RequirePeer(n2, null, "terms");
        Assert.ThrowsExactly<ReplicationUnauthorizedException>(() => auth.RequirePeer(n2, "n3", "vote"), "a member acting as another member");
        Assert.ThrowsExactly<ReplicationUnauthorizedException>(() => auth.RequirePeer(stranger, "n9", "vote"), "not a member");
        Assert.ThrowsExactly<ReplicationUnauthorizedException>(() => auth.RequirePeer(self, "n1", "vote"), "a node is not its own peer");
        Assert.ThrowsExactly<ReplicationUnauthorizedException>(() => auth.RequirePeer(client, null, "terms"));
        Assert.ThrowsExactly<ReplicationUnauthorizedException>(() => auth.RequirePeer(op, null, "terms"));
        Assert.ThrowsExactly<ReplicationUnauthorizedException>(() => auth.RequirePeer(default, null, "terms"), "no identity at all");

        auth.RequireOperatorOrPeer(op, "handoff");
        auth.RequireOperatorOrPeer(n2, "handoff");
        Assert.ThrowsExactly<ReplicationUnauthorizedException>(() => auth.RequireOperatorOrPeer(client, "handoff"));
        Assert.ThrowsExactly<ReplicationUnauthorizedException>(() => auth.RequireOperatorOrPeer(stranger, "handoff"));

        auth.RequireAuthenticated(client, "write");
        auth.RequireAuthenticated(op, "write");
        auth.RequireAuthenticated(stranger, "status");
        Assert.ThrowsExactly<ReplicationUnauthorizedException>(() => auth.RequireAuthenticated(default, "write"));

        // The explicit opt-out of authentication passes everything.
        auth.RequirePeer(ReplicationCaller.Unauthenticated, "n3", "vote");
        auth.RequireOperatorOrPeer(ReplicationCaller.Unauthenticated, "handoff");

        ReplicationAuthorizer replicas = ReplicationAuthorizer.ForReplicas(["r0", "r1"]);
        replicas.RequireReplica(ReplicationCaller.Node("r1"), 1, "report");
        Assert.ThrowsExactly<ReplicationUnauthorizedException>(() => replicas.RequireReplica(ReplicationCaller.Node("r1"), 0, "report"), "another replica's index");
        Assert.ThrowsExactly<ReplicationUnauthorizedException>(() => replicas.RequireReplica(ReplicationCaller.Node("r1"), 2, "report"));
        Assert.ThrowsExactly<ReplicationUnauthorizedException>(() => replicas.RequireReplica(ReplicationCaller.Node("r7"), 0, "report"));
        Assert.ThrowsExactly<ReplicationUnauthorizedException>(() => replicas.RequirePeer(ReplicationCaller.Client("app"), null, "stream"));
    }

    // ---- the in-process transport (the hosts' authorizers, without gRPC) --------------------------------------

    [TestMethod]
    public async Task ClusterNodesOnlyAcceptMembersUnderTheirOwnName()
    {
        await using var h = new ClusterHarness(3, null);
        await h.StartAsync("n1");
        await h.StartAsync("n2");
        await h.StartAsync("n3");
        ClusterNodeHost leader = await h.WaitForLeaderAsync(null);
        string target = leader.NodeId, a = target == "n1" ? "n2" : "n1", b = target == "n3" ? "n2" : "n3";
        long term = leader.Node.CurrentTerm;

        // The member `a`, as itself: accepted (whatever the Raft answer is).
        var asItself = new InMemoryClusterClient(h.Network, a, target);
        await asItself.ReportProgressAsync(a, 0, term, CancellationToken.None);

        // The member `a` acting as the member `b`.
        await Assert.ThrowsExactlyAsync<ReplicationUnauthorizedException>(() => asItself.ReportProgressAsync(b, ulong.MaxValue, term, CancellationToken.None));
        await Assert.ThrowsExactlyAsync<ReplicationUnauthorizedException>(
            () => asItself.RequestVoteAsync(new RequestVotePayload(term + 10, b, ulong.MaxValue, term + 10), CancellationToken.None));
        await Assert.ThrowsExactlyAsync<ReplicationUnauthorizedException>(
            () => asItself.HeartbeatAsync(new HeartbeatPayload(term + 10, b, 0, 0, 0, ClusterMode.Cluster), CancellationToken.None));
        await Assert.ThrowsExactlyAsync<ReplicationUnauthorizedException>(() => asItself.CheckLogConsistencyAsync(b, 0, 0, CancellationToken.None));
        await Assert.ThrowsExactlyAsync<ReplicationUnauthorizedException>(async () =>
        {
            await foreach (WalStreamMessage _ in asItself.StreamUpdatesAsync(b, 1, CancellationToken.None)) break;
        });

        // A node that is not a member, a client and an operator: no node-to-node endpoint.
        foreach (ReplicationCaller outsider in new[] { ReplicationCaller.Node("n9"), ReplicationCaller.Client("app"), ReplicationCaller.Operator("ops") })
        {
            var c = new InMemoryClusterClient(h.Network, a, target) { Caller = outsider };
            await Assert.ThrowsExactlyAsync<ReplicationUnauthorizedException>(
                () => c.RequestVoteAsync(new RequestVotePayload(term + 10, outsider.Name, ulong.MaxValue, term + 10), CancellationToken.None), outsider.ToString());
            await Assert.ThrowsExactlyAsync<ReplicationUnauthorizedException>(() => c.ReportProgressAsync(outsider.Name, 0, term, CancellationToken.None),
                outsider.ToString());
            Assert.IsNotNull(await c.GetStatusAsync(CancellationToken.None), "the status is for every authenticated caller");
        }
        // Nothing of that disturbed the cluster: same leader, same term.
        Assert.AreEqual(target, (await h.WaitForLeaderAsync(null)).NodeId);
        Assert.AreEqual(term, leader.Node.CurrentTerm);
    }

    [TestMethod]
    public async Task PairNodesOnlyAcceptTheirPeer()
    {
        await using var p = new PairHarness();
        await p.StartAsync("A");
        await p.StartAsync("B");
        foreach (ReplicationCaller outsider in new[] { ReplicationCaller.Node("C"), ReplicationCaller.Client("app"), ReplicationCaller.Operator("ops") })
        {
            var c = new InMemoryPairClient(p.Network, "A", "B") { Caller = outsider };
            await Assert.ThrowsExactlyAsync<ReplicationUnauthorizedException>(
                () => c.PromoteAsync(new PromoteRequest(99, "A", 0, 1000), CancellationToken.None), outsider.ToString());
            await Assert.ThrowsExactlyAsync<ReplicationUnauthorizedException>(() => c.RejectEpochAsync(99, CancellationToken.None), outsider.ToString());
            await Assert.ThrowsExactlyAsync<ReplicationUnauthorizedException>(() => c.GetEpochsAsync(CancellationToken.None), outsider.ToString());
            await Assert.ThrowsExactlyAsync<ReplicationUnauthorizedException>(() => c.ReportProgressAsync(ulong.MaxValue, 99, CancellationToken.None), outsider.ToString());
            Assert.IsNotNull(await c.GetStatusAsync(CancellationToken.None));
        }
        // The peer cannot ask for a promotion in another node's name either.
        var peer = new InMemoryPairClient(p.Network, "A", "B");
        await Assert.ThrowsExactlyAsync<ReplicationUnauthorizedException>(() => peer.PromoteAsync(new PromoteRequest(99, "C", 0, 1000), CancellationToken.None));
        Assert.AreEqual(ReplicationRole.Replica, p["B"].Node.Role, "nobody was promoted");
    }

    // ---- gRPC ---------------------------------------------------------------------------------------------------

    private static PairNodeHost PairHost(TempDir dir, string id, IPairNodeClient peer) => new(new PairNodeHostOptions
    {
        NodeId = id,
        PeerId = id == "A" ? "B" : "A",
        DbPath = dir.Sub(id),
        InitialRole = id == "A" ? ReplicationRole.Primary : ReplicationRole.Replica,
        DbOptionsFactory = static () => new DbOptions { WalTtlSeconds = 600 },
        RetryDelay = TimeSpan.FromMilliseconds(50),
    }, peer);

    [TestMethod]
    public async Task PairOverGrpc_ClientsWriteOperatorsHandOffNodesReplicate()
    {
        using var dir = new TempDir("grpc-identity-pair");
        byte[] nodes = Secret(9), clients = Secret(10);
        using var tlsA = new SharedSecretTls(nodes, clients, ReplicationCaller.Node("A"));
        using var tlsB = new SharedSecretTls(nodes, clients, ReplicationCaller.Node("B"));
        using var tlsClient = SharedSecretTls.ForClient(clients, "app");
        using var tlsOperator = new SharedSecretTls(nodes, ReplicationCaller.Operator("ops"));
        using var tlsStranger = new SharedSecretTls(nodes, ReplicationCaller.Node("C"));
        using var tlsFakeNode = new SharedSecretTls(clients, ReplicationCaller.Node("B")); // the client secret used as the nodes'
        int portA = GrpcNodeServer.FindFreePort(), portB = GrpcNodeServer.FindFreePort();
        string endpointA = "https://127.0.0.1:" + portA, endpointB = "https://127.0.0.1:" + portB;
        var toB = new GrpcPairClient("B", endpointB, TimeSpan.FromSeconds(2), GrpcSecurity.SharedSecret(tlsA));
        var toA = new GrpcPairClient("A", endpointA, TimeSpan.FromSeconds(2), GrpcSecurity.SharedSecret(tlsB));
        await using PairNodeHost a = PairHost(dir, "A", toB);
        await using PairNodeHost b = PairHost(dir, "B", toA);
        await using GrpcNodeServer serverA = await GrpcNodeServer.StartAsync<PairService, PairNodeHost>(a, System.Net.IPAddress.Loopback, portA,
            new GrpcNodeServerOptions { Security = GrpcSecurity.SharedSecret(tlsA) }, CancellationToken.None);
        await using GrpcNodeServer serverB = await GrpcNodeServer.StartAsync<PairService, PairNodeHost>(b, System.Net.IPAddress.Loopback, portB,
            new GrpcNodeServerOptions { Security = GrpcSecurity.SharedSecret(tlsB) }, CancellationToken.None);
        await a.StartAsync(CancellationToken.None);
        await b.StartAsync(CancellationToken.None);
        TimeSpan t = TimeSpan.FromSeconds(5);

        // A client that holds only the client secret: status and writes, nothing between nodes, no role change.
        using (var app = new GrpcPairClient("A", endpointA, t, GrpcSecurity.SharedSecret(tlsClient)))
        {
            Assert.IsTrue((await app.WriteAsync("k", "v", CancellationToken.None)).Applied);
            Assert.IsTrue((await app.WriteAsync("big", new string('x', 1024 * 1024), CancellationToken.None)).Applied, "an upload in pieces");
            Assert.IsNotNull(await app.GetStatusAsync(CancellationToken.None));
            Assert.AreEqual(GrpcStatus.PermissionDenied, await CodeOfAsync(async () => await app.Service.PromoteAsync(new PromoteDto { Epoch = 99, FromPrimaryId = "A" })));
            Assert.AreEqual(GrpcStatus.PermissionDenied, await CodeOfAsync(async () => await app.Service.HandOffAsync()));
            Assert.AreEqual(GrpcStatus.PermissionDenied, await CodeOfAsync(async () => await app.Service.ReportProgressAsync(ulong.MaxValue, 99)));
            Assert.AreEqual(GrpcStatus.PermissionDenied, await CodeOfAsync(async () => await app.Service.GetEpochsAsync()));
            Assert.AreEqual(GrpcStatus.PermissionDenied, await CodeOfAsync(async () => await app.Service.RejectEpochAsync(99)));
            Assert.AreEqual(GrpcStatus.PermissionDenied, await CodeOfAsync(async () =>
            {
                using var call = await app.Service.SyncUpdatesAsync(1);
                await call.ResponseStream.MoveNext(CancellationToken.None);
            }));
        }
        // The client secret passed off as the nodes' secret: refused in the handshake.
        using (var fake = new GrpcPairClient("A", endpointA, t, GrpcSecurity.SharedSecret(tlsFakeNode)))
        {
            AssertHandshakeRefused(await CodeOfAsync(async () => await fake.Service.GetStatusAsync()), "a node certificate made with the client secret");
        }
        // A node of the deployment that is not this pair's other node.
        using (var stranger = new GrpcPairClient("A", endpointA, t, GrpcSecurity.SharedSecret(tlsStranger)))
        {
            Assert.AreEqual(GrpcStatus.PermissionDenied, await CodeOfAsync(async () => await stranger.Service.GetEpochsAsync()));
            Assert.AreEqual(GrpcStatus.PermissionDenied, await CodeOfAsync(async () => await stranger.Service.PromoteAsync(new PromoteDto { Epoch = 99, FromPrimaryId = "C" })));
            Assert.AreEqual(GrpcStatus.PermissionDenied, await CodeOfAsync(async () => await stranger.Service.HandOffAsync()));
        }
        // A client that reaches another node than the one it meant refuses the server.
        using (var misdirected = new GrpcPairClient("B", endpointA, t, GrpcSecurity.SharedSecret(tlsClient)))
        {
            AssertHandshakeRefused(await CodeOfAsync(async () => await misdirected.Service.GetStatusAsync()), "node A answering for node B");
        }

        await WaitUntilAsync(() => a.Db.GetLatestSequenceNumber() == b.Db.GetLatestSequenceNumber(), "B to receive the writes (the nodes replicate as themselves)");
        // An operator: no node-to-node endpoint either, but the handoff.
        using (var ops = new GrpcPairClient("A", endpointA, TimeSpan.FromSeconds(20), GrpcSecurity.SharedSecret(tlsOperator)))
        {
            Assert.AreEqual(GrpcStatus.PermissionDenied, await CodeOfAsync(async () => await ops.Service.GetEpochsAsync()));
            SwitchoverResultDto r = await ops.Service.WithDeadline(DateTime.UtcNow.AddSeconds(30)).HandOffAsync();
            Assert.AreEqual((int)SwitchoverOutcome.Completed, r.Outcome, r.Reason);
        }
        await WaitUntilAsync(() => b.Node.Role == ReplicationRole.Primary && a.Node.Role == ReplicationRole.Replica, "the handoff");
        toA.Dispose();
        toB.Dispose();
    }

    [TestMethod]
    public async Task ClusterOverGrpc_MembersCannotActForEachOther()
    {
        using var dir = new TempDir("grpc-identity-cluster");
        byte[] secret = Secret(11);
        var members = new List<ClusterMember>();
        for (int i = 1; i <= 3; i++) members.Add(new ClusterMember("n" + i, "https://127.0.0.1:" + GrpcNodeServer.FindFreePort()));
        var tls = new Dictionary<string, SharedSecretTls>();
        var clients = new List<GrpcClusterClient>();
        var hosts = new Dictionary<string, ClusterNodeHost>();
        var servers = new Dictionary<string, GrpcNodeServer>();
        using var tlsClient = new SharedSecretTls(secret, ReplicationCaller.Client("app"));
        using var tlsStranger = new SharedSecretTls(secret, ReplicationCaller.Node("n9"));
        try
        {
            foreach (ClusterMember m in members)
            {
                tls[m.NodeId] = new SharedSecretTls(secret, ReplicationCaller.Node(m.NodeId));
                // Every node has its own clients: a channel carries the identity of the node that opened it.
                var mine = new Dictionary<string, GrpcClusterClient>();
                foreach (ClusterMember peer in members)
                {
                    if (peer.NodeId == m.NodeId) continue;
                    mine[peer.NodeId] = new GrpcClusterClient(peer, TimeSpan.FromSeconds(1), GrpcSecurity.SharedSecret(tls[m.NodeId]));
                    clients.Add(mine[peer.NodeId]);
                }
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
                }, peer => mine[peer.NodeId]);
                hosts[m.NodeId] = host;
                servers[m.NodeId] = await GrpcNodeServer.StartAsync<ClusterService, ClusterNodeHost>(host, System.Net.IPAddress.Loopback,
                    new Uri(m.Endpoint).Port, new GrpcNodeServerOptions { Security = GrpcSecurity.SharedSecret(tls[m.NodeId]) }, CancellationToken.None);
            }
            await hosts["n1"].StartAsync(CancellationToken.None);
            // The bootstrap primary has data before the others join (an empty log gives them nothing to confirm).
            for (int i = 0; i < 20; i++) Assert.IsTrue(await hosts["n1"].TryWriteAsLeaderAsync(U(K(1000 + i)), U(V(i, 0)), CancellationToken.None));
            await hosts["n2"].StartAsync(CancellationToken.None);
            await hosts["n3"].StartAsync(CancellationToken.None);
            await WaitUntilAsync(() => hosts.Values.All(h => h.Node.Mode == ClusterMode.Cluster), "Cluster mode over gRPC with identities");

            // A client writes through the leader.
            ClusterMember leader = members.First(m => hosts[m.NodeId].Node.Role == RaftRole.Leader);
            using (var app = new GrpcClusterClient(leader, TimeSpan.FromSeconds(10), GrpcSecurity.SharedSecret(tlsClient)))
            {
                for (int i = 0; i < 20; i++) Assert.IsTrue(await app.WriteAsync(K(i), V(i, 0), CancellationToken.None), "committed write " + i);
                Assert.AreEqual(GrpcStatus.PermissionDenied, await CodeOfAsync(async () => await app.Service.GetRaftTermsAsync()));
                Assert.AreEqual(GrpcStatus.PermissionDenied,
                    await CodeOfAsync(async () => await app.Service.ReportLastSyncSequenceNumber("n2", ulong.MaxValue, hosts[leader.NodeId].Node.CurrentTerm)));
            }
            long term = hosts[leader.NodeId].Node.CurrentTerm;
            string a = members.First(m => m.NodeId != leader.NodeId).NodeId, b = members.First(m => m.NodeId != leader.NodeId && m.NodeId != a).NodeId;
            // The member `a` on its own channel, acting as the member `b`.
            using (var asA = new GrpcClusterClient(leader, TimeSpan.FromSeconds(5), GrpcSecurity.SharedSecret(tls[a])))
            {
                Assert.IsNotNull(await asA.Service.GetRaftTermsAsync());
                Assert.AreEqual(GrpcStatus.PermissionDenied, await CodeOfAsync(async () => await asA.Service.ReportLastSyncSequenceNumber(b, ulong.MaxValue, term)));
                Assert.AreEqual(GrpcStatus.PermissionDenied, await CodeOfAsync(async () =>
                    await asA.Service.RequestVoteAsync(new RequestVoteDto { Term = term + 10, CandidateId = b, LastLogSeq = ulong.MaxValue, LastLogTerm = term + 10 })));
                Assert.AreEqual(GrpcStatus.PermissionDenied, await CodeOfAsync(async () =>
                    await asA.Service.HeartbeatAsync(new HeartbeatDto { Term = term + 10, LeaderId = b, LeaderMode = (int)ClusterMode.Cluster })));
            }
            // A node with the secret that is not a member.
            using (var stranger = new GrpcClusterClient(leader, TimeSpan.FromSeconds(5), GrpcSecurity.SharedSecret(tlsStranger)))
            {
                Assert.AreEqual(GrpcStatus.PermissionDenied, await CodeOfAsync(async () =>
                    await stranger.Service.RequestVoteAsync(new RequestVoteDto { Term = term + 10, CandidateId = "n9", LastLogSeq = ulong.MaxValue, LastLogTerm = term + 10 })));
                Assert.AreEqual(GrpcStatus.PermissionDenied, await CodeOfAsync(async () => await stranger.Service.GetRaftTermsAsync()));
            }
            Assert.AreEqual(term, hosts[leader.NodeId].Node.CurrentTerm, "no forged vote request or heartbeat moved the term");
            await WaitUntilAsync(() => hosts.Values.All(h => h.Db.GetLatestSequenceNumber() == hosts[leader.NodeId].Db.GetLatestSequenceNumber()), "the followers to catch up");
        }
        finally
        {
            foreach (GrpcNodeServer s in servers.Values) await s.DisposeAsync();
            foreach (ClusterNodeHost h in hosts.Values) await h.DisposeAsync();
            foreach (GrpcClusterClient c in clients) c.Dispose();
            foreach (SharedSecretTls s in tls.Values) s.Dispose();
        }
    }

    [TestMethod]
    public async Task PlainPrimaryOverGrpc_OnlyItsReplicasEachUnderItsIndex()
    {
        using var dir = new TempDir("grpc-identity-plain");
        byte[] secret = Secret(12);
        using var primaryTls = new SharedSecretTls(secret, ReplicationCaller.Node("primary"));
        using var r0 = new SharedSecretTls(secret, ReplicationCaller.Node("r0"));
        using var r1 = new SharedSecretTls(secret, ReplicationCaller.Node("r1"));
        using var stranger = new SharedSecretTls(secret, ReplicationCaller.Node("r7"));
        using var app = new SharedSecretTls(secret, ReplicationCaller.Client("app"));
        DbOptions o = DbOpts(null);
        o.WalTtlSeconds = 600;
        await using RocksDatabase source = await RocksDatabase.OpenAsync(dir.Sub("primary"), o);
        await WriteWorkloadAsync(source, 0, 50, 0);
        var primary = new ReplicationPrimary(source, dir.Sub("checkpoints"), new AdaptiveCommitDelayController(2));

        // With authentication the primary must be told who its replicas are.
        ArgumentException missing = await Assert.ThrowsExactlyAsync<ArgumentException>(() => GrpcNodeServer.StartAsync<ReplicationService, ReplicationPrimary>(
            primary, new GrpcNodeServerOptions { Security = GrpcSecurity.SharedSecret(primaryTls) }, CancellationToken.None));
        StringAssert.Contains(missing.Message, "ReplicaNodeIds");

        await using GrpcNodeServer server = await GrpcNodeServer.StartAsync<ReplicationService, ReplicationPrimary>(primary,
            new GrpcNodeServerOptions { Security = GrpcSecurity.SharedSecret(primaryTls), ReplicaNodeIds = ["r0", "r1"] }, CancellationToken.None);
        using var c0 = new GrpcReplicationClient(server.Endpoint, TimeSpan.FromSeconds(5), GrpcSecurity.SharedSecret(r0));
        using var c1 = new GrpcReplicationClient(server.Endpoint, TimeSpan.FromSeconds(5), GrpcSecurity.SharedSecret(r1));
        using var c7 = new GrpcReplicationClient(server.Endpoint, TimeSpan.FromSeconds(5), GrpcSecurity.SharedSecret(stranger));
        using var cApp = new GrpcReplicationClient(server.Endpoint, TimeSpan.FromSeconds(5), GrpcSecurity.SharedSecret(app));

        await c0.ReportProgressAsync(0, 1, CancellationToken.None);
        await c1.ReportProgressAsync(1, 1, CancellationToken.None);
        Assert.AreEqual(GrpcStatus.PermissionDenied, await CodeOfAsync(() => c1.ReportProgressAsync(0, ulong.MaxValue, CancellationToken.None)), "another replica's index");
        Assert.AreEqual(GrpcStatus.PermissionDenied, await CodeOfAsync(() => c7.ReportProgressAsync(0, ulong.MaxValue, CancellationToken.None)), "not a replica");
        Assert.AreEqual(GrpcStatus.PermissionDenied, await CodeOfAsync(() => cApp.ReportProgressAsync(0, ulong.MaxValue, CancellationToken.None)), "a client");
        Assert.AreEqual(GrpcStatus.PermissionDenied, await CodeOfAsync(async () =>
        {
            await foreach (WalStreamMessage _ in c7.StreamUpdatesAsync(1, CancellationToken.None)) break;
        }), "the WAL stream is for the replicas");
        Assert.AreEqual(GrpcStatus.PermissionDenied, await CodeOfAsync(async () =>
        {
            await foreach (SnapshotChunk _ in cApp.StreamInitialStateAsync(CancellationToken.None)) break;
        }), "the snapshot is for the replicas");
        // And a replica reads it.
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        await foreach (WalStreamMessage m in c0.StreamUpdatesAsync(1, cts.Token))
        {
            Assert.IsFalse(m.ResyncRequired, m.Reason);
            break;
        }
    }

    // ---- no authentication is a decision ------------------------------------------------------------------------

    [TestMethod]
    public async Task SecurityMustBeStatedAndMustFitTheEndpoint()
    {
        using var dir = new TempDir("grpc-identity-explicit");
        byte[] secret = Secret(13);
        using var nodeA = new SharedSecretTls(secret, ReplicationCaller.Node("A"));
        using var nodeB = new SharedSecretTls(secret, ReplicationCaller.Node("B"));
        using var client = new SharedSecretTls(secret, ReplicationCaller.Client("app"));

        // GrpcNodeServerOptions.Security is a required member and the clients take a GrpcSecurity: leaving it out does
        // not compile. What can still go wrong at run time is a setting that contradicts the endpoint or the host.
        Assert.IsNull(GrpcSecurity.InsecureNoAuthentication.Tls);
        Assert.AreSame(nodeA, GrpcSecurity.SharedSecret(nodeA).Tls);
        Assert.ThrowsExactly<ArgumentNullException>(() => GrpcSecurity.SharedSecret(null!));

        // A shared secret needs TLS; without authentication an https endpoint is a mistake.
        ArgumentException e = Assert.ThrowsExactly<ArgumentException>(
            () => new GrpcPairClient("B", "http://127.0.0.1:1", TimeSpan.FromSeconds(1), GrpcSecurity.SharedSecret(nodeA)));
        StringAssert.Contains(e.Message, "https://");
        e = Assert.ThrowsExactly<ArgumentException>(
            () => new GrpcPairClient("B", "https://127.0.0.1:1", TimeSpan.FromSeconds(1), GrpcSecurity.InsecureNoAuthentication));
        StringAssert.Contains(e.Message, "InsecureNoAuthentication");
        Assert.ThrowsExactly<ArgumentException>(
            () => new GrpcClusterClient(new ClusterMember("n1", "http://127.0.0.1:1"), TimeSpan.FromSeconds(1), GrpcSecurity.SharedSecret(nodeA)));
        Assert.ThrowsExactly<ArgumentException>(() => new GrpcReplicationClient("http://127.0.0.1:1", GrpcSecurity.SharedSecret(nodeA)));

        // A server is a node, and the node its host is.
        using var peer = new GrpcPairClient("B", "https://127.0.0.1:1", TimeSpan.FromSeconds(1), GrpcSecurity.SharedSecret(nodeA));
        await using PairNodeHost host = PairHost(dir, "A", peer);
        e = await Assert.ThrowsExactlyAsync<ArgumentException>(() => GrpcNodeServer.StartAsync<PairService, PairNodeHost>(host,
            new GrpcNodeServerOptions { Security = GrpcSecurity.SharedSecret(client) }, CancellationToken.None));
        StringAssert.Contains(e.Message, "node");
        e = await Assert.ThrowsExactlyAsync<ArgumentException>(() => GrpcNodeServer.StartAsync<PairService, PairNodeHost>(host,
            new GrpcNodeServerOptions { Security = GrpcSecurity.SharedSecret(nodeB) }, CancellationToken.None));
        StringAssert.Contains(e.Message, "'A'");

        // The explicit opt-out: a plaintext server answers every caller as before.
        await using GrpcNodeServer plain = await GrpcNodeServer.StartAsync<PairService, PairNodeHost>(host,
            new GrpcNodeServerOptions { Security = GrpcSecurity.InsecureNoAuthentication }, CancellationToken.None);
        StringAssert.StartsWith(plain.Endpoint, "http://");
        using var anyone = new GrpcPairClient("A", plain.Endpoint, TimeSpan.FromSeconds(5), GrpcSecurity.InsecureNoAuthentication);
        // The host is not started, so the handler itself fails: what matters is that nothing refused the caller.
        GrpcStatus? code = await CodeOfAsync(async () => await anyone.Service.GetEpochsAsync());
        Assert.AreNotEqual(GrpcStatus.PermissionDenied, code);
        Assert.AreNotEqual(GrpcStatus.Unauthenticated, code);
    }
}
