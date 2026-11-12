using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Grpc.Net.Client;
using RocksDb.Managed.Replication.Hosting;
using RocksDb.Managed.Replication.MagicOnion;
using RocksDb.Managed.Replication.Switchover;

namespace RocksDb.Managed.Replication.Tests;

/// <summary>
/// <see cref="SharedSecretTls"/>: the gRPC transport encrypted and mutually authenticated from one shared secret.
/// Holders of the secret talk to each other; everyone else is refused in the TLS handshake, in both directions.
/// </summary>
[TestClass]
public sealed class GrpcSharedSecretTlsTests
{
    private static byte[] Secret(byte seed)
    {
        byte[] s = new byte[32];
        Array.Fill(s, seed);
        return s;
    }

    private static X509Certificate2 SelfSigned(Func<PublicKey, X509Extension?> extension)
    {
        using ECDsa key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var request = new CertificateRequest("CN=someone", key, HashAlgorithmName.SHA256);
        if (extension(request.PublicKey) is { } e)
        {
            request.CertificateExtensions.Add(e);
        }
        return request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
    }

    [TestMethod]
    public void OnlyACertificateMadeWithTheSecretIsTrusted()
    {
        using var a = new SharedSecretTls(Secret(1));
        using var sameSecret = new SharedSecretTls(Secret(1));
        using var otherSecret = new SharedSecretTls(Secret(2));

        Assert.IsTrue(a.IsTrustedPeer(a.Certificate));
        Assert.IsTrue(a.IsTrustedPeer(sameSecret.Certificate), "another process with the same secret");
        Assert.AreNotEqual(a.Certificate.Thumbprint, sameSecret.Certificate.Thumbprint, "every instance has its own key pair");
        Assert.IsFalse(a.IsTrustedPeer(otherSecret.Certificate), "another secret");
        Assert.IsFalse(a.IsTrustedPeer(null), "no certificate");

        using X509Certificate2 plain = SelfSigned(_ => null);
        Assert.IsFalse(a.IsTrustedPeer(plain), "a certificate without the proof");

        // The proof of a trusted certificate copied into a certificate with another key: it covers the public key.
        X509Extension proof = sameSecret.Certificate.Extensions[SharedSecretTls.ProofExtensionOid]!;
        using X509Certificate2 copied = SelfSigned(_ => new X509Extension(proof.Oid!, proof.RawData, critical: false));
        Assert.IsFalse(a.IsTrustedPeer(copied), "a copied proof on another key");

        using X509Certificate2 garbage = SelfSigned(_ => new X509Extension(SharedSecretTls.ProofExtensionOid, [1, 2, 3], critical: false));
        Assert.IsFalse(a.IsTrustedPeer(garbage), "a malformed proof");

        // The public certificate alone (what an observer of a handshake sees) passes the check; the handshake itself
        // is what refuses a peer without the private key (see the gRPC test below).
        using X509Certificate2 publicOnly = X509CertificateLoader.LoadCertificate(sameSecret.Certificate.RawData);
        Assert.IsFalse(publicOnly.HasPrivateKey);

        // The two places the check is installed. The gRPC test below cannot tell them apart for a wrong secret (each
        // side refuses the other), so they are called directly here.
        Grpc.Net.Client.GrpcChannelOptions channel = a.CreateChannelOptions();
        using var handler = (System.Net.Http.SocketsHttpHandler)channel.HttpHandler!;
        System.Net.Security.RemoteCertificateValidationCallback client = handler.SslOptions.RemoteCertificateValidationCallback!;
        Assert.IsTrue(client(this, sameSecret.Certificate, null, System.Net.Security.SslPolicyErrors.RemoteCertificateChainErrors), "chain errors of a self-signed certificate do not matter");
        Assert.IsFalse(client(this, otherSecret.Certificate, null, System.Net.Security.SslPolicyErrors.None), "a server with another secret");
        Assert.IsFalse(client(this, null, null, System.Net.Security.SslPolicyErrors.RemoteCertificateNotAvailable));
        Assert.AreEqual(System.Net.Http.HttpKeepAlivePingPolicy.Always, handler.KeepAlivePingPolicy, "the keep-alive settings of every channel");

        var https = new Microsoft.AspNetCore.Server.Kestrel.Https.HttpsConnectionAdapterOptions();
        a.ConfigureServer(https);
        Assert.AreSame(a.Certificate, https.ServerCertificate);
        Assert.AreEqual(Microsoft.AspNetCore.Server.Kestrel.Https.ClientCertificateMode.RequireCertificate, https.ClientCertificateMode);
        Assert.IsTrue(https.ClientCertificateValidation!(sameSecret.Certificate, null, System.Net.Security.SslPolicyErrors.RemoteCertificateChainErrors));
        Assert.IsFalse(https.ClientCertificateValidation!(otherSecret.Certificate, null, System.Net.Security.SslPolicyErrors.None), "a client with another secret");

        ArgumentException tooShort = Assert.ThrowsExactly<ArgumentException>(() => new SharedSecretTls(new byte[SharedSecretTls.MinSecretBytes - 1]));
        Assert.AreEqual("secret", tooShort.ParamName);
    }

    [TestMethod]
    public async Task ReplicationOverTlsWithTheSharedSecret()
    {
        using var dir = new TempDir("grpc-tls-repl");
        // Declared first, disposed last: after the server and the channel.
        using var primaryTls = new SharedSecretTls(Secret(7));
        using var replicaTls = new SharedSecretTls(Secret(7));
        DbOptions o = DbOpts(null);
        o.WalTtlSeconds = 600;
        await using RocksDatabase source = await RocksDatabase.OpenAsync(dir.Sub("primary"), o);
        await WriteWorkloadAsync(source, 0, 500, 0);
        var primary = new ReplicationPrimary(source, dir.Sub("checkpoints"), new AdaptiveCommitDelayController(1)) { MaxChunkBytes = 16 * 1024 };
        await using GrpcNodeServer server = await GrpcNodeServer.StartAsync<ReplicationService, ReplicationPrimary>(primary, System.Net.IPAddress.Loopback, 0,
            new GrpcNodeServerOptions { Tls = primaryTls }, CancellationToken.None);
        StringAssert.StartsWith(server.Endpoint, "https://");

        using var client = new GrpcReplicationClient(server.Endpoint, TimeSpan.FromSeconds(10), replicaTls.CreateChannelOptions());
        SnapshotRestoreResult r = await ReplicaFollower.SyncInitialStateAsync(client, LocalFileSystem.Instance, dir.Sub("replica"), [], CancellationToken.None);
        Assert.IsTrue(r.TransferredFiles > 0);
        await using RocksDatabase replica = await RocksDatabase.OpenAsync(dir.Sub("replica"), DbOpts(null));
        AssertReplicaEquals(source, replica, "after the initial state");

        using var cts = new CancellationTokenSource();
        Task<long> follow = ReplicaFollower.FollowAsync(client, replica, 0, cts.Token);
        await WriteWorkloadAsync(source, 500, 1500, 1);
        await WaitForSequenceAsync(replica, source.GetLatestSequenceNumber(), "gRPC stream over TLS");
        AssertReplicaEquals(source, replica, "after streaming over TLS");
        await WaitUntilAsync(() => primary.Source.SlowestReplicaSequence == source.GetLatestSequenceNumber(), "progress reports");
        await cts.CancelAsync();
        try
        {
            await follow;
        }
        catch (OperationCanceledException)
        {
        }
        catch (Grpc.Core.RpcException e) when (e.StatusCode == Grpc.Core.StatusCode.Cancelled)
        {
        }
    }

    /// <summary>
    /// A switchover pair whose two nodes and whose writing client all use the secret: status calls, the WAL stream
    /// between the nodes and a client write above the chunk size (the client-streaming upload).
    /// </summary>
    [TestMethod]
    public async Task SwitchoverPairOverTlsWithTheSharedSecret()
    {
        using var dir = new TempDir("grpc-tls-pair");
        using var tlsA = new SharedSecretTls(Secret(9));
        using var tlsB = new SharedSecretTls(Secret(9));
        using var tlsClient = new SharedSecretTls(Secret(9));
        int portA = GrpcNodeServer.FindFreePort(), portB = GrpcNodeServer.FindFreePort();
        var toB = new GrpcPairClient("B", "https://127.0.0.1:" + portB, TimeSpan.FromSeconds(2), tlsA.CreateChannelOptions());
        var toA = new GrpcPairClient("A", "https://127.0.0.1:" + portA, TimeSpan.FromSeconds(2), tlsB.CreateChannelOptions());
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
        await using GrpcNodeServer serverA = await GrpcNodeServer.StartAsync<PairService, PairNodeHost>(a, System.Net.IPAddress.Loopback, portA,
            new GrpcNodeServerOptions { Tls = tlsA }, CancellationToken.None);
        await using GrpcNodeServer serverB = await GrpcNodeServer.StartAsync<PairService, PairNodeHost>(b, System.Net.IPAddress.Loopback, portB,
            new GrpcNodeServerOptions { Tls = tlsB }, CancellationToken.None);
        await a.StartAsync(CancellationToken.None);
        await b.StartAsync(CancellationToken.None);

        using (var writer = new GrpcPairClient("client", serverA.Endpoint, TimeSpan.FromSeconds(5), tlsClient.CreateChannelOptions()))
        {
            Assert.IsTrue((await writer.WriteAsync("k", "v", CancellationToken.None)).Applied);
            Assert.IsTrue((await writer.WriteAsync("big", new string('x', 1024 * 1024), CancellationToken.None)).Applied, "an upload in pieces");
        }
        await WaitUntilAsync(() => a.Db.GetLatestSequenceNumber() == b.Db.GetLatestSequenceNumber(), "B to receive the writes over TLS");
        Assert.IsTrue(b.Db.GetLatestSequenceNumber() >= 2);
        Assert.AreEqual((await toA.Service.ChecksumAsync()).ToDigest(), (await toB.Service.ChecksumAsync()).ToDigest());
        toA.Dispose();
        toB.Dispose();
    }

    [TestMethod]
    public async Task CallersWithoutTheSecretAreRefused()
    {
        using var dir = new TempDir("grpc-tls-refuse");
        using var serverTls = new SharedSecretTls(Secret(3));
        using var goodTls = new SharedSecretTls(Secret(3));
        using var wrongTls = new SharedSecretTls(Secret(4));
        await using RocksDatabase source = await RocksDatabase.OpenAsync(dir.Sub("primary"), DbOpts(null));
        var primary = new ReplicationPrimary(source, dir.Sub("checkpoints"), new AdaptiveCommitDelayController(1));
        await using GrpcNodeServer server = await GrpcNodeServer.StartAsync<ReplicationService, ReplicationPrimary>(primary, System.Net.IPAddress.Loopback, 0,
            new GrpcNodeServerOptions { Tls = serverTls }, CancellationToken.None);

        async Task<Grpc.Core.StatusCode?> ProbeAsync(string endpoint, GrpcChannelOptions options)
        {
            using var client = new GrpcReplicationClient(endpoint, TimeSpan.FromSeconds(5), options);
            try
            {
                await client.ReportProgressAsync(0, 0, CancellationToken.None);
                return null;
            }
            catch (Grpc.Core.RpcException e)
            {
                return e.StatusCode;
            }
        }

        // A failed handshake surfaces as Unavailable or Internal, depending on which side ended it; never as a reply.
        static void AssertRefused(Grpc.Core.StatusCode? code) =>
            Assert.IsTrue(code is Grpc.Core.StatusCode.Unavailable or Grpc.Core.StatusCode.Internal, "the call must fail in the transport, got " + (code?.ToString() ?? "a reply"));

        Assert.IsNull(await ProbeAsync(server.Endpoint, goodTls.CreateChannelOptions()), "the same secret is accepted");

        // Another secret: the client refuses the server's certificate (and the server would refuse the client's).
        AssertRefused(await ProbeAsync(server.Endpoint, wrongTls.CreateChannelOptions()));

        // A client that accepts any server and presents no certificate: the server ends the handshake.
        var noCertificate = new System.Net.Http.SocketsHttpHandler();
        noCertificate.SslOptions.RemoteCertificateValidationCallback = static (_, _, _, _) => true;
        AssertRefused(await ProbeAsync(server.Endpoint, new GrpcChannelOptions { HttpHandler = noCertificate, DisposeHttpClient = true }));

        // A client that accepts any server and presents a certificate made with another secret: the server refuses it.
        // This isolates the server's check from the client's.
        var wrongCertificate = new System.Net.Http.SocketsHttpHandler();
        wrongTls.ConfigureClient(wrongCertificate);
        wrongCertificate.SslOptions.RemoteCertificateValidationCallback = static (_, _, _, _) => true;
        AssertRefused(await ProbeAsync(server.Endpoint, new GrpcChannelOptions { HttpHandler = wrongCertificate, DisposeHttpClient = true }));

        // A client that presents the public certificate of a trusted node without its private key (what an observer
        // of a handshake could copy) cannot complete the handshake as that node.
        var stolen = new System.Net.Http.SocketsHttpHandler();
        stolen.SslOptions.RemoteCertificateValidationCallback = static (_, _, _, _) => true;
        stolen.SslOptions.ClientCertificates = [X509CertificateLoader.LoadCertificate(goodTls.Certificate.RawData)];
        AssertRefused(await ProbeAsync(server.Endpoint, new GrpcChannelOptions { HttpHandler = stolen, DisposeHttpClient = true }));

        // Plaintext to the TLS port.
        AssertRefused(await ProbeAsync(server.Endpoint.Replace("https://", "http://", StringComparison.Ordinal), new GrpcChannelOptions()));

        // And a client with the secret refuses a server without it (a plain TLS server is not needed to show it: a
        // server with another secret is such a server).
        await using RocksDatabase other = await RocksDatabase.OpenAsync(dir.Sub("other"), DbOpts(null));
        var otherPrimary = new ReplicationPrimary(other, dir.Sub("other-checkpoints"), new AdaptiveCommitDelayController(1));
        await using GrpcNodeServer impostor = await GrpcNodeServer.StartAsync<ReplicationService, ReplicationPrimary>(otherPrimary, System.Net.IPAddress.Loopback, 0,
            new GrpcNodeServerOptions { Tls = wrongTls }, CancellationToken.None);
        AssertRefused(await ProbeAsync(impostor.Endpoint, goodTls.CreateChannelOptions()));
    }
}
