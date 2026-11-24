using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using RocksDb.Managed.Cluster;
using RocksDb.Managed.Replication.Hosting;
using RocksDb.Managed.Replication.Switchover;

namespace RocksDb.Managed.Replication.Tests.Infrastructure;

/// <summary>
/// An in-memory "network" between in-process nodes. Unary calls go straight to the target host's handler (or fail when
/// the target is down / partitioned away); streams are pumped through a bounded <see cref="Channel{T}"/> by a producer
/// task, with message payloads copied - like serialization over a real connection - so the consumer never sees the
/// producer's reusable buffers and the two sides run decoupled.
/// Fault hooks: <see cref="SetPartitioned"/> / <see cref="Isolate"/> cut links (unary calls fail and the streams already
/// open across a cut link are torn down, like a dropped TCP connection), <see cref="Heal"/> restores every link.
/// </summary>
public sealed class InMemoryNetwork
{
    private readonly ConcurrentDictionary<string, object> _hosts = new(StringComparer.Ordinal);
    // Cancelled when the host is unregistered: like a server shutting down, it tears down the streams it serves even
    // when their consumer is not reading.
    private readonly ConcurrentDictionary<string, CancellationTokenSource> _lifetimes = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<(string, string), bool> _cut = new();
    // Per directed link: cancelled when the link is cut, so streams across it fail.
    private readonly ConcurrentDictionary<(string, string), CancellationTokenSource> _links = new();
    private readonly Lock _linkLock = new();

    public void Register(string nodeId, object host)
    {
        _lifetimes[nodeId] = new CancellationTokenSource();
        _hosts[nodeId] = host;
    }

    public void Unregister(string nodeId)
    {
        _hosts.TryRemove(nodeId, out _);
        if (_lifetimes.TryRemove(nodeId, out CancellationTokenSource? cts)) cts.Cancel();
    }

    public CancellationToken Lifetime(string nodeId) =>
        _lifetimes.TryGetValue(nodeId, out CancellationTokenSource? cts) ? cts.Token : new CancellationToken(canceled: true);

    /// <summary>Cuts (or heals) the link between two nodes in both directions; cutting also breaks the open streams.</summary>
    public void SetPartitioned(string a, string b, bool partitioned)
    {
        lock (_linkLock)
        {
            if (partitioned)
            {
                _cut[(a, b)] = true;
                _cut[(b, a)] = true;
                if (_links.TryRemove((a, b), out CancellationTokenSource? ab)) ab.Cancel();
                if (_links.TryRemove((b, a), out CancellationTokenSource? ba)) ba.Cancel();
            }
            else
            {
                _cut.TryRemove((a, b), out _);
                _cut.TryRemove((b, a), out _);
            }
        }
    }

    /// <summary>
    /// Cuts only the directed link <paramref name="from"/> -> <paramref name="to"/>: calls made by <paramref name="from"/>
    /// fail (and its streams from <paramref name="to"/> break), calls the other way still work.
    /// </summary>
    public void CutLink(string from, string to)
    {
        lock (_linkLock)
        {
            _cut[(from, to)] = true;
            if (_links.TryRemove((from, to), out CancellationTokenSource? l)) l.Cancel();
        }
    }

    /// <summary>Partitions <paramref name="node"/> from every node in <paramref name="others"/>.</summary>
    public void Isolate(string node, IEnumerable<string> others)
    {
        foreach (string o in others)
        {
            if (o != node) SetPartitioned(node, o, true);
        }
    }

    /// <summary>Heals only the directed link <paramref name="from"/> -> <paramref name="to"/>.</summary>
    public void HealLink(string from, string to)
    {
        lock (_linkLock) _cut.TryRemove((from, to), out _);
    }

    /// <summary>Heals every cut link.</summary>
    public void Heal()
    {
        lock (_linkLock)
        {
            _cut.Clear();
        }
    }

    /// <summary>
    /// Token of the directed link <paramref name="from"/> -> <paramref name="to"/>: cancelled when the link is cut (already
    /// cancelled while it is cut).
    /// </summary>
    public CancellationToken Link(string from, string to)
    {
        lock (_linkLock)
        {
            if (_cut.ContainsKey((from, to))) return new CancellationToken(canceled: true);
            return _links.GetOrAdd((from, to), static _ => new CancellationTokenSource()).Token;
        }
    }

    public T? Resolve<T>(string from, string to) where T : class =>
        !_cut.ContainsKey((from, to)) && _hosts.TryGetValue(to, out object? h) ? h as T : null;

    public T Require<T>(string from, string to) where T : class =>
        Resolve<T>(from, to) ?? throw new IOException("node " + to + " is unreachable from " + from);

    /// <summary>Runs <paramref name="source"/> on a producer task and hands its (copied) items to the consumer.</summary>
    public static IAsyncEnumerable<T> Pump<T>(Func<CancellationToken, IAsyncEnumerable<T>> source, Func<T, T> copy,
        CancellationToken serverLifetime, CancellationToken cancellationToken) =>
        Pump(source, copy, serverLifetime, CancellationToken.None, cancellationToken);

    /// <summary>
    /// <see cref="Pump{T}(Func{CancellationToken, IAsyncEnumerable{T}}, Func{T, T}, CancellationToken, CancellationToken)"/>
    /// over a link (<see cref="Link"/>): cutting the link fails the stream like a closed server does.
    /// </summary>
    public static async IAsyncEnumerable<T> Pump<T>(Func<CancellationToken, IAsyncEnumerable<T>> source, Func<T, T> copy,
        CancellationToken serverLifetime, CancellationToken link, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var channel = Channel.CreateBounded<T>(new BoundedChannelOptions(16) { SingleReader = true, SingleWriter = true });
        using var connection = CancellationTokenSource.CreateLinkedTokenSource(serverLifetime, link);
        serverLifetime = connection.Token;
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, serverLifetime);
        Task producer = Task.Run(async () =>
        {
            try
            {
                await foreach (T item in source(cts.Token).WithCancellation(cts.Token))
                {
                    await channel.Writer.WriteAsync(copy(item), cts.Token);
                }
                channel.Writer.TryComplete();
            }
            catch (Exception e)
            {
                // A stream torn down by the server (or failing) is an error for the consumer, never a clean end.
                channel.Writer.TryComplete(e is OperationCanceledException && !serverLifetime.IsCancellationRequested ? null
                    : e is OperationCanceledException ? new IOException("connection closed by the server") : e);
            }
        });
        try
        {
            await foreach (T item in channel.Reader.ReadAllAsync(cancellationToken)) yield return item;
        }
        finally
        {
            await cts.CancelAsync();
            try
            {
                await producer;
            }
            catch (Exception)
            {
            }
        }
    }
}

/// <summary><see cref="IClusterNodeClient"/> over an <see cref="InMemoryNetwork"/>.</summary>
public sealed class InMemoryClusterClient(InMemoryNetwork network, string from, string to) : IClusterNodeClient
{
    public string NodeId => to;

    /// <summary>
    /// Who this client is to the target, as a real transport would have authenticated it: the node it calls from. The
    /// target's <see cref="ReplicationAuthorizer"/> is asked before every call, like the gRPC services do (a refusal
    /// throws <see cref="ReplicationUnauthorizedException"/>). Tests set another identity to play an impostor.
    /// </summary>
    public ReplicationCaller Caller { get; set; } = ReplicationCaller.Node(from);

    private ClusterNodeHost? Target => network.Resolve<ClusterNodeHost>(from, to);

    /// <summary>The target if it is reachable, after it accepted this caller for a node-to-node call.</summary>
    private ClusterNodeHost? Peer(string? claimedNodeId, string operation)
    {
        ClusterNodeHost? t = Target;
        t?.Authorization.RequirePeer(Caller, claimedNodeId, operation);
        return t;
    }

    private ClusterNodeHost RequirePeer(string? claimedNodeId, string operation)
    {
        ClusterNodeHost t = network.Require<ClusterNodeHost>(from, to);
        t.Authorization.RequirePeer(Caller, claimedNodeId, operation);
        return t;
    }

    public async Task<RequestVoteResult?> RequestVoteAsync(RequestVotePayload request, CancellationToken cancellationToken) =>
        Peer(request.CandidateId, nameof(RequestVoteAsync)) is { } t ? await t.HandleRequestVoteAsync(request, cancellationToken).ConfigureAwait(false) : null;

    public async Task<AppendEntriesResult?> HeartbeatAsync(HeartbeatPayload payload, CancellationToken cancellationToken) =>
        Peer(payload.LeaderId, nameof(HeartbeatAsync)) is { } t ? await t.HandleHeartbeatAsync(payload, cancellationToken).ConfigureAwait(false) : null;

    public Task<ClusterNodeStatus?> GetStatusAsync(CancellationToken cancellationToken)
    {
        ClusterNodeHost? t = Target;
        t?.Authorization.RequireAuthenticated(Caller, nameof(GetStatusAsync));
        return Task.FromResult(t?.GetStatus());
    }

    public async Task<ClusterAttachResult?> CheckLogConsistencyAsync(string followerId, ulong latestSeq, long termAtLatest, CancellationToken cancellationToken) =>
        Peer(followerId, nameof(CheckLogConsistencyAsync)) is { } t
            ? await t.HandleCheckLogConsistencyAsync(latestSeq, termAtLatest, cancellationToken).ConfigureAwait(false)
            : null;

    public IAsyncEnumerable<WalStreamMessage> StreamUpdatesAsync(string followerId, ulong startSeq, CancellationToken cancellationToken) =>
        InMemoryNetwork.Pump(ct => RequirePeer(followerId, nameof(StreamUpdatesAsync)).StreamUpdatesAsync(followerId, startSeq, ct), static m => m.Detach(), network.Lifetime(to), network.Link(from, to), cancellationToken);

    public async Task ReportProgressAsync(string followerId, ulong appliedThrough, long leaderTerm, CancellationToken cancellationToken)
    {
        if (Peer(followerId, nameof(ReportProgressAsync)) is { } t)
            await t.HandleReportProgressAsync(followerId, appliedThrough, leaderTerm, cancellationToken).ConfigureAwait(false);
    }

    public Task<IReadOnlyDictionary<string, string>> GetFileHashesAsync(IReadOnlyList<ReplicationFileInfo> candidates, CancellationToken cancellationToken) =>
        RequirePeer(null, nameof(GetFileHashesAsync)).HandleGetFileHashesAsync(candidates, cancellationToken);

    public IAsyncEnumerable<SnapshotChunk> StreamSnapshotAsync(IReadOnlyList<ReplicationFileInfo> verifiedInventory, CancellationToken cancellationToken) =>
        InMemoryNetwork.Pump(ct => RequirePeer(null, nameof(StreamSnapshotAsync)).StreamSnapshotAsync(verifiedInventory, ct), static c => c.Detach(), network.Lifetime(to), network.Link(from, to), cancellationToken);
}

/// <summary><see cref="IPairNodeClient"/> over an <see cref="InMemoryNetwork"/>, with the fault injection of rocksdb-sharp's SwitchoverTest.</summary>
public sealed class InMemoryPairClient(InMemoryNetwork network, string from, string to) : IPairNodeClient
{
    /// <summary>Drop the answer of the next Promote (the target still promotes).</summary>
    public volatile bool DropNextPromoteReply;

    /// <summary>Delay the next Promote request before it reaches the target.</summary>
    public TimeSpan DelayNextPromote;

    public string NodeId => to;

    /// <summary>
    /// Who this client is to the target (see <see cref="InMemoryClusterClient.Caller"/>): the node it calls from. The
    /// target's <see cref="ReplicationAuthorizer"/> is asked before every call.
    /// </summary>
    public ReplicationCaller Caller { get; set; } = ReplicationCaller.Node(from);

    private PairNodeHost? Target => network.Resolve<PairNodeHost>(from, to);

    private PairNodeHost? Peer(string? claimedNodeId, string operation)
    {
        PairNodeHost? t = Target;
        t?.Authorization.RequirePeer(Caller, claimedNodeId, operation);
        return t;
    }

    private PairNodeHost RequirePeer(string? claimedNodeId, string operation)
    {
        PairNodeHost t = network.Require<PairNodeHost>(from, to);
        t.Authorization.RequirePeer(Caller, claimedNodeId, operation);
        return t;
    }

    public Task<ReplicationNodeStatus?> GetStatusAsync(CancellationToken cancellationToken)
    {
        PairNodeHost? t = Target;
        t?.Authorization.RequireAuthenticated(Caller, nameof(GetStatusAsync));
        return Task.FromResult(t?.HandleGetStatus());
    }

    public async Task<PromoteResult?> PromoteAsync(PromoteRequest request, CancellationToken cancellationToken)
    {
        // Faults apply to real promote requests, not to the warm-up probe sent before writes are held.
        if (request.IsProbe) return await RequirePeer(request.FromPrimaryId, nameof(PromoteAsync)).HandlePromoteAsync(request);
        LastPromote = request;
        TimeSpan delay = DelayNextPromote;
        DelayNextPromote = TimeSpan.Zero;
        if (delay > TimeSpan.Zero)
        {
            // The request is "on the wire": it lands after the caller gave up waiting.
            _ = Task.Run(async () =>
            {
                await Task.Delay(delay);
                PairNodeHost? late = Peer(request.FromPrimaryId, nameof(PromoteAsync));
                if (late != null) LateResult = await late.HandlePromoteAsync(request);
            });
            await Task.Delay(System.Threading.Timeout.Infinite, cancellationToken);
        }
        PairNodeHost t = RequirePeer(request.FromPrimaryId, nameof(PromoteAsync));
        PromoteResult r = await t.HandlePromoteAsync(request);
        if (DropNextPromoteReply)
        {
            DropNextPromoteReply = false;
            throw new IOException("promote reply lost");
        }
        return r;
    }

    /// <summary>The latest real (non-probe) promote request sent through this client.</summary>
    public volatile PromoteRequest? LastPromote;

    /// <summary>The result of a delayed promote once it landed.</summary>
    public volatile PromoteResult? LateResult;

    public async Task<ReplicationNodeStatus?> RejectEpochAsync(long epoch, CancellationToken cancellationToken)
    {
        PairNodeHost? t = Peer(null, nameof(RejectEpochAsync));
        return t == null ? null : await t.HandleRejectEpochAsync(epoch);
    }

    public Task<ReplicaLogCheck?> CheckReplicaLogAsync(ulong replicaLatestSeq, long replicaEpochAtLatest, CancellationToken cancellationToken) =>
        Task.FromResult(Peer(null, nameof(CheckReplicaLogAsync))?.HandleCheckReplicaLog(replicaLatestSeq, replicaEpochAtLatest));

    /// <summary>Rewrites (or passes through) every message of the WAL streams opened from now on; null for none.</summary>
    public Func<WalStreamMessage, WalStreamMessage>? RewriteStream;

    /// <summary>Called with every message of a WAL stream as the replica receives it.</summary>
    public Action<WalStreamMessage>? OnStreamMessage;

    public async IAsyncEnumerable<WalStreamMessage> StreamUpdatesAsync(ulong startSeq, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        Func<WalStreamMessage, WalStreamMessage>? rewrite = RewriteStream;
        await foreach (WalStreamMessage m in InMemoryNetwork.Pump(ct => RequirePeer(null, nameof(StreamUpdatesAsync)).StreamUpdatesAsync(startSeq, ct),
                           static m => m.Detach(), network.Lifetime(to), network.Link(from, to), cancellationToken))
        {
            WalStreamMessage r = rewrite is null ? m : rewrite(m);
            OnStreamMessage?.Invoke(r);
            yield return r;
        }
    }

    public Task<string?> GetEpochsAsync(CancellationToken cancellationToken) => Task.FromResult(Peer(null, nameof(GetEpochsAsync))?.HandleGetEpochs());

    /// <summary>Drop the replica's progress reports (they fail, as on a broken connection).</summary>
    public volatile bool DropProgressReports;

    public Task ReportProgressAsync(ulong appliedThrough, long epochAtApplied, CancellationToken cancellationToken)
    {
        if (DropProgressReports) throw new IOException("progress report lost");
        RequirePeer(null, nameof(ReportProgressAsync)).HandleReportProgress(appliedThrough, epochAtApplied);
        return Task.CompletedTask;
    }

    public IAsyncEnumerable<SnapshotChunk> StreamSnapshotAsync(CancellationToken cancellationToken) =>
        InMemoryNetwork.Pump(ct => RequirePeer(null, nameof(StreamSnapshotAsync)).StreamSnapshotAsync(ct), static c => c.Detach(), network.Lifetime(to), network.Link(from, to), cancellationToken);
}
