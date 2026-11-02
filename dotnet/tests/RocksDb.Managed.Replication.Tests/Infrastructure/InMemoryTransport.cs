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

    /// <summary>Partitions <paramref name="node"/> from every node in <paramref name="others"/>.</summary>
    public void Isolate(string node, IEnumerable<string> others)
    {
        foreach (string o in others)
        {
            if (o != node) SetPartitioned(node, o, true);
        }
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

    private ClusterNodeHost? Target => network.Resolve<ClusterNodeHost>(from, to);

    public Task<RequestVoteResult?> RequestVoteAsync(RequestVotePayload request, CancellationToken cancellationToken) =>
        Task.FromResult(Target?.HandleRequestVote(request));

    public Task<AppendEntriesResult?> HeartbeatAsync(HeartbeatPayload payload, CancellationToken cancellationToken) =>
        Task.FromResult(Target?.HandleHeartbeat(payload));

    public Task<ClusterNodeStatus?> GetStatusAsync(CancellationToken cancellationToken) => Task.FromResult(Target?.GetStatus());

    public Task<ClusterAttachResult?> CheckLogConsistencyAsync(string followerId, ulong latestSeq, long termAtLatest, CancellationToken cancellationToken) =>
        Task.FromResult(Target?.HandleCheckLogConsistency(latestSeq, termAtLatest));

    public IAsyncEnumerable<WalStreamMessage> StreamUpdatesAsync(string followerId, ulong startSeq, CancellationToken cancellationToken) =>
        InMemoryNetwork.Pump(ct => network.Require<ClusterNodeHost>(from, to).StreamUpdatesAsync(followerId, startSeq, ct), static m => m.Detach(), network.Lifetime(to), network.Link(from, to), cancellationToken);

    public Task ReportProgressAsync(string followerId, ulong appliedThrough, CancellationToken cancellationToken)
    {
        Target?.HandleReportProgress(followerId, appliedThrough);
        return Task.CompletedTask;
    }

    public Task<IReadOnlyDictionary<string, string>> GetFileHashesAsync(IReadOnlyList<ReplicationFileInfo> candidates, CancellationToken cancellationToken) =>
        network.Require<ClusterNodeHost>(from, to).HandleGetFileHashesAsync(candidates, cancellationToken);

    public IAsyncEnumerable<SnapshotChunk> StreamSnapshotAsync(IReadOnlyList<ReplicationFileInfo> verifiedInventory, CancellationToken cancellationToken) =>
        InMemoryNetwork.Pump(ct => network.Require<ClusterNodeHost>(from, to).StreamSnapshotAsync(verifiedInventory, ct), static c => c.Detach(), network.Lifetime(to), network.Link(from, to), cancellationToken);

    public Task<string?> GetTermsAsync(CancellationToken cancellationToken) => Task.FromResult(Target?.HandleGetTerms());
}

/// <summary><see cref="IPairNodeClient"/> over an <see cref="InMemoryNetwork"/>, with the fault injection of rocksdb-sharp's SwitchoverTest.</summary>
public sealed class InMemoryPairClient(InMemoryNetwork network, string from, string to) : IPairNodeClient
{
    /// <summary>Drop the answer of the next Promote (the target still promotes).</summary>
    public volatile bool DropNextPromoteReply;

    /// <summary>Delay the next Promote request before it reaches the target.</summary>
    public TimeSpan DelayNextPromote;

    public string NodeId => to;

    private PairNodeHost? Target => network.Resolve<PairNodeHost>(from, to);

    public Task<ReplicationNodeStatus?> GetStatusAsync(CancellationToken cancellationToken) => Task.FromResult(Target?.HandleGetStatus());

    public async Task<PromoteResult?> PromoteAsync(PromoteRequest request, CancellationToken cancellationToken)
    {
        TimeSpan delay = DelayNextPromote;
        DelayNextPromote = TimeSpan.Zero;
        if (delay > TimeSpan.Zero)
        {
            // The request is "on the wire": it lands after the caller gave up waiting.
            _ = Task.Run(async () =>
            {
                await Task.Delay(delay);
                PairNodeHost? late = Target;
                if (late != null) LateResult = await late.HandlePromoteAsync(request);
            });
            await Task.Delay(System.Threading.Timeout.Infinite, cancellationToken);
        }
        PairNodeHost t = network.Require<PairNodeHost>(from, to);
        PromoteResult r = await t.HandlePromoteAsync(request);
        if (DropNextPromoteReply)
        {
            DropNextPromoteReply = false;
            throw new IOException("promote reply lost");
        }
        return r;
    }

    /// <summary>The result of a delayed promote once it landed.</summary>
    public volatile PromoteResult? LateResult;

    public async Task<ReplicationNodeStatus?> RejectEpochAsync(long epoch, CancellationToken cancellationToken)
    {
        PairNodeHost? t = Target;
        return t == null ? null : await t.HandleRejectEpochAsync(epoch);
    }

    public Task<ReplicaLogCheck?> CheckReplicaLogAsync(ulong replicaLatestSeq, long replicaEpochAtLatest, CancellationToken cancellationToken) =>
        Task.FromResult(Target?.HandleCheckReplicaLog(replicaLatestSeq, replicaEpochAtLatest));

    public IAsyncEnumerable<WalStreamMessage> StreamUpdatesAsync(ulong startSeq, CancellationToken cancellationToken) =>
        InMemoryNetwork.Pump(ct => network.Require<PairNodeHost>(from, to).StreamUpdatesAsync(startSeq, ct), static m => m.Detach(), network.Lifetime(to), network.Link(from, to), cancellationToken);

    public Task<string?> GetEpochsAsync(CancellationToken cancellationToken) => Task.FromResult(Target?.HandleGetEpochs());

    public IAsyncEnumerable<SnapshotChunk> StreamSnapshotAsync(CancellationToken cancellationToken) =>
        InMemoryNetwork.Pump(ct => network.Require<PairNodeHost>(from, to).StreamSnapshotAsync(ct), static c => c.Detach(), network.Lifetime(to), network.Link(from, to), cancellationToken);
}
