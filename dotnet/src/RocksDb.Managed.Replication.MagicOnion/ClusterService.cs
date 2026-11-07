using System.Runtime.CompilerServices;
using System.Text;
using Grpc.Core;
using Grpc.Net.Client;
using MagicOnion;
using MagicOnion.Client;
using MagicOnion.Server;
using RocksDb.Managed.Cluster;
using RocksDb.Managed.Replication.Hosting;

namespace RocksDb.Managed.Replication.MagicOnion;

/// <summary>
/// gRPC contract of a Raft cluster node (port of rocksdb-sharp's Tests/ClusterTest/IClusterService): the Raft RPCs, the
/// data plane (snapshot + WAL stream + progress) and client/test helpers.
/// </summary>
public interface IClusterService : IService<IClusterService>
{
    UnaryResult<RequestVoteResultDto> RequestVoteAsync(RequestVoteDto request);
    UnaryResult<AppendEntriesDto> HeartbeatAsync(HeartbeatDto request);
    UnaryResult<ClusterStatusDto> GetStatusAsync();
    UnaryResult<AttachDto> CheckLogConsistencyAsync(string followerId, ulong latestSeq, long termAtLatest);
    Task<ServerStreamingResult<WalChunkDto>> SyncUpdatesAsync(string followerId, ulong startSeq);
    UnaryResult<bool> ReportLastSyncSequenceNumber(string nodeId, ulong seq);
    UnaryResult<Dictionary<string, string>> GetFileHashesAsync(List<FileInfoDto> candidates);
    Task<ServerStreamingResult<SnapshotChunkDto>> SyncInitialStateAsync(List<FileInfoDto> verifiedInventory);
    UnaryResult<string> GetRaftTermsAsync();

    /// <summary>Client write, acknowledged once committed (replicated to a quorum).</summary>
    UnaryResult<bool> WriteAsync(string key, string value);
    UnaryResult<DigestDto> ChecksumAsync();
}

/// <summary>Server side of <see cref="IClusterService"/>: delegates to the process's <see cref="ClusterNodeHost"/>.</summary>
public sealed class ClusterService(ClusterNodeHost host) : ServiceBase<IClusterService>, IClusterService
{
    public UnaryResult<RequestVoteResultDto> RequestVoteAsync(RequestVoteDto request)
    {
        RequestVoteResult r = host.HandleRequestVote(new RequestVotePayload(request.Term, request.CandidateId, request.LastLogSeq, request.LastLogTerm));
        return UnaryResult.FromResult(new RequestVoteResultDto { Term = r.Term, VoteGranted = r.VoteGranted });
    }

    public UnaryResult<AppendEntriesDto> HeartbeatAsync(HeartbeatDto request)
    {
        AppendEntriesResult r = host.HandleHeartbeat(new HeartbeatPayload(request.Term, request.LeaderId, request.PrevLogSeq, request.PrevLogTerm,
            request.LeaderCommit, (ClusterMode)request.LeaderMode));
        return UnaryResult.FromResult(new AppendEntriesDto { Term = r.Term, Success = r.Success, LastSeq = r.LastSeq, Reason = r.Reason });
    }

    public UnaryResult<ClusterStatusDto> GetStatusAsync() => UnaryResult.FromResult(ClusterStatusDto.From(host.GetStatus()));

    public UnaryResult<AttachDto> CheckLogConsistencyAsync(string followerId, ulong latestSeq, long termAtLatest)
    {
        ClusterAttachResult r = host.HandleCheckLogConsistency(latestSeq, termAtLatest);
        return UnaryResult.FromResult(new AttachDto { IsLeader = r.IsLeader, Consistent = r.Consistent, Term = r.Term, Terms = r.Terms });
    }

    public async Task<ServerStreamingResult<WalChunkDto>> SyncUpdatesAsync(string followerId, ulong startSeq)
    {
        ServerStreamingContext<WalChunkDto> stream = GetServerStreamingContext<WalChunkDto>();
        CancellationToken ct = Context.CallContext.CancellationToken;
        try
        {
            await foreach (WalStreamMessage m in host.StreamUpdatesAsync(followerId, startSeq, ct).ConfigureAwait(false))
            {
                await stream.WriteAsync(WalChunkDto.From(m)).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
        }
        return stream.Result();
    }

    public UnaryResult<bool> ReportLastSyncSequenceNumber(string nodeId, ulong seq)
    {
        host.HandleReportProgress(nodeId, seq);
        return UnaryResult.FromResult(true);
    }

    public async UnaryResult<Dictionary<string, string>> GetFileHashesAsync(List<FileInfoDto> candidates)
    {
        IReadOnlyDictionary<string, string> r = await host.HandleGetFileHashesAsync(FileInfoDto.To(candidates), Context.CallContext.CancellationToken)
            .ConfigureAwait(false);
        return new Dictionary<string, string>(r, StringComparer.Ordinal);
    }

    public async Task<ServerStreamingResult<SnapshotChunkDto>> SyncInitialStateAsync(List<FileInfoDto> verifiedInventory)
    {
        ServerStreamingContext<SnapshotChunkDto> stream = GetServerStreamingContext<SnapshotChunkDto>();
        await foreach (SnapshotChunk c in host.StreamSnapshotAsync(FileInfoDto.To(verifiedInventory), Context.CallContext.CancellationToken).ConfigureAwait(false))
        {
            await stream.WriteAsync(SnapshotChunkDto.From(c)).ConfigureAwait(false);
        }
        return stream.Result();
    }

    public UnaryResult<string> GetRaftTermsAsync() => UnaryResult.FromResult(host.HandleGetTerms());

    public async UnaryResult<bool> WriteAsync(string key, string value) =>
        await host.WriteCommittedAsync(Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes(value), TimeSpan.FromSeconds(10), Context.CallContext.CancellationToken)
            .ConfigureAwait(false);

    public UnaryResult<DigestDto> ChecksumAsync() => UnaryResult.FromResult(DigestDto.From(host.Checksum()));
}

/// <summary>
/// <see cref="IClusterNodeClient"/> over gRPC (rocksdb-sharp's ClusterPeerTransport, extended to the data plane). One
/// channel per peer; unary calls carry a deadline so a hung peer cannot stall elections.
/// </summary>
public sealed class GrpcClusterClient : IClusterNodeClient, IDisposable
{
    private readonly GrpcChannel _channel;
    private readonly IClusterService _client;
    private readonly TimeSpan _unaryTimeout;

    public GrpcClusterClient(ClusterMember peer, TimeSpan unaryTimeout) : this(peer, unaryTimeout, new GrpcChannelOptions())
    {
    }

    /// <summary>
    /// A client whose channel uses <paramref name="channelOptions"/> (e.g. a short <see cref="GrpcChannelOptions.MaxReconnectBackoff"/>
    /// so a healed network partition is noticed quickly).
    /// </summary>
    public GrpcClusterClient(ClusterMember peer, TimeSpan unaryTimeout, GrpcChannelOptions channelOptions)
    {
        ArgumentNullException.ThrowIfNull(peer);
        ArgumentNullException.ThrowIfNull(channelOptions);
        NodeId = peer.NodeId;
        _channel = GrpcChannel.ForAddress(peer.Endpoint, channelOptions);
        _client = MagicOnionClient.Create<IClusterService>(_channel);
        _unaryTimeout = unaryTimeout;
    }

    public string NodeId { get; }

    /// <summary>The raw service client (for coordinators and tests).</summary>
    public IClusterService Service => _client;

    private IClusterService Unary(CancellationToken ct) =>
        _client.WithOptions(new CallOptions(deadline: DateTime.UtcNow + _unaryTimeout, cancellationToken: ct));

    public async Task<RequestVoteResult?> RequestVoteAsync(RequestVotePayload request, CancellationToken cancellationToken)
    {
        try
        {
            RequestVoteResultDto r = await Unary(cancellationToken).RequestVoteAsync(new RequestVoteDto
            {
                Term = request.Term,
                CandidateId = request.CandidateId,
                LastLogSeq = request.LastLogSeq,
                LastLogTerm = request.LastLogTerm,
            });
            return new RequestVoteResult(r.Term, r.VoteGranted);
        }
        catch (RpcException)
        {
            return null;
        }
    }

    public async Task<AppendEntriesResult?> HeartbeatAsync(HeartbeatPayload payload, CancellationToken cancellationToken)
    {
        try
        {
            AppendEntriesDto r = await Unary(cancellationToken).HeartbeatAsync(new HeartbeatDto
            {
                Term = payload.Term,
                LeaderId = payload.LeaderId,
                PrevLogSeq = payload.PrevLogSeq,
                PrevLogTerm = payload.PrevLogTerm,
                LeaderCommit = payload.LeaderCommit,
                LeaderMode = (int)payload.LeaderMode,
            });
            return new AppendEntriesResult(r.Term, r.Success, r.LastSeq, r.Reason);
        }
        catch (RpcException)
        {
            return null;
        }
    }

    public async Task<ClusterNodeStatus?> GetStatusAsync(CancellationToken cancellationToken)
    {
        try
        {
            return (await Unary(cancellationToken).GetStatusAsync()).ToStatus();
        }
        catch (RpcException)
        {
            return null;
        }
    }

    public async Task<ClusterAttachResult?> CheckLogConsistencyAsync(string followerId, ulong latestSeq, long termAtLatest, CancellationToken cancellationToken)
    {
        try
        {
            AttachDto r = await Unary(cancellationToken).CheckLogConsistencyAsync(followerId, latestSeq, termAtLatest);
            return new ClusterAttachResult(r.IsLeader, r.Consistent, r.Term, r.Terms);
        }
        catch (RpcException)
        {
            return null;
        }
    }

    public async IAsyncEnumerable<WalStreamMessage> StreamUpdatesAsync(string followerId, ulong startSeq, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using ServerStreamingResult<WalChunkDto> call = await _client.WithCancellationToken(cancellationToken).SyncUpdatesAsync(followerId, startSeq);
        while (await call.ResponseStream.MoveNext(cancellationToken).ConfigureAwait(false))
        {
            yield return call.ResponseStream.Current.ToMessage();
        }
    }

    public async Task ReportProgressAsync(string followerId, ulong appliedThrough, CancellationToken cancellationToken) =>
        await Unary(cancellationToken).ReportLastSyncSequenceNumber(followerId, appliedThrough);

    public async Task<IReadOnlyDictionary<string, string>> GetFileHashesAsync(IReadOnlyList<ReplicationFileInfo> candidates, CancellationToken cancellationToken) =>
        await _client.WithCancellationToken(cancellationToken).GetFileHashesAsync(FileInfoDto.From(candidates));

    public async IAsyncEnumerable<SnapshotChunk> StreamSnapshotAsync(IReadOnlyList<ReplicationFileInfo> verifiedInventory,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using ServerStreamingResult<SnapshotChunkDto> call = await _client.WithCancellationToken(cancellationToken).SyncInitialStateAsync(FileInfoDto.From(verifiedInventory));
        while (await call.ResponseStream.MoveNext(cancellationToken).ConfigureAwait(false))
        {
            yield return call.ResponseStream.Current.ToChunk();
        }
    }

    public async Task<string?> GetTermsAsync(CancellationToken cancellationToken)
    {
        try
        {
            return await Unary(cancellationToken).GetRaftTermsAsync();
        }
        catch (RpcException)
        {
            return null;
        }
    }

    public void Dispose() => _channel.Dispose();
}
