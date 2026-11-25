using System.Runtime.CompilerServices;
using Grpc.Net.Client;
using MagicOnion;
using MagicOnion.Client;
using MagicOnion.Server;
using RocksDb.Managed.Replication.Hosting;

namespace RocksDb.Managed.Replication.MagicOnion;

/// <summary>gRPC contract of a plain replication primary (port of rocksdb-sharp's Tests/ReplicationTest/IReplicationService).</summary>
public interface IReplicationService : IService<IReplicationService>
{
    Task<ServerStreamingResult<SnapshotChunkDto>> SyncInitialStateAsync();
    Task<ServerStreamingResult<WalChunkDto>> SyncUpdatesAsync(ulong startSeq);
    UnaryResult<bool> ReportLastSyncSequenceNumber(int replicaIndex, ulong seqNumber);
    /// <summary>The attach check (<see cref="ReplicationPrimary.CheckReplicaLog"/>). Not implemented by servers from before it existed.</summary>
    UnaryResult<ReplicaAttachCheckDto> CheckReplicaLog(ulong latestSeq, long incarnationAtLatest);
}

/// <summary>
/// Server side of <see cref="IReplicationService"/>, over the process's <see cref="ReplicationPrimary"/>. Every endpoint
/// is for the primary's replicas (<see cref="GrpcNodeServerOptions.ReplicaNodeIds"/>): only they may read its streams
/// and ask for the attach check, and each reports its progress under its own replica index. A refused call fails with
/// PermissionDenied.
/// </summary>
public sealed class ReplicationService(ReplicationPrimary primary, GrpcCallerResolver callers, ReplicationAuthorizer replicas)
    : ServiceBase<IReplicationService>, IReplicationService
{
    private void Replica(string operation) => GrpcAuthorization.Peer(callers, Context.CallContext, replicas, null, operation);

    public async Task<ServerStreamingResult<SnapshotChunkDto>> SyncInitialStateAsync()
    {
        Replica(nameof(SyncInitialStateAsync));
        ServerStreamingContext<SnapshotChunkDto> stream = GetServerStreamingContext<SnapshotChunkDto>();
        await foreach (SnapshotChunk c in primary.StreamInitialStateAsync(Context.CallContext.CancellationToken).ConfigureAwait(false))
        {
            await StreamWrites.WriteOrAbortAsync(stream, SnapshotChunkDto.From(c), Context.CallContext, Context.CallContext.CancellationToken).ConfigureAwait(false);
        }
        return stream.Result();
    }

    public async Task<ServerStreamingResult<WalChunkDto>> SyncUpdatesAsync(ulong startSeq)
    {
        Replica(nameof(SyncUpdatesAsync));
        ServerStreamingContext<WalChunkDto> stream = GetServerStreamingContext<WalChunkDto>();
        CancellationToken ct = Context.CallContext.CancellationToken;
        try
        {
            await foreach (WalStreamMessage m in primary.StreamUpdatesAsync(startSeq, ct).ConfigureAwait(false))
            {
                await StreamWrites.WriteOrAbortAsync(stream, WalChunkDto.From(m), Context.CallContext, ct).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
        }
        return stream.Result();
    }

    public UnaryResult<bool> ReportLastSyncSequenceNumber(int replicaIndex, ulong seqNumber)
    {
        // The index comes from a peer: one that is out of range is an invalid request, not a server fault.
        if (replicaIndex < 0) throw new ReturnStatusException(Grpc.Core.StatusCode.InvalidArgument, "replica index " + replicaIndex);
        GrpcAuthorization.Replica(callers, Context.CallContext, replicas, replicaIndex, nameof(ReportLastSyncSequenceNumber));
        primary.ReportProgress(replicaIndex, seqNumber);
        return UnaryResult.FromResult(true);
    }

    public UnaryResult<ReplicaAttachCheckDto> CheckReplicaLog(ulong latestSeq, long incarnationAtLatest)
    {
        Replica(nameof(CheckReplicaLog));
        ReplicaAttachCheck r = primary.CheckReplicaLog(latestSeq, incarnationAtLatest);
        return UnaryResult.FromResult(new ReplicaAttachCheckDto { Consistent = r.Consistent, Lineage = r.Lineage, Reason = r.Reason });
    }
}

/// <summary><see cref="IReplicationSourceClient"/> over gRPC, for <see cref="ReplicaFollower"/>.</summary>
public sealed class GrpcReplicationClient : IReplicationSourceClient, IDisposable
{
    private readonly GrpcChannel _channel;
    private readonly IReplicationService _client;
    private readonly TimeSpan _unaryTimeout;

    public GrpcReplicationClient(string endpoint, GrpcSecurity security) : this(endpoint, TimeSpan.FromSeconds(10), security)
    {
    }

    /// <param name="endpoint">The primary's address.</param>
    /// <param name="unaryTimeout">Deadline of a progress report (a report that misses it is sent again).</param>
    /// <param name="security">With a shared secret the channel presents this replica's identity and accepts only a server that proves to be a node.</param>
    public GrpcReplicationClient(string endpoint, TimeSpan unaryTimeout, GrpcSecurity security) : this(endpoint, unaryTimeout, security, new GrpcChannelOptions())
    {
    }

    /// <summary>A client whose channel uses <paramref name="channelOptions"/>, completed with <see cref="GrpcTransportDefaults.Apply"/>.</summary>
    public GrpcReplicationClient(string endpoint, TimeSpan unaryTimeout, GrpcSecurity security, GrpcChannelOptions channelOptions)
    {
        ArgumentNullException.ThrowIfNull(security);
        ArgumentNullException.ThrowIfNull(channelOptions);
        _channel = GrpcChannel.ForAddress(endpoint, security.ApplyToChannel(channelOptions, endpoint, null));
        _client = MagicOnionClient.Create<IReplicationService>(_channel);
        _unaryTimeout = unaryTimeout;
    }

    public async IAsyncEnumerable<SnapshotChunk> StreamInitialStateAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using ServerStreamingResult<SnapshotChunkDto> call = await _client.WithCancellationToken(cancellationToken).SyncInitialStateAsync();
        while (await call.ResponseStream.MoveNext(cancellationToken).ConfigureAwait(false)) yield return call.ResponseStream.Current.ToChunk();
    }

    public async IAsyncEnumerable<WalStreamMessage> StreamUpdatesAsync(ulong startSeq, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using ServerStreamingResult<WalChunkDto> call = await _client.WithCancellationToken(cancellationToken).SyncUpdatesAsync(startSeq);
        while (await call.ResponseStream.MoveNext(cancellationToken).ConfigureAwait(false)) yield return call.ResponseStream.Current.ToMessage();
    }

    public async Task ReportProgressAsync(int replicaIndex, ulong appliedThrough, CancellationToken cancellationToken) =>
        await _client.WithOptions(new Grpc.Core.CallOptions(deadline: DateTime.UtcNow + _unaryTimeout, cancellationToken: cancellationToken))
            .ReportLastSyncSequenceNumber(replicaIndex, appliedThrough);

    public async Task<ReplicaAttachCheck> CheckReplicaLogAsync(ulong latestSeq, long incarnationAtLatest, CancellationToken cancellationToken)
    {
        ReplicaAttachCheckDto r = await _client
            .WithOptions(new Grpc.Core.CallOptions(deadline: DateTime.UtcNow + _unaryTimeout, cancellationToken: cancellationToken))
            .CheckReplicaLog(latestSeq, incarnationAtLatest);
        return new ReplicaAttachCheck(r.Consistent, r.Lineage ?? "", r.Reason);
    }

    public void Dispose() => _channel.Dispose();
}
