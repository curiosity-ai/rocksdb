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
}

/// <summary>Server side of <see cref="IReplicationService"/>, over the process's <see cref="ReplicationPrimary"/>.</summary>
public sealed class ReplicationService(ReplicationPrimary primary) : ServiceBase<IReplicationService>, IReplicationService
{
    public async Task<ServerStreamingResult<SnapshotChunkDto>> SyncInitialStateAsync()
    {
        ServerStreamingContext<SnapshotChunkDto> stream = GetServerStreamingContext<SnapshotChunkDto>();
        await foreach (SnapshotChunk c in primary.StreamInitialStateAsync(Context.CallContext.CancellationToken).ConfigureAwait(false))
        {
            await stream.WriteAsync(SnapshotChunkDto.From(c)).WaitAsync(Context.CallContext.CancellationToken).ConfigureAwait(false);
        }
        return stream.Result();
    }

    public async Task<ServerStreamingResult<WalChunkDto>> SyncUpdatesAsync(ulong startSeq)
    {
        ServerStreamingContext<WalChunkDto> stream = GetServerStreamingContext<WalChunkDto>();
        CancellationToken ct = Context.CallContext.CancellationToken;
        try
        {
            await foreach (WalStreamMessage m in primary.StreamUpdatesAsync(startSeq, ct).ConfigureAwait(false))
            {
                await stream.WriteAsync(WalChunkDto.From(m)).WaitAsync(ct).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
        }
        return stream.Result();
    }

    public UnaryResult<bool> ReportLastSyncSequenceNumber(int replicaIndex, ulong seqNumber)
    {
        primary.ReportProgress(replicaIndex, seqNumber);
        return UnaryResult.FromResult(true);
    }
}

/// <summary><see cref="IReplicationSourceClient"/> over gRPC, for <see cref="ReplicaFollower"/>.</summary>
public sealed class GrpcReplicationClient : IReplicationSourceClient, IDisposable
{
    private readonly GrpcChannel _channel;
    private readonly IReplicationService _client;
    private readonly TimeSpan _unaryTimeout;

    public GrpcReplicationClient(string endpoint) : this(endpoint, TimeSpan.FromSeconds(10))
    {
    }

    /// <param name="endpoint">The primary's address.</param>
    /// <param name="unaryTimeout">Deadline of a progress report (a report that misses it is sent again).</param>
    public GrpcReplicationClient(string endpoint, TimeSpan unaryTimeout) : this(endpoint, unaryTimeout, new GrpcChannelOptions())
    {
    }

    /// <summary>A client whose channel uses <paramref name="channelOptions"/>, completed with <see cref="GrpcTransportDefaults.Apply"/>.</summary>
    public GrpcReplicationClient(string endpoint, TimeSpan unaryTimeout, GrpcChannelOptions channelOptions)
    {
        ArgumentNullException.ThrowIfNull(channelOptions);
        _channel = GrpcChannel.ForAddress(endpoint, GrpcTransportDefaults.Apply(channelOptions));
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

    public void Dispose() => _channel.Dispose();
}
