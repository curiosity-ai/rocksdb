using System.Runtime.CompilerServices;
using System.Text;
using Grpc.Core;
using Grpc.Net.Client;
using MagicOnion;
using MagicOnion.Client;
using MagicOnion.Server;
using RocksDb.Managed.Db;
using RocksDb.Managed.Replication.Hosting;
using RocksDb.Managed.Replication.Switchover;

namespace RocksDb.Managed.Replication.MagicOnion;

/// <summary>gRPC contract of a primary/replica pair node (port of rocksdb-sharp's Tests/SwitchoverTest/IPairService).</summary>
public interface IPairService : IService<IPairService>
{
    UnaryResult<PairStatusDto> GetStatusAsync();
    UnaryResult<PromoteResultDto> PromoteAsync(PromoteDto request);
    UnaryResult<PairStatusDto> RejectEpochAsync(long epoch);
    UnaryResult<ReplicaLogCheckDto> CheckReplicaLogAsync(ulong replicaLatestSeq, long replicaEpochAtLatest);
    Task<ServerStreamingResult<WalChunkDto>> SyncUpdatesAsync(ulong startSeq);
    UnaryResult<string> GetEpochsAsync();
    Task<ServerStreamingResult<SnapshotChunkDto>> SyncInitialStateAsync();

    /// <summary>Client write; refused with a primary hint when this node is not the primary.</summary>
    UnaryResult<PairWriteResultDto> WriteAsync(string key, string value);
    /// <summary>Operator / coordinator: hand the primary role to the other node.</summary>
    UnaryResult<SwitchoverResultDto> HandOffAsync();
    UnaryResult<DigestDto> ChecksumAsync();
}

/// <summary>Server side of <see cref="IPairService"/>: delegates to the process's <see cref="PairNodeHost"/>.</summary>
public sealed class PairService(PairNodeHost host) : ServiceBase<IPairService>, IPairService
{
    public UnaryResult<PairStatusDto> GetStatusAsync() => UnaryResult.FromResult(PairStatusDto.From(host.HandleGetStatus()));

    public async UnaryResult<PromoteResultDto> PromoteAsync(PromoteDto request)
    {
        PromoteResult r = await host.HandlePromoteAsync(new PromoteRequest(request.Epoch, request.FromPrimaryId, request.HandoffSeq, request.CatchUpTimeoutMs))
            .ConfigureAwait(false);
        return new PromoteResultDto { Promoted = r.Promoted, Epoch = r.Epoch, Role = (int)r.Role, Reason = r.Reason };
    }

    public async UnaryResult<PairStatusDto> RejectEpochAsync(long epoch) => PairStatusDto.From(await host.HandleRejectEpochAsync(epoch).ConfigureAwait(false));

    public UnaryResult<ReplicaLogCheckDto> CheckReplicaLogAsync(ulong replicaLatestSeq, long replicaEpochAtLatest)
    {
        ReplicaLogCheck r = host.HandleCheckReplicaLog(replicaLatestSeq, replicaEpochAtLatest);
        return UnaryResult.FromResult(new ReplicaLogCheckDto { IsPrimary = r.IsPrimary, Consistent = r.Consistent, Epoch = r.Epoch, Epochs = r.Epochs });
    }

    public async Task<ServerStreamingResult<WalChunkDto>> SyncUpdatesAsync(ulong startSeq)
    {
        ServerStreamingContext<WalChunkDto> stream = GetServerStreamingContext<WalChunkDto>();
        // The call's token (client gone, deadline, server stopping) or the host shutting down ends a write that a
        // stalled client would otherwise block forever - with the host's database still in use by this handler.
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(Context.CallContext.CancellationToken, host.Stopping);
        CancellationToken ct = linked.Token;
        try
        {
            await foreach (WalStreamMessage m in host.StreamUpdatesAsync(startSeq, ct).ConfigureAwait(false))
            {
                await stream.WriteAsync(WalChunkDto.From(m)).WaitAsync(ct).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
        }
        return stream.Result();
    }

    public UnaryResult<string> GetEpochsAsync() => UnaryResult.FromResult(host.HandleGetEpochs());

    public async Task<ServerStreamingResult<SnapshotChunkDto>> SyncInitialStateAsync()
    {
        ServerStreamingContext<SnapshotChunkDto> stream = GetServerStreamingContext<SnapshotChunkDto>();
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(Context.CallContext.CancellationToken, host.Stopping);
        CancellationToken ct = linked.Token;
        await foreach (SnapshotChunk c in host.StreamSnapshotAsync(ct).ConfigureAwait(false))
        {
            await stream.WriteAsync(SnapshotChunkDto.From(c)).WaitAsync(ct).ConfigureAwait(false);
        }
        return stream.Result();
    }

    public async UnaryResult<PairWriteResultDto> WriteAsync(string key, string value)
    {
        using var batch = new WriteBatch();
        batch.Put(Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes(value));
        PairWriteResult r = await host.WriteAsync(batch, TimeSpan.FromSeconds(5), Context.CallContext.CancellationToken).ConfigureAwait(false);
        return new PairWriteResultDto { Applied = r.Status == PairWriteStatus.Applied, PrimaryHint = r.PrimaryHint, Sequence = r.Sequence };
    }

    public async UnaryResult<SwitchoverResultDto> HandOffAsync() =>
        SwitchoverResultDto.From(await host.HandOffAsync(CancellationToken.None).ConfigureAwait(false));

    public UnaryResult<DigestDto> ChecksumAsync() => UnaryResult.FromResult(DigestDto.From(host.Checksum()));
}

/// <summary><see cref="IPairNodeClient"/> over gRPC (rocksdb-sharp SwitchoverTest's PairPeer).</summary>
public sealed class GrpcPairClient : IPairNodeClient, IDisposable
{
    private readonly GrpcChannel _channel;
    private readonly IPairService _client;
    private readonly TimeSpan _statusTimeout;

    public GrpcPairClient(string nodeId, string endpoint, TimeSpan statusTimeout)
    {
        NodeId = nodeId;
        _channel = GrpcChannel.ForAddress(endpoint);
        _client = MagicOnionClient.Create<IPairService>(_channel);
        _statusTimeout = statusTimeout;
    }

    public string NodeId { get; }

    public IPairService Service => _client;

    // Wait-for-ready: a channel whose last connection attempt failed (the other node was still starting) otherwise fails
    // every call instantly until its reconnect backoff runs out. Every call has a deadline or a token, so this is bounded.
    private IPairService Call(CancellationToken ct) => _client.WithOptions(new CallOptions(cancellationToken: ct).WithWaitForReady());

    private IPairService CallWithDeadline(CancellationToken ct) =>
        _client.WithOptions(new CallOptions(deadline: DateTime.UtcNow + _statusTimeout, cancellationToken: ct).WithWaitForReady());

    public async Task<ReplicationNodeStatus?> GetStatusAsync(CancellationToken cancellationToken)
    {
        try
        {
            return (await CallWithDeadline(cancellationToken).GetStatusAsync()).ToStatus();
        }
        catch (RpcException)
        {
            return null;
        }
    }

    public async Task<PromoteResult?> PromoteAsync(PromoteRequest request, CancellationToken cancellationToken)
    {
        PromoteResultDto r = await Call(cancellationToken).PromoteAsync(new PromoteDto
        {
            Epoch = request.Epoch,
            FromPrimaryId = request.FromPrimaryId,
            HandoffSeq = request.HandoffSeq,
            CatchUpTimeoutMs = request.CatchUpTimeoutMs,
        });
        return new PromoteResult(r.Promoted, r.Epoch, (ReplicationRole)r.Role, r.Reason);
    }

    public async Task<ReplicationNodeStatus?> RejectEpochAsync(long epoch, CancellationToken cancellationToken)
    {
        try
        {
            return (await Call(cancellationToken).RejectEpochAsync(epoch)).ToStatus();
        }
        catch (RpcException)
        {
            return null;
        }
    }

    public async Task<ReplicaLogCheck?> CheckReplicaLogAsync(ulong replicaLatestSeq, long replicaEpochAtLatest, CancellationToken cancellationToken)
    {
        try
        {
            ReplicaLogCheckDto r = await CallWithDeadline(cancellationToken).CheckReplicaLogAsync(replicaLatestSeq, replicaEpochAtLatest);
            return new ReplicaLogCheck(r.IsPrimary, r.Consistent, r.Epoch, r.Epochs);
        }
        catch (RpcException)
        {
            return null;
        }
    }

    public async IAsyncEnumerable<WalStreamMessage> StreamUpdatesAsync(ulong startSeq, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using ServerStreamingResult<WalChunkDto> call = await Call(cancellationToken).SyncUpdatesAsync(startSeq);
        while (await call.ResponseStream.MoveNext(cancellationToken).ConfigureAwait(false))
        {
            yield return call.ResponseStream.Current.ToMessage();
        }
    }

    public async Task<string?> GetEpochsAsync(CancellationToken cancellationToken)
    {
        try
        {
            return await CallWithDeadline(cancellationToken).GetEpochsAsync();
        }
        catch (RpcException)
        {
            return null;
        }
    }

    public async IAsyncEnumerable<SnapshotChunk> StreamSnapshotAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using ServerStreamingResult<SnapshotChunkDto> call = await Call(cancellationToken).SyncInitialStateAsync();
        while (await call.ResponseStream.MoveNext(cancellationToken).ConfigureAwait(false))
        {
            yield return call.ResponseStream.Current.ToChunk();
        }
    }

    public void Dispose() => _channel.Dispose();
}
