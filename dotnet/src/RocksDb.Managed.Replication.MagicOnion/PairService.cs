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
    /// <summary>
    /// Replica -> primary (<see cref="IPairNodeClient.ReportProgressAsync"/>). Declared last: a node built without it
    /// answers Unimplemented, which the client treats as "this primary does not count reports".
    /// </summary>
    UnaryResult<bool> ReportProgressAsync(ulong appliedThrough, long epochAtApplied);

    /// <summary>
    /// <see cref="WriteAsync"/> for a payload larger than one request should be: the key's UTF-8 bytes followed by the
    /// value's, sent in pieces (<see cref="WriteFragmentDto"/>) and applied as one write once the last piece arrived -
    /// through the same host call, with the same result. Nothing is applied when the upload ends early, is malformed
    /// (InvalidArgument) or exceeds the node's limits (ResourceExhausted; <see cref="FragmentedWriteOptions"/>).
    /// Declared last: a node built without it answers Unimplemented and the client falls back to <see cref="WriteAsync"/>.
    /// Use <see cref="GrpcPairClient.WriteAsync(string, string, CancellationToken)"/>, which picks the RPC by size.
    /// </summary>
    Task<ClientStreamingResult<WriteFragmentDto, PairWriteResultDto>> WriteFragmentsAsync();
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
            WalStreamCapabilities capabilities = WalStreamCapabilityHeader.Read(Context.CallContext.RequestHeaders);
            await foreach (WalStreamMessage m in host.StreamUpdatesAsync(startSeq, capabilities, ct).ConfigureAwait(false))
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

    public async UnaryResult<PairWriteResultDto> WriteAsync(string key, string value) =>
        await ApplyAsync(Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes(value), Context.CallContext.CancellationToken).ConfigureAwait(false);

    public async Task<ClientStreamingResult<WriteFragmentDto, PairWriteResultDto>> WriteFragmentsAsync()
    {
        ClientStreamingContext<WriteFragmentDto, PairWriteResultDto> stream = GetClientStreamingContext<WriteFragmentDto, PairWriteResultDto>();
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(Context.CallContext.CancellationToken, host.Stopping);
        CancellationToken ct = Context.CallContext.CancellationToken;
        return stream.Result(await FragmentedWriteReceiver.ReceiveAndApplyAsync(stream, Context.ServiceProvider, linked.Token,
            (key, value) => ApplyAsync(key, value, ct)).ConfigureAwait(false));
    }

    /// <summary>The one way a client write reaches the host, whichever RPC carried it.</summary>
    private async ValueTask<PairWriteResultDto> ApplyAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken)
    {
        using var batch = new WriteBatch();
        batch.Put(key.Span, value.Span);
        PairWriteResult r = await host.WriteAsync(batch, TimeSpan.FromSeconds(5), cancellationToken).ConfigureAwait(false);
        return new PairWriteResultDto { Applied = r.Status == PairWriteStatus.Applied, PrimaryHint = r.PrimaryHint, Sequence = r.Sequence, Status = (int)r.Status };
    }

    public UnaryResult<bool> ReportProgressAsync(ulong appliedThrough, long epochAtApplied)
    {
        host.HandleReportProgress(appliedThrough, epochAtApplied);
        return UnaryResult.FromResult(true);
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

    public GrpcPairClient(string nodeId, string endpoint, TimeSpan statusTimeout) : this(nodeId, endpoint, statusTimeout, new GrpcChannelOptions())
    {
    }

    /// <summary>A client whose channel uses <paramref name="channelOptions"/>, completed with <see cref="GrpcTransportDefaults.Apply"/>.</summary>
    public GrpcPairClient(string nodeId, string endpoint, TimeSpan statusTimeout, GrpcChannelOptions channelOptions)
    {
        ArgumentNullException.ThrowIfNull(channelOptions);
        NodeId = nodeId;
        _channel = GrpcChannel.ForAddress(endpoint, GrpcTransportDefaults.Apply(channelOptions));
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

    public IAsyncEnumerable<WalStreamMessage> StreamUpdatesAsync(ulong startSeq, CancellationToken cancellationToken) =>
        StreamUpdatesAsync(startSeq, WalStreamCapabilities.None, cancellationToken);

    public async IAsyncEnumerable<WalStreamMessage> StreamUpdatesAsync(ulong startSeq, WalStreamCapabilities capabilities,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using ServerStreamingResult<WalChunkDto> call = await _client
            .WithOptions(new CallOptions(headers: WalStreamCapabilityHeader.Create(capabilities), cancellationToken: cancellationToken).WithWaitForReady())
            .SyncUpdatesAsync(startSeq);
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

    public async Task ReportProgressAsync(ulong appliedThrough, long epochAtApplied, CancellationToken cancellationToken)
    {
        try
        {
            await CallWithDeadline(cancellationToken).ReportProgressAsync(appliedThrough, epochAtApplied);
        }
        catch (RpcException e) when (e.StatusCode == Grpc.Core.StatusCode.Unimplemented)
        {
            // A primary built before this call existed: it acknowledges writes on its own, nothing to report.
        }
    }

    /// <summary><see cref="WriteAsync(string, string, GrpcWriteOptions, CancellationToken)"/> with <see cref="GrpcWriteOptions.Default"/>.</summary>
    public Task<PairWriteResultDto> WriteAsync(string key, string value, CancellationToken cancellationToken) =>
        WriteAsync(key, value, GrpcWriteOptions.Default, cancellationToken);

    /// <summary>
    /// A client write. A payload (UTF-8 bytes of key and value) of at most <see cref="GrpcWriteOptions.MaxChunkBytes"/>
    /// is the unary <see cref="IPairService.WriteAsync"/>, exactly as calling it on <see cref="Service"/>; a larger one
    /// is uploaded in pieces (<see cref="IPairService.WriteFragmentsAsync"/>) and applied by the node as one write with
    /// the same result. A node without that RPC gets the unary one; if it refuses the request for its size, this throws
    /// <see cref="LargeWriteException"/> (<see cref="LargeWriteError.NotSupportedByServer"/>), as it does when the node
    /// refuses the upload (too large, busy). Nothing was applied then.
    /// <para>
    /// Retries are the caller's, as for the unary RPC, and the guarantee is the same: a write whose call failed without
    /// a result (connection, deadline) may or may not have been applied, and sending it again applies it again -
    /// harmless for the same key and value. This method sends a write a second time only after Unimplemented, which
    /// the node answers without applying anything.
    /// </para>
    /// </summary>
    public async Task<PairWriteResultDto> WriteAsync(string key, string value, GrpcWriteOptions options, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        ArgumentNullException.ThrowIfNull(options);
        if (FragmentedWriteClient.IsSmall(key, value, options, out long bytes))
            return await WriteCall(FragmentedWriteClient.Deadline(options, 0), cancellationToken).WriteAsync(key, value);
        if (!_noFragmentedWrites)
        {
            try
            {
                return await FragmentedWriteClient.UploadAsync(
                    () => WriteCall(FragmentedWriteClient.Deadline(options, bytes), cancellationToken).WriteFragmentsAsync(), key, value, bytes, options,
                    cancellationToken).ConfigureAwait(false);
            }
            catch (RpcException e) when (e.StatusCode == Grpc.Core.StatusCode.Unimplemented)
            {
                // A node built before this RPC existed. It applied nothing.
                _noFragmentedWrites = true;
            }
        }
        return await FragmentedWriteClient.UnaryToOldServerAsync(
            () => WriteCall(FragmentedWriteClient.Deadline(options, bytes), cancellationToken).WriteAsync(key, value), bytes).ConfigureAwait(false);
    }

    // Set once the node answered Unimplemented to WriteFragmentsAsync.
    private volatile bool _noFragmentedWrites;

    private IPairService WriteCall(DateTime deadline, CancellationToken ct) =>
        _client.WithOptions(new CallOptions(deadline: deadline, cancellationToken: ct).WithWaitForReady());

    public void Dispose() => _channel.Dispose();
}
