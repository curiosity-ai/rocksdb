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
    /// <summary>
    /// <paramref name="leaderTerm"/> is the term of the leadership the report is for. The result is false when the
    /// report was ignored.
    /// </summary>
    UnaryResult<bool> ReportLastSyncSequenceNumber(string nodeId, ulong seq, long leaderTerm);
    UnaryResult<Dictionary<string, string>> GetFileHashesAsync(List<FileInfoDto> candidates);
    Task<ServerStreamingResult<SnapshotChunkDto>> SyncInitialStateAsync(List<FileInfoDto> verifiedInventory);
    UnaryResult<string> GetRaftTermsAsync();

    /// <summary>Client write, acknowledged once committed (replicated to a quorum).</summary>
    UnaryResult<bool> WriteAsync(string key, string value);
    UnaryResult<DigestDto> ChecksumAsync();

    /// <summary>
    /// <see cref="WriteAsync"/> for a payload larger than one request should be: the key's UTF-8 bytes followed by the
    /// value's, sent in pieces (<see cref="WriteFragmentDto"/>) and written as one log entry once the last piece
    /// arrived - through the same host call, acknowledged once committed. Nothing is written when the upload ends
    /// early, is malformed (InvalidArgument) or exceeds the node's limits (ResourceExhausted;
    /// <see cref="FragmentedWriteOptions"/>). Use
    /// <see cref="GrpcClusterClient.WriteAsync(string, string, CancellationToken)"/>, which picks the RPC by size.
    /// </summary>
    Task<ClientStreamingResult<WriteFragmentDto, bool>> WriteFragmentsAsync();
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
        // The call's token (client gone, deadline, server stopping) or the host shutting down ends a write that a
        // stalled client would otherwise block forever - with the host's database still in use by this handler.
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(Context.CallContext.CancellationToken, host.Stopping);
        CancellationToken ct = linked.Token;
        try
        {
            await foreach (WalStreamMessage m in host.StreamUpdatesAsync(followerId, startSeq, ct).ConfigureAwait(false))
            {
                await stream.WriteAsync(WalChunkDto.From(m)).WaitAsync(ct).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
        }
        return stream.Result();
    }

    public UnaryResult<bool> ReportLastSyncSequenceNumber(string nodeId, ulong seq, long leaderTerm) =>
        UnaryResult.FromResult(host.HandleReportProgress(nodeId, seq, leaderTerm));

    public async UnaryResult<Dictionary<string, string>> GetFileHashesAsync(List<FileInfoDto> candidates)
    {
        IReadOnlyDictionary<string, string> r = await host.HandleGetFileHashesAsync(FileInfoDto.To(candidates), Context.CallContext.CancellationToken)
            .ConfigureAwait(false);
        return new Dictionary<string, string>(r, StringComparer.Ordinal);
    }

    public async Task<ServerStreamingResult<SnapshotChunkDto>> SyncInitialStateAsync(List<FileInfoDto> verifiedInventory)
    {
        ServerStreamingContext<SnapshotChunkDto> stream = GetServerStreamingContext<SnapshotChunkDto>();
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(Context.CallContext.CancellationToken, host.Stopping);
        CancellationToken ct = linked.Token;
        await foreach (SnapshotChunk c in host.StreamSnapshotAsync(FileInfoDto.To(verifiedInventory), ct).ConfigureAwait(false))
        {
            await stream.WriteAsync(SnapshotChunkDto.From(c)).WaitAsync(ct).ConfigureAwait(false);
        }
        return stream.Result();
    }

    public UnaryResult<string> GetRaftTermsAsync() => UnaryResult.FromResult(host.HandleGetTerms());

    public async UnaryResult<bool> WriteAsync(string key, string value) =>
        await ApplyAsync(Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes(value), Context.CallContext.CancellationToken).ConfigureAwait(false);

    public async Task<ClientStreamingResult<WriteFragmentDto, bool>> WriteFragmentsAsync()
    {
        ClientStreamingContext<WriteFragmentDto, bool> stream = GetClientStreamingContext<WriteFragmentDto, bool>();
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(Context.CallContext.CancellationToken, host.Stopping);
        CancellationToken ct = Context.CallContext.CancellationToken;
        return stream.Result(await FragmentedWriteReceiver.ReceiveAndApplyAsync(stream, Context.ServiceProvider, linked.Token,
            (key, value) => ApplyAsync(key, value, ct)).ConfigureAwait(false));
    }

    /// <summary>The one way a client write reaches the host, whichever RPC carried it.</summary>
    private ValueTask<bool> ApplyAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken) =>
        host.WriteCommittedAsync(key, value, TimeSpan.FromSeconds(10), cancellationToken);

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
    /// so a healed network partition is noticed quickly). What the options leave open is filled in with
    /// <see cref="GrpcTransportDefaults.Apply"/>: keep-alive pings unless a handler or client is set.
    /// </summary>
    public GrpcClusterClient(ClusterMember peer, TimeSpan unaryTimeout, GrpcChannelOptions channelOptions)
    {
        ArgumentNullException.ThrowIfNull(peer);
        ArgumentNullException.ThrowIfNull(channelOptions);
        NodeId = peer.NodeId;
        _channel = GrpcChannel.ForAddress(peer.Endpoint, GrpcTransportDefaults.Apply(channelOptions));
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

    public async IAsyncEnumerable<WalStreamMessage> StreamUpdatesAsync(string followerId, ulong startSeq,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using ServerStreamingResult<WalChunkDto> call = await _client.WithCancellationToken(cancellationToken).SyncUpdatesAsync(followerId, startSeq);
        while (await call.ResponseStream.MoveNext(cancellationToken).ConfigureAwait(false))
        {
            yield return call.ResponseStream.Current.ToMessage();
        }
    }

    public async Task ReportProgressAsync(string followerId, ulong appliedThrough, long leaderTerm, CancellationToken cancellationToken) =>
        await Unary(cancellationToken).ReportLastSyncSequenceNumber(followerId, appliedThrough, leaderTerm);

    /// <summary>
    /// Asked in as many calls as it takes to keep every request and every answer below
    /// <see cref="GrpcTransportDefaults.MaxFileListBytes"/>, however many files the database has.
    /// </summary>
    public async Task<IReadOnlyDictionary<string, string>> GetFileHashesAsync(IReadOnlyList<ReplicationFileInfo> candidates, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(candidates);
        var all = new Dictionary<string, string>(StringComparer.Ordinal);
        for (int start = 0; start < candidates.Count;)
        {
            int count = FileListLimit.CountThatFits(candidates, start);
            var batch = new List<FileInfoDto>(count);
            for (int i = start; i < start + count; i++) batch.Add(new FileInfoDto { Name = candidates[i].FileName, Size = candidates[i].Size, Hash = candidates[i].Hash });
            foreach (KeyValuePair<string, string> kv in await _client.WithCancellationToken(cancellationToken).GetFileHashesAsync(batch)) all[kv.Key] = kv.Value;
            start += count;
        }
        return all;
    }

    /// <summary>
    /// The inventory travels in the one request that opens the stream, so it is cut to what fits into
    /// <see cref="GrpcTransportDefaults.MaxFileListBytes"/>, largest files first; the files left out are transferred
    /// again instead of reused (the restore keeps exactly the files the source's plan names).
    /// </summary>
    public async IAsyncEnumerable<SnapshotChunk> StreamSnapshotAsync(IReadOnlyList<ReplicationFileInfo> verifiedInventory,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(verifiedInventory);
        using ServerStreamingResult<SnapshotChunkDto> call = await _client.WithCancellationToken(cancellationToken)
            .SyncInitialStateAsync(FileInfoDto.From(FileListLimit.LargestThatFit(verifiedInventory)));
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

    /// <summary><see cref="WriteAsync(string, string, GrpcWriteOptions, CancellationToken)"/> with <see cref="GrpcWriteOptions.Default"/>.</summary>
    public Task<bool> WriteAsync(string key, string value, CancellationToken cancellationToken) =>
        WriteAsync(key, value, GrpcWriteOptions.Default, cancellationToken);

    /// <summary>
    /// A client write, true once committed. A payload (UTF-8 bytes of key and value) of at most
    /// <see cref="GrpcWriteOptions.MaxChunkBytes"/> is the unary <see cref="IClusterService.WriteAsync"/> (one request,
    /// no stream to set up), exactly as calling it on <see cref="Service"/>; a larger one is uploaded in pieces
    /// (<see cref="IClusterService.WriteFragmentsAsync"/>) and written by the node as one log entry with the same
    /// result. Throws <see cref="LargeWriteException"/> when the node refuses the upload (too large, busy, malformed).
    /// Nothing was written then.
    /// <para>
    /// Retries are the caller's, and the guarantee is the same for both RPCs: false, or a call that failed without a
    /// result, may be a write that still commits, and sending it again writes it again - harmless for the same key and
    /// value. This method never sends a write twice.
    /// </para>
    /// </summary>
    public async Task<bool> WriteAsync(string key, string value, GrpcWriteOptions options, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        ArgumentNullException.ThrowIfNull(options);
        if (FragmentedWriteClient.IsSmall(key, value, options, out long bytes))
            return await WriteCall(FragmentedWriteClient.Deadline(options, 0), cancellationToken).WriteAsync(key, value);
        return await FragmentedWriteClient.UploadAsync(
            () => WriteCall(FragmentedWriteClient.Deadline(options, bytes), cancellationToken).WriteFragmentsAsync(), key, value, bytes, options,
            cancellationToken).ConfigureAwait(false);
    }

    private IClusterService WriteCall(DateTime deadline, CancellationToken ct) =>
        _client.WithOptions(new CallOptions(deadline: deadline, cancellationToken: ct));

    public void Dispose() => _channel.Dispose();
}

/// <summary>
/// Keeps a list of files sent in one gRPC message below <see cref="GrpcTransportDefaults.MaxFileListBytes"/>. The size of
/// an entry is estimated from above (name and hash as UTF-8, 16 bytes for the size and the MessagePack framing).
/// </summary>
internal static class FileListLimit
{
    // A SHA-256 signature as hex: what the answer to a hash request adds per file.
    private const int HashChars = 64;

    private static long EntryBytes(ReplicationFileInfo f) => 3L * (f.FileName?.Length ?? 0) + Math.Max(HashChars, 3L * (f.Hash?.Length ?? 0)) + 16;

    /// <summary>How many of <paramref name="files"/> from <paramref name="start"/> fit into one message (at least one).</summary>
    public static int CountThatFits(IReadOnlyList<ReplicationFileInfo> files, int start)
    {
        long bytes = 0;
        int n = 0;
        while (start + n < files.Count)
        {
            bytes += EntryBytes(files[start + n]);
            if (n > 0 && bytes > GrpcTransportDefaults.MaxFileListBytes) break;
            n++;
        }
        return n;
    }

    /// <summary><paramref name="files"/> when they fit into one message; otherwise the largest files that do.</summary>
    public static IReadOnlyList<ReplicationFileInfo> LargestThatFit(IReadOnlyList<ReplicationFileInfo> files)
    {
        long bytes = 0;
        foreach (ReplicationFileInfo f in files) bytes += EntryBytes(f);
        if (bytes <= GrpcTransportDefaults.MaxFileListBytes) return files;
        var bySize = new List<ReplicationFileInfo>(files);
        bySize.Sort(static (a, b) => b.Size.CompareTo(a.Size));
        bytes = 0;
        int n = 0;
        while (n < bySize.Count && (bytes += EntryBytes(bySize[n])) <= GrpcTransportDefaults.MaxFileListBytes) n++;
        bySize.RemoveRange(n, bySize.Count - n);
        return bySize;
    }
}
