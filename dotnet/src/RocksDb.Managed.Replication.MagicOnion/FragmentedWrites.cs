using System.Text;
using Grpc.Core;
using MagicOnion;
using MagicOnion.Server;
using MessagePack;
using Microsoft.Extensions.DependencyInjection;

namespace RocksDb.Managed.Replication.MagicOnion;

// Large client writes. A write RPC of the services (IPairService.WriteAsync, IClusterService.WriteAsync) is one unary
// request, and a server refuses a request above its receive limit (4 MiB by default). A client therefore sends a write
// whose payload is larger than a chunk through the client-streaming RPC WriteFragmentsAsync of the same service: the
// payload - the key's UTF-8 bytes followed by the value's - in pieces of at most a chunk, put together on the node in
// one pooled buffer and applied through the same host call as the unary write, as one write, only after the last
// piece arrived. The counterpart of fragments on the WAL stream (WalStreamMessage.IsFragment).

/// <summary>
/// One piece of a client write sent through <c>WriteFragmentsAsync</c>. Every piece carries the same
/// <see cref="TotalLength"/> and <see cref="KeyLength"/>; the first one (offset 0) is what makes the node check its
/// limits and allocate.
/// </summary>
[MessagePackObject]
public sealed class WriteFragmentDto
{
    /// <summary>The length of the whole payload: the key's bytes followed by the value's.</summary>
    [Key(0)] public int TotalLength { get; set; }
    /// <summary>How many of the payload's first bytes are the key.</summary>
    [Key(1)] public int KeyLength { get; set; }
    /// <summary>Where <see cref="Data"/> starts in the payload.</summary>
    [Key(2)] public int Offset { get; set; }
    [Key(3)] public ReadOnlyMemory<byte> Data { get; set; }
}

/// <summary>Why a large write was refused before anything was applied (<see cref="LargeWriteException"/>).</summary>
public enum LargeWriteError
{
    /// <summary>The write is larger than the node accepts (<see cref="FragmentedWriteOptions.MaxWriteBytes"/>). Retrying does not help.</summary>
    TooLarge = 0,
    /// <summary>
    /// The node holds as many bytes of unfinished large writes as it allows
    /// (<see cref="FragmentedWriteOptions.MaxPendingBytes"/>). Retry later.
    /// </summary>
    ServerBusy = 1,
    /// <summary>The node refused the upload as malformed (offsets, lengths) or as stalled.</summary>
    Rejected = 2,
    /// <summary>
    /// The node was built before large writes existed (it has no <c>WriteFragmentsAsync</c>) and refused the write as
    /// one request because it exceeds its receive limit.
    /// </summary>
    NotSupportedByServer = 3,
}

/// <summary>
/// A write larger than a chunk was refused by the node, or cannot be sent to it. Nothing was applied. Other failures
/// of a large write (the connection, the deadline) surface as <see cref="RpcException"/> like those of a small one,
/// and are indeterminate in the same way.
/// </summary>
public sealed class LargeWriteException(LargeWriteError error, string message, Exception? innerException) : Exception(message, innerException)
{
    public LargeWriteError Error { get; } = error;
}

/// <summary>Node-side limits of large client writes (<c>WriteFragmentsAsync</c>); see <see cref="GrpcNodeServerOptions.FragmentedWrites"/>.</summary>
public sealed class FragmentedWriteOptions
{
    /// <summary>
    /// The largest payload (key + value bytes) of one write: 64 MiB by default, the default size of a memtable
    /// (<c>write_buffer_size</c>) - a single write that fills one on its own is more likely a mistake than a workload,
    /// and unlike a follower (<see cref="Hosting.WalStreamAssembler.DefaultMaxBatchBytes"/>) a node has no other way
    /// to take a write it refuses. Checked against the announced length before anything is allocated.
    /// </summary>
    public int MaxWriteBytes { get; init; } = 64 * 1024 * 1024;

    /// <summary>
    /// How many bytes of large writes the node holds at a time, over all uploads that are being received or applied:
    /// 256 MiB by default. An upload that would exceed it is refused (<see cref="LargeWriteError.ServerBusy"/>)
    /// before anything is allocated. A write larger than this is still accepted while no other is in flight.
    /// </summary>
    public long MaxPendingBytes { get; init; } = 256L * 1024 * 1024;

    /// <summary>
    /// How long the node waits for the next piece of an upload before it gives the upload up and frees its buffer
    /// (30 s by default): a client that stops sending without closing the call must not hold memory.
    /// </summary>
    public TimeSpan IdleTimeout { get; init; } = TimeSpan.FromSeconds(30);
}

/// <summary>
/// The large writes a node is holding: enforces <see cref="FragmentedWriteOptions.MaxPendingBytes"/> over all uploads.
/// One per server (<see cref="GrpcNodeServer.FragmentedWrites"/>); services hosted without one share
/// <see cref="Shared"/>.
/// </summary>
public sealed class FragmentedWriteBudget
{
    /// <summary>The budget (default options) of services whose host registered none.</summary>
    public static FragmentedWriteBudget Shared { get; } = new(new FragmentedWriteOptions());

    private long _pending;
    private long _uploads;
    private long _completed;

    public FragmentedWriteBudget(FragmentedWriteOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentOutOfRangeException.ThrowIfLessThan(options.MaxWriteBytes, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(options.MaxPendingBytes, 1);
        Options = options;
    }

    public FragmentedWriteOptions Options { get; }

    /// <summary>Bytes of the uploads being received or applied right now (their buffers).</summary>
    public long PendingBytes => Interlocked.Read(ref _pending);

    /// <summary>How many uploads were started (a first piece was accepted).</summary>
    public long UploadsStarted => Interlocked.Read(ref _uploads);

    /// <summary>How many uploads were received completely and handed to the host.</summary>
    public long UploadsCompleted => Interlocked.Read(ref _completed);

    internal static FragmentedWriteBudget From(IServiceProvider services) => services.GetService<FragmentedWriteBudget>() ?? Shared;

    internal bool TryReserve(int bytes)
    {
        long now = Interlocked.Add(ref _pending, bytes);
        // The first upload in flight always fits (MaxWriteBytes may be configured above MaxPendingBytes).
        if (now > Options.MaxPendingBytes && now != bytes)
        {
            Interlocked.Add(ref _pending, -bytes);
            return false;
        }
        Interlocked.Increment(ref _uploads);
        return true;
    }

    internal void Release(int bytes) => Interlocked.Add(ref _pending, -bytes);

    internal void OnCompleted() => Interlocked.Increment(ref _completed);
}

/// <summary>
/// Node side of one <c>WriteFragmentsAsync</c> call: checks every piece, collects the payload in a pooled buffer
/// (<see cref="FragmentBuffer"/>) accounted in the <see cref="FragmentedWriteBudget"/>, and gives buffer and budget
/// back when disposed - however the call ends.
/// </summary>
internal sealed class FragmentedWriteUpload(FragmentedWriteBudget budget) : IDisposable
{
    // Leading tokens of the status detail: how the client tells the refusals apart.
    internal const string TooLargeToken = "large-write:too-large: ";
    internal const string BusyToken = "large-write:busy: ";
    internal const string InvalidToken = "large-write:invalid: ";

    private readonly FragmentBuffer _buffer = new();
    private int _reserved;
    private int _keyLength;

    public bool IsComplete => _buffer.IsComplete;
    public ReadOnlyMemory<byte> Key => _buffer.Memory[.._keyLength];
    public ReadOnlyMemory<byte> Value => _buffer.Memory[_keyLength..];

    public void Accept(WriteFragmentDto f)
    {
        if (!_buffer.IsActive)
        {
            if (f.TotalLength <= 0 || f.KeyLength < 0 || f.KeyLength > f.TotalLength)
                throw Invalid("a write of " + f.TotalLength + " bytes with a key of " + f.KeyLength + " bytes");
            if (f.Offset != 0) throw Invalid("the upload starts at offset " + f.Offset + ", not 0");
            // Both before anything is allocated.
            if (f.TotalLength > budget.Options.MaxWriteBytes)
            {
                throw new ReturnStatusException(Grpc.Core.StatusCode.ResourceExhausted, TooLargeToken + "the write has " + f.TotalLength +
                    " bytes, more than this node accepts (MaxWriteBytes = " + budget.Options.MaxWriteBytes + ")");
            }
            if (!budget.TryReserve(f.TotalLength))
            {
                throw new ReturnStatusException(Grpc.Core.StatusCode.ResourceExhausted, BusyToken + "this node already holds " + budget.PendingBytes +
                    " bytes of large writes in flight (MaxPendingBytes = " + budget.Options.MaxPendingBytes + "); retry later");
            }
            _reserved = f.TotalLength;
            _keyLength = f.KeyLength;
            _buffer.Begin(f.TotalLength);
        }
        else if (f.TotalLength != _buffer.TotalLength || f.KeyLength != _keyLength)
        {
            throw Invalid("a piece announces " + f.TotalLength + " bytes with a key of " + f.KeyLength + "; the upload began with " + _buffer.TotalLength +
                " and " + _keyLength);
        }
        if (!_buffer.TryAppend(f.Offset, f.Data.Span))
        {
            throw Invalid("a piece at offset " + f.Offset + " with " + f.Data.Length + " bytes does not continue the upload at offset " +
                _buffer.ReceivedLength + " of " + _buffer.TotalLength);
        }
    }

    /// <summary>The request stream ended: the payload must be whole.</summary>
    public void Complete()
    {
        if (!_buffer.IsComplete)
        {
            throw Invalid("the upload ended after " + _buffer.ReceivedLength + " of " + (_buffer.IsActive ? _buffer.TotalLength : 0) + " bytes");
        }
        budget.OnCompleted();
    }

    private static ReturnStatusException Invalid(string what) => new(Grpc.Core.StatusCode.InvalidArgument, InvalidToken + what);

    /// <summary>
    /// Receives a whole upload from <paramref name="next"/> (false at the end of the request stream). A piece that does
    /// not arrive within <see cref="FragmentedWriteOptions.IdleTimeout"/> ends the call with DeadlineExceeded.
    /// </summary>
    public async Task ReceiveAsync(Func<CancellationToken, Task<bool>> next, Func<WriteFragmentDto> current, CancellationToken cancellationToken)
    {
        using var idle = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        TimeSpan timeout = budget.Options.IdleTimeout;
        try
        {
            while (true)
            {
                idle.CancelAfter(timeout);
                if (!await next(idle.Token).ConfigureAwait(false)) break;
                Accept(current());
            }
        }
        catch (Exception e) when (e is not ReturnStatusException && idle.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            throw new ReturnStatusException(Grpc.Core.StatusCode.DeadlineExceeded, InvalidToken + "no piece of the upload arrived within " + timeout +
                " (" + _buffer.ReceivedLength + " of " + _buffer.TotalLength + " bytes received)");
        }
        Complete();
    }

    public void Dispose()
    {
        _buffer.Release();
        int reserved = _reserved;
        _reserved = 0;
        if (reserved != 0) budget.Release(reserved);
    }
}

/// <summary>
/// The node side of <c>WriteFragmentsAsync</c>, for services that implement <see cref="IPairService"/> /
/// <see cref="IClusterService"/> themselves (the library's services use it too).
/// </summary>
public static class FragmentedWriteReceiver
{
    /// <summary>
    /// Receives one upload from <paramref name="stream"/> under the limits of the server's
    /// <see cref="FragmentedWriteBudget"/> (looked up in <paramref name="services"/>; <see cref="FragmentedWriteBudget.Shared"/>
    /// when none is registered) and, once the payload is whole, hands key and value to <paramref name="apply"/> - the
    /// same call the unary write makes. The memory is only valid until <paramref name="apply"/> completes.
    /// <paramref name="apply"/> is not called for an upload that is refused, malformed, cancelled, cut off or stalled;
    /// those end the call with a status (ResourceExhausted, InvalidArgument, DeadlineExceeded) or the transport's
    /// error. The buffer is given back however the call ends.
    /// </summary>
    public static async Task<TResult> ReceiveAndApplyAsync<TResult>(ClientStreamingContext<WriteFragmentDto, TResult> stream, IServiceProvider services,
        CancellationToken cancellationToken, Func<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>, ValueTask<TResult>> apply)
    {
        ArgumentNullException.ThrowIfNull(stream);
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(apply);
        using var upload = new FragmentedWriteUpload(FragmentedWriteBudget.From(services));
        await upload.ReceiveAsync(stream.MoveNext, () => stream.Current, cancellationToken).ConfigureAwait(false);
        return await apply(upload.Key, upload.Value).ConfigureAwait(false);
    }
}

/// <summary>Client-side settings of a write sent with <c>GrpcPairClient.WriteAsync</c> / <c>GrpcClusterClient.WriteAsync</c>.</summary>
public sealed class GrpcWriteOptions
{
    public static GrpcWriteOptions Default { get; } = new();

    /// <summary>
    /// A write whose payload (UTF-8 bytes of key and value) is larger than this is sent in pieces of at most this
    /// size through <c>WriteFragmentsAsync</c>; a smaller one is the unary RPC, as always. 256 KiB by default, the
    /// chunk size of the WAL stream (<see cref="WalTailer.DefaultMaxChunkBytes"/>).
    /// </summary>
    public int MaxChunkBytes { get; init; } = WalTailer.DefaultMaxChunkBytes;

    /// <summary>
    /// The deadline of a write: how long the node may take to apply and acknowledge it (it waits up to 5 s for a
    /// handoff on a pair node and up to 10 s for the commit on a cluster node). 30 s by default.
    /// </summary>
    public TimeSpan Timeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The transfer rate a large write is expected to reach at least: its deadline is <see cref="Timeout"/> plus its
    /// size divided by this. 1 MiB/s by default (a 64 MiB write gets 64 s more).
    /// </summary>
    public int MinBytesPerSecond { get; init; } = 1024 * 1024;
}

/// <summary>What the two clients share: choosing between the unary and the fragmented RPC, the upload, the error mapping.</summary>
internal static class FragmentedWriteClient
{
    /// <summary>
    /// Whether the write fits into one request. Decided from the string lengths when they settle it (a UTF-16 code unit
    /// is at most 3 UTF-8 bytes), so the common small write costs two additions.
    /// </summary>
    public static bool IsSmall(string key, string value, GrpcWriteOptions options, out long payloadBytes)
    {
        payloadBytes = 0;
        if (((long)key.Length + value.Length) * 3 <= options.MaxChunkBytes) return true;
        payloadBytes = (long)Encoding.UTF8.GetByteCount(key) + Encoding.UTF8.GetByteCount(value);
        return payloadBytes <= options.MaxChunkBytes;
    }

    public static DateTime Deadline(GrpcWriteOptions options, long payloadBytes) =>
        DateTime.UtcNow + options.Timeout + TimeSpan.FromSeconds((double)payloadBytes / Math.Max(1, options.MinBytesPerSecond));

    /// <summary>
    /// Sends key + value through the opened client-streaming call. Throws <see cref="LargeWriteException"/> for a
    /// refusal of the node and <see cref="RpcException"/> otherwise - with Unimplemented when
    /// the node has no such RPC (nothing was applied; the caller falls back to the unary RPC).
    /// </summary>
    public static async Task<TResult> UploadAsync<TResult>(Func<Task<ClientStreamingResult<WriteFragmentDto, TResult>>> open, string key, string value,
        long payloadBytes, GrpcWriteOptions options, CancellationToken cancellationToken)
    {
        if (payloadBytes > Array.MaxLength)
            throw new LargeWriteException(LargeWriteError.TooLarge, "a write of " + payloadBytes + " bytes does not fit into one buffer", null);
        int total = (int)payloadBytes;
        byte[] buffer = RocksArrayPool<byte>.Shared.Rent(total);
        try
        {
            int keyLength = Encoding.UTF8.GetBytes(key, buffer);
            int written = keyLength + Encoding.UTF8.GetBytes(value, buffer.AsSpan(keyLength));
            if (written != total) throw new InvalidOperationException("encoded " + written + " bytes, expected " + total);
            try
            {
                using ClientStreamingResult<WriteFragmentDto, TResult> call = await open().ConfigureAwait(false);
                try
                {
                    for (int offset = 0; offset < total; offset += options.MaxChunkBytes)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        int n = Math.Min(options.MaxChunkBytes, total - offset);
                        // The buffer is not touched until the awaited write returns: nothing is copied before serialization.
                        await call.RequestStream.WriteAsync(new WriteFragmentDto { TotalLength = total, KeyLength = keyLength, Offset = offset, Data = buffer.AsMemory(offset, n) })
                            .ConfigureAwait(false);
                    }
                    await call.RequestStream.CompleteAsync().ConfigureAwait(false);
                }
                catch (Exception e) when (e is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
                {
                    // The node ended the call while pieces were still being sent (a refusal, or it went away): the
                    // call's status says which. If the call has none to give, the write error is the answer.
                    try
                    {
                        await call.ResponseAsync.ConfigureAwait(false);
                    }
                    catch (RpcException status)
                    {
                        throw Map(status) ?? (Exception)status;
                    }
                    throw;
                }
                return await call.ResponseAsync.ConfigureAwait(false);
            }
            catch (RpcException e) when (Map(e) is { } mapped)
            {
                throw mapped;
            }
        }
        finally
        {
            RocksArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static LargeWriteException? Map(RpcException e)
    {
        string detail = e.Status.Detail ?? "";
        if (detail.StartsWith(FragmentedWriteUpload.TooLargeToken, StringComparison.Ordinal))
            return new LargeWriteException(LargeWriteError.TooLarge, detail[FragmentedWriteUpload.TooLargeToken.Length..], e);
        if (detail.StartsWith(FragmentedWriteUpload.BusyToken, StringComparison.Ordinal))
            return new LargeWriteException(LargeWriteError.ServerBusy, detail[FragmentedWriteUpload.BusyToken.Length..], e);
        if (detail.StartsWith(FragmentedWriteUpload.InvalidToken, StringComparison.Ordinal))
            return new LargeWriteException(LargeWriteError.Rejected, detail[FragmentedWriteUpload.InvalidToken.Length..], e);
        return null;
    }

    /// <summary>
    /// The unary RPC for a write that is larger than a chunk, sent to a node without <c>WriteFragmentsAsync</c>: a
    /// refusal for its size becomes <see cref="LargeWriteError.NotSupportedByServer"/>.
    /// </summary>
    public static async Task<TResult> UnaryToOldServerAsync<TResult>(Func<UnaryResult<TResult>> call, long payloadBytes)
    {
        try
        {
            return await call();
        }
        catch (RpcException e) when (e.StatusCode == Grpc.Core.StatusCode.ResourceExhausted)
        {
            throw new LargeWriteException(LargeWriteError.NotSupportedByServer,
                "the server does not support large writes (it has no WriteFragmentsAsync) and refused the write of " + payloadBytes +
                " bytes as one request: " + e.Status.Detail, e);
        }
    }
}
