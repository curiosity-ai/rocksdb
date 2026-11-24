using Grpc.Core;
using MagicOnion.Server;

namespace RocksDb.Managed.Replication.MagicOnion;

/// <summary>
/// Writes of the server-streaming handlers (WAL stream, snapshot stream). The messages point into the producer's
/// pooled buffer (the WAL tailer's, the snapshot session's), which the producing enumeration reuses or returns as soon as
/// it moves on or is disposed. A write is therefore never left behind: when the token fires while a write is pending
/// (a client that stopped reading, the host shutting down), the call is aborted first and the write awaited to its end,
/// and only then does the handler unwind into the enumeration's disposal. Abandoning the write
/// (<c>WriteAsync(...).WaitAsync(token)</c>) let the transport serialize from a buffer that had been returned.
/// </summary>
internal static class StreamWrites
{
    // How long an aborted call's pending write is given to end. It ends at once in practice (the abort fails it); the
    // bound only keeps a handler from hanging on a transport that never completes it.
    private static readonly TimeSpan s_abortedWriteGrace = TimeSpan.FromSeconds(10);

    public static async ValueTask WriteOrAbortAsync<T>(ServerStreamingContext<T> stream, T message, ServerCallContext call, CancellationToken cancellationToken)
    {
        Task write = stream.WriteAsync(message);
        if (write.IsCompletedSuccessfully) return;
        try
        {
            await write.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!write.IsCompleted)
        {
            try
            {
                call.GetHttpContext().Abort();
            }
            catch (Exception)
            {
            }
            try
            {
                await write.WaitAsync(s_abortedWriteGrace).ConfigureAwait(false);
            }
            catch (Exception)
            {
                // The aborted write's own failure (or the grace period) is not the handler's error.
            }
            throw;
        }
    }
}
