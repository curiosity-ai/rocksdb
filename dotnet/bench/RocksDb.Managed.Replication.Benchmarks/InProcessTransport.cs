using System.Runtime.CompilerServices;
using System.Threading.Channels;
using RocksDb.Managed.Replication.Hosting;
using RocksDb.Managed.Replication.Switchover;

namespace RocksDb.Managed.Replication.Benchmarks;

/// <summary>
/// The in-memory transport of the benchmarks (same model as the test suite's InMemoryNetwork): unary calls go straight to
/// the target, streams are pumped through a bounded channel by a producer task with payloads copied, as serialization
/// over a connection would.
/// </summary>
internal static class InProcessTransport
{
    public static async IAsyncEnumerable<T> Pump<T>(Func<CancellationToken, IAsyncEnumerable<T>> source, Func<T, T> copy,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var channel = Channel.CreateBounded<T>(new BoundedChannelOptions(16) { SingleReader = true, SingleWriter = true });
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        Task producer = Task.Run(async () =>
        {
            try
            {
                await foreach (T item in source(cts.Token).WithCancellation(cts.Token)) await channel.Writer.WriteAsync(copy(item), cts.Token);
                channel.Writer.TryComplete();
            }
            catch (Exception e)
            {
                channel.Writer.TryComplete(e is OperationCanceledException ? null : e);
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

/// <summary><see cref="IReplicationSourceClient"/> calling a <see cref="ReplicationPrimary"/> in process.</summary>
internal sealed class InProcessReplicationClient(ReplicationPrimary primary) : IReplicationSourceClient
{
    public IAsyncEnumerable<SnapshotChunk> StreamInitialStateAsync(CancellationToken cancellationToken) =>
        InProcessTransport.Pump(primary.StreamInitialStateAsync, static c => c.Detach(), cancellationToken);

    public IAsyncEnumerable<WalStreamMessage> StreamUpdatesAsync(ulong startSeq, CancellationToken cancellationToken) =>
        InProcessTransport.Pump(ct => primary.StreamUpdatesAsync(startSeq, ct), static m => m.Detach(), cancellationToken);

    public Task ReportProgressAsync(int replicaIndex, ulong appliedThrough, CancellationToken cancellationToken)
    {
        primary.ReportProgress(replicaIndex, appliedThrough);
        return Task.CompletedTask;
    }
}

/// <summary><see cref="IPairNodeClient"/> calling the other <see cref="PairNodeHost"/> in process.</summary>
internal sealed class InProcessPairClient(string nodeId, Func<PairNodeHost> target) : IPairNodeClient
{
    public string NodeId => nodeId;

    public Task<ReplicationNodeStatus?> GetStatusAsync(CancellationToken cancellationToken) => Task.FromResult(target().HandleGetStatus());

    public async Task<PromoteResult?> PromoteAsync(PromoteRequest request, CancellationToken cancellationToken) =>
        await target().HandlePromoteAsync(request).WaitAsync(cancellationToken).ConfigureAwait(false);

    public async Task<ReplicationNodeStatus?> RejectEpochAsync(long epoch, CancellationToken cancellationToken) =>
        await target().HandleRejectEpochAsync(epoch).WaitAsync(cancellationToken).ConfigureAwait(false);

    public Task<ReplicaLogCheck?> CheckReplicaLogAsync(ulong replicaLatestSeq, long replicaEpochAtLatest, CancellationToken cancellationToken) =>
        Task.FromResult<ReplicaLogCheck?>(target().HandleCheckReplicaLog(replicaLatestSeq, replicaEpochAtLatest));

    public IAsyncEnumerable<WalStreamMessage> StreamUpdatesAsync(ulong startSeq, CancellationToken cancellationToken) =>
        InProcessTransport.Pump(ct => target().StreamUpdatesAsync(startSeq, ct), static m => m.Detach(), cancellationToken);

    public Task<string?> GetEpochsAsync(CancellationToken cancellationToken) => Task.FromResult<string?>(target().HandleGetEpochs());

    public IAsyncEnumerable<SnapshotChunk> StreamSnapshotAsync(CancellationToken cancellationToken) =>
        InProcessTransport.Pump(ct => target().StreamSnapshotAsync(ct), static c => c.Detach(), cancellationToken);
}

/// <summary>Percentiles over a sample (sorted in place).</summary>
internal static class Percentiles
{
    public static double At(double[] sorted, double p)
    {
        if (sorted.Length == 0) return double.NaN;
        int i = (int)Math.Ceiling(p / 100 * sorted.Length) - 1;
        return sorted[Math.Clamp(i, 0, sorted.Length - 1)];
    }
}
