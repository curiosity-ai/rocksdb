using System.Runtime.CompilerServices;
using RocksDb.Managed.Replication.Internal;

namespace RocksDb.Managed.Replication.Hosting;

/// <summary>
/// The source side of plain (single primary, N replicas) replication - the logic of rocksdb-sharp's
/// Tests/ReplicationTest/ReplicationService without the transport: initial state as a snapshot stream, the WAL stream,
/// and progress reports that retire archived WALs once every replica has applied them and feed an
/// <see cref="AdaptiveCommitDelayController"/>.
/// </summary>
public sealed class ReplicationPrimary
{
    private readonly RocksDatabase _db;
    private readonly string _checkpointRoot;
    private readonly ReplicationSource _source;
    private readonly AdaptiveCommitDelayController? _delay;
    // "replica-N" ids by index (copy-on-grow), so progress reports do not build a string each.
    private volatile string[] _replicaIds = [];
    private readonly Lock _idsLock = new();

    /// <param name="db">The primary database.</param>
    /// <param name="checkpointRoot">Directory (on the database's file system) under which per-request checkpoints are created.</param>
    /// <param name="commitDelay">Optional back-pressure controller fed with replica lag; null for none.</param>
    public ReplicationPrimary(RocksDatabase db, string checkpointRoot, AdaptiveCommitDelayController? commitDelay)
    {
        ArgumentNullException.ThrowIfNull(db);
        ArgumentException.ThrowIfNullOrEmpty(checkpointRoot);
        _db = db;
        _checkpointRoot = checkpointRoot;
        _source = new ReplicationSource(db);
        _delay = commitDelay;
    }

    public RocksDatabase Database => _db;
    public ReplicationSource Source => _source;
    public string Id { get; init; } = "primary";
    public int MaxChunkBytes { get; init; } = WalTailer.DefaultMaxChunkBytes;
    public int SnapshotChunkBytes { get; init; } = 1024 * 1024;

    /// <summary>A full snapshot of the primary. File deletions stay disabled while it streams.</summary>
    public async IAsyncEnumerable<SnapshotChunk> StreamInitialStateAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        _db.FileSystem.CreateDirectoryIfMissing(_checkpointRoot);
        string dir = FileSystemHelpers.Combine(_checkpointRoot, "checkpoint-" + Guid.NewGuid().ToString("N"));
        using ReplicationSession session = await _source.GetInitialStateAsync(dir, cancellationToken).ConfigureAwait(false);
        await foreach (SnapshotChunk c in session.StreamSnapshotAsync([], SnapshotChunkBytes, cancellationToken).ConfigureAwait(false)) yield return c;
    }

    /// <summary>The WAL stream from <paramref name="startSeq"/> (a single resync message if those WALs are gone).</summary>
    public IAsyncEnumerable<WalStreamMessage> StreamUpdatesAsync(ulong startSeq, CancellationToken cancellationToken) =>
        WalStreamSender.StreamAsync(_db, startSeq, Tag, static _ => 0, MaxChunkBytes, cancellationToken);

    private bool Tag(out string sourceId, out long term)
    {
        sourceId = Id;
        term = 0;
        return true;
    }

    /// <summary>
    /// A replica applied everything up to <paramref name="appliedThrough"/>: retire archived WALs every replica is done
    /// with and report the lag to the commit-delay controller. Returns the number of WAL files retired.
    /// </summary>
    public int ReportProgress(int replicaIndex, ulong appliedThrough)
    {
        _source.ReportReplicaProgress(ReplicaId(replicaIndex), appliedThrough);
        ulong latest = _db.GetLatestSequenceNumber();
        _delay?.ReportLag(replicaIndex, latest > appliedThrough ? (long)(latest - appliedThrough) : 0);
        return _source.RetireArchivedWals();
    }

    private string ReplicaId(int replicaIndex)
    {
        string[] ids = _replicaIds;
        if ((uint)replicaIndex < (uint)ids.Length) return ids[replicaIndex];
        if (replicaIndex < 0 || replicaIndex >= 1024) return "replica-" + replicaIndex;
        lock (_idsLock)
        {
            ids = _replicaIds;
            if (replicaIndex >= ids.Length)
            {
                var grown = new string[replicaIndex + 1];
                ids.CopyTo(grown, 0);
                for (int i = ids.Length; i < grown.Length; i++) grown[i] = "replica-" + i;
                _replicaIds = ids = grown;
            }
            return ids[replicaIndex];
        }
    }

    /// <summary>Waits as long as the commit-delay controller recommends (writers call this between batches).</summary>
    public ValueTask DelayIfNeededAsync(CancellationToken cancellationToken) => _delay?.DelayIfNeededAsync(cancellationToken) ?? ValueTask.CompletedTask;
}

/// <summary>What a replica asks of its primary (implemented over gRPC by RocksDb.Managed.Replication.MagicOnion).</summary>
public interface IReplicationSourceClient
{
    IAsyncEnumerable<SnapshotChunk> StreamInitialStateAsync(CancellationToken cancellationToken);
    IAsyncEnumerable<WalStreamMessage> StreamUpdatesAsync(ulong startSeq, CancellationToken cancellationToken);
    Task ReportProgressAsync(int replicaIndex, ulong appliedThrough, CancellationToken cancellationToken);
}

/// <summary>Thrown by <see cref="ReplicaFollower.FollowAsync"/> when the replica must be rebuilt from a snapshot.</summary>
public sealed class ReplicationResyncRequiredException(string message) : Exception(message);

/// <summary>The replica side of plain replication (rocksdb-sharp ReplicationTest's RunReplicaAsync, transport factored out).</summary>
public static class ReplicaFollower
{
    /// <summary>Restores a snapshot of the primary into <paramref name="dbPath"/> (everything there except <paramref name="preserveNames"/> is replaced).</summary>
    public static ValueTask<SnapshotRestoreResult> SyncInitialStateAsync(IReplicationSourceClient primary, IFileSystem fileSystem, string dbPath,
        IEnumerable<string> preserveNames, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(primary);
        return SnapshotReceiver.ReceiveAsync(primary.StreamInitialStateAsync(cancellationToken), fileSystem, dbPath, preserveNames, cancellationToken);
    }

    /// <summary>
    /// Applies the primary's WAL stream to <paramref name="replica"/> from its latest sequence number + 1 until cancelled
    /// or the stream ends, reporting progress after every chunk (coalesced). Throws
    /// <see cref="ReplicationResyncRequiredException"/> when the primary no longer has the WAL needed or a chunk does not
    /// start at the replica's next sequence number. Returns the number of chunks applied.
    /// </summary>
    public static async Task<long> FollowAsync(IReplicationSourceClient primary, RocksDatabase replica, int replicaIndex, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(primary);
        ArgumentNullException.ThrowIfNull(replica);
        var consumer = new ReplicationConsumer(replica);
        await using var progress = new ProgressReporter((seq, ct) => primary.ReportProgressAsync(replicaIndex, seq, ct));
        long chunks = 0;
        ulong start = replica.GetLatestSequenceNumber() + 1;
        await foreach (WalStreamMessage m in primary.StreamUpdatesAsync(start, cancellationToken).ConfigureAwait(false))
        {
            if (m.ResyncRequired) throw new ReplicationResyncRequiredException("primary cannot stream from " + consumer.ExpectedNextSequence + ": " + m.Reason);
            if (!await consumer.TryIngestChunkAsync(m.Data, cancellationToken).ConfigureAwait(false))
                throw new ReplicationResyncRequiredException("stream mismatch: expected seq " + consumer.ExpectedNextSequence + ", got " + m.FirstSequence);
            chunks++;
            progress.Report(replica.GetLatestSequenceNumber());
        }
        return chunks;
    }
}
