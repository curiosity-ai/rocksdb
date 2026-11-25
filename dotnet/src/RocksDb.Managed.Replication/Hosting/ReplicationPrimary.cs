using System.Runtime.CompilerServices;
using RocksDb.Managed.Replication.Internal;

namespace RocksDb.Managed.Replication.Hosting;

/// <summary>
/// The source side of plain (single primary, N replicas) replication - the logic of rocksdb-sharp's
/// Tests/ReplicationTest/ReplicationService without the transport: initial state as a snapshot stream, the WAL stream,
/// and progress reports that retire archived WALs once every replica has applied them and feed an
/// <see cref="AdaptiveCommitDelayController"/>.
/// </summary>
public sealed class ReplicationPrimary : IReplicationChunkSizes
{
    private readonly RocksDatabase _db;
    private readonly string _checkpointRoot;
    private readonly ReplicationSource _source;
    private readonly AdaptiveCommitDelayController? _delay;
    private readonly ReplicationLineage _lineage;
    private readonly Func<ulong, long> _incarnationForSeq;
    private readonly SnapshotAttachPin _attachPin = new();

    /// <summary>
    /// How long the primary keeps its obsolete files (its WAL) after a snapshot stream ended, waiting for the replica to
    /// start following; the first WAL stream it serves ends the wait. Zero releases them with the end of the snapshot.
    /// </summary>
    public TimeSpan SnapshotAttachTimeout { get; init; } = TimeSpan.FromSeconds(30);
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
        // What this primary writes from here on belongs to a new incarnation (ReplicationLineage): a replica whose
        // tail lies past the log this database has now (it lost its unsynced end, or was restored from a backup) fails
        // the attach check instead of continuing behind entries this log never had.
        _lineage = ReplicationLineage.Load(db);
        _lineage.StartIncarnation(db.GetLatestSequenceNumber());
        _incarnationForSeq = _lineage.GetIncarnationForSeq;
        // Checkpoints of snapshot streams that an earlier process did not live to remove: each pins (hard links) every
        // table file of its moment.
        HostResync.DeleteLeftoverCheckpoints(db.FileSystem, checkpointRoot, "checkpoint-", null);
    }

    /// <summary>The lineage of this primary's log (which incarnation wrote which sequence numbers).</summary>
    public ReplicationLineage Lineage => _lineage;

    /// <summary>
    /// A replica's attach check, asked before it follows from <paramref name="latestSeq"/> + 1: whether its log can be a
    /// prefix of this one. <paramref name="incarnationAtLatest"/> is the incarnation the replica has recorded for its
    /// latest entry. An empty replica (<paramref name="latestSeq"/> 0) is consistent. Not consistent when the replica
    /// holds entries past the end of this log, when it has no incarnation for its latest entry (0: its log comes with no
    /// lineage, so nothing says it is this log's prefix - a snapshot brings the lineage with it), or when this log's
    /// entry at that sequence number belongs to another incarnation: the replica's tail was written by an incarnation
    /// that lost it. The answer carries the lineage for the replica to adopt.
    /// </summary>
    public ReplicaAttachCheck CheckReplicaLog(ulong latestSeq, long incarnationAtLatest)
    {
        ulong latest = _db.GetLatestSequenceNumber();
        string map = _lineage.Serialize();
        if (latestSeq > latest)
            return new ReplicaAttachCheck(false, map, "the replica's log ends at " + latestSeq + ", past the primary's at " + latest);
        if (latestSeq == 0) return new ReplicaAttachCheck(true, map, null);
        if (incarnationAtLatest == 0)
            return new ReplicaAttachCheck(false, map, "the replica has no lineage for its log (ending at " + latestSeq + ")");
        long mine = _lineage.GetIncarnationForSeq(latestSeq);
        if (mine != incarnationAtLatest)
        {
            return new ReplicaAttachCheck(false, map, "the replica's entry " + latestSeq + " belongs to incarnation " + incarnationAtLatest +
                ", the primary's to " + mine + ": the logs diverged");
        }
        return new ReplicaAttachCheck(true, map, null);
    }

    public RocksDatabase Database => _db;
    public ReplicationSource Source => _source;
    public string Id { get; init; } = "primary";
    /// <summary>
    /// The most bytes of write batches one message of the WAL stream carries (a larger batch travels in fragments of
    /// this size). Must be larger than a write batch header (12 bytes) and fit into one message of the transport: over
    /// gRPC at most <c>GrpcTransportDefaults.MaxPayloadBytes</c>, which <c>GrpcNodeServer</c> checks at start.
    /// </summary>
    public int MaxChunkBytes
    {
        get => _maxChunkBytes;
        init
        {
            WalTailer.ValidateChunkSizes(value, 1);
            _maxChunkBytes = value;
        }
    }

    /// <summary>The most file bytes one message of a snapshot carries. At least 1; the same transport limit applies.</summary>
    public int SnapshotChunkBytes
    {
        get => _snapshotChunkBytes;
        init
        {
            WalTailer.ValidateChunkSizes(WalTailer.DefaultMaxChunkBytes, value);
            _snapshotChunkBytes = value;
        }
    }

    private readonly int _maxChunkBytes = WalTailer.DefaultMaxChunkBytes;
    private readonly int _snapshotChunkBytes = 1024 * 1024;

    /// <summary>A full snapshot of the primary. File deletions stay disabled while it streams.</summary>
    public async IAsyncEnumerable<SnapshotChunk> StreamInitialStateAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        _db.FileSystem.CreateDirectoryIfMissing(_checkpointRoot);
        string dir = FileSystemHelpers.Combine(_checkpointRoot, "checkpoint-" + Guid.NewGuid().ToString("N"));
        using ReplicationSession session = await _source.GetInitialStateAsync(dir, cancellationToken).ConfigureAwait(false);
        // The lineage travels with the files it describes (SnapshotChunk.LogDescription).
        string lineage = _lineage.Serialize();
        await foreach (SnapshotChunk c in session.StreamSnapshotAsync([], SnapshotChunkBytes, cancellationToken).ConfigureAwait(false))
        {
            yield return c.Kind == SnapshotChunkKind.Plan ? c.WithLogDescription(lineage) : c;
        }
        // Sent completely: keep the WAL until the replica follows (before the session re-enables deletions).
        _attachPin.HoldUntilAttach(_db, SnapshotAttachTimeout);
    }

    /// <summary>
    /// The WAL stream from <paramref name="startSeq"/> (a single resync message if those WALs are gone). A write batch
    /// larger than <see cref="MaxChunkBytes"/> is sent in pieces (<see cref="WalStreamMessage.IsFragment"/>).
    /// </summary>
    public IAsyncEnumerable<WalStreamMessage> StreamUpdatesAsync(ulong startSeq, CancellationToken cancellationToken)
    {
        IAsyncEnumerable<WalStreamMessage> stream = WalStreamSender.StreamAsync(_db, startSeq, Tag, _incarnationForSeq, MaxChunkBytes, null, cancellationToken);
        return _attachPin.HasHolds ? ReleasePinAtFirstMessage(stream, cancellationToken) : stream;
    }

    private async IAsyncEnumerable<WalStreamMessage> ReleasePinAtFirstMessage(IAsyncEnumerable<WalStreamMessage> stream,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (WalStreamMessage m in stream.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            // A replica is reading the WAL: the files kept for a snapshot's receiver may go.
            if (_attachPin.HasHolds) _attachPin.ReleaseAll();
            yield return m;
        }
    }

    private bool Tag(out string sourceId, out long term)
    {
        sourceId = Id;
        term = _lineage.CurrentIncarnation;
        return true;
    }

    /// <summary>
    /// A replica applied everything up to <paramref name="appliedThrough"/>: retire archived WALs every replica is done
    /// with and report the lag to the commit-delay controller. Returns the number of WAL files retired.
    /// </summary>
    public int ReportProgress(int replicaIndex, ulong appliedThrough)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(replicaIndex);
        _source.ReportReplicaProgress(ReplicaId(replicaIndex), appliedThrough);
        ulong latest = _db.GetLatestSequenceNumber();
        // A replica the controller has no slot for is not a reason to skip the WAL retirement below (the report failed
        // for good and that replica's archived WALs were never retired).
        if (_delay is { } delay && replicaIndex < delay.ReplicaCount) delay.ReportLag(replicaIndex, latest > appliedThrough ? (long)(latest - appliedThrough) : 0);
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

    /// <summary>
    /// The attach check (<see cref="ReplicationPrimary.CheckReplicaLog"/>). Every replica is checked before it follows:
    /// a transport that cannot ask the primary fails the call.
    /// </summary>
    Task<ReplicaAttachCheck> CheckReplicaLogAsync(ulong latestSeq, long incarnationAtLatest, CancellationToken cancellationToken);
}

/// <summary>
/// Thrown by <see cref="ReplicaFollower.FollowAsync(IReplicationSourceClient, RocksDatabase, int, ReplicaFollowerOptions, CancellationToken)"/>
/// when the stream broke the fragment protocol (<see cref="WalAssemblyStatus.Invalid"/>). Nothing of the batch was
/// applied. Follow again from the replica's position; if it happens again with the replica at the same
/// <see cref="AppliedThrough"/>, the primary sends that batch wrongly every time - rebuild the replica from a snapshot
/// (<see cref="FollowerResyncPolicy.ShouldRebuildAfterStreamFault"/> implements that rule).
/// </summary>
public sealed class WalStreamProtocolException(string message, ulong appliedThrough) : IOException(message)
{
    /// <summary>The replica's latest sequence number when the stream was dropped.</summary>
    public ulong AppliedThrough { get; } = appliedThrough;
}

/// <summary>Options of <see cref="ReplicaFollower.FollowAsync(IReplicationSourceClient, RocksDatabase, int, ReplicaFollowerOptions, CancellationToken)"/>.</summary>
public sealed class ReplicaFollowerOptions
{
    /// <summary>
    /// The largest serialized write batch taken from the stream (<see cref="WalStreamAssembler.MaxBatchBytes"/>; default
    /// 1 GiB). A larger one ends the stream with <see cref="ReplicationResyncRequiredException"/>: a snapshot carries
    /// the same data as files.
    /// </summary>
    public int MaxBatchBytes { get; init; } = WalStreamAssembler.DefaultMaxBatchBytes;
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
        // The primary's lineage (ReplicationLineage) comes with the plan and is stored before CURRENT appears: the
        // replica then knows which incarnation wrote its entries from its first attach on.
        return SnapshotReceiver.ReceiveAsync(primary.StreamInitialStateAsync(cancellationToken), fileSystem, dbPath, preserveNames,
            new SnapshotRestoreOptions
            {
                // Without the lineage the replica could not pass its attach check: such a snapshot is refused.
                RequireLogDescription = true,
                OnLogDescription = description => ReplicationLineage.Load(fileSystem, dbPath).Replace(description),
            }, cancellationToken);
    }

    /// <summary>
    /// Prepares the options a replica's database is opened with: adds <see cref="ReplicaRecoveryVeto"/>, so that the
    /// engine does not recover on its own from a failed write of a replicated chunk (which would publish the chunk's
    /// sequence numbers without its data, see <see cref="ReplicaApplyGuard"/>). <see cref="FollowAsync(IReplicationSourceClient, RocksDatabase, int, CancellationToken)"/>
    /// detects such a replica either way and asks for a resync; with this the database also stays consistent across a
    /// restart. Returns <paramref name="options"/>.
    /// </summary>
    public static DbOptions ProtectReplicaOptions(DbOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Listeners.Add(new ReplicaRecoveryVeto(static () => true));
        return options;
    }

    /// <summary>
    /// Applies the primary's WAL stream to <paramref name="replica"/> from its latest sequence number + 1 until cancelled
    /// or the stream ends, reporting progress after every chunk (coalesced). Throws
    /// <see cref="ReplicationResyncRequiredException"/> when the primary no longer has the WAL needed or a chunk does not
    /// start at the replica's next sequence number. Returns the number of chunks applied.
    /// </summary>
    public static Task<long> FollowAsync(IReplicationSourceClient primary, RocksDatabase replica, int replicaIndex, CancellationToken cancellationToken) =>
        FollowAsync(primary, replica, replicaIndex, new ReplicaFollowerOptions(), cancellationToken);

    /// <summary>
    /// <see cref="FollowAsync(IReplicationSourceClient, RocksDatabase, int, CancellationToken)"/> with options. A write
    /// batch the primary sends in fragments is applied (and reported) only once it is complete; one larger than
    /// <see cref="ReplicaFollowerOptions.MaxBatchBytes"/> throws <see cref="ReplicationResyncRequiredException"/>, a
    /// stream that breaks the fragment protocol throws <see cref="WalStreamProtocolException"/>. Whenever this method
    /// ends, a partial batch is discarded; following again starts at the replica's latest sequence number + 1.
    /// <para>
    /// Before it follows, the replica asks the primary whether its log can be a prefix of the primary's
    /// (<see cref="IReplicationSourceClient.CheckReplicaLogAsync"/>, see <see cref="ReplicationLineage"/>) and throws
    /// <see cref="ReplicationResyncRequiredException"/> when it is not. It throws the same when an earlier chunk failed
    /// to apply and the replica's log has since moved past it (<see cref="ReplicaApplyGuard"/>). A chunk whose write
    /// fails is recorded and the failure rethrown.
    /// </para>
    /// </summary>
    public static async Task<long> FollowAsync(IReplicationSourceClient primary, RocksDatabase replica, int replicaIndex, ReplicaFollowerOptions options,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(primary);
        ArgumentNullException.ThrowIfNull(replica);
        ArgumentNullException.ThrowIfNull(options);
        var consumer = new ReplicationConsumer(replica);
        await using var progress = new ProgressReporter((seq, ct) => primary.ReportProgressAsync(replicaIndex, seq, ct));
        long chunks = 0;
        if (ReplicaApplyGuard.IsLogEndUnknown(replica))
        {
            throw new ReplicationResyncRequiredException("an earlier chunk failed to apply and the replica's log end (" + replica.GetLatestSequenceNumber() +
                ") moved past it: what the replica holds is unknown");
        }
        ulong latest = replica.GetLatestSequenceNumber();
        ReplicationLineage lineage = ReplicationLineage.Load(replica);
        ReplicaAttachCheck check = await primary.CheckReplicaLogAsync(latest, lineage.GetIncarnationForSeq(latest), cancellationToken).ConfigureAwait(false)
            ?? throw new InvalidOperationException("the primary did not answer the attach check");
        if (!check.Consistent) throw new ReplicationResyncRequiredException("the replica's log is not a prefix of the primary's: " + check.Reason);
        lineage.AdoptAfter(latest, check.Lineage);
        ulong start = latest + 1;
        using var assembler = new WalStreamAssembler(options.MaxBatchBytes);
        await foreach (WalStreamMessage m in primary.StreamUpdatesAsync(start, cancellationToken).ConfigureAwait(false))
        {
            if (m.ResyncRequired) throw new ReplicationResyncRequiredException("primary cannot stream from " + consumer.ExpectedNextSequence + ": " + m.Reason);
            WalAssemblyStatus assembled = assembler.Accept(m, out ReadOnlyMemory<byte> chunk);
            if (assembled == WalAssemblyStatus.Incomplete) continue;
            if (assembled == WalAssemblyStatus.TooLarge)
                throw new ReplicationResyncRequiredException("primary's stream cannot be followed from " + consumer.ExpectedNextSequence + ": " + assembler.LastError);
            if (assembled == WalAssemblyStatus.Invalid)
                throw new WalStreamProtocolException("stream from " + consumer.ExpectedNextSequence + " dropped: " + assembler.LastError, replica.GetLatestSequenceNumber());
            ulong appliedThrough = replica.GetLatestSequenceNumber();
            bool applied;
            try
            {
                applied = await consumer.TryIngestChunkAsync(chunk, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e) when (e is not OperationCanceledException and not OutOfMemoryException)
            {
                // The write failed: whatever the engine does with the chunk's sequence numbers from here on, the
                // replica holds its log through appliedThrough and nothing after it.
                if (!ReplicaApplyGuard.RecordFailure(replica, appliedThrough))
                {
                    // Nothing durable says that a chunk failed here: do not follow this database again.
                    throw new ReplicationResyncRequiredException("the chunk at " + (appliedThrough + 1) + " could not be applied (" + e.Message +
                        ") and the failure could not be recorded in the replica's directory: rebuild the replica from a snapshot");
                }
                throw;
            }
            if (!applied)
                throw new ReplicationResyncRequiredException("stream mismatch: expected seq " + consumer.ExpectedNextSequence + ", got " + m.FirstSequence);
            chunks++;
            progress.Report(replica.GetLatestSequenceNumber());
        }
        return chunks;
    }
}
