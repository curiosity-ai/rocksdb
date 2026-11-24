using System.Runtime.CompilerServices;
using RocksDb.Managed.Replication.Internal;
using RocksDb.Managed.Replication.Switchover;

namespace RocksDb.Managed.Replication.Hosting;

/// <summary>
/// The calls a <see cref="PairNodeHost"/> makes to the other node of a primary/replica pair: the switchover RPCs
/// (<see cref="ISwitchoverPeer"/>) plus the replica's data plane. Implemented in memory by the tests and over gRPC by
/// RocksDb.Managed.Replication.MagicOnion.
/// </summary>
public interface IPairNodeClient : ISwitchoverPeer
{
    Task<ReplicaLogCheck?> CheckReplicaLogAsync(ulong replicaLatestSeq, long replicaEpochAtLatest, CancellationToken cancellationToken);
    IAsyncEnumerable<WalStreamMessage> StreamUpdatesAsync(ulong startSeq, CancellationToken cancellationToken);
    Task<string?> GetEpochsAsync(CancellationToken cancellationToken);
    IAsyncEnumerable<SnapshotChunk> StreamSnapshotAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Tells the primary that this replica holds its log through <paramref name="appliedThrough"/> (fsync'd, with
    /// <see cref="PairNodeHostOptions.SyncWal"/>), and the epoch the replica has for that entry - the primary counts
    /// the report only if it holds the same entry in the same epoch. Sent by a replica configured with
    /// <see cref="PairWriteAck.Replica"/>. Throws when the report did not arrive (it is then sent again).
    /// </summary>
    Task ReportProgressAsync(ulong appliedThrough, long epochAtApplied, CancellationToken cancellationToken);
}

/// <summary>When <see cref="PairNodeHost.WriteAsync"/> acknowledges a write (<see cref="PairNodeHostOptions.WriteAck"/>).</summary>
public enum PairWriteAck
{
    /// <summary>Once the primary has written it. The replica receives it afterwards (asynchronous replication).</summary>
    Primary = 0,
    /// <summary>
    /// Once the replica has reported that it holds it as well (synchronous replication). Without a replica that
    /// follows, writes are not acknowledged (<see cref="PairWriteStatus.NotReplicated"/>).
    /// </summary>
    Replica = 1,
}

/// <summary>Configuration of a <see cref="PairNodeHost"/>.</summary>
public sealed class PairNodeHostOptions
{
    public required string NodeId { get; init; }
    public required string PeerId { get; init; }
    /// <summary>Database directory; role state lives in its "role" subdirectory (kept across snapshot restores).</summary>
    public required string DbPath { get; init; }
    /// <summary>The role a brand-new node starts in. Ignored once the node has role state.</summary>
    public required ReplicationRole InitialRole { get; init; }
    public required Func<DbOptions> DbOptionsFactory { get; init; }
    public SwitchoverOptions Switchover { get; init; } = new();
    /// <summary>
    /// The most bytes of write batches one message of the WAL stream carries (a larger batch travels in fragments of
    /// this size). Must be larger than a write batch header (12 bytes) and fit into one message of the transport: over
    /// gRPC at most <c>GrpcTransportDefaults.MaxPayloadBytes</c>, which <c>GrpcNodeServer</c> checks at start.
    /// </summary>
    public int MaxChunkBytes { get; init; } = WalTailer.DefaultMaxChunkBytes;
    /// <summary>The most file bytes one message of a snapshot carries. At least 1; the same transport limit applies.</summary>
    public int SnapshotChunkBytes { get; init; } = 1024 * 1024;
    public TimeSpan RetryDelay { get; init; } = TimeSpan.FromMilliseconds(200);
    /// <summary>
    /// How often a node that is <see cref="ReplicationRole.HandingOff"/> outside a running handoff (the other node was
    /// unreachable when the handoff had to be settled, or this node restarted in the middle of one) asks the other node
    /// how the handoff ended. Until one of these attempts succeeds the pair has no writable primary.
    /// <see cref="Timeout.InfiniteTimeSpan"/> leaves it to the caller (<see cref="PairNodeHost.ResolveAsync"/>).
    /// </summary>
    public TimeSpan ResolveInterval { get; init; } = TimeSpan.FromSeconds(1);
    /// <summary>
    /// How long <see cref="PairNodeHost.DisposeAsync"/> waits for RPC handlers that still use the database (a stream
    /// whose consumer stopped reading holds it until its transport gives up) before it closes the database anyway.
    /// </summary>
    public TimeSpan DisposeDrainTimeout { get; init; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// How long this node, as the source of a snapshot, keeps its obsolete files (its WAL) after the snapshot stream
    /// ended, waiting for the replica to attach to the WAL stream; the first WAL stream it serves ends the wait. Zero
    /// releases them with the end of the snapshot stream (a flush in between can then force another snapshot).
    /// </summary>
    public TimeSpan SnapshotAttachTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// How a resync treats the copy this node has (<see cref="SnapshotRestoreMode"/>). Default
    /// <see cref="SnapshotRestoreMode.Auto"/>: the snapshot is restored next to the current copy and swapped in when the
    /// volume has room for it (the node then keeps an openable copy for the whole resync), and the current copy is
    /// deleted first when it has not, or when the free space cannot be determined. The host logs which was chosen.
    /// </summary>
    public SnapshotRestoreMode ResyncRestoreMode { get; init; } = SnapshotRestoreMode.Auto;

    /// <summary>
    /// <see cref="SnapshotRestoreMode.Auto"/>: the free space that must remain beyond the bytes the snapshot announces
    /// (default 64 MiB).
    /// </summary>
    public long ResyncFreeSpaceMarginBytes { get; init; } = SnapshotRestoreOptions.DefaultFreeSpaceMarginBytes;

    /// <summary>
    /// Whether log entries are fsync'd before they count (the counterpart of <see cref="ClusterNodeHostOptions.SyncWal"/>):
    /// a client write before <see cref="PairNodeHost.WriteAsync"/> acknowledges it (and before it is streamed to the
    /// replica), a replicated chunk before the replica's latest sequence number moves past it - which is what a
    /// promotion and a <see cref="PairWriteAck.Replica"/> report rely on. Concurrent writes share one fsync.
    /// <para>
    /// False (the default) keeps only process-crash durability: after a power loss the node's log ends at whatever the
    /// OS had written. Acknowledged writes are then lost if the primary loses power and (a) the replica had not received
    /// them, or (b) the replica cannot be reached when the primary restarts, or
    /// <see cref="YieldToReplicaAheadOnRestart"/> is off (the replica is rebuilt from the primary); and if a node
    /// promoted by a handoff loses power before the entries it took over with reached its disk.
    /// With true, a power loss of either node loses no acknowledged write. Set it on both nodes: either can be primary.
    /// See the README ("Durability of a pair") for the combinations with <see cref="WriteAck"/>.
    /// </para>
    /// </summary>
    public bool SyncWal { get; init; }

    /// <summary>
    /// When a client write is acknowledged. <see cref="PairWriteAck.Primary"/> (the default): once the primary wrote
    /// it; an acknowledged write the replica had not received yet is lost if the primary is lost for good
    /// (<see cref="PairNodeHost.ForcePromoteAsync"/>). <see cref="PairWriteAck.Replica"/>: once the replica reported
    /// that it holds it too, so an acknowledged write is on both nodes (on both disks with <see cref="SyncWal"/>) and
    /// survives the permanent loss of either; the price is one round trip per write and no acknowledged writes while
    /// the replica is down. Set it on both nodes: the replica sends its reports only when it is configured with
    /// <see cref="PairWriteAck.Replica"/>.
    /// </summary>
    public PairWriteAck WriteAck { get; init; } = PairWriteAck.Primary;

    /// <summary>
    /// With <see cref="PairWriteAck.Replica"/>: how long a write waits for the replica's report before it ends as
    /// <see cref="PairWriteStatus.NotReplicated"/>. A handoff waits for the writes in flight, so this also bounds how
    /// long one can be delayed by a replica that stopped reporting.
    /// </summary>
    public TimeSpan ReplicaAckTimeout { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// What a node that restarts as primary does when the replica holds entries past the end of the log it recovered -
    /// its own writes, lost here in a power loss because they were not fsync'd. True (the default): it hands the primary
    /// role to the replica (<see cref="SwitchoverNode.YieldToReplicaAheadAsync"/>, the handoff protocol with the
    /// replica's log end as the handoff point) and follows it, so those writes survive. False: it continues as primary
    /// and the replica is rebuilt from it - they are lost. Either way a replica that cannot be reached
    /// during the restart is rebuilt later. Never needed with <see cref="SyncWal"/> (the replica is never ahead).
    /// </summary>
    public bool YieldToReplicaAheadOnRestart { get; init; } = true;

    /// <summary>
    /// The largest serialized write batch this node takes from a WAL stream while it follows
    /// (<see cref="WalStreamAssembler.MaxBatchBytes"/>; default <see cref="WalStreamAssembler.DefaultMaxBatchBytes"/>,
    /// 1 GiB). A follower holds a whole batch in memory before applying it, however it arrived; a batch announced
    /// larger than this is refused before anything is allocated and the node rebuilds from a snapshot instead, which
    /// carries the same data as files.
    /// </summary>
    public int MaxBatchBytes { get; init; } = WalStreamAssembler.DefaultMaxBatchBytes;

    public Action<string>? Log { get; init; }
}

/// <summary>Result of a client write sent to a pair node.</summary>
public enum PairWriteStatus
{
    Applied = 0,
    /// <summary>This node is not the primary (or the hold timed out): retry on <see cref="PairWriteResult.PrimaryHint"/>.</summary>
    NotPrimary = 1,
    /// <summary>
    /// Only with <see cref="PairWriteAck.Replica"/>, and indeterminate: the write is in this primary's log, but the
    /// replica did not report it within <see cref="PairNodeHostOptions.ReplicaAckTimeout"/> (or this node stopped). It
    /// reaches the replica when the replica follows again - unless this node is lost first. Retry it (idempotently) or
    /// treat it as failed; it must not be counted as acknowledged.
    /// </summary>
    NotReplicated = 2,
}

public readonly record struct PairWriteResult(PairWriteStatus Status, string? PrimaryHint, ulong Sequence);

/// <summary>Status of a pair node.</summary>
public sealed record PairNodeStatus(ReplicationNodeStatus Node, bool Resyncing, int Resyncs, bool WritesOpen, ulong AttachedFromSeq)
{
    /// <summary><see cref="PairNodeHost.FollowerFault"/>: why the node stopped following for good, or null.</summary>
    public string? FollowerFault { get; init; }
}

/// <summary>Fault injection for tests (rocksdb-sharp SwitchoverTest's InjectedFaults).</summary>
public sealed class PairFaults
{
    /// <summary>While set, the replica does not apply received chunks.</summary>
    public volatile bool PauseIngest;
}

/// <summary>
/// One node of a primary/replica pair with planned switchover: owns the database, the <see cref="SwitchoverNode"/> and,
/// while replica, the WAL stream from the other node - the managed port of rocksdb-sharp's
/// <c>Tests/SwitchoverTest/PairNodeHost</c> with the transport factored out (<see cref="IPairNodeClient"/>).
/// </summary>
public sealed class PairNodeHost : IAsyncDisposable, IReplicationChunkSizes
{
    private const string RoleDirName = "role";

    private readonly PairNodeHostOptions _cfg;
    private readonly IPairNodeClient _peer;
    private readonly IFileSystem _fs;
    private readonly CancellationTokenSource _cts = new();
    private readonly SemaphoreSlim _resyncGate = new(1, 1);
    private readonly Lock _followerLock = new();
    private readonly SemaphoreSlim _ingestLock = new(1, 1);
    private readonly DatabaseUsageGate _dbGate = new();

    private ReplicationRoleState _state = null!;
    private SwitchoverNode _node = null!;
    private RocksDatabase _db = null!;
    private Task? _followerTask;
    private CancellationTokenSource? _followerCts;
    private volatile bool _resyncing;
    private Task _resyncTask = Task.CompletedTask;
    private int _resyncs;
    private ulong _attachedFromSeq;
    private int _walStreamFaults;
    private string? _lastWalStreamFault;
    private readonly FollowerResyncPolicy _resyncPolicy = new();
    private readonly SnapshotAttachPin _attachPin = new();
    // Cancelled whenever this node becomes a replica: ends the WAL streams it serves as primary.
    private CancellationTokenSource _primaryCts = new();
    private Task _resolveTask = Task.CompletedTask;
    // False until StartAsync has settled this node's role: a restarted primary does not answer the replica's attach
    // check or stream before it decided whether to continue (a replica that is ahead would rebuild on the answer).
    private volatile bool _serving;
    // Completed when _serving is set (or the host is disposed first): client writes that arrive earlier wait on it.
    private readonly TaskCompletionSource _started = new(TaskCreationOptions.RunContinuationsAsynchronously);
    // PairWriteAck.Replica: the highest sequence number the replica reported holding (0 after this node was a replica).
    private readonly Lock _ackLock = new();
    private readonly RocksDb.Managed.Util.AsyncBroadcastSignal _replicaProgress = new();
    private ulong _replicaAckedSeq;
    private int _ackGeneration;
    private readonly Stack<CancellationTokenSource> _timeoutSources = new();

    public PairNodeHost(PairNodeHostOptions options, IPairNodeClient peer)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(peer);
        WalTailer.ValidateChunkSizes(options.MaxChunkBytes, options.SnapshotChunkBytes);
        _cfg = options;
        _peer = peer;
        Authorization = new ReplicationAuthorizer(options.NodeId, [options.PeerId]);
        _fs = options.DbOptionsFactory().FileSystem;
    }

    public string NodeId => _cfg.NodeId;
    /// <summary>
    /// Who may call this node's endpoints: its node-to-node endpoints are for the other node of the pair. A transport
    /// asks it before it hands a call to this host (see <see cref="ReplicationAuthorizer"/>).
    /// </summary>
    public ReplicationAuthorizer Authorization { get; }
    public int MaxChunkBytes => _cfg.MaxChunkBytes;
    public int SnapshotChunkBytes => _cfg.SnapshotChunkBytes;
    public RocksDatabase Db => _db;
    public SwitchoverNode Node => _node;
    public bool IsResyncing => _resyncing;
    public int Resyncs => Volatile.Read(ref _resyncs);
    public PairFaults Faults { get; } = new();
    /// <summary>Cancelled when the host starts shutting down; transports use it to abandon a blocked stream write.</summary>
    public CancellationToken Stopping => _cts.Token;
    /// <summary>How often a WAL stream served by this node ended on an error of its own WAL (Corruption, NotSupported).</summary>
    public int WalStreamFaults => Volatile.Read(ref _walStreamFaults);
    /// <summary>The message of the latest such error, or null.</summary>
    public string? LastWalStreamFault => Volatile.Read(ref _lastWalStreamFault);

    private void OnWalStreamFault(RocksDbException e)
    {
        Interlocked.Increment(ref _walStreamFaults);
        Volatile.Write(ref _lastWalStreamFault, e.Message);
        Log("WAL stream fault (" + e.Code + "): " + e.Message);
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        // A resync that was swapping the restored copy in when the process ended (SnapshotRestoreSwap): completed or
        // discarded before the directory and the role state in it are touched.
        string? restoredEpochs = SnapshotRestoreSwap.Recover(_fs, _cfg.DbPath, [RoleDirName], Log);
        _fs.CreateDirectoryIfMissing(_cfg.DbPath);
        HostResync.DeleteLeftoverCheckpointsOf(_fs, _cfg.DbPath, Log);
        _state = new ReplicationRoleState(_fs, FileSystemHelpers.Combine(_cfg.DbPath, RoleDirName));
        if (restoredEpochs is not null)
        {
            // The swap was done but the epoch map of the restored log had not been stored yet.
            _state.ReplaceEpochs(restoredEpochs);
            SnapshotRestoreSwap.ClearPendingDescription(_fs, _cfg.DbPath);
            Log("applied the epoch map of the snapshot restored before the restart");
        }
        bool replica = _state.IsFresh ? _cfg.InitialRole == ReplicationRole.Replica : _state.Role == ReplicationRole.Replica;
        if (replica && !_fs.FileExists(FileSystemHelpers.Combine(_cfg.DbPath, "CURRENT")))
        {
            // A new replica, or one that died while restoring a snapshot.
            await PullSnapshotWithRetryAsync(cancellationToken).ConfigureAwait(false);
            if (_state.IsFresh) _state.Set(0, ReplicationRole.Replica, _cfg.PeerId);
        }
        _db = await OpenDatabaseAsync(cancellationToken).ConfigureAwait(false);
        // A chunk failed to apply before this node stopped, and its log does not end where it ended then (the engine
        // published the failed range, or the restart replayed the failed write): the log end no longer says what this
        // replica holds. Rebuild before following or being promoted.
        if (replica && ReplicaApplyGuard.TryReadMarker(_fs, _cfg.DbPath, out ulong appliedBeforeFailure) && ReplicaApplyGuard.IsLogEndUnknown(_db))
        {
            Log("a chunk failed to apply before the restart and the log no longer ends at " + appliedBeforeFailure + ": rebuilding from a snapshot");
            await HostResync.CloseDatabaseAsync(_db, Log).ConfigureAwait(false);
            await PullSnapshotWithRetryAsync(cancellationToken).ConfigureAwait(false);
            _db = await OpenDatabaseAsync(cancellationToken).ConfigureAwait(false);
        }
        if (_cfg.InitialRole == ReplicationRole.Primary) SwitchoverNode.BootstrapPrimary(_state, _db, _cfg.NodeId);
        bool restarted = !_state.IsFresh;
        _node = new SwitchoverNode(_cfg.NodeId, _cfg.PeerId, _state, _db, DetachFromPrimaryAsync);

        // A node restarting as primary may have been replaced while it was down (ForcePromoteAsync on the other node):
        // ask before serving writes. If the other node cannot be reached, serve anyway - with two nodes there is nobody
        // else to ask.
        if (_node.Role != ReplicationRole.Replica)
        {
            SwitchoverResult? resolved = await _node.ResolveAsync(_peer, _cfg.Switchover, cancellationToken).ConfigureAwait(false);
            if (resolved != null) Log("resolve at startup: " + resolved.Outcome + " (epoch " + resolved.Epoch + ") " + resolved.Reason);
            // Still primary after a restart: the recovered log may end before entries the replica holds (unless every
            // write is fsync'd). If the replica can show they are this node's own, it takes over and this node gets
            // them back from it; rebuilding the replica from this node would lose them.
            bool newEpoch = resolved?.Outcome == SwitchoverOutcome.Aborted;
            if (restarted && _node.Role == ReplicationRole.Primary && !newEpoch && _cfg.YieldToReplicaAheadOnRestart)
            {
                SwitchoverResult yielded = await _node.YieldToReplicaAheadAsync(_peer, _peer.GetEpochsAsync, _cfg.Switchover, cancellationToken).ConfigureAwait(false);
                if (yielded.Outcome != SwitchoverOutcome.Rejected)
                    Log("restarted behind " + _cfg.PeerId + ": " + yielded.Outcome + " (epoch " + yielded.Epoch + ", handoff seq " + yielded.HandoffSeq + ") " + yielded.Reason);
                newEpoch = yielded.Outcome == SwitchoverOutcome.Aborted;
            }
            // Continuing as primary: what this node writes from here on belongs to a new epoch, so that a tail the
            // replica holds past the recovered log fails the attach check instead of passing for this log. A handoff
            // aborted just now already started one.
            if (restarted && _node.Role == ReplicationRole.Primary && !newEpoch) _node.StartNewEpochAfterRestart();
        }
        _node.RoleChanged += (_, e) =>
        {
            Log("role -> " + e.Role + " in epoch " + e.Epoch + " (primary: " + (e.PrimaryId ?? "unknown") + ")");
            lock (_followerLock)
            {
                // Asynchronously: Cancel() would tear down the WAL streams this node serves (gRPC calls) inline, delaying the
                // end of a handoff by tens of milliseconds (rocksdb-sharp Switchover/README.md "Measured cost").
                if (e.Role == ReplicationRole.Replica) _ = _primaryCts.CancelAsync();
                else if (_primaryCts.IsCancellationRequested) _primaryCts = new CancellationTokenSource();
            }
            // What the replica reported is about the log this node had as primary; as a replica it may be rebuilt.
            if (e.Role == ReplicationRole.Replica) ResetReplicaProgress();
            if (e.Role == ReplicationRole.Replica) EnsureFollowerStream();
        };
        if (_node.Role == ReplicationRole.Replica) _primaryCts.Cancel();
        Log("started as " + _node.Role + " in epoch " + _node.Epoch + " at seq " + _db.GetLatestSequenceNumber());
        _serving = true;
        _started.TrySetResult();
        if (_node.Role == ReplicationRole.Replica) EnsureFollowerStream();
        _resolveTask = Task.Run(ResolveLoopAsync);
    }

    /// <summary>
    /// Settles a handoff this node could not finish - the other node was unreachable, or this node restarted while
    /// handing off (rocksdb-sharp PairNodeHost.ResolveLoopAsync). <see cref="SwitchoverNode.ResolveAsync"/> does nothing
    /// while a handoff is running.
    /// </summary>
    private async Task ResolveLoopAsync()
    {
        CancellationToken ct = _cts.Token;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                if (_node.Role == ReplicationRole.HandingOff)
                {
                    SwitchoverResult? r = await _node.ResolveAsync(_peer, _cfg.Switchover, ct).ConfigureAwait(false);
                    if (r != null) Log("resolve: " + r.Outcome + " (epoch " + r.Epoch + ") " + r.Reason);
                }
                await Task.Delay(_cfg.ResolveInterval, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                Log("resolve failed: " + ex.Message);
                try
                {
                    await Task.Delay(_cfg.ResolveInterval, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }
    }

    private async ValueTask<RocksDatabase> OpenDatabaseAsync(CancellationToken cancellationToken)
    {
        DbOptions o = _cfg.DbOptionsFactory();
        o.CreateIfMissing = true;
        // While this node is the replica, the engine must not recover from a background error on its own: the recovery
        // from a failed WAL write publishes the sequence numbers of a chunk that was not applied (ReplicaApplyGuard).
        // The node rebuilds from a snapshot instead.
        o.Listeners.Add(new ReplicaRecoveryVeto(() => Volatile.Read(ref _node) is { } n && n.Role == ReplicationRole.Replica));
        return await RocksDatabase.OpenAsync(_cfg.DbPath, o, cancellationToken).ConfigureAwait(false);
    }

    // ---------------------------------------------------------------------------------------------------
    // Client writes and switchover

    /// <summary>
    /// Applies a client write if this node is the primary; while a handoff holds writes it waits up to
    /// <paramref name="holdTimeout"/>. Otherwise answers <see cref="PairWriteStatus.NotPrimary"/> with a hint.
    /// Allocation-free in steady state.
    /// <para>
    /// What <see cref="PairWriteStatus.Applied"/> promises depends on <see cref="PairNodeHostOptions.SyncWal"/> and
    /// <see cref="PairNodeHostOptions.WriteAck"/>. By default (no fsync, acknowledged by the primary alone) the write
    /// survives a crash of the process, and a power loss of the primary only if the replica already held it and can be
    /// reached when the primary restarts. With <see cref="PairNodeHostOptions.SyncWal"/> it is on the primary's disk.
    /// With <see cref="PairWriteAck.Replica"/> it is on both nodes. An exception from the database write is
    /// indeterminate, like <see cref="PairWriteStatus.NotReplicated"/>.
    /// </para>
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<PairWriteResult> WriteAsync(WriteBatch batch, TimeSpan holdTimeout, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(batch);
        if (!_serving)
        {
            // Not before StartAsync has settled this node's role: a node restarting as primary first asks the other
            // node whether it was replaced, and whether to hand over to a replica that is ahead of the recovered log
            // (YieldToReplicaAheadAsync); a write applied meanwhile would be acknowledged in the old epoch at a sequence
            // number the replica holds another entry for. Such a write waits like one held by a handoff.
            long deadline = Environment.TickCount64 + (long)Math.Min(holdTimeout.TotalMilliseconds, int.MaxValue);
            if (!await WaitUntilServingAsync(holdTimeout, cancellationToken).ConfigureAwait(false))
                return new PairWriteResult(PairWriteStatus.NotPrimary, null, 0);
            long remaining = deadline - Environment.TickCount64;
            holdTimeout = remaining > 0 ? TimeSpan.FromMilliseconds(remaining) : TimeSpan.Zero;
        }
        if (!await _node.Writes.TryEnterAsync(holdTimeout, cancellationToken).ConfigureAwait(false))
            return new PairWriteResult(PairWriteStatus.NotPrimary, _node.PrimaryId == _cfg.NodeId ? null : _node.PrimaryId, 0);
        try
        {
            await _db.WriteAsync(_cfg.SyncWal ? WriteOptions.SyncWrites : null, batch, cancellationToken).ConfigureAwait(false);
            ulong seq = _db.GetLatestSequenceNumber();
            // Inside the write gate: a handoff takes its handoff point only after the writes in flight were
            // acknowledged or gave up, so no acknowledgement is ever pending across a change of primary.
            if (_cfg.WriteAck == PairWriteAck.Replica && !await WaitForReplicaAsync(seq, cancellationToken).ConfigureAwait(false))
                return new PairWriteResult(PairWriteStatus.NotReplicated, null, seq);
            return new PairWriteResult(PairWriteStatus.Applied, null, seq);
        }
        finally
        {
            _node.Writes.Exit();
        }
    }

    private async ValueTask<bool> WaitUntilServingAsync(TimeSpan timeout, CancellationToken cancellationToken)
    {
        if (timeout <= TimeSpan.Zero) return false;
        try
        {
            await _started.Task.WaitAsync(TimeSpan.FromMilliseconds(Math.Min(timeout.TotalMilliseconds, int.MaxValue)), cancellationToken).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            return false;
        }
        // False when the host was disposed before it ever served.
        return _serving;
    }

    /// <summary>
    /// A report of the replica (<see cref="IPairNodeClient.ReportProgressAsync"/>): it holds this node's log through
    /// <paramref name="appliedThrough"/>. Counted only while this node is primary (or handing off) and holds that
    /// entry in the same epoch - the attach check's rule, under which the replica's log through that entry is this
    /// node's. Anything else (a report that was on the wire across a change of primary, a replica with another
    /// history) is dropped.
    /// </summary>
    public void HandleReportProgress(ulong appliedThrough, long epochAtApplied)
    {
        if (!_serving || appliedThrough == 0) return;
        int generation = Volatile.Read(ref _ackGeneration);
        ReplicaLogCheck check = _node.CheckReplicaLog(appliedThrough, epochAtApplied);
        if (!check.IsPrimary || !check.Consistent) return;
        lock (_ackLock)
        {
            // A report checked before a demotion is not counted after it.
            if (generation != _ackGeneration) return;
            if (appliedThrough > _replicaAckedSeq) _replicaAckedSeq = appliedThrough;
            _replicaProgress.SignalAll();
        }
    }

    /// <summary>The highest sequence number the replica reported holding since this node became primary (0 if none).</summary>
    public ulong ReplicaAckedSequence
    {
        get
        {
            lock (_ackLock) return _replicaAckedSeq;
        }
    }

    private void ResetReplicaProgress()
    {
        lock (_ackLock)
        {
            _ackGeneration++;
            _replicaAckedSeq = 0;
            _replicaProgress.SignalAll();
        }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<bool> WaitForReplicaAsync(ulong seq, CancellationToken cancellationToken)
    {
        bool linked = cancellationToken.CanBeCanceled;
        CancellationTokenSource cts = linked ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken) : RentTimeoutSource();
        try
        {
            cts.CancelAfter(_cfg.ReplicaAckTimeout);
            while (true)
            {
                RocksDb.Managed.Util.AsyncBroadcastSignal.Waiter changed;
                lock (_ackLock)
                {
                    if (_replicaAckedSeq >= seq) return true;
                    // Not primary any more (replaced by a forced promotion), or stopping: nobody will report it here.
                    if (_cts.IsCancellationRequested || _node.Role == ReplicationRole.Replica) return false;
                    // Registered under the lock every signal is raised under: no wake-up can be missed.
                    changed = _replicaProgress.Register();
                }
                await _replicaProgress.WaitAsync(changed, cts.Token).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return false;
        }
        finally
        {
            if (linked) cts.Dispose();
            else ReturnTimeoutSource(cts);
        }
    }

    private CancellationTokenSource RentTimeoutSource()
    {
        lock (_timeoutSources)
        {
            if (_timeoutSources.TryPop(out CancellationTokenSource? cts)) return cts;
        }
        return new CancellationTokenSource();
    }

    private void ReturnTimeoutSource(CancellationTokenSource cts)
    {
        if (cts.TryReset())
        {
            lock (_timeoutSources)
            {
                if (_timeoutSources.Count < 64)
                {
                    _timeoutSources.Push(cts);
                    return;
                }
            }
        }
        cts.Dispose();
    }

    public Task<SwitchoverResult> HandOffAsync(CancellationToken cancellationToken) => HandOffAsync(_cfg.Switchover, cancellationToken);

    public async Task<SwitchoverResult> HandOffAsync(SwitchoverOptions options, CancellationToken cancellationToken)
    {
        Log("handoff to " + _cfg.PeerId + " requested");
        SwitchoverResult r = await _node.HandOffAsync(_peer, options, cancellationToken).ConfigureAwait(false);
        Log("handoff to " + _cfg.PeerId + ": " + r.Outcome + " (epoch " + r.Epoch + ", handoff seq " + r.HandoffSeq + ", writes held "
            + r.WritesHeld.TotalMilliseconds.ToString("F1", System.Globalization.CultureInfo.InvariantCulture) + "ms) " + r.Reason);
        return r;
    }

    /// <summary>Settles a handoff this node could not finish (after a restart, or when the other node was unreachable).</summary>
    public Task<SwitchoverResult?> ResolveAsync(CancellationToken cancellationToken) => _node.ResolveAsync(_peer, _cfg.Switchover, cancellationToken);

    public async Task<long> ForcePromoteAsync(CancellationToken cancellationToken) => await _node.ForcePromoteAsync(cancellationToken).ConfigureAwait(false);

    public PairNodeStatus GetStatus() =>
        new(GetNodeStatus(), _resyncing, Resyncs, _node.Writes.IsOpen, Volatile.Read(ref _attachedFromSeq)) { FollowerFault = _followerFault };

    public DatabaseDigest? Checksum()
    {
        if (!_dbGate.TryEnter()) return null;
        try
        {
            return DatabaseDigest.Compute(_db);
        }
        finally
        {
            _dbGate.Exit();
        }
    }

    // ---------------------------------------------------------------------------------------------------
    // Incoming RPCs

    /// <summary>Status for the other node; null while resyncing (an answer then would be about no database).</summary>
    public ReplicationNodeStatus? HandleGetStatus() => _resyncing ? null : GetNodeStatus();

    private ReplicationNodeStatus GetNodeStatus() => _node.GetStatus();

    public Task<PromoteResult> HandlePromoteAsync(PromoteRequest request) => _node.HandlePromoteAsync(request);

    public async Task<ReplicationNodeStatus?> HandleRejectEpochAsync(long epoch)
    {
        ReplicationNodeStatus s = await _node.HandleRejectEpochAsync(epoch).ConfigureAwait(false);
        return _resyncing ? null : s;
    }

    public ReplicaLogCheck HandleCheckReplicaLog(ulong replicaLatestSeq, long replicaEpochAtLatest) =>
        // Not before this node settled its role at startup: "inconsistent" makes the replica rebuild itself.
        _serving ? _node.CheckReplicaLog(replicaLatestSeq, replicaEpochAtLatest) : new ReplicaLogCheck(false, false, 0, string.Empty);

    public string HandleGetEpochs() => _node.SerializeEpochs();

    /// <summary>
    /// The WAL stream for the replica while this node is primary (or handing off). A write batch larger than
    /// <see cref="PairNodeHostOptions.MaxChunkBytes"/> is sent in pieces (<see cref="WalStreamMessage.IsFragment"/>).
    /// </summary>
    public async IAsyncEnumerable<WalStreamMessage> StreamUpdatesAsync(ulong startSeq, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        CancellationToken primary;
        lock (_followerLock) primary = _primaryCts.Token;
        if (!_serving || _node.Role == ReplicationRole.Replica || primary.IsCancellationRequested || !_dbGate.TryEnter()) yield break;
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _cts.Token, primary);
            SwitchoverNode node = _node;
            await foreach (WalStreamMessage m in WalStreamSender.StreamAsync(_db, startSeq, Tag, node.GetEpochForSeq, _cfg.MaxChunkBytes, OnWalStreamFault, cts.Token)
                               .ConfigureAwait(false))
            {
                // The replica is reading the WAL: the files kept for its snapshot may go.
                if (_attachPin.HasHolds) _attachPin.ReleaseAll();
                yield return m;
            }
        }
        finally
        {
            _dbGate.Exit();
        }
    }

    private bool Tag(out string sourceId, out long epoch)
    {
        sourceId = _cfg.NodeId;
        epoch = _node.Epoch;
        // A node handing off keeps streaming: the replica needs every entry up to the handoff point.
        return _node.Role != ReplicationRole.Replica;
    }

    /// <summary>A full snapshot for the replica (empty when this node is a replica).</summary>
    public async IAsyncEnumerable<SnapshotChunk> StreamSnapshotAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (!_serving || _node.Role == ReplicationRole.Replica || !_dbGate.TryEnter()) yield break;
        try
        {
            string dir = _cfg.DbPath.TrimEnd('/') + ".checkpoint-" + Guid.NewGuid().ToString("N");
            using ReplicationSession session = await new ReplicationSource(_db).GetInitialStateAsync(dir, cancellationToken).ConfigureAwait(false);
            // The epoch map travels with the files it describes, captured now (see ClusterNodeHost.StreamSnapshotAsync).
            string epochs = _node.SerializeEpochs();
            // A full transfer: after a switchover the two nodes flushed their own SSTs, so a delta would reuse little.
            await foreach (SnapshotChunk c in session.StreamSnapshotAsync([], _cfg.SnapshotChunkBytes, cancellationToken).ConfigureAwait(false))
            {
                yield return c.Kind == SnapshotChunkKind.Plan ? c.WithLogDescription(epochs) : c;
            }
            // Sent completely: keep the WAL until the replica attaches (before the session re-enables deletions).
            _attachPin.HoldUntilAttach(_db, _cfg.SnapshotAttachTimeout);
        }
        finally
        {
            _dbGate.Exit();
        }
    }

    // ---------------------------------------------------------------------------------------------------
    // Replica side: the WAL stream from the primary

    // Set when a failed apply could not be recorded durably: this node stops following until it is rebuilt.
    private volatile string? _followerFault;

    /// <summary>
    /// Why this node stopped following for good, or null. Set when a chunk failed to apply and the failure could not be
    /// recorded in the database directory (<see cref="ReplicaApplyGuard"/>): nothing durable would tell a later process
    /// that its log end may not say what it holds, so the node does not go on as if nothing happened. A resync
    /// (<see cref="TriggerResync"/>) clears it.
    /// </summary>
    public string? FollowerFault => _followerFault;

    private void EnterFollowerFault(string reason)
    {
        _followerFault = reason;
        Log("FOLLOWER FAULT, not following any more (rebuild this node with a resync): " + reason);
    }

    private void EnsureFollowerStream()
    {
        lock (_followerLock)
        {
            if (_resyncing || _cts.IsCancellationRequested || _followerFault is not null) return;
            if (_followerCts != null && _followerTask is { IsCompleted: false }) return;
            // Not linked to _cts: a linked source keeps a registration on the host's source until it is disposed, one
            // per attach for the life of the host, and it cannot be disposed safely while another flow may still cancel
            // it. DisposeAsync cancels the current one instead (under this lock, after _cts, which is checked above).
            _followerCts = new CancellationTokenSource();
            CancellationToken ct = _followerCts.Token;
            Task previous = _followerTask ?? Task.CompletedTask;
            _followerTask = Task.Run(async () =>
            {
                try
                {
                    await previous.ConfigureAwait(false);
                }
                catch (Exception)
                {
                }
                await RunFollowerStreamAsync(ct).ConfigureAwait(false);
            });
        }
    }

    /// <summary>
    /// Called by the switchover once this replica holds the handoff point: returns as soon as nothing ingests into the
    /// database any more. Writes are held until then, so it does not wait for the stream to shut down - only for a chunk
    /// being applied; the stream task sees its token and ends on its own.
    /// </summary>
    private async Task DetachFromPrimaryAsync(CancellationToken cancellationToken)
    {
        CancellationTokenSource? cts;
        lock (_followerLock)
        {
            cts = _followerCts;
            _followerCts = null;
        }
        if (cts != null) _ = cts.CancelAsync();
        await _ingestLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        _ingestLock.Release();
    }

    private async Task StopFollowerStreamAsync()
    {
        Task? running;
        lock (_followerLock)
        {
            _followerCts?.Cancel();
            _followerCts = null;
            running = _followerTask;
        }
        if (running != null)
        {
            try
            {
                await running.ConfigureAwait(false);
            }
            catch (Exception)
            {
            }
        }
    }

    private async Task RunFollowerStreamAsync(CancellationToken ct)
    {
        bool stalled = false;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                // A chunk that failed to apply earlier, and a log that no longer ends where it ended then: the failed
                // range was published without its data (ReplicaApplyGuard). Attaching would stream from behind it.
                if (ReplicaApplyGuard.IsLogEndUnknown(_db))
                {
                    if (ct.IsCancellationRequested) return;
                    TriggerResync("a chunk failed to apply and the log end moved past it: the log end is unknown");
                    return;
                }
                // The attach check: our log must be a prefix of the primary's.
                ulong latest = _db.GetLatestSequenceNumber();
                ReplicaLogCheck? check = await _peer.CheckReplicaLogAsync(latest, _node.GetEpochForSeq(latest), ct).ConfigureAwait(false);
                if (check is null || !check.IsPrimary)
                {
                    await Task.Delay(_cfg.RetryDelay, ct).ConfigureAwait(false);
                    continue;
                }
                if (!check.Consistent)
                {
                    // An answer that arrives after this stream was told to stop (this node was promoted, or stops) is
                    // about a role it no longer has: it must not drop the database.
                    if (ct.IsCancellationRequested) return;
                    TriggerResync("log at " + latest + " (epoch " + _node.GetEpochForSeq(latest) + ") is not a prefix of " + _cfg.PeerId + "'s");
                    return;
                }
                if (!_node.AcceptStream(_cfg.PeerId, check.Epoch))
                {
                    Log(_cfg.PeerId + " is primary in epoch " + check.Epoch + ", older than " + _node.Epoch + "; not following it");
                    await Task.Delay(_cfg.RetryDelay, ct).ConfigureAwait(false);
                    continue;
                }
                _state.MergeEpochs(check.Epochs);
                long seenEpoch = check.Epoch;
                Volatile.Write(ref _attachedFromSeq, latest + 1);
                Log("following " + _cfg.PeerId + " (epoch " + check.Epoch + ") from seq " + (latest + 1));

                // Synced chunks (SyncWal): a promotion and the reports below only ever rely on durable entries.
                var consumer = new ReplicationConsumer(_db) { SyncWrites = _cfg.SyncWal };
                // The report names the epoch this node has for the entry; the primary counts it only if it agrees.
                await using ProgressReporter? progress = _cfg.WriteAck == PairWriteAck.Replica
                    ? new ProgressReporter((seq, c) => _peer.ReportProgressAsync(seq, _node.GetEpochForSeq(seq), c), _cfg.RetryDelay)
                    : null;
                progress?.Report(latest);
                // Per attachment: a stream that ends or fails takes its partial batch with it.
                using var assembler = new WalStreamAssembler(_cfg.MaxBatchBytes);
                await foreach (WalStreamMessage m in _peer.StreamUpdatesAsync(latest + 1, ct).ConfigureAwait(false))
                {
                    if (m.ResyncRequired)
                    {
                        if (!_resyncPolicy.ShouldRebuild(m, _db.GetLatestSequenceNumber()))
                        {
                            // The snapshot this node was just rebuilt from did not get it past the fault of the
                            // primary's WAL; another one would not either.
                            if (!stalled) Log(_cfg.PeerId + " still cannot stream from " + (_db.GetLatestSequenceNumber() + 1) + " after a resync; retrying the stream: " + m.Reason);
                            stalled = true;
                            break;
                        }
                        TriggerResync(_cfg.PeerId + " cannot stream from " + (_db.GetLatestSequenceNumber() + 1) + ": " + m.Reason);
                        return;
                    }
                    stalled = false;
                    while (Faults.PauseIngest) await Task.Delay(5, ct).ConfigureAwait(false);
                    if (!_node.AcceptStream(m.SourceId, m.SourceTerm))
                    {
                        Log("dropping the stream from " + m.SourceId + ": epoch " + m.SourceTerm + " is older than " + _node.Epoch);
                        break;
                    }
                    if (m.SourceTerm != seenEpoch)
                    {
                        // The primary moved to a new epoch (it resumed after an aborted handoff): learn where it starts.
                        string? epochs = await _peer.GetEpochsAsync(ct).ConfigureAwait(false);
                        if (epochs != null) _state.MergeEpochs(epochs);
                        seenEpoch = m.SourceTerm;
                    }
                    WalAssemblyStatus assembled = assembler.Accept(m, out ReadOnlyMemory<byte> chunk);
                    if (assembled != WalAssemblyStatus.Complete)
                    {
                        // Nothing of an incomplete batch is applied or reported.
                        if (assembled == WalAssemblyStatus.Incomplete) continue;
                        ulong at = _db.GetLatestSequenceNumber();
                        if (assembled == WalAssemblyStatus.TooLarge || _resyncPolicy.ShouldRebuildAfterStreamFault(at))
                        {
                            // The stream cannot get this node past the batch; a snapshot carries it as files.
                            TriggerResync(_cfg.PeerId + "'s stream cannot be followed from " + (at + 1) + ": " + assembler.LastError);
                            return;
                        }
                        Log("dropping the stream from " + _cfg.PeerId + " at " + (at + 1) + ": " + assembler.LastError);
                        break;
                    }
                    await _ingestLock.WaitAsync(ct).ConfigureAwait(false);
                    try
                    {
                        if (ct.IsCancellationRequested) return;
                        // A replica applies the primary's entries at exactly the same sequence numbers - which is what
                        // lets it take over at the handoff point. Anything else diverges.
                        ulong appliedThrough = _db.GetLatestSequenceNumber();
                        bool applied;
                        try
                        {
                            applied = await consumer.TryIngestChunkAsync(chunk, CancellationToken.None).ConfigureAwait(false);
                        }
                        catch (Exception ex) when (ex is not OutOfMemoryException)
                        {
                            // The write failed: whatever the engine does with the chunk's sequence numbers from here
                            // on, this node holds its log through appliedThrough and nothing after it.
                            if (!ReplicaApplyGuard.RecordFailure(_db, appliedThrough))
                            {
                                EnterFollowerFault("the chunk at " + (appliedThrough + 1) + " could not be applied (" + ex.Message +
                                    ") and the failure could not be recorded in the database directory");
                                return;
                            }
                            if (_resyncPolicy.ShouldRebuildAfterApplyFault(appliedThrough))
                            {
                                TriggerResync("the chunk at " + (appliedThrough + 1) + " cannot be applied: " + ex.Message);
                                return;
                            }
                            throw;
                        }
                        if (!applied)
                        {
                            TriggerResync("stream mismatch: expected seq " + consumer.ExpectedNextSequence + ", got " + m.FirstSequence);
                            return;
                        }
                        progress?.Report(_db.GetLatestSequenceNumber());
                    }
                    finally
                    {
                        _ingestLock.Release();
                    }
                }
                await Task.Delay(_cfg.RetryDelay, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                Log("stream from " + _cfg.PeerId + " failed: " + ex.GetType().Name + ": " + ex.Message + "; retrying");
                try
                {
                    await Task.Delay(_cfg.RetryDelay, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }
    }

    // ---------------------------------------------------------------------------------------------------
    // Snapshot: initial sync and resync

    private async Task PullSnapshotWithRetryAsync(CancellationToken cancellationToken)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _cts.Token);
        for (int attempt = 0; ; attempt++)
        {
            try
            {
                await PullSnapshotAsync(linked.Token).ConfigureAwait(false);
                return;
            }
            catch (Exception ex) when (attempt < 100 && !linked.IsCancellationRequested)
            {
                Log("snapshot from " + _cfg.PeerId + " failed: " + ex.Message + "; retrying");
                await Task.Delay(_cfg.RetryDelay, linked.Token).ConfigureAwait(false);
            }
        }
    }

    private async Task PullSnapshotAsync(CancellationToken ct)
    {
        // The restored log is byte-for-byte the primary's, so its epoch map is the right description of it. It comes with
        // the snapshot's plan and is persisted before CURRENT appears, so the files and the map are from one moment.
        // A snapshot without the map is refused. While this node has a database, the snapshot is restored next to it and
        // swapped in (SnapshotRestoreMode; the role state moves with the swap, under its own lock), so the node keeps a
        // copy it can be promoted on if the primary dies during the resync.
        SnapshotRestoreResult r = await SnapshotReceiver.ReceiveAsync(_peer.StreamSnapshotAsync(ct), _fs, _cfg.DbPath, [RoleDirName],
            new SnapshotRestoreOptions
            {
                Mode = _cfg.ResyncRestoreMode,
                FreeSpaceMarginBytes = _cfg.ResyncFreeSpaceMarginBytes,
                RequireLogDescription = true,
                OnLogDescription = description => _state.ReplaceEpochs(description),
                SwapGuard = _state.RunExclusive,
                Log = Log,
            }, ct).ConfigureAwait(false);
        Log("snapshot from " + _cfg.PeerId + ": " + r.TransferredFiles + " files, " + r.TransferredBytes + " bytes" + (r.Swapped ? " (swapped in)" : ""));
    }

    /// <summary>
    /// Rebuilds this replica from the primary's snapshot (also triggered automatically when the logs diverged or the
    /// primary can no longer stream from this node's position). Ignored on a node that is not a replica.
    /// </summary>
    public void TriggerResync(string reason)
    {
        // Read outside _followerLock: role changes are raised under the node's lock and take _followerLock.
        if (_node.Role != ReplicationRole.Replica)
        {
            Log("resync not started (" + reason + "): this node is " + _node.Role);
            return;
        }
        lock (_followerLock)
        {
            if (_resyncing || _cts.IsCancellationRequested) return;
            _resyncing = true;
            // The rebuild replaces the log whose end was in doubt.
            _followerFault = null;
            Log("resync: " + reason);
            _resyncTask = Task.Run(PerformResyncAsync);
        }
    }

    /// <summary>Completes when no resync is running.</summary>
    public Task WaitForResyncAsync()
    {
        lock (_followerLock) return _resyncTask;
    }

    private async Task PerformResyncAsync()
    {
        await _resyncGate.WaitAsync().ConfigureAwait(false);
        try
        {
            try
            {
                _node.DetachDatabase();
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                // InvalidOperationException: promoted between the trigger and here; a primary keeps its database.
                // Anything else is as unexpected as it is final for this attempt. Either way the database stays
                // attached, and leaving the flag set would make this node answer "no status" for good.
                lock (_followerLock) _resyncing = false;
                Log("resync abandoned: " + e.Message);
                // A replica follows again, so that a divergence still there is seen again and the resync retried.
                if (e is not InvalidOperationException && _node.Role == ReplicationRole.Replica) EnsureFollowerStream();
                return;
            }
            await StopFollowerStreamAsync().ConfigureAwait(false);
            await _dbGate.DrainAsync(_cts.Token).ConfigureAwait(false);
            await HostResync.CloseDatabaseAsync(_db, Log).ConfigureAwait(false);
            while (!_cts.IsCancellationRequested)
            {
                try
                {
                    await PullSnapshotAsync(_cts.Token).ConfigureAwait(false);
                    _db = await OpenDatabaseAsync(_cts.Token).ConfigureAwait(false);
                    _node.AttachDatabase(_db);
                    _resyncPolicy.OnRebuilt(_db.GetLatestSequenceNumber());
                    _dbGate.Reopen();
                    Interlocked.Increment(ref _resyncs);
                    lock (_followerLock) _resyncing = false;
                    Log("resync complete at seq " + _db.GetLatestSequenceNumber());
                    EnsureFollowerStream();
                    return;
                }
                catch (Exception ex) when (!_cts.IsCancellationRequested)
                {
                    Log("resync attempt failed: " + ex.Message + "; retrying");
                    await Task.Delay(_cfg.RetryDelay, _cts.Token).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            _resyncGate.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync().ConfigureAwait(false);
        lock (_followerLock)
        {
            _primaryCts.Cancel();
            _followerCts?.Cancel(); // not linked to _cts (EnsureFollowerStream)
        }
        // Writes waiting for the replica's report give up (NotReplicated).
        lock (_ackLock) _replicaProgress.SignalAll();
        // Writes still waiting for the start give up (NotPrimary: _serving stays false).
        _started.TrySetResult();
        if (_node != null)
        {
            // Sealed first: a promotion or a handoff being settled right now (ResumeAsPrimary, RunPromotionAsync,
            // ForcePromoteAsync) must not reopen the gate once it is closed here.
            _node.Writes.Seal();
            // Let admitted client writes finish, refuse new ones.
            await _node.Writes.HoldAsync(CancellationToken.None).ConfigureAwait(false);
            _node.Writes.Close();
        }
        await StopFollowerStreamAsync().ConfigureAwait(false);
        try
        {
            await _resyncTask.ConfigureAwait(false);
        }
        catch (Exception)
        {
        }
        try
        {
            await _resolveTask.ConfigureAwait(false);
        }
        catch (Exception)
        {
        }
        // Bounded: a stream whose consumer stopped reading keeps its handler (and the gate) until the transport gives up.
        using (var drain = new CancellationTokenSource(_cfg.DisposeDrainTimeout))
        {
            try
            {
                await _dbGate.DrainAsync(drain.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                Log("closing the database although an RPC handler still uses it (not finished within " + _cfg.DisposeDrainTimeout + ")");
            }
            catch (Exception)
            {
            }
        }
        if (_db != null) await _db.DisposeAsync().ConfigureAwait(false);
    }

    private void Log(string message)
    {
        try
        {
            _cfg.Log?.Invoke(_cfg.NodeId + ": " + message);
        }
        catch (Exception)
        {
            // The application's logger: an exception from it must not escape a catch block or end a loop of this host.
        }
    }
}
