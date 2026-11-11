using System.Diagnostics;

namespace RocksDb.Managed.Replication.Switchover;

/// <summary>
/// Planned switchover for a primary/replica pair built on the WAL streaming of this library (port of rocksdb-sharp
/// <c>SwitchoverNode</c>): the primary hands its role to an up-to-date replica without losing an acknowledged write, so
/// either node can be stopped and upgraded while the other serves.
/// <para>
/// Transport-agnostic, like <see cref="Cluster.RaftClusterNode"/>: the host owns the network, the database and the
/// replication stream, routes the incoming RPCs to <see cref="HandlePromoteAsync"/>, <see cref="HandleRejectEpochAsync"/>,
/// <see cref="CheckReplicaLog"/> and <see cref="GetStatus"/>, and gives this class an <see cref="ISwitchoverPeer"/> to
/// call the other node with.
/// </para>
/// The handoff (<see cref="HandOffAsync"/>), from the primary's side:
/// <list type="number">
/// <item>check the replica is following this node and close to its tail - refusing here costs nothing;</item>
/// <item>persist <see cref="ReplicationRole.HandingOff"/>, so a crash from here on restarts this node read-only;</item>
/// <item>hold writes (<see cref="PrimaryWriteGate.HoldAsync"/>): new writes wait, in-flight ones finish, and the latest
/// sequence number - the handoff point - is final;</item>
/// <item>ask the replica to promote itself in epoch + 1 once it holds exactly the handoff point; it persists that
/// promise before it acts;</item>
/// <item>promoted: close the gate - held writes are refused and redirected - persist Replica and let the host follow the
/// new primary from the handoff point (its log is a prefix of the new primary's, so no snapshot);</item>
/// <item>not promoted, or no answer: make the replica promise the proposed epoch (<see cref="ISwitchoverPeer.RejectEpochAsync"/>)
/// so a delayed promote cannot succeed, and act on its answer: it is primary (complete the demotion) or it is not
/// (resume as primary in a newer epoch). If it cannot be reached the outcome is <see cref="SwitchoverOutcome.Unresolved"/>
/// and writes stay off: two writable primaries are worse than none.</item>
/// </list>
/// <para>Managed differences: a promoted replica waits for the handoff point with
/// <see cref="RocksDatabase.WaitForSequenceAsync"/> (signalled by its write path) instead of spinning and polling; and
/// before holding writes the primary sends a probe promote (<see cref="PromoteRequest.Probe"/>, refused without side
/// effects) so that the promote round trip made while writes are held runs warm. The first promote call of a process
/// otherwise pays one-time costs on both ends (gRPC/MagicOnion method binding, serializer and JIT warm-up) - measured at
/// 15-30 ms per side over MagicOnion on loopback. The fsync'd role-state writes made while writes are held are also
/// fewer: the promoted replica persists its epoch map and role together (<see cref="ReplicationRoleState.BecomePrimary"/>)
/// and the old primary closes its gate before persisting its demotion (it is durably HandingOff already).</para>
/// </summary>
public sealed class SwitchoverNode
{
    private readonly string _nodeId;
    private readonly ReplicationRoleState _state;
    private readonly Func<CancellationToken, Task> _detachFromPrimaryAsync;
    private readonly Lock _gate = new();
    private readonly SemaphoreSlim _handoff = new(1, 1);

    private RocksDatabase? _db;
    private Task<PromoteResult>? _promotion;
    private CancellationTokenSource? _promotionCts;
    private bool _rejected;

    public event EventHandler<ReplicationRoleChangedEventArgs>? RoleChanged;

    /// <param name="nodeId">This node's id.</param>
    /// <param name="state">Its persisted role state.</param>
    /// <param name="db">Its database.</param>
    /// <param name="detachFromPrimaryAsync">
    /// Called on a replica that is about to become primary, after it caught up to the handoff point: stop the replication
    /// stream and return once nothing will ingest into the database any more.
    /// </param>
    public SwitchoverNode(string nodeId, ReplicationRoleState state, RocksDatabase db, Func<CancellationToken, Task> detachFromPrimaryAsync)
    {
        ArgumentException.ThrowIfNullOrEmpty(nodeId);
        ArgumentNullException.ThrowIfNull(state);
        ArgumentNullException.ThrowIfNull(db);
        ArgumentNullException.ThrowIfNull(detachFromPrimaryAsync);
        _nodeId = nodeId;
        _state = state;
        _db = db;
        _detachFromPrimaryAsync = detachFromPrimaryAsync;
        Writes = new PrimaryWriteGate(open: state.Role == ReplicationRole.Primary);
    }

    /// <summary>
    /// Writes the first role state of the node a new pair starts from: primary in epoch 1, with every entry from the next
    /// sequence number on in that epoch. Does nothing if the node already has role state.
    /// </summary>
    public static void BootstrapPrimary(ReplicationRoleState state, RocksDatabase db, string nodeId)
    {
        ArgumentNullException.ThrowIfNull(state);
        ArgumentNullException.ThrowIfNull(db);
        if (!state.IsFresh || state.Epoch != 0) return;
        state.StartEpoch(db.GetLatestSequenceNumber() + 1, 1);
        state.Set(1, ReplicationRole.Primary, nodeId);
    }

    public string NodeId => _nodeId;
    public ReplicationRole Role => _state.Role;
    public long Epoch => _state.Epoch;
    public string? PrimaryId => _state.PrimaryId;

    /// <summary>Client writes go through this gate; it is open only on the primary.</summary>
    public PrimaryWriteGate Writes { get; }

    public long GetEpochForSeq(ulong seq) => _state.GetEpochForSeq(seq);

    public string SerializeEpochs() => _state.SerializeEpochs();

    /// <summary>
    /// Takes the database away from this node so a replica can rebuild it from a snapshot: until
    /// <see cref="AttachDatabase"/>, this node refuses promotions and replication streams and reports sequence number 0.
    /// </summary>
    public void DetachDatabase()
    {
        lock (_gate)
        {
            if (_state.Role != ReplicationRole.Replica)
                throw new InvalidOperationException("only a replica may rebuild its database (" + _nodeId + " is " + _state.Role + ")");
            _db = null;
            _promotionCts?.Cancel();
        }
    }

    /// <summary>Hands this node the database rebuilt by a resync.</summary>
    public void AttachDatabase(RocksDatabase db)
    {
        ArgumentNullException.ThrowIfNull(db);
        lock (_gate) _db = db;
    }

    private ulong LatestSeq
    {
        get
        {
            lock (_gate) return _db?.GetLatestSequenceNumber() ?? 0;
        }
    }

    public ReplicationNodeStatus GetStatus()
    {
        lock (_gate)
        {
            return new ReplicationNodeStatus(_nodeId, _state.Role, _state.Epoch, _state.PromisedEpoch, _state.PrimaryId,
                _db?.GetLatestSequenceNumber() ?? 0, _promotion != null);
        }
    }

    // ===== Primary side =====

    /// <summary>
    /// Hands the primary role to <paramref name="target"/> (see the class remarks). Only the outcomes
    /// <see cref="SwitchoverOutcome.Rejected"/> and <see cref="SwitchoverOutcome.Aborted"/> leave this node primary.
    /// </summary>
    public async Task<SwitchoverResult> HandOffAsync(ISwitchoverPeer target, SwitchoverOptions options, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(target);
        ArgumentNullException.ThrowIfNull(options);
        if (!await _handoff.WaitAsync(0, cancellationToken).ConfigureAwait(false))
            return SwitchoverResult.Rejected(_state.Epoch, "a handoff is already in progress");
        try
        {
            if (_state.Role != ReplicationRole.Primary) return SwitchoverResult.Rejected(_state.Epoch, "this node is " + _state.Role + ", not Primary");
            long epoch = _state.Epoch;

            ReplicationNodeStatus? status = await GetStatusSafeAsync(target, cancellationToken).ConfigureAwait(false);
            if (status == null) return SwitchoverResult.Rejected(epoch, target.NodeId + " is unreachable");
            if (status.Role != ReplicationRole.Replica || status.PrimaryId != _nodeId)
                return SwitchoverResult.Rejected(epoch, target.NodeId + " is " + status.Role + " following " + (status.PrimaryId ?? "nobody") + ", not a replica of " + _nodeId);
            ulong latest = LatestSeq;
            if (latest > status.LatestSeq && latest - status.LatestSeq > options.MaxLagToStart)
                return SwitchoverResult.Rejected(epoch, target.NodeId + " is " + (latest - status.LatestSeq) + " entries behind (limit " + options.MaxLagToStart + ")");
            await WarmUpPromoteAsync(target, options, cancellationToken).ConfigureAwait(false);

            // From here on the outcome is not "nothing happened", so the caller's token no longer applies: every path
            // below ends with this node primary, replica, or explicitly unresolved. The proposed epoch is persisted (as
            // this node's own promise) before the request goes out, so that settling the handoff after a restart
            // rejects exactly the epoch a promote still on the wire carries.
            long proposedEpoch = Math.Max(_state.PromisedEpoch, status.PromisedEpoch) + 1;
            _state.Promise(proposedEpoch);
            _state.Set(epoch, ReplicationRole.HandingOff, _nodeId, target.NodeId);
            RaiseRoleChanged();

            Stopwatch held = Stopwatch.StartNew();
            await Writes.HoldAsync(CancellationToken.None).ConfigureAwait(false);
            ulong handoffSeq = LatestSeq;

            PromoteResult? result;
            using (var rpcTimeout = new CancellationTokenSource(options.CatchUpTimeout + options.RpcGrace))
            {
                try
                {
                    result = await target.PromoteAsync(new PromoteRequest(proposedEpoch, _nodeId, handoffSeq, (int)options.CatchUpTimeout.TotalMilliseconds),
                        rpcTimeout.Token).ConfigureAwait(false);
                }
                catch (Exception)
                {
                    result = null;
                }
            }

            if (result is { Promoted: true })
            {
                CompleteDemotion(result.Epoch, target.NodeId);
                return new SwitchoverResult(SwitchoverOutcome.Completed, result.Epoch, handoffSeq, held.Elapsed, null);
            }
            return await SettleHandoffAsync(target, proposedEpoch, handoffSeq, held, result?.Reason ?? "no answer to the promote request", options)
                .ConfigureAwait(false);
        }
        finally
        {
            _handoff.Release();
        }
    }

    /// <summary>
    /// Settles a node that is <see cref="ReplicationRole.HandingOff"/> after a restart, or a primary that may have been
    /// replaced while it was down. Call it at startup and, while it returns <see cref="SwitchoverOutcome.Unresolved"/>,
    /// periodically. Returns null when there was nothing to settle.
    /// </summary>
    public async Task<SwitchoverResult?> ResolveAsync(ISwitchoverPeer peer, SwitchoverOptions options, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(peer);
        ArgumentNullException.ThrowIfNull(options);
        if (!await _handoff.WaitAsync(0, cancellationToken).ConfigureAwait(false)) return null;
        try
        {
            switch (_state.Role)
            {
                case ReplicationRole.HandingOff:
                    return await SettleHandoffAsync(peer, _state.PromisedEpoch, LatestSeq, Stopwatch.StartNew(), "settling an unfinished handoff", options)
                        .ConfigureAwait(false);
                case ReplicationRole.Primary:
                    // Only an unplanned promotion (ForcePromoteAsync) can replace a primary without its knowledge. If one
                    // did, this node is stale: step down and let the attach check decide whether its log can be kept.
                    ReplicationNodeStatus? status = await GetStatusSafeAsync(peer, cancellationToken).ConfigureAwait(false);
                    if (status is { Role: ReplicationRole.Primary } && status.Epoch > _state.Epoch)
                    {
                        CompleteDemotion(status.Epoch, peer.NodeId);
                        return new SwitchoverResult(SwitchoverOutcome.Completed, status.Epoch, 0, TimeSpan.Zero, peer.NodeId + " is primary in a newer epoch");
                    }
                    return null;
                default:
                    return null;
            }
        }
        finally
        {
            _handoff.Release();
        }
    }

    /// <summary>
    /// Sends a probe promote (refused by the target without side effects) so that the real promote, made while writes are
    /// held, does not pay the first-call costs of the transport on either side. Failures are ignored: the handoff decides
    /// on the real promote.
    /// </summary>
    private async Task WarmUpPromoteAsync(ISwitchoverPeer target, SwitchoverOptions options, CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(options.RpcGrace);
        try
        {
            await target.PromoteAsync(PromoteRequest.Probe(_nodeId), timeout.Token).ConfigureAwait(false);
        }
        catch (Exception) when (!cancellationToken.IsCancellationRequested)
        {
        }
    }

    private static async Task<ReplicationNodeStatus?> GetStatusSafeAsync(ISwitchoverPeer peer, CancellationToken cancellationToken)
    {
        try
        {
            return await peer.GetStatusAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception) when (!cancellationToken.IsCancellationRequested)
        {
            return null;
        }
    }

    private async Task<SwitchoverResult> SettleHandoffAsync(ISwitchoverPeer target, long proposedEpoch, ulong handoffSeq, Stopwatch held, string reason,
        SwitchoverOptions options)
    {
        ReplicationNodeStatus? status;
        using (var rpcTimeout = new CancellationTokenSource(options.CatchUpTimeout + options.RpcGrace))
        {
            try
            {
                status = await target.RejectEpochAsync(proposedEpoch, rpcTimeout.Token).ConfigureAwait(false);
            }
            catch (Exception)
            {
                status = null;
            }
        }

        if (status == null)
        {
            // Unknown whether the target took over. Accepting writes now could make two primaries.
            Writes.Close();
            return new SwitchoverResult(SwitchoverOutcome.Unresolved, _state.Epoch, handoffSeq, held.Elapsed,
                reason + "; " + target.NodeId + " unreachable, so the handoff cannot be settled yet");
        }
        if (status.Role == ReplicationRole.Primary && status.Epoch >= proposedEpoch)
        {
            CompleteDemotion(status.Epoch, target.NodeId);
            return new SwitchoverResult(SwitchoverOutcome.Completed, status.Epoch, handoffSeq, held.Elapsed, "the promote answer was lost, but the target took over");
        }
        if (status.PromisedEpoch >= proposedEpoch)
        {
            // The target promised the proposed epoch and is not primary in it, so it never will be. Resume in an epoch
            // newer than its promise.
            long resumeEpoch = Math.Max(status.PromisedEpoch, _state.PromisedEpoch) + 1;
            ResumeAsPrimary(resumeEpoch);
            return new SwitchoverResult(SwitchoverOutcome.Aborted, resumeEpoch, handoffSeq, held.Elapsed, reason);
        }
        Writes.Close();
        return new SwitchoverResult(SwitchoverOutcome.Unresolved, _state.Epoch, handoffSeq, held.Elapsed,
            reason + "; " + target.NodeId + " did not promise epoch " + proposedEpoch);
    }

    private void CompleteDemotion(long epoch, string newPrimaryId)
    {
        // The gate closes (held writes are refused and redirected) before the new role is persisted: this node is
        // already durably HandingOff (or, settling an unplanned promotion, a primary that ResolveAsync demotes again after
        // a crash), so it restarts read-only and asks the other node either way. Persisting first kept writes held for
        // one more fsync'd write.
        Writes.Close();
        lock (_gate) _state.Set(epoch, ReplicationRole.Replica, newPrimaryId);
        RaiseRoleChanged();
    }

    private void ResumeAsPrimary(long epoch)
    {
        lock (_gate)
        {
            _state.BecomePrimary(_db!.GetLatestSequenceNumber() + 1, epoch, _nodeId);
        }
        Writes.OpenGate();
        RaiseRoleChanged();
    }

    /// <summary>
    /// For a node that restarted as primary: starts a new epoch at the end of the log it recovered, and returns it.
    /// Client writes are not necessarily fsync'd, so after a power loss the recovered log may end before entries the
    /// replica already holds. Continuing in the old epoch would write different entries at those sequence numbers in
    /// the same epoch, and the attach check (<see cref="CheckReplicaLog"/>: same sequence number, same epoch) could no
    /// longer tell the replica's stale tail from this log - the replica would keep it and stream on, diverged. With a
    /// new epoch a replica that holds nothing past the recovered log attaches as before (its last entry is in an epoch
    /// both know), and one that holds more is rebuilt. Call it before the node serves writes or streams; no
    /// <see cref="RoleChanged"/> is raised (the role does not change).
    /// </summary>
    public long StartNewEpochAfterRestart()
    {
        lock (_gate)
        {
            if (_state.Role != ReplicationRole.Primary || _db == null)
                throw new InvalidOperationException(_nodeId + " is " + _state.Role + ", not a primary with a database");
            long epoch = _state.PromisedEpoch + 1;
            _state.BecomePrimary(_db.GetLatestSequenceNumber() + 1, epoch, _nodeId);
            return epoch;
        }
    }

    /// <summary>
    /// Primary-side half of the replica attach check: the replica sends its latest sequence number and the epoch it has
    /// for it; its log is a prefix of ours exactly when we hold that entry in the same epoch. A replica that is ahead of
    /// us, or whose tail carries another epoch (an old primary's unreplicated writes after an unplanned promotion), must
    /// be rebuilt from a snapshot.
    /// </summary>
    public ReplicaLogCheck CheckReplicaLog(ulong replicaLatestSeq, long replicaEpochAtLatest)
    {
        lock (_gate)
        {
            if (_state.Role == ReplicationRole.Replica || _db == null) return new ReplicaLogCheck(false, false, _state.Epoch, string.Empty);
            bool consistent = replicaLatestSeq == 0
                              || (replicaLatestSeq <= _db.GetLatestSequenceNumber() && _state.GetEpochForSeq(replicaLatestSeq) == replicaEpochAtLatest);
            return new ReplicaLogCheck(true, consistent, _state.Epoch, _state.SerializeEpochs());
        }
    }

    // ===== Replica side =====

    /// <summary>
    /// Replica-side handling of a replication stream message from <paramref name="primaryId"/> in
    /// <paramref name="primaryEpoch"/>. Returns false when the stream must be dropped: it comes from a primary older than
    /// the one this node follows. A newer primary is adopted.
    /// </summary>
    public bool AcceptStream(string primaryId, long primaryEpoch)
    {
        lock (_gate)
        {
            if (_state.Role != ReplicationRole.Replica || _db == null) return false;
            if (primaryEpoch < _state.Epoch) return false;
            if (primaryEpoch == _state.Epoch && primaryId != _state.PrimaryId) return false;
            if (primaryEpoch > _state.Epoch) _state.Set(primaryEpoch, ReplicationRole.Replica, primaryId);
            return true;
        }
    }

    /// <summary>
    /// The primary asks this replica to take over. The epoch is promised (and persisted) first; then the replica waits
    /// until it holds exactly the handoff point, stops replicating and becomes primary.
    /// </summary>
    public Task<PromoteResult> HandlePromoteAsync(PromoteRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.IsProbe) return Task.FromResult(Refuse("probe"));
        lock (_gate)
        {
            if (_state.Role != ReplicationRole.Replica) return Task.FromResult(Refuse(_nodeId + " is " + _state.Role + ", not a replica"));
            if (request.FromPrimaryId != _state.PrimaryId)
                return Task.FromResult(Refuse(_nodeId + " follows " + (_state.PrimaryId ?? "nobody") + ", not " + request.FromPrimaryId));
            if (_promotion != null) return Task.FromResult(Refuse("a promotion is already in progress"));
            if (_db == null) return Task.FromResult(Refuse(_nodeId + " is rebuilding its database"));
            if (!_state.Promise(request.Epoch))
                return Task.FromResult(Refuse("epoch " + request.Epoch + " is not newer than the promised " + _state.PromisedEpoch));

            _rejected = false;
            _promotionCts = new CancellationTokenSource(request.CatchUpTimeoutMs);
            _promotion = RunPromotionAsync(request, _db, _promotionCts.Token);
            return _promotion;
        }
    }

    private async Task<PromoteResult> RunPromotionAsync(PromoteRequest request, RocksDatabase db, CancellationToken ct)
    {
        await Task.Yield();
        bool detached = false;
        try
        {
            bool reached;
            try
            {
                reached = await db.WaitForSequenceAsync(request.HandoffSeq, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                reached = false;
            }
            catch (ObjectDisposedException)
            {
                reached = false;
            }
            if (!reached) return Refuse("did not catch up to the handoff point " + request.HandoffSeq + " in time (at " + LatestSeq + ")");
            if (LatestSeq != request.HandoffSeq)
                return Refuse("holds " + LatestSeq + ", past the handoff point " + request.HandoffSeq + ": the logs diverged");

            await _detachFromPrimaryAsync(CancellationToken.None).ConfigureAwait(false);
            detached = true;

            lock (_gate)
            {
                // A RejectEpoch that arrived while we were detaching wins: the old primary is waiting on its answer to
                // resume, and must be the only primary when it does.
                if (_rejected || _db == null) return Refuse("epoch " + request.Epoch + " was rejected before the promotion completed");
                _state.BecomePrimary(request.HandoffSeq + 1, request.Epoch, _nodeId);
            }
            Writes.OpenGate();
            RaiseRoleChanged();
            return new PromoteResult(true, request.Epoch, ReplicationRole.Primary, null);
        }
        finally
        {
            bool reattach;
            lock (_gate)
            {
                _promotion = null;
                _promotionCts?.Dispose();
                _promotionCts = null;
                _rejected = false;
                reattach = detached && _state.Role == ReplicationRole.Replica;
            }
            // Detached but not promoted: hand the stream back to the host.
            if (reattach) RaiseRoleChanged();
        }
    }

    /// <summary>
    /// The old primary got no usable answer to its promote request and wants a definitive one: promise
    /// <paramref name="epoch"/> - so a promote for it still on the wire is refused when it lands - cancel a promotion that
    /// is still catching up, and report the role this node ends up in.
    /// </summary>
    public async Task<ReplicationNodeStatus> HandleRejectEpochAsync(long epoch)
    {
        while (true)
        {
            Task<PromoteResult> running;
            lock (_gate)
            {
                if (_promotion == null)
                {
                    // The promise is made in the critical section that saw no promotion. Released in between (as
                    // rocksdb-sharp does), a promote for this epoch that was still on the wire could start here, and
                    // this call would then report "replica, epoch promised" - the old primary resumes on that - while
                    // the promotion completes: two primaries.
                    if (_state.Role == ReplicationRole.Replica) _state.Promise(epoch);
                    return GetStatus();
                }
                running = _promotion;
                _rejected = true;
                _promotionCts?.Cancel();
            }
            try
            {
                await running.ConfigureAwait(false);
            }
            catch (Exception)
            {
                // The outcome is read from the state in the next round.
            }
        }
    }

    /// <summary>
    /// Unplanned failover: makes this replica primary without the old primary's cooperation, for when the old primary is
    /// gone for good. Writes the old primary acknowledged but had not replicated are lost, and with only two nodes
    /// nothing can tell "dead" from "unreachable": use only under an operator's decision, or behind a fence that
    /// guarantees the old primary is down.
    /// </summary>
    public async Task<long> ForcePromoteAsync(CancellationToken cancellationToken)
    {
        long epoch;
        lock (_gate)
        {
            if (_state.Role != ReplicationRole.Replica || _db == null)
                throw new InvalidOperationException(_nodeId + " is " + _state.Role + ", not a replica with a database");
            epoch = _state.PromisedEpoch + 1;
            _state.Promise(epoch);
        }
        await _detachFromPrimaryAsync(cancellationToken).ConfigureAwait(false);
        lock (_gate)
        {
            // Checked again: the lock was released while the stream detached, and a resync (DetachDatabase) or a
            // planned promotion may have run meanwhile.
            if (_state.Role != ReplicationRole.Replica || _db == null)
                throw new InvalidOperationException(_nodeId + " is " + _state.Role + ", not a replica with a database");
            _state.BecomePrimary(_db.GetLatestSequenceNumber() + 1, epoch, _nodeId);
        }
        Writes.OpenGate();
        RaiseRoleChanged();
        return epoch;
    }

    private PromoteResult Refuse(string reason) => new(false, _state.PromisedEpoch, _state.Role, reason);

    private void RaiseRoleChanged() => RoleChanged?.Invoke(this, new ReplicationRoleChangedEventArgs(_state.Role, _state.Epoch, _state.PrimaryId));
}

public sealed record ReplicationNodeStatus(string NodeId, ReplicationRole Role, long Epoch, long PromisedEpoch, string? PrimaryId, ulong LatestSeq, bool Promoting);

public sealed record PromoteRequest(long Epoch, string FromPrimaryId, ulong HandoffSeq, int CatchUpTimeoutMs)
{
    /// <summary>The epoch of a probe request. Real epochs start at 1.</summary>
    public const long ProbeEpoch = 0;

    /// <summary>
    /// A request the replica refuses at once without side effects; sent before writes are held to warm the promote path
    /// (connection, serializers, JIT) on both nodes.
    /// </summary>
    public static PromoteRequest Probe(string fromPrimaryId) => new(ProbeEpoch, fromPrimaryId, 0, 0);

    public bool IsProbe => Epoch == ProbeEpoch;
}

/// <summary><see cref="Epoch"/> is the promoted epoch, or on a refusal the replica's promised epoch.</summary>
public sealed record PromoteResult(bool Promoted, long Epoch, ReplicationRole Role, string? Reason);

public sealed record ReplicaLogCheck(bool IsPrimary, bool Consistent, long Epoch, string Epochs);

public enum SwitchoverOutcome
{
    /// <summary>The target is primary; this node is its replica.</summary>
    Completed,
    /// <summary>Refused before writes were held; nothing changed.</summary>
    Rejected,
    /// <summary>Writes were held, the target did not take over; this node is primary again, in a new epoch.</summary>
    Aborted,
    /// <summary>The target could not be asked how the handoff ended. This node refuses writes until <see cref="SwitchoverNode.ResolveAsync"/> settles it.</summary>
    Unresolved,
}

public sealed record SwitchoverResult(SwitchoverOutcome Outcome, long Epoch, ulong HandoffSeq, TimeSpan WritesHeld, string? Reason)
{
    internal static SwitchoverResult Rejected(long epoch, string reason) => new(SwitchoverOutcome.Rejected, epoch, 0, TimeSpan.Zero, reason);
}

public sealed class SwitchoverOptions
{
    /// <summary>How far behind the target may be when the handoff starts. Writes are held for about as long as it takes to replicate this much.</summary>
    public ulong MaxLagToStart { get; set; } = 10_000;

    /// <summary>How long the target may take to reach the handoff point - the longest writes are held for.</summary>
    public TimeSpan CatchUpTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Added to <see cref="CatchUpTimeout"/> for the RPC deadline.</summary>
    public TimeSpan RpcGrace { get; set; } = TimeSpan.FromSeconds(2);
}

public sealed class ReplicationRoleChangedEventArgs(ReplicationRole role, long epoch, string? primaryId) : EventArgs
{
    public ReplicationRole Role { get; } = role;
    public long Epoch { get; } = epoch;
    public string? PrimaryId { get; } = primaryId;
}

/// <summary>
/// The calls a <see cref="SwitchoverNode"/> makes to the other node of the pair. Implementations return null (or throw)
/// when the peer gave no answer.
/// </summary>
public interface ISwitchoverPeer
{
    string NodeId { get; }
    Task<ReplicationNodeStatus?> GetStatusAsync(CancellationToken cancellationToken);
    Task<PromoteResult?> PromoteAsync(PromoteRequest request, CancellationToken cancellationToken);
    Task<ReplicationNodeStatus?> RejectEpochAsync(long epoch, CancellationToken cancellationToken);
}
