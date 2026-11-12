using System.Globalization;
using System.Text;

namespace RocksDb.Managed.Cluster;

/// <summary>
/// Transport-agnostic Raft orchestrator over a RocksDB WAL (port of rocksdb-sharp <c>RaftClusterNode</c>; see the
/// README for the design). The RocksDB sequence number is the Raft log index, the WAL stream is log replication and a
/// checkpoint is catch-up from far behind; this class adds term/vote bookkeeping, elections, heartbeats, quorum commit
/// and the Bootstrap -> Cluster transition.
/// <para>
/// The host owns the network, the database and the replication stream. It routes incoming RPCs to
/// <see cref="HandleRequestVote"/>, <see cref="HandleAppendEntries"/>, <see cref="HandleObservedLeader"/> and
/// <see cref="CheckFollowerLogConsistency"/>, reports follower progress to the leader through
/// <see cref="RecordFollowerProgress"/>, and gives this class an <see cref="IPeerTransport"/> for outgoing RPCs.
/// </para>
/// <para>
/// Managed differences: <see cref="WaitForCommitAsync"/> lets a leader acknowledge a write once it is committed without
/// polling <see cref="CommitSeq"/>; timers and delays use <see cref="RaftConfig.TimeProvider"/>;
/// <see cref="DisposeAsync"/> waits for the heartbeat loop so no RPC is sent after disposal.
/// </para>
/// Events are raised while the node's internal lock is held: handlers must be quick and must not block on work that
/// calls back into this node from another thread (schedule such work instead).
/// </summary>
public sealed class RaftClusterNode : IDisposable, IAsyncDisposable
{
    /// <summary>
    /// Key of the marker entry a leader writes immediately after winning an election. Raft (§5.4.2) forbids committing
    /// entries from previous terms by counting replicas, so replicating one current-term entry right away lets the
    /// commit index advance without waiting for a user write.
    /// </summary>
    public static ReadOnlySpan<byte> LeaderNoOpKey => "__raft_noop"u8;

    private readonly RaftConfig _config;
    private readonly RaftPersistentState _state;
    private readonly IPeerTransport _transport;
    private readonly ElectionTimer _electionTimer;
    private readonly Lock _gate = new();
    private readonly Dictionary<string, PeerState> _peers;
    private readonly CancellationTokenSource _cts = new();
    private readonly TimeProvider _time;

    private RocksDatabase _db;
    private RaftRole _role;
    private string? _currentLeaderId;
    private ulong _commitSeq;
    private CancellationTokenSource? _leaderCts;
    private Task _leaderLoop = Task.CompletedTask;
    private bool _resyncing;
    private int _bootstrapInSyncFollowers;
    // Woken whenever the commit index advances or leadership ends (WaitForCommitAsync). Signalled under _gate.
    private readonly RocksDb.Managed.Util.AsyncBroadcastSignal _commitChanged = new();
    // Scratch for the quorum computation (self + peers). Guarded by _gate.
    private readonly ulong[] _matchScratch;
    // The last successful heartbeat answer (reused while term and log tail are unchanged). Guarded by _gate.
    private AppendEntriesResult? _lastAccepted;
    private int _disposed;

    public event EventHandler<LeaderChangedEventArgs>? LeaderChanged;
    public event EventHandler<RoleChangedEventArgs>? RoleChanged;
    public event EventHandler<ModeChangedEventArgs>? ModeChanged;

    /// <summary>
    /// Raised when this node detects that its log conflicts with the current leader's (a term mismatch at a sequence
    /// number both logs contain). A RocksDB WAL cannot be truncated, so the host must rebuild this replica from a leader
    /// snapshot: <see cref="BeginResync"/>, dispose the database, restore a snapshot, <see cref="CompleteResync"/>.
    /// </summary>
    public event EventHandler? ResyncRequired;

    /// <summary>
    /// Raised when this node won an election but could not write the first entry of its term (or record the term) and
    /// therefore gave the leadership up. The node keeps campaigning when its election timer fires.
    /// </summary>
    public event EventHandler<LeadershipFailedEventArgs>? LeadershipFailed;

    public RaftClusterNode(RaftConfig config, RaftPersistentState state, RocksDatabase db, IPeerTransport transport)
    {
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(state);
        ArgumentNullException.ThrowIfNull(db);
        ArgumentNullException.ThrowIfNull(transport);
        config.Validate();
        _config = config;
        _state = state;
        _db = db;
        _transport = transport;
        _time = config.TimeProvider;

        _peers = new Dictionary<string, PeerState>(StringComparer.Ordinal);
        ulong next = db.GetLatestSequenceNumber() + 1;
        foreach (ClusterMember p in config.Peers) _peers.Add(p.NodeId, new PeerState(p, next));
        _matchScratch = new ulong[_peers.Count + 1];

        _electionTimer = new ElectionTimer(config.ElectionTimeoutMinMs, config.ElectionTimeoutMaxMs, OnElectionTimeout, _time);

        if (config.BootstrapPrimary && state.Mode == ClusterMode.Bootstrap)
        {
            // Bootstrap primary: act as leader without holding an election.
            _role = RaftRole.Leader;
            _currentLeaderId = config.NodeId;
            if (state.CurrentTerm == 0)
            {
                state.UpdateTermAndVote(1, config.NodeId);
                state.RecordTermRange(db.GetLatestSequenceNumber() + 1, 1);
            }
            else
            {
                // A restart. Leading again without an election must not continue the term of the previous run: if
                // the recovered log ends before entries a follower already holds (log writes not fsync'd, see
                // RaftConfig.SyncLogWrites, and a power loss), this node would write different entries at those
                // sequence numbers in the same term, and (sequence, term) would no longer identify an entry - the
                // follower's stale tail passes the attach check and the heartbeat check. A new term starting at the
                // end of the recovered log keeps Raft's log matching property, as an election would.
                long term = state.CurrentTerm + 1;
                state.UpdateTermAndVote(term, config.NodeId);
                state.StartTerm(db.GetLatestSequenceNumber() + 1, term);
            }
        }
        else
        {
            _role = RaftRole.Follower;
        }
    }

    public string NodeId => _config.NodeId;
    public RaftConfig Config => _config;

    public RaftRole Role
    {
        get
        {
            lock (_gate) return _role;
        }
    }

    public long CurrentTerm => _state.CurrentTerm;

    public string? CurrentLeaderId
    {
        get
        {
            lock (_gate) return _currentLeaderId;
        }
    }

    public ClusterMode Mode => _state.Mode;
    public ulong CommitSeq => Volatile.Read(ref _commitSeq);
    public IReadOnlyCollection<PeerState> Peers => [.. _peers.Values];

    public bool IsResyncing
    {
        get
        {
            lock (_gate) return _resyncing;
        }
    }

    public int BootstrapInSyncCount => Volatile.Read(ref _bootstrapInSyncFollowers);

    public long GetTermForSeq(ulong seq) => _state.Terms.GetTermForSeq(seq);

    /// <summary>The serialized term map (sent to a follower that restored a snapshot of this node).</summary>
    public string SerializeTerms() => _state.Terms.Serialize();

    public void Start()
    {
        lock (_gate)
        {
            if (_role == RaftRole.Leader) StartLeaderLoopLocked();
            else _electionTimer.Start();
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1) return;
        _cts.Cancel();
        _electionTimer.Dispose();
        lock (_gate) _commitChanged.SignalAll();
    }

    /// <summary>Stops the node and waits for its heartbeat loop to finish.</summary>
    public async ValueTask DisposeAsync()
    {
        Dispose();
        Task loop;
        lock (_gate) loop = _leaderLoop;
        try
        {
            await loop.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
    }

    // ===== Incoming RPC handlers =====

    public RequestVoteResult HandleRequestVote(RequestVotePayload request)
    {
        ArgumentNullException.ThrowIfNull(request);
        (long term, string candidateId, ulong lastLogSeq, long lastLogTerm) = request;
        lock (_gate)
        {
            // While our log is being rebuilt from a snapshot we have nothing to compare the candidate's log against;
            // granting a vote could help elect a candidate that lacks committed entries.
            if (_resyncing) return new RequestVoteResult(_state.CurrentTerm, false);

            // Only a member can become leader of this cluster. A request from anyone else (a node configured for another
            // cluster, a member list that was changed on one node only) must neither get a vote - a majority here does
            // not overlap with a majority of its member list - nor move this node's term and depose its leader.
            if (!_peers.ContainsKey(candidateId)) return new RequestVoteResult(_state.CurrentTerm, false);

            if (term > _state.CurrentTerm)
            {
                // Elections can only be started by nodes already in Cluster mode, so a higher-term RequestVote proves the
                // promotion quorum was reached somewhere. Adopt Cluster mode, otherwise a cluster whose leader promoted
                // and then died before broadcasting the new mode could never elect a successor.
                if (_state.Mode == ClusterMode.Bootstrap) PromoteToClusterLocked();
                _state.UpdateTermAndVote(term, null);
                BecomeFollowerLocked(null);
            }

            if (term < _state.CurrentTerm) return new RequestVoteResult(_state.CurrentTerm, false);

            // In Bootstrap mode the bootstrap primary is fixed: a partition must not produce a leader early.
            if (_state.Mode == ClusterMode.Bootstrap) return new RequestVoteResult(_state.CurrentTerm, false);

            string? votedFor = _state.VotedFor;
            if (votedFor != null && votedFor != candidateId) return new RequestVoteResult(_state.CurrentTerm, false);

            // The candidate's log must be at least as up-to-date as ours.
            ulong myLastSeq = _db.GetLatestSequenceNumber();
            long myLastTerm = _state.Terms.GetTermForSeq(myLastSeq);
            bool upToDate = lastLogTerm > myLastTerm || (lastLogTerm == myLastTerm && lastLogSeq >= myLastSeq);
            if (!upToDate) return new RequestVoteResult(_state.CurrentTerm, false);

            _state.UpdateTermAndVote(term, candidateId);
            _electionTimer.Reset();
            return new RequestVoteResult(_state.CurrentTerm, true);
        }
    }

    public AppendEntriesResult HandleAppendEntries(HeartbeatPayload heartbeat)
    {
        ArgumentNullException.ThrowIfNull(heartbeat);
        AppendEntriesResult result;
        bool conflict = false;
        lock (_gate)
        {
            // A heartbeat of a node that is not a member is not this cluster's leader speaking (see HandleRequestVote):
            // it must not move the term, reset the election timer or set the commit index.
            if (!_peers.ContainsKey(heartbeat.LeaderId))
                return new AppendEntriesResult(_state.CurrentTerm, false, 0, AppendEntriesRejectReason.StaleTerm);

            if (heartbeat.Term > _state.CurrentTerm) _state.UpdateTermAndVote(heartbeat.Term, null);


            // The database is being rebuilt: answer before touching it.
            if (_resyncing) return new AppendEntriesResult(_state.CurrentTerm, false, 0, AppendEntriesRejectReason.Resyncing);

            if (heartbeat.Term < _state.CurrentTerm)
                return new AppendEntriesResult(_state.CurrentTerm, false, _db.GetLatestSequenceNumber(), AppendEntriesRejectReason.StaleTerm);

            BecomeFollowerLocked(heartbeat.LeaderId);

            // Adopt cluster mode if the leader has promoted.
            if (heartbeat.LeaderMode == ClusterMode.Cluster && _state.Mode == ClusterMode.Bootstrap) PromoteToClusterLocked();

            // Log consistency check.
            ulong myLatest = _db.GetLatestSequenceNumber();
            if (heartbeat.PrevLogSeq > 0 && heartbeat.PrevLogSeq > myLatest)
            {
                result = new AppendEntriesResult(_state.CurrentTerm, false, myLatest, AppendEntriesRejectReason.Behind);
            }
            else if (heartbeat.PrevLogSeq > 0 && _state.Terms.GetTermForSeq(heartbeat.PrevLogSeq) != heartbeat.PrevLogTerm)
            {
                // Our entry at prevLogSeq disagrees with the leader's. Raft would truncate the conflicting suffix; a
                // RocksDB WAL cannot be truncated, so this replica must be rebuilt from a leader snapshot.
                conflict = true;
                result = new AppendEntriesResult(_state.CurrentTerm, false, myLatest, AppendEntriesRejectReason.TermMismatch);
            }
            else
            {
                // Raft (AppendEntries receiver rule 5): commitIndex = min(leaderCommit, index of last new entry). A
                // heartbeat carries no entries, so the last entry known to match the leader's log is PrevLogSeq;
                // entries past it may be a deposed leader's divergent tail and must not be marked committed.
                ulong newCommit = Math.Min(heartbeat.LeaderCommit, heartbeat.PrevLogSeq);
                if (newCommit > _commitSeq) SetCommitLocked(newCommit);
                // Results are immutable: an idle follower answers every heartbeat with the same one.
                AppendEntriesResult? last = _lastAccepted;
                long term = _state.CurrentTerm;
                if (last is null || last.Term != term || last.LastSeq != myLatest)
                {
                    _lastAccepted = last = new AppendEntriesResult(term, true, myLatest, AppendEntriesRejectReason.None);
                }
                result = last;
            }
        }
        if (conflict) ResyncRequired?.Invoke(this, EventArgs.Empty);
        return result;
    }

    /// <summary>
    /// Followers call this for every message of the WAL stream: the leader tags its node id and current term on every
    /// chunk it streams.
    /// </summary>
    public void HandleObservedLeader(string leaderId, long term)
    {
        lock (_gate)
        {
            if (term < _state.CurrentTerm) return;
            // A leader can never be replaced within its own term; only a strictly higher term may depose us.
            if (term == _state.CurrentTerm && _role == RaftRole.Leader) return;
            if (term > _state.CurrentTerm) _state.UpdateTermAndVote(term, null);
            BecomeFollowerLocked(leaderId);
        }
    }

    /// <summary>
    /// Leader-side half of the stream-attach consistency check: the follower sends its (latestSeq, termAtLatest) pair and
    /// the leader confirms that entry exists identically in its own log. A follower whose tail diverged (e.g. a deposed
    /// leader with uncommitted writes) is told to rebuild itself from a snapshot.
    /// </summary>
    public LogConsistencyResult CheckFollowerLogConsistency(ulong followerLatestSeq, long followerTermAtLatest)
    {
        lock (_gate)
        {
            if (_role != RaftRole.Leader || _resyncing) return new LogConsistencyResult(false, false, _state.CurrentTerm);
            if (followerLatestSeq == 0) return new LogConsistencyResult(true, true, _state.CurrentTerm);
            ulong myLatest = _db.GetLatestSequenceNumber();
            // A follower with entries the leader does not have holds an uncommitted tail from a previous term.
            if (followerLatestSeq > myLatest) return new LogConsistencyResult(true, false, _state.CurrentTerm);
            bool consistent = _state.Terms.GetTermForSeq(followerLatestSeq) == followerTermAtLatest;
            return new LogConsistencyResult(true, consistent, _state.CurrentTerm);
        }
    }

    // ===== Resync lifecycle (host-driven) =====

    /// <summary>
    /// Puts the node into resync mode: the election timer is paused and vote / append handling answers conservatively
    /// without touching the database, so the host can dispose and rebuild it.
    /// </summary>
    public void BeginResync()
    {
        lock (_gate)
        {
            _resyncing = true;
            // A node whose log is about to be replaced cannot lead (or win a pending election): as a leader it would
            // keep sending heartbeats - holding the other nodes' election timers - while refusing every stream and
            // attach check, and its own resync would find no leader to pull from.
            if (_role != RaftRole.Follower) BecomeFollowerLocked(null);
            _electionTimer.Pause();
        }
    }

    /// <summary>Completes a resync with the freshly restored database instance.</summary>
    public void CompleteResync(RocksDatabase db)
    {
        ArgumentNullException.ThrowIfNull(db);
        lock (_gate)
        {
            _db = db;
            _resyncing = false;
            if (_role != RaftRole.Leader) _electionTimer.Reset();
        }
    }

    // ===== Leader-only: log replication advance =====

    /// <summary>
    /// A follower applied every entry up to <paramref name="matchSeq"/> of the log this node streamed to it while leading
    /// <paramref name="leaderTerm"/> (its WAL-stream progress report). Returns false, changing nothing, unless this node
    /// currently leads exactly that term and its own log reaches <paramref name="matchSeq"/>: a report is a statement
    /// about one leadership's log. One that was addressed to an earlier leadership of this node (delayed across
    /// elections) may name entries the follower has since discarded in a resync, and counting it could commit an entry
    /// that fewer than a quorum hold.
    /// </summary>
    public bool RecordFollowerProgress(string nodeId, ulong matchSeq, long leaderTerm)
    {
        if (!_peers.TryGetValue(nodeId, out PeerState? peer)) return false;
        lock (_gate)
        {
            if (_resyncing || _role != RaftRole.Leader || leaderTerm != _state.CurrentTerm) return false;
            // A follower of this leadership holds a prefix of this log, never more.
            if (matchSeq > _db.GetLatestSequenceNumber()) return false;
            peer.OnAppendSucceeded(matchSeq);
            TryAdvanceCommitLocked();
            MaybePromoteToClusterLocked();
            return true;
        }
    }

    public void RecordFollowerHeartbeat(string nodeId)
    {
        if (_peers.TryGetValue(nodeId, out PeerState? peer)) peer.OnHeartbeatAcknowledged();
    }

    public void RecordFollowerUnreachable(string nodeId)
    {
        if (_peers.TryGetValue(nodeId, out PeerState? peer)) peer.OnAppendFailed();
    }

    /// <summary>
    /// For a leader acknowledging a client write it made as leader of <paramref name="term"/> (read
    /// <see cref="CurrentTerm"/> before the write): completes with true once <see cref="CommitSeq"/> reaches
    /// <paramref name="sequence"/> (the write is replicated to a quorum) and the entry there still carries
    /// <paramref name="term"/> in this node's log - by the log matching property that is the caller's entry; after a
    /// step-down and a rebuild of this node's log another entry can stand at <paramref name="sequence"/>. Completes
    /// with false once this node no longer leads <paramref name="term"/> and the entry is not known to be committed
    /// (the write may never commit). Signalled, no polling; allocation-free in steady state (pooled state machine and
    /// wait, cancellation included).
    /// </summary>
    [System.Runtime.CompilerServices.AsyncMethodBuilder(typeof(System.Runtime.CompilerServices.PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<bool> WaitForCommitAsync(ulong sequence, long term, CancellationToken cancellationToken)
    {
        while (true)
        {
            RocksDb.Managed.Util.AsyncBroadcastSignal.Waiter changed;
            lock (_gate)
            {
                bool leading = _role == RaftRole.Leader && _state.CurrentTerm == term && _disposed == 0;
                if (CommitSeq >= sequence) return leading || _state.Terms.GetTermForSeq(sequence) == term;
                if (!leading) return false;
                // Registered under _gate, where every signal is raised: no wake-up can be missed.
                changed = _commitChanged.Register();
            }
            await _commitChanged.WaitAsync(changed, cancellationToken).ConfigureAwait(false);
        }
    }

    private void SetCommitLocked(ulong commit)
    {
        Volatile.Write(ref _commitSeq, commit);
        SignalCommitWaitersLocked();
    }

    private void SignalCommitWaitersLocked() => _commitChanged.SignalAll();

    private void TryAdvanceCommitLocked()
    {
        if (_role != RaftRole.Leader) return;
        // The quorum-th largest matchSeq (self included) is the new commit index.
        Span<ulong> seqs = _matchScratch;
        int n = 0;
        foreach (PeerState p in _peers.Values) seqs[n++] = p.MatchSeq;
        seqs[n++] = _db.GetLatestSequenceNumber(); // self
        int q = _config.Quorum;
        if (n < q) return;
        seqs[..n].Sort();
        ulong candidate = seqs[n - q];
        if (candidate <= _commitSeq) return;
        // §5.4.2: a leader may only commit entries from its own term by counting replicas; entries from earlier terms
        // become committed implicitly once a current-term entry is committed.
        if (_state.Terms.GetTermForSeq(candidate) != _state.CurrentTerm) return;
        SetCommitLocked(candidate);
    }

    private void MaybePromoteToClusterLocked()
    {
        if (_state.Mode != ClusterMode.Bootstrap || _role != RaftRole.Leader) return;
        ulong latest = _db.GetLatestSequenceNumber();
        ulong threshold = _config.BootstrapInSyncLagThreshold;
        int inSync = 1; // self
        foreach (PeerState p in _peers.Values)
        {
            if (p.MatchSeq > 0 && p.MatchSeq + threshold >= latest) inSync++;
        }
        Volatile.Write(ref _bootstrapInSyncFollowers, inSync);
        if (inSync >= _config.BootstrapPromotionQuorum) PromoteToClusterLocked();
    }

    private void PromoteToClusterLocked()
    {
        if (_state.Mode == ClusterMode.Cluster) return;
        _state.SetMode(ClusterMode.Cluster);
        ModeChanged?.Invoke(this, new ModeChangedEventArgs(ClusterMode.Cluster));
    }

    // ===== State transitions (REQUIRES: _gate) =====

    private void BecomeFollowerLocked(string? leaderId)
    {
        bool roleChanged = _role != RaftRole.Follower;
        bool leaderChanged = _currentLeaderId != leaderId;
        _role = RaftRole.Follower;
        _currentLeaderId = leaderId;
        _leaderCts?.Cancel();
        _leaderCts = null;
        if (roleChanged) SignalCommitWaitersLocked();
        if (roleChanged) RoleChanged?.Invoke(this, new RoleChangedEventArgs(_role, _state.CurrentTerm));
        if (leaderChanged) LeaderChanged?.Invoke(this, new LeaderChangedEventArgs(leaderId, _state.CurrentTerm));
        if (!_resyncing) _electionTimer.Reset();
    }

    private void BecomeCandidateLocked()
    {
        bool wasLeader = _role == RaftRole.Leader;
        _role = RaftRole.Candidate;
        _state.UpdateTermAndVote(_state.CurrentTerm + 1, _config.NodeId);
        _currentLeaderId = null;
        if (wasLeader) SignalCommitWaitersLocked();
        RoleChanged?.Invoke(this, new RoleChangedEventArgs(_role, _state.CurrentTerm));
        LeaderChanged?.Invoke(this, new LeaderChangedEventArgs(null, _state.CurrentTerm));
    }

    private void BecomeLeaderLocked()
    {
        ulong nextSeq = _db.GetLatestSequenceNumber() + 1;
        try
        {
            _state.StartTerm(nextSeq, _state.CurrentTerm);
            // Replicate one entry from the new term right away so the commit index can advance past entries inherited
            // from previous terms (§5.4.2).
            // Synced like every log entry: the leader counts its own latest sequence towards the quorum.
            byte[] noop = Encoding.UTF8.GetBytes(_state.CurrentTerm.ToString(CultureInfo.InvariantCulture));
            if (_config.SyncLogWrites) _db.Put(WriteOptions.SyncWrites, LeaderNoOpKey, noop);
            else _db.Put(LeaderNoOpKey, noop);
        }
        catch (Exception e) when (e is RocksDbException or ObjectDisposedException or IOException)
        {
            // A node that cannot write its log cannot lead. Taking the role first would leave a leader without a
            // heartbeat loop that its own election timer ignores: give the election up instead (the timer runs on, and
            // the other nodes elect someone once their timers fire).
            _role = RaftRole.Follower;
            _currentLeaderId = null;
            RoleChanged?.Invoke(this, new RoleChangedEventArgs(_role, _state.CurrentTerm));
            LeadershipFailed?.Invoke(this, new LeadershipFailedEventArgs(_state.CurrentTerm, e));
            if (!_resyncing) _electionTimer.Reset();
            return;
        }
        _role = RaftRole.Leader;
        _currentLeaderId = _config.NodeId;
        // matchSeq must be reset on every election: values learned during a previous leadership are stale and could
        // falsely advance the commit index before this term replicated anything.
        foreach (PeerState p in _peers.Values) p.ResetForNewTerm(nextSeq);
        RoleChanged?.Invoke(this, new RoleChangedEventArgs(_role, _state.CurrentTerm));
        LeaderChanged?.Invoke(this, new LeaderChangedEventArgs(_config.NodeId, _state.CurrentTerm));
        _electionTimer.Pause();
        StartLeaderLoopLocked();
    }

    private void StartLeaderLoopLocked()
    {
        // Cancel any loop left over from a previous leadership so two loops never run concurrently.
        _leaderCts?.Cancel();
        _leaderCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
        CancellationToken token = _leaderCts.Token;
        Task previous = _leaderLoop;
        _leaderLoop = Task.Run(async () =>
        {
            try
            {
                await previous.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
            await LeaderHeartbeatLoopAsync(token).ConfigureAwait(false);
        });
    }

    private async Task LeaderHeartbeatLoopAsync(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                long term;
                ulong commit;
                ClusterMode leaderMode;
                lock (_gate)
                {
                    if (_role != RaftRole.Leader) break;
                    commit = _commitSeq;
                    term = _state.CurrentTerm;
                    leaderMode = _state.Mode;
                }
                foreach (PeerState peer in _peers.Values)
                {
                    ulong prevLogSeq = peer.NextSeq > 0 ? peer.NextSeq - 1 : 0;
                    long prevLogTerm = _state.Terms.GetTermForSeq(prevLogSeq);
                    var beat = new HeartbeatPayload(term, _config.NodeId, prevLogSeq, prevLogTerm, commit, leaderMode);
                    _ = SendHeartbeatToPeerAsync(peer, beat, ct);
                }
                await Task.Delay(TimeSpan.FromMilliseconds(_config.HeartbeatIntervalMs), _time, ct).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    private async Task SendHeartbeatToPeerAsync(PeerState peer, HeartbeatPayload beat, CancellationToken ct)
    {
        try
        {
            AppendEntriesResult? resp = await _transport.SendHeartbeatAsync(peer.Member, beat, ct).ConfigureAwait(false);
            if (resp == null)
            {
                peer.OnAppendFailed();
                return;
            }
            if (resp.Term > _state.CurrentTerm)
            {
                lock (_gate)
                {
                    if (resp.Term > _state.CurrentTerm)
                    {
                        _state.UpdateTermAndVote(resp.Term, null);
                        BecomeFollowerLocked(null);
                    }
                }
                return;
            }
            if (resp.Success)
            {
                lock (_gate)
                {
                    if (_role != RaftRole.Leader || _state.CurrentTerm != beat.Term) return;
                    // Only the prefix ending at PrevLogSeq has been validated against our log; anything past it is
                    // confirmed by the WAL-stream progress reports, not by the heartbeat.
                    peer.OnAppendSucceeded(beat.PrevLogSeq);
                    TryAdvanceCommitLocked();
                    MaybePromoteToClusterLocked();
                }
            }
            else if (resp.Reason == AppendEntriesRejectReason.Behind)
            {
                // The follower's log is shorter; probe at its tip next time.
                peer.RewindOnConflict(Math.Max(1, resp.LastSeq + 1));
            }
            else
            {
                // Term mismatch (the follower resyncs itself) or resyncing: treat the peer as unreachable until it is back.
                peer.OnAppendFailed();
            }
        }
        catch (Exception)
        {
            peer.OnAppendFailed();
        }
    }

    private void OnElectionTimeout()
    {
        long electionTerm;
        ulong lastLogSeq;
        long lastLogTerm;
        lock (_gate)
        {
            if (_role == RaftRole.Leader || _resyncing || _disposed != 0) return;
            if (_state.Mode == ClusterMode.Bootstrap)
            {
                // Only the bootstrap primary may lead before the cluster promoted: wait.
                _electionTimer.Reset();
                return;
            }
            BecomeCandidateLocked();
            // The campaign is bound to the exact term in which we became candidate; capturing it outside this lock
            // would let a concurrent vote or heartbeat move the term and make us campaign where we already voted.
            electionTerm = _state.CurrentTerm;
            lastLogSeq = _db.GetLatestSequenceNumber();
            lastLogTerm = _state.Terms.GetTermForSeq(lastLogSeq);
            _electionTimer.Reset();
        }
        _ = Task.Run(() => RunElectionAsync(electionTerm, lastLogSeq, lastLogTerm));
    }

    private async Task RunElectionAsync(long term, ulong lastLogSeq, long lastLogTerm)
    {
        List<ClusterMember> peers;
        lock (_gate)
        {
            if (_role != RaftRole.Candidate || _state.CurrentTerm != term) return;
            peers = new List<ClusterMember>(_peers.Count);
            foreach (PeerState p in _peers.Values) peers.Add(p.Member);
        }
        int votes = 1; // self
        int needed = _config.Quorum;
        if (votes >= needed)
        {
            lock (_gate)
            {
                if (_role == RaftRole.Candidate && _state.CurrentTerm == term && !_resyncing && _disposed == 0) BecomeLeaderLocked();
            }
            return;
        }
        var tasks = new List<Task<RequestVoteResult?>>(peers.Count);
        foreach (ClusterMember p in peers) tasks.Add(RequestVoteSafeAsync(p, term, lastLogSeq, lastLogTerm));
        while (tasks.Count > 0)
        {
            Task<RequestVoteResult?> done = await Task.WhenAny(tasks).ConfigureAwait(false);
            tasks.Remove(done);
            RequestVoteResult? resp = await done.ConfigureAwait(false);
            if (resp == null) continue;
            if (resp.Term > term)
            {
                lock (_gate)
                {
                    if (resp.Term > _state.CurrentTerm)
                    {
                        _state.UpdateTermAndVote(resp.Term, null);
                        BecomeFollowerLocked(null);
                    }
                }
                return;
            }
            if (resp.VoteGranted && ++votes >= needed)
            {
                lock (_gate)
                {
                    // Still candidate for this term?
                    if (_role == RaftRole.Candidate && _state.CurrentTerm == term && !_resyncing && _disposed == 0) BecomeLeaderLocked();
                }
                return;
            }
        }
        // Election failed; the election timer fires again.
    }

    private async Task<RequestVoteResult?> RequestVoteSafeAsync(ClusterMember peer, long term, ulong lastLogSeq, long lastLogTerm)
    {
        try
        {
            return await _transport.SendRequestVoteAsync(peer, new RequestVotePayload(term, _config.NodeId, lastLogSeq, lastLogTerm), _cts.Token)
                .ConfigureAwait(false);
        }
        catch (Exception)
        {
            return null;
        }
    }
}

/// <summary>RequestVote arguments (Raft §5.2).</summary>
public sealed record RequestVotePayload(long Term, string CandidateId, ulong LastLogSeq, long LastLogTerm);

public sealed record RequestVoteResult(long Term, bool VoteGranted);

/// <summary>A heartbeat (AppendEntries without entries: data flows on the WAL stream).</summary>
public sealed record HeartbeatPayload(long Term, string LeaderId, ulong PrevLogSeq, long PrevLogTerm, ulong LeaderCommit, ClusterMode LeaderMode);

/// <summary>Why a follower rejected a heartbeat (<see cref="AppendEntriesResult.Reason"/>).</summary>
public enum AppendEntriesRejectReason
{
    /// <summary>Not rejected (<see cref="AppendEntriesResult.Success"/>).</summary>
    None = 0,
    /// <summary>The follower's log ends before the leader's PrevLogSeq; the leader probes at the follower's tip.</summary>
    Behind,
    /// <summary>The follower's term at PrevLogSeq differs from the leader's: the follower rebuilds from a snapshot.</summary>
    TermMismatch,
    /// <summary>The follower is rebuilding its database from a leader snapshot.</summary>
    Resyncing,
    /// <summary>The heartbeat's term is older than the follower's current term.</summary>
    StaleTerm,
}

public sealed record AppendEntriesResult(long Term, bool Success, ulong LastSeq, AppendEntriesRejectReason Reason);

public sealed record LogConsistencyResult(bool IsLeader, bool Consistent, long Term);

/// <summary>
/// The outgoing Raft RPCs of a <see cref="RaftClusterNode"/>. Implementations return null (or throw) when the peer gave
/// no answer; they should bound each call with a timeout well below the election timeout.
/// </summary>
public interface IPeerTransport
{
    Task<RequestVoteResult?> SendRequestVoteAsync(ClusterMember peer, RequestVotePayload request, CancellationToken cancellationToken);
    Task<AppendEntriesResult?> SendHeartbeatAsync(ClusterMember peer, HeartbeatPayload payload, CancellationToken cancellationToken);
}
