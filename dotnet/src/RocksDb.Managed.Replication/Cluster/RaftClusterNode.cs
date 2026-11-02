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
    private TaskCompletionSource _commitChanged = NewSignal();
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
        lock (_gate) _commitChanged.TrySetResult();
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

    public RequestVoteResult HandleRequestVote(long term, string candidateId, ulong lastLogSeq, long lastLogTerm)
    {
        lock (_gate)
        {
            // While our log is being rebuilt from a snapshot we have nothing to compare the candidate's log against;
            // granting a vote could help elect a candidate that lacks committed entries.
            if (_resyncing) return new RequestVoteResult(_state.CurrentTerm, false);

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

    /// <summary>The rocksdb-sharp signature of <see cref="HandleAppendEntries(HeartbeatPayload)"/>.</summary>
    public AppendEntriesResult HandleAppendEntries(long term, string leaderId, ulong prevLogSeq, long prevLogTerm, ulong leaderCommit, ClusterMode leaderMode) =>
        HandleAppendEntries(new HeartbeatPayload(term, leaderId, prevLogSeq, prevLogTerm, leaderCommit, leaderMode));

    public AppendEntriesResult HandleAppendEntries(HeartbeatPayload heartbeat)
    {
        ArgumentNullException.ThrowIfNull(heartbeat);
        AppendEntriesResult result;
        bool conflict = false;
        lock (_gate)
        {
            if (heartbeat.Term > _state.CurrentTerm) _state.UpdateTermAndVote(heartbeat.Term, null);

            // The database is being rebuilt: answer before touching it.
            if (_resyncing) return new AppendEntriesResult(_state.CurrentTerm, false, 0, AppendEntriesResult.ReasonResyncing);

            if (heartbeat.Term < _state.CurrentTerm)
                return new AppendEntriesResult(_state.CurrentTerm, false, _db.GetLatestSequenceNumber(), AppendEntriesResult.ReasonStaleTerm);

            BecomeFollowerLocked(heartbeat.LeaderId);

            // Adopt cluster mode if the leader has promoted.
            if (heartbeat.LeaderMode == ClusterMode.Cluster && _state.Mode == ClusterMode.Bootstrap) PromoteToClusterLocked();

            // Log consistency check.
            ulong myLatest = _db.GetLatestSequenceNumber();
            if (heartbeat.PrevLogSeq > 0 && heartbeat.PrevLogSeq > myLatest)
            {
                result = new AppendEntriesResult(_state.CurrentTerm, false, myLatest, AppendEntriesResult.ReasonBehind);
            }
            else if (heartbeat.PrevLogSeq > 0 && _state.Terms.GetTermForSeq(heartbeat.PrevLogSeq) != heartbeat.PrevLogTerm)
            {
                // Our entry at prevLogSeq disagrees with the leader's. Raft would truncate the conflicting suffix; a
                // RocksDB WAL cannot be truncated, so this replica must be rebuilt from a leader snapshot.
                conflict = true;
                result = new AppendEntriesResult(_state.CurrentTerm, false, myLatest, AppendEntriesResult.ReasonTermMismatch);
            }
            else
            {
                ulong newCommit = Math.Min(heartbeat.LeaderCommit, myLatest);
                if (newCommit > _commitSeq) SetCommitLocked(newCommit);
                result = new AppendEntriesResult(_state.CurrentTerm, true, myLatest, null);
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

    /// <summary>A follower applied every entry up to <paramref name="matchSeq"/> (its WAL-stream progress report).</summary>
    public void RecordFollowerProgress(string nodeId, ulong matchSeq)
    {
        if (!_peers.TryGetValue(nodeId, out PeerState? peer)) return;
        lock (_gate)
        {
            if (_resyncing) return;
            peer.OnAppendSucceeded(matchSeq);
            TryAdvanceCommitLocked();
            MaybePromoteToClusterLocked();
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
    /// For a leader acknowledging a client write: completes with true once <see cref="CommitSeq"/> reaches
    /// <paramref name="sequence"/> (the write is replicated to a quorum), or with false as soon as this node is no longer
    /// leader of the term it led when called (the write may never commit). Signalled, no polling.
    /// </summary>
    public async ValueTask<bool> WaitForCommitAsync(ulong sequence, CancellationToken cancellationToken)
    {
        long term;
        lock (_gate)
        {
            if (_role != RaftRole.Leader) return CommitSeq >= sequence;
            term = _state.CurrentTerm;
        }
        while (true)
        {
            Task changed;
            lock (_gate)
            {
                if (CommitSeq >= sequence) return true;
                if (_role != RaftRole.Leader || _state.CurrentTerm != term || _disposed != 0) return false;
                changed = _commitChanged.Task;
            }
            await changed.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private void SetCommitLocked(ulong commit)
    {
        Volatile.Write(ref _commitSeq, commit);
        SignalCommitWaitersLocked();
    }

    private void SignalCommitWaitersLocked()
    {
        TaskCompletionSource prev = _commitChanged;
        _commitChanged = NewSignal();
        prev.TrySetResult();
    }

    private static TaskCompletionSource NewSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private void TryAdvanceCommitLocked()
    {
        if (_role != RaftRole.Leader) return;
        // Sort matchSeqs descending; the quorum-th value is the new commit index.
        var seqs = new List<ulong>(_peers.Count + 1);
        foreach (PeerState p in _peers.Values) seqs.Add(p.MatchSeq);
        seqs.Add(_db.GetLatestSequenceNumber()); // self
        seqs.Sort(static (a, b) => b.CompareTo(a));
        int q = _config.Quorum;
        if (seqs.Count < q) return;
        ulong candidate = seqs[q - 1];
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
        _role = RaftRole.Leader;
        _currentLeaderId = _config.NodeId;
        ulong nextSeq = _db.GetLatestSequenceNumber() + 1;
        // matchSeq must be reset on every election: values learned during a previous leadership are stale and could
        // falsely advance the commit index before this term replicated anything.
        foreach (PeerState p in _peers.Values) p.ResetForNewTerm(nextSeq);
        _state.RecordTermRange(nextSeq, _state.CurrentTerm);
        // Replicate one entry from the new term right away so the commit index can advance past entries inherited
        // from previous terms (§5.4.2).
        _db.Put(LeaderNoOpKey, Encoding.UTF8.GetBytes(_state.CurrentTerm.ToString(CultureInfo.InvariantCulture)));
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
            else if (resp.Reason == AppendEntriesResult.ReasonBehind)
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
                if (_role == RaftRole.Candidate && _state.CurrentTerm == term) BecomeLeaderLocked();
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
                    if (_role == RaftRole.Candidate && _state.CurrentTerm == term && _disposed == 0) BecomeLeaderLocked();
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

public sealed record AppendEntriesResult(long Term, bool Success, ulong LastSeq, string? Reason)
{
    public const string ReasonBehind = "behind";
    public const string ReasonTermMismatch = "term-mismatch";
    public const string ReasonResyncing = "resyncing";
    public const string ReasonStaleTerm = "stale term";
}

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
