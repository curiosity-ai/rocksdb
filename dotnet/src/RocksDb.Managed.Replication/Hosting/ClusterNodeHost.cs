using System.Runtime.CompilerServices;
using RocksDb.Managed.Cluster;
using RocksDb.Managed.Replication.Internal;

namespace RocksDb.Managed.Replication.Hosting;

/// <summary>Status of a cluster node, as reported by <see cref="ClusterNodeHost.GetStatus"/>.</summary>
public sealed record ClusterNodeStatus(string NodeId, RaftRole Role, long Term, string? LeaderId, ulong LatestSeq, ulong CommitSeq, ClusterMode Mode,
    int BootstrapInSyncCount, bool Resyncing, int Resyncs);

/// <summary>Outcome of <see cref="ClusterNodeHost.WriteCommittedAsync(WriteBatch, TimeSpan, CancellationToken)"/>.</summary>
public enum ClusterWriteStatus
{
    /// <summary>The write is committed (replicated to a quorum).</summary>
    Committed = 0,
    /// <summary>Definitely not applied: this node was not the leader (or was rebuilding) and wrote nothing.</summary>
    NotLeader = 1,
    /// <summary>Indeterminate: written to this node's log, but it stopped leading before the write committed. A later
    /// leader may still commit it (if it was replicated) or discard it.</summary>
    LeadershipLost = 2,
    /// <summary>Indeterminate: written to this node's log, not committed within the timeout (it may still commit).</summary>
    TimedOut = 3,
}

/// <summary>Answer to a follower's attach check: <see cref="LogConsistencyResult"/> plus the leader's term map.</summary>
public sealed record ClusterAttachResult(bool IsLeader, bool Consistent, long Term, string Terms);

/// <summary>
/// The calls a <see cref="ClusterNodeHost"/> makes to another node: the Raft RPCs plus the data plane (WAL stream,
/// snapshot, progress reports). Implemented over an in-memory network in the tests and over gRPC by
/// RocksDb.Managed.Replication.MagicOnion. Methods may throw or return null when the node does not answer.
/// </summary>
public interface IClusterNodeClient
{
    string NodeId { get; }
    Task<RequestVoteResult?> RequestVoteAsync(RequestVotePayload request, CancellationToken cancellationToken);
    Task<AppendEntriesResult?> HeartbeatAsync(HeartbeatPayload payload, CancellationToken cancellationToken);
    Task<ClusterNodeStatus?> GetStatusAsync(CancellationToken cancellationToken);
    Task<ClusterAttachResult?> CheckLogConsistencyAsync(string followerId, ulong latestSeq, long termAtLatest, CancellationToken cancellationToken);
    IAsyncEnumerable<WalStreamMessage> StreamUpdatesAsync(string followerId, ulong startSeq, CancellationToken cancellationToken);
    /// <summary>
    /// Tells the leader that <paramref name="followerId"/> holds its log through <paramref name="appliedThrough"/>.
    /// <paramref name="leaderTerm"/> is the term of the leadership whose stream produced that position (the term of the
    /// attach check and of every chunk); the leader ignores a report for any other term.
    /// </summary>
    Task ReportProgressAsync(string followerId, ulong appliedThrough, long leaderTerm, CancellationToken cancellationToken);
    Task<IReadOnlyDictionary<string, string>> GetFileHashesAsync(IReadOnlyList<ReplicationFileInfo> candidates, CancellationToken cancellationToken);
    IAsyncEnumerable<SnapshotChunk> StreamSnapshotAsync(IReadOnlyList<ReplicationFileInfo> verifiedInventory, CancellationToken cancellationToken);
    Task<string?> GetTermsAsync(CancellationToken cancellationToken);
}

/// <summary>Configuration of a <see cref="ClusterNodeHost"/>.</summary>
public sealed class ClusterNodeHostOptions
{
    public required string NodeId { get; init; }
    public required string Endpoint { get; init; }
    /// <summary>Database directory; Raft state lives in its "raft" subdirectory (kept across snapshot restores).</summary>
    public required string DbPath { get; init; }
    /// <summary>Every member, including this node.</summary>
    public required IReadOnlyList<ClusterMember> Members { get; init; }
    /// <summary>The member that leads during Bootstrap.</summary>
    public required string BootstrapPrimaryId { get; init; }
    /// <summary>Creates the options the database is opened with (CreateIfMissing is forced on; FileSystem selects the storage).</summary>
    public required Func<DbOptions> DbOptionsFactory { get; init; }
    /// <summary>Options of the default column family (e.g. a merge operator); null opens it with default options.</summary>
    public Func<ColumnFamilyOptions>? DefaultColumnFamilyOptionsFactory { get; init; }
    /// <summary>
    /// Whether every log entry (client writes, the leader no-op, replicated chunks on followers) is fsync'd before it
    /// counts: before the leader counts itself towards the commit quorum, before a follower reports or acknowledges it,
    /// before a node's latest sequence number (used in votes and heartbeats) moves past it. This is Raft's stable-storage
    /// rule; without it a power loss of a node can drop entries the quorum relied on and lose acknowledged writes. Writes
    /// of concurrent clients share one fsync (group commit) and a follower syncs once per replicated chunk. False keeps
    /// only process-crash durability (the OS still has the data) for lower latency.
    /// </summary>
    public bool SyncWal { get; init; } = true;

    /// <summary>Clock of the Raft election timer and heartbeats (<see cref="RaftConfig.TimeProvider"/>).</summary>
    public TimeProvider TimeProvider { get; init; } = TimeProvider.System;
    public int ElectionTimeoutMinMs { get; init; } = 1500;
    public int ElectionTimeoutMaxMs { get; init; } = 3000;
    public int HeartbeatIntervalMs { get; init; } = 300;
    public ulong BootstrapInSyncLagThreshold { get; init; } = 1000;
    public int BootstrapPromotionQuorum { get; init; }
    public int MaxChunkBytes { get; init; } = WalTailer.DefaultMaxChunkBytes;
    public int SnapshotChunkBytes { get; init; } = 1024 * 1024;
    /// <summary>Back-off between attempts to reach a leader / retry a failed stream or snapshot.</summary>
    public TimeSpan RetryDelay { get; init; } = TimeSpan.FromMilliseconds(200);
    /// <summary>
    /// How long <see cref="ClusterNodeHost.DisposeAsync"/> waits for RPC handlers that still use the database (a stream
    /// whose consumer stopped reading holds it until its transport gives up) before it closes the database anyway.
    /// </summary>
    public TimeSpan DisposeDrainTimeout { get; init; } = TimeSpan.FromSeconds(10);

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

/// <summary>
/// One node of a Raft cluster: owns the database, the <see cref="RaftClusterNode"/>, the follower's WAL stream from the
/// current leader and the snapshot resync - the managed port of rocksdb-sharp's <c>Tests/ClusterTest/ClusterNodeHost</c>
/// with the transport factored out (<see cref="IClusterNodeClient"/> for outgoing calls; incoming calls are the public
/// <c>Handle*</c>/<c>Stream*</c> methods). See the README for the protocol.
/// </summary>
public sealed class ClusterNodeHost : IAsyncDisposable
{
    private const string RaftDirName = "raft";

    private readonly ClusterNodeHostOptions _cfg;
    private readonly Func<ClusterMember, IClusterNodeClient> _clients;
    private readonly CancellationTokenSource _cts = new();
    private readonly SemaphoreSlim _resyncGate = new(1, 1);
    private readonly Lock _followerLock = new();
    private readonly DatabaseUsageGate _dbGate = new();
    private readonly IFileSystem _fs;

    private RocksDatabase _db = null!;
    private RaftPersistentState _state = null!;
    private RaftClusterNode _node = null!;
    private Task? _followerTask;
    private CancellationTokenSource? _followerCts;
    private string? _following;
    private CancellationTokenSource _leadershipCts = new();
    private volatile bool _resyncing;
    private Task _resyncTask = Task.CompletedTask;
    private int _resyncs;
    private int _walStreamFaults;
    private string? _lastWalStreamFault;
    private readonly FollowerResyncPolicy _resyncPolicy = new();

    public ClusterNodeHost(ClusterNodeHostOptions options, Func<ClusterMember, IClusterNodeClient> clientFactory)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(clientFactory);
        _cfg = options;
        _clients = clientFactory;
        _fs = options.DbOptionsFactory().FileSystem;
    }

    public string NodeId => _cfg.NodeId;
    public RocksDatabase Db => _db;
    public RaftClusterNode Node => _node;
    public bool IsResyncing => _resyncing;
    public int Resyncs => Volatile.Read(ref _resyncs);
    public string DbPath => _cfg.DbPath;
    /// <summary>Cancelled when the host starts shutting down; transports use it to abandon a blocked stream write.</summary>
    public CancellationToken Stopping => _cts.Token;
    /// <summary>How often a WAL stream served by this node ended on an error of its own WAL (Corruption, NotSupported).</summary>
    public int WalStreamFaults => Volatile.Read(ref _walStreamFaults);
    /// <summary>The message of the latest such error, or null.</summary>
    public string? LastWalStreamFault => Volatile.Read(ref _lastWalStreamFault);

    private bool IsBootstrapPrimary => _cfg.BootstrapPrimaryId == _cfg.NodeId;
    private string RaftDir => FileSystemHelpers.Combine(_cfg.DbPath, RaftDirName);

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _fs.CreateDirectoryIfMissing(_cfg.DbPath);
        _state = new RaftPersistentState(_fs, RaftDir);

        // A node without a database pulls a snapshot before it takes part: a new follower, and any node whose Raft state
        // outlived its database (killed during a snapshot restore, which deletes the old files before CURRENT arrives).
        // With an empty log such a node would grant its vote to any candidate, including one that lacks entries this
        // node had helped commit. Only the bootstrap primary of a new cluster (or one still in Bootstrap mode, where no
        // other node can lead) starts from an empty database.
        if (!_fs.FileExists(FileSystemHelpers.Combine(_cfg.DbPath, "CURRENT")) &&
            (!IsBootstrapPrimary || (!_state.IsFresh && _state.Mode == ClusterMode.Cluster)))
        {
            await PullInitialSnapshotAsync(cancellationToken).ConfigureAwait(false);
        }
        _db = await OpenDatabaseAsync(cancellationToken).ConfigureAwait(false);

        var peers = new List<ClusterMember>();
        foreach (ClusterMember m in _cfg.Members)
        {
            if (m.NodeId != _cfg.NodeId) peers.Add(m);
        }
        var raft = new RaftConfig(_cfg.NodeId, _cfg.Endpoint, peers)
        {
            BootstrapPrimary = IsBootstrapPrimary,
            ElectionTimeoutMinMs = _cfg.ElectionTimeoutMinMs,
            ElectionTimeoutMaxMs = _cfg.ElectionTimeoutMaxMs,
            HeartbeatIntervalMs = _cfg.HeartbeatIntervalMs,
            BootstrapInSyncLagThreshold = _cfg.BootstrapInSyncLagThreshold,
            BootstrapPromotionQuorum = _cfg.BootstrapPromotionQuorum,
            TimeProvider = _cfg.TimeProvider,
            SyncLogWrites = _cfg.SyncWal,
        };
        _node = new RaftClusterNode(raft, _state, _db, new Transport(this));
        _node.LeaderChanged += OnLeaderChanged;
        _node.RoleChanged += OnRoleChanged;
        _node.ModeChanged += (_, e) => Log("mode -> " + e.Mode);
        _node.ResyncRequired += (_, _) => TriggerResync("heartbeat term mismatch");
        _node.LeadershipFailed += (_, e) => Log("won the election of term " + e.Term + " but cannot write the log: " + e.Error.Message);
        _node.Start();
        Log("started as " + _node.Role + " in term " + _node.CurrentTerm + " (" + _node.Mode + ") at seq " + _db.GetLatestSequenceNumber());

        if (!IsBootstrapPrimary && _node.Mode == ClusterMode.Bootstrap) EnsureFollowerStream(_cfg.BootstrapPrimaryId);
    }

    private ClusterMember BootstrapMember()
    {
        foreach (ClusterMember m in _cfg.Members)
        {
            if (m.NodeId == _cfg.BootstrapPrimaryId) return m;
        }
        throw new InvalidOperationException("bootstrap primary " + _cfg.BootstrapPrimaryId + " is not a member");
    }

    private WriteOptions? LogWriteOptions => _cfg.SyncWal ? WriteOptions.SyncWrites : null;

    private ClusterMember? Member(string nodeId)
    {
        foreach (ClusterMember m in _cfg.Members)
        {
            if (m.NodeId == nodeId) return m;
        }
        return null;
    }

    private async ValueTask<RocksDatabase> OpenDatabaseAsync(CancellationToken cancellationToken)
    {
        DbOptions o = _cfg.DbOptionsFactory();
        o.CreateIfMissing = true;
        Func<ColumnFamilyOptions>? cf = _cfg.DefaultColumnFamilyOptionsFactory;
        return cf is null
            ? await RocksDatabase.OpenAsync(_cfg.DbPath, o, cancellationToken).ConfigureAwait(false)
            : await RocksDatabase.OpenAsync(_cfg.DbPath, o, cf(), cancellationToken).ConfigureAwait(false);
    }

    // ---------------------------------------------------------------------------------------------------
    // Client-facing writes (leader side)

    /// <summary>Writes without waiting for commit; false when this node is not the leader (or is rebuilding).</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<bool> TryWriteAsLeaderAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken)
    {
        if (_resyncing || _node.Role != RaftRole.Leader || !_dbGate.TryEnter()) return false;
        try
        {
            using WriteBatch b = _db.CreateWriteBatch();
            b.Put(key.Span, value.Span);
            await _db.WriteAsync(LogWriteOptions, b, cancellationToken).ConfigureAwait(false);
            return true;
        }
        finally
        {
            _dbGate.Exit();
        }
    }

    /// <summary>
    /// Writes and acknowledges only once the entry is committed (replicated to a quorum) - the durability contract Raft
    /// offers clients. False when this node is not (or stops being) the leader, or the commit takes longer than
    /// <paramref name="timeout"/> (the write may still commit later). Allocation-free in steady state when
    /// <paramref name="cancellationToken"/> cannot be cancelled (the timeout source is pooled); a cancellable token costs a
    /// linked token source per write.
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<bool> WriteCommittedAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, TimeSpan timeout, CancellationToken cancellationToken)
    {
        if (_resyncing || _node.Role != RaftRole.Leader || !_dbGate.TryEnter()) return false;
        ulong seq;
        // Read before the write: the commit wait accepts the entry only as one of this term.
        long term = _node.CurrentTerm;
        try
        {
            using WriteBatch b = _db.CreateWriteBatch();
            b.Put(key.Span, value.Span);
            await _db.WriteAsync(LogWriteOptions, b, cancellationToken).ConfigureAwait(false);
            seq = _db.GetLatestSequenceNumber();
        }
        finally
        {
            _dbGate.Exit();
        }
        return await WaitForCommitAsync(seq, term, timeout, cancellationToken).ConfigureAwait(false) == ClusterWriteStatus.Committed;
    }

    /// <summary>
    /// Writes <paramref name="batch"/> (atomically: one log entry range) and waits until it is committed. Unlike the
    /// key/value overload the result tells a definite rejection (<see cref="ClusterWriteStatus.NotLeader"/>: nothing was
    /// written) from the indeterminate outcomes (<see cref="ClusterWriteStatus.LeadershipLost"/>,
    /// <see cref="ClusterWriteStatus.TimedOut"/>: the entry is in this node's log and may or may not commit). An exception
    /// from the database write is indeterminate as well.
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<ClusterWriteStatus> WriteCommittedAsync(WriteBatch batch, TimeSpan timeout, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(batch);
        if (_resyncing || _node.Role != RaftRole.Leader || !_dbGate.TryEnter()) return ClusterWriteStatus.NotLeader;
        ulong seq;
        // Read before the write: the commit wait accepts the entry only as one of this term.
        long term = _node.CurrentTerm;
        try
        {
            await _db.WriteAsync(LogWriteOptions, batch, cancellationToken).ConfigureAwait(false);
            seq = _db.GetLatestSequenceNumber();
        }
        finally
        {
            _dbGate.Exit();
        }
        return await WaitForCommitAsync(seq, term, timeout, cancellationToken).ConfigureAwait(false);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<ClusterWriteStatus> WaitForCommitAsync(ulong seq, long term, TimeSpan timeout, CancellationToken cancellationToken)
    {
        bool linked = cancellationToken.CanBeCanceled;
        CancellationTokenSource cts = linked ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken) : RentTimeoutSource();
        try
        {
            cts.CancelAfter(timeout);
            return await _node.WaitForCommitAsync(seq, term, cts.Token).ConfigureAwait(false) ? ClusterWriteStatus.Committed : ClusterWriteStatus.LeadershipLost;
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return ClusterWriteStatus.TimedOut;
        }
        finally
        {
            if (linked) cts.Dispose();
            else ReturnTimeoutSource(cts);
        }
    }

    /// <summary>
    /// Runs <paramref name="use"/> on the current database instance unless it is being rebuilt (resync) or closed; the
    /// instance cannot be disposed while <paramref name="use"/> runs. For reads and diagnostics: writing to a node's
    /// database outside the Raft log diverges the replica.
    /// </summary>
    public bool TryUseDatabase<TState, TResult>(TState state, Func<RocksDatabase, TState, TResult> use, out TResult result)
    {
        ArgumentNullException.ThrowIfNull(use);
        if (!_dbGate.TryEnter())
        {
            result = default!;
            return false;
        }
        try
        {
            result = use(_db, state);
            return true;
        }
        finally
        {
            _dbGate.Exit();
        }
    }

    // Timeout sources of WriteCommittedAsync: reset (TryReset stops the timer) and reused while they did not fire.
    private readonly Stack<CancellationTokenSource> _timeoutSources = new();

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

    /// <summary>A digest of the database (null while resyncing).</summary>
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

    public RequestVoteResult HandleRequestVote(RequestVotePayload request) => _node.HandleRequestVote(request);

    public AppendEntriesResult HandleHeartbeat(HeartbeatPayload payload) => _node.HandleAppendEntries(payload);

    public ClusterNodeStatus GetStatus()
    {
        ulong latest = 0;
        if (_dbGate.TryEnter())
        {
            try
            {
                latest = _db.GetLatestSequenceNumber();
            }
            finally
            {
                _dbGate.Exit();
            }
        }
        return new ClusterNodeStatus(_cfg.NodeId, _node.Role, _node.CurrentTerm, _node.CurrentLeaderId, latest, _node.CommitSeq, _node.Mode,
            _node.BootstrapInSyncCount, _resyncing, Resyncs);
    }

    public ClusterAttachResult HandleCheckLogConsistency(ulong followerLatestSeq, long followerTermAtLatest)
    {
        LogConsistencyResult r = _node.CheckFollowerLogConsistency(followerLatestSeq, followerTermAtLatest);
        return new ClusterAttachResult(r.IsLeader, r.Consistent, r.Term, r.IsLeader ? _node.SerializeTerms() : string.Empty);
    }

    /// <summary>A follower's progress report; false when it was not for this node's current leadership (see <see cref="RaftClusterNode.RecordFollowerProgress"/>).</summary>
    public bool HandleReportProgress(string followerId, ulong appliedThrough, long leaderTerm) =>
        _node.RecordFollowerProgress(followerId, appliedThrough, leaderTerm);

    public string HandleGetTerms() => _node.SerializeTerms();

    /// <summary>
    /// Hash-request half of the delta transfer: signatures only for files this node holds at exactly the requested name
    /// and size. SSTs are immutable, so hashing the live file equals hashing the copy a checkpoint would hard-link.
    /// </summary>
    public async Task<IReadOnlyDictionary<string, string>> HandleGetFileHashesAsync(IReadOnlyList<ReplicationFileInfo> candidates, CancellationToken cancellationToken)
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        if (!_dbGate.TryEnter()) return result;
        try
        {
            foreach (ReplicationFileInfo f in candidates)
            {
                string? hash = await ReplicationDelta.TryHashCandidateAsync(_fs, _cfg.DbPath, f.FileName, f.Size, cancellationToken).ConfigureAwait(false);
                if (hash != null) result[f.FileName] = hash;
            }
        }
        finally
        {
            _dbGate.Exit();
        }
        return result;
    }

    /// <summary>The snapshot stream for a follower (only while leader; empty otherwise, which the follower treats as a refusal).</summary>
    public async IAsyncEnumerable<SnapshotChunk> StreamSnapshotAsync(IReadOnlyList<ReplicationFileInfo> verifiedInventory,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (_node.Role != RaftRole.Leader || !_dbGate.TryEnter()) yield break;
        try
        {
            string dir = _cfg.DbPath.TrimEnd('/') + ".checkpoint-" + Guid.NewGuid().ToString("N");
            using ReplicationSession session = await new ReplicationSource(_db).GetInitialStateAsync(dir, cancellationToken).ConfigureAwait(false);
            await foreach (SnapshotChunk c in session.StreamSnapshotAsync(verifiedInventory, _cfg.SnapshotChunkBytes, cancellationToken).ConfigureAwait(false))
            {
                yield return c;
            }
        }
        finally
        {
            _dbGate.Exit();
        }
    }

    /// <summary>
    /// The WAL stream for a follower, while this node leads; it ends when leadership is lost. A write batch larger than
    /// <see cref="ClusterNodeHostOptions.MaxChunkBytes"/> is sent in pieces (<see cref="WalStreamMessage.IsFragment"/>).
    /// </summary>
    public async IAsyncEnumerable<WalStreamMessage> StreamUpdatesAsync(string followerId, ulong startSeq,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        CancellationToken leadership;
        lock (_followerLock) leadership = _leadershipCts.Token;
        if (_node.Role != RaftRole.Leader || leadership.IsCancellationRequested || !_dbGate.TryEnter()) yield break;
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, leadership, _cts.Token);
            Log("streaming to " + followerId + " from seq " + startSeq);
            RaftClusterNode node = _node;
            await foreach (WalStreamMessage m in WalStreamSender.StreamAsync(_db, startSeq, Tag, node.GetTermForSeq, _cfg.MaxChunkBytes, OnWalStreamFault, cts.Token)
                               .ConfigureAwait(false))
            {
                yield return m;
            }
        }
        finally
        {
            _dbGate.Exit();
        }
    }

    private void OnWalStreamFault(RocksDbException e)
    {
        Interlocked.Increment(ref _walStreamFaults);
        Volatile.Write(ref _lastWalStreamFault, e.Message);
        Log("WAL stream fault (" + e.Code + "): " + e.Message);
    }

    // Read the term before the role: if the role still says Leader afterwards, the term belongs to this leadership. A node
    // that streamed while deposed would otherwise tag batches with the new term while claiming to lead.
    private bool Tag(out string sourceId, out long term)
    {
        sourceId = _cfg.NodeId;
        term = _node.CurrentTerm;
        return _node.Role == RaftRole.Leader && !_resyncing;
    }

    // ---------------------------------------------------------------------------------------------------
    // Role / leader changes (raised under the node's lock: only schedule work here)

    private void OnRoleChanged(object? sender, RoleChangedEventArgs e)
    {
        Log("role -> " + e.Role + " (term " + e.Term + ")");
        lock (_followerLock)
        {
            if (e.Role == RaftRole.Leader)
            {
                if (_leadershipCts.IsCancellationRequested) _leadershipCts = new CancellationTokenSource();
            }
            else
            {
                _leadershipCts.Cancel();
            }
        }
    }

    private void OnLeaderChanged(object? sender, LeaderChangedEventArgs e)
    {
        Log("leader -> " + (e.LeaderId ?? "(none)") + " term " + e.Term);
        if (e.LeaderId == null || e.LeaderId == _cfg.NodeId)
        {
            CancelFollowerStream();
            return;
        }
        EnsureFollowerStream(e.LeaderId);
    }

    // ---------------------------------------------------------------------------------------------------
    // Follower stream

    private void CancelFollowerStream()
    {
        lock (_followerLock)
        {
            _followerCts?.Cancel();
            _followerCts = null;
            _following = null;
        }
    }

    private void EnsureFollowerStream(string leaderId)
    {
        lock (_followerLock)
        {
            if (_resyncing || _cts.IsCancellationRequested) return;
            if (_following == leaderId && _followerTask is { IsCompleted: false }) return;
            _followerCts?.Cancel();
            _followerCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
            _following = leaderId;
            CancellationToken ct = _followerCts.Token;
            Task previous = _followerTask ?? Task.CompletedTask;
            _followerTask = Task.Run(async () =>
            {
                // Never two ingesting loops: the previous one finishes its current chunk first.
                try
                {
                    await previous.ConfigureAwait(false);
                }
                catch (Exception)
                {
                }
                await RunFollowerStreamAsync(leaderId, ct).ConfigureAwait(false);
            });
        }
    }

    private async Task RunFollowerStreamAsync(string leaderId, CancellationToken ct)
    {
        ClusterMember? leader = Member(leaderId);
        if (leader == null) return;
        IClusterNodeClient client = _clients(leader);
        bool stalled = false;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                // Raft's AppendEntries consistency check, done once at attach time: the leader confirms that our
                // (latestSeq, term) tail exists identically in its log. A diverged tail cannot be truncated out of a
                // RocksDB WAL, so the whole replica is rebuilt instead.
                ulong myLatest = _db.GetLatestSequenceNumber();
                ClusterAttachResult? check = await client.CheckLogConsistencyAsync(_cfg.NodeId, myLatest, _node.GetTermForSeq(myLatest), ct).ConfigureAwait(false);
                if (check is null || !check.IsLeader)
                {
                    await Task.Delay(_cfg.RetryDelay, ct).ConfigureAwait(false);
                    continue;
                }
                if (!check.Consistent)
                {
                    TriggerResync("log at " + myLatest + " diverged from leader " + leaderId);
                    return;
                }
                // Learn the term ranges of the entries about to arrive (the stream merges batches across terms); the
                // leader's map replaces whatever we recorded past our log's end.
                _state.AdoptTermsAfter(myLatest, RaftLogTerms.Deserialize(check.Terms));

                Log("following " + leaderId + " from seq " + (myLatest + 1));
                // Synced chunks: the progress reports and heartbeat answers below only ever cover durable entries.
                var consumer = new ReplicationConsumer(_db) { SyncWrites = _cfg.SyncWal };
                // A progress report acknowledges entries of the leader's term: once this node moved to a later term (it
                // granted a vote, possibly while a chunk was being applied, to a candidate whose log ends before that
                // chunk), the old leader must not count them towards a quorum - the new term's leader may lack them.
                // Checked after the entries were applied: a vote granted later saw them in our log.
                // The report names the leadership (term) it belongs to; the leader ignores it in any other term.
                long followTerm = check.Term;
                await using var progress = new ProgressReporter((seq, c) =>
                    _node.CurrentTerm <= followTerm ? client.ReportProgressAsync(_cfg.NodeId, seq, followTerm, c) : Task.CompletedTask, _cfg.RetryDelay);
                progress.Report(myLatest);
                // Per attachment: a stream that ends or fails takes its partial batch with it.
                using var assembler = new WalStreamAssembler(_cfg.MaxBatchBytes);
                await foreach (WalStreamMessage m in client.StreamUpdatesAsync(_cfg.NodeId, myLatest + 1, ct).ConfigureAwait(false))
                {
                    if (m.ResyncRequired)
                    {
                        if (!_resyncPolicy.ShouldRebuild(m, _db.GetLatestSequenceNumber()))
                        {
                            // The snapshot this node was just rebuilt from did not get it past the fault of the leader's
                            // WAL; another one would not either.
                            if (!stalled) Log("leader still cannot stream from " + (_db.GetLatestSequenceNumber() + 1) + " after a resync; retrying the stream: " + m.Reason);
                            stalled = true;
                            break;
                        }
                        TriggerResync("leader cannot stream from " + (_db.GetLatestSequenceNumber() + 1) + ": " + m.Reason);
                        return;
                    }
                    stalled = false;
                    _node.HandleObservedLeader(m.SourceId, m.SourceTerm);
                    // A deposed leader's chunk (we already are in a later term): Raft rejects stale AppendEntries.
                    if (m.SourceTerm < _node.CurrentTerm) break;
                    // The attach check compared our log with the leader's log of followTerm. A chunk of another term
                    // comes from a different leadership of that node (it lost and regained the role in between, possibly
                    // rebuilding its log): check again before applying anything of it.
                    if (m.SourceTerm != followTerm) break;
                    WalAssemblyStatus assembled = assembler.Accept(m, out ReadOnlyMemory<byte> chunk);
                    if (assembled != WalAssemblyStatus.Complete)
                    {
                        // Nothing of an incomplete batch is applied, recorded in the term log or reported.
                        if (assembled == WalAssemblyStatus.Incomplete) continue;
                        ulong at = _db.GetLatestSequenceNumber();
                        if (assembled == WalAssemblyStatus.TooLarge || _resyncPolicy.ShouldRebuildAfterStreamFault(at))
                        {
                            // The stream cannot get this node past the batch; a snapshot carries it as files.
                            TriggerResync("the leader's stream cannot be followed from " + (at + 1) + ": " + assembler.LastError);
                            return;
                        }
                        Log("dropping the stream from " + leaderId + " at " + (at + 1) + ": " + assembler.LastError);
                        break;
                    }
                    if (m.EntryTerm > 0) _state.RecordTermRange(m.FirstSequence, m.EntryTerm);
                    // A faithful replica ingests the leader's batches at exactly the same sequence numbers; a gap or an
                    // overlap diverges the copy.
                    if (!await consumer.TryIngestChunkAsync(chunk, ct).ConfigureAwait(false))
                    {
                        TriggerResync("WAL stream mismatch: expected seq " + consumer.ExpectedNextSequence + ", got " + m.FirstSequence);
                        return;
                    }
                    progress.Report(_db.GetLatestSequenceNumber());
                }
                await Task.Delay(_cfg.RetryDelay, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                Log("follower stream error: " + ex.Message + "; retrying");
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
    // Snapshot pull + full resync

    /// <summary>
    /// Pulls the initial snapshot from the current leader (any member that leads or knows the leader), or from the
    /// bootstrap primary while the cluster is still in Bootstrap mode; retries until it succeeds or the host stops.
    /// </summary>
    private async Task PullInitialSnapshotAsync(CancellationToken cancellationToken)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _cts.Token);
        CancellationToken ct = linked.Token;
        while (true)
        {
            ClusterMember? source = await FindLeaderAsync(ct).ConfigureAwait(false);
            if (source == null && _state.Mode == ClusterMode.Bootstrap && !IsBootstrapPrimary) source = BootstrapMember();
            if (source != null)
            {
                try
                {
                    await PullSnapshotAsync(source, ct).ConfigureAwait(false);
                    return;
                }
                catch (Exception ex) when (!ct.IsCancellationRequested)
                {
                    Log("snapshot from " + source.NodeId + " failed: " + ex.Message + "; retrying");
                }
            }
            await Task.Delay(_cfg.RetryDelay, ct).ConfigureAwait(false);
        }
    }

    private async Task PullSnapshotAsync(ClusterMember source, CancellationToken ct)
    {
        IClusterNodeClient client = _clients(source);
        // Per-file delta with lazy hashing: scan local immutable files by name + size only, ask the source for hashes of
        // the ones it also holds at that exact name + size, and hash our copies of just those candidates.
        List<ReplicationFileInfo> local = ReplicationDelta.ScanImmutableFiles(_fs, _cfg.DbPath);
        List<ReplicationFileInfo> verified = [];
        if (local.Count > 0)
        {
            IReadOnlyDictionary<string, string> sourceHashes = await client.GetFileHashesAsync(local, ct).ConfigureAwait(false);
            verified = await ReplicationDelta.VerifyCandidatesAsync(_fs, _cfg.DbPath, local, sourceHashes, ct).ConfigureAwait(false);
        }
        SnapshotRestoreResult r = await SnapshotReceiver.ReceiveAsync(client.StreamSnapshotAsync(verified, ct), _fs, _cfg.DbPath, [RaftDirName], ct)
            .ConfigureAwait(false);
        // The restored log is byte-for-byte the leader's, so the leader's term map is its correct description.
        string? terms = await client.GetTermsAsync(ct).ConfigureAwait(false) ?? throw new InvalidOperationException("no term map from " + source.NodeId);
        _state.ReplaceTerms(RaftLogTerms.Deserialize(terms));
        Log("snapshot from " + source.NodeId + ": reused " + r.ReusedFiles + " files, transferred " + r.TransferredFiles + " files / " + r.TransferredBytes + " bytes");
    }

    /// <summary>Rebuilds this node from the current leader's snapshot (also triggered automatically on divergence).</summary>
    public void TriggerResync(string reason)
    {
        lock (_followerLock)
        {
            if (_resyncing || _cts.IsCancellationRequested) return;
            _resyncing = true;
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
            _node.BeginResync();
            Task? follower;
            lock (_followerLock)
            {
                _followerCts?.Cancel();
                _followerCts = null;
                _following = null;
                follower = _followerTask;
            }
            if (follower != null)
            {
                try
                {
                    await follower.ConfigureAwait(false);
                }
                catch (Exception)
                {
                }
            }
            // Wait until no RPC handler uses the old instance, then close it.
            await _dbGate.DrainAsync(_cts.Token).ConfigureAwait(false);
            await _db.DisposeAsync().ConfigureAwait(false);

            while (!_cts.IsCancellationRequested)
            {
                try
                {
                    ClusterMember? leader = await FindLeaderAsync(_cts.Token).ConfigureAwait(false);
                    if (leader == null)
                    {
                        await Task.Delay(_cfg.RetryDelay, _cts.Token).ConfigureAwait(false);
                        continue;
                    }
                    await PullSnapshotAsync(leader, _cts.Token).ConfigureAwait(false);
                    _db = await OpenDatabaseAsync(_cts.Token).ConfigureAwait(false);
                    _node.CompleteResync(_db);
                    _resyncPolicy.OnRebuilt(_db.GetLatestSequenceNumber());
                    _dbGate.Reopen();
                    Interlocked.Increment(ref _resyncs);
                    lock (_followerLock) _resyncing = false;
                    Log("resync complete at seq " + _db.GetLatestSequenceNumber() + "; rejoining " + leader.NodeId);
                    EnsureFollowerStream(leader.NodeId);
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

    private async Task<ClusterMember?> FindLeaderAsync(CancellationToken ct)
    {
        // Every reachable member is asked and the answer of the highest term wins: a deposed leader that has not heard of
        // its successor still reports itself as leader, and a snapshot of its log may lack entries this node already
        // acknowledged to the real one. A member in a newer term that knows no leader yet means "wait".
        ClusterMember? best = null;
        long bestTerm = long.MinValue;
        foreach (ClusterMember m in _cfg.Members)
        {
            if (m.NodeId == _cfg.NodeId) continue;
            try
            {
                ClusterNodeStatus? s = await _clients(m).GetStatusAsync(ct).ConfigureAwait(false);
                if (s == null) continue;
                ClusterMember? named = null;
                if (s.Role == RaftRole.Leader && !s.Resyncing) named = m;
                else if (!string.IsNullOrEmpty(s.LeaderId) && s.LeaderId != _cfg.NodeId) named = Member(s.LeaderId);
                if (s.Term > bestTerm)
                {
                    bestTerm = s.Term;
                    best = named;
                }
                else if (s.Term == bestTerm && best == null)
                {
                    best = named;
                }
            }
            catch (Exception) when (!ct.IsCancellationRequested)
            {
            }
        }
        return best;
    }

    // ---------------------------------------------------------------------------------------------------

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync().ConfigureAwait(false);
        Task? follower;
        lock (_followerLock)
        {
            _leadershipCts.Cancel();
            follower = _followerTask;
        }
        if (_node != null) await _node.DisposeAsync().ConfigureAwait(false);
        foreach (Task t in new[] { follower ?? Task.CompletedTask, _resyncTask })
        {
            try
            {
                await t.ConfigureAwait(false);
            }
            catch (Exception)
            {
            }
        }
        // Wait for RPC handlers (streams end with the cancelled tokens) before closing the database.
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
        // Closing is idempotent, so a database already closed by an unfinished resync is fine.
        if (_db != null) await _db.DisposeAsync().ConfigureAwait(false);
    }

    private void Log(string message) => _cfg.Log?.Invoke(_cfg.NodeId + ": " + message);

    private sealed class Transport(ClusterNodeHost host) : IPeerTransport
    {
        public Task<RequestVoteResult?> SendRequestVoteAsync(ClusterMember peer, RequestVotePayload request, CancellationToken cancellationToken) =>
            host._clients(peer).RequestVoteAsync(request, cancellationToken);

        public Task<AppendEntriesResult?> SendHeartbeatAsync(ClusterMember peer, HeartbeatPayload payload, CancellationToken cancellationToken) =>
            host._clients(peer).HeartbeatAsync(payload, cancellationToken);
    }
}
