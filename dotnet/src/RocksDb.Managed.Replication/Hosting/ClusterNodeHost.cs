using System.Runtime.CompilerServices;
using RocksDb.Managed.Cluster;
using RocksDb.Managed.Replication.Internal;

namespace RocksDb.Managed.Replication.Hosting;

/// <summary>Status of a cluster node, as reported by <see cref="ClusterNodeHost.GetStatus"/>.</summary>
public sealed record ClusterNodeStatus(string NodeId, RaftRole Role, long Term, string? LeaderId, ulong LatestSeq, ulong CommitSeq, ClusterMode Mode,
    int BootstrapInSyncCount, bool Resyncing, int Resyncs);

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
    Task ReportProgressAsync(string followerId, ulong appliedThrough, CancellationToken cancellationToken);
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
    public int ElectionTimeoutMinMs { get; init; } = 1500;
    public int ElectionTimeoutMaxMs { get; init; } = 3000;
    public int HeartbeatIntervalMs { get; init; } = 300;
    public ulong BootstrapInSyncLagThreshold { get; init; } = 1000;
    public int BootstrapPromotionQuorum { get; init; }
    public int MaxChunkBytes { get; init; } = WalTailer.DefaultMaxChunkBytes;
    public int SnapshotChunkBytes { get; init; } = 1024 * 1024;
    /// <summary>Back-off between attempts to reach a leader / retry a failed stream or snapshot.</summary>
    public TimeSpan RetryDelay { get; init; } = TimeSpan.FromMilliseconds(200);
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

    private bool IsBootstrapPrimary => _cfg.BootstrapPrimaryId == _cfg.NodeId;
    private string RaftDir => FileSystemHelpers.Combine(_cfg.DbPath, RaftDirName);

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _fs.CreateDirectoryIfMissing(_cfg.DbPath);
        _state = new RaftPersistentState(_fs, RaftDir);

        // Non-bootstrap nodes pull an initial snapshot only when the DB is fresh.
        if (!IsBootstrapPrimary && !_fs.FileExists(FileSystemHelpers.Combine(_cfg.DbPath, "CURRENT")))
        {
            await PullSnapshotWithRetryAsync(BootstrapMember(), cancellationToken).ConfigureAwait(false);
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
        };
        _node = new RaftClusterNode(raft, _state, _db, new Transport(this));
        _node.LeaderChanged += OnLeaderChanged;
        _node.RoleChanged += OnRoleChanged;
        _node.ModeChanged += (_, e) => Log("mode -> " + e.Mode);
        _node.ResyncRequired += (_, _) => TriggerResync("heartbeat term mismatch");
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
        return await RocksDatabase.OpenAsync(_cfg.DbPath, o, cancellationToken).ConfigureAwait(false);
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
            await _db.PutAsync(key, value, cancellationToken).ConfigureAwait(false);
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
        try
        {
            using WriteBatch b = _db.CreateWriteBatch();
            b.Put(key.Span, value.Span);
            await _db.WriteAsync(b, cancellationToken).ConfigureAwait(false);
            seq = _db.GetLatestSequenceNumber();
        }
        finally
        {
            _dbGate.Exit();
        }
        if (_node.CommitSeq >= seq) return true;
        bool linked = cancellationToken.CanBeCanceled;
        CancellationTokenSource cts = linked ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken) : RentTimeoutSource();
        try
        {
            cts.CancelAfter(timeout);
            return await _node.WaitForCommitAsync(seq, cts.Token).ConfigureAwait(false);
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

    public RequestVoteResult HandleRequestVote(RequestVotePayload request) =>
        _node.HandleRequestVote(request.Term, request.CandidateId, request.LastLogSeq, request.LastLogTerm);

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

    public void HandleReportProgress(string followerId, ulong appliedThrough) => _node.RecordFollowerProgress(followerId, appliedThrough);

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

    /// <summary>The WAL stream for a follower, while this node leads; it ends when leadership is lost.</summary>
    public async IAsyncEnumerable<WalStreamMessage> StreamUpdatesAsync(string followerId, ulong startSeq, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        CancellationToken leadership;
        lock (_followerLock) leadership = _leadershipCts.Token;
        if (_node.Role != RaftRole.Leader || leadership.IsCancellationRequested || !_dbGate.TryEnter()) yield break;
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, leadership, _cts.Token);
            Log("streaming to " + followerId + " from seq " + startSeq);
            RaftClusterNode node = _node;
            await foreach (WalStreamMessage m in WalStreamSender.StreamAsync(_db, startSeq, Tag, node.GetTermForSeq, _cfg.MaxChunkBytes, cts.Token)
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
                // Learn the term ranges of the entries about to arrive (the stream merges batches across terms).
                foreach ((ulong start, long term) in RaftLogTerms.Deserialize(check.Terms).GetRanges())
                {
                    if (start > myLatest) _state.RecordTermRange(start, term);
                }

                Log("following " + leaderId + " from seq " + (myLatest + 1));
                var consumer = new ReplicationConsumer(_db);
                await using var progress = new ProgressReporter((seq, c) => client.ReportProgressAsync(_cfg.NodeId, seq, c));
                progress.Report(myLatest);
                await foreach (WalStreamMessage m in client.StreamUpdatesAsync(_cfg.NodeId, myLatest + 1, ct).ConfigureAwait(false))
                {
                    if (m.ResyncRequired)
                    {
                        TriggerResync("leader cannot stream from " + (_db.GetLatestSequenceNumber() + 1) + ": " + m.Reason);
                        return;
                    }
                    _node.HandleObservedLeader(m.SourceId, m.SourceTerm);
                    if (m.EntryTerm > 0) _state.RecordTermRange(m.FirstSequence, m.EntryTerm);
                    // A faithful replica ingests the leader's batches at exactly the same sequence numbers; a gap or an
                    // overlap diverges the copy.
                    if (!await consumer.TryIngestChunkAsync(m.Data, ct).ConfigureAwait(false))
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

    private async Task PullSnapshotWithRetryAsync(ClusterMember source, CancellationToken cancellationToken)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _cts.Token);
        for (int attempt = 0; ; attempt++)
        {
            try
            {
                await PullSnapshotAsync(source, linked.Token).ConfigureAwait(false);
                return;
            }
            catch (Exception ex) when (attempt < 100 && !linked.IsCancellationRequested)
            {
                Log("snapshot from " + source.NodeId + " failed: " + ex.Message + "; retrying");
                await Task.Delay(_cfg.RetryDelay, linked.Token).ConfigureAwait(false);
            }
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
        foreach (ClusterMember m in _cfg.Members)
        {
            if (m.NodeId == _cfg.NodeId) continue;
            try
            {
                ClusterNodeStatus? s = await _clients(m).GetStatusAsync(ct).ConfigureAwait(false);
                if (s == null) continue;
                if (s.Role == RaftRole.Leader && !s.Resyncing) return m;
                if (!string.IsNullOrEmpty(s.LeaderId) && s.LeaderId != _cfg.NodeId)
                {
                    ClusterMember? l = Member(s.LeaderId);
                    if (l != null) return l;
                }
            }
            catch (Exception) when (!ct.IsCancellationRequested)
            {
            }
        }
        return null;
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
        try
        {
            await _dbGate.DrainAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception)
        {
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
