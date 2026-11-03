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
    public int MaxChunkBytes { get; init; } = WalTailer.DefaultMaxChunkBytes;
    public int SnapshotChunkBytes { get; init; } = 1024 * 1024;
    public TimeSpan RetryDelay { get; init; } = TimeSpan.FromMilliseconds(200);
    public Action<string>? Log { get; init; }
}

/// <summary>Result of a client write sent to a pair node.</summary>
public enum PairWriteStatus
{
    Applied = 0,
    /// <summary>This node is not the primary (or the hold timed out): retry on <see cref="PairWriteResult.PrimaryHint"/>.</summary>
    NotPrimary = 1,
}

public readonly record struct PairWriteResult(PairWriteStatus Status, string? PrimaryHint, ulong Sequence);

/// <summary>Status of a pair node.</summary>
public sealed record PairNodeStatus(ReplicationNodeStatus Node, bool Resyncing, int Resyncs, bool WritesOpen, ulong AttachedFromSeq);

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
public sealed class PairNodeHost : IAsyncDisposable
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
    // Cancelled whenever this node becomes a replica: ends the WAL streams it serves as primary.
    private CancellationTokenSource _primaryCts = new();

    public PairNodeHost(PairNodeHostOptions options, IPairNodeClient peer)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(peer);
        _cfg = options;
        _peer = peer;
        _fs = options.DbOptionsFactory().FileSystem;
    }

    public string NodeId => _cfg.NodeId;
    public RocksDatabase Db => _db;
    public SwitchoverNode Node => _node;
    public bool IsResyncing => _resyncing;
    public int Resyncs => Volatile.Read(ref _resyncs);
    public PairFaults Faults { get; } = new();

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _fs.CreateDirectoryIfMissing(_cfg.DbPath);
        _state = new ReplicationRoleState(_fs, FileSystemHelpers.Combine(_cfg.DbPath, RoleDirName));
        bool replica = _state.IsFresh ? _cfg.InitialRole == ReplicationRole.Replica : _state.Role == ReplicationRole.Replica;
        if (replica && !_fs.FileExists(FileSystemHelpers.Combine(_cfg.DbPath, "CURRENT")))
        {
            // A new replica, or one that died while restoring a snapshot.
            await PullSnapshotWithRetryAsync(cancellationToken).ConfigureAwait(false);
            if (_state.IsFresh) _state.Set(0, ReplicationRole.Replica, _cfg.PeerId);
        }
        _db = await OpenDatabaseAsync(cancellationToken).ConfigureAwait(false);
        if (_cfg.InitialRole == ReplicationRole.Primary) SwitchoverNode.BootstrapPrimary(_state, _db, _cfg.NodeId);
        _node = new SwitchoverNode(_cfg.NodeId, _state, _db, DetachFromPrimaryAsync);

        // A node restarting as primary may have been replaced while it was down (ForcePromoteAsync on the other node):
        // ask before serving writes. If the other node cannot be reached, serve anyway - with two nodes there is nobody
        // else to ask.
        if (_node.Role != ReplicationRole.Replica)
        {
            SwitchoverResult? resolved = await _node.ResolveAsync(_peer, _cfg.Switchover, cancellationToken).ConfigureAwait(false);
            if (resolved != null) Log("resolve at startup: " + resolved.Outcome + " (epoch " + resolved.Epoch + ") " + resolved.Reason);
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
            if (e.Role == ReplicationRole.Replica) EnsureFollowerStream();
        };
        if (_node.Role == ReplicationRole.Replica) _primaryCts.Cancel();
        Log("started as " + _node.Role + " in epoch " + _node.Epoch + " at seq " + _db.GetLatestSequenceNumber());
        if (_node.Role == ReplicationRole.Replica) EnsureFollowerStream();
    }

    private async ValueTask<RocksDatabase> OpenDatabaseAsync(CancellationToken cancellationToken)
    {
        DbOptions o = _cfg.DbOptionsFactory();
        o.CreateIfMissing = true;
        return await RocksDatabase.OpenAsync(_cfg.DbPath, o, cancellationToken).ConfigureAwait(false);
    }

    // ---------------------------------------------------------------------------------------------------
    // Client writes and switchover

    /// <summary>
    /// Applies a client write if this node is the primary; while a handoff holds writes it waits up to
    /// <paramref name="holdTimeout"/>. Otherwise answers <see cref="PairWriteStatus.NotPrimary"/> with a hint.
    /// </summary>
    public async ValueTask<PairWriteResult> WriteAsync(WriteBatch batch, TimeSpan holdTimeout, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(batch);
        if (!await _node.Writes.TryEnterAsync(holdTimeout, cancellationToken).ConfigureAwait(false))
            return new PairWriteResult(PairWriteStatus.NotPrimary, _node.PrimaryId == _cfg.NodeId ? null : _node.PrimaryId, 0);
        try
        {
            await _db.WriteAsync(batch, cancellationToken).ConfigureAwait(false);
            return new PairWriteResult(PairWriteStatus.Applied, null, _db.GetLatestSequenceNumber());
        }
        finally
        {
            _node.Writes.Exit();
        }
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
        new(GetNodeStatus(), _resyncing, Resyncs, _node.Writes.IsOpen, Volatile.Read(ref _attachedFromSeq));

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
        _node.CheckReplicaLog(replicaLatestSeq, replicaEpochAtLatest);

    public string HandleGetEpochs() => _node.SerializeEpochs();

    /// <summary>The WAL stream for the replica while this node is primary (or handing off).</summary>
    public async IAsyncEnumerable<WalStreamMessage> StreamUpdatesAsync(ulong startSeq, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        CancellationToken primary;
        lock (_followerLock) primary = _primaryCts.Token;
        if (_node.Role == ReplicationRole.Replica || primary.IsCancellationRequested || !_dbGate.TryEnter()) yield break;
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _cts.Token, primary);
            SwitchoverNode node = _node;
            await foreach (WalStreamMessage m in WalStreamSender.StreamAsync(_db, startSeq, Tag, node.GetEpochForSeq, _cfg.MaxChunkBytes, cts.Token)
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
        if (_node.Role == ReplicationRole.Replica || !_dbGate.TryEnter()) yield break;
        try
        {
            string dir = _cfg.DbPath.TrimEnd('/') + ".checkpoint-" + Guid.NewGuid().ToString("N");
            using ReplicationSession session = await new ReplicationSource(_db).GetInitialStateAsync(dir, cancellationToken).ConfigureAwait(false);
            // A full transfer: after a switchover the two nodes flushed their own SSTs, so a delta would reuse little.
            await foreach (SnapshotChunk c in session.StreamSnapshotAsync([], _cfg.SnapshotChunkBytes, cancellationToken).ConfigureAwait(false))
            {
                yield return c;
            }
        }
        finally
        {
            _dbGate.Exit();
        }
    }

    // ---------------------------------------------------------------------------------------------------
    // Replica side: the WAL stream from the primary

    private void EnsureFollowerStream()
    {
        lock (_followerLock)
        {
            if (_resyncing || _cts.IsCancellationRequested) return;
            if (_followerCts != null && _followerTask is { IsCompleted: false }) return;
            _followerCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
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
        while (!ct.IsCancellationRequested)
        {
            try
            {
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

                var consumer = new ReplicationConsumer(_db);
                await foreach (WalStreamMessage m in _peer.StreamUpdatesAsync(latest + 1, ct).ConfigureAwait(false))
                {
                    if (m.ResyncRequired)
                    {
                        TriggerResync(_cfg.PeerId + " cannot stream from " + (_db.GetLatestSequenceNumber() + 1) + ": " + m.Reason);
                        return;
                    }
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
                    await _ingestLock.WaitAsync(ct).ConfigureAwait(false);
                    try
                    {
                        if (ct.IsCancellationRequested) return;
                        // A replica applies the primary's entries at exactly the same sequence numbers - which is what
                        // lets it take over at the handoff point. Anything else diverges.
                        if (!await consumer.TryIngestChunkAsync(m.Data, CancellationToken.None).ConfigureAwait(false))
                        {
                            TriggerResync("stream mismatch: expected seq " + consumer.ExpectedNextSequence + ", got " + m.FirstSequence);
                            return;
                        }
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
        SnapshotRestoreResult r = await SnapshotReceiver.ReceiveAsync(_peer.StreamSnapshotAsync(ct), _fs, _cfg.DbPath, [RoleDirName], ct).ConfigureAwait(false);
        // The restored log is byte-for-byte the primary's, so its epoch map is the right description of it.
        string? epochs = await _peer.GetEpochsAsync(ct).ConfigureAwait(false) ?? throw new InvalidOperationException("no epoch map from " + _cfg.PeerId);
        _state.ReplaceEpochs(epochs);
        Log("snapshot from " + _cfg.PeerId + ": " + r.TransferredFiles + " files, " + r.TransferredBytes + " bytes");
    }

    private void TriggerResync(string reason)
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
            _node.DetachDatabase();
            await StopFollowerStreamAsync().ConfigureAwait(false);
            await _dbGate.DrainAsync(_cts.Token).ConfigureAwait(false);
            await _db.DisposeAsync().ConfigureAwait(false);
            while (!_cts.IsCancellationRequested)
            {
                try
                {
                    await PullSnapshotAsync(_cts.Token).ConfigureAwait(false);
                    _db = await OpenDatabaseAsync(_cts.Token).ConfigureAwait(false);
                    _node.AttachDatabase(_db);
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
        lock (_followerLock) _primaryCts.Cancel();
        if (_node != null)
        {
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
            await _dbGate.DrainAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception)
        {
        }
        if (_db != null) await _db.DisposeAsync().ConfigureAwait(false);
    }

    private void Log(string message) => _cfg.Log?.Invoke(_cfg.NodeId + ": " + message);
}
