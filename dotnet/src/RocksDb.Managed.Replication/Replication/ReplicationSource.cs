using System.Runtime.CompilerServices;

namespace RocksDb.Managed.Replication;

/// <summary>
/// The primary side of replication (port of rocksdb-sharp <c>ReplicationSource</c>, plus the WAL retention logic of its
/// sample <c>ReplicationService</c>):
/// <list type="bullet">
/// <item><see cref="GetInitialStateAsync"/>: a checkpoint for a new (or diverged) replica, as a <see cref="ReplicationSession"/>;</item>
/// <item><see cref="TailWal(ulong)"/>: a long-lived <see cref="WalTailer"/> for the continuous stream;</item>
/// <item><see cref="GetWalUpdatesAsync"/>: the batches available now, as pooled <see cref="WalBatch"/> views;</item>
/// <item><see cref="ReportReplicaProgress"/> / <see cref="RetireArchivedWals()"/>: retire archived WALs only once every
/// replica has applied them (rocksdb-sharp parsed WAL files with <c>RocksDbWalInspector</c> for this; the managed port
/// uses <see cref="RocksDatabase.GetSortedWalFiles"/>, which also handles compressed and recyclable WALs).</item>
/// </list>
/// Thread-safe.
/// <para>
/// Invariant everything rests on: a replica applies the source's batches at exactly the same sequence numbers. Writes
/// with <c>DisableWal</c> and <c>IngestExternalFiles</c> on the source bypass the WAL and are never replicated; writes on
/// a replica make it diverge.
/// </para>
/// </summary>
public sealed class ReplicationSource
{
    private readonly RocksDatabase _db;
    private readonly Lock _gate = new();
    private readonly Dictionary<string, ulong> _replicaProgress = new(StringComparer.Ordinal);
    private int _sessionsInFlight;

    public ReplicationSource(RocksDatabase db)
    {
        ArgumentNullException.ThrowIfNull(db);
        _db = db;
    }

    public RocksDatabase Database => _db;

    /// <summary>
    /// Creates a checkpoint of the database in <paramref name="checkpointDirectory"/> (which must not exist; it is created
    /// through the database's file system). File deletions stay disabled until the returned session releases them (see
    /// <see cref="ReplicationSession"/>), so the WALs a replica needs right after the checkpoint cannot be purged in
    /// between - rocksdb-sharp required callers to <c>DisableFileDeletions</c> themselves. Memtables are flushed first.
    /// </summary>
    public async ValueTask<ReplicationSession> GetInitialStateAsync(string checkpointDirectory, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(checkpointDirectory);
        _db.DisableFileDeletions();
        lock (_gate) _sessionsInFlight++;
        try
        {
            ulong seq = await _db.CreateCheckpointAsync(checkpointDirectory, cancellationToken).ConfigureAwait(false);
            return new ReplicationSession(_db.FileSystem, checkpointDirectory, seq, this);
        }
        catch
        {
            OnSessionReleased();
            throw;
        }
    }

    internal void OnSessionReleased()
    {
        lock (_gate) _sessionsInFlight--;
        try
        {
            _db.EnableFileDeletions();
        }
        catch (ObjectDisposedException)
        {
            // The database was closed before the session: nothing left to re-enable.
        }
    }

    /// <summary>Number of <see cref="ReplicationSession"/>s still holding their file-deletion pin.</summary>
    public int SessionsInFlight
    {
        get
        {
            lock (_gate) return _sessionsInFlight;
        }
    }

    /// <summary>Opens a long-lived tail on the WAL starting at <paramref name="sequenceNumber"/> (chunks of up to 256 KiB).</summary>
    public WalTailer TailWal(ulong sequenceNumber) => new(_db, sequenceNumber, WalTailer.DefaultMaxChunkBytes);

    public WalTailer TailWal(ulong sequenceNumber, int maxChunkBytes) => new(_db, sequenceNumber, maxChunkBytes);

    /// <summary>
    /// The batches the log holds from the one containing <paramref name="sequenceNumber"/> on, without waiting for new
    /// writes (rocksdb-sharp <c>GetWalUpdates</c> / <c>GetPooledWalUpdates</c>: no <c>byte[]</c> per batch any more; each
    /// <see cref="WalBatch"/> is a view valid until the enumeration moves on). Prefer <see cref="TailWal(ulong)"/> for a
    /// continuous stream.
    /// </summary>
    public async IAsyncEnumerable<WalBatch> GetWalUpdatesAsync(ulong sequenceNumber, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (sequenceNumber > _db.GetLatestSequenceNumber()) yield break;
        using TransactionLogIterator it = _db.GetUpdatesSince(sequenceNumber);
        await foreach (WalBatch b in it.WithCancellation(cancellationToken).ConfigureAwait(false)) yield return b;
    }

    // ---- WAL retention by replica progress ----------------------------------------------------------------

    /// <summary>
    /// Records that <paramref name="replicaId"/> has applied every entry up to <paramref name="appliedThrough"/>. The
    /// slowest replica decides what may be retired - a minimum across replicas, not across time, and a replica never
    /// moves backwards.
    /// </summary>
    public void ReportReplicaProgress(string replicaId, ulong appliedThrough)
    {
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        lock (_gate)
        {
            if (!_replicaProgress.TryGetValue(replicaId, out ulong prev) || appliedThrough > prev) _replicaProgress[replicaId] = appliedThrough;
        }
    }

    /// <summary>Stops tracking a replica (it was removed; it no longer holds WAL back).</summary>
    public void ForgetReplica(string replicaId)
    {
        lock (_gate) _replicaProgress.Remove(replicaId);
    }

    /// <summary>The smallest sequence number every tracked replica has applied, or null when no replica reported yet.</summary>
    public ulong? SlowestReplicaSequence
    {
        get
        {
            lock (_gate)
            {
                if (_replicaProgress.Count == 0) return null;
                ulong min = ulong.MaxValue;
                foreach (ulong v in _replicaProgress.Values) min = Math.Min(min, v);
                return min;
            }
        }
    }

    /// <summary>
    /// Deletes the archived WALs every tracked replica has applied. Nothing is retired while a checkpoint session holds
    /// its pin (the replica it is for will ask for updates from where the checkpoint ended) or before any replica
    /// reported. Live WALs are never touched: they are moved to the archive by the database first (requires
    /// <see cref="DbOptions.WalTtlSeconds"/> or <see cref="DbOptions.WalSizeLimitMb"/>; otherwise obsolete WALs are deleted
    /// right away and replicas that fall behind need a checkpoint). Returns the number of files deleted.
    /// </summary>
    public int RetireArchivedWals()
    {
        ulong slowest;
        lock (_gate)
        {
            if (_sessionsInFlight > 0 || _replicaProgress.Count == 0) return 0;
            slowest = ulong.MaxValue;
            foreach (ulong v in _replicaProgress.Values) slowest = Math.Min(slowest, v);
        }
        return RetireArchivedWals(_db, slowest);
    }

    /// <summary>
    /// Deletes the archived WALs of <paramref name="db"/> whose entries are all at or below
    /// <paramref name="appliedThrough"/>. A WAL's last entry is the next WAL's first sequence number minus one, so the
    /// newest WAL is always kept.
    /// </summary>
    public static int RetireArchivedWals(RocksDatabase db, ulong appliedThrough)
    {
        ArgumentNullException.ThrowIfNull(db);
        IReadOnlyList<WalFileInfo> files = db.GetSortedWalFiles();
        int deleted = 0;
        for (int i = 0; i + 1 < files.Count; i++)
        {
            WalFileInfo f = files[i];
            if (f.Type != WalFileType.ArchivedLogFile) continue;
            ulong last = files[i + 1].StartSequence - 1;
            if (last > appliedThrough) break;
            try
            {
                db.DeleteArchivedWalFile(f.LogNumber);
                deleted++;
            }
            catch (RocksDbException e) when (e.Code == StatusCode.NotFound)
            {
                // Purged by WAL_ttl_seconds in the meantime.
            }
        }
        return deleted;
    }
}
