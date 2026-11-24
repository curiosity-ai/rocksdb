using System.Globalization;
using System.Runtime.CompilerServices;
using RocksDb.Managed.Replication.Internal;

namespace RocksDb.Managed.Replication.Hosting;

/// <summary>
/// What a follower remembers about a chunk of the source's WAL stream whose write failed, so that the entries of that
/// chunk are never taken for applied.
/// <para>
/// The hazard: a write group allocates its sequence numbers before the WAL append. When the append or its fsync fails,
/// the engine sets a WAL-related background error, and the recovery from it (automatic by default, or a manual
/// <c>Resume</c>) publishes the allocated range as the latest sequence number - by design, like upstream's
/// <c>DBImpl::ResumeImpl</c>: the bytes may have reached storage, so the range must not be reused - although the data
/// is in neither the memtable nor the surviving WAL. A replica identifies its position by its latest sequence number
/// (<see cref="ReplicationConsumer.ExpectedNextSequence"/>), so after such a recovery it would attach behind the failed
/// chunk, report it as held and never receive it: a silent hole, and for a Raft leader or a primary acknowledging on the
/// replica's report, an acknowledged write that exists on fewer nodes than counted.
/// </para>
/// <para>
/// Two measures. <see cref="ReplicaRecoveryVeto"/> keeps the engine from recovering on its own while the database is a
/// replica's, so the range is not published and a restart replays a consistent prefix of the log. And the last
/// sequence number that was applied is recorded when an apply fails (<see cref="RecordFailure"/>: in memory for the
/// database instance and durably in the database directory, so it also survives a restart): before the follower
/// attaches again it compares (<see cref="IsLogEndUnknown(RocksDatabase)"/>), and a database whose log now ends anywhere
/// else is rebuilt from a snapshot instead of followed. The marker lives in the database directory, which a snapshot
/// restore replaces, so a rebuild removes it.
/// </para>
/// </summary>
public static class ReplicaApplyGuard
{
    /// <summary>Name of the marker file in the database directory (one line: the last applied sequence number).</summary>
    public const string MarkerFileName = "REPLICA_APPLY_FAILED";

    private sealed class Recorded
    {
        public ulong LastApplied;
        public bool Set;
    }

    private static readonly ConditionalWeakTable<RocksDatabase, Recorded> s_recorded = new();

    /// <summary>
    /// Records that a chunk could not be applied to <paramref name="replica"/> while its log ended at
    /// <paramref name="lastApplied"/>. The marker file is best effort (the disk that failed the write may fail this one
    /// too); the in-memory record always holds for this database instance.
    /// </summary>
    public static void RecordFailure(RocksDatabase replica, ulong lastApplied)
    {
        ArgumentNullException.ThrowIfNull(replica);
        Recorded r = s_recorded.GetOrCreateValue(replica);
        lock (r)
        {
            // The first failure since the last consistent attach names the position; later ones happen at or after it.
            if (!r.Set)
            {
                r.LastApplied = lastApplied;
                r.Set = true;
            }
            lastApplied = r.LastApplied;
        }
        try
        {
            IFileSystem fs = replica.FileSystem;
            string path = FileSystemHelpers.Combine(replica.Path, MarkerFileName);
            if (!fs.FileExists(path)) FileSystemHelpers.AtomicWrite(fs, path, lastApplied.ToString(CultureInfo.InvariantCulture));
        }
        catch (Exception e) when (e is IOException or RocksDbException or UnauthorizedAccessException)
        {
        }
    }

    /// <summary>
    /// Whether an earlier failed apply left <paramref name="replica"/>'s log ending somewhere else than at the last
    /// applied entry (the failed range was published, or a restart replayed the failed write from the WAL): its log end
    /// no longer says what it holds, and it must be rebuilt from a snapshot. When the log still ends where it did, the
    /// record is cleared and false is returned: following can go on from there.
    /// </summary>
    public static bool IsLogEndUnknown(RocksDatabase replica)
    {
        ArgumentNullException.ThrowIfNull(replica);
        ulong latest = replica.GetLatestSequenceNumber();
        bool unknown = false;
        if (s_recorded.TryGetValue(replica, out Recorded? r))
        {
            lock (r)
            {
                if (r.Set)
                {
                    if (r.LastApplied != latest) unknown = true;
                    else r.Set = false;
                }
            }
        }
        IFileSystem fs = replica.FileSystem;
        if (TryReadMarker(fs, replica.Path, out ulong recorded))
        {
            if (recorded != latest) unknown = true;
            else if (!unknown) DeleteMarker(fs, replica.Path);
        }
        return unknown;
    }

    /// <summary>Reads the marker of the database directory <paramref name="dbPath"/>; false when there is none.</summary>
    public static bool TryReadMarker(IFileSystem fileSystem, string dbPath, out ulong lastApplied)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        lastApplied = 0;
        string? text;
        try
        {
            text = FileSystemHelpers.TryReadAllText(fileSystem, FileSystemHelpers.Combine(dbPath, MarkerFileName));
        }
        catch (Exception e) when (e is IOException or RocksDbException)
        {
            return false;
        }
        if (text is null) return false;
        // A marker that cannot be read as a number still says "an apply failed": no sequence number equals it.
        if (!ulong.TryParse(text.Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out lastApplied)) lastApplied = ulong.MaxValue;
        return true;
    }

    private static void DeleteMarker(IFileSystem fs, string dbPath)
    {
        try
        {
            fs.DeleteFile(FileSystemHelpers.Combine(dbPath, MarkerFileName));
            fs.SyncDirectory(dbPath);
        }
        catch (Exception e) when (e is IOException or RocksDbException)
        {
        }
    }
}

/// <summary>
/// Event listener for a replica's database: disables the engine's automatic recovery from a background error of the
/// write path (and from any background error once <c>isReplica</c> says the database is being followed into), see
/// <see cref="ReplicaApplyGuard"/>. A replica with such an error is rebuilt from a snapshot by its host (or, with plain
/// replication, by the application when <see cref="ReplicaFollower"/> asks for a resync); recovering in place would
/// publish sequence numbers whose data the replica does not have. Add it with
/// <see cref="ReplicaFollower.ProtectReplicaOptions"/>; the pair and cluster hosts add it themselves.
/// </summary>
public sealed class ReplicaRecoveryVeto : EventListener
{
    private readonly Func<bool> _isReplica;

    /// <param name="isReplica">Whether the database is currently written by a replication stream (asked per error).</param>
    public ReplicaRecoveryVeto(Func<bool> isReplica)
    {
        ArgumentNullException.ThrowIfNull(isReplica);
        _isReplica = isReplica;
    }

    public override void OnErrorRecoveryBegin(ErrorRecoveryBeginEventArgs e)
    {
        bool replica;
        try
        {
            replica = _isReplica();
        }
        catch (Exception)
        {
            replica = true;
        }
        if (replica) e.AutoRecovery = false;
    }
}
