using RocksDb.Managed.Cluster;
using RocksDb.Managed.Replication.Internal;

namespace RocksDb.Managed.Replication.Hosting;

/// <summary>
/// The lineage of a plainly replicated log (<see cref="ReplicationPrimary"/> / <see cref="ReplicaFollower"/>): which
/// incarnation of the primary wrote which sequence numbers. It is to plain replication what the epoch map is to the
/// pair host and the term map to the Raft cluster.
/// <para>
/// The problem it solves: a primary whose writes are not fsync'd can lose the end of its log in a power loss (or be
/// restored from a backup) and then write different entries at the same sequence numbers. A replica that still holds
/// the lost entries would simply continue behind them - same sequence numbers, different history, no error. Sequence
/// numbers alone cannot tell the two logs apart.
/// </para>
/// <para>
/// Every time a <see cref="ReplicationPrimary"/> is created over a database it starts a new incarnation at the
/// database's latest sequence number + 1 and persists it (fsync'd) before it serves anything. A replica learns the map
/// when it attaches and remembers it; when it attaches again it names the incarnation of its latest entry, and the
/// primary confirms that entry belongs to that incarnation in its own map. After the scenario above the replica's tail
/// lies in an incarnation whose range, on the primary, now belongs to the newer one: the check fails and the replica is
/// told to rebuild from a snapshot.
/// </para>
/// <para>
/// Incarnation ids are increasing and derived from the wall clock (at least the previous id + 1), so that a primary
/// whose own map was lost with its directory (a restore from a backup) does not hand out an id it used before. The map
/// lives in the <c>replication</c> subdirectory of the database directory, on the database's file system. Thread-safe.
/// </para>
/// </summary>
public sealed class ReplicationLineage
{
    /// <summary>Name of the subdirectory of the database directory that holds the map.</summary>
    public const string DirectoryName = "replication";
    private const string FileName = "lineage";

    private readonly IFileSystem _fs;
    private readonly string _path;
    private readonly Lock _gate = new();
    private RaftLogTerms _map;

    private ReplicationLineage(IFileSystem fs, string path, RaftLogTerms map)
    {
        _fs = fs;
        _path = path;
        _map = map;
    }

    /// <summary>Loads the lineage kept with the database directory <paramref name="dbPath"/> (empty if there is none).</summary>
    public static ReplicationLineage Load(IFileSystem fileSystem, string dbPath)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        ArgumentException.ThrowIfNullOrEmpty(dbPath);
        string dir = FileSystemHelpers.Combine(dbPath, DirectoryName);
        string path = FileSystemHelpers.Combine(dir, FileName);
        return new ReplicationLineage(fileSystem, path, RaftLogTerms.Deserialize(FileSystemHelpers.TryReadAllText(fileSystem, path)));
    }

    /// <summary>The lineage of <paramref name="database"/>.</summary>
    public static ReplicationLineage Load(RocksDatabase database)
    {
        ArgumentNullException.ThrowIfNull(database);
        return Load(database.FileSystem, database.Path);
    }

    /// <summary>The incarnation that wrote <paramref name="sequence"/>; 0 when the map says nothing about it.</summary>
    public long GetIncarnationForSeq(ulong sequence)
    {
        lock (_gate) return _map.GetTermForSeq(sequence);
    }

    /// <summary>The newest incarnation in the map (0 when it is empty).</summary>
    public long CurrentIncarnation
    {
        get
        {
            lock (_gate) return _map.GetLast().Term;
        }
    }

    /// <summary>The map as text (one "first sequence,incarnation" line per incarnation).</summary>
    public string Serialize()
    {
        lock (_gate) return _map.Serialize();
    }

    /// <summary>
    /// Starts a new incarnation for the entries after <paramref name="latestSequence"/> and persists it. Ranges the map
    /// had past <paramref name="latestSequence"/> described entries this log no longer has (or never had) and are
    /// dropped. Returns the new incarnation.
    /// </summary>
    public long StartIncarnation(ulong latestSequence)
    {
        lock (_gate)
        {
            RaftLogTerms next = _map.Clone();
            next.TruncateAfter(latestSequence);
            long id = Math.Max(_map.GetLast().Term + 1, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            // The first incarnation of a log covers what the log already holds: every entry then belongs to an
            // incarnation, and "no incarnation" (0) only ever means a replica that has no lineage for its log.
            next.Record(next.GetLast().Term == 0 ? 1 : latestSequence + 1, id);
            Persist(next);
            _map = next;
            return id;
        }
    }

    /// <summary>
    /// Replica side, after a successful attach check at <paramref name="latestSequence"/>: keeps what this map says up
    /// to that entry and takes the primary's description (<paramref name="primaryMap"/>, its <see cref="Serialize"/>)
    /// of everything after it - and, when this map knew nothing about the entry, of everything before it too.
    /// </summary>
    public void AdoptAfter(ulong latestSequence, string? primaryMap)
    {
        RaftLogTerms theirs = RaftLogTerms.Deserialize(primaryMap);
        lock (_gate)
        {
            RaftLogTerms next;
            if (_map.GetTermForSeq(latestSequence) == 0)
            {
                next = theirs;
            }
            else
            {
                next = _map.Clone();
                next.TruncateAfter(latestSequence);
                foreach ((ulong start, long id) in theirs.GetRanges())
                {
                    if (start > latestSequence) next.Record(start, id);
                }
            }
            if (next.Serialize() == _map.Serialize()) return;
            Persist(next);
            _map = next;
        }
    }

    /// <summary>Replaces the whole map with <paramref name="primaryMap"/> (a replica restored from the primary's snapshot).</summary>
    public void Replace(string? primaryMap)
    {
        RaftLogTerms theirs = RaftLogTerms.Deserialize(primaryMap);
        lock (_gate)
        {
            Persist(theirs);
            _map = theirs;
        }
    }

    private void Persist(RaftLogTerms map)
    {
        string? dir = System.IO.Path.GetDirectoryName(_path);
        if (!string.IsNullOrEmpty(dir)) _fs.CreateDirectoryIfMissing(dir);
        FileSystemHelpers.AtomicWrite(_fs, _path, map.Serialize());
    }
}

/// <summary>Answer of a plain primary to a replica's attach check (<see cref="ReplicationPrimary.CheckReplicaLog"/>).</summary>
/// <param name="Consistent">Whether the replica's log, as far as the primary can tell, is a prefix of the primary's.</param>
/// <param name="Lineage">The primary's lineage map (<see cref="ReplicationLineage.Serialize"/>), for the replica to adopt.</param>
/// <param name="Reason">Why the check failed (null when consistent).</param>
public sealed record ReplicaAttachCheck(bool Consistent, string Lineage, string? Reason);
