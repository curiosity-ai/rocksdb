using System.Globalization;
using System.Text;
using RocksDb.Managed.Replication.Internal;

namespace RocksDb.Managed.Cluster;

/// <summary>
/// The Raft state that must survive a crash (rocksdb-sharp <c>RaftPersistentState</c>): currentTerm, votedFor (in the
/// current term), the cluster mode, and the per-sequence term log (<see cref="RaftLogTerms"/>) so a recovering node can
/// answer "what was the term of entry X?". Files <c>raft.state</c> and <c>raft.terms</c> in the given directory.
/// <para>
/// Everything is written through a temp file that is fsync'd and renamed over the target (and the directory fsync'd),
/// so a crash mid-write cannot corrupt the state and an acknowledged vote/term is never rolled back by power loss.
/// The in-memory term, vote and mode change only after that write succeeded.
/// Managed difference: the files go through an <see cref="IFileSystem"/> (e.g. the database's), not System.IO.
/// </para>
/// </summary>
public sealed class RaftPersistentState
{
    private readonly IFileSystem _fs;
    private readonly string _statePath;
    private readonly string _termsPath;
    private readonly Lock _gate = new();

    private long _currentTerm;
    private string? _votedFor;
    private int _mode; // ClusterMode
    private RaftLogTerms _terms = new();

    public RaftPersistentState(string directory) : this(LocalFileSystem.Instance, directory)
    {
    }

    public RaftPersistentState(IFileSystem fileSystem, string directory)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        ArgumentException.ThrowIfNullOrEmpty(directory);
        _fs = fileSystem;
        _fs.CreateDirectoryIfMissing(directory);
        _statePath = FileSystemHelpers.Combine(directory, "raft.state");
        _termsPath = FileSystemHelpers.Combine(directory, "raft.terms");
        IsFresh = !_fs.FileExists(_statePath);
        Load();
    }

    /// <summary>True when no state had been written before this instance was created.</summary>
    public bool IsFresh { get; }

    public long CurrentTerm => Interlocked.Read(ref _currentTerm);

    public string? VotedFor
    {
        get
        {
            lock (_gate) return _votedFor;
        }
    }

    public ClusterMode Mode => (ClusterMode)Volatile.Read(ref _mode);

    public RaftLogTerms Terms
    {
        get
        {
            lock (_gate) return _terms;
        }
    }

    public void SetMode(ClusterMode mode)
    {
        lock (_gate)
        {
            Persist(_currentTerm, _votedFor, (int)mode);
            Volatile.Write(ref _mode, (int)mode);
        }
    }

    /// <summary>
    /// Atomically updates currentTerm and votedFor. A higher term replaces both; in the same term a vote is only
    /// recorded when none was cast yet.
    /// </summary>
    public void UpdateTermAndVote(long newTerm, string? newVotedFor)
    {
        lock (_gate)
        {
            long term = _currentTerm;
            string? votedFor = _votedFor;
            if (newTerm > term)
            {
                term = newTerm;
                votedFor = newVotedFor;
            }
            else if (newTerm == term && newVotedFor != null && votedFor == null)
            {
                votedFor = newVotedFor;
            }
            else
            {
                return;
            }
            // Durable first: if the write fails nothing changes in memory either, so a term or vote this node acts on
            // (answers an RPC with) is always one it will still hold after a restart. Updating memory first would let a
            // retried RequestVote be granted from memory without ever reaching the disk.
            Persist(term, votedFor, _mode);
            Interlocked.Exchange(ref _currentTerm, term);
            _votedFor = votedFor;
        }
    }

    /// <summary>Records the term of a log range starting at <paramref name="startSeq"/>; persists if the map changed.</summary>
    public void RecordTermRange(ulong startSeq, long term)
    {
        lock (_gate)
        {
            if (_terms.Record(startSeq, term)) FileSystemHelpers.AtomicWrite(_fs, _termsPath, _terms.Serialize());
        }
    }

    /// <summary>
    /// A follower attaching to a leader whose log matches ours up to <paramref name="lastLocalSeq"/>: the leader's term map
    /// is the truth for every entry after it. Our own ranges past that point describe entries we never received or lost
    /// (a deposed leader's discarded tail learned at an earlier attach, a range recorded just before a power loss) and
    /// are dropped first; otherwise <see cref="RaftLogTerms.Record"/> would ignore the leader's earlier-starting ranges
    /// and the entries about to arrive would carry the wrong term (wrong lastLogTerm in votes, spurious resyncs).
    /// Persists if the map changed.
    /// </summary>
    public void AdoptTermsAfter(ulong lastLocalSeq, RaftLogTerms leaderTerms)
    {
        ArgumentNullException.ThrowIfNull(leaderTerms);
        lock (_gate)
        {
            bool changed = _terms.TruncateAfter(lastLocalSeq);
            foreach ((ulong start, long term) in leaderTerms.GetRanges())
            {
                if (start > lastLocalSeq) changed |= _terms.Record(start, term);
            }
            if (changed) FileSystemHelpers.AtomicWrite(_fs, _termsPath, _terms.Serialize());
        }
    }

    /// <summary>
    /// Replaces the whole term map. Used after a snapshot restore, when the local log is byte-for-byte the leader's log,
    /// so the leader's term map is the correct description of it.
    /// </summary>
    public void ReplaceTerms(RaftLogTerms terms)
    {
        ArgumentNullException.ThrowIfNull(terms);
        lock (_gate)
        {
            _terms = terms;
            FileSystemHelpers.AtomicWrite(_fs, _termsPath, _terms.Serialize());
        }
    }

    private void Persist(long currentTerm, string? votedFor, int mode)
    {
        var sb = new StringBuilder();
        sb.Append(currentTerm.ToString(CultureInfo.InvariantCulture)).Append('\n');
        sb.Append(votedFor ?? string.Empty).Append('\n');
        sb.Append(mode.ToString(CultureInfo.InvariantCulture)).Append('\n');
        FileSystemHelpers.AtomicWrite(_fs, _statePath, sb.ToString());
    }

    private void Load()
    {
        try
        {
            string? state = FileSystemHelpers.TryReadAllText(_fs, _statePath);
            if (state != null)
            {
                string[] lines = state.Split('\n');
                if (lines.Length >= 1 && long.TryParse(lines[0], NumberStyles.Integer, CultureInfo.InvariantCulture, out long t)) _currentTerm = t;
                if (lines.Length >= 2) _votedFor = string.IsNullOrEmpty(lines[1]) ? null : lines[1];
                if (lines.Length >= 3 && int.TryParse(lines[2], NumberStyles.Integer, CultureInfo.InvariantCulture, out int m)) _mode = m;
            }
        }
        catch (RocksDbException)
        {
            // Unreadable file: start fresh; the node re-snapshots from the current leader.
            _currentTerm = 0;
            _votedFor = null;
            _mode = 0;
        }
        try
        {
            _terms = RaftLogTerms.Deserialize(FileSystemHelpers.TryReadAllText(_fs, _termsPath));
        }
        catch (RocksDbException)
        {
            _terms = new RaftLogTerms();
        }
    }
}
