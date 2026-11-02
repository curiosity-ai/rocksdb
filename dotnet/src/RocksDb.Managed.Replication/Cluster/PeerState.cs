namespace RocksDb.Managed.Cluster;

/// <summary>
/// Per-peer tracking maintained by the leader (rocksdb-sharp <c>PeerState</c>). nextIndex / matchIndex of Raft §5.3,
/// named with a "Seq" suffix because the log is the RocksDB WAL keyed by sequence number. Thread-safe.
/// </summary>
public sealed class PeerState
{
    private long _matchSeq;
    private long _nextSeq;
    private long _lastSuccessfulContactTicks;
    private int _reachable; // 1/0

    public PeerState(ClusterMember member, ulong initialNextSeq)
    {
        ArgumentNullException.ThrowIfNull(member);
        Member = member;
        _nextSeq = (long)initialNextSeq;
    }

    public ClusterMember Member { get; }
    public ulong MatchSeq => (ulong)Interlocked.Read(ref _matchSeq);
    public ulong NextSeq => (ulong)Interlocked.Read(ref _nextSeq);
    public bool IsReachable => Volatile.Read(ref _reachable) == 1;
    public DateTime LastSuccessfulContactUtc => new(Interlocked.Read(ref _lastSuccessfulContactTicks), DateTimeKind.Utc);

    public void OnAppendSucceeded(ulong matchSeq)
    {
        // Progress reports race with heartbeat acknowledgements; whichever confirmed less must never roll an
        // already-confirmed prefix back.
        InterlockedMax(ref _matchSeq, (long)matchSeq);
        InterlockedMax(ref _nextSeq, (long)matchSeq + 1);
        Interlocked.Exchange(ref _lastSuccessfulContactTicks, DateTime.UtcNow.Ticks);
        Volatile.Write(ref _reachable, 1);
    }

    public void OnAppendFailed() => Volatile.Write(ref _reachable, 0);

    public void OnHeartbeatAcknowledged()
    {
        Interlocked.Exchange(ref _lastSuccessfulContactTicks, DateTime.UtcNow.Ticks);
        Volatile.Write(ref _reachable, 1);
    }

    public void RewindOnConflict(ulong newNextSeq) => Interlocked.Exchange(ref _nextSeq, (long)newNextSeq);

    /// <summary>
    /// Called when this node wins an election: matchSeq goes back to 0 (nothing is known about the peer's log in the new
    /// term yet) and nextSeq is positioned at the end of our own log (§5.3).
    /// </summary>
    public void ResetForNewTerm(ulong nextSeq)
    {
        Interlocked.Exchange(ref _matchSeq, 0);
        Interlocked.Exchange(ref _nextSeq, (long)nextSeq);
    }

    private static void InterlockedMax(ref long location, long value)
    {
        long current = Interlocked.Read(ref location);
        while (value > current)
        {
            long prev = Interlocked.CompareExchange(ref location, value, current);
            if (prev == current) break;
            current = prev;
        }
    }
}
