namespace RocksDb.Managed.Cluster;

/// <summary>
/// The two phases of a cluster's life cycle (rocksdb-sharp <c>ClusterMode</c>).
/// <para>Bootstrap: one designated primary accepts writes while peers catch up through the snapshot + WAL replication
/// path. No election is performed.</para>
/// <para>Cluster: full Raft. The leader is elected by quorum and any member may be promoted after a leader failure.
/// The transition is one-way.</para>
/// </summary>
public enum ClusterMode
{
    Bootstrap = 0,
    Cluster = 1,
}

public enum RaftRole
{
    Follower = 0,
    Candidate = 1,
    Leader = 2,
}

/// <summary>A cluster peer: its id (unique) and an endpoint the transport understands. Equality is by <see cref="NodeId"/>.</summary>
public sealed class ClusterMember : IEquatable<ClusterMember>
{
    public ClusterMember(string nodeId, string endpoint)
    {
        if (string.IsNullOrWhiteSpace(nodeId)) throw new ArgumentException("NodeId is required", nameof(nodeId));
        if (string.IsNullOrWhiteSpace(endpoint)) throw new ArgumentException("Endpoint is required", nameof(endpoint));
        NodeId = nodeId;
        Endpoint = endpoint;
    }

    public string NodeId { get; }
    public string Endpoint { get; }

    public bool Equals(ClusterMember? other) => other != null && other.NodeId == NodeId;
    public override bool Equals(object? obj) => obj is ClusterMember m && Equals(m);
    public override int GetHashCode() => NodeId.GetHashCode(StringComparison.Ordinal);
    public override string ToString() => NodeId + "@" + Endpoint;
}

public sealed class LeaderChangedEventArgs(string? leaderId, long term) : EventArgs
{
    public string? LeaderId { get; } = leaderId;
    public long Term { get; } = term;
}

public sealed class ModeChangedEventArgs(ClusterMode mode) : EventArgs
{
    public ClusterMode Mode { get; } = mode;
}

public sealed class RoleChangedEventArgs(RaftRole role, long term) : EventArgs
{
    public RaftRole Role { get; } = role;
    public long Term { get; } = term;
}

/// <summary>
/// Per-node configuration (rocksdb-sharp <c>RaftConfig</c>). The required values are constructor parameters; timings
/// and bootstrap tuning are init properties (instead of defaulted constructor parameters) and are validated by
/// <see cref="Validate"/>, which <see cref="RaftClusterNode"/> calls.
/// </summary>
public sealed class RaftConfig
{
    public RaftConfig(string nodeId, string endpoint, IEnumerable<ClusterMember> peers)
    {
        if (string.IsNullOrWhiteSpace(nodeId)) throw new ArgumentException("NodeId required", nameof(nodeId));
        if (string.IsNullOrWhiteSpace(endpoint)) throw new ArgumentException("Endpoint required", nameof(endpoint));
        ArgumentNullException.ThrowIfNull(peers);
        NodeId = nodeId;
        Endpoint = endpoint;
        Peers = [.. peers];
    }

    public string NodeId { get; }
    public string Endpoint { get; }
    public IReadOnlyList<ClusterMember> Peers { get; }

    /// <summary>Exactly one node of a new cluster sets this: it leads without an election during Bootstrap.</summary>
    public bool BootstrapPrimary { get; init; }

    public int ElectionTimeoutMinMs { get; init; } = 1500;
    public int ElectionTimeoutMaxMs { get; init; } = 3000;
    /// <summary>Must be well below <see cref="ElectionTimeoutMinMs"/> (at most a third is a good rule).</summary>
    public int HeartbeatIntervalMs { get; init; } = 300;

    /// <summary>
    /// In Bootstrap mode, how close to the primary (in WAL sequence numbers) a follower must be to count as in-sync
    /// towards the quorum that switches the cluster to Cluster mode.
    /// </summary>
    public ulong BootstrapInSyncLagThreshold { get; init; } = 1000;

    /// <summary>
    /// Voting members (including the bootstrap primary) that must be in-sync before switching to Cluster mode;
    /// 0 or less means a strict majority of peers + self.
    /// </summary>
    public int BootstrapPromotionQuorum
    {
        get => field > 0 ? field : Quorum;
        init;
    }

    /// <summary>Clock for election timeouts and heartbeats (tests can use a fake one).</summary>
    public TimeProvider TimeProvider { get; init; } = TimeProvider.System;

    /// <summary>Quorum threshold (majority) including self.</summary>
    public int Quorum => ((Peers.Count + 1) / 2) + 1;

    public void Validate()
    {
        if (ElectionTimeoutMinMs <= 0) throw new ArgumentOutOfRangeException(nameof(ElectionTimeoutMinMs));
        if (ElectionTimeoutMaxMs < ElectionTimeoutMinMs) throw new ArgumentOutOfRangeException(nameof(ElectionTimeoutMaxMs));
        if (HeartbeatIntervalMs <= 0 || HeartbeatIntervalMs >= ElectionTimeoutMinMs)
            throw new ArgumentOutOfRangeException(nameof(HeartbeatIntervalMs), "Heartbeat must be smaller than ElectionTimeoutMin");
        ArgumentNullException.ThrowIfNull(TimeProvider);
        foreach (ClusterMember p in Peers)
        {
            if (p.NodeId == NodeId) throw new ArgumentException("the peer list must not contain this node", nameof(Peers));
        }
    }
}
