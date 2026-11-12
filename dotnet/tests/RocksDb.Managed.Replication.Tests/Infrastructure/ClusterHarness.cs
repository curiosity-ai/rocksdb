using System.Collections.Concurrent;
using RocksDb.Managed.Cluster;
using RocksDb.Managed.Replication.Hosting;

namespace RocksDb.Managed.Replication.Tests.Infrastructure;

/// <summary>
/// A Raft cluster of <see cref="ClusterNodeHost"/>s ("n1" is the bootstrap primary) on an <see cref="InMemoryNetwork"/>,
/// with short election timeouts - the in-process counterpart of rocksdb-sharp's ClusterTest coordinator. Every status
/// observed through <see cref="Statuses"/> is checked for Election Safety (at most one leader per term) and monotonic
/// terms.
/// </summary>
public sealed class ClusterHarness : IAsyncDisposable
{
    private readonly TempDir _dir = new("cluster");
    private readonly ConcurrentDictionary<string, ClusterNodeHost> _hosts = new();
    private readonly ConcurrentQueue<string> _log = new();
    private readonly ConcurrentDictionary<long, string> _leaderOfTerm = new();
    private readonly ConcurrentDictionary<string, long> _lastTerm = new();

    public ClusterHarness(int nodes, IFileSystem? fileSystem)
    {
        FileSystem = fileSystem ?? LocalFileSystem.Instance;
        Root = fileSystem is null ? _dir.Path : "/cluster";
        var members = new List<ClusterMember>();
        for (int i = 1; i <= nodes; i++) members.Add(new ClusterMember("n" + i, "mem://n" + i));
        Members = members;
    }

    public InMemoryNetwork Network { get; } = new();
    public IReadOnlyList<ClusterMember> Members { get; }
    public IFileSystem FileSystem { get; }
    public string Root { get; }
    public ulong BootstrapInSyncLagThreshold { get; set; } = 1000;
    /// <summary>Per-node file system wrapper (node id, shared file system) -> the node's file system.</summary>
    public Func<string, IFileSystem, IFileSystem>? WrapFileSystem { get; set; }
    /// <summary>When false, obsolete WALs are deleted at once (no archive): a follower that falls behind needs a snapshot.</summary>
    public bool ArchiveWals { get; set; } = true;

    /// <summary>Wraps the client a node uses for a peer (node id, peer id, client); null for none.</summary>
    public Func<string, string, IClusterNodeClient, IClusterNodeClient>? WrapClient { get; set; }

    public ClusterNodeHost this[string id] => _hosts[id];

    public IEnumerable<ClusterNodeHost> Running => _hosts.Values;

    public string LogText => string.Join("\n", _log);

    public string DbPath(string id) => Root + "/" + id;

    public async Task<ClusterNodeHost> StartAsync(string id)
    {
        IFileSystem fs = WrapFileSystem?.Invoke(id, FileSystem) ?? FileSystem;
        bool archive = ArchiveWals;
        var host = new ClusterNodeHost(new ClusterNodeHostOptions
        {
            NodeId = id,
            Endpoint = "mem://" + id,
            DbPath = DbPath(id),
            Members = Members,
            BootstrapPrimaryId = "n1",
            DbOptionsFactory = () => new DbOptions { FileSystem = fs, WalTtlSeconds = archive ? 600UL : 0UL },
            ElectionTimeoutMinMs = 300,
            ElectionTimeoutMaxMs = 600,
            HeartbeatIntervalMs = 50,
            BootstrapInSyncLagThreshold = BootstrapInSyncLagThreshold,
            MaxChunkBytes = 16 * 1024,
            SnapshotChunkBytes = 64 * 1024,
            RetryDelay = TimeSpan.FromMilliseconds(50),
            Log = _log.Enqueue,
        }, m =>
        {
            IClusterNodeClient client = new InMemoryClusterClient(Network, id, m.NodeId);
            return WrapClient?.Invoke(id, m.NodeId, client) ?? client;
        });
        // Registered before starting: a follower's first stream attach and the leader's first heartbeats must reach it.
        // Its handlers are only used once StartAsync has created the node (the in-memory client resolves lazily).
        await host.StartAsync(CancellationToken.None);
        _hosts[id] = host;
        Network.Register(id, host);
        return host;
    }

    public async Task StopAsync(string id)
    {
        Network.Unregister(id);
        if (_hosts.TryRemove(id, out ClusterNodeHost? h)) await h.DisposeAsync();
    }

    /// <summary>
    /// A power loss of every node at once: the file system stops accepting writes (nothing written while the hosts shut
    /// down reaches it), the hosts are disposed, and everything not synced is dropped.
    /// </summary>
    public async Task PowerLossAllAsync(MemoryFileSystem fs)
    {
        fs.SetFilesystemActive(false);
        foreach (string id in _hosts.Keys)
        {
            try
            {
                await StopAsync(id);
            }
            catch (RocksDbException)
            {
                // closing on an inactive file system fails; the crash below decides what survives
            }
        }
        fs.SimulateCrash();
        fs.SetFilesystemActive(true);
    }

    /// <summary>Statuses of the running nodes, checked for Election Safety and monotonic terms.</summary>
    public List<ClusterNodeStatus> Statuses()
    {
        var list = new List<ClusterNodeStatus>();
        foreach (ClusterNodeHost h in _hosts.Values)
        {
            ClusterNodeStatus s = h.GetStatus();
            list.Add(s);
            if (s.Role == RaftRole.Leader)
            {
                string leader = _leaderOfTerm.GetOrAdd(s.Term, s.NodeId);
                Assert.AreEqual(leader, s.NodeId, "two leaders in term " + s.Term);
            }
            long last = _lastTerm.GetOrAdd(s.NodeId, s.Term);
            Assert.IsTrue(s.Term >= last, s.NodeId + "'s term went backwards");
            _lastTerm[s.NodeId] = s.Term;
        }
        return list;
    }

    /// <summary>The single leader all running nodes agree on (same term), once there is one.</summary>
    public async Task<ClusterNodeHost> WaitForLeaderAsync(string? notThis)
    {
        ClusterNodeHost? found = null;
        await WaitUntilAsync(() =>
        {
            List<ClusterNodeStatus> s = Statuses();
            ClusterNodeStatus? leader = s.Find(x => x.Role == RaftRole.Leader && x.NodeId != notThis);
            if (leader == null) return false;
            foreach (ClusterNodeStatus x in s)
            {
                if (x.Term != leader.Term || (x.NodeId != leader.NodeId && x.LeaderId != leader.NodeId)) return false;
            }
            found = _hosts[leader.NodeId];
            return true;
        }, TimeSpan.FromSeconds(15), "an agreed leader\n" + LogText);
        return found!;
    }

    /// <summary>
    /// A leader that every node of <paramref name="nodes"/> agrees on (same term, it leads or they follow it) - the
    /// majority side of a partition, while the nodes outside the set may still believe in an older leader.
    /// </summary>
    public async Task<ClusterNodeHost> WaitForLeaderAmongAsync(IReadOnlyCollection<string> nodes, long minTerm)
    {
        ClusterNodeHost? found = null;
        await WaitUntilAsync(() =>
        {
            List<ClusterNodeStatus> all = Statuses();
            List<ClusterNodeStatus> s = all.FindAll(x => nodes.Contains(x.NodeId));
            ClusterNodeStatus? leader = s.Find(x => x.Role == RaftRole.Leader && x.Term >= minTerm);
            if (leader == null) return false;
            foreach (ClusterNodeStatus x in s)
            {
                if (x.Term != leader.Term || (x.NodeId != leader.NodeId && x.LeaderId != leader.NodeId)) return false;
            }
            found = _hosts[leader.NodeId];
            return true;
        }, TimeSpan.FromSeconds(15), "an agreed leader among " + string.Join(",", nodes) + "\n" + LogText);
        return found!;
    }

    /// <summary>Waits until every running node has the leader's tail and commit index, then compares digests.</summary>
    public async Task AssertConvergedAsync(ClusterNodeHost leader)
    {
        await WaitUntilAsync(() =>
        {
            ulong latest = leader.Db.GetLatestSequenceNumber();
            if (leader.Node.CommitSeq != latest) return false;
            foreach (ClusterNodeHost h in _hosts.Values)
            {
                if (h.IsResyncing || h.Db.GetLatestSequenceNumber() != latest || h.Node.CommitSeq != latest) return false;
            }
            return true;
        }, TimeSpan.FromSeconds(15), "convergence\n" + LogText);
        DatabaseDigest? expected = leader.Checksum();
        foreach (ClusterNodeHost h in _hosts.Values) Assert.AreEqual(expected, h.Checksum(), h.NodeId + " digest");
    }

    public async ValueTask DisposeAsync()
    {
        foreach (string id in _hosts.Keys) await StopAsync(id);
        _dir.Dispose();
    }
}
