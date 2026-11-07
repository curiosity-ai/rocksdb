using RocksDb.Managed.Cluster;
using RocksDb.Managed.Replication.Hosting;

namespace RocksDb.Managed.Replication.Tests;

/// <summary>
/// The scenarios of rocksdb-sharp's Tests/ClusterTest in process: bootstrap -> cluster mode, leader election and
/// re-election after the leader stops, quorum commit, a follower catching up from far behind through the snapshot path,
/// and term log persistence across restarts. Election Safety is checked on every status poll.
/// </summary>
[TestClass]
public sealed class ClusterTests
{
    private static async Task<ulong> WriteCommittedAsync(ClusterHarness c, int from, int to)
    {
        ulong last = 0;
        for (int i = from; i < to; i++)
        {
            while (true)
            {
                ClusterNodeHost leader = await c.WaitForLeaderAsync(null);
                if (await leader.WriteCommittedAsync(U(K(i)), U(V(i, 0)), TimeSpan.FromSeconds(5), CancellationToken.None))
                {
                    last = leader.Db.GetLatestSequenceNumber();
                    break;
                }
            }
        }
        return last;
    }

    private static async Task<ClusterHarness> StartClusterAsync(IFileSystem? fs, int bootstrapWrites)
    {
        var c = new ClusterHarness(3, fs);
        ClusterNodeHost n1 = await c.StartAsync("n1");
        Assert.AreEqual(RaftRole.Leader, n1.Node.Role, "the bootstrap primary leads without an election");
        Assert.AreEqual(ClusterMode.Bootstrap, n1.Node.Mode);
        for (int i = 0; i < bootstrapWrites; i++) Assert.IsTrue(await n1.TryWriteAsLeaderAsync(U(K(i)), U(V(i, 0)), CancellationToken.None));
        await c.StartAsync("n2");
        await c.StartAsync("n3");
        await WaitUntilAsync(() =>
        {
            foreach (ClusterNodeStatus s in c.Statuses())
            {
                if (s.Mode != ClusterMode.Cluster) return false;
            }
            return true;
        }, "every node in Cluster mode\n" + c.LogText);
        return c;
    }

    [TestMethod]
    public async Task BootstrapPromotesToClusterModeAndConverges()
    {
        await using ClusterHarness c = await StartClusterAsync(null, 500);
        ClusterNodeHost leader = await c.WaitForLeaderAsync(null);
        Assert.AreEqual("n1", leader.NodeId, "no election was needed");
        Assert.AreEqual(1, leader.Node.CurrentTerm);
        await WriteCommittedAsync(c, 500, 600);
        await c.AssertConvergedAsync(leader);
        foreach (ClusterNodeHost h in c.Running) Assert.AreEqual(0, h.Resyncs);
    }

    [TestMethod]
    public async Task ClusterOnMemoryFileSystem()
    {
        await using ClusterHarness c = await StartClusterAsync(new MemoryFileSystem(), 300);
        await WriteCommittedAsync(c, 300, 400);
        await c.AssertConvergedAsync(await c.WaitForLeaderAsync(null));
    }

    [TestMethod]
    public async Task LeaderStopCausesReelectionAndTheOldLeaderRejoins()
    {
        await using ClusterHarness c = await StartClusterAsync(null, 200);
        ulong committed = await WriteCommittedAsync(c, 200, 300);
        await c.StopAsync("n1");

        ClusterNodeHost newLeader = await c.WaitForLeaderAsync("n1");
        Assert.AreNotEqual("n1", newLeader.NodeId);
        Assert.IsTrue(newLeader.Node.CurrentTerm >= 2);
        // Leader Completeness: every committed write survived the failover.
        Assert.IsTrue(newLeader.Db.GetLatestSequenceNumber() >= committed);
        for (int i = 200; i < 300; i++) Assert.IsNotNull(newLeader.Db.Get(U(K(i))), "committed key " + i + " lost");
        // The new leader's no-op entry lets the commit index pass the inherited entries.
        await WaitUntilAsync(() => newLeader.Node.CommitSeq == newLeader.Db.GetLatestSequenceNumber(), "commit of the no-op");
        await WriteCommittedAsync(c, 300, 400);

        ClusterNodeHost n1 = await c.StartAsync("n1");
        await WaitUntilAsync(() => n1.Node.Role == RaftRole.Follower && n1.Node.CurrentLeaderId == newLeader.NodeId, "n1 to follow\n" + c.LogText);
        await WriteCommittedAsync(c, 400, 450);
        await c.AssertConvergedAsync(newLeader);
        Assert.AreEqual(0, n1.Resyncs, "n1's log was a prefix of the new leader's");
    }

    [TestMethod]
    public async Task WritesCommitOnlyWithAQuorum()
    {
        await using ClusterHarness c = await StartClusterAsync(null, 100);
        ClusterNodeHost leader = await c.WaitForLeaderAsync(null);
        string[] followers = ["n1", "n2", "n3"];
        followers = Array.FindAll(followers, id => id != leader.NodeId);

        await c.StopAsync(followers[0]);
        Assert.IsTrue(await leader.WriteCommittedAsync(U("q1"), U("with two of three"), TimeSpan.FromSeconds(5), CancellationToken.None));

        await c.StopAsync(followers[1]);
        Assert.IsFalse(await leader.WriteCommittedAsync(U("q2"), U("alone"), TimeSpan.FromMilliseconds(500), CancellationToken.None),
            "no quorum: not acknowledged");
        ulong pending = leader.Db.GetLatestSequenceNumber();
        Assert.IsTrue(leader.Node.CommitSeq < pending);

        // A follower returns: the pending write commits.
        await c.StartAsync(followers[1]);
        await WaitUntilAsync(() => leader.Node.CommitSeq >= pending, "the pending write to commit\n" + c.LogText);
        Assert.AreEqual(RaftRole.Leader, leader.Node.Role);
    }

    [TestMethod]
    public async Task FollowerFarBehindCatchesUpThroughASnapshot()
    {
        await using var c = new ClusterHarness(3, null) { ArchiveWals = false };
        await c.StartAsync("n1");
        for (int i = 0; i < 100; i++) await c["n1"].TryWriteAsLeaderAsync(U(K(i)), U(V(i, 0)), CancellationToken.None);
        await c.StartAsync("n2");
        await c.StartAsync("n3");
        ClusterNodeHost leader = await c.WaitForLeaderAsync(null);
        await WriteCommittedAsync(c, 100, 200);
        await c.AssertConvergedAsync(leader);

        // n3 is down while the leader writes and flushes: the WAL n3 needs is deleted.
        await c.StopAsync("n3");
        await WriteCommittedAsync(c, 200, 800);
        await leader.Db.FlushAsync();
        await WriteCommittedAsync(c, 800, 850);

        ClusterNodeHost n3 = await c.StartAsync("n3");
        await WaitUntilAsync(() => n3.Resyncs == 1 && !n3.IsResyncing, "n3 to resync from a snapshot\n" + c.LogText);
        await WriteCommittedAsync(c, 850, 900);
        await c.AssertConvergedAsync(leader);
        Assert.AreEqual(leader.Node.SerializeTerms(), n3.Node.SerializeTerms(), "term map taken from the leader");
    }

    [TestMethod]
    public async Task TermLogPersistsAcrossRestart()
    {
        await using ClusterHarness c = await StartClusterAsync(null, 50);
        await c.StopAsync("n1"); // force an election: a second term range
        ClusterNodeHost leader = await c.WaitForLeaderAsync("n1");
        await WriteCommittedAsync(c, 50, 120);
        await c.AssertConvergedAsync(leader);

        string follower = leader.NodeId == "n2" ? "n3" : "n2";
        string terms = c[follower].Node.SerializeTerms();
        long term = c[follower].Node.CurrentTerm;
        Assert.IsTrue(RaftLogTerms.Deserialize(terms).GetRanges().Count >= 2, terms);
        await c.StopAsync(follower);

        var persisted = new RaftPersistentState(c.FileSystem, c.DbPath(follower) + "/raft");
        Assert.IsFalse(persisted.IsFresh);
        Assert.AreEqual(term, persisted.CurrentTerm);
        Assert.AreEqual(ClusterMode.Cluster, persisted.Mode);
        Assert.AreEqual(terms, persisted.Terms.Serialize());

        ClusterNodeHost restarted = await c.StartAsync(follower);
        Assert.AreEqual(ClusterMode.Cluster, restarted.Node.Mode, "mode survives the restart");
        Assert.IsTrue(restarted.Node.CurrentTerm >= term);
        ulong latest = restarted.Db.GetLatestSequenceNumber();
        Assert.AreEqual(leader.Node.GetTermForSeq(latest), restarted.Node.GetTermForSeq(latest));
        await WriteCommittedAsync(c, 120, 150);
        await c.AssertConvergedAsync(leader);
        Assert.AreEqual(0, restarted.Resyncs);
    }

    /// <summary>
    /// Raft's durability contract: an acknowledged (committed) write survives the loss of every node's unsynced data.
    /// The leader counted itself and the followers counted towards the quorum on entries that were only in the page
    /// cache (no WAL fsync before acknowledging / reporting progress), so a power loss of the whole cluster (or of the
    /// leader plus one follower, which the cluster soak hit) lost acknowledged writes.
    /// </summary>
    [TestMethod]
    public async Task AcknowledgedWritesSurviveAPowerLossOfTheWholeCluster()
    {
        var fs = new MemoryFileSystem();
        await using ClusterHarness c = await StartClusterAsync(fs, 50);
        await WriteCommittedAsync(c, 50, 120);

        await c.PowerLossAllAsync(fs);
        foreach (string id in new[] { "n1", "n2", "n3" }) await c.StartAsync(id);
        ClusterNodeHost leader = await c.WaitForLeaderAsync(null);
        await c.AssertConvergedAsync(leader);
        foreach (ClusterNodeHost h in c.Running)
        {
            for (int i = 50; i < 120; i++) Assert.IsNotNull(h.Db.Get(U(K(i))), "acknowledged key " + i + " lost on " + h.NodeId + " after the power loss\n" + c.LogText);
        }
    }

    /// <summary>
    /// A node killed during a snapshot resync (after the restore deleted its old files, before CURRENT arrived) restarts
    /// without a database but with its Raft term and vote. It must rebuild from the current leader before it takes part
    /// again: with an empty log it would vote for any candidate, and the bootstrap primary used to open an empty
    /// database right away, so a candidate missing a committed write could win with its vote and the write was lost.
    /// (Non-bootstrap nodes pulled only from the bootstrap member, which may not lead, and gave up after 100 tries.)
    /// </summary>
    [TestMethod]
    public async Task NodeRestartedWithoutItsDatabaseRebuildsFromTheLeaderBeforeVoting()
    {
        await using var c = new ClusterHarness(3, null) { ArchiveWals = false };
        await c.StartAsync("n1");
        for (int i = 0; i < 50; i++) await c["n1"].TryWriteAsLeaderAsync(U(K(i)), U(V(i, 0)), CancellationToken.None);
        await c.StartAsync("n2");
        await c.StartAsync("n3");
        await WaitUntilAsync(() => c.Statuses().TrueForAll(s => s.Mode == ClusterMode.Cluster), "cluster mode\n" + c.LogText);
        // n1 (the bootstrap primary) becomes a follower.
        await c.StopAsync("n1");
        ClusterNodeHost leader = await c.WaitForLeaderAsync("n1");
        await c.StartAsync("n1");
        await WriteCommittedAsync(c, 50, 80);
        await c.AssertConvergedAsync(leader);
        string other = leader.NodeId == "n2" ? "n3" : "n2";

        // The other follower is down; an acknowledged write commits on the leader and n1.
        await c.StopAsync(other);
        Assert.IsTrue(await leader.WriteCommittedAsync(U("acked"), U("must survive"), TimeSpan.FromSeconds(5), CancellationToken.None));
        // n1 dies in the middle of a resync: its database files are gone, its Raft state is not.
        await c.StopAsync("n1");
        foreach (string f in Directory.GetFiles(c.DbPath("n1"))) File.Delete(f);
        // The only node with the write besides n1 goes down; the lagging node and n1 come back.
        string leaderId = leader.NodeId;
        await c.StopAsync(leaderId);
        await c.StartAsync(other);
        Task n1Start = c.StartAsync("n1");
        await Task.Delay(3000);
        foreach (ClusterNodeStatus s in c.Statuses())
            Assert.AreNotEqual(RaftRole.Leader, s.Role, s.NodeId + " was elected without the acknowledged write\n" + c.LogText);

        await c.StartAsync(leaderId);
        await n1Start;
        ClusterNodeHost final = await c.WaitForLeaderAsync(null);
        await WriteCommittedAsync(c, 80, 90);
        await c.AssertConvergedAsync(final);
        foreach (ClusterNodeHost h in c.Running) Assert.IsNotNull(h.Db.Get(U("acked")), "acknowledged write lost on " + h.NodeId + "\n" + c.LogText);
    }
}
