using System.Collections.Concurrent;
using RocksDb.Managed.Cluster;
using RocksDb.Managed.Replication.Hosting;

namespace RocksDb.Managed.Replication.Tests;

/// <summary>
/// Network partitions of the 3-node Raft cluster over the in-memory transport (links cut with
/// <see cref="InMemoryNetwork.SetPartitioned"/>, which also breaks open WAL streams): the leader cut off from the
/// majority (a new leader is elected by the majority, the stale leader cannot acknowledge writes, healing converges and
/// discards the stale leader's unacknowledged entries), and a follower cut off and healed (its term inflation forces a
/// re-election). A client keeps writing through whichever node claims leadership throughout; no acknowledged write may
/// be lost (Raft's Leader Completeness, rocksdb-sharp Cluster/README.md "Failure modes"). Election Safety is checked on
/// every status poll by <see cref="ClusterHarness.Statuses"/>.
/// </summary>
[TestClass]
public sealed class ClusterPartitionTests
{
    private static readonly string[] s_nodes = ["n1", "n2", "n3"];

    private static async Task<ClusterHarness> StartClusterAsync(int bootstrapWrites)
    {
        var c = new ClusterHarness(3, null);
        ClusterNodeHost n1 = await c.StartAsync("n1");
        for (int i = 0; i < bootstrapWrites; i++) Assert.IsTrue(await n1.TryWriteAsLeaderAsync(U(K(i)), U(V(i, 0)), CancellationToken.None));
        await c.StartAsync("n2");
        await c.StartAsync("n3");
        await WaitUntilAsync(() => c.Statuses().TrueForAll(s => s.Mode == ClusterMode.Cluster), "every node in Cluster mode\n" + c.LogText);
        return c;
    }

    /// <summary>
    /// A client writing unique keys through whichever node claims to lead (possibly a stale leader), recording the
    /// writes that were acknowledged (committed).
    /// </summary>
    private sealed class Client(ClusterHarness cluster)
    {
        public readonly ConcurrentDictionary<string, string> Acked = new(StringComparer.Ordinal);
        public int Attempts;

        public async Task RunAsync(CancellationToken ct)
        {
            for (int i = 0; !ct.IsCancellationRequested; i++)
            {
                ClusterNodeHost? leader = null;
                long best = -1;
                foreach (ClusterNodeHost h in cluster.Running)
                {
                    ClusterNodeStatus s = h.GetStatus();
                    if (s.Role == RaftRole.Leader && s.Term > best && !s.Resyncing)
                    {
                        leader = h;
                        best = s.Term;
                    }
                }
                if (leader is null)
                {
                    await Task.Delay(10, CancellationToken.None);
                    continue;
                }
                string key = "client-" + i.ToString("D6", System.Globalization.CultureInfo.InvariantCulture), value = "c" + i;
                Interlocked.Increment(ref Attempts);
                try
                {
                    if (await leader.WriteCommittedAsync(U(key), U(value), TimeSpan.FromMilliseconds(300), CancellationToken.None)) Acked[key] = value;
                }
                catch (Exception e) when (e is ObjectDisposedException or RocksDbException or InvalidOperationException)
                {
                    // the node is rebuilding its database (resync): not acknowledged
                }
            }
        }
    }

    private static void AssertAckedEverywhere(ClusterHarness c, IReadOnlyDictionary<string, string> acked, string context)
    {
        foreach (ClusterNodeHost h in c.Running)
        {
            foreach (KeyValuePair<string, string> kv in acked)
            {
                byte[]? v = h.Db.Get(U(kv.Key));
                Assert.IsNotNull(v, context + ": acknowledged write " + kv.Key + " lost on " + h.NodeId);
                Assert.AreEqual(kv.Value, System.Text.Encoding.UTF8.GetString(v), context + ": " + kv.Key + " on " + h.NodeId);
            }
        }
    }

    [TestMethod]
    public async Task LeaderPartitionedFromTheMajorityIsReplacedAndRejoinsAfterHealing()
    {
        await using ClusterHarness c = await StartClusterAsync(200);
        ClusterNodeHost oldLeader = await c.WaitForLeaderAsync(null);
        long oldTerm = oldLeader.Node.CurrentTerm;
        string[] majority = Array.FindAll(s_nodes, id => id != oldLeader.NodeId);

        var client = new Client(c);
        using var stop = new CancellationTokenSource();
        Task clientTask = Task.Run(() => client.RunAsync(stop.Token));
        await WaitUntilAsync(() => client.Acked.Count >= 20, "acknowledged writes before the partition\n" + c.LogText);

        c.Network.Isolate(oldLeader.NodeId, s_nodes);
        ulong oldLeaderCommit = oldLeader.Node.CommitSeq;

        // The majority elects a new leader in a later term.
        ClusterNodeHost newLeader = await c.WaitForLeaderAmongAsync(majority, oldTerm + 1);
        Assert.AreNotEqual(oldLeader.NodeId, newLeader.NodeId);

        // The stale leader (if it still believes it leads) cannot commit: no acknowledgement on the minority side.
        for (int i = 0; i < 5; i++)
        {
            bool acked = await oldLeader.WriteCommittedAsync(U("minority-" + i), U("lost"), TimeSpan.FromMilliseconds(300), CancellationToken.None);
            Assert.IsFalse(acked, "a write on the partitioned minority was acknowledged");
        }
        Assert.AreEqual(oldLeaderCommit, oldLeader.Node.CommitSeq, "the minority's commit index advanced");

        // The majority keeps acknowledging writes.
        int ackedBefore = client.Acked.Count;
        await WaitUntilAsync(() => client.Acked.Count >= ackedBefore + 20, "acknowledged writes on the majority side\n" + c.LogText);

        c.Network.Heal();
        await WaitUntilAsync(() =>
        {
            ClusterNodeStatus s = oldLeader.GetStatus();
            return s.Role == RaftRole.Follower && s.Term > oldTerm && !s.Resyncing && s.LeaderId != null && s.LeaderId != oldLeader.NodeId;
        }, "the old leader to step down and follow\n" + c.LogText);

        int ackedAtHeal = client.Acked.Count;
        await WaitUntilAsync(() => client.Acked.Count >= ackedAtHeal + 20, "acknowledged writes after healing\n" + c.LogText);
        await stop.CancelAsync();
        await clientTask;

        ClusterNodeHost leader = await c.WaitForLeaderAsync(null);
        await c.AssertConvergedAsync(leader);
        AssertAckedEverywhere(c, client.Acked, "after healing");
        foreach (ClusterNodeHost h in c.Running)
        {
            for (int i = 0; i < 5; i++) Assert.IsNull(h.Db.Get(U("minority-" + i)), "the stale leader's unacknowledged entry survived on " + h.NodeId);
        }
        Assert.IsGreaterThanOrEqualTo(1, oldLeader.Resyncs, "the old leader's diverged log is rebuilt from a snapshot");
    }

    [TestMethod]
    public async Task FollowerPartitionedAndHealedDoesNotLoseAcknowledgedWrites()
    {
        await using ClusterHarness c = await StartClusterAsync(100);
        ClusterNodeHost leader = await c.WaitForLeaderAsync(null);
        string isolated = Array.Find(s_nodes, id => id != leader.NodeId)!;
        string[] majority = Array.FindAll(s_nodes, id => id != isolated);

        var client = new Client(c);
        using var stop = new CancellationTokenSource();
        Task clientTask = Task.Run(() => client.RunAsync(stop.Token));
        await WaitUntilAsync(() => client.Acked.Count >= 10, "acknowledged writes before the partition\n" + c.LogText);

        c.Network.Isolate(isolated, s_nodes);
        // Two of three still form a quorum: writes keep committing, and the cut-off follower's elections cannot succeed.
        int ackedBefore = client.Acked.Count;
        await WaitUntilAsync(() => client.Acked.Count >= ackedBefore + 30, "acknowledged writes with one follower cut off\n" + c.LogText);
        await WaitUntilAsync(() => c[isolated].Node.CurrentTerm > leader.Node.CurrentTerm || c[isolated].Node.Role == RaftRole.Candidate,
            "the cut-off follower to time out\n" + c.LogText);
        Assert.AreNotEqual(RaftRole.Leader, c[isolated].Node.Role, "a single node cannot win an election");
        await c.WaitForLeaderAmongAsync(majority, 0);

        c.Network.Heal();
        int ackedAtHeal = client.Acked.Count;
        await WaitUntilAsync(() => client.Acked.Count >= ackedAtHeal + 30, "acknowledged writes after healing\n" + c.LogText);
        await stop.CancelAsync();
        await clientTask;

        ClusterNodeHost finalLeader = await c.WaitForLeaderAsync(null);
        Assert.AreNotEqual(isolated, finalLeader.NodeId, "the lagging node cannot win (Raft election restriction)");
        await c.AssertConvergedAsync(finalLeader);
        AssertAckedEverywhere(c, client.Acked, "after healing");
    }

    /// <summary>
    /// A follower's progress report belongs to the term it follows. Here the follower grants its vote in a later term
    /// (to a candidate whose log ends where the follower's did at the time of the vote) while a chunk from the old leader
    /// is in the middle of being applied; when that write completes the follower must not report it to the old leader,
    /// which would count it towards a quorum the new term's candidate never had: the write would be acknowledged and
    /// then lost when that candidate wins. The old leader's heartbeats to the follower are cut, so it does not learn of
    /// the new term from the follower.
    /// </summary>
    [TestMethod]
    public async Task FollowerThatVotedInALaterTermDoesNotAcknowledgeTheOldLeadersEntries()
    {
        var gates = new Dictionary<string, GatedFileSystem>(StringComparer.Ordinal);
        var c = new ClusterHarness(3, null)
        {
            WrapFileSystem = (id, fs) => gates[id] = new GatedFileSystem(fs),
        };
        await using (c)
        {
            ClusterNodeHost n1 = await c.StartAsync("n1");
            for (int i = 0; i < 50; i++) Assert.IsTrue(await n1.TryWriteAsLeaderAsync(U(K(i)), U(V(i, 0)), CancellationToken.None));
            await c.StartAsync("n2");
            await c.StartAsync("n3");
            await WaitUntilAsync(() => c.Statuses().TrueForAll(s => s.Mode == ClusterMode.Cluster), "cluster mode\n" + c.LogText);
            ClusterNodeHost leader = await c.WaitForLeaderAsync(null);
            await c.AssertConvergedAsync(leader);
            string[] others = Array.FindAll(s_nodes, id => id != leader.NodeId);
            string follower = others[0], candidate = others[1];
            long term = leader.Node.CurrentTerm;

            // The candidate does not get the next entry; the leader cannot reach the follower (its stream and progress
            // reports to the leader still flow).
            c.Network.SetPartitioned(leader.NodeId, candidate, true);
            c.Network.CutLink(leader.NodeId, follower);
            ulong followerLatest = c[follower].Db.GetLatestSequenceNumber();

            gates[follower].Arm();
            Task<bool> write = leader.WriteCommittedAsync(U("contested"), U("x"), TimeSpan.FromSeconds(3), CancellationToken.None).AsTask();
            await gates[follower].Blocked.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.AreEqual(followerLatest, c[follower].Db.GetLatestSequenceNumber(), "the chunk is not applied yet");
            RequestVoteResult vote = c[follower].HandleRequestVote(new RequestVotePayload(term + 1, candidate, followerLatest, leader.Node.GetTermForSeq(followerLatest)));
            Assert.IsTrue(vote.VoteGranted, "the follower votes for the candidate in term " + (term + 1));
            gates[follower].Release();

            Assert.IsFalse(await write, "the old leader acknowledged an entry only the leader and a follower that had voted for a candidate without it held\n" + c.LogText);
            c.Network.Heal();
        }
    }
}
