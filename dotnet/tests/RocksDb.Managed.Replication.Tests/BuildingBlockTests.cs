using RocksDb.Managed.Cluster;
using RocksDb.Managed.Replication.Switchover;

namespace RocksDb.Managed.Replication.Tests;

/// <summary>Unit tests of the building blocks: term map, persistent state, write gate, commit delay, delta plan.</summary>
[TestClass]
public sealed class BuildingBlockTests
{
    [TestMethod]
    public void RaftLogTermsLookupAndSerialization()
    {
        var t = new RaftLogTerms();
        Assert.AreEqual(0, t.GetTermForSeq(5));
        Assert.IsTrue(t.Record(1, 1));
        Assert.IsTrue(t.Record(100, 2));
        Assert.IsFalse(t.Record(150, 2), "same term again");
        Assert.IsFalse(t.Record(50, 3), "out of order");
        Assert.IsTrue(t.Record(200, 3));
        Assert.IsTrue(t.Record(200, 4), "an empty term is superseded");
        Assert.AreEqual(1, t.GetTermForSeq(99));
        Assert.AreEqual(2, t.GetTermForSeq(100));
        Assert.AreEqual(2, t.GetTermForSeq(199));
        Assert.AreEqual(4, t.GetTermForSeq(ulong.MaxValue));
        RaftLogTerms back = RaftLogTerms.Deserialize(t.Serialize());
        Assert.AreEqual(t.Serialize(), back.Serialize());
        Assert.AreEqual((200UL, 4L), back.GetLast());
    }

    /// <summary>
    /// A follower at seq 90 that learned term 3 from seq 100 (a leader whose term-3 entries were never committed) then
    /// attaches to the term-4 leader, whose term starts at 96. The stale range must not win: entries 96.. are term 4.
    /// Before AdoptTermsAfter the follower kept (100, 3) and ignored (96, 4), so 96..99 read as term 1 and 100.. as 3.
    /// </summary>
    [TestMethod]
    public void AttachingFollowerAdoptsTheLeadersTermsPastItsLog()
    {
        var fs = new MemoryFileSystem();
        var s = new RaftPersistentState(fs, "/f/raft");
        s.RecordTermRange(1, 1);
        s.RecordTermRange(100, 3);
        var leader = new RaftLogTerms();
        leader.Record(1, 1);
        leader.Record(96, 4);
        s.AdoptTermsAfter(90, leader);
        Assert.AreEqual(1, s.Terms.GetTermForSeq(90));
        Assert.AreEqual(4, s.Terms.GetTermForSeq(96));
        Assert.AreEqual(4, s.Terms.GetTermForSeq(120));
        Assert.AreEqual(leader.Serialize(), s.Terms.Serialize());
        Assert.AreEqual(leader.Serialize(), new RaftPersistentState(fs, "/f/raft").Terms.Serialize(), "persisted");

        // Ranges at or before our log's end are ours (the attach check verified the term at it) and stay.
        var t = new RaftLogTerms();
        t.Record(1, 1);
        t.Record(50, 2);
        Assert.IsFalse(t.TruncateAfter(50));
        Assert.IsTrue(t.TruncateAfter(49));
        Assert.AreEqual((1UL, 1L), t.GetLast());
    }

    [TestMethod]
    public void RaftPersistentStateSurvivesReopenOnMemoryFileSystem()
    {
        var fs = new MemoryFileSystem();
        var s = new RaftPersistentState(fs, "/n/raft");
        Assert.IsTrue(s.IsFresh);
        s.UpdateTermAndVote(3, "n2");
        s.UpdateTermAndVote(3, "n3"); // already voted in term 3
        s.SetMode(ClusterMode.Cluster);
        s.RecordTermRange(10, 3);
        var r = new RaftPersistentState(fs, "/n/raft");
        Assert.IsFalse(r.IsFresh);
        Assert.AreEqual(3, r.CurrentTerm);
        Assert.AreEqual("n2", r.VotedFor);
        Assert.AreEqual(ClusterMode.Cluster, r.Mode);
        Assert.AreEqual(3, r.Terms.GetTermForSeq(11));
        r.UpdateTermAndVote(4, null);
        Assert.IsNull(new RaftPersistentState(fs, "/n/raft").VotedFor, "a new term clears the vote");
    }

    [TestMethod]
    public void ReplicationRoleStatePromisesAndEpochs()
    {
        var fs = new MemoryFileSystem();
        var s = new ReplicationRoleState(fs, "/n/role");
        s.StartEpoch(1, 1);
        s.Set(1, ReplicationRole.Primary, "A");
        Assert.IsTrue(s.Promise(2));
        Assert.IsFalse(s.Promise(2));
        Assert.ThrowsExactly<InvalidOperationException>(() => s.Set(0, ReplicationRole.Replica, "B"));
        s.MergeEpochs("1,1\n50,2\n");
        s.StartEpoch(40, 3); // drops the learned range at 50
        var r = new ReplicationRoleState(fs, "/n/role");
        Assert.AreEqual(2, r.PromisedEpoch);
        Assert.AreEqual(ReplicationRole.Primary, r.Role);
        Assert.AreEqual(3, r.GetEpochForSeq(60));
        Assert.AreEqual(1, r.GetEpochForSeq(39));
    }

    [TestMethod]
    public async Task WriteGateHoldDrainsInFlightWritesAndQueuesNewOnes()
    {
        var gate = new PrimaryWriteGate(open: true);
        Assert.IsTrue(await gate.TryEnterAsync(TimeSpan.Zero, CancellationToken.None));
        Task hold = gate.HoldAsync(CancellationToken.None);
        Assert.IsFalse(hold.IsCompleted, "one write in flight");
        Task<bool> waiting = gate.TryEnterAsync(TimeSpan.FromSeconds(10), CancellationToken.None).AsTask();
        Assert.IsFalse(waiting.IsCompleted, "new writes wait while holding");
        gate.Exit();
        await hold.WaitAsync(TimeSpan.FromSeconds(5));
        gate.OpenGate(); // aborted switchover: the waiting write proceeds
        Assert.IsTrue(await waiting.WaitAsync(TimeSpan.FromSeconds(5)));
        gate.Exit();

        Task hold2 = gate.HoldAsync(CancellationToken.None);
        await hold2.WaitAsync(TimeSpan.FromSeconds(5));
        Task<bool> refused = gate.TryEnterAsync(TimeSpan.FromSeconds(10), CancellationToken.None).AsTask();
        gate.Close(); // completed switchover: held writes are refused
        Assert.IsFalse(await refused.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.IsFalse(await gate.TryEnterAsync(TimeSpan.FromSeconds(1), CancellationToken.None));
        Assert.AreEqual(0, gate.InFlight);
    }

    [TestMethod]
    public void AdaptiveCommitDelayGrowsWithLagAndClamps()
    {
        var c = new AdaptiveCommitDelayController(2, new AdaptiveCommitDelayOptions { DelayPerLagUnitMs = 5, LagUnit = 1000, MaxDelayMs = 100 });
        Assert.AreEqual(0, c.GetRecommendedDelayMs());
        c.ReportLag(0, 1000);
        int one = c.GetRecommendedDelayMs();
        Assert.AreEqual(5, one);
        c.ReportLag(1, 5000); // cluster lag is the max over replicas
        Assert.IsTrue(c.GetRecommendedDelayMs() > one);
        for (int i = 0; i < 50; i++) c.ReportLag(1, 1_000_000);
        Assert.AreEqual(100, c.GetRecommendedDelayMs());
        CommitDelaySnapshot s = c.GetSnapshot();
        Assert.AreEqual(1_000_000, s.CurrentLag);
        Assert.AreEqual(20, s.History.Count);
        CollectionAssert.AreEqual(new long[] { 1000, 1_000_000 }, s.ReplicaLags.ToArray());
        Assert.ThrowsExactly<ArgumentOutOfRangeException>(() => c.ReportLag(2, 1));
    }

    [TestMethod]
    public void DeltaPlanReusesOnlyMatchingImmutableFiles()
    {
        ReplicationFileInfo F(string n, long s, string h = "") => new() { FileName = n, Size = s, Hash = h };
        ReplicationDeltaPlan p = ReplicationDelta.Compute(
            [F("000010.sst", 100, "AA"), F("000011.sst", 200), F("000012.sst", 300, "BB"), F("MANIFEST-000005", 50), F("CURRENT", 16)],
            [F("000010.sst", 100, "aa"), F("000011.sst", 201), F("000012.sst", 300, "CC"), F("MANIFEST-000005", 50), F("CURRENT", 16)]);
        CollectionAssert.AreEqual(new[] { "000010.sst" }, p.FilesToReuse);
        CollectionAssert.AreEquivalent(new[] { "000011.sst", "000012.sst", "MANIFEST-000005", "CURRENT" }, p.FilesToTransfer);
        Assert.IsTrue(ReplicationDelta.IsImmutableFile("x.BLOB"));
        Assert.AreEqual("CURRENT", ReplicationSession.OrderForTransfer(["CURRENT", "b.sst", "a.sst"])[2]);
    }

    private sealed class NullPeerTransport : IPeerTransport
    {
        public Task<RequestVoteResult?> SendRequestVoteAsync(ClusterMember peer, RequestVotePayload request, CancellationToken cancellationToken) =>
            Task.FromResult<RequestVoteResult?>(null);

        public Task<AppendEntriesResult?> SendHeartbeatAsync(ClusterMember peer, HeartbeatPayload payload, CancellationToken cancellationToken) =>
            Task.FromResult<AppendEntriesResult?>(null);
    }

    /// <summary>
    /// A deposed leader keeps an uncommitted tail from its own term. The new leader's heartbeat only proves that the log
    /// matches up to PrevLogSeq, so the follower's commit index must stop there (Raft: min(leaderCommit, index of last
    /// new entry)) even though leaderCommit and the follower's latest sequence are both higher.
    /// </summary>
    [TestMethod]
    public void FollowerCommitIndexStopsAtTheLastEntryKnownToMatchTheLeader()
    {
        using var dir = new TempDir("raft-commit");
        using RocksDatabase db = RocksDatabase.Open(dir.Sub("db"), new DbOptions { CreateIfMissing = true });
        for (int i = 1; i <= 10; i++) db.Put(U(K(i)), U("v"));
        Assert.AreEqual(10UL, db.GetLatestSequenceNumber());
        var state = new RaftPersistentState(dir.Sub("raft"));
        state.SetMode(ClusterMode.Cluster);
        state.UpdateTermAndVote(2, "n1");
        state.RecordTermRange(1, 1);
        state.RecordTermRange(6, 2); // entries 6..10: this node's own (divergent, uncommitted) term-2 tail
        var cfg = new RaftConfig("n1", "e1", [new ClusterMember("n2", "e2"), new ClusterMember("n3", "e3")]);
        using var node = new RaftClusterNode(cfg, state, db, new NullPeerTransport());

        // New leader n2 (term 3) has entries 1..5 of term 1 followed by its own term-3 entries 6..7, committed up to 7.
        AppendEntriesResult r = node.HandleAppendEntries(new HeartbeatPayload(3, "n2", 5, 1, 7, ClusterMode.Cluster));
        Assert.IsTrue(r.Success);
        Assert.AreEqual(5UL, node.CommitSeq, "the divergent tail past PrevLogSeq was marked committed");

        // Probing further back never lowers the commit index.
        r = node.HandleAppendEntries(new HeartbeatPayload(3, "n2", 3, 1, 7, ClusterMode.Cluster));
        Assert.IsTrue(r.Success);
        Assert.AreEqual(5UL, node.CommitSeq);

        // A heartbeat whose PrevLogSeq is inside the divergent tail fails the term check and asks for a resync.
        bool resync = false;
        node.ResyncRequired += (_, _) => resync = true;
        r = node.HandleAppendEntries(new HeartbeatPayload(3, "n2", 7, 3, 7, ClusterMode.Cluster));
        Assert.IsFalse(r.Success);
        Assert.AreEqual(AppendEntriesRejectReason.TermMismatch, r.Reason);
        Assert.IsTrue(resync);
        Assert.AreEqual(5UL, node.CommitSeq);
    }

    [TestMethod]
    public void WalBatchRejectsShortInput()
    {
        var e = Assert.ThrowsExactly<RocksDbException>(() => new WalBatch(new byte[11]));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
    }
}
