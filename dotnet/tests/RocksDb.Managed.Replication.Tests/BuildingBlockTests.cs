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

    [TestMethod]
    public void WalBatchRejectsShortInput()
    {
        var e = Assert.ThrowsExactly<RocksDbException>(() => new WalBatch(new byte[11]));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
    }
}
