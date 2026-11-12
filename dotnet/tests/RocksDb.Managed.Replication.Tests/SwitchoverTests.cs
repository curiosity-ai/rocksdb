using RocksDb.Managed.Replication.Hosting;
using RocksDb.Managed.Replication.Switchover;

namespace RocksDb.Managed.Replication.Tests;

/// <summary>
/// The scenarios of rocksdb-sharp's Tests/SwitchoverTest in process, under live client writes: a rolling upgrade
/// (A -> B handoff, restart A, B -> A handoff, restart B) with no snapshot, an aborted handoff, a lost promote reply, a
/// late promote, and a forced failover where the old primary's divergent tail is detected and rebuilt.
/// </summary>
[TestClass]
public sealed class SwitchoverTests
{
    private static async Task<SwitchoverResult> HandOffAsync(PairHarness h, string from) => await h[from].HandOffAsync(h.Switchover, CancellationToken.None);

    /// <summary>The deploy health check before the next handoff: the replica is close to the primary's tail.</summary>
    private static Task WaitCaughtUpAsync(PairHarness h, string primary, string replica) =>
        WaitUntilAsync(() => h[replica].Db.GetLatestSequenceNumber() + 1000 >= h[primary].Db.GetLatestSequenceNumber(), replica + " caught up");

    private static async Task WaitFollowingAsync(PairHarness h, string replica)
    {
        await WaitUntilAsync(() => h[replica].GetStatus().AttachedFromSeq > 0 && h[replica].Node.Role == ReplicationRole.Replica,
            replica + " following\n" + h.LogText);
    }

    [TestMethod]
    public async Task RollingUpgradeHandsOffBothWaysWithoutLosingWrites()
    {
        await using var h = new PairHarness();
        await h.StartAsync("A");
        await h.StartAsync("B");
        await WaitFollowingAsync(h, "B");
        (CancellationTokenSource stop, Task writers) = h.StartWriters(3);

        await WaitUntilAsync(() => h.Acknowledged.Count > 300, "some writes");
        SwitchoverResult r1 = await HandOffAsync(h, "A");
        Assert.AreEqual(SwitchoverOutcome.Completed, r1.Outcome, r1.Reason + "\n" + h.LogText);
        Assert.AreEqual(2, r1.Epoch);
        Assert.AreEqual(ReplicationRole.Primary, h["B"].Node.Role);
        Assert.AreEqual(ReplicationRole.Replica, h["A"].Node.Role);
        Assert.IsFalse(h["A"].Node.Writes.IsOpen);

        // Upgrade A: stop and restart it; it rejoins from the WAL, no snapshot.
        int before = h.Acknowledged.Count;
        await h.StopAsync("A");
        await WaitUntilAsync(() => h.Acknowledged.Count > before + 200, "writes while A is down");
        PairNodeHost a = await h.StartAsync("A");
        await WaitFollowingAsync(h, "A");
        Assert.AreEqual(0, a.Resyncs, "A rejoined without a snapshot");
        await WaitCaughtUpAsync(h, "B", "A");

        SwitchoverResult r2 = await HandOffAsync(h, "B");
        Assert.AreEqual(SwitchoverOutcome.Completed, r2.Outcome, r2.Reason + "\n" + h.LogText);
        Assert.AreEqual(3, r2.Epoch);

        before = h.Acknowledged.Count;
        await h.StopAsync("B");
        await WaitUntilAsync(() => h.Acknowledged.Count > before + 200, "writes while B is down");
        PairNodeHost b = await h.StartAsync("B");
        await WaitFollowingAsync(h, "B");
        Assert.AreEqual(0, b.Resyncs, "B rejoined without a snapshot");

        await stop.CancelAsync();
        await writers;
        await h.AssertConvergedAsync("A", "B");
        Assert.AreEqual(3, h["A"].Node.Epoch);
        Assert.AreEqual(3, h["B"].Node.Epoch);
    }

    [TestMethod]
    public async Task LaggingReplicaIsRejectedThenAbortedThenCompleted()
    {
        await using var h = new PairHarness();
        h.Switchover = new SwitchoverOptions { MaxLagToStart = 50, CatchUpTimeout = TimeSpan.FromMilliseconds(300), RpcGrace = TimeSpan.FromMilliseconds(500) };
        await h.StartAsync("A");
        await h.StartAsync("B");
        await WaitFollowingAsync(h, "B");
        (CancellationTokenSource stop, Task writers) = h.StartWriters(2);

        h["B"].Faults.PauseIngest = true;
        await WaitUntilAsync(() => h["A"].Db.GetLatestSequenceNumber() > h["B"].Db.GetLatestSequenceNumber() + 200, "B to lag");
        SwitchoverResult rejected = await HandOffAsync(h, "A");
        Assert.AreEqual(SwitchoverOutcome.Rejected, rejected.Outcome);
        Assert.AreEqual(TimeSpan.Zero, rejected.WritesHeld);
        Assert.AreEqual(1, h["A"].Node.Epoch);

        // Lag check bypassed: B cannot reach the handoff point in time, A resumes in a new epoch.
        h.Switchover.MaxLagToStart = ulong.MaxValue;
        SwitchoverResult aborted = await HandOffAsync(h, "A");
        Assert.AreEqual(SwitchoverOutcome.Aborted, aborted.Outcome, aborted.Reason + "\n" + h.LogText);
        Assert.AreEqual(ReplicationRole.Primary, h["A"].Node.Role);
        Assert.IsTrue(h["A"].Node.Writes.IsOpen);
        // Past the epoch B promised (2), and even: the epochs A starts on its own never collide with B's (odd).
        Assert.AreEqual(4, h["A"].Node.Epoch, "resumes past the epoch B promised");

        h["B"].Faults.PauseIngest = false;
        await WaitUntilAsync(() => h["B"].Node.Epoch == 4, "B to follow A's new epoch\n" + h.LogText);
        h.Switchover.CatchUpTimeout = TimeSpan.FromSeconds(3);
        SwitchoverResult completed = await HandOffAsync(h, "A");
        Assert.AreEqual(SwitchoverOutcome.Completed, completed.Outcome, completed.Reason + "\n" + h.LogText);
        Assert.AreEqual(5, completed.Epoch);

        await stop.CancelAsync();
        await writers;
        await WaitFollowingAsync(h, "A");
        await h.AssertConvergedAsync("B", "A");
    }

    [TestMethod]
    public async Task LostPromoteReplyIsSettledThroughRejectEpoch()
    {
        await using var h = new PairHarness();
        await h.StartAsync("A");
        await h.StartAsync("B");
        await WaitFollowingAsync(h, "B");
        (CancellationTokenSource stop, Task writers) = h.StartWriters(2);
        await WaitUntilAsync(() => h.Acknowledged.Count > 200, "some writes");

        h.AtoB.DropNextPromoteReply = true;
        SwitchoverResult r = await HandOffAsync(h, "A");
        Assert.AreEqual(SwitchoverOutcome.Completed, r.Outcome, r.Reason + "\n" + h.LogText);
        StringAssert.Contains(r.Reason, "answer was lost");
        Assert.AreEqual(ReplicationRole.Primary, h["B"].Node.Role);

        await stop.CancelAsync();
        await writers;
        await WaitFollowingAsync(h, "A");
        await h.AssertConvergedAsync("B", "A");
    }

    [TestMethod]
    public async Task LatePromoteIsRefusedAfterTheHandoffAborted()
    {
        await using var h = new PairHarness();
        h.Switchover = new SwitchoverOptions { CatchUpTimeout = TimeSpan.FromMilliseconds(300), RpcGrace = TimeSpan.FromMilliseconds(200) };
        await h.StartAsync("A");
        await h.StartAsync("B");
        await WaitFollowingAsync(h, "B");
        (CancellationTokenSource stop, Task writers) = h.StartWriters(2);
        await WaitUntilAsync(() => h.Acknowledged.Count > 200, "some writes");

        h.AtoB.DelayNextPromote = TimeSpan.FromMilliseconds(1500);
        SwitchoverResult r = await HandOffAsync(h, "A");
        Assert.AreEqual(SwitchoverOutcome.Aborted, r.Outcome, r.Reason + "\n" + h.LogText);
        Assert.AreEqual(ReplicationRole.Primary, h["A"].Node.Role);
        await WaitUntilAsync(() => h.AtoB.LateResult != null, "the late promote to land");
        Assert.IsFalse(h.AtoB.LateResult!.Promoted, "B promised the epoch, so the stale promote is refused");
        Assert.AreEqual(ReplicationRole.Replica, h["B"].Node.Role, "still exactly one primary");

        await stop.CancelAsync();
        await writers;
        await h.AssertConvergedAsync("A", "B");
    }

    [TestMethod]
    public async Task UnreachableTargetLeavesHandoffUnresolvedUntilResolved()
    {
        await using var h = new PairHarness();
        h.Switchover = new SwitchoverOptions { CatchUpTimeout = TimeSpan.FromMilliseconds(200), RpcGrace = TimeSpan.FromMilliseconds(100) };
        // This test settles the handoff itself (the host's own resolve loop would race it).
        h.ResolveInterval = System.Threading.Timeout.InfiniteTimeSpan;
        await h.StartAsync("A");
        await h.StartAsync("B");
        await WaitFollowingAsync(h, "B");

        // The promote request is lost on the wire and B then becomes unreachable.
        h.AtoB.DelayNextPromote = TimeSpan.FromSeconds(30);
        Task<SwitchoverResult> handoff = HandOffAsync(h, "A");
        await WaitUntilAsync(() => h["A"].Node.Role == ReplicationRole.HandingOff, "handoff to start");
        h.Network.SetPartitioned("A", "B", true);
        SwitchoverResult r = await handoff;
        Assert.AreEqual(SwitchoverOutcome.Unresolved, r.Outcome, r.Reason);
        Assert.IsFalse(h["A"].Node.Writes.IsOpen, "an unresolved handoff refuses writes");
        SwitchoverResult? still = await h["A"].ResolveAsync(CancellationToken.None);
        Assert.AreEqual(SwitchoverOutcome.Unresolved, still?.Outcome, "still cannot ask B");

        h.Network.SetPartitioned("A", "B", false);
        SwitchoverResult? resolved = await h["A"].ResolveAsync(CancellationToken.None);
        Assert.IsNotNull(resolved);
        Assert.AreEqual(SwitchoverOutcome.Aborted, resolved.Outcome, resolved.Reason);
        Assert.AreEqual(ReplicationRole.Primary, h["A"].Node.Role);
        Assert.AreEqual(ReplicationRole.Replica, h["B"].Node.Role);
        Assert.IsTrue(h["A"].Node.Writes.IsOpen);
    }

    [TestMethod]
    public async Task ForcedFailoverRebuildsTheOldPrimarysDivergentLog()
    {
        await using var h = new PairHarness();
        await h.StartAsync("A");
        await h.StartAsync("B");
        await WaitFollowingAsync(h, "B");
        await WriteThroughAsync(h["A"], 0, 200);
        await WaitUntilAsync(() => h["B"].Db.GetLatestSequenceNumber() == h["A"].Db.GetLatestSequenceNumber(), "B caught up");

        // A acknowledges writes B never receives, then dies.
        h["B"].Faults.PauseIngest = true;
        await WriteThroughAsync(h["A"], 200, 260);
        await h.StopAsync("A");
        h["B"].Faults.PauseIngest = false;

        long epoch = await h["B"].ForcePromoteAsync(CancellationToken.None);
        Assert.AreEqual(3, epoch, "the first epoch past 1 that B may start on its own (odd)");

        await WriteThroughAsync(h["B"], 1000, 1100);

        // A restarts as primary, learns B is primary in a newer epoch, steps down; its tail diverged -> snapshot.
        PairNodeHost a = await h.StartAsync("A");
        Assert.AreEqual(ReplicationRole.Replica, a.Node.Role, h.LogText);
        await WaitUntilAsync(() => a.Resyncs == 1 && !a.IsResyncing, "A rebuilt from a snapshot\n" + h.LogText);
        await WriteThroughAsync(h["B"], 1100, 1150);
        await h.AssertConvergedAsync("B", "A");
        Assert.IsNull(Contents(a.Db).GetValueOrDefault(K(230)), "A's unreplicated write is gone, as expected for async replication");
    }

    private static async Task WriteThroughAsync(PairNodeHost host, int from, int to)
    {
        for (int i = from; i < to; i++)
        {
            using var b = new WriteBatch();
            b.Put(U(K(i)), U(V(i, 0)));
            PairWriteResult r = await host.WriteAsync(b, TimeSpan.FromSeconds(1), CancellationToken.None);
            Assert.AreEqual(PairWriteStatus.Applied, r.Status);
        }
    }
}
