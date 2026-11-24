using RocksDb.Managed.Cluster;
using RocksDb.Managed.Replication.Hosting;
using RocksDb.Managed.Replication.Switchover;

namespace RocksDb.Managed.Replication.Tests;

/// <summary>
/// Audit #13 (multi-threading), replication layer: races between client writes and role changes, and background loops
/// that stopped for good on an exception (docs/validation.md "Audit #13").
/// </summary>
[TestClass]
public sealed class Audit13ReplicationThreadingTests
{
    private static readonly string[] s_nodes = ["n1", "n2", "n3"];

    private static WriteBatch Batch(string key)
    {
        var batch = new WriteBatch();
        batch.Put(U(key), U("v-" + key));
        return batch;
    }

    // ---- write gate ----------------------------------------------------------------------------------------

    [TestMethod]
    public async Task WriteGateDrainWaitsForAdmittedWritesWithoutChangingTheState()
    {
        var gate = new PrimaryWriteGate(open: true);
        Assert.IsTrue(await gate.TryEnterAsync(TimeSpan.Zero, CancellationToken.None));
        gate.Close();
        Task drain = gate.DrainAsync(CancellationToken.None);
        Assert.IsFalse(drain.IsCompleted, "one write in flight");
        Assert.IsFalse(await gate.TryEnterAsync(TimeSpan.Zero, CancellationToken.None), "closed: nothing new is admitted");
        Assert.IsFalse(drain.IsCompleted, "a refused write does not end the drain");
        gate.Exit();
        await drain.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.IsTrue(gate.IsClosed);
        await gate.DrainAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(5)); // idle: completes at once
    }

    [TestMethod]
    public async Task SealedWriteGateIsNotReopened()
    {
        // PairNodeHost.DisposeAsync closes the gate; a promotion or a handoff being settled at that moment
        // (ResumeAsPrimary, RunPromotionAsync, ForcePromoteAsync) called OpenGate afterwards and admitted writes into
        // a database that was being closed.
        var gate = new PrimaryWriteGate(open: true);
        gate.Seal();
        Assert.IsTrue(gate.IsOpen, "sealing does not change the state");
        await gate.HoldAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(5));
        gate.Close();
        gate.OpenGate();
        Assert.IsTrue(gate.IsClosed);
        Assert.IsFalse(await gate.TryEnterAsync(TimeSpan.Zero, CancellationToken.None));
    }

    // ---- pair ----------------------------------------------------------------------------------------------

    /// <summary><see cref="IPairNodeClient"/> whose status call waits until the test lets it through.</summary>
    private sealed class BlockedStatusClient(IPairNodeClient inner) : IPairNodeClient
    {
        public readonly TaskCompletionSource Asked = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public readonly TaskCompletionSource Release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public string NodeId => inner.NodeId;

        public async Task<ReplicationNodeStatus?> GetStatusAsync(CancellationToken cancellationToken)
        {
            Asked.TrySetResult();
            await Release.Task.WaitAsync(cancellationToken);
            return await inner.GetStatusAsync(cancellationToken);
        }

        public Task<PromoteResult?> PromoteAsync(PromoteRequest request, CancellationToken cancellationToken) => inner.PromoteAsync(request, cancellationToken);
        public Task<ReplicationNodeStatus?> RejectEpochAsync(long epoch, CancellationToken cancellationToken) => inner.RejectEpochAsync(epoch, cancellationToken);
        public Task<ReplicaLogCheck?> CheckReplicaLogAsync(ulong replicaLatestSeq, long replicaEpochAtLatest, CancellationToken cancellationToken) =>
            inner.CheckReplicaLogAsync(replicaLatestSeq, replicaEpochAtLatest, cancellationToken);
        public IAsyncEnumerable<WalStreamMessage> StreamUpdatesAsync(ulong startSeq, CancellationToken cancellationToken) => inner.StreamUpdatesAsync(startSeq, cancellationToken);
        public Task<string?> GetEpochsAsync(CancellationToken cancellationToken) => inner.GetEpochsAsync(cancellationToken);
        public IAsyncEnumerable<SnapshotChunk> StreamSnapshotAsync(CancellationToken cancellationToken) => inner.StreamSnapshotAsync(cancellationToken);
        public Task ReportProgressAsync(ulong appliedThrough, long epochAtApplied, CancellationToken cancellationToken) =>
            inner.ReportProgressAsync(appliedThrough, epochAtApplied, cancellationToken);
    }

    [TestMethod]
    public async Task RestartedPrimaryAdmitsNoWriteBeforeItSettledItsRole()
    {
        // A node restarting as primary first asks the other node whether it was replaced and whether the replica is
        // ahead of the log it recovered (StartAsync -> ResolveAsync, YieldToReplicaAheadAsync). The write gate was
        // opened from the persisted role as soon as the SwitchoverNode existed and WriteAsync did not check that the
        // host serves yet, so a client retry arriving during those calls was applied and acknowledged in the old epoch -
        // at a sequence number the replica may hold another entry for, which a handoff to that replica then keeps.
        await using var h = new PairHarness();
        await h.StartAsync("A");
        await h.StartAsync("B");
        await WaitUntilAsync(() => h["B"].GetStatus().AttachedFromSeq > 0, "B following");
        using (WriteBatch b = Batch("before")) Assert.AreEqual(PairWriteStatus.Applied, (await h["A"].WriteAsync(b, TimeSpan.FromSeconds(2), CancellationToken.None)).Status);
        await h.StopAsync("A");

        var peer = new BlockedStatusClient(h.AtoB);
        var restarted = new PairNodeHost(new PairNodeHostOptions
        {
            NodeId = "A",
            PeerId = "B",
            DbPath = h.DbPath("A"),
            InitialRole = ReplicationRole.Primary,
            DbOptionsFactory = () => new DbOptions { WalTtlSeconds = 600, FileSystem = LocalFileSystem.Instance },
            Switchover = h.Switchover,
            RetryDelay = TimeSpan.FromMilliseconds(50),
        }, peer);
        try
        {
            Task start = restarted.StartAsync(CancellationToken.None);
            await peer.Asked.Task.WaitAsync(TimeSpan.FromSeconds(10)); // StartAsync is asking B; the role is not settled
            Assert.IsFalse(start.IsCompleted);

            using WriteBatch early = Batch("early");
            PairWriteResult refused = await restarted.WriteAsync(early, TimeSpan.Zero, CancellationToken.None);
            Assert.AreEqual(PairWriteStatus.NotPrimary, refused.Status, "a write was applied before the node settled its role");

            // With a hold timeout the write waits for the start, like a write held by a handoff.
            using WriteBatch held = Batch("held");
            Task<PairWriteResult> waiting = restarted.WriteAsync(held, TimeSpan.FromSeconds(20), CancellationToken.None).AsTask();
            await Task.Delay(100);
            Assert.IsFalse(waiting.IsCompleted, "the write did not wait for the start");

            peer.Release.TrySetResult();
            await start.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.AreEqual(PairWriteStatus.Applied, (await waiting.WaitAsync(TimeSpan.FromSeconds(10))).Status);
            Assert.IsNull(restarted.Db.Get(U("early")));
            Assert.IsNotNull(restarted.Db.Get(U("held")));
        }
        finally
        {
            peer.Release.TrySetResult();
            await restarted.DisposeAsync();
        }
    }

    [TestMethod]
    public async Task CancelledForcePromoteLeavesTheReplicaFollowing()
    {
        // ForcePromoteAsync detaches the replica's stream and then waits (cancellably) for a chunk being applied. When
        // that wait was cancelled - or the new role could not be persisted - the node stayed a replica with no stream,
        // and nothing attached it again: it never received another entry.
        await using var h = new PairHarness();
        await h.StartAsync("A");
        await h.StartAsync("B");
        await WaitUntilAsync(() => h["B"].GetStatus().AttachedFromSeq > 0, "B following");

        try
        {
            await h["B"].ForcePromoteAsync(new CancellationToken(canceled: true));
            Assert.Fail("the promotion was not cancelled");
        }
        catch (OperationCanceledException)
        {
        }
        Assert.AreEqual(ReplicationRole.Replica, h["B"].Node.Role);

        for (int i = 0; i < 5; i++)
        {
            using WriteBatch b = Batch("after" + i);
            Assert.AreEqual(PairWriteStatus.Applied, (await h["A"].WriteAsync(b, TimeSpan.FromSeconds(2), CancellationToken.None)).Status);
        }
        await WaitUntilAsync(() => h["B"].Db.GetLatestSequenceNumber() == h["A"].Db.GetLatestSequenceNumber(), "B to follow A again\n" + h.LogText);
        await h.AssertConvergedAsync("A", "B");
    }

    // ---- progress reports ----------------------------------------------------------------------------------

    [TestMethod]
    public async Task ProgressReporterDisposeIgnoresATransportsOwnCancellationException()
    {
        // A cancelled gRPC call throws RpcException(Cancelled), not OperationCanceledException. That exception escaped
        // the reporter's loop, and DisposeAsync rethrew it - from the follower loop's "await using", in place of what
        // that loop was returning (a resync it had just requested).
        var sending = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var reporter = new ProgressReporter(async (_, ct) =>
        {
            sending.TrySetResult();
            try
            {
                await Task.Delay(System.Threading.Timeout.Infinite, ct);
            }
            catch (OperationCanceledException)
            {
                throw new IOException("call cancelled");
            }
        }, TimeSpan.FromMilliseconds(10));
        reporter.Report(1);
        await sending.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await reporter.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
    }

    // ---- cluster -------------------------------------------------------------------------------------------

    private static async Task<ClusterNodeHost> StartConvergedClusterAsync(ClusterHarness c)
    {
        ClusterNodeHost n1 = await c.StartAsync("n1");
        for (int i = 0; i < 20; i++) Assert.IsTrue(await n1.TryWriteAsLeaderAsync(U(K(i)), U(V(i, 0)), CancellationToken.None));
        await c.StartAsync("n2");
        await c.StartAsync("n3");
        await WaitUntilAsync(() => c.Statuses().TrueForAll(s => s.Mode == ClusterMode.Cluster), "cluster mode\n" + c.LogText);
        ClusterNodeHost leader = await c.WaitForLeaderAsync(null);
        await c.AssertConvergedAsync(leader);
        return leader;
    }

    [TestMethod]
    public async Task ElectionTimerIsRearmedWhenStartingACampaignThrows()
    {
        // The election timer is one-shot. When BecomeCandidate threw (the term could not be persisted, or an event
        // handler threw - the host logs there), the reset after it was skipped and the exception swallowed: the node
        // never campaigned again unless a heartbeat or a granted vote happened to reset the timer.
        await using var c = new ClusterHarness(3, null);
        ClusterNodeHost leader = await StartConvergedClusterAsync(c);
        ClusterNodeHost follower = c[Array.Find(s_nodes, id => id != leader.NodeId)!];
        int campaigns = 0;
        follower.Node.RoleChanged += (_, e) =>
        {
            if (e.Role == RaftRole.Candidate && Interlocked.Increment(ref campaigns) == 1) throw new IOException("injected failure while starting the campaign");
        };
        c.Network.Isolate(follower.NodeId, s_nodes);
        await WaitUntilAsync(() => Volatile.Read(ref campaigns) >= 2, "a second campaign after the failed one\n" + c.LogText);
        c.Network.Heal();
    }

    [TestMethod]
    public async Task ClientWriteAdmittedBeforeAStepDownDoesNotInterleaveWithFollowerIngest()
    {
        // The leader check and the database write of a client write were not atomic with a role change. A write that
        // passed the check and was still waiting in the write path (here: its WAL sync) when the node was deposed was
        // applied after the node had attached to the new leader with "my log ends at N" - between that and the leader's
        // entry N+1, which then landed at N+2. The node reported N+2 as held for the new term although its N+1 is the
        // stale write and its N+2 the leader's N+1, and with that report the leader committed (and acknowledged) an
        // entry only it held. Now client writes go through a gate that closes with the leadership and is drained
        // before the follower reads the end of its log: the stale write lands first, the attach check sees it, and the
        // node is rebuilt before it reports anything.
        var gates = new Dictionary<string, GatedFileSystem>(StringComparer.Ordinal);
        var c = new ClusterHarness(3, null)
        {
            WrapFileSystem = (id, fs) => gates[id] = new GatedFileSystem(fs),
        };
        await using (c)
        {
            ClusterNodeHost old = await StartConvergedClusterAsync(c);
            long term = old.Node.CurrentTerm;
            string[] others = Array.FindAll(s_nodes, id => id != old.NodeId);

            // A client write on the leader, stopped in its WAL sync (admitted, not applied yet).
            gates[old.NodeId].Arm();
            Task<bool> stale = old.WriteCommittedAsync(U("stale"), U("s"), TimeSpan.FromSeconds(2), CancellationToken.None).AsTask();
            await gates[old.NodeId].Blocked.WaitAsync(TimeSpan.FromSeconds(10));

            // The other two elect a new leader; then only the new leader and the old one remain (a quorum of two).
            c.Network.Isolate(old.NodeId, s_nodes);
            ClusterNodeHost leader = await c.WaitForLeaderAmongAsync(others, term + 1);
            await c.StopAsync(Array.Find(others, id => id != leader.NodeId)!);
            c.Network.Heal();
            await WaitUntilAsync(() => old.GetStatus() is { Role: RaftRole.Follower } s && s.LeaderId == leader.NodeId, "the old leader to follow the new one\n" + c.LogText);
            // Before the fix the old leader has attached by now and holds the new leader's first entry in its write
            // queue, behind the stale write.
            await Task.Delay(500);

            Task<bool> write = leader.WriteCommittedAsync(U("x"), U("1"), TimeSpan.FromSeconds(30), CancellationToken.None).AsTask();
            await Task.Delay(200);
            Assert.IsFalse(write.IsCompleted, "no quorum holds the entry yet");
            gates[old.NodeId].Release();

            Assert.IsTrue(await write.WaitAsync(TimeSpan.FromSeconds(40)), "the write did not commit\n" + c.LogText);
            // Committed means a quorum holds it: the leader and the old leader.
            Assert.IsFalse(old.IsResyncing, "acknowledged while the only other node was rebuilding\n" + c.LogText);
            Assert.IsTrue(old.TryUseDatabase(0, static (db, _) => db.Get(U("x")) is not null, out bool held) && held,
                "the entry was acknowledged as committed, but the old leader does not hold it\n" + c.LogText);
            try
            {
                await stale.WaitAsync(TimeSpan.FromSeconds(10));
            }
            catch (Exception e) when (e is RocksDbException or ObjectDisposedException)
            {
                // the old leader's database was replaced under the stale write: indeterminate, as documented
            }
            await c.AssertConvergedAsync(leader);
        }
    }
}
