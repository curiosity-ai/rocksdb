using RocksDb.Managed.Replication.Hosting;
using RocksDb.Managed.Replication.Switchover;

namespace RocksDb.Managed.Replication.Tests;

/// <summary>
/// Audit #4, durability of the replication layer judged from first principles (no acknowledged write lost, no two
/// primaries, one history per epoch): what a pair promises for an acknowledged write under a power loss, in each of
/// its modes (<see cref="PairNodeHostOptions.SyncWal"/>, <see cref="PairNodeHostOptions.WriteAck"/>,
/// <see cref="PairNodeHostOptions.YieldToReplicaAheadOnRestart"/>), and what a snapshot delta may reuse. A power loss
/// is a <see cref="MemoryFileSystem"/> that drops everything not fsync'd (<see cref="PairHarness.PowerLossAsync"/>).
/// </summary>
[TestClass]
public sealed class Audit4ReplicationDurabilityTests
{
    private static PairHarness MemoryPair(out MemoryFileSystem fsA, out MemoryFileSystem fsB)
    {
        var a = new MemoryFileSystem();
        var b = new MemoryFileSystem();
        fsA = a;
        fsB = b;
        return new PairHarness { FileSystemOf = id => id == "A" ? a : b };
    }

    private static async Task<PairWriteResult> PutAsync(PairNodeHost host, string key)
    {
        using var batch = new WriteBatch();
        batch.Put(U(key), U("v-" + key));
        return await host.WriteAsync(batch, TimeSpan.FromSeconds(2), CancellationToken.None);
    }

    /// <summary>Writes <paramref name="count"/> keys "{prefix}{i}"; every one must be acknowledged.</summary>
    private static async Task WriteAcknowledgedAsync(PairNodeHost host, string prefix, int count)
    {
        for (int i = 0; i < count; i++) Assert.AreEqual(PairWriteStatus.Applied, (await PutAsync(host, prefix + i)).Status, prefix + i);
    }

    private static int CountKeys(RocksDatabase db, string prefix)
    {
        int n = 0;
        foreach (string key in Contents(db).Keys)
        {
            if (key.StartsWith(prefix, StringComparison.Ordinal)) n++;
        }
        return n;
    }

    private static Task CaughtUpAsync(PairHarness h, string primary, string replica) =>
        WaitUntilAsync(() => !h[replica].IsResyncing && h[replica].Db.GetLatestSequenceNumber() == h[primary].Db.GetLatestSequenceNumber(),
            replica + " to hold " + primary + "'s log\n" + h.LogText);

    /// <summary>A started pair with 20 durable "base" writes on A and 10 acknowledged "tail" writes that B holds too.</summary>
    private static async Task WriteBaseAndReplicatedTailAsync(PairHarness h)
    {
        await h.StartAsync("A");
        await h.StartAsync("B");
        await WaitUntilAsync(() => h["B"].GetStatus().AttachedFromSeq > 0, "B following");
        await WriteAcknowledgedAsync(h["A"], "base", 20);
        h["A"].Db.SyncWal();
        await WriteAcknowledgedAsync(h["A"], "tail", 10);
        await CaughtUpAsync(h, "A", "B");
    }

    /// <summary>
    /// The loss, as the pair behaved before this audit (rocksdb-sharp behaves the same): client writes are not fsync'd,
    /// the primary acknowledges them, the replica receives them - and after a power loss of the primary the replica,
    /// the only node that still holds them, is rebuilt from the primary. That is what
    /// <see cref="PairNodeHostOptions.YieldToReplicaAheadOnRestart"/> = false still does, and what happens in every
    /// mode without <see cref="PairNodeHostOptions.SyncWal"/> when the replica cannot be reached during the restart.
    /// </summary>
    [TestMethod]
    [DataRow(false, false, DisplayName = "replica reachable, primary does not yield (the behaviour before the audit)")]
    [DataRow(true, true, DisplayName = "replica unreachable while the primary restarts")]
    public async Task WithoutSyncWalAPrimaryPowerLossLosesAcknowledgedWritesTheReplicaHeld(bool yield, bool partitioned)
    {
        await using PairHarness h = MemoryPair(out MemoryFileSystem fsA, out _);
        h.YieldToReplicaAheadOnRestart = yield;
        await WriteBaseAndReplicatedTailAsync(h);
        Assert.AreEqual(10, CountKeys(h["B"].Db, "tail"), "the replica holds the acknowledged tail");

        if (partitioned) h.Network.SetPartitioned("A", "B", true);
        await h.PowerLossAsync("A", fsA);
        await h.StartAsync("A");
        Assert.AreEqual(ReplicationRole.Primary, h["A"].Node.Role);
        Assert.AreEqual(20, CountKeys(h["A"].Db, "base"));
        Assert.AreEqual(0, CountKeys(h["A"].Db, "tail"), "the tail was only in the page cache of A");
        await WriteAcknowledgedAsync(h["A"], "new", 15);
        h.Network.Heal();

        await WaitUntilAsync(() => h["B"].Resyncs == 1, "B to be rebuilt from A\n" + h.LogText);
        await CaughtUpAsync(h, "A", "B");
        AssertSameContents(Contents(h["A"].Db), Contents(h["B"].Db), "pair after the rebuild");
        Assert.AreEqual(0, CountKeys(h["B"].Db, "tail"), "documented loss: 10 acknowledged writes are gone from both nodes");
    }

    /// <summary>
    /// The fix for the default mode: a primary that restarts with a log ending before the replica's - and the replica's
    /// extra entries are its own, by the epoch map - hands the role to the replica (the handoff protocol with the
    /// replica's log end as the handoff point) and gets its lost tail back from it. No acknowledged write is lost, and
    /// the node that holds them is not rebuilt. (The restarted node may be: a replica stores a received chunk as one
    /// WAL batch, and a stream cannot start in the middle of one.) Failed before: the replica was rebuilt from the
    /// primary.
    /// </summary>
    [TestMethod]
    public async Task PrimaryThatLostItsUnsyncedTailYieldsToTheReplicaThatHoldsIt()
    {
        await using PairHarness h = MemoryPair(out MemoryFileSystem fsA, out _);
        await WriteBaseAndReplicatedTailAsync(h);
        ulong tail = h["B"].Db.GetLatestSequenceNumber();
        long epoch = h["A"].Node.Epoch;

        await h.PowerLossAsync("A", fsA);
        await h.StartAsync("A");

        Assert.AreEqual(ReplicationRole.Replica, h["A"].Node.Role, "A must not continue as primary without its tail\n" + h.LogText);
        Assert.AreEqual(ReplicationRole.Primary, h["B"].Node.Role);
        Assert.IsTrue(h["B"].Node.Epoch > epoch);
        Assert.AreEqual(tail, h["B"].Db.GetLatestSequenceNumber(), "B took over exactly at the end of its log");
        await WriteAcknowledgedAsync(h["B"], "new", 15);
        await CaughtUpAsync(h, "B", "A");
        AssertSameContents(Contents(h["B"].Db), Contents(h["A"].Db), "pair after A got its tail back");
        Assert.AreEqual(10, CountKeys(h["A"].Db, "tail"), "acknowledged writes lost");
        Assert.AreEqual(0, h["B"].Resyncs, "the node that holds the acknowledged writes must not be rebuilt\n" + h.LogText);

    }

    /// <summary>
    /// The same after a handoff: the promoted node took over with entries that were not on its disk, loses power, and
    /// restarts as primary behind the old primary (now its replica). It yields back instead of rebuilding it.
    /// </summary>
    [TestMethod]
    public async Task PromotedNodeThatLostTheEntriesItTookOverWithYieldsBack()
    {
        await using PairHarness h = MemoryPair(out _, out MemoryFileSystem fsB);
        await WriteBaseAndReplicatedTailAsync(h);
        SwitchoverResult handoff = await h["A"].HandOffAsync(CancellationToken.None);
        Assert.AreEqual(SwitchoverOutcome.Completed, handoff.Outcome, handoff.Reason);
        await WaitUntilAsync(() => h["A"].GetStatus().AttachedFromSeq > 0, "A following B");

        await h.PowerLossAsync("B", fsB);
        await h.StartAsync("B");

        Assert.AreEqual(ReplicationRole.Primary, h["A"].Node.Role, h.LogText);
        Assert.AreEqual(ReplicationRole.Replica, h["B"].Node.Role);
        await WriteAcknowledgedAsync(h["A"], "new", 5);
        await CaughtUpAsync(h, "A", "B");
        Assert.AreEqual(30, CountKeys(h["B"].Db, "base") + CountKeys(h["B"].Db, "tail"), "acknowledged writes lost");
        Assert.AreEqual(0, h["A"].Resyncs, "the node that holds the acknowledged writes must not be rebuilt\n" + h.LogText);
    }

    /// <summary>
    /// <see cref="PairNodeHostOptions.SyncWal"/>: an acknowledged write is on the primary's disk. A power loss of the
    /// primary loses nothing, whether or not the replica can be reached, and the replica - which only ever receives
    /// fsync'd entries - is never ahead, so it just goes on following.
    /// </summary>
    [TestMethod]
    public async Task SyncWalKeepsEveryAcknowledgedWriteAcrossAPrimaryPowerLoss()
    {
        await using PairHarness h = MemoryPair(out MemoryFileSystem fsA, out _);
        h.SyncWal = true;
        await WriteBaseAndReplicatedTailAsync(h);

        h.Network.SetPartitioned("A", "B", true);
        await h.PowerLossAsync("A", fsA);
        await h.StartAsync("A");
        Assert.AreEqual(ReplicationRole.Primary, h["A"].Node.Role);
        Assert.AreEqual(10, CountKeys(h["A"].Db, "tail"), "an fsync'd acknowledged write was lost");
        await WriteAcknowledgedAsync(h["A"], "new", 15);
        h.Network.Heal();

        await CaughtUpAsync(h, "A", "B");
        AssertSameContents(Contents(h["A"].Db), Contents(h["B"].Db), "pair");
        Assert.AreEqual(0, h["B"].Resyncs, "the replica was never ahead of the primary's disk\n" + h.LogText);
    }

    /// <summary>
    /// <see cref="PairNodeHostOptions.SyncWal"/> on the replica: a chunk is fsync'd before the replica's sequence number
    /// moves past it, so the node a handoff promotes holds the handoff point on disk. It loses power right after taking
    /// over, while the old primary is unreachable, and still has every acknowledged write.
    /// </summary>
    [TestMethod]
    public async Task SyncWalReplicaTakesOverWithEntriesThatAreOnItsDisk()
    {
        await using PairHarness h = MemoryPair(out _, out MemoryFileSystem fsB);
        h.SyncWal = true;
        await WriteBaseAndReplicatedTailAsync(h);
        SwitchoverResult handoff = await h["A"].HandOffAsync(CancellationToken.None);
        Assert.AreEqual(SwitchoverOutcome.Completed, handoff.Outcome, handoff.Reason);

        h.Network.SetPartitioned("A", "B", true);
        await h.PowerLossAsync("B", fsB);
        await h.StartAsync("B");
        Assert.AreEqual(ReplicationRole.Primary, h["B"].Node.Role);
        Assert.AreEqual(handoff.HandoffSeq, h["B"].Db.GetLatestSequenceNumber(), "the promoted node lost entries it took over with");
        Assert.AreEqual(30, CountKeys(h["B"].Db, "base") + CountKeys(h["B"].Db, "tail"));
    }

    /// <summary>
    /// With <see cref="PairNodeHostOptions.SyncWal"/> a write is not visible (and so not streamed) before its fsync
    /// returned: the replica cannot hold an entry the primary's disk does not.
    /// </summary>
    [TestMethod]
    public async Task SyncWalWriteIsNotPublishedBeforeItsFsync()
    {
        var gated = new GatedFileSystem(new MemoryFileSystem());
        var fsB = new MemoryFileSystem();
        await using var h = new PairHarness { FileSystemOf = id => id == "A" ? gated : fsB, SyncWal = true };
        await h.StartAsync("A");
        await WriteAcknowledgedAsync(h["A"], "base", 3);
        ulong before = h["A"].Db.GetLatestSequenceNumber();

        gated.Arm();
        Task<PairWriteResult> write = PutAsync(h["A"], "pending");
        await gated.Blocked.WaitAsync(TestUtil.Timeout);
        Assert.IsFalse(write.IsCompleted, "acknowledged before the fsync");
        Assert.AreEqual(before, h["A"].Db.GetLatestSequenceNumber(), "the entry is visible to the WAL stream before its fsync");
        gated.Release();
        Assert.AreEqual(PairWriteStatus.Applied, (await write.WaitAsync(TestUtil.Timeout)).Status);
        Assert.AreEqual(before + 1, h["A"].Db.GetLatestSequenceNumber());
    }

    /// <summary>
    /// <see cref="PairWriteAck.Replica"/>: a write is acknowledged once the replica reported that it holds it. While
    /// the replica does not apply, the write ends as NotReplicated (indeterminate, not acknowledged); every acknowledged
    /// write is on the replica at the moment of the acknowledgement, so it survives the permanent loss of the primary
    /// (<see cref="PairNodeHost.ForcePromoteAsync"/>).
    /// </summary>
    [TestMethod]
    public async Task ReplicaAckAcknowledgesOnlyWhatTheReplicaHolds()
    {
        await using PairHarness h = MemoryPair(out _, out _);
        h.WriteAck = PairWriteAck.Replica;
        h.ReplicaAckTimeout = TimeSpan.FromMilliseconds(250);
        await h.StartAsync("A");
        await h.StartAsync("B");
        for (int i = 0; i < 20; i++)
        {
            PairWriteResult r = await PutAsync(h["A"], "acked" + i);
            Assert.AreEqual(PairWriteStatus.Applied, r.Status, "write " + i + "\n" + h.LogText);
            Assert.IsTrue(h["B"].Db.GetLatestSequenceNumber() >= r.Sequence, "acknowledged before the replica held it");
            Assert.IsTrue(h["A"].ReplicaAckedSequence >= r.Sequence);
        }

        // The replica stops applying: nothing is acknowledged, although the primary wrote it.
        h["B"].Faults.PauseIngest = true;
        PairWriteResult stuck = await PutAsync(h["A"], "unacked");
        Assert.AreEqual(PairWriteStatus.NotReplicated, stuck.Status);
        Assert.AreEqual(stuck.Sequence, h["A"].Db.GetLatestSequenceNumber(), "the write is in the primary's log (indeterminate)");
        Assert.IsTrue(h["B"].Db.GetLatestSequenceNumber() < stuck.Sequence);

        // A lost report is sent again: the write after it is acknowledged.
        h.BtoA.DropProgressReports = true;
        h["B"].Faults.PauseIngest = false;
        await WaitUntilAsync(() => h["B"].Db.GetLatestSequenceNumber() == stuck.Sequence, "B to apply");
        h.BtoA.DropProgressReports = false;
        await WaitUntilAsync(() => h["A"].ReplicaAckedSequence == stuck.Sequence, "the repeated report\n" + h.LogText);

        // The primary is lost for good: the replica has every acknowledged write.
        await h.StopAsync("A");
        await h["B"].ForcePromoteAsync(CancellationToken.None);
        Assert.AreEqual(20, CountKeys(h["B"].Db, "acked"));
    }

    /// <summary>
    /// A progress report counts only under the attach check's rule (the primary holds that entry in the same epoch):
    /// a replica with another history, or one that claims entries the primary does not have, acknowledges nothing.
    /// </summary>
    [TestMethod]
    public async Task ReplicaAckIgnoresAReportThatIsNotAboutThePrimarysLog()
    {
        await using PairHarness h = MemoryPair(out _, out _);
        h.WriteAck = PairWriteAck.Replica;
        h.ReplicaAckTimeout = TimeSpan.FromMilliseconds(50);
        PairNodeHost a = await h.StartAsync("A");
        Assert.AreEqual(PairWriteStatus.NotReplicated, (await PutAsync(a, "k1")).Status, "no replica, no acknowledgement");
        ulong latest = a.Db.GetLatestSequenceNumber();
        long epoch = a.Node.Epoch;

        a.HandleReportProgress(latest, epoch + 1);
        Assert.AreEqual(0UL, a.ReplicaAckedSequence, "a report for another epoch's entry was counted");
        a.HandleReportProgress(latest + 5, epoch);
        Assert.AreEqual(0UL, a.ReplicaAckedSequence, "a report past the end of the primary's log was counted");
        a.HandleReportProgress(latest, epoch);
        Assert.AreEqual(latest, a.ReplicaAckedSequence);
    }

    /// <summary>An <see cref="ISwitchoverPeer"/> that answers from fields and records the promote it was sent.</summary>
    private sealed class ScriptedPeer(string id) : ISwitchoverPeer
    {
        public ReplicationNodeStatus? Status;
        public PromoteRequest? Promote;
        public long RejectedEpoch;

        public string NodeId => id;
        public Task<ReplicationNodeStatus?> GetStatusAsync(CancellationToken cancellationToken) => Task.FromResult(Status);

        public Task<PromoteResult?> PromoteAsync(PromoteRequest request, CancellationToken cancellationToken)
        {
            if (request.IsProbe) return Task.FromResult<PromoteResult?>(new PromoteResult(false, 0, ReplicationRole.Replica, "probe"));
            Promote = request;
            Status = Status! with { Role = ReplicationRole.Primary, Epoch = request.Epoch, PrimaryId = id };
            return Task.FromResult<PromoteResult?>(new PromoteResult(true, request.Epoch, ReplicationRole.Primary, null));
        }

        public Task<ReplicationNodeStatus?> RejectEpochAsync(long epoch, CancellationToken cancellationToken)
        {
            RejectedEpoch = epoch;
            return Task.FromResult(Status);
        }
    }

    /// <summary>
    /// The safety condition of the yield: the replica's entries past this node's log must be this node's own - the
    /// replica's last entry carries the epoch this node's (fsync'd) epoch map has for that sequence number. A replica
    /// whose tail has another epoch (its own writes as a force-promoted primary, before it was demoted again) is not
    /// yielded to: that would replace this node's history with a divergent one.
    /// </summary>
    [TestMethod]
    public async Task YieldIsRefusedUnlessTheReplicasTailIsThisNodesOwnLog()
    {
        await using RocksDatabase db = await RocksDatabase.OpenAsync("/db", DbOpts(new MemoryFileSystem()));
        for (int i = 0; i < 5; i++) db.Put(U("k" + i), U("v"));
        var state = new ReplicationRoleState(new MemoryFileSystem(), "/role");
        SwitchoverNode.BootstrapPrimary(state, db, "A");
        var node = new SwitchoverNode("A", "B", state, db, static _ => Task.CompletedTask);
        long epoch = node.Epoch;
        var options = new SwitchoverOptions { CatchUpTimeout = TimeSpan.FromSeconds(1), RpcGrace = TimeSpan.FromSeconds(1) };
        var peer = new ScriptedPeer("B") { Status = new ReplicationNodeStatus("B", ReplicationRole.Replica, epoch, epoch, "A", 8, false) };
        string own = node.SerializeEpochs();
        var other = Cluster.RaftLogTerms.Deserialize(own);
        Assert.IsTrue(other.Record(6, epoch + 1));
        string foreign = other.Serialize();

        // Not ahead, unreachable, no epoch map, not following this node, tail in another epoch: all refused, nothing changes.
        SwitchoverResult r = await node.YieldToReplicaAheadAsync(peer, _ => Task.FromResult<string?>(foreign), options, CancellationToken.None);
        Assert.AreEqual(SwitchoverOutcome.Rejected, r.Outcome, "yielded to a replica whose tail is in an epoch this node never wrote");
        r = await node.YieldToReplicaAheadAsync(peer, _ => Task.FromResult<string?>(null), options, CancellationToken.None);
        Assert.AreEqual(SwitchoverOutcome.Rejected, r.Outcome);
        peer.Status = peer.Status with { LatestSeq = 5 };
        r = await node.YieldToReplicaAheadAsync(peer, _ => Task.FromResult<string?>(own), options, CancellationToken.None);
        Assert.AreEqual(SwitchoverOutcome.Rejected, r.Outcome, "yielded to a replica that is not ahead");
        peer.Status = peer.Status with { LatestSeq = 8, PrimaryId = "C" };
        r = await node.YieldToReplicaAheadAsync(peer, _ => Task.FromResult<string?>(own), options, CancellationToken.None);
        Assert.AreEqual(SwitchoverOutcome.Rejected, r.Outcome, "yielded to a node that does not follow this one");
        peer.Status = peer.Status with { PrimaryId = "A", Role = ReplicationRole.Primary };
        r = await node.YieldToReplicaAheadAsync(peer, _ => Task.FromResult<string?>(own), options, CancellationToken.None);
        Assert.AreEqual(SwitchoverOutcome.Rejected, r.Outcome);
        Assert.IsNull(peer.Promote, "a refused yield sent a promote");
        Assert.AreEqual(ReplicationRole.Primary, node.Role);
        Assert.AreEqual(epoch, node.Epoch);
        Assert.IsTrue(node.Writes.IsOpen);

        // The replica's tail is this node's log: the handoff runs with the replica's log end as the handoff point.
        peer.Status = peer.Status with { Role = ReplicationRole.Replica };
        r = await node.YieldToReplicaAheadAsync(peer, _ => Task.FromResult<string?>(own), options, CancellationToken.None);
        Assert.AreEqual(SwitchoverOutcome.Completed, r.Outcome, r.Reason);
        Assert.AreEqual(8UL, peer.Promote!.HandoffSeq);
        Assert.AreEqual(8UL, r.HandoffSeq);
        Assert.AreEqual(ReplicationRole.Replica, node.Role);
        Assert.IsTrue(node.Writes.IsClosed);
        Assert.IsTrue(state.PromisedEpoch >= peer.Promote.Epoch, "the proposed epoch is promised before the promote goes out");
    }

    // ---------------------------------------------------------------------------------------------------
    // Snapshot delta reuse

    private static async Task<RocksDatabase> OpenBlobDbAsync(MemoryFileSystem fs) =>
        await RocksDatabase.OpenAsync("/db", new DbOptions { CreateIfMissing = true, FileSystem = fs },
            new ColumnFamilyOptions { EnableBlobFiles = true, MinBlobSize = 16 });

    /// <summary>Hash exchange of a snapshot delta: which of the consumer's files would be kept instead of transferred.</summary>
    private static async Task<List<string>> ReusedFilesAsync(MemoryFileSystem sourceFs, MemoryFileSystem consumerFs)
    {
        List<ReplicationFileInfo> candidates = ReplicationDelta.ScanImmutableFiles(consumerFs, "/db");
        var sourceSignatures = new Dictionary<string, string>();
        foreach (ReplicationFileInfo c in candidates)
        {
            string? signature = await ReplicationDelta.TryHashCandidateAsync(sourceFs, "/db", c.FileName, c.Size, CancellationToken.None);
            if (signature != null) sourceSignatures[c.FileName] = signature;
        }
        List<ReplicationFileInfo> verified = await ReplicationDelta.VerifyCandidatesAsync(consumerFs, "/db", candidates, sourceSignatures, CancellationToken.None);
        var names = new List<string>();
        foreach (ReplicationFileInfo f in verified) names.Add(f.FileName);
        return names;
    }

    /// <summary>
    /// A snapshot delta kept a consumer's file when name, size and a hash of the length plus the last 1 MiB agreed with
    /// the source's. That identifies an SST - its tail holds the properties block with the DB session id and the file
    /// number of the one creation event - but not a blob file, which carries no identity at all: header, records (key,
    /// value, CRCs - no sequence number), footer. Two nodes that descend from one checkpoint allocate the same file
    /// numbers; when their histories differ in one value of the same length (a deposed leader's unreplicated write,
    /// retried with another value on the new leader) followed by more than 1 MiB of identical writes, their blob files
    /// have the same name, size and tail. The rebuilt node kept its own file under the source's MANIFEST and served
    /// its old value from then on, with every checksum intact. Blob files are now compared by a hash of the whole file.
    /// Failed before: the blob file was reused.
    /// </summary>
    [TestMethod]
    public async Task BlobFileOfAnotherHistoryWithTheSameNameSizeAndTailIsNotReused()
    {
        MemoryFileSystem leaderFs = new(), deposedFs = new();
        byte[] filler = new byte[64 * 1024];
        new Random(11).NextBytes(filler);
        foreach ((MemoryFileSystem fs, string firstValue) in new[] { (leaderFs, "value-written-by-new-leader"), (deposedFs, "value-of-the-deposed-leader") })
        {
            await using RocksDatabase db = await OpenBlobDbAsync(fs);
            db.Put(U("a"), U(firstValue)); // sorts first: the flush writes it at the start of the blob file
            for (int i = 0; i < 24; i++) db.Put(U("filler" + i.ToString("D2", System.Globalization.CultureInfo.InvariantCulture)), filler);
            await db.FlushAsync();
        }

        // The situation: same blob file name, same size, same signature over the last 1 MiB - different content.
        List<ReplicationFileInfo> leaderFiles = ReplicationDelta.ScanImmutableFiles(leaderFs, "/db").FindAll(static f => f.FileName.EndsWith(".blob", StringComparison.Ordinal));
        List<ReplicationFileInfo> deposedFiles = ReplicationDelta.ScanImmutableFiles(deposedFs, "/db").FindAll(static f => f.FileName.EndsWith(".blob", StringComparison.Ordinal));
        Assert.AreEqual(1, leaderFiles.Count);
        Assert.AreEqual(1, deposedFiles.Count);
        string blob = leaderFiles[0].FileName;
        Assert.AreEqual(blob, deposedFiles[0].FileName, "both histories allocate the same file number");
        Assert.AreEqual(leaderFiles[0].Size, deposedFiles[0].Size);
        Assert.IsTrue(leaderFiles[0].Size > 1024 * 1024 + 4096);
        Assert.AreEqual(
            await ReplicationDelta.ComputeFileSignatureAsync(leaderFs, "/db/" + blob, CancellationToken.None),
            await ReplicationDelta.ComputeFileSignatureAsync(deposedFs, "/db/" + blob, CancellationToken.None),
            "the tail signature cannot tell the two files apart");
        CollectionAssert.AreNotEqual(leaderFs.ReadAllBytes("/db/" + blob), deposedFs.ReadAllBytes("/db/" + blob));

        List<string> reused = await ReusedFilesAsync(leaderFs, deposedFs);
        Assert.IsFalse(reused.Contains(blob), "a blob file with another history's content was kept for the restored database");
        // SSTs of the two databases are told apart by their tail (session id), as before.
        Assert.AreEqual(0, reused.Count, string.Join(",", reused));
    }

    /// <summary>
    /// The delta still reuses what is byte-identical: a consumer restored from the source's checkpoint keeps every
    /// table and blob file of it, and nothing the source wrote since.
    /// </summary>
    [TestMethod]
    public async Task FilesCopiedFromTheSourceAreStillReused()
    {
        MemoryFileSystem sourceFs = new(), consumerFs = new();
        byte[] filler = new byte[64 * 1024];
        new Random(12).NextBytes(filler);
        await using RocksDatabase db = await OpenBlobDbAsync(sourceFs);
        for (int i = 0; i < 24; i++) db.Put(U("a" + i), filler);
        await db.FlushAsync();
        using (ReplicationSession session = await new ReplicationSource(db).GetInitialStateAsync("/checkpoint", CancellationToken.None))
        {
            await SnapshotReceiver.ReceiveAsync(session.StreamSnapshotAsync([], 256 * 1024, CancellationToken.None), consumerFs, "/db", [], CancellationToken.None);
        }
        for (int i = 0; i < 24; i++) db.Put(U("b" + i), filler);
        await db.FlushAsync();

        List<string> reused = await ReusedFilesAsync(sourceFs, consumerFs);
        List<ReplicationFileInfo> consumerFiles = ReplicationDelta.ScanImmutableFiles(consumerFs, "/db");
        Assert.IsTrue(consumerFiles.Exists(static f => f.FileName.EndsWith(".blob", StringComparison.Ordinal)));
        Assert.IsTrue(consumerFiles.Exists(static f => f.FileName.EndsWith(".sst", StringComparison.Ordinal)));
        Assert.AreEqual(consumerFiles.Count, reused.Count, "every file the consumer got from the source is reusable");
    }

    /// <summary>
    /// One epoch, one history. A replica force-promoted while the primary restarts out of its reach took "promised + 1"
    /// - and so did the restarted primary (the new epoch a restarted primary starts was added earlier in this audit),
    /// and a primary resuming after an aborted handoff. Two primaries in one epoch with different logs: once one of
    /// them is demoted, the attach check (same sequence number, same epoch) takes its log for a prefix of the other's.
    /// The epochs a node starts on its own now have a parity per node. Failed before: both nodes were in epoch 2.
    /// </summary>
    [TestMethod]
    public async Task ForcePromotedReplicaAndRestartedPrimaryNeverShareAnEpoch()
    {
        await using PairHarness h = MemoryPair(out MemoryFileSystem fsA, out _);
        await WriteBaseAndReplicatedTailAsync(h);
        h.Network.SetPartitioned("A", "B", true);
        long forced = await h["B"].ForcePromoteAsync(CancellationToken.None);
        await h.PowerLossAsync("A", fsA);
        await h.StartAsync("A");
        Assert.AreEqual(ReplicationRole.Primary, h["A"].Node.Role);
        Assert.AreEqual(ReplicationRole.Primary, h["B"].Node.Role);
        Assert.AreNotEqual(forced, h["A"].Node.Epoch, "two primaries with different logs in the same epoch");
        // However often either starts another epoch on its own, they never meet.
        for (int i = 0; i < 3; i++)
            Assert.AreNotEqual(forced & 1, h["A"].Node.StartNewEpochAfterRestart() & 1, "epochs the two nodes start on their own must differ in parity");
    }
}
