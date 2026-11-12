using RocksDb.Managed.Cluster;
using RocksDb.Managed.Replication.Switchover;

namespace RocksDb.Managed.Replication.Tests;

/// <summary>
/// Regression tests of the correctness audit of the replication / switchover / Raft layer ("Audit #4"): each test
/// failed before its fix.
/// </summary>
[TestClass]
public sealed class Audit4ReplicationTests
{
    /// <summary>Forwards to an inner file system; <see cref="BeforeCreate"/> / <see cref="BeforeDelete"/> may throw to inject a fault.</summary>
    private sealed class HookedFileSystem(IFileSystem inner) : IFileSystem
    {
        public Action<string>? BeforeCreate { get; set; }
        public Action<string>? BeforeDelete { get; set; }

        public IWritableFile CreateWritable(string path, bool truncate = true)
        {
            BeforeCreate?.Invoke(path);
            return inner.CreateWritable(path, truncate);
        }

        public IRandomAccessFile OpenRandomAccess(string path) => inner.OpenRandomAccess(path);
        public ISequentialFile OpenSequential(string path) => inner.OpenSequential(path);
        public bool FileExists(string path) => inner.FileExists(path);
        public long GetFileSize(string path) => inner.GetFileSize(path);

        public void DeleteFile(string path)
        {
            BeforeDelete?.Invoke(path);
            inner.DeleteFile(path);
        }

        public void RenameFile(string source, string target) => inner.RenameFile(source, target);
        public void CreateDirectoryIfMissing(string path) => inner.CreateDirectoryIfMissing(path);
        public IEnumerable<string> GetChildren(string directory) => inner.GetChildren(directory);
        public IDisposable LockFile(string path) => inner.LockFile(path);
        public void SyncDirectory(string path) => inner.SyncDirectory(path);
        public bool DirectoryExists(string path) => inner.DirectoryExists(path);
        public void DeleteDirectory(string path) => inner.DeleteDirectory(path);
    }

    private static void FailStateWrites(HookedFileSystem fs) =>
        fs.BeforeCreate = static path => throw new RocksDbException(StatusCode.IOError, "IO error: injected failure writing " + path);

    private static StatusCode CodeOf(Action action)
    {
        try
        {
            action();
        }
        catch (RocksDbException e)
        {
            return e.Code;
        }
        return StatusCode.Ok;
    }

    private static void Create(IFileSystem fs, string path)
    {
        using IWritableFile f = fs.CreateWritable(path, truncate: true);
        f.Append(U("x"));
    }

    /// <summary>
    /// Raft: votedFor and currentTerm must be on stable storage before the node answers. The state changed in memory
    /// before it was written, so after a failed write the candidate's retry found "already voted for you" in memory and
    /// the vote was granted without ever reaching the disk; after a restart the node could vote again in the same term.
    /// </summary>
    [TestMethod]
    public void RaftVoteAndTermAreNotHeldInMemoryWhenTheyCouldNotBePersisted()
    {
        var fs = new HookedFileSystem(new MemoryFileSystem());
        var state = new RaftPersistentState(fs, "/n/raft");
        state.UpdateTermAndVote(5, null);

        FailStateWrites(fs);
        Assert.AreEqual(StatusCode.IOError, CodeOf(() => state.UpdateTermAndVote(5, "a")));
        Assert.IsNull(state.VotedFor, "a vote that is not durable must not be remembered as cast");
        Assert.AreEqual(StatusCode.IOError, CodeOf(() => state.UpdateTermAndVote(6, "b")));
        Assert.AreEqual(5, state.CurrentTerm);
        Assert.IsNull(state.VotedFor);
        Assert.AreEqual(StatusCode.IOError, CodeOf(() => state.SetMode(ClusterMode.Cluster)));
        Assert.AreEqual(ClusterMode.Bootstrap, state.Mode);

        // The retry persists (it is not skipped as "nothing to change"), and a restart sees the vote.
        fs.BeforeCreate = null;
        state.UpdateTermAndVote(5, "a");
        var restarted = new RaftPersistentState(fs, "/n/raft");
        Assert.AreEqual(5, restarted.CurrentTerm);
        Assert.AreEqual("a", restarted.VotedFor);
    }

    /// <summary>
    /// Switchover: the promised epoch is the pair's equivalent of a vote. A promise that failed to persist stayed in
    /// memory, so a RejectEpoch reported it as kept (the old primary then resumes) while a restart forgot it and a late
    /// promote request for that epoch could still be accepted. Same for the role itself.
    /// </summary>
    [TestMethod]
    public void RolePromiseAndRoleAreNotHeldInMemoryWhenTheyCouldNotBePersisted()
    {
        var fs = new HookedFileSystem(new MemoryFileSystem());
        var state = new ReplicationRoleState(fs, "/n/role");
        state.StartEpoch(1, 1);
        state.Set(1, ReplicationRole.Primary, "n");

        FailStateWrites(fs);
        Assert.AreEqual(StatusCode.IOError, CodeOf(() => state.Promise(7)));
        Assert.AreEqual(1, state.PromisedEpoch, "a promise that is not durable must not be reported as made");
        Assert.AreEqual(StatusCode.IOError, CodeOf(() => state.Set(1, ReplicationRole.HandingOff, "n", "m")));
        Assert.AreEqual(ReplicationRole.Primary, state.Role);
        Assert.IsNull(state.HandoffTarget);
        Assert.AreEqual(StatusCode.IOError, CodeOf(() => state.BecomePrimary(10, 3, "n")));
        Assert.AreEqual(1, state.Epoch);
        Assert.AreEqual(1, state.GetEpochForSeq(20));

        fs.BeforeCreate = null;
        Assert.IsTrue(state.Promise(7));
        var restarted = new ReplicationRoleState(fs, "/n/role");
        Assert.AreEqual(7, restarted.PromisedEpoch);
        Assert.AreEqual(ReplicationRole.Primary, restarted.Role);
    }

    /// <summary>
    /// A snapshot restore first clears the database directory. The hosts decide "this node has no database, pull a
    /// snapshot before taking part" by CURRENT being absent, so CURRENT must be the first file to go: files were deleted
    /// in listing order, and a node killed after its WAL was deleted but before CURRENT restarted on a database that
    /// opened with a shorter log - a Raft node would then vote without entries it had acknowledged.
    /// </summary>
    [TestMethod]
    public void RestoreDeletesCurrentBeforeAnyOtherFile()
    {
        var fs = new HookedFileSystem(new MemoryFileSystem());
        fs.CreateDirectoryIfMissing("/db");
        fs.CreateDirectoryIfMissing("/db/raft");
        foreach (string name in new[] { "000007.log", "000009.sst", "CURRENT", "MANIFEST-000005", "raft/raft.state" }) Create(fs, "/db/" + name);

        // The process dies at the first file that is not CURRENT.
        fs.BeforeDelete = static path =>
        {
            if (!path.EndsWith("CURRENT", StringComparison.Ordinal)) throw new RocksDbException(StatusCode.IOError, "IO error: killed while deleting " + path);
        };
        Assert.AreEqual(StatusCode.IOError, CodeOf(() => ReplicationDelta.PrepareForRestore(fs, "/db", ["000009.sst"], ["raft"])));
        Assert.IsFalse(fs.FileExists("/db/CURRENT"), "an interrupted restore must not leave a CURRENT behind");
        Assert.IsTrue(fs.FileExists("/db/000007.log"));

        // The retry completes: reused and preserved entries stay, the rest is gone.
        fs.BeforeDelete = null;
        ReplicationDelta.PrepareForRestore(fs, "/db", ["000009.sst"], ["raft"]);
        Assert.IsTrue(fs.FileExists("/db/000009.sst"));
        Assert.IsTrue(fs.FileExists("/db/raft/raft.state"));
        Assert.IsFalse(fs.FileExists("/db/000007.log"));
        Assert.IsFalse(fs.FileExists("/db/MANIFEST-000005"));
    }

    /// <summary>
    /// A replica whose log is longer than the source's (it was written to, or the source was restored from an older
    /// backup) asks for a sequence number the source has not reached. The stream used to wait until the source's log
    /// grew to that point and then sent its entries, which the replica applied at "the right" sequence numbers on top of
    /// unrelated history - silent divergence in plain replication, which has no attach check. It now ends with a resync.
    /// </summary>
    [TestMethod]
    public async Task WalStreamRefusesAFollowerThatIsAheadOfTheSource()
    {
        using var dir = new TempDir("audit4-ahead");
        await using RocksDatabase source = await RocksDatabase.OpenAsync(dir.Sub("source"), DbOpts(null));
        await using RocksDatabase replica = await RocksDatabase.OpenAsync(dir.Sub("replica"), DbOpts(null));
        source.Put(U("s1"), U("1"));
        for (int i = 0; i < 5; i++) replica.Put(U("r" + i), U("x"));
        ulong start = replica.GetLatestSequenceNumber() + 1;
        Assert.IsTrue(start > source.GetLatestSequenceNumber() + 1);

        using var cts = new CancellationTokenSource(TestUtil.Timeout);
        var primary = new Hosting.ReplicationPrimary(source, dir.Sub("checkpoints"), null);
        await using IAsyncEnumerator<Hosting.WalStreamMessage> stream = primary.StreamUpdatesAsync(start, cts.Token).GetAsyncEnumerator(cts.Token);
        Task<bool> next = stream.MoveNextAsync().AsTask();
        // The source's log grows past the requested sequence number.
        for (int i = 0; i < 8; i++) source.Put(U("s-later" + i), U("y"));
        Assert.IsTrue(await next);
        Assert.IsTrue(stream.Current.ResyncRequired, "streamed sequence " + stream.Current.FirstSequence + " to a replica with a different history");
        Assert.IsFalse(await stream.MoveNextAsync());
    }

    /// <summary>
    /// Engine contract after Audit #4: listing the WAL files throws Corruption for a WAL whose first record cannot be
    /// read. A tail that is already open follows a WAL rotation by listing the files and takes a failed listing for
    /// "nothing new", so the tailer kept answering "no chunk" (and its wait kept completing) while the database moved on:
    /// a replication stream that silently never advances. The tailer now surfaces the error.
    /// </summary>
    [TestMethod]
    public async Task WalTailerSurfacesAWalListingErrorInsteadOfStalling()
    {
        using var dir = new TempDir("audit4-tail");
        string path = dir.Sub("db");
        await using RocksDatabase db = await RocksDatabase.OpenAsync(path, DbOpts(null));
        db.Put(U("a"), U("1"));
        using WalTailer tailer = new ReplicationSource(db).TailWal(1);
        Assert.IsTrue(tailer.TryReadChunk(out WalChunk first));
        Assert.AreEqual(1UL, first.FirstSequenceNumber);
        Assert.IsFalse(tailer.TryReadChunk(out _));

        // An old WAL with a damaged first record (a full record of 20 bytes with a wrong checksum), kept on disk.
        db.DisableFileDeletions();
        byte[] damaged = new byte[64];
        damaged[4] = 20;
        damaged[6] = 1;
        string damagedWal = Path.Combine(path, "000001.log");
        Assert.IsFalse(File.Exists(damagedWal));
        File.WriteAllBytes(damagedWal, damaged);
        Assert.AreEqual(StatusCode.Corruption, CodeOf(() => db.GetSortedWalFiles()), "precondition: the listing reports the damaged WAL");

        // Rotate the WAL and write: the new batch lives in a WAL the open tail has to discover.
        ulong walBefore = db.CurrentWalNumber;
        await db.FlushAsync();
        db.Put(U("b"), U("2"));
        Assert.AreNotEqual(walBefore, db.CurrentWalNumber);
        Assert.IsTrue(tailer.HasUpdates);

        Assert.AreEqual(StatusCode.Corruption, CodeOf(() => tailer.TryReadChunk(out _)), "the tail must not report 'nothing new' forever");
    }

    // ---------------------------------------------------------------------------------------------------
    // Second pass: the suspicions the first review left open.

    private static readonly string[] s_nodes = ["n1", "n2", "n3"];

    private static async Task<ClusterHarness> StartClusterAsync()
    {
        var c = new ClusterHarness(3, null);
        Hosting.ClusterNodeHost n1 = await c.StartAsync("n1");
        for (int i = 0; i < 20; i++) Assert.IsTrue(await n1.TryWriteAsLeaderAsync(U(K(i)), U(V(i, 0)), CancellationToken.None));
        await c.StartAsync("n2");
        await c.StartAsync("n3");
        await WaitUntilAsync(() => c.Statuses().TrueForAll(s => s.Mode == ClusterMode.Cluster), "every node in Cluster mode\n" + c.LogText);
        await c.AssertConvergedAsync(n1);
        return c;
    }

    private sealed class NoTransport : IPeerTransport
    {
        public Task<RequestVoteResult?> SendRequestVoteAsync(ClusterMember peer, RequestVotePayload request, CancellationToken cancellationToken) =>
            Task.FromResult<RequestVoteResult?>(null);

        public Task<AppendEntriesResult?> SendHeartbeatAsync(ClusterMember peer, HeartbeatPayload payload, CancellationToken cancellationToken) =>
            Task.FromResult<AppendEntriesResult?>(null);
    }

    /// <summary>
    /// Raft: a progress report is a statement about one leadership's log. Reports carried no term, so one that reached a
    /// leader late - addressed to an earlier leadership, after which the follower may have discarded those entries in a
    /// resync - was counted like a fresh one: with the leader's own copy it made a quorum of 3 and committed an entry
    /// that only the leader held. A report now names the leader term it is for and is ignored in any other term.
    /// </summary>
    [TestMethod]
    public async Task RaftProgressReportOfAnotherLeadershipIsNotCounted()
    {
        await using ClusterHarness c = await StartClusterAsync();
        long firstTerm = c["n1"].Node.CurrentTerm;
        await c.StopAsync("n1");
        Hosting.ClusterNodeHost leader = await c.WaitForLeaderAsync("n1");
        Assert.IsTrue(leader.Node.CurrentTerm > firstTerm);
        string follower = leader.NodeId == "n2" ? "n3" : "n2";
        await c.AssertConvergedAsync(leader);

        // The follower is cut off: what the leader writes now exists on the leader only.
        c.Network.SetPartitioned(leader.NodeId, follower, true);
        ulong committed = leader.Node.CommitSeq;
        Assert.IsTrue(await leader.TryWriteAsLeaderAsync(U("leader-only"), U("x"), CancellationToken.None));
        ulong seq = leader.Db.GetLatestSequenceNumber();

        // The late report: same follower, a position the leader's log has reached, but for the earlier term.
        Assert.IsFalse(leader.HandleReportProgress(follower, seq, firstTerm), "a report for term " + firstTerm + " was accepted in term " + leader.Node.CurrentTerm);
        Assert.AreEqual(committed, leader.Node.CommitSeq, "an entry held by one node of three was committed");
        // A position past the leader's own log cannot come from a follower of this leadership either.
        Assert.IsFalse(leader.HandleReportProgress(follower, seq + 100, leader.Node.CurrentTerm));
        Assert.AreEqual(committed, leader.Node.CommitSeq);
        // The same report for the current term is what a real follower sends.
        Assert.IsTrue(leader.HandleReportProgress(follower, seq, leader.Node.CurrentTerm));
        Assert.AreEqual(seq, leader.Node.CommitSeq);
    }

    /// <summary>
    /// A WAL the source cannot read (Corruption) used to surface on the follower as a failed stream that was retried
    /// every RetryDelay forever, with a log line only. The stream now ends with a resync request - a snapshot does not
    /// need the damaged WAL - and the source host counts the fault.
    /// </summary>
    [TestMethod]
    public async Task CorruptSourceWalEndsTheStreamWithAResyncAndIsCountedByTheHost()
    {
        await using var h = new PairHarness();
        Hosting.PairNodeHost a = await h.StartAsync("A");
        using (var batch = new WriteBatch())
        {
            batch.Put(U("a"), U("1"));
            Assert.AreEqual(Hosting.PairWriteStatus.Applied, (await a.WriteAsync(batch, TimeSpan.FromSeconds(1), CancellationToken.None)).Status);
        }
        using var cts = new CancellationTokenSource(TestUtil.Timeout);
        await using IAsyncEnumerator<Hosting.WalStreamMessage> stream = a.StreamUpdatesAsync(1, cts.Token).GetAsyncEnumerator(cts.Token);
        Assert.IsTrue(await stream.MoveNextAsync());
        Assert.IsFalse(stream.Current.ResyncRequired);
        Assert.AreEqual(0, a.WalStreamFaults);

        // An old WAL with a damaged first record, then a WAL rotation: the open tail has to list the WAL files.
        a.Db.DisableFileDeletions();
        byte[] damaged = new byte[64];
        damaged[4] = 20;
        damaged[6] = 1;
        string damagedWal = Path.Combine(h.DbPath("A"), "000001.log");
        Assert.IsFalse(File.Exists(damagedWal));
        File.WriteAllBytes(damagedWal, damaged);
        Assert.AreEqual(StatusCode.Corruption, CodeOf(() => a.Db.GetSortedWalFiles()), "precondition: the listing reports the damaged WAL");
        await a.Db.FlushAsync();
        a.Db.Put(U("b"), U("2"));

        Assert.IsTrue(await stream.MoveNextAsync(), "the stream must end with a message, not just stop");
        Assert.IsTrue(stream.Current.ResyncRequired);
        StringAssert.Contains(stream.Current.Reason, "cannot read its WAL");
        Assert.IsFalse(await stream.MoveNextAsync());
        Assert.AreEqual(1, a.WalStreamFaults);
        Assert.IsNotNull(a.LastWalStreamFault);
    }

    /// <summary>
    /// A resync that starts on a node which is no longer a replica (promoted between the trigger and the rebuild) failed
    /// in DetachDatabase with the "resyncing" flag left set: the node answered "no status" from then on, so the other
    /// node could never resolve a handoff with it.
    /// </summary>
    [TestMethod]
    public async Task PairResyncOnAPromotedNodeDoesNotLeaveItResyncingForever()
    {
        await using var h = new PairHarness();
        await h.StartAsync("A");
        Hosting.PairNodeHost b = await h.StartAsync("B");
        await WaitUntilAsync(() => b.GetStatus().AttachedFromSeq > 0, "B to follow A\n" + h.LogText);
        await h.StopAsync("A");
        await b.ForcePromoteAsync(CancellationToken.None);
        Assert.AreEqual(ReplicationRole.Primary, b.Node.Role);

        b.TriggerResync("test");
        await b.WaitForResyncAsync().WaitAsync(TestUtil.Timeout);
        Assert.IsFalse(b.IsResyncing, h.LogText);
        Assert.IsNotNull(b.HandleGetStatus());
        Assert.AreEqual(0, b.Resyncs);
    }

    /// <summary>
    /// A candidate that wins but cannot write the first entry of its term took the leader role before the write: the
    /// exception left a leader without a heartbeat loop whose own election timer ignores it - no leader and no further
    /// election on that node. It now gives the election up and keeps campaigning.
    /// </summary>
    [TestMethod]
    public async Task RaftCandidateThatCannotWriteItsFirstEntryDoesNotBecomeAZombieLeader()
    {
        var dbFs = new MemoryFileSystem();
        await using RocksDatabase db = await RocksDatabase.OpenAsync("/db", DbOpts(dbFs));
        var state = new RaftPersistentState(new MemoryFileSystem(), "/raft");
        state.SetMode(ClusterMode.Cluster);
        var config = new RaftConfig("n1", "mem://n1", []) { ElectionTimeoutMinMs = 20, ElectionTimeoutMaxMs = 40, HeartbeatIntervalMs = 5 };
        int failures = 0;
        await using (var node = new RaftClusterNode(config, state, db, new NoTransport()))
        {
            node.LeadershipFailed += (_, _) => Interlocked.Increment(ref failures);
            dbFs.SetFilesystemActive(false);
            node.Start();
            // Single-member cluster: every election is won at once. A zombie leader stays in its first term.
            await WaitUntilAsync(() => node.CurrentTerm >= 3, "the node to keep campaigning (role " + node.Role + ", term " + node.CurrentTerm + ")");
            Assert.AreNotEqual(RaftRole.Leader, node.Role);
            Assert.IsTrue(Volatile.Read(ref failures) >= 2);
        }
        dbFs.SetFilesystemActive(true);
    }

    /// <summary>A node told to rebuild its log steps down first: a resyncing leader kept the others' election timers quiet while refusing every stream.</summary>
    [TestMethod]
    public async Task RaftLeaderStepsDownWhenItsLogIsRebuilt()
    {
        await using RocksDatabase db = await RocksDatabase.OpenAsync("/db", DbOpts(new MemoryFileSystem()));
        var state = new RaftPersistentState(new MemoryFileSystem(), "/raft");
        var config = new RaftConfig("n1", "mem://n1", [new ClusterMember("n2", "mem://n2")]) { BootstrapPrimary = true };
        await using var node = new RaftClusterNode(config, state, db, new NoTransport());
        Assert.AreEqual(RaftRole.Leader, node.Role);
        node.BeginResync();
        Assert.AreEqual(RaftRole.Follower, node.Role);
        Assert.IsNull(node.CurrentLeaderId);
    }

    /// <summary>
    /// The term map follows the persist-before-assign rule of the term and the vote: a range kept in memory after a
    /// failed write made the retry a no-op ("already recorded"), so it never reached the disk and was gone after a
    /// restart - the node then mislabelled the entries of that term.
    /// </summary>
    [TestMethod]
    public void RaftTermMapIsNotChangedInMemoryWhenItCouldNotBePersisted()
    {
        var fs = new HookedFileSystem(new MemoryFileSystem());
        var state = new RaftPersistentState(fs, "/raft");
        state.RecordTermRange(1, 1);

        FailStateWrites(fs);
        Assert.AreEqual(StatusCode.IOError, CodeOf(() => state.RecordTermRange(10, 2)));
        Assert.AreEqual(1L, state.Terms.GetTermForSeq(10));
        Assert.AreEqual(StatusCode.IOError, CodeOf(() => state.AdoptTermsAfter(5, RaftLogTerms.Deserialize("10,3\n"))));
        Assert.AreEqual(1L, state.Terms.GetTermForSeq(10));
        Assert.AreEqual(StatusCode.IOError, CodeOf(() => state.ReplaceTerms(RaftLogTerms.Deserialize("1,7\n"))));
        Assert.AreEqual(1L, state.Terms.GetTermForSeq(1));

        fs.BeforeCreate = null;
        state.RecordTermRange(10, 2);
        Assert.AreEqual(2L, state.Terms.GetTermForSeq(10));
        Assert.AreEqual(2L, new RaftPersistentState(fs, "/raft").Terms.GetTermForSeq(10), "the retried range must be on disk");
    }

    /// <summary>The same rule for the switchover epoch map (<see cref="ReplicationRoleState.MergeEpochs"/>).</summary>
    [TestMethod]
    public void SwitchoverEpochMapIsNotChangedInMemoryWhenItCouldNotBePersisted()
    {
        var fs = new HookedFileSystem(new MemoryFileSystem());
        var state = new ReplicationRoleState(fs, "/role");
        state.MergeEpochs("1,1\n");

        FailStateWrites(fs);
        Assert.AreEqual(StatusCode.IOError, CodeOf(() => state.MergeEpochs("1,1\n10,2\n")));
        Assert.AreEqual(1L, state.GetEpochForSeq(10));

        fs.BeforeCreate = null;
        state.MergeEpochs("1,1\n10,2\n");
        Assert.AreEqual(2L, state.GetEpochForSeq(10));
        Assert.AreEqual(2L, new ReplicationRoleState(fs, "/role").GetEpochForSeq(10), "the retried range must be on disk");
    }

    /// <summary>
    /// The snapshot receiver checked that chunks were contiguous and that the number of files matched, not that each file
    /// reached its announced size: a file whose tail never arrived was restored truncated (and CURRENT, sent last, made
    /// the directory open as a database).
    /// </summary>
    [TestMethod]
    public async Task SnapshotReceiverRejectsAFileShorterOrLongerThanAnnounced()
    {
        CancellationToken ct = CancellationToken.None;
        var fs = new MemoryFileSystem();
        await using (var truncated = new SnapshotReceiver(fs, "/short", []))
        {
            await truncated.WriteAsync(SnapshotChunk.Plan(5, []), ct);
            await truncated.WriteAsync(SnapshotChunk.FileData("000007.sst", 10, 0, new byte[4]), ct);
            InvalidOperationException e = await Assert.ThrowsExactlyAsync<InvalidOperationException>(async () => await truncated.WriteAsync(SnapshotChunk.Complete(1), ct));
            StringAssert.Contains(e.Message, "announced 10");
        }
        await using (var next = new SnapshotReceiver(fs, "/next", []))
        {
            await next.WriteAsync(SnapshotChunk.Plan(5, []), ct);
            await next.WriteAsync(SnapshotChunk.FileData("000007.sst", 10, 0, new byte[4]), ct);
            // The next file starts although the previous one is incomplete.
            await Assert.ThrowsExactlyAsync<InvalidOperationException>(async () => await next.WriteAsync(SnapshotChunk.FileData("000008.sst", 3, 0, new byte[3]), ct));
        }
        await using (var overrun = new SnapshotReceiver(fs, "/long", []))
        {
            await overrun.WriteAsync(SnapshotChunk.Plan(5, []), ct);
            await overrun.WriteAsync(SnapshotChunk.FileData("000007.sst", 10, 0, new byte[4]), ct);
            await Assert.ThrowsExactlyAsync<InvalidOperationException>(async () => await overrun.WriteAsync(SnapshotChunk.FileData("000007.sst", 10, 4, new byte[8]), ct));
        }
        await using (var exact = new SnapshotReceiver(fs, "/exact", []))
        {
            await exact.WriteAsync(SnapshotChunk.Plan(5, []), ct);
            await exact.WriteAsync(SnapshotChunk.FileData("000007.sst", 10, 0, new byte[4]), ct);
            await exact.WriteAsync(SnapshotChunk.FileData("000007.sst", 10, 4, new byte[6]), ct);
            await exact.WriteAsync(SnapshotChunk.FileData("CURRENT", 0, 0, ReadOnlyMemory<byte>.Empty), ct);
            await exact.WriteAsync(SnapshotChunk.Complete(2), ct);
            Assert.AreEqual(2, (await exact.CompleteAsync(ct)).TransferredFiles);
        }
    }

    /// <summary>
    /// Disposing a host waited without bound for RPC handlers that use the database. A WAL stream whose consumer stopped
    /// reading (a stalled gRPC client: the handler sits in a blocked write) never leaves, so DisposeAsync never returned.
    /// </summary>
    [TestMethod]
    public async Task HostDisposeIsNotBlockedForeverByAStalledStreamConsumer()
    {
        await using var h = new PairHarness { DisposeDrainTimeout = TimeSpan.FromMilliseconds(200) };
        Hosting.PairNodeHost a = await h.StartAsync("A");
        using (var batch = new WriteBatch())
        {
            batch.Put(U("a"), U("1"));
            await a.WriteAsync(batch, TimeSpan.FromSeconds(1), CancellationToken.None);
        }
        // The handler of a stream: it took one chunk and its consumer never asks for the next.
        IAsyncEnumerator<Hosting.WalStreamMessage> stalled = a.StreamUpdatesAsync(1, CancellationToken.None).GetAsyncEnumerator(CancellationToken.None);
        Assert.IsTrue(await stalled.MoveNextAsync());

        await h.StopAsync("A").WaitAsync(TestUtil.Timeout);
        StringAssert.Contains(h.LogText, "an RPC handler still uses it");
        await stalled.DisposeAsync();
    }

    /// <summary>
    /// The crash point between a snapshot restore and the replacement of the term map: the node restarts with the
    /// leader's log and its own older term map (here: without the range of the term in which the leader was elected).
    /// It under-labels entries it never acknowledged, which costs no safety (see the review notes), and the first
    /// consistency check against the leader sees the mismatch and rebuilds the node again.
    /// </summary>
    [TestMethod]
    public async Task NodeRestartedBetweenSnapshotRestoreAndTermMapReplacementRebuildsItself()
    {
        await using ClusterHarness c = await StartClusterAsync();
        await c.StopAsync("n1");
        Hosting.ClusterNodeHost leader = await c.WaitForLeaderAsync("n1");
        string follower = leader.NodeId == "n2" ? "n3" : "n2";
        Assert.IsTrue(await leader.WriteCommittedAsync(U("second-term"), U("x"), TimeSpan.FromSeconds(10), CancellationToken.None));
        await c.AssertConvergedAsync(leader);
        ulong latest = leader.Db.GetLatestSequenceNumber();
        Assert.IsTrue(leader.Node.GetTermForSeq(latest) > 1);

        await c.StopAsync(follower);
        File.WriteAllText(Path.Combine(c.DbPath(follower), "raft", "raft.terms"), "1,1\n");
        Hosting.ClusterNodeHost restarted = await c.StartAsync(follower);
        Assert.AreEqual(1L, restarted.Node.GetTermForSeq(latest), "precondition: the stale term map was loaded");

        await WaitUntilAsync(() => restarted.Resyncs >= 1 && !restarted.IsResyncing, "the node to rebuild itself\n" + c.LogText);
        await c.AssertConvergedAsync(leader);
        ulong tip = leader.Db.GetLatestSequenceNumber();
        Assert.AreEqual(leader.Node.GetTermForSeq(tip), restarted.Node.GetTermForSeq(tip));
        Assert.AreEqual(leader.Node.GetTermForSeq(latest), restarted.Node.GetTermForSeq(latest));
    }

    /// <summary>
    /// A resync took its snapshot from the first member that reported itself as leader. A deposed leader that has not
    /// heard of its successor (cut off from the majority, but reachable by the resyncing node) still says so, and its log
    /// lacks what the successor committed - entries the resyncing node had already acknowledged. The node now asks every
    /// member and follows the answer of the highest term.
    /// </summary>
    [TestMethod]
    public async Task ResyncDoesNotTakeItsSnapshotFromADeposedLeader()
    {
        await using ClusterHarness c = await StartClusterAsync();
        long firstTerm = c["n1"].Node.CurrentTerm;
        c.Network.Isolate("n1", s_nodes);
        Hosting.ClusterNodeHost leader = await c.WaitForLeaderAmongAsync(["n2", "n3"], firstTerm + 1);
        Hosting.ClusterNodeHost follower = c[leader.NodeId == "n2" ? "n3" : "n2"];
        Assert.IsTrue(await leader.WriteCommittedAsync(U("second-term"), U("x"), TimeSpan.FromSeconds(10), CancellationToken.None));
        await WaitUntilAsync(() => follower.Db.GetLatestSequenceNumber() == leader.Db.GetLatestSequenceNumber(), "the follower to hold the write");
        Assert.AreEqual(RaftRole.Leader, c["n1"].GetStatus().Role, "precondition: n1 still believes it leads");

        // The follower can reach n1 again (n1 still cannot reach anyone).
        c.Network.HealLink(follower.NodeId, "n1");
        follower.TriggerResync("test");
        await WaitUntilAsync(() => follower.Resyncs >= 1 && !follower.IsResyncing, "the resync\n" + c.LogText);
        Assert.IsTrue(Contents(follower.Db).ContainsKey("second-term"), "the node replaced its log with a deposed leader's and lost an entry it had acknowledged\n" + c.LogText);
    }

    // Third pass.

    private static async Task PutAsync(Hosting.PairNodeHost primary, string key, string value)
    {
        using var batch = new WriteBatch();
        batch.Put(U(key), U(value));
        Assert.AreEqual(Hosting.PairWriteStatus.Applied, (await primary.WriteAsync(batch, TimeSpan.FromSeconds(1), CancellationToken.None)).Status);
    }

    /// <summary>
    /// A source that cannot list its WAL files (a damaged WAL it still keeps) answers every stream that has something to
    /// send with "resync: I cannot read my WAL" - also the stream of a replica it has just rebuilt. The replica rebuilt
    /// itself again on every such answer, for as long as the source took writes. A snapshot that did not get the replica
    /// past the fault is not taken again: the replica retries the stream until the source can read its WAL.
    /// </summary>
    [TestMethod]
    public async Task ReplicaIsRebuiltOnceNotInALoopWhileTheSourceCannotReadItsWal()
    {
        await using var h = new PairHarness();
        Hosting.PairNodeHost a = await h.StartAsync("A");
        await PutAsync(a, "a", "1");
        Hosting.PairNodeHost b = await h.StartAsync("B");
        await WaitUntilAsync(() => Contents(b.Db).ContainsKey("a"), "B to follow A\n" + h.LogText);

        a.Db.DisableFileDeletions();
        byte[] damaged = new byte[64];
        damaged[4] = 20;
        damaged[6] = 1;
        string damagedWal = Path.Combine(h.DbPath("A"), "000001.log");
        File.WriteAllBytes(damagedWal, damaged);
        await a.Db.FlushAsync();
        await PutAsync(a, "b", "2");
        await WaitUntilAsync(() => b.Resyncs >= 1 && !b.IsResyncing, "the resync\n" + h.LogText);
        Assert.IsTrue(Contents(b.Db).ContainsKey("b"), "the snapshot carried what the WAL stream could not");

        // The fault is still there: the streams the rebuilt replica opens are refused the same way.
        Assert.AreEqual(StatusCode.Corruption, CodeOf(() => a.Db.GetSortedWalFiles()));
        int faults = a.WalStreamFaults;
        for (int i = 0; i < 20; i++) await PutAsync(a, "c" + i, "3");
        await WaitUntilAsync(() => a.WalStreamFaults >= faults + 4, "further refused streams\n" + h.LogText);
        Assert.AreEqual(1, b.Resyncs, h.LogText);
        Assert.IsFalse(b.IsResyncing, h.LogText);

        // Once the source can read its WAL again the replica catches up through the stream.
        File.Delete(damagedWal);
        await PutAsync(a, "d", "4");
        await WaitUntilAsync(() => !b.IsResyncing && Contents(b.Db).ContainsKey("d"), "B to follow A again\n" + h.LogText);
        Assert.IsTrue(Contents(b.Db).ContainsKey("c19"));
        Assert.AreEqual(1, b.Resyncs, h.LogText);
    }

    /// <summary>
    /// The race behind <see cref="PairResyncOnAPromotedNodeDoesNotLeaveItResyncingForever"/> itself: the node is still a
    /// replica when the resync is triggered and primary when the rebuild starts.
    /// </summary>
    [TestMethod]
    public async Task PairResyncTriggeredJustBeforeAPromotionIsAbandoned()
    {
        await using var h = new PairHarness();
        await h.StartAsync("A");
        Hosting.PairNodeHost b = await h.StartAsync("B");
        await WaitUntilAsync(() => b.GetStatus().AttachedFromSeq > 0, "B to follow A\n" + h.LogText);
        await h.StopAsync("A");

        // The host logs "resync: ..." after it decided to rebuild and before the rebuild task exists.
        Task<long>? promotion = null;
        h.OnLog = (id, line) =>
        {
            if (id == "B" && line.Contains("resync: test", StringComparison.Ordinal)) promotion = b.ForcePromoteAsync(CancellationToken.None);
        };
        b.TriggerResync("test");
        Assert.IsNotNull(promotion, h.LogText);
        await promotion.WaitAsync(TestUtil.Timeout);
        await b.WaitForResyncAsync().WaitAsync(TestUtil.Timeout);
        Assert.AreEqual(ReplicationRole.Primary, b.Node.Role);
        Assert.IsFalse(b.IsResyncing, h.LogText);
        Assert.IsNotNull(b.HandleGetStatus());
        Assert.AreEqual(0, b.Resyncs);
        StringAssert.Contains(h.LogText, "resync abandoned");
        await PutAsync(b, "after", "1");
    }

    /// <summary>
    /// The pair's version of <see cref="NodeRestartedBetweenSnapshotRestoreAndTermMapReplacementRebuildsItself"/>: a
    /// replica that died after restoring the primary's snapshot and before replacing its epoch map restarts with the
    /// primary's log and its own older map (here: none, a new replica). It labels the log's end with an older epoch than
    /// the primary does, so the attach check fails and the replica is rebuilt - it never follows on a wrong map.
    /// </summary>
    [TestMethod]
    public async Task ReplicaRestartedBetweenSnapshotRestoreAndEpochMapReplacementRebuildsItself()
    {
        await using var h = new PairHarness();
        Hosting.PairNodeHost a = await h.StartAsync("A");
        await PutAsync(a, "a", "1");
        Hosting.PairNodeHost b = await h.StartAsync("B");
        await WaitUntilAsync(() => Contents(b.Db).ContainsKey("a"), "B to follow A\n" + h.LogText);
        ulong latest = a.Db.GetLatestSequenceNumber();
        Assert.AreEqual(1L, b.Node.GetEpochForSeq(latest));

        await h.StopAsync("B");
        File.WriteAllText(Path.Combine(h.DbPath("B"), "role", "role.epochs"), "");
        b = await h.StartAsync("B");
        Assert.AreEqual(0, b.Resyncs);
        await WaitUntilAsync(() => b.Resyncs >= 1 && !b.IsResyncing, "the replica to rebuild itself\n" + h.LogText);
        Assert.AreEqual(1L, b.Node.GetEpochForSeq(latest));
        await PutAsync(a, "b", "2");
        await WaitUntilAsync(() => Contents(b.Db).ContainsKey("b"), "B to follow A again\n" + h.LogText);
        Assert.AreEqual(1, b.Resyncs, h.LogText);
    }

    /// <summary>
    /// The commit delay controller's range checks let NaN through (every comparison with NaN is false) and accepted
    /// infinite weights: the EMA and the score then became NaN and the recommended delay fell below MinDelayMs.
    /// </summary>
    [TestMethod]
    public void CommitDelayControllerRejectsNonFiniteTuning()
    {
        AdaptiveCommitDelayOptions[] invalid =
        [
            new() { EmaAlpha = double.NaN },
            new() { DelayPerLagUnitMs = double.NaN },
            new() { DelayPerLagUnitMs = double.PositiveInfinity },
            new() { LagUnit = double.NaN },
            new() { LagUnit = double.PositiveInfinity },
            new() { TrendWeight = double.NaN },
            new() { TrendWeight = double.PositiveInfinity },
            new() { BurstWeight = double.NaN },
            new() { BurstWeight = double.PositiveInfinity },
        ];
        for (int i = 0; i < invalid.Length; i++)
        {
            AdaptiveCommitDelayOptions options = invalid[i];
            Assert.ThrowsExactly<ArgumentOutOfRangeException>(() => new AdaptiveCommitDelayController(1, options), "case " + i);
        }
        var controller = new AdaptiveCommitDelayController(2, new AdaptiveCommitDelayOptions { MinDelayMs = 3, MaxDelayMs = 50, EmaAlpha = 1 });
        Assert.AreEqual(3, controller.GetRecommendedDelayMs());
        controller.ReportLag(0, 4);
        Assert.AreEqual(20, controller.GetRecommendedDelayMs());
        controller.ReportLag(1, long.MaxValue);
        Assert.AreEqual(50, controller.GetRecommendedDelayMs());
    }

    /// <summary>
    /// A forced promotion whose replica starts rebuilding its database while the promotion waits for the stream to
    /// detach dereferenced the detached database (NullReferenceException) instead of refusing.
    /// </summary>
    [TestMethod]
    public async Task ForcePromoteRefusesWhenTheDatabaseIsDetachedMeanwhile()
    {
        await using RocksDatabase db = await RocksDatabase.OpenAsync("/db", DbOpts(new MemoryFileSystem()));
        var state = new ReplicationRoleState(new MemoryFileSystem(), "/role");
        state.Set(0, ReplicationRole.Replica, "A");
        SwitchoverNode? node = null;
        bool detach = true;
        node = new SwitchoverNode("B", "A", state, db, _ =>
        {
            if (detach) node!.DetachDatabase();
            detach = false;
            return Task.CompletedTask;
        });
        await Assert.ThrowsExactlyAsync<InvalidOperationException>(async () => await node.ForcePromoteAsync(CancellationToken.None));
        Assert.AreEqual(ReplicationRole.Replica, node.Role);
        node.AttachDatabase(db);
        Assert.IsTrue(await node.ForcePromoteAsync(CancellationToken.None) > 0);
        Assert.AreEqual(ReplicationRole.Primary, node.Role);
    }
}
