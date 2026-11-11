using RocksDb.Managed.Cluster;
using RocksDb.Managed.Replication.Hosting;
using RocksDb.Managed.Replication.Switchover;

namespace RocksDb.Managed.Replication.Tests;

/// <summary>
/// Audit #4, replication compared with its original (rocksdb-sharp <c>csharp/src/Replication</c>,
/// <c>Replication/Switchover</c>, <c>Cluster</c> and the hosts of <c>Tests/{ClusterTest,SwitchoverTest,ReplicationTest}</c>):
/// behaviour of the original that the port had lost. Each test failed before its fix.
/// </summary>
[TestClass]
public sealed class Audit4ReplicationOriginalTests
{
    /// <summary>An <see cref="IClusterNodeClient"/> whose next <see cref="FailReports"/> progress reports are lost.</summary>
    private sealed class LossyReportClient(IClusterNodeClient inner) : IClusterNodeClient
    {
        public int FailReports;
        public int Failed;

        public string NodeId => inner.NodeId;
        public Task<RequestVoteResult?> RequestVoteAsync(RequestVotePayload request, CancellationToken cancellationToken) => inner.RequestVoteAsync(request, cancellationToken);
        public Task<AppendEntriesResult?> HeartbeatAsync(HeartbeatPayload payload, CancellationToken cancellationToken) => inner.HeartbeatAsync(payload, cancellationToken);
        public Task<ClusterNodeStatus?> GetStatusAsync(CancellationToken cancellationToken) => inner.GetStatusAsync(cancellationToken);

        public Task<ClusterAttachResult?> CheckLogConsistencyAsync(string followerId, ulong latestSeq, long termAtLatest, CancellationToken cancellationToken) =>
            inner.CheckLogConsistencyAsync(followerId, latestSeq, termAtLatest, cancellationToken);

        public IAsyncEnumerable<WalStreamMessage> StreamUpdatesAsync(string followerId, ulong startSeq, CancellationToken cancellationToken) =>
            inner.StreamUpdatesAsync(followerId, startSeq, cancellationToken);

        public Task ReportProgressAsync(string followerId, ulong appliedThrough, long leaderTerm, CancellationToken cancellationToken)
        {
            if (Interlocked.Decrement(ref FailReports) >= 0)
            {
                Interlocked.Increment(ref Failed);
                return Task.FromException(new IOException("progress report lost"));
            }
            return inner.ReportProgressAsync(followerId, appliedThrough, leaderTerm, cancellationToken);
        }

        public Task<IReadOnlyDictionary<string, string>> GetFileHashesAsync(IReadOnlyList<ReplicationFileInfo> candidates, CancellationToken cancellationToken) =>
            inner.GetFileHashesAsync(candidates, cancellationToken);

        public IAsyncEnumerable<SnapshotChunk> StreamSnapshotAsync(IReadOnlyList<ReplicationFileInfo> verifiedInventory, CancellationToken cancellationToken) =>
            inner.StreamSnapshotAsync(verifiedInventory, cancellationToken);

        public Task<string?> GetTermsAsync(CancellationToken cancellationToken) => inner.GetTermsAsync(cancellationToken);
    }

    /// <summary>
    /// rocksdb-sharp's hosts report follower progress on a timer (ClusterNodeHost.cs:244-264: every 200 ms, "so the
    /// leader's matchSeq - and therefore the commit index - keeps advancing even when the write load pauses"), so a report
    /// that fails is repeated. The port reports once per applied chunk and dropped a failed report ("the next report
    /// carries newer progress"): a report sent again only with the next chunk.
    /// </summary>
    [TestMethod]
    public async Task FailedProgressReportIsSentAgain()
    {
        int calls = 0;
        var delivered = new TaskCompletionSource<ulong>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var reporter = new ProgressReporter((seq, _) =>
        {
            if (Interlocked.Increment(ref calls) == 1) throw new IOException("progress report lost");
            delivered.TrySetResult(seq);
            return Task.CompletedTask;
        });
        reporter.Report(7);
        Task first = await Task.WhenAny(delivered.Task, Task.Delay(TimeSpan.FromSeconds(3)));
        Assert.AreSame(delivered.Task, first, "the report that failed was never sent again");
        Assert.AreEqual(7UL, await delivered.Task);
    }

    /// <summary>
    /// The consequence in a cluster: the follower holds the leader's last entry, but its only report of it was lost and
    /// nothing else is written. Heartbeats only confirm the prefix the leader already knows (PrevLogSeq = NextSeq - 1), so
    /// the leader's commit index never reached the entry and the client write timed out although a quorum held it.
    /// </summary>
    [TestMethod]
    public async Task ClusterWriteCommitsAlthoughTheFollowersProgressReportWasLost()
    {
        using var dir = new TempDir("orig-report");
        var network = new InMemoryNetwork();
        ClusterMember[] members = [new("n1", "mem://n1"), new("n2", "mem://n2")];
        var n2ToN1 = new LossyReportClient(new InMemoryClusterClient(network, "n2", "n1"));
        ClusterNodeHost Make(string id, Func<ClusterMember, IClusterNodeClient> clients) => new(new ClusterNodeHostOptions
        {
            NodeId = id,
            Endpoint = "mem://" + id,
            DbPath = dir.Sub(id),
            Members = members,
            BootstrapPrimaryId = "n1",
            DbOptionsFactory = static () => new DbOptions { WalTtlSeconds = 600 },
            ElectionTimeoutMinMs = 300,
            ElectionTimeoutMaxMs = 600,
            HeartbeatIntervalMs = 50,
            RetryDelay = TimeSpan.FromMilliseconds(50),
        }, clients);

        await using ClusterNodeHost n1 = Make("n1", m => new InMemoryClusterClient(network, "n1", m.NodeId));
        await n1.StartAsync(CancellationToken.None);
        network.Register("n1", n1);
        await using ClusterNodeHost n2 = Make("n2", _ => n2ToN1);
        await n2.StartAsync(CancellationToken.None);
        network.Register("n2", n2);
        try
        {
            Assert.IsTrue(await n1.WriteCommittedAsync(U("k1"), U("v1"), TestUtil.Timeout, CancellationToken.None), "first write (both nodes up)");
            Assert.AreEqual(0, n2ToN1.Failed);

            Volatile.Write(ref n2ToN1.FailReports, 1);
            bool committed = await n1.WriteCommittedAsync(U("k2"), U("v2"), TimeSpan.FromSeconds(3), CancellationToken.None);
            Assert.AreEqual(1, Volatile.Read(ref n2ToN1.Failed), "exactly the report of the second write was lost");
            Assert.AreEqual(n1.Db.GetLatestSequenceNumber(), n2.Db.GetLatestSequenceNumber(), "the follower holds the entry");
            Assert.IsTrue(committed, "a write held by both nodes was not committed: the leader never learned of the follower's progress");
        }
        finally
        {
            network.Unregister("n2");
            network.Unregister("n1");
        }
    }

    /// <summary>
    /// rocksdb-sharp's PairNodeHost runs a resolve loop (PairNodeHost.cs:94 and 145-161): while the node is HandingOff it
    /// asks the other node every second how the handoff ended. The port's host only offered ResolveAsync to its caller, so
    /// after an Unresolved handoff (target unreachable) the pair stayed without a writable primary until someone called it
    /// or restarted the node - although the target was reachable again.
    /// </summary>
    [TestMethod]
    public async Task UnresolvedHandoffIsSettledByTheHostOnceTheTargetIsReachableAgain()
    {
        await using var h = new PairHarness();
        h.Switchover = new SwitchoverOptions { CatchUpTimeout = TimeSpan.FromMilliseconds(200), RpcGrace = TimeSpan.FromMilliseconds(100) };
        await h.StartAsync("A");
        await h.StartAsync("B");
        await WaitUntilAsync(() => h["B"].GetStatus().AttachedFromSeq > 0, "B following");

        // The promote request is lost on the wire and B then becomes unreachable.
        h.AtoB.DelayNextPromote = TimeSpan.FromSeconds(30);
        Task<SwitchoverResult> handoff = h["A"].HandOffAsync(h.Switchover, CancellationToken.None);
        await WaitUntilAsync(() => h["A"].Node.Role == ReplicationRole.HandingOff, "handoff to start");
        h.Network.SetPartitioned("A", "B", true);
        SwitchoverResult r = await handoff;
        Assert.AreEqual(SwitchoverOutcome.Unresolved, r.Outcome, r.Reason);
        Assert.IsFalse(h["A"].Node.Writes.IsOpen, "an unresolved handoff refuses writes");

        // B is back. Nobody calls ResolveAsync: the host settles the handoff by itself.
        h.Network.SetPartitioned("A", "B", false);
        await WaitUntilAsync(() => h["A"].Node.Role == ReplicationRole.Primary && h["A"].Node.Writes.IsOpen, TimeSpan.FromSeconds(4),
            "A to settle its unresolved handoff by itself (still " + h["A"].Node.Role + ")\n" + h.LogText);
        Assert.AreEqual(ReplicationRole.Replica, h["B"].Node.Role, "exactly one primary");
        Assert.IsTrue(h["A"].Node.Epoch > r.Epoch, "A resumed in a newer epoch than the one it handed off in");
    }

    /// <summary>
    /// rocksdb-sharp keeps the last position each replica reported (ReplicationService.cs:30-43:
    /// <c>_lastSyncedPerReplica[replicaIndex] = seqNumber</c>; "a minimum across replicas, not across time" is about
    /// different replicas). The port kept each replica's maximum ("a replica never moves backwards"), but a replica does:
    /// it does not sync its writes by default and loses its tail in a crash, or it is restored from an older copy. Its new,
    /// lower report was ignored and the primary retired archived WALs the replica still had to read - a full resync.
    /// </summary>
    [TestMethod]
    public async Task ReplicaThatReportsAnEarlierPositionHoldsTheWalItStillNeeds()
    {
        using var dir = new TempDir("orig-retire");
        DbOptions o = DbOpts(null);
        o.WalTtlSeconds = 3600;
        await using RocksDatabase source = await RocksDatabase.OpenAsync(dir.Sub("source"), o);
        var src = new ReplicationSource(source);
        var boundaries = new List<ulong>();
        for (int round = 0; round < 3; round++)
        {
            await WriteWorkloadAsync(source, round * 100, round * 100 + 100, round);
            boundaries.Add(source.GetLatestSequenceNumber());
            await source.FlushAsync();
        }
        await WriteWorkloadAsync(source, 300, 310, 9);
        int archived = 0;
        foreach (WalFileInfo f in source.GetSortedWalFiles()) archived += f.Type == WalFileType.ArchivedLogFile ? 1 : 0;
        Assert.AreEqual(3, archived);

        // The replica had applied everything, then restarted without the tail it had not synced: it is back in the
        // first WAL and reads on from there.
        src.ReportReplicaProgress("r1", boundaries[2]);
        src.ReportReplicaProgress("r1", boundaries[0] - 1);
        Assert.AreEqual(boundaries[0] - 1, src.SlowestReplicaSequence, "the replica's current position, not its highest ever");
        Assert.AreEqual(0, src.RetireArchivedWals(), "WALs the replica still has to read were retired");
        using WalTailer tailer = src.TailWal(1);
        Assert.IsTrue(tailer.TryReadChunk(out WalChunk chunk), "the first WAL is still there");
        Assert.AreEqual(1UL, chunk.FirstSequenceNumber);
    }

    private sealed class NoTransport : IPeerTransport
    {
        public Task<RequestVoteResult?> SendRequestVoteAsync(ClusterMember peer, RequestVotePayload request, CancellationToken cancellationToken) =>
            Task.FromResult<RequestVoteResult?>(null);

        public Task<AppendEntriesResult?> SendHeartbeatAsync(ClusterMember peer, HeartbeatPayload payload, CancellationToken cancellationToken) =>
            Task.FromResult<AppendEntriesResult?>(null);
    }

    /// <summary>
    /// Approach review (Raft log matching), a bug of the port only: a follower adopts its leader's term map at attach,
    /// including ranges for entries it has not received yet (rocksdb-sharp's follower records a term only for a batch
    /// it ingests, ClusterNodeHost.cs:285-286). When such a follower wins an election before it received them, its own
    /// term starts before the last adopted range and <c>RaftLogTerms.Record</c> ignored it: no entry of the new leader
    /// carried its term (nothing could commit, RaftClusterNode.TryAdvanceCommitLocked) and its entries past the stale
    /// range were labelled with the old leader's term, so the old leader's divergent tail passed the attach check and
    /// was kept.
    /// </summary>
    [TestMethod]
    public async Task LeaderElectedBeforeItReceivedAnAdoptedTermRangeLabelsItsEntriesWithItsOwnTerm()
    {
        await using RocksDatabase db = await RocksDatabase.OpenAsync("/db", DbOpts(new MemoryFileSystem()));
        for (int i = 0; i < 5; i++) db.Put(U("k" + i), U("v"));
        Assert.AreEqual(5UL, db.GetLatestSequenceNumber());
        var state = new RaftPersistentState(new MemoryFileSystem(), "/raft");
        state.SetMode(ClusterMode.Cluster);
        state.UpdateTermAndVote(3, null);
        // Attached to the leader of term 3, whose log has entries 6..9 of term 1 and its own from 10 on; none arrived.
        state.AdoptTermsAfter(5, RaftLogTerms.Deserialize("1,1\n10,3\n"));
        Assert.AreEqual(3, state.Terms.GetTermForSeq(10));

        var config = new RaftConfig("n1", "mem://n1", []) { ElectionTimeoutMinMs = 20, ElectionTimeoutMaxMs = 40, HeartbeatIntervalMs = 5 };
        await using var node = new RaftClusterNode(config, state, db, new NoTransport());
        node.Start();
        await WaitUntilAsync(() => node.Role == RaftRole.Leader, "the node to win its election");
        long term = node.CurrentTerm;
        Assert.IsTrue(term > 3);
        Assert.AreEqual(6UL, db.GetLatestSequenceNumber(), "the leader's first entry");
        Assert.AreEqual(term, node.GetTermForSeq(6), "the leader's first entry does not carry its term: it can never commit");
        for (int i = 0; i < 10; i++) db.Put(U("n" + i), U("v"));
        Assert.AreEqual(term, node.GetTermForSeq(12), "an entry of this leader is labelled with the old leader's term");
        // The old leader comes back with its own entries 6..12 (term 3 at 12) and asks whether it may keep them.
        LogConsistencyResult check = node.CheckFollowerLogConsistency(12, 3);
        Assert.IsTrue(check.IsLeader);
        Assert.IsFalse(check.Consistent, "the deposed leader's divergent tail passed the attach check");
    }

    /// <summary>Forwards to an inner file system; <see cref="BeforeCreate"/> runs before a file is created (and may block).</summary>
    private sealed class HookedFileSystem(IFileSystem inner) : IFileSystem
    {
        public Action<string>? BeforeCreate { get; set; }
        public Action<string>? BeforeRead { get; set; }

        public IWritableFile CreateWritable(string path, bool truncate = true)
        {
            BeforeCreate?.Invoke(path);
            return inner.CreateWritable(path, truncate);
        }

        public IRandomAccessFile OpenRandomAccess(string path) => inner.OpenRandomAccess(path);
        public ISequentialFile OpenSequential(string path)
        {
            BeforeRead?.Invoke(path);
            return inner.OpenSequential(path);
        }

        public bool FileExists(string path) => inner.FileExists(path);
        public long GetFileSize(string path) => inner.GetFileSize(path);
        public void DeleteFile(string path) => inner.DeleteFile(path);
        public void RenameFile(string source, string target) => inner.RenameFile(source, target);
        public void CreateDirectoryIfMissing(string path) => inner.CreateDirectoryIfMissing(path);
        public IEnumerable<string> GetChildren(string directory) => inner.GetChildren(directory);
        public IDisposable LockFile(string path) => inner.LockFile(path);
        public void SyncDirectory(string path) => inner.SyncDirectory(path);
        public bool DirectoryExists(string path) => inner.DirectoryExists(path);
        public void DeleteDirectory(string path) => inner.DeleteDirectory(path);
    }

    private static void WaitUntilBlocked(Thread thread)
    {
        var spin = new SpinWait();
        while ((thread.ThreadState & ThreadState.WaitSleepJoin) == 0) spin.SpinOnce();
    }

    /// <summary>
    /// Approach review (switchover: never two primaries), a bug of rocksdb-sharp (SwitchoverNode.cs:432-453) that the
    /// port copied: HandleRejectEpochAsync looked for a running promotion under the node's lock, released it, and took
    /// it again to promise the epoch and report its role. A promote request for that epoch that was still on the wire
    /// (the reason the old primary asks) could run between the two: it promised the epoch itself and started the
    /// promotion, the reject then found the epoch promised and reported "replica, epoch promised" - on which the old
    /// primary resumes writing - while the promotion ran to its end. Two primaries. The reject now makes its promise in
    /// the same critical section in which it saw no promotion.
    /// <para>To get the promote between the reject's two critical sections, both requests are queued on the node's lock
    /// while a third call holds it (blocked in its state write) for longer than the lock lets a releasing thread
    /// overtake its waiters. Which of the two runs first is up to the lock, so the scenario is repeated; every round
    /// checks the invariant "a reject that reports Replica means the promote for that epoch does not succeed".</para>
    /// </summary>
    [TestMethod]
    public async Task RejectedEpochIsNeverPromotedByAPromoteRacingTheReject()
    {
        await using RocksDatabase db = await RocksDatabase.OpenAsync("/db", DbOpts(new MemoryFileSystem()));
        db.Put(U("k"), U("v"));
        var fs = new HookedFileSystem(new MemoryFileSystem());
        var state = new ReplicationRoleState(fs, "/role");
        state.Set(0, ReplicationRole.Replica, "A");
        var node = new SwitchoverNode("B", state, db, static _ => Task.CompletedTask);
        ulong handoffSeq = db.GetLatestSequenceNumber();
        for (int round = 0; round < 12; round++)
        {
            long epoch = state.PromisedEpoch + 2;
            using var holding = new ManualResetEventSlim();
            using var release = new ManualResetEventSlim();
            fs.BeforeCreate = _ =>
            {
                fs.BeforeCreate = null;
                holding.Set();
                release.Wait();
            };
            // Holds the node's lock inside its state write (a stream message of the primary's next epoch).
            var holder = new Thread(() => node.AcceptStream("A", epoch - 1));
            holder.Start();
            holding.Wait();
            ReplicationNodeStatus? status = null;
            PromoteResult? result = null;
            var reject = new Thread(() => status = node.HandleRejectEpochAsync(epoch).GetAwaiter().GetResult());
            reject.Start();
            WaitUntilBlocked(reject);
            var promote = new Thread(() => result = node.HandlePromoteAsync(new PromoteRequest(epoch, "A", handoffSeq, 1000)).GetAwaiter().GetResult());
            promote.Start();
            WaitUntilBlocked(promote);
            // Past the time after which the lock hands itself to its waiters in turn.
            Thread.Sleep(150);
            release.Set();
            holder.Join();
            reject.Join();
            promote.Join();
            if (status!.Role == ReplicationRole.Replica)
            {
                Assert.IsTrue(status.PromisedEpoch >= epoch, "a replica answers a reject with the epoch promised");
                Assert.IsFalse(result!.Promoted, "round " + round + ": the reject of epoch " + epoch + " reported Replica, but the promote for it succeeded");
                Assert.AreEqual(ReplicationRole.Replica, node.Role, "round " + round);
            }
            else
            {
                Assert.IsTrue(result!.Promoted);
                Assert.AreEqual(epoch, status.Epoch);
                // Back to a replica of A for the next round.
                state.Set(state.Epoch, ReplicationRole.Replica, "A");
            }
        }
    }

    /// <summary>Copies the database files of a pair node's directory; its "role" directory (fsync'd state) is left alone.</summary>
    private static void CopyDatabaseFiles(string from, string to)
    {
        Directory.CreateDirectory(to);
        foreach (string f in Directory.GetFiles(from)) File.Copy(f, Path.Combine(to, Path.GetFileName(f)), overwrite: true);
        foreach (string d in Directory.GetDirectories(from))
        {
            if (Path.GetFileName(d) != "role") CopyDatabaseFiles(d, Path.Combine(to, Path.GetFileName(d)));
        }
    }

    private static void DeleteDatabaseFiles(string dir)
    {
        foreach (string f in Directory.GetFiles(dir)) File.Delete(f);
        foreach (string d in Directory.GetDirectories(dir))
        {
            if (Path.GetFileName(d) != "role") Directory.Delete(d, recursive: true);
        }
    }

    private static async Task WritePairAsync(PairNodeHost host, string prefix, int count)
    {
        for (int i = 0; i < count; i++)
        {
            using var b = new WriteBatch();
            b.Put(U(prefix + i), U("v-" + prefix + i));
            PairWriteResult r = await host.WriteAsync(b, TimeSpan.FromSeconds(2), CancellationToken.None);
            Assert.AreEqual(PairWriteStatus.Applied, r.Status);
        }
    }

    /// <summary>
    /// Approach review (switchover / WAL tailing: a replica is a prefix of its primary or is rebuilt), a weakness of
    /// rocksdb-sharp's design that the port copied: client writes of a pair are not fsync'd, so a primary that loses
    /// power comes back without the tail its replica already holds. It restarted as primary in the SAME epoch
    /// (SwitchoverTest PairNodeHost.cs:71-79, port PairNodeHost.StartAsync), wrote new entries at the lost sequence
    /// numbers, and the replica's attach check - "do you hold my last entry in the same epoch?" (SwitchoverNode
    /// CheckReplicaLog) - passed as soon as the primary's new log was long enough: same sequence number, same epoch,
    /// different entry. The replica kept the lost entries and streamed on: two different databases, silently.
    /// A primary that restarts now starts a new epoch at the end of its recovered log, so whatever a replica holds past
    /// that point carries the old epoch and fails the check.
    /// </summary>
    [TestMethod]
    public async Task PrimaryRestartedWithoutItsUnsyncedTailDoesNotLeaveTheReplicaSilentlyDiverged()
    {
        await using var h = new PairHarness();
        using var backup = new TempDir("orig-powerloss");
        await h.StartAsync("A");
        await h.StartAsync("B");
        await WaitUntilAsync(() => h["B"].GetStatus().AttachedFromSeq > 0, "B following");
        await WritePairAsync(h["A"], "base", 50);
        await WaitUntilAsync(() => h["B"].Db.GetLatestSequenceNumber() == h["A"].Db.GetLatestSequenceNumber(), "B caught up");

        // What A's disk holds at the moment of the power loss: everything up to here.
        await h.StopAsync("A");
        CopyDatabaseFiles(h.DbPath("A"), backup.Sub("A"));
        await h.StartAsync("A");
        // Acknowledged, replicated to B, but only in A's page cache.
        await WritePairAsync(h["A"], "lost", 10);
        await WaitUntilAsync(() => h["B"].Db.GetLatestSequenceNumber() == h["A"].Db.GetLatestSequenceNumber(), "B holds the tail\n" + h.LogText);
        ulong replicaTail = h["B"].Db.GetLatestSequenceNumber();

        // Power loss of A: its database is back at the copy (the role state is fsync'd on every change and survives).
        // B cannot reach it for a while after it is back.
        h.Network.SetPartitioned("A", "B", true);
        await h.StopAsync("A");
        DeleteDatabaseFiles(h.DbPath("A"));
        CopyDatabaseFiles(backup.Sub("A"), h.DbPath("A"));
        await h.StartAsync("A");
        Assert.AreEqual(ReplicationRole.Primary, h["A"].Node.Role);
        Assert.AreEqual(replicaTail - 10, h["A"].Db.GetLatestSequenceNumber(), "A lost its last 10 entries");
        await WritePairAsync(h["A"], "new", 25);
        Assert.IsTrue(h["A"].Db.GetLatestSequenceNumber() > replicaTail);

        h.Network.SetPartitioned("A", "B", false);
        await WaitUntilAsync(() => !h["B"].IsResyncing && h["B"].Db.GetLatestSequenceNumber() == h["A"].Db.GetLatestSequenceNumber(),
            "B to follow A again\n" + h.LogText);
        AssertSameContents(Contents(h["A"].Db), Contents(h["B"].Db), "replica vs primary after the primary lost its tail\n" + h.LogText);
        Assert.AreEqual(1, h["B"].Resyncs, "the replica's divergent tail can only be removed by a rebuild");
    }

    /// <summary>
    /// Approach review (Raft log matching), in rocksdb-sharp (RaftClusterNode.cs:76-87) and the port: a bootstrap
    /// primary that restarts in Bootstrap mode leads again without an election, and it did so in the term of its
    /// previous run. If its recovered log ends before entries a follower holds (log writes not fsync'd and a power
    /// loss; SyncWal = false in the port, always in rocksdb-sharp, which never syncs), it wrote different entries at
    /// the same sequence numbers in the same term, and the follower's stale tail passed the attach check. It now
    /// starts a new term at the end of its recovered log.
    /// </summary>
    [TestMethod]
    public async Task BootstrapPrimaryThatRestartsDoesNotReuseItsTermForNewEntries()
    {
        var raftFs = new MemoryFileSystem();
        await using RocksDatabase db = await RocksDatabase.OpenAsync("/db", DbOpts(new MemoryFileSystem()));
        var config = new RaftConfig("n1", "mem://n1", [new ClusterMember("n2", "mem://n2")]) { BootstrapPrimary = true };
        long firstTerm;
        await using (var first = new RaftClusterNode(config, new RaftPersistentState(raftFs, "/raft"), db, new NoTransport()))
        {
            Assert.AreEqual(RaftRole.Leader, first.Role);
            firstTerm = first.CurrentTerm;
            for (int i = 0; i < 5; i++) db.Put(U("k" + i), U("v"));
        }
        // The node restarts with its log ending at 5; its follower had received 6..8 of the first run, which are gone here.
        await using var node = new RaftClusterNode(config, new RaftPersistentState(raftFs, "/raft"), db, new NoTransport());
        Assert.AreEqual(RaftRole.Leader, node.Role);
        Assert.IsTrue(node.CurrentTerm > firstTerm, "the restarted primary leads in the term of its previous run");
        for (int i = 0; i < 7; i++) db.Put(U("n" + i), U("v"));
        Assert.AreEqual(firstTerm, node.GetTermForSeq(5));
        Assert.AreEqual(node.CurrentTerm, node.GetTermForSeq(6));
        Assert.IsTrue(node.CheckFollowerLogConsistency(5, firstTerm).Consistent, "a follower that holds nothing past the recovered log attaches");
        Assert.IsFalse(node.CheckFollowerLogConsistency(8, firstTerm).Consistent, "a follower's entries of the first run that this node lost passed the attach check");
    }

    /// <summary>
    /// Approach review (Raft election safety: at most one vote per term, on stable storage), in rocksdb-sharp
    /// (RaftPersistentState.cs Load: "corrupt file; start fresh") and the port: a node that could not read its state
    /// file at startup started with term 0 and no vote - and voted again in a term it had already voted in.
    /// </summary>
    [TestMethod]
    public async Task NodeThatCannotReadItsVoteDoesNotStartWithoutIt()
    {
        var fs = new HookedFileSystem(new MemoryFileSystem());
        await using RocksDatabase db = await RocksDatabase.OpenAsync("/db", DbOpts(new MemoryFileSystem()));
        var config = new RaftConfig("n1", "mem://n1", [new ClusterMember("n2", "mem://n2"), new ClusterMember("n3", "mem://n3")]);
        var state = new RaftPersistentState(fs, "/raft");
        state.SetMode(ClusterMode.Cluster);
        await using (var node = new RaftClusterNode(config, state, db, new NoTransport()))
        {
            Assert.IsTrue(node.HandleRequestVote(7, "n2", 0, 0).VoteGranted);
        }

        // Restart; the state file cannot be read this time.
        fs.BeforeRead = static path => throw new RocksDbException(StatusCode.IOError, "IO error: injected failure reading " + path);
        RaftPersistentState? reloaded = null;
        StatusCode code = StatusCode.Ok;
        try
        {
            reloaded = new RaftPersistentState(fs, "/raft");
        }
        catch (RocksDbException e)
        {
            code = e.Code;
        }
        if (reloaded != null)
        {
            await using var again = new RaftClusterNode(config, reloaded, db, new NoTransport());
            Assert.IsFalse(again.HandleRequestVote(7, "n3", 0, 0).VoteGranted, "a second vote in term 7, for another candidate");
        }
        Assert.AreEqual(StatusCode.IOError, code, "the read error is reported");

        // Readable again: the vote is still there, and a repeated request of the same candidate is answered the same way.
        fs.BeforeRead = null;
        var recovered = new RaftPersistentState(fs, "/raft");
        Assert.AreEqual(7, recovered.CurrentTerm);
        Assert.AreEqual("n2", recovered.VotedFor);
        await using var restarted = new RaftClusterNode(config, recovered, db, new NoTransport());
        Assert.IsFalse(restarted.HandleRequestVote(7, "n3", 0, 0).VoteGranted);
        Assert.IsTrue(restarted.HandleRequestVote(7, "n2", 0, 0).VoteGranted, "the candidate's repeated request (its first answer was lost)");
        Assert.IsFalse(restarted.HandleRequestVote(6, "n3", 0, 0).VoteGranted, "a request of an older term");
    }

    /// <summary>
    /// Approach review (switchover fencing across a crash), validated: the target persists its promise before it acts,
    /// so a target that dies between the promise and the promotion comes back as a replica that refuses the promote
    /// when it is delivered again and tells the old primary (RejectEpoch) that the epoch is promised and not taken; and
    /// an old primary that dies while handing off comes back read-only until it has asked.
    /// </summary>
    [TestMethod]
    public async Task HandoffStateSurvivesACrashOfEitherNodeBetweenPersistAndReply()
    {
        await using RocksDatabase db = await RocksDatabase.OpenAsync("/db", DbOpts(new MemoryFileSystem()));
        db.Put(U("k"), U("v"));
        ulong seq = db.GetLatestSequenceNumber();

        // Target: promised epoch 2, then died while detaching from the primary.
        var targetFs = new MemoryFileSystem();
        var targetState = new ReplicationRoleState(targetFs, "/role");
        targetState.Set(1, ReplicationRole.Replica, "A");
        var detaching = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var never = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var crashed = new SwitchoverNode("B", targetState, db, _ =>
        {
            detaching.TrySetResult();
            return never.Task;
        });
        _ = crashed.HandlePromoteAsync(new PromoteRequest(2, "A", seq, 5000));
        await detaching.Task;
        var target = new SwitchoverNode("B", new ReplicationRoleState(targetFs, "/role"), db, static _ => Task.CompletedTask);
        Assert.AreEqual(ReplicationRole.Replica, target.Role);
        Assert.IsFalse(target.Writes.IsOpen);
        PromoteResult again = await target.HandlePromoteAsync(new PromoteRequest(2, "A", seq, 5000));
        Assert.IsFalse(again.Promoted, "the promote of an epoch promised before the crash was accepted again");
        ReplicationNodeStatus status = await target.HandleRejectEpochAsync(2);
        Assert.AreEqual(ReplicationRole.Replica, status.Role);
        Assert.IsTrue(status.PromisedEpoch >= 2);

        // Old primary: persisted HandingOff (and its proposal), then died.
        var primaryFs = new MemoryFileSystem();
        var primaryState = new ReplicationRoleState(primaryFs, "/role");
        SwitchoverNode.BootstrapPrimary(primaryState, db, "A");
        primaryState.Promise(2);
        primaryState.Set(1, ReplicationRole.HandingOff, "A", "B");
        var primary = new SwitchoverNode("A", new ReplicationRoleState(primaryFs, "/role"), db, static _ => Task.CompletedTask);
        Assert.AreEqual(ReplicationRole.HandingOff, primary.Role);
        Assert.IsFalse(primary.Writes.IsOpen, "a node that restarts while handing off accepts writes before it asked the target");
        Assert.IsFalse(await primary.Writes.TryEnterAsync(TimeSpan.Zero, CancellationToken.None));
    }

    /// <summary>
    /// Approach review (Raft under delayed, duplicated and reordered messages), validated: a deposed leader's heartbeat
    /// and stream tag are refused and change nothing, a heartbeat that arrives late (lower commit index, same term) does
    /// not move the commit index back, a repeated one is harmless, and a follower never reports an entry committed that
    /// the heartbeat did not verify against the leader's log.
    /// </summary>
    [TestMethod]
    public async Task FollowerIgnoresStaleDuplicatedAndReorderedLeaderMessages()
    {
        await using RocksDatabase db = await RocksDatabase.OpenAsync("/db", DbOpts(new MemoryFileSystem()));
        for (int i = 0; i < 10; i++) db.Put(U("k" + i), U("v"));
        var state = new RaftPersistentState(new MemoryFileSystem(), "/raft");
        state.SetMode(ClusterMode.Cluster);
        state.RecordTermRange(1, 1);
        state.RecordTermRange(7, 5);
        var config = new RaftConfig("n3", "mem://n3", [new ClusterMember("n1", "mem://n1"), new ClusterMember("n2", "mem://n2")]);
        await using var node = new RaftClusterNode(config, state, db, new NoTransport());

        AppendEntriesResult r = node.HandleAppendEntries(new HeartbeatPayload(5, "n2", 9, 5, 8, ClusterMode.Cluster));
        Assert.IsTrue(r.Success);
        Assert.AreEqual(8UL, node.CommitSeq);
        Assert.AreEqual("n2", node.CurrentLeaderId);

        // The deposed leader of term 4: heartbeat and stream tag.
        r = node.HandleAppendEntries(new HeartbeatPayload(4, "n1", 10, 1, 10, ClusterMode.Cluster));
        Assert.IsFalse(r.Success);
        Assert.AreEqual(AppendEntriesRejectReason.StaleTerm, r.Reason);
        Assert.AreEqual(5, r.Term, "the stale leader learns the newer term from the answer");
        node.HandleObservedLeader("n1", 4);
        Assert.AreEqual("n2", node.CurrentLeaderId);
        Assert.AreEqual(5, node.CurrentTerm);
        Assert.AreEqual(8UL, node.CommitSeq);

        // An older heartbeat of the current leader arrives after a newer one; then the newer one again.
        Assert.IsTrue(node.HandleAppendEntries(new HeartbeatPayload(5, "n2", 6, 1, 6, ClusterMode.Cluster)).Success);
        Assert.AreEqual(8UL, node.CommitSeq, "the commit index moved back");
        Assert.IsTrue(node.HandleAppendEntries(new HeartbeatPayload(5, "n2", 9, 5, 8, ClusterMode.Cluster)).Success);
        Assert.AreEqual(8UL, node.CommitSeq);

        // The leader committed 10, but this heartbeat only verified the log up to 9.
        Assert.IsTrue(node.HandleAppendEntries(new HeartbeatPayload(5, "n2", 9, 5, 10, ClusterMode.Cluster)).Success);
        Assert.AreEqual(9UL, node.CommitSeq);

        // A heartbeat whose previous entry carries another term here: nothing is committed, the node asks for a rebuild.
        int resyncs = 0;
        node.ResyncRequired += (_, _) => resyncs++;
        r = node.HandleAppendEntries(new HeartbeatPayload(6, "n1", 10, 6, 10, ClusterMode.Cluster));
        Assert.IsFalse(r.Success);
        Assert.AreEqual(AppendEntriesRejectReason.TermMismatch, r.Reason);
        Assert.AreEqual(1, resyncs);
        Assert.AreEqual(9UL, node.CommitSeq);
    }
}
