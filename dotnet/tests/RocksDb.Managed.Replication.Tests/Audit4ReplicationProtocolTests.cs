using RocksDb.Managed.Cluster;
using RocksDb.Managed.Replication.Hosting;
using RocksDb.Managed.Replication.MagicOnion;
using RocksDb.Managed.Replication.Switchover;

namespace RocksDb.Managed.Replication.Tests;

/// <summary>
/// Audit #4, the transport layer (RocksDb.Managed.Replication.MagicOnion, snapshot stream) read for safety on its own
/// terms, and the Raft / switchover rules that had only been validated by reading, now validated by test: election
/// restriction, current-term-only commit, at most one leader per term, membership of the sender of a Raft RPC, and
/// the order in which a handoff holds writes and takes its handoff point.
/// </summary>
[TestClass]
public sealed class Audit4ReplicationProtocolTests
{
    // ---------------------------------------------------------------------------------------------------
    // Transport

    /// <summary>
    /// gRPC refuses a received message above 4 MB by default, and a WAL chunk is at least one whole write batch: a
    /// replica could not receive a batch larger than that, failed the stream, attached again at the same position and
    /// failed again - for good, while the primary went on acknowledging writes. The in-memory transport of the tests
    /// has no such limit. The channels of this library no longer limit received messages. Failed before: B never
    /// received the entry.
    /// </summary>
    [TestMethod]
    public async Task WalChunkLargerThanTheDefaultGrpcMessageLimitReachesTheReplica()
    {
        using var dir = new TempDir("grpc-large");
        int portA = GrpcNodeServer.FindFreePort(), portB = GrpcNodeServer.FindFreePort();
        using var toB = new GrpcPairClient("B", "http://127.0.0.1:" + portB, TimeSpan.FromSeconds(2));
        using var toA = new GrpcPairClient("A", "http://127.0.0.1:" + portA, TimeSpan.FromSeconds(2));
        PairNodeHost Host(string id, IPairNodeClient peer) => new(new PairNodeHostOptions
        {
            NodeId = id,
            PeerId = id == "A" ? "B" : "A",
            DbPath = dir.Sub(id),
            InitialRole = id == "A" ? ReplicationRole.Primary : ReplicationRole.Replica,
            DbOptionsFactory = static () => new DbOptions { WalTtlSeconds = 600 },
            RetryDelay = TimeSpan.FromMilliseconds(50),
        }, peer);
        PairNodeHost a = Host("A", toB), b = Host("B", toA);
        await using GrpcNodeServer serverA = await GrpcNodeServer.StartAsync<PairService, PairNodeHost>(a, System.Net.IPAddress.Loopback, portA, CancellationToken.None);
        await using GrpcNodeServer serverB = await GrpcNodeServer.StartAsync<PairService, PairNodeHost>(b, System.Net.IPAddress.Loopback, portB, CancellationToken.None);
        try
        {
            await a.StartAsync(CancellationToken.None);
            await b.StartAsync(CancellationToken.None);
            await WaitUntilAsync(() => b.GetStatus().AttachedFromSeq > 0, "B following");

            byte[] large = new byte[6 * 1024 * 1024];
            new Random(4).NextBytes(large);
            using (var batch = new WriteBatch())
            {
                batch.Put(U("large"), large);
                Assert.AreEqual(PairWriteStatus.Applied, (await a.WriteAsync(batch, TimeSpan.FromSeconds(2), CancellationToken.None)).Status);
            }
            await WaitUntilAsync(() => b.Db.GetLatestSequenceNumber() == a.Db.GetLatestSequenceNumber(), "B to receive a 6 MB batch");
            Assert.AreEqual(a.Checksum(), b.Checksum());
        }
        finally
        {
            // The hosts first: a server waits for its open streams when it stops.
            await b.DisposeAsync();
            await a.DisposeAsync();
        }
    }

    /// <summary>
    /// The channels and servers of the gRPC transport send HTTP/2 keep-alive pings. A replica reading a WAL stream
    /// sends nothing; without pings it never notices a primary that vanished without closing the connection (power
    /// loss) and waits on the dead stream for good. Failed before: no handler with pings was configured.
    /// </summary>
    [TestMethod]
    public void GrpcChannelsSendKeepAlivePingsAndDoNotLimitReceivedMessages()
    {
        Grpc.Net.Client.GrpcChannelOptions options = GrpcTransportDefaults.CreateChannelOptions();
        var handler = (System.Net.Http.SocketsHttpHandler)options.HttpHandler!;
        Assert.AreEqual(System.Net.Http.HttpKeepAlivePingPolicy.Always, handler.KeepAlivePingPolicy, "pings must flow while only a stream is open");
        Assert.AreEqual(GrpcTransportDefaults.KeepAlivePingDelay, handler.KeepAlivePingDelay);
        Assert.AreEqual(GrpcTransportDefaults.KeepAlivePingTimeout, handler.KeepAlivePingTimeout);
        Assert.IsNull(options.MaxReceiveMessageSize);
        handler.Dispose();

        // Options of the caller: only what they leave open is filled in.
        using var own = new System.Net.Http.SocketsHttpHandler();
        var custom = new Grpc.Net.Client.GrpcChannelOptions { HttpHandler = own, MaxReceiveMessageSize = 1 << 20 };
        Assert.AreSame(custom, GrpcTransportDefaults.Apply(custom));
        Assert.AreSame(own, custom.HttpHandler);
        Assert.AreEqual(1 << 20, custom.MaxReceiveMessageSize);
    }

    /// <summary>
    /// A snapshot chunk of a kind this build does not know was taken for file data. It is refused; so is an end marker
    /// with an impossible file count. The write status of a pair travels with its number, and a peer built before that
    /// field existed is read through the old flag. Failed before: the unknown kind became a file data chunk.
    /// </summary>
    [TestMethod]
    public void UnknownSnapshotChunkKindIsRefusedAndWriteStatusSurvivesOldPeers()
    {
        Assert.ThrowsExactly<InvalidDataException>(() => new SnapshotChunkDto { Kind = 7, FileName = "000001.sst" }.ToChunk());
        Assert.ThrowsExactly<InvalidDataException>(() => new SnapshotChunkDto { Kind = (int)SnapshotChunkKind.Complete, FileSize = -1 }.ToChunk());
        Assert.AreEqual(SnapshotChunkKind.Plan, new SnapshotChunkDto { Kind = 0, KeepFiles = null! }.ToChunk().Kind, "a plan without a keep list keeps nothing");
        Assert.AreEqual(SnapshotChunkKind.FileData, SnapshotChunkDto.From(SnapshotChunk.FileData("a.sst", 1, 0, new byte[1])).ToChunk().Kind);

        Assert.AreEqual(PairWriteStatus.NotReplicated, new PairWriteResultDto { Status = (int)PairWriteStatus.NotReplicated }.ToStatus());
        Assert.AreEqual(PairWriteStatus.Applied, new PairWriteResultDto { Applied = true }.ToStatus(), "an old peer sends no status");
        Assert.AreEqual(PairWriteStatus.NotPrimary, new PairWriteResultDto().ToStatus());
    }

    private static void CreateFile(IFileSystem fs, string path, string content)
    {
        using IWritableFile f = fs.CreateWritable(path, truncate: true);
        f.Append(U(content));
        f.Sync();
    }

    /// <summary>
    /// Restore atomicity must not depend on the sender. The receiver wrote CURRENT where it arrived in the stream, so a
    /// sender that does not put it last (another implementation, a bug) and a cut afterwards left a directory that
    /// opens as a database with files missing. CURRENT is now written under a temporary name and renamed into place
    /// by CompleteAsync. Failed before: CURRENT existed after the cut.
    /// </summary>
    [TestMethod]
    public async Task InterruptedRestoreLeavesNoCurrentWhateverOrderTheFilesArriveIn()
    {
        var fs = new MemoryFileSystem();
        byte[] current = U("MANIFEST-000004\n"), table = U("table-bytes");
        await using (var cut = new SnapshotReceiver(fs, "/db", []))
        {
            await cut.WriteAsync(SnapshotChunk.Plan(9, []), CancellationToken.None);
            await cut.WriteAsync(SnapshotChunk.FileData("CURRENT", current.Length, 0, current), CancellationToken.None);
            await cut.WriteAsync(SnapshotChunk.FileData("000007.sst", table.Length, 0, table), CancellationToken.None);
            Assert.IsFalse(fs.FileExists("/db/CURRENT"), "CURRENT is in place before the transfer completed");
            await Assert.ThrowsExactlyAsync<InvalidOperationException>(async () => await cut.CompleteAsync(CancellationToken.None));
        }
        Assert.IsFalse(fs.FileExists("/db/CURRENT"), "a cut-off restore left an openable database directory");

        await using var whole = new SnapshotReceiver(fs, "/db", []);
        await whole.WriteAsync(SnapshotChunk.Plan(9, []), CancellationToken.None);
        await whole.WriteAsync(SnapshotChunk.FileData("CURRENT", current.Length, 0, current), CancellationToken.None);
        await whole.WriteAsync(SnapshotChunk.FileData("000007.sst", table.Length, 0, table), CancellationToken.None);
        await whole.WriteAsync(SnapshotChunk.Complete(2), CancellationToken.None);
        SnapshotRestoreResult r = await whole.CompleteAsync(CancellationToken.None);
        Assert.AreEqual(2, r.TransferredFiles);
        CollectionAssert.AreEqual(current, fs.ReadAllBytes("/db/CURRENT"));
        CollectionAssert.AreEqual(table, fs.ReadAllBytes("/db/000007.sst"));
        foreach (string name in fs.GetChildren("/db")) Assert.IsTrue(name is "CURRENT" or "000007.sst", "left behind: " + name);
    }

    /// <summary>
    /// The plan decides what the receiver deletes. A plan that keeps CURRENT, a MANIFEST or a WAL would carry the old
    /// database (or its divergent log) into the restored one, and one that keeps a file this node does not hold
    /// restores a database with a table missing. Both are refused before anything is deleted, and so is a file sent
    /// under the name of the node's own state directory. Failed before: the plan was applied as sent.
    /// </summary>
    [TestMethod]
    [DataRow("CURRENT")]
    [DataRow("MANIFEST-000003")]
    [DataRow("000011.log")]
    [DataRow("000099.sst")]
    public async Task SnapshotPlanMayOnlyKeepImmutableFilesTheReceiverHolds(string kept)
    {
        var fs = new MemoryFileSystem();
        fs.CreateDirectoryIfMissing("/db");
        CreateFile(fs, "/db/CURRENT", "MANIFEST-000003\n");
        CreateFile(fs, "/db/MANIFEST-000003", "m");
        CreateFile(fs, "/db/000011.log", "w");
        CreateFile(fs, "/db/000005.sst", "t");
        await using var receiver = new SnapshotReceiver(fs, "/db", ["raft"]);
        await Assert.ThrowsExactlyAsync<InvalidOperationException>(async () =>
            await receiver.WriteAsync(SnapshotChunk.Plan(1, ["000005.sst", kept]), CancellationToken.None));
        foreach (string name in (string[])["CURRENT", "MANIFEST-000003", "000011.log", "000005.sst"])
            Assert.IsTrue(fs.FileExists("/db/" + name), "a refused plan deleted " + name);

        await using var valid = new SnapshotReceiver(fs, "/db", ["raft"]);
        await valid.WriteAsync(SnapshotChunk.Plan(1, ["000005.sst"]), CancellationToken.None);
        Assert.IsTrue(fs.FileExists("/db/000005.sst"));
        Assert.IsFalse(fs.FileExists("/db/CURRENT"));
        await Assert.ThrowsExactlyAsync<InvalidOperationException>(async () =>
            await valid.WriteAsync(SnapshotChunk.FileData("raft", 1, 0, new byte[1]), CancellationToken.None));
    }

    // ---------------------------------------------------------------------------------------------------
    // Raft rules

    /// <summary>A clock whose timers fire only when the test says so; each node gets its own.</summary>
    private sealed class ManualTime : TimeProvider
    {
        private readonly List<ManualTimer> _timers = [];

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            var timer = new ManualTimer(callback, state) { Armed = dueTime != global::System.Threading.Timeout.InfiniteTimeSpan };
            lock (_timers) _timers.Add(timer);
            return timer;
        }

        /// <summary>Lets the node's election timer elapse (if it is armed).</summary>
        public void ElapseElectionTimeout() => Fire(static t => t.State is ElectionTimer);

        /// <summary>Lets every pending delay elapse (the leader's wait between two heartbeat rounds).</summary>
        public void ElapseDelays() => Fire(static t => t.State is not ElectionTimer);

        private void Fire(Func<ManualTimer, bool> which)
        {
            ManualTimer[] timers;
            lock (_timers) timers = [.. _timers];
            foreach (ManualTimer t in timers)
            {
                if (which(t) && t.TryDisarm()) t.Callback(t.State);
            }
        }

        private sealed class ManualTimer(TimerCallback callback, object? state) : ITimer
        {
            private int _armed;
            public TimerCallback Callback { get; } = callback;
            public object? State { get; } = state;
            public bool Armed
            {
                get => Volatile.Read(ref _armed) == 1;
                init => _armed = value ? 1 : 0;
            }

            public bool TryDisarm() => Interlocked.Exchange(ref _armed, 0) == 1;

            public bool Change(TimeSpan dueTime, TimeSpan period)
            {
                Volatile.Write(ref _armed, dueTime != global::System.Threading.Timeout.InfiniteTimeSpan ? 1 : 0);
                return true;
            }

            public void Dispose() => Volatile.Write(ref _armed, 0);
            public ValueTask DisposeAsync()
            {
                Dispose();
                return ValueTask.CompletedTask;
            }
        }
    }

    /// <summary>
    /// Three <see cref="RaftClusterNode"/>s in Cluster mode on manual clocks, wired directly to each other. A node's
    /// vote requests can be held back (<see cref="HoldVotesOf"/>) and answers are counted.
    /// </summary>
    private sealed class RaftTrio : IPeerTransport, IAsyncDisposable
    {
        private static readonly string[] s_ids = ["n1", "n2", "n3"];
        private readonly Dictionary<string, TaskCompletionSource> _held = new();
        private readonly Dictionary<string, int> _voteAnswers = new();
        private readonly Dictionary<string, int> _voteRequests = new();
        private readonly Dictionary<string, MemoryFileSystem> _stateFs = new();
        private int _heartbeatAnswers;

        public Dictionary<string, RaftClusterNode> Nodes { get; } = new();
        public Dictionary<string, RocksDatabase> Dbs { get; } = new();
        public Dictionary<string, RaftPersistentState> States { get; } = new();
        public Dictionary<string, ManualTime> Time { get; } = new();

        /// <summary>
        /// <paramref name="prepare"/> gives each node its log before it starts (entries and the terms they belong to).
        /// </summary>
        public static async Task<RaftTrio> StartAsync(Action<string, RocksDatabase, RaftPersistentState> prepare)
        {
            var trio = new RaftTrio();
            foreach (string id in s_ids)
            {
                RocksDatabase db = await RocksDatabase.OpenAsync("/db", DbOpts(new MemoryFileSystem()));
                trio._stateFs[id] = new MemoryFileSystem();
                var state = new RaftPersistentState(trio._stateFs[id], "/raft");
                state.SetMode(ClusterMode.Cluster);
                prepare(id, db, state);
                trio.Dbs[id] = db;
                trio.States[id] = state;
                trio.Time[id] = new ManualTime();
                trio.Nodes[id] = trio.CreateNode(id);
                trio.Nodes[id].Start();
            }
            return trio;
        }

        /// <summary>A restart of <paramref name="id"/>: a new node over the Raft state read back from its file system.</summary>
        public async Task RestartAsync(string id)
        {
            await Nodes[id].DisposeAsync();
            States[id] = new RaftPersistentState(_stateFs[id], "/raft");
            Nodes[id] = CreateNode(id);
        }

        private RaftClusterNode CreateNode(string id)
        {
            var peers = new List<ClusterMember>();
            foreach (string other in s_ids)
            {
                if (other != id) peers.Add(new ClusterMember(other, "mem://" + other));
            }
            return new RaftClusterNode(new RaftConfig(id, "mem://" + id, peers) { TimeProvider = Time[id] }, States[id], Dbs[id], this);
        }

        /// <summary>The vote requests <paramref name="candidate"/> sends from now on wait until the returned source completes.</summary>
        public TaskCompletionSource HoldVotesOf(string candidate)
        {
            var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            lock (_held) _held[candidate] = gate;
            return gate;
        }

        /// <summary>How many vote requests <paramref name="candidate"/> has sent (held ones included).</summary>
        public int VoteRequestsOf(string candidate)
        {
            lock (_voteAnswers) return _voteRequests.GetValueOrDefault(candidate);
        }

        public int VoteAnswersTo(string candidate)
        {
            lock (_voteAnswers) return _voteAnswers.GetValueOrDefault(candidate);
        }

        public int HeartbeatAnswers => Volatile.Read(ref _heartbeatAnswers);

        public async Task<RequestVoteResult?> SendRequestVoteAsync(ClusterMember peer, RequestVotePayload request, CancellationToken cancellationToken)
        {
            TaskCompletionSource? gate;
            lock (_held) _held.TryGetValue(request.CandidateId, out gate);
            lock (_voteAnswers) _voteRequests[request.CandidateId] = _voteRequests.GetValueOrDefault(request.CandidateId) + 1;
            if (gate != null) await gate.Task.WaitAsync(cancellationToken);
            RequestVoteResult r = Nodes[peer.NodeId].HandleRequestVote(request.Term, request.CandidateId, request.LastLogSeq, request.LastLogTerm);
            lock (_voteAnswers) _voteAnswers[request.CandidateId] = _voteAnswers.GetValueOrDefault(request.CandidateId) + 1;
            return r;
        }

        public Task<AppendEntriesResult?> SendHeartbeatAsync(ClusterMember peer, HeartbeatPayload payload, CancellationToken cancellationToken)
        {
            AppendEntriesResult r = Nodes[peer.NodeId].HandleAppendEntries(payload);
            Interlocked.Increment(ref _heartbeatAnswers);
            return Task.FromResult<AppendEntriesResult?>(r);
        }

        public int Leaders(long term)
        {
            int n = 0;
            foreach (RaftClusterNode node in Nodes.Values)
            {
                if (node.Role == RaftRole.Leader && node.CurrentTerm == term) n++;
            }
            return n;
        }

        public async ValueTask DisposeAsync()
        {
            foreach (RaftClusterNode n in Nodes.Values) await n.DisposeAsync();
            foreach (RocksDatabase db in Dbs.Values) await db.DisposeAsync();
        }
    }

    /// <summary>Writes entries up to sequence number <paramref name="through"/> and records that those from <paramref name="from"/> on belong to <paramref name="term"/>.</summary>
    private static void Append(RocksDatabase db, RaftPersistentState state, ulong from, ulong through, long term)
    {
        Assert.AreEqual(from, db.GetLatestSequenceNumber() + 1);
        state.RecordTermRange(from, term);
        for (ulong s = from; s <= through; s++) db.Put(U("t" + term + "-" + s), U("v"));
        Assert.AreEqual(through, db.GetLatestSequenceNumber());
        if (state.CurrentTerm < term) state.UpdateTermAndVote(term, null);
    }

    /// <summary>
    /// Election restriction (Raft 5.4.1): a vote is only granted to a candidate whose log is at least as up to date -
    /// a later last term wins, and with equal last terms the longer log. n3 holds a longer log that ends in an older
    /// term; n1 and n2 hold entries of term 2 (possibly committed). n3 cannot be elected however often it tries, and
    /// its higher term does not keep n1 from winning afterwards.
    /// </summary>
    [TestMethod]
    public async Task CandidateWithALessUpToDateLogIsNotElected()
    {
        await using RaftTrio trio = await RaftTrio.StartAsync(static (id, db, state) =>
        {
            Append(db, state, 1, 3, 1);
            if (id == "n3") Append(db, state, 4, 9, 1);
            else Append(db, state, 4, 5, 2);
            if (state.CurrentTerm < 2) state.UpdateTermAndVote(2, null);
        });
        RaftClusterNode n1 = trio.Nodes["n1"], n3 = trio.Nodes["n3"];

        for (int attempt = 1; attempt <= 2; attempt++)
        {
            trio.Time["n3"].ElapseElectionTimeout();
            int expected = attempt * 2;
            await WaitUntilAsync(() => trio.VoteAnswersTo("n3") == expected, "n3's vote requests to be answered");
            Assert.AreNotEqual(RaftRole.Leader, n3.Role, "a candidate whose log ends in an older term was elected");
            Assert.AreEqual(2L + attempt, n1.CurrentTerm, "the candidate's term is adopted even though the vote is refused");
            Assert.IsNull(trio.States["n1"].VotedFor);
            Assert.IsNull(trio.States["n2"].VotedFor);
        }

        // Directly: same last term but shorter is refused, at least as long is granted.
        long term = n1.CurrentTerm;
        Assert.IsFalse(n1.HandleRequestVote(term, "n2", 4, 2).VoteGranted, "a shorter log of the same last term got the vote");
        Assert.IsFalse(n1.HandleRequestVote(term, "n2", 100, 1).VoteGranted, "a longer log of an older last term got the vote");
        Assert.IsTrue(n1.HandleRequestVote(term, "n2", 5, 2).VoteGranted);
        Assert.IsFalse(n1.HandleRequestVote(term, "n3", 9, 3).VoteGranted, "a second vote in the same term");

        trio.Time["n1"].ElapseElectionTimeout();
        await WaitUntilAsync(() => n1.Role == RaftRole.Leader, "n1 to be elected");
        Assert.AreEqual(1, trio.Leaders(n1.CurrentTerm));
        Assert.AreNotEqual(RaftRole.Leader, n3.Role);
    }

    /// <summary>
    /// Commit rule (Raft 5.4.2, figure 8): a leader does not commit an entry of an earlier term by counting replicas,
    /// even when every node holds it; it becomes committed together with the first entry of the leader's own term
    /// that a quorum holds.
    /// </summary>
    [TestMethod]
    public async Task EntryOfAnEarlierTermIsNotCommittedByCountingReplicas()
    {
        await using RaftTrio trio = await RaftTrio.StartAsync(static (_, db, state) => Append(db, state, 1, 5, 2));
        RaftClusterNode n1 = trio.Nodes["n1"];
        trio.Time["n1"].ElapseElectionTimeout();
        await WaitUntilAsync(() => n1.Role == RaftRole.Leader, "n1 to be elected");
        Assert.AreEqual(3L, n1.CurrentTerm);
        Assert.AreEqual(6UL, trio.Dbs["n1"].GetLatestSequenceNumber(), "the leader's first entry of its term");

        // The first heartbeat round confirms that both followers hold the log through 5 (term 2).
        await WaitUntilAsync(() =>
        {
            foreach (PeerState p in n1.Peers)
            {
                if (p.MatchSeq != 5) return false;
            }
            return true;
        }, "both followers to confirm sequence 5");
        // A progress report for it changes nothing either (and evaluates the commit rule synchronously).
        Assert.IsTrue(n1.RecordFollowerProgress("n2", 5, 3));
        Assert.IsTrue(n1.RecordFollowerProgress("n3", 5, 3));
        Assert.AreEqual(0UL, n1.CommitSeq, "an entry of term 2 was committed in term 3 by counting replicas");

        Assert.IsTrue(n1.RecordFollowerProgress("n2", 6, 3));
        Assert.AreEqual(6UL, n1.CommitSeq, "a quorum holds the term-3 entry: it and everything before it is committed");
    }

    /// <summary>
    /// Election safety: two candidates of the same term, at most one leader. Both voted for themselves; the third
    /// node's vote goes to whoever asks first, is persisted, and is not given again in that term - not to the other
    /// candidate, and not after a restart.
    /// </summary>
    [TestMethod]
    public async Task TwoCandidatesOfOneTermElectAtMostOneLeader()
    {
        await using RaftTrio trio = await RaftTrio.StartAsync(static (_, db, state) => Append(db, state, 1, 4, 1));
        RaftClusterNode n1 = trio.Nodes["n1"], n2 = trio.Nodes["n2"];
        TaskCompletionSource votesOfN1 = trio.HoldVotesOf("n1"), votesOfN2 = trio.HoldVotesOf("n2");
        trio.Time["n1"].ElapseElectionTimeout();
        trio.Time["n2"].ElapseElectionTimeout();
        // Both campaigns are under way: each candidate's two requests are on the wire (held).
        await WaitUntilAsync(() => trio.VoteRequestsOf("n1") == 2 && trio.VoteRequestsOf("n2") == 2, "two candidates asking for votes");
        Assert.AreEqual(RaftRole.Candidate, n1.Role);
        Assert.AreEqual(RaftRole.Candidate, n2.Role);
        Assert.AreEqual(2L, n1.CurrentTerm);
        Assert.AreEqual(2L, n2.CurrentTerm);

        votesOfN1.SetResult();
        await WaitUntilAsync(() => n1.Role == RaftRole.Leader, "n1 to win with n3's vote");
        Assert.AreEqual("n1", trio.States["n3"].VotedFor);
        votesOfN2.SetResult();
        await WaitUntilAsync(() => trio.VoteAnswersTo("n2") == 2, "n2's vote requests to be answered");
        Assert.AreNotEqual(RaftRole.Leader, n2.Role, "two leaders in term 2");
        Assert.AreEqual(1, trio.Leaders(2));
        Assert.AreEqual(2L, n2.CurrentTerm);

        // n3 restarts: its vote of term 2 is on disk.
        await trio.RestartAsync("n3");
        Assert.IsFalse(trio.Nodes["n3"].HandleRequestVote(2, "n2", 4, 1).VoteGranted, "a node voted twice in one term after a restart");
        Assert.IsTrue(trio.Nodes["n3"].HandleRequestVote(2, "n1", 4, 1).VoteGranted, "the same candidate may be answered again");
    }

    /// <summary>
    /// Only members take part in an election. A vote request or a heartbeat of a node that is not in the member list
    /// (a node configured for another cluster, a member list changed on one node only) was handled like a member's: it
    /// got the vote if its log looked newer, and its higher term deposed the leader. Failed before the fix.
    /// </summary>
    [TestMethod]
    public async Task RaftRequestsOfANodeThatIsNotAMemberAreRefused()
    {
        await using RaftTrio trio = await RaftTrio.StartAsync(static (_, db, state) => Append(db, state, 1, 4, 1));
        RaftClusterNode n1 = trio.Nodes["n1"], n2 = trio.Nodes["n2"];
        trio.Time["n1"].ElapseElectionTimeout();
        await WaitUntilAsync(() => n1.Role == RaftRole.Leader && n2.CurrentLeaderId == "n1", "n1 to lead");
        long term = n1.CurrentTerm;
        ulong commit = n2.CommitSeq;

        RequestVoteResult vote = n2.HandleRequestVote(term + 5, "stranger", 1000, term + 5);
        Assert.IsFalse(vote.VoteGranted, "a node outside the member list got a vote");
        Assert.AreEqual(term, n2.CurrentTerm, "a node outside the member list moved the term");
        Assert.AreEqual(term, n1.HandleRequestVote(term + 5, "stranger", 1000, term + 5).Term);
        Assert.AreEqual(RaftRole.Leader, n1.Role, "a node outside the member list deposed the leader");

        AppendEntriesResult beat = n2.HandleAppendEntries(new HeartbeatPayload(term + 5, "stranger", 0, 0, 1000, ClusterMode.Cluster));
        Assert.IsFalse(beat.Success);
        Assert.AreEqual(term, n2.CurrentTerm);
        Assert.AreEqual("n1", n2.CurrentLeaderId);
        Assert.AreEqual(commit, n2.CommitSeq);
    }

    // ---------------------------------------------------------------------------------------------------
    // Switchover

    /// <summary>
    /// The handoff point is taken only after every admitted write has reached the log, and nothing is admitted after
    /// it: with one write in the middle of its fsync, the handoff holds (no promote goes out), a second write waits at
    /// the gate, and once the first one finished the promote names exactly its sequence number. The second write is
    /// refused with a hint to the new primary and is in neither log.
    /// </summary>
    [TestMethod]
    public async Task HandoffPointIsTakenAfterTheAdmittedWritesAndNoWriteIsAcceptedAfterIt()
    {
        var gated = new GatedFileSystem(new MemoryFileSystem());
        var fsB = new MemoryFileSystem();
        await using var h = new PairHarness { FileSystemOf = id => id == "A" ? gated : fsB, SyncWal = true };
        PairNodeHost a = await h.StartAsync("A");
        PairNodeHost b = await h.StartAsync("B");
        await WaitUntilAsync(() => b.GetStatus().AttachedFromSeq > 0, "B following");
        for (int i = 0; i < 5; i++) Assert.AreEqual(PairWriteStatus.Applied, (await PutAsync(a, "k" + i)).Status);
        await WaitUntilAsync(() => b.Db.GetLatestSequenceNumber() == a.Db.GetLatestSequenceNumber(), "B caught up");
        ulong before = a.Db.GetLatestSequenceNumber();

        gated.Arm();
        Task<PairWriteResult> admitted = PutAsync(a, "admitted");
        await gated.Blocked.WaitAsync(TestUtil.Timeout);
        Task<SwitchoverResult> handoff = a.HandOffAsync(CancellationToken.None);
        await WaitUntilAsync(() => a.Node.Writes.IsHolding, "the handoff to hold writes");
        Task<PairWriteResult> late = PutAsync(a, "late");
        await WaitUntilAsync(() => a.Node.Role == ReplicationRole.HandingOff, "A to be handing off");

        Assert.AreEqual(1, a.Node.Writes.InFlight, "only the write admitted before the hold is in flight");
        Assert.IsFalse(late.IsCompleted, "a write was admitted (or refused) while the gate holds");
        Assert.IsFalse(handoff.IsCompleted);
        Assert.IsNull(h.AtoB.LastPromote, "the promote went out while an admitted write had not reached the log");
        Assert.AreEqual(ReplicationRole.Replica, b.Node.Role);

        gated.Release();
        PairWriteResult first = await admitted.WaitAsync(TestUtil.Timeout);
        SwitchoverResult result = await handoff.WaitAsync(TestUtil.Timeout);
        PairWriteResult second = await late.WaitAsync(TestUtil.Timeout);

        Assert.AreEqual(PairWriteStatus.Applied, first.Status);
        Assert.AreEqual(before + 1, first.Sequence);
        Assert.AreEqual(SwitchoverOutcome.Completed, result.Outcome, result.Reason + "\n" + h.LogText);
        Assert.AreEqual(first.Sequence, result.HandoffSeq, "the handoff point is the last admitted write");
        Assert.AreEqual(first.Sequence, h.AtoB.LastPromote!.HandoffSeq);
        Assert.AreEqual(PairWriteStatus.NotPrimary, second.Status, "a write was accepted after the handoff point was taken");
        Assert.AreEqual("B", second.PrimaryHint);
        Assert.AreEqual(result.HandoffSeq, a.Db.GetLatestSequenceNumber(), "the old primary's log grew past the handoff point");
        Assert.AreEqual(ReplicationRole.Primary, b.Node.Role);
        Assert.IsFalse(Contents(a.Db).ContainsKey("late"));
        Assert.IsFalse(Contents(b.Db).ContainsKey("late"));
        Assert.IsTrue(Contents(b.Db).ContainsKey("admitted"));
    }

    private static async Task<PairWriteResult> PutAsync(PairNodeHost host, string key)
    {
        using var batch = new WriteBatch();
        batch.Put(U(key), U("v-" + key));
        return await host.WriteAsync(batch, TimeSpan.FromSeconds(10), CancellationToken.None);
    }
}
