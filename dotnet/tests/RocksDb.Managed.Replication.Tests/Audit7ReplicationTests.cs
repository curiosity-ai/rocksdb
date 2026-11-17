using System.Runtime.CompilerServices;
using RocksDb.Managed.Cluster;
using RocksDb.Managed.Replication.Hosting;
using RocksDb.Managed.Replication.Switchover;

namespace RocksDb.Managed.Replication.Tests;

/// <summary>
/// Audit #7 (failure modes and edge cases) of the replication layer. The reference is rocksdb-sharp (its
/// <c>Replication/</c>, <c>Switchover/</c>, <c>Cluster/</c> folders and the hosts under <c>Tests/</c>); the classes it ships
/// in the RocksDB NuGet package (namespace <c>RocksDbSharp</c>) are run side by side where the scenario allows.
/// </summary>
[TestClass]
public sealed class Audit7ReplicationTests
{
    // ---------------------------------------------------------------------------------------------------
    // ProgressReporter

    /// <summary>
    /// A progress report whose send fails with an <see cref="OperationCanceledException"/> of its own (a transport's
    /// internal timeout, a call cancelled by a broken connection) is a failed report like any other. The reporter took
    /// it for its own shutdown and ended its loop: every later report was dropped, so a Raft leader could no longer
    /// commit, a pair with <see cref="PairWriteAck.Replica"/> answered NotReplicated, and plain replication never
    /// retired an archived WAL again. Reference: rocksdb-sharp's hosts report on a timer that swallows every failure of
    /// the call (Tests/ClusterTest/ClusterNodeHost.cs: <c>try { await rclient.ReportLastSyncSequenceNumber(...); } catch { }</c>).
    /// </summary>
    [TestMethod]
    public async Task ProgressReporterKeepsReportingAfterASendThatThrowsItsOwnCancellation()
    {
        var delivered = new List<ulong>();
        int calls = 0;
        var second = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var reporter = new ProgressReporter((seq, ct) =>
        {
            if (Interlocked.Increment(ref calls) == 1) throw new TaskCanceledException("the transport timed the call out");
            lock (delivered) delivered.Add(seq);
            second.TrySetResult();
            return Task.CompletedTask;
        }, TimeSpan.FromMilliseconds(1));

        reporter.Report(5);
        Task done = await Task.WhenAny(second.Task, Task.Delay(TestUtil.Timeout));
        Assert.AreSame(second.Task, done, "the report was never sent again after a send threw OperationCanceledException");
        lock (delivered) CollectionAssert.AreEqual(new ulong[] { 5 }, delivered);

        // And the loop is still alive for later reports.
        reporter.Report(9);
        await WaitUntilAsync(() =>
        {
            lock (delivered) return delivered.Count == 2;
        }, "the next report");
        lock (delivered) Assert.AreEqual(9UL, delivered[1]);
    }

    // ---------------------------------------------------------------------------------------------------
    // Plain replication: a transport that duplicates, loses or reorders messages, and cancellation

    /// <summary>
    /// In-process plain-replication transport whose <see cref="Mutate"/> turns the n-th message of a stream (copied, as
    /// serialization would) into zero or more messages: a duplicate, a lost message, a reordering.
    /// </summary>
    private sealed class FaultyClient(ReplicationPrimary primary) : IReplicationSourceClient
    {
        public Func<int, WalStreamMessage, IEnumerable<WalStreamMessage>>? Mutate;

        public IAsyncEnumerable<SnapshotChunk> StreamInitialStateAsync(CancellationToken cancellationToken) => primary.StreamInitialStateAsync(cancellationToken);

        public async IAsyncEnumerable<WalStreamMessage> StreamUpdatesAsync(ulong startSeq, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            int n = 0;
            WalStreamMessage? held = null;
            await foreach (WalStreamMessage m in primary.StreamUpdatesAsync(startSeq, cancellationToken))
            {
                WalStreamMessage copy = m.Detach();
                IEnumerable<WalStreamMessage> output = Mutate is null ? [copy] : Mutate(n++, copy);
                foreach (WalStreamMessage o in output)
                {
                    if (held is { } h && !o.ResyncRequired)
                    {
                        held = null;
                        yield return o;
                        yield return h;
                        continue;
                    }
                    yield return o;
                }
                if (Mutate is not null && HoldNext) { held = copy; HoldNext = false; }
            }
        }

        /// <summary>Set by <see cref="Mutate"/> to send the current message after the next one (a reordering).</summary>
        public bool HoldNext;

        public Task ReportProgressAsync(int replicaIndex, ulong appliedThrough, CancellationToken cancellationToken)
        {
            primary.ReportProgress(replicaIndex, appliedThrough);
            return Task.CompletedTask;
        }
    }

    private static async Task<(RocksDatabase Source, RocksDatabase Replica, ReplicationPrimary Primary)> OpenPlainAsync(TempDir dir)
    {
        RocksDatabase source = await RocksDatabase.OpenAsync(dir.Sub("primary"), DbOpts(null));
        RocksDatabase replica = await RocksDatabase.OpenAsync(dir.Sub("replica"), DbOpts(null));
        // Small chunks: every few batches make one message, so a stream has many.
        var primary = new ReplicationPrimary(source, dir.Sub("checkpoints"), null) { MaxChunkBytes = 256 };
        return (source, replica, primary);
    }

    /// <summary>Follows until the stream fails or the replica reaches <paramref name="target"/>; returns the failure (null when it got there).</summary>
    private static async Task<Exception?> FollowAsync(IReplicationSourceClient client, RocksDatabase replica, ulong target)
    {
        using var cts = new CancellationTokenSource(TestUtil.Timeout);
        Task<long> follow = ReplicaFollower.FollowAsync(client, replica, 0, cts.Token);
        Task<bool> reached = replica.WaitForSequenceAsync(target, cts.Token).AsTask();
        await Task.WhenAny(follow, reached);
        await cts.CancelAsync();
        try
        {
            await follow;
            return null;
        }
        catch (OperationCanceledException)
        {
            Assert.AreEqual(target, replica.GetLatestSequenceNumber(), "the follower neither failed nor reached the source");
            return null;
        }
        catch (Exception e)
        {
            return e;
        }
    }

    /// <summary>
    /// A transport that delivers a message twice, loses one, or swaps two: the follower applies nothing out of order -
    /// <see cref="ReplicationConsumer.TryIngestChunkAsync"/> refuses a chunk that does not start at the replica's next
    /// sequence number - and ends with <see cref="ReplicationResyncRequiredException"/>; the replica holds a prefix of the
    /// source's log (each chunk exactly once), and following again from its position converges. Reference: rocksdb-sharp
    /// <c>ReplicationConsumer.IngestChunk</c> writes whatever it is given (no check; its hosts check the sequence number
    /// themselves, Tests/ReplicationTest/Program.cs).
    /// </summary>
    [TestMethod]
    [DataRow("duplicate")]
    [DataRow("lose")]
    [DataRow("reorder")]
    public async Task PlainFollowerAppliesEachChunkOnceWhateverTheTransportDoes(string fault)
    {
        using var dir = new TempDir("a7-plain-" + fault);
        (RocksDatabase source, RocksDatabase replica, ReplicationPrimary primary) = await OpenPlainAsync(dir);
        await using (source)
        await using (replica)
        {
            await WriteWorkloadAsync(source, 0, 200, 1);
            ulong target = source.GetLatestSequenceNumber();
            var client = new FaultyClient(primary);
            ulong faultAt = 0;
            client.Mutate = (n, m) =>
            {
                if (n != 3) return [m];
                faultAt = m.FirstSequence;
                switch (fault)
                {
                    case "duplicate": return [m, m];
                    case "lose": return [];
                    default:
                        client.HoldNext = true;
                        return [];
                }
            };

            Exception? failure = await FollowAsync(client, replica, target);
            Assert.IsInstanceOfType<ReplicationResyncRequiredException>(failure, "fault " + fault);
            ulong held = replica.GetLatestSequenceNumber();
            // duplicate: the chunk was applied once, the copy refused; lose / reorder: nothing from the fault on.
            if (fault == "duplicate") Assert.IsTrue(held >= faultAt, "the chunk before the duplicate was not applied");
            else Assert.AreEqual(faultAt - 1, held, "something after the missing chunk was applied");

            // Following again (a healthy transport) from the replica's own position converges.
            Assert.IsNull(await FollowAsync(new FaultyClient(primary), replica, target));
            AssertReplicaEquals(source, replica, "after the " + fault);
        }
    }

    /// <summary>
    /// Cancelling a follower while it waits for the next write ends it promptly - the source's stream ends at the
    /// cancellation, so FollowAsync returns its chunk count (a transport may surface an
    /// <see cref="OperationCanceledException"/> instead) - with no half-applied chunk, and a new follower continues
    /// from the replica's position.
    /// </summary>
    [TestMethod]
    public async Task CancelledFollowerLeavesTheReplicaAtAChunkBoundaryAndCanFollowAgain()
    {
        using var dir = new TempDir("a7-cancel");
        (RocksDatabase source, RocksDatabase replica, ReplicationPrimary primary) = await OpenPlainAsync(dir);
        await using (source)
        await using (replica)
        {
            for (int round = 0; round < 3; round++)
            {
                await WriteWorkloadAsync(source, round * 100, round * 100 + 100, round);
                ulong target = source.GetLatestSequenceNumber();
                using var cts = new CancellationTokenSource(TestUtil.Timeout);
                Task<long> follow = ReplicaFollower.FollowAsync(new FaultyClient(primary), replica, 0, cts.Token);
                await WaitForSequenceAsync(replica, target, "round " + round);
                await cts.CancelAsync();
                try
                {
                    Assert.IsTrue(await follow.WaitAsync(TestUtil.Timeout) > 0, "round " + round);
                }
                catch (OperationCanceledException)
                {
                }
                AssertReplicaEquals(source, replica, "round " + round);
            }
        }
    }

    // ---------------------------------------------------------------------------------------------------
    // Raft cluster: a resync that cannot start

    /// <summary>
    /// Forwards to an inner file system; while <see cref="FailLogEnd"/> is set, writing raft.logend fails; while
    /// <see cref="FailWalClose"/> is set, closing the next WAL file (*.log) fails after it was closed.
    /// </summary>
    private sealed class LogEndFailingFileSystem(IFileSystem inner) : IFileSystem
    {
        public volatile bool FailLogEnd;
        public volatile bool FailWalClose;
        public int Failures;

        private IWritableFile Wrap(string path, IWritableFile f) => path.EndsWith(".log", StringComparison.Ordinal) ? new Writable(this, f) : f;

        private sealed class Writable(LogEndFailingFileSystem fs, IWritableFile inner) : IWritableFile
        {
            public long Size => inner.Size;
            public void Append(ReadOnlySpan<byte> data) => inner.Append(data);
            public ValueTask AppendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default) => inner.AppendAsync(data, cancellationToken);
            public ValueTask FlushAsync(CancellationToken cancellationToken = default) => inner.FlushAsync(cancellationToken);
            public ValueTask SyncAsync(CancellationToken cancellationToken = default) => inner.SyncAsync(cancellationToken);
            public void Flush() => inner.Flush();
            public void Sync() => inner.Sync();

            private void FailOnce()
            {
                if (!fs.FailWalClose) return;
                fs.FailWalClose = false;
                Interlocked.Increment(ref fs.Failures);
                throw new RocksDbException(StatusCode.IOError, "IO error: injected failure closing a WAL");
            }

            public void Dispose()
            {
                inner.Dispose();
                FailOnce();
            }

            public async ValueTask DisposeAsync()
            {
                await inner.DisposeAsync();
                FailOnce();
            }
        }

        private void Check(string path)
        {
            if (FailLogEnd && path.Contains("raft.logend", StringComparison.Ordinal))
            {
                Interlocked.Increment(ref Failures);
                throw new RocksDbException(StatusCode.IOError, "IO error: injected failure writing " + path);
            }
        }

        public IWritableFile CreateWritable(string path, bool truncate = true)
        {
            Check(path);
            return Wrap(path, inner.CreateWritable(path, truncate));
        }

        public IWritableFile CreateWritable(string path, bool truncate, WritableFileOptions options)
        {
            Check(path);
            return Wrap(path, inner.CreateWritable(path, truncate, options));
        }

        public IWritableFile ReopenWritable(string path, WritableFileOptions options) => Wrap(path, inner.ReopenWritable(path, options));
        public IWritableFile ReuseWritable(string path, string oldPath, WritableFileOptions options) => Wrap(path, inner.ReuseWritable(path, oldPath, options));
        public IRandomAccessFile OpenRandomAccess(string path) => inner.OpenRandomAccess(path);
        public IRandomAccessFile OpenRandomAccess(string path, RandomAccessFileOptions options) => inner.OpenRandomAccess(path, options);
        public ISequentialFile OpenSequential(string path) => inner.OpenSequential(path);
        public ISequentialFile OpenSequential(string path, SequentialFileOptions options) => inner.OpenSequential(path, options);
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
        public void LinkFile(string source, string target) => inner.LinkFile(source, target);
        public DateTimeOffset GetFileModificationTime(string path) => inner.GetFileModificationTime(path);
        public long GetFreeSpace(string path) => inner.GetFreeSpace(path);
        public void WriteAt(string path, long offset, ReadOnlySpan<byte> data) => inner.WriteAt(path, offset, data);
    }

    /// <summary>Forwards to an inner client; while <see cref="DuplicateNext"/> is set, the next whole chunk of a WAL stream arrives twice.</summary>
    private sealed class DuplicatingClusterClient(IClusterNodeClient inner) : IClusterNodeClient
    {
        public volatile bool DuplicateNext;

        public string NodeId => inner.NodeId;
        public Task<RequestVoteResult?> RequestVoteAsync(RequestVotePayload request, CancellationToken cancellationToken) => inner.RequestVoteAsync(request, cancellationToken);
        public Task<AppendEntriesResult?> HeartbeatAsync(HeartbeatPayload payload, CancellationToken cancellationToken) => inner.HeartbeatAsync(payload, cancellationToken);
        public Task<ClusterNodeStatus?> GetStatusAsync(CancellationToken cancellationToken) => inner.GetStatusAsync(cancellationToken);
        public Task<ClusterAttachResult?> CheckLogConsistencyAsync(string followerId, ulong latestSeq, long termAtLatest, CancellationToken cancellationToken) =>
            inner.CheckLogConsistencyAsync(followerId, latestSeq, termAtLatest, cancellationToken);

        public async IAsyncEnumerable<WalStreamMessage> StreamUpdatesAsync(string followerId, ulong startSeq, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await foreach (WalStreamMessage m in inner.StreamUpdatesAsync(followerId, startSeq, cancellationToken))
            {
                yield return m;
                if (!m.ResyncRequired && !m.IsFragment && DuplicateNext)
                {
                    DuplicateNext = false;
                    yield return m;
                }
            }
        }

        public Task ReportProgressAsync(string followerId, ulong appliedThrough, long leaderTerm, CancellationToken cancellationToken) =>
            inner.ReportProgressAsync(followerId, appliedThrough, leaderTerm, cancellationToken);
        public Task<IReadOnlyDictionary<string, string>> GetFileHashesAsync(IReadOnlyList<ReplicationFileInfo> candidates, CancellationToken cancellationToken) =>
            inner.GetFileHashesAsync(candidates, cancellationToken);
        public IAsyncEnumerable<SnapshotChunk> StreamSnapshotAsync(IReadOnlyList<ReplicationFileInfo> verifiedInventory, CancellationToken cancellationToken) =>
            inner.StreamSnapshotAsync(verifiedInventory, cancellationToken);
        public Task<string?> GetTermsAsync(CancellationToken cancellationToken) => inner.GetTermsAsync(cancellationToken);
    }

    private static async Task WriteCommittedAsync(ClusterHarness c, int from, int to)
    {
        for (int i = from; i < to; i++)
        {
            while (true)
            {
                ClusterNodeHost leader = await c.WaitForLeaderAsync(null);
                if (await leader.WriteCommittedAsync(U(K(i)), U(V(i, 0)), TimeSpan.FromSeconds(5), CancellationToken.None)) break;
            }
        }
    }

    /// <summary>
    /// A follower whose stream showed a divergence (here: a chunk delivered twice) ends its stream and asks for a resync.
    /// When the resync cannot start - the log end it must record before dropping its log cannot be written - the node
    /// keeps its database, as it should, but nothing ever attached it to the leader again: no stream ran until the
    /// leader changed, and the follower stayed behind for good under a stable leader. Reference: rocksdb-sharp's
    /// Tests/ClusterTest/ClusterNodeHost.PerformResyncAsync always ends a resync with <c>EnsureFollowerStream</c> (its
    /// BeginResync cannot fail; the durable log end is a managed addition), so the node rejoins.
    /// </summary>
    [TestMethod]
    public async Task FollowerWhoseResyncCannotStartFollowsTheLeaderAgain()
    {
        (ClusterHarness c, ClusterNodeHost n1, ClusterNodeHost n3, LogEndFailingFileSystem n3Fs, DuplicatingClusterClient n3ToN1) =
            await StartClusterWithFaultyN3Async();
        await using ClusterHarness _ = c;
        n3Fs.FailLogEnd = true;
        n3ToN1.DuplicateNext = true;
        await WriteCommittedAsync(c, 60, 80);
        await WaitUntilAsync(() => c.LogText.Contains("n3: resync not started", StringComparison.Ordinal), "n3's failed resync\n" + c.LogText);
        Assert.AreEqual(0, n3.Resyncs);

        // The disk works again: n3 must rejoin (and rebuild, since its stream showed a divergence) without a leader change.
        n3Fs.FailLogEnd = false;
        await WriteCommittedAsync(c, 80, 100);
        Assert.AreEqual("n1", (await c.WaitForLeaderAsync(null)).NodeId);
        await c.AssertConvergedAsync(n1);
    }

    /// <summary>
    /// A resync closes the follower's database before it restores the snapshot. When that close reports an error (here:
    /// closing the WAL file fails; the close itself completes and releases the LOCK, see DbImpl.CloseAsync), the
    /// exception escaped the resync task: the node stayed "resyncing" for good - no database, no stream, every heartbeat
    /// refused - until restarted. Reference: rocksdb-sharp's PerformResyncAsync closes with <c>Db.Dispose()</c>, which
    /// never throws (rocksdb_close returns no status), and goes on to pull the snapshot.
    /// </summary>
    [TestMethod]
    public async Task ResyncGoesOnWhenClosingTheOldDatabaseReportsAnError()
    {
        (ClusterHarness c, ClusterNodeHost n1, ClusterNodeHost n3, LogEndFailingFileSystem n3Fs, DuplicatingClusterClient n3ToN1) =
            await StartClusterWithFaultyN3Async();
        await using (c)
        {
            n3Fs.FailWalClose = true;
            n3ToN1.DuplicateNext = true;
            await WriteCommittedAsync(c, 60, 80);
            await WaitUntilAsync(() => n3.Resyncs == 1, "n3's resync\n" + c.LogText);
            Assert.AreEqual(1, n3Fs.Failures, "the close of the old database did not fail as injected\n" + c.LogText);
            await WriteCommittedAsync(c, 80, 100);
            await c.AssertConvergedAsync(n1);
        }
    }

    /// <summary>The same for a pair's replica (PairNodeHost.PerformResyncAsync closes its database the same way).</summary>
    [TestMethod]
    public async Task PairResyncGoesOnWhenClosingTheOldDatabaseReportsAnError()
    {
        var memory = new MemoryFileSystem();
        var replicaFs = new LogEndFailingFileSystem(memory);
        await using var h = new PairHarness { FileSystemOf = id => id == "B" ? replicaFs : memory };
        await h.StartAsync("A");
        await h.StartAsync("B");
        using (WriteBatch b = new WriteBatch().Put(U("before"), U("1")))
            Assert.AreEqual(PairWriteStatus.Applied, (await h["A"].WriteAsync(b, TimeSpan.FromSeconds(2), CancellationToken.None)).Status);
        await h.AssertConvergedAsync("A", "B");

        replicaFs.FailWalClose = true;
        h["B"].TriggerResync("audit #7");
        await h["B"].WaitForResyncAsync().WaitAsync(TestUtil.Timeout);
        Assert.AreEqual(1, replicaFs.Failures, "the close of the old database did not fail as injected\n" + h.LogText);
        Assert.AreEqual(1, h["B"].Resyncs, h.LogText);
        using (WriteBatch b = new WriteBatch().Put(U("after"), U("2")))
            Assert.AreEqual(PairWriteStatus.Applied, (await h["A"].WriteAsync(b, TimeSpan.FromSeconds(2), CancellationToken.None)).Status);
        await h.AssertConvergedAsync("A", "B");
    }

    /// <summary>
    /// Three nodes on one <see cref="MemoryFileSystem"/>, n1 leading, 60 entries committed and converged; n3's file system
    /// and its client to n1 can inject faults.
    /// </summary>
    private static async Task<(ClusterHarness, ClusterNodeHost N1, ClusterNodeHost N3, LogEndFailingFileSystem, DuplicatingClusterClient)>
        StartClusterWithFaultyN3Async()
    {
        LogEndFailingFileSystem? n3Fs = null;
        DuplicatingClusterClient? n3ToN1 = null;
        var c = new ClusterHarness(3, new MemoryFileSystem())
        {
            WrapFileSystem = (id, fs) => id == "n3" ? n3Fs = new LogEndFailingFileSystem(fs) : fs,
            WrapClient = (id, peer, client) => id == "n3" && peer == "n1" ? n3ToN1 = new DuplicatingClusterClient(client) : client,
        };
        ClusterNodeHost n1 = await c.StartAsync("n1");
        for (int i = 0; i < 50; i++) Assert.IsTrue(await n1.TryWriteAsLeaderAsync(U(K(i)), U(V(i, 0)), CancellationToken.None));
        await c.StartAsync("n2");
        ClusterNodeHost n3 = await c.StartAsync("n3");
        await WaitUntilAsync(() => c.Statuses().TrueForAll(s => s.Mode == ClusterMode.Cluster), "every node in Cluster mode\n" + c.LogText);
        await WriteCommittedAsync(c, 50, 60);
        await c.AssertConvergedAsync(n1);
        return (c, n1, n3, n3Fs!, n3ToN1!);
    }

    // ---------------------------------------------------------------------------------------------------
    // Reference behavior (recorded, not changed)

    /// <summary>
    /// A write that waits on a held gate with <see cref="System.Threading.Timeout.InfiniteTimeSpan"/> is refused at once instead of
    /// waiting: the deadline is computed as now + (-1 ms). Reference behavior: rocksdb-sharp's
    /// <c>PrimaryWriteGate.TryEnterAsync</c> computes <c>Environment.TickCount64 + (long)holdTimeout.TotalMilliseconds</c>
    /// the same way and gives the same answer (run side by side here).
    /// </summary>
    [TestMethod]
    public async Task HeldGateRefusesAnInfiniteHoldTimeoutAtOnceLikeTheReference()
    {
        var gate = new PrimaryWriteGate(open: true);
        await gate.HoldAsync(CancellationToken.None);
        Assert.IsFalse(await gate.TryEnterAsync(System.Threading.Timeout.InfiniteTimeSpan, CancellationToken.None));

        var reference = new RocksDbSharp.PrimaryWriteGate(open: true);
        await reference.HoldAsync();
        Assert.IsFalse(await reference.TryEnterAsync(System.Threading.Timeout.InfiniteTimeSpan));
    }

    /// <summary>
    /// A <c>raft.state</c> whose content does not parse (not a read error: those are thrown) loads as term 0 with the
    /// vote of its second line. Reference behavior: rocksdb-sharp's <c>RaftPersistentState.Load</c> parses the same way
    /// ("corrupt file; start fresh"). The file is only ever replaced atomically (temp file, fsync, rename), so this takes a
    /// damaged medium or a foreign writer.
    /// </summary>
    [TestMethod]
    public void UnparsableRaftStateLoadsAsTermZeroLikeTheReference()
    {
        using var dir = new TempDir("a7-raftstate");
        string managedDir = dir.Sub("managed"), referenceDir = dir.Sub("reference");
        Directory.CreateDirectory(managedDir);
        Directory.CreateDirectory(referenceDir);
        foreach (string d in new[] { managedDir, referenceDir }) File.WriteAllText(Path.Combine(d, "raft.state"), "\0\0\0\nnode-b\n1\n");

        var managed = new RaftPersistentState(LocalFileSystem.Instance, managedDir);
        var reference = new RocksDbSharp.RaftPersistentState(referenceDir);
        Assert.AreEqual(0, managed.CurrentTerm);
        Assert.AreEqual(reference.CurrentTerm, managed.CurrentTerm);
        Assert.AreEqual(reference.VotedFor, managed.VotedFor);
    }
}
