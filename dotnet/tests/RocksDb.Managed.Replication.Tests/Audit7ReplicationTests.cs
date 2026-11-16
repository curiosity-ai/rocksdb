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
