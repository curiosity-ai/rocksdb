using System.Runtime.CompilerServices;
using System.Reflection;
using RocksDb.Managed.Cluster;
using RocksDb.Managed.Replication.Hosting;

namespace RocksDb.Managed.Replication.Tests;

/// <summary>
/// Steady-state allocation bounds and functional coverage of the per-batch replication paths: WAL retention checks on
/// progress reports, the coalescing progress reporter, the tailer -> consumer pipeline, Raft progress / commit waits /
/// heartbeat answers and the pooled commit timeout of <see cref="ClusterNodeHost.WriteCommittedAsync"/>.
/// </summary>
[TestClass]
public sealed class AllocationTests
{
    private static readonly bool Optimized =
        typeof(ReplicationConsumer).Assembly.GetCustomAttribute<System.Diagnostics.DebuggableAttribute>() is not { IsJITOptimizerDisabled: true };

    private static readonly byte[] Value100 = new byte[100];

    private static RocksDatabase OpenQuiet(IFileSystem fs, string path, bool archive)
    {
        fs.CreateDirectoryIfMissing(path);
        return RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true, FileSystem = fs, WalTtlSeconds = archive ? 3600UL : 0UL },
            new ColumnFamilyOptions { WriteBufferSize = 256UL << 20 });
    }

    private static int ArchivedCount(RocksDatabase db)
    {
        int n = 0;
        foreach (WalFileInfo f in db.GetSortedWalFiles()) n += f.Type == WalFileType.ArchivedLogFile ? 1 : 0;
        return n;
    }

    /// <summary>Bytes allocated on this thread by <paramref name="ops"/> calls, after <paramref name="warmup"/> calls.</summary>
    private static long MeasureThread(Action<int> op, int warmup, int ops)
    {
        for (int i = 0; i < warmup; i++) op(i);
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < ops; i++) op(warmup + i);
        return GC.GetAllocatedBytesForCurrentThread() - before;
    }

    private static async Task<double> MeasureAllThreadsAsync(Func<int, ValueTask> op, int warmup, int ops)
    {
        for (int i = 0; i < warmup; i++) await op(i);
        long before = GC.GetTotalAllocatedBytes(precise: true);
        for (int i = 0; i < ops; i++) await op(warmup + i);
        return (GC.GetTotalAllocatedBytes(precise: true) - before) / (double)ops;
    }

    // =====================================================================================================
    // WAL retention on progress reports
    // =====================================================================================================

    [TestMethod]
    public async Task ProgressReportsListWalsOnlyWhenOneCanBeRetired()
    {
        var fs = new MemoryFileSystem();
        using RocksDatabase db = OpenQuiet(fs, "/p", archive: true);
        var primary = new ReplicationPrimary(db, "/checkpoints", null);
        int k = 0;
        db.Put(Key(k++), Value100);
        db.Flush();
        db.Put(Key(k++), Value100);                    // in a new WAL
        IReadOnlyList<WalFileInfo> files = db.GetSortedWalFiles();
        Assert.AreEqual(2, files.Count, string.Join(", ", files));
        ulong endOfFirst = files[1].StartSequence - 1;
        if (files[0].Type == WalFileType.AliveLogFile)
        {
            // Applied but not archived yet (the database archives it after the flush): retired once it is, without a
            // rotation in between.
            // (The background archival may also win the race with this first report.)
            if (primary.ReportProgress(0, endOfFirst) == 0)
            {
                await WaitUntilAsync(() => ArchivedCount(db) == 1, "the flushed WAL archived");
                Assert.AreEqual(1, primary.ReportProgress(0, endOfFirst));
            }
            Assert.AreEqual(0, ArchivedCount(db));
            primary.Source.ForgetReplica("replica-0");
            db.Flush();
            db.Put(Key(k++), Value100);
            files = db.GetSortedWalFiles();
            endOfFirst = files[1].StartSequence - 1;
        }
        await WaitUntilAsync(() => ArchivedCount(db) == 1, "the flushed WAL archived");
        Assert.AreEqual(0, primary.ReportProgress(0, endOfFirst - 1), "not applied yet");
        Assert.AreEqual(0, primary.ReportProgress(0, endOfFirst - 1), "still not (no listing needed)");
        Assert.AreEqual(1, primary.ReportProgress(0, endOfFirst), "retired once applied");
        Assert.AreEqual(0, ArchivedCount(db));

        // Steady state at the tip: no WAL can be retired, so a report lists nothing and allocates nothing (the writes
        // in between are not measured: the memory file system grows the WAL buffer).
        long bytes = 0;
        for (int i = 0; i < 1100; i++)
        {
            db.Put(Key(k++), Value100);
            long before = GC.GetAllocatedBytesForCurrentThread();
            Assert.AreEqual(0, primary.ReportProgress(0, db.GetLatestSequenceNumber()));
            if (i >= 100) bytes += GC.GetAllocatedBytesForCurrentThread() - before;
        }
        // Before: a WAL directory listing per report (~2.6 KB).
        if (Optimized) Assert.AreEqual(0L, bytes, "1000 Put + ReportProgress at the tip");
        else Assert.IsTrue(bytes < 1000 * 200, bytes + " B for 1000 ReportProgress");

        // A rotation is noticed without a listing per report: the archived WAL is retired as soon as it is applied.
        primary.Source.ForgetReplica("replica-0");
        db.Flush();
        db.Put(Key(k++), Value100);
        await WaitUntilAsync(() => ArchivedCount(db) == 1, "the flushed WAL archived");
        ulong endOfSecond = db.GetSortedWalFiles()[1].StartSequence - 1;
        Assert.AreEqual(0, primary.ReportProgress(0, endOfSecond - 1), "not applied yet (the rotation forced a listing)");
        Assert.AreEqual(0, primary.ReportProgress(0, endOfSecond - 1));
        Assert.AreEqual(1, primary.ReportProgress(0, endOfSecond));
        Assert.AreEqual(0, ArchivedCount(db));

        // A second replica that lags holds WALs back until it catches up.
        db.Flush();
        db.Put(Key(k++), Value100);
        await WaitUntilAsync(() => ArchivedCount(db) == 1, "the flushed WAL archived");
        ulong tip = db.GetLatestSequenceNumber();
        Assert.AreEqual(0, primary.ReportProgress(1, 1));
        Assert.AreEqual(0, primary.ReportProgress(0, tip));
        Assert.AreEqual(1, ArchivedCount(db));
        Assert.AreEqual(1, primary.ReportProgress(1, tip));
        Assert.AreEqual(0, ArchivedCount(db));
    }

    // =====================================================================================================
    // ProgressReporter
    // =====================================================================================================

    [TestMethod]
    public async Task ProgressReporterDeliversTheNewestValueAndStopsOnDispose()
    {
        long sent = 0;
        int calls = 0;
        var gate = new SemaphoreSlim(0);
        var reporter = new ProgressReporter(async (seq, ct) =>
        {
            Interlocked.Increment(ref calls);
            await gate.WaitAsync(ct);
            Volatile.Write(ref sent, (long)seq);
        });
        reporter.Report(1);
        await WaitUntilAsync(() => Volatile.Read(ref calls) == 1, "first send started");
        // While the first send is in flight, newer reports coalesce into one.
        for (ulong s = 2; s <= 50; s++) reporter.Report(s);
        gate.Release();
        await WaitUntilAsync(() => Volatile.Read(ref calls) == 2, "the coalesced send started");
        gate.Release();
        await WaitUntilAsync(() => Volatile.Read(ref sent) == 50, "newest value sent");
        Assert.AreEqual(2, Volatile.Read(ref calls), "49 reports during one send coalesce into one");

        // A failing callback does not stop the loop.
        long sentAfterFailure = 0;
        var failing = new ProgressReporter((seq, ct) =>
        {
            if (seq == 1) throw new IOException("lost");
            Volatile.Write(ref sentAfterFailure, (long)seq);
            return Task.CompletedTask;
        });
        failing.Report(1);
        await Task.Delay(20);
        failing.Report(2);
        await WaitUntilAsync(() => Volatile.Read(ref sentAfterFailure) == 2, "report after a failed send");
        await failing.DisposeAsync();

        await reporter.DisposeAsync();
        reporter.Report(99); // ignored after disposal
        Assert.AreEqual(50, Volatile.Read(ref sent));
    }

    [TestMethod]
    public async Task ProgressReportsAllocateNothingInSteadyState()
    {
        long sent = 0;
        await using var reporter = new ProgressReporter((seq, _) =>
        {
            Volatile.Write(ref sent, (long)seq);
            return Task.CompletedTask;
        });
        ulong s = 0;
        double bytes = await MeasureAllThreadsAsync([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int i) =>
        {
            reporter.Report(++s);
            while (Volatile.Read(ref sent) != (long)s) await Task.Yield();
        }, 500, 2000);
        // Before: a channel read operation per wake-up (~100 B). Task.Yield and the test's own async lambda remain.
        Assert.IsTrue(bytes < (Optimized ? 200 : 1024), bytes + " B per report");
    }

    // =====================================================================================================
    // Tailer -> consumer
    // =====================================================================================================

    [TestMethod]
    public async Task LockstepTailAndApplyAllocatesNothingInSteadyState()
    {
        // Local files: the memory file system grows its file buffers on writes, which would drown the numbers.
        using var dir = new TempDir("tailapply");
        await using RocksDatabase source = OpenQuiet(LocalFileSystem.Instance, dir.Sub("s"), archive: false);
        await using RocksDatabase replica = OpenQuiet(LocalFileSystem.Instance, dir.Sub("r"), archive: false);
        var consumer = new ReplicationConsumer(replica) { ApplyParallelism = 1 };
        using WalTailer tailer = new ReplicationSource(source).TailWal(1);
        using var cts = new CancellationTokenSource();
        await using IAsyncEnumerator<WalChunk> chunks = tailer.ReadChunksAsync(cts.Token).GetAsyncEnumerator(cts.Token);
        int k = 0;
        double putBytes = await MeasureAllThreadsAsync([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int i) => await replica.PutAsync(Key(1_000_000 + k++), Value100), 300, 1000);
        // Writes on the replica above moved its sequence: apply to a fresh consumer check from here on.
        await source.PutAsync(Key(0), Value100);
        Assert.IsTrue(await chunks.MoveNextAsync());
        double bytes = await MeasureAllThreadsAsync([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int i) =>
        {
            ValueTask<bool> next = chunks.MoveNextAsync();
            await source.PutAsync(Key(k++), Value100);
            Assert.IsTrue(await next);
            await consumer.IngestChunkAsync(chunks.Current.Memory, cts.Token);
        }, 300, 1000);
        // One PutAsync on each side per op: compare with two plain writes (Debug: per-call async state machine classes).
        double extra = bytes - 2 * putBytes;
        Assert.IsTrue(extra < (Optimized ? 48 : 2048), "tail + apply: " + extra + " B/op over two PutAsync (" + putBytes + " B/op each)");
        Assert.AreEqual(source.GetLatestSequenceNumber(), tailer.NextSequenceNumber - 1);
        Assert.IsTrue(replica.TryGet(Key(k - 1), new byte[200], out _));
    }

    // =====================================================================================================
    // Raft paths
    // =====================================================================================================

    private sealed class NoTransport : IPeerTransport
    {
        public Task<RequestVoteResult?> SendRequestVoteAsync(ClusterMember peer, RequestVotePayload request, CancellationToken cancellationToken) =>
            Task.FromResult<RequestVoteResult?>(null);

        public Task<AppendEntriesResult?> SendHeartbeatAsync(ClusterMember peer, HeartbeatPayload payload, CancellationToken cancellationToken) =>
            Task.FromResult<AppendEntriesResult?>(null);
    }

    private static RaftClusterNode NewNode(MemoryFileSystem fs, RocksDatabase db, string id, bool bootstrapPrimary)
    {
        var peers = new List<ClusterMember>();
        for (int i = 1; i <= 3; i++)
        {
            if ("n" + i != id) peers.Add(new ClusterMember("n" + i, "mem://n" + i));
        }
        var config = new RaftConfig(id, "mem://" + id, peers) { BootstrapPrimary = bootstrapPrimary, ElectionTimeoutMinMs = 60_000, ElectionTimeoutMaxMs = 120_000 };
        return new RaftClusterNode(config, new RaftPersistentState(fs, "/raft-" + id), db, new NoTransport());
    }

    [TestMethod]
    public async Task RaftCommitWaitsAndProgressAllocateNothingInSteadyState()
    {
        var fs = new MemoryFileSystem();
        using var dir = new TempDir("raftalloc");
        await using RocksDatabase db = OpenQuiet(LocalFileSystem.Instance, dir.Sub("n1"), archive: false);
        await using RaftClusterNode node = NewNode(fs, db, "n1", bootstrapPrimary: true);
        Assert.AreEqual(RaftRole.Leader, node.Role);
        int k = 0;

        // Quorum commit on a progress report (sync): no list, no comparison delegate, no signal per advance.
        long bytes = 0;
        for (int i = 0; i < 1100; i++)
        {
            db.Put(Key(k++), Value100);
            ulong latest = db.GetLatestSequenceNumber();
            long before = GC.GetAllocatedBytesForCurrentThread();
            node.RecordFollowerProgress("n2", latest, node.CurrentTerm);
            if (i >= 100) bytes += GC.GetAllocatedBytesForCurrentThread() - before;
            Assert.AreEqual(latest, node.CommitSeq);
        }
        if (Optimized) Assert.AreEqual(0L, bytes, "1000 RecordFollowerProgress advancing the commit index");

        // A pending commit wait completed by the progress report, compared with the same loop without the wait.
        using var cts = new CancellationTokenSource();
        double baseline = await MeasureAllThreadsAsync([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int i) =>
        {
            db.Put(Key(k++), Value100);
            node.RecordFollowerProgress("n2", db.GetLatestSequenceNumber(), node.CurrentTerm);
            await Task.CompletedTask;
        }, 300, 1000);
        double waitBytes = await MeasureAllThreadsAsync([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int i) =>
        {
            db.Put(Key(k++), Value100);
            ulong latest = db.GetLatestSequenceNumber();
            ValueTask<bool> committed = node.WaitForCommitAsync(latest, cts.Token);
            node.RecordFollowerProgress("n2", latest, node.CurrentTerm);
            Assert.IsTrue(await committed);
        }, 300, 1000);
        waitBytes -= baseline;
        // Before: a TaskCompletionSource per commit advance plus Task.WaitAsync and the state machine (~470 B).
        Assert.IsTrue(waitBytes < (Optimized ? 48 : 1024), "Put + WaitForCommitAsync + RecordFollowerProgress: " + waitBytes + " B/op");
    }

    [TestMethod]
    public async Task RaftCommitWaitEndsOnLeadershipLossAndCancellation()
    {
        var fs = new MemoryFileSystem();
        await using RocksDatabase db = OpenQuiet(fs, "/n1", archive: false);
        await using RaftClusterNode node = NewNode(fs, db, "n1", bootstrapPrimary: true);
        db.Put(Key(0), Value100);
        ulong seq = db.GetLatestSequenceNumber();

        using (var cts = new CancellationTokenSource())
        {
            Task<bool> cancelled = node.WaitForCommitAsync(seq, cts.Token).AsTask();
            await cts.CancelAsync();
            OperationCanceledException e = await Assert.ThrowsExactlyAsync<OperationCanceledException>(() => cancelled);
            Assert.AreEqual(cts.Token, e.CancellationToken);
        }
        Task<bool> waiting = node.WaitForCommitAsync(seq, CancellationToken.None).AsTask();
        Task<bool> waiting2 = node.WaitForCommitAsync(seq + 10, CancellationToken.None).AsTask();
        Assert.IsFalse(waiting.IsCompleted);
        node.RecordFollowerProgress("n2", seq, node.CurrentTerm);
        Assert.IsTrue(await waiting.WaitAsync(TestUtil.Timeout));
        Assert.IsFalse(waiting2.IsCompleted, "a later sequence is not committed yet");
        // A higher term deposes the leader: the wait ends with false.
        node.HandleAppendEntries(new HeartbeatPayload(5, "n2", 0, 0, 0, ClusterMode.Bootstrap));
        Assert.AreEqual(RaftRole.Follower, node.Role);
        Assert.IsFalse(await waiting2.WaitAsync(TestUtil.Timeout));
    }

    [TestMethod]
    public void HeartbeatAnswersAreReusedWhileNothingChanges()
    {
        var fs = new MemoryFileSystem();
        using RocksDatabase db = OpenQuiet(fs, "/n2", archive: false);
        using RaftClusterNode node = NewNode(fs, db, "n2", bootstrapPrimary: false);
        db.Put(Key(0), Value100);
        var beat = new HeartbeatPayload(1, "n1", 0, 0, 0, ClusterMode.Bootstrap);
        AppendEntriesResult first = node.HandleAppendEntries(beat);
        Assert.IsTrue(first.Success);
        Assert.AreEqual(1UL, first.LastSeq);
        Assert.AreSame(first, node.HandleAppendEntries(beat), "same term and tail: same answer");
        long bytes = MeasureThread(_ => node.HandleAppendEntries(beat), 10, 1000);
        if (Optimized) Assert.AreEqual(0L, bytes, "1000 idle heartbeats");
        db.Put(Key(1), Value100);
        AppendEntriesResult moved = node.HandleAppendEntries(beat);
        Assert.AreNotSame(first, moved);
        Assert.AreEqual(2UL, moved.LastSeq);
        AppendEntriesResult newTerm = node.HandleAppendEntries(new HeartbeatPayload(2, "n1", 0, 0, 0, ClusterMode.Bootstrap));
        Assert.AreEqual(2L, newTerm.Term);
        Assert.IsTrue(newTerm.Success);
        // A rejection is never confused with the cached success.
        AppendEntriesResult behind = node.HandleAppendEntries(new HeartbeatPayload(2, "n1", 100, 2, 0, ClusterMode.Bootstrap));
        Assert.IsFalse(behind.Success);
        Assert.AreEqual(AppendEntriesRejectReason.Behind, behind.Reason);
    }

    [TestMethod]
    public async Task WriteCommittedTimesOutWithoutAQuorumAndReusesItsTimeoutSource()
    {
        var fs = new MemoryFileSystem();
        var members = new List<ClusterMember> { new("n1", "mem://n1"), new("n2", "mem://n2"), new("n3", "mem://n3") };
        await using var host = new ClusterNodeHost(new ClusterNodeHostOptions
        {
            NodeId = "n1",
            Endpoint = "mem://n1",
            DbPath = "/n1",
            Members = members,
            BootstrapPrimaryId = "n1",
            DbOptionsFactory = () => new DbOptions { FileSystem = fs },
            HeartbeatIntervalMs = 50,
        }, _ => throw new IOException("no network"));
        await host.StartAsync(CancellationToken.None);
        Assert.AreEqual(RaftRole.Leader, host.Node.Role);
        // No follower ever reports: every write times out (the pooled timeout source fired, so it is not reused) ...
        for (int i = 0; i < 3; i++)
        {
            Assert.IsFalse(await host.WriteCommittedAsync(Key(i), Value100, TimeSpan.FromMilliseconds(30), CancellationToken.None));
        }
        // ... a caller's token still cancels (linked source) ...
        using (var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(30)))
        {
            await Assert.ThrowsExactlyAsync<OperationCanceledException>(
                async () => await host.WriteCommittedAsync(Key(10), Value100, TimeSpan.FromSeconds(30), cts.Token));
        }
        // ... and once a follower reports, writes are acknowledged; the reset source is reused without firing late.
        for (int i = 0; i < 5; i++)
        {
            ValueTask<bool> w = host.WriteCommittedAsync(Key(20 + i), Value100, TimeSpan.FromSeconds(30), CancellationToken.None);
            await WaitUntilAsync(() =>
            {
                host.HandleReportProgress("n2", host.Db.GetLatestSequenceNumber(), host.Node.CurrentTerm);
                return host.Node.CommitSeq == host.Db.GetLatestSequenceNumber();
            }, "commit");
            Assert.IsTrue(await w);
        }
    }

    private static byte[] Key(int i) => U(K(i));
}
