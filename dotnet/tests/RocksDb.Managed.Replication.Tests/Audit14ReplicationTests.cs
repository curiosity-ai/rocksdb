using System.Runtime.CompilerServices;
using RocksDb.Managed.Cluster;
using RocksDb.Managed.Replication.Hosting;
using RocksDb.Managed.Replication.MagicOnion;
using RocksDb.Managed.Replication.Switchover;

namespace RocksDb.Managed.Replication.Tests;

/// <summary>
/// Audit #14, replication: regression tests for the findings of the second review pass over the stream internals
/// (a chunk whose write fails on a replica, the lineage of plain replication, the commit-delay controller, leftover
/// checkpoints, snapshot restore, the term map, uploads).
/// </summary>
[TestClass]
public sealed class Audit14ReplicationTests
{
    private static readonly string[] s_nodes = ["n1", "n2", "n3"];

    private static WriteBatch Batch(string key)
    {
        var b = new WriteBatch();
        b.Put(U(key), U("value-of-" + key));
        return b;
    }

    /// <summary>Forwards to an inner file system; fails the next <c>n</c> appends to a WAL file (*.log) with a retryable I/O error.</summary>
    private sealed class FailingWalFileSystem(IFileSystem inner) : IFileSystem
    {
        private int _failAppends;
        public int Failed;

        public void FailWalAppends(int count) => Volatile.Write(ref _failAppends, count);

        private void MaybeFail(string path)
        {
            if (!path.EndsWith(".log", StringComparison.Ordinal)) return;
            while (true)
            {
                int n = Volatile.Read(ref _failAppends);
                if (n <= 0) return;
                if (Interlocked.CompareExchange(ref _failAppends, n - 1, n) != n) continue;
                Interlocked.Increment(ref Failed);
                throw RocksDbException.RetryableIOError("injected WAL append failure: " + path);
            }
        }

        private IWritableFile Wrap(string path, IWritableFile f) => new Writable(this, path, f);

        public IWritableFile CreateWritable(string path, bool truncate = true) => Wrap(path, inner.CreateWritable(path, truncate));
        public IWritableFile CreateWritable(string path, bool truncate, WritableFileOptions options) => Wrap(path, inner.CreateWritable(path, truncate, options));
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

        private sealed class Writable(FailingWalFileSystem fs, string path, IWritableFile inner) : IWritableFile
        {
            public long Size => inner.Size;

            public void Append(ReadOnlySpan<byte> data)
            {
                fs.MaybeFail(path);
                inner.Append(data);
            }

            public ValueTask AppendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
            {
                fs.MaybeFail(path);
                return inner.AppendAsync(data, cancellationToken);
            }

            public ValueTask FlushAsync(CancellationToken cancellationToken = default) => inner.FlushAsync(cancellationToken);
            public void Flush() => inner.Flush();
            public void Sync() => inner.Sync();
            public ValueTask SyncAsync(CancellationToken cancellationToken = default) => inner.SyncAsync(cancellationToken);
            public void Dispose() => inner.Dispose();
            public ValueTask DisposeAsync() => inner.DisposeAsync();
        }
    }

    // ---- a chunk whose write fails on a replica ---------------------------------------------------------------

    [TestMethod]
    public async Task ClusterFollowerDoesNotSkipAChunkWhoseWalWriteFailed()
    {
        // A write group allocates its sequence numbers before the WAL append; when the append fails, the engine's
        // recovery publishes the range (as upstream's ResumeImpl) although the data is nowhere. The follower then
        // attached behind the chunk, reported it as held - the leader counted it towards the quorum - and never got it:
        // same latest sequence number as the leader, different contents. Now the follower's engine does not recover on
        // its own, and a chunk that cannot be applied rebuilds the node.
        var failing = new Dictionary<string, FailingWalFileSystem>(StringComparer.Ordinal);
        var c = new ClusterHarness(3, null)
        {
            WrapFileSystem = (id, fs) => failing.TryGetValue(id, out FailingWalFileSystem? f) ? f : failing[id] = new FailingWalFileSystem(fs),
        };
        await using (c)
        {
            ClusterNodeHost n1 = await c.StartAsync("n1");
            for (int i = 0; i < 20; i++) Assert.IsTrue(await n1.TryWriteAsLeaderAsync(U(K(i)), U(V(i, 0)), CancellationToken.None));
            await c.StartAsync("n2");
            await c.StartAsync("n3");
            await WaitUntilAsync(() => c.Statuses().TrueForAll(s => s.Mode == ClusterMode.Cluster), "cluster mode\n" + c.LogText);
            ClusterNodeHost leader = await c.WaitForLeaderAsync(null);
            await c.AssertConvergedAsync(leader);
            ClusterNodeHost follower = c[Array.Find(s_nodes, id => id != leader.NodeId)!];
            int resyncsBefore = follower.Resyncs;

            failing[follower.NodeId].FailWalAppends(1);
            for (int i = 100; i < 110; i++) Assert.IsTrue(await leader.WriteCommittedAsync(U(K(i)), U(V(i, 0)), TimeSpan.FromSeconds(20), CancellationToken.None), c.LogText);
            await WaitUntilAsync(() => Volatile.Read(ref failing[follower.NodeId].Failed) == 1, "the injected failure\n" + c.LogText);
            // The follower ends with every entry (pulled as a snapshot), not with a log that merely ends at the same number.
            await WaitUntilAsync(() => follower.Resyncs > resyncsBefore && !follower.IsResyncing, "the follower to rebuild\n" + c.LogText);
            for (int i = 110; i < 115; i++) Assert.IsTrue(await leader.WriteCommittedAsync(U(K(i)), U(V(i, 0)), TimeSpan.FromSeconds(20), CancellationToken.None), c.LogText);
            await c.AssertConvergedAsync(leader);
            for (int i = 100; i < 115; i++)
            {
                int key = i;
                Assert.IsTrue(follower.TryUseDatabase(0, (db, _) => db.Get(U(K(key))) is not null, out bool held) && held, "the follower lacks " + K(i) + "\n" + c.LogText);
            }
        }
    }

    [TestMethod]
    public async Task PairReplicaDoesNotSkipAChunkWhoseWalWriteFailed()
    {
        var fsA = new MemoryFileSystem();
        var fsB = new FailingWalFileSystem(new MemoryFileSystem());
        await using var h = new PairHarness { FileSystemOf = id => id == "A" ? fsA : fsB };
        await h.StartAsync("A");
        await h.StartAsync("B");
        await WaitUntilAsync(() => h["B"].GetStatus().AttachedFromSeq > 0, "B following");
        for (int i = 0; i < 5; i++)
        {
            using WriteBatch b = Batch("before" + i);
            Assert.AreEqual(PairWriteStatus.Applied, (await h["A"].WriteAsync(b, TimeSpan.FromSeconds(2), CancellationToken.None)).Status);
        }
        await WaitUntilAsync(() => h["B"].Db.GetLatestSequenceNumber() == h["A"].Db.GetLatestSequenceNumber(), "B caught up\n" + h.LogText);
        int resyncsBefore = h["B"].Resyncs;

        fsB.FailWalAppends(1);
        for (int i = 0; i < 10; i++)
        {
            using WriteBatch b = Batch("after" + i);
            Assert.AreEqual(PairWriteStatus.Applied, (await h["A"].WriteAsync(b, TimeSpan.FromSeconds(2), CancellationToken.None)).Status);
        }
        await WaitUntilAsync(() => Volatile.Read(ref fsB.Failed) == 1, "the injected failure\n" + h.LogText);
        await WaitUntilAsync(() => h["B"].Resyncs > resyncsBefore && !h["B"].IsResyncing, "B to rebuild\n" + h.LogText);
        await h.AssertConvergedAsync("A", "B");
        for (int i = 0; i < 10; i++) Assert.IsNotNull(h["B"].Db.Get(U("after" + i)), "B lacks after" + i + "\n" + h.LogText);
    }

    /// <summary>Plain replication in process, with the attach check.</summary>
    private sealed class PlainClient(Func<ReplicationPrimary> primary) : IReplicationSourceClient
    {
        public IAsyncEnumerable<SnapshotChunk> StreamInitialStateAsync(CancellationToken cancellationToken) => Detached(primary().StreamInitialStateAsync(cancellationToken));

        private static async IAsyncEnumerable<SnapshotChunk> Detached(IAsyncEnumerable<SnapshotChunk> source)
        {
            await foreach (SnapshotChunk c in source) yield return c.Detach();
        }

        public async IAsyncEnumerable<WalStreamMessage> StreamUpdatesAsync(ulong startSeq, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await foreach (WalStreamMessage m in primary().StreamUpdatesAsync(startSeq, cancellationToken)) yield return m.Detach();
        }

        public Task ReportProgressAsync(int replicaIndex, ulong appliedThrough, CancellationToken cancellationToken)
        {
            primary().ReportProgress(replicaIndex, appliedThrough);
            return Task.CompletedTask;
        }

        public Task<ReplicaAttachCheck?> CheckReplicaLogAsync(ulong latestSeq, long incarnationAtLatest, CancellationToken cancellationToken) =>
            Task.FromResult<ReplicaAttachCheck?>(primary().CheckReplicaLog(latestSeq, incarnationAtLatest));
    }

    /// <summary>Follows until the replica reaches <paramref name="target"/>, then stops the stream.</summary>
    private static async Task FollowUntilAsync(IReplicationSourceClient client, RocksDatabase replica, ulong target)
    {
        using var cts = new CancellationTokenSource(TestUtil.Timeout);
        Task<long> follow = ReplicaFollower.FollowAsync(client, replica, 0, cts.Token);
        Task<bool> reached = replica.WaitForSequenceAsync(target, cts.Token).AsTask();
        Task first = await Task.WhenAny(follow, reached);
        if (first == follow) await follow;
        Assert.IsTrue(await reached, "replica closed");
        await cts.CancelAsync();
        try
        {
            await follow;
        }
        catch (OperationCanceledException)
        {
        }
    }

    private static DbOptions Options(IFileSystem fs) => new() { CreateIfMissing = true, FileSystem = fs, WalTtlSeconds = 600, BgErrorResumeRetryInterval = 20_000 };

    [TestMethod]
    public async Task PlainFollowerAsksForAResyncAfterAFailedChunkWasPublished()
    {
        var fsP = new MemoryFileSystem();
        var fsR = new FailingWalFileSystem(new MemoryFileSystem());
        await using RocksDatabase primaryDb = await RocksDatabase.OpenAsync("/p", Options(fsP));
        var primary = new ReplicationPrimary(primaryDb, "/p-checkpoints", null);
        var client = new PlainClient(() => primary);
        RocksDatabase replica = await RocksDatabase.OpenAsync("/r", Options(fsR));
        try
        {
            for (int i = 0; i < 10; i++) await primaryDb.PutAsync(U(K(i)), U(V(i, 0)));
            await FollowUntilAsync(client, replica, primaryDb.GetLatestSequenceNumber());
            ulong appliedThrough = replica.GetLatestSequenceNumber();

            // The next chunk's WAL append fails on the replica. Its engine recovers on its own here (the options were
            // not passed through ReplicaFollower.ProtectReplicaOptions), which publishes the chunk's sequence numbers.
            fsR.FailWalAppends(1);
            for (int i = 10; i < 20; i++) await primaryDb.PutAsync(U(K(i)), U(V(i, 0)));
            using (var cts = new CancellationTokenSource(TestUtil.Timeout))
            {
                try
                {
                    await ReplicaFollower.FollowAsync(client, replica, 0, cts.Token);
                    Assert.Fail("the failed write was not reported");
                }
                catch (RocksDbException e)
                {
                    Assert.AreEqual(StatusCode.IOError, e.Code, e.Message);
                }
            }
            Assert.IsTrue(ReplicaApplyGuard.TryReadMarker(fsR, "/r", out ulong recorded));
            Assert.AreEqual(appliedThrough, recorded);
            await WaitUntilAsync(() => replica.BackgroundError is null, "the replica's engine to recover");
            Assert.IsTrue(replica.GetLatestSequenceNumber() > appliedThrough, "the recovery published the failed range (the premise of this test)");

            // Following again would stream from behind the hole: same sequence numbers as the primary, entries missing.
            await AssertResyncRequiredAsync(client, replica);

            // The same after a restart: the marker is on disk.
            await replica.DisposeAsync();
            replica = await RocksDatabase.OpenAsync("/r", Options(fsR));
            await AssertResyncRequiredAsync(client, replica);

            // A snapshot replaces the database (and the marker) and the replica follows again.
            await replica.DisposeAsync();
            await ReplicaFollower.SyncInitialStateAsync(client, fsR, "/r", [], CancellationToken.None);
            Assert.IsFalse(ReplicaApplyGuard.TryReadMarker(fsR, "/r", out _));
            replica = await RocksDatabase.OpenAsync("/r", Options(fsR));
            for (int i = 20; i < 25; i++) await primaryDb.PutAsync(U(K(i)), U(V(i, 0)));
            await FollowUntilAsync(client, replica, primaryDb.GetLatestSequenceNumber());
            Assert.AreEqual(DatabaseDigest.Compute(primaryDb), DatabaseDigest.Compute(replica));
        }
        finally
        {
            await replica.DisposeAsync();
        }
    }

    private static async Task AssertResyncRequiredAsync(IReplicationSourceClient client, RocksDatabase replica)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        try
        {
            await ReplicaFollower.FollowAsync(client, replica, 0, cts.Token);
            Assert.Fail("the replica followed although its log end is unknown");
        }
        catch (OperationCanceledException)
        {
            Assert.Fail("the replica followed although its log end is unknown");
        }
        catch (ReplicationResyncRequiredException)
        {
        }
    }

    [TestMethod]
    public async Task ProtectedReplicaDoesNotPublishAFailedChunk()
    {
        var fsP = new MemoryFileSystem();
        var fsR = new FailingWalFileSystem(new MemoryFileSystem());
        await using RocksDatabase primaryDb = await RocksDatabase.OpenAsync("/p", Options(fsP));
        var primary = new ReplicationPrimary(primaryDb, "/p-checkpoints", null);
        var client = new PlainClient(() => primary);
        await using RocksDatabase replica = await RocksDatabase.OpenAsync("/r", ReplicaFollower.ProtectReplicaOptions(Options(fsR)));
        for (int i = 0; i < 10; i++) await primaryDb.PutAsync(U(K(i)), U(V(i, 0)));
        await FollowUntilAsync(client, replica, primaryDb.GetLatestSequenceNumber());
        ulong appliedThrough = replica.GetLatestSequenceNumber();

        fsR.FailWalAppends(1);
        for (int i = 10; i < 20; i++) await primaryDb.PutAsync(U(K(i)), U(V(i, 0)));
        using (var cts = new CancellationTokenSource(TestUtil.Timeout))
        {
            await Assert.ThrowsExactlyAsync<RocksDbException>(() => ReplicaFollower.FollowAsync(client, replica, 0, cts.Token));
        }
        // No recovery on its own: the error stays and the log end does not move.
        await Task.Delay(300);
        Assert.IsNotNull(replica.BackgroundError);
        Assert.AreEqual(appliedThrough, replica.GetLatestSequenceNumber());
    }

    [TestMethod]
    public void ApplyFaultAtTheSamePositionTwiceRebuilds()
    {
        var policy = new FollowerResyncPolicy();
        Assert.IsFalse(policy.ShouldRebuildAfterApplyFault(10), "the first failure is retried");
        Assert.IsTrue(policy.ShouldRebuildAfterApplyFault(10), "the same position again: retrying does not help");
        Assert.IsFalse(policy.ShouldRebuildAfterApplyFault(11), "progress in between");
    }

    // ---- lineage of plain replication ------------------------------------------------------------------------

    [TestMethod]
    public async Task PlainReplicaAheadOfARestartedPrimaryIsToldToResync()
    {
        // The primary's writes are not fsync'd: a power loss drops the end of its log, and it then writes other entries
        // at those sequence numbers. The replica still holds the lost ones. Without a lineage it continued behind them
        // (start <= the primary's new end), ending at the primary's sequence number with different contents.
        var fsP = new MemoryFileSystem();
        var fsR = new MemoryFileSystem();
        RocksDatabase primaryDb = await RocksDatabase.OpenAsync("/p", Options(fsP));
        ReplicationPrimary primary = new(primaryDb, "/p-checkpoints", null);
        var client = new PlainClient(() => primary);
        await using RocksDatabase replica = await RocksDatabase.OpenAsync("/r", Options(fsR));
        try
        {
            for (int i = 0; i < 100; i++) await primaryDb.PutAsync(U(K(i)), U(V(i, 0)));
            primaryDb.SyncWal();
            for (int i = 100; i < 110; i++) await primaryDb.PutAsync(U(K(i)), U(V(i, 0)));
            await FollowUntilAsync(client, replica, primaryDb.GetLatestSequenceNumber());
            Assert.AreEqual(110UL, replica.GetLatestSequenceNumber());

            // Power loss of the primary: the unsynced tail is gone.
            fsP.SetFilesystemActive(false);
            try
            {
                await primaryDb.DisposeAsync();
            }
            catch (RocksDbException)
            {
            }
            fsP.SimulateCrash();
            fsP.SetFilesystemActive(true);
            primaryDb = await RocksDatabase.OpenAsync("/p", Options(fsP));
            Assert.IsTrue(primaryDb.GetLatestSequenceNumber() < 110, "the primary kept its unsynced tail (the premise of this test)");
            primary = new ReplicationPrimary(primaryDb, "/p-checkpoints", null);
            // Different entries, past the replica's end.
            for (int i = 0; i < 30; i++) await primaryDb.PutAsync(U("other" + i), U("x"));
            Assert.IsTrue(primaryDb.GetLatestSequenceNumber() > 110);

            await AssertResyncRequiredAsync(client, replica);
        }
        finally
        {
            await primaryDb.DisposeAsync();
        }
    }

    [TestMethod]
    public async Task PlainReplicaFollowsAcrossPrimaryRestartsThatLostNothing()
    {
        var fsP = new MemoryFileSystem();
        var fsR = new MemoryFileSystem();
        RocksDatabase primaryDb = await RocksDatabase.OpenAsync("/p", Options(fsP));
        ReplicationPrimary primary = new(primaryDb, "/p-checkpoints", null);
        var client = new PlainClient(() => primary);
        // From a snapshot: the lineage comes with it.
        for (int i = 0; i < 20; i++) await primaryDb.PutAsync(U(K(i)), U(V(i, 0)));
        await ReplicaFollower.SyncInitialStateAsync(client, fsR, "/r", [], CancellationToken.None);
        Assert.AreEqual(primary.Lineage.Serialize(), ReplicationLineage.Load(fsR, "/r").Serialize());
        await using RocksDatabase replica = await RocksDatabase.OpenAsync("/r", Options(fsR));
        try
        {
            for (int round = 0; round < 3; round++)
            {
                for (int i = 0; i < 10; i++) await primaryDb.PutAsync(U(K(100 * round + i)), U(V(i, round)));
                await FollowUntilAsync(client, replica, primaryDb.GetLatestSequenceNumber());
                // A clean restart of the primary: a new incarnation, nothing lost.
                await primaryDb.DisposeAsync();
                primaryDb = await RocksDatabase.OpenAsync("/p", Options(fsP));
                primary = new ReplicationPrimary(primaryDb, "/p-checkpoints", null);
            }
            for (int i = 0; i < 10; i++) await primaryDb.PutAsync(U("last" + i), U("x"));
            await FollowUntilAsync(client, replica, primaryDb.GetLatestSequenceNumber());
            Assert.AreEqual(DatabaseDigest.Compute(primaryDb), DatabaseDigest.Compute(replica));
            Assert.AreEqual(primary.Lineage.GetIncarnationForSeq(replica.GetLatestSequenceNumber()),
                ReplicationLineage.Load(replica).GetIncarnationForSeq(replica.GetLatestSequenceNumber()));
        }
        finally
        {
            await primaryDb.DisposeAsync();
        }
    }

    // ---- commit delay controller ----------------------------------------------------------------------------

    private sealed class ManualClock : TimeProvider
    {
        private long _ticks;
        public override long TimestampFrequency => TimeSpan.TicksPerSecond;
        public override long GetTimestamp() => Volatile.Read(ref _ticks);
        public void Advance(TimeSpan by) => Interlocked.Add(ref _ticks, by.Ticks);
    }

    [TestMethod]
    public void ReplicaThatStoppedReportingDoesNotPinTheCommitDelay()
    {
        // The cluster lag is the maximum over the replicas' last reports. A replica that died while it lagged kept that
        // maximum for good: every report of the others pushed it into the history again and the delay stayed at its cap.
        var clock = new ManualClock();
        var options = new AdaptiveCommitDelayOptions { MaxDelayMs = 2000, ReplicaLagStaleAfter = TimeSpan.FromSeconds(30) };
        var controller = new AdaptiveCommitDelayController(2, options, clock);
        controller.ReportLag(1, 1000);
        Assert.AreEqual(2000, controller.GetRecommendedDelayMs());
        clock.Advance(TimeSpan.FromSeconds(60)); // replica 1 is gone
        for (int i = 0; i < 100; i++)
        {
            controller.ReportLag(0, 0);
            clock.Advance(TimeSpan.FromMilliseconds(100));
        }
        Assert.AreEqual(0, controller.GetRecommendedDelayMs());

        // Nobody reports any more: no delay for replicas that are not there.
        controller.ReportLag(0, 500);
        Assert.IsTrue(controller.GetRecommendedDelayMs() > 0);
        clock.Advance(TimeSpan.FromSeconds(60));
        Assert.AreEqual(0, controller.GetRecommendedDelayMs());

        // Forgetting a replica has the same effect at once.
        controller.ReportLag(1, 1000);
        Assert.IsTrue(controller.GetRecommendedDelayMs() > 0);
        controller.ForgetReplica(1);
        Assert.AreEqual(0, controller.GetRecommendedDelayMs());
    }

    [TestMethod]
    public async Task ProgressOfAReplicaTheControllerHasNoSlotForStillRetiresWal()
    {
        var fs = new MemoryFileSystem();
        await using RocksDatabase db = await RocksDatabase.OpenAsync("/p", Options(fs));
        var primary = new ReplicationPrimary(db, "/p-checkpoints", new AdaptiveCommitDelayController(1));
        await db.PutAsync(U("k"), U("v"));
        primary.ReportProgress(5, db.GetLatestSequenceNumber()); // threw ArgumentOutOfRangeException before the WAL retirement
        Assert.ThrowsExactly<ArgumentOutOfRangeException>(() => primary.ReportProgress(-1, 1));
    }

    // ---- leftover checkpoints -----------------------------------------------------------------------------

    [TestMethod]
    public async Task LeftoverSnapshotCheckpointsAreRemovedAtStart()
    {
        // A snapshot stream's checkpoint is removed when the stream ends - not when the process dies during it. Each
        // leftover hard links every table file of its moment.
        await using var h = new PairHarness();
        string leftover = h.DbPath("A") + ".checkpoint-" + Guid.NewGuid().ToString("N");
        Directory.CreateDirectory(leftover);
        File.WriteAllText(Path.Combine(leftover, "000001.sst"), "x");
        Directory.CreateDirectory(leftover + ".tmp");
        await h.StartAsync("A");
        Assert.IsFalse(Directory.Exists(leftover), "the leftover checkpoint is still there");
        Assert.IsFalse(Directory.Exists(leftover + ".tmp"), "the leftover staging directory is still there");

        var fs = new MemoryFileSystem();
        await using RocksDatabase db = await RocksDatabase.OpenAsync("/p", Options(fs));
        fs.CreateDirectoryIfMissing("/p-checkpoints");
        fs.CreateDirectoryIfMissing("/p-checkpoints/checkpoint-dead");
        using (IWritableFile f = fs.CreateWritable("/p-checkpoints/checkpoint-dead/000001.sst")) f.Append(U("x"));
        _ = new ReplicationPrimary(db, "/p-checkpoints", null);
        Assert.IsFalse(fs.FileExists("/p-checkpoints/checkpoint-dead/000001.sst"));
    }

    // ---- snapshot restore, term map, uploads ---------------------------------------------------------------

    private static async IAsyncEnumerable<SnapshotChunk> Chunks(params SnapshotChunk[] chunks)
    {
        foreach (SnapshotChunk c in chunks)
        {
            await Task.Yield();
            yield return c;
        }
    }

    [TestMethod]
    public async Task RestoreWithoutCurrentIsRefused()
    {
        // A stream that ends properly but never sent CURRENT left a directory the host then created an empty database in.
        var fs = new MemoryFileSystem();
        var e = await Assert.ThrowsExactlyAsync<InvalidOperationException>(async () =>
            await SnapshotReceiver.ReceiveAsync(Chunks(SnapshotChunk.Plan(5, []), SnapshotChunk.Complete(0)), fs, "/r", [], CancellationToken.None));
        StringAssert.Contains(e.Message, "CURRENT");
        Assert.IsFalse(fs.FileExists("/r/CURRENT"));
    }

    [TestMethod]
    public async Task LogDescriptionOfASnapshotIsPersistedBeforeCurrentAppears()
    {
        var fs = new MemoryFileSystem();
        byte[] current = U("MANIFEST-000001\n");
        bool currentThere = true;
        string? seen = null;
        SnapshotChunk plan = SnapshotChunkDto.From(SnapshotChunk.Plan(5, []).WithLogDescription("1,7\n")).ToChunk(); // across the wire
        SnapshotRestoreResult r = await SnapshotReceiver.ReceiveAsync(
            Chunks(plan, SnapshotChunk.FileData("CURRENT", current.Length, 0, current), SnapshotChunk.Complete(1)), fs, "/r", [],
            description =>
            {
                seen = description;
                currentThere = fs.FileExists("/r/CURRENT");
            }, CancellationToken.None);
        Assert.AreEqual("1,7\n", seen);
        Assert.IsFalse(currentThere, "the description was handed over after the directory became an openable database");
        Assert.AreEqual("1,7\n", r.LogDescription);
        Assert.IsTrue(fs.FileExists("/r/CURRENT"));
    }

    [TestMethod]
    public void DamagedTermMapIsRefused()
    {
        // Lines that could not be read were skipped: a map with a range missing describes another log, and the attach
        // check and the votes rely on it.
        Assert.AreEqual(2, RaftLogTerms.Deserialize("1,1\n50,3\n").GetRanges().Count);
        Assert.AreEqual(0, RaftLogTerms.Deserialize("").GetRanges().Count);
        Assert.ThrowsExactly<InvalidDataException>(() => RaftLogTerms.Deserialize("1,1\ngarbage\n50,3\n"));
        Assert.ThrowsExactly<InvalidDataException>(() => RaftLogTerms.Deserialize("1,1\n50\n"));
        Assert.ThrowsExactly<InvalidDataException>(() => RaftLogTerms.Deserialize("50,3\n1,1\n"));
    }

    [TestMethod]
    public async Task ThrowingLogDelegateDoesNotStopTheHost()
    {
        using var dir = new TempDir("log");
        var network = new InMemoryNetwork();
        var host = new PairNodeHost(new PairNodeHostOptions
        {
            NodeId = "A",
            PeerId = "B",
            DbPath = dir.Sub("A"),
            InitialRole = ReplicationRole.Primary,
            DbOptionsFactory = () => new DbOptions { FileSystem = LocalFileSystem.Instance },
            RetryDelay = TimeSpan.FromMilliseconds(50),
            Log = _ => throw new InvalidOperationException("the application's logger failed"),
        }, new InMemoryPairClient(network, "A", "B"));
        try
        {
            await host.StartAsync(CancellationToken.None);
            using WriteBatch b = Batch("k");
            Assert.AreEqual(PairWriteStatus.Applied, (await host.WriteAsync(b, TimeSpan.FromSeconds(2), CancellationToken.None)).Status);
        }
        finally
        {
            await host.DisposeAsync();
        }
    }

    [TestMethod]
    public async Task TrickledUploadIsEndedByTheTotalDuration()
    {
        // The idle timeout restarts with every piece: a client sending a byte now and then held its buffer, and its share
        // of the node's budget for large writes, for as long as it liked.
        var budget = new FragmentedWriteBudget(new FragmentedWriteOptions { IdleTimeout = TimeSpan.FromSeconds(30), MaxUploadDuration = TimeSpan.FromMilliseconds(400) });
        using var upload = new FragmentedWriteUpload(budget);
        int sent = 0;
        var e = await Assert.ThrowsExactlyAsync<global::MagicOnion.ReturnStatusException>(() => upload.ReceiveAsync(async ct =>
        {
            await Task.Delay(100, ct);
            return true;
        }, () => new WriteFragmentDto { TotalLength = 1000, KeyLength = 1, Offset = sent++, Data = new byte[1] }, CancellationToken.None));
        Assert.AreEqual(Grpc.Core.StatusCode.DeadlineExceeded, e.StatusCode);
        Assert.IsTrue(sent < 20, "the upload went on for " + sent + " pieces");
    }
}
