using System.Runtime.CompilerServices;
using RocksDb.Managed.Cluster;
using RocksDb.Managed.Replication.Hosting;

namespace RocksDb.Managed.Replication.Tests;

/// <summary>
/// Audit #15, replication data path: the lineage of plain replication is mandatory, a follower's database cannot be
/// recovered in place (also not by a manual Resume), a failure that cannot be recorded stops the follower, and a resync
/// restores next to the current copy and swaps when the disk has room.
/// </summary>
[TestClass]
public sealed class Audit15ReplicationDataPathTests
{
    // ---- helpers ---------------------------------------------------------------------------------------------

    /// <summary>Forwards to an inner file system with a few scripted faults.</summary>
    private sealed class ScriptedFileSystem(IFileSystem inner) : IFileSystem
    {
        private int _failWalAppends;
        public int FailedWalAppends;
        /// <summary>Creating a file whose path contains this fails (null: none).</summary>
        public volatile string? FailCreatesContaining;
        /// <summary>GetFreeSpace throws NotSupported.</summary>
        public volatile bool FreeSpaceUnknown;

        public void FailWalAppends(int count) => Volatile.Write(ref _failWalAppends, count);

        private void MaybeFailAppend(string path)
        {
            if (!path.EndsWith(".log", StringComparison.Ordinal)) return;
            while (true)
            {
                int n = Volatile.Read(ref _failWalAppends);
                if (n <= 0) return;
                if (Interlocked.CompareExchange(ref _failWalAppends, n - 1, n) != n) continue;
                Interlocked.Increment(ref FailedWalAppends);
                throw RocksDbException.RetryableIOError("injected WAL append failure: " + path);
            }
        }

        private IWritableFile Create(string path, Func<IWritableFile> create)
        {
            if (FailCreatesContaining is { } s && path.Contains(s, StringComparison.Ordinal)) throw RocksDbException.IOError("injected create failure: " + path);
            return new Writable(this, path, create());
        }

        public IWritableFile CreateWritable(string path, bool truncate = true) => Create(path, () => inner.CreateWritable(path, truncate));
        public IWritableFile CreateWritable(string path, bool truncate, WritableFileOptions options) => Create(path, () => inner.CreateWritable(path, truncate, options));
        public IWritableFile ReopenWritable(string path, WritableFileOptions options) => Create(path, () => inner.ReopenWritable(path, options));
        public IWritableFile ReuseWritable(string path, string oldPath, WritableFileOptions options) => Create(path, () => inner.ReuseWritable(path, oldPath, options));
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
        public long GetFreeSpace(string path) => FreeSpaceUnknown ? throw RocksDbException.NotSupported("no free space information") : inner.GetFreeSpace(path);
        public void WriteAt(string path, long offset, ReadOnlySpan<byte> data) => inner.WriteAt(path, offset, data);

        private sealed class Writable(ScriptedFileSystem fs, string path, IWritableFile inner) : IWritableFile
        {
            public long Size => inner.Size;

            public void Append(ReadOnlySpan<byte> data)
            {
                fs.MaybeFailAppend(path);
                inner.Append(data);
            }

            public ValueTask AppendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
            {
                fs.MaybeFailAppend(path);
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

    /// <summary>Plain replication in process; <see cref="RewriteSnapshot"/> edits the snapshot stream.</summary>
    private sealed class PlainClient(Func<ReplicationPrimary> primary) : IReplicationSourceClient
    {
        public Func<SnapshotChunk, SnapshotChunk>? RewriteSnapshot;

        public async IAsyncEnumerable<SnapshotChunk> StreamInitialStateAsync([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await foreach (SnapshotChunk c in primary().StreamInitialStateAsync(cancellationToken))
            {
                SnapshotChunk d = c.Detach();
                yield return RewriteSnapshot is null ? d : RewriteSnapshot(d);
            }
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

        public Task<ReplicaAttachCheck> CheckReplicaLogAsync(ulong latestSeq, long incarnationAtLatest, CancellationToken cancellationToken) =>
            Task.FromResult(primary().CheckReplicaLog(latestSeq, incarnationAtLatest));
    }

    private static DbOptions Options(IFileSystem fs) => new() { CreateIfMissing = true, FileSystem = fs, WalTtlSeconds = 600, BgErrorResumeRetryInterval = 20_000 };

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

    private static async Task AssertResyncRequiredAsync(IReplicationSourceClient client, RocksDatabase replica, string expectedInMessage)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        try
        {
            await ReplicaFollower.FollowAsync(client, replica, 0, cts.Token);
            Assert.Fail("the replica followed");
        }
        catch (OperationCanceledException)
        {
            Assert.Fail("the replica followed");
        }
        catch (ReplicationResyncRequiredException e)
        {
            StringAssert.Contains(e.Message, expectedInMessage);
        }
    }

    // ---- 1. the lineage is mandatory ---------------------------------------------------------------------------

    [TestMethod]
    public async Task ReplicaWithoutLineageMustResyncAndASnapshotBringsTheLineage()
    {
        var fsP = new MemoryFileSystem();
        var fsR = new MemoryFileSystem();
        await using RocksDatabase primaryDb = await RocksDatabase.OpenAsync("/p", Options(fsP));
        // The primary already holds entries when replication is set up: its first incarnation covers them.
        for (int i = 0; i < 10; i++) await primaryDb.PutAsync(U(K(i)), U(V(i, 0)));
        var primary = new ReplicationPrimary(primaryDb, "/p-checkpoints", null);
        Assert.AreNotEqual(0L, primary.Lineage.GetIncarnationForSeq(1), "the entries written before replication belong to no incarnation");
        var client = new PlainClient(() => primary);

        await ReplicaFollower.SyncInitialStateAsync(client, fsR, "/r", [], CancellationToken.None);
        RocksDatabase replica = await RocksDatabase.OpenAsync("/r", Options(fsR));
        try
        {
            // Fresh from the snapshot, nothing written since: the replica's last entry has an incarnation and it attaches.
            for (int i = 10; i < 15; i++) await primaryDb.PutAsync(U(K(i)), U(V(i, 0)));
            await FollowUntilAsync(client, replica, primaryDb.GetLatestSequenceNumber());
            Assert.AreEqual(DatabaseDigest.Compute(primaryDb), DatabaseDigest.Compute(replica));

            // The same replica without its lineage: nothing says its log is a prefix of the primary's. It used to be
            // accepted on its sequence number alone.
            await replica.DisposeAsync();
            fsR.DeleteFile("/r/" + ReplicationLineage.DirectoryName + "/lineage");
            replica = await RocksDatabase.OpenAsync("/r", Options(fsR));
            await AssertResyncRequiredAsync(client, replica, "no lineage");
            Assert.IsFalse(primary.CheckReplicaLog(replica.GetLatestSequenceNumber(), 0).Consistent);
            // An empty replica has nothing that could diverge.
            Assert.IsTrue(primary.CheckReplicaLog(0, 0).Consistent);
        }
        finally
        {
            await replica.DisposeAsync();
        }
    }

    [TestMethod]
    public async Task SnapshotWithoutTheLogDescriptionIsRefusedBeforeAnythingIsDeleted()
    {
        var fsP = new MemoryFileSystem();
        var fsR = new MemoryFileSystem();
        await using RocksDatabase primaryDb = await RocksDatabase.OpenAsync("/p", Options(fsP));
        for (int i = 0; i < 10; i++) await primaryDb.PutAsync(U(K(i)), U(V(i, 0)));
        var primary = new ReplicationPrimary(primaryDb, "/p-checkpoints", null);
        var client = new PlainClient(() => primary)
        {
            RewriteSnapshot = c => c.Kind == SnapshotChunkKind.Plan ? SnapshotChunk.Plan(c.SequenceNumber, c.KeepFiles, c.TransferBytes) : c,
        };
        // The replica directory holds a database of its own.
        await using (RocksDatabase own = await RocksDatabase.OpenAsync("/r", Options(fsR))) await own.PutAsync(U("own"), U("1"));

        var e = await Assert.ThrowsExactlyAsync<InvalidOperationException>(async () =>
            await ReplicaFollower.SyncInitialStateAsync(client, fsR, "/r", [], CancellationToken.None));
        StringAssert.Contains(e.Message, "no description of the log");
        Assert.IsTrue(fsR.FileExists("/r/CURRENT"), "the replica's database was deleted for a snapshot that was then refused");
        await using RocksDatabase still = await RocksDatabase.OpenAsync("/r", Options(fsR));
        Assert.IsNotNull(still.Get(U("own")));
    }

    // ---- 2. no recovery in place, and a failure that cannot be recorded ---------------------------------------

    [TestMethod]
    public async Task ManualResumeIsRefusedOnAReplicaDatabase()
    {
        // The automatic recovery was vetoed already; a manual Resume publishes the failed write's sequence numbers
        // just the same. The listener hook (EventListener.OnManualRecoveryRequested) closes that path.
        var fsP = new MemoryFileSystem();
        var fsR = new ScriptedFileSystem(new MemoryFileSystem());
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
            var failed = await Assert.ThrowsExactlyAsync<RocksDbException>(async () => await ReplicaFollower.FollowAsync(client, replica, 0, cts.Token));
            Assert.AreEqual(StatusCode.IOError, failed.Code, failed.Message);
        }
        Assert.IsNotNull(replica.BackgroundError, "the engine recovered on its own");

        var refused = Assert.ThrowsExactly<RocksDbException>(replica.Resume);
        Assert.AreEqual(StatusCode.NotSupported, refused.Code, refused.Message);
        StringAssert.Contains(refused.Message, "follower");
        var refusedAsync = await Assert.ThrowsExactlyAsync<RocksDbException>(async () => await replica.ResumeAsync());
        Assert.AreEqual(StatusCode.NotSupported, refusedAsync.Code, refusedAsync.Message);
        Assert.IsNotNull(replica.BackgroundError);
        Assert.AreEqual(appliedThrough, replica.GetLatestSequenceNumber(), "the failed range was published");
    }

    [TestMethod]
    public async Task PairReplicaStopsFollowingWhenAFailedApplyCannotBeRecorded()
    {
        var fsA = new MemoryFileSystem();
        var fsB = new ScriptedFileSystem(new MemoryFileSystem());
        await using var h = new PairHarness { FileSystemOf = id => id == "A" ? fsA : fsB };
        await h.StartAsync("A");
        await h.StartAsync("B");
        await WaitUntilAsync(() => h["B"].GetStatus().AttachedFromSeq > 0, "B following");
        for (int i = 0; i < 5; i++)
        {
            using var b = new WriteBatch();
            b.Put(U("before" + i), U("1"));
            Assert.AreEqual(PairWriteStatus.Applied, (await h["A"].WriteAsync(b, TimeSpan.FromSeconds(2), CancellationToken.None)).Status);
        }
        await WaitUntilAsync(() => h["B"].Db.GetLatestSequenceNumber() == h["A"].Db.GetLatestSequenceNumber(), "B caught up\n" + h.LogText);
        ulong applied = h["B"].Db.GetLatestSequenceNumber();
        int resyncsBefore = h["B"].Resyncs;
        Assert.IsNull(h["B"].FollowerFault);

        // The chunk's WAL append fails, and so does the marker that would record it.
        fsB.FailCreatesContaining = ReplicaApplyGuard.MarkerFileName;
        fsB.FailWalAppends(1);
        for (int i = 0; i < 5; i++)
        {
            using var b = new WriteBatch();
            b.Put(U("after" + i), U("1"));
            Assert.AreEqual(PairWriteStatus.Applied, (await h["A"].WriteAsync(b, TimeSpan.FromSeconds(2), CancellationToken.None)).Status);
        }
        await WaitUntilAsync(() => h["B"].FollowerFault is not null, "B to report the fault\n" + h.LogText);
        StringAssert.Contains(h["B"].FollowerFault, "could not be recorded");
        Assert.AreEqual(h["B"].FollowerFault, h["B"].GetStatus().FollowerFault);
        // It does not carry on: no retry of the stream, no resync on its own, the log where it was.
        await Task.Delay(400);
        Assert.AreEqual(1, Volatile.Read(ref fsB.FailedWalAppends));
        Assert.AreEqual(resyncsBefore, h["B"].Resyncs);
        Assert.AreEqual(applied, h["B"].Db.GetLatestSequenceNumber());
        Assert.IsFalse(fsB.FileExists(h.DbPath("B") + "/" + ReplicaApplyGuard.MarkerFileName));

        // An operator's resync rebuilds the node and clears the fault.
        fsB.FailCreatesContaining = null;
        h["B"].TriggerResync("operator");
        await WaitUntilAsync(() => h["B"].Resyncs > resyncsBefore && !h["B"].IsResyncing, "B to rebuild\n" + h.LogText);
        Assert.IsNull(h["B"].FollowerFault);
        await h.AssertConvergedAsync("A", "B");
    }

    // ---- 3. restore-then-swap ---------------------------------------------------------------------------------

    private const string Src = "/src", Dst = "/dst", StateDir = "state";

    /// <summary>A source database with <paramref name="keys"/> entries and a destination database with one key of its own.</summary>
    private static async Task<(RocksDatabase Source, ReplicationPrimary Primary)> NewSourceAndDestinationAsync(MemoryFileSystem fs, int keys)
    {
        RocksDatabase source = await RocksDatabase.OpenAsync(Src, Options(fs));
        for (int i = 0; i < keys; i++) await source.PutAsync(U(K(i)), U(V(i, 0)));
        await source.FlushAsync();
        var primary = new ReplicationPrimary(source, "/src-checkpoints", null);
        await using (RocksDatabase old = await RocksDatabase.OpenAsync(Dst, Options(fs)))
        {
            await old.PutAsync(U("old"), U("copy"));
            await old.FlushAsync();
        }
        // The owner's own state, preserved across restores.
        fs.CreateDirectoryIfMissing(Dst + "/" + StateDir);
        using (IWritableFile f = fs.CreateWritable(Dst + "/" + StateDir + "/owner", truncate: true)) f.Append(U("mine"));
        return (source, primary);
    }

    private static async IAsyncEnumerable<SnapshotChunk> Detached(IAsyncEnumerable<SnapshotChunk> stream, int failAfter = int.MaxValue,
        Func<SnapshotChunk, SnapshotChunk>? rewrite = null)
    {
        int n = 0;
        await foreach (SnapshotChunk c in stream)
        {
            if (n++ >= failAfter) throw new IOException("the source died mid-snapshot");
            SnapshotChunk d = c.Detach();
            yield return rewrite is null ? d : rewrite(d);
        }
    }

    private static async Task AssertHoldsAsync(IFileSystem fs, string path, bool restored)
    {
        await using RocksDatabase db = await RocksDatabase.OpenAsync(path, new DbOptions { FileSystem = fs });
        if (restored)
        {
            Assert.IsNotNull(db.Get(U(K(3))), "the restored entries are missing");
            Assert.IsNull(db.Get(U("old")), "the old copy's entry survived the restore");
        }
        else
        {
            Assert.IsNotNull(db.Get(U("old")), "the old copy is gone");
            Assert.IsNull(db.Get(U(K(3))));
        }
    }

    private static void AssertNoSwapDirectories(IFileSystem fs)
    {
        Assert.IsFalse(fs.DirectoryExists(SnapshotRestoreSwap.StagePath(Dst)), "staging directory left behind");
        Assert.IsFalse(fs.DirectoryExists(SnapshotRestoreSwap.OldPath(Dst)), "old copy left behind");
    }

    [TestMethod]
    [DataRow(SnapshotRestoreMode.Auto, "plenty", true)]
    [DataRow(SnapshotRestoreMode.Auto, "little", false)]
    [DataRow(SnapshotRestoreMode.Auto, "unknown", false)]
    [DataRow(SnapshotRestoreMode.Auto, "unsized", false)]
    [DataRow(SnapshotRestoreMode.AlwaysSwap, "little", true)]
    [DataRow(SnapshotRestoreMode.AlwaysSwap, "unknown", true)]
    [DataRow(SnapshotRestoreMode.AlwaysDeleteFirst, "plenty", false)]
    public async Task RestoreModeFollowsTheFreeDiskSpace(SnapshotRestoreMode mode, string space, bool expectSwap)
    {
        var mem = new MemoryFileSystem();
        (RocksDatabase source, ReplicationPrimary primary) = await NewSourceAndDestinationAsync(mem, 200);
        await using RocksDatabase _ = source;
        var fs = new ScriptedFileSystem(mem);
        long announced = -1;
        if (space == "little") mem.FreeSpace = 1024; // less than the margin alone
        if (space == "unknown") fs.FreeSpaceUnknown = true;
        var lines = new List<string>();
        string? described = null;
        SnapshotRestoreResult r = await SnapshotReceiver.ReceiveAsync(
            Detached(primary.StreamInitialStateAsync(CancellationToken.None), rewrite: c =>
            {
                if (c.Kind != SnapshotChunkKind.Plan) return c;
                announced = c.TransferBytes;
                // "unsized": a source that does not announce the size of the transfer.
                return space == "unsized" ? SnapshotChunk.Plan(c.SequenceNumber, c.KeepFiles).WithLogDescription(c.LogDescription) : c;
            }),
            fs, Dst, [StateDir],
            new SnapshotRestoreOptions { Mode = mode, RequireLogDescription = true, OnLogDescription = d => described = d, Log = lines.Add },
            CancellationToken.None);

        Assert.IsTrue(announced > 0, "the plan did not announce the size of the transfer");
        Assert.AreEqual(expectSwap, r.Swapped, string.Join("\n", lines));
        Assert.AreEqual(1, lines.Count, string.Join("\n", lines));
        StringAssert.Contains(lines[0], expectSwap ? "restore-then-swap" : "delete-first");
        if (mode == SnapshotRestoreMode.Auto && !expectSwap)
            StringAssert.Contains(lines[0], space switch { "little" => "not enough free disk space", "unknown" => "free disk space is unknown", _ => "did not announce" });
        Assert.IsNotNull(described);
        await AssertHoldsAsync(fs, Dst, restored: true);
        AssertNoSwapDirectories(fs);
        // The owner's state came along, and the description did not stay behind as a file.
        Assert.IsTrue(fs.FileExists(Dst + "/" + StateDir + "/owner"));
        Assert.IsFalse(fs.FileExists(Dst + "/" + SnapshotRestoreSwap.PendingDescriptionFileName));
    }

    [TestMethod]
    [DataRow(true)]
    [DataRow(false)]
    public async Task SourceDyingMidSnapshotLeavesTheOldCopyOnlyWithRestoreThenSwap(bool swap)
    {
        var fs = new MemoryFileSystem();
        (RocksDatabase source, ReplicationPrimary primary) = await NewSourceAndDestinationAsync(fs, 200);
        await using RocksDatabase _ = source;
        string? described = null;
        await Assert.ThrowsExactlyAsync<IOException>(async () => await SnapshotReceiver.ReceiveAsync(
            Detached(primary.StreamInitialStateAsync(CancellationToken.None), failAfter: 2), fs, Dst, [StateDir],
            new SnapshotRestoreOptions
            {
                Mode = swap ? SnapshotRestoreMode.AlwaysSwap : SnapshotRestoreMode.AlwaysDeleteFirst,
                RequireLogDescription = true,
                OnLogDescription = d => described = d,
            }, CancellationToken.None));
        Assert.IsNull(described, "the description of a log that never arrived was applied");
        if (swap)
        {
            // The node still has the copy it had (and can be promoted on it).
            Assert.IsNull(SnapshotRestoreSwap.Recover(fs, Dst, [StateDir], null));
            AssertNoSwapDirectories(fs);
            await AssertHoldsAsync(fs, Dst, restored: false);
            Assert.IsTrue(fs.FileExists(Dst + "/" + StateDir + "/owner"));
        }
        else
        {
            Assert.IsFalse(fs.FileExists(Dst + "/CURRENT"), "delete-first keeps no copy (the premise of the other mode)");
        }
    }

    /// <summary>Where the process "dies" during the swap.</summary>
    public enum CrashPoint
    {
        BeforeTheSwap,
        BetweenTheRenames,
        AfterTheRenames,
    }

    private sealed class Crash : Exception;

    [TestMethod]
    [DataRow(CrashPoint.BeforeTheSwap)]
    [DataRow(CrashPoint.BetweenTheRenames)]
    [DataRow(CrashPoint.AfterTheRenames)]
    public async Task InterruptedSwapIsCompletedOrDiscardedAtTheNextStart(CrashPoint point)
    {
        var fs = new MemoryFileSystem();
        (RocksDatabase source, ReplicationPrimary primary) = await NewSourceAndDestinationAsync(fs, 200);
        await using RocksDatabase _ = source;
        string? described = null;
        // The guard runs where the swap would: it does the directory steps up to the crash point and "dies".
        void Guard(Action swap)
        {
            if (point >= CrashPoint.BetweenTheRenames) fs.RenameFile(Dst, SnapshotRestoreSwap.OldPath(Dst));
            if (point >= CrashPoint.AfterTheRenames) fs.RenameFile(SnapshotRestoreSwap.StagePath(Dst), Dst);
            throw new Crash();
        }
        await Assert.ThrowsExactlyAsync<Crash>(async () => await SnapshotReceiver.ReceiveAsync(
            Detached(primary.StreamInitialStateAsync(CancellationToken.None)), fs, Dst, [StateDir],
            new SnapshotRestoreOptions
            {
                Mode = SnapshotRestoreMode.AlwaysSwap, RequireLogDescription = true, OnLogDescription = d => described = d, SwapGuard = Guard,
            }, CancellationToken.None));
        Assert.IsNull(described);

        var log = new List<string>();
        string? pending = SnapshotRestoreSwap.Recover(fs, Dst, [StateDir], log.Add);
        AssertNoSwapDirectories(fs);
        Assert.IsTrue(fs.FileExists(Dst + "/" + StateDir + "/owner"), "the owner's state was lost\n" + string.Join("\n", log));
        if (point == CrashPoint.BeforeTheSwap)
        {
            // The old database with the state that describes it; the staged copy is discarded.
            Assert.IsNull(pending);
            await AssertHoldsAsync(fs, Dst, restored: false);
        }
        else
        {
            // The new database; its description is handed to the owner, who applies it before using its state.
            Assert.IsNotNull(pending, string.Join("\n", log));
            Assert.AreEqual(primary.Lineage.Serialize(), pending);
            SnapshotRestoreSwap.ClearPendingDescription(fs, Dst);
            Assert.IsNull(SnapshotRestoreSwap.Recover(fs, Dst, [StateDir], null));
            await AssertHoldsAsync(fs, Dst, restored: true);
        }
    }

    [TestMethod]
    [DataRow(SnapshotRestoreMode.Auto, "plenty", "restore-then-swap")]
    [DataRow(SnapshotRestoreMode.Auto, "little", "delete-first (not enough free disk space")]
    [DataRow(SnapshotRestoreMode.AlwaysDeleteFirst, "plenty", "delete-first (forced")]
    public async Task PairResyncRestoresNextToTheCopyWhenTheDiskHasRoom(SnapshotRestoreMode mode, string space, string expectedLog)
    {
        var fsA = new MemoryFileSystem();
        var fsB = new MemoryFileSystem();
        await using var h = new PairHarness { FileSystemOf = id => id == "A" ? fsA : fsB, ResyncRestoreMode = mode };
        await h.StartAsync("A");
        await h.StartAsync("B");
        await WaitUntilAsync(() => h["B"].GetStatus().AttachedFromSeq > 0, "B following");
        for (int i = 0; i < 20; i++)
        {
            using var b = new WriteBatch();
            b.Put(U(K(i)), U(V(i, 0)));
            Assert.AreEqual(PairWriteStatus.Applied, (await h["A"].WriteAsync(b, TimeSpan.FromSeconds(2), CancellationToken.None)).Status);
        }
        await WaitUntilAsync(() => h["B"].Db.GetLatestSequenceNumber() == h["A"].Db.GetLatestSequenceNumber(), "B caught up\n" + h.LogText);
        if (space == "little") fsB.FreeSpace = 1024;
        int before = h["B"].Resyncs;
        h["B"].TriggerResync("test");
        await WaitUntilAsync(() => h["B"].Resyncs > before && !h["B"].IsResyncing, "B to rebuild\n" + h.LogText);
        StringAssert.Contains(h.LogText, "snapshot restore: " + expectedLog);
        await h.AssertConvergedAsync("A", "B");
        Assert.IsFalse(fsB.DirectoryExists(SnapshotRestoreSwap.StagePath(h.DbPath("B"))));
        Assert.IsFalse(fsB.DirectoryExists(SnapshotRestoreSwap.OldPath(h.DbPath("B"))));
        // The replica still works as one: new writes arrive, and it restarts on the swapped-in copy with its role state.
        using (var b = new WriteBatch())
        {
            b.Put(U("after-resync"), U("1"));
            Assert.AreEqual(PairWriteStatus.Applied, (await h["A"].WriteAsync(b, TimeSpan.FromSeconds(2), CancellationToken.None)).Status);
        }
        await h.AssertConvergedAsync("A", "B");
        await h.StopAsync("B");
        await h.StartAsync("B");
        Assert.AreEqual(Switchover.ReplicationRole.Replica, h["B"].GetStatus().Node.Role);
        await h.AssertConvergedAsync("A", "B");
    }

    [TestMethod]
    public async Task PairReplicaCompletesASwapInterruptedByACrashWhenItStarts()
    {
        var fsA = new MemoryFileSystem();
        var fsB = new MemoryFileSystem();
        await using var h = new PairHarness { FileSystemOf = id => id == "A" ? fsA : fsB };
        await h.StartAsync("A");
        await h.StartAsync("B");
        await WaitUntilAsync(() => h["B"].GetStatus().AttachedFromSeq > 0, "B following");
        for (int i = 0; i < 20; i++)
        {
            using var b = new WriteBatch();
            b.Put(U(K(i)), U(V(i, 0)));
            Assert.AreEqual(PairWriteStatus.Applied, (await h["A"].WriteAsync(b, TimeSpan.FromSeconds(2), CancellationToken.None)).Status);
        }
        await h.AssertConvergedAsync("A", "B");
        await h.StopAsync("B");
        // More entries on the primary, then B's resync "crashes" right after the two renames: the new database is in
        // place, B's role state is still in the old copy and the epoch map of the snapshot is not stored yet.
        for (int i = 20; i < 30; i++)
        {
            using var b = new WriteBatch();
            b.Put(U(K(i)), U(V(i, 0)));
            Assert.AreEqual(PairWriteStatus.Applied, (await h["A"].WriteAsync(b, TimeSpan.FromSeconds(2), CancellationToken.None)).Status);
        }
        string dbB = h.DbPath("B");
        await Assert.ThrowsExactlyAsync<Crash>(async () => await SnapshotReceiver.ReceiveAsync(
            Detached(h["A"].StreamSnapshotAsync(CancellationToken.None)), fsB, dbB, ["role"],
            new SnapshotRestoreOptions
            {
                Mode = SnapshotRestoreMode.AlwaysSwap,
                RequireLogDescription = true,
                SwapGuard = _ =>
                {
                    fsB.RenameFile(dbB, SnapshotRestoreSwap.OldPath(dbB));
                    fsB.RenameFile(SnapshotRestoreSwap.StagePath(dbB), dbB);
                    throw new Crash();
                },
            }, CancellationToken.None));
        Assert.IsFalse(fsB.DirectoryExists(dbB + "/role"), "the premise: the role state is still in the old copy");

        await h.StartAsync("B");
        StringAssert.Contains(h.LogText, "applied the epoch map of the snapshot restored before the restart");
        Assert.AreEqual(Switchover.ReplicationRole.Replica, h["B"].GetStatus().Node.Role);
        Assert.IsFalse(fsB.DirectoryExists(SnapshotRestoreSwap.OldPath(dbB)));
        Assert.IsFalse(fsB.FileExists(dbB + "/" + SnapshotRestoreSwap.PendingDescriptionFileName));
        int resyncs = h["B"].Resyncs;
        await h.AssertConvergedAsync("A", "B");
        Assert.AreEqual(resyncs, h["B"].Resyncs, "the swapped-in copy was not accepted by the attach check\n" + h.LogText);
    }

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)] // real directories: the renames, the hard links of the reused files and the free space of a disk
    public async Task ClusterFollowerResyncSwapsAndRejoins(bool onDisk)
    {
        await using var c = new ClusterHarness(3, onDisk ? null : new MemoryFileSystem());
        IFileSystem fs = c.FileSystem;
        ClusterNodeHost n1 = await c.StartAsync("n1");
        for (int i = 0; i < 20; i++) Assert.IsTrue(await n1.TryWriteAsLeaderAsync(U(K(i)), U(V(i, 0)), CancellationToken.None));
        await c.StartAsync("n2");
        await c.StartAsync("n3");
        await WaitUntilAsync(() => c.Statuses().TrueForAll(s => s.Mode == ClusterMode.Cluster), "cluster mode\n" + c.LogText);
        ClusterNodeHost leader = await c.WaitForLeaderAsync(null);
        await c.AssertConvergedAsync(leader);
        ClusterNodeHost follower = c[new[] { "n1", "n2", "n3" }.First(id => id != leader.NodeId)];
        int before = follower.Resyncs;
        follower.TriggerResync("test");
        await WaitUntilAsync(() => follower.Resyncs > before && !follower.IsResyncing, "the follower to rebuild\n" + c.LogText);
        StringAssert.Contains(c.LogText, "snapshot restore: restore-then-swap");
        Assert.IsFalse(fs.DirectoryExists(SnapshotRestoreSwap.OldPath(c.DbPath(follower.NodeId))));
        Assert.IsFalse(fs.DirectoryExists(SnapshotRestoreSwap.StagePath(c.DbPath(follower.NodeId))));
        leader = await c.WaitForLeaderAsync(null);
        for (int i = 100; i < 105; i++) Assert.IsTrue(await leader.WriteCommittedAsync(U(K(i)), U(V(i, 0)), TimeSpan.FromSeconds(20), CancellationToken.None), c.LogText);
        await c.AssertConvergedAsync(leader);
        Assert.IsNull(follower.GetStatus().FollowerFault);
    }
}
