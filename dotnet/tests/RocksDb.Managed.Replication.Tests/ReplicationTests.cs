using RocksDb.Managed.Replication.Hosting;

namespace RocksDb.Managed.Replication.Tests;

/// <summary>
/// The scenarios of rocksdb-sharp's Tests/ReplicationTest as in-process tests: initial sync via checkpoint + streaming,
/// catch-up after a replica restart, streaming under concurrent writes, WAL purge -> NotFound -> resync (with the
/// per-file delta), WAL retention by replica progress, and the whole pipeline on a MemoryFileSystem.
/// </summary>
[TestClass]
public sealed class ReplicationTests
{
    private static CancellationToken Ct => TestContextCancellation.None;

    /// <summary>Checkpoint the source and restore it into <paramref name="replicaPath"/> through a (copying) snapshot stream.</summary>
    internal static async Task<(RocksDatabase Replica, SnapshotRestoreResult Result)> InitialSyncAsync(RocksDatabase source, string checkpointDir,
        string replicaPath, IFileSystem fs, IReadOnlyList<ReplicationFileInfo> inventory)
    {
        var src = new ReplicationSource(source);
        using ReplicationSession session = await src.GetInitialStateAsync(checkpointDir, Ct);
        Assert.AreEqual(1, src.SessionsInFlight);
        Assert.IsFalse(source.IsFileDeletionsEnabled, "the session pins file deletions");
        SnapshotRestoreResult r = await SnapshotReceiver.ReceiveAsync(
            InMemoryNetwork.Pump(ct => session.StreamSnapshotAsync(inventory, 16 * 1024, ct), static c => c.Detach(), CancellationToken.None, Ct), fs, replicaPath, ["keep-me"], Ct);
        Assert.AreEqual(session.SequenceNumber, r.SequenceNumber);
        RocksDatabase replica = await RocksDatabase.OpenAsync(replicaPath, DbOpts(fs));
        Assert.IsTrue(replica.GetLatestSequenceNumber() >= session.SequenceNumber, "checkpoint covers its sequence number");
        session.Dispose();
        Assert.AreEqual(0, src.SessionsInFlight);
        Assert.IsTrue(source.IsFileDeletionsEnabled);
        Assert.IsFalse(fs.DirectoryExists(checkpointDir), "session deletes its checkpoint");
        return (replica, r);
    }

    /// <summary>Streams source -> replica until cancelled; returns the number of chunks applied.</summary>
    internal static async Task<int> StreamAsync(RocksDatabase source, RocksDatabase replica, int maxChunkBytes, CancellationToken ct)
    {
        var consumer = new ReplicationConsumer(replica);
        using WalTailer tailer = new ReplicationSource(source).TailWal(replica.GetLatestSequenceNumber() + 1, maxChunkBytes);
        int chunks = 0;
        try
        {
            await foreach (WalChunk c in tailer.ReadChunksAsync(ct))
            {
                Assert.AreEqual(consumer.ExpectedNextSequence, c.FirstSequenceNumber);
                Assert.AreEqual(c.FirstSequenceNumber + (ulong)c.EntryCount, c.NextSequenceNumber);
                Assert.IsTrue(await consumer.TryIngestChunkAsync(c.Memory, ct));
                Assert.AreEqual(c.NextSequenceNumber - 1, replica.GetLatestSequenceNumber());
                chunks++;
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
        }
        return chunks;
    }

    private static async Task RunInitialSyncThenStreamingAsync(IFileSystem fs, string root)
    {
        string srcPath = root + "/source", replicaPath = root + "/replica";
        await using RocksDatabase source = await RocksDatabase.OpenAsync(srcPath, DbOpts(fs));
        await WriteWorkloadAsync(source, 0, 600, 0);
        await source.FlushAsync();
        await WriteWorkloadAsync(source, 600, 1200, 0);

        (RocksDatabase replica, SnapshotRestoreResult r) = await InitialSyncAsync(source, root + "/cp1", replicaPath, fs, []);
        await using (replica)
        {
            Assert.AreEqual(0, r.ReusedFiles);
            Assert.IsTrue(r.TransferredFiles >= 4, "CURRENT, MANIFEST, OPTIONS and SSTs");
            AssertReplicaEquals(source, replica, "after initial sync");

            using var cts = new CancellationTokenSource();
            Task<int> streaming = StreamAsync(source, replica, 4096, cts.Token);
            await WriteWorkloadAsync(source, 1200, 1800, 1);
            await source.FlushAsync(); // WAL rotation while tailing
            await WriteWorkloadAsync(source, 1800, 2400, 2);
            await WaitForSequenceAsync(replica, source.GetLatestSequenceNumber(), "streaming");
            await cts.CancelAsync();
            int chunks = await streaming;
            Assert.IsTrue(chunks > 1, "small chunks: many messages");
            AssertReplicaEquals(source, replica, "after streaming");
        }
    }

    [TestMethod]
    public async Task InitialSyncViaCheckpointThenStreaming()
    {
        using var dir = new TempDir("sync");
        await RunInitialSyncThenStreamingAsync(LocalFileSystem.Instance, dir.Path);
    }

    [TestMethod]
    public async Task InitialSyncAndStreamingOnMemoryFileSystem()
    {
        var fs = new MemoryFileSystem();
        await RunInitialSyncThenStreamingAsync(fs, "/mem");
    }

    [TestMethod]
    public async Task ReplicaCatchesUpAfterRestart()
    {
        using var dir = new TempDir("restart");
        DbOptions sourceOptions = DbOpts(null);
        sourceOptions.WalTtlSeconds = 3600; // obsolete WALs are archived, so a restarted replica can still read them
        await using RocksDatabase source = await RocksDatabase.OpenAsync(dir.Sub("source"), sourceOptions);
        await WriteWorkloadAsync(source, 0, 300, 0);
        (RocksDatabase replica, _) = await InitialSyncAsync(source, dir.Sub("cp"), dir.Sub("replica"), LocalFileSystem.Instance, []);
        using (var cts = new CancellationTokenSource())
        {
            Task<int> streaming = StreamAsync(source, replica, WalTailer.DefaultMaxChunkBytes, cts.Token);
            await WriteWorkloadAsync(source, 300, 600, 1);
            await WaitForSequenceAsync(replica, source.GetLatestSequenceNumber(), "first stream");
            await cts.CancelAsync();
            await streaming;
        }
        ulong stoppedAt = replica.GetLatestSequenceNumber();
        await replica.DisposeAsync();

        // The replica is down: the source keeps writing and rolls its WAL twice.
        await WriteWorkloadAsync(source, 600, 900, 2);
        await source.FlushAsync();
        await WriteWorkloadAsync(source, 900, 1200, 3);
        await source.FlushAsync();
        await WriteWorkloadAsync(source, 1200, 1300, 4);
        Assert.IsTrue(source.GetSortedWalFiles().Count >= 2);

        await using RocksDatabase restarted = await RocksDatabase.OpenAsync(dir.Sub("replica"), DbOpts(null));
        Assert.AreEqual(stoppedAt, restarted.GetLatestSequenceNumber(), "replica recovers its own WAL");
        using (var cts = new CancellationTokenSource())
        {
            Task<int> streaming = StreamAsync(source, restarted, 64 * 1024, cts.Token);
            await WaitForSequenceAsync(restarted, source.GetLatestSequenceNumber(), "catch-up");
            await cts.CancelAsync();
            await streaming;
        }
        AssertReplicaEquals(source, restarted, "after catch-up");
    }

    [TestMethod]
    public async Task ContinuousStreamingUnderConcurrentWrites()
    {
        using var dir = new TempDir("concurrent");
        await using RocksDatabase source = await RocksDatabase.OpenAsync(dir.Sub("source"), DbOpts(null));
        (RocksDatabase replica, _) = await InitialSyncAsync(source, dir.Sub("cp"), dir.Sub("replica"), LocalFileSystem.Instance, []);
        await using (replica)
        {
            using var cts = new CancellationTokenSource();
            Task<int> streaming = StreamAsync(source, replica, 32 * 1024, cts.Token);
            const int Writers = 4, PerWriter = 1500;
            var writers = new Task[Writers];
            for (int w = 0; w < Writers; w++)
            {
                int writer = w;
                writers[w] = Task.Run(async () =>
                {
                    for (int i = 0; i < PerWriter; i++)
                    {
                        using WriteBatch b = source.CreateWriteBatch();
                        FillBatch(b, writer * 1_000_000 + i, writer);
                        await source.WriteAsync(b);
                        if (writer == 0 && i % 500 == 499) await source.FlushAsync();
                    }
                });
            }
            await Task.WhenAll(writers);
            await WaitForSequenceAsync(replica, source.GetLatestSequenceNumber(), "concurrent streaming");
            await cts.CancelAsync();
            int chunks = await streaming;
            Assert.IsTrue(chunks > 0);
            AssertReplicaEquals(source, replica, "after concurrent writes");
            Assert.AreEqual(DatabaseDigest.Compute(source), DatabaseDigest.Compute(replica));
        }
    }

    [TestMethod]
    public async Task SourceWalPurgeGivesNotFoundThenResyncFromCheckpoint()
    {
        using var dir = new TempDir("purge");
        // No WAL_ttl_seconds: a flushed WAL is deleted at once.
        await using RocksDatabase source = await RocksDatabase.OpenAsync(dir.Sub("source"), DbOpts(null));
        await WriteWorkloadAsync(source, 0, 400, 0);
        await source.FlushAsync();
        await WriteWorkloadAsync(source, 400, 800, 0);
        (RocksDatabase replica, _) = await InitialSyncAsync(source, dir.Sub("cp1"), dir.Sub("replica"), LocalFileSystem.Instance, []);
        ulong replicaAt = replica.GetLatestSequenceNumber();
        await replica.DisposeAsync();

        await WriteWorkloadAsync(source, 800, 1200, 1);
        await source.FlushAsync(); // the WAL holding replicaAt + 1 is gone

        // Nothing written since the flush: the tailer must still report the purge instead of waiting forever.
        using (WalTailer tailer = new ReplicationSource(source).TailWal(replicaAt + 1))
        {
            var e = Assert.ThrowsExactly<RocksDbException>(() => tailer.TryReadChunk(out _));
            Assert.AreEqual(StatusCode.NotFound, e.Code);
        }
        await WriteWorkloadAsync(source, 1200, 1300, 2);
        using (WalTailer tailer = new ReplicationSource(source).TailWal(replicaAt + 1))
        {
            var e = Assert.ThrowsExactly<RocksDbException>(() => tailer.TryReadChunk(out _));
            Assert.AreEqual(StatusCode.NotFound, e.Code);
        }
        // Through the hosting stream: one terminal resync message.
        var messages = new List<WalStreamMessage>();
        await foreach (WalStreamMessage m in WalStreamSender.StreamAsync(source, replicaAt + 1, Tag, static _ => 0, 4096, Ct)) messages.Add(m);
        Assert.AreEqual(1, messages.Count);
        Assert.IsTrue(messages[0].ResyncRequired);

        // Resync with the per-file delta: the replica's SSTs are hard-linked copies of the source's, so they are reused.
        List<ReplicationFileInfo> local = ReplicationDelta.ScanImmutableFiles(dir.Sub("replica"));
        Assert.IsTrue(local.Count > 0);
        var sourceHashes = new Dictionary<string, string>();
        foreach (ReplicationFileInfo f in local)
        {
            string? h = await ReplicationDelta.TryHashCandidateAsync(dir.Sub("source"), f.FileName, f.Size, Ct);
            if (h != null) sourceHashes[f.FileName] = h;
        }
        List<ReplicationFileInfo> verified = await ReplicationDelta.VerifyCandidatesAsync(LocalFileSystem.Instance, dir.Sub("replica"), local, sourceHashes, Ct);
        Assert.IsTrue(verified.Count > 0, "unchanged SSTs verify");
        Directory.CreateDirectory(dir.Sub("replica/keep-me"));
        File.WriteAllText(dir.Sub("replica/stale.sst"), "junk that must not survive");

        (RocksDatabase resynced, SnapshotRestoreResult r) = await InitialSyncAsync(source, dir.Sub("cp2"), dir.Sub("replica"), LocalFileSystem.Instance, verified);
        await using (resynced)
        {
            Assert.AreEqual(verified.Count, r.ReusedFiles);
            Assert.IsFalse(File.Exists(dir.Sub("replica/stale.sst")));
            Assert.IsTrue(Directory.Exists(dir.Sub("replica/keep-me")), "preserved names survive the restore");
            AssertReplicaEquals(source, resynced, "after resync");
            using var cts = new CancellationTokenSource();
            Task<int> streaming = StreamAsync(source, resynced, 4096, cts.Token);
            await WriteWorkloadAsync(source, 1300, 1500, 3);
            await WaitForSequenceAsync(resynced, source.GetLatestSequenceNumber(), "stream after resync");
            await cts.CancelAsync();
            await streaming;
            AssertReplicaEquals(source, resynced, "after resync + stream");
        }

        static bool Tag(out string id, out long term)
        {
            id = "source";
            term = 1;
            return true;
        }
    }

    [TestMethod]
    public async Task LargeChunksAppliedInParallelMatchSerialApply()
    {
        using var dir = new TempDir("parallel-apply");
        DbOptions sourceOptions = DbOpts(null);
        sourceOptions.WalTtlSeconds = 600; // keep the flushed WAL for the tailer
        await using RocksDatabase source = await RocksDatabase.OpenAsync(dir.Sub("source"), sourceOptions);
        // Concurrent writers with overlapping keys (puts, deletes and range deletes of each other's keys).
        const int Writers = 4, Batches = 40, PerBatch = 50;
        var writers = new Task[Writers];
        for (int w = 0; w < Writers; w++)
        {
            int writer = w;
            writers[w] = Task.Run(async () =>
            {
                for (int i = 0; i < Batches; i++)
                {
                    using WriteBatch b = source.CreateWriteBatch();
                    for (int j = 0; j < PerBatch; j++) FillBatch(b, (i * PerBatch + j) * 2 + writer % 2, writer);
                    await source.WriteAsync(b);
                    if (writer == 0 && i == Batches / 2) await source.FlushAsync();
                }
            });
        }
        await Task.WhenAll(writers);

        var chunks = new List<byte[]>();
        var boundaries = new HashSet<ulong> { 0 };
        using (WalTailer tailer = new ReplicationSource(source).TailWal(1, WalTailer.DefaultMaxChunkBytes))
        {
            while (tailer.TryReadChunk(out WalChunk c))
            {
                chunks.Add(c.Memory.ToArray());
                boundaries.Add(c.NextSequenceNumber - 1);
            }
        }
        Assert.IsTrue(chunks.Exists(c => new WalBatch(c).Count >= 1024), "the workload produces chunks large enough to split");

        foreach (int parallelism in new[] { 1, 4 })
        {
            await using RocksDatabase replica = await RocksDatabase.OpenAsync(dir.Sub("replica" + parallelism), DbOpts(null));
            var consumer = new ReplicationConsumer(replica) { ApplyParallelism = parallelism };
            Assert.AreEqual(parallelism, consumer.ApplyParallelism);
            // A reader never observes a partially applied chunk: the published sequence is always a chunk boundary.
            using var stop = new CancellationTokenSource();
            ulong torn = 0;
            Task reader = Task.Run(() =>
            {
                while (!stop.IsCancellationRequested)
                {
                    ulong seq = replica.GetLatestSequenceNumber();
                    if (!boundaries.Contains(seq)) torn = seq;
                }
            });
            foreach (byte[] c in chunks) Assert.IsTrue(await consumer.TryIngestChunkAsync(c, Ct));
            await stop.CancelAsync();
            await reader;
            Assert.AreEqual(0UL, torn, "published sequence inside a chunk");
            AssertReplicaEquals(source, replica, "parallelism " + parallelism);
            Assert.AreEqual(DatabaseDigest.Compute(source), DatabaseDigest.Compute(replica));
            await replica.FlushAsync();
            AssertReplicaEquals(source, replica, "parallelism " + parallelism + " after flush");
        }
        Assert.ThrowsExactly<ArgumentOutOfRangeException>(() => new ReplicationConsumer(source) { ApplyParallelism = 0 });
    }

    [TestMethod]
    public async Task ConsumerRejectsGapsAndOverlaps()
    {
        using var dir = new TempDir("gap");
        await using RocksDatabase source = await RocksDatabase.OpenAsync(dir.Sub("source"), DbOpts(null));
        await using RocksDatabase replica = await RocksDatabase.OpenAsync(dir.Sub("replica"), DbOpts(null));
        await WriteWorkloadAsync(source, 0, 10, 0);
        var batches = new List<byte[]>();
        await foreach (WalBatch b in new ReplicationSource(source).GetWalUpdatesAsync(1, Ct)) batches.Add(b.Span.ToArray());
        Assert.AreEqual(10, batches.Count);
        var consumer = new ReplicationConsumer(replica);
        Assert.IsFalse(await consumer.TryIngestChunkAsync(batches[1], Ct), "gap");
        Assert.AreEqual(0UL, replica.GetLatestSequenceNumber());
        Assert.IsTrue(await consumer.TryIngestChunkAsync(batches[0], Ct));
        Assert.IsFalse(await consumer.TryIngestChunkAsync(batches[0], Ct), "overlap");
        for (int i = 1; i < batches.Count; i++) await consumer.IngestBatchAsync(new WalBatch(batches[i]), Ct);
        AssertReplicaEquals(source, replica, "batch by batch");

        // A tail cannot start inside a multi-entry batch.
        var e = Assert.ThrowsExactly<RocksDbException>(() =>
        {
            using WalTailer t = new ReplicationSource(source).TailWal(2); // batch 0 holds sequences 1..2 (i = 0 puts twice)
            t.TryReadChunk(out _);
        });
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
    }

    [TestMethod]
    public async Task ArchivedWalsAreRetiredByReplicaProgress()
    {
        using var dir = new TempDir("retire");
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
        Assert.AreEqual(0, src.RetireArchivedWals(), "no replica reported yet");
        src.ReportReplicaProgress("r1", boundaries[1]);
        src.ReportReplicaProgress("r2", boundaries[0]);
        Assert.AreEqual(boundaries[0], src.SlowestReplicaSequence);
        using (ReplicationSession s = await src.GetInitialStateAsync(dir.Sub("cp"), Ct))
        {
            Assert.AreEqual(0, src.RetireArchivedWals(), "nothing retired while a checkpoint session is in flight");
        }
        await WriteWorkloadAsync(source, 300, 310, 9);
        IReadOnlyList<WalFileInfo> before = source.GetSortedWalFiles();
        int archived = 0;
        foreach (WalFileInfo f in before) archived += f.Type == WalFileType.ArchivedLogFile ? 1 : 0;
        Assert.AreEqual(3, archived);

        Assert.AreEqual(1, src.RetireArchivedWals(), "only the WAL the slowest replica finished");
        src.ReportReplicaProgress("r2", boundaries[1] - 1);
        Assert.AreEqual(0, src.RetireArchivedWals(), "the second WAL is not finished by r2 yet");
        src.ReportReplicaProgress("r2", boundaries[2]);
        Assert.AreEqual(1, src.RetireArchivedWals(), "r1 still holds the third one back");
        src.ForgetReplica("r1");
        Assert.AreEqual(1, src.RetireArchivedWals());
        foreach (WalFileInfo f in source.GetSortedWalFiles()) Assert.AreEqual(WalFileType.AliveLogFile, f.Type);

        // Retired WALs: a replica that asks for them gets NotFound.
        using WalTailer tailer = src.TailWal(1);
        Assert.AreEqual(StatusCode.NotFound, Assert.ThrowsExactly<RocksDbException>(() => tailer.TryReadChunk(out _)).Code);
    }
}

/// <summary>Shorthand for tests that do not cancel.</summary>
internal static class TestContextCancellation
{
    public static CancellationToken None => CancellationToken.None;
}
