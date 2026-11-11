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
}
