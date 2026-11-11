using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>
/// Ingestion with IngestExternalFileArg.atomic_replace_range (db/external_sst_file_ingestion_job.cc, DBImpl::
/// IngestExternalFiles). Ports of db/external_sst_file_basic_test.cc (AtomicReplaceRangeGeneratesTombstoneForPartialOverlap,
/// AtomicReplaceRangeNotSupportedWithFIFOCompaction, AtomicReplaceRangeRacesWithIntraL0UniversalCompaction, the
/// atomic_replace_range cases of IngestWithTemperature / ReplaceRange validation) and db/external_sst_file_test.cc,
/// plus conflict and crash-atomicity tests.
/// </summary>
[TestClass]
public sealed class AtomicReplaceRangeTests
{
    [TestCleanup]
    public void Cleanup() => TestHooks.ClearAll();

    private static string WriteSst(IFileSystem fs, string path, params (string Key, string Value)[] entries)
    {
        using var w = new SstFileWriter(new ColumnFamilyOptions(), fs);
        w.Open(path);
        foreach ((string k, string v) in entries) w.Put(B(k), B(v));
        w.Finish();
        return path;
    }

    private static MemoryFileSystem NewFsWithExt()
    {
        MemoryFileSystem fs = NewFs();
        fs.CreateDirectoryIfMissing("/ext");
        return fs;
    }

    private static string FilesPerLevel(RocksDatabase db, int numLevels)
    {
        var parts = new string[numLevels];
        for (int l = 0; l < numLevels; l++) parts[l] = db.DefaultColumnFamily.GetProperty("rocksdb.num-files-at-level" + l)!;
        return string.Join(",", parts);
    }

    private static IngestExternalFileArg ReplaceArg(string file, string? start, string? limit) => new()
    {
        ExternalFiles = [file],
        Options = new IngestExternalFileOptions { SnapshotConsistency = false },
        AtomicReplaceRange = new IngestReplaceRange(start is null ? null : B(start), limit is null ? null : B(limit)),
    };

    private static IngestExternalFileArg With(IngestExternalFileArg a, IngestExternalFileOptions o) =>
        new() { ExternalFiles = a.ExternalFiles, Options = o, AtomicReplaceRange = a.AtomicReplaceRange };

    private static void AssertCode(StatusCode code, Action a)
    {
        RocksDbException e = Assert.ThrowsExactly<RocksDbException>(a);
        Assert.AreEqual(code, e.Code, e.Message);
    }

    /// <summary>Three old files ingested to the bottommost level: {a, c}, {d}, {e, f, h}.</summary>
    private static void IngestOldFiles(MemoryFileSystem fs, RocksDatabase db)
    {
        db.IngestExternalFiles(
        [
            WriteSst(fs, "/ext/old-0.sst", ("a", "old-a"), ("c", "old-c")),
            WriteSst(fs, "/ext/old-1.sst", ("d", "old-d")),
            WriteSst(fs, "/ext/old-2.sst", ("e", "old-e"), ("f", "old-f"), ("h", "old-h")),
        ], new IngestExternalFileOptions());
    }

    /// <summary>Port of AtomicReplaceRangeGeneratesTombstoneForPartialOverlap (universal compaction).</summary>
    [TestMethod]
    public void UniversalPartialOverlapIngestsRangeTombstoneBelowNewFiles()
    {
        MemoryFileSystem fs = NewFsWithExt();
        var cfo = new ColumnFamilyOptions { CompactionStyle = CompactionStyle.Universal };
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
        IngestOldFiles(fs, db);
        Assert.AreEqual("0,0,0,0,0,0,3", FilesPerLevel(db, 7));

        string replacement = WriteSst(fs, "/ext/replacement.sst", ("b", "new-b"), ("e", "new-e"));
        IngestExternalFileArg arg = ReplaceArg(replacement, "b", "f");

        // A partial overlap without global seqnos cannot be handled.
        AssertCode(StatusCode.InvalidArgument, () => db.IngestExternalFiles(db.DefaultColumnFamily,
            With(arg, new IngestExternalFileOptions { SnapshotConsistency = false, AllowGlobalSeqNo = false })));
        // The retained file requires the tombstone and the replacement files above Lmax.
        AssertCode(StatusCode.TryAgain, () => db.IngestExternalFiles(db.DefaultColumnFamily,
            With(arg, new IngestExternalFileOptions { SnapshotConsistency = false, FailIfNotBottommostLevel = true })));
        Assert.AreEqual("0,0,0,0,0,0,3", FilesPerLevel(db, 7));
        Assert.AreEqual(3, fs.GetChildren(DbPath).Count(n => n.EndsWith(".sst", StringComparison.Ordinal)), "failed attempts leave no files behind");

        using Snapshot snapshot = db.GetSnapshot();
        ulong before = db.GetLatestSequenceNumber();
        db.IngestExternalFiles(db.DefaultColumnFamily, arg);
        // One sequence number for the tombstone, one for the replacement file.
        Assert.AreEqual(before + 2, db.GetLatestSequenceNumber());

        Assert.AreEqual("old-a", GetString(db, "a"));
        Assert.AreEqual("new-b", GetString(db, "b"));
        Assert.IsNull(GetString(db, "c"));
        Assert.IsNull(GetString(db, "d"));
        Assert.AreEqual("new-e", GetString(db, "e"));
        Assert.AreEqual("old-f", GetString(db, "f"));
        Assert.AreEqual("old-h", GetString(db, "h"));
        // {d} was replaced; the tombstone sits above the retained files, the replacement above the tombstone.
        Assert.AreEqual("0,0,0,0,1,1,2", FilesPerLevel(db, 7));

        db.WaitForCompact();
        // The snapshot still sees the retained data (the tombstone hides it only for newer reads).
        Assert.AreEqual("old-c", GetString(db, "c", snapshot));
        Assert.AreEqual("old-e", GetString(db, "e", snapshot));

        snapshot.Dispose();
        db.CompactRange();
        Assert.IsNull(GetString(db, "c"));
        Assert.IsNull(GetString(db, "d"));
        Assert.AreEqual("new-e", GetString(db, "e"));
        Assert.AreEqual("old-f", GetString(db, "f"));
        Assert.AreEqual("new-b", GetString(db, "b"));

        db.Dispose();
        using RocksDatabase re = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
        Assert.IsNull(GetString(re, "c"));
        Assert.AreEqual("new-e", GetString(re, "e"));
        Assert.AreEqual("old-a", GetString(re, "a"));
    }

    [TestMethod]
    public void ContainedFilesAreDeletedAndPartialOverlapFailsWithLeveledCompaction()
    {
        MemoryFileSystem fs = NewFsWithExt();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        IngestOldFiles(fs, db);
        string replacement = WriteSst(fs, "/ext/replacement.sst", ("c", "new-c"));

        // [b, f) partially overlaps {a, c} and {e, f, h}: no range tombstone with leveled compaction.
        AssertCode(StatusCode.InvalidArgument, () => db.IngestExternalFiles(db.DefaultColumnFamily, ReplaceArg(replacement, "b", "f")));
        Assert.AreEqual("old-c", GetString(db, "c"));

        // [c, e) only partially overlaps {a, c}, too.
        AssertCode(StatusCode.InvalidArgument, () => db.IngestExternalFiles(db.DefaultColumnFamily, ReplaceArg(replacement, "c", "e")));

        // [a, e) contains {a, c} and {d}: both are deleted, {e, f, h} stays (the limit is exclusive).
        db.IngestExternalFiles(db.DefaultColumnFamily, ReplaceArg(replacement, "a", "e"));
        Assert.IsNull(GetString(db, "a"));
        Assert.AreEqual("new-c", GetString(db, "c"));
        Assert.IsNull(GetString(db, "d"));
        Assert.AreEqual("old-e", GetString(db, "e"));
        Assert.AreEqual("old-h", GetString(db, "h"));
        Assert.AreEqual("0,0,0,0,0,0,2", FilesPerLevel(db, 7), "the replacement goes to the bottommost level");
        // The replaced files are gone from disk (obsolete files are purged).
        Assert.AreEqual(2, fs.GetChildren(DbPath).Count(n => n.EndsWith(".sst", StringComparison.Ordinal)));

        db.Dispose();
        using RocksDatabase re = RocksDatabase.Open(DbPath, NewOptions(fs));
        Assert.IsNull(GetString(re, "a"));
        Assert.AreEqual("new-c", GetString(re, "c"));
        Assert.AreEqual("old-e", GetString(re, "e"));
    }

    [TestMethod]
    public void EntireColumnFamilyIsReplacedIncludingMemtableData()
    {
        MemoryFileSystem fs = NewFsWithExt();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        IngestOldFiles(fs, db);
        db.Put(B("m"), B("mem-m"));
        db.Flush();
        db.Put(B("z"), B("mem-z")); // unflushed: the whole-CF replacement flushes it first
        string replacement = WriteSst(fs, "/ext/replacement.sst", ("c", "new-c"), ("x", "new-x"));

        // A blocking flush is required (the memtable is not empty).
        AssertCode(StatusCode.InvalidArgument, () => db.IngestExternalFiles(db.DefaultColumnFamily,
            With(ReplaceArg(replacement, null, null), new IngestExternalFileOptions { SnapshotConsistency = false, AllowBlockingFlush = false })));

        db.IngestExternalFiles(db.DefaultColumnFamily, new IngestExternalFileArg
        {
            ExternalFiles = [replacement],
            Options = new IngestExternalFileOptions { SnapshotConsistency = false },
            AtomicReplaceRange = IngestReplaceRange.EntireColumnFamily,
        });
        foreach (string k in new[] { "a", "d", "e", "h", "m", "z" }) Assert.IsNull(GetString(db, k), k);
        Assert.AreEqual("new-c", GetString(db, "c"));
        Assert.AreEqual("new-x", GetString(db, "x"));
        Assert.AreEqual(1, db.GetLiveFilesMetadata().Count);
        Assert.AreEqual(6, db.GetLiveFilesMetadata()[0].Level);

        db.Dispose();
        using RocksDatabase re = RocksDatabase.Open(DbPath, NewOptions(fs));
        Assert.IsNull(GetString(re, "z"), "the replaced memtable data must not come back from the WAL");
        Assert.AreEqual("new-c", GetString(re, "c"));
    }

    [TestMethod]
    public void MemtableOverlapIsCheckedAgainstTheReplacedRange()
    {
        MemoryFileSystem fs = NewFsWithExt();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        IngestOldFiles(fs, db);
        db.Put(B("d1"), B("mem-d1")); // inside [c0, e) but outside the file range [d, d]
        string replacement = WriteSst(fs, "/ext/replacement.sst", ("d", "new-d"));
        var noFlush = new IngestExternalFileOptions { SnapshotConsistency = false, AllowBlockingFlush = false };
        AssertCode(StatusCode.InvalidArgument, () => db.IngestExternalFiles(db.DefaultColumnFamily, With(ReplaceArg(replacement, "c0", "e"), noFlush)));
        db.Flush(); // d1 moves to an L0 file inside the range: it is replaced too
        db.Put(B("e"), B("mem-e"));   // at the exclusive limit: not in the range
        db.Put(B("b0"), B("mem-b0")); // before the range start
        db.IngestExternalFiles(db.DefaultColumnFamily, With(ReplaceArg(replacement, "c0", "e"), noFlush));
        Assert.AreEqual("new-d", GetString(db, "d"));
        Assert.IsNull(GetString(db, "d1"));
        Assert.AreEqual("mem-e", GetString(db, "e"));
        Assert.AreEqual("mem-b0", GetString(db, "b0"));
        Assert.AreEqual("old-c", GetString(db, "c"));
        Assert.AreEqual("old-h", GetString(db, "h"));
    }

    [TestMethod]
    public void ArgumentValidation()
    {
        MemoryFileSystem fs = NewFsWithExt();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        IngestOldFiles(fs, db);
        string replacement = WriteSst(fs, "/ext/replacement.sst", ("c", "new-c"));

        // snapshot_consistency = true is not yet supported.
        AssertCode(StatusCode.NotSupported, () => db.IngestExternalFiles(db.DefaultColumnFamily,
            With(ReplaceArg(replacement, "a", "e"), new IngestExternalFileOptions())));
        // Only one bound.
        AssertCode(StatusCode.NotSupported, () => db.IngestExternalFiles(db.DefaultColumnFamily, ReplaceArg(replacement, null, "e")));
        AssertCode(StatusCode.NotSupported, () => db.IngestExternalFiles(db.DefaultColumnFamily, ReplaceArg(replacement, "a", null)));
        // ingest_behind.
        AssertCode(StatusCode.InvalidArgument, () => db.IngestExternalFiles(db.DefaultColumnFamily,
            With(ReplaceArg(replacement, "a", "e"), new IngestExternalFileOptions { SnapshotConsistency = false, IngestBehind = true })));
        // limit <= start.
        AssertCode(StatusCode.InvalidArgument, () => db.IngestExternalFiles(db.DefaultColumnFamily, ReplaceArg(replacement, "e", "a")));
        AssertCode(StatusCode.InvalidArgument, () => db.IngestExternalFiles(db.DefaultColumnFamily, ReplaceArg(replacement, "c", "c")));
        // The files must be inside the range ([a, c) excludes c).
        AssertCode(StatusCode.InvalidArgument, () => db.IngestExternalFiles(db.DefaultColumnFamily, ReplaceArg(replacement, "a", "c")));
        AssertCode(StatusCode.InvalidArgument, () => db.IngestExternalFiles(db.DefaultColumnFamily, ReplaceArg(replacement, "x", "z")));

        Assert.AreEqual("old-c", GetString(db, "c"));
        Assert.IsTrue(fs.FileExists(replacement));
        Assert.AreEqual(3, fs.GetChildren(DbPath).Count(n => n.EndsWith(".sst", StringComparison.Ordinal)));
    }

    /// <summary>Port of AtomicReplaceRangeNotSupportedWithFIFOCompaction.</summary>
    [TestMethod]
    public void NotSupportedWithFifoCompaction()
    {
        foreach (bool replaceAll in new[] { false, true })
        {
            MemoryFileSystem fs = NewFsWithExt();
            var cfo = new ColumnFamilyOptions { CompactionStyle = CompactionStyle.Fifo, NumLevels = 1, DisableAutoCompactions = true };
            using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
            db.Put(B("c"), B("old-c"));
            db.Put(B("d"), B("old-d"));
            db.Flush();
            db.Put(B("x"), B("old-x"));
            db.Flush();
            string replacement = WriteSst(fs, "/ext/replacement.sst", ("c", "new-c"));
            IngestExternalFileArg arg = ReplaceArg(replacement, replaceAll ? null : "c", replaceAll ? null : "e");
            arg = With(arg, new IngestExternalFileOptions { SnapshotConsistency = false, MoveFiles = true });
            AssertCode(StatusCode.NotSupported, () => db.IngestExternalFiles(db.DefaultColumnFamily, arg));
            Assert.IsTrue(fs.FileExists(replacement));
            Assert.AreEqual("old-c", GetString(db, "c"));
            Assert.AreEqual("old-d", GetString(db, "d"));

            db.IngestExternalFiles(db.DefaultColumnFamily, new IngestExternalFileArg { ExternalFiles = [replacement], Options = arg.Options });
            Assert.AreEqual("new-c", GetString(db, "c"));
            Assert.AreEqual("old-d", GetString(db, "d"));
            Assert.AreEqual("old-x", GetString(db, "x"));
        }
    }

    /// <summary>
    /// A running compaction over the range makes the ingestion fail (Run: RangeOverlapWithCompaction / being_compacted;
    /// any running compaction for the whole column family); it succeeds once the compaction is done.
    /// </summary>
    [TestMethod]
    public void RunningCompactionOverTheRangeConflicts()
    {
        // (compacted keys, replace the whole column family, conflict expected)
        foreach ((string key, bool whole, bool expectConflict) in new[] { ("c", false, true), ("x", true, true), ("x", false, false) })
        {
            string context = key + " whole=" + whole;
            MemoryFileSystem fs = NewFsWithExt();
            using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { DisableAutoCompactions = true });
            // {c, d} and {x} in the last level.
            db.IngestExternalFiles([WriteSst(fs, "/ext/cd.sst", ("c", "old-c"), ("d", "old-d")), WriteSst(fs, "/ext/x.sst", ("x", "old-x"))],
                new IngestExternalFileOptions());
            // More L0 files of the compacted key: a real (not trivial move) compaction to L1.
            db.Put(B(key + "1"), B("v1"));
            db.Flush();
            db.Put(B(key + "2"), B("v2"));
            db.Put(B(key), B("old-" + key));
            db.Flush();
            string[] inputs = db.GetLiveFilesMetadata().Where(m => m.Level == 0).Select(m => m.Name).ToArray();
            Assert.AreEqual(2, inputs.Length, context);
            string replacement = WriteSst(fs, "/ext/replacement.sst", ("c", "new-c"));

            using var started = new ManualResetEventSlim();
            using var release = new ManualResetEventSlim();
            TestHooks.Set("DBImpl::RunCompaction:Start", () =>
            {
                started.Set();
                release.Wait();
            });
            Task compaction = Task.Run(() => db.CompactFiles(inputs, 6));
            try
            {
                Assert.IsTrue(started.Wait(TimeSpan.FromSeconds(30)), context);
                TestHooks.Set("DBImpl::RunCompaction:Start", null);
                IngestExternalFileArg arg = ReplaceArg(replacement, whole ? null : "c", whole ? null : "e");
                if (expectConflict)
                {
                    AssertCode(StatusCode.InvalidArgument, () => db.IngestExternalFiles(db.DefaultColumnFamily, arg));
                    Assert.AreEqual("old-c", GetString(db, "c"), context);
                }
                else
                {
                    // [c, e) does not overlap the running compaction of x*: no conflict.
                    db.IngestExternalFiles(db.DefaultColumnFamily, arg);
                    Assert.AreEqual("new-c", GetString(db, "c"), context);
                }
            }
            finally
            {
                TestHooks.Set("DBImpl::RunCompaction:Start", null);
                release.Set();
                compaction.GetAwaiter().GetResult();
            }
            Assert.AreEqual("old-x", GetString(db, "x"), context);
        }
    }

    /// <summary>
    /// Port of AtomicReplaceRangeRacesWithIntraL0UniversalCompaction: while the ingestion writes its MANIFEST record
    /// (DB mutex released) the replaced file is marked as being compacted, so no compaction can take it as input.
    /// </summary>
    [TestMethod]
    public void ReplacedFilesCannotBeCompactedWhileTheManifestIsWritten()
    {
        MemoryFileSystem fs = NewFsWithExt();
        var cfo = new ColumnFamilyOptions { CompactionStyle = CompactionStyle.Universal, NumLevels = 1, DisableAutoCompactions = true };
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
        db.Put(B("a"), B("old-a"));
        db.Put(B("b"), B("old-b"));
        db.Flush();
        db.Put(B("c"), B("old-c"));
        db.Put(B("d"), B("old-d"));
        db.Flush();
        string replaced = db.GetLiveFilesMetadata().Single(m => m.SmallestKey.AsSpan().SequenceEqual(B("c"))).Name;
        string other = db.GetLiveFilesMetadata().Single(m => m.SmallestKey.AsSpan().SequenceEqual(B("a"))).Name;
        string replacement = WriteSst(fs, "/ext/replacement.sst", ("c", "new-c"));

        RocksDbException? replacedError = null;
        bool hookRan = false;
        TestHooks.Set("DBImpl::IngestExternalFiles:BeforeLogAndApply", () =>
        {
            hookRan = true;
            // The DB mutex is released here: the compaction must not take the file the ingestion is deleting.
            try { db.CompactFiles([replaced, other], 0); }
            catch (RocksDbException e) { replacedError = e; }
        });
        db.IngestExternalFiles(db.DefaultColumnFamily, ReplaceArg(replacement, "c", "e"));
        TestHooks.Set("DBImpl::IngestExternalFiles:BeforeLogAndApply", null);
        Assert.IsTrue(hookRan);
        Assert.IsNotNull(replacedError, "a compaction picked the replaced file");
        Assert.AreEqual("old-a", GetString(db, "a"));
        Assert.AreEqual("new-c", GetString(db, "c"));
        Assert.IsNull(GetString(db, "d"));

        // The marks are released: the remaining files can be compacted.
        db.CompactRange();
        Assert.AreEqual("old-a", GetString(db, "a"));
        Assert.AreEqual("new-c", GetString(db, "c"));
        Assert.IsNull(GetString(db, "d"));
    }

    /// <summary>
    /// A failure while writing the MANIFEST leaves the column family as it was: the replaced files are released (no
    /// longer marked as being compacted) and compactable. The files written for the edit are kept (see below).
    /// </summary>
    [TestMethod]
    public void FailedCommitReleasesReplacedFiles()
    {
        var fs = new FaultInjectionFileSystem(NewFs());
        fs.CreateDirectoryIfMissing("/ext");
        var cfo = new ColumnFamilyOptions { CompactionStyle = CompactionStyle.Universal, DisableAutoCompactions = true };
        DbOptions opts = NewOptions(fs);
        using RocksDatabase db = RocksDatabase.Open(DbPath, opts, cfo);
        db.IngestExternalFiles(
        [
            WriteSst(fs, "/ext/old-0.sst", ("a", "old-a"), ("c", "old-c")),
            WriteSst(fs, "/ext/old-1.sst", ("d", "old-d")),
        ], new IngestExternalFileOptions());
        string replacement = WriteSst(fs, "/ext/replacement.sst", ("c", "new-c"));
        fs.FailMatching("MANIFEST", FaultOps.Append | FaultOps.Sync, 1, InjectedFaultKind.IOError);
        Assert.ThrowsExactly<RocksDbException>(() => db.IngestExternalFiles(db.DefaultColumnFamily, ReplaceArg(replacement, "b", "e")));
        fs.ClearFailMatching();
        // The edit's record may have reached the MANIFEST before the I/O error, so the copied file and the generated
        // tombstone file are kept (upstream quarantines the files of such an edit); the obsolete-file scan of a later
        // open removes them when the record is not there.
        Assert.AreEqual(4, fs.Inner.GetChildren(DbPath).Count(n => n.EndsWith(".sst", StringComparison.Ordinal)), "files of an edit that hit a MANIFEST I/O error are kept");
        foreach (LiveFileMetadata m in db.GetLiveFilesMetadata()) Assert.AreEqual(6, m.Level);
    }

    /// <summary>
    /// Crash atomicity: a crash (power loss) at any file system operation of an atomic replace (with a generated range
    /// tombstone) recovers either the old contents of the range or the new ones, never a mix.
    /// </summary>
    [TestMethod]
    public void CrashAtAnyPointIsAtomic()
    {
        long total = RunCrashCase(crashAt: 0, out _);
        Assert.IsTrue(total > 5, "operations counted: " + total);
        int sawOld = 0, sawNew = 0;
        for (long at = 1; at <= total + 1; at++)
        {
            RunCrashCase(at, out bool replaced);
            if (replaced) sawNew++;
            else sawOld++;
        }
        Assert.IsTrue(sawOld > 0 && sawNew > 0, "old=" + sawOld + " new=" + sawNew);
    }

    private static readonly ColumnFamilyOptions CrashCfOptions = new() { CompactionStyle = CompactionStyle.Universal, DisableAutoCompactions = true };

    /// <summary>Runs the replacement crashing at operation <paramref name="crashAt"/> (0: none); returns the operations used.</summary>
    private static long RunCrashCase(long crashAt, out bool replaced)
    {
        string context = "crashAt=" + crashAt;
        var fs = new FaultInjectionFileSystem(NewFs());
        fs.CreateDirectoryIfMissing("/ext");
        RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), CrashCfOptions);
        db.IngestExternalFiles(
        [
            WriteSst(fs, "/ext/old-0.sst", ("a", "old-a"), ("c", "old-c")),
            WriteSst(fs, "/ext/old-1.sst", ("d", "old-d")),
            WriteSst(fs, "/ext/old-2.sst", ("e", "old-e"), ("f", "old-f")),
        ], new IngestExternalFileOptions());
        string replacement = WriteSst(fs, "/ext/replacement.sst", ("b", "new-b"), ("e", "new-e"));
        long start = fs.Operations;
        if (crashAt > 0) fs.CrashAtOperation = start + crashAt;
        bool acked = false;
        try
        {
            db.IngestExternalFiles(db.DefaultColumnFamily, ReplaceArg(replacement, "b", "f"));
            acked = true;
        }
        catch (Exception e) when (e is RocksDbException or IOException)
        {
        }
        long used = fs.Operations - start;
        try { db.Dispose(); }
        catch (Exception e) when (e is RocksDbException or IOException) { }
        fs.PowerLoss();
        fs.Restart();

        using RocksDatabase re = RocksDatabase.Open(DbPath, NewOptions(fs), CrashCfOptions);
        string? b = GetString(re, "b");
        replaced = b is not null;
        if (acked) Assert.IsTrue(replaced, "acknowledged replacement lost: " + context);
        // Outside the range: always the old data.
        Assert.AreEqual("old-a", GetString(re, "a"), context);
        Assert.AreEqual("old-f", GetString(re, "f"), context);
        if (replaced)
        {
            Assert.AreEqual("new-b", b, context);
            Assert.IsNull(GetString(re, "c"), context);
            Assert.IsNull(GetString(re, "d"), context);
            Assert.AreEqual("new-e", GetString(re, "e"), context);
        }
        else
        {
            Assert.AreEqual("old-c", GetString(re, "c"), context);
            Assert.AreEqual("old-d", GetString(re, "d"), context);
            Assert.AreEqual("old-e", GetString(re, "e"), context);
        }
        // The DB is usable afterwards.
        re.CompactRange();
        Assert.AreEqual(replaced ? null : "old-d", GetString(re, "d"), context);
        return used;
    }
}
