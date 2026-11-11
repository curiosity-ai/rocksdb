using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Tests.Db;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4, slice "FinalLifecycle": column family lifecycle (db/column_family.cc, DBImpl::CreateColumnFamilyImpl /
/// DropColumnFamilyImpl), obsolete file deletion (db/db_impl/db_impl_files.cc) and background flush scheduling.
/// </summary>
[TestClass]
public sealed class Audit4FinalLifecycleTests
{
    private const string Path = "/audit4lifecycle";

    private static FaultInjectionFileSystem NewFaultFs()
    {
        var mem = new MemoryFileSystem();
        mem.CreateDirectoryIfMissing(Path);
        mem.SyncDirectory("/");
        return new FaultInjectionFileSystem(mem);
    }

    private static List<string> TableFiles(IFileSystem fs)
    {
        var list = new List<string>();
        foreach (string child in fs.GetChildren(Path))
        {
            if (child.EndsWith(".sst", StringComparison.Ordinal)) list.Add(child);
        }
        list.Sort(StringComparer.Ordinal);
        return list;
    }

    /// <summary>Like upstream's EnableAutoRecovery(false): the test resumes by hand.</summary>
    private sealed class NoAutoRecoveryListener : EventListener
    {
        public override void OnErrorRecoveryBegin(ErrorRecoveryBeginEventArgs e) => e.AutoRecovery = false;
    }

    [TestCleanup]
    public void Cleanup() => TestHooks.ClearAll();

    /// <summary>
    /// VersionSet::ProcessManifestWrites (db/version_set.cc ~6451): after a failed MANIFEST write the files the edits add
    /// are quarantined (ErrorHandler::AddFilesToQuarantine) and DBImpl::PurgeObsoleteFiles skips them
    /// (db/db_impl/db_impl_files.cc ~685), because the record may be in the MANIFEST that CURRENT names. The port's full
    /// scan (run by every writable close, CloseHelper's FindObsoleteFiles(force = true)) deleted the flush output, and
    /// the next open failed on the missing table file.
    /// </summary>
    [TestMethod]
    public void FlushOutputOfFailedManifestWriteSurvivesTheFullScanAtClose()
    {
        FaultInjectionFileSystem fs = NewFaultFs();
        DbOptions options = NewOptions(fs);
        using (RocksDatabase db = RocksDatabase.Open(Path, options))
        {
            db.Put(B("a"), B("1"));
            // The whole record reaches the MANIFEST, then the write reports an error (an ambiguous outcome).
            fs.FailAppend("MANIFEST", null, AppendFailMode.FullyWritten, InjectedFaultKind.IOError);
            RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.Flush());
            Assert.AreEqual(StatusCode.IOError, ex.Code);
            Assert.AreEqual(1, TableFiles(fs).Count, "the flush output exists after the failed MANIFEST write");
        }
        Assert.AreEqual(1, TableFiles(fs).Count, "the close deleted a file the MANIFEST may reference");
        using (RocksDatabase db = RocksDatabase.Open(Path, NewOptions(fs)))
        {
            Assert.AreEqual("1", GetString(db, "a"));
        }
    }

    /// <summary>
    /// Same quarantine, for the runtime full scan (delete_obsolete_files_period_micros = 0: every purge scans), and its
    /// end: ErrorHandler::ClearFilesToQuarantine once a MANIFEST write succeeds (version_set.cc ~6462). The next write
    /// starts a new MANIFEST from the in-memory state, which does not contain the failed flush's file, so the file is
    /// then an orphan and the scan deletes it.
    /// </summary>
    [TestMethod]
    public void QuarantineEndsWithTheNextSuccessfulManifestWrite()
    {
        FaultInjectionFileSystem fs = NewFaultFs();
        DbOptions options = NewOptions(fs);
        options.DeleteObsoleteFilesPeriodMicros = 0;
        options.Listeners.Add(new NoAutoRecoveryListener());
        using RocksDatabase db = RocksDatabase.Open(Path, options);
        DbImpl impl = db.DefaultColumnFamily.Db;
        db.Put(B("a"), B("1"));
        fs.FailAppend("MANIFEST", null, AppendFailMode.FullyWritten, InjectedFaultKind.NoSpace);
        Assert.ThrowsExactly<RocksDbException>(() => db.Flush());
        List<string> afterFailure = TableFiles(fs);
        Assert.AreEqual(1, afterFailure.Count);
        impl.PurgeObsoleteFiles();
        CollectionAssert.AreEqual(afterFailure, TableFiles(fs), "quarantined file deleted by a full scan");
        // DBImpl::ResumeImpl flushes the memtables again; its MANIFEST write succeeds.
        db.Resume();
        db.Flush();
        impl.PurgeObsoleteFiles();
        List<string> afterRecovery = TableFiles(fs);
        Assert.AreEqual(1, afterRecovery.Count, string.Join(",", afterRecovery));
        Assert.AreNotEqual(afterFailure[0], afterRecovery[0]);
        Assert.AreEqual("1", GetString(db, "a"));
    }

    /// <summary>
    /// DBImpl::CreateColumnFamilyWithPreparedOptions (db/db_impl/db_impl.cc ~5108) takes the id from
    /// ColumnFamilySet::GetNextColumnFamilyID (db/column_family.cc 1904: "return ++max_column_family_") before the
    /// MANIFEST write, so a creation whose MANIFEST write failed has consumed its id. The port computed
    /// max_column_family + 1 without storing it and gave the same id to the next column family.
    /// </summary>
    [TestMethod]
    public void FailedColumnFamilyCreationConsumesItsId()
    {
        FaultInjectionFileSystem fs = NewFaultFs();
        using RocksDatabase db = RocksDatabase.Open(Path, NewOptions(fs));
        ColumnFamily one = db.CreateColumnFamily("one", new ColumnFamilyOptions());
        Assert.AreEqual(1u, one.Id);
        fs.FailAppend("MANIFEST", null, AppendFailMode.NothingWritten, InjectedFaultKind.IOError);
        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.CreateColumnFamily("two", new ColumnFamilyOptions()));
        Assert.AreEqual(StatusCode.IOError, ex.Code);
        ColumnFamily three = db.CreateColumnFamily("three", new ColumnFamilyOptions());
        Assert.AreEqual(3u, three.Id);
        three.Put(B("k"), B("v"));
        Assert.AreEqual("v", GetString(three, "k"));
    }

    /// <summary>
    /// DBImpl::DropColumnFamilyImpl (db/db_impl/db_impl.cc 5287-5295): InvalidArgument "Can't drop default column family"
    /// and, for a second drop, InvalidArgument "Column family already dropped!\n" (the message ends with a newline).
    /// </summary>
    [TestMethod]
    public void DropColumnFamilyErrors()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbTestUtil.DbPath, NewOptions(fs));
        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.DropColumnFamily(db.DefaultColumnFamily));
        Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);
        StringAssert.Contains(ex.Message, "Can't drop default column family");
        ColumnFamily cf = db.CreateColumnFamily("x", new ColumnFamilyOptions());
        cf.Put(B("k"), B("v"));
        db.DropColumnFamily(cf);
        ex = Assert.ThrowsExactly<RocksDbException>(() => db.DropColumnFamily(cf));
        Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);
        Assert.IsTrue(ex.Message.EndsWith("Column family already dropped!\n", StringComparison.Ordinal), ex.Message);
        // The id of a dropped column family is not reused (LogAndApplyCFHelper persists max_column_family).
        Assert.AreEqual(2u, db.CreateColumnFamily("y", new ColumnFamilyOptions()).Id);
    }

    /// <summary>
    /// DBImpl::FindObsoleteFiles / PurgeObsoleteFiles (db/db_impl/db_impl_files.cc 258, 706): a table file that is being
    /// written (number >= min_pending_output) survives a full scan that runs before the flush installs it, while an
    /// orphan table file is deleted by the same scan.
    /// </summary>
    [TestMethod]
    public void PendingFlushOutputSurvivesAConcurrentFullScan()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions options = NewOptions(fs);
        options.DeleteObsoleteFilesPeriodMicros = 0;
        using RocksDatabase db = RocksDatabase.Open(DbTestUtil.DbPath, options);
        DbImpl impl = db.DefaultColumnFamily.Db;
        string orphan = DbTestUtil.DbPath + "/000001.sst";
        using (IWritableFile f = fs.CreateWritable(orphan)) f.Append(B("not a table"));
        db.Put(B("a"), B("1"));
        int tablesDuringScan = -1;
        bool orphanAfterScan = true;
        TestHooks.Set("DBImpl::FlushMemTables:BeforeLogAndApply", () =>
        {
            // The L0 file is complete but no version references it yet.
            impl.PurgeObsoleteFiles();
            orphanAfterScan = fs.FileExists(orphan);
            int n = 0;
            foreach (string child in fs.GetChildren(DbTestUtil.DbPath))
            {
                if (child.EndsWith(".sst", StringComparison.Ordinal)) n++;
            }
            tablesDuringScan = n;
        });
        db.Flush();
        TestHooks.Set("DBImpl::FlushMemTables:BeforeLogAndApply", null);
        Assert.IsFalse(orphanAfterScan, "the scan did not run");
        Assert.AreEqual(1, tablesDuringScan, "the pending flush output was deleted by the scan");
        Assert.AreEqual("1", GetString(db, "a"));
    }

    // ---- best-efforts recovery: VersionEditHandlerPointInTime --------------------------------------------------

    private sealed class MissingFiles(params ulong[] missing) : IRecoveryFileVerifier
    {
        private readonly HashSet<ulong> _missing = [.. missing];
        public bool VerifyTableFile(ColumnFamilyVersion cf, int level, FileMetaData meta) => !_missing.Contains(meta.FileNumber);
        public bool VerifyBlobFile(ColumnFamilyVersion cf, BlobFileAddition addition) => !_missing.Contains(addition.BlobFileNumber);
    }

    private static FileMetaData F(ulong number, ulong seq, ulong epoch) =>
        VersionEditTests.File(number, 0, 1000 + number, VersionEditTests.IKey("a", seq, EntryType.Value), VersionEditTests.IKey("b", seq + 1, EntryType.Value),
            seq, seq + 1, epoch);

    private static VersionEdit Add(uint cf, int level, FileMetaData f)
    {
        var e = new VersionEdit { ColumnFamily = cf };
        e.AddFile(level, f);
        return e;
    }

    private static List<ulong> Files(ColumnFamilyVersion cf)
    {
        var r = new List<ulong>();
        foreach ((_, FileMetaData f) in cf.AllFiles()) r.Add(f.FileNumber);
        r.Sort();
        return r;
    }

    /// <summary>
    /// VersionEditHandlerPointInTime::MaybeCreateVersionBeforeApplyEdit (db/version_edit_handler.cc 885-908): no version
    /// is created while log number, next file number or last sequence are unknown ("missing_info", evaluated before the
    /// edit's own fields are extracted). An edit that makes the column family invalid while the next file number is still
    /// unknown therefore leaves it without a version (empty), and the port kept the state before that edit.
    /// </summary>
    [TestMethod]
    public void PointInTimeCreatesNoVersionWhileManifestInfoIsMissing()
    {
        foreach (bool infoInFirstEdit in new[] { false, true })
        {
            var r = new ManifestReplayer(null, new MissingFiles(20));
            var first = new VersionEdit { ComparatorName = "leveldb.BytewiseComparator", PersistUserDefinedTimestamps = true, LogNumber = 0 };
            first.AddFile(1, F(10, 1, 0));
            // The L2 file 20 is missing: a negative edge (valid before, invalid after).
            VersionEdit second = Add(0, 2, F(20, 3, 0));
            if (infoInFirstEdit)
            {
                first.NextFileNumber = 30;
                first.LastSequence = 9;
            }
            else
            {
                second.NextFileNumber = 30;
                second.LastSequence = 9;
            }
            r.Apply(first);
            r.Apply(second);
            RecoveredVersionState s = r.Finish();
            CollectionAssert.AreEqual(infoInFirstEdit ? new ulong[] { 10 } : Array.Empty<ulong>(), Files(s.ColumnFamilies[0]), "info in first edit: " + infoInFirstEdit);
        }
    }

    /// <summary>
    /// VersionBuilder::Rep::Apply (db/version_builder.cc 1430-1434): edited_in_atomic_group_ is only set by an edit that
    /// changed table or blob files. A member of an atomic flush without an output (only a log number) leaves its column
    /// family able to recover with a missing L0 suffix (ValidVersionAvailable -> OnlyMissingL0Suffix), which also
    /// completes the atomic update of the other column family. The port marked every column family edited in a group.
    /// </summary>
    [TestMethod]
    public void PointInTimeAtomicGroupMemberWithoutFileChangeStillAcceptsMissingL0Suffix()
    {
        var r = new ManifestReplayer(null, new MissingFiles(21));
        r.Apply(new VersionEdit { ComparatorName = "leveldb.BytewiseComparator", PersistUserDefinedTimestamps = true, LogNumber = 0, NextFileNumber = 2, LastSequence = 0 });
        var addCf = new VersionEdit { ColumnFamily = 1, ComparatorName = "leveldb.BytewiseComparator" };
        addCf.AddColumnFamily("cf1");
        r.Apply(addCf);
        r.Apply(Add(0, 0, F(10, 1, 1)));
        r.Apply(Add(1, 0, F(20, 2, 1)));
        // Atomic flush: cf0 gets file 30, cf1 had nothing to flush.
        VersionEdit g0 = Add(0, 0, F(30, 3, 2));
        var g1 = new VersionEdit { ColumnFamily = 1, LogNumber = 5 };
        g0.MarkAtomicGroup(1);
        g1.MarkAtomicGroup(0);
        r.Apply(g0);
        r.Apply(g1);
        // Later cf1 flushes file 21, which is missing: a missing L0 suffix.
        r.Apply(Add(1, 0, F(21, 5, 2)));
        // ... and compacts file 20 into L1: only valid as "missing L0 suffix", which a column family edited in an atomic
        // group does not accept (it needs a complete version).
        var compaction = new VersionEdit { ColumnFamily = 1 };
        compaction.DeleteFile(0, 20);
        compaction.AddFile(1, F(40, 2, 0));
        r.Apply(compaction);
        RecoveredVersionState s = r.Finish();
        CollectionAssert.AreEqual(new ulong[] { 40 }, Files(s.ColumnFamilies[1]));
        CollectionAssert.AreEqual(new ulong[] { 10, 30 }, Files(s.ColumnFamilies[0]));
    }
}
