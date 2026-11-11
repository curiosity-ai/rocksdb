using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Tests.Db;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4, closing the open items: atomic_flush for automatic and manual flushes (DBImpl::ScheduleFlushes,
/// AtomicFlushMemTables, AtomicFlushMemTablesToOutputFiles, MemTableList::InstallMemtableAtomicFlushResults), the sticky
/// error state of file writers (WritableFileWriter::seen_error_) and the cleanup of ingestion / import jobs after a
/// failed MANIFEST write.
/// </summary>
[TestClass]
public sealed class Audit4ClosingOpenItemsTests
{
    private const string DbPath = "/audit4closing";
    private static readonly WriteOptions NoWal = new() { DisableWal = true };

    [TestCleanup]
    public void Cleanup() => TestHooks.ClearAll();

    private static ColumnFamilyDescriptor[] TwoCfs(ulong defaultWriteBufferSize) =>
    [
        new ColumnFamilyDescriptor("default", new ColumnFamilyOptions { WriteBufferSize = defaultWriteBufferSize, DisableAutoCompactions = true }),
        new ColumnFamilyDescriptor("other", new ColumnFamilyOptions { DisableAutoCompactions = true }),
    ];

    private static DbOptions AtomicOptions(IFileSystem fs) => new()
    {
        CreateIfMissing = true,
        CreateMissingColumnFamilies = true,
        FileSystem = fs,
        AtomicFlush = true,
    };

    private static int L0(ColumnFamily cf) => cf.Data.Current.Storage.LevelFiles(0).Length;

    private static List<VersionEdit> ReadManifest(IFileSystem fs)
    {
        string manifest = FileNames.GetCurrentManifestPath(fs, DbPath, out _);
        using ManifestLogReader r = ManifestLogReader.Open(fs, manifest);
        return ManifestReader.ReadAllEdits(r);
    }

    /// <summary>
    /// The atomic groups of the MANIFEST that add table files: for each, the column families it adds a file to. Checks
    /// the remaining_entries countdown of every group (VersionEdit::MarkAtomicGroup).
    /// </summary>
    private static List<HashSet<uint>> FlushGroups(List<VersionEdit> edits)
    {
        var groups = new List<HashSet<uint>>();
        for (int i = 0; i < edits.Count; i++)
        {
            if (!edits[i].IsInAtomicGroup)
            {
                Assert.AreEqual(0, edits[i].NewFiles.Count, "a table file was added outside an atomic group");
                continue;
            }
            int size = (int)edits[i].RemainingEntries + 1;
            var cfs = new HashSet<uint>();
            for (int j = 0; j < size; j++)
            {
                Assert.IsTrue(edits[i + j].IsInAtomicGroup);
                Assert.AreEqual((uint)(size - 1 - j), edits[i + j].RemainingEntries);
                if (edits[i + j].NewFiles.Count > 0) cfs.Add(edits[i + j].ColumnFamily);
            }
            if (cfs.Count > 0) groups.Add(cfs);
            i += size - 1;
        }
        return groups;
    }

    /// <summary>
    /// DBImpl::ScheduleFlushes (db_impl_write.cc: with atomic_flush SelectColumnFamiliesForAtomicFlush picks every column
    /// family with unflushed data, all are switched and one FlushRequest covers them) and
    /// MemTableList::InstallMemtableAtomicFlushResults (one MANIFEST atomic group holds the results of all of them).
    /// </summary>
    [TestMethod]
    public void FullMemtableFlushesEveryColumnFamilyInOneAtomicGroup()
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, AtomicOptions(fs), TwoCfs(64 * 1024)))
        {
            ColumnFamily other = db.GetColumnFamily("other");
            other.Put(B("o"), B("1"));
            for (int i = 0; i < 2000 && L0(db.DefaultColumnFamily) == 0; i++)
            {
                db.Put(Key(i), new byte[200]);
                if (i % 100 == 99) db.WaitForCompact();
            }
            db.WaitForCompact();
            Assert.AreEqual(1, L0(db.DefaultColumnFamily));
            Assert.AreEqual(1, L0(other), "the other column family is flushed with the full one");
            Assert.AreEqual("1", GetString(other, "o"));
            List<HashSet<uint>> groups = FlushGroups(ReadManifest(fs));
            Assert.AreEqual(1, groups.Count);
            Assert.IsTrue(groups[0].SetEquals([0u, other.Id]));
        }
        using RocksDatabase reopened = RocksDatabase.Open(DbPath, AtomicOptions(fs), TwoCfs(64 * 1024));
        Assert.AreEqual("1", GetString(reopened.GetColumnFamily("other"), "o"));
    }

    /// <summary>
    /// DBImpl::Flush -> AtomicFlushMemTables (db_impl_compaction_flush.cc): with atomic_flush a manual flush of one column
    /// family switches and flushes every column family with data and waits for all of them; a column family without data
    /// gets no file.
    /// </summary>
    [TestMethod]
    public void ManualFlushOfOneColumnFamilyFlushesAll()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, AtomicOptions(fs), TwoCfs(64 << 20));
        ColumnFamily other = db.GetColumnFamily("other");
        ColumnFamily empty = db.CreateColumnFamily("empty", new ColumnFamilyOptions());
        db.Put(B("a"), B("1"));
        other.Put(B("o"), B("1"));
        other.Flush();
        Assert.AreEqual(1, L0(db.DefaultColumnFamily), "the default column family is flushed by the flush of another one");
        Assert.AreEqual(1, L0(other));
        Assert.AreEqual(0, L0(empty));
        List<HashSet<uint>> groups = FlushGroups(ReadManifest(fs));
        Assert.AreEqual(1, groups.Count);
        Assert.IsTrue(groups[0].SetEquals([0u, other.Id]));

        // Only one column family holds data: still an atomic group (of that column family alone).
        db.Put(B("b"), B("2"));
        other.Flush();
        Assert.AreEqual(2, L0(db.DefaultColumnFamily));
        Assert.AreEqual(1, L0(other));
        groups = FlushGroups(ReadManifest(fs));
        Assert.AreEqual(2, groups.Count);
        Assert.IsTrue(groups[1].SetEquals([0u]));

        // CompactRange's flush (DBImpl::CompactRangeInternal -> AtomicFlushMemTables).
        db.Put(B("c"), B("3"));
        other.Put(B("p"), B("3"));
        other.CompactRange();
        Assert.AreEqual(3, L0(db.DefaultColumnFamily));
        Assert.AreEqual("3", GetString(db, "c"));
        Assert.AreEqual("3", GetString(other, "p"));
    }

    /// <summary>
    /// The point of atomic_flush (AtomicFlushMemTablesToOutputFiles + InstallMemtableAtomicFlushResults): without a WAL a
    /// write batch spanning two column families is recovered in both or in neither. Here the flush completes before the
    /// power loss (both), or fails right before its MANIFEST write (neither).
    /// </summary>
    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void CrossColumnFamilyBatchWithoutWalSurvivesInBothOrNeither(bool failBeforeManifest)
    {
        var fs = new FaultInjectionFileSystem(NewFs());
        using (RocksDatabase db = RocksDatabase.Open(DbPath, AtomicOptions(fs), TwoCfs(64 << 20)))
        {
            ColumnFamily other = db.GetColumnFamily("other");
            using (WriteBatch batch = db.CreateWriteBatch())
            {
                batch.Put(B("a"), B("1")).Put(other.Id, B("o"), B("1"));
                db.Write(NoWal, batch);
            }
            if (failBeforeManifest)
            {
                // Both L0 files are written; the power fails before the atomic group reaches the MANIFEST.
                TestHooks.Set("DBImpl::AtomicFlush:BeforeLogAndApply", () => fs.Inner.SetFilesystemActive(false));
                Assert.ThrowsExactly<RocksDbException>(() => db.Flush());
            }
            else
            {
                db.Flush();
                fs.Inner.SetFilesystemActive(false);
            }
            fs.PowerLoss();
            try { db.Dispose(); }
            catch (Exception e) when (e is RocksDbException or IOException) { }
        }
        TestHooks.ClearAll();
        fs.Restart();
        using RocksDatabase reopened = RocksDatabase.Open(DbPath, AtomicOptions(fs), TwoCfs(64 << 20));
        string? expected = failBeforeManifest ? null : "1";
        Assert.AreEqual(expected, GetString(reopened, "a"));
        Assert.AreEqual(expected, GetString(reopened.GetColumnFamily("other"), "o"));
    }

    /// <summary>
    /// Crash consistency of automatic atomic flushes: batches that write the same key to both column families (no WAL)
    /// while full memtables trigger flushes, with a power loss at varying points. After recovery both column families
    /// hold the same keys: the flushed memtables of all column families are one cut of the write sequence (the max
    /// memtable ids of DBImpl::GenerateFlushRequest) installed by one atomic group.
    /// </summary>
    [TestMethod]
    public void AutomaticAtomicFlushesKeepColumnFamiliesConsistentAcrossPowerLoss()
    {
        const int batches = 900;
        long total;
        {
            var fs = new FaultInjectionFileSystem(NewFs());
            using RocksDatabase db = RocksDatabase.Open(DbPath, AtomicOptions(fs), TwoCfs(64 * 1024));
            long start = fs.Operations;
            Assert.AreEqual(batches, RunBatches(db, batches));
            db.WaitForCompact();
            total = fs.Operations - start;
            Assert.IsTrue(L0(db.DefaultColumnFamily) >= 2, "the workload must trigger several automatic flushes");
            Assert.AreEqual(L0(db.DefaultColumnFamily), L0(db.GetColumnFamily("other")));
        }
        int recoveredSomething = 0;
        for (int step = 1; step <= 24; step++)
        {
            long crashAt = total * step / 25;
            var fs = new FaultInjectionFileSystem(NewFs());
            RocksDatabase db = RocksDatabase.Open(DbPath, AtomicOptions(fs), TwoCfs(64 * 1024));
            fs.CrashAtOperation = fs.Operations + crashAt;
            RunBatches(db, batches);
            try
            {
                db.WaitForCompact();
            }
            catch (RocksDbException)
            {
            }
            try { db.Dispose(); }
            catch (Exception e) when (e is RocksDbException or IOException) { }
            fs.PowerLoss();
            fs.Restart();
            using RocksDatabase reopened = RocksDatabase.Open(DbPath, AtomicOptions(fs), TwoCfs(64 * 1024));
            ColumnFamily other = reopened.GetColumnFamily("other");
            int present = 0;
            for (int i = 0; i < batches; i++)
            {
                bool inDefault = reopened.Get(Key(i)) is not null;
                bool inOther = other.Get(Key(i)) is not null;
                Assert.AreEqual(inDefault, inOther, "crashAt=" + crashAt + " key " + i + ": half of a batch was recovered");
                if (inDefault) present++;
            }
            if (present > 0) recoveredSomething++;
        }
        Assert.IsTrue(recoveredSomething > 0, "no crash point recovered flushed data: the test did not exercise flushes");
    }

    /// <summary>Writes batch i = Put(default, key i) + Put(other, key i) without WAL; returns the number acknowledged.</summary>
    private static int RunBatches(RocksDatabase db, int count)
    {
        ColumnFamily other = db.GetColumnFamily("other");
        byte[] value = new byte[200];
        for (int i = 0; i < count; i++)
        {
            try
            {
                using WriteBatch batch = db.CreateWriteBatch();
                batch.Put(Key(i), value).Put(other.Id, Key(i), B("o"));
                db.Write(NoWal, batch);
            }
            catch (Exception e) when (e is RocksDbException or IOException)
            {
                return i;
            }
        }
        return count;
    }

    /// <summary>
    /// DBImpl::AtomicFlushMemTablesToOutputFiles: when the flush job of one column family fails, the results of the
    /// other jobs are not installed either (their output files are deleted and the memtables rolled back,
    /// MemTableList::RollbackMemtableFlush); the retry (DBImpl::RetryFlushesForErrorRecovery, atomic too) installs all
    /// of them as one atomic group.
    /// </summary>
    [TestMethod]
    public void FailedFlushJobOfOneColumnFamilyInstallsNothingAndTheRetrySucceeds()
    {
        var fs = new FaultInjectionFileSystem(NewFs());
        DbOptions options = AtomicOptions(fs);
        options.MaxBgErrorResumeCount = 0; // no automatic recovery: the retry is the explicit Resume below
        using (RocksDatabase db = RocksDatabase.Open(DbPath, options, TwoCfs(64 << 20)))
        {
            ColumnFamily other = db.GetColumnFamily("other");
            db.Put(NoWal, B("a"), B("1"));
            other.Put(NoWal, B("o"), B("1"));
            fs.FailMatching(".sst", FaultOps.Append, 1, InjectedFaultKind.Retryable);
            RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.Flush());
            Assert.AreEqual(StatusCode.IOError, ex.Code);
            Assert.AreEqual(1, fs.TargetedFaults);
            db.DefaultColumnFamily.Db.WaitForBackgroundWorkForTest();
            Assert.AreEqual(0, L0(db.DefaultColumnFamily));
            Assert.AreEqual(0, L0(other), "the flush of the column family whose job succeeded must not be installed");
            Assert.AreEqual(0, FlushGroups(ReadManifest(fs)).Count);
            foreach (string f in fs.GetChildren(DbPath))
            {
                Assert.IsFalse(f.EndsWith(".sst", StringComparison.Ordinal), "output of the failed atomic flush left behind: " + f);
            }
            // The data is still readable from the rolled back memtables.
            Assert.AreEqual("1", GetString(db, "a"));
            Assert.AreEqual("1", GetString(other, "o"));

            db.Resume();
            Assert.IsNull(db.BackgroundError);
            Assert.AreEqual(1, L0(db.DefaultColumnFamily));
            Assert.AreEqual(1, L0(other));
            List<HashSet<uint>> groups = FlushGroups(ReadManifest(fs));
            Assert.AreEqual(1, groups.Count);
            Assert.IsTrue(groups[0].SetEquals([0u, other.Id]));
        }
        using RocksDatabase reopened = RocksDatabase.Open(DbPath, AtomicOptions(fs), TwoCfs(64 << 20));
        Assert.AreEqual("1", GetString(reopened, "a"));
        Assert.AreEqual("1", GetString(reopened.GetColumnFamily("other"), "o"));
    }

    // ---- sticky error state of file writers (WritableFileWriter::seen_error_) -------------------------------

    /// <summary>
    /// WritableFileWriter (file/writable_file_writer.cc): after a failed Append or Sync ("set_seen_error(s)") every later
    /// Append, Flush and Sync returns IOError "Writer has previous error." without reaching the file
    /// ("if (seen_error())"). The writer of table and blob files (FileChecksumHelper.CreateWritable) behaves the same.
    /// </summary>
    [TestMethod]
    [DataRow(true)]
    [DataRow(false)]
    public void TableFileWriterFailsEveryOperationAfterAnError(bool failSync)
    {
        var fs = new FaultInjectionFileSystem(NewFs());
        fs.CreateDirectoryIfMissing(DbPath);
        using IWritableFile file = FileChecksumHelper.CreateWritable(fs, DbPath + "/000001.sst", WritableFileOptions.Default);
        file.Append(new byte[100]);
        fs.FailMatching(".sst", failSync ? FaultOps.Sync : FaultOps.Append, 1, InjectedFaultKind.Retryable);
        RocksDbException first = Assert.ThrowsExactly<RocksDbException>(() =>
        {
            if (failSync) file.Sync();
            else file.Append(new byte[10]);
        });
        Assert.AreEqual(StatusCode.IOError, first.Code);
        Assert.AreEqual(1, fs.TargetedFaults);
        long ops = fs.Operations;
        // The file system would accept these now; the writer must not let them through.
        AssertPreviousError(() => file.Sync());
        AssertPreviousError(() => file.Append(new byte[10]));
        AssertPreviousError(() => file.Flush());
        AssertPreviousError(() => file.SyncAsync().AsTask().GetAwaiter().GetResult());
        AssertPreviousError(() => file.AppendAsync(new byte[10]).AsTask().GetAwaiter().GetResult());
        AssertPreviousError(() => file.FlushAsync().AsTask().GetAwaiter().GetResult());
        Assert.AreEqual(ops, fs.Operations, "an operation reached the file after the error");
    }

    private static void AssertPreviousError(Action action)
    {
        RocksDbException e = Assert.ThrowsExactly<RocksDbException>(action);
        Assert.AreEqual(StatusCode.IOError, e.Code);
        StringAssert.Contains(e.Message, "Writer has previous error");
    }

    /// <summary>
    /// SstFileWriter::Put / Finish (table/sst_file_writer.cc: Finish returns the builder's status, which stays failed
    /// after an I/O error): once an append to the file failed the file cannot be finished, even if the caller ignores the
    /// failed Put and the file system works again.
    /// </summary>
    [TestMethod]
    public void SstFileWriterCannotFinishAFileWhoseAppendFailed()
    {
        var fs = new FaultInjectionFileSystem(NewFs());
        fs.CreateDirectoryIfMissing(DbPath);
        string path = DbPath + "/ext.sst";
        using var writer = new SstFileWriter(new ColumnFamilyOptions(), fs);
        writer.Open(path);
        var random = new Random(42);
        byte[] value = new byte[400];
        fs.FailMatching("ext.sst", FaultOps.Append, 1, InjectedFaultKind.Retryable);
        int failures = 0;
        for (int i = 0; i < 4000; i++)
        {
            random.NextBytes(value);
            try
            {
                writer.Put(Key(i), value);
            }
            catch (RocksDbException e)
            {
                Assert.AreEqual(StatusCode.IOError, e.Code);
                failures++;
            }
        }
        Assert.AreEqual(1, fs.TargetedFaults, "the workload must reach the file");
        Assert.IsTrue(failures >= 1);
        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => writer.Finish());
        Assert.AreEqual(StatusCode.IOError, ex.Code);
        Assert.IsFalse(fs.FileExists(path), "the unfinished file is deleted");
    }

    /// <summary>
    /// WritableFileWriter::SyncInternal sets seen_error_ and the flush job fails: a table or blob file whose sync failed
    /// is never installed (the retried flush writes new files), and after a failed MANIFEST sync the next version edit
    /// goes to a new MANIFEST (VersionSet::ProcessManifestWrites: "descriptor_log_.reset()"). The data survives a reopen.
    /// </summary>
    [TestMethod]
    [DataRow(".blob")]
    [DataRow(".sst")]
    [DataRow("MANIFEST")]
    public void FileWhoseSyncFailedIsNotInstalledByTheRetry(string fragment)
    {
        var fs = new FaultInjectionFileSystem(NewFs());
        var options = new DbOptions { CreateIfMissing = true, FileSystem = fs, MaxBgErrorResumeCount = 0 };
        ColumnFamilyDescriptor[] cfs = [new ColumnFamilyDescriptor("default", new ColumnFamilyOptions { EnableBlobFiles = true, MinBlobSize = 16 })];
        byte[] big = new byte[500];
        new Random(7).NextBytes(big);
        using (RocksDatabase db = RocksDatabase.Open(DbPath, options, cfs))
        {
            db.Put(B("a"), big);
            string manifestBefore = FileNames.GetCurrentManifestPath(fs, DbPath, out _);
            fs.FailMatching(fragment, FaultOps.Sync, 1, InjectedFaultKind.Retryable);
            RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.Flush());
            Assert.AreEqual(StatusCode.IOError, ex.Code);
            Assert.AreEqual(1, fs.TargetedFaults);
            db.DefaultColumnFamily.Db.WaitForBackgroundWorkForTest();
            var failedFiles = new HashSet<string>();
            foreach (string f in fs.GetChildren(DbPath))
            {
                if (f.Contains(fragment, StringComparison.Ordinal)) failedFiles.Add(Path.GetFileName(f));
            }
            db.Resume();
            Assert.IsNull(db.BackgroundError);
            CollectionAssert.AreEqual(big, db.Get(B("a")));
            if (fragment == "MANIFEST")
            {
                Assert.AreNotEqual(manifestBefore, FileNames.GetCurrentManifestPath(fs, DbPath, out _));
            }
            else
            {
                // None of the files that existed when the sync failed is referenced by the MANIFEST.
                int added = 0;
                foreach (VersionEdit e in ReadManifest(fs))
                {
                    foreach (NewFileEntry nf in e.NewFiles)
                    {
                        added++;
                        if (fragment == ".sst") Assert.IsFalse(failedFiles.Contains(FileNames.MakeTableFileName(nf.Meta.FileNumber)), "the table file whose sync failed was installed");
                    }
                    foreach (BlobFileAddition b in e.BlobFileAdditions)
                    {
                        added++;
                        if (fragment == ".blob") Assert.IsFalse(failedFiles.Contains(FileNames.BlobFileName(b.BlobFileNumber)), "the blob file whose sync failed was installed");
                    }
                }
                Assert.AreEqual(2, added, "one table file and one blob file");
            }
        }
        using RocksDatabase reopened = RocksDatabase.Open(DbPath, new DbOptions { FileSystem = fs }, cfs);
        CollectionAssert.AreEqual(big, reopened.Get(B("a")));
    }

    // ---- ingestion / import cleanup after a failed MANIFEST write (remaining.md R1) ---------------------------

    private static string WriteSst(IFileSystem fs, string path, int from, int to, string tag)
    {
        using var w = new SstFileWriter(new ColumnFamilyOptions(), fs);
        w.Open(path);
        for (int i = from; i < to; i++) w.Put(Key(i), B(tag + i));
        w.Finish();
        return path;
    }

    /// <summary>
    /// A MANIFEST write that failed in its sync may still have made the record durable (VersionSet::io_status_; upstream
    /// quarantines the files of such an edit, VersionSet::ProcessManifestWrites "files_to_quarantine_if_commit_fail" /
    /// ErrorHandler::AddFilesToQuarantine). The ingested file must therefore stay: the MANIFEST that CURRENT names may
    /// reference it, and the DB must open again.
    /// </summary>
    [TestMethod]
    public void IngestedFileIsKeptWhenTheManifestWriteFailedAfterTheRecordWasWritten()
    {
        var fs = new FaultInjectionFileSystem(NewFs());
        fs.CreateDirectoryIfMissing("/ext");
        var options = new DbOptions { CreateIfMissing = true, FileSystem = fs, MaxBgErrorResumeCount = 0 };
        using (RocksDatabase db = RocksDatabase.Open(DbPath, options))
        {
            db.Put(B("a"), B("1"));
            string ext = WriteSst(fs, "/ext/1.sst", 0, 10, "x");
            fs.FailMatching("MANIFEST", FaultOps.Sync, 1, InjectedFaultKind.Retryable);
            RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.IngestExternalFiles([ext], new IngestExternalFileOptions()));
            Assert.AreEqual(StatusCode.IOError, ex.Code);
            Assert.AreEqual(1, fs.TargetedFaults);
            // The failed ingestion is not visible in this process.
            Assert.IsNull(db.Get(Key(3)));
            fs.Inner.SetFilesystemActive(false); // the process dies here: nothing more reaches the files
            try { db.Dispose(); }
            catch (Exception e) when (e is RocksDbException or IOException) { }
        }
        fs.Restart();
        using RocksDatabase reopened = RocksDatabase.Open(DbPath, new DbOptions { FileSystem = fs });
        Assert.AreEqual("1", GetString(reopened, "a"));
        // The record reached the MANIFEST (only its sync failed), so the ingested data is there.
        Assert.AreEqual("x3", S(reopened.Get(Key(3))));
    }

    /// <summary>
    /// Same for CreateColumnFamilyWithImport (ImportColumnFamilyJob::Cleanup): when the MANIFEST write of the import and
    /// that of the column family drop that follows both fail, the MANIFEST named by CURRENT may hold the import record;
    /// the imported files must not be deleted.
    /// </summary>
    [TestMethod]
    public void ImportedFilesAreKeptWhenTheManifestWriteFailedAfterTheRecordWasWritten()
    {
        var fs = new FaultInjectionFileSystem(NewFs());
        ExportImportFilesMetadata meta;
        using (RocksDatabase src = RocksDatabase.Open("/audit4src", new DbOptions { CreateIfMissing = true, FileSystem = fs }))
        {
            for (int i = 0; i < 10; i++) src.Put(Key(i), B("x" + i));
            src.Flush();
            meta = src.DefaultColumnFamily.ExportColumnFamily("/audit4export");
        }
        var options = new DbOptions { CreateIfMissing = true, FileSystem = fs, MaxBgErrorResumeCount = 0 };
        using (RocksDatabase db = RocksDatabase.Open(DbPath, options))
        {
            db.Put(B("a"), B("1"));
            // The import edit and the drop of the column family that follows the failure.
            TestHooks.Set("DBImpl::CreateColumnFamilyWithImport:BeforeLogAndApply", () => fs.FailMatching("MANIFEST", FaultOps.Sync, 2, InjectedFaultKind.Retryable));
            RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() =>
                db.CreateColumnFamilyWithImport("imported", new ColumnFamilyOptions(), new ImportColumnFamilyOptions(), meta));
            Assert.AreEqual(StatusCode.IOError, ex.Code);
            Assert.AreEqual(2, fs.TargetedFaults);
            fs.Inner.SetFilesystemActive(false);
            try { db.Dispose(); }
            catch (Exception e) when (e is RocksDbException or IOException) { }
        }
        TestHooks.ClearAll();
        fs.Restart();
        using RocksDatabase reopened = RocksDatabase.Open(DbPath, new DbOptions { FileSystem = fs },
            [new ColumnFamilyDescriptor("default", new ColumnFamilyOptions()), new ColumnFamilyDescriptor("imported", new ColumnFamilyOptions())]);
        Assert.AreEqual("1", GetString(reopened, "a"));
        Assert.AreEqual("x3", S(reopened.GetColumnFamily("imported").Get(Key(3))));
    }
}
