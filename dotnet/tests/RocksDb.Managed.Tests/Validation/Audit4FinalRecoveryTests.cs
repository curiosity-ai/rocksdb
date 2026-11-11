using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Tests.Db;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4, slice "FinalRecovery": best-efforts recovery versions (db/version_edit_handler.cc), the pause before a
/// failed background flush is retried (db/db_impl/db_impl_compaction_flush.cc) and the OPTIONS files kept for running
/// remote compactions (db/db_impl/db_impl_files.cc).
/// </summary>
[TestClass]
public sealed class Audit4FinalRecoveryTests
{
    private const string Path = "/audit4recovery";
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);

    private static FaultInjectionFileSystem NewFaultFs()
    {
        var mem = new MemoryFileSystem();
        mem.CreateDirectoryIfMissing(Path);
        mem.SyncDirectory("/");
        return new FaultInjectionFileSystem(mem);
    }

    private static int CountFiles(IFileSystem fs, FileType type)
    {
        int n = 0;
        foreach (string child in fs.GetChildren(Path))
        {
            if (FileNames.TryParseFileName(child, out _, out FileType t) && t == type) n++;
        }
        return n;
    }

    // ---- best-efforts recovery ---------------------------------------------------------------------------------

    private static VersionEdit Add(ulong number, ulong order)
    {
        var e = new VersionEdit();
        e.AddFile(0, VersionEditTests.File(number, 0, 1000 + number, VersionEditTests.IKey("a", 2 * order, EntryType.Value),
            VersionEditTests.IKey("b", 2 * order + 1, EntryType.Value), 2 * order, 2 * order + 1, order));
        return e;
    }

    /// <summary>Files in <paramref name="missing"/> do not exist; files in <paramref name="corrupt"/> exist but cannot be opened.</summary>
    private sealed class TableFiles(ulong[] missing, ulong[] corrupt) : IRecoveryFileVerifier
    {
        public readonly List<ulong> Loaded = new();
        public bool VerifyTableFile(ColumnFamilyVersion cf, int level, FileMetaData meta) => Array.IndexOf(missing, meta.FileNumber) < 0;
        public bool VerifyBlobFile(ColumnFamilyVersion cf, BlobFileAddition addition) => true;

        public bool LoadTableFile(ColumnFamilyVersion cf, int level, FileMetaData meta)
        {
            Loaded.Add(meta.FileNumber);
            return Array.IndexOf(corrupt, meta.FileNumber) < 0;
        }
    }

    private static VersionEdit FirstEdit() =>
        new() { ComparatorName = "leveldb.BytewiseComparator", PersistUserDefinedTimestamps = true, LogNumber = 0, NextFileNumber = 2, LastSequence = 0 };

    private static ulong[] FileNumbers(RecoveredVersionState s)
    {
        var list = new List<ulong>();
        foreach ((_, FileMetaData f) in s.ColumnFamilies[0].AllFiles()) list.Add(f.FileNumber);
        list.Sort();
        return list.ToArray();
    }

    private static ulong[] Recover(TableFiles files, params ulong[] l0)
    {
        var r = new ManifestReplayer(null, files);
        r.Apply(FirstEdit());
        ulong order = 1;
        foreach (ulong n in l0) r.Apply(Add(n, order++));
        return FileNumbers(r.Finish());
    }

    /// <summary>
    /// VersionEditHandlerPointInTime::MaybeCreateVersionBeforeApplyEdit (db/version_edit_handler.cc 906-950): a version is
    /// created only on a negative edge (valid before the edit, invalid after), at an atomic group begin and at the end,
    /// and VersionBuilder::LoadTableHandlers runs on exactly those versions; a Corruption drops the version ("This point
    /// in time cannot be recovered; skip it and continue"). The port saved a version before every edit and never opened
    /// the tables (the open then failed with Corruption).
    /// </summary>
    [TestMethod]
    public void PointInTimeVersionsAreCreatedOnNegativeEdgesAndSkippedWhenATableIsCorrupt()
    {
        // No file is missing: the only version is the final one. It holds the corrupt file and is dropped; nothing older
        // was ever saved, so the column family is empty (not files 10 and 11).
        var f = new TableFiles([], [12]);
        CollectionAssert.AreEqual(Array.Empty<ulong>(), Recover(f, 10, 11, 12, 13));
        Assert.IsTrue(f.Loaded.Contains(12) && f.Loaded.TrueForAll(n => n >= 10 && n <= 13) && f.Loaded.Count <= 4,
            "tables are only opened for the final version: " + string.Join(",", f.Loaded));

        // The newest file is missing (a valid L0 suffix): no negative edge, the final version (10, 11, 12) is dropped.
        CollectionAssert.AreEqual(Array.Empty<ulong>(), Recover(new TableFiles([13], [12]), 10, 11, 12, 13));

        // 12 is missing and 13 present: the edit adding 13 is a negative edge. The version before it (10, 11) loads and
        // is kept; the corrupt 13 is never part of a version.
        f = new TableFiles([12], [13]);
        CollectionAssert.AreEqual(new ulong[] { 10, 11 }, Recover(f, 10, 11, 12, 13));
        f.Loaded.Sort();
        CollectionAssert.AreEqual(new ulong[] { 10, 11 }, f.Loaded.ToArray());

        // Same negative edge, but the version before it holds a corrupt file: dropped, and the final state is invalid.
        CollectionAssert.AreEqual(Array.Empty<ulong>(), Recover(new TableFiles([12], [10]), 10, 11, 12, 13));

        // Nothing corrupt: unchanged behaviour.
        CollectionAssert.AreEqual(new ulong[] { 10, 11 }, Recover(new TableFiles([12], []), 10, 11, 12, 13));
        CollectionAssert.AreEqual(new ulong[] { 10, 11, 12 }, Recover(new TableFiles([13], []), 10, 11, 12, 13));
    }

    /// <summary>
    /// OnAtomicGroupReplayBegin (db/version_edit_handler.cc 761-772) forces a version for every column family before the
    /// group; when its tables cannot be loaded that version is dropped as well, and a group that then leaves the column
    /// family without a complete version recovers nothing for it.
    /// </summary>
    [TestMethod]
    public void PointInTimeVersionAtAtomicGroupBeginIsSkippedWhenATableIsCorrupt()
    {
        foreach (bool corrupt in new[] { false, true })
        {
            var r = new ManifestReplayer(null, new TableFiles([20], corrupt ? [10] : []));
            r.Apply(FirstEdit());
            r.Apply(Add(10, 1));
            VersionEdit g = Add(20, 2); // missing: the column family, edited in an atomic group, needs a complete version
            g.MarkAtomicGroup(0);
            r.Apply(g);
            CollectionAssert.AreEqual(corrupt ? Array.Empty<ulong>() : new ulong[] { 10 }, FileNumbers(r.Finish()), "corrupt: " + corrupt);
        }
    }

    // ---- flush retry back-off ----------------------------------------------------------------------------------

    private static RocksDatabase OpenForFlushRetry(FaultInjectionFileSystem fs, bool atomicFlush, SemaphoreSlim entered, TaskCompletionSource release)
    {
        DbOptions options = NewOptions(fs);
        options.ParanoidChecks = false;
        options.AtomicFlush = atomicFlush;
        RocksDatabase db = RocksDatabase.Open(Path, options);
        db.Impl.FlushRetryDelayForTest = _ =>
        {
            entered.Release();
            return release.Task;
        };
        return db;
    }

    /// <summary>
    /// DBImpl::BackgroundCallFlush (db/db_impl/db_impl_compaction_flush.cc ~5015-5033): after a failed flush ("!s.ok() &amp;&amp;
    /// !s.IsShutdownInProgress() &amp;&amp; !s.IsColumnFamilyDropped() &amp;&amp; reason != FlushReason::kErrorRecovery") the flush
    /// thread waits one second before anything is flushed again. With paranoid_checks = false a flush I/O error sets no
    /// background error (ErrorHandler's severity map: kFlush, IOError, paranoid false -> kNoError), so background work
    /// goes on; the port re-enqueued the column family and rescheduled at once, retrying in a tight loop for as long as
    /// the file system kept failing. Each attempt creates one table file, which the fault counts.
    /// </summary>
    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void FailedFlushIsRetriedOnlyAfterTheBackoff(bool atomicFlush)
    {
        FaultInjectionFileSystem fs = NewFaultFs();
        using var entered = new SemaphoreSlim(0);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using RocksDatabase db = OpenForFlushRetry(fs, atomicFlush, entered, release);
        fs.FailMatching(".sst", FaultOps.Create, int.MaxValue, InjectedFaultKind.IOError);
        db.Put(B("a"), B("1"));
        db.Flush(new FlushOptions { Wait = false });
        Assert.IsTrue(entered.Wait(Timeout), "the failed flush must wait before the retry");
        // While the back-off lasts nothing is retried, whatever else the DB does.
        for (int i = 0; i < 200; i++)
        {
            db.Put(B("b" + i), B("2"));
            Assert.AreEqual("1", GetString(db, "a"));
        }
        Assert.AreEqual(1, fs.TargetedFaults, "flush attempts during the back-off");
        Assert.AreEqual(1L, db.Impl.FlushRetryBackoffCount);
        Assert.IsNull(db.Impl.BackgroundError, "paranoid_checks = false: a flush error is not a background error");
        Assert.AreEqual(0, CountFiles(fs, FileType.TableFile));

        // After the back-off the memtable is flushed again.
        fs.ClearFailMatching();
        release.SetResult();
        db.Flush();
        Assert.AreEqual(1, fs.TargetedFaults);
        Assert.IsTrue(CountFiles(fs, FileType.TableFile) >= 1);
        Assert.AreEqual("1", GetString(db, "a"));
        Assert.AreEqual("2", GetString(db, "b199"));
    }

    /// <summary>
    /// Upstream consumes the FlushRequest of the failed flush; the memtables are flushed by the next request (a memtable
    /// switch calls SchedulePendingFlush), which is not held back by the pause of the failed one.
    /// </summary>
    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void NewFlushRequestIsNotHeldBackByTheBackoff(bool atomicFlush)
    {
        FaultInjectionFileSystem fs = NewFaultFs();
        using var entered = new SemaphoreSlim(0);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using RocksDatabase db = OpenForFlushRetry(fs, atomicFlush, entered, release);
        fs.FailMatching(".sst", FaultOps.Create, 1, InjectedFaultKind.IOError);
        db.Put(B("a"), B("1"));
        db.Flush(new FlushOptions { Wait = false });
        Assert.IsTrue(entered.Wait(Timeout));
        db.Put(B("b"), B("2"));
        db.Flush(); // a new request: flushes both memtables although the back-off has not ended
        Assert.IsTrue(CountFiles(fs, FileType.TableFile) >= 1);
        Assert.AreEqual(1L, db.Impl.FlushRetryBackoffCount);
        release.SetResult(); // the delayed retry finds nothing to flush
        Assert.AreEqual("1", GetString(db, "a"));
        Assert.AreEqual("2", GetString(db, "b"));
    }

    // ---- OPTIONS files of running remote compactions -----------------------------------------------------------

    /// <summary>A service whose worker starts (OpenAndCompact) only after <paramref name="beforeWorker"/> ran.</summary>
    private sealed class DelayedWorkerService(IFileSystem fs, Action<ulong> beforeWorker) : CompactionService
    {
        private byte[]? _input;
        public int Completed;
        public string? Failure;

        public override string Name => "DelayedWorker";

        public override ValueTask<CompactionServiceScheduleResponse> ScheduleAsync(CompactionServiceJobInfo info, ReadOnlyMemory<byte> input,
            CancellationToken cancellationToken)
        {
            _input = input.ToArray();
            return ValueTask.FromResult(new CompactionServiceScheduleResponse("job", CompactionServiceJobStatus.Success));
        }

        public override async ValueTask<CompactionServiceWaitResult> WaitAsync(string scheduledJobId, CancellationToken cancellationToken)
        {
            beforeWorker(CompactionServiceFormat.ReadInput(_input!).OptionsFileNumber);
            try
            {
                byte[] result = await RocksDatabase.OpenAndCompactAsync(new OpenAndCompactOptions(), Path, scheduledJobId, _input!,
                    new CompactionServiceOptionsOverride { FileSystem = fs }, cancellationToken);
                Completed++;
                return new CompactionServiceWaitResult(CompactionServiceJobStatus.Success, result);
            }
            catch (Exception e) when (e is OpenAndCompactException or RocksDbException or IOException)
            {
                Failure = e.Message;
                return new CompactionServiceWaitResult(CompactionServiceJobStatus.Failure, (e as OpenAndCompactException)?.Result);
            }
        }
    }

    /// <summary>
    /// DBImpl::CaptureOptionsFileNumber / MinOptionsFileNumberToKeep and PurgeObsoleteFiles (db/db_impl/db_impl_files.cc
    /// ~664: "For remote compactions, we need to keep OPTIONS file that may get referenced by the remote worker":
    /// optsfile_num2 = min(optsfile_num2, min_options_file_number)). The worker reads the OPTIONS file named in its
    /// input; the port kept only the two newest, so two option changes during the job deleted it and the worker failed.
    /// </summary>
    [TestMethod]
    public void OptionsFileOfARunningRemoteCompactionSurvivesLaterOptionsFiles()
    {
        var fs = new MemoryFileSystem();
        fs.CreateDirectoryIfMissing(Path);
        RocksDatabase? dbRef = null;
        ulong jobOptions = 0;
        bool presentBeforeWorker = false;
        var svc = new DelayedWorkerService(fs, n =>
        {
            jobOptions = n;
            dbRef!.SetOptions([new KeyValuePair<string, string>("level0_slowdown_writes_trigger", "30")]);
            dbRef!.SetOptions([new KeyValuePair<string, string>("level0_slowdown_writes_trigger", "31")]);
            presentBeforeWorker = fs.FileExists(FileNames.OptionsFileName(Path, n));
        });
        DbOptions o = NewOptions(fs);
        o.CompactionService = svc;
        using RocksDatabase db = RocksDatabase.Open(Path, o, new ColumnFamilyOptions { DisableAutoCompactions = true });
        dbRef = db;
        for (int f = 0; f < 3; f++)
        {
            for (int i = 0; i < 10; i++) db.Put(B("k" + i), B("v" + f));
            db.Flush();
        }
        db.CompactRange(null, null);
        Assert.IsNull(svc.Failure);
        Assert.AreEqual(1, svc.Completed);
        Assert.IsTrue(jobOptions != 0);
        Assert.IsTrue(presentBeforeWorker, "the OPTIONS file named in the job input was deleted while the job ran");
        Assert.AreEqual(3, CountFiles(fs, FileType.OptionsFile), "the job's OPTIONS file and the two newest");
        Assert.AreEqual("v2", GetString(db, "k3"));
        // ReleaseOptionsFileNumber: once the job is done only the two newest OPTIONS files are kept again.
        db.SetOptions([new KeyValuePair<string, string>("level0_slowdown_writes_trigger", "32")]);
        Assert.AreEqual(2, CountFiles(fs, FileType.OptionsFile));
        Assert.IsFalse(fs.FileExists(FileNames.OptionsFileName(Path, jobOptions)));
    }
}
