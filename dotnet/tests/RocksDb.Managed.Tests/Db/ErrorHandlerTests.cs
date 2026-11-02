using System.Collections.Concurrent;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>
/// Background error handling and recovery. Ports of the scenarios of db/error_handler_fs_test.cc: flush, compaction,
/// WAL and MANIFEST write failures (plain, retryable, no-space), automatic resume, manual Resume, listener overrides.
/// </summary>
[TestClass]
public sealed class ErrorHandlerTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);

    /// <summary>Records background error events (ErrorHandlerFSListener in error_handler_fs_test.cc).</summary>
    private sealed class ErrorListener : EventListener
    {
        public readonly ConcurrentQueue<BackgroundErrorEventArgs> Errors = new();
        public readonly ConcurrentQueue<BackgroundErrorRecoveryInfo> RecoveryEnds = new();
        public readonly ManualResetEventSlim RecoveryEnded = new(false);
        public bool DisableAutoRecovery { get; set; }
        public bool SuppressErrors { get; set; }
        public int RecoveryBegins;

        public override void OnBackgroundError(BackgroundErrorEventArgs e)
        {
            Errors.Enqueue(new BackgroundErrorEventArgs(e.Reason, e.Error!));
            if (SuppressErrors) e.Error = null;
        }

        public override void OnErrorRecoveryBegin(ErrorRecoveryBeginEventArgs e)
        {
            Interlocked.Increment(ref RecoveryBegins);
            if (DisableAutoRecovery) e.AutoRecovery = false;
        }

        public override void OnErrorRecoveryEnd(BackgroundErrorRecoveryInfo info)
        {
            RecoveryEnds.Enqueue(info);
            RecoveryEnded.Set();
        }

        public BackgroundErrorRecoveryInfo WaitForRecoveryEnd()
        {
            Assert.IsTrue(RecoveryEnded.Wait(Timeout), "recovery did not end");
            Assert.IsTrue(RecoveryEnds.TryPeek(out BackgroundErrorRecoveryInfo? info));
            return info!;
        }
    }

    private static (FaultInjectionFileSystem Fs, DbOptions Options, ErrorListener Listener) Setup()
    {
        var fs = new FaultInjectionFileSystem(NewFs());
        var listener = new ErrorListener();
        var options = new DbOptions { CreateIfMissing = true, FileSystem = fs, BgErrorResumeRetryInterval = 20_000 };
        options.Listeners.Add(listener);
        return (fs, options, listener);
    }

    private static void AssertReopens(FaultInjectionFileSystem fs, params string[] keys)
    {
        using RocksDatabase db = RocksDatabase.Open(DbPath, new DbOptions { FileSystem = fs });
        foreach (string k in keys) Assert.AreEqual("v-" + k, GetString(db, k), k);
    }

    private static void Put(RocksDatabase db, string key) => db.Put(B(key), B("v-" + key));

    [TestMethod]
    public void ClassificationMatchesUpstreamMaps()
    {
        Assert.AreEqual(ErrorSeverity.SoftError, ErrorHandler.Classify(BackgroundErrorReason.Compaction, StatusCode.IOError, StatusSubCode.NoSpace, true));
        Assert.AreEqual(ErrorSeverity.NoError, ErrorHandler.Classify(BackgroundErrorReason.Compaction, StatusCode.IOError, StatusSubCode.NoSpace, false));
        Assert.AreEqual(ErrorSeverity.HardError, ErrorHandler.Classify(BackgroundErrorReason.Flush, StatusCode.IOError, StatusSubCode.NoSpace, true));
        Assert.AreEqual(ErrorSeverity.HardError, ErrorHandler.Classify(BackgroundErrorReason.ManifestWrite, StatusCode.IOError, StatusSubCode.NoSpace, false));
        Assert.AreEqual(ErrorSeverity.FatalError, ErrorHandler.Classify(BackgroundErrorReason.FlushNoWal, StatusCode.IOError, StatusSubCode.IOFenced, false));
        Assert.AreEqual(ErrorSeverity.HardError, ErrorHandler.Classify(BackgroundErrorReason.Compaction, StatusCode.IOError, StatusSubCode.SpaceLimit, true));
        Assert.AreEqual(ErrorSeverity.UnrecoverableError, ErrorHandler.Classify(BackgroundErrorReason.Flush, StatusCode.Corruption, StatusSubCode.None, true));
        Assert.AreEqual(ErrorSeverity.NoError, ErrorHandler.Classify(BackgroundErrorReason.Flush, StatusCode.Corruption, StatusSubCode.None, false));
        Assert.AreEqual(ErrorSeverity.FatalError, ErrorHandler.Classify(BackgroundErrorReason.Flush, StatusCode.IOError, StatusSubCode.None, true));
        Assert.AreEqual(ErrorSeverity.FatalError, ErrorHandler.Classify(BackgroundErrorReason.ManifestWriteNoWal, StatusCode.IOError, StatusSubCode.None, false));
        Assert.AreEqual(ErrorSeverity.FatalError, ErrorHandler.Classify(BackgroundErrorReason.MemTable, StatusCode.Busy, StatusSubCode.None, false));
        Assert.AreEqual(ErrorSeverity.NoError, ErrorHandler.Classify(BackgroundErrorReason.WriteCallback, StatusCode.IOError, StatusSubCode.None, false));
        Assert.AreEqual(ErrorSeverity.NoError, ErrorHandler.Classify(BackgroundErrorReason.Compaction, StatusCode.Aborted, StatusSubCode.None, false));
    }

    [TestMethod]
    public void NoSpaceExceptionMapsToNoSpaceStatus()
    {
        RocksDbException e = RocksDbException.FromException(new IOException("disk full", 28));
        Assert.AreEqual(StatusCode.IOError, e.Code);
        Assert.AreEqual(StatusSubCode.NoSpace, e.SubCode);
        e = RocksDbException.FromException(new IOException("other", 5));
        Assert.AreEqual(StatusSubCode.None, e.SubCode);
    }

    [TestMethod]
    public void FlushRetryableErrorIsSoftAndAutoRecovers()
    {
        (FaultInjectionFileSystem fs, DbOptions options, ErrorListener listener) = Setup();
        using var gate = new ManualResetEventSlim(false);
        TestHooks.Set("RecoverFromRetryableBGIOError:BeforeResume", () => gate.Wait(Timeout));
        try
        {
            using (RocksDatabase db = RocksDatabase.Open(DbPath, options))
            {
                Put(db, "a");
                fs.FailMatching(".sst", FaultOps.Append, 1, InjectedFaultKind.Retryable);
                RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.Flush());
                Assert.AreEqual(StatusCode.IOError, ex.Code);
                Assert.AreEqual(ErrorSeverity.SoftError, ex.Severity);
                Assert.IsTrue(ex.Retryable);
                Assert.IsTrue(listener.Errors.TryPeek(out BackgroundErrorEventArgs? e));
                Assert.AreEqual(BackgroundErrorReason.FlushNoWal, e!.Reason);
                // Soft error: writes continue while the recovery waits.
                Put(db, "b");
                Assert.AreEqual(ErrorSeverity.SoftError, db.BackgroundError!.Severity);
                gate.Set();
                BackgroundErrorRecoveryInfo end = listener.WaitForRecoveryEnd();
                Assert.IsNull(end.NewBackgroundError);
                Assert.AreEqual(StatusCode.IOError, end.OldBackgroundError.Code);
                Assert.IsNull(db.BackgroundError);
                Assert.AreEqual(1, listener.RecoveryBegins);
                Put(db, "c");
                db.Flush();
                Assert.AreEqual("v-a", GetString(db, "a"));
                Assert.AreEqual("1", db.GetProperty(DbProperties.BackgroundErrors));
            }
            AssertReopens(fs, "a", "b", "c");
        }
        finally
        {
            gate.Set();
            TestHooks.ClearAll();
        }
    }

    [TestMethod]
    public void FlushRetryableErrorWithoutAutoResumeIsHardUntilResume()
    {
        (FaultInjectionFileSystem fs, DbOptions options, ErrorListener listener) = Setup();
        options.MaxBgErrorResumeCount = 0;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, options))
        {
            Put(db, "a");
            fs.FailMatching(".sst", FaultOps.Append, 1, InjectedFaultKind.Retryable);
            RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.Flush());
            Assert.AreEqual(ErrorSeverity.HardError, ex.Severity, ex.ToString());
            ex = Assert.ThrowsExactly<RocksDbException>(() => Put(db, "b"));
            Assert.AreEqual(StatusCode.IOError, ex.Code);
            Assert.AreEqual(ErrorSeverity.HardError, ex.Severity);
            Assert.AreEqual(0, listener.RecoveryBegins);
            db.Resume();
            Assert.IsNull(db.BackgroundError);
            Assert.AreEqual(1, listener.RecoveryEnds.Count);
            Put(db, "b");
            // Resume without an error is a no-op.
            db.Resume();
        }
        AssertReopens(fs, "a", "b");
    }

    [TestMethod]
    public void FlushNoSpaceErrorNeedsManualResume()
    {
        (FaultInjectionFileSystem fs, DbOptions options, ErrorListener listener) = Setup();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, options))
        {
            Put(db, "a");
            fs.FailMatching(".sst", FaultOps.Append, 1, InjectedFaultKind.NoSpace);
            RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.Flush());
            Assert.AreEqual(StatusSubCode.NoSpace, ex.SubCode);
            Assert.AreEqual(ErrorSeverity.HardError, ex.Severity);
            ex = Assert.ThrowsExactly<RocksDbException>(() => Put(db, "b"));
            Assert.AreEqual(StatusSubCode.NoSpace, ex.SubCode);
            Assert.AreEqual(0, listener.RecoveryBegins);
            // Still out of space: Resume fails and keeps the error.
            fs.FailMatching(".sst", FaultOps.Append, 1, InjectedFaultKind.NoSpace);
            ex = Assert.ThrowsExactly<RocksDbException>(() => db.Resume());
            Assert.AreEqual(StatusSubCode.NoSpace, ex.SubCode);
            Assert.IsNotNull(db.BackgroundError);
            db.Resume();
            Put(db, "b");
        }
        AssertReopens(fs, "a", "b");
    }

    [TestMethod]
    public void FlushPlainIOErrorIsFatal()
    {
        (FaultInjectionFileSystem fs, DbOptions options, _) = Setup();
        using RocksDatabase db = RocksDatabase.Open(DbPath, options);
        Put(db, "a");
        fs.FailMatching(".sst", FaultOps.Append, 1, InjectedFaultKind.IOError);
        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.Flush());
        Assert.AreEqual(ErrorSeverity.FatalError, ex.Severity);
        ex = Assert.ThrowsExactly<RocksDbException>(() => db.Resume());
        Assert.AreEqual(ErrorSeverity.FatalError, ex.Severity);
        ex = Assert.ThrowsExactly<RocksDbException>(() => Put(db, "b"));
        Assert.AreEqual(StatusCode.IOError, ex.Code);
        Assert.AreEqual("v-a", GetString(db, "a"));
    }

    [TestMethod]
    public void DataLossErrorIsUnrecoverable()
    {
        (FaultInjectionFileSystem fs, DbOptions options, _) = Setup();
        using RocksDatabase db = RocksDatabase.Open(DbPath, options);
        Put(db, "a");
        fs.FailMatching(".sst", FaultOps.Append, 1, InjectedFaultKind.DataLoss);
        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.Flush());
        Assert.AreEqual(ErrorSeverity.UnrecoverableError, ex.Severity);
        Assert.ThrowsExactly<RocksDbException>(() => db.Resume());
    }

    [TestMethod]
    public void ManifestRetryableErrorRecoversWithNewManifest()
    {
        (FaultInjectionFileSystem fs, DbOptions options, ErrorListener listener) = Setup();
        using var gate = new ManualResetEventSlim(false);
        TestHooks.Set("RecoverFromRetryableBGIOError:BeforeResume", () => gate.Wait(Timeout));
        try
        {
            using (RocksDatabase db = RocksDatabase.Open(DbPath, options))
            {
                Put(db, "a");
                string manifest = CurrentManifest(fs);
                fs.FailMatching("MANIFEST", FaultOps.Append, 1, InjectedFaultKind.Retryable);
                // The manual flush reports the (soft) error unless the automatic recovery already finished.
                RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.Flush());
                Assert.AreEqual(ErrorSeverity.SoftError, ex.Severity);
                gate.Set();
                BackgroundErrorRecoveryInfo end = listener.WaitForRecoveryEnd();
                Assert.IsNull(end.NewBackgroundError);
                Assert.IsTrue(listener.Errors.TryPeek(out BackgroundErrorEventArgs? e));
                Assert.AreEqual(BackgroundErrorReason.ManifestWriteNoWal, e!.Reason);
                Assert.AreNotEqual(manifest, CurrentManifest(fs));
                Put(db, "b");
                db.Flush();
            }
            AssertReopens(fs, "a", "b");
        }
        finally
        {
            gate.Set();
            TestHooks.ClearAll();
        }
    }

    [TestMethod]
    public void ManifestNoSpaceErrorRecoversOnResume()
    {
        (FaultInjectionFileSystem fs, DbOptions options, _) = Setup();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, options))
        {
            Put(db, "a");
            string manifest = CurrentManifest(fs);
            fs.FailMatching("MANIFEST", FaultOps.Append, 1, InjectedFaultKind.NoSpace);
            RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.Flush());
            Assert.AreEqual(ErrorSeverity.HardError, ex.Severity);
            Assert.ThrowsExactly<RocksDbException>(() => Put(db, "b"));
            db.Resume();
            Assert.AreNotEqual(manifest, CurrentManifest(fs));
            Put(db, "b");
        }
        AssertReopens(fs, "a", "b");
    }

    [TestMethod]
    public void WalRetryableErrorReplacesWalAndAutoRecovers()
    {
        (FaultInjectionFileSystem fs, DbOptions options, ErrorListener listener) = Setup();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, options))
        {
            ColumnFamily cf = db.CreateColumnFamily("other", new ColumnFamilyOptions());
            Put(db, "a");
            cf.Put(B("x"), B("v-x"));
            fs.FailMatching(".log", FaultOps.Append, 1, InjectedFaultKind.Retryable);
            RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => Put(db, "lost"));
            Assert.AreEqual(StatusCode.IOError, ex.Code);
            Assert.IsTrue(listener.Errors.TryPeek(out BackgroundErrorEventArgs? e));
            Assert.AreEqual(BackgroundErrorReason.WriteCallback, e!.Reason);
            Assert.AreEqual(ErrorSeverity.HardError, e.Error!.Severity);
            BackgroundErrorRecoveryInfo end = listener.WaitForRecoveryEnd();
            Assert.IsNull(end.NewBackgroundError);
            Put(db, "b");
            Assert.IsNull(GetString(db, "lost"));
            Assert.AreEqual("v-x", GetString(cf, "x"));
        }
        using RocksDatabase db2 = RocksDatabase.Open(DbPath, new DbOptions { FileSystem = fs },
            [new ColumnFamilyDescriptor("default", new ColumnFamilyOptions()), new ColumnFamilyDescriptor("other", new ColumnFamilyOptions())]);
        Assert.AreEqual("v-a", GetString(db2, "a"));
        Assert.AreEqual("v-b", GetString(db2, "b"));
        Assert.IsNull(GetString(db2, "lost"));
        Assert.AreEqual("v-x", GetString(db2.GetColumnFamily("other"), "x"));
    }

    [TestMethod]
    public void WalSyncNoSpaceErrorManualResume()
    {
        (FaultInjectionFileSystem fs, DbOptions options, _) = Setup();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, options))
        {
            Put(db, "a");
            fs.FailMatching(".log", FaultOps.Sync, 1, InjectedFaultKind.NoSpace);
            Assert.ThrowsExactly<RocksDbException>(() => db.Put(new WriteOptions { Sync = true }, B("s"), B("v-s")));
            RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => Put(db, "b"));
            Assert.AreEqual(StatusSubCode.NoSpace, ex.SubCode);
            db.Resume();
            Put(db, "b");
        }
        AssertReopens(fs, "a", "b");
    }

    [TestMethod]
    public void CompactionRetryableErrorIsRetried()
    {
        (FaultInjectionFileSystem fs, DbOptions options, ErrorListener listener) = Setup();
        TimeSpan delay = DbImpl.CompactionErrorRetryDelay;
        DbImpl.CompactionErrorRetryDelay = TimeSpan.FromMilliseconds(10);
        try
        {
            var cfo = new ColumnFamilyOptions { Level0FileNumCompactionTrigger = 2 };
            using var gate = new ManualResetEventSlim(false);
            TestHooks.Set("DBImpl::RunCompaction:Start", () => gate.Wait(Timeout));
            using (RocksDatabase db = RocksDatabase.Open(DbPath, options, cfo))
            {
                // Overlapping files, so the compaction rewrites them (no trivial move).
                Put(db, "a");
                Put(db, "b");
                db.Flush();
                Put(db, "a");
                Put(db, "b");
                db.Flush();
                // The compaction triggered by the second file waits at the gate; its first output write fails.
                fs.FailMatching(".sst", FaultOps.Append, 1, InjectedFaultKind.Retryable);
                gate.Set();
                SpinWait.SpinUntil(() => fs.TargetedFaults > 0, Timeout);
                TestHooks.ClearAll();
                db.WaitForCompact();
                Assert.AreEqual("0", db.GetProperty("rocksdb.num-files-at-level0"));
                Assert.IsTrue(listener.Errors.TryPeek(out BackgroundErrorEventArgs? e));
                Assert.AreEqual(BackgroundErrorReason.Compaction, e!.Reason);
                Assert.AreEqual(ErrorSeverity.SoftError, e.Error!.Severity);
                Assert.IsNull(db.BackgroundError);
                Put(db, "c");
            }
            AssertReopens(fs, "a", "b", "c");
        }
        finally
        {
            TestHooks.ClearAll();
            DbImpl.CompactionErrorRetryDelay = delay;
        }
    }

    [TestMethod]
    public void ListenerCanSuppressBackgroundError()
    {
        (FaultInjectionFileSystem fs, DbOptions options, ErrorListener listener) = Setup();
        listener.SuppressErrors = true;
        using RocksDatabase db = RocksDatabase.Open(DbPath, options);
        Put(db, "a");
        fs.FailMatching(".sst", FaultOps.Append, 1, InjectedFaultKind.IOError);
        db.Flush(new FlushOptions { Wait = false });
        SpinWait.SpinUntil(() => fs.TargetedFaults > 0 && listener.Errors.Count > 0, Timeout);
        Assert.AreEqual(1, listener.Errors.Count);
        Assert.IsNull(db.BackgroundError);
        Put(db, "b");
        db.Flush();
        Assert.AreEqual("v-a", GetString(db, "a"));
    }

    [TestMethod]
    public void ListenerCanDisableAutoRecovery()
    {
        (FaultInjectionFileSystem fs, DbOptions options, ErrorListener listener) = Setup();
        listener.DisableAutoRecovery = true;
        using RocksDatabase db = RocksDatabase.Open(DbPath, options);
        Put(db, "a");
        fs.FailMatching(".log", FaultOps.Append, 1, InjectedFaultKind.Retryable);
        Assert.ThrowsExactly<RocksDbException>(() => Put(db, "lost"));
        Assert.AreEqual(1, listener.RecoveryBegins);
        Assert.IsFalse(listener.RecoveryEnded.Wait(100));
        Assert.ThrowsExactly<RocksDbException>(() => Put(db, "b"));
        db.Resume();
        Put(db, "b");
    }

    [TestMethod]
    public void AutoResumeGivesUpAfterMaxRetries()
    {
        (FaultInjectionFileSystem fs, DbOptions options, ErrorListener listener) = Setup();
        options.MaxBgErrorResumeCount = 3;
        using RocksDatabase db = RocksDatabase.Open(DbPath, options);
        Put(db, "a");
        fs.FailMatching(".log", FaultOps.Append | FaultOps.Create, 100, InjectedFaultKind.Retryable);
        Assert.ThrowsExactly<RocksDbException>(() => Put(db, "lost"));
        BackgroundErrorRecoveryInfo end = listener.WaitForRecoveryEnd();
        Assert.IsNotNull(end.NewBackgroundError);
        Assert.AreEqual(StatusCode.Aborted, end.NewBackgroundError!.Code);
        Assert.IsNotNull(db.BackgroundError);
        fs.ClearFailMatching();
        db.Resume();
        Put(db, "b");
        Assert.AreEqual("v-a", GetString(db, "a"));
    }

    [TestMethod]
    public void CloseDuringAutoRecoveryEndsRecovery()
    {
        (FaultInjectionFileSystem fs, DbOptions options, ErrorListener listener) = Setup();
        options.BgErrorResumeRetryInterval = 10_000_000; // 10 s between attempts
        RocksDatabase db = RocksDatabase.Open(DbPath, options);
        Put(db, "a");
        fs.FailMatching(".log", FaultOps.Append | FaultOps.Create, 100, InjectedFaultKind.Retryable);
        Assert.ThrowsExactly<RocksDbException>(() => Put(db, "lost"));
        Thread.Sleep(50);
        var sw = System.Diagnostics.Stopwatch.StartNew();
        db.Dispose();
        Assert.IsTrue(sw.Elapsed < TimeSpan.FromSeconds(8), "close waited for the retry interval");
        BackgroundErrorRecoveryInfo end = listener.WaitForRecoveryEnd();
        Assert.IsNotNull(end.NewBackgroundError);
        fs.ClearFailMatching();
        Assert.AreEqual(0, fs.OpenFiles);
        AssertReopens(fs, "a");
    }

    private static string CurrentManifest(FaultInjectionFileSystem fs)
    {
        using ISequentialFile f = fs.OpenSequential(DbPath + "/CURRENT");
        var buf = new byte[256];
        int n = f.Read(buf);
        return System.Text.Encoding.ASCII.GetString(buf, 0, n).Trim();
    }
}
