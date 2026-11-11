using System.Collections.Concurrent;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Tests.Db;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4 (closing, scheduling slice): background compaction / flush scheduling, WaitForCompact and the error
/// handler's recovery paths, checked against db/db_impl/db_impl_compaction_flush.cc, db/flush_job.cc and
/// db/error_handler.cc.
/// </summary>
[TestClass]
public sealed class Audit4ClosingSchedulingTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);

    private sealed class RecoveryListener : EventListener
    {
        public readonly ConcurrentQueue<BackgroundErrorRecoveryInfo> RecoveryEnds = new();
        public readonly ManualResetEventSlim RecoveryEnded = new(false);

        public override void OnErrorRecoveryEnd(BackgroundErrorRecoveryInfo info)
        {
            RecoveryEnds.Enqueue(info);
            RecoveryEnded.Set();
        }
    }

    private static (FaultInjectionFileSystem Fs, DbOptions Options, RecoveryListener Listener) Setup()
    {
        var fs = new FaultInjectionFileSystem(NewFs());
        var listener = new RecoveryListener();
        var options = new DbOptions { CreateIfMissing = true, FileSystem = fs, BgErrorResumeRetryInterval = 20_000 };
        options.Listeners.Add(listener);
        return (fs, options, listener);
    }

    /// <summary>
    /// R41. DBImpl::WaitForCompact (db_impl_compaction_flush.cc, the loop): "if (shutting_down_) return
    /// Status::ShutdownInProgress()" is checked before anything else, also when no job is running.
    /// </summary>
    [TestMethod]
    public void WaitForCompactAfterShutdownBeganIsShutdownInProgress()
    {
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(NewFs()));
        db.Put(B("a"), B("1"));
        db.WaitForCompact();
        db.CancelAllBackgroundWork(wait: true);
        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.WaitForCompact());
        Assert.AreEqual(StatusCode.ShutdownInProgress, ex.Code);
    }

    /// <summary>
    /// R41. DBImpl::WaitForCompact waits only while "error_handler_.GetBGError().ok()" and otherwise returns
    /// error_handler_.GetBGError(): any background error is returned, whether or not it stops background work.
    /// </summary>
    [TestMethod]
    public void WaitForCompactReturnsTheBackgroundError()
    {
        (FaultInjectionFileSystem fs, DbOptions options, _) = Setup();
        options.MaxBgErrorResumeCount = 0;
        using RocksDatabase db = RocksDatabase.Open(DbPath, options);
        db.Put(B("a"), B("1"));
        fs.FailMatching(".sst", FaultOps.Append, 1, InjectedFaultKind.Retryable);
        Assert.ThrowsExactly<RocksDbException>(() => db.Flush());
        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.WaitForCompact());
        Assert.AreEqual(StatusCode.IOError, ex.Code);
        db.Resume();
        db.WaitForCompact();
    }

    /// <summary>
    /// R42. ErrorHandler::StartRecoverFromRetryableBGIOError (error_handler.cc): with end_recovery_ set no recovery is
    /// started, but the listeners are told NotifyOnErrorRecoveryEnd(bg_error_, Status::ShutdownInProgress()).
    /// </summary>
    [TestMethod]
    public void RetryableErrorAfterRecoveryEndedReportsShutdownToListeners()
    {
        (FaultInjectionFileSystem fs, DbOptions options, RecoveryListener listener) = Setup();
        using RocksDatabase db = RocksDatabase.Open(DbPath, options);
        db.Put(B("a"), B("1"));
        // EndAutoRecovery (the first step of closing the DB).
        db.DefaultColumnFamily.Db.ErrorHandler.EndAutoRecoveryAsync(synchronous: true).GetAwaiter().GetResult();
        fs.FailMatching(".sst", FaultOps.Append, 1, InjectedFaultKind.Retryable);
        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.Flush());
        Assert.AreEqual(StatusCode.IOError, ex.Code);
        Assert.IsTrue(listener.RecoveryEnded.Wait(Timeout), "OnErrorRecoveryEnd was not raised");
        Assert.AreEqual(1, listener.RecoveryEnds.Count);
        Assert.IsTrue(listener.RecoveryEnds.TryPeek(out BackgroundErrorRecoveryInfo? info));
        Assert.AreEqual(StatusCode.IOError, info!.OldBackgroundError.Code);
        Assert.AreEqual(StatusCode.ShutdownInProgress, info.NewBackgroundError!.Code);
        Assert.IsNotNull(db.BackgroundError);
    }

    /// <summary>
    /// R43. ErrorHandler::RecoverFromRetryableBGIOError: the recovery is retried only when
    /// "recovery_error_.GetRetryable()". A file-scoped error that is not retryable (it enters SetBGError through the
    /// "scope == kIOErrorScopeFile || GetRetryable()" branch and becomes recovery_error_) ends the automatic recovery:
    /// NotifyOnErrorRecoveryEnd(bg_error_, recovery_error_), the background error stays.
    /// </summary>
    [TestMethod]
    public void FileScopedErrorDuringAutoRecoveryEndsTheRecovery()
    {
        (FaultInjectionFileSystem fs, DbOptions options, RecoveryListener listener) = Setup();
        int resumes = 0;
        TestHooks.Set("RecoverFromRetryableBGIOError:BeforeResume", () =>
        {
            // The recovery's retry of the flush fails with a file-scoped, not retryable error.
            if (Interlocked.Increment(ref resumes) == 1) fs.FailMatching(".sst", FaultOps.Append, 1, InjectedFaultKind.FileScope);
        });
        try
        {
            using RocksDatabase db = RocksDatabase.Open(DbPath, options);
            db.Put(B("a"), B("1"));
            fs.FailMatching(".sst", FaultOps.Append, 1, InjectedFaultKind.Retryable);
            Assert.ThrowsExactly<RocksDbException>(() => db.Flush());
            Assert.IsTrue(listener.RecoveryEnded.Wait(Timeout), "recovery did not end");
            Assert.IsTrue(listener.RecoveryEnds.TryPeek(out BackgroundErrorRecoveryInfo? info));
            Assert.IsNotNull(info!.NewBackgroundError, "the recovery was retried after a file-scoped error that is not retryable");
            Assert.AreEqual(StatusCode.IOError, info.NewBackgroundError!.Code);
            Assert.IsFalse(info.NewBackgroundError.Retryable);
            Assert.AreEqual(1, Volatile.Read(ref resumes));
            Assert.IsNotNull(db.BackgroundError);
            // A manual Resume still recovers.
            TestHooks.ClearAll();
            db.Resume();
            Assert.IsNull(db.BackgroundError);
            Assert.AreEqual("1", GetString(db, "a"));
        }
        finally
        {
            TestHooks.ClearAll();
        }
    }

    /// <summary>
    /// R44. ErrorHandler::RecoverFromBGError: "if (is_manual || ...) recovery_in_prog_ = false" runs whatever
    /// ResumeImpl returned. A failure thrown out of the managed ResumeImpl must not leave the flag set, or every later
    /// Resume() is refused with Busy("Recovery in progress").
    /// </summary>
    [TestMethod]
    public void ManualResumeThatThrowsDoesNotLeaveRecoveryInProgress()
    {
        (FaultInjectionFileSystem fs, DbOptions options, _) = Setup();
        options.MaxBgErrorResumeCount = 0;
        try
        {
            using RocksDatabase db = RocksDatabase.Open(DbPath, options);
            db.Put(B("a"), B("1"));
            fs.FailMatching(".sst", FaultOps.Append, 1, InjectedFaultKind.Retryable);
            Assert.ThrowsExactly<RocksDbException>(() => db.Flush());
            TestHooks.Set("DBImpl::ResumeImpl:Start", static () => throw new IOException("injected resume failure"));
            RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.Resume());
            Assert.AreEqual(StatusCode.IOError, ex.Code);
            TestHooks.ClearAll();
            Assert.IsNotNull(db.BackgroundError);
            db.Resume(); // was: Busy
            Assert.IsNull(db.BackgroundError);
            db.Put(B("b"), B("2"));
            Assert.AreEqual("1", GetString(db, "a"));
        }
        finally
        {
            TestHooks.ClearAll();
        }
    }

    /// <summary>
    /// DBImpl::BackgroundCompaction, "if (!enough_room)": the picked compaction is released, the compaction score is
    /// recomputed and the column family is put back into the compaction queue (EnqueuePendingCompaction), so the
    /// compaction is picked again once there is room, without another flush or write.
    /// </summary>
    [TestMethod]
    public void CompactionCancelledForLackOfRoomIsRetriedOnceThereIsRoom()
    {
        MemoryFileSystem fs = NewFs();
        using var sfm = new SstFileManager(fs, 0, 0.25, 64UL << 20);
        sfm.SetMaxAllowedSpaceUsage(10UL << 20);
        sfm.SetCompactionBufferSize(10UL << 20);
        var stats = new Statistics();
        DbOptions o = NewOptions(fs);
        o.SstFileManager = sfm;
        o.Statistics = stats;
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, new ColumnFamilyOptions { Level0FileNumCompactionTrigger = 2 });
        for (int round = 0; round < 4; round++)
        {
            for (int i = 0; i < 50; i++) db.Put(Key(i), Value(i, round));
            db.Flush();
        }
        db.WaitForCompact();
        Assert.IsTrue(stats.GetTickerCount(Ticker.CompactionCancelled) > 0);
        Assert.AreEqual("4", db.GetProperty("rocksdb.num-files-at-level0"));
        // Room again: the queued column family is compacted by the next scheduling round.
        sfm.SetCompactionBufferSize(0);
        db.WaitForCompact();
        Assert.AreEqual("0", db.GetProperty("rocksdb.num-files-at-level0"));
        Assert.AreEqual(0UL, sfm.CompactionsReservedSize);
        Assert.AreEqual(S(Value(3, 3)), GetString(db, S(Key(3))!));
    }

    /// <summary>
    /// FlushJob::Run (db/flush_job.cc): after WriteLevel0Table, a flush that is not an error-recovery flush does not
    /// install its result when "!error_handler->GetBGError().ok() && error_handler->IsBGWorkStopped()": the memtables are
    /// rolled back (RollbackMemtableFlush) and the flush returns the background error.
    /// </summary>
    [TestMethod]
    [Ignore("audit4: open, Db/DbImpl.Flush.cs is being changed by the atomic-flush work (read-only for this slice): FlushAndInstallAsync installs the flush result although a background error stopped background work meanwhile")]
    public void FlushDoesNotInstallItsResultOnceBackgroundWorkIsStopped()
    {
        try
        {
            using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(NewFs()));
            DbImpl impl = db.DefaultColumnFamily.Db;
            db.Put(B("a"), B("1"));
            TestHooks.Set("DBImpl::FlushMemTables:BeforeLogAndApply",
                () => impl.SetBackgroundError(RocksDbException.IOError("injected"), BackgroundErrorReason.Flush, walRelated: false));
            RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.Flush());
            Assert.AreEqual(StatusCode.IOError, ex.Code);
            TestHooks.ClearAll();
            Assert.AreEqual("0", db.GetProperty("rocksdb.num-files-at-level0"));
            Assert.AreEqual("1", db.GetProperty("rocksdb.num-immutable-mem-table"));
        }
        finally
        {
            TestHooks.ClearAll();
        }
    }
}
