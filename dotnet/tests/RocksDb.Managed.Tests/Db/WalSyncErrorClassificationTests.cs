using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>
/// How a failed WAL sync is classified as a background error, as in DBImpl::SyncWalImpl and the flush job's
/// SyncClosedWals (db/db_impl/db_impl.cc, db/db_impl/db_impl_compaction_flush.cc). Both cases were found by the soak
/// (tools/RocksDb.Managed.Soak, IoError cycles), where they turned recoverable I/O errors into fatal ones:
/// <list type="bullet">
/// <item>a SyncWAL / FlushWAL(sync) that only sees its writer's previous error is not classified with paranoid_checks
/// (the synthetic "Writer has previous error." status lost the original failure's scope and retryable flag);</item>
/// <item>a failed sync of a closed WAL or of the WAL directory during a flush is a flush error (kFlush), not a
/// memtable switch error (kMemTable, always fatal).</item>
/// </list>
/// </summary>
[TestClass]
public sealed class WalSyncErrorClassificationTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    [TestCleanup]
    public void Cleanup() => TestHooks.ClearAll();

    private sealed class Listener : EventListener
    {
        public readonly ManualResetEventSlim Ended = new(false);
        public volatile BackgroundErrorRecoveryInfo? Info;

        public override void OnErrorRecoveryEnd(BackgroundErrorRecoveryInfo info)
        {
            Info = info;
            Ended.Set();
        }
    }

    private static DbOptions Options(FaultInjectionFileSystem fs, Listener? listener)
    {
        var o = new DbOptions { CreateIfMissing = true, FileSystem = fs, ParanoidChecks = true, BgErrorResumeRetryInterval = 10_000 };
        if (listener is not null) o.Listeners.Add(listener);
        return o;
    }

    private static void AssertWritable(RocksDatabase db)
    {
        db.Put(new WriteOptions { Sync = true }, B("after"), B("ok"));
        Assert.AreEqual("ok", S(db.Get(B("after"))));
    }

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task SyncWalThatOnlySeesThePreviousErrorDoesNotMakeItFatal(bool async)
    {
        var fs = new FaultInjectionFileSystem(NewFs());
        var listener = new Listener();
        using var gate = new ManualResetEventSlim(false);
        TestHooks.Set("RecoverFromRetryableBGIOError:BeforeResume", () => gate.Wait(Timeout));
        using RocksDatabase db = RocksDatabase.Open(DbPath, Options(fs, listener));
        db.Put(B("a"), B("1"));

        // A retryable WAL append error stops writes; the automatic recovery is held at the gate, so the failed WAL is
        // still the current one when SyncWAL runs.
        fs.FailAppend(".log", null, AppendFailMode.NothingWritten, InjectedFaultKind.Retryable);
        RocksDbException put = Assert.ThrowsExactly<RocksDbException>(() => db.Put(B("b"), B("2")));
        Assert.AreEqual(StatusCode.IOError, put.Code, put.ToString());
        Assert.AreEqual(ErrorSeverity.HardError, db.BackgroundError?.Severity, db.BackgroundError?.ToString());

        RocksDbException sync = async
            ? await Assert.ThrowsExactlyAsync<RocksDbException>(async () => await db.SyncWalAsync())
            : Assert.ThrowsExactly<RocksDbException>(() => db.SyncWal());
        Assert.AreEqual(StatusCode.IOError, sync.Code);
        StringAssert.Contains(sync.Message, "Writer has previous error");
        Assert.AreEqual(ErrorSeverity.HardError, db.BackgroundError?.Severity,
            "a sync that only saw the writer's previous error changed the background error: " + db.BackgroundError);

        gate.Set();
        Assert.IsTrue(listener.Ended.Wait(Timeout), "recovery did not end");
        Assert.IsNull(listener.Info!.NewBackgroundError, listener.Info.NewBackgroundError?.ToString());
        Assert.IsNull(db.BackgroundError);
        AssertWritable(db);
        Assert.AreEqual("1", S(db.Get(B("a"))));
    }

    [TestMethod]
    public void SyncWalWithAFreshSyncErrorIsStillClassified()
    {
        // Control: a WAL sync that fails by itself (not through the writer's previous error) stops writes as before.
        var fs = new FaultInjectionFileSystem(NewFs());
        using RocksDatabase db = RocksDatabase.Open(DbPath, Options(fs, null));
        db.Put(B("a"), B("1"));
        fs.FailMatching(".log", FaultOps.Sync, 1, InjectedFaultKind.IOError);
        RocksDbException sync = Assert.ThrowsExactly<RocksDbException>(() => db.SyncWal());
        Assert.AreEqual(StatusCode.IOError, sync.Code);
        Assert.AreEqual(ErrorSeverity.FatalError, db.BackgroundError?.Severity, db.BackgroundError?.ToString());
    }

    /// <summary>
    /// A NoSpace failure while the flush syncs the closed WAL (several column families) or the WAL directory: a
    /// recoverable hard error, as in upstream's flush job, which the default SstFileManager recovers from.
    /// </summary>
    [TestMethod]
    [DataRow(false, DisplayName = "closed WAL sync")]
    [DataRow(true, DisplayName = "WAL directory sync")]
    public void ClosedWalSyncNoSpaceDuringFlushIsRecoverable(bool walDirectory)
    {
        var fs = new FaultInjectionFileSystem(NewFs());
        var listener = new Listener();
        using RocksDatabase db = RocksDatabase.Open(DbPath, Options(fs, listener));
        ColumnFamily cf = db.CreateColumnFamily("other", new ColumnFamilyOptions());
        // A new WAL whose directory entry is not synced yet, holding data of both column families.
        db.Flush();
        db.Put(B("a"), B("1"));
        cf.Put(B("x"), B("1"));

        fs.FailMatching(walDirectory ? DbPath : ".log", walDirectory ? FaultOps.SyncDirectory : FaultOps.Sync, 1, InjectedFaultKind.NoSpace);
        RocksDbException flush = Assert.ThrowsExactly<RocksDbException>(() => db.Flush());
        Assert.AreEqual(StatusCode.IOError, flush.Code, flush.ToString());
        Assert.AreEqual(StatusSubCode.NoSpace, flush.SubCode, flush.ToString());
        Assert.AreEqual(1, fs.TargetedFaults, "the injected fault was not used");

        // Before the fix the error was a memtable switch error (kMemTable): fatal, so no recovery ever started.
        Assert.IsTrue(listener.Ended.Wait(Timeout), "no recovery; background error: " + db.BackgroundError);
        Assert.AreEqual(ErrorSeverity.HardError, listener.Info!.OldBackgroundError.Severity, listener.Info.OldBackgroundError.ToString());
        Assert.IsNull(listener.Info.NewBackgroundError, listener.Info.NewBackgroundError?.ToString());
        Assert.IsNull(db.BackgroundError);
        AssertWritable(db);
        cf.Put(new WriteOptions { Sync = true }, B("y"), B("2"));
        db.Flush();
        Assert.AreEqual("1", S(db.Get(B("a"))));
        Assert.AreEqual("1", S(cf.Get(B("x"))));
        Assert.AreEqual("2", S(cf.Get(B("y"))));
    }
}
