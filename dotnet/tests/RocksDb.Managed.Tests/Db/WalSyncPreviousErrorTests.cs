using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>
/// Repro from the soak (cycle 27, IoError retryable): a retryable WAL append error stops writes and starts the
/// automatic recovery; before the recovery replaces the WAL, a SyncWAL / FlushWAL(sync) reaches the failed writer and
/// fails with the synthetic "Writer has previous error.". Upstream (DBImpl::SyncWalImpl) does not classify that status
/// with paranoid_checks ("it has lost the original scope and could incorrectly make the error fatal"); the port must not
/// either, or the retryable error becomes a fatal one and Resume never succeeds.
/// </summary>
[TestClass]
public sealed class WalSyncPreviousErrorTests
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

    [TestMethod]
    [Ignore("Known bug, repro only: DbImpl.FlushWalAsync classifies the synthetic 'Writer has previous error.' status (FatalError); upstream SyncWalImpl skips it with paranoid_checks")]
    [DataRow(false)]
    [DataRow(true)]
    public async Task SyncWalAfterRetryableWalErrorDoesNotMakeItFatal(bool async)
    {
        var fs = new FaultInjectionFileSystem(NewFs());
        var listener = new Listener();
        using var gate = new ManualResetEventSlim(false);
        TestHooks.Set("RecoverFromRetryableBGIOError:BeforeResume", () => gate.Wait(Timeout));
        var o = new DbOptions { CreateIfMissing = true, FileSystem = fs, ParanoidChecks = true, BgErrorResumeRetryInterval = 10_000 };
        o.Listeners.Add(listener);
        using RocksDatabase db = RocksDatabase.Open(DbPath, o);
        db.Put(B("a"), B("1"));

        fs.FailAppend(".log", null, AppendFailMode.NothingWritten, InjectedFaultKind.Retryable);
        RocksDbException put = Assert.ThrowsExactly<RocksDbException>(() => db.Put(B("b"), B("2")));
        Assert.AreEqual(StatusCode.IOError, put.Code, put.ToString());
        Assert.AreEqual(ErrorSeverity.HardError, db.BackgroundError?.Severity, db.BackgroundError?.ToString());

        // The recovery is held at the gate: the failed WAL is still the current one.
        RocksDbException sync = async
            ? await Assert.ThrowsExactlyAsync<RocksDbException>(async () => await db.SyncWalAsync())
            : Assert.ThrowsExactly<RocksDbException>(() => db.SyncWal());
        StringAssert.Contains(sync.Message, "Writer has previous error");
        Assert.AreEqual(ErrorSeverity.HardError, db.BackgroundError?.Severity,
            "a sync that only saw the writer's previous error changed the background error: " + db.BackgroundError);

        gate.Set();
        Assert.IsTrue(listener.Ended.Wait(Timeout), "recovery did not end");
        Assert.IsNull(listener.Info!.NewBackgroundError, listener.Info.NewBackgroundError?.ToString());
        Assert.IsNull(db.BackgroundError);
        db.Put(new WriteOptions { Sync = true }, B("c"), B("3"));
        Assert.AreEqual("1", S(db.Get(B("a"))));
        Assert.AreEqual("3", S(db.Get(B("c"))));
    }
}
