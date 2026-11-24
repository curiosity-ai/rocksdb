using RocksDb.Managed.Db;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #14, WAL lifecycle: LockWAL against memtable / WAL switches and manual flushes, UnlockWAL against stalled
/// leaders, SwitchWAL with empty column families, closed WALs while file deletions are disabled (docs/validation.md
/// "Audit #14").
/// </summary>
[TestClass]
[DoNotParallelize] // TestHooks are process-wide
public sealed class Audit14WalTests
{
    private static readonly WriteOptions s_wo = new();

    private static void Step(string name, Action action)
    {
        Task t = Task.Factory.StartNew(action, TaskCreationOptions.LongRunning);
        Assert.IsTrue(t.Wait(20_000), "hung in: " + name);
    }

    private static RocksDatabase OpenSmallBuffer(string path)
    {
        var cfo = new ColumnFamilyOptions { WriteBufferSize = 64 * 1024 };
        return RocksDatabase.Open(path, NewOptions(NewFs()), [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, cfo)]);
    }

    [TestMethod]
    [Timeout(60_000, CooperativeCancellation = true)]
    public void WriteDoesNotSwitchTheWalWhileItIsLocked()
    {
        // DBImpl::WaitForPendingWrites ends with "Wait for any LockWAL to clear", and it precedes every memtable switch
        // of PreprocessWrite. The port switched first and stalled afterwards (DelayWrite): a write that found a full
        // memtable rotated the WAL under the lock, and the flush then let the locked WAL be purged.
        using RocksDatabase db = OpenSmallBuffer("/audit14wal-switch");
        db.Put(B("big"), new byte[100 * 1024]); // fills the memtable: the next write switches it
        WalLock walLock = db.LockWal();
        ulong lockedWal = db.Impl.GetCurrentWalFile().LogNumber;
        using var blocked = new ManualResetEventSlim();
        TestHooks.Set("DBImpl::WaitForPendingWrites:LockWal", blocked.Set);
        TestHooks.Set("DBImpl::DelayWrite:Wait", blocked.Set);
        try
        {
            // A no_slowdown write does not wait for the lock (and does not switch either).
            var e = Assert.ThrowsExactly<RocksDbException>(() => db.Put(new WriteOptions { NoSlowdown = true }, B("n"), B("v")));
            Assert.AreEqual(StatusCode.Incomplete, e.Code, e.Message);
            Assert.AreEqual(lockedWal, db.Impl.GetCurrentWalFile().LogNumber);
            Task put = Task.Factory.StartNew(() => db.Put(B("k"), B("v")), TaskCreationOptions.LongRunning);
            Assert.IsTrue(blocked.Wait(30_000), "the write did not block on the WAL lock");
            Assert.IsFalse(put.Wait(200));
            Assert.AreEqual(lockedWal, db.Impl.GetCurrentWalFile().LogNumber, "the WAL was switched while it was locked");
            Assert.IsNull(GetString(db, "k"));
            walLock.Dispose();
            Assert.IsTrue(put.Wait(30_000));
        }
        finally
        {
            walLock.Dispose();
            TestHooks.ClearAll();
        }
        Assert.AreEqual("v", GetString(db, "k"));
        Assert.IsTrue(db.Impl.GetCurrentWalFile().LogNumber > lockedWal, "the switch happens once the WAL is unlocked");
    }

    [TestMethod]
    [Timeout(60_000, CooperativeCancellation = true)]
    public void ManualFlushRespectsTheWalLock()
    {
        // DBImpl::FlushMemTable: with FlushOptions::wait a flush from the thread that holds LockWAL fails with Aborted
        // ("Likely deadlock"), from another thread it waits for UnlockWAL (WaitForPendingWrites); without wait it fails
        // with TryAgain while writes are stopped. The port flushed and rotated the WAL under the lock.
        using RocksDatabase db = RocksDatabase.Open("/audit14wal-flush", NewOptions(NewFs()));
        db.Put(B("a"), B("1"));
        WalLock walLock = db.LockWal();
        try
        {
            ulong lockedWal = db.Impl.GetCurrentWalFile().LogNumber;
            var e = Assert.ThrowsExactly<RocksDbException>(() => db.Flush());
            Assert.AreEqual(StatusCode.Aborted, e.Code, e.Message);
            e = Assert.ThrowsExactly<RocksDbException>(() => db.DefaultColumnFamily.Flush(new FlushOptions { Wait = false }));
            Assert.AreEqual(StatusCode.TryAgain, e.Code, e.Message);

            // Allowed under the lock, as upstream: FlushWAL and creating a column family.
            db.FlushWal(sync: false);
            ColumnFamily cf = db.CreateColumnFamily("cf", new ColumnFamilyOptions());
            Assert.IsNotNull(cf);

            Task flush = Task.Factory.StartNew(() => db.Flush(), TaskCreationOptions.LongRunning);
            Assert.IsFalse(flush.Wait(300), "a flush from another thread completed while the WAL was locked");
            Assert.AreEqual(lockedWal, db.Impl.GetCurrentWalFile().LogNumber);
            db.FlushWal(sync: false); // still possible while the flush waits in the write queue
            walLock.Dispose();
            Assert.IsTrue(flush.Wait(30_000));
            Assert.IsTrue(db.Impl.GetCurrentWalFile().LogNumber > lockedWal);
        }
        finally
        {
            walLock.Dispose();
        }
        db.Flush(); // the thread no longer owns a lock
        Assert.AreEqual("1", GetString(db, "a"));
    }

    [TestMethod]
    [Timeout(120_000, CooperativeCancellation = true)]
    public void NoSlowdownWriteSucceedsRightAfterUnlockWal()
    {
        // DBImpl::UnlockWAL waits (GetBegunCountOfOutstandingStall / WaitForStallEndedCount) until the leaders stalled
        // by the lock have called EndWriteStall, "so that no_slowdown writes after UnlockWAL() are guaranteed to
        // succeed if there's no other source of stall". The port returned at once: the stall barrier of a leader that
        // had only been made runnable failed the next no_slowdown write with Incomplete "Write stall".
        using RocksDatabase db = RocksDatabase.Open("/audit14wal-unlock", NewOptions(NewFs()));
        var noSlowdown = new WriteOptions { NoSlowdown = true };
        using var stalled = new AutoResetEvent(false);
        TestHooks.Set("DBImpl::DelayWrite:Wait", () => stalled.Set());
        try
        {
            for (int round = 0; round < 300; round++)
            {
                WalLock walLock = db.LockWal();
                Task put = Task.Factory.StartNew(() => db.Put(B("stalled"), B("v")), TaskCreationOptions.LongRunning);
                Assert.IsTrue(stalled.WaitOne(30_000));
                walLock.Dispose();
                db.Put(noSlowdown, B("fast"), B(round.ToString())); // must not see the stalled leader's barrier
                Assert.IsTrue(put.Wait(30_000));
            }
        }
        finally
        {
            TestHooks.ClearAll();
        }
    }

    [TestMethod]
    [Timeout(60_000, CooperativeCancellation = true)]
    public void SwitchWalReleasesAWalHeldOnlyByAnEmptyColumnFamily()
    {
        // DBImpl::SwitchWAL switches every column family whose log number is at or below the oldest alive WAL, empty
        // or not: the flush of the empty memtable persists its new log number, which releases the WAL. The port skipped
        // empty memtables, so an idle column family with a stale log number kept the oldest WAL alive (marked as
        // "getting flushed") and max_total_wal_size did not bound the WALs.
        using RocksDatabase db = RocksDatabase.Open("/audit14wal-empty", NewOptions(NewFs()));
        ColumnFamily a = db.DefaultColumnFamily;
        // B keeps a sealed memtable unflushed (min_write_buffer_number_to_merge = 2) while A switches to a newer WAL.
        ColumnFamily b = db.CreateColumnFamily("b", new ColumnFamilyOptions { WriteBufferSize = 64 * 1024, MaxWriteBufferNumber = 4, MinWriteBufferNumberToMerge = 2 });
        try
        {
            b.Put(B("b1"), new byte[100 * 1024]); // fills B's memtable
            a.Put(B("a1"), B("v"));               // this write switches B (new WAL); B's sealed memtable is not flushed
            ulong staleWal = db.Impl.GetCurrentWalFile().LogNumber;
            a.Put(B("a2"), B("v"));
            Step("flush of a", () => a.Flush()); // A moves to a newer WAL; B, with a sealed memtable, keeps its log number
            Step("flush of b", () => b.Flush()); // nothing to switch: B commits the stale log number and is now empty and idle
            Step("background work", db.Impl.WaitForBackgroundWorkForTest);
            a.Put(B("a3"), new byte[4096]);
            Assert.IsTrue(db.Impl.GetSortedWalFiles(includeArchived: false, needSeqnos: false).Exists(w => w.LogNumber == staleWal),
                "setup: the stale WAL should still be alive");

            Step("SetDBOptions", () => db.SetDBOptions([new KeyValuePair<string, string>("max_total_wal_size", "1024")]));
            Step("puts", () =>
            {
                a.Put(B("a4"), B("v")); // PreprocessWrite: the WALs exceed the limit
                a.Put(B("a5"), B("v"));
            });
            // No flush of A here: its switch would advance the empty B's log number and hide the problem.
            Step("flush of the empty b", () => b.Flush(new FlushOptions { Wait = true }));
            Step("background work", db.Impl.WaitForBackgroundWorkForTest);
            Assert.IsFalse(db.Impl.GetSortedWalFiles(includeArchived: false, needSeqnos: false).Exists(w => w.LogNumber <= staleWal),
                "the WAL only an empty column family still named was not released");
        }
        finally
        {
            TestHooks.ClearAll();
        }
        Assert.AreEqual(100 * 1024, b.Get(B("b1"))!.Length);
        Assert.AreEqual("v", GetString(a, "a2"));
    }

    [TestMethod]
    public void ClosedWalStaysOpenForCheckpointsWhileFileDeletionsAreDisabled()
    {
        // FindObsoleteFiles returns at once while file deletions are disabled, so logs_ keeps a closed WAL and
        // GetOpenWalSizes reports it: a checkpoint copies and syncs it. The port released closed WALs whose data had
        // been flushed whatever the setting, so a no-flush checkpoint hard-linked a WAL that was never synced.
        using RocksDatabase db = RocksDatabase.Open("/audit14wal-closed", NewOptions(NewFs()));
        DbImpl impl = db.Impl;
        db.Put(B("a"), B("1"));
        ulong firstWal = impl.GetCurrentWalFile().LogNumber;
        impl.DisableFileDeletions();
        try
        {
            db.Flush(); // closes the first WAL (not synced) and makes its data obsolete
            impl.WaitForBackgroundWorkForTest();
            db.Put(B("b"), B("2"));
            db.Flush(); // the next switch is where obsolete closed WALs are released
            impl.WaitForBackgroundWorkForTest();
            var openWals = new HashSet<ulong>();
            RocksDb.Managed.Util.SyncValueTask.GetResult(impl.FlushWalAsync(sync: false, synchronous: true, openWals, CancellationToken.None));
            Assert.IsTrue(openWals.Contains(firstWal), "a closed, unsynced WAL was released while file deletions were disabled");
        }
        finally
        {
            impl.EnableFileDeletions(force: false);
        }
    }
}
