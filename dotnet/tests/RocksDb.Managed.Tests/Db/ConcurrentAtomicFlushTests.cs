using System.Collections.Concurrent;
using System.Text;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>
/// Several atomic flushes at once (atomic_flush): each picks the memtables of every column family that no other flush
/// picked, writes its tables, then waits until the older atomic flushes of those column families are installed
/// (DBImpl::AtomicFlushMemTablesToOutputFiles, atomic_flush_install_cv_) and commits all its results as one MANIFEST atomic
/// group. Scenarios after db/db_flush_test.cc: DBAtomicFlushTest.BgThreadNoWaitAfterManifestError,
/// DBFlushTest.DBStuckAfterAtomicFlushError, DBAtomicFlushTest.AtomicFlushRollbackSomeJobs and TriggerFlushAndClose.
/// </summary>
[TestClass]
public sealed class ConcurrentAtomicFlushTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);
    private static readonly WriteOptions Sync = new() { Sync = true };
    private static readonly string[] CfNames = ["a", "b"];

    private sealed class Listener : EventListener
    {
        public readonly ConcurrentQueue<(string Cf, ulong File)> Completed = new();
        public readonly ManualResetEventSlim Recovered = new(false);

        public override void OnFlushCompleted(RocksDatabase? db, FlushJobInfo info) => Completed.Enqueue((info.ColumnFamilyName, info.FileNumber));

        public override void OnErrorRecoveryEnd(BackgroundErrorRecoveryInfo info)
        {
            if (info.NewBackgroundError is null) Recovered.Set();
        }
    }

    /// <summary>
    /// The first atomic flush blocks right after it picked its memtables until <see cref="Release"/>;
    /// <see cref="WaitForSecondToWait"/> returns once another atomic flush wrote its tables and waits to be installed.
    /// </summary>
    private sealed class TwoAtomicFlushes : IDisposable
    {
        private readonly ManualResetEventSlim _firstPicked = new(false);
        private readonly ManualResetEventSlim _gate = new(false);
        private readonly ManualResetEventSlim _secondWaiting = new(false);
        private readonly ManualResetEventSlim _secondFinished = new(false);
        private int _picks;
        private int _finishes;

        public TwoAtomicFlushes()
        {
            TestHooks.Set("DBImpl::AtomicFlushMemTablesToOutputFiles:Picked", () =>
            {
                if (Interlocked.Increment(ref _picks) != 1) return;
                _firstPicked.Set();
                _gate.Wait(Timeout);
            });
            // Only a flush that is not the oldest waits; while the first one is held that is the second one.
            TestHooks.Set("DBImpl::AtomicFlushMemTablesToOutputFiles:WaitCV", () =>
            {
                if (!_gate.IsSet) _secondWaiting.Set();
            });
            TestHooks.Set("DBImpl::BackgroundCallFlush:FlushFinish:0", () =>
            {
                if (Interlocked.Increment(ref _finishes) == 1 && !_gate.IsSet) _secondFinished.Set();
            });
        }

        public void WaitForFirstToPick() => Assert.IsTrue(_firstPicked.Wait(Timeout), "the first atomic flush did not start");

        public void WaitForSecondToWait() => Assert.IsTrue(_secondWaiting.Wait(Timeout), "the second atomic flush did not wait for the first one");

        public void WaitForSecondToFinish() => Assert.IsTrue(_secondFinished.Wait(Timeout), "the second atomic flush did not end while the first one ran");

        public bool SecondFinished => _secondFinished.IsSet;

        public void Release() => _gate.Set();

        public void Dispose()
        {
            _gate.Set();
            TestHooks.ClearAll();
        }
    }

    private static DbOptions Options(IFileSystem fs, Listener? listener = null)
    {
        DbOptions o = NewOptions(fs);
        o.CreateMissingColumnFamilies = true;
        o.AtomicFlush = true;
        o.MaxBackgroundFlushes = 4;
        if (listener is not null) o.Listeners.Add(listener);
        return o;
    }

    private static ColumnFamilyDescriptor[] Cfs() =>
    [
        new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, new ColumnFamilyOptions()),
        new ColumnFamilyDescriptor(CfNames[0], new ColumnFamilyOptions()),
        new ColumnFamilyDescriptor(CfNames[1], new ColumnFamilyOptions()),
    ];

    private static RocksDatabase Open(IFileSystem fs, Listener? listener = null) => RocksDatabase.Open(DbPath, Options(fs, listener), Cfs());

    private static ColumnFamily[] All(RocksDatabase db) => [db.DefaultColumnFamily, db.GetColumnFamily(CfNames[0]), db.GetColumnFamily(CfNames[1])];

    /// <summary>One write batch that spans every column family: an atomic flush persists all of it or none of it.</summary>
    private static void PutAll(RocksDatabase db, string key, string value)
    {
        using WriteBatch batch = db.CreateWriteBatch();
        foreach (ColumnFamily cf in All(db)) batch.Put(cf, B(key), B(value));
        db.Write(Sync, batch);
    }

    private static void AssertAll(RocksDatabase db, string key, string? expected)
    {
        foreach (ColumnFamily cf in All(db)) Assert.AreEqual(expected, GetString(cf, key), cf.Name + ": " + key);
    }

    private static void AssertProperty(RocksDatabase db, string property, string expected)
    {
        foreach (ColumnFamily cf in All(db)) Assert.AreEqual(expected, cf.GetProperty(property), cf.Name + ": " + property);
    }

    private static int TableFiles(IFileSystem fs)
    {
        int n = 0;
        foreach (string c in fs.GetChildren(DbPath))
        {
            if (FileNames.TryParseFileName(c, out _, out FileType t) && t == FileType.TableFile) n++;
        }
        return n;
    }

    private static readonly FlushOptions NoWait = new() { Wait = false };

    /// <summary>
    /// Starts the first atomic flush (v1 in every column family) and, while it is held after picking, a second one (v2)
    /// that writes its tables and waits for the first to be installed.
    /// </summary>
    private static void StartTwoAtomicFlushes(RocksDatabase db, TwoAtomicFlushes flushes)
    {
        PutAll(db, "k", "v1");
        PutAll(db, "only1", "x");
        db.Flush(NoWait);
        flushes.WaitForFirstToPick();
        PutAll(db, "k", "v2");
        PutAll(db, "only2", "y");
        db.Flush(NoWait);
        flushes.WaitForSecondToWait();
    }

    /// <summary>
    /// Two atomic flushes of three column families run at once and the older one finishes last: the newer one waits for
    /// it (nothing installed meanwhile), then both are installed in memtable order, oldest first.
    /// </summary>
    [TestMethod]
    public void NewerAtomicFlushWaitsForOlderOneAndBothInstallInOrder()
    {
        MemoryFileSystem fs = NewFs();
        var listener = new Listener();
        using (var flushes = new TwoAtomicFlushes())
        using (RocksDatabase db = Open(fs, listener))
        {
            StartTwoAtomicFlushes(db, flushes);
            Assert.AreEqual("2", db.GetProperty(DbProperties.NumRunningFlushes), "both atomic flushes run at once");
            AssertProperty(db, "rocksdb.num-files-at-level0", "0");
            AssertProperty(db, "rocksdb.num-immutable-mem-table", "2");
            Assert.IsEmpty(listener.Completed);
            AssertAll(db, "k", "v2");
            flushes.Release();
            db.Flush(); // nothing to switch: waits for every sealed memtable
            AssertProperty(db, "rocksdb.num-files-at-level0", "2");
            AssertProperty(db, "rocksdb.num-immutable-mem-table", "0");
            (string Cf, ulong File)[] completed = listener.Completed.ToArray();
            Assert.HasCount(6, completed);
            // The older flush picked (and numbered) its files first, and is installed (and reported) first.
            ulong olderMax = 0, newerMin = ulong.MaxValue;
            for (int i = 0; i < 3; i++) olderMax = Math.Max(olderMax, completed[i].File);
            for (int i = 3; i < 6; i++) newerMin = Math.Min(newerMin, completed[i].File);
            Assert.IsLessThan(newerMin, olderMax, "installed in memtable order");
            AssertAll(db, "k", "v2");
        }
        using (RocksDatabase db = Open(fs))
        {
            AssertProperty(db, "rocksdb.num-files-at-level0", "2");
            AssertAll(db, "k", "v2");
            AssertAll(db, "only1", "x");
            AssertAll(db, "only2", "y");
            db.CompactRange(null, null);
            AssertAll(db, "k", "v2");
        }
    }

    /// <summary>
    /// DBAtomicFlushTest.AtomicFlushRollbackSomeJobs / DBFlushTest.DBStuckAfterAtomicFlushError: the older atomic flush
    /// fails writing a table (out of space: a hard error, recovered automatically). It rolls back the memtables of every
    /// column family, and the newer flush, waiting for it, gives up once the error stopped background work and rolls back
    /// too (nothing of either is installed, their tables are deleted). The recovery flush then picks all the memtables and
    /// writes one table per column family.
    /// </summary>
    [TestMethod]
    public void FailedOlderAtomicFlushRollsBackAndTheNewerOneAborts()
    {
        var fs = new FaultInjectionFileSystem(NewFs());
        var listener = new Listener();
        using (var flushes = new TwoAtomicFlushes())
        using (RocksDatabase db = Open(fs, listener))
        {
            StartTwoAtomicFlushes(db, flushes);
            Assert.AreEqual(3, TableFiles(fs), "the newer flush's tables");
            // The next table write is the older flush's.
            fs.FailMatching(".sst", FaultOps.Append, 1, InjectedFaultKind.NoSpace);
            flushes.Release();
            Assert.IsTrue(listener.Recovered.Wait(Timeout), "the recovery did not succeed");
            db.Impl.WaitForBackgroundWorkForTest();
            Assert.AreEqual(1, fs.TargetedFaults);
            // Neither flush installed anything: the recovery flush wrote both memtables of each column family into one
            // table, and the tables of the rolled back flushes were deleted.
            Assert.AreEqual(3, TableFiles(fs));
            AssertProperty(db, "rocksdb.num-files-at-level0", "1");
            AssertProperty(db, "rocksdb.num-immutable-mem-table", "0");
            AssertAll(db, "k", "v2");
            PutAll(db, "after", "z");
            db.Flush();
            AssertProperty(db, "rocksdb.num-files-at-level0", "2");
        }
        using (RocksDatabase db = Open(fs))
        {
            AssertAll(db, "k", "v2");
            AssertAll(db, "only1", "x");
            AssertAll(db, "only2", "y");
            AssertAll(db, "after", "z");
        }
    }

    /// <summary>
    /// DBFlushTest.DBStuckAfterAtomicFlushError with automatic recovery: the older atomic flush fails with a retryable
    /// error (a soft error that stops background work), the newer one aborts, and the automatic recovery flush installs
    /// all the memtables of both in one table per column family.
    /// </summary>
    [TestMethod]
    public void RetryableFailureOfOlderAtomicFlushIsRecoveredWithOneFlush()
    {
        var fs = new FaultInjectionFileSystem(NewFs());
        var listener = new Listener();
        using var flushes = new TwoAtomicFlushes();
        DbOptions o = Options(fs, listener);
        o.BgErrorResumeRetryInterval = 1000;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o, Cfs()))
        {
            StartTwoAtomicFlushes(db, flushes);
            fs.FailMatching(".sst", FaultOps.Append, 1, InjectedFaultKind.Retryable);
            flushes.Release();
            Assert.IsTrue(listener.Recovered.Wait(Timeout), "the automatic recovery did not succeed");
            db.Impl.WaitForBackgroundWorkForTest();
            Assert.AreEqual(1, fs.TargetedFaults);
            AssertProperty(db, "rocksdb.num-files-at-level0", "1");
            AssertProperty(db, "rocksdb.num-immutable-mem-table", "0");
            Assert.AreEqual(3, TableFiles(fs));
            AssertAll(db, "k", "v2");
        }
        using (RocksDatabase db = Open(fs))
        {
            AssertProperty(db, "rocksdb.num-files-at-level0", "1");
            AssertAll(db, "k", "v2");
            AssertAll(db, "only1", "x");
            AssertAll(db, "only2", "y");
        }
    }

    /// <summary>
    /// DBAtomicFlushTest.BgThreadNoWaitAfterManifestError: the older atomic flush fails writing the MANIFEST while the
    /// newer one waits for it; the newer one sees the MANIFEST error (versions_->io_status()) and stops waiting. Neither
    /// is installed, and the recovery (of the out-of-space hard error) installs everything with one table per column family.
    /// </summary>
    [TestMethod]
    public void NewerAtomicFlushStopsWaitingAfterManifestError()
    {
        var fs = new FaultInjectionFileSystem(NewFs());
        var listener = new Listener();
        using (var flushes = new TwoAtomicFlushes())
        using (RocksDatabase db = Open(fs, listener))
        {
            StartTwoAtomicFlushes(db, flushes);
            fs.FailMatching("MANIFEST", FaultOps.Append, 1, InjectedFaultKind.NoSpace);
            flushes.Release();
            Assert.IsTrue(listener.Recovered.Wait(Timeout), "the recovery did not succeed");
            db.Impl.WaitForBackgroundWorkForTest();
            Assert.AreEqual(1, fs.TargetedFaults);
            AssertProperty(db, "rocksdb.num-files-at-level0", "1");
            AssertProperty(db, "rocksdb.num-immutable-mem-table", "0");
            AssertAll(db, "k", "v2");
        }
        using (RocksDatabase db = Open(fs))
        {
            AssertProperty(db, "rocksdb.num-files-at-level0", "1");
            AssertAll(db, "k", "v2");
            AssertAll(db, "only1", "x");
            AssertAll(db, "only2", "y");
        }
    }

    /// <summary>
    /// A crash (power loss) while the newer atomic flush waits for the older one: nothing of either was installed, so
    /// recovery replays every memtable from the WALs, and every column family holds every write batch.
    /// </summary>
    [TestMethod]
    public void CrashWhileNewerAtomicFlushWaitsRecoversEveryColumnFamilyFromTheWal()
    {
        var fs = new FaultInjectionFileSystem(NewFs());
        RocksDatabase db = Open(fs);
        using (var flushes = new TwoAtomicFlushes())
        {
            StartTwoAtomicFlushes(db, flushes);
            // Power loss: every mutating operation from now on fails, the older flush included.
            fs.CrashAtOperation = fs.Operations + 1;
            flushes.Release();
            try { db.Dispose(); }
            catch (Exception e) when (e is RocksDbException or IOException) { }
        }
        fs.PowerLoss();
        fs.Restart();
        using (RocksDatabase reopened = Open(fs))
        {
            AssertAll(reopened, "k", "v2");
            AssertAll(reopened, "only1", "x");
            AssertAll(reopened, "only2", "y");
            reopened.Flush();
            AssertAll(reopened, "k", "v2");
        }
        using (RocksDatabase reopened = Open(fs))
        {
            AssertAll(reopened, "k", "v2");
            AssertAll(reopened, "only1", "x");
            AssertAll(reopened, "only2", "y");
        }
    }

    /// <summary>
    /// Shutdown while the newer atomic flush waits for its install (wait_to_install_func: "shutting_down_ ->
    /// ShutdownInProgress"): it gives up at once and rolls back, Close waits only for the older flush, which then gives
    /// up too; nothing hangs and the WAL holds every write.
    /// </summary>
    [TestMethod]
    public void ShutdownWhileWaitingToInstallDoesNotHang()
    {
        MemoryFileSystem fs = NewFs();
        using (var flushes = new TwoAtomicFlushes())
        {
            RocksDatabase db = Open(fs);
            StartTwoAtomicFlushes(db, flushes);
            Task close = Task.Run(db.Dispose);
            // The newer flush ends while the older one is still held: the shutdown woke it.
            flushes.WaitForSecondToFinish();
            Assert.IsFalse(close.IsCompleted, "Close waits for the running older flush");
            flushes.Release();
            Assert.IsTrue(close.Wait(Timeout), "Close hung");
        }
        using (RocksDatabase db = Open(fs))
        {
            AssertAll(db, "k", "v2");
            AssertAll(db, "only1", "x");
            AssertAll(db, "only2", "y");
        }
    }

    /// <summary>
    /// Many writers of batches that span every column family, with frequent atomic flushes and four flush slots: every
    /// batch survives, in every column family, in memory and after a reopen.
    /// </summary>
    [TestMethod]
    public void ConcurrentAtomicFlushesUnderLoadKeepEveryBatchInEveryColumnFamily()
    {
        MemoryFileSystem fs = NewFs();
        const int Writers = 4;
        const int PerWriter = 1500;
        using (RocksDatabase db = Open(fs))
        {
            var tasks = new Task[Writers];
            for (int w = 0; w < Writers; w++)
            {
                int writer = w;
                tasks[w] = Task.Run(() =>
                {
                    for (int i = 0; i < PerWriter; i++)
                    {
                        PutAll(db, Encoding.UTF8.GetString(Key(writer * PerWriter + i)), "v" + i.ToString(System.Globalization.CultureInfo.InvariantCulture));
                        if (i % 100 == 0) db.Flush(NoWait);
                    }
                });
            }
            Task.WaitAll(tasks);
            db.Flush();
            AssertProperty(db, "rocksdb.num-immutable-mem-table", "0");
        }
        using (RocksDatabase db = Open(fs))
        {
            for (int i = 0; i < Writers * PerWriter; i++)
            {
                AssertAll(db, Encoding.UTF8.GetString(Key(i)), "v" + (i % PerWriter).ToString(System.Globalization.CultureInfo.InvariantCulture));
            }
        }
    }
}
