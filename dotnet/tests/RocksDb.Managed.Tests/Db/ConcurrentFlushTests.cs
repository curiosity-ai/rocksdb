using System.Text;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>
/// Several flushes of one column family at once (DBImpl::MaybeScheduleFlushOrCompaction schedules a flush per pending
/// request, PickMemtablesToFlush skips the memtables another flush picked) and the in-order commit of their results
/// (MemTableList::TryInstallMemtableFlushResults: a flush that completes before an older one leaves its result for the
/// older one to commit; RollbackMemtableFlush rolls the later completed flushes back with a failed older one). Scenarios
/// after db/db_flush_test.cc (FlushOrderingTest-style parallel flushes) and db/memtable_list_test.cc.
/// </summary>
[TestClass]
public sealed class ConcurrentFlushTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);
    private static readonly WriteOptions Sync = new() { Sync = true };

    private sealed class FlushListener : EventListener
    {
        public int Completed;

        public override void OnFlushCompleted(RocksDatabase? db, FlushJobInfo info) => Interlocked.Increment(ref Completed);
    }

    /// <summary>
    /// The first flush job blocks at its start until <see cref="Release"/>; <see cref="WaitForSecondToFinish"/> returns
    /// once a second flush job has finished.
    /// </summary>
    private sealed class TwoFlushes : IDisposable
    {
        private readonly ManualResetEventSlim _firstStarted = new(false);
        private readonly ManualResetEventSlim _gate = new(false);
        private readonly ManualResetEventSlim _secondFinished = new(false);
        private int _starts;
        private int _finishes;

        public TwoFlushes()
        {
            TestHooks.Set("DBImpl::FlushMemTables:Start", () =>
            {
                if (Interlocked.Increment(ref _starts) != 1) return;
                _firstStarted.Set();
                _gate.Wait(Timeout);
            });
            TestHooks.Set("DBImpl::FlushMemTableToOutputFile:Finish", () =>
            {
                // The first job to finish while the first one is held is the second one.
                if (Interlocked.Increment(ref _finishes) == 1 && !_gate.IsSet) _secondFinished.Set();
            });
        }

        public void WaitForFirstToStart() => Assert.IsTrue(_firstStarted.Wait(Timeout), "the first flush did not start");

        public void WaitForSecondToFinish() => Assert.IsTrue(_secondFinished.Wait(Timeout), "the second flush did not finish while the first ran");

        public void Release() => _gate.Set();

        public void Dispose()
        {
            _gate.Set();
            TestHooks.ClearAll();
        }
    }

    private static DbOptions Options(IFileSystem fs, FlushListener? listener = null)
    {
        DbOptions o = NewOptions(fs);
        o.MaxBackgroundFlushes = 2;
        if (listener is not null) o.Listeners.Add(listener);
        return o;
    }

    private static string L0Files(RocksDatabase db) => db.GetProperty("rocksdb.num-files-at-level0")!;

    private static int TableFiles(IFileSystem fs)
    {
        int n = 0;
        foreach (string c in fs.GetChildren(DbPath))
        {
            if (FileNames.TryParseFileName(c, out _, out FileType t) && t == FileType.TableFile) n++;
        }
        return n;
    }

    /// <summary>
    /// Two flush jobs of the default column family run at once and the older one finishes last: the newer result waits
    /// for it, both are committed in memtable order, and the newer value wins (in memory, after the commit and after a
    /// reopen).
    /// </summary>
    [TestMethod]
    public void NewerFlushWaitsForOlderOneAndBothCommitInOrder()
    {
        MemoryFileSystem fs = NewFs();
        var listener = new FlushListener();
        using (var flushes = new TwoFlushes())
        using (RocksDatabase db = RocksDatabase.Open(DbPath, Options(fs, listener)))
        {
            db.Put(Sync, B("k1"), B("v1"));
            db.Flush(new FlushOptions { Wait = false });
            flushes.WaitForFirstToStart();
            db.Put(Sync, B("k1"), B("v1b"));
            db.Put(Sync, B("k2"), B("v2"));
            db.Flush(new FlushOptions { Wait = false });
            flushes.WaitForSecondToFinish();
            // The newer flush wrote its table but cannot be committed before the older memtable.
            Assert.AreEqual("0", L0Files(db));
            Assert.AreEqual(2UL, db.GetIntProperty(DbProperties.NumImmutableMemTable));
            Assert.AreEqual(0, Volatile.Read(ref listener.Completed));
            Assert.AreEqual("v1b", GetString(db, "k1"));
            flushes.Release();
            db.Flush(); // nothing to switch: waits for both sealed memtables
            Assert.AreEqual("2", L0Files(db));
            Assert.AreEqual(0UL, db.GetIntProperty(DbProperties.NumImmutableMemTable));
            Assert.AreEqual(2, Volatile.Read(ref listener.Completed));
            Assert.AreEqual("v1b", GetString(db, "k1"));
            Assert.AreEqual("v2", GetString(db, "k2"));
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, Options(fs)))
        {
            Assert.AreEqual("2", L0Files(db));
            Assert.AreEqual("v1b", GetString(db, "k1"));
            Assert.AreEqual("v2", GetString(db, "k2"));
            db.CompactRange(null, null);
            Assert.AreEqual("v1b", GetString(db, "k1"));
        }
    }

    /// <summary>
    /// A crash while the newer flush result waits for the older flush: nothing of either was committed, so recovery
    /// replays both memtables from the WALs (which the uncommitted flushes did not make obsolete).
    /// </summary>
    [TestMethod]
    public void CrashWhileNewerFlushWaitsRecoversFromTheWal()
    {
        var fs = new FaultInjectionFileSystem(NewFs());
        RocksDatabase db = RocksDatabase.Open(DbPath, Options(fs));
        using (var flushes = new TwoFlushes())
        {
            db.Put(Sync, B("k1"), B("v1"));
            db.Flush(new FlushOptions { Wait = false });
            flushes.WaitForFirstToStart();
            db.Put(Sync, B("k1"), B("v1b"));
            db.Put(Sync, B("k2"), B("v2"));
            db.Flush(new FlushOptions { Wait = false });
            flushes.WaitForSecondToFinish();
            // Power loss: every mutating operation from now on fails, the older flush included.
            fs.CrashAtOperation = fs.Operations + 1;
            flushes.Release();
            try { db.Dispose(); }
            catch (Exception e) when (e is RocksDbException or IOException) { }
        }
        fs.PowerLoss();
        fs.Restart();
        using (RocksDatabase reopened = RocksDatabase.Open(DbPath, Options(fs)))
        {
            Assert.AreEqual("v1b", GetString(reopened, "k1"));
            Assert.AreEqual("v2", GetString(reopened, "k2"));
            reopened.Flush();
            Assert.AreEqual("v1b", GetString(reopened, "k1"));
        }
        using (RocksDatabase reopened = RocksDatabase.Open(DbPath, Options(fs)))
        {
            Assert.AreEqual("v1b", GetString(reopened, "k1"));
            Assert.AreEqual("v2", GetString(reopened, "k2"));
        }
    }

    /// <summary>
    /// The older flush fails after the newer one completed: RollbackMemtableFlush(rollback_succeeding_memtables) rolls
    /// the newer result back too (it could only be committed after the older memtable), its table is deleted, and the
    /// retry flushes both memtables into one table.
    /// </summary>
    [TestMethod]
    public void FailedOlderFlushRollsBackTheCompletedNewerOne()
    {
        var fs = new FaultInjectionFileSystem(NewFs());
        DbOptions o = Options(fs);
        o.ParanoidChecks = false; // the failed flush is retried, background work does not stop
        using var flushes = new TwoFlushes();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o))
        {
            db.Impl.FlushRetryDelayForTest = static _ => Task.CompletedTask;
            db.Put(Sync, B("k1"), B("v1"));
            db.Flush(new FlushOptions { Wait = false });
            flushes.WaitForFirstToStart();
            db.Put(Sync, B("k1"), B("v1b"));
            db.Put(Sync, B("k2"), B("v2"));
            db.Flush(new FlushOptions { Wait = false });
            flushes.WaitForSecondToFinish();
            Assert.AreEqual(1, TableFiles(fs), "the newer flush's table");
            // The next table write is the older flush's.
            fs.FailMatching(".sst", FaultOps.Append, 1, InjectedFaultKind.IOError);
            flushes.Release();
            db.Flush(); // waits until both memtables are committed (by the retry)
            Assert.AreEqual(1, fs.TargetedFaults);
            Assert.AreEqual("1", L0Files(db), "the retry flushes both memtables into one table");
            Assert.AreEqual(1, TableFiles(fs), "the rolled back table was deleted");
            Assert.AreEqual(0UL, db.GetIntProperty(DbProperties.NumImmutableMemTable));
            Assert.AreEqual("v1b", GetString(db, "k1"));
            Assert.AreEqual("v2", GetString(db, "k2"));
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, Options(fs)))
        {
            Assert.AreEqual("v1b", GetString(db, "k1"));
            Assert.AreEqual("v2", GetString(db, "k2"));
        }
    }

    /// <summary>Many writers and frequent flushes of one column family with two flush slots: no write is lost.</summary>
    [TestMethod]
    public void ConcurrentFlushesUnderLoadKeepEveryWrite()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = Options(fs);
        o.MaxBackgroundFlushes = 4;
        var cfo = new ColumnFamilyOptions { WriteBufferSize = 64 << 10, MaxWriteBufferNumber = 16, Level0FileNumCompactionTrigger = 1000,
            Level0SlowdownWritesTrigger = 1000, Level0StopWritesTrigger = 1000 };
        const int Writers = 4;
        const int PerWriter = 3000;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o, [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, cfo)]))
        {
            var tasks = new Task[Writers];
            for (int w = 0; w < Writers; w++)
            {
                int writer = w;
                tasks[w] = Task.Run(() =>
                {
                    for (int i = 0; i < PerWriter; i++)
                    {
                        db.Put(Key(writer * PerWriter + i), Value(i, 0));
                        if (i % 500 == 0) db.Flush(new FlushOptions { Wait = false });
                    }
                });
            }
            Task.WaitAll(tasks);
            db.Flush();
            for (int i = 0; i < Writers * PerWriter; i += 7) Assert.AreEqual(Encoding.UTF8.GetString(Value(i % PerWriter, 0)), GetString(db, Encoding.UTF8.GetString(Key(i))));
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, Options(fs), [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, cfo)]))
        {
            for (int i = 0; i < Writers * PerWriter; i++) Assert.IsNotNull(db.Get(Key(i)), "key " + i);
        }
    }
}
