using System.Collections.Concurrent;
using System.Reflection;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table.Filters;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4 follow-up: listener notifications (DBImpl::NotifyOn*, EventHelpers::LogAndNotifyTableFileCreationFinished)
/// and the perf counters of filter probes against the C++ sources.
/// </summary>
[TestClass]
public sealed class Audit4FollowUpListenersTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);

    private sealed class Recorder : EventListener
    {
        public readonly ConcurrentQueue<(string Event, object Info)> Entries = new();
        public Action<FlushJobInfo>? FlushBegin;
        public Action<CompactionJobInfo>? CompactionBegin;
        public Action<TableFileCreationInfo>? TableFileCreated;

        public override void OnFlushBegin(RocksDatabase? db, FlushJobInfo info)
        {
            Entries.Enqueue(("FlushBegin", info));
            FlushBegin?.Invoke(info);
        }

        public override void OnFlushCompleted(RocksDatabase? db, FlushJobInfo info) => Entries.Enqueue(("FlushCompleted", info));

        public override void OnCompactionBegin(RocksDatabase? db, CompactionJobInfo info)
        {
            Entries.Enqueue(("CompactionBegin", info));
            CompactionBegin?.Invoke(info);
        }

        public override void OnCompactionCompleted(RocksDatabase? db, CompactionJobInfo info) => Entries.Enqueue(("CompactionCompleted", info));

        public override void OnMemTableSealed(MemTableInfo info) => Entries.Enqueue(("MemTableSealed", info));

        public override void OnTableFileCreated(TableFileCreationInfo info)
        {
            Entries.Enqueue(("TableFileCreated", info));
            TableFileCreated?.Invoke(info);
        }

        public List<T> All<T>(string name)
        {
            var list = new List<T>();
            foreach ((string e, object info) in Entries)
            {
                if (e == name) list.Add((T)info);
            }
            return list;
        }

        public int Count(string name) => All<object>(name).Count;
    }

    private static (MemoryFileSystem Fs, DbOptions Options, Recorder Listener) Setup()
    {
        MemoryFileSystem fs = NewFs();
        var l = new Recorder();
        DbOptions o = NewOptions(fs);
        o.AvoidFlushDuringShutdown = true;
        o.Listeners.Add(l);
        return (fs, o, l);
    }

    private static void FillTwoLevel0Files(RocksDatabase db)
    {
        for (int f = 0; f < 2; f++)
        {
            db.Put(B("a"), B("v" + f));
            db.Put(B("z"), B("v" + f));
            db.Flush();
        }
    }

    // builder.cc BuildTable: LogAndNotifyTableFileCreationFinished runs when the table is finished, i.e. before
    // FlushJob::Run installs it (TryInstallMemtableFlushResults), and before OnFlushCompleted.
    [TestMethod]
    public void FlushOutputCreationIsReportedBeforeInstall()
    {
        (_, DbOptions options, Recorder l) = Setup();
        using RocksDatabase db = RocksDatabase.Open(DbPath, options);
        string? level0AtCreation = null;
        l.TableFileCreated = _ => level0AtCreation = db.GetProperty("rocksdb.num-files-at-level0");
        db.Put(B("a"), B("1"));
        db.Flush();
        Assert.IsTrue(SpinWait.SpinUntil(() => l.Count("FlushCompleted") == 1, Timeout));
        Assert.AreEqual("0", level0AtCreation, "the file is not installed yet when its creation is reported");
        TableFileCreationInfo info = l.All<TableFileCreationInfo>("TableFileCreated")[0];
        Assert.IsNull(info.Status);
        Assert.IsTrue(info.FileSize > 0);
        Assert.IsNotNull(info.TableProperties);
        Assert.AreEqual(l.All<FlushJobInfo>("FlushCompleted")[0].FilePath, info.FilePath);
    }

    // builder.cc BuildTable: the creation event is raised for a failed table too, with the failure status
    // (status_for_listener = s); no OnFlushCompleted follows (FlushMemTableToOutputFile: "if (s.ok() ...)").
    [TestMethod]
    public void FailedFlushReportsCreationWithStatus()
    {
        (MemoryFileSystem fs, DbOptions options, Recorder l) = Setup();
        using RocksDatabase db = RocksDatabase.Open(DbPath, options);
        l.FlushBegin = _ => fs.SetFilesystemActive(false);
        db.Put(B("a"), B("1"));
        try
        {
            db.Flush();
        }
        catch (RocksDbException)
        {
        }
        Assert.IsTrue(SpinWait.SpinUntil(() => l.Count("TableFileCreated") == 1, Timeout));
        fs.SetFilesystemActive(true);
        l.FlushBegin = null;
        TableFileCreationInfo info = l.All<TableFileCreationInfo>("TableFileCreated")[0];
        Assert.IsNotNull(info.Status);
        Assert.AreEqual(StatusCode.IOError, info.Status!.Code);
        Assert.AreEqual(TableFileCreationReason.Flush, info.Reason);
        Assert.AreEqual(l.All<FlushJobInfo>("FlushBegin")[0].FilePath, info.FilePath);
        Assert.AreEqual(0, l.Count("FlushCompleted"));
    }

    // DBImpl::WriteLevel0TableForRecovery -> BuildTable(..., TableFileCreationReason::kRecovery): the recovery flush
    // reports its table file; there is no OnFlushBegin / OnFlushCompleted (no NotifyOnFlush* call in recovery).
    [TestMethod]
    public void RecoveryFlushReportsCreationWithRecoveryReason()
    {
        (MemoryFileSystem fs, DbOptions options, Recorder l) = Setup();
        DbOptions first = NewOptions(fs);
        first.AvoidFlushDuringShutdown = true;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, first))
        {
            db.Put(B("a"), B("1"));
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, options))
        {
            List<TableFileCreationInfo> created = l.All<TableFileCreationInfo>("TableFileCreated");
            Assert.AreEqual(1, created.Count);
            Assert.AreEqual(TableFileCreationReason.Recovery, created[0].Reason);
            Assert.IsNull(created[0].Status);
            Assert.IsTrue(created[0].FilePath.EndsWith(".sst", StringComparison.Ordinal));
            Assert.IsTrue(created[0].FileSize > 0);
            Assert.AreEqual(0, l.Count("FlushBegin") + l.Count("FlushCompleted"));
            Assert.AreEqual("1", db.GetProperty("rocksdb.num-files-at-level0"));
        }
    }

    // DBImpl::NotifyOnCompactionCompleted: "if (shutting_down_.load(std::memory_order_acquire)) return;" -- a
    // compaction canceled by CancelAllBackgroundWork does not report OnCompactionCompleted.
    [TestMethod]
    public void NoCompactionCompletedAfterShutdownBegan()
    {
        (_, DbOptions options, Recorder l) = Setup();
        using RocksDatabase db = RocksDatabase.Open(DbPath, options, new ColumnFamilyOptions { DisableAutoCompactions = true });
        FillTwoLevel0Files(db);
        l.CompactionBegin = _ => db.CancelAllBackgroundWork(wait: false);
        try
        {
            db.CompactRange();
        }
        catch (RocksDbException)
        {
        }
        db.CancelAllBackgroundWork(wait: true);
        Assert.AreEqual(1, l.Count("CompactionBegin"));
        Assert.AreEqual(0, l.Count("CompactionCompleted"));
        // NotifyOnFlushBegin / NotifyOnFlushCompleted have the same guard; no flush is scheduled after the shutdown.
        Assert.AreEqual(2, l.Count("FlushCompleted"));
    }

    // DBImpl::NotifyOnCompactionBegin: "if (c->is_manual_compaction() && manual_compaction_paused_ > 0) return;" before
    // c->SetNotifyOnCompactionCompleted(), and NotifyOnCompactionCompleted: "if (c->ShouldNotifyOnCompactionCompleted()
    // == false) return;" -- a manual compaction that starts while manual compactions are paused reports neither.
    [TestMethod]
    public void PausedManualCompactionReportsNeitherBeginNorCompleted()
    {
        (_, DbOptions options, Recorder l) = Setup();
        using RocksDatabase db = RocksDatabase.Open(DbPath, options, new ColumnFamilyOptions { DisableAutoCompactions = true });
        FillTwoLevel0Files(db);
        object impl = typeof(RocksDatabase).GetField("_db", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(db)!;
        FieldInfo paused = impl.GetType().GetField("_manualCompactionPaused", BindingFlags.NonPublic | BindingFlags.Instance)!;
        Task? disable = null;
        TestHooks.Set("DBImpl::RunCompaction:Start", () =>
        {
            disable = Task.Run(db.DisableManualCompaction);
            SpinWait.SpinUntil(() => (int)paused.GetValue(impl)! > 0, Timeout);
        });
        try
        {
            RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => db.CompactRange());
            Assert.AreEqual(StatusCode.Incomplete, e.Code);
            Assert.IsNotNull(disable);
            Assert.IsTrue(disable!.Wait(Timeout));
            TestHooks.ClearAll();
            Assert.AreEqual(0, l.Count("CompactionBegin"));
            Assert.AreEqual(0, l.Count("CompactionCompleted"));

            // Once enabled again the compaction reports both, with its output created before the install.
            db.EnableManualCompaction();
            string? level1AtCreation = null;
            l.TableFileCreated = _ => level1AtCreation = db.GetProperty("rocksdb.num-files-at-level1");
            db.CompactRange();
            Assert.IsTrue(SpinWait.SpinUntil(() => l.Count("CompactionCompleted") == 1, Timeout));
            Assert.AreEqual(1, l.Count("CompactionBegin"));
            Assert.AreEqual("0", level1AtCreation);
            CompactionJobInfo done = l.All<CompactionJobInfo>("CompactionCompleted")[0];
            Assert.IsNull(done.Status);
            Assert.AreEqual(0, done.BaseInputLevel); // c->start_level()
            Assert.AreEqual(2, done.InputFiles.Count);
            Assert.AreEqual(1, done.OutputFiles.Count);
        }
        finally
        {
            TestHooks.ClearAll();
        }
    }

    // DBImpl::NotifyOnMemTableSealed (db_impl_write.cc): "if (shutting_down_.load(std::memory_order_acquire)) return;"
    // -- the DB stays writable after CancelAllBackgroundWork, but memtable switches are no longer reported.
    [TestMethod]
    public void NoMemTableSealedAfterShutdownBegan()
    {
        (_, DbOptions options, Recorder l) = Setup();
        using RocksDatabase db = RocksDatabase.Open(DbPath, options, new ColumnFamilyOptions { WriteBufferSize = 64 * 1024 });
        db.Put(B("a"), B("1"));
        db.Flush();
        Assert.IsTrue(SpinWait.SpinUntil(() => l.Count("MemTableSealed") == 1, Timeout));
        db.CancelAllBackgroundWork(wait: true);
        ulong before = ulong.Parse(db.GetProperty("rocksdb.num-immutable-mem-table")!);
        byte[] value = new byte[4096];
        for (int i = 0; i < 40 && ulong.Parse(db.GetProperty("rocksdb.num-immutable-mem-table")!) == before; i++) db.Put(B("k" + i), value);
        Assert.IsTrue(ulong.Parse(db.GetProperty("rocksdb.num-immutable-mem-table")!) > before, "the memtable was switched");
        object impl = typeof(RocksDatabase).GetField("_db", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(db)!;
        PropertyInfo pending = impl.GetType().GetProperty("HasPendingNotifications", BindingFlags.NonPublic | BindingFlags.Instance)!;
        Assert.IsTrue(SpinWait.SpinUntil(() => !(bool)pending.GetValue(impl)!, Timeout));
        Assert.AreEqual(1, l.Count("MemTableSealed"));
    }

    // FullFilterBlockReader::MayMatch (table/block_based/full_filter_block.cc) counts bloom_sst_hit_count /
    // bloom_sst_miss_count for every probe; PrefixMayMatch goes through it, so a Get against a prefix-only filter
    // (BlockBasedTable::FullFilterKeyMayMatch, whole_key_filtering = false) and a prefix seek
    // (FilterBlockReaderCommon::RangeMayExist) count too.
    [TestMethod]
    public void PrefixFilterProbesCountBloomSstPerfCounters()
    {
        MemoryFileSystem fs = NewFs();
        var cf = new ColumnFamilyOptions
        {
            PrefixExtractor = SliceTransform.CreateFixedPrefix(3),
            TableOptions = new BlockBasedTableOptions { FilterPolicy = new BloomFilterPolicy(10), WholeKeyFiltering = false },
        };
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cf);
        db.Put(B("aaa1"), B("v"));
        db.Put(B("ccc1"), B("v"));
        db.Flush();
        PerfContext.Level = PerfLevel.EnableCount;
        try
        {
            PerfContext.Current.Reset();
            Assert.IsNotNull(db.Get(B("aaa1")));
            Assert.AreEqual(1UL, PerfContext.Current[PerfMetric.BloomSstHitCount]);
            Assert.AreEqual(0UL, PerfContext.Current[PerfMetric.BloomSstMissCount]);

            PerfContext.Current.Reset();
            Assert.IsNull(db.Get(B("bbb1"))); // inside the file's key range, prefix absent
            Assert.AreEqual(0UL, PerfContext.Current[PerfMetric.BloomSstHitCount]);
            Assert.AreEqual(1UL, PerfContext.Current[PerfMetric.BloomSstMissCount]);

            PerfContext.Current.Reset();
            using (Iterator it = db.NewIterator(new ReadOptions()))
            {
                it.Seek(B("bbb1"));
                Assert.IsFalse(it.Valid);
            }
            Assert.AreEqual(1UL, PerfContext.Current[PerfMetric.BloomSstMissCount]);
        }
        finally
        {
            PerfContext.Level = PerfLevel.Disable;
        }
    }
}
