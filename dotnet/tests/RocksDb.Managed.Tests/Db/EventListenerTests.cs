using System.Collections.Concurrent;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>Event listener ordering and contents (ports of the scenarios of db/listener_test.cc).</summary>
[TestClass]
public sealed class EventListenerTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);

    /// <summary>Records every callback as a line, in order.</summary>
    private sealed class RecordingListener : EventListener
    {
        // One queue of (event, info) pairs: callbacks arrive concurrently (flush thread vs. the ordered notification
        // queue), so separate name/info queues could interleave and misalign.
        private readonly ConcurrentQueue<(string Event, object Info)> _entries = new();
        public RocksDatabase? LastDb;

        private void Add(string e, object info) => _entries.Enqueue((e, info));

        public override void OnFlushBegin(RocksDatabase? db, FlushJobInfo info)
        {
            LastDb = db;
            Add("FlushBegin:" + info.ColumnFamilyName + ":" + info.FileNumber, info);
        }

        public override void OnFlushCompleted(RocksDatabase? db, FlushJobInfo info) => Add("FlushCompleted:" + info.ColumnFamilyName + ":" + info.FileNumber, info);
        public override void OnCompactionBegin(RocksDatabase? db, CompactionJobInfo info) => Add("CompactionBegin:" + info.JobId, info);
        public override void OnCompactionCompleted(RocksDatabase? db, CompactionJobInfo info) => Add("CompactionCompleted:" + info.JobId, info);
        public override void OnTableFileCreated(TableFileCreationInfo info) => Add("TableFileCreated:" + info.Reason + ":" + Path.GetFileName(info.FilePath), info);
        public override void OnTableFileDeleted(TableFileDeletionInfo info) => Add("TableFileDeleted:" + Path.GetFileName(info.FilePath), info);
        public override void OnStallConditionsChanged(WriteStallInfo info) => Add("Stall:" + info.Previous + "->" + info.Current, info);
        public override void OnMemTableSealed(MemTableInfo info) => Add("MemTableSealed:" + info.ColumnFamilyName + ":" + info.NumEntries, info);
        public override void OnColumnFamilyHandleDeletionStarted(ColumnFamily handle) => Add("HandleDeletion:" + handle.Name, handle);
        public override void OnExternalFileIngested(RocksDatabase? db, ExternalFileIngestionInfo info) => Add("Ingested:" + info.ColumnFamilyName, info);

        public List<string> Snapshot()
        {
            var list = new List<string>();
            foreach ((string e, _) in _entries) list.Add(e);
            return list;
        }

        public T Single<T>(string prefix)
        {
            (string Event, object Info)[] entries = _entries.ToArray();
            foreach ((string e, object info) in entries)
            {
                if (e.StartsWith(prefix, StringComparison.Ordinal)) return (T)info;
            }
            Assert.Fail("no event " + prefix + " in " + string.Join(", ", Snapshot()));
            return default!;
        }
    }

    private static (MemoryFileSystem Fs, DbOptions Options, RecordingListener Listener) Setup()
    {
        MemoryFileSystem fs = NewFs();
        var l = new RecordingListener();
        DbOptions o = NewOptions(fs);
        o.Listeners.Add(l);
        return (fs, o, l);
    }

    private static int IndexOf(List<string> events, string prefix)
    {
        for (int i = 0; i < events.Count; i++)
        {
            if (events[i].StartsWith(prefix, StringComparison.Ordinal)) return i;
        }
        return -1;
    }

    [TestMethod]
    public void FlushEventsInOrderWithContents()
    {
        (_, DbOptions options, RecordingListener l) = Setup();
        using RocksDatabase db = RocksDatabase.Open(DbPath, options);
        db.Put(B("a"), B("1"));
        db.Put(B("b"), B("2"));
        db.Delete(B("c"));
        db.Flush();
        SpinWait.SpinUntil(() => IndexOf(l.Snapshot(), "MemTableSealed") >= 0, Timeout);
        List<string> ev = l.Snapshot();
        int begin = IndexOf(ev, "FlushBegin:default");
        int created = IndexOf(ev, "TableFileCreated:Flush");
        int completed = IndexOf(ev, "FlushCompleted:default");
        Assert.IsTrue(begin >= 0 && begin < created && created < completed, string.Join(", ", ev));
        Assert.AreSame(db, l.LastDb);

        FlushJobInfo b = l.Single<FlushJobInfo>("FlushBegin");
        FlushJobInfo c = l.Single<FlushJobInfo>("FlushCompleted");
        Assert.AreEqual(FlushReason.ManualFlush, b.FlushReason);
        Assert.AreEqual(b.FileNumber, c.FileNumber);
        Assert.AreEqual(b.JobId, c.JobId);
        Assert.AreEqual(1UL, c.SmallestSeqno);
        Assert.AreEqual(3UL, c.LargestSeqno);
        Assert.IsNotNull(c.TableProperties);
        Assert.AreEqual(3UL, c.TableProperties!.NumEntries);
        StringAssert.EndsWith(c.FilePath, c.FileNumber.ToString("D6", System.Globalization.CultureInfo.InvariantCulture) + ".sst");

        TableFileCreationInfo t = l.Single<TableFileCreationInfo>("TableFileCreated");
        Assert.IsNull(t.Status);
        Assert.AreEqual(c.FilePath, t.FilePath);
        Assert.AreEqual(DbPath, t.DbName);
        Assert.IsTrue(t.FileSize > 0);

        MemTableInfo m = l.Single<MemTableInfo>("MemTableSealed");
        Assert.AreEqual(3UL, m.NumEntries);
        Assert.AreEqual(1UL, m.NumDeletes);
        Assert.AreEqual(1UL, m.FirstSeqno);
    }

    [TestMethod]
    public void CompactionEventsAndTableFileDeletion()
    {
        (_, DbOptions options, RecordingListener l) = Setup();
        using RocksDatabase db = RocksDatabase.Open(DbPath, options);
        for (int f = 0; f < 3; f++)
        {
            for (int i = 0; i < 10; i++) db.Put(Key(i), Value(i, f));
            db.Flush();
        }
        db.CompactRange();
        List<string> ev = l.Snapshot();
        int begin = IndexOf(ev, "CompactionBegin");
        int created = IndexOf(ev, "TableFileCreated:Compaction");
        int completed = IndexOf(ev, "CompactionCompleted");
        Assert.IsTrue(begin >= 0 && begin < created && created < completed, string.Join(", ", ev));
        CompactionJobInfo b = l.Single<CompactionJobInfo>("CompactionBegin");
        CompactionJobInfo c = l.Single<CompactionJobInfo>("CompactionCompleted");
        Assert.AreEqual(b.JobId, c.JobId);
        Assert.AreEqual(3, c.InputFiles.Count);
        Assert.AreEqual(1, c.OutputFiles.Count);
        Assert.AreEqual(0, b.OutputFiles.Count);
        Assert.AreEqual(CompactionReason.ManualCompaction, c.CompactionReason);
        Assert.IsNull(c.Status);
        Assert.AreEqual(30L, c.NumInputRecords);
        Assert.AreEqual(10L, c.NumOutputRecords);
        Assert.AreEqual(0, c.BaseInputLevel);
        // The three inputs are deleted once obsolete.
        SpinWait.SpinUntil(() => l.Snapshot().FindAll(static e => e.StartsWith("TableFileDeleted", StringComparison.Ordinal)).Count >= 3, Timeout);
        ev = l.Snapshot();
        foreach (string input in c.InputFiles) Assert.IsTrue(ev.Contains("TableFileDeleted:" + Path.GetFileName(input)), input);
        Assert.IsTrue(IndexOf(ev, "TableFileDeleted") > completed);
    }

    [TestMethod]
    public void StallConditionChanges()
    {
        (_, DbOptions options, RecordingListener l) = Setup();
        var cfo = new ColumnFamilyOptions { Level0FileNumCompactionTrigger = 2, Level0SlowdownWritesTrigger = 2, Level0StopWritesTrigger = 100 };
        using var gate = new ManualResetEventSlim(false);
        TestHooks.Set("DBImpl::RunCompaction:Start", () => gate.Wait(Timeout));
        try
        {
            using RocksDatabase db = RocksDatabase.Open(DbPath, options, cfo);
            for (int f = 0; f < 2; f++)
            {
                db.Put(Key(f), Value(f, 0));
                db.Put(Key(f + 1), Value(f, 0));
                db.Flush();
            }
            SpinWait.SpinUntil(() => IndexOf(l.Snapshot(), "Stall:Normal->Delayed") >= 0, Timeout);
            WriteStallInfo w = l.Single<WriteStallInfo>("Stall:Normal->Delayed");
            Assert.AreEqual("default", w.ColumnFamilyName);
            gate.Set();
            db.WaitForCompact();
            SpinWait.SpinUntil(() => IndexOf(l.Snapshot(), "Stall:Delayed->Normal") >= 0, Timeout);
            Assert.IsTrue(IndexOf(l.Snapshot(), "Stall:Delayed->Normal") > IndexOf(l.Snapshot(), "Stall:Normal->Delayed"));
        }
        finally
        {
            gate.Set();
            TestHooks.ClearAll();
        }
    }

    [TestMethod]
    public void IngestionAndColumnFamilyHandleEvents()
    {
        (MemoryFileSystem fs, DbOptions options, RecordingListener l) = Setup();
        RocksDatabase db = RocksDatabase.Open(DbPath, options);
        ColumnFamily cf = db.CreateColumnFamily("x", new ColumnFamilyOptions());
        ColumnFamily cf2 = db.CreateColumnFamily("y", new ColumnFamilyOptions());
        using (var w = new SstFileWriter(new ColumnFamilyOptions(), fs))
        {
            w.Open("/ext.sst");
            w.Put(B("k1"), B("v1"));
            w.Finish();
        }
        db.IngestExternalFiles(cf, ["/ext.sst"], new IngestExternalFileOptions());
        ExternalFileIngestionInfo info = l.Single<ExternalFileIngestionInfo>("Ingested:x");
        Assert.AreEqual("/ext.sst", info.ExternalFilePath);
        StringAssert.StartsWith(info.InternalFilePath, DbPath + "/");
        Assert.IsNotNull(info.TableProperties);
        Assert.AreEqual(1UL, info.TableProperties!.NumEntries);
        Assert.AreEqual("v1", GetString(cf, "k1"));

        db.DropColumnFamily(cf2);
        Assert.IsTrue(l.Snapshot().Contains("HandleDeletion:y"));
        db.Dispose();
        List<string> ev = l.Snapshot();
        Assert.IsTrue(ev.Contains("HandleDeletion:x"));
        Assert.IsTrue(ev.Contains("HandleDeletion:default"));
        Assert.AreEqual(1, ev.FindAll(static e => e == "HandleDeletion:y").Count);
    }

    [TestMethod]
    public void ThrowingListenerDoesNotBreakTheDb()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        o.Listeners.Add(new ThrowingListener());
        var rec = new RecordingListener();
        o.Listeners.Add(rec);
        using RocksDatabase db = RocksDatabase.Open(DbPath, o);
        db.Put(B("a"), B("1"));
        db.Flush();
        Assert.IsTrue(IndexOf(rec.Snapshot(), "FlushCompleted") >= 0);
        Assert.AreEqual("1", GetString(db, "a"));
    }

    private sealed class ThrowingListener : EventListener
    {
        public override void OnFlushBegin(RocksDatabase? db, FlushJobInfo info) => throw new InvalidOperationException("boom");
        public override void OnFlushCompleted(RocksDatabase? db, FlushJobInfo info) => throw new InvalidOperationException("boom");
        public override void OnTableFileCreated(TableFileCreationInfo info) => throw new InvalidOperationException("boom");
    }
}
