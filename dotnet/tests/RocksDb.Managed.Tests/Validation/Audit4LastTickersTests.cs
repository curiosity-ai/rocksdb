using System.Collections.Concurrent;
using System.Text.Json;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Logging;
using RocksDb.Managed.Monitoring;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4, slice LastTickers: the tickers the port did not record (per-job I/O bytes, per-level seek tickers,
/// super version tickers) and failing-first tests of fixes that were merged without one (flush without an output
/// file, the shutdown guards of the flush / compaction notifications, EVENT_LOG formatting at its call sites).
/// </summary>
[TestClass]
public sealed class Audit4LastTickersTests
{
    private static readonly TimeSpan s_timeout = TimeSpan.FromSeconds(20);

    private sealed class Recorder : EventListener
    {
        public readonly ConcurrentQueue<(string Event, object Info)> Entries = new();
        public Action<FlushJobInfo>? FlushBegin;

        public override void OnFlushBegin(RocksDatabase? db, FlushJobInfo info)
        {
            Entries.Enqueue(("FlushBegin", info));
            FlushBegin?.Invoke(info);
        }

        public override void OnFlushCompleted(RocksDatabase? db, FlushJobInfo info) => Entries.Enqueue(("FlushCompleted", info));

        public override void OnCompactionBegin(RocksDatabase? db, CompactionJobInfo info) => Entries.Enqueue(("CompactionBegin", info));

        public override void OnCompactionCompleted(RocksDatabase? db, CompactionJobInfo info) => Entries.Enqueue(("CompactionCompleted", info));

        public override void OnTableFileCreated(TableFileCreationInfo info) => Entries.Enqueue(("TableFileCreated", info));

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

    private sealed class CollectingLogger() : InfoLogger(InfoLogLevel.Info)
    {
        private readonly List<string> _lines = new();

        protected override void LogCore(InfoLogLevel level, string message)
        {
            lock (_lines) _lines.Add(message);
        }

        public List<JsonElement> Events(string name)
        {
            var result = new List<JsonElement>();
            lock (_lines)
            {
                foreach (string line in _lines)
                {
                    int i = line.IndexOf("EVENT_LOG_v1 ", StringComparison.Ordinal);
                    if (i < 0) continue;
                    JsonElement e = JsonDocument.Parse(line[(i + "EVENT_LOG_v1 ".Length)..]).RootElement;
                    if (e.TryGetProperty("event", out JsonElement ev) && ev.GetString() == name) result.Add(e);
                }
            }
            return result;
        }
    }

    private static (MemoryFileSystem Fs, DbOptions Options, Recorder Listener, CollectingLogger Log) Setup()
    {
        MemoryFileSystem fs = NewFs();
        var l = new Recorder();
        var log = new CollectingLogger();
        DbOptions o = NewOptions(fs);
        o.AvoidFlushDuringShutdown = true;
        o.InfoLog = log;
        o.Listeners.Add(l);
        return (fs, o, l, log);
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

    // ---- B1: a flush that produces no file ---------------------------------------------------------------------

    /// <summary>
    /// BuildTable (db/builder.cc:590-602): a flush whose entries were all dropped keeps no file: the creation event names "(nil)" with
    /// Status::Aborted("Empty SST file not kept") and is logged with meta->fd, i.e. the file number allocated for the
    /// flush; FlushJob::Run still installs the (file-less) result and DBImpl::NotifyOnFlushCompleted reports the flush
    /// with FlushJob::GetFlushJobInfo's allocated file number and an empty descriptor's seqno bounds.
    /// </summary>
    [TestMethod]
    public void FlushWithoutOutputReportsNilCreationWithTheAllocatedFileNumber()
    {
        (MemoryFileSystem fs, DbOptions options, Recorder l, CollectingLogger log) = Setup();
        using RocksDatabase db = RocksDatabase.Open(DbPath, options);
        db.Put(B("a"), B("1"));
        // The managed flush has no natural empty output (see FlushOfAPutAndItsSingleDeleteKeepsNoFile): the hook makes
        // it abandon its table like BuildTable does for an output without entries.
        FlushJob.TestForceEmptyOutput = static () => true;
        try
        {
            db.Flush();
        }
        finally
        {
            FlushJob.TestForceEmptyOutput = null;
        }
        Assert.IsTrue(SpinWait.SpinUntil(() => l.Count("FlushCompleted") == 1, s_timeout));
        Assert.AreEqual("0", db.GetProperty("rocksdb.num-files-at-level0"));
        foreach (string c in fs.GetChildren(DbPath)) Assert.IsFalse(c.EndsWith(".sst", StringComparison.Ordinal), c);

        FlushJobInfo begin = l.All<FlushJobInfo>("FlushBegin")[0];
        FlushJobInfo done = l.All<FlushJobInfo>("FlushCompleted")[0];
        Assert.IsTrue(done.FileNumber > 0);
        Assert.AreEqual(begin.FileNumber, done.FileNumber);
        Assert.AreEqual(begin.FilePath, done.FilePath);
        Assert.IsNull(done.TableProperties);
        Assert.AreEqual(DbFormat.MaxSequenceNumber, done.SmallestSeqno);
        Assert.AreEqual(0UL, done.LargestSeqno);

        List<TableFileCreationInfo> created = l.All<TableFileCreationInfo>("TableFileCreated");
        Assert.AreEqual(1, created.Count);
        Assert.AreEqual("(nil)", created[0].FilePath);
        Assert.AreEqual(0UL, created[0].FileSize);
        Assert.AreEqual(TableFileCreationReason.Flush, created[0].Reason);
        Assert.IsNotNull(created[0].Status);
        Assert.AreEqual(StatusCode.Aborted, created[0].Status!.Code);
        StringAssert.Contains(created[0].Status!.Message, "Empty SST file not kept");

        List<JsonElement> events = log.Events("table_file_creation");
        Assert.AreEqual(1, events.Count);
        Assert.AreEqual(done.FileNumber, events[0].GetProperty("file_number").GetUInt64());
        Assert.AreEqual(0UL, events[0].GetProperty("file_size").GetUInt64());
        StringAssert.Contains(events[0].GetProperty("status").GetString()!, "Empty SST file not kept");
        // The memtable is gone: the key reads as deleted and a second flush has nothing to do.
        Assert.IsFalse(db.TryGet(B("a"), new System.Buffers.ArrayBufferWriter<byte>()));
    }

    /// <summary>
    /// BuildTable runs the memtable through a CompactionIterator, which drops a SingleDelete together with the Put it
    /// meets in the same snapshot stripe (compaction_iterator.cc, "the two cancel out"): upstream's flush of such a
    /// memtable keeps no file. Documented deviation (docs/validation.md, FlushJob row): the managed flush only drops
    /// hidden versions, writes both entries and leaves the pair to compaction. Reads are the same either way.
    /// </summary>
    [TestMethod]
    public void FlushOfAPutAndItsSingleDeleteKeepsThePairForCompaction()
    {
        (_, DbOptions options, Recorder l, _) = Setup();
        using RocksDatabase db = RocksDatabase.Open(DbPath, options);
        db.Put(B("a"), B("1"));
        db.SingleDelete(B("a"));
        db.Flush();
        Assert.IsTrue(SpinWait.SpinUntil(() => l.Count("FlushCompleted") == 1, s_timeout));
        Assert.AreEqual("1", db.GetProperty("rocksdb.num-files-at-level0"), "managed flush keeps the pair (upstream: no file)");
        Assert.IsNull(db.Get(B("a")));
        db.CompactRange();
        Assert.IsNull(db.Get(B("a")));
        Assert.AreEqual(0, db.GetLiveFilesMetadata().Count, "the compaction drops the pair");
    }

    // ---- B2: shutdown guards ---------------------------------------------------------------------------------

    /// <summary>
    /// DBImpl::NotifyOnFlushBegin (db_impl_compaction_flush.cc): "if (shutting_down_.load(std::memory_order_acquire))
    /// return;" - a flush job that starts after CancelAllBackgroundWork began reports no OnFlushBegin.
    /// </summary>
    [TestMethod]
    public void NoFlushBeginAfterShutdownBegan()
    {
        (_, DbOptions options, Recorder l, _) = Setup();
        using RocksDatabase db = RocksDatabase.Open(DbPath, options);
        db.Put(B("a"), B("1"));
        int hookRuns = 0;
        TestHooks.Set("DBImpl::FlushMemTables:Start", () =>
        {
            hookRuns++;
            db.CancelAllBackgroundWork(wait: false);
        });
        try
        {
            try
            {
                db.Flush();
            }
            catch (Exception e) when (e is RocksDbException or ObjectDisposedException)
            {
            }
            db.CancelAllBackgroundWork(wait: true);
        }
        finally
        {
            TestHooks.ClearAll();
        }
        Assert.AreEqual(1, hookRuns, "the flush job started");
        Assert.AreEqual(0, l.Count("FlushBegin"));
        Assert.AreEqual(0, l.Count("FlushCompleted"));
    }

    /// <summary>
    /// DBImpl::NotifyOnFlushCompleted: "if (shutting_down_.load(std::memory_order_acquire)) return;" - a flush whose
    /// begin was reported, and that finishes after the shutdown began, reports no OnFlushCompleted although its file
    /// is installed.
    /// </summary>
    [TestMethod]
    public void NoFlushCompletedAfterShutdownBegan()
    {
        (_, DbOptions options, Recorder l, _) = Setup();
        using RocksDatabase db = RocksDatabase.Open(DbPath, options);
        db.Put(B("a"), B("1"));
        l.FlushBegin = _ => db.CancelAllBackgroundWork(wait: false);
        try
        {
            db.Flush();
        }
        catch (Exception e) when (e is RocksDbException or ObjectDisposedException)
        {
        }
        db.CancelAllBackgroundWork(wait: true);
        Assert.AreEqual(1, l.Count("FlushBegin"));
        Assert.AreEqual(1, l.Count("TableFileCreated"), "the flush ran to its end");
        Assert.AreEqual("1", db.GetProperty("rocksdb.num-files-at-level0"));
        Assert.AreEqual(0, l.Count("FlushCompleted"));
    }

    /// <summary>
    /// DBImpl::NotifyOnCompactionBegin: "if (shutting_down_.load(std::memory_order_acquire)) return;" - and, as
    /// SetNotifyOnCompactionCompleted is not reached, no OnCompactionCompleted either.
    /// </summary>
    [TestMethod]
    public void NoCompactionBeginAfterShutdownBegan()
    {
        (_, DbOptions options, Recorder l, _) = Setup();
        using RocksDatabase db = RocksDatabase.Open(DbPath, options, new ColumnFamilyOptions { DisableAutoCompactions = true });
        FillTwoLevel0Files(db);
        int hookRuns = 0;
        TestHooks.Set("DBImpl::RunCompaction:Start", () =>
        {
            hookRuns++;
            db.CancelAllBackgroundWork(wait: false);
        });
        try
        {
            try
            {
                db.CompactRange();
            }
            catch (Exception e) when (e is RocksDbException or ObjectDisposedException)
            {
            }
            db.CancelAllBackgroundWork(wait: true);
        }
        finally
        {
            TestHooks.ClearAll();
        }
        Assert.AreEqual(1, hookRuns, "the compaction job started");
        Assert.AreEqual(0, l.Count("CompactionBegin"));
        Assert.AreEqual(0, l.Count("CompactionCompleted"));
    }

    // ---- B4: EVENT_LOG formatting at the call sites -------------------------------------------------------------

    /// <summary>
    /// EventHelpers::LogAndNotifyTableFileCreationFinished (db/event_helpers.cc:151): the table_file_creation event of
    /// a flush carries the table's seqno-to-time mapping as SeqnoToTimeMapping::ToHumanString ("seqno->time," per
    /// pair). The port wrote the placeholder "(mapping)". (Audit4ClosingToolsTests only pins the formatting helper.)
    /// </summary>
    [TestMethod]
    public void TableFileCreationEventOfAFlushPrintsTheSeqnoToTimeMapping()
    {
        (_, DbOptions options, _, CollectingLogger log) = Setup();
        using RocksDatabase db = RocksDatabase.Open(DbPath, options, new ColumnFamilyOptions { PreserveInternalTimeSeconds = 10000 });
        for (int i = 0; i < 10; i++) db.Put(Key(i), Value(i, 0));
        db.Flush();
        Assert.IsTrue(SpinWait.SpinUntil(() => log.Events("table_file_creation").Count == 1, s_timeout));
        string text = log.Events("table_file_creation")[0].GetProperty("table_properties").GetProperty("seqno_to_time_mapping").GetString()!;
        Assert.IsTrue(System.Text.RegularExpressions.Regex.IsMatch(text, "^(\\d+->\\d+,)+$"), text);
    }

    /// <summary>
    /// CompactionJob::LogCompaction writes "score" with JSONWriter's operator&lt;&lt; of a double (printf "%g"): four L0
    /// files against a trigger of three (score 4/3, times 10 with dynamic level bytes and an empty base level) log
    /// 13.3333; the port printed six decimals, 13.333333.
    /// (Audit4ClosingToolsTests only pins the formatting helper.)
    /// </summary>
    [TestMethod]
    public void CompactionStartedEventPrintsTheScoreWithSixSignificantDigits()
    {
        (MemoryFileSystem fs, DbOptions first, _, _) = Setup();
        var log = new CollectingLogger();
        DbOptions options = NewOptions(fs);
        options.InfoLog = log;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, first, new ColumnFamilyOptions { DisableAutoCompactions = true }))
        {
            for (int f = 0; f < 4; f++)
            {
                db.Put(B("a"), B("v" + f)); // overlapping files: no trivial move
                db.Put(B("z"), B("v" + f));
                db.Flush();
            }
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, options, new ColumnFamilyOptions { Level0FileNumCompactionTrigger = 3 }))
        {
            Assert.IsTrue(SpinWait.SpinUntil(() => log.Events("compaction_started").Count >= 1, s_timeout));
            Assert.AreEqual("13.3333", log.Events("compaction_started")[0].GetProperty("score").GetRawText());
            db.WaitForCompact();
        }
    }

    // ---- A: tickers ------------------------------------------------------------------------------------------

    private static (RocksDatabase Db, Statistics Stats) OpenWithStatistics()
    {
        var stats = new Statistics();
        DbOptions o = NewOptions(NewFs());
        o.Statistics = stats;
        var cf = new ColumnFamilyOptions { DisableAutoCompactions = true, Compression = Managed.Compression.CompressionType.None };
        cf.TableOptions.BlockSize = 1024;
        return (RocksDatabase.Open(DbPath, o, cf), stats);
    }

    /// <summary>
    /// BlockBasedTableIterator::InitDataBlock (block_based_table_iterator.cc:495-503) records
    /// {NON_,}LAST_LEVEL_SEEK_DATA for the first data block loaded since the last seek, except for a compaction input;
    /// value() (block_based_table_iterator.h:133-143) then records ..._SEEK_DATA_USEFUL_NO_FILTER once.
    /// </summary>
    [TestMethod]
    public void SeekDataTickersCountTheFirstDataBlockOfASeekPerLevel()
    {
        (RocksDatabase db, Statistics st) = OpenWithStatistics();
        using (db)
        {
            for (int i = 0; i < 100; i++) db.Put(Key(i), Value(i, 0));
            db.Flush();
            using (Iterator it = db.NewIterator())
            {
                it.Seek(Key(50));
                Assert.IsTrue(it.Valid);
                Assert.AreEqual(1UL, st.GetTickerCount(Ticker.NonLastLevelSeekData));
                Assert.IsTrue(it.Value.Length > 0);
                Assert.AreEqual(1UL, st.GetTickerCount(Ticker.NonLastLevelSeekDataUsefulNoFilter));
                // Moving on to other blocks is not a seek: nothing more is counted.
                for (int i = 0; i < 40 && it.Valid; i++) it.Next();
                Assert.AreEqual(1UL, st.GetTickerCount(Ticker.NonLastLevelSeekData));
                Assert.AreEqual(1UL, st.GetTickerCount(Ticker.NonLastLevelSeekDataUsefulNoFilter));
                it.Seek(Key(1));
                Assert.IsTrue(it.Value.Length > 0);
                Assert.AreEqual(2UL, st.GetTickerCount(Ticker.NonLastLevelSeekData));
                Assert.AreEqual(2UL, st.GetTickerCount(Ticker.NonLastLevelSeekDataUsefulNoFilter));
            }
            // A second, overlapping file makes the manual compaction a real one (no trivial move).
            for (int i = 0; i < 100; i++) db.Put(Key(i), Value(i, 1));
            db.Flush();
            db.CompactRange();
            // The compaction's input iterators record nothing (is_for_compaction).
            Assert.AreEqual(2UL, st.GetTickerCount(Ticker.NonLastLevelSeekData));
            Assert.AreEqual(0UL, st.GetTickerCount(Ticker.LastLevelSeekData));
            using (Iterator it = db.NewIterator())
            {
                it.SeekToFirst();
                Assert.IsTrue(it.Valid);
                Assert.IsTrue(it.Value.Length > 0);
            }
            Assert.AreEqual(1UL, st.GetTickerCount(Ticker.LastLevelSeekData));
            Assert.AreEqual(1UL, st.GetTickerCount(Ticker.LastLevelSeekDataUsefulNoFilter));
            Assert.AreEqual(2UL, st.GetTickerCount(Ticker.NonLastLevelSeekData));
        }
    }

    /// <summary>
    /// ColumnFamilyData::GetThreadLocalSuperVersion (column_family.cc:1424-1425) ticks NUMBER_SUPERVERSION_ACQUIRES when
    /// the cached reference cannot be used, and DBImpl::CleanupSuperVersion (db_impl.cc:6459-6461), reached from
    /// ReturnAndCleanupSuperVersion when the reference cannot go back to the cache, NUMBER_SUPERVERSION_RELEASES (and
    /// _CLEANUPS for the last reference). The port's cache has a slot per processor: a reader that finds every slot in
    /// use takes and releases a plain reference.
    /// </summary>
    [TestMethod]
    public void SuperVersionTickersCountCacheMissesAndUncachedReleases()
    {
        (RocksDatabase db, Statistics st) = OpenWithStatistics();
        using (db)
        {
            db.Put(B("a"), B("1"));
            Assert.IsNotNull(db.Get(B("a")));
            Assert.IsTrue(st.GetTickerCount(Ticker.NumberSuperversionAcquires) >= 1, "the first read finds an empty slot");
            Assert.AreEqual(0UL, st.GetTickerCount(Ticker.NumberSuperversionReleases));

            ulong acquires = st.GetTickerCount(Ticker.NumberSuperversionAcquires);
            SuperVersionCache cache = db.DefaultColumnFamily.Data.LocalSuperVersions;
            object?[] held = cache.TestOccupyAll();
            try
            {
                Assert.IsNotNull(db.Get(B("a")));
            }
            finally
            {
                cache.TestRestore(held);
            }
            Assert.AreEqual(acquires + 1, st.GetTickerCount(Ticker.NumberSuperversionAcquires));
            Assert.AreEqual(1UL, st.GetTickerCount(Ticker.NumberSuperversionReleases));
            Assert.AreEqual(0UL, st.GetTickerCount(Ticker.NumberSuperversionCleanups));
        }
    }

    /// <summary>
    /// BlockBasedTable::Open reads the footer, the metaindex block, the properties block (ReadTablePropertiesHelper: one
    /// BlockFetcher read, the checksum is verified on the bytes read) and the index block: four reads, counted by
    /// RandomAccessFileReader's RecordIOStats. The port read the properties block a second time to verify its checksum.
    /// </summary>
    [TestMethod]
    public void TableOpenReadsThePropertiesBlockOnce()
    {
        (RocksDatabase db, Statistics st) = OpenWithStatistics();
        using (db)
        {
            for (int i = 0; i < 100; i++) db.Put(Key(i), Value(i, 0));
            db.Flush(); // the flush opens its output ("verify that the table is usable")
            Assert.AreEqual(4UL, st.GetTickerCount(Ticker.NonLastLevelReadCount));
        }
    }

    /// <summary>
    /// CompactionJob::RecordCompactionIOStats (compaction_job.cc:2762-2763): COMPACT_READ_BYTES is IOSTATS(bytes_read) of
    /// the compaction threads - the bytes they read from the input files, so nothing for blocks found in the block cache
    /// - and COMPACT_WRITE_BYTES the bytes they wrote. The port recorded the input file sizes.
    /// </summary>
    [TestMethod]
    public void CompactReadBytesCountsTheBytesTheJobReadFromItsInputs()
    {
        (RocksDatabase db, Statistics st) = OpenWithStatistics();
        using (db)
        {
            for (int f = 0; f < 2; f++)
            {
                for (int i = 0; i < 200; i++) db.Put(Key(i), Value(i, f));
                db.Flush();
            }
            ulong inputFileBytes = 0;
            foreach (LiveFileMetadata f in db.GetLiveFilesMetadata()) inputFileBytes += f.Size;
            db.CompactRange();
            ulong read = st.GetTickerCount(Ticker.CompactReadBytes);
            // At most the input files (all of them here: MemoryFileSystem has no OS prefetch, so the compaction
            // readahead buffer reads each file through to its end, as upstream's FilePrefetchBuffer does).
            Assert.IsTrue(read > 0 && read <= inputFileBytes, read + " of " + inputFileBytes);
            ulong outputBytes = 0;
            foreach (LiveFileMetadata f in db.GetLiveFilesMetadata()) outputBytes += f.Size;
            Assert.AreEqual(outputBytes, st.GetTickerCount(Ticker.CompactWriteBytes));

            // Inputs whose data blocks are all in the block cache: the compaction reads nothing from the files.
            for (int i = 0; i < 200; i++) db.Put(Key(i), Value(i, 2));
            db.Flush();
            using (Iterator it = db.NewIterator())
            {
                for (it.SeekToFirst(); it.Valid; it.Next())
                {
                }
            }
            db.CompactRange();
            Assert.AreEqual(read, st.GetTickerCount(Ticker.CompactReadBytes));
        }
    }
}
