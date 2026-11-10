using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Tests.Db;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4 follow-up, flush / compaction slice (FlushJob vs db/flush_job.cc + db/builder.cc, DbImpl.Flush.cs and
/// DbImpl.Compaction.cs vs db/db_impl/db_impl_compaction_flush.cc, compaction.cc helpers): regression tests for the
/// divergences found and coverage for branches without a test.
/// </summary>
[TestClass]
public sealed class Audit4FollowUpCompactionTests
{
    private const string DbPath = "/audit4fc";

    [TestCleanup]
    public void Cleanup() => TestHooks.ClearAll();

    private static RocksDatabase Open(MemoryFileSystem fs, ColumnFamilyOptions cf) => Open(fs, cf, new DbOptions());

    private static RocksDatabase Open(MemoryFileSystem fs, ColumnFamilyOptions cf, DbOptions options)
    {
        options.CreateIfMissing = true;
        options.FileSystem = fs;
        return RocksDatabase.Open(DbPath, options, [new ColumnFamilyDescriptor("default", cf)]);
    }

    private static VersionStorageInfo Storage(RocksDatabase db) => db.DefaultColumnFamily.Data.Current.Storage;

    private sealed class ReasonListener : EventListener
    {
        public BackgroundErrorReason? Reason;

        public override void OnBackgroundError(BackgroundErrorEventArgs e) => Reason ??= e.Reason;
    }

    /// <summary>
    /// DBImpl::ScheduleFlushes (db_impl_write.cc: "if (immutable_db_options_.atomic_flush) { ...
    /// SelectColumnFamiliesForAtomicFlush(&amp;cfds); }"): with atomic_flush a full memtable switches the memtables of
    /// every column family with unflushed data and one flush request covers them all, so that their results are
    /// committed together (AtomicFlushMemTablesToOutputFiles). The port switches and flushes only the full one.
    /// </summary>
    [TestMethod]
    [Ignore("audit4: automatic flushes ignore atomic_flush (only the full column family is switched and flushed); needs ScheduleFlushes / the flush queue to carry multi-CF requests")]
    public void AtomicFlushFlushesEveryColumnFamilyWhenOneMemtableIsFull()
    {
        MemoryFileSystem fs = NewFs();
        var options = new DbOptions { CreateIfMissing = true, CreateMissingColumnFamilies = true, FileSystem = fs, AtomicFlush = true };
        using RocksDatabase db = RocksDatabase.Open(DbPath, options, [
            new ColumnFamilyDescriptor("default", new ColumnFamilyOptions { WriteBufferSize = 64 * 1024, DisableAutoCompactions = true }),
            new ColumnFamilyDescriptor("other", new ColumnFamilyOptions { DisableAutoCompactions = true })]);
        ColumnFamily other = db.GetColumnFamily("other");
        other.Put(B("o"), B("1"));
        for (int i = 0; i < 2000 && db.DefaultColumnFamily.Data.Current.Storage.LevelFiles(0).Length == 0; i++)
        {
            db.Put(Key(i), new byte[200]);
            if (i % 100 == 99) db.WaitForCompact();
        }
        db.WaitForCompact();
        Assert.IsTrue(db.DefaultColumnFamily.Data.Current.Storage.LevelFiles(0).Length > 0, "the default column family flushed");
        Assert.AreEqual(1, other.Data.Current.Storage.LevelFiles(0).Length, "the other column family is flushed with it");
    }

    /// <summary>
    /// CompactionOutputs::AddRangeDels (compaction_outputs.cc: "if (!bottommost_level) ... approximate_covered_size =
    /// ...ApproximateSize(approx_opts, read_options, input_version, tombstone_start, tombstone_end, output_level + 1,
    /// -1, kCompaction); meta.compensated_range_deletion_size += approximate_covered_size"): an output file above the
    /// bottommost level is compensated by the size of the data below the output level its range tombstones cover.
    /// </summary>
    [TestMethod]
    public void CompactionOutputIsCompensatedForTheDataBelowItsRangeTombstonesCover()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = Open(fs, new ColumnFamilyOptions { DisableAutoCompactions = true });
        for (int i = 0; i < 200; i++) db.Put(Key(i), Value(i, 0));
        db.Flush();
        db.CompactRange();
        int last = Storage(db).NumLevels - 1;
        Assert.AreEqual(1, Storage(db).LevelFiles(last).Length);
        ulong covered = Storage(db).LevelFiles(last)[0].FileSize;
        db.DeleteRange(Key(0), Key(200));
        db.Flush();
        FileMetaData l0 = Storage(db).LevelFiles(0)[0];
        // BuildTable (builder.cc): the flush looks at every level (start_level 0).
        Assert.IsTrue(l0.CompensatedRangeDeletionSize > 0 && l0.CompensatedRangeDeletionSize <= covered, "flush: " + l0.CompensatedRangeDeletionSize);

        db.DefaultColumnFamily.CompactFiles(new CompactFilesOptions(), [l0.FileNumber], last - 1);
        Assert.AreEqual(0, Storage(db).LevelFiles(0).Length);
        FileMetaData output = Storage(db).LevelFiles(last - 1)[0];
        Assert.AreEqual(l0.CompensatedRangeDeletionSize, output.CompensatedRangeDeletionSize);
        // VersionStorageInfo::ComputeCompensatedSizes adds it to the compensated file size.
        Assert.IsTrue(VersionStorageInfo.CompensatedFileSize(output) >= output.FileSize + output.CompensatedRangeDeletionSize);
    }

    /// <summary>
    /// CompactionOutputs::AddRangeDels: no compensation at the bottommost level ("if (!bottommost_level)"), where a
    /// surviving tombstone (kept for a snapshot) covers nothing below it.
    /// </summary>
    [TestMethod]
    public void BottommostCompactionOutputIsNotCompensated()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = Open(fs, new ColumnFamilyOptions { DisableAutoCompactions = true });
        for (int i = 0; i < 200; i++) db.Put(Key(i), Value(i, 0));
        db.Flush();
        db.CompactRange();
        using Snapshot snapshot = db.GetSnapshot();
        db.DeleteRange(Key(0), Key(200));
        db.Flush();
        db.CompactRange();
        int last = Storage(db).NumLevels - 1;
        long tombstones = 0;
        foreach (FileMetaData f in Storage(db).LevelFiles(last))
        {
            tombstones += (long)f.NumRangeDeletions;
            Assert.AreEqual(0UL, f.CompensatedRangeDeletionSize);
        }
        Assert.AreEqual(1L, tombstones, "the snapshot keeps the tombstone");
    }

    /// <summary>
    /// VersionSet::ProcessManifestWrites / ErrorHandler::AddFilesToQuarantine and DBImpl::FindObsoleteFiles (quarantined
    /// files are not deleted): after a MANIFEST write error the record may still be in the MANIFEST that CURRENT names,
    /// so the files the edit added are kept until a later MANIFEST write succeeds. Here the append of a compaction's
    /// edit reaches the MANIFEST and its sync fails; the process then stops. The port deleted the compaction outputs,
    /// so the next open found a MANIFEST naming a table file that no longer existed.
    /// </summary>
    [TestMethod]
    public void CompactionOutputsSurviveAFailedManifestSync()
    {
        var fs = new FaultInjectionFileSystem(NewFs());
        var options = new DbOptions { CreateIfMissing = true, FileSystem = fs, MaxBgErrorResumeCount = 0 };
        var listener = new ReasonListener();
        options.Listeners.Add(listener);
        using (RocksDatabase db = RocksDatabase.Open(DbPath, options, [new ColumnFamilyDescriptor("default", new ColumnFamilyOptions { DisableAutoCompactions = true })]))
        {
            for (int round = 0; round < 2; round++)
            {
                for (int i = 0; i < 50; i++) db.Put(Key(i), Value(i, round));
                db.Flush();
            }
            fs.FailMatching("MANIFEST", FaultOps.Sync, 1, InjectedFaultKind.IOError);
            RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.CompactRange());
            Assert.AreEqual(StatusCode.IOError, ex.Code);
            Assert.AreEqual(1, fs.TargetedFaults);
            // DBImpl::BackgroundCompaction: "auto err_reason = versions_->io_status().ok() ? kCompaction : kManifestWrite".
            Assert.AreEqual(BackgroundErrorReason.ManifestWrite, listener.Reason);
        }
        // A new process: everything written (also the unsynced MANIFEST record) is still there.
        fs.ClearFailMatching();
        using (RocksDatabase reopened = RocksDatabase.Open(DbPath, new DbOptions { FileSystem = fs }))
        {
            for (int i = 0; i < 50; i++) CollectionAssert.AreEqual(Value(i, 1), reopened.Get(Key(i)), "key " + i);
        }
    }

    /// <summary>
    /// CompactionOutputs::CloseOutput (compaction_outputs.h: "Handle subcompaction containing only range deletions ...
    /// if (status.ok() &amp;&amp; !HasBuilder() &amp;&amp; !HasOutput() &amp;&amp; range_del_agg &amp;&amp; !range_del_agg->IsEmpty())
    /// status = open_file_func(*this)") followed by AddRangeDels: a bottommost compaction whose point keys are all
    /// deleted still visits its range tombstones for the (then empty, deleted) output and counts the dropped one in
    /// num_range_del_drop_obsolete and num_record_drop_obsolete. The port opened no file and counted nothing.
    /// </summary>
    [TestMethod]
    public void TombstoneDroppedByACompactionWithoutOutputIsCounted()
    {
        MemoryFileSystem fs = NewFs();
        var stats = new Statistics();
        using RocksDatabase db = Open(fs, new ColumnFamilyOptions { DisableAutoCompactions = true }, new DbOptions { Statistics = stats });
        for (int i = 0; i < 20; i++) db.Put(Key(i), Value(i, 0));
        db.Flush();
        db.CompactRange();
        db.DeleteRange(Key(0), Key(20));
        db.Flush();
        db.CompactRange();
        Assert.AreEqual(0, db.GetLiveFilesMetadata().Count);
        Assert.AreEqual(1UL, stats.GetTickerCount(Ticker.CompactionRangeDelDropObsolete));
        Assert.AreEqual(1UL, stats.GetTickerCount(Ticker.CompactionKeyDropObsolete));
        Assert.AreEqual(20UL, stats.GetTickerCount(Ticker.CompactionKeyDropRangeDel));
    }

    /// <summary>
    /// DBImpl::DisableManualCompaction has no failure path: manual_compaction_paused_ is raised and stays raised until
    /// EnableManualCompaction. The managed call takes a cancellation token for its wait; a canceled wait left the
    /// pause count raised with nobody to lower it, so every later CompactRange / CompactFiles failed with
    /// Incomplete(kManualCompactionPaused).
    /// </summary>
    [TestMethod]
    public void CanceledDisableManualCompactionDoesNotLeaveManualCompactionsDisabled()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = Open(fs, new ColumnFamilyOptions { DisableAutoCompactions = true });
        for (int round = 0; round < 2; round++)
        {
            for (int i = 0; i < 20; i++) db.Put(Key(i), Value(i, round));
            db.Flush();
        }
        using var started = new ManualResetEventSlim(false);
        using var release = new ManualResetEventSlim(false);
        TestHooks.Set("DBImpl::RunCompaction:Start", () =>
        {
            started.Set();
            release.Wait(TimeSpan.FromSeconds(30));
        });
        Task<RocksDbException?> running = Task.Run(() =>
        {
            try
            {
                db.CompactRange();
                return null;
            }
            catch (RocksDbException e)
            {
                return e;
            }
        });
        Assert.IsTrue(started.Wait(TimeSpan.FromSeconds(30)));
        using var canceled = new CancellationTokenSource();
        canceled.Cancel();
        try
        {
            db.DisableManualCompactionAsync(canceled.Token).AsTask().GetAwaiter().GetResult();
            Assert.Fail("the wait for the running manual compaction was canceled");
        }
        catch (OperationCanceledException)
        {
        }
        release.Set();
        // The running compaction was canceled by the disable request (manual_compaction->canceled = true).
        RocksDbException? paused = running.GetAwaiter().GetResult();
        Assert.IsNotNull(paused);
        Assert.AreEqual(StatusCode.Incomplete, paused.Code);
        Assert.AreEqual(StatusSubCode.ManualCompactionPaused, paused.SubCode);
        TestHooks.ClearAll();
        db.CompactRange();
        Assert.AreEqual(0, Storage(db).LevelFiles(0).Length);
    }

    /// <summary>
    /// InternalStats::DumpCFMapStats (internal_stats.cc: "Count # of files being compacted for each level ... if
    /// (f->being_compacted) ++files_being_compacted[level]"): the CompactedFiles column of the level rows and of the
    /// Sum row. The port always reported 0.
    /// </summary>
    [TestMethod]
    public void CfStatsReportTheFilesBeingCompacted()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = Open(fs, new ColumnFamilyOptions { DisableAutoCompactions = true });
        for (int round = 0; round < 2; round++)
        {
            for (int i = 0; i < 20; i++) db.Put(Key(i), Value(i, round));
            db.Flush();
        }
        Assert.AreEqual("0.000000", db.GetMapProperty("rocksdb.cfstats")!["compaction.L0.CompactedFiles"]);
        using var started = new ManualResetEventSlim(false);
        using var release = new ManualResetEventSlim(false);
        TestHooks.Set("DBImpl::RunCompaction:Start", () =>
        {
            started.Set();
            release.Wait(TimeSpan.FromSeconds(30));
        });
        Task running = Task.Run(() => db.CompactRange());
        try
        {
            Assert.IsTrue(started.Wait(TimeSpan.FromSeconds(30)));
            Dictionary<string, string> map = db.GetMapProperty("rocksdb.cfstats")!;
            Assert.AreEqual("2.000000", map["compaction.L0.CompactedFiles"]);
            Assert.AreEqual("2.000000", map["compaction.Sum.CompactedFiles"]);
        }
        finally
        {
            release.Set();
            running.GetAwaiter().GetResult();
        }
        Assert.AreEqual("0.000000", db.GetMapProperty("rocksdb.cfstats")!["compaction.Sum.CompactedFiles"]);
    }

    /// <summary>
    /// DBImpl::CompactRange (db_impl_compaction_flush.cc: "auto [begin, end] = MaybeAddTimestampsToRange(
    /// begin_without_ts, end_without_ts, ts_sz, &amp;begin_str, &amp;end_str, false /*exclusive_end*/)"): on a column family
    /// with user-defined timestamps the bounds are given without a timestamp; the begin key gets the maximum and the
    /// end key the minimum timestamp before they are compared with the files' keys. The port used the bounds as given,
    /// so the comparator took their last 8 bytes for a timestamp.
    /// </summary>
    [TestMethod]
    public void CompactRangeBoundsOfATimestampedColumnFamilyAreUserKeysWithoutTimestamp()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = Open(fs, new ColumnFamilyOptions { Comparator = Comparator.BytewiseWithU64Ts, DisableAutoCompactions = true });
        byte[] ts = new byte[8];
        ts[0] = 10;
        foreach (string k in new[] { "a", "m", "z" })
        {
            db.Put(B(k), ts, B("v-" + k));
            db.Flush();
        }
        Assert.AreEqual(3, Storage(db).LevelFiles(0).Length);
        // Only the file holding "m" overlaps [k, n] (both bounds inclusive): it alone leaves level 0.
        db.CompactRange(B("k"), B("n"));
        Assert.AreEqual(2, Storage(db).LevelFiles(0).Length);
        foreach (FileMetaData f in Storage(db).LevelFiles(0)) Assert.AreNotEqual((byte)'m', f.Smallest[0]);
        // Inclusive bounds that are exactly a file's only user key.
        db.CompactRange(B("z"), B("z"));
        Assert.AreEqual(1, Storage(db).LevelFiles(0).Length);
        Assert.AreEqual((byte)'a', Storage(db).LevelFiles(0)[0].Smallest[0]);
        var read = new ReadOptions { Timestamp = ts };
        foreach (string k in new[] { "a", "m", "z" }) Assert.AreEqual("v-" + k, S(db.Get(read, B(k))));
    }

    /// <summary>
    /// DBImpl::CompactRangeInternal (db_impl_compaction_flush.cc: "if (begin != nullptr &amp;&amp; end != nullptr) ...
    /// cfd->RangesOverlapWithMemtables({range}, super_version, ..., &amp;flush_needed)"): with both bounds given, the
    /// memtables are flushed only when they hold a key (or a range tombstone) inside the range.
    /// </summary>
    [TestMethod]
    public void CompactRangeWithBothBoundsFlushesOnlyOverlappingMemtables()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = Open(fs, new ColumnFamilyOptions { DisableAutoCompactions = true });
        db.Put(B("a"), B("1"));
        db.CompactRange(B("m"), B("z"));
        Assert.AreEqual(0, db.GetLiveFilesMetadata().Count, "no overlap: the memtable is not flushed");
        // A range tombstone reaching into the range counts (RangeDelAggregator::IsRangeOverlapped).
        db.DeleteRange(B("b"), B("n"));
        db.CompactRange(B("m"), B("z"));
        Assert.AreEqual(1, db.GetLiveFilesMetadata().Count, "tombstone [b, n) overlaps [m, z]");
        // One-sided ranges always flush ("TODO(ajkr): We could also optimize away the flush in certain cases").
        db.Put(B("a"), B("2"));
        db.CompactRange(B("m"), null);
        Assert.AreEqual("2", GetString(db, "a"));
        Assert.AreEqual("0", db.GetProperty("rocksdb.num-entries-active-mem-table"));
        // The limit is inclusive (range_limit_exclusive = false).
        db.Put(B("z"), B("3"));
        db.CompactRange(B("m"), B("z"));
        Assert.AreEqual("0", db.GetProperty("rocksdb.num-entries-active-mem-table"));
    }
}
