using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4, compaction job slice (CompactionIterator, CompactionJob, MergeHelper::MergeUntil, FlushJob, the compaction
/// and flush parts of DBImpl): regression tests for the divergences found and coverage for branches without a test.
/// </summary>
[TestClass]
public sealed class Audit4CompactionJobTests
{
    private const string DbPath = "/audit4cj";

    [TestCleanup]
    public void Cleanup() => TestHooks.ClearAll();

    private static RocksDatabase Open(MemoryFileSystem fs, ColumnFamilyOptions cf) => Open(fs, cf, new DbOptions());

    private static RocksDatabase Open(MemoryFileSystem fs, ColumnFamilyOptions cf, DbOptions options)
    {
        options.CreateIfMissing = true;
        options.FileSystem = fs;
        return RocksDatabase.Open(DbPath, options, [new ColumnFamilyDescriptor("default", cf)]);
    }

    private static LiveFileMetadata FileWith(RocksDatabase db, string userKey)
    {
        LiveFileMetadata? found = null;
        foreach (LiveFileMetadata f in db.GetLiveFilesMetadata())
        {
            if (string.CompareOrdinal(S(f.SmallestKey), userKey) <= 0 && string.CompareOrdinal(userKey, S(f.LargestKey)) <= 0)
            {
                Assert.IsNull(found, "more than one file holds " + userKey);
                found = f;
            }
        }
        Assert.IsNotNull(found, "no file holds " + userKey);
        return found;
    }

    /// <summary>
    /// CompactionJob::Prepare (compaction_job.cc: "if (c->compaction_reason() == CompactionReason::kBottommostFiles)
    /// preserve_time_min_seqno_floor_ = storage_info->GetPreserveTimeMinSeqno(); preserve_time_min_seqno =
    /// std::max(preserve_time_min_seqno, preserve_time_min_seqno_floor_)"). The bottommost-file marker
    /// (ComputeBottommostFilesMarkedForCompaction) decides with the DB-wide seqno-to-time mapping that a file's
    /// sequence numbers can be zeroed; the job derived its own boundary only from the input file's persisted mapping,
    /// which is empty for a file written before preserve_internal_time_seconds was enabled. The port then zeroed
    /// nothing, the output was marked again, and the file was rewritten forever.
    /// </summary>
    [TestMethod]
    public void BottommostFileCompactionZeroesTheSeqnosItsMarkerFoundZeroable()
    {
        MemoryFileSystem fs = NewFs();
        ulong largestSeqno;
        // A bottommost file with non-zero sequence numbers and no seqno-to-time mapping (the feature is off).
        using (RocksDatabase db = Open(fs, new ColumnFamilyOptions { DisableAutoCompactions = true }))
        {
            for (int i = 0; i < 20; i++) db.Put(Key(i), Value(i, 0));
            db.Flush();
            db.CompactRange(); // one file, nothing below: moved down without being rewritten
            LiveFileMetadata f = FileWith(db, "key000000");
            Assert.IsTrue(f.Level > 0, "level " + f.Level);
            largestSeqno = f.LargestSeqno;
            Assert.AreEqual(20UL, largestSeqno);
        }

        using var compacted = new ManualResetEventSlim();
        using (RocksDatabase db = Open(fs, new ColumnFamilyOptions { PreserveInternalTimeSeconds = 100_000 }))
        {
            Assert.AreEqual(largestSeqno, FileWith(db, "key000000").LargestSeqno);
            // The DB knows that the existing data is older than the preserve window.
            ulong now = (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            db.DefaultColumnFamily.Db.TestAddSeqnoToTimeSample(largestSeqno, now - 200_000);
            TestHooks.Set("DBImpl::RunCompaction:AfterLogAndApply", compacted.Set);
            // A new super version recomputes the preserve boundary of the version (MaybeUpdatePreserveTimeMinSeqno) ...
            db.Put(B("zzz"), B("v"));
            db.Flush();
            // ... and a snapshot release past the file's sequence numbers marks it (DBImpl::ReleaseSnapshot).
            db.GetSnapshot().Dispose();
            Assert.IsTrue(compacted.Wait(TimeSpan.FromSeconds(60)), "the marked bottommost file was not compacted");
            LiveFileMetadata f = FileWith(db, "key000000");
            Assert.AreEqual(0UL, f.LargestSeqno, "the bottommost-file compaction must zero the sequence numbers");
            Assert.AreEqual(0UL, f.SmallestSeqno);
            for (int i = 0; i < 20; i++) CollectionAssert.AreEqual(Value(i, 0), db.Get(Key(i)));
            // The rewritten file is not marked again.
            db.WaitForCompact();
            Assert.AreEqual(f.FileNumber, FileWith(db, "key000000").FileNumber);
        }
    }

    /// <summary>
    /// BuildTable (db/builder.cc) and CompactionOutputs::AddRangeDels: "meta->compensated_range_deletion_size +=
    /// versions->ApproximateSize(...)" -- a file with range tombstones is compensated by the size of the older data its
    /// tombstones cover, which raises its compaction priority and is persisted in the MANIFEST (NewFileCustomTag 14).
    /// The port never computes it.
    /// </summary>
    [TestMethod]
    [Ignore("audit4: compensated_range_deletion_size is not computed by flush / compaction (needs VersionSet::ApproximateSize with a start level, outside this slice)")]
    public void FlushCompensatesFilesForTheDataTheirRangeTombstonesCover()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = Open(fs, new ColumnFamilyOptions { DisableAutoCompactions = true });
        for (int i = 0; i < 200; i++) db.Put(Key(i), Value(i, 0));
        db.Flush();
        db.CompactRange();
        db.DeleteRange(Key(0), Key(200));
        db.Flush();
        ulong compensated = 0;
        foreach (FileMetaData f in db.DefaultColumnFamily.Data.Current.Storage.LevelFiles(0)) compensated += f.CompensatedRangeDeletionSize;
        Assert.IsTrue(compensated > 0, "compensated_range_deletion_size of the flushed tombstone file");
    }

    /// <summary>
    /// CompactionJob::FinishCompactionOutputFile calls RecordDroppedKeys(range_del_out_stats) for every output file, and
    /// CompactionOutputs::AddRangeDels counts a dropped tombstone in both num_range_del_drop_obsolete and
    /// num_record_drop_obsolete ("tombstones that span multiple output files are counted for each compaction output
    /// file"). The port overwrote the counters of all but the last output with the iterator's statistics and never
    /// counted the tombstone as an obsolete record, so COMPACTION_RANGE_DEL_DROP_OBSOLETE was 1 whatever the number of
    /// outputs and COMPACTION_KEY_DROP_OBSOLETE 0.
    /// </summary>
    [TestMethod]
    public void DroppedRangeTombstoneIsCountedForEveryOutputFile()
    {
        MemoryFileSystem fs = NewFs();
        var stats = new Statistics();
        var cf = new ColumnFamilyOptions { DisableAutoCompactions = true, TargetFileSizeBase = 4 * 1024, Compression = RocksDb.Managed.Compression.CompressionType.None };
        using RocksDatabase db = Open(fs, cf, new DbOptions { Statistics = stats });
        const int n = 600;
        for (int i = 0; i < n; i++) db.Put(Key(i), Value(i, 0));
        db.Flush();
        db.DeleteRange(Key(0), Key(n)); // one tombstone over the whole key range
        db.Flush();
        for (int i = 0; i < n; i++) db.Put(Key(i), Value(i, 1)); // newer than the tombstone: the outputs span its range
        db.Flush();
        db.CompactRange(); // one bottommost compaction, no snapshot: the tombstone is dropped

        ulong outputs = (ulong)db.GetLiveFilesMetadata().Count;
        Assert.IsTrue(outputs >= 3, "output files: " + outputs);
        Assert.AreEqual(0L, db.DefaultColumnFamily.Data.Current.Storage.LevelFiles(0).Length);
        Assert.AreEqual(outputs, stats.GetTickerCount(Ticker.CompactionRangeDelDropObsolete));
        // No point deletion in the input: the obsolete records are the dropped tombstones.
        Assert.AreEqual(outputs, stats.GetTickerCount(Ticker.CompactionKeyDropObsolete));
        // The n old values are hidden by the newer ones in the same snapshot stripe: rule (A) of NextFromInput comes
        // before the range tombstone check, so they count as num_record_drop_hidden only.
        Assert.AreEqual((ulong)n, stats.GetTickerCount(Ticker.CompactionKeyDropNewerEntry));
        Assert.AreEqual(0UL, stats.GetTickerCount(Ticker.CompactionKeyDropRangeDel));
        for (int i = 0; i < n; i += 37) CollectionAssert.AreEqual(Value(i, 1), db.Get(Key(i)));
    }

    /// <summary>
    /// CompactionOutputs::AddRangeDels with subcompactions: a range tombstone spanning subcompaction boundaries must keep
    /// covering the keys on both sides of every boundary (the earlier output ends at boundary@kMaxSequenceNumber, the
    /// later one starts at the boundary user key), while the snapshot taken before it still reads the old values.
    /// </summary>
    [TestMethod]
    public void RangeTombstoneSpanningSubcompactionBoundariesKeepsCoverageAndSnapshots()
    {
        MemoryFileSystem fs = NewFs();
        var cf = new ColumnFamilyOptions { DisableAutoCompactions = true, TargetFileSizeBase = 4 * 1024, WriteBufferSize = 1 << 20 };
        var stats = new Statistics();
        using RocksDatabase db = Open(fs, cf, new DbOptions { MaxSubcompactions = 4, Statistics = stats });
        const int n = 2000;
        for (int round = 0; round < 3; round++)
        {
            for (int i = round; i < n; i += 3) db.Put(Key(i), Value(i, 0));
            db.Flush();
        }
        using Snapshot before = db.GetSnapshot();
        db.DeleteRange(Key(100), Key(1900));
        for (int i = 150; i < n; i += 250) db.Put(Key(i), Value(i, 1)); // newer than the tombstone
        db.Flush();
        using Snapshot after = db.GetSnapshot();
        db.Put(Key(777), Value(777, 2));
        db.CompactRange(new CompactRangeOptions { BottommostLevelCompaction = BottommostLevelCompaction.Force }, null, null);
        Assert.IsTrue(stats.GetHistogramData(Histogram.NumSubcompactionsScheduled).Max >= 2, "the compaction was not split into subcompactions");
        Assert.IsTrue(db.GetLiveFilesMetadata().Count > 4, "expected many output files");

        var atBefore = new ReadOptions { Snapshot = before };
        var atAfter = new ReadOptions { Snapshot = after };
        for (int i = 0; i < n; i++)
        {
            byte[] k = Key(i);
            CollectionAssert.AreEqual(Value(i, 0), db.Get(atBefore, k), "before the tombstone: " + i);
            bool rewritten = i >= 150 && (i - 150) % 250 == 0;
            byte[]? expectedAfter = rewritten ? Value(i, 1) : i >= 100 && i < 1900 ? null : Value(i, 0);
            CollectionAssert.AreEqual(expectedAfter, db.Get(atAfter, k), "after the tombstone: " + i);
            CollectionAssert.AreEqual(i == 777 ? Value(777, 2) : expectedAfter, db.Get(k), "tip: " + i);
        }
        // Scans agree with the point lookups (file boundaries extended by the tombstone).
        int seen = 0;
        using (Iterator it = db.NewIterator(atAfter))
        {
            for (it.SeekToFirst(); it.Valid; it.Next()) seen++;
        }
        Assert.AreEqual(100 + (n - 1900) + 7, seen);
    }
}
