using System.Text;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #5: standalone range tombstone files in universal compaction. Read against FileMetaData::
/// FileIsStandAloneRangeTombstone (db/version_edit.h), UniversalCompactionBuilder (CalculateSortedRuns,
/// ShouldSkipMarkedFile, PickDeleteTriggeredCompaction, BuildCompactionToNextLevel; db/compaction/
/// compaction_picker_universal.cc), VersionStorageInfo::ComputeFilesMarkedForCompaction (db/version_set.cc) and the
/// snapshot release path of DBImpl (db/db_impl/db_impl.cc). Ports of CompactionPickerTest.
/// StandaloneRangeDeletionOnlyPicksOlderFiles, ExternalSSTFileBasicTest.AtomicReplaceDataWithStandaloneRangeDeletion /
/// StandaloneRangeDeletionEndKeyIsExclusive (without the filtered-input statistics, see validation.md), plus cases
/// derived from the code. DBCompactionTest.StandaloneRangeTombstoneSnapshotAdvanceRequeuesScheduling drives the
/// compaction queue directly; its behaviour (a release past the threshold schedules again) is covered through the
/// public API by OnlyReleasingTheOldestSnapshotSchedulesTheCompaction.
/// </summary>
[TestClass]
public sealed class Audit5StandaloneRangeTombstoneTests
{
    [TestCleanup]
    public void Cleanup() => TestHooks.ClearAll();

    // ---------------------------------------------------------------------------------------------------------
    // synthetic versions

    private static byte[] IKey(string user, int tsSize, ulong seq, EntryType t)
    {
        // A user key with user-defined timestamps ends with the (here zero) timestamp.
        byte[] u = new byte[user.Length + tsSize];
        Encoding.ASCII.GetBytes(user, u);
        var k = new byte[u.Length + 8];
        DbFormat.AppendInternalKey(k, u, seq, t);
        return k;
    }

    /// <summary>A synthetic version (CompactionPickerTestBase): L0 files newest first in the order they are added.</summary>
    private sealed class Lsm
    {
        public readonly ColumnFamilyOptions Options;
        private readonly InternalKeyComparator _icmp;
        private readonly List<FileMetaData>[] _files;
        private ulong _epoch = 1000;

        public Lsm(int numLevels) : this(numLevels, InternalKeyComparator.Bytewise)
        {
        }

        public Lsm(int numLevels, InternalKeyComparator icmp)
        {
            _icmp = icmp;
            Options = new ColumnFamilyOptions { NumLevels = numLevels, CompactionStyle = CompactionStyle.Universal, Ttl = 0, Comparator = icmp.UserComparator };
            _files = new List<FileMetaData>[numLevels];
            for (int i = 0; i < numLevels; i++) _files[i] = new List<FileMetaData>();
        }

        /// <summary>CompactionPickerTestBase::Add.</summary>
        public FileMetaData Add(int level, ulong number, string smallest, string largest, ulong size, ulong seq)
        {
            var f = new FileMetaData
            {
                FileNumber = number,
                FileSize = size,
                Smallest = IKey(smallest, _icmp.UserComparator.TimestampSize, seq, EntryType.Value),
                Largest = IKey(largest, _icmp.UserComparator.TimestampSize, seq, EntryType.Value),
                SmallestSeqno = seq,
                LargestSeqno = seq,
                EpochNumber = level == 0 ? _epoch-- : 0,
            };
            _files[level].Add(f);
            return f;
        }

        /// <summary>A marked file holding one range tombstone (num_entries = num_range_deletions = 1).</summary>
        public FileMetaData AddStandalone(int level, ulong number, string smallest, string largest, ulong seq)
        {
            FileMetaData f = Add(level, number, smallest, largest, 1, seq);
            f.MarkedForCompaction = true;
            f.NumEntries = 1;
            f.NumDeletions = 1;
            f.NumRangeDeletions = 1;
            f.EntryStatsKnown = true;
            Assert.IsTrue(f.FileIsStandAloneRangeTombstone());
            return f;
        }

        public VersionStorageInfo Build()
        {
            InternalKeyComparator icmp = _icmp;
            var arrays = new FileMetaData[_files.Length][];
            for (int l = 0; l < _files.Length; l++)
            {
                if (l > 0) _files[l].Sort((a, b) => icmp.Compare(a.Smallest, b.Smallest));
                arrays[l] = _files[l].ToArray();
            }
            return new VersionStorageInfo(icmp, _files.Length, Options, arrays, [], null);
        }

        public UniversalCompactionPicker Picker() => new(Options, _icmp);
    }

    private static List<ulong> Numbers(CompactionInputFiles inputs)
    {
        var l = new List<ulong>();
        foreach (FileMetaData f in inputs.Files) l.Add(f.FileNumber);
        return l;
    }

    /// <summary>A WritePrepared-style checker that has not seen anything commit.</summary>
    private sealed class NothingCommittedChecker : SnapshotChecker
    {
        public override SnapshotCheckerResult CheckInSnapshot(ulong sequence, ulong snapshotSequence) => SnapshotCheckerResult.NotInSnapshot;
    }

    /// <summary>
    /// FileIsStandAloneRangeTombstone: exactly one entry, which is a range deletion. Files whose stats are not loaded
    /// (num_entries = 0) or that hold anything else are not standalone.
    /// </summary>
    [TestMethod]
    public void FileIsStandAloneRangeTombstoneNeedsOneRangeDeletionOnly()
    {
        var f = new FileMetaData { SmallestSeqno = 5, LargestSeqno = 5 };
        Assert.IsFalse(f.FileIsStandAloneRangeTombstone(), "stats not loaded");
        f.NumEntries = 1;
        f.NumRangeDeletions = 1;
        Assert.IsTrue(f.FileIsStandAloneRangeTombstone());
        f.NumEntries = 2;
        Assert.IsFalse(f.FileIsStandAloneRangeTombstone(), "a point entry beside the tombstone");
        f.NumEntries = 2;
        f.NumRangeDeletions = 2;
        Assert.IsFalse(f.FileIsStandAloneRangeTombstone(), "two range tombstones");
    }

    /// <summary>
    /// standalone_range_tombstone_files_mark_threshold_ (ComputeFilesMarkedForCompaction): the smallest seqno of the marked
    /// standalone files that are not being compacted; other marked files do not count.
    /// </summary>
    [TestMethod]
    public void MarkThresholdIsSmallestSeqnoOfMarkedStandaloneFiles()
    {
        var lsm = new Lsm(6);
        lsm.AddStandalone(0, 1, "100", "200", 300);
        FileMetaData older = lsm.AddStandalone(0, 2, "100", "200", 250);
        FileMetaData marked = lsm.Add(0, 3, "100", "200", 1, 200);
        marked.MarkedForCompaction = true; // marked by a collector, not standalone
        lsm.Add(5, 10, "000", "400", 100, 50);
        VersionStorageInfo vs = lsm.Build();
        Assert.AreEqual(3, vs.FilesMarkedForCompaction.Count);
        Assert.AreEqual(250UL, vs.StandaloneRangeTombstoneFilesMarkThreshold);

        // RemoveFilesBeingCompactedFromCompactionCandidates recomputes it without the file being compacted.
        older.BeingCompacted = true;
        vs.ComputeCompactionScore();
        Assert.AreEqual(300UL, vs.StandaloneRangeTombstoneFilesMarkThreshold);
    }

    /// <summary>
    /// Port of CompactionPickerTest.StandaloneRangeDeletionOnlyPicksOlderFiles: the delete-triggered compaction of an L0
    /// standalone range deletion file does not pull in the newer L0 file it overlaps (starting_l0_file).
    /// </summary>
    [TestMethod]
    public void StandaloneRangeDeletionOnlyPicksOlderFiles()
    {
        var lsm = new Lsm(6);
        lsm.Add(0, 1, "100", "200", 1, 100);             // newest regular file
        lsm.AddStandalone(0, 2, "150", "250", 200);      // standalone range deletion
        lsm.Add(4, 10, "000", "400", 1, 50);
        lsm.Add(5, 20, "000", "400", 100, 40);
        VersionStorageInfo vs = lsm.Build();
        UniversalCompactionPicker picker = lsm.Picker();
        Assert.IsTrue(picker.NeedsCompaction(vs));

        Compaction? c = picker.PickCompaction(vs, []);
        Assert.IsNotNull(c);
        Assert.AreEqual(CompactionReason.FilesMarkedForCompaction, c.Reason);
        Assert.AreEqual(2, c.NumInputLevels);
        Assert.AreEqual(0, c.Inputs(0).Level);
        CollectionAssert.AreEqual(new List<ulong> { 2 }, Numbers(c.Inputs(0)));
        Assert.AreEqual(4, c.Inputs(1).Level);
        CollectionAssert.AreEqual(new List<ulong> { 10 }, Numbers(c.Inputs(1)));
        Assert.AreEqual(4, c.OutputLevel);
    }

    /// <summary>
    /// ShouldSkipMarkedFile: the file waits while the earliest snapshot does not definitely see it -- a snapshot older than
    /// the file, or a snapshot checker that does not report it committed -- and is picked once it does.
    /// </summary>
    [TestMethod]
    public void StandaloneFileWaitsForTheEarliestSnapshot()
    {
        foreach ((ulong[] snapshots, SnapshotChecker? checker, bool picked) in new (ulong[], SnapshotChecker?, bool)[]
        {
            ([150, 400], null, false),                   // the oldest snapshot predates the tombstone
            ([199], null, false),
            ([200], null, true),                         // DataIsDefinitelyInSnapshot: seqno <= snapshot
            ([250, 260], null, true),
            ([], null, true),                            // no snapshot: kMaxSequenceNumber
            ([], new NothingCommittedChecker(), false),  // the checker decides
            ([300], new NothingCommittedChecker(), false),
        })
        {
            var lsm = new Lsm(6);
            lsm.AddStandalone(4, 2, "150", "250", 200);
            lsm.Add(4, 3, "300", "400", 1, 190); // a second L4 file, not marked
            lsm.Add(5, 20, "000", "900", 100, 40);
            VersionStorageInfo vs = lsm.Build();
            string what = "snapshots [" + string.Join(",", snapshots) + "] checker " + (checker is null ? "none" : "nothing committed");
            Compaction? c = lsm.Picker().PickCompaction(vs, snapshots, checker, requireMaxOutputLevel: false);
            if (!picked)
            {
                Assert.IsNull(c, what);
                continue;
            }
            Assert.IsNotNull(c, what);
            Assert.AreEqual(CompactionReason.FilesMarkedForCompaction, c.Reason, what);
            // SetupOtherInputs expands L4 to the second file: it adds no L5 file.
            CollectionAssert.AreEqual(new List<ulong> { 2, 3 }, Numbers(c.Inputs(0)), what);
            CollectionAssert.AreEqual(new List<ulong> { 20 }, Numbers(c.Inputs(1)), what);
        }
    }

    /// <summary>
    /// ShouldSkipMarkedFile: a marked standalone file whose next older sorted run also has one is skipped, so the older
    /// one is the start input level of the delete-triggered compaction.
    /// </summary>
    [TestMethod]
    public void OlderStandaloneFileGoesFirst()
    {
        var lsm = new Lsm(6);
        lsm.AddStandalone(0, 1, "100", "200", 300); // newer
        lsm.AddStandalone(0, 2, "150", "250", 200); // older: the next sorted run
        lsm.Add(4, 10, "000", "400", 1, 50);
        lsm.Add(5, 20, "000", "400", 100, 40);
        VersionStorageInfo vs = lsm.Build();
        Compaction? c = lsm.Picker().PickCompaction(vs, []);
        Assert.IsNotNull(c);
        Assert.AreEqual(CompactionReason.FilesMarkedForCompaction, c.Reason);
        CollectionAssert.AreEqual(new List<ulong> { 2 }, Numbers(c.Inputs(0)));
        CollectionAssert.AreEqual(new List<ulong> { 10 }, Numbers(c.Inputs(1)));
    }

    /// <summary>
    /// level_has_marked_standalone_rangedel: the size-ratio compaction stops at a sorted run holding a marked standalone
    /// file (here deferred by a snapshot), compacting only the newer runs; without the flag all five L0 files would be
    /// picked.
    /// </summary>
    [TestMethod]
    public void SizeRatioCompactionStopsAtStandaloneRun()
    {
        var lsm = new Lsm(1);
        lsm.Add(0, 1, "a", "z", 1, 500);
        lsm.Add(0, 2, "a", "z", 1, 400);
        lsm.AddStandalone(0, 3, "a", "z", 300);
        lsm.Add(0, 4, "a", "z", 1, 200);
        lsm.Add(0, 5, "a", "z", 1, 100);
        VersionStorageInfo vs = lsm.Build();
        Compaction? c = lsm.Picker().PickCompaction(vs, [250], null, requireMaxOutputLevel: false);
        Assert.IsNotNull(c);
        Assert.AreEqual(CompactionReason.UniversalSizeRatio, c.Reason);
        CollectionAssert.AreEqual(new List<ulong> { 1, 2 }, Numbers(c.Inputs(0)));
        Assert.AreEqual(0, c.OutputLevel);
    }

    /// <summary>
    /// Single-level universal (PickDeleteTriggeredCompaction, num_levels = 1): from the first marked file the picker can
    /// use to the oldest run, stopping before another run with a marked standalone file. With a snapshot older than the
    /// tombstone nothing is picked; once it is released, the standalone file and the older runs are compacted.
    /// </summary>
    [TestMethod]
    public void SingleLevelDeleteTriggeredCompactionDefersStandaloneFile()
    {
        var lsm = new Lsm(1);
        lsm.Options.Level0FileNumCompactionTrigger = 100;
        lsm.Add(0, 1, "a", "z", 1, 500);
        lsm.AddStandalone(0, 2, "b", "c", 300);
        lsm.Add(0, 3, "a", "z", 1, 200);
        lsm.Add(0, 4, "a", "z", 1, 100);
        VersionStorageInfo vs = lsm.Build();
        Assert.IsNull(lsm.Picker().PickCompaction(vs, [250], null, requireMaxOutputLevel: false));
        Compaction? c = lsm.Picker().PickCompaction(vs, [], null, requireMaxOutputLevel: false);
        Assert.IsNotNull(c);
        Assert.AreEqual(CompactionReason.FilesMarkedForCompaction, c.Reason);
        CollectionAssert.AreEqual(new List<ulong> { 2, 3, 4 }, Numbers(c.Inputs(0)));
    }

    /// <summary>
    /// With user-defined timestamps the picker gets no snapshots (earliest_snapshot_ stays unset), so the standalone file
    /// is not deferred.
    /// </summary>
    [TestMethod]
    public void UserDefinedTimestampsDoNotDeferStandaloneFile()
    {
        var lsm = new Lsm(6, new InternalKeyComparator(Comparator.BytewiseWithU64Ts));
        lsm.AddStandalone(4, 2, "150", "250", 200);
        lsm.Add(4, 3, "300", "400", 1, 190);
        lsm.Add(5, 20, "000", "900", 100, 40);
        VersionStorageInfo vs = lsm.Build();
        Compaction? c = lsm.Picker().PickCompaction(vs, [100], null, requireMaxOutputLevel: false);
        Assert.IsNotNull(c);
        Assert.AreEqual(CompactionReason.FilesMarkedForCompaction, c.Reason);
    }

    // ---------------------------------------------------------------------------------------------------------
    // DB level

    private static string WriteSst(IFileSystem fs, string path, params (string Key, string Value)[] entries)
    {
        using var w = new SstFileWriter(new ColumnFamilyOptions(), fs);
        w.Open(path);
        foreach ((string k, string v) in entries) w.Put(B(k), B(v));
        w.Finish();
        return path;
    }

    private static string WriteRangeDeletionSst(IFileSystem fs, string path, string begin, string end)
    {
        using var w = new SstFileWriter(new ColumnFamilyOptions(), fs);
        w.Open(path);
        w.DeleteRange(B(begin), B(end));
        w.Finish();
        return path;
    }

    private static MemoryFileSystem NewFsWithExt()
    {
        MemoryFileSystem fs = NewFs();
        fs.CreateDirectoryIfMissing("/ext");
        return fs;
    }

    private static string FilesPerLevel(RocksDatabase db)
    {
        var parts = new string[7];
        for (int l = 0; l < parts.Length; l++) parts[l] = db.DefaultColumnFamily.GetProperty("rocksdb.num-files-at-level" + l)!;
        return string.Join(",", parts);
    }

    /// <summary>Records the reason and input file count of every compaction that completed.</summary>
    private sealed class CompactionRecorder : EventListener
    {
        private readonly List<(CompactionReason Reason, int Inputs, int OutputLevel)> _seen = new();

        public override void OnCompactionCompleted(RocksDatabase? db, CompactionJobInfo info)
        {
            lock (_seen) _seen.Add((info.CompactionReason, info.InputFiles.Count, info.OutputLevel));
        }

        public List<(CompactionReason Reason, int Inputs, int OutputLevel)> Seen
        {
            get
            {
                lock (_seen) return new List<(CompactionReason, int, int)>(_seen);
            }
        }
    }

    private static DbOptions OptionsWith(MemoryFileSystem fs, CompactionRecorder recorder)
    {
        DbOptions o = NewOptions(fs);
        o.Listeners.Add(recorder);
        return o;
    }

    /// <summary>
    /// Port of ExternalSSTFileBasicTest.AtomicReplaceDataWithStandaloneRangeDeletion (data and shape): the old data in
    /// L6, a standalone range deletion over all of it in L5 and the new data in L4. While the snapshot is held nothing is
    /// compacted; releasing it schedules the delete-triggered compaction, which drops L5 and L6.
    /// </summary>
    [TestMethod]
    public void AtomicReplaceDataWithStandaloneRangeDeletion()
    {
        MemoryFileSystem fs = NewFsWithExt();
        var recorder = new CompactionRecorder();
        var cfo = new ColumnFamilyOptions { CompactionStyle = CompactionStyle.Universal };
        using RocksDatabase db = RocksDatabase.Open(DbPath, OptionsWith(fs, recorder), cfo);
        db.IngestExternalFiles(
        [
            WriteSst(fs, "/ext/file1.sst", ("a", "a1"), ("b", "b1")),
            WriteSst(fs, "/ext/file2.sst", ("x", "x1"), ("y", "y1")),
        ], new IngestExternalFileOptions());
        Assert.AreEqual("a1", GetString(db, "a"));
        Assert.AreEqual("y1", GetString(db, "y"));
        Assert.AreEqual("0,0,0,0,0,0,2", FilesPerLevel(db));

        Snapshot snapshot = db.GetSnapshot();
        ulong before = db.GetLatestSequenceNumber();
        db.IngestExternalFiles(
        [
            WriteRangeDeletionSst(fs, "/ext/rangedel.sst", "a", "z"),
            WriteSst(fs, "/ext/file3.sst", ("a", "a2"), ("b", "b2")),
            WriteSst(fs, "/ext/file4.sst", ("x", "x2"), ("y", "y2")),
        ], new IngestExternalFileOptions());
        // Overlapping files each occupy one new sequence number.
        Assert.AreEqual(before + 3, db.GetLatestSequenceNumber());
        Assert.AreEqual("0,0,0,0,2,1,2", FilesPerLevel(db));

        db.WaitForCompact();
        Assert.AreEqual("0,0,0,0,2,1,2", FilesPerLevel(db));
        Assert.AreEqual(0, recorder.Seen.Count, "nothing is compacted while the snapshot predates the tombstone");
        Assert.AreEqual("a1", GetString(db, "a", snapshot));

        snapshot.Dispose();
        db.WaitForCompact();
        Assert.AreEqual("0,0,0,0,2,0,0", FilesPerLevel(db));
        List<(CompactionReason Reason, int Inputs, int OutputLevel)> seen = recorder.Seen;
        Assert.AreEqual(1, seen.Count);
        Assert.AreEqual(CompactionReason.FilesMarkedForCompaction, seen[0].Reason);
        Assert.AreEqual(3, seen[0].Inputs, "the tombstone and both L6 files (upstream filters the L6 files unread)");
        Assert.AreEqual(6, seen[0].OutputLevel);
        Assert.AreEqual("a2", GetString(db, "a"));
        Assert.AreEqual("b2", GetString(db, "b"));
        Assert.AreEqual("x2", GetString(db, "x"));
        Assert.AreEqual("y2", GetString(db, "y"));
    }

    /// <summary>
    /// Port of ExternalSSTFileBasicTest.StandaloneRangeDeletionEndKeyIsExclusive: without a snapshot the standalone file
    /// is compacted at once with the file it overlaps; its exclusive end leaves "b". With a listener registered: the
    /// OnExternalFileIngested notification used to open the ingested table through the table cache without its global
    /// sequence number, and the compaction then read the tombstone at sequence 0 and kept "a".
    /// </summary>
    [TestMethod]
    public void StandaloneRangeDeletionEndKeyIsExclusive()
    {
        MemoryFileSystem fs = NewFsWithExt();
        var recorder = new CompactionRecorder();
        var cfo = new ColumnFamilyOptions { CompactionStyle = CompactionStyle.Universal };
        using RocksDatabase db = RocksDatabase.Open(DbPath, OptionsWith(fs, recorder), cfo);
        db.IngestExternalFiles([WriteSst(fs, "/ext/file1.sst", ("a", "a"), ("b", "b"))], new IngestExternalFileOptions());
        Assert.AreEqual("0,0,0,0,0,0,1", FilesPerLevel(db));

        db.IngestExternalFiles([WriteRangeDeletionSst(fs, "/ext/file2.sst", "a", "b")], new IngestExternalFileOptions());
        db.WaitForCompact();
        Assert.AreEqual("0,0,0,0,0,0,1", FilesPerLevel(db));
        List<(CompactionReason Reason, int Inputs, int OutputLevel)> seen = recorder.Seen;
        Assert.AreEqual(1, seen.Count);
        Assert.AreEqual(CompactionReason.FilesMarkedForCompaction, seen[0].Reason);
        Assert.AreEqual(2, seen[0].Inputs);
        Assert.IsNull(GetString(db, "a"));
        Assert.AreEqual("b", GetString(db, "b"));
    }

    /// <summary>
    /// The snapshot release path (standalone_range_deletion_files_mark_threshold_): releasing a snapshot that is not the
    /// oldest leaves the file deferred; releasing the oldest one schedules the compaction.
    /// </summary>
    [TestMethod]
    public void OnlyReleasingTheOldestSnapshotSchedulesTheCompaction()
    {
        MemoryFileSystem fs = NewFsWithExt();
        var recorder = new CompactionRecorder();
        var cfo = new ColumnFamilyOptions { CompactionStyle = CompactionStyle.Universal };
        using RocksDatabase db = RocksDatabase.Open(DbPath, OptionsWith(fs, recorder), cfo);
        db.IngestExternalFiles([WriteSst(fs, "/ext/old.sst", ("a", "old-a"), ("m", "old-m"))], new IngestExternalFileOptions());
        Snapshot oldest = db.GetSnapshot();
        db.Put(B("zz"), B("v")); // moves the sequence number between the snapshots
        Snapshot newer = db.GetSnapshot();
        db.IngestExternalFiles([WriteRangeDeletionSst(fs, "/ext/rd.sst", "a", "n")], new IngestExternalFileOptions());
        db.WaitForCompact();
        Assert.AreEqual("0,0,0,0,0,1,1", FilesPerLevel(db));

        newer.Dispose();
        db.WaitForCompact();
        Assert.AreEqual("0,0,0,0,0,1,1", FilesPerLevel(db));
        Assert.AreEqual(0, recorder.Seen.Count);
        Assert.AreEqual("old-a", GetString(db, "a", oldest));

        oldest.Dispose();
        db.WaitForCompact();
        Assert.AreEqual("0,0,0,0,0,0,0", FilesPerLevel(db));
        Assert.AreEqual(1, recorder.Seen.Count);
        Assert.IsNull(GetString(db, "a"));
        Assert.AreEqual("v", GetString(db, "zz"));
    }

    /// <summary>
    /// The ingested file carries its entry stats from the table properties read while validating it, so the picker and the
    /// snapshot release path see it as standalone without a table read (upstream: MaybeInitializeFileMetaData).
    /// </summary>
    [TestMethod]
    public void IngestedStandaloneFileHasEntryStats()
    {
        MemoryFileSystem fs = NewFsWithExt();
        var cfo = new ColumnFamilyOptions { CompactionStyle = CompactionStyle.Universal, DisableAutoCompactions = true };
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
        db.IngestExternalFiles([WriteSst(fs, "/ext/old.sst", ("a", "old-a"), ("m", "old-m"))], new IngestExternalFileOptions());
        db.IngestExternalFiles([WriteRangeDeletionSst(fs, "/ext/rd.sst", "a", "n")], new IngestExternalFileOptions());
        VersionStorageInfo vs = db.DefaultColumnFamily.Data.Current.Storage;
        ReadOnlySpan<FileMetaData> l5 = vs.LevelFiles(5);
        Assert.AreEqual(1, l5.Length);
        Assert.IsTrue(l5[0].EntryStatsKnown);
        Assert.IsTrue(l5[0].MarkedForCompaction);
        Assert.IsTrue(l5[0].FileIsStandAloneRangeTombstone());
        Assert.AreEqual(l5[0].SmallestSeqno, vs.StandaloneRangeTombstoneFilesMarkThreshold);
        Assert.IsFalse(vs.LevelFiles(6)[0].FileIsStandAloneRangeTombstone());
    }

    /// <summary>
    /// The leveled picker never defers a standalone file (CompactionPicker::PickFilesMarkedForCompaction with a
    /// skip_marked_file that returns false): it is compacted into L6 although a snapshot predates it, and the tombstone
    /// is kept for the snapshot.
    /// </summary>
    [TestMethod]
    public void LeveledCompactionDoesNotDeferStandaloneFile()
    {
        MemoryFileSystem fs = NewFsWithExt();
        var recorder = new CompactionRecorder();
        using RocksDatabase db = RocksDatabase.Open(DbPath, OptionsWith(fs, recorder), new ColumnFamilyOptions());
        db.IngestExternalFiles([WriteSst(fs, "/ext/old.sst", ("a", "old-a"), ("m", "old-m"))], new IngestExternalFileOptions());
        using Snapshot snapshot = db.GetSnapshot();
        db.IngestExternalFiles([WriteRangeDeletionSst(fs, "/ext/rd.sst", "a", "n")], new IngestExternalFileOptions());
        db.WaitForCompact();
        Assert.AreEqual("0", db.DefaultColumnFamily.GetProperty("rocksdb.num-files-at-level5"));
        Assert.AreEqual(1, recorder.Seen.Count);
        Assert.AreEqual(CompactionReason.FilesMarkedForCompaction, recorder.Seen[0].Reason);
        Assert.AreEqual("old-a", GetString(db, "a", snapshot));
        Assert.IsNull(GetString(db, "a"));
    }
}
