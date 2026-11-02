using System.Text;
using RocksDb.Managed.Db;

namespace RocksDb.Managed.Tests.Db;

/// <summary>
/// Picker tests on synthetic versions. Ports of cases from db/compaction/compaction_picker_test.cc and the
/// base-level / level-target cases of db/version_set_test.cc (VersionStorageInfoTest).
/// </summary>
[TestClass]
public sealed class CompactionPickerTests
{
    private sealed class Fixture
    {
        public readonly ColumnFamilyOptions Options;
        public readonly List<FileMetaData>[] Files;
        public readonly List<RocksDb.Managed.Db.Blob.BlobFileMetaData> BlobFiles = new();
        private ulong _epoch = 1000;

        public Fixture(int numLevels, CompactionStyle style = CompactionStyle.Level, bool dynamic = false)
        {
            // CompactionPickerTest defaults: level_compaction_dynamic_level_bytes off unless the case enables it.
            Options = new ColumnFamilyOptions
            {
                NumLevels = numLevels,
                CompactionStyle = style,
                LevelCompactionDynamicLevelBytes = dynamic,
                Ttl = 0,
            };
            Files = new List<FileMetaData>[numLevels];
            for (int i = 0; i < numLevels; i++) Files[i] = new List<FileMetaData>();
        }

        private static byte[] IKey(string user, ulong seq, EntryType t)
        {
            byte[] u = Encoding.ASCII.GetBytes(user);
            var k = new byte[u.Length + 8];
            DbFormat.AppendInternalKey(k, u, seq, t);
            return k;
        }

        /// <summary>CompactionPickerTestBase::Add.</summary>
        public FileMetaData Add(int level, ulong number, string smallest, string largest, ulong size = 1, ulong smallestSeq = 100, ulong largestSeq = 100)
        {
            var f = new FileMetaData
            {
                FileNumber = number,
                FileSize = size,
                Smallest = IKey(smallest, smallestSeq, EntryType.Value),
                Largest = IKey(largest, largestSeq, EntryType.Value),
                SmallestSeqno = smallestSeq,
                LargestSeqno = largestSeq,
                EpochNumber = level == 0 ? _epoch-- : 0,
            };
            Files[level].Add(f);
            return f;
        }

        public VersionStorageInfo Build()
        {
            var arrays = new FileMetaData[Files.Length][];
            InternalKeyComparator icmp = InternalKeyComparator.Bytewise;
            for (int l = 0; l < Files.Length; l++)
            {
                List<FileMetaData> files = Files[l];
                if (l == 0) files.Sort(static (a, b) => b.EpochNumber.CompareTo(a.EpochNumber));
                else files.Sort((a, b) => icmp.Compare(a.Smallest, b.Smallest));
                arrays[l] = files.ToArray();
            }
            return new VersionStorageInfo(icmp, Files.Length, Options, arrays, BlobFiles.ToArray());
        }

        /// <summary>CompactionPickerTestBase::AddBlobFile (one blob, no garbage, no linked SSTs).</summary>
        public void AddBlobFile(ulong number, ulong totalBlobBytes) =>
            BlobFiles.Add(new RocksDb.Managed.Db.Blob.BlobFileMetaData(new BlobFileAddition(number, 1, totalBlobBytes, "", []), 0, 0, []));

        public UniversalCompactionPicker UniversalPicker() => new(Options, InternalKeyComparator.Bytewise);
        public FifoCompactionPicker FifoPicker() => new(Options, InternalKeyComparator.Bytewise);

        public LevelCompactionPicker LevelPicker() => new(Options, InternalKeyComparator.Bytewise);
    }

    private static List<ulong> Numbers(CompactionInputFiles inputs)
    {
        var l = new List<ulong>();
        foreach (FileMetaData f in inputs.Files) l.Add(f.FileNumber);
        l.Sort();
        return l;
    }

    [TestMethod]
    public void Empty()
    {
        var fx = new Fixture(6);
        VersionStorageInfo vs = fx.Build();
        Assert.IsFalse(fx.LevelPicker().NeedsCompaction(vs));
        Assert.IsNull(fx.LevelPicker().PickCompaction(vs, []));
    }

    [TestMethod]
    public void Single()
    {
        var fx = new Fixture(6);
        fx.Options.Level0FileNumCompactionTrigger = 2;
        fx.Add(0, 1, "p", "q");
        Assert.IsNull(fx.LevelPicker().PickCompaction(fx.Build(), []));
    }

    [TestMethod]
    public void Level0Trigger()
    {
        var fx = new Fixture(6);
        fx.Options.Level0FileNumCompactionTrigger = 2;
        fx.Add(0, 1, "150", "200");
        fx.Add(0, 2, "200", "250");
        Compaction? c = fx.LevelPicker().PickCompaction(fx.Build(), []);
        Assert.IsNotNull(c);
        CollectionAssert.AreEqual(new List<ulong> { 1, 2 }, Numbers(c.Inputs(0)));
        Assert.AreEqual(1, c.OutputLevel);
        Assert.AreEqual(CompactionReason.LevelL0FilesNum, c.Reason);
    }

    [TestMethod]
    public void Level1Trigger()
    {
        var fx = new Fixture(6);
        fx.Add(1, 66, "150", "200", 1000000000);
        Compaction? c = fx.LevelPicker().PickCompaction(fx.Build(), []);
        Assert.IsNotNull(c);
        Assert.AreEqual(1, c.NumInputFiles(0));
        Assert.AreEqual(66UL, c.Input(0, 0).FileNumber);
    }

    [TestMethod]
    public void Level1Trigger2()
    {
        var fx = new Fixture(6);
        fx.Options.TargetFileSizeBase = 10000000000;
        fx.Add(1, 66, "150", "200", 1000000001);
        fx.Add(1, 88, "201", "300", 1000000000);
        fx.Add(2, 6, "150", "179", 1000000000);
        fx.Add(2, 7, "180", "220", 1000000000);
        fx.Add(2, 8, "221", "300", 1000000000);
        Compaction? c = fx.LevelPicker().PickCompaction(fx.Build(), []);
        Assert.IsNotNull(c);
        Assert.AreEqual(1, c.NumInputFiles(0));
        Assert.AreEqual(2, c.NumInputFiles(1));
        Assert.AreEqual(66UL, c.Input(0, 0).FileNumber);
        Assert.AreEqual(6UL, c.Input(1, 0).FileNumber);
        Assert.AreEqual(7UL, c.Input(1, 1).FileNumber);
        Assert.IsFalse(c.IsTrivialMove());
    }

    [TestMethod]
    public void LevelMaxScore()
    {
        var fx = new Fixture(6);
        fx.Options.TargetFileSizeBase = 10000000;
        fx.Options.MaxBytesForLevelBase = 10 * 1024 * 1024;
        fx.Add(0, 1, "150", "200", 1000000);
        // Level 1 score 1.2
        fx.Add(1, 66, "150", "200", 6000000);
        fx.Add(1, 88, "201", "300", 6000000);
        // Level 2 score 1.8. File 7 is the largest. Should be picked
        fx.Add(2, 6, "150", "179", 60000000);
        fx.Add(2, 7, "180", "220", 60000001);
        fx.Add(2, 8, "221", "300", 60000000);
        // Level 3 score slightly larger than 1
        fx.Add(3, 26, "150", "170", 260000000);
        fx.Add(3, 27, "171", "179", 260000000);
        fx.Add(3, 28, "191", "220", 260000000);
        fx.Add(3, 29, "221", "300", 260000000);
        Compaction? c = fx.LevelPicker().PickCompaction(fx.Build(), []);
        Assert.IsNotNull(c);
        Assert.AreEqual(1, c.NumInputFiles(0));
        Assert.AreEqual(7UL, c.Input(0, 0).FileNumber);
    }

    [TestMethod]
    public void NeedsCompactionLevel()
    {
        const int Levels = 6, FileCount = 20;
        for (int level = 0; level < Levels - 1; ++level)
        {
            ulong fileSize = new Fixture(Levels).Build().MaxBytesForLevel(level) * 2 / FileCount;
            for (int fileCount = 1; fileCount <= FileCount; ++fileCount)
            {
                var fx = new Fixture(Levels);
                for (int i = 0; i < fileCount; ++i)
                {
                    fx.Add(level, (ulong)i, ((i + 100) * 1000).ToString(System.Globalization.CultureInfo.InvariantCulture),
                        ((i + 100) * 1000 + 999).ToString(System.Globalization.CultureInfo.InvariantCulture), fileSize, (ulong)(i * 100), (ulong)(i * 100 + 99));
                }
                VersionStorageInfo vs = fx.Build();
                Assert.AreEqual(level, vs.CompactionScoreLevel(0));
                Assert.AreEqual(vs.CompactionScore(0) >= 1, fx.LevelPicker().NeedsCompaction(vs));
            }
        }
    }

    [TestMethod]
    public void Level0TriggerDynamic()
    {
        var fx = new Fixture(7, dynamic: true);
        fx.Options.Level0FileNumCompactionTrigger = 2;
        fx.Options.MaxBytesForLevelBase = 200;
        fx.Options.MaxBytesForLevelMultiplier = 10;
        fx.Add(0, 1, "150", "200");
        fx.Add(0, 2, "200", "250");
        Compaction? c = fx.LevelPicker().PickCompaction(fx.Build(), []);
        Assert.IsNotNull(c);
        CollectionAssert.AreEqual(new List<ulong> { 1, 2 }, Numbers(c.Inputs(0)));
        Assert.AreEqual(1, c.NumInputLevels);
        Assert.AreEqual(6, c.OutputLevel); // empty DB: L0 compacts to the last level
    }

    [TestMethod]
    public void Level0TriggerDynamic2()
    {
        var fx = new Fixture(7, dynamic: true);
        fx.Options.Level0FileNumCompactionTrigger = 2;
        fx.Options.MaxBytesForLevelBase = 200;
        fx.Options.MaxBytesForLevelMultiplier = 10;
        fx.Add(0, 1, "150", "200");
        fx.Add(0, 2, "200", "250");
        fx.Add(6, 3, "200", "250", 300);
        VersionStorageInfo vs = fx.Build();
        Assert.AreEqual(5, vs.BaseLevel);
        Compaction? c = fx.LevelPicker().PickCompaction(vs, []);
        Assert.IsNotNull(c);
        CollectionAssert.AreEqual(new List<ulong> { 1, 2 }, Numbers(c.Inputs(0)));
        Assert.AreEqual(1, c.NumInputLevels);
        Assert.AreEqual(5, c.OutputLevel);
    }

    [TestMethod]
    public void DynamicBaseLevelAndTargets()
    {
        // VersionStorageInfoTest.MaxBytesForLevelDynamic-style: base level moves up as the last level grows.
        var fx = new Fixture(6, dynamic: true);
        fx.Options.MaxBytesForLevelBase = 1000;
        fx.Options.MaxBytesForLevelMultiplier = 5;
        fx.Add(5, 1, "1", "2", 500);
        VersionStorageInfo vs = fx.Build();
        Assert.AreEqual(5, vs.BaseLevel);
        Assert.AreEqual(1000UL, vs.MaxBytesForLevel(5));

        fx = new Fixture(6, dynamic: true);
        fx.Options.MaxBytesForLevelBase = 1000;
        fx.Options.MaxBytesForLevelMultiplier = 5;
        fx.Add(5, 1, "1", "2", 6000);
        vs = fx.Build();
        // 6000 / 5 = 1200 > base (1000): one more level up, 240 <= 1000.
        Assert.AreEqual(3, vs.BaseLevel);
        Assert.AreEqual(1000UL, vs.MaxBytesForLevel(3));
        Assert.AreEqual(1200UL, vs.MaxBytesForLevel(4));
        Assert.AreEqual(6000UL, vs.MaxBytesForLevel(5));

        fx = new Fixture(6, dynamic: true);
        fx.Options.MaxBytesForLevelBase = 1000;
        fx.Options.MaxBytesForLevelMultiplier = 5;
        fx.Add(5, 1, "1", "2", 600000);
        vs = fx.Build();
        Assert.AreEqual(1, vs.BaseLevel);
        Assert.AreEqual(600000UL, vs.MaxBytesForLevel(5));
        Assert.AreEqual(ulong.MaxValue, vs.MaxBytesForLevel(0));
    }

    [TestMethod]
    public void OverlappingUserKeysExpandToCleanCut()
    {
        // CompactionPickerTest.OverlappingUserKeys: a user key spanning two L1 files pulls both in.
        var fx = new Fixture(6);
        fx.Options.MaxBytesForLevelBase = 1000;
        fx.Add(1, 1, "100", "150", 1, 200, 200);
        fx.Add(1, 2, "150", "200", 1000000000, 50, 50);
        Compaction? c = fx.LevelPicker().PickCompaction(fx.Build(), []);
        Assert.IsNotNull(c);
        Assert.AreEqual(1, c.NumInputLevels);
        CollectionAssert.AreEqual(new List<ulong> { 1, 2 }, Numbers(c.Inputs(0)));
    }

    [TestMethod]
    public void L0ExpansionAndOtherL0Files()
    {
        // L0 compaction pulls in every L0 file transitively overlapping the picked range.
        var fx = new Fixture(6);
        fx.Options.Level0FileNumCompactionTrigger = 3;
        fx.Add(0, 1, "100", "150");
        fx.Add(0, 2, "140", "300");
        fx.Add(0, 3, "290", "400");
        fx.Add(0, 4, "500", "600");
        fx.Add(1, 10, "120", "130");
        fx.Add(1, 11, "700", "800");
        VersionStorageInfo vs = fx.Build();
        var outFiles = new List<FileMetaData>();
        vs.GetOverlappingInputs(0, fx.Files[0].Find(f => f.FileNumber == 1)!.Smallest, fx.Files[0].Find(f => f.FileNumber == 1)!.Largest, outFiles);
        var nums = outFiles.ConvertAll(f => f.FileNumber);
        nums.Sort();
        CollectionAssert.AreEqual(new List<ulong> { 1, 2, 3 }, nums);
        Compaction? c = fx.LevelPicker().PickCompaction(vs, []);
        Assert.IsNotNull(c);
        Assert.AreEqual(0, c.StartLevel);
        Assert.AreEqual(1, c.OutputLevel);
        // The two oldest files (4, then 3) neither overlap each other nor L1: moved trivially (TryPickL0TrivialMove).
        CollectionAssert.AreEqual(new List<ulong> { 3, 4 }, Numbers(c.Inputs(0)));
        Assert.IsTrue(c.IsTrivialMove());
    }

    [TestMethod]
    public void L0TrivialMove()
    {
        // CompactionPickerTest.L0TrivialMove1: non-overlapping oldest L0 files not overlapping L1 move trivially.
        var fx = new Fixture(6);
        fx.Options.Level0FileNumCompactionTrigger = 4;
        fx.Options.MaxCompactionBytes = 10000000;
        fx.Add(0, 1, "100", "150", 3000, 50, 50);
        fx.Add(0, 2, "151", "200", 3000, 40, 40);
        fx.Add(0, 3, "301", "350", 3000, 30, 30);
        fx.Add(0, 4, "251", "300", 3000, 20, 20);
        fx.Add(1, 5, "400", "500", 3000);
        Compaction? c = fx.LevelPicker().PickCompaction(fx.Build(), []);
        Assert.IsNotNull(c);
        Assert.AreEqual(1, c.NumInputLevels);
        Assert.IsTrue(c.NumInputFiles(0) >= 1);
        Assert.IsTrue(c.IsTrivialMove());
    }

    [TestMethod]
    public void IntraL0WhenL0ToBaseIsBlocked()
    {
        // CompactionPickerTest.IntraL0MaxCompactionBytesNotHit-style: an ongoing compaction of the base level blocks
        // L0->L1, so a span of L0 files is compacted within L0.
        var fx = new Fixture(6);
        fx.Options.Level0FileNumCompactionTrigger = 3;
        fx.Options.MaxCompactionBytes = 1000000;
        for (int i = 0; i < 6; i++) fx.Add(0, (ulong)(i + 1), "100", "150", 100);
        FileMetaData l1 = fx.Add(1, 10, "100", "150", 100);
        VersionStorageInfo vs = fx.Build();
        l1.BeingCompacted = true;
        vs.ComputeCompactionScore();
        Compaction? c = fx.LevelPicker().PickCompaction(vs, []);
        Assert.IsNotNull(c);
        Assert.AreEqual(0, c.OutputLevel);
        Assert.AreEqual(0, c.StartLevel);
        Assert.IsTrue(c.NumInputFiles(0) >= 4);
    }

    [TestMethod]
    public void RunningCompactionBlocksSecondL0Compaction()
    {
        var fx = new Fixture(6);
        fx.Options.Level0FileNumCompactionTrigger = 2;
        fx.Add(0, 1, "150", "200");
        fx.Add(0, 2, "200", "250");
        VersionStorageInfo vs = fx.Build();
        LevelCompactionPicker picker = fx.LevelPicker();
        Compaction? c = picker.PickCompaction(vs, []);
        Assert.IsNotNull(c);
        Assert.IsTrue(c.Input(0, 0).BeingCompacted);
        Assert.IsNull(picker.PickCompaction(vs, []));
        picker.ReleaseCompactionFiles(c, succeeded: false);
        Assert.IsFalse(c.Input(0, 0).BeingCompacted);
        vs.ComputeCompactionScore();
        Assert.IsNotNull(picker.PickCompaction(vs, []));
    }

    [TestMethod]
    public void BottommostDetectionAndKeyNotExistsBeyondOutputLevel()
    {
        var fx = new Fixture(6);
        fx.Options.MaxBytesForLevelBase = 1000;
        fx.Add(1, 1, "100", "200", 100000);
        fx.Add(2, 2, "100", "150", 10);
        fx.Add(3, 3, "300", "400", 10);
        VersionStorageInfo vs = fx.Build();
        Compaction? c = fx.LevelPicker().PickCompaction(vs, []);
        Assert.IsNotNull(c);
        Assert.AreEqual(1, c.StartLevel);
        Assert.AreEqual(2, c.OutputLevel);
        // L3 holds 300..400, which doesn't overlap [100, 200]: the output is bottommost for this key range.
        Assert.IsTrue(c.BottommostLevel);
        var ptrs = new int[6];
        Assert.IsTrue(c.KeyNotExistsBeyondOutputLevel("120"u8, ptrs));

        fx = new Fixture(6);
        fx.Options.MaxBytesForLevelBase = 1000;
        fx.Add(1, 1, "100", "200", 100000);
        fx.Add(2, 2, "100", "150", 10);
        fx.Add(3, 3, "180", "400", 10);
        c = fx.LevelPicker().PickCompaction(fx.Build(), []);
        Assert.IsNotNull(c);
        Assert.IsFalse(c.BottommostLevel);
        ptrs = new int[6];
        Assert.IsTrue(c.KeyNotExistsBeyondOutputLevel("120"u8, ptrs));
        Assert.IsFalse(c.KeyNotExistsBeyondOutputLevel("190"u8, ptrs));
    }

    /// <summary>
    /// Compaction::max_output_file_size_ (db/compaction/compaction.cc): outputs of a non-bottommost compaction with
    /// grandparents may grow to 2x the target file size (they are normally cut at a grandparent boundary first);
    /// bottommost outputs, and outputs without grandparents, are capped at the target size.
    /// </summary>
    [TestMethod]
    public void MaxOutputFileSizeIsTwiceTheTargetAboveTheBottommostLevel()
    {
        const ulong Target = 2 * 1024 * 1024;
        // Grandparent file 3 (L3) overlaps the compaction range: not bottommost.
        var fx = new Fixture(6);
        fx.Options.MaxBytesForLevelBase = 1000;
        fx.Options.TargetFileSizeBase = Target;
        fx.Add(1, 1, "100", "200", 100000);
        fx.Add(2, 2, "100", "150", 10);
        fx.Add(3, 3, "180", "400", 10);
        Compaction? c = fx.LevelPicker().PickCompaction(fx.Build(), []);
        Assert.IsNotNull(c);
        Assert.AreEqual(2, c.OutputLevel);
        Assert.IsFalse(c.BottommostLevel);
        Assert.AreEqual(1, c.Grandparents.Count);
        Assert.AreEqual(Target, c.TargetOutputFileSize);
        Assert.AreEqual(2 * Target, c.MaxOutputFileSize);

        // Nothing below the output level overlaps: bottommost, no grandparents.
        fx = new Fixture(6);
        fx.Options.MaxBytesForLevelBase = 1000;
        fx.Options.TargetFileSizeBase = Target;
        fx.Add(1, 1, "100", "200", 100000);
        fx.Add(2, 2, "100", "150", 10);
        fx.Add(3, 3, "300", "400", 10);
        c = fx.LevelPicker().PickCompaction(fx.Build(), []);
        Assert.IsNotNull(c);
        Assert.IsTrue(c.BottommostLevel);
        Assert.AreEqual(0, c.Grandparents.Count);
        Assert.AreEqual(Target, c.MaxOutputFileSize);

        // L0 -> L1 with no files below L1: bottommost.
        fx = new Fixture(6);
        fx.Options.Level0FileNumCompactionTrigger = 2;
        fx.Options.TargetFileSizeBase = Target;
        fx.Add(0, 1, "150", "200");
        fx.Add(0, 2, "160", "250");
        c = fx.LevelPicker().PickCompaction(fx.Build(), []);
        Assert.IsNotNull(c);
        Assert.AreEqual(Target, c.MaxOutputFileSize);
    }

    [TestMethod]
    public void ManualCompactRangePicksOverlappingFiles()
    {
        var fx = new Fixture(6);
        fx.Add(1, 1, "100", "150");
        fx.Add(1, 2, "200", "250");
        fx.Add(1, 3, "300", "350");
        fx.Add(2, 4, "120", "220");
        VersionStorageInfo vs = fx.Build();
        byte[] begin = new byte[11], end = new byte[11];
        DbFormat.AppendInternalKey(begin, "140"u8, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
        DbFormat.AppendInternalKey(end, "210"u8, 0, 0);
        Compaction? c = fx.LevelPicker().PickCompactionForCompactRange(vs, 1, 2, 0, begin, end, out byte[]? compactionEnd, out bool conflict,
            ulong.MaxValue, false);
        Assert.IsFalse(conflict);
        Assert.IsNotNull(c);
        Assert.IsNull(compactionEnd);
        CollectionAssert.AreEqual(new List<ulong> { 1, 2 }, Numbers(c.Inputs(0)));
        CollectionAssert.AreEqual(new List<ulong> { 4 }, Numbers(c.Inputs(1)));
        Assert.IsTrue(c.IsManual);
    }

    [TestMethod]
    public void UniversalSizeAmplificationPicksAllRuns()
    {
        var fx = new Fixture(7, CompactionStyle.Universal);
        fx.Options.Level0FileNumCompactionTrigger = 2;
        fx.Add(0, 1, "100", "200", 300, 400, 400);
        fx.Add(0, 2, "100", "200", 300, 300, 300);
        fx.Add(6, 3, "100", "200", 100, 1, 1);
        VersionStorageInfo vs = fx.Build();
        var picker = new UniversalCompactionPicker(fx.Options, InternalKeyComparator.Bytewise);
        Assert.IsTrue(picker.NeedsCompaction(vs));
        Compaction? c = picker.PickCompaction(vs, []);
        Assert.IsNotNull(c);
        Assert.AreEqual(CompactionReason.UniversalSizeAmplification, c.Reason);
        Assert.AreEqual(6, c.OutputLevel);
        Assert.AreEqual(3, c.TotalInputFiles);
    }

    [TestMethod]
    public void FifoSizeDeletesOldest()
    {
        var fx = new Fixture(1, CompactionStyle.Fifo);
        fx.Options.CompactionOptionsFifo.MaxTableFilesSize = 250;
        fx.Add(0, 1, "100", "200", 100, 1, 1);
        fx.Add(0, 2, "100", "200", 100, 2, 2);
        fx.Add(0, 3, "100", "200", 100, 3, 3);
        VersionStorageInfo vs = fx.Build();
        var picker = new FifoCompactionPicker(fx.Options, InternalKeyComparator.Bytewise);
        Assert.IsTrue(picker.NeedsCompaction(vs));
        Compaction? c = picker.PickCompaction(vs, []);
        Assert.IsNotNull(c);
        Assert.IsTrue(c.IsDeletionCompaction);
        // The oldest file (file 3, smallest epoch) is dropped.
        CollectionAssert.AreEqual(new List<ulong> { 3 }, Numbers(c.Inputs(0)));
    }

    [TestMethod]
    public void FifoTtlUsesNewestKeyTime()
    {
        // Audit #3 (compaction_picker_fifo.cc PickTTLCompaction): a file expires when its newest key time
        // (TryGetNewestKeyTime) is older than ttl, not its oldest ancestor time. An intra-L0 compaction output holds
        // keys as new as its newest input; it must not be dropped once only its oldest input has expired.
        var fx = new Fixture(1, CompactionStyle.Fifo);
        fx.Options.Ttl = 1000;
        ulong now = (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        FileMetaData compacted = fx.Add(0, 1, "100", "200", 100, 1, 1);
        compacted.OldestAncestorTime = now - 5000;
        compacted.NewestKeyTime = now - 10;
        FileMetaData expired = fx.Add(0, 2, "100", "200", 100, 2, 2);
        expired.OldestAncestorTime = now - 4000;
        expired.NewestKeyTime = now - 3000;
        // Make file 2 the oldest (rightmost) L0 file.
        compacted.EpochNumber = 10;
        expired.EpochNumber = 5;
        VersionStorageInfo vs = fx.Build();
        var picker = new FifoCompactionPicker(fx.Options, InternalKeyComparator.Bytewise);
        Compaction? c = picker.PickCompaction(vs, []);
        Assert.IsNotNull(c);
        Assert.AreEqual(CompactionReason.FifoTtl, c.Reason);
        CollectionAssert.AreEqual(new List<ulong> { 2 }, Numbers(c.Inputs(0)));

        // Both expired by oldest ancestor time but the newer one is still live by its newest key time.
        compacted.EpochNumber = 5;
        expired.EpochNumber = 10;
        vs = fx.Build();
        c = new FifoCompactionPicker(fx.Options, InternalKeyComparator.Bytewise).PickCompaction(vs, []);
        Assert.IsNull(c, "the compacted file (oldest) still holds keys newer than ttl");
    }
    // ---- universal (compaction_picker_test.cc) ----

    private const ulong FileSize = 100000;

    /// <summary>A universal fixture as in the upstream cases (5 levels, periodic compaction 1000 s when asked).</summary>
    private static Fixture Universal(int numLevels, bool periodic = false)
    {
        var fx = new Fixture(numLevels, CompactionStyle.Universal);
        if (periodic) fx.Options.PeriodicCompactionSeconds = 1000;
        return fx;
    }

    [TestMethod]
    public void UniversalPeriodicCompaction1()
    {
        // The case where universal periodic compaction can be picked with some newer files being compacted.
        Fixture fx = Universal(5, periodic: true);
        fx.Add(0, 1, "150", "200", FileSize, 500, 550);
        fx.Add(0, 2, "201", "250", FileSize, 401, 450).BeingCompacted = true;
        fx.Add(0, 4, "260", "300", FileSize, 260, 300);
        fx.Add(3, 5, "010", "080", FileSize, 200, 251);
        FileMetaData f3 = fx.Add(4, 3, "301", "350", FileSize, 101, 150);
        fx.Add(4, 6, "501", "750", FileSize, 101, 150);
        VersionStorageInfo vs = fx.Build();
        vs.FilesMarkedForPeriodicCompaction.Add((4, f3));
        Compaction? c = fx.UniversalPicker().PickCompaction(vs, []);
        Assert.IsNotNull(c);
        Assert.AreEqual(CompactionReason.PeriodicCompaction, c.Reason);
        Assert.AreEqual(4, c.OutputLevel);
        Assert.AreEqual(0, c.StartLevel);
        Assert.AreEqual(1, c.NumInputFiles(0));
    }

    [TestMethod]
    public void UniversalPeriodicCompaction2()
    {
        // Universal periodic compaction does not pick up only the last level if it doesn't cover any marked file.
        Fixture fx = Universal(5, periodic: true);
        FileMetaData f1 = fx.Add(0, 1, "150", "200", FileSize, 500, 550);
        fx.Add(3, 5, "010", "080", FileSize, 200, 251).BeingCompacted = true;
        fx.Add(4, 3, "301", "350", FileSize, 101, 150);
        fx.Add(4, 6, "501", "750", FileSize, 101, 150);
        VersionStorageInfo vs = fx.Build();
        vs.FilesMarkedForPeriodicCompaction.Add((0, f1));
        Assert.IsNull(fx.UniversalPicker().PickCompaction(vs, []));
    }

    [TestMethod]
    public void UniversalPeriodicCompaction3()
    {
        // ... nor only the last sorted run when it is an L0 file that isn't marked.
        Fixture fx = Universal(5, periodic: true);
        FileMetaData f1 = fx.Add(0, 1, "150", "200", FileSize, 500, 550);
        fx.Add(0, 5, "010", "080", FileSize, 200, 251).BeingCompacted = true;
        fx.Add(0, 6, "501", "750", FileSize, 101, 150);
        VersionStorageInfo vs = fx.Build();
        vs.FilesMarkedForPeriodicCompaction.Add((0, f1));
        Assert.IsNull(fx.UniversalPicker().PickCompaction(vs, []));
    }

    [TestMethod]
    public void UniversalPeriodicCompaction5()
    {
        // Single L0 file periodic compaction.
        Fixture fx = Universal(5, periodic: true);
        FileMetaData f6 = fx.Add(0, 6, "150", "200", FileSize, 500, 550);
        VersionStorageInfo vs = fx.Build();
        vs.FilesMarkedForPeriodicCompaction.Add((0, f6));
        UniversalCompactionPicker picker = fx.UniversalPicker();
        Assert.IsTrue(picker.NeedsCompaction(vs));
        Compaction? c = picker.PickCompaction(vs, []);
        Assert.IsNotNull(c);
        Assert.AreEqual(0, c.StartLevel);
        Assert.AreEqual(1, c.NumInputFiles(0));
        Assert.AreEqual(6UL, c.Input(0, 0).FileNumber);
        Assert.AreEqual(4, c.OutputLevel);
        // As upstream (Compaction::IsTrivialMove), a single non-overlapping L0 file may still move; the next periodic
        // pick then rewrites the single L4 run in place (UniversalPeriodicCompaction6).
        Assert.IsTrue(c.IsTrivialMove());
    }

    [TestMethod]
    public void UniversalPeriodicCompaction6()
    {
        // Single sorted run non-L0 periodic compaction.
        Fixture fx = Universal(5, periodic: true);
        fx.Add(4, 5, "150", "200", FileSize, 500, 550);
        FileMetaData f6 = fx.Add(4, 6, "350", "400", FileSize, 500, 550);
        VersionStorageInfo vs = fx.Build();
        vs.FilesMarkedForPeriodicCompaction.Add((4, f6));
        Compaction? c = fx.UniversalPicker().PickCompaction(vs, []);
        Assert.IsNotNull(c);
        Assert.AreEqual(4, c.StartLevel);
        Assert.AreEqual(2, c.NumInputFiles(0));
        Assert.AreEqual(5UL, c.Input(0, 0).FileNumber);
        Assert.AreEqual(6UL, c.Input(0, 1).FileNumber);
        Assert.AreEqual(4, c.OutputLevel);
    }

    [TestMethod]
    public void UniversalIncrementalSpace1()
    {
        Fixture fx = Universal(5);
        fx.Options.MaxCompactionBytes = 555555;
        fx.Options.CompactionOptionsUniversal.Incremental = true;
        fx.Options.CompactionOptionsUniversal.MaxSizeAmplificationPercent = 30;
        fx.Add(0, 1, "150", "200", FileSize, 500, 550);
        fx.Add(2, 2, "010", "080", FileSize, 200, 251);
        fx.Add(3, 5, "310", "380", FileSize, 200, 251);
        fx.Add(3, 6, "410", "880", FileSize, 200, 251);
        fx.Add(3, 7, "910", "980", 1, 200, 251);
        fx.Add(4, 10, "201", "250", FileSize, 101, 150);
        fx.Add(4, 11, "301", "350", FileSize, 101, 150);
        fx.Add(4, 12, "401", "450", FileSize, 101, 150);
        fx.Add(4, 13, "501", "750", FileSize, 101, 150);
        fx.Add(4, 14, "801", "850", FileSize, 101, 150);
        fx.Add(4, 15, "901", "950", FileSize, 101, 150);
        Compaction? c = fx.UniversalPicker().PickCompaction(fx.Build(), []);
        Assert.IsNotNull(c);
        Assert.AreEqual(CompactionReason.UniversalSizeAmplification, c.Reason);
        Assert.AreEqual(4, c.OutputLevel);
        Assert.AreEqual(3, c.StartLevel);
        CollectionAssert.AreEqual(new List<ulong> { 5, 6 }, Numbers(c.Inputs(0)));
        CollectionAssert.AreEqual(new List<ulong> { 11, 12, 13, 14 }, Numbers(c.Inputs(1)));
        Assert.AreEqual(fx.Options.TargetFileSizeBase / 2 * 3, c.MaxCompactionBytes, "GetMaxOverlappingBytes");
    }

    [TestMethod]
    public void UniversalMarkedL0NoOverlap()
    {
        // A delete triggered compaction in single-level universal compacts from the marked file to the oldest run.
        Fixture fx = Universal(1);
        fx.Add(0, 4, "260", "300", 1 * FileSize, 260, 300).MarkedForCompaction = true;
        fx.Add(0, 5, "240", "290", 2 * FileSize, 201, 250);
        fx.Add(0, 3, "301", "350", 4 * FileSize, 101, 150);
        fx.Add(0, 6, "501", "750", 8 * FileSize, 50, 100);
        VersionStorageInfo vs = fx.Build();
        UniversalCompactionPicker picker = fx.UniversalPicker();
        Assert.IsTrue(picker.NeedsCompaction(vs));
        Compaction? c = picker.PickCompaction(vs, []);
        Assert.IsNotNull(c);
        Assert.AreEqual(CompactionReason.FilesMarkedForCompaction, c.Reason);
        Assert.AreEqual(0, c.OutputLevel);
        Assert.AreEqual(0, c.StartLevel);
        Assert.AreEqual(4, c.NumInputFiles(0));
        foreach (FileMetaData f in c.Inputs(0).Files) Assert.IsTrue(f.BeingCompacted);
    }

    [TestMethod]
    public void UniversalMarkedL0WithOverlap()
    {
        // ... and stops at the first file being compacted.
        Fixture fx = Universal(1);
        fx.Add(0, 4, "260", "300", 1 * FileSize, 260, 300).MarkedForCompaction = true;
        fx.Add(0, 5, "240", "290", 2 * FileSize, 201, 250);
        FileMetaData f3 = fx.Add(0, 3, "301", "350", 4 * FileSize, 101, 150);
        fx.Add(0, 6, "501", "750", 8 * FileSize, 50, 100);
        VersionStorageInfo vs = fx.Build();
        f3.BeingCompacted = true;
        Compaction? c = fx.UniversalPicker().PickCompaction(vs, []);
        Assert.IsNotNull(c);
        Assert.AreEqual(CompactionReason.FilesMarkedForCompaction, c.Reason);
        Assert.AreEqual(0, c.OutputLevel);
        CollectionAssert.AreEqual(new List<ulong> { 4, 5 }, Numbers(c.Inputs(0)));
    }

    [TestMethod]
    public void UniversalMarkedFileInMultiLevelGoesToNextNonEmptyLevel()
    {
        // BuildCompactionToNextLevel: an L0 file marked for compaction (CompactOnDeletionCollector) is compacted with
        // the overlapping files of the first non-empty level; the last level is never a start level.
        Fixture fx = Universal(5);
        fx.Options.Level0FileNumCompactionTrigger = 4;
        fx.Add(0, 1, "150", "200", FileSize, 500, 550).MarkedForCompaction = true;
        fx.Add(3, 5, "100", "180", FileSize, 200, 251);
        fx.Add(3, 6, "300", "400", FileSize, 200, 251);
        fx.Add(4, 3, "100", "400", 8 * FileSize, 101, 150);
        VersionStorageInfo vs = fx.Build();
        Assert.AreEqual(1, vs.FilesMarkedForCompaction.Count);
        UniversalCompactionPicker picker = fx.UniversalPicker();
        Assert.IsTrue(picker.NeedsCompaction(vs));
        Compaction? c = picker.PickCompaction(vs, []);
        Assert.IsNotNull(c);
        Assert.AreEqual(CompactionReason.FilesMarkedForCompaction, c.Reason);
        Assert.AreEqual(0, c.StartLevel);
        Assert.AreEqual(3, c.OutputLevel);
        CollectionAssert.AreEqual(new List<ulong> { 1 }, Numbers(c.Inputs(0)));
        CollectionAssert.AreEqual(new List<ulong> { 5 }, Numbers(c.Inputs(1)));
        CollectionAssert.AreEqual(new List<ulong> { 3 }, Numbers(new CompactionInputFiles(4, c.Grandparents)));
    }

    [TestMethod]
    public void UniversalCompressionSizePercentDisablesCompressionOfNewerRuns()
    {
        // PickCompactionToReduceSortedRuns: with compression_size_percent, the output is compressed only if the older
        // runs (not compacted) hold less than that percentage of the data.
        Fixture fx = Universal(5);
        fx.Options.Level0FileNumCompactionTrigger = 2;
        fx.Options.Compression = RocksDb.Managed.Compression.CompressionType.Snappy;
        fx.Options.CompactionOptionsUniversal.CompressionSizePercent = 50;
        fx.Options.CompactionOptionsUniversal.MaxSizeAmplificationPercent = 10000;
        fx.Add(0, 1, "100", "200", 10, 300, 300);
        fx.Add(0, 2, "100", "200", 10, 200, 200);
        fx.Add(4, 3, "100", "200", 1000, 1, 100);
        Compaction? c = fx.UniversalPicker().PickCompaction(fx.Build(), []);
        Assert.IsNotNull(c);
        Assert.AreEqual(CompactionReason.UniversalSizeRatio, c.Reason);
        Assert.AreEqual(3, c.OutputLevel);
        Assert.IsFalse(c.EnableCompression);
        Assert.AreEqual(RocksDb.Managed.Compression.CompressionType.None, c.OutputCompression);
        // Universal flushes are not compressed with compression_size_percent >= 0 (GetCompressionFlush).
        Assert.AreEqual(RocksDb.Managed.Compression.CompressionType.None, TableFactory.GetCompressionFlush(fx.Options));

        fx.Options.CompactionOptionsUniversal.CompressionSizePercent = 99;
        foreach (List<FileMetaData> l in fx.Files) foreach (FileMetaData f in l) f.BeingCompacted = false;
        c = fx.UniversalPicker().PickCompaction(fx.Build(), []);
        Assert.IsNotNull(c);
        Assert.IsTrue(c.EnableCompression);
        Assert.AreEqual(RocksDb.Managed.Compression.CompressionType.Snappy, c.OutputCompression);
    }

    [TestMethod]
    public void UniversalMaxReadAmpLimitsSortedRuns()
    {
        // max_read_amp N > trigger: the sorted-run-count compaction only starts above N runs.
        Fixture fx = Universal(5);
        fx.Options.Level0FileNumCompactionTrigger = 2;
        fx.Options.CompactionOptionsUniversal.MaxReadAmp = 4;
        fx.Options.CompactionOptionsUniversal.SizeRatio = 0;
        fx.Options.CompactionOptionsUniversal.MaxSizeAmplificationPercent = 10000;
        // Sizes doubling from newest to oldest: no size-ratio compaction (each run larger than all newer ones).
        fx.Add(0, 1, "100", "200", 10, 400, 400);
        fx.Add(0, 2, "100", "200", 21, 300, 300);
        fx.Add(0, 3, "100", "200", 43, 200, 200);
        fx.Add(0, 4, "100", "200", 87, 100, 100);
        Assert.IsNull(fx.UniversalPicker().PickCompaction(fx.Build(), []), "4 runs <= max_read_amp");
        fx.Add(0, 5, "100", "200", 175, 50, 50);
        Compaction? c = fx.UniversalPicker().PickCompaction(fx.Build(), []);
        Assert.IsNotNull(c);
        Assert.AreEqual(CompactionReason.UniversalSortedRunNum, c.Reason);
        Assert.AreEqual(2, c.NumInputFiles(0), "5 runs - 4 + 1");
    }

    // ---- FIFO (compaction_picker_test.cc) ----

    [TestMethod]
    public void FifoToCold()
    {
        // FIFOToCold1: the oldest file passed the cold threshold; with an unknown newest key time the next newer file's
        // oldest ancestor time is the estimate.
        foreach (bool newestKeyTimeKnown in new[] { false, true })
        {
            var fx = new Fixture(1, CompactionStyle.Fifo);
            const ulong ColdThreshold = 2000;
            fx.Options.CompactionOptionsFifo.MaxTableFilesSize = FileSize * 100000;
            fx.Options.CompactionOptionsFifo.FileTemperatureAgeThresholds = [new FileTemperatureAge(Temperature.Cold, ColdThreshold)];
            fx.Options.Level0FileNumCompactionTrigger = 100;
            ulong thresholdTime = (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds() - ColdThreshold;
            FileMetaData f4 = fx.Add(0, 4, "260", "300", FileSize, 2500, 2600);
            FileMetaData f3 = fx.Add(0, 3, "200", "300", 4 * FileSize, 2300, 2400);
            if (newestKeyTimeKnown)
            {
                f4.NewestKeyTime = thresholdTime - 2000;
                f3.NewestKeyTime = thresholdTime - 3000;
            }
            else
            {
                f4.OldestAncestorTime = thresholdTime - 2000;
                f3.OldestAncestorTime = thresholdTime - 3000;
            }
            VersionStorageInfo vs = fx.Build();
            FifoCompactionPicker picker = fx.FifoPicker();
            Assert.IsTrue(picker.NeedsCompaction(vs));
            Compaction? c = picker.PickCompaction(vs, []);
            Assert.IsNotNull(c);
            Assert.AreEqual(CompactionReason.ChangeTemperature, c.Reason);
            Assert.AreEqual(Temperature.Cold, c.OutputTemperature);
            CollectionAssert.AreEqual(new List<ulong> { 3 }, Numbers(c.Inputs(0)));
            Assert.IsFalse(c.IsTrivialMove());
        }
    }

    [TestMethod]
    public void FifoToHotAndWarm()
    {
        // Multiple thresholds: the oldest file that needs a change gets the temperature of the oldest threshold it passed.
        var fx = new Fixture(1, CompactionStyle.Fifo);
        const ulong WarmThreshold = 10000, HotThreshold = 2000;
        fx.Options.CompactionOptionsFifo.MaxTableFilesSize = FileSize * 100000;
        fx.Options.CompactionOptionsFifo.FileTemperatureAgeThresholds =
            [new FileTemperatureAge(Temperature.Hot, HotThreshold), new FileTemperatureAge(Temperature.Warm, WarmThreshold)];
        fx.Options.Level0FileNumCompactionTrigger = 100;
        ulong now = (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        ulong hot = now - HotThreshold, warm = now - WarmThreshold;
        fx.Add(0, 6, "240", "290", 2 * FileSize, 2900, 3000).NewestKeyTime = now - 100;
        fx.Add(0, 5, "240", "290", 2 * FileSize, 2700, 2800).NewestKeyTime = hot + 100;
        fx.Add(0, 4, "260", "300", 1 * FileSize, 2500, 2600).NewestKeyTime = hot - 200;
        fx.Add(0, 3, "200", "300", 4 * FileSize, 2300, 2400).NewestKeyTime = warm - 100;
        fx.Add(0, 2, "200", "300", 4 * FileSize, 2100, 2200).NewestKeyTime = warm - 4000;
        fx.Add(0, 1, "200", "300", 4 * FileSize, 2000, 2100).NewestKeyTime = warm - 5000;
        Compaction? c = fx.FifoPicker().PickCompaction(fx.Build(), []);
        Assert.IsNotNull(c);
        Assert.AreEqual(CompactionReason.ChangeTemperature, c.Reason);
        Assert.AreEqual(Temperature.Warm, c.OutputTemperature);
        CollectionAssert.AreEqual(new List<ulong> { 1 }, Numbers(c.Inputs(0)));
    }

    /// <summary>SetupFIFORatioBased.</summary>
    private static Fixture FifoRatioBased(ulong maxTableFilesSize, ulong maxDataFilesSize, int trigger, bool useKvRatio = true)
    {
        var fx = new Fixture(1, CompactionStyle.Fifo);
        fx.Options.CompactionOptionsFifo.MaxTableFilesSize = maxTableFilesSize;
        fx.Options.CompactionOptionsFifo.MaxDataFilesSize = maxDataFilesSize;
        fx.Options.CompactionOptionsFifo.AllowCompaction = true;
        fx.Options.CompactionOptionsFifo.UseKvRatioCompaction = useKvRatio;
        fx.Options.Level0FileNumCompactionTrigger = trigger;
        // max_compaction_bytes = 0: auto-calculate the target (SanitizeCfOptions keeps 0 with use_kv_ratio_compaction).
        fx.Options.MaxCompactionBytes = 0;
        return fx;
    }

    [TestMethod]
    public void FifoRatioBasedCompactionFileCountThreshold()
    {
        // Fewer than trigger: no compaction.
        Fixture fx = FifoRatioBased(10 << 20, 1UL << 30, 4);
        fx.Add(0, 1, "100", "200", 64 * 1024);
        fx.Add(0, 2, "200", "300", 64 * 1024);
        fx.Add(0, 3, "300", "400", 64 * 1024);
        for (ulong i = 0; i < 3; i++) fx.AddBlobFile(100 + i, 64UL << 20);
        Assert.IsNull(fx.FifoPicker().PickCompaction(fx.Build(), []));

        // Exactly trigger: compaction fires (sst_ratio ~ 240KB/256MB, target ~ 250KB).
        fx = FifoRatioBased(10 << 20, 1UL << 30, 4);
        fx.Add(0, 1, "100", "200", 64 * 1024);
        fx.Add(0, 2, "200", "300", 32 * 1024);
        fx.Add(0, 3, "300", "400", 48 * 1024);
        fx.Add(0, 4, "400", "500", 96 * 1024);
        for (ulong i = 0; i < 4; i++) fx.AddBlobFile(100 + i, 64UL << 20);
        Compaction? c = fx.FifoPicker().PickCompaction(fx.Build(), []);
        Assert.IsNotNull(c);
        Assert.AreEqual(CompactionReason.FifoReduceNumFiles, c.Reason);
        Assert.AreEqual(0, c.OutputLevel);

        // More than trigger.
        fx = FifoRatioBased(100 << 20, 500UL << 20, 4);
        string[] k = ["100", "200", "300", "400", "500", "600", "700", "800"];
        ulong[] sizes = [64, 32, 48, 96, 64, 48, 64, 64];
        for (int i = 0; i < 8; i++) fx.Add(0, (ulong)i + 1, k[i], k[i][0] + "99", sizes[i] * 1024);
        for (ulong i = 0; i < 8; i++) fx.AddBlobFile(100 + i, 50UL << 20);
        c = fx.FifoPicker().PickCompaction(fx.Build(), []);
        Assert.IsNotNull(c);
        Assert.AreEqual(CompactionReason.FifoReduceNumFiles, c.Reason);
        Assert.IsTrue(c.NumInputFiles(0) >= 2);
    }

    [TestMethod]
    public void FifoBlobAwareSizeDropping()
    {
        // SST = 200KB, blob = 500MB, limit = 200MB: effective size ~500MB, drops from L0.
        Fixture fx = FifoRatioBased(200UL << 20, 200UL << 20, 4, useKvRatio: false);
        for (int i = 0; i < 5; i++) fx.Add(0, (ulong)i + 1, (100 * (i + 1)).ToString(System.Globalization.CultureInfo.InvariantCulture), (100 * (i + 1) + 99).ToString(System.Globalization.CultureInfo.InvariantCulture), 40 * 1024);
        for (ulong i = 0; i < 5; i++) fx.AddBlobFile(100 + i, 100UL << 20);
        VersionStorageInfo vs = fx.Build();
        Assert.IsTrue(vs.CompactionScore(0) > 1, "FIFOBlobAwareScoreComputation: blob files count towards the score");
        Compaction? c = fx.FifoPicker().PickCompaction(vs, []);
        Assert.IsNotNull(c);
        Assert.AreEqual(CompactionReason.FifoMaxSize, c.Reason);
        Assert.IsTrue(c.NumInputFiles(0) >= 1);
        Assert.IsTrue(c.IsDeletionCompaction);

        // Without max_data_files_size only the 200KB of SSTs count: nothing to drop.
        fx.Options.CompactionOptionsFifo.MaxDataFilesSize = 0;
        foreach (FileMetaData f in fx.Files[0]) f.BeingCompacted = false;
        Assert.IsNull(fx.FifoPicker().PickCompaction(fx.Build(), []));
    }

    // ---- output cutting ----

    [TestMethod]
    public void RoundRobinCompactionSetsOutputSplitKey()
    {
        // Compaction::Compaction: with kRoundRobin, the output level's compact cursor inside the compaction's user-key
        // range becomes the output split key (CompactionOutputs cuts the first output before it).
        var fx = new Fixture(6);
        fx.Options.CompactionPri = CompactionPri.RoundRobin;
        fx.Options.Level0FileNumCompactionTrigger = 2;
        fx.Add(0, 1, "150", "200");
        fx.Add(0, 2, "200", "250");
        fx.Add(1, 3, "100", "300");
        LevelCompactionPicker picker = fx.LevelPicker();
        byte[] cursor = new byte[3 + 8];
        DbFormat.AppendInternalKey(cursor, "180"u8, 100, EntryType.Value);
        picker.CompactCursors[1] = cursor;
        Compaction? c = picker.PickCompaction(fx.Build(), []);
        Assert.IsNotNull(c);
        Assert.AreEqual(1, c.OutputLevel);
        Assert.AreSame(cursor, c.OutputSplitKey);

        // A cursor outside the range is ignored.
        var fx2 = new Fixture(6);
        fx2.Options.CompactionPri = CompactionPri.RoundRobin;
        fx2.Options.Level0FileNumCompactionTrigger = 2;
        fx2.Add(0, 1, "150", "200");
        fx2.Add(0, 2, "200", "250");
        LevelCompactionPicker picker2 = fx2.LevelPicker();
        byte[] outside = new byte[3 + 8];
        DbFormat.AppendInternalKey(outside, "900"u8, 100, EntryType.Value);
        picker2.CompactCursors[1] = outside;
        c = picker2.PickCompaction(fx2.Build(), []);
        Assert.IsNotNull(c);
        Assert.IsNull(c.OutputSplitKey);
    }

    [TestMethod]
    public void FilesToCutForTtlAreOldLargeOutputLevelInputs()
    {
        // CompactionOutputs::FillFilesToCutForTtl: leveled + kMinOverlappingRatio + ttl, non-bottommost: output-level
        // inputs older than ttl/2 and larger than target_file_size_base/2.
        var fx = new Fixture(6);
        fx.Options.Ttl = 1000;
        fx.Options.TargetFileSizeBase = 100;
        ulong now = (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        FileMetaData l1 = fx.Add(1, 1, "100", "500", 100);
        l1.OldestAncestorTime = now;
        FileMetaData oldBig = fx.Add(2, 2, "100", "200", 200);
        oldBig.OldestAncestorTime = now - 900;
        FileMetaData oldSmall = fx.Add(2, 3, "210", "300", 10);
        oldSmall.OldestAncestorTime = now - 900;
        FileMetaData young = fx.Add(2, 4, "310", "400", 200);
        young.OldestAncestorTime = now - 100;
        fx.Add(3, 5, "100", "900", 1000); // below: not bottommost
        VersionStorageInfo vs = fx.Build();
        var c = new Compaction(vs, fx.Options, [new CompactionInputFiles(1, [l1]), new CompactionInputFiles(2, [oldBig, oldSmall, young])], 2, 100,
            ulong.MaxValue, 0, RocksDb.Managed.Compression.CompressionType.None, [], CompactionReason.LevelMaxLevelSize, 1, false, false);
        List<FileMetaData>? cut = CompactionJob.FillFilesToCutForTtl(c);
        Assert.IsNotNull(cut);
        CollectionAssert.AreEqual(new List<ulong> { 2 }, Numbers(new CompactionInputFiles(2, cut)));
        fx.Options.CompactionPri = CompactionPri.OldestLargestSeqFirst;
        Assert.IsNull(CompactionJob.FillFilesToCutForTtl(c));
    }
}
