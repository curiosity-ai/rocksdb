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
            return new VersionStorageInfo(icmp, Files.Length, Options, arrays);
        }

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
}
