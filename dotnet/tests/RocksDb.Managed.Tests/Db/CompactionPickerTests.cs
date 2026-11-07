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

        public VersionStorageInfo Build() => Build(null);

        public VersionStorageInfo Build(VersionBuildContext? context)
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
            return new VersionStorageInfo(icmp, Files.Length, Options, arrays, BlobFiles.ToArray(), context);
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
        Compaction? c = picker.PickCompaction(fx.Build(new VersionBuildContext { CompactCursors = [new CompactCursor(1, cursor)] }), []);
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
        c = picker2.PickCompaction(fx2.Build(new VersionBuildContext { CompactCursors = [new CompactCursor(1, outside)] }), []);
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
    // ---- read-triggered compaction (compaction_picker_test.cc ReadTriggered*) -------------------------------------

    private static void SetCollapsibleReads(FileMetaData f, uint n) => f.AddCollapsibleEntryReadsSampled(n);

    [TestMethod]
    public void ReadTriggeredCompactionDisabled()
    {
        var fx = new Fixture(6);
        fx.Options.ReadTriggeredCompactionThreshold = 0.0; // threshold=0 means disabled, no files should be marked
        SetCollapsibleReads(fx.Add(0, 1, "100", "200", 1000), 999999);
        Assert.AreEqual(0, fx.Build().ReadTriggeredCompactionFiles.Count);
    }

    [TestMethod]
    public void ReadTriggeredCompactionBelowThreshold()
    {
        var fx = new Fixture(6);
        fx.Options.ReadTriggeredCompactionThreshold = 1.0;
        // file_size=1000, reads=500 => reads_per_byte=0.5 < 1.0
        SetCollapsibleReads(fx.Add(0, 1, "100", "200", 1000), 500);
        Assert.AreEqual(0, fx.Build().ReadTriggeredCompactionFiles.Count);
    }

    [TestMethod]
    public void ReadTriggeredCompactionAboveThreshold()
    {
        var fx = new Fixture(6);
        fx.Options.ReadTriggeredCompactionThreshold = 0.5;
        SetCollapsibleReads(fx.Add(0, 1, "100", "200", 1000), 600); // 0.6 > 0.5
        SetCollapsibleReads(fx.Add(1, 2, "300", "400", 1000), 300); // 0.3 < 0.5 (not marked)
        SetCollapsibleReads(fx.Add(2, 3, "500", "600", 1000), 800); // 0.8 > 0.5 (hottest)
        fx.Add(4, 4, "700", "800", 1000); // L2 is not the last non-empty level
        List<(int Level, FileMetaData File)> marked = fx.Build().ReadTriggeredCompactionFiles;
        Assert.AreEqual(2, marked.Count);
        // Sorted by reads_per_byte descending: file 3 (0.8) then file 1 (0.6)
        Assert.AreEqual(3UL, marked[0].File.FileNumber);
        Assert.AreEqual(1UL, marked[1].File.FileNumber);
    }

    [TestMethod]
    public void ReadTriggeredCompactionIgnoresFifoAndFilesBeingCompacted()
    {
        var fx = new Fixture(1, CompactionStyle.Fifo);
        fx.Options.ReadTriggeredCompactionThreshold = 0.1;
        SetCollapsibleReads(fx.Add(0, 1, "100", "200", 1000), 5000);
        Assert.AreEqual(0, fx.Build().ReadTriggeredCompactionFiles.Count);

        var lv = new Fixture(6);
        lv.Options.ReadTriggeredCompactionThreshold = 0.1;
        FileMetaData busy = lv.Add(1, 1, "100", "200", 1000);
        SetCollapsibleReads(busy, 5000);
        lv.Add(3, 2, "300", "400", 1000);
        busy.BeingCompacted = true;
        Assert.AreEqual(0, lv.Build().ReadTriggeredCompactionFiles.Count);
    }

    [TestMethod]
    public void NeedsCompactionReadTriggered()
    {
        var fx = new Fixture(6);
        fx.Options.ReadTriggeredCompactionThreshold = 0.1;
        SetCollapsibleReads(fx.Add(1, 1, "100", "200", 1000), 500);
        fx.Add(3, 2, "300", "400", 1000);
        VersionStorageInfo vs = fx.Build();
        Assert.AreNotEqual(0, vs.ReadTriggeredCompactionFiles.Count);
        Assert.IsTrue(fx.LevelPicker().NeedsCompaction(vs));
    }

    [TestMethod]
    public void ReadTriggeredPicksFile()
    {
        var fx = new Fixture(6);
        fx.Options.ReadTriggeredCompactionThreshold = 0.1;
        SetCollapsibleReads(fx.Add(1, 1, "100", "200", 1000), 500);
        fx.Add(3, 2, "300", "400", 1000);
        Compaction? c = fx.LevelPicker().PickCompaction(fx.Build(), []);
        Assert.IsNotNull(c);
        Assert.AreEqual(CompactionReason.ReadTriggered, c.Reason);
        Assert.AreEqual(1, c.StartLevel);
        Assert.AreEqual(2, c.OutputLevel);
    }

    [TestMethod]
    public void UniversalReadTriggeredCompaction()
    {
        const ulong FileSize = 100000;
        var fx = new Fixture(5, CompactionStyle.Universal);
        fx.Options.ReadTriggeredCompactionThreshold = 0.001;
        // Set trigger high so size amp / sorted run pickers don't fire
        fx.Options.Level0FileNumCompactionTrigger = 10;
        // Hot file at L2 with data at L4 below it
        fx.Add(0, 1, "150", "200", FileSize, 500, 550);
        SetCollapsibleReads(fx.Add(2, 2, "301", "350", FileSize, 201, 250), (uint)FileSize);
        fx.Add(4, 3, "301", "400", FileSize, 101, 150);
        VersionStorageInfo vs = fx.Build();
        UniversalCompactionPicker picker = fx.UniversalPicker();
        Assert.IsTrue(picker.NeedsCompaction(vs));
        Compaction? c = picker.PickCompaction(vs, []);
        Assert.IsNotNull(c);
        Assert.AreEqual(CompactionReason.ReadTriggered, c.Reason);
        Assert.AreEqual(2, c.StartLevel);
        Assert.AreEqual(4, c.OutputLevel);
    }

    [TestMethod]
    public void ReadTriggeredSkipsLastLevel()
    {
        const ulong FileSize = 100000;
        var fx = new Fixture(5, CompactionStyle.Universal);
        fx.Options.ReadTriggeredCompactionThreshold = 0.001;
        fx.Options.Level0FileNumCompactionTrigger = 10;
        fx.Add(0, 1, "150", "200", FileSize, 500, 550);
        // File 3 is at the last non-empty level -- should NOT be marked for read-triggered compaction.
        SetCollapsibleReads(fx.Add(4, 3, "301", "350", FileSize, 101, 150), (uint)FileSize);
        Assert.AreEqual(0, fx.Build().ReadTriggeredCompactionFiles.Count);
    }

    [TestMethod]
    public void UniversalReadTriggeredNoPickWhenNotMarked()
    {
        const ulong FileSize = 100000;
        var fx = new Fixture(5, CompactionStyle.Universal);
        fx.Options.ReadTriggeredCompactionThreshold = 0.001;
        fx.Add(0, 1, "150", "200", FileSize, 500, 550);
        fx.Add(4, 3, "301", "350", FileSize, 101, 150);
        VersionStorageInfo vs = fx.Build();
        Assert.AreEqual(0, vs.ReadTriggeredCompactionFiles.Count);
        // Not enough sorted runs to trigger compaction either
        Assert.IsFalse(fx.UniversalPicker().NeedsCompaction(vs));
    }

    [TestMethod]
    public void UniversalReadTriggeredIntraL0()
    {
        const ulong FileSize = 100000;
        var fx = new Fixture(1, CompactionStyle.Universal);
        fx.Options.ReadTriggeredCompactionThreshold = 0.001;
        fx.Options.Level0FileNumCompactionTrigger = 10;
        // Single-level universal with overlapping L0 files; file 1 is hot.
        SetCollapsibleReads(fx.Add(0, 1, "100", "300", FileSize, 500, 550), (uint)FileSize);
        fx.Add(0, 2, "200", "400", FileSize, 400, 450);
        VersionStorageInfo vs = fx.Build();
        Assert.AreNotEqual(0, vs.ReadTriggeredCompactionFiles.Count);
        UniversalCompactionPicker picker = fx.UniversalPicker();
        Assert.IsTrue(picker.NeedsCompaction(vs));
        Compaction? c = picker.PickCompaction(vs, []);
        Assert.IsNotNull(c);
        Assert.AreEqual(CompactionReason.ReadTriggered, c.Reason);
        Assert.AreEqual(0, c.StartLevel);
        Assert.AreEqual(0, c.OutputLevel);
        Assert.AreEqual(2, c.Inputs(0).Count);
    }

    // ---- bottommost files marked for compaction ----

    private static List<ulong> MarkedBottommost(VersionStorageInfo vs)
    {
        var l = new List<ulong>();
        foreach ((int _, FileMetaData f) in vs.BottommostFilesMarkedForCompaction) l.Add(f.FileNumber);
        return l;
    }

    [TestMethod]
    public void BottommostFilesMarkedOnlyBelowOldestSnapshot()
    {
        // VersionStorageInfo::ComputeBottommostFilesMarkedForCompaction: a bottommost file (nothing older overlaps it)
        // with a nonzero largest seqno is marked once the oldest snapshot passes it; until then it sets the threshold.
        var fx = new Fixture(6);
        fx.Add(5, 1, "100", "200", 1000, 90, 100);
        fx.Add(5, 2, "300", "400", 1000, 0, 0); // seqnos already zeroed: never marked
        fx.Add(4, 3, "150", "160", 1000, 200, 300); // overlaps L5: not bottommost
        fx.Add(4, 4, "500", "600", 1000, 200, 250); // bottommost (nothing below)
        VersionStorageInfo vs = fx.Build();
        CollectionAssert.AreEquivalent(new ulong[] { 1, 2, 4 }, BottommostFileNumbers(vs));
        Assert.AreEqual(0, vs.BottommostFilesMarkedForCompaction.Count);
        Assert.AreEqual(100UL, vs.BottommostFilesMarkThreshold);

        vs.UpdateOldestSnapshot(200);
        CollectionAssert.AreEqual(new ulong[] { 1 }, MarkedBottommost(vs));
        Assert.AreEqual(250UL, vs.BottommostFilesMarkThreshold);
        vs.UpdateOldestSnapshot(1000);
        CollectionAssert.AreEquivalent(new ulong[] { 1, 4 }, MarkedBottommost(vs));
        Assert.AreEqual(DbFormat.MaxSequenceNumber, vs.BottommostFilesMarkThreshold);

        // The leveled picker compacts it within its level (kBottommostFiles, CompactToNextLevel::kNo).
        LevelCompactionPicker picker = fx.LevelPicker();
        Assert.IsTrue(picker.NeedsCompaction(vs));
        Compaction? c = picker.PickCompaction(vs, []);
        Assert.IsNotNull(c);
        Assert.AreEqual(CompactionReason.BottommostFiles, c.Reason);
        Assert.AreEqual(c.StartLevel, c.OutputLevel);
        Assert.AreEqual(1, c.NumInputFiles(0));
    }

    private static List<ulong> BottommostFileNumbers(VersionStorageInfo vs)
    {
        var l = new List<ulong>();
        foreach ((int _, FileMetaData f) in vs.BottommostFiles) l.Add(f.FileNumber);
        return l;
    }

    [TestMethod]
    public void BottommostFileMarkingRespectsPreserveWindow()
    {
        // compaction_picker_test.cc BottommostFileNotMarkedWithinPreserveWindow / BottommostFileMarkedBelowPreserveWindow:
        // a file whose largest seqno is inside the preserve window cannot be zeroed, so it must not be marked.
        foreach ((ulong preserveMin, bool marked) in new[] { (100UL, false), (150UL, true) })
        {
            var fx = new Fixture(6);
            fx.Add(5, 1, "100", "200", 1000, 90, 100);
            VersionStorageInfo vs = fx.Build();
            vs.SetPreserveTimeMinSeqno(preserveMin);
            vs.UpdateOldestSnapshot(200);
            Assert.AreEqual(marked ? 1 : 0, vs.BottommostFilesMarkedForCompaction.Count, "preserve_time_min_seqno " + preserveMin);
        }
        Assert.IsTrue(VersionStorageInfo.BottommostSeqnoCanBeZeroed(99, 99, 0, false, false));
        Assert.IsFalse(VersionStorageInfo.BottommostSeqnoCanBeZeroed(100, 99, 0, false, false));
        Assert.IsFalse(VersionStorageInfo.BottommostSeqnoCanBeZeroed(10, 99, 8, false, true));
        Assert.IsTrue(VersionStorageInfo.BottommostSeqnoCanBeZeroed(10, 99, 8, true, true));
    }

    [TestMethod]
    public void BottommostFileCompactionDelay()
    {
        // bottommost_file_compaction_delay: only files created at least that long ago (or of unknown creation time).
        var fx = new Fixture(6);
        fx.Options.BottommostFileCompactionDelay = 3600;
        long now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        fx.Add(5, 1, "100", "200", 1000, 90, 100).FileCreationTime = (ulong)now;
        fx.Add(5, 2, "300", "400", 1000, 90, 100).FileCreationTime = (ulong)(now - 7200);
        fx.Add(5, 3, "500", "600", 1000, 90, 100).FileCreationTime = FileMetaData.UnknownFileCreationTime;
        VersionStorageInfo vs = fx.Build();
        vs.UpdateOldestSnapshot(200);
        CollectionAssert.AreEqual(new ulong[] { 2, 3 }, MarkedBottommost(vs));
        // The too-new file does not lower the threshold; it is retried on the next recomputation.
        Assert.AreEqual(DbFormat.MaxSequenceNumber, vs.BottommostFilesMarkThreshold);
        Assert.AreEqual(3600UL, RocksDb.Managed.Db.DbImpl.GetMinTimeBasedCompactionInterval(new ColumnFamilyOptions
        {
            Ttl = 0, PeriodicCompactionSeconds = 0, BottommostFileCompactionDelay = 3600,
        }));
    }

    [TestMethod]
    public void BottommostFilesNotMarkedWithIngestBehind()
    {
        var fx = new Fixture(6);
        fx.Add(5, 1, "100", "200", 1000, 90, 100);
        VersionStorageInfo vs = fx.Build(new VersionBuildContext { AllowIngestBehind = true });
        vs.UpdateOldestSnapshot(200);
        Assert.AreEqual(0, vs.BottommostFilesMarkedForCompaction.Count);
    }

    // ---- compensated file size ----

    private static void SetStats(FileMetaData f, ulong entries, ulong deletions, ulong rawKey, ulong rawValue)
    {
        f.SetEntryStats(new RocksDb.Managed.Table.TableProperties
        {
            NumEntries = entries, NumDeletions = deletions, RawKeySize = rawKey, RawValueSize = rawValue,
        });
    }

    [TestMethod]
    public void CompensatedSizeBoostsPointDeletions()
    {
        // Version::UpdateAccumulatedStats + VersionStorageInfo::ComputeCompensatedSizes: a file whose point deletions
        // are at least half of its entries is boosted by (2 * deletions - entries) * average value size * 2.
        var fx = new Fixture(6);
        FileMetaData values = fx.Add(1, 1, "100", "200", 10000);
        SetStats(values, 100, 0, 1000, 9000); // average value size = 9000 / 100 * 10000 / 10000 = 90
        FileMetaData tombstones = fx.Add(1, 2, "300", "400", 1000);
        SetStats(tombstones, 100, 80, 1000, 0);
        FileMetaData balanced = fx.Add(1, 3, "500", "600", 1000);
        SetStats(balanced, 100, 40, 1000, 5400);
        var files = new FileMetaData[6][];
        for (int l = 0; l < 6; l++) files[l] = fx.Files[l].ToArray();
        int loads = 0;
        AccumulatedFileStats stats = VersionStorageInfo.UpdateAccumulatedStats(files, default, infiniteTableCache: false, f => { loads++; return null; });
        Assert.AreEqual(0, loads, "stats known from the builder need no table read");
        Assert.AreEqual(12000UL, stats.FileSize);
        Assert.AreEqual(180UL, stats.NumNonDeletions);
        Assert.AreEqual(120UL, stats.NumDeletions);
        VersionStorageInfo vs = fx.Build(new VersionBuildContext { AccumulatedStats = stats });
        ulong avg = stats.AverageValueSize;
        Assert.AreEqual(14400UL / 180 * 12000 / 17400, avg);
        Assert.AreEqual(10000UL, VersionStorageInfo.CompensatedFileSize(values));
        Assert.AreEqual(1000UL + (160 - 100) * avg * 2, VersionStorageInfo.CompensatedFileSize(tombstones));
        Assert.AreEqual(1000UL, VersionStorageInfo.CompensatedFileSize(balanced));
        // The level score uses the compensated sizes.
        double expected = (10000.0 + VersionStorageInfo.CompensatedFileSize(tombstones) + 1000) / vs.MaxBytesForLevel(1);
        Assert.AreEqual(expected, vs.ScoreOfLevel(1), 1e-9);
        // Computed once: a later version keeps it (only the first version containing a file computes it).
        ulong once = VersionStorageInfo.CompensatedFileSize(tombstones);
        fx.Build(new VersionBuildContext { AccumulatedStats = new AccumulatedFileStats(1, 1, 1_000_000, 1, 0) });
        Assert.AreEqual(once, VersionStorageInfo.CompensatedFileSize(tombstones));
    }

    [TestMethod]
    public void UpdateAccumulatedStatsCapsTableReads()
    {
        // kMaxInitCount = 20 files per version unless the table cache is infinite (max_open_files = -1); lower levels first.
        var fx = new Fixture(3);
        for (int i = 0; i < 30; i++) fx.Add(1, (ulong)i + 1, (100 + i * 10).ToString(System.Globalization.CultureInfo.InvariantCulture),
            (105 + i * 10).ToString(System.Globalization.CultureInfo.InvariantCulture), 100);
        var files = new FileMetaData[3][];
        for (int l = 0; l < 3; l++) files[l] = fx.Files[l].ToArray();
        var props = new RocksDb.Managed.Table.TableProperties { NumEntries = 10, RawKeySize = 50, RawValueSize = 50 };
        int loads = 0;
        AccumulatedFileStats s = VersionStorageInfo.UpdateAccumulatedStats(files, default, infiniteTableCache: false, f => { loads++; return props; });
        Assert.AreEqual(VersionStorageInfo.MaxStatsInitCount, loads);
        Assert.AreEqual(200UL, s.NumNonDeletions);
        // Each file is initialized only once.
        AccumulatedFileStats s2 = VersionStorageInfo.UpdateAccumulatedStats(files, s, infiniteTableCache: true, f => { loads++; return props; });
        Assert.AreEqual(30, loads);
        Assert.AreEqual(300UL, s2.NumNonDeletions);
    }

    [TestMethod]
    public void GetNextCompactCursorFollowsPriorityOrderAndWraps()
    {
        // VersionStorageInfo::GetNextCompactCursor with kRoundRobin: the files are rotated to start at the cursor; the
        // next cursor is the smallest key of the file after the compacted ones, wrapping to the first.
        var fx = new Fixture(6);
        fx.Options.CompactionPri = CompactionPri.RoundRobin;
        fx.Add(1, 1, "100", "150");
        fx.Add(1, 2, "200", "250");
        fx.Add(1, 3, "300", "350");
        var cursor = new byte[3 + 8];
        DbFormat.AppendInternalKey(cursor, "200"u8, 100, EntryType.Value);
        VersionStorageInfo vs = fx.Build(new VersionBuildContext { CompactCursors = [new CompactCursor(1, cursor)] });
        CollectionAssert.AreEqual(new[] { 1, 2, 0 }, vs.FilesByCompactionPri(1));
        Assert.AreEqual("300", Encoding.ASCII.GetString(DbFormat.ExtractUserKey(vs.GetNextCompactCursor(1, 1))));
        Assert.AreEqual("100", Encoding.ASCII.GetString(DbFormat.ExtractUserKey(vs.GetNextCompactCursor(1, 2))));
        Assert.AreEqual("200", Encoding.ASCII.GetString(DbFormat.ExtractUserKey(vs.GetNextCompactCursor(1, 3))));
        // Inherited by the next version, overridden by an edit; levels beyond num_levels are dropped.
        var next = new VersionStorageInfo(InternalKeyComparator.Bytewise, 6, fx.Options, vs.LevelFilesArrays(), [],
            new VersionBuildContext { RefStorage = vs, CompactCursors = [new CompactCursor(9, cursor)] });
        Assert.AreSame(cursor, next.GetCompactCursor(1));
        Assert.AreEqual(1, new List<CompactCursor>(next.ValidCompactCursors()).Count);
    }
}

/// <summary>
/// Write stall conditions and the delayed write rate. Port of db/column_family_test.cc WriteStallSingleColumnFamily
/// (ColumnFamilyData::RecalculateWriteStallConditions, SetupDelay, WriteController).
/// </summary>
[TestClass]
public sealed class WriteStallTests
{
    [TestMethod]
    public void WriteStallSingleColumnFamily()
    {
        const ulong BaseRate = 800000;
        var wc = new WriteController(BaseRate);
        var state = new ColumnFamilyStallState();
        var o = new ColumnFamilyOptions
        {
            Level0SlowdownWritesTrigger = 20,
            Level0StopWritesTrigger = 10000,
            SoftPendingCompactionBytesLimit = 200,
            HardPendingCompactionBytesLimit = 2000,
            DisableAutoCompactions = false,
        };
        var vs = new VersionStorageInfo(InternalKeyComparator.Bytewise, 7, o, null, []);
        ulong Rate() => wc.NeedsDelay ? wc.DelayedWriteRate : 0;
        void Step(ulong pending, bool stopped, bool delayed, ulong expectedRate)
        {
            vs.TestSetEstimatedCompactionNeededBytes(pending);
            state.Recalculate(wc, 1, vs, o);
            Assert.AreEqual(stopped, wc.IsStopped, "stopped at pending " + pending);
            Assert.AreEqual(delayed, wc.NeedsDelay, "delayed at pending " + pending);
            if (delayed) Assert.AreEqual(expectedRate, wc.DelayedWriteRate, "rate at pending " + pending);
        }

        Step(50, false, false, 0);
        Step(201, false, true, BaseRate);
        Assert.IsTrue(wc.NeedSpeedupCompaction);
        Step(400, false, true, (ulong)(BaseRate / 1.25));
        Step(500, false, true, (ulong)(BaseRate / 1.25 / 1.25));
        Step(450, false, true, (ulong)(BaseRate / 1.25));
        Step(205, false, true, BaseRate);
        Step(202, false, true, BaseRate);
        Step(201, false, true, BaseRate);
        Step(198, false, false, 0);
        Step(399, false, true, BaseRate);
        Step(599, false, true, (ulong)(BaseRate / 1.25));
        Step(2001, true, false, 0);
        Assert.IsTrue(wc.NeedSpeedupCompaction);
        Step(3001, true, false, 0);
        Step(390, false, true, (ulong)(BaseRate / 1.25));
        Step(100, false, false, 0);

        vs.TestSetL0DelayTriggerCount(100);
        state.Recalculate(wc, 1, vs, o);
        Assert.IsTrue(wc.NeedsDelay);
        Assert.AreEqual(BaseRate, Rate());
        vs.TestSetL0DelayTriggerCount(101);
        state.Recalculate(wc, 1, vs, o);
        Assert.AreEqual((ulong)(BaseRate / 1.25), Rate());
        vs.TestSetL0DelayTriggerCount(0);
        Step(300, false, true, (ulong)(BaseRate / 1.25 / 1.25));
        vs.TestSetL0DelayTriggerCount(101);
        state.Recalculate(wc, 1, vs, o);
        Assert.AreEqual((ulong)(BaseRate / 1.25 / 1.25 / 1.25), Rate());
        Step(200, false, true, (ulong)(BaseRate / 1.25 / 1.25));
        vs.TestSetL0DelayTriggerCount(0);
        Step(0, false, false, 0);

        // With auto compactions disabled neither L0 nor pending bytes stall writes; the rate is kept.
        o.DisableAutoCompactions = true;
        wc.SetDelayedWriteRate(BaseRate);
        state.Recalculate(wc, 1, vs, o);
        Assert.IsFalse(wc.NeedsDelay);
        vs.TestSetL0DelayTriggerCount(50);
        state.Recalculate(wc, 1, vs, o);
        Assert.IsFalse(wc.NeedsDelay);
        Assert.AreEqual(BaseRate, wc.DelayedWriteRate);
        vs.TestSetL0DelayTriggerCount(60);
        Step(300, false, false, 0);
        Assert.AreEqual(BaseRate, wc.DelayedWriteRate);
    }

    [TestMethod]
    public void ContinuedDelayKeepsWriteControllerDebt()
    {
        // GetDelayToken resets the credit and refill time only when the delayed count goes from 0 to 1. A
        // recalculation that keeps the CF delayed acquires its new token before the old one is released, so the
        // writes already paid for in advance (next_refill_time_ in the future) still have to wait.
        const ulong Rate = 1_000_000;
        var wc = new WriteController(Rate);
        var state = new ColumnFamilyStallState();
        var o = new ColumnFamilyOptions { Level0SlowdownWritesTrigger = 20, Level0StopWritesTrigger = 10000 };
        var vs = new VersionStorageInfo(InternalKeyComparator.Bytewise, 7, o, null, []);
        vs.TestSetL0DelayTriggerCount(20);
        Assert.AreEqual(WriteStallCondition.Delayed, state.Recalculate(wc, 1, vs, o));
        // One second worth of bytes: the first write waits ~1 s and the refill time moves ~1 s ahead.
        Assert.IsGreaterThan(900_000UL, wc.GetDelay(Rate));
        Assert.AreEqual(WriteStallCondition.Delayed, state.Recalculate(wc, 1, vs, o));
        Assert.AreEqual(Rate, wc.DelayedWriteRate, "no compaction debt: the rate is kept");
        // A small write right after still waits for the debt (a reset would give it a fresh 1 ms allotment: no delay).
        Assert.IsGreaterThan(500_000UL, wc.GetDelay(100));
        vs.TestSetL0DelayTriggerCount(0);
        Assert.AreEqual(WriteStallCondition.Normal, state.Recalculate(wc, 1, vs, o));
        Assert.IsFalse(wc.NeedsDelay);
    }

    [TestMethod]
    public void CompactionSpeedupByDebtRelativeToBottommostData()
    {
        // GetPendingCompactionBytesForCompactionSpeedup: min(max(bottommost / 8, 2 * max_bytes_for_level_base), soft / 4).
        var o = new ColumnFamilyOptions { MaxBytesForLevelBase = 1000, SoftPendingCompactionBytesLimit = 1_000_000 };
        var files = new FileMetaData[7][];
        for (int l = 0; l < 7; l++) files[l] = [];
        var k1 = new byte[11];
        DbFormat.AppendInternalKey(k1, "100"u8, 1, EntryType.Value);
        var k2 = new byte[11];
        DbFormat.AppendInternalKey(k2, "200"u8, 1, EntryType.Value);
        files[6] = [new FileMetaData { FileNumber = 1, FileSize = 80_000, Smallest = k1, Largest = k2, SmallestSeqno = 1, LargestSeqno = 1 }];
        var vs = new VersionStorageInfo(InternalKeyComparator.Bytewise, 7, o, files, []);
        Assert.AreEqual(10_000UL, ColumnFamilyStallState.GetPendingCompactionBytesForCompactionSpeedup(o, vs));
        o.MaxBytesForLevelBase = 20_000;
        Assert.AreEqual(40_000UL, ColumnFamilyStallState.GetPendingCompactionBytesForCompactionSpeedup(o, vs));
        o.SoftPendingCompactionBytesLimit = 100_000;
        Assert.AreEqual(25_000UL, ColumnFamilyStallState.GetPendingCompactionBytesForCompactionSpeedup(o, vs));

        var wc = new WriteController(1 << 20);
        var state = new ColumnFamilyStallState();
        vs.TestSetEstimatedCompactionNeededBytes(24_999);
        state.Recalculate(wc, 1, vs, o);
        Assert.IsFalse(wc.NeedSpeedupCompaction);
        vs.TestSetEstimatedCompactionNeededBytes(25_000);
        state.Recalculate(wc, 1, vs, o);
        Assert.IsTrue(wc.NeedSpeedupCompaction);
        Assert.IsFalse(wc.NeedsDelay);
    }
}
