using System.Text;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table;
using RocksDb.Managed.Tests.Db;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4, compaction pickers slice (CompactionPicker, Level / Universal / FIFO pickers, VersionStorageInfo compaction
/// state, Compaction). Expected values are derived from the C++ functions named in each test.
/// </summary>
[TestClass]
public sealed class Audit4CompactionPickersTests
{
    private static byte[] IKey(string user, ulong seq, EntryType t)
    {
        byte[] u = Encoding.ASCII.GetBytes(user);
        var k = new byte[u.Length + 8];
        DbFormat.AppendInternalKey(k, u, seq, t);
        return k;
    }

    /// <summary>A synthetic version (like CompactionPickerTestBase): L0 files newest first in the order they are added.</summary>
    private sealed class Lsm
    {
        public readonly ColumnFamilyOptions Options;
        private readonly List<FileMetaData>[] _files;
        private ulong _epoch = 1000;

        public Lsm(int numLevels, CompactionStyle style)
        {
            Options = new ColumnFamilyOptions { NumLevels = numLevels, CompactionStyle = style, LevelCompactionDynamicLevelBytes = false, Ttl = 0 };
            _files = new List<FileMetaData>[numLevels];
            for (int i = 0; i < numLevels; i++) _files[i] = new List<FileMetaData>();
        }

        public FileMetaData Add(int level, ulong number, string smallest, string largest, ulong size = 1, ulong smallestSeq = 100, ulong largestSeq = 100,
            EntryType smallestType = EntryType.Value)
        {
            var f = new FileMetaData
            {
                FileNumber = number,
                FileSize = size,
                Smallest = IKey(smallest, largestSeq, smallestType),
                Largest = IKey(largest, smallestSeq, EntryType.Value),
                SmallestSeqno = smallestSeq,
                LargestSeqno = largestSeq,
                EpochNumber = level == 0 ? _epoch-- : 0,
            };
            _files[level].Add(f);
            return f;
        }

        public VersionStorageInfo Build()
        {
            InternalKeyComparator icmp = InternalKeyComparator.Bytewise;
            var arrays = new FileMetaData[_files.Length][];
            for (int l = 0; l < _files.Length; l++)
            {
                if (l > 0) _files[l].Sort((a, b) => icmp.Compare(a.Smallest, b.Smallest));
                arrays[l] = _files[l].ToArray();
            }
            return new VersionStorageInfo(icmp, _files.Length, Options, arrays, [], null);
        }
    }

    private static List<ulong> Numbers(CompactionInputFiles inputs)
    {
        var l = new List<ulong>();
        foreach (FileMetaData f in inputs.Files) l.Add(f.FileNumber);
        return l;
    }

    /// <summary>
    /// VersionStorageInfo::GenerateLevel0NonOverlapping (db/version_set.cc): "internal_comparator_->Compare(prev.largest_key,
    /// f.smallest_key) >= 0" means overlap. Two L0 files that share only a boundary USER key (newer file k@3, older file
    /// k@1..q@2) do not overlap; files sharing an internal-key range do.
    /// </summary>
    [TestMethod]
    public void Level0NonOverlappingComparesInternalKeys()
    {
        var lsm = new Lsm(7, CompactionStyle.Level);
        lsm.Add(0, 2, "k", "k", smallestSeq: 3, largestSeq: 3, smallestType: EntryType.Deletion); // [k@3 .. k@3]
        lsm.Add(0, 1, "k", "q", smallestSeq: 1, largestSeq: 2); // [k@2 .. q@1]
        Assert.IsTrue(lsm.Build().Level0NonOverlapping);

        var overlapping = new Lsm(7, CompactionStyle.Level);
        overlapping.Add(0, 2, "k", "m", smallestSeq: 3, largestSeq: 4);
        overlapping.Add(0, 1, "k", "q", smallestSeq: 1, largestSeq: 2);
        Assert.IsFalse(overlapping.Build().Level0NonOverlapping);
    }

    /// <summary>
    /// SortFileByRoundRobin (db/version_set.cc): "if (level == 0 &amp;&amp; !level0_non_overlapping)" sorts by
    /// kOldestSmallestSeqFirst. PrepareForVersionAppend runs UpdateFilesByCompactionPri before
    /// GenerateLevel0NonOverlapping, so level0_non_overlapping_ still has its initial value (false) there: L0 is always
    /// ordered oldest first, also when its files do not overlap.
    /// </summary>
    [TestMethod]
    public void RoundRobinOrdersL0OldestFirstEvenWhenNonOverlapping()
    {
        var lsm = new Lsm(7, CompactionStyle.Level);
        lsm.Options.CompactionPri = CompactionPri.RoundRobin;
        lsm.Add(0, 2, "b", "c", smallestSeq: 3, largestSeq: 4); // newest, index 0, smallest keys
        lsm.Add(0, 1, "m", "n", smallestSeq: 1, largestSeq: 2); // oldest, index 1
        VersionStorageInfo vs = lsm.Build();
        Assert.IsTrue(vs.Level0NonOverlapping);
        CollectionAssert.AreEqual(new[] { 1, 0 }, vs.FilesByCompactionPri(0));

        var overlapping = new Lsm(7, CompactionStyle.Level);
        overlapping.Options.CompactionPri = CompactionPri.RoundRobin;
        overlapping.Add(0, 2, "b", "n", smallestSeq: 3, largestSeq: 4);
        overlapping.Add(0, 1, "c", "d", smallestSeq: 1, largestSeq: 2);
        CollectionAssert.AreEqual(new[] { 1, 0 }, overlapping.Build().FilesByCompactionPri(0));
    }

    /// <summary>
    /// SortFileByOverlappingRatio with FileTtlBooster (db/version_set.cc, db/compaction/file_pri.h): the order value is
    /// overlapping_bytes * 1024 / compensated_file_size / ttl_boost_score. With ttl = 3,200,000 s, 4 non-empty levels and
    /// level 1: boost_age_range = (3,100,000 - 1,600,000) >> 2 = 375,000, boost_age_start = 1,975,000, boost_step = 23,437.
    /// A file aged 3,000,000 s gets score (3,000,000 - 1,975,000) / 23,437 + 1 = 44, so its order drops from 10,240 to 232
    /// and it is picked before the file with ratio 5,120. Without ttl the lower ratio wins.
    /// </summary>
    [TestMethod]
    public void MinOverlappingRatioBoostsFilesCloseToTtl()
    {
        ulong now = (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        foreach (ulong ttl in new ulong[] { 3_200_000, 0 })
        {
            var lsm = new Lsm(5, CompactionStyle.Level);
            lsm.Options.CompactionPri = CompactionPri.MinOverlappingRatio;
            lsm.Options.Ttl = ttl;
            lsm.Add(1, 1, "a", "b", size: 100).OldestAncestorTime = now - 3_000_000; // old: boosted
            lsm.Add(1, 2, "m", "n", size: 100).OldestAncestorTime = now - 10;
            lsm.Add(2, 3, "a", "b", size: 1000).OldestAncestorTime = now - 10;
            lsm.Add(2, 4, "m", "n", size: 500).OldestAncestorTime = now - 10;
            lsm.Add(3, 5, "a", "z", size: 10000).OldestAncestorTime = now - 10;
            int[] order = lsm.Build().FilesByCompactionPri(1);
            CollectionAssert.AreEqual(ttl != 0 ? new[] { 0, 1 } : new[] { 1, 0 }, order, "ttl " + ttl);
        }
    }

    /// <summary>
    /// LevelCompactionBuilder::TryPickL0TrivialMove (compaction_picker_level.cc) requires "ioptions_.db_paths.size() &lt;= 1".
    /// With one path the two L0 files that do not overlap L1 form an L0 trivial move (oldest first, both files); with two
    /// paths the file is picked by compaction priority instead (one file; same ratio, smaller key first).
    /// </summary>
    [TestMethod]
    public void L0TrivialMoveIsNotPickedWithSeveralDbPaths()
    {
        foreach (int paths in new[] { 1, 2 })
        {
            var lsm = new Lsm(4, CompactionStyle.Level);
            lsm.Options.Level0FileNumCompactionTrigger = 2;
            lsm.Add(0, 2, "m", "n", smallestSeq: 3, largestSeq: 4);
            lsm.Add(0, 1, "a", "b", smallestSeq: 1, largestSeq: 2);
            lsm.Add(1, 3, "x", "z");
            VersionStorageInfo vs = lsm.Build();
            var picker = new LevelCompactionPicker(lsm.Options, InternalKeyComparator.Bytewise);
            if (paths == 2) picker.PathTargetSizes = [1UL << 40, 1UL << 40];
            Compaction? c = picker.PickCompaction(vs, []);
            Assert.IsNotNull(c);
            Assert.AreEqual(1, c.OutputLevel);
            Assert.AreEqual(1, c.NumInputLevels);
            CollectionAssert.AreEqual(paths == 1 ? new List<ulong> { 1, 2 } : new List<ulong> { 1 }, Numbers(c.Inputs(0)), "paths " + paths);
        }
    }

    /// <summary>
    /// Compaction::IsTrivialMove: "num_input_levels() == 1" is false for the compactions UniversalCompactionBuilder creates
    /// from sorted runs ("std::vector&lt;CompactionInputFiles&gt; inputs(max_output_level + 1)", empty levels included), so
    /// without allow_trivial_move four non-overlapping L0 files are merged, not moved. With allow_trivial_move the
    /// builder's IsInputFilesNonOverlapping decides.
    /// </summary>
    [TestMethod]
    public void UniversalSortedRunCompactionIsNotATrivialMoveWithoutAllowTrivialMove()
    {
        foreach (bool allow in new[] { false, true })
        {
            var lsm = new Lsm(7, CompactionStyle.Universal);
            lsm.Options.CompactionOptionsUniversal.AllowTrivialMove = allow;
            lsm.Add(0, 4, "d0", "d9", size: 100, smallestSeq: 31, largestSeq: 40);
            lsm.Add(0, 3, "c0", "c9", size: 100, smallestSeq: 21, largestSeq: 30);
            lsm.Add(0, 2, "b0", "b9", size: 100, smallestSeq: 11, largestSeq: 20);
            lsm.Add(0, 1, "a0", "a9", size: 100, smallestSeq: 1, largestSeq: 10);
            VersionStorageInfo vs = lsm.Build();
            Assert.IsTrue(vs.Level0NonOverlapping);
            Compaction? c = new UniversalCompactionPicker(lsm.Options, InternalKeyComparator.Bytewise).PickCompaction(vs, []);
            Assert.IsNotNull(c);
            Assert.AreEqual(6, c.OutputLevel);
            Assert.AreEqual(4, c.Inputs(0).Count);
            Assert.AreEqual(allow, c.IsTrivialMove(), "allow_trivial_move " + allow);
        }
    }

    /// <summary>
    /// UniversalCompactionBuilder::BuildCompactionToNextLevel passes exactly the non-empty inputs ({start} when the output
    /// level has no overlapping file), so a delete-triggered compaction of one marked file into an empty range of the next
    /// level can be a trivial move (num_input_levels() == 1) even without allow_trivial_move.
    /// </summary>
    [TestMethod]
    public void UniversalDeleteTriggeredCompactionCanStillBeATrivialMove()
    {
        var lsm = new Lsm(7, CompactionStyle.Universal);
        lsm.Add(0, 2, "a", "b", size: 100, smallestSeq: 11, largestSeq: 20).MarkedForCompaction = true;
        lsm.Add(6, 1, "x", "z", size: 100000, smallestSeq: 1, largestSeq: 10);
        VersionStorageInfo vs = lsm.Build();
        Compaction? c = new UniversalCompactionPicker(lsm.Options, InternalKeyComparator.Bytewise).PickCompaction(vs, []);
        Assert.IsNotNull(c);
        Assert.AreEqual(CompactionReason.FilesMarkedForCompaction, c.Reason);
        Assert.AreEqual(6, c.OutputLevel);
        Assert.AreEqual(1, c.NumInputLevels);
        Assert.IsTrue(c.IsTrivialMove());
    }

    /// <summary>
    /// FIFOCompactionPicker::PickIntraL0Compaction (compaction_picker_fifo.cc) creates the compaction with
    /// "mutable_cf_options.compression", not GetCompressionType(): bottommost_compression does not apply to FIFO's
    /// intra-L0 outputs although L0 is the last level with data.
    /// </summary>
    [TestMethod]
    public void FifoIntraL0CompactionUsesTheColumnFamilyCompression()
    {
        var lsm = new Lsm(1, CompactionStyle.Fifo);
        lsm.Options.CompactionOptionsFifo.AllowCompaction = true;
        lsm.Options.Level0FileNumCompactionTrigger = 4;
        lsm.Options.Compression = CompressionType.None;
        lsm.Options.BottommostCompression = CompressionType.LZ4;
        for (ulong i = 4; i >= 1; i--) lsm.Add(0, i, "a", "z", size: 1000, smallestSeq: i * 10, largestSeq: i * 10 + 5);
        VersionStorageInfo vs = lsm.Build();
        Compaction? c = new FifoCompactionPicker(lsm.Options, InternalKeyComparator.Bytewise).PickCompaction(vs, []);
        Assert.IsNotNull(c);
        Assert.AreEqual(CompactionReason.FifoReduceNumFiles, c.Reason);
        Assert.AreEqual(4, c.Inputs(0).Count);
        Assert.AreEqual(CompressionType.None, c.OutputCompression);
    }

    private static List<string> CompressionNames(RocksDatabase db)
    {
        var names = new List<string>();
        foreach (TableProperties p in db.DefaultColumnFamily.GetPropertiesOfAllTables().Values) names.Add(p.CompressionName);
        return names;
    }

    /// <summary>
    /// DB level: the table builder of a compaction output uses Compaction::output_compression()
    /// (CompactionJob::OpenCompactionOutputFile). For a FIFO intra-L0 compaction that is the column family's `compression`
    /// even when bottommost_compression is set.
    /// </summary>
    [TestMethod]
    public void FifoIntraL0OutputIsWrittenWithTheColumnFamilyCompression()
    {
        MemoryFileSystem fs = NewFs();
        var cf = new ColumnFamilyOptions
        {
            CompactionStyle = CompactionStyle.Fifo,
            Compression = CompressionType.None,
            BottommostCompression = CompressionType.LZ4,
            Level0FileNumCompactionTrigger = 4,
        };
        cf.CompactionOptionsFifo.AllowCompaction = true;
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cf);
        string uncompressedName = "";
        for (int i = 0; i < 4; i++)
        {
            for (int j = 0; j < 20; j++) db.Put(Key(i * 100 + j), Value(j, i));
            db.Flush();
            if (i == 0) uncompressedName = CompressionNames(db)[0]; // flushes use `compression` (L0 is not bottommost for a flush)
        }
        Assert.AreNotEqual("BuiltinV2;04;", uncompressedName);
        db.WaitForCompact();
        List<string> names = CompressionNames(db);
        Assert.AreEqual(1, names.Count, "the four L0 files are merged into one");
        Assert.AreEqual(uncompressedName, names[0]);
        Assert.AreEqual(S(Value(3, 2)), S(db.Get(Key(203))));
    }

    /// <summary>
    /// DB::CompactFiles with CompactionOptions::compression set: CompactionPicker::PickCompactionForCompactFiles passes it
    /// as the compaction's output compression, and the outputs are written with it (not with the column family's).
    /// </summary>
    [TestMethod]
    public void CompactFilesWritesOutputsWithTheRequestedCompression()
    {
        MemoryFileSystem fs = NewFs();
        var cf = new ColumnFamilyOptions { Compression = CompressionType.None };
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cf);
        for (int i = 0; i < 2; i++)
        {
            for (int j = 0; j < 50; j++) db.Put(Key(j), Value(j, i));
            db.Flush();
        }
        var inputs = new List<string>();
        foreach (LiveFileMetadata f in db.GetLiveFilesMetadata()) inputs.Add(f.Name);
        Assert.AreEqual(2, inputs.Count);
        List<string> before = CompressionNames(db);
        Assert.AreEqual(before[0], before[1]);
        db.DefaultColumnFamily.CompactFiles(new CompactFilesOptions { Compression = CompressionType.LZ4 }, inputs, 1);
        List<string> after = CompressionNames(db);
        Assert.AreEqual(1, after.Count);
        // The rocksdb.compression property of an LZ4 file (compression type 0x04), not the uncompressed inputs' name.
        Assert.AreEqual("BuiltinV2;04;", after[0]);
        Assert.AreNotEqual(before[0], after[0]);
        Assert.AreEqual(S(Value(7, 1)), S(db.Get(Key(7))));
    }
}
