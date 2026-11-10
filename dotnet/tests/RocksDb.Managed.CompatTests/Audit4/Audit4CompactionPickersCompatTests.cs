using System.Runtime.InteropServices;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;

namespace RocksDb.Managed.CompatTests.Audit4;

/// <summary>
/// Audit #4, compaction pickers slice: small deterministic scenarios whose LSM shape (level and key range of every
/// table file) must be the same as native RocksDB's. Each pins one picker / VersionStorageInfo decision.
/// </summary>
[TestClass]
public sealed class Audit4CompactionPickersCompatTests
{
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);

    private static string Shape(RocksDbSharp.RocksDb db)
    {
        var l = new List<string>();
        foreach (RocksDbSharp.LiveFileMetadata f in db.GetLiveFilesMetadata())
        {
            l.Add("L" + f.FileMetadata.FileLevel + "[" + f.FileDataMetadata.SmallestKeyInFile + ".." + f.FileDataMetadata.LargestKeyInFile + "]");
        }
        l.Sort(string.CompareOrdinal);
        return string.Join(' ', l);
    }

    private static string Shape(RocksDatabase db)
    {
        var l = new List<string>();
        foreach (LiveFileMetadata f in db.GetLiveFilesMetadata())
        {
            l.Add("L" + f.Level + "[" + Encoding.UTF8.GetString(f.SmallestKey) + ".." + Encoding.UTF8.GetString(f.LargestKey) + "]");
        }
        l.Sort(string.CompareOrdinal);
        return string.Join(' ', l);
    }

    private static RocksDbSharp.RocksDb OpenNative(string path, Action<RocksDbSharp.DbOptions, RocksDbSharp.Native> configure)
    {
        RocksDbSharp.Native native = RocksDbSharp.Native.Instance;
        var options = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetCompression(RocksDbSharp.Compression.No);
        native.rocksdb_options_set_max_background_jobs(options.Handle, 1);
        configure(options, native);
        s_keepAlive.Add(options);
        return RocksDbSharp.RocksDb.Open(options, path);
    }

    private static RocksDatabase OpenManaged(string path, Action<ColumnFamilyOptions> configure)
    {
        var cf = new ColumnFamilyOptions { Compression = RocksDb.Managed.Compression.CompressionType.None };
        configure(cf);
        return RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true, MaxBackgroundJobs = 1 }, cf);
    }

    private static void FlushAndWait(RocksDbSharp.RocksDb db)
    {
        RocksDbSharp.Native native = RocksDbSharp.Native.Instance;
        var fo = new RocksDbSharp.FlushOptions().SetWaitForFlush(true);
        db.Flush(fo);
        GC.KeepAlive(fo);
        IntPtr wfc = native.rocksdb_wait_for_compact_options_create();
        try
        {
            native.rocksdb_wait_for_compact(db.Handle, wfc, out IntPtr err);
            if (err != IntPtr.Zero)
            {
                string msg = Marshal.PtrToStringUTF8(err) ?? "error";
                native.rocksdb_free(err);
                Assert.Fail("native WaitForCompact: " + msg);
            }
        }
        finally
        {
            native.rocksdb_wait_for_compact_options_destroy(wfc);
        }
    }

    private static void FlushAndWait(RocksDatabase db)
    {
        db.Flush();
        db.WaitForCompact();
    }

    /// <summary>
    /// VersionStorageInfo::GenerateLevel0NonOverlapping (db/version_set.cc) compares L0 files with the INTERNAL key
    /// comparator: a file holding only <c>k@3</c> (a tombstone) and a file <c>k@1..q@2</c> do not overlap, so
    /// Compaction::IsTrivialMove lets the manual compaction move both to the base level. The port compared user keys and
    /// merged them into one file.
    /// </summary>
    [TestMethod]
    public void Level0NonOverlappingUsesInternalKeysLikeNative()
    {
        using var nativeDir = new TempDir("a4pick-l0-native");
        using var managedDir = new TempDir("a4pick-l0-managed");
        string nativeShape;
        using (RocksDbSharp.RocksDb db = OpenNative(nativeDir.Path, static (_, _) => { }))
        {
            db.Put(U("k"), U("v1"));
            db.Put(U("q"), U("v2"));
            FlushAndWait(db);
            db.Remove(U("k"));
            FlushAndWait(db);
            db.CompactRange((byte[]?)null!, null!);
            nativeShape = Shape(db);
            Assert.IsNull(db.Get(U("k")));
        }
        using (RocksDatabase db = OpenManaged(managedDir.Path, static _ => { }))
        {
            db.Put(U("k"), U("v1"));
            db.Put(U("q"), U("v2"));
            FlushAndWait(db);
            using Snapshot beforeDelete = db.GetSnapshot();
            db.Delete(U("k"));
            FlushAndWait(db);
            db.CompactRange();
            Assert.AreEqual(nativeShape, Shape(db));
            // One user key in two adjacent files of a level > 0: the newest version wins, the snapshot sees the old one.
            Assert.IsNull(db.Get(U("k")));
            Assert.AreEqual("v2", Encoding.UTF8.GetString(db.Get(U("q"))!));
            Assert.AreEqual("v1", Encoding.UTF8.GetString(db.Get(new ReadOptions { Snapshot = beforeDelete }, U("k"))!));
            var keys = new List<string>();
            using (Iterator it = db.NewIterator())
            {
                for (it.SeekToFirst(); it.Valid; it.Next()) keys.Add(Encoding.UTF8.GetString(it.Key));
            }
            CollectionAssert.AreEqual(new[] { "q" }, keys);
            keys.Clear();
            using (Iterator it = db.NewIterator(new ReadOptions { Snapshot = beforeDelete }))
            {
                for (it.SeekToLast(); it.Valid; it.Prev()) keys.Add(Encoding.UTF8.GetString(it.Key));
            }
            CollectionAssert.AreEqual(new[] { "q", "k" }, keys);
        }
    }

    private const int RoundRobinPri = 4; // kRoundRobin

    /// <summary>
    /// SortFileByRoundRobin (db/version_set.cc) orders L0 by kOldestSmallestSeqFirst unless level0_non_overlapping, but
    /// VersionStorageInfo::PrepareForVersionAppend calls UpdateFilesByCompactionPri BEFORE GenerateLevel0NonOverlapping,
    /// so the flag is still false there: L0 candidates are always oldest first. An L0 -> Lbase compaction that cannot be
    /// a trivial move therefore starts from the OLDEST L0 file: here [m..n], merged with the base-level file [l..p], while
    /// the newer [b..c] stays in L0. The port sorted non-overlapping L0 files by smallest key and picked [b..c].
    /// </summary>
    [TestMethod]
    public void RoundRobinPicksOldestL0FileLikeNative()
    {
        using var nativeDir = new TempDir("a4pick-rr-native");
        using var managedDir = new TempDir("a4pick-rr-managed");
        string nativeBase, nativeShape;
        using (RocksDbSharp.RocksDb db = OpenNative(nativeDir.Path, static (o, native) =>
               {
                   native.rocksdb_options_set_compaction_pri(o.Handle, RoundRobinPri);
                   o.SetLevel0FileNumCompactionTrigger(2);
               }))
        {
            // Two files on the base level: [a..d] and [l..p].
            db.Put(U("a"), U("1"));
            db.Put(U("d"), U("1"));
            FlushAndWait(db);
            db.CompactRange((byte[]?)null!, null!);
            db.Put(U("l"), U("1"));
            db.Put(U("p"), U("1"));
            FlushAndWait(db);
            db.CompactRange((byte[]?)null!, null!);
            FlushAndWait(db);
            nativeBase = Shape(db);
            db.Put(U("m"), U("2"));
            db.Put(U("n"), U("2"));
            FlushAndWait(db);
            db.Put(U("b"), U("3"));
            db.Put(U("c"), U("3"));
            FlushAndWait(db);
            nativeShape = Shape(db);
        }
        using (RocksDatabase db = OpenManaged(managedDir.Path, static cf =>
               {
                   cf.CompactionPri = CompactionPri.RoundRobin;
                   cf.Level0FileNumCompactionTrigger = 2;
               }))
        {
            db.Put(U("a"), U("1"));
            db.Put(U("d"), U("1"));
            FlushAndWait(db);
            db.CompactRange();
            db.Put(U("l"), U("1"));
            db.Put(U("p"), U("1"));
            FlushAndWait(db);
            db.CompactRange();
            FlushAndWait(db);
            Assert.AreEqual("L6[a..d] L6[l..p]", nativeBase);
            Assert.AreEqual(nativeBase, Shape(db));
            db.Put(U("m"), U("2"));
            db.Put(U("n"), U("2"));
            FlushAndWait(db);
            db.Put(U("b"), U("3"));
            db.Put(U("c"), U("3"));
            FlushAndWait(db);
            // Native compacts the oldest L0 file [m..n] with [l..p] and leaves the newer [b..c] in L0.
            Assert.AreEqual("L0[b..c] L6[a..d] L6[l..p]", nativeShape);
            Assert.AreEqual(nativeShape, Shape(db));
        }
    }

    /// <summary>
    /// Compaction::IsTrivialMove (db/compaction/compaction.cc) requires num_input_levels() == 1, and the universal
    /// builder always creates <c>max_output_level + 1</c> input levels (PickCompactionWithSortedRunRange /
    /// PickCompactionToReduceSortedRuns), most of them empty. Without allow_trivial_move a multi-level universal
    /// compaction of non-overlapping L0 files is therefore a real compaction into one file. The port dropped the empty
    /// input levels and moved the four files to the last level.
    /// </summary>
    [TestMethod]
    public void UniversalCompactionOfNonOverlappingRunsIsNotATrivialMoveLikeNative()
    {
        using var nativeDir = new TempDir("a4pick-univ-native");
        using var managedDir = new TempDir("a4pick-univ-managed");
        string nativeShape;
        using (RocksDbSharp.RocksDb db = OpenNative(nativeDir.Path, static (o, _) => o.SetCompactionStyle(RocksDbSharp.Compaction.Universal)))
        {
            for (int i = 0; i < 4; i++)
            {
                for (int j = 0; j < 10; j++) db.Put(U("k" + i + j), U("value"));
                FlushAndWait(db);
            }
            nativeShape = Shape(db);
        }
        using (RocksDatabase db = OpenManaged(managedDir.Path, static cf => cf.CompactionStyle = CompactionStyle.Universal))
        {
            for (int i = 0; i < 4; i++)
            {
                for (int j = 0; j < 10; j++) db.Put(U("k" + i + j), U("value"));
                FlushAndWait(db);
            }
            Assert.AreEqual(nativeShape, Shape(db));
        }
    }
}
