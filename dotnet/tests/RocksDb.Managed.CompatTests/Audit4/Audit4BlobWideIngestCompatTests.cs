using System.Runtime.InteropServices;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Env;

namespace RocksDb.Managed.CompatTests.Audit4;

/// <summary>
/// Audit #4 (slice BlobWideIngest): differential tests of external file ingestion against native RocksDB. Every scenario
/// builds the same LSM shape on both engines, ingests the same files with the same options and compares the outcome
/// (success or the failure), the level and sequence numbers of every live file and the visible contents.
/// </summary>
[TestClass]
public sealed class Audit4BlobWideIngestCompatTests
{
    private static readonly RocksDbSharp.Native N = RocksDbSharp.Native.Instance;

    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);

    private static string WriteSst(string path, params string[] keys)
    {
        using var w = new SstFileWriter(new ColumnFamilyOptions(), LocalFileSystem.Instance);
        w.Open(path);
        foreach (string k in keys) w.Put(U(k), U("ingested-" + k));
        w.Finish();
        return path;
    }

    private sealed record FileInfo(int Level, string Smallest, string Largest, ulong SmallestSeqno, ulong LargestSeqno);

    private static string Describe(List<FileInfo> files) => string.Join("; ", files);

    private static List<FileInfo> NativeFiles(RocksDbSharp.RocksDb db)
    {
        var result = new List<FileInfo>();
        IntPtr lf = N.rocksdb_livefiles(db.Handle);
        try
        {
            int n = N.rocksdb_livefiles_count(lf);
            for (int i = 0; i < n; i++)
            {
                IntPtr s = N.rocksdb_livefiles_smallestkey(lf, i, out UIntPtr slen);
                IntPtr l = N.rocksdb_livefiles_largestkey(lf, i, out UIntPtr llen);
                result.Add(new FileInfo(N.rocksdb_livefiles_level(lf, i), Marshal.PtrToStringUTF8(s, (int)slen)!, Marshal.PtrToStringUTF8(l, (int)llen)!,
                    N.rocksdb_livefiles_smallest_seqno(lf, i), N.rocksdb_livefiles_largest_seqno(lf, i)));
            }
        }
        finally
        {
            N.rocksdb_livefiles_destroy(lf);
        }
        result.Sort(static (a, b) => (a.Smallest, a.LargestSeqno, a.Level).CompareTo((b.Smallest, b.LargestSeqno, b.Level)));
        return result;
    }

    private static List<FileInfo> ManagedFiles(RocksDatabase db)
    {
        var result = new List<FileInfo>();
        foreach (LiveFileMetadata f in db.GetLiveFilesMetadata())
        {
            result.Add(new FileInfo(f.Level, Encoding.UTF8.GetString(f.SmallestKey), Encoding.UTF8.GetString(f.LargestKey), f.SmallestSeqno, f.LargestSeqno));
        }
        result.Sort(static (a, b) => (a.Smallest, a.LargestSeqno, a.Level).CompareTo((b.Smallest, b.LargestSeqno, b.Level)));
        return result;
    }

    private static SortedDictionary<string, string> NativeScan(RocksDbSharp.RocksDb db)
    {
        var d = new SortedDictionary<string, string>(StringComparer.Ordinal);
        using RocksDbSharp.Iterator it = db.NewIterator();
        for (it.SeekToFirst(); it.Valid(); it.Next()) d[Encoding.UTF8.GetString(it.Key())] = Encoding.UTF8.GetString(it.Value());
        return d;
    }

    private static SortedDictionary<string, string> ManagedScan(RocksDatabase db)
    {
        var d = new SortedDictionary<string, string>(StringComparer.Ordinal);
        using Iterator it = db.NewIterator();
        for (it.SeekToFirst(); it.Valid; it.Next()) d[Encoding.UTF8.GetString(it.Key)] = Encoding.UTF8.GetString(it.Value);
        return d;
    }

    /// <summary>One write of a scenario's preparation: P = put, R = delete range [Key, End), F = flush.</summary>
    private readonly record struct Step(char Op, string Key, string End)
    {
        public static Step Put(string k) => new('P', k, "");
        public static Step DelRange(string a, string b) => new('R', a, b);
        public static readonly Step Flush = new('F', "", "");
    }

    private sealed record Scenario(string Name, Step[] Steps, string[][] Files, bool AllowBlockingFlush, bool AllowGlobalSeqno);

    private static void NativeApply(RocksDbSharp.RocksDb db, Step s)
    {
        switch (s.Op)
        {
            case 'P':
                db.Put(U(s.Key), U("db-" + s.Key));
                break;
            case 'R':
                {
                    using var batch = new RocksDbSharp.WriteBatch();
                    byte[] a = U(s.Key), b = U(s.End);
                    batch.DeleteRange(a, (ulong)a.Length, b, (ulong)b.Length);
                    db.Write(batch);
                    break;
                }
            default:
                db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
                break;
        }
    }

    private static void ManagedApply(RocksDatabase db, Step s)
    {
        switch (s.Op)
        {
            case 'P':
                db.Put(U(s.Key), U("db-" + s.Key));
                break;
            case 'R':
                db.DeleteRange(U(s.Key), U(s.End));
                break;
            default:
                db.Flush();
                break;
        }
    }

    private static void RunScenario(Scenario sc)
    {
        using var dir = new TempDir("audit4-ingest");
        var files = new string[sc.Files.Length];
        for (int i = 0; i < files.Length; i++) files[i] = WriteSst(Path.Combine(dir.Path, i + ".sst"), sc.Files[i]);

        string nativeOutcome = "ok";
        List<FileInfo> nativeFiles;
        SortedDictionary<string, string> nativeData;
        // Auto compactions off on both sides: the placement under test is ingestion's.
        var no = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetDisableAutoCompactions(1);
        using (var db = RocksDbSharp.RocksDb.Open(no, Path.Combine(dir.Path, "native")))
        {
            foreach (Step s in sc.Steps) NativeApply(db, s);
            try
            {
                db.IngestExternalFiles(files, new RocksDbSharp.IngestExternalFileOptions().SetAllowBlockingFlush(sc.AllowBlockingFlush)
                    .SetAllowGlobalSeqno(sc.AllowGlobalSeqno));
            }
            catch (RocksDbSharp.RocksDbException e)
            {
                nativeOutcome = e.Message;
            }
            nativeFiles = NativeFiles(db);
            nativeData = NativeScan(db);
        }

        string managedOutcome = "ok";
        using (RocksDatabase db = RocksDatabase.Open(Path.Combine(dir.Path, "managed"), new DbOptions { CreateIfMissing = true },
                   new ColumnFamilyOptions { DisableAutoCompactions = true }))
        {
            foreach (Step s in sc.Steps) ManagedApply(db, s);
            try
            {
                db.IngestExternalFiles(files, new IngestExternalFileOptions { AllowBlockingFlush = sc.AllowBlockingFlush, AllowGlobalSeqNo = sc.AllowGlobalSeqno });
            }
            catch (RocksDbException e)
            {
                managedOutcome = e.Code + ": " + e.Message;
            }
            // Success or failure must agree (native messages are "Invalid argument: ..."; compare the message tail).
            if (nativeOutcome == "ok") Assert.AreEqual("ok", managedOutcome, sc.Name + ": native succeeded");
            else
            {
                Assert.AreNotEqual("ok", managedOutcome, sc.Name + ": native failed with " + nativeOutcome);
                string tail = nativeOutcome[(nativeOutcome.IndexOf(": ", StringComparison.Ordinal) + 2)..];
                StringAssert.Contains(managedOutcome, tail, sc.Name);
            }
            Assert.AreEqual(Describe(nativeFiles), Describe(ManagedFiles(db)), sc.Name + ": levels and sequence numbers");
            CollectionAssert.AreEqual(nativeData, ManagedScan(db), sc.Name + ": contents");
        }
    }

    /// <summary>
    /// ColumnFamilyData::RangesOverlapWithMemtables and Version::OverlapWithLevelIterator end with
    /// range_del_agg.IsRangeOverlapped(smallest, largest): a range tombstone in the memtable / in a level file counts as
    /// an overlap only when it intersects the ingested file's key range (start inclusive, end exclusive). The port
    /// treated any memtable holding a range tombstone, and any overlapping level file holding one, as an overlap, so
    /// files were flushed for / placed above / given a sequence number (or the ingestion failed with
    /// allow_blocking_flush = false / allow_global_seqno = false) where native RocksDB ingests them at seqno 0.
    /// </summary>
    [TestMethod]
    public void RangeTombstonesOnlyCountWhenTheyIntersectTheFile()
    {
        Step[] l0WithTombstone = [Step.Put("a"), Step.Put("z"), Step.DelRange("m", "n"), Step.Flush];
        Step[] memWithTombstone = [Step.Put("a"), Step.Put("z"), Step.Flush, Step.DelRange("m", "n")];
        Scenario[] scenarios =
        [
            // Level file with a tombstone elsewhere: no overlap, bottommost level, seqno 0.
            new("l0-tombstone-outside", l0WithTombstone, [["c", "d"]], true, true),
            new("l0-tombstone-outside-no-global-seqno", l0WithTombstone, [["c", "d"]], true, false),
            // The tombstone's end is exclusive, its start inclusive.
            new("l0-tombstone-end-touches", l0WithTombstone, [["n", "p"]], true, true),
            new("l0-tombstone-start-touches", l0WithTombstone, [["k", "m"]], true, true),
            new("l0-tombstone-covers", l0WithTombstone, [["m1", "m2"]], true, true),
            new("l0-tombstone-covers-no-global-seqno", l0WithTombstone, [["m1", "m2"]], true, false),
            new("l0-file-spans-tombstone", l0WithTombstone, [["l", "o"]], true, true),
            // Memtable with a tombstone elsewhere: no flush needed.
            new("mem-tombstone-outside-no-flush", memWithTombstone, [["c", "d"]], false, true),
            new("mem-tombstone-end-touches-no-flush", memWithTombstone, [["n", "p"]], false, true),
            new("mem-tombstone-start-touches-no-flush", memWithTombstone, [["k", "m"]], false, true),
            new("mem-tombstone-covers-no-flush", memWithTombstone, [["m1", "m2"]], false, true),
            new("mem-tombstone-covers-flush", memWithTombstone, [["m1", "m2"]], true, true),
            new("mem-tombstone-outside-flush-allowed", memWithTombstone, [["c", "d"]], true, true),
            // Several files in one call, one of them under the tombstone.
            new("l0-two-files", l0WithTombstone, [["c", "d"], ["m5", "m6"]], true, true),
        ];
        foreach (Scenario sc in scenarios) RunScenario(sc);
    }

    /// <summary>
    /// ExternalSstFileIngestionJob::Prepare has no check for a path listed twice: the two copies are overlapping files,
    /// each ingested with its own sequence number. The outcome must match native.
    /// </summary>
    [TestMethod]
    public void SameFileListedTwiceMatchesNative()
    {
        using var dir = new TempDir("audit4-ingest-dup");
        string f = WriteSst(Path.Combine(dir.Path, "f.sst"), "b", "c");
        string nativeOutcome = "ok";
        List<FileInfo> nativeFiles;
        var no = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetDisableAutoCompactions(1);
        using (var db = RocksDbSharp.RocksDb.Open(no, Path.Combine(dir.Path, "native")))
        {
            db.Put(U("a"), U("1"));
            try
            {
                db.IngestExternalFiles([f, f], new RocksDbSharp.IngestExternalFileOptions());
            }
            catch (RocksDbSharp.RocksDbException e)
            {
                nativeOutcome = e.Message;
            }
            nativeFiles = NativeFiles(db);
        }
        using (RocksDatabase db = RocksDatabase.Open(Path.Combine(dir.Path, "managed"), new DbOptions { CreateIfMissing = true },
                   new ColumnFamilyOptions { DisableAutoCompactions = true }))
        {
            db.Put(U("a"), U("1"));
            string managedOutcome = "ok";
            try
            {
                db.IngestExternalFiles([f, f], new IngestExternalFileOptions());
            }
            catch (RocksDbException e)
            {
                managedOutcome = e.Code + ": " + e.Message;
            }
            Assert.AreEqual(nativeOutcome == "ok", managedOutcome == "ok", "native: " + nativeOutcome + ", managed: " + managedOutcome);
            Assert.AreEqual(Describe(nativeFiles), Describe(ManagedFiles(db)));
            Assert.AreEqual("ingested-b", Encoding.UTF8.GetString(db.Get(U("b"))!));
        }
    }
}
