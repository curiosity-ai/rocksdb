using System.Globalization;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Env;

namespace RocksDb.Managed.CompatTests.Audit5;

/// <summary>
/// Audit #5: standalone range tombstone files in universal compaction, against native RocksDB. The same SST files (a
/// range deletion alone in a file, ingested over older data) are ingested into a native and a managed universal DB with
/// automatic compactions on, and the files per level are compared after every WaitForCompact: while a snapshot older
/// than the tombstone exists, neither engine compacts the tombstone file (UniversalCompactionBuilder::ShouldSkipMarkedFile);
/// releasing the snapshot (DBImpl::ReleaseSnapshot) lets both compact it the same way.
/// </summary>
[TestClass]
public sealed class Audit5StandaloneRangeTombstoneCompatTests
{
    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);

    private enum Op { Ingest, Snapshot, Release, Wait }

    /// <summary>One step: ingest <see cref="Files"/> (one call), take or release snapshot <see cref="Id"/>, or wait.</summary>
    private readonly record struct Step(Op Op, string[] Files, int Id);

    private static Step Ingest(params string[] files) => new(Op.Ingest, files, 0);
    private static Step Snap(int id) => new(Op.Snapshot, [], id);
    private static Step Release(int id) => new(Op.Release, [], id);
    private static readonly Step Wait = new(Op.Wait, [], 0);

    private static string WriteSst(string path, params (string Key, string Value)[] entries)
    {
        using var w = new SstFileWriter(new ColumnFamilyOptions(), LocalFileSystem.Instance);
        w.Open(path);
        foreach ((string k, string v) in entries) w.Put(U(k), U(v));
        w.Finish();
        return path;
    }

    private static string WriteRangeDeletionSst(string path, string begin, string end)
    {
        using var w = new SstFileWriter(new ColumnFamilyOptions(), LocalFileSystem.Instance);
        w.Open(path);
        w.DeleteRange(U(begin), U(end));
        w.Finish();
        return path;
    }

    private static void NativeWaitForCompact(RocksDbSharp.RocksDb db)
    {
        RocksDbSharp.Native native = RocksDbSharp.Native.Instance;
        IntPtr wfc = native.rocksdb_wait_for_compact_options_create();
        try
        {
            native.rocksdb_wait_for_compact(db.Handle, wfc, out IntPtr err);
            if (err != IntPtr.Zero) Assert.Fail("native WaitForCompact failed");
        }
        finally
        {
            native.rocksdb_wait_for_compact_options_destroy(wfc);
        }
    }

    private static string Shape(Func<int, string> filesAtLevel)
    {
        var parts = new string[7];
        for (int l = 0; l < parts.Length; l++) parts[l] = filesAtLevel(l);
        return string.Join(",", parts);
    }

    /// <summary>The files per level after every Wait step, then the value of every key in <paramref name="keys"/>.</summary>
    private static List<string> RunNative(string path, Step[] steps, string[] keys)
    {
        var observed = new List<string>();
        var snapshots = new Dictionary<int, RocksDbSharp.Snapshot>();
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetCompactionStyle(RocksDbSharp.Compaction.Universal);
        using (var db = RocksDbSharp.RocksDb.Open(o, path))
        {
            foreach (Step s in steps)
            {
                switch (s.Op)
                {
                    case Op.Ingest: db.IngestExternalFiles(s.Files, new RocksDbSharp.IngestExternalFileOptions()); break;
                    case Op.Snapshot: snapshots[s.Id] = db.CreateSnapshot(); break;
                    case Op.Release:
                        snapshots[s.Id].Dispose();
                        snapshots.Remove(s.Id);
                        break;
                    case Op.Wait:
                        NativeWaitForCompact(db);
                        observed.Add(Shape(l => db.GetProperty("rocksdb.num-files-at-level" + l.ToString(CultureInfo.InvariantCulture))));
                        break;
                }
            }
            foreach (string k in keys) observed.Add(k + "=" + (db.Get(U(k)) is { } v ? Encoding.UTF8.GetString(v) : "NOT_FOUND"));
            foreach (RocksDbSharp.Snapshot snap in snapshots.Values) snap.Dispose();
        }
        return observed;
    }

    private static List<string> RunManaged(string path, Step[] steps, string[] keys)
    {
        var observed = new List<string>();
        var snapshots = new Dictionary<int, Snapshot>();
        using (RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true },
                   new ColumnFamilyOptions { CompactionStyle = CompactionStyle.Universal }))
        {
            foreach (Step s in steps)
            {
                switch (s.Op)
                {
                    case Op.Ingest: db.IngestExternalFiles(s.Files, new IngestExternalFileOptions()); break;
                    case Op.Snapshot: snapshots[s.Id] = db.GetSnapshot(); break;
                    case Op.Release:
                        snapshots[s.Id].Dispose();
                        snapshots.Remove(s.Id);
                        break;
                    case Op.Wait:
                        db.WaitForCompact();
                        observed.Add(Shape(l => db.GetProperty("rocksdb.num-files-at-level" + l.ToString(CultureInfo.InvariantCulture))!));
                        break;
                }
            }
            foreach (string k in keys) observed.Add(k + "=" + (db.Get(U(k)) is { } v ? Encoding.UTF8.GetString(v) : "NOT_FOUND"));
            foreach (Snapshot snap in snapshots.Values) snap.Dispose();
        }
        return observed;
    }

    private static void AssertSameAsNative(string name, Step[] steps, string[] keys, string[] expected)
    {
        using var dir = new TempDir("standalone-rangedel-" + name);
        // Both engines ingest the same external files (copied into each DB: move_files is off).
        List<string> native = RunNative(Path.Combine(dir.Path, "native"), steps, keys);
        List<string> managed = RunManaged(Path.Combine(dir.Path, "managed"), steps, keys);
        CollectionAssert.AreEqual(expected, native, name + " native: " + string.Join(" | ", native));
        CollectionAssert.AreEqual(native, managed, name + " managed: " + string.Join(" | ", managed));
    }

    /// <summary>
    /// ExternalSSTFileBasicTest.AtomicReplaceDataWithStandaloneRangeDeletion: old data in L6, a range deletion over all of
    /// it in L5 and the new data in L4. The tombstone file waits for the snapshot; after the release it and both L6
    /// files are compacted away.
    /// </summary>
    [TestMethod]
    public void TombstoneFileWaitsForSnapshotLikeNative()
    {
        using var files = new TempDir("standalone-rangedel-files");
        string file1 = WriteSst(Path.Combine(files.Path, "file1.sst"), ("a", "a1"), ("b", "b1"));
        string file2 = WriteSst(Path.Combine(files.Path, "file2.sst"), ("x", "x1"), ("y", "y1"));
        string rangeDel = WriteRangeDeletionSst(Path.Combine(files.Path, "rangedel.sst"), "a", "z");
        string file3 = WriteSst(Path.Combine(files.Path, "file3.sst"), ("a", "a2"), ("b", "b2"));
        string file4 = WriteSst(Path.Combine(files.Path, "file4.sst"), ("x", "x2"), ("y", "y2"));
        AssertSameAsNative("replace",
            [Ingest(file1, file2), Wait, Snap(1), Ingest(rangeDel, file3, file4), Wait, Release(1), Wait],
            ["a", "b", "x", "y"],
            ["0,0,0,0,0,0,2", "0,0,0,0,2,1,2", "0,0,0,0,2,0,0", "a=a2", "b=b2", "x=x2", "y=y2"]);
    }

    /// <summary>
    /// Two snapshots older than the tombstone: releasing the newer one leaves the file deferred, releasing the older one
    /// compacts it into L6.
    /// </summary>
    [TestMethod]
    public void OnlyTheOldestSnapshotReleaseCompactsLikeNative()
    {
        using var files = new TempDir("standalone-rangedel-files");
        string old = WriteSst(Path.Combine(files.Path, "old.sst"), ("a", "old-a"), ("c", "old-c"), ("m", "old-m"));
        string other = WriteSst(Path.Combine(files.Path, "other.sst"), ("q", "q"));
        string rangeDel = WriteRangeDeletionSst(Path.Combine(files.Path, "rd.sst"), "b", "n");
        AssertSameAsNative("two-snapshots",
            [Ingest(old), Snap(1), Ingest(other), Snap(2), Ingest(rangeDel), Wait, Release(2), Wait, Release(1), Wait],
            ["a", "c", "m", "q"],
            ["0,0,0,0,0,1,2", "0,0,0,0,0,1,2", "0,0,0,0,0,0,2", "a=old-a", "c=NOT_FOUND", "m=NOT_FOUND", "q=q"]);
    }

    /// <summary>
    /// ExternalSSTFileBasicTest.StandaloneRangeDeletionEndKeyIsExclusive: without a snapshot the tombstone file is
    /// compacted at once with the file it overlaps; its exclusive end keeps "b".
    /// </summary>
    [TestMethod]
    public void TombstoneFileWithoutSnapshotCompactsLikeNative()
    {
        using var files = new TempDir("standalone-rangedel-files");
        string data = WriteSst(Path.Combine(files.Path, "file1.sst"), ("a", "a"), ("b", "b"));
        string rangeDel = WriteRangeDeletionSst(Path.Combine(files.Path, "file2.sst"), "a", "b");
        AssertSameAsNative("no-snapshot",
            [Ingest(data), Ingest(rangeDel), Wait],
            ["a", "b"],
            ["0,0,0,0,0,0,1", "a=NOT_FOUND", "b=b"]);
    }
}
