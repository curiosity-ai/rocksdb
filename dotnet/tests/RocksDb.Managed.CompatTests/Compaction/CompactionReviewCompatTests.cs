using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Db;

namespace RocksDb.Managed.CompatTests.Compaction;

/// <summary>
/// Native agreement on the compaction / version / MANIFEST findings of the second correctness review: consistency
/// checks on MANIFEST replay, persisted round-robin cursors after trivial moves, db_paths placement and the MANIFEST
/// auto-tuning record.
/// </summary>
[TestClass]
public sealed class CompactionReviewCompatTests
{
    private static byte[] B(string s) => Encoding.UTF8.GetBytes(s);

    /// <summary>
    /// A MANIFEST whose L1 holds overlapping files (written by the managed port with force_consistency_checks off):
    /// the managed port refuses to open it with Corruption (VersionBuilder::SaveTo -> CheckConsistency, C++ 11.14).
    /// Native 11.12 does not fail the open but does not use the inconsistent version either (it recovers an LSM without
    /// those files); neither engine serves the overlapping level.
    /// </summary>
    [TestMethod]
    public void OverlappingL1ManifestIsNotAcceptedByEitherEngine()
    {
        using var dir = new TempDir("overlap-l1");
        var unchecked_ = new ColumnFamilyOptions { DisableAutoCompactions = true, ForceConsistencyChecks = false };
        using (RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions { CreateIfMissing = true }, unchecked_))
        {
            db.Put(B("a"), B("1"));
            db.Put(B("c"), B("1"));
            db.Flush();
            db.Put(B("b"), B("2"));
            db.Put(B("d"), B("2"));
            db.Flush();
            ColumnFamilyData cfd = db.DefaultColumnFamily.Data;
            var edit = new VersionEdit { ColumnFamily = cfd.Id };
            foreach (FileMetaData f in cfd.Current.Storage.LevelFiles(0))
            {
                edit.DeleteFile(0, f.FileNumber);
                edit.AddFile(1, f.Clone());
            }
            db.Impl.Versions.LogAndApplyAsync(cfd, edit, synchronous: true, CancellationToken.None).GetAwaiter().GetResult();
        }
        var managed = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(dir.Path, new DbOptions(), new ColumnFamilyOptions()).Dispose());
        Assert.AreEqual(StatusCode.Corruption, managed.Code);
        StringAssert.Contains(managed.Message, "L1 has overlapping ranges");
        // The failed managed open left the DB as it was; native (no automatic compaction, so nothing moves on open)
        // does not present the overlapping files.
        var options = new RocksDbSharp.DbOptions().SetLevelCompactionDynamicLevelBytes(false).SetDisableAutoCompactions(1);
        using var native = RocksDbSharp.RocksDb.Open(options, dir.Path);
        Assert.AreEqual("0", native.GetProperty("rocksdb.num-files-at-level1"));
    }

    private static ulong CurrentManifestNumber(string dir)
    {
        string current = File.ReadAllText(Path.Combine(dir, "CURRENT")).Trim();
        return ulong.Parse(current.AsSpan("MANIFEST-".Length), System.Globalization.CultureInfo.InvariantCulture);
    }

    private static List<VersionEdit> ReadManifest(string dir)
    {
        var edits = new List<VersionEdit>();
        using ManifestLogReader reader = ManifestLogReader.Open(RocksDb.Managed.Env.LocalFileSystem.Instance, FileNames.DescriptorFileName(dir, CurrentManifestNumber(dir)));
        while (reader.TryReadRecord(out ReadOnlySpan<byte> record)) edits.Add(VersionEdit.DecodeFrom(record));
        return edits;
    }

    /// <summary>
    /// kLastCompactedManifestFileSize both ways: the record native 11.12 writes at the end of a new MANIFEST's snapshot
    /// is decoded by the managed port (and its MANIFEST opens), and the record the managed port writes is accepted by
    /// native, which opens and reads the DB.
    /// </summary>
    [TestMethod]
    public void LastCompactedManifestFileSizeRecordBothWays()
    {
        using var dir = new TempDir("lcm");
        using (var ndb = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions().SetCreateIfMissing(true), dir.Path))
        {
            ndb.Put(B("n"), B("1"));
            ndb.Flush(new RocksDbSharp.FlushOptions());
        }
        VersionEdit? nativeRecord = ReadManifest(dir.Path).Find(e => e.LastCompactedManifestFileSize is not null);
        Assert.IsNotNull(nativeRecord, "native MANIFEST has no kLastCompactedManifestFileSize record");
        using (RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions(), new ColumnFamilyOptions()))
        {
            Assert.AreEqual("1", Encoding.UTF8.GetString(db.Get(B("n"))!));
            db.Put(B("m"), B("2"));
            db.Flush();
        }
        Assert.IsNotNull(ReadManifest(dir.Path).Find(e => e.LastCompactedManifestFileSize is not null), "managed MANIFEST has no record");
        using var native = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions(), dir.Path);
        Assert.AreEqual("2", native.Get("m"));
        Assert.AreEqual("1", native.Get("n"));
    }

    /// <summary>rocksdb_options_set_db_paths with the given directories (unlimited target sizes).</summary>
    private static unsafe void SetNativeDbPaths(RocksDbSharp.DbOptions options, string[] paths)
    {
        RocksDbSharp.Native native = RocksDbSharp.Native.Instance;
        var handles = new IntPtr[paths.Length];
        for (int i = 0; i < paths.Length; i++) handles[i] = native.rocksdb_dbpath_create(paths[i], ulong.MaxValue);
        fixed (IntPtr* p = handles) native.rocksdb_options_set_db_paths(options.Handle, (IntPtr)p, (UIntPtr)paths.Length);
        foreach (IntPtr h in handles) native.rocksdb_dbpath_destroy(h);
    }

    /// <summary>
    /// db_paths path ids agree with native: managed flushes to db_paths[0] and compacts with target_path_id = 1 into
    /// db_paths[1]; native, opened with the same db_paths, finds every file, and vice versa.
    /// </summary>
    [TestMethod]
    public void DbPathsPlacementMatchesNative()
    {
        using var dir = new TempDir("dbpaths");
        string db = Path.Combine(dir.Path, "db");
        string[] paths = [Path.Combine(dir.Path, "p0"), Path.Combine(dir.Path, "p1")];
        var o = new DbOptions { CreateIfMissing = true };
        foreach (string p in paths) o.DbPaths.Add((p, 1UL << 40));
        using (RocksDatabase m = RocksDatabase.Open(db, o, new ColumnFamilyOptions { DisableAutoCompactions = true }))
        {
            for (int i = 0; i < 200; i++) m.Put(B("k" + i.ToString("D4", System.Globalization.CultureInfo.InvariantCulture)), B("v" + i));
            m.Flush();
            m.CompactRange(new CompactRangeOptions { TargetPathId = 1, BottommostLevelCompaction = BottommostLevelCompaction.Force }, null, null);
            m.Put(B("z"), B("flushed"));
            m.Flush();
        }
        Assert.IsTrue(Directory.GetFiles(paths[1], "*.sst").Length > 0);
        Assert.AreEqual(1, Directory.GetFiles(paths[0], "*.sst").Length);
        Assert.AreEqual(0, Directory.GetFiles(db, "*.sst").Length);
        var nativeOptions = new RocksDbSharp.DbOptions();
        SetNativeDbPaths(nativeOptions, paths);
        using (var n = RocksDbSharp.RocksDb.Open(nativeOptions, db))
        {
            for (int i = 0; i < 200; i++) Assert.AreEqual("v" + i, n.Get("k" + i.ToString("D4", System.Globalization.CultureInfo.InvariantCulture)));
            Assert.AreEqual("flushed", n.Get("z"));
            n.Put("y", "native");
            n.Flush(new RocksDbSharp.FlushOptions());
        }
        using RocksDatabase again = RocksDatabase.Open(db, new DbOptions { DbPaths = { (paths[0], 1UL << 40), (paths[1], 1UL << 40) } }, new ColumnFamilyOptions());
        Assert.AreEqual("native", Encoding.UTF8.GetString(again.Get(B("y"))!));
        Assert.AreEqual("v7", Encoding.UTF8.GetString(again.Get(B("k0007"))!));
    }
}
