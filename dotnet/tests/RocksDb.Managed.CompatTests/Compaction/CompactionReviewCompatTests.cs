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
}
