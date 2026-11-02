using System.Text;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>
/// External file ingestion features of db/external_sst_file_ingestion_job.cc: overlapping files in one call (batches),
/// ingest_behind, the legacy write_global_seqno and allow_db_generated_files.
/// </summary>
[TestClass]
public sealed class IngestionFeatureTests
{
    private static string WriteSst(IFileSystem fs, string path, int from, int to, string tag)
    {
        using var w = new SstFileWriter(new ColumnFamilyOptions(), fs);
        w.Open(path);
        for (int i = from; i < to; i++) w.Put(Key(i), B(tag + i));
        w.Finish();
        return path;
    }

    private static MemoryFileSystem NewFsWithExt()
    {
        MemoryFileSystem fs = NewFs();
        fs.CreateDirectoryIfMissing("/ext");
        return fs;
    }

    private static LiveFileMetadata FileStarting(RocksDatabase db, int key) =>
        db.GetLiveFilesMetadata().Single(m => m.SmallestKey.AsSpan().SequenceEqual(Key(key)));

    [TestMethod]
    public void OverlappingFilesAreIngestedInBatchesAndLaterFilesWin()
    {
        MemoryFileSystem fs = NewFsWithExt();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        db.Put(B("unrelated"), B("x")); // seq 1
        // Batches: [1, 2] (disjoint), [3] (overlaps 1), [4] (overlaps 3).
        db.IngestExternalFiles(
        [
            WriteSst(fs, "/ext/1.sst", 0, 10, "a"), WriteSst(fs, "/ext/2.sst", 20, 30, "b"),
            WriteSst(fs, "/ext/3.sst", 5, 25, "c"), WriteSst(fs, "/ext/4.sst", 8, 9, "d"),
        ], new IngestExternalFileOptions());
        Assert.AreEqual("a4", GetString(db, S(Key(4))!));
        Assert.AreEqual("c5", GetString(db, S(Key(5))!));
        Assert.AreEqual("d8", GetString(db, S(Key(8))!));
        Assert.AreEqual("c24", GetString(db, S(Key(24))!));
        Assert.AreEqual("b25", GetString(db, S(Key(25))!));
        // Every file gets its own sequence number, in argument order (AssignLevelsForOneBatch).
        Assert.AreEqual(2UL, FileStarting(db, 0).LargestSeqno);
        Assert.AreEqual(3UL, FileStarting(db, 20).LargestSeqno);
        Assert.AreEqual(4UL, FileStarting(db, 5).LargestSeqno);
        Assert.AreEqual(5UL, FileStarting(db, 8).LargestSeqno);
        Assert.AreEqual(5UL, db.GetLatestSequenceNumber());
        // Later batches sit above earlier ones.
        Assert.IsTrue(FileStarting(db, 5).Level < FileStarting(db, 0).Level);
        // A batch above an L0 batch must go to L0 too (must_assign_to_l0), newer epoch first.
        Assert.AreEqual(0, FileStarting(db, 5).Level, "levels between L0 and the base level are skipped");
        Assert.AreEqual(0, FileStarting(db, 8).Level);
        Assert.IsTrue(FileStarting(db, 8).EpochNumber > FileStarting(db, 5).EpochNumber);

        db.Dispose();
        using RocksDatabase re = RocksDatabase.Open(DbPath, NewOptions(fs));
        Assert.AreEqual("d8", GetString(re, S(Key(8))!));
        Assert.AreEqual("c9", GetString(re, S(Key(9))!));
        re.CompactRange();
        Assert.AreEqual("d8", GetString(re, S(Key(8))!));
        Assert.AreEqual("b29", GetString(re, S(Key(29))!));
    }

    [TestMethod]
    public void OverlappingFilesRequireGlobalSeqnoAndRejectIngestBehind()
    {
        MemoryFileSystem fs = NewFsWithExt();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        string[] files = [WriteSst(fs, "/ext/1.sst", 0, 10, "a"), WriteSst(fs, "/ext/2.sst", 5, 15, "b")];
        RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => db.IngestExternalFiles(files, new IngestExternalFileOptions { AllowGlobalSeqNo = false }));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        Assert.AreEqual(0, db.GetLiveFilesMetadata().Count);
        Assert.IsFalse(fs.GetChildren(DbPath).Any(c => c.EndsWith(".sst", StringComparison.Ordinal)), "prepared copies are removed");
    }

    private static ColumnFamilyOptions IngestBehindCf() => new()
    {
        CompactionStyle = CompactionStyle.Universal,
        CfAllowIngestBehind = true,
        NumLevels = 3,
    };

    [TestMethod]
    public void IngestBehindPlacesFilesBelowExistingData()
    {
        MemoryFileSystem fs = NewFsWithExt();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), IngestBehindCf());
        db.Put(Key(3), B("new3"));
        db.Flush();
        db.Put(Key(4), B("new4"));
        db.IngestExternalFiles([WriteSst(fs, "/ext/1.sst", 0, 10, "old")], new IngestExternalFileOptions { IngestBehind = true });
        // Existing keys win; the back-filled ones are visible; no sequence number consumed.
        Assert.AreEqual("new3", GetString(db, S(Key(3))!));
        Assert.AreEqual("new4", GetString(db, S(Key(4))!));
        Assert.AreEqual("old5", GetString(db, S(Key(5))!));
        Assert.AreEqual(2UL, db.GetLatestSequenceNumber());
        LiveFileMetadata ingested = FileStarting(db, 0);
        Assert.AreEqual(2, ingested.Level);
        Assert.AreEqual(0UL, ingested.LargestSeqno);
        Assert.AreEqual(FileMetaData.ReservedEpochNumberForFileIngestedBehind, ingested.EpochNumber);

        // Compaction never writes to the reserved last level.
        db.CompactRange();
        Assert.AreEqual("new3", GetString(db, S(Key(3))!));
        Assert.AreEqual("new4", GetString(db, S(Key(4))!));
        Assert.AreEqual("old9", GetString(db, S(Key(9))!));
        Assert.IsTrue(db.GetLiveFilesMetadata().Where(m => m.Level == 2).All(m => m.LargestSeqno == 0));

        // A second file must fit in the last level.
        RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() =>
            db.IngestExternalFiles([WriteSst(fs, "/ext/2.sst", 5, 15, "x")], new IngestExternalFileOptions { IngestBehind = true }));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        db.IngestExternalFiles([WriteSst(fs, "/ext/3.sst", 20, 30, "y")], new IngestExternalFileOptions { IngestBehind = true });
        Assert.AreEqual("y25", GetString(db, S(Key(25))!));
    }

    [TestMethod]
    public void IngestBehindRequiresAllowIngestBehind()
    {
        MemoryFileSystem fs = NewFsWithExt();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() =>
                db.IngestExternalFiles([WriteSst(fs, "/ext/1.sst", 0, 10, "a")], new IngestExternalFileOptions { IngestBehind = true }));
            Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
            e = Assert.ThrowsExactly<RocksDbException>(() =>
                db.IngestExternalFiles(["/ext/1.sst"], new IngestExternalFileOptions { MoveFiles = true, LinkFiles = true }));
            Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        }
        // The deprecated DB-wide allow_ingest_behind also enables it (ColumnFamilyData::AllowIngestBehind).
        DbOptions o = NewOptions(fs);
        o.AllowIngestBehind = true;
        using RocksDatabase db2 = RocksDatabase.Open("/db2", o, new ColumnFamilyOptions { CompactionStyle = CompactionStyle.Universal });
        db2.Put(Key(1), B("new"));
        db2.IngestExternalFiles(["/ext/1.sst"], new IngestExternalFileOptions { IngestBehind = true });
        Assert.AreEqual("new", GetString(db2, S(Key(1))!));
        Assert.AreEqual("a2", GetString(db2, S(Key(2))!));
        Assert.AreEqual(db2.DefaultColumnFamily.Options.NumLevels - 1, FileStarting(db2, 0).Level);
    }

    /// <summary>Offset of the 8-byte value of rocksdb.external_sst_file.global_seqno in a properties block (restart interval 1).</summary>
    private static int GlobalSeqnoValueOffset(byte[] file)
    {
        // The key may share a prefix with the previous property key; its non-shared tail ends with this name.
        byte[] name = Encoding.ASCII.GetBytes("global_seqno");
        int at = file.AsSpan().IndexOf(name);
        Assert.IsTrue(at > 0);
        return at + name.Length;
    }

    [TestMethod]
    public void WriteGlobalSeqnoRewritesThePropertiesBlock()
    {
        MemoryFileSystem fs = NewFsWithExt();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            db.Put(Key(5), B("old"));
            db.Flush();
            string ext = WriteSst(fs, "/ext/1.sst", 0, 10, "a");
            byte[] original = fs.ReadAllBytes(ext);
            db.IngestExternalFiles([ext], new IngestExternalFileOptions { WriteGlobalSeqno = true });
            LiveFileMetadata f = FileStarting(db, 0);
            Assert.AreEqual(2UL, f.LargestSeqno);
            byte[] ingested = fs.ReadAllBytes(f.Directory + "/" + f.Name.TrimStart('/'));
            int off = GlobalSeqnoValueOffset(ingested);
            Assert.AreEqual(2UL, BitConverter.ToUInt64(ingested, off), "seqno written in place (little-endian fixed64)");
            Assert.AreEqual(0UL, BitConverter.ToUInt64(original, off), "the external file is untouched");
            // Only those 8 bytes differ.
            Assert.AreEqual(original.Length, ingested.Length);
            for (int i = 0; i < original.Length; i++)
            {
                if (i < off || i >= off + 8) Assert.AreEqual(original[i], ingested[i], "byte " + i);
            }
            Assert.AreEqual("a5", GetString(db, S(Key(5))!));
            // Without write_global_seqno the file bytes stay as written by SstFileWriter.
            string ext2 = WriteSst(fs, "/ext/2.sst", 0, 3, "b");
            db.IngestExternalFiles([ext2], new IngestExternalFileOptions());
            LiveFileMetadata f2 = db.GetLiveFilesMetadata().MaxBy(m => m.LargestSeqno)!;
            Assert.AreEqual(3UL, f2.LargestSeqno);
            CollectionAssert.AreEqual(fs.ReadAllBytes(ext2), fs.ReadAllBytes(f2.Directory + "/" + f2.Name.TrimStart('/')));
        }
        // Reopen with checksum verification: the stale properties checksum is accepted through the zero placeholder.
        using RocksDatabase re = RocksDatabase.Open(DbPath, NewOptions(fs));
        Assert.AreEqual("a5", GetString(re, S(Key(5))!));
        Assert.AreEqual("b1", GetString(re, S(Key(1))!));
        Assert.AreEqual("a7", GetString(re, S(Key(7))!));
    }

    [TestMethod]
    public void CorruptedPropertiesBlockIsStillDetected()
    {
        MemoryFileSystem fs = NewFsWithExt();
        string ext = WriteSst(fs, "/ext/1.sst", 0, 10, "a");
        byte[] bytes = fs.ReadAllBytes(ext);
        // Flip a byte of the global seqno property name: not explained by the legacy rewrite.
        int off = GlobalSeqnoValueOffset(bytes);
        bytes[off - 3] ^= 0x20;
        using (IWritableFile w = fs.CreateWritable("/ext/bad.sst")) w.Append(bytes);
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => db.IngestExternalFiles(["/ext/bad.sst"], new IngestExternalFileOptions()));
        Assert.AreEqual(StatusCode.Corruption, e.Code);
    }

    [TestMethod]
    public void AllowDbGeneratedFilesKeepsSequenceNumbers()
    {
        MemoryFileSystem fs = NewFsWithExt();
        LiveFileMetadata src;
        using (RocksDatabase s = RocksDatabase.Open("/src", NewOptions(fs)))
        {
            for (int i = 0; i < 20; i++) s.Put(Key(i), B("s" + i));
            s.Delete(Key(3));
            s.Flush();
            src = s.GetLiveFilesMetadata().Single();
            // Not an SstFileWriter file: rejected without the option.
            using RocksDatabase plain = RocksDatabase.Open("/plain", NewOptions(fs));
            RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() =>
                plain.IngestExternalFiles([src.Directory + "/" + src.Name.TrimStart('/')], new IngestExternalFileOptions()));
            Assert.AreEqual(StatusCode.Corruption, e.Code);
        }
        string path = src.Directory + "/" + src.Name.TrimStart('/');
        var opts = new IngestExternalFileOptions { AllowDbGeneratedFiles = true, LinkFiles = true, SnapshotConsistency = false, AllowBlockingFlush = false };
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            db.IngestExternalFiles([path], opts);
            Assert.IsTrue(fs.FileExists(path), "link_files keeps the source");
            LiveFileMetadata f = db.GetLiveFilesMetadata().Single();
            Assert.AreEqual(src.SmallestSeqno, f.SmallestSeqno);
            Assert.AreEqual(src.LargestSeqno, f.LargestSeqno);
            Assert.AreEqual(21UL, db.GetLatestSequenceNumber(), "last sequence raised to the file's largest seqno");
            Assert.AreEqual("s7", GetString(db, S(Key(7))!));
            Assert.IsNull(GetString(db, S(Key(3))!));
            db.Put(Key(100), B("later"));
            Assert.AreEqual(22UL, db.GetLatestSequenceNumber());

            // Overlap with existing data needs a new seqno, which DB-generated files can't get.
            db.Flush();
            RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => db.IngestExternalFiles([path], opts));
            Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        }
        using RocksDatabase re = RocksDatabase.Open(DbPath, NewOptions(fs));
        Assert.AreEqual("s19", GetString(re, S(Key(19))!));
        Assert.AreEqual("later", GetString(re, S(Key(100))!));
    }
}
