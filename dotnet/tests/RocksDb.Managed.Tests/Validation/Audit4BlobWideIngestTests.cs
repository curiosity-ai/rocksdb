using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4, slice BlobWideIngest (blob files, wide columns, external file ingestion, column family export / import,
/// checkpoints): regression tests for divergences from the C++ sources, plus coverage of verified edge cases.
/// </summary>
[TestClass]
public sealed class Audit4BlobWideIngestTests
{
    private const string Path = "/audit4bwi";

    private static RocksDatabase OpenNoAutoCompaction(MemoryFileSystem fs) =>
        RocksDatabase.Open(Path, new DbOptions { CreateIfMissing = true, FileSystem = fs }, new ColumnFamilyOptions { DisableAutoCompactions = true });

    private static string WriteSst(IFileSystem fs, string path, params string[] keys)
    {
        using var w = new SstFileWriter(new ColumnFamilyOptions(), fs);
        w.Open(path);
        foreach (string k in keys) w.Put(B(k), B("ingested-" + k));
        w.Finish();
        return path;
    }

    private static LiveFileMetadata FileWithSmallest(RocksDatabase db, string smallest) =>
        db.GetLiveFilesMetadata().Single(m => m.SmallestKey.AsSpan().SequenceEqual(B(smallest)));

    // ---- ingestion: range tombstones ------------------------------------------------------------------------

    [TestMethod]
    public void IngestIgnoresMemtableRangeTombstoneOutsideTheFileRange()
    {
        // ColumnFamilyData::RangesOverlapWithMemtables (db/column_family.cc): after the point-key seek the only other
        // overlap is range_del_agg.IsRangeOverlapped(start, limit). A range tombstone [m, n) in the memtable does not
        // intersect a file covering [c, d], so ExternalSstFileIngestionJob::NeedsFlush reports no flush and the file is
        // ingested even with allow_blocking_flush = false. The port asked for a flush whenever the memtable held any
        // range tombstone ("External file requires flush").
        MemoryFileSystem fs = NewFs();
        fs.CreateDirectoryIfMissing("/ext");
        using RocksDatabase db = OpenNoAutoCompaction(fs);
        db.Put(B("a"), B("1"));
        db.DeleteRange(B("m"), B("n"));
        var noFlush = new IngestExternalFileOptions { AllowBlockingFlush = false };
        db.IngestExternalFiles([WriteSst(fs, "/ext/1.sst", "c", "d")], noFlush);
        LiveFileMetadata f = db.GetLiveFilesMetadata().Single();
        Assert.AreEqual(6, f.Level, "nothing overlaps: bottommost level");
        Assert.AreEqual(0UL, f.LargestSeqno, "no global seqno needed");
        Assert.AreEqual("ingested-c", GetString(db, "c"));

        // IsRangeOverlapped: the tombstone's end is exclusive ([n, p] does not touch [m, n))...
        db.IngestExternalFiles([WriteSst(fs, "/ext/2.sst", "n", "p")], noFlush);
        Assert.AreEqual(2, db.GetLiveFilesMetadata().Count);
        // ...its start is inclusive ([k, m] does), and a file inside it overlaps: flush required.
        foreach (string[] keys in new[] { new[] { "k", "m" }, new[] { "m1", "m2" }, new[] { "l", "o1" } })
        {
            string sst = WriteSst(fs, "/ext/x-" + keys[0] + ".sst", keys);
            RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => db.IngestExternalFiles([sst], noFlush));
            Assert.AreEqual(StatusCode.InvalidArgument, e.Code, keys[0]);
            StringAssert.Contains(e.Message, "External file requires flush");
        }
        Assert.AreEqual(2, db.GetLiveFilesMetadata().Count, "failed ingestions add nothing");
    }

    [TestMethod]
    public void IngestIgnoresLevelFileRangeTombstoneOutsideTheFileRange()
    {
        // Version::OverlapWithLevelIterator (db/version_set.cc): overlap = a point key in [smallest, largest], or
        // range_del_agg.IsRangeOverlapped(smallest, largest, end_exclusive = false) over the level's tombstones. The L0
        // file below spans [a, z] with a tombstone [m, n): a file covering [c, d] overlaps nothing, so
        // AssignLevelAndSeqnoForIngestedFile leaves assigned_seqno = 0 and descends to the last level, also with
        // allow_global_seqno = false. The port treated any tombstone in a file whose key range overlaps as an overlap
        // (L0, new seqno; "Global seqno is required, but disabled").
        MemoryFileSystem fs = NewFs();
        fs.CreateDirectoryIfMissing("/ext");
        using RocksDatabase db = OpenNoAutoCompaction(fs);
        db.Put(B("a"), B("1"));
        db.Put(B("z"), B("2"));
        db.DeleteRange(B("m"), B("n"));
        db.Flush();
        ulong last = db.GetLatestSequenceNumber();
        db.IngestExternalFiles([WriteSst(fs, "/ext/1.sst", "c", "d")], new IngestExternalFileOptions { AllowGlobalSeqNo = false });
        LiveFileMetadata f = FileWithSmallest(db, "c");
        Assert.AreEqual(6, f.Level);
        Assert.AreEqual(0UL, f.LargestSeqno);
        Assert.AreEqual(last, db.GetLatestSequenceNumber(), "no sequence number consumed");

        // A file under the tombstone does overlap: it stays above the L0 file and gets the next sequence number, so its
        // keys are not deleted by the tombstone.
        db.IngestExternalFiles([WriteSst(fs, "/ext/2.sst", "m1", "m2")], new IngestExternalFileOptions());
        LiveFileMetadata g = FileWithSmallest(db, "m1");
        Assert.AreEqual(0, g.Level);
        Assert.AreEqual(last + 1, g.LargestSeqno);
        Assert.AreEqual("ingested-m1", GetString(db, "m1"));
        // The same file without global seqnos cannot be ingested.
        RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() =>
            db.IngestExternalFiles([WriteSst(fs, "/ext/3.sst", "m3", "m4")], new IngestExternalFileOptions { AllowGlobalSeqNo = false }));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
    }

    // ---- checkpoint -----------------------------------------------------------------------------------------

    [TestMethod]
    public void CheckpointCleansStaleStagingDirectory()
    {
        // CheckpointImpl::CreateCheckpointImpl (utilities/checkpoint/checkpoint_impl.cc) calls CleanStagingDirectory on
        // "<dir>.tmp" before creating it: the leftovers of an interrupted checkpoint are deleted and the checkpoint
        // succeeds. The port failed with IOError "While mkdir ... File exists".
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(Path, NewOptions(fs)))
        {
            db.Put(B("k"), B("v"));
            fs.CreateDirectoryIfMissing("/cp.tmp");
            using (IWritableFile stale = fs.CreateWritable("/cp.tmp/000099.sst", truncate: true)) stale.Append(B("garbage"));
            db.CreateCheckpoint("/cp");
            Assert.IsFalse(fs.DirectoryExists("/cp.tmp"));
            Assert.IsFalse(fs.FileExists("/cp/000099.sst"), "the stale file is not part of the checkpoint");
        }
        using RocksDatabase c = RocksDatabase.Open("/cp", NewOptions(fs));
        Assert.AreEqual("v", GetString(c, "k"));
    }

    [TestMethod]
    public void CheckpointAlwaysFlushesWithAllow2Pc()
    {
        // DBImpl::GetLiveFilesStorageInfoImpl (db/db_filesnapshot.cc): `if (!immutable_db_options_.allow_2pc)` guards the
        // whole wal_size_for_flush logic, so with allow_2pc the memtables are flushed whatever log_size_for_flush says.
        // Without it, log_size_for_flush = max never flushes.
        foreach (bool allow2Pc in new[] { false, true })
        {
            MemoryFileSystem fs = NewFs();
            using RocksDatabase db = RocksDatabase.Open(Path, new DbOptions { CreateIfMissing = true, FileSystem = fs, Allow2Pc = allow2Pc });
            db.Put(B("k"), B("v"));
            db.CreateCheckpoint("/cp", ulong.MaxValue);
            Assert.AreEqual(allow2Pc ? 1 : 0, db.GetLiveFilesMetadata().Count, "allow_2pc=" + allow2Pc);
            using RocksDatabase c = RocksDatabase.Open("/cp", new DbOptions { FileSystem = fs, Allow2Pc = allow2Pc });
            Assert.AreEqual("v", GetString(c, "k"));
        }
    }

    // ---- import ---------------------------------------------------------------------------------------------

    [TestMethod]
    public void ImportOrdersLevel0BySequenceNumber()
    {
        // ImportColumnFamilyJob::Run builds a dummy VersionStorageInfo with EpochNumberRequirement::kMightMissing, so
        // VersionBuilder::SaveTo orders L0 with NewestFirstBySeqNo (largest seqno first) and
        // RecoverEpochNumbers(force = true) hands out the new epoch numbers from the back of that list. The exported
        // epoch numbers play no role. The port ordered L0 by the exported epoch numbers.
        MemoryFileSystem fs = NewFs();
        fs.CreateDirectoryIfMissing("/ext");
        ExportImportFilesMetadata meta;
        using (RocksDatabase src = RocksDatabase.Open(Path, NewOptions(fs)))
        {
            ColumnFamily cf = src.CreateColumnFamily("data", new ColumnFamilyOptions { DisableAutoCompactions = true });
            cf.Put(B("a"), B("old")); // seq 1
            cf.Flush(); // L0, epoch 1
            cf.Put(B("m"), B("mem")); // seq 2, stays in the memtable (does not overlap the file below)
            // Overlaps the L0 file: ingested into L0 with seq 3 and the next epoch number.
            src.IngestExternalFiles(cf, [WriteSst(fs, "/ext/a.sst", "a")], new IngestExternalFileOptions());
            meta = cf.ExportColumnFamily("/export"); // flushes "m": seq 2, but the newest epoch number
        }
        Assert.AreEqual(3, meta.Files.Count);
        LiveFileMetadata ingested = meta.Files.Single(f => f.LargestSeqno == 3);
        LiveFileMetadata flushed = meta.Files.Single(f => f.LargestSeqno == 2);
        Assert.IsTrue(flushed.EpochNumber > ingested.EpochNumber, "precondition: epoch order differs from seqno order");

        fs.CreateDirectoryIfMissing("/dst");
        using RocksDatabase dst = RocksDatabase.Open("/dst", NewOptions(fs));
        ColumnFamily imported = dst.CreateColumnFamilyWithImport("imported", new ColumnFamilyOptions { DisableAutoCompactions = true },
            new ImportColumnFamilyOptions(), meta);
        var bySeq = new SortedDictionary<ulong, ulong>();
        foreach (LiveFileMetadata f in dst.GetLiveFilesMetadata())
        {
            if (f.ColumnFamilyName == "imported") bySeq[f.LargestSeqno] = f.EpochNumber;
        }
        // Oldest (smallest seqno) first: epoch numbers 1, 2, 3.
        CollectionAssert.AreEqual(new ulong[] { 1, 2, 3 }, bySeq.Values.ToArray());
        Assert.AreEqual("ingested-a", GetString(imported, "a"));
        Assert.AreEqual("mem", GetString(imported, "m"));
    }

    [TestMethod]
    public void ExportFailsOnStaleStagingDirectoryAndRemovesIt()
    {
        // CheckpointImpl::ExportColumnFamily (utilities/checkpoint/checkpoint_impl.cc): `s = CreateDir(tmp_export_dir)`
        // fails when "<export_dir>.tmp" exists (unlike CreateCheckpoint there is no CleanStagingDirectory first), and the
        // failure path deletes the children of the staging directory and the directory itself. The port reused the
        // stale directory, so its leftover files ended up in the exported directory.
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(Path, NewOptions(fs));
        ColumnFamily cf = db.CreateColumnFamily("data", new ColumnFamilyOptions());
        cf.Put(B("k"), B("v"));
        fs.CreateDirectoryIfMissing("/export.tmp");
        using (IWritableFile stale = fs.CreateWritable("/export.tmp/000099.sst", truncate: true)) stale.Append(B("garbage"));
        RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => cf.ExportColumnFamily("/export"));
        Assert.AreEqual(StatusCode.IOError, e.Code);
        Assert.IsFalse(fs.DirectoryExists("/export"));
        Assert.IsFalse(fs.DirectoryExists("/export.tmp"), "the failed export cleans the staging directory");
        ExportImportFilesMetadata meta = cf.ExportColumnFamily("/export");
        Assert.AreEqual(1, meta.Files.Count);
        Assert.IsFalse(fs.FileExists("/export/000099.sst"));
    }

    // ---- ingestion: move_files with the copy fallback -------------------------------------------------------

    /// <summary>A file system without hard links: LinkFile keeps the interface default (NotSupported).</summary>
    private sealed class NoLinkFileSystem(MemoryFileSystem inner) : IFileSystem
    {
        public IWritableFile CreateWritable(string path, bool truncate = true) => inner.CreateWritable(path, truncate);
        public IWritableFile CreateWritable(string path, bool truncate, WritableFileOptions options) => inner.CreateWritable(path, truncate, options);
        public IWritableFile ReopenWritable(string path, WritableFileOptions options) => inner.ReopenWritable(path, options);
        public IRandomAccessFile OpenRandomAccess(string path) => inner.OpenRandomAccess(path);
        public ISequentialFile OpenSequential(string path) => inner.OpenSequential(path);
        public bool FileExists(string path) => inner.FileExists(path);
        public long GetFileSize(string path) => inner.GetFileSize(path);
        public void DeleteFile(string path) => inner.DeleteFile(path);
        public void RenameFile(string source, string target) => inner.RenameFile(source, target);
        public void CreateDirectoryIfMissing(string path) => inner.CreateDirectoryIfMissing(path);
        public IEnumerable<string> GetChildren(string directory) => inner.GetChildren(directory);
        public IDisposable LockFile(string path) => inner.LockFile(path);
        public void SyncDirectory(string path) => inner.SyncDirectory(path);
        public bool DirectoryExists(string path) => inner.DirectoryExists(path);
        public void DeleteDirectory(string path) => inner.DeleteDirectory(path);
        public void WriteAt(string path, long offset, ReadOnlySpan<byte> data) => inner.WriteAt(path, offset, data);
        public long GetFreeSpace(string path) => inner.GetFreeSpace(path);
    }

    [TestMethod]
    public void MoveFilesDeletesTheOriginalAlsoAfterTheCopyFallback()
    {
        // ExternalSstFileIngestionJob::Cleanup (db/external_sst_file_ingestion_job.cc): `if (status.ok() &&
        // ingestion_options_.move_files)` deletes f.external_file_path of every file that was not generated for the
        // ingestion, whether Prepare hard-linked it or fell back to a copy (LinkFile NotSupported with
        // failed_move_fall_back_to_copy). The port kept the original of a copied file, so a "move" left the data twice.
        MemoryFileSystem mem = NewFs();
        var fs = new NoLinkFileSystem(mem);
        mem.CreateDirectoryIfMissing("/ext");
        using RocksDatabase db = RocksDatabase.Open(Path, new DbOptions { CreateIfMissing = true, FileSystem = fs },
            new ColumnFamilyOptions { DisableAutoCompactions = true });
        string moved = WriteSst(fs, "/ext/moved.sst", "a", "b");
        db.IngestExternalFiles([moved], new IngestExternalFileOptions { MoveFiles = true });
        Assert.AreEqual("ingested-a", GetString(db, "a"));
        Assert.IsFalse(mem.FileExists(moved), "move_files removes the original after a successful ingestion");

        // link_files (and the default copy) keep the original.
        string linked = WriteSst(fs, "/ext/linked.sst", "c", "d");
        db.IngestExternalFiles([linked], new IngestExternalFileOptions { LinkFiles = true });
        Assert.IsTrue(mem.FileExists(linked));
        // Without the fallback the unsupported link fails the ingestion and nothing is deleted or added.
        string strict = WriteSst(fs, "/ext/strict.sst", "e", "f");
        RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() =>
            db.IngestExternalFiles([strict], new IngestExternalFileOptions { MoveFiles = true, FailedMoveFallBackToCopy = false }));
        Assert.AreEqual(StatusCode.NotSupported, e.Code);
        Assert.IsTrue(mem.FileExists(strict));
        Assert.IsNull(GetString(db, "e"));
        Assert.AreEqual(2, db.GetLiveFilesMetadata().Count);
    }

    // ---- checkpoint: blob direct write ----------------------------------------------------------------------

    [TestMethod]
    public void CheckpointWithoutFlushIsRefusedWhileBlobDirectWriteFilesAreInFlight()
    {
        // DBImpl::GetLiveFilesStorageInfoImpl (db/db_filesnapshot.cc): `if (needs_blob_direct_write_flush &&
        // !flush_memtable) return Status::NotSupported("Blob direct write requires flushing active blob files before
        // capturing live files. Retry with flush enabled.")`. The blob files written by blob direct write are only
        // registered in the MANIFEST by the flush of their memtable; a checkpoint taken without that flush would carry
        // WAL records whose blob indexes point at files it does not contain. The port created such a checkpoint.
        MemoryFileSystem fs = NewFs();
        var dbo = new DbOptions { CreateIfMissing = true, FileSystem = fs, AllowConcurrentMemtableWrite = false };
        var cfo = new ColumnFamilyOptions { EnableBlobFiles = true, EnableBlobDirectWrite = true, MinBlobSize = 32, DisableAutoCompactions = true };
        string large = new('v', 200);
        using (RocksDatabase db = RocksDatabase.Open(Path, dbo, cfo))
        {
            db.Put(B("k"), B(large));
            RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => db.CreateCheckpoint("/cp-noflush", ulong.MaxValue));
            Assert.AreEqual(StatusCode.NotSupported, e.Code);
            StringAssert.Contains(e.Message, "Retry with flush enabled");
            Assert.IsFalse(fs.DirectoryExists("/cp-noflush"));
            Assert.IsFalse(fs.DirectoryExists("/cp-noflush.tmp"), "the staging directory is cleaned up");

            // With the flush (the default) the checkpoint holds the blob file and is complete.
            db.CreateCheckpoint("/cp");
            // Nothing in flight any more: a checkpoint without flush is fine again.
            db.CreateCheckpoint("/cp2", ulong.MaxValue);
        }
        foreach (string dir in new[] { "/cp", "/cp2" })
        {
            using RocksDatabase c = RocksDatabase.Open(dir, new DbOptions { FileSystem = fs, AllowConcurrentMemtableWrite = false }, cfo);
            Assert.AreEqual(large, GetString(c, "k"), dir);
        }
    }
}
