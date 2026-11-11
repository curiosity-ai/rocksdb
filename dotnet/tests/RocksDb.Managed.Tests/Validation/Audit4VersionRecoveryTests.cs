using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Wal;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4, slice VersionRecovery: MANIFEST replay, DB open / WAL recovery, read-only open, obsolete files
/// (db/version_edit_handler.cc, db/db_impl/db_impl_open.cc, db/db_impl/db_impl_files.cc, db/db_impl/db_impl_readonly.cc).
/// </summary>
[TestClass]
public sealed class Audit4VersionRecoveryTests
{
    private const string DbPath = "/audit4vr";

    private static MemoryFileSystem NewDbFs()
    {
        var fs = new MemoryFileSystem();
        fs.CreateDirectoryIfMissing(DbPath);
        fs.SyncDirectory("/");
        return fs;
    }

    private static List<ulong> WalNumbers(MemoryFileSystem fs)
    {
        var r = new List<ulong>();
        foreach (string c in fs.GetChildren(DbPath))
        {
            if (FileNames.TryParseFileName(c, out ulong n, out FileType t) && t == FileType.WalFile) r.Add(n);
        }
        r.Sort();
        return r;
    }

    private static void WriteFile(MemoryFileSystem fs, string path, ReadOnlySpan<byte> data)
    {
        using IWritableFile w = fs.CreateWritable(path, truncate: true);
        w.Append(data);
        w.Sync();
    }

    private static byte[] PutBatch(ulong sequence, string key, string value)
    {
        using var b = new WriteBatch();
        b.Put(B(key), B(value));
        b.Sequence = sequence;
        return b.Data.ToArray();
    }

    /// <summary>
    /// A DB holding a=1, b=2 (sequences 1, 2) in its WAL, plus a newer WAL with three records with valid checksums:
    /// Put(c) at sequence 3, a batch with Put(e) and then an unknown tag (count 2) at sequence 4, Put(d) at 5.
    /// </summary>
    private static MemoryFileSystem DbWithMalformedBatchInWal()
    {
        MemoryFileSystem fs = NewDbFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, new DbOptions { CreateIfMissing = true, FileSystem = fs }))
        {
            db.Put(B("a"), B("1"));
            db.Put(B("b"), B("2"));
        }
        List<ulong> wals = WalNumbers(fs);
        ulong number = wals[^1] + 10;
        // Put(e) followed by a byte that is not a ValueType: WriteBatchInternal::Iterate applies the Put, then fails with
        // Corruption("unknown WriteBatch tag").
        byte[] good = PutBatch(4, "e", "4");
        byte[] bad = new byte[good.Length + 1];
        good.CopyTo(bad, 0);
        RocksDb.Managed.Util.Coding.EncodeFixed32(bad.AsSpan(8), 2);
        bad[^1] = 0xEE;
        using (var log = new LogWriter(fs.CreateWritable(FileNames.LogFileName(DbPath, number)), number, recycleLogFiles: false))
        {
            log.AddRecord(PutBatch(3, "c", "3"));
            log.AddRecord(bad);
            log.AddRecord(PutBatch(5, "d", "5"));
            log.WriteBuffer();
            log.Sync();
        }
        return fs;
    }

    [TestMethod]
    public void MalformedBatchInWalFailsTheOpenInEveryRecoveryMode()
    {
        // DBImpl::InitializeWriteBatchForLogRecord: WriteBatchInternal::UpdateProtectionInfo iterates the batch
        // (WriteBatch::Iterate -> Corruption "unknown WriteBatch tag") and ProcessLogRecord / ProcessLogFile return that
        // status directly: it never reaches MaybeIgnoreError or HandleNonOkStatusOrOldLogRecord, so the open fails in
        // every wal_recovery_mode, with or without paranoid_checks. Native does exactly that
        // (Audit4FollowUpRecoveryCompatTests.MalformedOrRefusedBatchInWalRecoversLikeNative). The first audit #4 pass
        // had the port treat it like a read error (point-in-time stop, skip, ignore without paranoid_checks).
        foreach (WalRecoveryMode mode in Enum.GetValues<WalRecoveryMode>())
        {
            foreach (bool paranoid in new[] { true, false })
            {
                MemoryFileSystem fs = DbWithMalformedBatchInWal();
                var ex = Assert.ThrowsExactly<RocksDbException>(
                    () => RocksDatabase.Open(DbPath, new DbOptions { FileSystem = fs, WalRecoveryMode = mode, ParanoidChecks = paranoid }).Dispose());
                Assert.AreEqual(StatusCode.Corruption, ex.Code, mode + " paranoid=" + paranoid);
            }
        }
    }

    [TestMethod]
    public void WalRecordWithSequenceAbove56BitsIsCorruption()
    {
        // DBImpl::ProcessLogRecord: "if (*last_seqno_observed > kMaxSequenceNumber) reporter->Corruption(... sequence N
        // is too large)". The port replayed the record: the key got a sequence that overflows the 56-bit field of the
        // internal key and the DB's last sequence jumped above kMaxSequenceNumber.
        MemoryFileSystem fs = NewDbFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, new DbOptions { CreateIfMissing = true, FileSystem = fs }))
        {
            db.Put(B("a"), B("1"));
        }
        ulong number = WalNumbers(fs)[^1] + 10;
        using (var log = new LogWriter(fs.CreateWritable(FileNames.LogFileName(DbPath, number)), number, recycleLogFiles: false))
        {
            log.AddRecord(PutBatch(1UL << 60, "z", "bad"));
            log.WriteBuffer();
            log.Sync();
        }
        // Point-in-time recovery stops at the record; the open succeeds without it.
        using (RocksDatabase db = RocksDatabase.Open(DbPath, new DbOptions { FileSystem = fs, WalRecoveryMode = WalRecoveryMode.PointInTimeRecovery }))
        {
            Assert.AreEqual("1", GetString(db, "a"));
            Assert.IsNull(GetString(db, "z"));
            Assert.AreEqual(1UL, db.GetLatestSequenceNumber());
        }
    }

    [TestMethod]
    public void CreatingNewDbWithExistingWalFileIsCorruption()
    {
        // DBImpl::Recover: "While creating a new Db, wal_dir contains existing log file: " -> Status::Corruption. The
        // port created the DB and replayed the stray WAL into it.
        MemoryFileSystem fs = NewDbFs();
        using (var log = new LogWriter(fs.CreateWritable(FileNames.LogFileName(DbPath, 5)), 5, recycleLogFiles: false))
        {
            log.AddRecord(PutBatch(1, "stray", "x"));
            log.WriteBuffer();
            log.Sync();
        }
        var ex = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbPath, new DbOptions { CreateIfMissing = true, FileSystem = fs }).Dispose());
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
        StringAssert.Contains(ex.Message, "While creating a new Db, wal_dir contains existing log file");
    }

    [TestMethod]
    public void OpenNeverReusesNumberOfFileFoundOnDisk()
    {
        // DBImpl::MaybeUpdateNextFileNumber (db/db_impl/db_impl_files.cc): any file of the DB paths whose number is >=
        // next_file_number (left by a crashed flush / compaction / ingestion that allocated the number after the last
        // MANIFEST write) advances next_file_number past the largest one, "otherwise the next NewFileNumber() would
        // collide with it". The port only did this for WAL files.
        MemoryFileSystem fs = NewDbFs();
        var options = new DbOptions { CreateIfMissing = true, FileSystem = fs };
        using (RocksDatabase db = RocksDatabase.Open(DbPath, options))
        {
            db.Put(B("a"), B("1"));
            db.Flush();
        }
        WriteFile(fs, DbPath + "/000100.sst", "not a table"u8);
        WriteFile(fs, FileNames.BlobFileName(DbPath, 250), "not a blob file"u8);
        using (RocksDatabase db = RocksDatabase.Open(DbPath, options))
        {
            db.Put(B("b"), B("2"));
            db.Flush();
            ulong newest = 0;
            foreach (LiveFileMetadata f in db.GetLiveFilesMetadata()) newest = Math.Max(newest, f.FileNumber);
            Assert.IsTrue(newest > 250, "new table file number " + newest);
            Assert.AreEqual("1", GetString(db, "a"));
            Assert.AreEqual("2", GetString(db, "b"));
            // The WAL created by this open is numbered past them too.
            Assert.IsTrue(WalNumbers(fs)[^1] > 250);
        }
        Assert.IsFalse(fs.FileExists(DbPath + "/000100.sst")); // obsolete: deleted by the open's full scan
    }

    [TestMethod]
    public void ReadOnlyErrorIfWalFileExistsOnlyRejectsNonEmptyWals()
    {
        // DBImpl::Recover(read_only, error_if_wal_file_exists): Status::Corruption("The db was opened in readonly mode
        // with error_if_wal_file_exists flag but a non-empty WAL file already exists") only for a WAL with bytes > 0.
        // The port threw InvalidArgument for any WAL file, including the empty WAL every clean close leaves behind.
        MemoryFileSystem fs = NewDbFs();
        var options = new DbOptions { CreateIfMissing = true, FileSystem = fs };
        IReadOnlyList<ColumnFamilyDescriptor> cfs = [new ColumnFamilyDescriptor("default", new ColumnFamilyOptions())];
        using (RocksDatabase db = RocksDatabase.Open(DbPath, options))
        {
            db.Put(B("a"), B("1"));
            db.Flush(); // the data leaves the WAL; the current WAL is empty
        }
        List<ulong> wals = WalNumbers(fs);
        Assert.IsTrue(wals.Count > 0);
        foreach (ulong n in wals) Assert.AreEqual(0L, fs.GetFileSize(FileNames.LogFileName(DbPath, n)));
        using (RocksDatabase ro = RocksDatabase.OpenReadOnly(DbPath, options, cfs, errorIfWalFileExists: true))
        {
            Assert.AreEqual("1", GetString(ro, "a"));
        }

        using (RocksDatabase db = RocksDatabase.Open(DbPath, options))
        {
            db.Put(B("b"), B("2")); // stays in the WAL
        }
        var ex = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.OpenReadOnly(DbPath, options, cfs, errorIfWalFileExists: true).Dispose());
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
        using (RocksDatabase ro = RocksDatabase.OpenReadOnly(DbPath, options, cfs, errorIfWalFileExists: false))
        {
            Assert.AreEqual("2", GetString(ro, "b"));
        }
    }

    /// <summary>
    /// Rewrites the current MANIFEST with every new-file entry as a legacy kNewFile2 record (level, number, size,
    /// boundaries, seqnos; no custom fields), the way RocksDB wrote them before epoch numbers existed.
    /// </summary>
    private static void RewriteManifestWithoutEpochNumbers(MemoryFileSystem fs)
    {
        string path = FileNames.GetCurrentManifestPath(fs, DbPath, out _);
        List<VersionEdit> edits;
        using (ManifestLogReader reader = ManifestLogReader.Open(fs, path)) edits = ManifestReader.ReadAllEdits(reader);
        fs.DeleteFile(path);
        using ManifestLogWriter writer = ManifestLogWriter.Create(fs, path);
        foreach (VersionEdit edit in edits)
        {
            var files = new List<NewFileEntry>(edit.NewFiles);
            edit.NewFiles.Clear();
            var record = new System.Buffers.ArrayBufferWriter<byte>();
            Assert.IsTrue(edit.TryEncodeTo(record, 0));
            foreach (NewFileEntry nf in files)
            {
                RocksDb.Managed.Util.Coding.PutVarint32(record, (uint)VersionEditTag.NewFile2);
                RocksDb.Managed.Util.Coding.PutVarint32(record, (uint)nf.Level);
                RocksDb.Managed.Util.Coding.PutVarint64(record, nf.Meta.FileNumber);
                RocksDb.Managed.Util.Coding.PutVarint64(record, nf.Meta.FileSize);
                RocksDb.Managed.Util.Coding.PutLengthPrefixedSlice(record, nf.Meta.Smallest);
                RocksDb.Managed.Util.Coding.PutLengthPrefixedSlice(record, nf.Meta.Largest);
                RocksDb.Managed.Util.Coding.PutVarint64(record, nf.Meta.SmallestSeqno);
                RocksDb.Managed.Util.Coding.PutVarint64(record, nf.Meta.LargestSeqno);
            }
            writer.AddRecordAsync(record.WrittenMemory, CancellationToken.None).AsTask().GetAwaiter().GetResult();
        }
        writer.SyncAsync(CancellationToken.None).AsTask().GetAwaiter().GetResult();
    }

    [TestMethod]
    public void ManifestWithoutEpochNumbersGetsThemInferredAtOpen()
    {
        // VersionSet::Recover -> ColumnFamilyData::RecoverEpochNumbers -> VersionStorageInfo::RecoverEpochNumbers
        // (db/version_set.cc): when a file has no epoch number (MANIFEST written before they existed), every non-empty
        // level from the bottom up gets one epoch number, then the L0 files from oldest to newest get one each. The port
        // left them unknown, and VersionEdit::EncodeTo refuses a file without epoch number, so the first MANIFEST write
        // of the open failed: such a DB could not be opened.
        MemoryFileSystem fs = NewDbFs();
        var options = new DbOptions { CreateIfMissing = true, FileSystem = fs };
        IReadOnlyList<ColumnFamilyDescriptor> cfs = [new ColumnFamilyDescriptor("default", new ColumnFamilyOptions { DisableAutoCompactions = true })];
        using (RocksDatabase db = RocksDatabase.Open(DbPath, options, cfs))
        {
            db.Put(B("a"), B("0"));
            db.Put(B("z"), B("0"));
            db.Flush();
            db.CompactRange(); // one file below L0
            db.Put(B("a"), B("1"));
            db.Flush();
            db.Put(B("a"), B("2"));
            db.Put(B("b"), B("2"));
            db.Flush();
        }
        RewriteManifestWithoutEpochNumbers(fs);
        for (int round = 0; round < 2; round++) // the second open reads the MANIFEST written by the first
        {
            using RocksDatabase db = RocksDatabase.Open(DbPath, options, cfs);
            Assert.AreEqual("2", GetString(db, "a"));
            Assert.AreEqual("2", GetString(db, "b"));
            Assert.AreEqual("0", GetString(db, "z"));
            var files = db.GetLiveFilesMetadata();
            Assert.AreEqual(3, files.Count);
            files.Sort(static (x, y) => x.LargestSeqno.CompareTo(y.LargestSeqno)); // oldest first
            Assert.IsTrue(files[0].Level > 0);
            Assert.AreEqual(0, files[1].Level);
            Assert.AreEqual(0, files[2].Level);
            Assert.AreEqual(1UL, files[0].EpochNumber);
            Assert.AreEqual(2UL, files[1].EpochNumber);
            Assert.AreEqual(3UL, files[2].EpochNumber);
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, options, cfs))
        {
            db.Put(B("a"), B("3"));
            db.Flush();
            ulong max = 0;
            foreach (LiveFileMetadata f in db.GetLiveFilesMetadata()) max = Math.Max(max, f.EpochNumber);
            Assert.AreEqual(4UL, max); // next_epoch_number continues after the inferred ones
            Assert.AreEqual("3", GetString(db, "a"));
        }
    }

    [TestMethod]
    public void RecoveryReservesEpochNumberForFilesIngestedBehind()
    {
        // VersionStorageInfo::RecoverEpochNumbers(restart_epoch = true): with allow_ingest_behind the column family's
        // first epoch number (kReservedEpochNumberForFileIngestedBehind = 1) is reserved at every open, so the first
        // flushed file of a DB gets epoch number 2. The port handed out 1.
        foreach (bool ingestBehind in new[] { false, true })
        {
            MemoryFileSystem fs = NewDbFs();
            var options = new DbOptions { CreateIfMissing = true, FileSystem = fs, AllowIngestBehind = ingestBehind };
            using RocksDatabase db = RocksDatabase.Open(DbPath, options);
            db.Put(B("a"), B("1"));
            db.Flush();
            List<LiveFileMetadata> files = db.GetLiveFilesMetadata();
            Assert.AreEqual(1, files.Count);
            Assert.AreEqual(ingestBehind ? 2UL : 1UL, files[0].EpochNumber);
        }
    }

    [TestMethod]
    public void ReadOnlyOpenOfMissingDbIsIoErrorPathNotFound()
    {
        // DB::OpenForReadOnly -> OpenForReadOnlyCheckExistence -> GetCurrentManifestPath (db/manifest_ops.cc): the status
        // of reading CURRENT is returned as is, an IOError with subcode kPathNotFound (Status::IsPathNotFound), not
        // InvalidArgument (which only the read-write open reports, for create_if_missing = false).
        MemoryFileSystem fs = NewDbFs();
        IReadOnlyList<ColumnFamilyDescriptor> cfs = [new ColumnFamilyDescriptor("default", new ColumnFamilyOptions())];
        var ex = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.OpenReadOnly(DbPath, new DbOptions { FileSystem = fs }, cfs, errorIfWalFileExists: false).Dispose());
        Assert.AreEqual(StatusCode.IOError, ex.Code);
        Assert.AreEqual(StatusSubCode.PathNotFound, ex.SubCode);
        var rw = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbPath, new DbOptions { FileSystem = fs }).Dispose());
        Assert.AreEqual(StatusCode.InvalidArgument, rw.Code);
    }

    [TestMethod]
    public void OpenWithoutWriteIdentityFileDeletesObsoleteIdentityFile()
    {
        // DBImpl::Open: "if (s.ok() && !impl->immutable_db_options_.write_identity_file) env_->DeleteFile(IdentityFileName)"
        // after LogAndApplyForRecovery. The DB id stays the one recorded in the MANIFEST. The port kept the stale file.
        MemoryFileSystem fs = NewDbFs();
        string id;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, new DbOptions { CreateIfMissing = true, FileSystem = fs }))
        {
            id = db.DbId;
        }
        Assert.IsTrue(fs.FileExists(FileNames.IdentityFileName(DbPath)));
        using (RocksDatabase db = RocksDatabase.Open(DbPath, new DbOptions { FileSystem = fs, WriteIdentityFile = false }))
        {
            Assert.IsFalse(fs.FileExists(FileNames.IdentityFileName(DbPath)));
            Assert.AreEqual(id, db.DbId);
        }
    }
}
