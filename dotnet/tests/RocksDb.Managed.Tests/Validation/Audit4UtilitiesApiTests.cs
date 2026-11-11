using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Utilities.Backup;
using RocksDb.Managed.Wal;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4, slice UtilitiesApi: BackupEngine (utilities/backup/backup_engine.cc, utilities/copy_engine/copy_engine.cc),
/// the WAL listing behind GetSortedWalFiles / GetUpdatesSince (db/wal_manager.cc) and SeqnoToTimeMapping
/// (db/seqno_to_time_mapping.cc). Expected values are derived from the C++ functions named in each test.
/// </summary>
[TestClass]
public sealed class Audit4UtilitiesApiTests
{
    private const string BackupDir = "/backup";

    private static void Fill(RocksDatabase db, int from, int to, int round)
    {
        for (int i = from; i < to; i++) db.Put(Key(i), Value(i, round));
    }

    private static byte[] ReadAll(IFileSystem fs, string path)
    {
        var data = new byte[fs.GetFileSize(path)];
        using ISequentialFile f = fs.OpenSequential(path);
        int read = 0;
        while (read < data.Length) read += f.Read(data.AsSpan(read));
        return data;
    }

    private static void WriteAll(IFileSystem fs, string path, byte[] data)
    {
        using IWritableFile w = fs.CreateWritable(path, truncate: true);
        w.Append(data);
    }

    private static void Rewrite(IFileSystem fs, string path, Func<byte[], byte[]> change) => WriteAll(fs, path, change(ReadAll(fs, path)));

    /// <summary>Two backups (rounds 1 and 2 of 200 keys) in <see cref="BackupDir"/>; the engine is closed.</summary>
    private static MemoryFileSystem TwoBackups()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        using BackupEngine be = BackupEngine.Open(new BackupEngineOptions(BackupDir), fs);
        Fill(db, 0, 200, 1);
        Assert.AreEqual(1u, be.CreateNewBackup(db, flushBeforeBackup: true));
        Fill(db, 0, 200, 2);
        Assert.AreEqual(2u, be.CreateNewBackup(db, flushBeforeBackup: true));
        return fs;
    }

    // ---- backup meta files -----------------------------------------------------------------------------

    [TestMethod]
    public void BackupMetaWithoutFinalNewlineLosesItsLastEntry()
    {
        // BackupMeta::LoadFromFile reads with LineFileReader::ReadLine, which returns false for text after the last
        // '\n'. A meta file cut inside its last file entry therefore has one entry less than its file count:
        // Corruption "Inconsistent number of files or missing/incomplete header", and the backup is skipped at open.
        // The port parsed the fragment as an entry, so a cut crc32 value was accepted as that file's checksum.
        MemoryFileSystem fs = TwoBackups();
        foreach (int cut in (ReadOnlySpan<int>)[1, 2]) // the final '\n' only; then also the last digit of the crc32
        {
            Rewrite(fs, BackupDir + "/meta/2", d => d[..^1]);
            using BackupEngine be = BackupEngine.Open(new BackupEngineOptions(BackupDir), fs);
            CollectionAssert.AreEqual(new uint[] { 2 }, be.GetCorruptedBackups(), "cut " + cut);
            List<BackupInfo> infos = be.GetBackupInfo();
            Assert.AreEqual(1, infos.Count);
            Assert.AreEqual(1u, infos[0].BackupId);
            RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => be.VerifyBackup(2, verifyWithChecksum: false));
            Assert.AreEqual(StatusCode.Corruption, ex.Code);
            StringAssert.Contains(ex.Message, "Inconsistent number of files");
        }
        // The latest valid backup is 1: restoring "the latest" gives round 1 instead of failing on backup 2.
        using (BackupEngineReadOnly ro = BackupEngineReadOnly.Open(new BackupEngineOptions(BackupDir), fs))
        {
            ro.RestoreDbFromLatestBackup("/restored", "/restored", new RestoreOptions());
        }
        using RocksDatabase restored = RocksDatabase.Open("/restored", new DbOptions { FileSystem = fs });
        CollectionAssert.AreEqual(Value(7, 1), restored.Get(Key(7)));
    }

    [TestMethod]
    public void GetBackupInfoReportsAnUnsupportedBackupAsCorruption()
    {
        // BackupEngineImpl::GetBackupInfo(id): "return Status::Corruption(corrupt_itr->second.first.ToString())", while
        // VerifyBackup and RestoreDBFromBackup return the stored status (NotSupported for an unknown schema version).
        MemoryFileSystem fs = TwoBackups();
        Rewrite(fs, BackupDir + "/meta/2", d => [.. "schema_version 9\n"u8, .. d]);
        using BackupEngine be = BackupEngine.Open(new BackupEngineOptions(BackupDir), fs);
        CollectionAssert.AreEqual(new uint[] { 2 }, be.GetCorruptedBackups());
        RocksDbException info = Assert.ThrowsExactly<RocksDbException>(() => be.GetBackupInfo(2, includeFileDetails: false));
        Assert.AreEqual(StatusCode.Corruption, info.Code);
        StringAssert.Contains(info.Message, "Unsupported/unrecognized schema version: 9");
        Assert.AreEqual(StatusCode.NotSupported, Assert.ThrowsExactly<RocksDbException>(() => be.VerifyBackup(2, verifyWithChecksum: false)).Code);
        Assert.AreEqual(StatusCode.NotSupported,
            Assert.ThrowsExactly<RocksDbException>(() => be.RestoreDbFromBackup(2, "/r", "/r", new RestoreOptions())).Code);
        // Id 0 is the latest valid backup.
        Assert.AreEqual(1u, be.GetBackupInfo(0, includeFileDetails: false).BackupId);
    }

    [TestMethod]
    public void BackupFailsOnATableFileThatCannotBeRead()
    {
        // BackupEngineImpl::AddBackupFileWorkItem: with the kUseDbSessionId naming, GetFileDbIdentities opens the
        // table (SstFileDumper); only NotFound (no session id in the properties) is tolerated, any other status is
        // returned and fails the backup. The port swallowed the error and stored the damaged file under the legacy
        // <number>_<crc32c>_<size>.sst name.
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        using BackupEngine be = BackupEngine.Open(new BackupEngineOptions(BackupDir), fs);
        Fill(db, 0, 200, 1);
        db.Flush();
        string? sst = null;
        foreach (string f in fs.GetChildren(DbPath))
        {
            if (f.EndsWith(".sst", StringComparison.Ordinal)) sst = DbPath + "/" + f;
        }
        Assert.IsNotNull(sst);
        Rewrite(fs, sst, d =>
        {
            for (int i = d.Length - 8; i < d.Length; i++) d[i] ^= 0x5a; // the footer's magic number
            return d;
        });
        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => be.CreateNewBackup(db, flushBeforeBackup: false));
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
        Assert.AreEqual(0, be.GetBackupInfo().Count);
        Assert.AreEqual(0, new List<string>(fs.GetChildren(BackupDir + "/shared_checksum")).Count);
        Assert.AreEqual(0, new List<string>(fs.GetChildren(BackupDir + "/meta")).Count);
    }

    [TestMethod]
    public void ProgressCallbackFiresPerIntervalOfBytesAcrossFiles()
    {
        // CopyEngine worker (utilities/copy_engine/copy_engine.cc): bytes_toward_next_callback is a local of the
        // worker thread, grows by every chunk read from a source file and "while (bytes >= interval) { bytes -=
        // interval; progress_callback(); }". With one worker a first backup reports floor(copied bytes / interval)
        // times. The port counted per file and fired at most once per chunk.
        const ulong Interval = 100;
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        using BackupEngine be = BackupEngine.Open(new BackupEngineOptions(BackupDir) { CallbackTriggerIntervalSize = Interval }, fs);
        Fill(db, 0, 300, 1);
        db.Flush();
        Fill(db, 300, 400, 1); // stays in the WAL
        int progress = 0;
        uint id = be.CreateNewBackup(db, new CreateBackupOptions { ProgressCallback = () => progress++ });
        ulong copied = 0;
        foreach (BackupFileInfo f in be.GetBackupInfo(id, includeFileDetails: true).FileDetails!)
        {
            if (f.FileType != FileType.CurrentFile) copied += f.Size; // CURRENT is created from its contents, not read
        }
        Assert.IsTrue(copied > 10 * Interval);
        Assert.AreEqual((int)(copied / Interval), progress);
    }

    // ---- WAL listing -----------------------------------------------------------------------------------

    private static byte[] Batch(ulong sequence, string key)
    {
        using var b = new WriteBatch();
        b.Put(B(key), B("v"));
        b.Sequence = sequence;
        return b.Data.ToArray();
    }

    private static void WriteArchivedWal(IFileSystem fs, ulong number, params byte[][] records)
    {
        fs.CreateDirectoryIfMissing(FileNames.ArchivalDirectory(DbPath));
        using var log = new LogWriter(fs.CreateWritable(FileNames.ArchivedLogFileName(DbPath, number)), number, recycleLogFiles: false);
        foreach (byte[] r in records) log.AddRecord(r);
    }

    [TestMethod]
    public void WalWhoseFirstRecordIsTooSmallFailsTheListing()
    {
        // WalManager::ReadFirstLine reads ONE record; shorter than WriteBatchInternal::kHeader (12) it reports
        // Corruption("log record too small") and returns that status ("TODO read record's till the first no corrupt
        // entry?"), which GetSortedWalsOfType / GetSortedWalFiles / GetUpdatesSince return. The port skipped small
        // records and used the next one.
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        db.Put(B("a"), B("1"));
        Assert.AreEqual(1, db.GetSortedWalFiles().Count);
        WriteArchivedWal(fs, 900, [1, 2, 3, 4, 5], Batch(50, "k"));
        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.GetSortedWalFiles());
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
        StringAssert.Contains(ex.Message, "log record too small");
        Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => db.GetUpdatesSince(1).Dispose()).Code);
        // Once the file is gone the listing works again (nothing was cached for it).
        db.DeleteArchivedWalFile(900);
        Assert.AreEqual(1, db.GetSortedWalFiles().Count);
    }

    [TestMethod]
    public void WalWithACorruptFirstRecordFailsTheListingWithParanoidChecks()
    {
        // WalManager::ReadFirstLine: the reporter keeps the first error and, with paranoid_checks (the default), the
        // function returns it. The port read with a null reporter and treated the WAL as empty (left out silently).
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        db.Put(B("a"), B("1"));
        WriteArchivedWal(fs, 900, Batch(50, "k"), Batch(51, "l"));
        string path = FileNames.ArchivedLogFileName(DbPath, 900);
        Assert.AreEqual(50UL, db.Impl.GetSortedWalFiles(includeArchived: true)[0].StartSequence);
        db.DeleteArchivedWalFile(900); // forget the cached first sequence
        WriteArchivedWal(fs, 900, Batch(50, "k"), Batch(51, "l"));
        Rewrite(fs, path, d =>
        {
            d[LogFormat.HeaderSize + 3] ^= 0xff; // payload of the first record: checksum mismatch
            return d;
        });
        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.GetSortedWalFiles());
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
        StringAssert.Contains(ex.Message, "checksum mismatch");
    }

    [TestMethod]
    public void EmptyCompressedWalIsListedWithSequenceOne()
    {
        // WalManager::ReadFirstLine: "if (status.ok() && reader.IsCompressedAndEmptyFile()) *sequence = 1": a WAL that
        // holds only the kSetCompressionType record must not be skipped as empty by GetSortedWalsOfType.
        MemoryFileSystem fs = NewFs();
        DbOptions options = NewOptions(fs);
        options.WalCompression = RocksDb.Managed.Compression.CompressionType.Zstd;
        using RocksDatabase db = RocksDatabase.Open(DbPath, options);
        IReadOnlyList<WalFileInfo> files = db.GetSortedWalFiles();
        Assert.AreEqual(1, files.Count);
        Assert.AreEqual(db.CurrentWalNumber, files[0].LogNumber);
        Assert.AreEqual(1UL, files[0].StartSequence);
        // The first write of this DB gets sequence 1 as well, and updates can be read from that WAL.
        db.Put(B("a"), B("1"));
        db.Put(B("b"), B("2"));
        files = db.GetSortedWalFiles();
        Assert.AreEqual(1, files.Count);
        Assert.AreEqual(1UL, files[0].StartSequence);
        using TransactionLogIterator it = db.GetUpdatesSince(2);
        Assert.IsTrue(it.Valid);
        Assert.AreEqual(2UL, it.Current.Sequence);
    }

    [TestMethod]
    public void ArchiveSizeLimitDeletesTheOldestWalEvenWithoutAReadableRecord()
    {
        // WalManager::PurgeObsoleteWALFiles: the WALs to delete come from GetSortedWalsOfType(archival_dir, ...,
        // need_seqno = false), i.e. every archived WAL sorted by number. The port listed them with need_seqnos = true,
        // which leaves out WALs without a readable first record, so the wrong (newer) files were deleted, or none.
        MemoryFileSystem fs = NewFs();
        DbOptions options = NewOptions(fs);
        options.WalSizeLimitMb = 1;
        using RocksDatabase db = RocksDatabase.Open(DbPath, options);
        string archive = FileNames.ArchivalDirectory(DbPath);
        fs.CreateDirectoryIfMissing(archive);
        // Two archived WALs of 600 KiB with no complete record (zero filled, like a preallocated file): 1 MiB / 600 KiB
        // = 1 file may stay, so the oldest one goes.
        WriteAll(fs, FileNames.ArchivedLogFileName(DbPath, 901), new byte[600 * 1024]);
        WriteAll(fs, FileNames.ArchivedLogFileName(DbPath, 902), new byte[600 * 1024]);
        db.Impl.PurgeObsoleteWalFiles(force: true);
        Assert.IsFalse(fs.FileExists(FileNames.ArchivedLogFileName(DbPath, 901)));
        Assert.IsTrue(fs.FileExists(FileNames.ArchivedLogFileName(DbPath, 902)));
    }

    // ---- SeqnoToTimeMapping boundaries (verified correct; no existing test) ------------------------------

    [TestMethod]
    public void SeqnoToTimeMappingCapacityZeroAndOne()
    {
        // SeqnoToTimeMapping::Append: "if (capacity_ == 0) return false"; EnforceCapacity: "Treat cap of 1 as 2".
        var none = new SeqnoToTimeMapping();
        none.SetCapacity(0);
        Assert.IsFalse(none.Append(10, 100));
        Assert.IsTrue(none.Empty);
        Assert.AreEqual(0UL, none.GetProximalTimeBeforeSeqno(ulong.MaxValue));
        Assert.AreEqual(0UL, none.GetProximalSeqnoBeforeTime(ulong.MaxValue));
        Assert.AreEqual(0, none.Encode().Length);

        var one = new SeqnoToTimeMapping();
        one.SetCapacity(1);
        Assert.IsTrue(one.Append(1, 10));
        Assert.IsTrue(one.Append(2, 20));
        Assert.IsTrue(one.Append(3, 30)); // 3 > 2 + 2 / 8: the only inner entry is removed
        Assert.AreEqual("1->10,3->30,", one.ToHumanString());
        Assert.IsFalse(one.Append(0, 40)); // seqno 0 is never recorded
        Assert.AreEqual(2, one.Size);
        // Boundaries of the proximal lookups: strictly-below seqno, at-or-before time.
        Assert.AreEqual(0UL, one.GetProximalTimeBeforeSeqno(1));
        Assert.AreEqual(10UL, one.GetProximalTimeBeforeSeqno(2));
        Assert.AreEqual(0UL, one.GetProximalSeqnoBeforeTime(9));
        Assert.AreEqual(1UL, one.GetProximalSeqnoBeforeTime(10));
        Assert.AreEqual(3UL, one.GetProximalSeqnoBeforeTime(ulong.MaxValue));
    }

    [TestMethod]
    public void SeqnoToTimeMappingCopyRangeAndDecodeRollback()
    {
        var src = new SeqnoToTimeMapping();
        src.Append(10, 100);
        src.Append(20, 200);
        src.Append(30, 300);
        // CopyFromSeqnoRange: a nonsensical range [25, 5] copies nothing but the entry before from_seqno.
        var dst = new SeqnoToTimeMapping();
        dst.CopyFromSeqnoRange(src, 25, 5);
        Assert.AreEqual("20->200,", dst.ToHumanString());
        // [20, 20]: the entry before 20 (to answer GetProximalTimeBeforeSeqno(20)) and 20 itself.
        var two = new SeqnoToTimeMapping();
        two.CopyFromSeqnoRange(src, 20, 20);
        Assert.AreEqual("10->100,20->200,", two.ToHumanString());
        // DecodeFrom rolls back on corrupt input (count 2, one complete pair, then a truncated one).
        Assert.AreEqual("Invalid sequence number", two.DecodeFrom([2, 5, 7]));
        Assert.AreEqual("10->100,20->200,", two.ToHumanString());
        Assert.AreEqual("Extra bytes at end of sequence number time mapping", two.DecodeFrom([1, 5, 7, 9]));
        Assert.AreEqual(2, two.Size);
    }
}
