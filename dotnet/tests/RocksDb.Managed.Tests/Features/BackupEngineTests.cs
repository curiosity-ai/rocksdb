using RocksDb.Managed.Env;
using RocksDb.Managed.Tests.Db;
using RocksDb.Managed.Utilities.Backup;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Features;

/// <summary>BackupEngine (ports of the main scenarios of utilities/backup/backup_engine_test.cc), on MemoryFileSystem.</summary>
[TestClass]
public sealed class BackupEngineTests
{
    private const string BackupDir = "/backup";

    private static BackupEngineOptions Opts() => new(BackupDir);

    private static void Fill(RocksDatabase db, int from, int to, int round)
    {
        for (int i = from; i < to; i++) db.Put(Key(i), Value(i, round));
    }

    private static void Check(IFileSystem fs, string path, int count, Func<int, int?> round)
    {
        using RocksDatabase db = RocksDatabase.Open(path, new DbOptions { FileSystem = fs });
        for (int i = 0; i < count; i++)
        {
            int? r = round(i);
            byte[]? v = db.Get(Key(i));
            if (r is null) Assert.IsNull(v, "key " + i);
            else CollectionAssert.AreEqual(Value(i, r.Value), v, "key " + i);
        }
    }

    private static List<string> Children(IFileSystem fs, string dir) => fs.DirectoryExists(dir) ? new List<string>(fs.GetChildren(dir)) : [];

    private static void Rewrite(IFileSystem fs, string path, Func<byte[], byte[]> change)
    {
        var data = new byte[fs.GetFileSize(path)];
        using (ISequentialFile f = fs.OpenSequential(path))
        {
            int read = 0;
            while (read < data.Length) read += f.Read(data.AsSpan(read));
        }
        byte[] changed = change(data);
        using IWritableFile w = fs.CreateWritable(path, truncate: true);
        w.Append(changed);
    }

    [TestMethod]
    public void BackupRestoreAndIncrementalSharing()
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        using (BackupEngine be = BackupEngine.Open(Opts(), fs))
        {
            Fill(db, 0, 200, 1);
            db.Flush();
            Fill(db, 100, 300, 2); // WAL only
            Assert.AreEqual(1u, be.CreateNewBackupWithMetadata(db, "first", new CreateBackupOptions()));
            int sharedAfter1 = Children(fs, BackupDir + "/shared_checksum").Count;
            Assert.AreEqual(1, sharedAfter1);
            Fill(db, 0, 50, 3);
            Assert.AreEqual(2u, be.CreateNewBackup(db, flushBeforeBackup: true));
            // Incremental: the first SST is shared, only new files were copied.
            List<string> shared = Children(fs, BackupDir + "/shared_checksum");
            Assert.AreEqual(sharedAfter1 + 1, shared.Count);
            foreach (string s in shared) StringAssert.Matches(s, new System.Text.RegularExpressions.Regex(@"^\d{6}_s[0-9A-Z]{20}_\d+\.sst$"));

            List<BackupInfo> infos = be.GetBackupInfo(includeFileDetails: true);
            Assert.AreEqual(2, infos.Count);
            Assert.AreEqual("first", infos[0].AppMetadata);
            Assert.AreEqual("", infos[1].AppMetadata);
            Assert.AreEqual(BackupDir + "/private/1", infos[0].NameForOpen);
            Assert.AreEqual((uint)infos[0].FileDetails!.Count, infos[0].NumberFiles);
            Assert.IsTrue(infos[0].Timestamp > 0);
            be.VerifyBackup(1, verifyWithChecksum: true);
            be.VerifyBackup(2, verifyWithChecksum: true);

            be.RestoreDbFromBackup(1, "/r1", "/r1", new RestoreOptions());
            be.RestoreDbFromLatestBackup("/r2", "/r2", new RestoreOptions());
        }
        Check(fs, "/r1", 300, i => i < 100 ? 1 : 2);
        Check(fs, "/r2", 300, i => i < 50 ? 3 : i < 100 ? 1 : 2);

        // Reopen: everything is loaded from the meta files.
        using (BackupEngine be = BackupEngine.Open(Opts(), fs))
        {
            Assert.AreEqual(2, be.GetBackupInfo().Count);
            be.DeleteBackup(1);
            Assert.AreEqual(1, be.GetBackupInfo().Count);
            be.VerifyBackup(2, verifyWithChecksum: true);
            Assert.IsFalse(fs.DirectoryExists(BackupDir + "/private/1"));
            RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => be.DeleteBackup(1));
            Assert.AreEqual(StatusCode.NotFound, ex.Code);
        }
    }

    [TestMethod]
    public void PurgeOldBackupsDeletesUnreferencedFiles()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        using BackupEngine be = BackupEngine.Open(Opts(), fs);
        for (int round = 0; round < 4; round++)
        {
            Fill(db, round * 10, round * 10 + 100, round);
            db.CompactRange(); // every backup has a different single SST
            be.CreateNewBackup(db, flushBeforeBackup: true);
        }
        Assert.AreEqual(4, Children(fs, BackupDir + "/shared_checksum").Count);
        be.PurgeOldBackups(2);
        List<BackupInfo> infos = be.GetBackupInfo();
        CollectionAssert.AreEqual(new uint[] { 3, 4 }, infos.ConvertAll(static i => i.BackupId));
        Assert.AreEqual(2, Children(fs, BackupDir + "/shared_checksum").Count);
        CollectionAssert.AreEquivalent(new[] { "3", "4" }, Children(fs, BackupDir + "/private"));
        be.PurgeOldBackups(0);
        Assert.AreEqual(0, be.GetBackupInfo().Count);
        Assert.AreEqual(0, Children(fs, BackupDir + "/shared_checksum").Count);
        Assert.AreEqual(0, Children(fs, BackupDir + "/meta").Count);
    }

    [TestMethod]
    public void VerifyDetectsMissingTruncatedAndCorruptFiles()
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        using (BackupEngine be = BackupEngine.Open(Opts(), fs))
        {
            Fill(db, 0, 100, 1);
            be.CreateNewBackup(db, flushBeforeBackup: true);
        }
        string sst = BackupDir + "/shared_checksum/" + Children(fs, BackupDir + "/shared_checksum")[0];
        using (BackupEngine be = BackupEngine.Open(Opts(), fs))
        {
            // Same size, different contents: only the checksum verification notices.
            Rewrite(fs, sst, static d =>
            {
                d[10] ^= 0xff;
                return d;
            });
            be.VerifyBackup(1, verifyWithChecksum: false);
            RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => be.VerifyBackup(1, verifyWithChecksum: true));
            Assert.AreEqual(StatusCode.Corruption, ex.Code);
            // A restore verifies the checksum of what it copies.
            ex = Assert.ThrowsExactly<RocksDbException>(() => be.RestoreDbFromLatestBackup("/r", "/r", new RestoreOptions()));
            Assert.AreEqual(StatusCode.Corruption, ex.Code);
            Assert.IsFalse(fs.FileExists("/r/CURRENT"));
            Rewrite(fs, sst, static d => d[..^1]);
            ex = Assert.ThrowsExactly<RocksDbException>(() => be.VerifyBackup(1, verifyWithChecksum: false));
            Assert.AreEqual(StatusCode.Corruption, ex.Code);
            fs.DeleteFile(sst);
            ex = Assert.ThrowsExactly<RocksDbException>(() => be.VerifyBackup(1, verifyWithChecksum: false));
            Assert.AreEqual(StatusCode.NotFound, ex.Code);
            ex = Assert.ThrowsExactly<RocksDbException>(() => be.VerifyBackup(7, verifyWithChecksum: false));
            Assert.AreEqual(StatusCode.NotFound, ex.Code);
        }
        // Reopened, the backup whose file is missing is corrupt; it can be deleted.
        using (BackupEngine be = BackupEngine.Open(Opts(), fs))
        {
            CollectionAssert.AreEqual(new uint[] { 1 }, be.GetCorruptedBackups());
            Assert.AreEqual(0, be.GetBackupInfo().Count);
            RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => be.RestoreDbFromBackup(1, "/r", "/r", new RestoreOptions()));
            Assert.AreEqual(StatusCode.Corruption, ex.Code);
            be.DeleteBackup(1);
            Assert.AreEqual(0, be.GetCorruptedBackups().Count);
        }
    }

    [TestMethod]
    public void CorruptMetaFileIsReportedAndOthersStayUsable()
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        using (BackupEngine be = BackupEngine.Open(Opts(), fs))
        {
            Fill(db, 0, 100, 1);
            be.CreateNewBackup(db, flushBeforeBackup: true);
            Fill(db, 0, 100, 2);
            be.CreateNewBackup(db, flushBeforeBackup: true);
        }
        Rewrite(fs, BackupDir + "/meta/1", static d => System.Text.Encoding.ASCII.GetBytes("123\n456\n2\nprivate/1/CURRENT crc32 xyz\n"));
        using (BackupEngineReadOnly ro = BackupEngineReadOnly.Open(Opts(), fs))
        {
            CollectionAssert.AreEqual(new uint[] { 1 }, ro.GetCorruptedBackups());
            Assert.AreEqual(1, ro.GetBackupInfo().Count);
            ro.RestoreDbFromLatestBackup("/r", "/r", new RestoreOptions());
        }
        Check(fs, "/r", 100, static _ => 2);
        // A future, non-ignorable field makes the backup unreadable; ignorable fields are skipped (schema 2).
        Rewrite(fs, BackupDir + "/meta/2", static d => System.Text.Encoding.ASCII.GetBytes(
            "schema_version 2.7\n" + System.Text.Encoding.ASCII.GetString(d).Replace(" crc32 ", " future_field x crc32 ", StringComparison.Ordinal)));
        using (BackupEngineReadOnly ro = BackupEngineReadOnly.Open(Opts(), fs))
        {
            ro.VerifyBackup(2, verifyWithChecksum: true);
        }
        Rewrite(fs, BackupDir + "/meta/2", static d => System.Text.Encoding.ASCII.GetBytes(
            System.Text.Encoding.ASCII.GetString(d).Replace("future_field", "ni::future", StringComparison.Ordinal)));
        using (BackupEngineReadOnly ro = BackupEngineReadOnly.Open(Opts(), fs))
        {
            CollectionAssert.AreEqual(new uint[] { 1, 2 }, ro.GetCorruptedBackups());
        }
    }

    [TestMethod]
    public void SchemaVersion2AndUnsharedTablesAndLegacyNaming()
    {
        foreach ((bool shareTables, ShareFilesNaming naming, int schema) in new[]
                 {
                     (false, ShareFilesNaming.UseDbSessionId, 1),
                     (true, ShareFilesNaming.LegacyCrc32cAndFileSize, 2),
                     (true, ShareFilesNaming.UseDbSessionId, 2),
                 })
        {
            MemoryFileSystem fs = NewFs();
            var o = new BackupEngineOptions(BackupDir) { ShareTableFiles = shareTables, ShareFilesWithChecksumNaming = naming, SchemaVersion = schema };
            using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
            using (BackupEngine be = BackupEngine.Open(o, fs))
            {
                Fill(db, 0, 100, 1);
                be.CreateNewBackup(db, flushBeforeBackup: true);
                be.RestoreDbFromLatestBackup("/r", "/r", new RestoreOptions());
            }
            Check(fs, "/r", 100, static _ => 1);
            string meta = ReadText(fs, BackupDir + "/meta/1");
            Assert.AreEqual(schema == 2, meta.StartsWith("schema_version 2.1\n", StringComparison.Ordinal), meta);
            if (!shareTables) StringAssert.Contains(meta, "private/1/000");
            else if (naming == ShareFilesNaming.LegacyCrc32cAndFileSize)
                StringAssert.Matches(meta, new System.Text.RegularExpressions.Regex(@"shared_checksum/\d{6}_\d+_\d+\.sst crc32 \d+"));
            else
                StringAssert.Matches(meta, new System.Text.RegularExpressions.Regex(@"shared_checksum/\d{6}_s[0-9A-Z]{20}\.sst crc32 \d+"));
            using BackupEngineReadOnly ro = BackupEngineReadOnly.Open(o, fs);
            ro.VerifyBackup(1, verifyWithChecksum: true);
        }
    }

    private static string ReadText(IFileSystem fs, string path)
    {
        var data = new byte[fs.GetFileSize(path)];
        using ISequentialFile f = fs.OpenSequential(path);
        int read = 0;
        while (read < data.Length) read += f.Read(data.AsSpan(read));
        return System.Text.Encoding.UTF8.GetString(data);
    }

    [TestMethod]
    public void KeepLogFilesAndIncrementalRestoreModes()
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        using (BackupEngine be = BackupEngine.Open(new BackupEngineOptions(BackupDir) { BackupLogFiles = false }, fs))
        {
            Fill(db, 0, 100, 1);
            be.CreateNewBackup(db, flushBeforeBackup: true);
            Fill(db, 100, 150, 2); // WAL only, not backed up
            // An in-memory-style DB keeps its own WALs: restore over the DB dir with keep_log_files.
            db.FlushWal(sync: true);
        }
        using (BackupEngineReadOnly be = BackupEngineReadOnly.Open(Opts(), fs))
        {
            be.RestoreDbFromLatestBackup(DbPath, DbPath, new RestoreOptions { KeepLogFiles = true });
            // Incremental restores into a populated directory keep matching table files.
            be.RestoreDbFromLatestBackup("/r", "/r", new RestoreOptions());
            be.RestoreDbFromLatestBackup("/r", "/r", new RestoreOptions { Mode = RestoreMode.VerifyChecksum });
            be.RestoreDbFromLatestBackup("/r", "/r", new RestoreOptions { Mode = RestoreMode.KeepLatestDbSessionIdFiles });
        }
        Check(fs, DbPath, 150, static i => i < 100 ? 1 : 2);
        Check(fs, "/r", 150, static i => i < 100 ? 1 : null);
    }

    [TestMethod]
    public void ReadOnlyEngineAndDestroyOldData()
    {
        MemoryFileSystem fs = NewFs();
        Assert.AreEqual(StatusCode.NotFound, Assert.ThrowsExactly<RocksDbException>(() => BackupEngineReadOnly.Open(Opts(), fs)).Code);
        Assert.AreEqual(StatusCode.InvalidArgument,
            Assert.ThrowsExactly<RocksDbException>(() => BackupEngineReadOnly.Open(new BackupEngineOptions(BackupDir) { DestroyOldData = true }, fs)).Code);
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        using (BackupEngine be = BackupEngine.Open(Opts(), fs))
        {
            Fill(db, 0, 10, 1);
            be.CreateNewBackup(db, flushBeforeBackup: false);
            be.CreateNewBackup(db, flushBeforeBackup: true);
        }
        using (BackupEngineReadOnly ro = BackupEngineReadOnly.Open(new BackupEngineOptions(BackupDir) { MaxValidBackupsToOpen = 1 }, fs))
        {
            CollectionAssert.AreEqual(new uint[] { 2 }, ro.GetBackupInfo().ConvertAll(static i => i.BackupId));
        }
        using (BackupEngine be = BackupEngine.Open(new BackupEngineOptions(BackupDir) { DestroyOldData = true }, fs))
        {
            Assert.AreEqual(0, be.GetBackupInfo().Count);
            Assert.AreEqual(0, Children(fs, BackupDir + "/shared_checksum").Count);
            Assert.AreEqual(0, Children(fs, BackupDir + "/private").Count);
        }
    }

    [TestMethod]
    public void FailedBackupIsCleanedUpAndProgressAndRateLimiterHooks()
    {
        var fs = new FaultInjectionFileSystem(NewFs());
        var limiter = new CountingLimiter();
        int progress = 0;
        var o = new BackupEngineOptions(BackupDir) { BackupRateLimiter = limiter, CallbackTriggerIntervalSize = 1024 };
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        using BackupEngine be = BackupEngine.Open(o, fs);
        Fill(db, 0, 500, 1);
        db.Flush();
        fs.FailMatching("/backup/shared_checksum/", FaultOps.Append, 1, InjectedFaultKind.IOError);
        Assert.ThrowsExactly<RocksDbException>(() => be.CreateNewBackup(db, flushBeforeBackup: false));
        Assert.AreEqual(0, be.GetBackupInfo().Count);
        Assert.AreEqual(0, Children(fs, BackupDir + "/shared_checksum").Count);
        Assert.AreEqual(0, Children(fs, BackupDir + "/private").Count);
        Assert.AreEqual(0, Children(fs, BackupDir + "/meta").Count);
        // The id is reused by the next (successful) backup.
        Assert.AreEqual(1u, be.CreateNewBackup(db, new CreateBackupOptions { ProgressCallback = () => progress++ }));
        Assert.IsTrue(progress > 0);
        Assert.IsTrue(limiter.Bytes > 0);
        // A throwing progress callback aborts the backup.
        Fill(db, 0, 500, 2);
        db.Flush();
        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() =>
            be.CreateNewBackup(db, new CreateBackupOptions { ProgressCallback = static () => throw new InvalidOperationException("stop") }));
        Assert.AreEqual(StatusCode.Aborted, ex.Code);
        Assert.AreEqual(1, be.GetBackupInfo().Count);
        be.VerifyBackup(1, verifyWithChecksum: true);
        // The DB's obsolete-file deletions were re-enabled.
        Assert.AreEqual(1, db.DisableFileDeletions());
        db.EnableFileDeletions();
        be.StopBackup();
        Assert.AreEqual(StatusCode.Incomplete, Assert.ThrowsExactly<RocksDbException>(() => be.CreateNewBackup(db, flushBeforeBackup: false)).Code);
    }

    private sealed class CountingLimiter : BackupRateLimiter
    {
        public long Bytes;

        public override ValueTask RequestAsync(long bytes, CancellationToken cancellationToken)
        {
            Interlocked.Add(ref Bytes, bytes);
            return ValueTask.CompletedTask;
        }

        public override void Request(long bytes) => Interlocked.Add(ref Bytes, bytes);
    }

    [TestMethod]
    public async Task AsyncApiAndColumnFamilies()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        o.CreateMissingColumnFamilies = true;
        var cfs = new[] { new ColumnFamilyDescriptor("default", new ColumnFamilyOptions()), new ColumnFamilyDescriptor("x", new ColumnFamilyOptions()) };
        await using (RocksDatabase db = await RocksDatabase.OpenAsync(DbPath, o, cfs))
        {
            ColumnFamily x = db.GetColumnFamily("x");
            Fill(db, 0, 50, 1);
            x.Put(B("cf-key"), B("cf-value"));
            using BackupEngine be = BackupEngine.Open(new BackupEngineOptions(BackupDir) { BackupRateLimit = 1 << 30 }, fs);
            Assert.AreEqual(1u, await be.CreateNewBackupAsync(db, flushBeforeBackup: false));
            await be.VerifyBackupAsync(1, verifyWithChecksum: true);
            await be.RestoreDbFromLatestBackupAsync("/r", "/r", new RestoreOptions());
        }
        await using RocksDatabase r = await RocksDatabase.OpenAsync("/r", new DbOptions { FileSystem = fs }, cfs);
        Assert.AreEqual("cf-value", GetString(r.GetColumnFamily("x"), "cf-key"));
        CollectionAssert.AreEqual(Value(7, 1), r.Get(Key(7)));
    }
}
