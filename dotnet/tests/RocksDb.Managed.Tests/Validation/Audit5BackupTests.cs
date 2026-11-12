using System.Text;
using RocksDb.Managed.Env;
using RocksDb.Managed.Tests.Db;
using RocksDb.Managed.Utilities.Backup;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #5: the restore, verify, delete, purge and garbage collection bodies of the backup engine, read against
/// utilities/backup/backup_engine.cc (RestoreDBFromBackup, InferDBFilesToRetainInRestore, DeleteChildren, VerifyBackup,
/// GarbageCollect, DeleteBackupNoGC, PurgeOldBackups, BackupMeta::LoadFromFile / AddFile / Delete, Initialize).
/// </summary>
[TestClass]
public sealed class Audit5BackupTests
{
    private const string BackupDir = "/backup";
    private const string Shared = BackupDir + "/shared_checksum";

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

    private static List<string> Children(IFileSystem fs, string dir)
    {
        var list = fs.DirectoryExists(dir) ? new List<string>(fs.GetChildren(dir)) : [];
        list.Sort(StringComparer.Ordinal);
        return list;
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
        w.Sync();
    }

    private static string ReadText(IFileSystem fs, string path) => Encoding.ASCII.GetString(ReadAll(fs, path));

    private static void WriteText(IFileSystem fs, string path, string text) => WriteAll(fs, path, Encoding.ASCII.GetBytes(text));

    /// <summary>The shared files a meta file lists.</summary>
    private static List<string> SharedFilesOf(IFileSystem fs, uint id)
    {
        var list = new List<string>();
        foreach (string line in ReadText(fs, BackupDir + "/meta/" + id).Split('\n'))
        {
            if (line.StartsWith("shared_checksum/", StringComparison.Ordinal)) list.Add(line.Split(' ')[0]["shared_checksum/".Length..]);
        }
        list.Sort(StringComparer.Ordinal);
        return list;
    }

    private static string? FirstWithExtension(IFileSystem fs, string dir, string extension)
    {
        foreach (string f in Children(fs, dir))
        {
            if (f.EndsWith(extension, StringComparison.Ordinal)) return f;
        }
        return null;
    }

    /// <summary>
    /// Backup 1 (keys 0..100 round 1, one table file) and backup 2 (plus keys 100..200 round 2, a second table file):
    /// backup 2 shares backup 1's table file and owns one of its own.
    /// </summary>
    private static MemoryFileSystem TwoBackups()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        using BackupEngine be = BackupEngine.Open(Opts(), fs);
        Fill(db, 0, 100, 1);
        Assert.AreEqual(1u, be.CreateNewBackup(db, flushBeforeBackup: true));
        Fill(db, 100, 200, 2);
        Assert.AreEqual(2u, be.CreateNewBackup(db, flushBeforeBackup: true));
        Assert.AreEqual(1, SharedFilesOf(fs, 1).Count);
        Assert.AreEqual(2, SharedFilesOf(fs, 2).Count);
        return fs;
    }

    // ---------------------------------------------------------------------------------------------------------
    // Garbage collection and backups that could not be loaded

    /// <summary>
    /// BackupEngineImpl::GarbageCollect deletes every shared file without a reference and every private directory whose
    /// id is not in backups_. A backup whose meta file could not be loaded (corrupt_backups_) holds no references and is
    /// not in backups_, so upstream's garbage collection (which runs by itself on the first DeleteBackup /
    /// PurgeOldBackups of a writable engine) deletes the data of a backup that only this version cannot read, e.g. one
    /// written with a newer schema. backup_engine_test.cc states the intent ("make sure that no corrupt backups have
    /// actually been deleted") without exercising a garbage collection. The port keeps what the meta file of such a
    /// backup names until the backup is deleted by id.
    /// </summary>
    [TestMethod]
    public void GarbageCollectionSparesBackupsThatCouldNotBeLoaded()
    {
        (string Name, Func<string, string> Damage, StatusCode Code)[] cases =
        [
            ("future schema version", static m => "schema_version 9\n" + m, StatusCode.NotSupported),
            ("non-ignorable field", static m => "schema_version 2\n" + m.Replace(" crc32 ", " ni::future x crc32 ", StringComparison.Ordinal), StatusCode.NotSupported),
            ("lost last entry", static m => m[..(m.LastIndexOf('\n', m.Length - 2) + 1)], StatusCode.Corruption),
        ];
        foreach ((string name, Func<string, string> damage, StatusCode code) in cases)
        {
            MemoryFileSystem fs = TwoBackups();
            string meta = ReadText(fs, BackupDir + "/meta/2");
            List<string> shared = Children(fs, Shared), privateFiles = Children(fs, BackupDir + "/private/2");
            WriteText(fs, BackupDir + "/meta/2", damage(meta));
            using (BackupEngine be = BackupEngine.Open(Opts(), fs))
            {
                CollectionAssert.AreEqual(new uint[] { 2 }, be.GetCorruptedBackups(), name);
                Assert.AreEqual(code, Assert.ThrowsExactly<RocksDbException>(() => be.VerifyBackup(2, verifyWithChecksum: false)).Code, name);
                be.GarbageCollect();
                CollectionAssert.AreEqual(shared, Children(fs, Shared), name + ": shared files after GarbageCollect");
                CollectionAssert.AreEqual(privateFiles, Children(fs, BackupDir + "/private/2"), name + ": private files after GarbageCollect");
                // Backup 1 goes; the table file it shares with backup 2 stays.
                be.DeleteBackup(1);
                be.PurgeOldBackups(0);
                CollectionAssert.AreEqual(shared, Children(fs, Shared), name + ": shared files after DeleteBackup(1)");
                CollectionAssert.AreEqual(privateFiles, Children(fs, BackupDir + "/private/2"), name + ": private files after DeleteBackup(1)");
            }
            // Once the meta file is readable again (a newer version, or a repaired file) the backup is complete.
            WriteText(fs, BackupDir + "/meta/2", meta);
            using (BackupEngine be = BackupEngine.Open(Opts(), fs))
            {
                be.VerifyBackup(2, verifyWithChecksum: true);
                be.RestoreDbFromBackup(2, "/r", "/r", new RestoreOptions());
            }
            Check(fs, "/r", 200, static i => i < 100 ? 1 : 2);
        }
    }

    /// <summary>Deleting such a backup by id releases everything it held (DeleteBackupNoGC + the garbage collection after it).</summary>
    [TestMethod]
    public void DeletingABackupThatCouldNotBeLoadedRemovesItsFiles()
    {
        MemoryFileSystem fs = TwoBackups();
        List<string> sharedOf1 = SharedFilesOf(fs, 1);
        WriteText(fs, BackupDir + "/meta/2", "schema_version 9\n" + ReadText(fs, BackupDir + "/meta/2"));
        using (BackupEngine be = BackupEngine.Open(Opts(), fs))
        {
            be.GarbageCollect();
            be.DeleteBackup(2);
            Assert.AreEqual(0, be.GetCorruptedBackups().Count);
            CollectionAssert.AreEqual(sharedOf1, Children(fs, Shared));
            CollectionAssert.AreEqual(new[] { "1" }, Children(fs, BackupDir + "/private"));
            CollectionAssert.AreEqual(new[] { "1" }, Children(fs, BackupDir + "/meta"));
            be.VerifyBackup(1, verifyWithChecksum: true);
            be.DeleteBackup(1);
            Assert.AreEqual(0, Children(fs, Shared).Count);
            Assert.AreEqual(0, Children(fs, BackupDir + "/private").Count);
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // Reference counting: delete, purge, failed and stopped backups

    [TestMethod]
    public void DeleteAndPurgeKeepTheSharedFilesOfTheSurvivingBackups()
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        using (BackupEngine be = BackupEngine.Open(Opts(), fs))
        {
            for (int b = 0; b < 4; b++)
            {
                Fill(db, b * 50, b * 50 + 50, b + 1);
                be.CreateNewBackup(db, flushBeforeBackup: true);
            }
            be.DeleteBackup(1);
            be.DeleteBackup(3);
            be.VerifyBackup(2, verifyWithChecksum: true);
            be.VerifyBackup(4, verifyWithChecksum: true);
            CollectionAssert.AreEqual(SharedFilesOf(fs, 4), Children(fs, Shared), "backup 4 lists every table file");
            Assert.AreEqual(StatusCode.NotFound, Assert.ThrowsExactly<RocksDbException>(() => be.DeleteBackup(3)).Code);
        }
        // The reference counts are rebuilt from the meta files at open.
        using (BackupEngine be = BackupEngine.Open(Opts(), fs))
        {
            be.GarbageCollect();
            be.VerifyBackup(2, verifyWithChecksum: true);
            be.VerifyBackup(4, verifyWithChecksum: true);
            be.DeleteBackup(4);
            CollectionAssert.AreEqual(SharedFilesOf(fs, 2), Children(fs, Shared));
            be.VerifyBackup(2, verifyWithChecksum: true);
            be.RestoreDbFromLatestBackup("/r", "/r", new RestoreOptions());
            be.PurgeOldBackups(0);
            Assert.AreEqual(0, Children(fs, Shared).Count);
            Assert.AreEqual(0, Children(fs, BackupDir + "/private").Count);
            Assert.AreEqual(0, Children(fs, BackupDir + "/meta").Count);
        }
        Check(fs, "/r", 200, static i => i < 100 ? i / 50 + 1 : null);
    }

    /// <summary>
    /// CreateNewBackupWithMetadata's failure path (DeleteBackup of the new id, then GarbageCollect): the files of the
    /// failed backup go, the shared files it reused from an earlier backup stay.
    /// </summary>
    [TestMethod]
    public void AFailedOrStoppedBackupKeepsTheFilesOfEarlierBackups()
    {
        foreach (string how in (string[])["fail the copy of the new table file", "fail the meta file", "fail the meta file rename", "stop"])
        {
            var fs = new FaultInjectionFileSystem(NewFs());
            using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
            using BackupEngine be = BackupEngine.Open(new BackupEngineOptions(BackupDir) { CallbackTriggerIntervalSize = 1 }, fs);
            Fill(db, 0, 100, 1);
            be.CreateNewBackup(db, flushBeforeBackup: true);
            List<string> shared = Children(fs, Shared);
            Fill(db, 100, 200, 2);
            db.Flush();
            var options = new CreateBackupOptions();
            StatusCode expected = StatusCode.IOError;
            switch (how)
            {
                case "fail the copy of the new table file": fs.FailMatching(Shared + "/", FaultOps.Append, 1, InjectedFaultKind.IOError); break;
                case "fail the meta file": fs.FailMatching(BackupDir + "/meta/", FaultOps.Append, 1, InjectedFaultKind.IOError); break;
                case "fail the meta file rename": fs.FailMatching(BackupDir + "/meta/", FaultOps.Rename, 1, InjectedFaultKind.IOError); break;
                default:
                    options.ProgressCallback = be.StopBackup;
                    expected = StatusCode.Incomplete;
                    break;
            }
            Assert.AreEqual(expected, Assert.ThrowsExactly<RocksDbException>(() => be.CreateNewBackup(db, options)).Code, how);
            fs.ClearFailMatching();
            CollectionAssert.AreEqual(shared, Children(fs, Shared), how);
            CollectionAssert.AreEqual(new[] { "1" }, Children(fs, BackupDir + "/private"), how);
            CollectionAssert.AreEqual(new[] { "1" }, Children(fs, BackupDir + "/meta"), how);
            CollectionAssert.AreEqual(new uint[] { 1 }, be.GetBackupInfo().ConvertAll(static i => i.BackupId), how);
            be.VerifyBackup(1, verifyWithChecksum: true);
            Assert.AreEqual(1, db.DisableFileDeletions(), how + ": file deletions were re-enabled");
            db.EnableFileDeletions();
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // Open

    /// <summary>
    /// Initialize: leftovers of interrupted operations (a private directory without a meta file, a temporary meta file,
    /// unrecognized meta file names) do not hide the valid backups; a read-only engine changes nothing; the writable
    /// engine removes the orphan directory with its next garbage collection and never reuses a private directory.
    /// </summary>
    [TestMethod]
    public void OpenToleratesLeftoversOfInterruptedOperations()
    {
        MemoryFileSystem fs = TwoBackups();
        fs.CreateDirectoryIfMissing(BackupDir + "/private/3");
        WriteText(fs, BackupDir + "/private/3/CURRENT", "MANIFEST-000005\n");
        WriteText(fs, BackupDir + "/private/3/000009.log", "partial");
        WriteText(fs, BackupDir + "/meta/.3.tmp", "123\n");
        WriteText(fs, BackupDir + "/meta/07", "123\n");
        WriteText(fs, BackupDir + "/meta/x", "123\n");
        WriteText(fs, Shared + "/.000099_sABCDEFGHIJKLMNOPQRST_10.sst.tmp", "half a copy");
        List<string> before = Snapshot(fs);
        using (BackupEngineReadOnly ro = BackupEngineReadOnly.Open(Opts(), fs))
        {
            CollectionAssert.AreEqual(new uint[] { 1, 2 }, ro.GetBackupInfo().ConvertAll(static i => i.BackupId));
            Assert.AreEqual(0, ro.GetCorruptedBackups().Count);
            ro.VerifyBackup(2, verifyWithChecksum: true);
            ro.RestoreDbFromLatestBackup("/r", "/r", new RestoreOptions());
        }
        CollectionAssert.AreEqual(before, Snapshot(fs), "a read-only engine does not modify the backup directory");
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        using (BackupEngine be = BackupEngine.Open(Opts(), fs))
        {
            Fill(db, 200, 300, 3);
            Assert.AreEqual(3u, be.CreateNewBackup(db, flushBeforeBackup: true));
            Assert.IsFalse(fs.FileExists(BackupDir + "/private/3/000009.log"), "the orphan directory was cleaned before reuse");
            Assert.IsNull(FirstWithExtension(fs, Shared, ".tmp"));
            be.VerifyBackup(1, verifyWithChecksum: true);
            be.VerifyBackup(2, verifyWithChecksum: true);
            be.VerifyBackup(3, verifyWithChecksum: true);
        }
        Check(fs, "/r", 300, static i => i < 100 ? 1 : i < 200 ? 2 : null);
    }

    private static List<string> Snapshot(MemoryFileSystem fs)
    {
        var list = new List<string>();
        Walk(fs, BackupDir, list);
        return list;
    }

    private static void Walk(MemoryFileSystem fs, string dir, List<string> list)
    {
        foreach (string c in Children(fs, dir))
        {
            string p = dir + "/" + c;
            if (fs.DirectoryExists(p)) Walk(fs, p, list);
            else list.Add(p + ":" + Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(ReadAll(fs, p))));
        }
    }

    private static MemoryFileSystem? s_metaValidationFs;

    /// <summary>BackupMeta::LoadFromFile validation, one damaged meta file per row.</summary>
    [TestMethod]
    public void MetaFileValidation()
    {
        (string Name, Func<string, string> Change, StatusCode? Code)[] cases =
        [
            ("unchanged", static m => m, null),
            ("schema 2 with sizes", static m => "schema_version 2.1\n" + WithSizes(m, 0), null),
            ("schema 2 with a wrong size", static m => "schema_version 2.1\n" + WithSizes(m, 1), StatusCode.Corruption),
            ("schema 2, ignorable fields and footer", static m => "schema_version 2\n" + m.Replace(" crc32 ", " future x crc32 ", StringComparison.Ordinal) + "// FOOTER\nfuture y\n", null),
            ("schema 2, non-ignorable footer field", static m => "schema_version 2\n" + m + "// FOOTER\nni::future y\n", StatusCode.NotSupported),
            ("schema 2, excluded value", static m => "schema_version 2\n" + m.Replace(" crc32 ", " ni::excluded maybe crc32 ", StringComparison.Ordinal), StatusCode.NotSupported),
            ("schema 3", static m => "schema_version 3\n" + m, StatusCode.NotSupported),
            ("schema 1 with a size field", static m => WithSizes(m, 0), StatusCode.Corruption),
            ("schema 1 with an unknown meta field", static m => InsertLine(m, 2, "future x"), StatusCode.Corruption),
            ("one file too many in the count", static m => ReplaceLine(m, 2, static c => (int.Parse(c) + 1).ToString()), StatusCode.Corruption),
            ("one file too few in the count", static m => ReplaceLine(m, 2, static c => (int.Parse(c) - 1).ToString()), StatusCode.Corruption),
            ("a file that does not exist", static m => m.Replace("/CURRENT ", "/CURRENT2 ", StringComparison.Ordinal), StatusCode.Corruption),
            ("a path outside the backup's directories", static m => m.Replace("private/2/CURRENT ", "private/2/../2/CURRENT ", StringComparison.Ordinal), StatusCode.Corruption),
            ("a checksum that is not a number", static m => m.Replace("/CURRENT crc32 ", "/CURRENT crc32 0x", StringComparison.Ordinal), StatusCode.Corruption),
            ("a checksum with a leading zero", static m => m.Replace("/CURRENT crc32 ", "/CURRENT crc32 0", StringComparison.Ordinal), StatusCode.Corruption),
            ("another checksum type", static m => m.Replace("/CURRENT crc32 ", "/CURRENT sha1 ", StringComparison.Ordinal), StatusCode.Corruption),
            ("an empty line", static m => InsertLine(m, 3, ""), StatusCode.Corruption),
            ("app metadata that is not hex", static m => InsertLine(m, 2, "metadata xyz"), StatusCode.Corruption),
            ("empty", static _ => "", StatusCode.Corruption),
            ("no final newline", static m => m[..^1], StatusCode.Corruption),
        ];
        MemoryFileSystem fs = s_metaValidationFs = TwoBackups();
        string meta = ReadText(fs, BackupDir + "/meta/2");
        foreach ((string name, Func<string, string> change, StatusCode? code) in cases)
        {
            WriteText(fs, BackupDir + "/meta/2", change(meta));
            using BackupEngineReadOnly ro = BackupEngineReadOnly.Open(Opts(), fs);
            if (code is null)
            {
                Assert.AreEqual(0, ro.GetCorruptedBackups().Count, name);
                ro.VerifyBackup(2, verifyWithChecksum: true);
            }
            else
            {
                CollectionAssert.AreEqual(new uint[] { 2 }, ro.GetCorruptedBackups(), name);
                Assert.AreEqual(code, Assert.ThrowsExactly<RocksDbException>(() => ro.VerifyBackup(2, verifyWithChecksum: false)).Code, name);
                Assert.AreEqual(code,
                    Assert.ThrowsExactly<RocksDbException>(() => ro.RestoreDbFromBackup(2, "/never", "/never", new RestoreOptions())).Code, name);
                Assert.IsFalse(fs.DirectoryExists("/never"), name + ": nothing is restored from a backup that could not be loaded");
                // The other backup stays usable and is the latest valid one.
                Assert.AreEqual(1u, ro.GetBackupInfo(0, includeFileDetails: false).BackupId, name);
                ro.VerifyBackup(1, verifyWithChecksum: true);
            }
        }

        static string WithSizes(string m, int delta)
        {
            MemoryFileSystem sizes = s_metaValidationFs!;
            var sb = new StringBuilder();
            foreach (string line in m.Split('\n'))
            {
                if (line.Length == 0) continue;
                int space = line.IndexOf(' ');
                if (space < 0 || !line.Contains('/')) sb.Append(line).Append('\n');
                else sb.Append(line).Append(" size ").Append(sizes.GetFileSize(BackupDir + "/" + line[..space]) + delta).Append('\n');
            }
            return sb.ToString();
        }

        static string InsertLine(string m, int index, string line)
        {
            var lines = new List<string>(m.Split('\n'));
            lines.Insert(index, line);
            return string.Join('\n', lines);
        }

        static string ReplaceLine(string m, int index, Func<string, string> change)
        {
            string[] lines = m.Split('\n');
            lines[index] = change(lines[index]);
            return string.Join('\n', lines);
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // Restore

    /// <summary>
    /// RestoreDBFromBackup deletes what the destination holds first: every file of the DB directory, the WAL directory
    /// and its archive (DeleteChildren), or with keep_log_files everything but the WALs, moving archived WALs back.
    /// </summary>
    [TestMethod]
    public void RestoreReplacesWhatTheDestinationHolds()
    {
        foreach (bool keepLogFiles in (bool[])[false, true])
        {
            MemoryFileSystem fs = TwoBackups();
            using BackupEngineReadOnly ro = BackupEngineReadOnly.Open(Opts(), fs);
            ro.RestoreDbFromBackup(2, "/r", "/wal", new RestoreOptions());
            string stale = "/r/" + FirstWithExtension(fs, "/r", ".sst");
            WriteText(fs, "/r/000900.sst", "a table file of another database");
            WriteText(fs, "/r/MANIFEST-000900", "a manifest of another database");
            WriteText(fs, "/r/junk", "junk");
            WriteText(fs, "/r/CURRENT.tmp", "MANIFEST-000900\n");
            WriteText(fs, "/wal/000901.log", "a log of another database");
            fs.CreateDirectoryIfMissing("/wal/archive");
            WriteText(fs, "/wal/archive/000902.log", "an archived log");
            WriteText(fs, "/wal/archive/junk", "junk");

            ro.RestoreDbFromBackup(1, "/r", "/wal", new RestoreOptions { KeepLogFiles = keepLogFiles });
            foreach (string gone in (string[])["/r/000900.sst", "/r/MANIFEST-000900", "/r/junk", "/r/CURRENT.tmp"])
            {
                Assert.IsFalse(fs.FileExists(gone), gone + " keepLogFiles=" + keepLogFiles);
            }
            Assert.IsTrue(fs.FileExists(stale));
            Assert.AreEqual(1, Children(fs, "/r").FindAll(static f => f.EndsWith(".sst", StringComparison.Ordinal)).Count);
            Assert.AreEqual(keepLogFiles, fs.FileExists("/wal/000901.log"));
            Assert.AreEqual(keepLogFiles, fs.FileExists("/wal/000902.log"), "archived logs are moved back with keep_log_files");
            Assert.IsFalse(fs.FileExists("/wal/archive/000902.log"));
            Assert.AreEqual(keepLogFiles, fs.FileExists("/wal/archive/junk"));
            Assert.AreEqual("MANIFEST-", ReadText(fs, "/r/CURRENT")[..9]);
        }
    }

    /// <summary>
    /// A restore that stops half-way must not leave something that opens: CURRENT is deleted with the rest of the
    /// destination, restored under a temporary name and renamed last; every copy is checked against the crc32c of the
    /// meta file.
    /// </summary>
    [TestMethod]
    public void AnInterruptedOrDamagedRestoreDoesNotLookLikeADatabase()
    {
        (string Name, Action<FaultInjectionFileSystem> Arrange, StatusCode Code)[] cases =
        [
            ("append to a table file fails", static fs => fs.FailMatching("/r/", FaultOps.Append, 1, InjectedFaultKind.IOError), StatusCode.IOError),
            ("sync of a restored file fails", static fs => fs.FailMatching("/r/", FaultOps.Sync, 1, InjectedFaultKind.IOError), StatusCode.IOError),
            ("append to CURRENT.tmp fails", static fs => fs.FailMatching("/r/CURRENT", FaultOps.Append, 1, InjectedFaultKind.IOError), StatusCode.IOError),
            ("directory sync fails", static fs => fs.FailMatching("/r", FaultOps.SyncDirectory, 1, InjectedFaultKind.IOError), StatusCode.IOError),
            ("rename of CURRENT.tmp fails", static fs => fs.FailMatching("/r/CURRENT", FaultOps.Rename, 1, InjectedFaultKind.IOError), StatusCode.IOError),
            ("damaged table file in the backup", static fs => Flip(fs, Shared + "/" + SharedFilesOf(fs, 2)[1]), StatusCode.Corruption),
            ("damaged MANIFEST in the backup", static fs => Flip(fs, BackupDir + "/private/2/" + Children(fs, BackupDir + "/private/2").Find(static f => f.StartsWith("MANIFEST", StringComparison.Ordinal))), StatusCode.Corruption),
            ("damaged CURRENT in the backup", static fs => Flip(fs, BackupDir + "/private/2/CURRENT"), StatusCode.Corruption),
        ];
        foreach ((string name, Action<FaultInjectionFileSystem> arrange, StatusCode code) in cases)
        {
            var fs = new FaultInjectionFileSystem(TwoBackups());
            using BackupEngineReadOnly ro = BackupEngineReadOnly.Open(Opts(), fs);
            // The destination holds a complete older database.
            ro.RestoreDbFromBackup(1, "/r", "/r", new RestoreOptions());
            arrange(fs);
            Assert.AreEqual(code, Assert.ThrowsExactly<RocksDbException>(() => ro.RestoreDbFromBackup(2, "/r", "/r", new RestoreOptions())).Code, name);
            fs.ClearFailMatching();
            Assert.IsFalse(fs.FileExists("/r/CURRENT"), name + ": CURRENT");
            Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open("/r", new DbOptions { FileSystem = fs }).Dispose(), name);
        }

        static void Flip(IFileSystem fs, string path)
        {
            byte[] d = ReadAll(fs, path);
            d[d.Length / 2] ^= 0x40;
            WriteAll(fs, path, d);
        }
    }

    /// <summary>
    /// With BackupEngineOptions::sync the restored files, the directory and the final rename of CURRENT are synced:
    /// everything survives a power loss right after the restore returns.
    /// </summary>
    [TestMethod]
    public void ASyncedRestoreSurvivesPowerLoss()
    {
        foreach (bool separateWalDir in (bool[])[false, true])
        {
            MemoryFileSystem mem = NewFs();
            var fs = new FaultInjectionFileSystem(mem);
            using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
            using (BackupEngine be = BackupEngine.Open(Opts(), fs))
            {
                Fill(db, 0, 100, 1);
                db.Flush();
                Fill(db, 100, 200, 2); // in the WAL
                be.CreateNewBackup(db, flushBeforeBackup: false);
                string wal = separateWalDir ? "/rwal" : "/r";
                fs.CreateDirectoryIfMissing("/r");
                fs.CreateDirectoryIfMissing(wal);
                fs.SyncDirectory("/");
                be.RestoreDbFromLatestBackup("/r", wal, new RestoreOptions());
            }
            fs.PowerLoss();
            fs.Restart();
            using RocksDatabase restored = RocksDatabase.Open("/r", new DbOptions { FileSystem = fs, WalDir = separateWalDir ? "/rwal" : "" });
            for (int i = 0; i < 200; i++) CollectionAssert.AreEqual(Value(i, i < 100 ? 1 : 2), restored.Get(Key(i)), "key " + i);
        }
    }

    /// <summary>
    /// DeleteChildren keeps a file when its number is in files_to_keep, whatever its type, and the restore loop skips
    /// a backup file the same way. files_to_keep holds table (and blob) file numbers of the destination that match the
    /// backup; a stale WAL of the destination with the number of a kept table file survived the restore upstream and
    /// would be replayed by the next open. Only the table / blob file itself is kept here.
    /// </summary>
    [TestMethod]
    public void AnIncrementalRestoreKeepsOnlyTheMatchingTableFileOfANumber()
    {
        foreach (RestoreMode mode in (RestoreMode[])[RestoreMode.VerifyChecksum, RestoreMode.KeepLatestDbSessionIdFiles])
        {
            MemoryFileSystem fs = TwoBackups();
            using BackupEngineReadOnly ro = BackupEngineReadOnly.Open(Opts(), fs);
            ro.RestoreDbFromBackup(2, "/r", "/r", new RestoreOptions());
            string sst = FirstWithExtension(fs, "/r", ".sst")!;
            string number = sst[..sst.IndexOf('.')];
            byte[] kept = ReadAll(fs, "/r/" + sst);
            WriteText(fs, "/r/" + number + ".log", "a stale log with the number of a table file");
            WriteText(fs, "/r/MANIFEST-" + number, "a stale manifest with the number of a table file");
            ro.RestoreDbFromBackup(2, "/r", "/r", new RestoreOptions { Mode = mode });
            Assert.IsFalse(fs.FileExists("/r/" + number + ".log"), mode + ": stale log");
            Assert.IsFalse(fs.FileExists("/r/MANIFEST-" + number), mode + ": stale manifest");
            CollectionAssert.AreEqual(kept, ReadAll(fs, "/r/" + sst));
            Check(fs, "/r", 200, static i => i < 100 ? 1 : 2);
        }
    }

    /// <summary>
    /// The incremental modes keep a destination file only when it is the backup's file: kVerifyChecksum compares the
    /// crc32c, kKeepLatestDbSessionIdFiles the session id and size of the shared_checksum name. A file that differs is
    /// replaced.
    /// </summary>
    [TestMethod]
    public void AnIncrementalRestoreReplacesFilesThatDiffer()
    {
        (RestoreMode Mode, string Change, bool Replaced)[] cases =
        [
            (RestoreMode.VerifyChecksum, "none", false),
            (RestoreMode.VerifyChecksum, "flip", true),
            (RestoreMode.VerifyChecksum, "truncate", true),
            (RestoreMode.KeepLatestDbSessionIdFiles, "none", false),
            (RestoreMode.KeepLatestDbSessionIdFiles, "truncate", true),
            (RestoreMode.KeepLatestDbSessionIdFiles, "garbage", true),
            (RestoreMode.PurgeAllFiles, "none", true),
        ];
        foreach ((RestoreMode mode, string change, bool replaced) in cases)
        {
            var fs = new FaultInjectionFileSystem(TwoBackups());
            using BackupEngineReadOnly ro = BackupEngineReadOnly.Open(Opts(), fs);
            ro.RestoreDbFromBackup(2, "/r", "/r", new RestoreOptions());
            string sst = "/r/" + FirstWithExtension(fs, "/r", ".sst");
            byte[] original = ReadAll(fs, sst);
            byte[] changed = change switch
            {
                "flip" => Flipped(original),
                "truncate" => original[..^1],
                "garbage" => new byte[original.Length],
                _ => original,
            };
            if (change != "none") WriteAll(fs, sst, changed);
            // Whether the restore writes the file again shows in a create of that path.
            fs.FailMatching(sst, FaultOps.Create, 1, InjectedFaultKind.IOError);
            if (replaced)
            {
                Assert.AreEqual(StatusCode.IOError,
                    Assert.ThrowsExactly<RocksDbException>(() => ro.RestoreDbFromBackup(2, "/r", "/r", new RestoreOptions { Mode = mode })).Code, mode + " " + change);
                fs.ClearFailMatching();
                if (change != "none") WriteAll(fs, sst, changed);
            }
            ro.RestoreDbFromBackup(2, "/r", "/r", new RestoreOptions { Mode = mode });
            fs.ClearFailMatching();
            CollectionAssert.AreEqual(original, ReadAll(fs, sst), mode + " " + change);
            Check(fs, "/r", 200, static i => i < 100 ? 1 : 2);
        }

        static byte[] Flipped(byte[] d)
        {
            byte[] c = (byte[])d.Clone();
            c[c.Length / 2] ^= 1;
            return c;
        }
    }

    private static void ExcludeAll(IReadOnlyList<MaybeExcludeBackupFile> files)
    {
        foreach (MaybeExcludeBackupFile f in files) f.ExcludeDecision = true;
    }

    /// <summary>
    /// RestoreDBFromBackup / InferDBFilesToRetainInRestore (unowned_backups): in kKeepLatestDbSessionIdFiles mode an
    /// excluded file that no alternate directory holds is acceptable when the destination already has the table file
    /// with that session id and size. The port failed with InvalidArgument before looking at the destination. Any
    /// other mode, or a destination without the file, fails before anything is deleted.
    /// </summary>
    [TestMethod]
    public void KeepLatestDbSessionIdFilesAcceptsExcludedFilesTheDestinationHolds()
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            Fill(db, 0, 100, 1);
            db.Flush();
            using (BackupEngine alt = BackupEngine.Open(new BackupEngineOptions("/alt"), fs))
            {
                alt.CreateNewBackup(db, flushBeforeBackup: false);
                alt.RestoreDbFromLatestBackup("/r", "/r", new RestoreOptions());
            }
            using BackupEngine excluding = BackupEngine.Open(new BackupEngineOptions(BackupDir) { SchemaVersion = 2 }, fs);
            excluding.CreateNewBackup(db, new CreateBackupOptions { ExcludeFilesCallback = ExcludeAll });
        }
        Assert.AreEqual(0, Children(fs, Shared).Count);
        StringAssert.Contains(ReadText(fs, BackupDir + "/meta/1"), " ni::excluded true");

        using BackupEngineReadOnly be = BackupEngineReadOnly.Open(Opts(), fs);
        List<string> before = Children(fs, "/r");
        foreach (RestoreMode mode in (RestoreMode[])[RestoreMode.PurgeAllFiles, RestoreMode.VerifyChecksum])
        {
            Assert.AreEqual(StatusCode.InvalidArgument,
                Assert.ThrowsExactly<RocksDbException>(() => be.RestoreDbFromLatestBackup("/r", "/r", new RestoreOptions { Mode = mode })).Code, mode.ToString());
            CollectionAssert.AreEqual(before, Children(fs, "/r"), mode + ": nothing was deleted");
        }
        be.RestoreDbFromLatestBackup("/r", "/r", new RestoreOptions { Mode = RestoreMode.KeepLatestDbSessionIdFiles });
        Check(fs, "/r", 100, static _ => 1);

        // Without the table file in the destination the excluded file is missing.
        string sst = "/r/" + FirstWithExtension(fs, "/r", ".sst");
        WriteAll(fs, sst, ReadAll(fs, sst)[..^1]);
        before = Children(fs, "/r");
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() =>
            be.RestoreDbFromLatestBackup("/r", "/r", new RestoreOptions { Mode = RestoreMode.KeepLatestDbSessionIdFiles })).Code);
        CollectionAssert.AreEqual(before, Children(fs, "/r"));
        Assert.IsTrue(fs.FileExists("/r/CURRENT"));
    }

    /// <summary>
    /// BackupEngineImplThreadSafe::RestoreDBFromBackup read-locks the engines of options.alternate_dirs along with its
    /// own before it looks into their backuped_file_infos_. The port read an alternate engine's file table without its
    /// lock, i.e. while that engine was in the middle of a CreateNewBackup: here the restore must wait for the backup
    /// that adds the excluded file to the alternate directory, and then find it.
    /// </summary>
    [TestMethod]
    public void RestoreWaitsForAnAlternateDirectoryThatIsBeingWritten()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        Fill(db, 0, 100, 1);
        db.Flush();
        using BackupEngine be = BackupEngine.Open(new BackupEngineOptions(BackupDir) { SchemaVersion = 2 }, fs);
        be.CreateNewBackup(db, new CreateBackupOptions { ExcludeFilesCallback = ExcludeAll });
        using BackupEngine alt = BackupEngine.Open(new BackupEngineOptions("/alt") { CallbackTriggerIntervalSize = 1 }, fs);
        Task? restore = null;
        bool finishedDuringTheBackup = false;
        alt.CreateNewBackup(db, new CreateBackupOptions
        {
            ProgressCallback = () =>
            {
                if (restore is not null) return;
                var options = new RestoreOptions();
                options.AlternateDirs.Add(alt);
                restore = Task.Run(() => be.RestoreDbFromLatestBackup("/r", "/r", options));
                try
                {
                    finishedDuringTheBackup = restore.Wait(TimeSpan.FromMilliseconds(500));
                }
                catch (AggregateException)
                {
                    finishedDuringTheBackup = true;
                }
            },
        });
        Assert.IsNotNull(restore);
        Assert.IsFalse(finishedDuringTheBackup, "the restore did not wait for the alternate engine");
        restore.GetAwaiter().GetResult();
        Check(fs, "/r", 100, static _ => 1);
    }

    /// <summary>
    /// A meta file entry whose name is not a DB file name fails the restore with Corruption ("Fail to parse filename").
    /// Upstream finds out in the copy loop, after DeleteChildren emptied the destination; the names are checked before
    /// anything is deleted here. A shared_checksum name with no '.' after its first '_' (GetFileFromChecksumFile erases
    /// with a wrapped-around length upstream) is such a name, not an ArgumentOutOfRangeException.
    /// </summary>
    [TestMethod]
    public void ABackupListingAFileThatIsNotADbFileIsRefusedBeforeTheDestinationIsTouched()
    {
        foreach (string odd in (string[])["shared_checksum/a.b_c", "shared_checksum/notadbfile", "private/2/notadbfile"])
        {
            MemoryFileSystem fs = TwoBackups();
            WriteAll(fs, BackupDir + "/" + odd, []);
            string[] lines = ReadText(fs, BackupDir + "/meta/2").Split('\n');
            lines[2] = (int.Parse(lines[2]) + 1).ToString();
            WriteText(fs, BackupDir + "/meta/2", string.Join('\n', lines) + odd + " crc32 0\n");
            using BackupEngineReadOnly ro = BackupEngineReadOnly.Open(Opts(), fs);
            Assert.AreEqual(0, ro.GetCorruptedBackups().Count, odd);
            BackupInfo info = ro.GetBackupInfo(2, includeFileDetails: true);
            Assert.AreEqual(odd, info.FileDetails![^1].RelativeFilename);
            ro.VerifyBackup(2, verifyWithChecksum: true);
            ro.RestoreDbFromBackup(1, "/r", "/r", new RestoreOptions());
            List<string> before = Children(fs, "/r");
            Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => ro.RestoreDbFromBackup(2, "/r", "/r", new RestoreOptions())).Code, odd);
            CollectionAssert.AreEqual(before, Children(fs, "/r"), odd + ": the destination is untouched");
            Check(fs, "/r", 200, static i => i < 100 ? 1 : null);
        }
    }
}
