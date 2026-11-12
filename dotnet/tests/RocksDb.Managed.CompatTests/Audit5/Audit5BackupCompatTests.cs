using System.Text;
using RocksDb.Managed.CompatTests.Api;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Utilities.Backup;

namespace RocksDb.Managed.CompatTests.Audit5;

/// <summary>
/// Audit #5: delete, purge, garbage collection and restore of the managed backup engine against native RocksDB
/// (utilities/backup/backup_engine.cc) on backup directories both engines wrote to.
/// </summary>
[TestClass]
public sealed class Audit5BackupCompatTests
{
    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);

    private static string K(int i) => "k" + i.ToString("D6");

    private static string V(int i, string tag) => tag + "-" + i.ToString("D6") + new string('v', i % 40);

    private static void CheckNative(string path, int count, Func<int, string?> expected)
    {
        using var db = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions().SetParanoidChecks(true), path);
        for (int i = 0; i < count; i++)
        {
            byte[]? v = db.Get(U(K(i)));
            Assert.AreEqual(expected(i), v is null ? null : Encoding.UTF8.GetString(v), "key " + i);
        }
    }

    private static void CheckManaged(string path, int count, Func<int, string?> expected)
    {
        using RocksDatabase db = RocksDatabase.Open(path, new DbOptions());
        for (int i = 0; i < count; i++)
        {
            byte[]? v = db.Get(U(K(i)));
            Assert.AreEqual(expected(i), v is null ? null : Encoding.UTF8.GetString(v), "key " + i);
        }
    }

    private static List<string> Names(string dir)
    {
        var list = new List<string>();
        if (Directory.Exists(dir))
        {
            foreach (string f in Directory.EnumerateFileSystemEntries(dir)) list.Add(Path.GetFileName(f));
        }
        list.Sort(StringComparer.Ordinal);
        return list;
    }

    private static List<string> SharedFilesOf(string backupDir, uint id)
    {
        var list = new List<string>();
        foreach (string line in File.ReadAllLines(Path.Combine(backupDir, "meta", id.ToString())))
        {
            if (line.StartsWith("shared_checksum/", StringComparison.Ordinal)) list.Add(line.Split(' ')[0]["shared_checksum/".Length..]);
        }
        list.Sort(StringComparer.Ordinal);
        return list;
    }

    private static void CopyTree(string from, string to)
    {
        Directory.CreateDirectory(to);
        foreach (string f in Directory.EnumerateFiles(from)) File.Copy(f, Path.Combine(to, Path.GetFileName(f)));
        foreach (string d in Directory.EnumerateDirectories(from)) CopyTree(d, Path.Combine(to, Path.GetFileName(d)));
    }

    /// <summary>
    /// Backups 1 and 2 by native, 3 and 4 by managed, of one DB that grows by one table file per backup. Managed
    /// deletes one backup of each engine and garbage collects: native verifies and restores what is left; then native
    /// purges and managed verifies (with checksums) and restores the last one.
    /// </summary>
    [TestMethod]
    public void ManagedDeleteAndGarbageCollectionKeepWhatTheRemainingBackupsNeed()
    {
        using var dir = new TempDir("a5-bk-del");
        string dbPath = Path.Combine(dir.Path, "db"), backupDir = Path.Combine(dir.Path, "backup");
        using (var db = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions().SetCreateIfMissing(true), dbPath))
        using (var nbe = new NativeBackupEngine(backupDir))
        {
            for (int i = 0; i < 100; i++) db.Put(U(K(i)), U(V(i, "a")));
            nbe.CreateNewBackup(db, flush: true);
            for (int i = 100; i < 200; i++) db.Put(U(K(i)), U(V(i, "b")));
            nbe.CreateNewBackup(db, flush: true);
        }
        BackupCompatTests.AssertNativeMetaIsNotEmpty(backupDir, 2);
        using (RocksDatabase db = RocksDatabase.Open(dbPath, new DbOptions()))
        using (BackupEngine be = BackupEngine.Open(new BackupEngineOptions(backupDir)))
        {
            for (int i = 200; i < 300; i++) db.Put(U(K(i)), U(V(i, "c")));
            Assert.AreEqual(3u, be.CreateNewBackup(db, flushBeforeBackup: true));
            for (int i = 300; i < 400; i++) db.Put(U(K(i)), U(V(i, "d")));
            Assert.AreEqual(4u, be.CreateNewBackup(db, flushBeforeBackup: true));
            Assert.AreEqual(4, SharedFilesOf(backupDir, 4).Count);

            be.DeleteBackup(1);
            be.DeleteBackup(3);
            be.GarbageCollect();
            be.VerifyBackup(2, verifyWithChecksum: true);
            be.VerifyBackup(4, verifyWithChecksum: true);
        }
        CollectionAssert.AreEqual(SharedFilesOf(backupDir, 4), Names(Path.Combine(backupDir, "shared_checksum")));
        CollectionAssert.AreEqual(new[] { "2", "4" }, Names(Path.Combine(backupDir, "private")));
        CollectionAssert.AreEqual(new[] { "2", "4" }, Names(Path.Combine(backupDir, "meta")));
        Func<int, string?> all = static i => i < 100 ? V(i, "a") : i < 200 ? V(i, "b") : i < 300 ? V(i, "c") : V(i, "d");
        using (var nbe = new NativeBackupEngine(backupDir))
        {
            CollectionAssert.AreEqual(new uint[] { 2, 4 }, nbe.Info().ConvertAll(static x => x.Id));
            nbe.Verify(2);
            nbe.Verify(4);
            nbe.Restore(2, Path.Combine(dir.Path, "r2"));
            nbe.Restore(4, Path.Combine(dir.Path, "r4"));
            CheckNative(Path.Combine(dir.Path, "r2"), 400, i => i < 200 ? all(i) : null);
            CheckNative(Path.Combine(dir.Path, "r4"), 400, all);
            nbe.Purge(1);
        }
        using (BackupEngine be = BackupEngine.Open(new BackupEngineOptions(backupDir)))
        {
            CollectionAssert.AreEqual(new uint[] { 4 }, be.GetBackupInfo().ConvertAll(static x => x.BackupId));
            Assert.AreEqual(0, be.GetCorruptedBackups().Count);
            be.VerifyBackup(4, verifyWithChecksum: true);
            be.GarbageCollect();
            be.VerifyBackup(4, verifyWithChecksum: true);
            be.RestoreDbFromLatestBackup(Path.Combine(dir.Path, "m4"), Path.Combine(dir.Path, "m4"), new RestoreOptions());
        }
        CheckManaged(Path.Combine(dir.Path, "m4"), 400, all);
    }

    /// <summary>
    /// A backup whose meta file this version cannot load (here: an unknown schema version) is in corrupt_backups_, holds
    /// no reference counts and is not in backups_. Native's GarbageCollect, which runs by itself on the first
    /// PurgeOldBackups / DeleteBackup of a writable engine, then deletes that backup's private directory and the shared
    /// files only it uses, leaving its meta file: a backup written by a newer version is destroyed by an older one that
    /// merely purged. The managed engine keeps what the meta file names until the backup is deleted by id; native then
    /// verifies and restores it once the meta file is readable again.
    /// </summary>
    [TestMethod]
    public void BackupsThatCannotBeLoadedSurviveManagedGarbageCollection()
    {
        using var dir = new TempDir("a5-bk-unloaded");
        string dbPath = Path.Combine(dir.Path, "db"), nativeDir = Path.Combine(dir.Path, "backup-native"), managedDir = Path.Combine(dir.Path, "backup-managed");
        using (RocksDatabase db = RocksDatabase.Open(dbPath, new DbOptions { CreateIfMissing = true }))
        using (BackupEngine be = BackupEngine.Open(new BackupEngineOptions(managedDir)))
        {
            for (int i = 0; i < 100; i++) db.Put(U(K(i)), U(V(i, "a")));
            be.CreateNewBackup(db, flushBeforeBackup: true);
            for (int i = 100; i < 200; i++) db.Put(U(K(i)), U(V(i, "b")));
            be.CreateNewBackup(db, flushBeforeBackup: true);
        }
        CopyTree(managedDir, nativeDir);
        string meta = File.ReadAllText(Path.Combine(managedDir, "meta", "2"));
        List<string> shared = Names(Path.Combine(managedDir, "shared_checksum")), privateFiles = Names(Path.Combine(managedDir, "private", "2"));
        Assert.AreEqual(2, shared.Count);
        foreach (string d in (string[])[nativeDir, managedDir]) File.WriteAllText(Path.Combine(d, "meta", "2"), "schema_version 9\n" + meta);

        // Native (librocksdb 11.12): nothing was asked to be deleted, yet backup 2's data is gone.
        using (var nbe = new NativeBackupEngine(nativeDir))
        {
            CollectionAssert.AreEqual(new uint[] { 1 }, nbe.Info().ConvertAll(static x => x.Id));
            nbe.Purge(10);
        }
        Assert.IsTrue(File.Exists(Path.Combine(nativeDir, "meta", "2")));
        CollectionAssert.AreEqual(SharedFilesOf(nativeDir, 1), Names(Path.Combine(nativeDir, "shared_checksum")), "native kept only backup 1's table file");
        Assert.AreEqual(0, Names(Path.Combine(nativeDir, "private", "2")).Count, "native deleted the private files of the backup it could not load");

        using (BackupEngine be = BackupEngine.Open(new BackupEngineOptions(managedDir)))
        {
            CollectionAssert.AreEqual(new uint[] { 2 }, be.GetCorruptedBackups());
            be.PurgeOldBackups(10);
            be.GarbageCollect();
            be.DeleteBackup(1);
        }
        CollectionAssert.AreEqual(shared, Names(Path.Combine(managedDir, "shared_checksum")));
        CollectionAssert.AreEqual(privateFiles, Names(Path.Combine(managedDir, "private", "2")));
        File.WriteAllText(Path.Combine(managedDir, "meta", "2"), meta);
        using (var nbe = new NativeBackupEngine(managedDir))
        {
            CollectionAssert.AreEqual(new uint[] { 2 }, nbe.Info().ConvertAll(static x => x.Id));
            nbe.Verify(2);
            nbe.Restore(2, Path.Combine(dir.Path, "r"));
        }
        CheckNative(Path.Combine(dir.Path, "r"), 200, static i => i < 100 ? V(i, "a") : V(i, "b"));
    }

    /// <summary>
    /// Incremental managed restores (kVerifyChecksum, kKeepLatestDbSessionIdFiles) of a native backup over a directory
    /// native restored and that then picked up stale and damaged files: the result opens in native with the backup's
    /// data, matching table files are kept, everything else is replaced or removed.
    /// </summary>
    [TestMethod]
    public void ManagedIncrementalRestoreOverANativeRestore()
    {
        using var dir = new TempDir("a5-bk-incr");
        string dbPath = Path.Combine(dir.Path, "db"), backupDir = Path.Combine(dir.Path, "backup"), restored = Path.Combine(dir.Path, "r");
        using (var db = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions().SetCreateIfMissing(true), dbPath))
        using (var nbe = new NativeBackupEngine(backupDir))
        {
            for (int i = 0; i < 300; i++) db.Put(U(K(i)), U(V(i, "a")));
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            for (int i = 200; i < 500; i++) db.Put(U(K(i)), U(V(i, "b")));
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            for (int i = 450; i < 600; i++) db.Put(U(K(i)), U(V(i, "c"))); // only in the WAL
            nbe.CreateNewBackup(db, flush: false);
            nbe.RestoreLatest(restored);
        }
        BackupCompatTests.AssertNativeMetaIsNotEmpty(backupDir, 1);
        Func<int, string?> expected = static i => i < 200 ? V(i, "a") : i < 450 ? V(i, "b") : V(i, "c");
        List<string> tables = Names(restored).FindAll(static f => f.EndsWith(".sst", StringComparison.Ordinal));
        Assert.AreEqual(2, tables.Count);
        var original = new Dictionary<string, byte[]>();
        foreach (string t in tables) original[t] = File.ReadAllBytes(Path.Combine(restored, t));

        using BackupEngineReadOnly be = BackupEngineReadOnly.Open(new BackupEngineOptions(backupDir));
        foreach (RestoreMode mode in (RestoreMode[])[RestoreMode.VerifyChecksum, RestoreMode.KeepLatestDbSessionIdFiles, RestoreMode.PurgeAllFiles])
        {
            string number = tables[0][..tables[0].IndexOf('.')];
            File.WriteAllText(Path.Combine(restored, number + ".log"), "a stale log with the number of a table file");
            File.WriteAllText(Path.Combine(restored, "000999.sst"), "a table file of another database");
            File.WriteAllText(Path.Combine(restored, "junk"), "junk");
            DateTime keptStamp = new(2001, 2, 3, 4, 5, 6, DateTimeKind.Utc);
            File.SetLastWriteTimeUtc(Path.Combine(restored, tables[0]), keptStamp);
            // The second table file is damaged without changing its size (cut short for the session id mode, which
            // identifies a file by session id and size).
            byte[] damaged = (byte[])original[tables[1]].Clone();
            damaged[damaged.Length / 2] ^= 0x10;
            File.WriteAllBytes(Path.Combine(restored, tables[1]), mode == RestoreMode.KeepLatestDbSessionIdFiles ? damaged[..^1] : damaged);

            be.RestoreDbFromLatestBackup(restored, restored, new RestoreOptions { Mode = mode });

            foreach (string gone in (string[])[number + ".log", "000999.sst", "junk", "CURRENT.tmp"])
            {
                Assert.IsFalse(File.Exists(Path.Combine(restored, gone)), mode + ": " + gone);
            }
            foreach (string t in tables) CollectionAssert.AreEqual(original[t], File.ReadAllBytes(Path.Combine(restored, t)), mode + ": " + t);
            Assert.AreEqual(mode != RestoreMode.PurgeAllFiles, File.GetLastWriteTimeUtc(Path.Combine(restored, tables[0])) == keptStamp,
                mode + ": the matching table file is kept only by the incremental modes");
            CheckNative(restored, 600, expected);
        }
    }
}
