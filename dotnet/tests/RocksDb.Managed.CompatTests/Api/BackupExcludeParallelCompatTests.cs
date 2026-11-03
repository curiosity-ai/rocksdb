using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Utilities.Backup;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>
/// BackupEngine exclude_files_callback / alternate_dirs and parallel copies (max_background_operations)
/// (utilities/backup/backup_engine.cc): layouts stay readable by native RocksDB 11.12 and restorable both ways.
/// </summary>
[TestClass]
public sealed class BackupExcludeParallelCompatTests
{
    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);
    private static string K(int i) => "k" + i.ToString("D6");
    private static string V(int i, string tag) => tag + "-" + i.ToString("D6") + new string('v', i % 40);

    private static void Write(RocksDatabase db, int from, int to, string tag)
    {
        for (int i = from; i < to; i++) db.Put(U(K(i)), U(V(i, tag)));
    }

    private static void CheckManaged(string path, int count, Func<int, string> expected)
    {
        using RocksDatabase db = RocksDatabase.Open(path, new DbOptions());
        for (int i = 0; i < count; i++)
        {
            byte[]? v = db.Get(U(K(i)));
            Assert.AreEqual(expected(i), v is null ? null : Encoding.UTF8.GetString(v), "key " + i);
        }
    }

    private static void CheckNative(string path, int count, Func<int, string> expected)
    {
        using var db = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions(), path);
        for (int i = 0; i < count; i++)
        {
            byte[]? v = db.Get(U(K(i)));
            Assert.AreEqual(expected(i), v is null ? null : Encoding.UTF8.GetString(v), "key " + i);
        }
    }

    [TestMethod]
    public void ParallelBackupAndRestoreInteroperateWithNative()
    {
        using var dir = new TempDir("bk-par");
        string dbPath = Path.Combine(dir.Path, "db"), backupDir = Path.Combine(dir.Path, "backup");
        string r1 = Path.Combine(dir.Path, "r1"), r2 = Path.Combine(dir.Path, "r2");
        var opts = new BackupEngineOptions(backupDir) { MaxBackgroundOperations = 4 };
        int progress = 0;
        using (RocksDatabase db = RocksDatabase.Open(dbPath, new DbOptions { CreateIfMissing = true }))
        using (BackupEngine be = BackupEngine.Open(opts))
        {
            for (int f = 0; f < 8; f++)
            {
                Write(db, f * 100, f * 100 + 100, "p");
                db.Flush();
            }
            Write(db, 800, 850, "w"); // WAL only
            be.CreateNewBackup(db, new CreateBackupOptions { ProgressCallback = () => Interlocked.Increment(ref progress) });
            Assert.IsTrue(be.GetBackupInfo(includeFileDetails: true)[0].FileDetails!.Count >= 10);
            be.VerifyBackup(1, verifyWithChecksum: true);
            be.RestoreDbFromLatestBackup(r2, r2, new RestoreOptions());
        }
        CheckManaged(r2, 850, i => V(i, i < 800 ? "p" : "w"));
        using (var nbe = new NativeBackupEngine(backupDir))
        {
            nbe.Verify(1);
            nbe.Restore(1, r1);
        }
        CheckNative(r1, 850, i => V(i, i < 800 ? "p" : "w"));
        // A parallel incremental backup into the same directory reuses the shared files.
        using (RocksDatabase db = RocksDatabase.Open(dbPath, new DbOptions()))
        using (BackupEngine be = BackupEngine.Open(opts))
        {
            Write(db, 0, 10, "z");
            Assert.AreEqual(2u, be.CreateNewBackup(db, flushBeforeBackup: true));
        }
        using (var nbe = new NativeBackupEngine(backupDir))
        {
            Assert.AreEqual(2, nbe.Info().Count);
            nbe.Verify(2);
        }
    }

    [TestMethod]
    public void ExcludedFilesAreRestoredFromAlternateDirectories()
    {
        using var dir = new TempDir("bk-excl");
        string dbPath = Path.Combine(dir.Path, "db"), dirA = Path.Combine(dir.Path, "A"), dirB = Path.Combine(dir.Path, "B");
        string r1 = Path.Combine(dir.Path, "r1"), r2 = Path.Combine(dir.Path, "r2");
        var optsA = new BackupEngineOptions(dirA) { SchemaVersion = 2 };
        var optsB = new BackupEngineOptions(dirB) { SchemaVersion = 2, MaxBackgroundOperations = 3 };
        var excluded = new List<string>();
        using (RocksDatabase db = RocksDatabase.Open(dbPath, new DbOptions { CreateIfMissing = true }))
        {
            for (int f = 0; f < 3; f++)
            {
                Write(db, f * 100, f * 100 + 100, "a");
                db.Flush();
            }
            using (BackupEngine a = BackupEngine.Open(optsA)) a.CreateNewBackup(db, flushBeforeBackup: true);
            Write(db, 300, 400, "b");
            db.Flush();
            using BackupEngine b = BackupEngine.Open(optsB);
            // Exclude every shared file that backup directory A already holds.
            b.CreateNewBackup(db, new CreateBackupOptions
            {
                FlushBeforeBackup = true,
                ExcludeFilesCallback = files =>
                {
                    foreach (MaybeExcludeBackupFile f in files)
                    {
                        if (File.Exists(Path.Combine(dirA, f.RelativeFile)))
                        {
                            f.ExcludeDecision = true;
                            excluded.Add(f.RelativeFile);
                        }
                    }
                },
            });
            Assert.AreEqual(3, excluded.Count);
            BackupInfo info = b.GetBackupInfo(includeFileDetails: true).Single();
            CollectionAssert.AreEquivalent(excluded, info.ExcludedFiles!.ToList());
            foreach (string e in excluded) Assert.IsFalse(File.Exists(Path.Combine(dirB, e)), e + " must not be copied");

            // Without the alternate directory the restore fails; with it, it succeeds.
            RocksDbException err = Assert.ThrowsExactly<RocksDbException>(() => b.RestoreDbFromLatestBackup(r1, r1, new RestoreOptions()));
            Assert.AreEqual(StatusCode.InvalidArgument, err.Code);
            using BackupEngineReadOnly ra = BackupEngineReadOnly.Open(optsA);
            var restore = new RestoreOptions();
            restore.AlternateDirs.Add(ra);
            b.RestoreDbFromLatestBackup(r1, r1, restore);
        }
        CheckManaged(r1, 400, i => V(i, i < 300 ? "a" : "b"));
        // Native opens the meta file with the non-ignorable "ni::excluded" field and verifies the backup's own files.
        using (var nbe = new NativeBackupEngine(dirB))
        {
            Assert.AreEqual(1, nbe.Info().Count);
            nbe.Verify(1);
        }
        // The restored DB is a regular DB for native too.
        CheckNative(r1, 400, i => V(i, i < 300 ? "a" : "b"));

        // exclude_files_callback requires schema_version >= 2; a throwing callback aborts the backup.
        using (RocksDatabase db = RocksDatabase.Open(dbPath, new DbOptions()))
        {
            using (BackupEngine v1 = BackupEngine.Open(new BackupEngineOptions(Path.Combine(dir.Path, "C"))))
            {
                RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() =>
                    v1.CreateNewBackup(db, new CreateBackupOptions { ExcludeFilesCallback = static _ => { } }));
                Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
            }
            using BackupEngine b2 = BackupEngine.Open(new BackupEngineOptions(Path.Combine(dir.Path, "D")) { SchemaVersion = 2 });
            RocksDbException e2 = Assert.ThrowsExactly<RocksDbException>(() =>
                b2.CreateNewBackup(db, new CreateBackupOptions { ExcludeFilesCallback = static _ => throw new InvalidOperationException("no") }));
            Assert.AreEqual(StatusCode.Aborted, e2.Code);
            Assert.AreEqual(0, b2.GetBackupInfo().Count);
            _ = r2;
        }
    }
}
