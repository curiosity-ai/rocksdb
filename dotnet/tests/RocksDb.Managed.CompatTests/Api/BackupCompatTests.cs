using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Utilities.Backup;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>Thin wrapper over the native BackupEngine C API (rocksdb_backup_engine_*), default BackupEngineOptions.</summary>
internal sealed class NativeBackupEngine : IDisposable
{
    private static readonly RocksDbSharp.Native N = RocksDbSharp.Native.Instance;
    private readonly RocksDbSharp.DbOptions _options = new();
    public IntPtr Handle { get; private set; }

    public NativeBackupEngine(string backupDir) => Handle = N.rocksdb_backup_engine_open(_options.Handle, backupDir);

    public void CreateNewBackup(RocksDbSharp.RocksDb db, bool flush) => N.rocksdb_backup_engine_create_new_backup_flush(Handle, db.Handle, flush ? (byte)1 : (byte)0);

    public void Verify(uint id) => N.rocksdb_backup_engine_verify_backup(Handle, id);

    public void Purge(uint keep) => N.rocksdb_backup_engine_purge_old_backups(Handle, keep);

    public void RestoreLatest(string dbDir)
    {
        IntPtr ro = N.rocksdb_restore_options_create();
        try { N.rocksdb_backup_engine_restore_db_from_latest_backup(Handle, dbDir, dbDir, ro); }
        finally { N.rocksdb_restore_options_destroy(ro); }
    }

    public void Restore(uint id, string dbDir)
    {
        IntPtr ro = N.rocksdb_restore_options_create();
        try { N.rocksdb_backup_engine_restore_db_from_backup(Handle, dbDir, dbDir, ro, id); }
        finally { N.rocksdb_restore_options_destroy(ro); }
    }

    public List<(uint Id, uint Files, ulong Size)> Info()
    {
        IntPtr info = N.rocksdb_backup_engine_get_backup_info(Handle);
        try
        {
            var list = new List<(uint, uint, ulong)>();
            int n = N.rocksdb_backup_engine_info_count(info);
            for (int i = 0; i < n; i++)
            {
                list.Add((N.rocksdb_backup_engine_info_backup_id(info, i), N.rocksdb_backup_engine_info_number_files(info, i),
                    N.rocksdb_backup_engine_info_size(info, i)));
            }
            return list;
        }
        finally
        {
            N.rocksdb_backup_engine_info_destroy(info);
        }
    }

    public void Dispose()
    {
        if (Handle != IntPtr.Zero) N.rocksdb_backup_engine_close(Handle);
        Handle = IntPtr.Zero;
    }
}

/// <summary>
/// BackupEngine compatibility with native RocksDB 11.12 (utilities/backup/backup_engine.cc): identical directory layout
/// and meta files, backups restorable by the other side, incremental backups added by either engine to a directory
/// created by the other.
/// </summary>
[TestClass]
public sealed class BackupCompatTests
{
    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);

    private static string V(int i, string tag) => tag + "-" + i.ToString("D6") + new string('v', i % 40);

    private static void WriteNative(RocksDbSharp.RocksDb db, int from, int to, string tag)
    {
        for (int i = from; i < to; i++) db.Put(U("k" + i.ToString("D6")), U(V(i, tag)));
    }

    private static void WriteManaged(RocksDatabase db, int from, int to, string tag)
    {
        for (int i = from; i < to; i++) db.Put(U("k" + i.ToString("D6")), U(V(i, tag)));
    }

    private static void CheckNative(string path, int count, Func<int, string> expected)
    {
        using var db = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions(), path);
        for (int i = 0; i < count; i++)
        {
            byte[]? v = db.Get(U("k" + i.ToString("D6")));
            Assert.AreEqual(expected(i), v is null ? null : Encoding.UTF8.GetString(v), "key " + i);
        }
    }

    private static void CheckManaged(string path, int count, Func<int, string> expected)
    {
        using RocksDatabase db = RocksDatabase.Open(path, new DbOptions());
        for (int i = 0; i < count; i++)
        {
            byte[]? v = db.Get(U("k" + i.ToString("D6")));
            Assert.AreEqual(expected(i), v is null ? null : Encoding.UTF8.GetString(v), "key " + i);
        }
    }

    /// <summary>The meta file lines of table files (shared_checksum/...sst crc32 N).</summary>
    private static SortedSet<string> SstLines(string backupDir, uint id)
    {
        var set = new SortedSet<string>(StringComparer.Ordinal);
        foreach (string line in File.ReadAllLines(Path.Combine(backupDir, "meta", id.ToString())))
        {
            if (line.StartsWith("shared_checksum/", StringComparison.Ordinal)) set.Add(line);
        }
        return set;
    }

    /// <summary>
    /// The native BackupEngine (librocksdb 11.12 from the RocksDB NuGet package) wrote empty <c>meta/&lt;id&gt;</c> files
    /// under a .NET 11 apphost: its statically linked libstdc++ throws std::bad_cast on numeric ostream insertion unless
    /// the system libstdc++.so.6 is globally visible (tests/Shared/NativeStdCxxPreload.cs). Fail clearly if that regresses.
    /// </summary>
    internal static void AssertNativeMetaIsNotEmpty(string backupDir, uint id)
    {
        if (new FileInfo(Path.Combine(backupDir, "meta", id.ToString())).Length == 0)
        {
            Assert.Fail("native BackupEngine wrote an empty meta file (librocksdb 11.12 hosted by .NET " + Environment.Version +
                "): is libstdc++.so.6 preloaded globally (NativeStdCxxPreload)?");
        }
    }

    /// <summary>The kinds of private files of a backup (CURRENT, MANIFEST, OPTIONS, log), in order of appearance.</summary>
    private static List<string> PrivateKinds(string backupDir, uint id)
    {
        var kinds = new SortedSet<string>(StringComparer.Ordinal);
        foreach (string line in File.ReadAllLines(Path.Combine(backupDir, "meta", id.ToString())))
        {
            if (!line.StartsWith("private/", StringComparison.Ordinal)) continue;
            string name = line.Split(' ')[0];
            name = name[(name.LastIndexOf('/') + 1)..];
            kinds.Add(name.StartsWith("MANIFEST", StringComparison.Ordinal) ? "MANIFEST" : name.StartsWith("OPTIONS", StringComparison.Ordinal) ? "OPTIONS"
                : name.EndsWith(".log", StringComparison.Ordinal) ? "log" : name);
            Assert.IsTrue(line.Contains(" crc32 ", StringComparison.Ordinal), line);
        }
        return kinds.ToList();
    }

    [TestMethod]
    public void NativeBackupIsReadAndExtendedByManaged()
    {
        using var dir = new TempDir("bk-n2m");
        string dbPath = Path.Combine(dir.Path, "db"), backupDir = Path.Combine(dir.Path, "backup");
        string restore1 = Path.Combine(dir.Path, "r1"), restore2 = Path.Combine(dir.Path, "r2");
        using (var db = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions().SetCreateIfMissing(true), dbPath))
        {
            WriteNative(db, 0, 300, "a");
            db.Flush(new RocksDbSharp.FlushOptions());
            WriteNative(db, 200, 500, "b");
            db.Flush(new RocksDbSharp.FlushOptions());
            WriteNative(db, 450, 600, "c"); // only in the WAL
            using var nbe = new NativeBackupEngine(backupDir);
            nbe.CreateNewBackup(db, flush: false);
        }
        AssertNativeMetaIsNotEmpty(backupDir, 1);
        Func<int, string> v1 = i => i < 200 ? V(i, "a") : i < 450 ? V(i, "b") : V(i, "c");

        using (BackupEngine be = BackupEngine.Open(new BackupEngineOptions(backupDir)))
        {
            List<BackupInfo> infos = be.GetBackupInfo(includeFileDetails: true);
            Assert.AreEqual(1, infos.Count);
            Assert.AreEqual(1u, infos[0].BackupId);
            Assert.IsTrue(infos[0].FileDetails!.Count >= 5); // 2 SSTs, MANIFEST, CURRENT, OPTIONS, WAL
            be.VerifyBackup(1, verifyWithChecksum: true);
            be.RestoreDbFromLatestBackup(restore1, restore1, new RestoreOptions());
        }
        CheckManaged(restore1, 600, v1);

        // The fully flushed native DB, backed up by managed: SST files are shared (same names and checksums).
        using (var db = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions(), dbPath))
        {
            db.Flush(new RocksDbSharp.FlushOptions());
            using var nbe = new NativeBackupEngine(backupDir);
            nbe.CreateNewBackup(db, flush: true); // backup 2: native, fully flushed
        }
        using (RocksDatabase mdb = RocksDatabase.Open(dbPath, new DbOptions()))
        using (BackupEngine be = BackupEngine.Open(new BackupEngineOptions(backupDir)))
        {
            Assert.AreEqual(3u, be.CreateNewBackup(mdb, flushBeforeBackup: false));
            WriteManaged(mdb, 0, 100, "d");
            Assert.AreEqual(4u, be.CreateNewBackupWithMetadata(mdb, "managed-app-meta", new CreateBackupOptions { FlushBeforeBackup = true }));
            be.VerifyBackup(4, verifyWithChecksum: true);
        }
        SortedSet<string> nativeLines = SstLines(backupDir, 2), managedLines = SstLines(backupDir, 3);
        Assert.IsTrue(nativeLines.Count >= 1);
        CollectionAssert.AreEqual(nativeLines.ToList(), managedLines.ToList(), "managed names and checksums of the same SST files differ from native");
        // (Native also lists the empty live WAL here, the managed live-file list does not; both restore the same data.)
        CollectionAssert.AreEqual(PrivateKinds(backupDir, 2).FindAll(static k => k != "log"), PrivateKinds(backupDir, 3).FindAll(static k => k != "log"));
        string[] meta3 = File.ReadAllLines(Path.Combine(backupDir, "meta", "3"));
        Assert.IsTrue(long.Parse(meta3[0]) > 1_600_000_000, "timestamp line");
        Assert.AreEqual(int.Parse(meta3[2]), meta3.Length - 3, "file count line");
        StringAssert.StartsWith(File.ReadAllLines(Path.Combine(backupDir, "meta", "4"))[2], "metadata " + Convert.ToHexString(U("managed-app-meta")));

        // Native restores the managed incremental backup and lists all four.
        using (var nbe = new NativeBackupEngine(backupDir))
        {
            List<(uint Id, uint Files, ulong Size)> info = nbe.Info();
            CollectionAssert.AreEqual(new uint[] { 1, 2, 3, 4 }, info.ConvertAll(static x => x.Id));
            nbe.Verify(3);
            nbe.Verify(4);
            nbe.RestoreLatest(restore2);
        }
        CheckNative(restore2, 600, i => i < 100 ? V(i, "d") : v1(i));
    }

    [TestMethod]
    public void ManagedBackupIsReadAndExtendedByNative()
    {
        using var dir = new TempDir("bk-m2n");
        string dbPath = Path.Combine(dir.Path, "db"), backupDir = Path.Combine(dir.Path, "backup");
        string restore1 = Path.Combine(dir.Path, "r1"), restore2 = Path.Combine(dir.Path, "r2");
        using (RocksDatabase db = RocksDatabase.Open(dbPath, new DbOptions { CreateIfMissing = true }))
        using (BackupEngine be = BackupEngine.Open(new BackupEngineOptions(backupDir)))
        {
            WriteManaged(db, 0, 400, "a");
            db.Flush();
            WriteManaged(db, 300, 500, "b"); // WAL only
            Assert.AreEqual(1u, be.CreateNewBackupWithMetadata(db, "hello", new CreateBackupOptions()));
            WriteManaged(db, 0, 50, "c");
            Assert.AreEqual(2u, be.CreateNewBackupAsync(db, flushBeforeBackup: true).AsTask().GetAwaiter().GetResult());
        }
        using (var nbe = new NativeBackupEngine(backupDir))
        {
            Assert.AreEqual(2, nbe.Info().Count);
            nbe.Verify(1);
            nbe.Verify(2);
            nbe.Restore(1, restore1);
        }
        CheckNative(restore1, 500, i => i < 300 ? V(i, "a") : V(i, "b"));

        // Native adds an incremental backup of the native-restored DB to the managed backup directory.
        using (var db = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions(), restore1))
        {
            WriteNative(db, 450, 520, "n");
            using var nbe = new NativeBackupEngine(backupDir);
            nbe.CreateNewBackup(db, flush: true);
            nbe.Purge(2); // drops backup 1
        }
        AssertNativeMetaIsNotEmpty(backupDir, 3);
        using (BackupEngine be = BackupEngine.Open(new BackupEngineOptions(backupDir)))
        {
            List<BackupInfo> infos = be.GetBackupInfo();
            CollectionAssert.AreEqual(new uint[] { 2, 3 }, infos.ConvertAll(static x => x.BackupId));
            Assert.AreEqual(0, be.GetCorruptedBackups().Count);
            be.VerifyBackup(3, verifyWithChecksum: true);
            be.RestoreDbFromLatestBackup(restore2, restore2, new RestoreOptions());
            be.PurgeOldBackups(1);
            be.VerifyBackup(3, verifyWithChecksum: true);
        }
        CheckManaged(restore2, 520, i => i < 300 ? V(i, "a") : i < 450 ? V(i, "b") : V(i, "n"));
        // Nothing unreferenced is left behind.
        using (var nbe = new NativeBackupEngine(backupDir))
        {
            Assert.AreEqual(1, nbe.Info().Count);
            nbe.Verify(3);
        }
    }
}
