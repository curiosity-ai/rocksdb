using System.Text;
using RocksDb.Managed.CompatTests.Api;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Utilities.Backup;

namespace RocksDb.Managed.CompatTests.Audit4;

/// <summary>
/// Audit #4, slice UtilitiesApi: BackupEngine error paths compared with native RocksDB (utilities/backup/
/// backup_engine.cc: BackupMeta::LoadFromFile with LineFileReader, AddBackupFileWorkItem / GetFileDbIdentities).
/// </summary>
[TestClass]
public sealed class Audit4UtilitiesApiCompatTests
{
    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);

    private static void CorruptFooterMagic(string dbPath)
    {
        string[] ssts = Directory.GetFiles(dbPath, "*.sst");
        Assert.AreEqual(1, ssts.Length);
        byte[] data = File.ReadAllBytes(ssts[0]);
        for (int i = data.Length - 8; i < data.Length; i++) data[i] ^= 0x5a;
        using FileStream f = new(ssts[0], FileMode.Open, FileAccess.Write, FileShare.ReadWrite | FileShare.Delete);
        f.Write(data);
    }

    [TestMethod]
    public void MetaFileWithoutFinalNewlineIsCorruptForBothEngines()
    {
        // LineFileReader::ReadLine does not return text after the last '\n', so BackupMeta::LoadFromFile misses the last
        // file entry and reports "Inconsistent number of files": the backup is skipped by Open, the older one stays.
        using var dir = new TempDir("a4ua-meta");
        string dbPath = Path.Combine(dir.Path, "db"), backup = Path.Combine(dir.Path, "b");
        using (RocksDatabase db = RocksDatabase.Open(dbPath, new DbOptions { CreateIfMissing = true }))
        using (BackupEngine be = BackupEngine.Open(new BackupEngineOptions(backup)))
        {
            for (int i = 0; i < 100; i++) db.Put(U("k" + i), U("a" + i));
            Assert.AreEqual(1u, be.CreateNewBackup(db, flushBeforeBackup: true));
            for (int i = 0; i < 100; i++) db.Put(U("k" + i), U("b" + i));
            Assert.AreEqual(2u, be.CreateNewBackup(db, flushBeforeBackup: true));
        }
        string meta = Path.Combine(backup, "meta", "2");
        using (var nbe = new NativeBackupEngine(backup))
        {
            Assert.AreEqual(2, nbe.Info().Count); // native reads the managed backups
        }
        byte[] bytes = File.ReadAllBytes(meta);
        Assert.AreEqual((byte)'\n', bytes[^1]);
        File.WriteAllBytes(meta, bytes[..^1]);
        using (var nbe = new NativeBackupEngine(backup))
        {
            List<(uint Id, uint Files, ulong Size)> info = nbe.Info();
            Assert.AreEqual(1, info.Count);
            Assert.AreEqual(1u, info[0].Id);
        }
        using (BackupEngineReadOnly be = BackupEngineReadOnly.Open(new BackupEngineOptions(backup)))
        {
            List<BackupInfo> info = be.GetBackupInfo();
            Assert.AreEqual(1, info.Count);
            Assert.AreEqual(1u, info[0].BackupId);
            CollectionAssert.AreEqual(new uint[] { 2 }, be.GetCorruptedBackups());
        }
    }

    [TestMethod]
    public void BackupOfAnUnreadableTableFileFailsForBothEngines()
    {
        // AddBackupFileWorkItem: GetFileDbIdentities (SstFileDumper) must be able to open the table to name the shared
        // file after its DB session id; a status other than NotFound fails CreateNewBackup.
        using var dir = new TempDir("a4ua-sst");
        string nativeDb = Path.Combine(dir.Path, "ndb"), managedDb = Path.Combine(dir.Path, "mdb");
        string nativeBackup = Path.Combine(dir.Path, "nb"), managedBackup = Path.Combine(dir.Path, "mb");
        using (var db = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions().SetCreateIfMissing(true), nativeDb))
        using (var nbe = new NativeBackupEngine(nativeBackup))
        {
            for (int i = 0; i < 100; i++) db.Put(U("k" + i), U("a" + i));
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            CorruptFooterMagic(nativeDb);
            var ne = Assert.ThrowsExactly<RocksDbSharp.RocksDbException>(() => nbe.CreateNewBackup(db, flush: false));
            StringAssert.StartsWith(ne.Message, "Corruption");
            Assert.AreEqual(0, nbe.Info().Count);
        }
        using (RocksDatabase db = RocksDatabase.Open(managedDb, new DbOptions { CreateIfMissing = true }))
        using (BackupEngine be = BackupEngine.Open(new BackupEngineOptions(managedBackup)))
        {
            for (int i = 0; i < 100; i++) db.Put(U("k" + i), U("a" + i));
            db.Flush();
            CorruptFooterMagic(managedDb);
            RocksDbException me = Assert.ThrowsExactly<RocksDbException>(() => be.CreateNewBackup(db, flushBeforeBackup: false));
            Assert.AreEqual(StatusCode.Corruption, me.Code);
            Assert.AreEqual(0, be.GetBackupInfo().Count);
        }
    }
}
