using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Db;

namespace RocksDb.Managed.CompatTests.Audit4;

/// <summary>
/// Audit #4, slice VersionRecovery: DB open edge cases compared with native RocksDB (DBImpl::Recover,
/// DBImpl::MaybeUpdateNextFileNumber, OpenForReadOnlyCheckExistence). Each engine works on its own directory; the
/// outcomes (status code, file numbering) must agree.
/// </summary>
[TestClass]
public sealed class Audit4VersionRecoveryCompatTests
{
    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);

    private static RocksDbSharp.DbOptions NativeOptions(bool create) => new RocksDbSharp.DbOptions().SetCreateIfMissing(create);

    private static ulong MaxTableFileNumber(string path)
    {
        ulong max = 0;
        foreach (string f in Directory.GetFiles(path, "*.sst"))
        {
            Assert.IsTrue(FileNames.TryParseFileName(Path.GetFileName(f), out ulong n, out _));
            max = Math.Max(max, n);
        }
        return max;
    }

    [TestMethod]
    public void NewDbWithStrayWalFileFailsLikeNative()
    {
        // DBImpl::Recover: "While creating a new Db, wal_dir contains existing log file: ".
        using var nativeDir = new TempDir("a4vr-n");
        using var managedDir = new TempDir("a4vr-m");
        File.WriteAllBytes(Path.Combine(nativeDir.Path, "000005.log"), []);
        File.WriteAllBytes(Path.Combine(managedDir.Path, "000005.log"), []);
        var ne = Assert.ThrowsExactly<RocksDbSharp.RocksDbException>(() => RocksDbSharp.RocksDb.Open(NativeOptions(true), nativeDir.Path).Dispose());
        StringAssert.StartsWith(ne.Message, "Corruption: While creating a new Db, wal_dir contains existing log file");
        var me = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true }).Dispose());
        Assert.AreEqual(StatusCode.Corruption, me.Code);
        StringAssert.Contains(me.Message, "While creating a new Db, wal_dir contains existing log file");
    }

    [TestMethod]
    public void FileNumbersContinueAfterOrphanFilesLikeNative()
    {
        // DBImpl::MaybeUpdateNextFileNumber: a file numbered at or above next_file_number advances the counter.
        using var nativeDir = new TempDir("a4vr-n");
        using var managedDir = new TempDir("a4vr-m");
        var flush = new RocksDbSharp.FlushOptions();
        using (var db = RocksDbSharp.RocksDb.Open(NativeOptions(true), nativeDir.Path))
        {
            db.Put(U("a"), U("1"));
            db.Flush(flush);
        }
        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true }))
        {
            db.Put(U("a"), U("1"));
            db.Flush();
        }
        File.WriteAllBytes(Path.Combine(nativeDir.Path, "000100.sst"), U("orphan"));
        File.WriteAllBytes(Path.Combine(managedDir.Path, "000100.sst"), U("orphan"));
        using (var db = RocksDbSharp.RocksDb.Open(NativeOptions(false), nativeDir.Path))
        {
            db.Put(U("b"), U("2"));
            db.Flush(flush);
            Assert.IsTrue(MaxTableFileNumber(nativeDir.Path) > 100, "native " + MaxTableFileNumber(nativeDir.Path));
        }
        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions()))
        {
            db.Put(U("b"), U("2"));
            db.Flush();
            Assert.IsTrue(MaxTableFileNumber(managedDir.Path) > 100, "managed " + MaxTableFileNumber(managedDir.Path));
        }
        Assert.IsFalse(File.Exists(Path.Combine(nativeDir.Path, "000100.sst")));
        Assert.IsFalse(File.Exists(Path.Combine(managedDir.Path, "000100.sst")));
    }

    [TestMethod]
    public void ReadOnlyErrorIfWalFileExistsLikeNative()
    {
        // DBImpl::Recover(read_only, error_if_wal_file_exists): only a non-empty WAL is an error, and it is Corruption.
        // (The overload without column families ignores the flag upstream, so the native side passes them.)
        using var nativeDir = new TempDir("a4vr-n");
        using var managedDir = new TempDir("a4vr-m");
        IReadOnlyList<ColumnFamilyDescriptor> cfs = [new ColumnFamilyDescriptor("default", new ColumnFamilyOptions())];
        using (var db = RocksDbSharp.RocksDb.Open(NativeOptions(true), nativeDir.Path))
        {
            db.Put(U("a"), U("1"));
            db.Flush(new RocksDbSharp.FlushOptions());
        }
        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true }))
        {
            db.Put(U("a"), U("1"));
            db.Flush();
        }
        // Only empty WAL files: both open.
        using (var ro = RocksDbSharp.RocksDb.OpenReadOnly(NativeOptions(false), nativeDir.Path, new RocksDbSharp.ColumnFamilies(), true))
        {
            CollectionAssert.AreEqual(U("1"), ro.Get(U("a")));
        }
        using (RocksDatabase ro = RocksDatabase.OpenReadOnly(managedDir.Path, new DbOptions(), cfs, errorIfWalFileExists: true))
        {
            CollectionAssert.AreEqual(U("1"), ro.Get(U("a")));
        }
        using (var db = RocksDbSharp.RocksDb.Open(NativeOptions(false), nativeDir.Path)) db.Put(U("b"), U("2"));
        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions())) db.Put(U("b"), U("2"));
        var ne = Assert.ThrowsExactly<RocksDbSharp.RocksDbException>(() => RocksDbSharp.RocksDb.OpenReadOnly(NativeOptions(false), nativeDir.Path, new RocksDbSharp.ColumnFamilies(), true).Dispose());
        StringAssert.StartsWith(ne.Message, "Corruption: ");
        var me = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.OpenReadOnly(managedDir.Path, new DbOptions(), cfs, errorIfWalFileExists: true).Dispose());
        Assert.AreEqual(StatusCode.Corruption, me.Code);
    }

    [TestMethod]
    public void ReadOnlyOpenOfMissingDbFailsLikeNative()
    {
        // OpenForReadOnlyCheckExistence -> GetCurrentManifestPath: the IOError of reading CURRENT.
        using var nativeDir = new TempDir("a4vr-n");
        using var managedDir = new TempDir("a4vr-m");
        IReadOnlyList<ColumnFamilyDescriptor> cfs = [new ColumnFamilyDescriptor("default", new ColumnFamilyOptions())];
        var ne = Assert.ThrowsExactly<RocksDbSharp.RocksDbException>(() => RocksDbSharp.RocksDb.OpenReadOnly(NativeOptions(false), nativeDir.Path, false).Dispose());
        StringAssert.StartsWith(ne.Message, "IO error: ");
        var me = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.OpenReadOnly(managedDir.Path, new DbOptions(), cfs, errorIfWalFileExists: false).Dispose());
        Assert.AreEqual(StatusCode.IOError, me.Code);
        Assert.AreEqual(StatusSubCode.PathNotFound, me.SubCode);
    }
}
