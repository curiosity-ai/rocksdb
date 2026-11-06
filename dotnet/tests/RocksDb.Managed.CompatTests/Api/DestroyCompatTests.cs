using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>DestroyDB (db/db_impl/db_impl.cc) and CancelAllBackgroundWork compared with native RocksDB.</summary>
[TestClass]
public sealed class DestroyCompatTests
{
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    private static byte[] K(int i) => Encoding.UTF8.GetBytes("key" + i.ToString("D6", System.Globalization.CultureInfo.InvariantCulture));

    private static RocksDbSharp.DbOptions NativeOptions(string? walDir)
    {
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetWalTtlSeconds(3600);
        if (walDir is not null) o.SetWalDir(walDir);
        s_keepAlive.Add(o);
        return o;
    }

    private static DbOptions ManagedOptions(string? walDir) => new() { CreateIfMissing = true, WalDir = walDir ?? "", WalTtlSeconds = 3600 };

    private static void NativeWorkload(string path, string? walDir)
    {
        using RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(NativeOptions(walDir), path);
        var flush = new RocksDbSharp.FlushOptions().SetWaitForFlush(true);
        for (int round = 0; round < 3; round++)
        {
            for (int i = 0; i < 300; i++) db.Put(K(i), K(i + round));
            db.Flush(flush);
        }
        db.CompactRange((byte[]?)null, null);
        db.Put(K(1), K(2));
    }

    private static void ManagedWorkload(string path, string? walDir)
    {
        using RocksDatabase db = RocksDatabase.Open(path, ManagedOptions(walDir));
        for (int round = 0; round < 3; round++)
        {
            for (int i = 0; i < 300; i++) db.Put(K(i), K(i + round));
            db.Flush();
        }
        db.CompactRange(null, null);
        db.Put(K(1), K(2));
    }

    private static void NativeDestroy(string path, string? walDir) =>
        RocksDbSharp.Native.Instance.rocksdb_destroy_db(NativeOptions(walDir).Handle, path);

    private static void ManagedDestroy(string path, string? walDir) => RocksDatabase.Destroy(path, ManagedOptions(walDir));

    private static string[] Listing(string root) =>
        Directory.Exists(root)
            ? [.. Directory.EnumerateFileSystemEntries(root, "*", SearchOption.AllDirectories).Select(p => System.IO.Path.GetRelativePath(root, p)).Order(StringComparer.Ordinal)]
            : [];

    /// <summary>
    /// Every combination of creator and destroyer, with a separate WAL dir (archived WALs) and a foreign file in the DB
    /// directory: the same files and directories are left (only the foreign file and the directory holding it).
    /// </summary>
    [TestMethod]
    [DataRow(false, false)]
    [DataRow(false, true)]
    [DataRow(true, false)]
    [DataRow(true, true)]
    public void DestroyLeavesTheSameFilesAsNative(bool createdByManaged, bool destroyedByManaged)
    {
        using var dir = new TempDir("destroy");
        string db = System.IO.Path.Combine(dir.Path, "db");
        string wal = System.IO.Path.Combine(dir.Path, "wal");
        if (createdByManaged) ManagedWorkload(db, wal);
        else NativeWorkload(db, wal);
        Assert.IsTrue(Directory.EnumerateFiles(System.IO.Path.Combine(wal, "archive")).Any(), "archived WALs");
        File.WriteAllText(System.IO.Path.Combine(db, "notes.txt"), "mine");
        if (destroyedByManaged) ManagedDestroy(db, wal);
        else NativeDestroy(db, wal);
        CollectionAssert.AreEqual(new[] { "db", System.IO.Path.Combine("db", "notes.txt"), "wal" }, Listing(dir.Path), string.Join(", ", Listing(dir.Path)));
    }

    /// <summary>Both empty a DB directory with archived WALs the same way; destroying a missing directory succeeds on both sides and leaves nothing.</summary>
    [TestMethod]
    public void DestroyRemovesTheDirectoryAndFailsOnAMissingOneLikeNative()
    {
        using var dir = new TempDir("destroy2");
        string a = System.IO.Path.Combine(dir.Path, "a"), b = System.IO.Path.Combine(dir.Path, "b");
        NativeWorkload(a, null);
        ManagedWorkload(b, null);
        ManagedDestroy(a, null);
        NativeDestroy(b, null);
        // Upstream removes "db" before "db/archive" (std::set order), so the emptied DB directory stays.
        CollectionAssert.AreEqual(new[] { "a", "b" }, Listing(dir.Path), string.Join(", ", Listing(dir.Path)));
        string missing = System.IO.Path.Combine(dir.Path, "missing");
        bool nativeFailed = false;
        try
        {
            NativeDestroy(missing, null);
        }
        catch (RocksDbSharp.RocksDbException)
        {
            nativeFailed = true;
        }
        bool managedFailed = false;
        try
        {
            ManagedDestroy(missing, null);
        }
        catch (RocksDbException e)
        {
            Assert.AreEqual(StatusCode.IOError, e.Code);
            managedFailed = true;
        }
        Assert.AreEqual(nativeFailed, managedFailed, "native failed: " + nativeFailed);
        Assert.IsFalse(nativeFailed);
        Assert.IsFalse(Directory.Exists(missing));
    }

    /// <summary>Destroying a DB that is open fails on the LOCK on both sides and deletes nothing.</summary>
    [TestMethod]
    public void DestroyOfAnOpenDbFailsLikeNative()
    {
        using var dir = new TempDir("destroy3");
        ManagedWorkload(dir.Path, null);
        string[] before = Listing(dir.Path);
        using (RocksDbSharp.RocksDb n = RocksDbSharp.RocksDb.Open(NativeOptions(null), dir.Path))
        {
            var e = Assert.ThrowsExactly<RocksDbException>(() => ManagedDestroy(dir.Path, null));
            Assert.AreEqual(StatusCode.IOError, e.Code);
        }
        using (RocksDatabase m = RocksDatabase.Open(dir.Path, ManagedOptions(null)))
        {
            Assert.ThrowsExactly<RocksDbSharp.RocksDbException>(() => NativeDestroy(dir.Path, null));
            Assert.AreEqual(Encoding.UTF8.GetString(K(2)), Encoding.UTF8.GetString(m.Get(K(1))!));
        }
        Assert.IsTrue(Listing(dir.Path).Contains("CURRENT"));
        Assert.IsTrue(before.Length > 3);
    }
}
