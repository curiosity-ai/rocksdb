using RocksDb.Managed.Env;

namespace RocksDb.Managed.Tests.Env;

/// <summary>Crash simulation of <see cref="MemoryFileSystem"/> (FaultInjectionTestFS-lite).</summary>
[TestClass]
public sealed class MemoryFileSystemTests
{
    private const string Dir = "/db";

    private static MemoryFileSystem NewFs()
    {
        var fs = new MemoryFileSystem();
        fs.CreateDirectoryIfMissing(Dir);
        fs.SyncDirectory("/");
        return fs;
    }

    [TestMethod]
    public void UnsyncedDataIsDroppedOnCrash()
    {
        MemoryFileSystem fs = NewFs();
        using (IWritableFile w = fs.CreateWritable("/db/000001.log"))
        {
            w.Append("synced"u8);
            w.Sync();
            w.Append("-lost"u8);
            w.Flush();
        }
        fs.SyncDirectory(Dir);
        CollectionAssert.AreEqual("synced-lost"u8.ToArray(), fs.ReadAllBytes("/db/000001.log"));
        fs.SimulateCrash();
        CollectionAssert.AreEqual("synced"u8.ToArray(), fs.ReadAllBytes("/db/000001.log"));
    }

    [TestMethod]
    public void FilesCreatedAfterLastDirSyncAreLost()
    {
        MemoryFileSystem fs = NewFs();
        using (IWritableFile w = fs.CreateWritable("/db/a")) { w.Append("a"u8); w.Sync(); }
        fs.SyncDirectory(Dir);
        using (IWritableFile w = fs.CreateWritable("/db/b")) { w.Append("b"u8); w.Sync(); }
        fs.SimulateCrash();
        Assert.IsTrue(fs.FileExists("/db/a"));
        Assert.IsFalse(fs.FileExists("/db/b"));
    }

    [TestMethod]
    public void UnsyncedRenameAndDeleteAreReverted()
    {
        MemoryFileSystem fs = NewFs();
        using (IWritableFile w = fs.CreateWritable("/db/CURRENT")) { w.Append("MANIFEST-1\n"u8); w.Sync(); }
        using (IWritableFile w = fs.CreateWritable("/db/old.sst")) { w.Append("x"u8); w.Sync(); }
        fs.SyncDirectory(Dir);

        // SetCurrentFile pattern without the final directory fsync.
        using (IWritableFile w = fs.CreateWritable("/db/000002.dbtmp")) { w.Append("MANIFEST-2\n"u8); w.Sync(); }
        fs.RenameFile("/db/000002.dbtmp", "/db/CURRENT");
        fs.DeleteFile("/db/old.sst");
        CollectionAssert.AreEqual("MANIFEST-2\n"u8.ToArray(), fs.ReadAllBytes("/db/CURRENT"));

        fs.SimulateCrash();
        CollectionAssert.AreEqual("MANIFEST-1\n"u8.ToArray(), fs.ReadAllBytes("/db/CURRENT"));
        Assert.IsTrue(fs.FileExists("/db/old.sst"));
        Assert.IsFalse(fs.FileExists("/db/000002.dbtmp"));

        // With the directory fsync the rename survives.
        using (IWritableFile w = fs.CreateWritable("/db/000003.dbtmp")) { w.Append("MANIFEST-3\n"u8); w.Sync(); }
        fs.RenameFile("/db/000003.dbtmp", "/db/CURRENT");
        fs.SyncDirectory(Dir);
        fs.SimulateCrash();
        CollectionAssert.AreEqual("MANIFEST-3\n"u8.ToArray(), fs.ReadAllBytes("/db/CURRENT"));
    }

    [TestMethod]
    public void InactiveFilesystemFailsWrites()
    {
        MemoryFileSystem fs = NewFs();
        using IWritableFile w = fs.CreateWritable("/db/x");
        w.Append("ok"u8);
        fs.SetFilesystemActive(false);
        var ex = Assert.ThrowsExactly<RocksDbException>(() => w.Append("no"u8));
        Assert.AreEqual(StatusCode.IOError, ex.Code);
        ex = Assert.ThrowsExactly<RocksDbException>(() => w.Sync());
        Assert.AreEqual(StatusCode.IOError, ex.Code);
        ex = Assert.ThrowsExactly<RocksDbException>(() => fs.CreateWritable("/db/y"));
        Assert.AreEqual(StatusCode.IOError, ex.Code);
        fs.SetFilesystemActive(true);
        w.Append("!"u8);
        CollectionAssert.AreEqual("ok!"u8.ToArray(), fs.ReadAllBytes("/db/x"));
    }

    [TestMethod]
    public void CreateInMissingDirectoryFails()
    {
        var fs = new MemoryFileSystem();
        var ex = Assert.ThrowsExactly<RocksDbException>(() => fs.CreateWritable("/nope/file"));
        Assert.AreEqual(StatusSubCode.PathNotFound, ex.SubCode);
    }
}
