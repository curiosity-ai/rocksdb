using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>DestroyDB (db/db_impl/db_impl.cc) and CancelAllBackgroundWork / OnDBShutdownBegin.</summary>
[TestClass]
public sealed class DestroyAndShutdownTests
{
    private static void FillAndFlush(RocksDatabase db, int n)
    {
        for (int i = 0; i < n; i++) db.Put(Key(i), Value(i, 0));
        db.Flush();
        for (int i = 0; i < n; i++) db.Put(Key(i), Value(i, 1)); // leaves a WAL with data
    }

    [TestMethod]
    public void DestroyDeletesEveryDbFileAndTheEmptyDirectory()
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs))) FillAndFlush(db, 100);
        Assert.IsTrue(fs.GetChildren(DbPath).Any(static f => f.EndsWith(".sst", StringComparison.Ordinal)));
        RocksDatabase.Destroy(DbPath, new DbOptions { FileSystem = fs });
        Assert.IsFalse(fs.DirectoryExists(DbPath));
        // Like upstream (whose info logger creates the directory), destroying a missing DB succeeds and leaves nothing.
        RocksDatabase.Destroy(DbPath, new DbOptions { FileSystem = fs });
        Assert.IsFalse(fs.DirectoryExists(DbPath));
    }

    [TestMethod]
    public void DestroyKeepsForeignFilesAndTheirDirectory()
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs))) FillAndFlush(db, 10);
        using (IWritableFile f = fs.CreateWritable(DbPath + "/notes.txt")) f.Append("mine"u8);
        RocksDatabase.Destroy(DbPath, new DbOptions { FileSystem = fs });
        CollectionAssert.AreEqual(new[] { "notes.txt" }, fs.GetChildren(DbPath).ToArray());
    }

    [TestMethod]
    public void DestroyOfAnOpenDbFailsWithoutDeletingAnything()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        FillAndFlush(db, 10);
        string[] before = [.. fs.GetChildren(DbPath)];
        var e = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Destroy(DbPath, new DbOptions { FileSystem = fs }));
        Assert.AreEqual(StatusCode.IOError, e.Code);
        CollectionAssert.AreEquivalent(before, fs.GetChildren(DbPath).ToArray());
        Assert.AreEqual("value-3-1xxx", GetString(db, "key000003"));
    }

    [TestMethod]
    public async Task DestroyAsyncRemovesWalDirArchiveAndDbPaths()
    {
        MemoryFileSystem fs = NewFs();
        fs.CreateDirectoryIfMissing("/wal");
        fs.CreateDirectoryIfMissing("/data2");
        DbOptions Options()
        {
            var o = new DbOptions { CreateIfMissing = true, FileSystem = fs, WalDir = "/wal", WalTtlSeconds = 3600 };
            o.DbPaths.Add((DbPath, 1));
            o.DbPaths.Add(("/data2", 1UL << 40));
            return o;
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, Options()))
        {
            for (int round = 0; round < 3; round++)
            {
                for (int i = 0; i < 200; i++) db.Put(Key(i), Value(i, round));
                db.Flush();
            }
            db.CompactRange(null, null);
        }
        // Table and blob files (and trash) of the extra paths are deleted; the files are not parsed, only named.
        foreach (string name in new[] { "000777.sst", "000778.blob", "000779.sst.trash" })
        {
            using IWritableFile f = fs.CreateWritable("/data2/" + name);
            f.Append("x"u8);
        }
        Assert.IsTrue(fs.DirectoryExists("/wal/archive") && fs.GetChildren("/wal/archive").Any(), "archived WALs");
        await RocksDatabase.DestroyAsync(DbPath, Options(), CancellationToken.None);
        Assert.IsFalse(fs.DirectoryExists(DbPath));
        Assert.IsFalse(fs.DirectoryExists("/data2"));
        Assert.IsFalse(fs.DirectoryExists("/wal/archive"));
        // Like upstream (directories removed in std::set order, "/wal" before "/wal/archive"), the WAL dir stays, empty.
        Assert.IsTrue(fs.DirectoryExists("/wal"));
        Assert.AreEqual(0, fs.GetChildren("/wal").Count());
    }

    private sealed class ShutdownListener : EventListener
    {
        public int Count;
        public RocksDatabase? LastDb;
        public bool LastWasNull;

        public override void OnDBShutdownBegin(RocksDatabase? db)
        {
            Interlocked.Increment(ref Count);
            LastDb = db;
            LastWasNull = db is null;
        }
    }

    /// <summary>listener_test.cc OnDBShutdownBeginOnceForCancelAndClose.</summary>
    [TestMethod]
    public void OnDbShutdownBeginOnceForCancelAndClose()
    {
        MemoryFileSystem fs = NewFs();
        var listener = new ShutdownListener();
        DbOptions o = NewOptions(fs);
        o.Listeners.Add(listener);
        RocksDatabase db = RocksDatabase.Open(DbPath, o);
        db.Put(B("a"), B("1"));
        db.CancelAllBackgroundWork(wait: false);
        Assert.AreEqual(1, listener.Count);
        Assert.AreSame(db, listener.LastDb);
        db.CancelAllBackgroundWork(wait: true);
        db.Dispose();
        Assert.AreEqual(1, listener.Count);

        // Close alone notifies once too.
        db = RocksDatabase.Open(DbPath, o);
        db.Dispose();
        Assert.AreEqual(2, listener.Count);
    }

    /// <summary>listener_test.cc OnDBShutdownBeginOnFailedOpen: the cleanup of a failed open notifies (with no DB object).</summary>
    [TestMethod]
    public void OnDbShutdownBeginOnFailedOpen()
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs))) db.Put(B("a"), B("1"));
        var listener = new ShutdownListener();
        // Opening with a different comparator fails after the DB object was created (MANIFEST recovery).
        var o = new DbOptions { FileSystem = fs };
        o.Listeners.Add(listener);
        var e = Assert.ThrowsExactly<RocksDbException>(() =>
            RocksDatabase.Open(DbPath, o, new ColumnFamilyOptions { Comparator = Comparator.ReverseBytewise }).Dispose());
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        Assert.AreEqual(1, listener.Count);
        Assert.IsTrue(listener.LastWasNull);
    }

    /// <summary>
    /// CancelAllBackgroundWork flushes data that is not in the WAL first, then stops background work: Flush fails, reads
    /// and WAL writes keep working, and the data survives the close.
    /// </summary>
    [TestMethod]
    public async Task CancelAllBackgroundWorkFlushesThenStopsBackgroundWork()
    {
        MemoryFileSystem fs = NewFs();
        RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        db.Put(new WriteOptions { DisableWal = true }, B("nowal"), B("1"));
        await db.CancelAllBackgroundWorkAsync(wait: true);
        Assert.AreEqual(1, fs.GetChildren(DbPath).Count(static f => f.EndsWith(".sst", StringComparison.Ordinal)), "the shutdown flush");
        db.Put(B("after"), B("2"));
        Assert.AreEqual("2", GetString(db, "after"));
        bool flushFailed = false;
        try
        {
            db.Flush();
        }
        catch (ObjectDisposedException)
        {
            flushFailed = true;
        }
        catch (RocksDbException ex) when (ex.Code == StatusCode.ShutdownInProgress)
        {
            flushFailed = true;
        }
        Assert.IsTrue(flushFailed, "no flush after CancelAllBackgroundWork");
        await db.DisposeAsync();
        Assert.AreEqual(1, fs.GetChildren(DbPath).Count(static f => f.EndsWith(".sst", StringComparison.Ordinal)), "close does not flush again");
        using (db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            Assert.AreEqual("1", GetString(db, "nowal"));
            Assert.AreEqual("2", GetString(db, "after"));
        }
    }

    [TestMethod]
    public void DestroyGoesThroughTheSstFileManagerTrash()
    {
        MemoryFileSystem fs = NewFs();
        using var sfm = new SstFileManager(fs, rateBytesPerSecond: 1L << 30, maxTrashDbRatio: 1000, bytesMaxDeleteChunk: 0);
        using (RocksDatabase db = RocksDatabase.Open(DbPath, new DbOptions { CreateIfMissing = true, FileSystem = fs, SstFileManager = sfm })) FillAndFlush(db, 100);
        // A trash file left by an earlier crash is deleted too.
        using (IWritableFile f = fs.CreateWritable(DbPath + "/000999.sst.trash")) f.Append("x"u8);
        RocksDatabase.Destroy(DbPath, new DbOptions { FileSystem = fs, SstFileManager = sfm });
        Assert.IsFalse(fs.DirectoryExists(DbPath));
        Assert.AreEqual(0UL, sfm.TotalTrashSize);
    }
}
