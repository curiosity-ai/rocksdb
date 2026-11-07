using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>RepairDB (port of the spirit of db/repair_test.cc).</summary>
[TestClass]
public sealed class RepairTests
{
    private static void DeleteManifests(IFileSystem fs)
    {
        foreach (string f in fs.GetChildren(DbPath))
        {
            if (f.StartsWith("MANIFEST-", StringComparison.Ordinal) || f == "CURRENT") fs.DeleteFile(DbPath + "/" + f);
        }
    }

    private static ColumnFamilyDescriptor[] Cfs() =>
        [new("default", new ColumnFamilyOptions()), new("one", new ColumnFamilyOptions())];

    [TestMethod]
    public void LostManifestIsRebuiltFromTablesAndWals()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        o.CreateMissingColumnFamilies = true;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o, Cfs()))
        {
            ColumnFamily one = db.GetColumnFamily("one");
            for (int i = 0; i < 100; i++) db.Put(Key(i), Value(i, 0));
            db.Flush();
            for (int i = 50; i < 150; i++) db.Put(Key(i), Value(i, 1));
            db.Flush();
            for (int i = 0; i < 20; i++) one.Put(Key(i), Value(i, 7));
            one.Flush();
            db.Delete(Key(3));
            db.Put(Key(1000), B("in-wal")); // only in the WAL
        }
        DeleteManifests(fs);
        Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbPath, new DbOptions { FileSystem = fs }).Dispose());

        RocksDatabase.Repair(DbPath, new DbOptions { FileSystem = fs });

        using (RocksDatabase db = RocksDatabase.Open(DbPath, new DbOptions { FileSystem = fs }, Cfs()))
        {
            for (int i = 0; i < 150; i++)
            {
                string? expected = i == 3 ? null : S(Value(i, i >= 50 ? 1 : 0));
                Assert.AreEqual(expected, GetString(db, S(Key(i))!), "key " + i);
            }
            Assert.AreEqual("in-wal", GetString(db, S(Key(1000))!));
            Assert.AreEqual(S(Value(5, 7)), GetString(db.GetColumnFamily("one"), S(Key(5))!));
            Assert.IsTrue(db.GetLatestSequenceNumber() >= 222);
            db.Put(Key(2000), B("after"));
        }
        // The repaired DB keeps working across reopen.
        using (RocksDatabase db = RocksDatabase.Open(DbPath, new DbOptions { FileSystem = fs }, Cfs()))
        {
            Assert.AreEqual("after", GetString(db, S(Key(2000))!));
        }
    }

    [TestMethod]
    public void CorruptTableIsMovedToLost()
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            db.Put(B("a"), B("1"));
            db.Flush();
            db.Put(B("b"), B("2"));
            db.Flush();
        }
        var tables = new List<string>();
        foreach (string f in fs.GetChildren(DbPath)) if (f.EndsWith(".sst", StringComparison.Ordinal)) tables.Add(f);
        tables.Sort(StringComparer.Ordinal);
        Assert.AreEqual(2, tables.Count);
        // Truncate the first table: its footer is gone.
        string bad = DbPath + "/" + tables[0];
        byte[] data = new byte[10];
        using (IWritableFile w = fs.CreateWritable(bad)) w.Append(data);
        DeleteManifests(fs);

        RocksDatabase.Repair(DbPath, new DbOptions { FileSystem = fs });

        Assert.IsTrue(fs.FileExists(DbPath + "/lost/" + tables[0]));
        using RocksDatabase repaired = RocksDatabase.Open(DbPath, new DbOptions { FileSystem = fs });
        Assert.IsNull(repaired.Get(B("a")));
        Assert.AreEqual("2", S(repaired.Get(B("b"))));
    }

    [TestMethod]
    public void UnknownColumnFamilyTablesAreDroppedWithoutOptions()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        o.CreateMissingColumnFamilies = true;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o, Cfs()))
        {
            db.GetColumnFamily("one").Put(B("x"), B("1"));
            db.GetColumnFamily("one").Flush();
        }
        DeleteManifests(fs);
        // The table of "one" is ignored (moved to lost) when its column family is unknown and none may be created.
        RocksDatabase.Repair(DbPath, new DbOptions { FileSystem = fs }, [new ColumnFamilyDescriptor("default", new ColumnFamilyOptions())], null);
        CollectionAssert.AreEqual(new[] { "default" }, RocksDatabase.ListColumnFamilies(DbPath, fs));
    }
}
