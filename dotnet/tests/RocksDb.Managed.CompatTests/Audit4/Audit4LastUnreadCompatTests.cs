using RocksDb.Managed.CompatTests.Infrastructure;

namespace RocksDb.Managed.CompatTests.Audit4;

/// <summary>Audit #4, slice "LastUnread": RepairDB (db/repair.cc) against native librocksdb.</summary>
[TestClass]
public sealed class Audit4LastUnreadCompatTests
{
    private static readonly List<object> s_keepAlive = new();

    private static void DeleteManifests(string dir)
    {
        foreach (string f in Directory.GetFiles(dir))
        {
            string n = Path.GetFileName(f);
            if (n.StartsWith("MANIFEST-", StringComparison.Ordinal) || n == "CURRENT") File.Delete(f);
        }
    }

    private static void CopyDirectory(string from, string to)
    {
        Directory.CreateDirectory(to);
        foreach (string f in Directory.GetFiles(from)) File.Copy(f, Path.Combine(to, Path.GetFileName(f)));
    }

    // Repairer::ScanTable: a table whose properties carry no column family (written by SstFileWriter without a column
    // family handle: id kUnknownColumnFamily, name "") is assigned to column family 0, fails
    // "cfd->GetName() != props->column_family_name" and is archived to lost/. Both implementations must agree.
    [TestMethod]
    public void NativeRepairArchivesTableWithoutColumnFamilyName()
    {
        using var dir = new TempDir("repair-unnamed");
        string nativeDb = Path.Combine(dir.Path, "native");
        string managedDb = Path.Combine(dir.Path, "managed");
        using (RocksDbSharp.RocksDb n = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions().SetCreateIfMissing(true), nativeDb))
        {
            n.Put("a", "1");
            n.Flush(new RocksDbSharp.FlushOptions());
        }
        var cfo = new RocksDbSharp.ColumnFamilyOptions();
        s_keepAlive.Add(cfo);
        using (var w = new RocksDbSharp.SstFileWriter(new RocksDbSharp.EnvOptions(), cfo))
        {
            w.Open(Path.Combine(nativeDb, "000900.sst"));
            w.Put("external"u8.ToArray(), "x"u8.ToArray());
            w.Finish();
        }
        DeleteManifests(nativeDb);
        CopyDirectory(nativeDb, managedDb);

        RocksDbSharp.Native.Instance.rocksdb_repair_db(new RocksDbSharp.DbOptions().Handle, nativeDb, out IntPtr err);
        Assert.AreEqual(IntPtr.Zero, err);
        RocksDatabase.Repair(managedDb, new DbOptions());

        bool nativeArchived = File.Exists(Path.Combine(nativeDb, "lost", "000900.sst"));
        Assert.IsTrue(nativeArchived, "native RepairDB archives the table");
        Assert.AreEqual(nativeArchived, File.Exists(Path.Combine(managedDb, "lost", "000900.sst")), "managed repair archives it too");
        using (RocksDbSharp.RocksDb n = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions(), nativeDb))
        {
            Assert.AreEqual("1", n.Get("a"));
            Assert.IsNull(n.Get("external"));
        }
        using RocksDatabase m = RocksDatabase.Open(managedDb, new DbOptions());
        Assert.IsNotNull(m.Get("a"u8));
        Assert.IsNull(m.Get("external"u8));
    }
}
