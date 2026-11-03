using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>RepairDB (db/repair.cc) across implementations: a managed repair of a native DB opens natively, and vice versa.</summary>
[TestClass]
public sealed class RepairCompatTests
{
    private static void DeleteManifests(string dir)
    {
        foreach (string f in Directory.GetFiles(dir))
        {
            string n = Path.GetFileName(f);
            if (n.StartsWith("MANIFEST-", StringComparison.Ordinal) || n == "CURRENT") File.Delete(f);
        }
    }

    private static RocksDbSharp.RocksDb OpenNative(string path)
    {
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetCreateMissingColumnFamilies(true);
        return RocksDbSharp.RocksDb.Open(o, path, new RocksDbSharp.ColumnFamilies { { "one", new RocksDbSharp.ColumnFamilyOptions() } });
    }

    [TestMethod]
    public void ManagedRepairOfNativeDbOpensNatively()
    {
        using var dir = new TempDir("repair");
        using (RocksDbSharp.RocksDb n = OpenNative(dir.Path))
        {
            for (int i = 0; i < 200; i++) n.Put("k" + i, "v" + i);
            n.Flush(new RocksDbSharp.FlushOptions());
            n.Put("x", "1", n.GetColumnFamily("one"));
            RocksDbSharp.Native.Instance.rocksdb_flush_cf(n.Handle, new RocksDbSharp.FlushOptions().Handle, n.GetColumnFamily("one").Handle);
            n.Remove("k7");
            n.Put("wal-only", "w");
        }
        DeleteManifests(dir.Path);
        RocksDatabase.Repair(dir.Path, new DbOptions());
        using RocksDbSharp.RocksDb repaired = OpenNative(dir.Path);
        Assert.AreEqual("v5", repaired.Get("k5"));
        Assert.IsNull(repaired.Get("k7"));
        Assert.AreEqual("w", repaired.Get("wal-only"));
        Assert.AreEqual("1", repaired.Get("x", repaired.GetColumnFamily("one")));
    }

    [TestMethod]
    public void NativeRepairOfManagedDbOpensManaged()
    {
        using var dir = new TempDir("repair");
        var cfs = new[] { new ColumnFamilyDescriptor("default", new ColumnFamilyOptions()), new ColumnFamilyDescriptor("one", new ColumnFamilyOptions()) };
        using (RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions { CreateIfMissing = true, CreateMissingColumnFamilies = true }, cfs))
        {
            for (int i = 0; i < 200; i++) db.Put(Encoding.UTF8.GetBytes("k" + i), Encoding.UTF8.GetBytes("v" + i));
            db.Flush();
            db.GetColumnFamily("one").Put("x"u8, "1"u8);
            db.GetColumnFamily("one").Flush();
            db.Put("wal-only"u8, "w"u8);
        }
        DeleteManifests(dir.Path);
        var o = new RocksDbSharp.DbOptions();
        RocksDbSharp.Native.Instance.rocksdb_repair_db(o.Handle, dir.Path, out IntPtr err);
        Assert.AreEqual(IntPtr.Zero, err);
        using RocksDatabase repaired = RocksDatabase.Open(dir.Path, new DbOptions(), cfs);
        Assert.AreEqual("v5", Encoding.UTF8.GetString(repaired.Get("k5"u8)!));
        Assert.AreEqual("w", Encoding.UTF8.GetString(repaired.Get("wal-only"u8)!));
        Assert.AreEqual("1", Encoding.UTF8.GetString(repaired.GetColumnFamily("one").Get("x"u8)!));
    }
}
