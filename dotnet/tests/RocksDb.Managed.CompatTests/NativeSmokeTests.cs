using RocksDb.Managed.CompatTests.Infrastructure;

namespace RocksDb.Managed.CompatTests;

/// <summary>Verifies the native reference implementation (rocksdb-sharp + librocksdb) loads and works.</summary>
[TestClass]
public sealed class NativeSmokeTests
{
    [TestMethod]
    public void NativeRocksDbPutGet()
    {
        using var dir = new TempDir("smoke");
        var options = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        using (var db = RocksDbSharp.RocksDb.Open(options, dir.Path))
        {
            db.Put("key"u8, "value"u8);
            CollectionAssert.AreEqual("value"u8.ToArray(), db.Get("key"u8));
        }
        Assert.IsTrue(File.Exists(System.IO.Path.Combine(dir.Path, "CURRENT")));
    }
}
