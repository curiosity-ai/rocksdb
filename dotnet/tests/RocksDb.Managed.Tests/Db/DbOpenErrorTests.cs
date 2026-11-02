using RocksDb.Managed.Env;

namespace RocksDb.Managed.Tests.Db;

/// <summary>Open-time error mapping on the real local file system (DBImpl::Recover / DB::Open semantics).</summary>
[TestClass]
public sealed class DbOpenErrorTests
{
    [TestMethod]
    public void OpenMissingDirectoryWithoutCreateIfMissingIsInvalidArgument()
    {
        string path = Path.Combine(Path.GetTempPath(), "rocksdb-managed-missing-" + Guid.NewGuid().ToString("N"));
        var ex = Assert.ThrowsExactly<RocksDbException>(() =>
            RocksDatabase.Open(path, new DbOptions { CreateIfMissing = false, FileSystem = LocalFileSystem.Instance }).Dispose());
        Assert.AreEqual(StatusCode.InvalidArgument, ex.Code, ex.Message);
        Assert.IsFalse(Directory.Exists(path), "open must not create the directory");
    }

    [TestMethod]
    public void OpenEmptyDirectoryWithoutCreateIfMissingIsInvalidArgument()
    {
        string path = Path.Combine(Path.GetTempPath(), "rocksdb-managed-empty-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        try
        {
            var ex = Assert.ThrowsExactly<RocksDbException>(() =>
                RocksDatabase.Open(path, new DbOptions { CreateIfMissing = false, FileSystem = LocalFileSystem.Instance }).Dispose());
            Assert.AreEqual(StatusCode.InvalidArgument, ex.Code, ex.Message);
        }
        finally
        {
            Directory.Delete(path, recursive: true);
        }
    }
}
