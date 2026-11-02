using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Env;

namespace RocksDb.Managed.CompatTests.Env;

/// <summary>
/// The DB LOCK file taken by <see cref="LocalFileSystem.LockFile"/> must exclude native RocksDB
/// (PosixFileSystem::LockFile uses fcntl(F_SETLK)) and vice versa, even within one process.
/// </summary>
[TestClass]
public sealed class LockCompatTests
{
    [TestMethod]
    public void ManagedAndNativeLocksExcludeEachOther()
    {
        if (!OperatingSystem.IsLinux() && !OperatingSystem.IsWindows()) Assert.Inconclusive("LOCK interop is only implemented on Linux and Windows");
        using var dir = new TempDir("lock");
        string lockPath = Path.Combine(dir.Path, "LOCK");
        var options = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);

        using (RocksDbSharp.RocksDb.Open(options, dir.Path))
        {
            var ex = Assert.ThrowsExactly<RocksDbException>(() => LocalFileSystem.Instance.LockFile(lockPath));
            Assert.AreEqual(StatusCode.IOError, ex.Code);
        }

        using (LocalFileSystem.Instance.LockFile(lockPath))
        {
            Exception? nativeError = null;
            try
            {
                using RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(options, dir.Path);
            }
            catch (RocksDbSharp.RocksDbException e)
            {
                nativeError = e;
            }
            Assert.IsNotNull(nativeError, "native RocksDB opened a DB whose LOCK is held by the managed port");
            StringAssert.Contains(nativeError.Message, "lock", StringComparison.OrdinalIgnoreCase);
        }

        // Released: native can open again.
        using (RocksDbSharp.RocksDb.Open(options, dir.Path)) { }
    }
}
