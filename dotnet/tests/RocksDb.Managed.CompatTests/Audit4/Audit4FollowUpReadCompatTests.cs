using System.Runtime.InteropServices;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Db;

namespace RocksDb.Managed.CompatTests.Audit4;

/// <summary>Audit #4 follow-up (read path and the suspicions of the first pass): differential tests against native.</summary>
[TestClass]
public sealed unsafe class Audit4FollowUpReadCompatTests
{
    private static readonly Lazy<IntPtr> s_library = new(LoadLibrary);

    private static IntPtr LoadLibrary()
    {
        _ = RocksDbSharp.Native.Instance; // make sure rocksdb-sharp loaded its library
        foreach (string line in File.ReadLines("/proc/self/maps"))
        {
            int slash = line.IndexOf('/', StringComparison.Ordinal);
            if (slash < 0) continue;
            string path = line[slash..];
            if (Path.GetFileName(path).StartsWith("librocksdb", StringComparison.Ordinal)) return NativeLibrary.Load(path);
        }
        throw new InvalidOperationException("librocksdb is not loaded");
    }

    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);

    /// <summary>rocksdb_writebatch_singledelete (not wrapped by rocksdb-sharp's WriteBatch).</summary>
    private static void NativeSingleDelete(RocksDbSharp.WriteBatch batch, byte[] key)
    {
        var f = (delegate* unmanaged<IntPtr, byte*, UIntPtr, void>)NativeLibrary.GetExport(s_library.Value, "rocksdb_writebatch_singledelete");
        fixed (byte* k = key) f(batch.Handle, k, (UIntPtr)key.Length);
    }

    /// <summary>
    /// S5. DBWithTTLImpl::Write (utilities/ttl/db_ttl_impl.cc:643-680) rewrites the batch through a handler that
    /// overrides PutCF / MergeCF / DeleteCF / DeleteRangeCF / LogData only. A SingleDelete of the batch therefore reaches
    /// WriteBatch::Handler::SingleDeleteCF (include/rocksdb/write_batch.h): for the default column family the no-op
    /// SingleDelete(key) and OK, so the entry is silently left out of the rewritten batch and the key stays.
    /// The native half pins that down. The managed TTL database applies the SingleDelete (its handler forwards it):
    /// the comparison is kept ignored until it is decided whether to mirror upstream's behaviour.
    /// </summary>
    [TestMethod]
    public void NativeTtlWriteDropsSingleDeleteOfABatch()
    {
        using var dir = new TempDir("audit4-fur-ttl-n");
        using RocksDbSharp.RocksDb ndb = RocksDbSharp.RocksDb.OpenWithTtl(new RocksDbSharp.DbOptions().SetCreateIfMissing(true), dir.Path, 3600);
        ndb.Put(U("k"), U("v"));
        ndb.Put(U("d"), U("v"));
        using (var b = new RocksDbSharp.WriteBatch())
        {
            NativeSingleDelete(b, U("k"));
            b.Delete(U("d"));
            b.Put(U("p"), U("1"));
            ndb.Write(b);
        }
        Assert.AreEqual("v", Encoding.UTF8.GetString(ndb.Get(U("k"))), "native: the SingleDelete of the batch was dropped");
        Assert.IsNull(ndb.Get(U("d")), "native: the Delete of the same batch was applied");
        Assert.AreEqual("1", Encoding.UTF8.GetString(ndb.Get(U("p"))));
    }

    [TestMethod]
    [Ignore("audit4: upstream DBWithTTL::Write silently drops a batch's SingleDelete (handler without SingleDeleteCF); the port applies it")]
    public void ManagedTtlWriteTreatsSingleDeleteOfABatchLikeNative()
    {
        using var dir = new TempDir("audit4-fur-ttl-m");
        using TtlDatabase db = TtlDatabase.Open(dir.Path, new DbOptions { CreateIfMissing = true }, 3600);
        db.Put(U("k"), U("v"));
        using (var b = new WriteBatch())
        {
            b.SingleDelete(U("k"));
            db.Write(b);
        }
        Assert.AreEqual("v", Encoding.UTF8.GetString(db.Get(U("k"))!));
    }
}
