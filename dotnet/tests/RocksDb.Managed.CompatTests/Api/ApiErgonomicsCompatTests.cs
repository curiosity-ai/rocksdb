using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Db;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>
/// Native observations of WriteBatch column family overloads (byte-identical rep), WriteBatch::MarkWalTerminationPoint
/// (native WAL recovery sees only the logged records), the TTL iterator (same user values as native DBWithTTL's
/// TtlIterator) and max_total_wal_size (the DB written by the port opens natively with the same data).
/// </summary>
[TestClass]
public sealed class ApiErgonomicsCompatTests
{
    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);
    private static string? S(byte[]? b) => b is null ? null : Encoding.UTF8.GetString(b);

    [TestMethod]
    public void ColumnFamilyOverloadsMatchNativeBatchRep()
    {
        using var dir = new TempDir("wb-cf");
        var no = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetCreateMissingColumnFamilies(true);
        var cfs = new RocksDbSharp.ColumnFamilies { { "users", new RocksDbSharp.ColumnFamilyOptions() } };
        byte[] nativeRep;
        using (var db = RocksDbSharp.RocksDb.Open(no, dir.Path, cfs))
        {
            RocksDbSharp.ColumnFamilyHandle users = db.GetColumnFamily("users");
            using var nb = new RocksDbSharp.WriteBatch();
            nb.Put(U("a"), U("1"), users);
            nb.Delete(U("b"), users);
            nb.Merge(U("m"), U("x"), users);
            nb.Put(U("d"), U("2"));
            nativeRep = nb.ToBytes();
        }
        using RocksDatabase mdb = RocksDatabase.Open(dir.Path, new DbOptions(), [new("default", new ColumnFamilyOptions()), new("users", new ColumnFamilyOptions())]);
        ColumnFamily mu = mdb.GetColumnFamily("users");
        using var mb = new WriteBatch();
        mb.Put(mu, U("a"), U("1")).Delete(mu, U("b")).Merge(mu, U("m"), U("x")).Put(mdb.DefaultColumnFamily, U("d"), U("2"));
        CollectionAssert.AreEqual(nativeRep, mb.Data.ToArray());
    }

    [TestMethod]
    public void NativeRecoveryReplaysOnlyTheLoggedPartOfABatch()
    {
        using var dir = new TempDir("wal-term");
        using (RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions { CreateIfMissing = true }))
        {
            using WriteBatch b = db.CreateWriteBatch();
            b.Put(U("logged"), U("1"));
            b.MarkWalTerminationPoint();
            b.Put(U("memtable-only"), U("2"));
            db.Write(b);
            db.Put(U("after"), U("3"));
            Assert.AreEqual("2", S(db.Get(U("memtable-only"))));
        }
        // The port closes without flushing (like upstream): native replays the WAL.
        using var ndb = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions(), dir.Path);
        Assert.AreEqual("1", ndb.Get("logged"));
        Assert.IsNull(ndb.Get("memtable-only"));
        Assert.AreEqual("3", ndb.Get("after"));
    }

    [TestMethod]
    public void TtlIteratorMatchesNativeTtlIterator()
    {
        using var dir = new TempDir("ttl-it");
        var expected = new List<string>();
        using (TtlDatabase ttl = TtlDatabase.Open(dir.Path, new DbOptions { CreateIfMissing = true }, 3600))
        {
            for (int i = 0; i < 50; i++)
            {
                ttl.Put(U("k" + i.ToString("D3")), U("value-" + i));
                expected.Add("k" + i.ToString("D3") + "=value-" + i);
            }
            ttl.Flush();
            ttl.Put(U("k999"), U("mem"));
            expected.Add("k999=mem");
            var managed = new List<string>();
            using (Iterator it = ttl.NewIterator())
            {
                for (it.SeekToFirst(); it.Valid; it.Next()) managed.Add(Encoding.UTF8.GetString(it.Key) + "=" + Encoding.UTF8.GetString(it.Value));
            }
            CollectionAssert.AreEqual(expected, managed);
        }
        var native = new List<string>();
        using (var ndb = RocksDbSharp.RocksDb.OpenWithTtl(new RocksDbSharp.DbOptions(), dir.Path, 3600))
        using (RocksDbSharp.Iterator it = ndb.NewIterator())
        {
            for (it.SeekToFirst(); it.Valid(); it.Next()) native.Add(Encoding.UTF8.GetString(it.Key()) + "=" + Encoding.UTF8.GetString(it.Value()));
        }
        CollectionAssert.AreEqual(expected, native);
    }

    [TestMethod]
    public void MaxTotalWalSizeDbOpensNatively()
    {
        using var dir = new TempDir("max-wal");
        var o = new DbOptions { CreateIfMissing = true, CreateMissingColumnFamilies = true, MaxTotalWalSize = 128 << 10 };
        var cfo = new ColumnFamilyOptions { WriteBufferSize = 32 << 20 };
        using (RocksDatabase db = RocksDatabase.Open(dir.Path, o, [new("default", cfo), new("cold", cfo.Clone())]))
        {
            db.GetColumnFamily("cold").Put(U("c"), U("cold"));
            db.Put(U("h"), U("0"));
            db.Flush();
            byte[] v = new byte[1000];
            for (int i = 0; i < 400; i++) db.Put(U("h" + i), v);
            db.WaitForCompact();
            Assert.AreEqual(1, db.GetLiveFilesMetadata().Count(f => f.ColumnFamilyName == "cold"));
        }
        var cfs = new RocksDbSharp.ColumnFamilies { { "cold", new RocksDbSharp.ColumnFamilyOptions() } };
        using var ndb = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions(), dir.Path, cfs);
        Assert.AreEqual("cold", ndb.Get("c", ndb.GetColumnFamily("cold")));
        Assert.AreEqual(1000, ndb.Get(U("h399"))!.Length);
    }
}
