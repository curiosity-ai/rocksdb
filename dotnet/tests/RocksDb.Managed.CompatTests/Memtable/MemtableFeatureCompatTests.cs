using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;

namespace RocksDb.Managed.CompatTests.Memtable;

/// <summary>
/// Differential tests of the memtable bloom (memtable_prefix_bloom_size_ratio / memtable_whole_key_filtering), insert
/// hints and inplace_update_support against native RocksDB 11.12: the same operations give the same reads, and a DB
/// written by the managed port with these options (OPTIONS file included) opens and reads in native RocksDB and back.
/// </summary>
[TestClass]
public sealed class MemtableFeatureCompatTests
{
    private static byte[] K(int p, int i) => Encoding.ASCII.GetBytes("p" + p.ToString("D3") + "/" + i.ToString("D5"));

    private static RocksDbSharp.DbOptions NativeOptions()
    {
        var native = RocksDbSharp.Native.Instance;
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        o.SetPrefixExtractor(RocksDbSharp.SliceTransform.CreateFixedPrefix(4));
        native.rocksdb_options_set_memtable_prefix_bloom_size_ratio(o.Handle, 0.1);
        native.rocksdb_options_set_memtable_whole_key_filtering(o.Handle, 1);
        native.rocksdb_options_set_inplace_update_support(o.Handle, 1);
        o.SetAllowConcurrentMemtableWrite(false);
        native.rocksdb_options_set_inplace_update_num_locks(o.Handle, (UIntPtr)100);
        o.SetCompression(RocksDbSharp.Compression.No);
        return o;
    }

    private static ColumnFamilyOptions ManagedOptions() => new()
    {
        PrefixExtractor = SliceTransform.CreateFixedPrefix(4),
        MemtablePrefixBloomSizeRatio = 0.1,
        MemtableWholeKeyFiltering = true,
        InplaceUpdateSupport = true,
        InplaceUpdateNumLocks = 100,
        MemtableInsertWithHintPrefixExtractor = SliceTransform.CreateFixedPrefix(4),
        Compression = RocksDb.Managed.Compression.CompressionType.None,
    };

    /// <summary>The operation script: puts with shrinking values (in-place updates), growing values, deletes.</summary>
    private static IEnumerable<(byte[] Key, byte[]? Value)> Script(int seed)
    {
        var rnd = new Random(seed);
        for (int n = 0; n < 4000; n++)
        {
            int p = rnd.Next(40) * 2, i = rnd.Next(50);
            int op = rnd.Next(10);
            if (op == 0) yield return (K(p, i), null);
            else yield return (K(p, i), Encoding.ASCII.GetBytes(new string((char)('a' + n % 26), rnd.Next(1, 40)) + n));
        }
    }

    private static void AssertSameReads(Func<byte[], byte[]?> native, Func<byte[], byte[]?> managed)
    {
        for (int p = 0; p < 80; p++)
        {
            for (int i = 0; i < 50; i++) CollectionAssert.AreEqual(native(K(p, i)), managed(K(p, i)), "p" + p + " i" + i);
        }
    }

    [TestMethod]
    public void SameReadsAsNative()
    {
        int seed = Environment.TickCount;
        using var ndir = new TempDir("memfeat-native");
        using var mdir = new TempDir("memfeat-managed");
        using var ndb = RocksDbSharp.RocksDb.Open(NativeOptions(), ndir.Path);
        using RocksDatabase mdb = RocksDatabase.Open(mdir.Path, new DbOptions { CreateIfMissing = true, AllowConcurrentMemtableWrite = false }, ManagedOptions());
        foreach ((byte[] k, byte[]? v) in Script(seed))
        {
            if (v is null)
            {
                ndb.Remove(k);
                mdb.Delete(k);
            }
            else
            {
                ndb.Put(k, v);
                mdb.Put(k, v);
            }
        }
        AssertSameReads(k => ndb.Get(k), k => mdb.Get(k));
        // Prefix seeks (bloom-filtered in both memtables).
        var nro = new RocksDbSharp.ReadOptions().SetPrefixSameAsStart(true);
        var mro = new ReadOptions { PrefixSameAsStart = true };
        for (int p = 0; p < 80; p++)
        {
            using RocksDbSharp.Iterator ni = ndb.NewIterator(null, nro);
            using Iterator mi = mdb.NewIterator(mro);
            ni.Seek(K(p, 0));
            mi.Seek(K(p, 0));
            while (ni.Valid())
            {
                Assert.IsTrue(mi.Valid, "prefix " + p + " seed=" + seed);
                CollectionAssert.AreEqual(ni.Key(), mi.Key.ToArray());
                CollectionAssert.AreEqual(ni.Value(), mi.Value.ToArray());
                ni.Next();
                mi.Next();
            }
            Assert.IsFalse(mi.Valid, "prefix " + p + " seed=" + seed);
        }
    }

    /// <summary>A managed DB with these options (memtable left unflushed: WAL replay) opens in native RocksDB.</summary>
    [TestMethod]
    public void ManagedDbReadByNative()
    {
        int seed = Environment.TickCount;
        using var dir = new TempDir("memfeat-roundtrip");
        var expected = new Dictionary<string, byte[]?>();
        using (RocksDatabase mdb = RocksDatabase.Open(dir.Path, new DbOptions { CreateIfMissing = true, AllowConcurrentMemtableWrite = false }, ManagedOptions()))
        {
            int n = 0;
            foreach ((byte[] k, byte[]? v) in Script(seed))
            {
                if (v is null) mdb.Delete(k);
                else mdb.Put(k, v);
                expected[Encoding.ASCII.GetString(k)] = v;
                if (++n == 2000) mdb.Flush();
            }
        }
        using (var ndb = RocksDbSharp.RocksDb.Open(NativeOptions(), dir.Path))
        {
            foreach (KeyValuePair<string, byte[]?> kv in expected) CollectionAssert.AreEqual(kv.Value, ndb.Get(Encoding.ASCII.GetBytes(kv.Key)), kv.Key + " seed=" + seed);
        }
        // And back: the managed port reads the native-written DB (OPTIONS file with the memtable options).
        using RocksDatabase again = RocksDatabase.Open(dir.Path, new DbOptions { AllowConcurrentMemtableWrite = false }, ManagedOptions());
        foreach (KeyValuePair<string, byte[]?> kv in expected) CollectionAssert.AreEqual(kv.Value, again.Get(Encoding.ASCII.GetBytes(kv.Key)), kv.Key);
    }
}
