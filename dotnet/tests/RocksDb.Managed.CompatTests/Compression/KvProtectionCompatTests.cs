using System.Text;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.CompatTests.Compression;

/// <summary>
/// Per key-value protection options (memtable_protection_bytes_per_key, block_protection_bytes_per_key; WriteOptions
/// protection_bytes_per_key) against native RocksDB: a native DB written with protection is opened by the managed DB
/// with the same settings (parsed from native's OPTIONS file), the managed DB writes protected batches and persists the
/// options, and native reopens and reads everything. The protection info itself is in-memory only (never persisted).
/// </summary>
[TestClass]
public sealed class KvProtectionCompatTests
{
    private const int Count = 3000;
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    private static byte[] Key(int i) => Encoding.ASCII.GetBytes("key" + i.ToString("D6", System.Globalization.CultureInfo.InvariantCulture));
    private static byte[] Value(int i, int round) => Encoding.ASCII.GetBytes("value-" + i + "-" + round + new string('x', i % 40));

    [TestMethod]
    public void ProtectedDbRoundTripWithNative()
    {
        using var dir = new Infrastructure.TempDir("kv-protection");
        var native = RocksDbSharp.Native.Instance;
        var ncf = new RocksDbSharp.ColumnFamilyOptions();
        native.rocksdb_options_set_memtable_protection_bytes_per_key(ncf.Handle, 4);
        native.rocksdb_options_set_block_protection_bytes_per_key(ncf.Handle, 2);
        var dbOptions = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        var nwo = new RocksDbSharp.WriteOptions();
        native.rocksdb_writeoptions_set_protection_bytes_per_key(nwo.Handle, 8);
        s_keepAlive.Add(ncf);
        s_keepAlive.Add(dbOptions);
        s_keepAlive.Add(nwo);
        using (var db = RocksDbSharp.RocksDb.Open(dbOptions, dir.Path, new RocksDbSharp.ColumnFamilies(ncf)))
        {
            for (int i = 0; i < Count; i += 2) db.Put(Key(i), Value(i, 0), null, nwo);
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            for (int i = 0; i < Count; i += 6) db.Put(Key(i), Value(i, 1), null, nwo);
        }

        // Native's OPTIONS file -> managed options.
        string nativeOptions = OptionsFile.FindLatestOptionsFile(RocksDb.Managed.Env.LocalFileSystem.Instance, dir.Path)!;
        ColumnFamilyOptions parsed = OptionsFile.Load(RocksDb.Managed.Env.LocalFileSystem.Instance, nativeOptions, preserveUnknownOptions: true).FindColumnFamily("default")!;
        Assert.AreEqual(4u, parsed.MemtableProtectionBytesPerKey);
        Assert.AreEqual(2u, parsed.BlockProtectionBytesPerKey);

        var mcf = new ColumnFamilyOptions { MemtableProtectionBytesPerKey = 4, BlockProtectionBytesPerKey = 2 };
        var wo = new WriteOptions { ProtectionBytesPerKey = 8 };
        using (RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions(), mcf))
        {
            using var value = new PooledBufferWriter();
            for (int i = 0; i < Count; i += 2)
            {
                value.Clear();
                Assert.IsTrue(db.TryGet(Key(i), value), "managed get " + i);
                CollectionAssert.AreEqual(Value(i, i % 6 == 0 ? 1 : 0), value.WrittenSpan.ToArray());
            }
            for (int i = 1; i < Count; i += 2) db.Put(wo, Key(i), Value(i, 2));
            db.Flush();
            db.CompactRange();
            for (int i = 3; i < Count; i += 10) db.Put(wo, Key(i), Value(i, 3)); // stays in the WAL
        }
        string managedOptions = File.ReadAllText(OptionsFile.FindLatestOptionsFile(RocksDb.Managed.Env.LocalFileSystem.Instance, dir.Path)!);
        StringAssert.Contains(managedOptions, "memtable_protection_bytes_per_key=4");
        StringAssert.Contains(managedOptions, "block_protection_bytes_per_key=2");

        using (var db = RocksDbSharp.RocksDb.Open(dbOptions, dir.Path, new RocksDbSharp.ColumnFamilies(ncf)))
        {
            using RocksDbSharp.Iterator it = db.NewIterator();
            int n = 0;
            for (it.SeekToFirst(); it.Valid(); it.Next(), n++)
            {
                int i = n;
                int round = i % 2 == 0 ? (i % 6 == 0 ? 1 : 0) : (i % 10 == 3 ? 3 : 2);
                CollectionAssert.AreEqual(Key(i), it.Key());
                CollectionAssert.AreEqual(Value(i, round), it.Value(), "native value " + i);
            }
            Assert.AreEqual(Count, n);
        }
    }
}
