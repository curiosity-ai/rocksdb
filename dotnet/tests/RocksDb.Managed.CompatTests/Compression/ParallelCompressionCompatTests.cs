using System.Text;
using RocksDb.Managed.Util;
using CompressionType = RocksDb.Managed.Compression.CompressionType;

namespace RocksDb.Managed.CompatTests.Compression;

/// <summary>
/// Parallel compression (CompressionOptions.parallel_threads &gt; 1): a managed DB flushing and compacting with
/// parallel compression is read by native RocksDB (also with parallel_threads), native writes more, and the managed DB
/// reads everything back. The option round-trips through the OPTIONS file in both directions.
/// </summary>
[TestClass]
public sealed class ParallelCompressionCompatTests
{
    private const int Count = 20000;
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    private static byte[] Key(int i) => Encoding.ASCII.GetBytes("key:" + (i * 7919 % 1000003).ToString("D7", System.Globalization.CultureInfo.InvariantCulture) + ":" + i);

    private static byte[] Value(int i, int round) => Encoding.ASCII.GetBytes(
        "{\"id\":" + i + ",\"round\":" + round + ",\"name\":\"customer-" + (i % 97) + "\",\"pad\":\"" + new string((char)('a' + i % 26), i % 64) + "\"}");

    [TestMethod]
    [DataRow(CompressionType.LZ4, RocksDbSharp.Compression.Lz4)]
    [DataRow(CompressionType.Zstd, RocksDbSharp.Compression.Zstd)]
    public void ParallelCompressedDbRoundTrip(CompressionType managedType, RocksDbSharp.Compression nativeType)
    {
        using var dir = new Infrastructure.TempDir("parallel-compression");
        var mcf = new ColumnFamilyOptions { Compression = managedType, DisableAutoCompactions = true, TargetFileSizeBase = 256 << 10 };
        mcf.TableOptions.BlockSize = 1024;
        mcf.CompressionOpts.ParallelThreads = 4;
        using (RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions { CreateIfMissing = true }, mcf))
        {
            for (int i = 0; i < Count; i++) db.Put(Key(i), Value(i, 0));
            db.Flush();
            for (int i = 0; i < Count; i += 3) db.Put(Key(i), Value(i, 1));
            db.Flush();
            db.CompactRange();
        }
        string options = File.ReadAllText(Directory.GetFiles(dir.Path, "OPTIONS-*")[^1]);
        StringAssert.Contains(options, "parallel_threads=4");

        var native = RocksDbSharp.Native.Instance;
        var bbto = new RocksDbSharp.BlockBasedTableOptions().SetBlockSize(1024);
        var ncf = new RocksDbSharp.ColumnFamilyOptions();
        ncf.SetBlockBasedTableFactory(bbto);
        ncf.SetCompression(nativeType);
        native.rocksdb_options_set_compression_options_parallel_threads(ncf.Handle, 3);
        var dbOptions = new RocksDbSharp.DbOptions().SetDisableAutoCompactions(1);
        s_keepAlive.Add(bbto);
        s_keepAlive.Add(ncf);
        s_keepAlive.Add(dbOptions);
        using (var db = RocksDbSharp.RocksDb.Open(dbOptions, dir.Path, new RocksDbSharp.ColumnFamilies(ncf)))
        {
            using (RocksDbSharp.Iterator it = db.NewIterator())
            {
                int n = 0;
                for (it.SeekToFirst(); it.Valid(); it.Next()) n++;
                Assert.AreEqual(Count, n);
            }
            for (int i = 0; i < Count; i += 7) CollectionAssert.AreEqual(Value(i, i % 3 == 0 ? 1 : 0), db.Get(Key(i)), "native get " + i);
            for (int i = 1; i < Count; i += 5) db.Put(Key(i), Value(i, 2));
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
        }
        // The native OPTIONS file carries parallel_threads=3 and the managed parser reads it.
        string nativeOptions = OptionsFile.FindLatestOptionsFile(RocksDb.Managed.Env.LocalFileSystem.Instance, dir.Path)!;
        Assert.AreEqual(3u, OptionsFile.Load(RocksDb.Managed.Env.LocalFileSystem.Instance, nativeOptions, preserveUnknownOptions: true).FindColumnFamily("default")!.CompressionOpts.ParallelThreads);

        using (RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions(), mcf))
        {
            using var value = new PooledBufferWriter();
            for (int i = 0; i < Count; i++)
            {
                value.Clear();
                Assert.IsTrue(db.TryGet(Key(i), value), "managed get " + i);
                int round = i % 5 == 1 ? 2 : i % 3 == 0 ? 1 : 0;
                CollectionAssert.AreEqual(Value(i, round), value.WrittenSpan.ToArray(), "value " + i);
            }
        }
    }
}
