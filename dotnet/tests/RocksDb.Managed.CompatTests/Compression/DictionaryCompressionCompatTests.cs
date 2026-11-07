using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table;
using RocksDb.Managed.Util;
using CompressionType = RocksDb.Managed.Compression.CompressionType;
using EntryType = RocksDb.Managed.Db.EntryType;

namespace RocksDb.Managed.CompatTests.Compression;

/// <summary>
/// Dictionary compression ("rocksdb.compression_dict"): native SstFileWriter tables with ZSTD (raw, trained and
/// finalized dictionaries), LZ4, zlib and Snappy dictionaries are read by the managed reader; managed tables are
/// ingested and read by native. Raw-sample dictionaries (zstd_max_train_bytes = 0) are byte-identical to native.
/// </summary>
[TestClass]
public sealed class DictionaryCompressionCompatTests
{
    private const int Count = 6000;
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    private static List<byte[]> Keys()
    {
        var keys = new List<byte[]>(Count);
        for (int i = 0; i < Count; i++) keys.Add(Encoding.ASCII.GetBytes("user:" + (i * 7919 % 100003).ToString("D6", System.Globalization.CultureInfo.InvariantCulture) + ":" + i.ToString("D5", System.Globalization.CultureInfo.InvariantCulture)));
        keys.Sort(static (a, b) => a.AsSpan().SequenceCompareTo(b));
        return keys;
    }

    // JSON-ish values sharing a lot of structure across blocks (dictionary-friendly).
    private static byte[] Value(int i) => Encoding.ASCII.GetBytes(
        "{\"id\":" + i + ",\"name\":\"customer-" + (i % 97) + "\",\"email\":\"user" + (i % 89) + "@example.com\",\"country\":\"" +
        (i % 3 == 0 ? "Portugal" : i % 3 == 1 ? "Germany" : "Brazil") + "\",\"active\":" + (i % 2 == 0 ? "true" : "false") + "}");

    private sealed record Settings(CompressionType Compression, uint MaxDictBytes, uint ZstdMaxTrainBytes, bool UseTrainer, int FormatVersion = 7)
    {
        public RocksDbSharp.ColumnFamilyOptions Native()
        {
            var native = RocksDbSharp.Native.Instance;
            var bbto = new RocksDbSharp.BlockBasedTableOptions().SetFormatVersion(FormatVersion).SetBlockSize(1024);
            var cf = new RocksDbSharp.ColumnFamilyOptions();
            cf.SetBlockBasedTableFactory(bbto);
            cf.SetCompression(Compression switch
            {
                CompressionType.Zstd => RocksDbSharp.Compression.Zstd,
                CompressionType.LZ4 => RocksDbSharp.Compression.Lz4,
                CompressionType.LZ4HC => RocksDbSharp.Compression.Lz4hc,
                CompressionType.Zlib => RocksDbSharp.Compression.Zlib,
                CompressionType.Snappy => RocksDbSharp.Compression.Snappy,
                _ => RocksDbSharp.Compression.No,
            });
            // window_bits, level, strategy, max_dict_bytes
            native.rocksdb_options_set_compression_options(cf.Handle, -14, 32767, 0, (int)MaxDictBytes);
            native.rocksdb_options_set_compression_options_zstd_max_train_bytes(cf.Handle, (int)ZstdMaxTrainBytes);
            native.rocksdb_options_set_compression_options_use_zstd_dict_trainer(cf.Handle, (byte)(UseTrainer ? 1 : 0));
            s_keepAlive.Add(bbto);
            s_keepAlive.Add(cf);
            return cf;
        }

        public ColumnFamilyOptions Managed()
        {
            var cf = new ColumnFamilyOptions { Compression = Compression };
            cf.TableOptions.FormatVersion = (uint)FormatVersion;
            cf.TableOptions.BlockSize = 1024;
            cf.CompressionOpts.MaxDictBytes = MaxDictBytes;
            cf.CompressionOpts.ZstdMaxTrainBytes = ZstdMaxTrainBytes;
            cf.CompressionOpts.UseZstdDictTrainer = UseTrainer;
            return cf;
        }
    }

    private static string WriteNative(string dir, Settings s, List<byte[]> keys)
    {
        string path = Path.Combine(dir, "native.sst");
        using (var writer = new RocksDbSharp.SstFileWriter(new RocksDbSharp.EnvOptions(), s.Native()))
        {
            writer.Open(path);
            for (int i = 0; i < keys.Count; i++) writer.Add(keys[i], Value(i));
            writer.Finish();
        }
        return path;
    }

    private static string WriteManaged(string dir, Settings s, List<byte[]> keys)
    {
        string path = Path.Combine(dir, "managed.sst");
        ColumnFamilyOptions cf = s.Managed();
        TableFormatOptions format = TableFactory.CreateFormatOptions(cf.TableOptions);
        TableBuilderOptions tbo = TableFactory.CreateExternalSstFileBuilderOptions(cf);
        using IWritableFile f = LocalFileSystem.Instance.CreateWritable(path);
        using (var b = new BlockBasedTableBuilder(f, format, tbo))
        {
            byte[] ik = new byte[64];
            for (int i = 0; i < keys.Count; i++)
            {
                DbFormat.AppendInternalKey(ik, keys[i], 0, EntryType.Value);
                b.Add(ik.AsSpan(0, keys[i].Length + 8), Value(i));
            }
            b.Finish();
        }
        f.Sync();
        return path;
    }

    private static TableReaderOptions ReaderOptions(Settings s)
    {
        ColumnFamilyOptions cf = s.Managed();
        return TableFactory.CreateReaderOptions(cf, TableFactory.CreateFormatOptions(cf.TableOptions));
    }

    private static void VerifyRead(SstFile sst, List<byte[]> keys)
    {
        using BlockBasedTableIterator it = sst.Reader.NewIterator();
        int n = 0;
        for (it.SeekToFirst(); it.Valid; it.Next(), n++)
        {
            CollectionAssert.AreEqual(keys[n], DbFormat.ExtractUserKey(it.Key).ToArray(), "key " + n);
            CollectionAssert.AreEqual(Value(n), it.Value.ToArray(), "value " + n);
        }
        Assert.AreEqual(keys.Count, n);
        using var value = new PooledBufferWriter();
        for (int i = 0; i < keys.Count; i += 11)
        {
            value.Clear();
            byte[] lk = new byte[keys[i].Length + 8];
            DbFormat.AppendInternalKey(lk, keys[i], DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
            Assert.IsTrue(sst.Reader.Get(lk, value).Found, "get " + i);
            CollectionAssert.AreEqual(Value(i), value.WrittenSpan.ToArray());
        }
    }

    private static void VerifyNativeIngest(string dir, Settings s, string sst, List<byte[]> keys)
    {
        var dbOptions = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        s_keepAlive.Add(dbOptions);
        using var db = RocksDbSharp.RocksDb.Open(dbOptions, Path.Combine(dir, "db"), new RocksDbSharp.ColumnFamilies(s.Native()));
        db.IngestExternalFiles([sst], new RocksDbSharp.IngestExternalFileOptions());
        for (int i = 0; i < keys.Count; i += 3) CollectionAssert.AreEqual(Value(i), db.Get(keys[i]), "native get " + i);
        using RocksDbSharp.Iterator it = db.NewIterator();
        int n = 0;
        for (it.SeekToFirst(); it.Valid(); it.Next(), n++) CollectionAssert.AreEqual(Value(n), it.Value());
        Assert.AreEqual(keys.Count, n);
    }

    /// <summary>
    /// Native dictionary-compressed table -> managed reader; managed dictionary-compressed table -> native. With a raw
    /// sample dictionary the dictionary block is byte-identical to native (same sampling of identical data blocks).
    /// </summary>
    [TestMethod]
    [DataRow(CompressionType.Zstd, 16384u, 0u, true)]
    [DataRow(CompressionType.Zstd, 4096u, 0u, true)]
    [DataRow(CompressionType.Zstd, 8192u, 65536u, true)]
    [DataRow(CompressionType.Zstd, 8192u, 65536u, false)]
    [DataRow(CompressionType.LZ4, 8192u, 0u, true)]
    [DataRow(CompressionType.LZ4HC, 8192u, 0u, true)]
    [DataRow(CompressionType.Zlib, 8192u, 0u, true)]
    [DataRow(CompressionType.Snappy, 8192u, 0u, true)]
    public void DictionaryCompressionMatchesNative(CompressionType compression, uint maxDictBytes, uint zstdMaxTrainBytes, bool useTrainer)
    {
        using var dir = new TempDir("dict-compat");
        List<byte[]> keys = Keys();
        var s = new Settings(compression, maxDictBytes, zstdMaxTrainBytes, useTrainer);
        using SstFile native = SstFile.Open(WriteNative(dir.Path, s, keys), ReaderOptions(s));
        Assert.IsTrue(native.Reader.CompressionDictionary.Length > 0, "native wrote a dictionary");
        VerifyRead(native, keys);

        string managedPath = WriteManaged(dir.Path, s, keys);
        using SstFile managed = SstFile.Open(managedPath, ReaderOptions(s));
        Assert.IsTrue(managed.Reader.CompressionDictionary.Length > 0, "managed wrote a dictionary");
        VerifyRead(managed, keys);
        Assert.AreEqual(native.Properties.NumDataBlocks, managed.Properties.NumDataBlocks, "data blocks; native raw " + native.Properties.UncompressedDataSize + " managed raw " + managed.Properties.UncompressedDataSize);
        Assert.AreEqual(native.Properties.CompressionName, managed.Properties.CompressionName);
        Assert.AreEqual(native.Properties.CompressionOptions, managed.Properties.CompressionOptions);
        if (zstdMaxTrainBytes == 0)
        {
            CollectionAssert.AreEqual(native.Reader.CompressionDictionary.ToArray(), managed.Reader.CompressionDictionary.ToArray(), "raw dictionary");
        }
        else
        {
            Assert.IsTrue(managed.Reader.CompressionDictionary.Length <= maxDictBytes);
        }
        if (compression == CompressionType.Zstd)
        {
            // ZstdSharp is a port of the same zstd release: trained (ZDICT_trainFromBuffer) and finalized
            // (ZDICT_finalizeDictionary) dictionaries and the dictionary-compressed blocks (ZSTD_CCtx_refCDict +
            // ZSTD_compress2) come out byte-identical too. Not a format requirement - a different zstd version would
            // still be compatible - but it keeps the comparison tight.
            CollectionAssert.AreEqual(native.Reader.CompressionDictionary.ToArray(), managed.Reader.CompressionDictionary.ToArray(), "zstd dictionary");
            Assert.AreEqual(native.Properties.DataSize, managed.Properties.DataSize, "data size");
            List<BlockHandle> nh = native.DataBlockHandles(), mh = managed.DataBlockHandles();
            CollectionAssert.AreEqual(nh, mh, "data block handles");
            for (int i = 0; i < nh.Count; i++) CollectionAssert.AreEqual(native.RawBlock(nh[i]), managed.RawBlock(mh[i]), "data block " + i);
        }
        VerifyNativeIngest(dir.Path, s, managedPath, keys);
    }

    /// <summary>
    /// Whole DB round trip with ZSTD dictionary compression: native flushes dictionary-compressed L0 files; the managed
    /// DB reads them, compacts (writing dictionary-compressed outputs bounded by target_file_size) and adds a flush;
    /// native reopens and reads everything.
    /// </summary>
    [TestMethod]
    [DataRow(0u)]
    [DataRow(32768u)]
    public void DictionaryCompressedDbRoundTrip(uint zstdMaxTrainBytes)
    {
        using var dir = new TempDir("dict-db");
        List<byte[]> keys = Keys();
        var s = new Settings(CompressionType.Zstd, 8192, zstdMaxTrainBytes, true);
        RocksDbSharp.ColumnFamilyOptions ncf = s.Native();
        var dbOptions = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetDisableAutoCompactions(1);
        s_keepAlive.Add(dbOptions);
        using (var db = RocksDbSharp.RocksDb.Open(dbOptions, dir.Path, new RocksDbSharp.ColumnFamilies(ncf)))
        {
            for (int i = 0; i < keys.Count; i += 2) db.Put(keys[i], Value(i));
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
        }

        ColumnFamilyOptions mcf = s.Managed();
        mcf.TargetFileSizeBase = 128 << 10;
        mcf.DisableAutoCompactions = true;
        using (RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions(), mcf))
        {
            using var value = new PooledBufferWriter();
            for (int i = 0; i < keys.Count; i += 2)
            {
                value.Clear();
                Assert.IsTrue(db.TryGet(keys[i], value), "managed get " + i);
                CollectionAssert.AreEqual(Value(i), value.WrittenSpan.ToArray());
            }
            for (int i = 1; i < keys.Count; i += 2) db.Put(keys[i], Value(i));
            db.Flush();
            db.CompactRange();
            db.Flush();
        }

        using (var db = RocksDbSharp.RocksDb.Open(dbOptions, dir.Path, new RocksDbSharp.ColumnFamilies(ncf)))
        {
            using RocksDbSharp.Iterator it = db.NewIterator();
            int n = 0;
            for (it.SeekToFirst(); it.Valid(); it.Next(), n++)
            {
                CollectionAssert.AreEqual(keys[n], it.Key());
                CollectionAssert.AreEqual(Value(n), it.Value());
            }
            Assert.AreEqual(keys.Count, n);
        }
        // The compaction outputs carry dictionaries.
        int withDict = 0;
        foreach (string f in Directory.GetFiles(dir.Path, "*.sst"))
        {
            using SstFile sst = SstFile.Open(f, ReaderOptions(s));
            if (sst.Reader.CompressionDictionary.Length > 0) withDict++;
        }
        Assert.IsTrue(withDict > 0, "tables with a dictionary");
    }
}
