using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table;
using RocksDb.Managed.Util;
using ValueType = RocksDb.Managed.Db.ValueType;

namespace RocksDb.Managed.CompatTests.Table;

/// <summary>
/// Binary compatibility of block-based SST files between native RocksDB (rocksdb-sharp) and the managed
/// builder/reader: (a) native SstFileWriter → managed reader, (b) managed builder → native ingestion,
/// (c) native flushed SST → managed reader.
/// </summary>
[TestClass]
public sealed class SstCompatTests
{
    private static byte[] Key(int i) => Encoding.ASCII.GetBytes("key" + i.ToString("D7"));

    private static byte[] Val(int i, int seed) => Encoding.ASCII.GetBytes(new string((char)('a' + (i + seed) % 26), (i * 7 + seed) % 300) + "#" + i);

    /// <summary>Native table options (compression must be None until the compression stream lands).</summary>
    private static RocksDbSharp.ColumnFamilyOptions NativeOptions(int formatVersion, char checksum, int restartInterval, RocksDbSharp.BlockBasedTableIndexType indexType, bool hashDataBlock, ulong blockSize)
    {
        var bbto = new RocksDbSharp.BlockBasedTableOptions()
            .SetFormatVersion(formatVersion)
            .SetBlockRestartInterval(restartInterval)
            .SetBlockSize(blockSize)
            .SetIndexType(indexType);
        RocksDbSharp.Native.Instance.rocksdb_block_based_options_set_checksum(bbto.Handle, checksum);
        if (hashDataBlock) RocksDbSharp.Native.Instance.rocksdb_block_based_options_set_data_block_index_type(bbto.Handle, 1);
        if (indexType == RocksDbSharp.BlockBasedTableIndexType.TwoLevelIndex)
            RocksDbSharp.Native.Instance.rocksdb_block_based_options_set_metadata_block_size(bbto.Handle, 256);
        var cf = new RocksDbSharp.ColumnFamilyOptions();
        cf.SetBlockBasedTableFactory(bbto);
        cf.SetCompression(RocksDbSharp.Compression.No);
        return cf;
    }

    public static IEnumerable<object[]> NativeMatrix()
    {
        int[] fvs = [2, 3, 4, 5, 6, 7];
        char[] checksums = [(char)1, (char)2, (char)3, (char)4, (char)0];
        int n = 0;
        foreach (int fv in fvs)
        foreach (char cs in checksums)
        {
            var it = (n % 3) switch
            {
                0 => RocksDbSharp.BlockBasedTableIndexType.Binary,
                1 => RocksDbSharp.BlockBasedTableIndexType.TwoLevelIndex,
                _ => RocksDbSharp.BlockBasedTableIndexType.Binary,
            };
            yield return [fv, (int)cs, 1 + (n % 5) * 4, (int)it, n % 2 == 0];
            n++;
        }
    }

    [TestMethod]
    [DynamicData(nameof(NativeMatrix))]
    public void NativeSstFileWriterToManagedReader(int formatVersion, int checksum, int restartInterval, int indexType, bool hashDataBlock)
    {
        using var dir = new TempDir("sst-native");
        string path = Path.Combine(dir.Path, "native.sst");
        const int count = 3000;
        const int seed = 5;
        var cfOptions = NativeOptions(formatVersion, (char)checksum, restartInterval, (RocksDbSharp.BlockBasedTableIndexType)indexType, hashDataBlock, 1024);
        using (var writer = new RocksDbSharp.SstFileWriter(new RocksDbSharp.EnvOptions(), cfOptions))
        {
            writer.Open(path);
            for (int i = 0; i < count; i++)
            {
                if (i % 10 == 9) writer.Delete(Key(i));
                else writer.Add(Key(i), Val(i, seed));
            }
            writer.Finish();
        }

        using var reader = BlockBasedTableReader.Open(LocalFileSystem.Instance.OpenRandomAccess(path), TableReaderOptions.Default);
        Assert.AreEqual(formatVersion, reader.Footer.FormatVersion);
        Assert.AreEqual((ChecksumType)checksum, reader.Footer.ChecksumType);
        TableProperties p = reader.Properties;
        Assert.AreEqual((ulong)count, p.NumEntries);
        Assert.AreEqual((ulong)(count / 10), p.NumDeletions);
        Assert.AreEqual("leveldb.BytewiseComparator", p.ComparatorName);
        Assert.AreEqual((IndexType)indexType, reader.IndexType);
        Assert.AreEqual(2u, Coding.DecodeFixed32(p.UserCollectedProperties[TablePropertiesNames.ExternalSstFileVersion]));
        if (indexType == (int)IndexType.TwoLevelIndexSearch) Assert.IsTrue(p.IndexPartitions > 1, "expected a partitioned index");

        // Full forward and backward scans.
        using BlockBasedTableIterator it = reader.NewIterator();
        int n = 0;
        for (it.SeekToFirst(); it.Valid; it.Next(), n++)
        {
            CollectionAssert.AreEqual(Key(n), DbFormat.ExtractUserKey(it.Key).ToArray());
            DbFormat.UnpackSequenceAndType(DbFormat.ExtractFooter(it.Key), out ulong seq, out ValueType t);
            Assert.AreEqual(0UL, seq);
            if (n % 10 == 9) Assert.AreEqual(ValueType.Deletion, t);
            else
            {
                Assert.AreEqual(ValueType.Value, t);
                CollectionAssert.AreEqual(Val(n, seed), it.Value.ToArray());
            }
        }
        Assert.AreEqual(count, n);
        for (it.SeekToLast(); it.Valid; it.Prev()) n--;
        Assert.AreEqual(0, n);

        // Point lookups and seeks.
        using var value = new PooledBufferWriter();
        for (int i = 0; i < count; i += 13)
        {
            value.Clear();
            TableGetResult r = reader.Get(LookupKey(Key(i)), value);
            Assert.IsTrue(r.Found);
            Assert.AreEqual(i % 10 == 9 ? ValueType.Deletion : ValueType.Value, r.Type);
            if (r.Type == ValueType.Value) CollectionAssert.AreEqual(Val(i, seed), value.WrittenSpan.ToArray());
            it.Seek(LookupKey(Key(i)));
            CollectionAssert.AreEqual(Key(i), DbFormat.ExtractUserKey(it.Key).ToArray());
        }
        Assert.IsFalse(reader.Get(LookupKey("key"u8.ToArray()), null).Found);
        Assert.IsFalse(reader.Get(LookupKey("zzz"u8.ToArray()), null).Found);
    }

    private static byte[] LookupKey(byte[] userKey)
    {
        byte[] k = new byte[userKey.Length + 8];
        DbFormat.AppendInternalKey(k, userKey, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
        return k;
    }

    private static byte[] IKey(byte[] userKey, ulong seq, ValueType t)
    {
        byte[] k = new byte[userKey.Length + 8];
        DbFormat.AppendInternalKey(k, userKey, seq, t);
        return k;
    }

    /// <summary>Managed-written properties that are fully determined by the inputs must equal the native SstFileWriter's.</summary>
    [TestMethod]
    [DataRow(5)]
    [DataRow(7)]
    public void ManagedPropertiesMatchNative(int formatVersion)
    {
        using var dir = new TempDir("sst-props");
        string nativePath = Path.Combine(dir.Path, "native.sst");
        using (var writer = new RocksDbSharp.SstFileWriter(new RocksDbSharp.EnvOptions(), NativeOptions(formatVersion, (char)4, 16, RocksDbSharp.BlockBasedTableIndexType.Binary, false, 4096)))
        {
            writer.Open(nativePath);
            for (int i = 0; i < 1000; i++) writer.Add(Key(i), Val(i, 1));
            writer.Finish();
        }
        string managedPath = Path.Combine(dir.Path, "managed.sst");
        WriteManaged(managedPath, new BlockBasedTableOptions { FormatVersion = formatVersion }, 1000, 1);

        using var nr = BlockBasedTableReader.Open(LocalFileSystem.Instance.OpenRandomAccess(nativePath), TableReaderOptions.Default);
        using var mr = BlockBasedTableReader.Open(LocalFileSystem.Instance.OpenRandomAccess(managedPath), TableReaderOptions.Default);
        TableProperties n = nr.Properties, m = mr.Properties;
        Assert.AreEqual(n.NumEntries, m.NumEntries);
        Assert.AreEqual(n.RawKeySize, m.RawKeySize);
        Assert.AreEqual(n.RawValueSize, m.RawValueSize);
        Assert.AreEqual(n.NumDataBlocks, m.NumDataBlocks);
        Assert.AreEqual(n.DataSize, m.DataSize);
        Assert.AreEqual(n.IndexSize, m.IndexSize);
        Assert.AreEqual(n.UncompressedDataSize, m.UncompressedDataSize);
        Assert.AreEqual(n.IndexKeyIsUserKey, m.IndexKeyIsUserKey);
        Assert.AreEqual(n.IndexValueIsDeltaEncoded, m.IndexValueIsDeltaEncoded);
        Assert.AreEqual(n.ComparatorName, m.ComparatorName);
        Assert.AreEqual(n.MergeOperatorName, m.MergeOperatorName);
        Assert.AreEqual(n.PrefixExtractorName, m.PrefixExtractorName);
        Assert.AreEqual(n.CompressionName, m.CompressionName);
        Assert.AreEqual(n.CompressionOptions, m.CompressionOptions);
        Assert.AreEqual(n.FormatVersion, m.FormatVersion);
        Assert.AreEqual(n.ColumnFamilyId, m.ColumnFamilyId);
        Assert.AreEqual(n.KeyLargestSeqno, m.KeyLargestSeqno);
        Assert.AreEqual(n.KeySmallestSeqno, m.KeySmallestSeqno);
        foreach (string name in new[] { TablePropertiesNames.BlockBasedIndexType, TablePropertiesNames.BlockBasedWholeKeyFiltering,
                     TablePropertiesNames.BlockBasedPrefixFiltering, TablePropertiesNames.ExternalSstFileVersion, TablePropertiesNames.ExternalSstFileGlobalSeqno })
            CollectionAssert.AreEqual(n.UserCollectedProperties[name], m.UserCollectedProperties[name], name);
        // Same data block bytes: the first data block of both files must be identical.
        byte[] nb = File.ReadAllBytes(nativePath), mb = File.ReadAllBytes(managedPath);
        int firstBlock = (int)(n.DataSize / n.NumDataBlocks / 2);
        CollectionAssert.AreEqual(nb.AsSpan(0, firstBlock).ToArray(), mb.AsSpan(0, firstBlock).ToArray());
    }

    private static void WriteManaged(string path, BlockBasedTableOptions options, int count, int seed)
    {
        using IWritableFile f = LocalFileSystem.Instance.CreateWritable(path);
        using (var b = new BlockBasedTableBuilder(f, options, new TableBuilderOptions { ExternalSstFile = true, DbId = "SST Writer" }))
        {
            for (int i = 0; i < count; i++) b.Add(IKey(Key(i), 0, ValueType.Value), Val(i, seed));
            b.Finish();
        }
        f.Sync();
    }

    public static IEnumerable<object[]> ManagedMatrix()
    {
        int n = 0;
        for (int fv = 2; fv <= 7; fv++)
            foreach (ChecksumType cs in new[] { ChecksumType.Crc32c, ChecksumType.XxHash, ChecksumType.XxHash64, ChecksumType.Xxh3 })
            {
                yield return [fv, (int)cs, 1 + n % 17, n % 2 == 0, n % 3 == 0];
                n++;
            }
    }

    [TestMethod]
    [DynamicData(nameof(ManagedMatrix))]
    public void ManagedBuilderToNativeIngest(int formatVersion, int checksum, int restartInterval, bool hashIndex, bool firstKeyIndex)
    {
        using var dir = new TempDir("sst-ingest");
        string sst = Path.Combine(dir.Path, "managed.sst");
        const int count = 5000;
        const int seed = 11;
        var options = new BlockBasedTableOptions
        {
            FormatVersion = formatVersion,
            Checksum = (ChecksumType)checksum,
            BlockRestartInterval = restartInterval,
            BlockSize = 2048,
            DataBlockIndexType = hashIndex ? DataBlockIndexType.BinaryAndHash : DataBlockIndexType.BinarySearch,
            IndexType = firstKeyIndex ? IndexType.BinarySearchWithFirstKey : IndexType.BinarySearch,
        };
        WriteManaged(sst, options, count, seed);

        string dbPath = Path.Combine(dir.Path, "db");
        var dbOptions = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        dbOptions.SetCompression(RocksDbSharp.Compression.No);
        using (var db = RocksDbSharp.RocksDb.Open(dbOptions, dbPath))
        {
            db.Put("aaa"u8, "before"u8); // existing data so the file gets a non-zero global seqno
            db.IngestExternalFiles([sst], new RocksDbSharp.IngestExternalFileOptions());
            for (int i = 0; i < count; i += 7) CollectionAssert.AreEqual(Val(i, seed), db.Get(Key(i)), "key " + i);
            Assert.IsNull(db.Get("key"u8.ToArray()));
            using RocksDbSharp.Iterator it = db.NewIterator();
            int n = 0;
            it.Seek(Key(0));
            for (; it.Valid(); it.Next(), n++)
            {
                CollectionAssert.AreEqual(Key(n), it.Key());
                CollectionAssert.AreEqual(Val(n, seed), it.Value());
            }
            Assert.AreEqual(count, n);
            List<RocksDbSharp.LiveFileMetadata> live = db.GetLiveFilesMetadata();
            Assert.IsTrue(live.Count >= 1);
        }
    }

    [TestMethod]
    public void NativeFlushedSstToManagedReader()
    {
        using var dir = new TempDir("sst-flush");
        var dbOptions = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        dbOptions.SetCompression(RocksDbSharp.Compression.No);
        const int count = 4000;
        var expected = new SortedDictionary<string, (ulong Seq, ValueType Type, string? Value)>(StringComparer.Ordinal);
        ulong seq = 0;
        using (var db = RocksDbSharp.RocksDb.Open(dbOptions, dir.Path))
        {
            for (int i = 0; i < count; i++)
            {
                string k = "k" + (i * 7919 % count).ToString("D6");
                string v = "value-" + i;
                db.Put(Encoding.ASCII.GetBytes(k), Encoding.ASCII.GetBytes(v));
                expected[k] = (++seq, ValueType.Value, v);
            }
            for (int i = 0; i < count; i += 5)
            {
                string k = "k" + i.ToString("D6");
                db.Remove(Encoding.ASCII.GetBytes(k));
                expected[k] = (++seq, ValueType.Deletion, null);
            }
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
        }
        string[] ssts = Directory.GetFiles(dir.Path, "*.sst");
        Assert.AreEqual(1, ssts.Length);
        using var reader = BlockBasedTableReader.Open(LocalFileSystem.Instance.OpenRandomAccess(ssts[0]), TableReaderOptions.Default);
        Assert.AreEqual("default", reader.Properties.ColumnFamilyName);
        Assert.AreEqual(0UL, reader.Properties.ColumnFamilyId);
        Assert.AreEqual((ulong)expected.Count, reader.Properties.NumEntries);
        using BlockBasedTableIterator it = reader.NewIterator();
        using var e = expected.GetEnumerator();
        for (it.SeekToFirst(); it.Valid; it.Next())
        {
            Assert.IsTrue(e.MoveNext());
            Assert.AreEqual(e.Current.Key, Encoding.ASCII.GetString(DbFormat.ExtractUserKey(it.Key)));
            DbFormat.UnpackSequenceAndType(DbFormat.ExtractFooter(it.Key), out ulong s, out ValueType t);
            Assert.AreEqual(e.Current.Value.Seq, s);
            Assert.AreEqual(e.Current.Value.Type, t);
            if (t == ValueType.Value) Assert.AreEqual(e.Current.Value.Value, Encoding.ASCII.GetString(it.Value));
        }
        Assert.IsFalse(e.MoveNext());
        using var value = new PooledBufferWriter();
        foreach (KeyValuePair<string, (ulong Seq, ValueType Type, string? Value)> kv in expected)
        {
            value.Clear();
            TableGetResult r = reader.Get(LookupKey(Encoding.ASCII.GetBytes(kv.Key)), value);
            Assert.IsTrue(r.Found);
            Assert.AreEqual(kv.Value.Seq, r.Sequence);
            Assert.AreEqual(kv.Value.Type, r.Type);
            // Snapshot below the entry's sequence number must not see it.
            Assert.IsFalse(reader.Get(IKey(Encoding.ASCII.GetBytes(kv.Key), kv.Value.Seq - 1, DbFormat.ValueTypeForSeek), null).Found);
        }
    }
}
