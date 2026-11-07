using System.Globalization;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table;
using RocksDb.Managed.Util;
using CompressionType = RocksDb.Managed.Compression.CompressionType;
using EntryType = RocksDb.Managed.Db.EntryType;

namespace RocksDb.Managed.CompatTests.Table;

/// <summary>
/// Binary compatibility of the optional block-based table format features against native RocksDB 11.12:
/// separated key/value data blocks (separate_key_value_in_data_block), the index "is_uniform" hint
/// (uniform_cv_threshold), block_align, super block alignment (super_block_alignment_size) and the kHashSearch index
/// (hash index meta blocks). For each configuration native SstFileWriter and the managed builder write the same keys:
/// data, index and hash index meta blocks must be byte-identical, the managed reader must read the native file and
/// native RocksDB must ingest and read the managed file.
/// </summary>
[TestClass]
public sealed class FormatFeatureCompatTests
{
    private const int PrefixLength = 4;
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    /// <summary>Equivalent native and managed table settings for one test case.</summary>
    public sealed record Case(string Name, int FormatVersion, int RestartInterval = 16, ulong BlockSize = 1024, bool SeparateKv = false,
        double UniformCv = -1, bool BlockAlign = false, ulong SuperBlockSize = 0, ulong SuperBlockRatio = 128, IndexType IndexType = IndexType.BinarySearch,
        bool Prefix = false, bool HashDataBlock = false, bool UniformKeys = false, ulong MetadataBlockSize = 512,
        IndexBlockSearchType SearchType = IndexBlockSearchType.Binary)
    {
        public override string ToString() => Name;

        public RocksDbSharp.ColumnFamilyOptions Native()
        {
            var native = RocksDbSharp.Native.Instance;
            var bbto = new RocksDbSharp.BlockBasedTableOptions().SetFormatVersion(FormatVersion).SetBlockSize(BlockSize).SetBlockRestartInterval(RestartInterval);
            native.rocksdb_block_based_options_set_index_type(bbto.Handle, (int)IndexType);
            native.rocksdb_block_based_options_set_metadata_block_size(bbto.Handle, MetadataBlockSize);
            native.rocksdb_block_based_options_set_separate_key_value_in_data_block(bbto.Handle, (byte)(SeparateKv ? 1 : 0));
            native.rocksdb_block_based_options_set_uniform_cv_threshold(bbto.Handle, UniformCv);
            native.rocksdb_block_based_options_set_block_align(bbto.Handle, (byte)(BlockAlign ? 1 : 0));
            native.rocksdb_block_based_options_set_super_block_alignment_size(bbto.Handle, (UIntPtr)SuperBlockSize);
            native.rocksdb_block_based_options_set_super_block_alignment_space_overhead_ratio(bbto.Handle, (UIntPtr)SuperBlockRatio);
            if (HashDataBlock) native.rocksdb_block_based_options_set_data_block_index_type(bbto.Handle, 1);
            native.rocksdb_block_based_options_set_index_block_search_type(bbto.Handle, (int)SearchType);
            s_keepAlive.Add(bbto);
            var cf = new RocksDbSharp.ColumnFamilyOptions();
            cf.SetBlockBasedTableFactory(bbto);
            cf.SetCompression(RocksDbSharp.Compression.No);
            if (Prefix) cf.SetPrefixExtractor(RocksDbSharp.SliceTransform.CreateFixedPrefix(PrefixLength));
            s_keepAlive.Add(cf);
            return cf;
        }

        public ColumnFamilyOptions Managed()
        {
            var cf = new ColumnFamilyOptions { Compression = CompressionType.None };
            if (Prefix) cf.PrefixExtractor = SliceTransform.CreateFixedPrefix(PrefixLength);
            BlockBasedTableOptions t = cf.TableOptions;
            t.FormatVersion = (uint)FormatVersion;
            t.BlockSize = BlockSize;
            t.BlockRestartInterval = RestartInterval;
            t.IndexType = IndexType;
            t.MetadataBlockSize = MetadataBlockSize;
            t.SeparateKeyValueInDataBlock = SeparateKv;
            t.UniformCvThreshold = UniformCv;
            t.BlockAlign = BlockAlign;
            t.SuperBlockAlignmentSize = SuperBlockSize;
            t.SuperBlockAlignmentSpaceOverheadRatio = SuperBlockRatio;
            t.DataBlockIndexType = HashDataBlock ? DataBlockIndexType.BinaryAndHash : DataBlockIndexType.BinarySearch;
            t.IndexBlockSearchType = SearchType;
            return cf;
        }
    }

    public static IEnumerable<object[]> Cases()
    {
        yield return [new Case("separated-kv fv7", 7, SeparateKv: true)];
        yield return [new Case("separated-kv fv8 common prefix + hash data block", 8, RestartInterval: 4, SeparateKv: true, HashDataBlock: true)];
        yield return [new Case("separated-kv fv5 restart 1", 5, RestartInterval: 1, SeparateKv: true)];
        yield return [new Case("separated-kv fv7 partitioned index", 7, SeparateKv: true, IndexType: IndexType.TwoLevelIndexSearch)];
        yield return [new Case("uniform fv7", 7, UniformCv: 2.0, UniformKeys: true)];
        yield return [new Case("uniform fv8 partitioned", 8, UniformCv: 2.0, UniformKeys: true, IndexType: IndexType.TwoLevelIndexSearch, MetadataBlockSize: 256)];
        yield return [new Case("uniform fv6 not uniform", 6, UniformCv: 0.05)];
        // index_block_search_type: interpolation search on the read side (native reads the managed file with it too).
        yield return [new Case("uniform fv7 kAuto search", 7, UniformCv: 2.0, UniformKeys: true, SearchType: IndexBlockSearchType.Auto)];
        yield return [new Case("uniform fv8 partitioned kAuto search", 8, UniformCv: 2.0, UniformKeys: true, IndexType: IndexType.TwoLevelIndexSearch,
            MetadataBlockSize: 256, SearchType: IndexBlockSearchType.Auto)];
        yield return [new Case("fv7 kInterpolation search skewed keys", 7, SearchType: IndexBlockSearchType.Interpolation)];
        yield return [new Case("fv5 kInterpolation search first-key index", 5, IndexType: IndexType.BinarySearchWithFirstKey,
            SearchType: IndexBlockSearchType.Interpolation)];
        yield return [new Case("uniform fv7 kInterpolation search", 7, UniformKeys: true, SearchType: IndexBlockSearchType.Interpolation)];
        yield return [new Case("block_align fv7", 7, BlockAlign: true)];
        yield return [new Case("block_align fv8 separated", 8, BlockAlign: true, SeparateKv: true)];
        yield return [new Case("super block fv7", 7, SuperBlockSize: 8192, SuperBlockRatio: 4)];
        yield return [new Case("super block fv8", 8, SuperBlockSize: 8192, SuperBlockRatio: 4)];
        yield return [new Case("super block fv5 first-key index", 5, SuperBlockSize: 4096, SuperBlockRatio: 4, IndexType: IndexType.BinarySearchWithFirstKey)];
        yield return [new Case("super block fv7 partitioned", 7, SuperBlockSize: 8192, SuperBlockRatio: 8, IndexType: IndexType.TwoLevelIndexSearch)];
        yield return [new Case("hash index fv5", 5, IndexType: IndexType.HashSearch, Prefix: true, BlockSize: 256)];
        yield return [new Case("hash index fv7", 7, IndexType: IndexType.HashSearch, Prefix: true, BlockSize: 256)];
        yield return [new Case("hash index fv8 separated uniform", 8, IndexType: IndexType.HashSearch, Prefix: true, BlockSize: 256, SeparateKv: true, UniformCv: 2.0)];
        yield return [new Case("hash index fv3", 3, IndexType: IndexType.HashSearch, Prefix: true, BlockSize: 512)];
    }

    /// <summary>Keys "pNNN/xxxxx": ~150 distinct 4-byte prefixes with 1..40 keys each, plus two keys outside the prefix domain.</summary>
    private static List<byte[]> PrefixKeys(int seed)
    {
        var rnd = new Random(seed);
        var set = new SortedSet<byte[]>(Comparer<byte[]>.Create(static (a, b) => a.AsSpan().SequenceCompareTo(b)));
        for (int p = 0; p < 150; p++)
        {
            int prefix = rnd.Next(1000);
            int n = 1 + rnd.Next(40);
            for (int i = 0; i < n; i++)
                set.Add(Encoding.ASCII.GetBytes("p" + prefix.ToString("D3", CultureInfo.InvariantCulture) + "/" + rnd.Next(100000).ToString("D5", CultureInfo.InvariantCulture)));
        }
        set.Add("p0"u8.ToArray());
        set.Add("q"u8.ToArray());
        return [.. set];
    }

    /// <summary>Fixed-width big-endian keys with a constant stride (uniformly distributed index separators).</summary>
    private static List<byte[]> UniformKeys()
    {
        var keys = new List<byte[]>();
        for (int i = 0; i < 6000; i++)
        {
            byte[] k = new byte[12];
            "uk__"u8.CopyTo(k);
            System.Buffers.Binary.BinaryPrimitives.WriteUInt64BigEndian(k.AsSpan(4), 1000UL + (ulong)i * 7919);
            keys.Add(k);
        }
        return keys;
    }

    private static byte[] Value(int i) => Encoding.ASCII.GetBytes("v" + i.ToString(CultureInfo.InvariantCulture) + new string((char)('a' + i % 26), i % 37));

    private static List<byte[]> KeysFor(Case c) => c.UniformKeys ? UniformKeys() : PrefixKeys(c.FormatVersion * 31 + (int)c.BlockSize);

    private static string WriteNative(string dir, Case c, List<byte[]> keys)
    {
        string path = Path.Combine(dir, "native.sst");
        using (var writer = new RocksDbSharp.SstFileWriter(new RocksDbSharp.EnvOptions(), c.Native()))
        {
            writer.Open(path);
            for (int i = 0; i < keys.Count; i++) writer.Add(keys[i], Value(i));
            writer.Finish();
        }
        return path;
    }

    private static string WriteManaged(string dir, Case c, List<byte[]> keys)
    {
        string path = Path.Combine(dir, "managed.sst");
        ColumnFamilyOptions cf = c.Managed();
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

    private static TableReaderOptions ReaderOptions(Case c)
    {
        ColumnFamilyOptions cf = c.Managed();
        return TableFactory.CreateReaderOptions(cf, TableFactory.CreateFormatOptions(cf.TableOptions));
    }

    private static byte[] LookupKey(byte[] userKey)
    {
        byte[] k = new byte[userKey.Length + 8];
        DbFormat.AppendInternalKey(k, userKey, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
        return k;
    }

    /// <summary>Data block handles in index order, through the reader's index (partitioned or not).</summary>
    private static List<BlockHandle> DataHandles(SstFile sst)
    {
        var result = new List<BlockHandle>();
        Block top = BlockFetcher.ReadBlock(new BytesFile(sst.Bytes), sst.Reader.Footer, sst.Reader.IndexHandle, verifyChecksums: true);
        try
        {
            using var idx = new BlockIter();
            sst.Reader.InitIndexIter(idx, top, isPartition: false);
            for (idx.SeekToFirst(); idx.Valid; idx.Next())
            {
                if (sst.Reader.IndexType != IndexType.TwoLevelIndexSearch)
                {
                    result.Add(idx.BlockHandle);
                    continue;
                }
                Block part = BlockFetcher.ReadBlock(new BytesFile(sst.Bytes), sst.Reader.Footer, idx.BlockHandle, verifyChecksums: true);
                try
                {
                    using var p = new BlockIter();
                    sst.Reader.InitIndexIter(p, part, isPartition: true);
                    for (p.SeekToFirst(); p.Valid; p.Next()) result.Add(p.BlockHandle);
                }
                finally
                {
                    part.Release();
                }
            }
        }
        finally
        {
            top.Release();
        }
        return result;
    }

    private static void AssertBlocksMatch(SstFile native, SstFile managed, Case c)
    {
        TableProperties n = native.Properties, m = managed.Properties;
        Assert.AreEqual(n.NumDataBlocks, m.NumDataBlocks, "num data blocks");
        Assert.AreEqual(n.DataSize, m.DataSize, "data size");
        Assert.AreEqual(n.IndexSize, m.IndexSize, "index size");
        Assert.AreEqual(n.IndexPartitions, m.IndexPartitions, "index partitions");
        Assert.AreEqual(n.NumUniformBlocks, m.NumUniformBlocks, "num uniform blocks");
        Assert.AreEqual(n.SeparateKeyValueInDataBlock, m.SeparateKeyValueInDataBlock, "separate kv property");
        Assert.AreEqual(n.IndexKeyIsUserKey, m.IndexKeyIsUserKey);
        Assert.AreEqual(c.SeparateKv ? 1UL : 0UL, m.SeparateKeyValueInDataBlock);
        CollectionAssert.AreEqual(n.UserCollectedProperties[TablePropertiesNames.BlockBasedIndexType], m.UserCollectedProperties[TablePropertiesNames.BlockBasedIndexType]);

        List<BlockHandle> nh = DataHandles(native), mh = DataHandles(managed);
        CollectionAssert.AreEqual(nh, mh, "data block handles");
        for (int i = 0; i < nh.Count; i++) CollectionAssert.AreEqual(native.RawBlock(nh[i]), managed.RawBlock(mh[i]), "data block " + i);
        CollectionAssert.AreEqual(native.RawBlock(native.Reader.IndexHandle), managed.RawBlock(managed.Reader.IndexHandle), "(top-level) index block");
        foreach (string meta in new[] { BlockBasedTableConstants.HashIndexPrefixesBlockName, BlockBasedTableConstants.HashIndexPrefixesMetadataBlockName })
        {
            BlockHandle? nm = native.FindMeta(meta), mm = managed.FindMeta(meta);
            Assert.AreEqual(nm.HasValue, mm.HasValue, meta);
            if (nm is { } a && mm is { } b) CollectionAssert.AreEqual(native.RawBlock(a), managed.RawBlock(b), meta);
        }
    }

    /// <summary>Scan forward/backward, Get every key and absent keys, Seek, through the managed reader.</summary>
    private static void VerifyManagedRead(SstFile sst, List<byte[]> keys)
    {
        using (BlockBasedTableIterator it = sst.Reader.NewIterator())
        {
            int n = 0;
            for (it.SeekToFirst(); it.Valid; it.Next(), n++)
            {
                CollectionAssert.AreEqual(keys[n], DbFormat.ExtractUserKey(it.Key).ToArray(), "key " + n);
                CollectionAssert.AreEqual(Value(n), it.Value.ToArray(), "value " + n);
            }
            Assert.AreEqual(keys.Count, n);
            for (it.SeekToLast(); it.Valid; it.Prev())
            {
                n--;
                CollectionAssert.AreEqual(keys[n], DbFormat.ExtractUserKey(it.Key).ToArray(), "prev key " + n);
                CollectionAssert.AreEqual(Value(n), it.Value.ToArray(), "prev value " + n);
            }
            Assert.AreEqual(0, n);
            for (int i = 0; i < keys.Count; i += 7)
            {
                it.Seek(LookupKey(keys[i]));
                Assert.IsTrue(it.Valid);
                CollectionAssert.AreEqual(keys[i], DbFormat.ExtractUserKey(it.Key).ToArray(), "seek " + i);
            }
        }
        using var value = new PooledBufferWriter();
        for (int i = 0; i < keys.Count; i++)
        {
            value.Clear();
            TableGetResult r = sst.Reader.Get(LookupKey(keys[i]), value);
            Assert.IsTrue(r.Found, "get " + i);
            CollectionAssert.AreEqual(Value(i), value.WrittenSpan.ToArray(), "get value " + i);
            // Absent keys: just after an existing key (same prefix) and with a prefix that does not exist.
            byte[] absent = [.. keys[i], (byte)'!'];
            Assert.IsFalse(sst.Reader.Get(LookupKey(absent), null).Found, "absent " + i);
        }
        Assert.IsFalse(sst.Reader.Get(LookupKey("zzzz/absent"u8.ToArray()), null).Found);
        Assert.IsFalse(sst.Reader.Get(LookupKey("a000/absent"u8.ToArray()), null).Found);
    }

    [TestMethod]
    [DynamicData(nameof(Cases))]
    public void FormatFeatureMatchesNative(Case c)
    {
        using var dir = new TempDir("fmt-feature");
        List<byte[]> keys = KeysFor(c);
        using SstFile native = SstFile.Open(WriteNative(dir.Path, c, keys), ReaderOptions(c));
        string managedPath = WriteManaged(dir.Path, c, keys);
        using SstFile managed = SstFile.Open(managedPath, ReaderOptions(c));

        if (c.UniformCv >= 1 && c.UniformKeys) Assert.IsTrue(native.Properties.NumUniformBlocks > 0, "expected uniform index blocks");
        if (c.IndexType == IndexType.HashSearch) Assert.IsNotNull(managed.Reader.PrefixIndex, "prefix index");
        AssertBlocksMatch(native, managed, c);
        VerifyManagedRead(native, keys);
        VerifyManagedRead(managed, keys);

        // Native ingests the managed file into a DB with the same table options and reads every key back.
        var dbOptions = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        s_keepAlive.Add(dbOptions);
        var cfs = new RocksDbSharp.ColumnFamilies(c.Native());
        using var db = RocksDbSharp.RocksDb.Open(dbOptions, Path.Combine(dir.Path, "db"), cfs);
        db.IngestExternalFiles([managedPath], new RocksDbSharp.IngestExternalFileOptions());
        for (int i = 0; i < keys.Count; i++)
        {
            // Native prefix-seeks the hash index with the extractor applied to out-of-domain keys too (it reads past the
            // user key), so it cannot find keys shorter than the fixed prefix (in its own files as well).
            if (c.IndexType == IndexType.HashSearch && keys[i].Length < PrefixLength) continue;
            CollectionAssert.AreEqual(Value(i), db.Get(keys[i]), "native get " + i);
        }
        Assert.IsNull(db.Get("zzzz/absent"u8.ToArray()));
        var ro = new RocksDbSharp.ReadOptions().SetTotalOrderSeek(true);
        using RocksDbSharp.Iterator iter = db.NewIterator(null, ro);
        int count = 0;
        for (iter.SeekToFirst(); iter.Valid(); iter.Next(), count++) CollectionAssert.AreEqual(keys[count], iter.Key());
        Assert.AreEqual(keys.Count, count);
        for (iter.SeekToLast(); iter.Valid(); iter.Prev()) count--;
        Assert.AreEqual(0, count);
    }

    /// <summary>A native DB flushing with these options writes SSTs the managed reader reads (flush path, sequence numbers).</summary>
    [TestMethod]
    [DynamicData(nameof(Cases))]
    public void NativeFlushReadByManaged(Case c)
    {
        using var dir = new TempDir("fmt-flush");
        List<byte[]> keys = KeysFor(c);
        var dbOptions = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        s_keepAlive.Add(dbOptions);
        var cfs = new RocksDbSharp.ColumnFamilies(c.Native());
        using (var db = RocksDbSharp.RocksDb.Open(dbOptions, dir.Path, cfs))
        {
            for (int i = 0; i < keys.Count; i++) db.Put(keys[i], Value(i));
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
        }
        using SstFile sst = SstFile.OpenSingle(dir.Path, ReaderOptions(c));
        Assert.AreEqual(c.SeparateKv ? 1UL : 0UL, sst.Properties.SeparateKeyValueInDataBlock);
        using BlockBasedTableIterator it = sst.Reader.NewIterator();
        int n = 0;
        for (it.SeekToFirst(); it.Valid; it.Next(), n++)
        {
            CollectionAssert.AreEqual(keys[n], DbFormat.ExtractUserKey(it.Key).ToArray());
            CollectionAssert.AreEqual(Value(n), it.Value.ToArray());
        }
        Assert.AreEqual(keys.Count, n);
        using var value = new PooledBufferWriter();
        for (int i = 0; i < keys.Count; i += 3)
        {
            value.Clear();
            Assert.IsTrue(sst.Reader.Get(LookupKey(keys[i]), value).Found, "get " + i);
            CollectionAssert.AreEqual(Value(i), value.WrittenSpan.ToArray());
        }
    }

    private sealed class BytesFile(byte[] data) : IRandomAccessFile
    {
        public long Length => data.Length;
        public int Read(long offset, Span<byte> destination)
        {
            int n = (int)Math.Min(destination.Length, data.Length - offset);
            data.AsSpan((int)offset, n).CopyTo(destination);
            return n;
        }
        public ValueTask<int> ReadAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken = default) => new(Read(offset, destination.Span));
        public void Dispose() { }
    }
}
