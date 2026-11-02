using System.Text;
using RocksDb.Managed.Cache;
using RocksDb.Managed.Db;
using RocksDb.Managed.Table;
using RocksDb.Managed.Table.Filters;
using RocksDb.Managed.Util;
using CompressionType = RocksDb.Managed.Compression.CompressionType;
using EntryType = RocksDb.Managed.Db.EntryType;

namespace RocksDb.Managed.Tests.Table;

/// <summary>
/// Managed round trips for prefix extractors / prefix filters, partitioned indexes and filters, dictionary compression
/// and optimize_filters_for_memory (native byte-identity is covered by the compat tests).
/// </summary>
[TestClass]
public sealed class TableFeatureTests
{
    // ------------------------------------------------------------------------------------------- SliceTransform

    [TestMethod]
    public void SliceTransformBuiltins()
    {
        SliceTransform f = SliceTransform.CreateFixedPrefix(3);
        Assert.AreEqual("rocksdb.FixedPrefix.3", f.AsString());
        Assert.AreEqual("rocksdb.FixedPrefix", f.Name);
        Assert.IsTrue(f.InDomain("abc"u8));
        Assert.IsFalse(f.InDomain("ab"u8));
        Assert.IsTrue(f.Transform("abcdef"u8).SequenceEqual("abc"u8));
        Assert.IsTrue(f.FullLengthEnabled(out int len));
        Assert.AreEqual(3, len);
        Assert.IsTrue(f.SameResultWhenAppended("abc"u8));
        Assert.IsFalse(f.SameResultWhenAppended("ab"u8));

        SliceTransform c = SliceTransform.CreateCappedPrefix(4);
        Assert.AreEqual("rocksdb.CappedPrefix.4", c.AsString());
        Assert.IsTrue(c.InDomain(""u8));
        Assert.IsTrue(c.Transform("ab"u8).SequenceEqual("ab"u8));
        Assert.IsTrue(c.Transform("abcdef"u8).SequenceEqual("abcd"u8));
        Assert.IsTrue(c.SameResultWhenAppended("abcd"u8));
        Assert.IsFalse(c.SameResultWhenAppended("abc"u8));

        SliceTransform n = SliceTransform.CreateNoop();
        Assert.AreEqual("rocksdb.Noop", n.AsString());
        Assert.IsTrue(n.Transform("xyz"u8).SequenceEqual("xyz"u8));
        Assert.IsFalse(n.FullLengthEnabled(out _));
    }

    [TestMethod]
    [DataRow("rocksdb.FixedPrefix.4", "rocksdb.FixedPrefix.4")]
    [DataRow("fixed:7", "rocksdb.FixedPrefix.7")]
    [DataRow("rocksdb.CappedPrefix.12", "rocksdb.CappedPrefix.12")]
    [DataRow("capped:2", "rocksdb.CappedPrefix.2")]
    [DataRow("rocksdb.Noop", "rocksdb.Noop")]
    [DataRow("{id=rocksdb.FixedPrefix.5}", "rocksdb.FixedPrefix.5")]
    [DataRow("id=rocksdb.CappedPrefix.3", "rocksdb.CappedPrefix.3")]
    [DataRow("nullptr", null)]
    [DataRow("", null)]
    [DataRow("my.CustomTransform", null)]
    [DataRow("rocksdb.FixedPrefix.x", null)]
    [DataRow("rocksdb.FixedPrefix.", null)]
    public void SliceTransformCreateFromString(string id, string? expected)
    {
        Assert.AreEqual(expected, SliceTransform.CreateFromString(id)?.AsString());
    }

    [TestMethod]
    public void PrefixExtractorOptionsRoundTrip()
    {
        var o = new ColumnFamilyOptions { PrefixExtractor = SliceTransform.CreateCappedPrefix(8) };
        Assert.AreEqual("rocksdb.CappedPrefix.8", o.GetOption("prefix_extractor"));
        var p = new ColumnFamilyOptions();
        p.SetOption("prefix_extractor", "rocksdb.FixedPrefix.6", preserveUnknown: false);
        Assert.AreEqual("rocksdb.FixedPrefix.6", p.PrefixExtractor?.AsString());
        Assert.AreSame(o.PrefixExtractor, o.Clone().PrefixExtractor);
        p.SetOption("prefix_extractor", "nullptr", preserveUnknown: false);
        Assert.IsNull(p.PrefixExtractor);
        Assert.AreEqual("nullptr", p.GetOption("prefix_extractor"));
    }

    // ------------------------------------------------------------------------------------------- tables

    private static List<byte[]> PrefixKeys(int seed, int prefixes)
    {
        var rnd = new Random(seed);
        var set = new SortedSet<byte[]>(Comparer<byte[]>.Create(static (a, b) => a.AsSpan().SequenceCompareTo(b)));
        for (int p = 0; p < prefixes; p++)
        {
            string prefix = "k" + rnd.Next(1000).ToString("D3", System.Globalization.CultureInfo.InvariantCulture);
            int n = 1 + rnd.Next(30);
            for (int i = 0; i < n; i++) set.Add(Encoding.ASCII.GetBytes(prefix + "-" + rnd.Next(1_000_000)));
        }
        return [.. set];
    }

    private static byte[] Value(int i) => Encoding.ASCII.GetBytes("value-" + i + new string('z', i % 40));

    private static byte[] Ikey(byte[] userKey, ulong seq = 0, EntryType type = EntryType.Value)
    {
        byte[] k = new byte[userKey.Length + 8];
        DbFormat.AppendInternalKey(k, userKey, seq, type);
        return k;
    }

    private static byte[] LookupKey(byte[] userKey) => Ikey(userKey, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);

    private static BlockBasedTableReader Build(List<byte[]> keys, TableFormatOptions format, TableBuilderOptions tbo, TableReaderOptions ro,
        out MemRandomAccessFile file)
    {
        var w = new MemWritableFile();
        using (var b = new BlockBasedTableBuilder(w, format, tbo))
        {
            for (int i = 0; i < keys.Count; i++) b.Add(Ikey(keys[i]), Value(i));
            b.Finish();
        }
        file = new MemRandomAccessFile(w.ToArray());
        return BlockBasedTableReader.Open(file, ro);
    }

    private static void VerifyAll(BlockBasedTableReader r, List<byte[]> keys)
    {
        using (BlockBasedTableIterator it = r.NewIterator())
        {
            int n = 0;
            for (it.SeekToFirst(); it.Valid; it.Next(), n++)
            {
                CollectionAssert.AreEqual(keys[n], DbFormat.ExtractUserKey(it.Key).ToArray());
                CollectionAssert.AreEqual(Value(n), it.Value.ToArray());
            }
            Assert.AreEqual(keys.Count, n);
            n = keys.Count - 1;
            for (it.SeekToLast(); it.Valid; it.Prev(), n--) CollectionAssert.AreEqual(keys[n], DbFormat.ExtractUserKey(it.Key).ToArray());
            Assert.AreEqual(-1, n);
        }
        using var v = new PooledBufferWriter();
        for (int i = 0; i < keys.Count; i++)
        {
            v.Clear();
            Assert.IsTrue(r.Get(LookupKey(keys[i]), v).Found, "get " + i);
            CollectionAssert.AreEqual(Value(i), v.WrittenSpan.ToArray());
            Assert.IsTrue(r.KeyMayMatch(keys[i]));
        }
    }

    public static IEnumerable<object[]> PartitionCases()
    {
        // fv, index type, partition filters, decouple, prefix, whole key, metadata block size
        yield return [7, IndexType.TwoLevelIndexSearch, true, true, true, true, 256];
        yield return [7, IndexType.TwoLevelIndexSearch, true, false, true, true, 256];
        yield return [5, IndexType.TwoLevelIndexSearch, true, true, false, true, 128];
        yield return [8, IndexType.TwoLevelIndexSearch, true, true, true, false, 512];
        yield return [3, IndexType.TwoLevelIndexSearch, false, true, true, true, 300];
        yield return [2, IndexType.TwoLevelIndexSearch, true, true, false, true, 256];
        yield return [7, IndexType.BinarySearch, true, true, true, true, 256]; // partition_filters sanitized away
        yield return [7, IndexType.BinarySearch, false, true, true, false, 256];
    }

    /// <summary>
    /// Partitioned index / filters and prefix filters: every key is found, the filter has no false negatives, absent
    /// prefixes are mostly filtered on prefix seek and the table records the extractor.
    /// </summary>
    [TestMethod]
    [DynamicData(nameof(PartitionCases))]
    public void PartitionedAndPrefixTablesRoundTrip(int fv, IndexType indexType, bool partitionFilters, bool decouple, bool prefix, bool wholeKey, int metadataBlockSize)
    {
        List<byte[]> keys = PrefixKeys(fv * 13 + metadataBlockSize, 200);
        SliceTransform? pe = prefix ? SliceTransform.CreateFixedPrefix(4) : null;
        var bbto = new BlockBasedTableOptions
        {
            FormatVersion = (uint)fv,
            BlockSize = 256,
            IndexType = indexType,
            PartitionFilters = partitionFilters,
            DecouplePartitionedFilters = decouple,
            MetadataBlockSize = (ulong)metadataBlockSize,
            FilterPolicy = new BloomFilterPolicy(10),
            WholeKeyFiltering = wholeKey,
        };
        TableFormatOptions format = TableFactory.CreateFormatOptions(bbto);
        Assert.AreEqual(partitionFilters && indexType == IndexType.TwoLevelIndexSearch, format.PartitionFilters);
        var tbo = new TableBuilderOptions { PrefixExtractor = pe };
        var stats = new BlockCacheStatistics();
        var ro = new TableReaderOptions { FilterPolicy = format.FilterPolicy, PrefixExtractor = pe, Statistics = stats, BlockCache = BlockCache.CreateLru(4 << 20) };
        using BlockBasedTableReader r = Build(keys, format, tbo, ro, out _);
        Assert.AreEqual(indexType, r.IndexType);
        Assert.IsTrue(r.HasFilter);
        Assert.AreEqual(prefix ? "rocksdb.FixedPrefix.4" : "nullptr", r.Properties.PrefixExtractorName);
        if (indexType == IndexType.TwoLevelIndexSearch) Assert.IsTrue(r.Properties.IndexPartitions > 1, "partitions " + r.Properties.IndexPartitions);
        Assert.AreEqual(format.PartitionFilters, r.MetaBlocks.ContainsKey(BlockBasedTableConstants.PartitionedFilterBlockPrefix + BloomLikeFilterPolicy.BuiltinFilterMetadataName));
        VerifyAll(r, keys);

        var present = new HashSet<string>(StringComparer.Ordinal);
        var prefixes = new HashSet<string>(StringComparer.Ordinal);
        foreach (byte[] k in keys)
        {
            present.Add(Encoding.ASCII.GetString(k));
            prefixes.Add(Encoding.ASCII.GetString(k, 0, 4));
        }
        // Absent keys: (whole-key or prefix) filter rejects most.
        int absent = 0, rejected = 0;
        for (int i = 0; i < 1000; i++)
        {
            byte[] k = Encoding.ASCII.GetBytes("k" + i.ToString("D3", System.Globalization.CultureInfo.InvariantCulture) + "-x" + i);
            if (present.Contains(Encoding.ASCII.GetString(k))) continue;
            if (!wholeKey && prefixes.Contains(Encoding.ASCII.GetString(k, 0, 4))) continue;
            absent++;
            if (!r.KeyMayMatch(k)) rejected++;
            Assert.IsFalse(r.Get(LookupKey(k), null).Found);
        }
        if (wholeKey || prefix) Assert.IsTrue(rejected > absent * 8 / 10, "rejected " + rejected + "/" + absent);

        if (!prefix) return;
        // Prefix seek: every present prefix is found, absent prefixes are mostly skipped by the filter.
        stats.Reset();
        TableIteratorPrefixOptions po = r.GetPrefixOptions(new ReadOptions { PrefixSameAsStart = true }, pe);
        int absentPrefixes = 0;
        using (BlockBasedTableIterator it = r.NewIterator(TableReadFlags.None, po))
        {
            for (int p = 0; p < 1000; p++)
            {
                byte[] target = Encoding.ASCII.GetBytes("k" + p.ToString("D3", System.Globalization.CultureInfo.InvariantCulture));
                it.Seek(LookupKey(target));
                if (prefixes.Contains(Encoding.ASCII.GetString(target)))
                {
                    Assert.IsTrue(it.Valid);
                    Assert.IsTrue(DbFormat.ExtractUserKey(it.Key).StartsWith(target));
                    // SeekForPrev to the end of the prefix lands on its last key.
                    it.SeekForPrev(LookupKey([.. target, 0xff]));
                    Assert.IsTrue(it.Valid);
                    Assert.IsTrue(DbFormat.ExtractUserKey(it.Key).StartsWith(target));
                }
                else
                {
                    absentPrefixes++;
                }
            }
        }
        Assert.IsTrue(stats.Get(BlockCacheCounter.SeekFiltered) > absentPrefixes * 8 / 10, "filtered " + stats.Get(BlockCacheCounter.SeekFiltered) + "/" + absentPrefixes);

        // A different (or no) configured extractor: total-order seek semantics are kept (no filtering).
        TableIteratorPrefixOptions other = r.GetPrefixOptions(new ReadOptions(), SliceTransform.CreateFixedPrefix(3));
        Assert.IsTrue(other.NeedUpperBoundCheck);
        using (BlockBasedTableIterator it = r.NewIterator(TableReadFlags.None, other))
        {
            it.Seek(LookupKey("k"u8.ToArray()));
            Assert.IsTrue(it.Valid);
            CollectionAssert.AreEqual(keys[0], DbFormat.ExtractUserKey(it.Key).ToArray());
        }
        // total_order_seek disables the filter.
        Assert.IsFalse(r.GetPrefixOptions(new ReadOptions { TotalOrderSeek = true }, pe).CheckFilter);
    }

    /// <summary>The table's own extractor is recreated from its id when the configured one differs (prefix seek still filters correctly).</summary>
    [TestMethod]
    public void TablePrefixExtractorRecreatedFromProperties()
    {
        List<byte[]> keys = PrefixKeys(3, 100);
        TableFormatOptions format = TableFactory.CreateFormatOptions(new BlockBasedTableOptions { FilterPolicy = new BloomFilterPolicy(10) });
        var tbo = new TableBuilderOptions { PrefixExtractor = SliceTransform.CreateFixedPrefix(4) };
        SliceTransform configured = SliceTransform.CreateCappedPrefix(4);
        using BlockBasedTableReader r = Build(keys, format, tbo, new TableReaderOptions { FilterPolicy = format.FilterPolicy, PrefixExtractor = configured }, out _);
        Assert.AreEqual("rocksdb.FixedPrefix.4", r.TablePrefixExtractor?.AsString());
        Assert.IsTrue(r.PrefixExtractorChanged(configured));
        Assert.IsFalse(r.PrefixExtractorChanged(r.TablePrefixExtractor));
        // auto_prefix_mode style seek (need_upper_bound_check): filter used only when the upper bound is in the same prefix.
        byte[] target = keys[0][..4];
        var po = r.GetPrefixOptions(new ReadOptions { IterateUpperBound = (byte[])[.. target, (byte)'~'] }, configured);
        using BlockBasedTableIterator it = r.NewIterator(TableReadFlags.None, po);
        it.Seek(LookupKey(target));
        Assert.IsTrue(it.Valid);
    }

    // ------------------------------------------------------------------------------------------- dictionary compression

    [TestMethod]
    [DataRow(CompressionType.Zstd, 0u, true)]
    [DataRow(CompressionType.Zstd, 32768u, true)]
    [DataRow(CompressionType.Zstd, 32768u, false)]
    [DataRow(CompressionType.LZ4, 0u, true)]
    [DataRow(CompressionType.LZ4HC, 0u, true)]
    [DataRow(CompressionType.Snappy, 0u, true)]
    [DataRow(CompressionType.Zlib, 0u, true)]
    public void DictionaryCompressionRoundTrip(CompressionType type, uint zstdMaxTrainBytes, bool useTrainer)
    {
        List<byte[]> keys = PrefixKeys(17, 300);
        TableFormatOptions format = TableFactory.CreateFormatOptions(new BlockBasedTableOptions { BlockSize = 1024 });
        var co = new CompressionOptions { MaxDictBytes = 4096, ZstdMaxTrainBytes = zstdMaxTrainBytes, UseZstdDictTrainer = useTrainer, MaxDictBufferBytes = 64 << 10 };
        var tbo = new TableBuilderOptions { CompressionType = type, CompressionOptions = co };
        using BlockBasedTableReader r = Build(keys, format, tbo, new TableReaderOptions(), out _);
        Assert.IsTrue(r.CompressionDictionary.Length > 0 && r.CompressionDictionary.Length <= 4096, "dict " + r.CompressionDictionary.Length);
        Assert.IsTrue(r.MetaBlocks.ContainsKey(BlockBasedTableConstants.CompressionDictBlockName));
        VerifyAll(r, keys);
        // max_dict_buffer_bytes = 64 KiB: blocks after the buffer limit are counted in num_data_blocks (upstream quirk).
        Assert.IsTrue(r.Properties.NumDataBlocks > 0);
    }

    [TestMethod]
    public void NoDictionaryWhenDisabled()
    {
        List<byte[]> keys = PrefixKeys(5, 50);
        TableFormatOptions format = TableFactory.CreateFormatOptions(new BlockBasedTableOptions());
        using BlockBasedTableReader r = Build(keys, format, new TableBuilderOptions { CompressionType = CompressionType.Zstd }, new TableReaderOptions(), out _);
        Assert.AreEqual(0, r.CompressionDictionary.Length);
        Assert.IsFalse(r.MetaBlocks.ContainsKey(BlockBasedTableConstants.CompressionDictBlockName));
        // max_dict_bytes with no compression: nothing to sample for.
        using BlockBasedTableReader n = Build(keys, format, new TableBuilderOptions { CompressionOptions = new CompressionOptions { MaxDictBytes = 1024 } },
            new TableReaderOptions(), out _);
        Assert.AreEqual(0, n.CompressionDictionary.Length);
        VerifyAll(n, keys);
    }

    // ------------------------------------------------------------------------------------------- optimize_filters_for_memory

    [TestMethod]
    public void GlibcUsableSizeModel()
    {
        Assert.AreEqual(24UL, FilterAllocationModel.GlibcUsableSize(1));
        Assert.AreEqual(24UL, FilterAllocationModel.GlibcUsableSize(24));
        Assert.AreEqual(40UL, FilterAllocationModel.GlibcUsableSize(25));
        Assert.AreEqual(3176UL, FilterAllocationModel.GlibcUsableSize(3162));
        // mmapped: chunk = align4096(request2size + 8), usable = chunk - 16
        Assert.AreEqual(135152UL, FilterAllocationModel.GlibcUsableSize(131072));
    }

    [TestMethod]
    public void OptimizeFiltersForMemoryKeepsFiltersValid()
    {
        FilterAllocatorKind saved = FilterAllocationModel.Allocator;
        try
        {
            FilterAllocationModel.Allocator = FilterAllocatorKind.Glibc;
            var policy = new RibbonFilterPolicy(10, -1);
            var ctx = new FilterBuildingContext(7, -1, false) { OptimizeFiltersForMemory = true };
            for (int round = 0; round < 20; round++)
            {
                using IFilterBitsBuilder b = policy.CreateBuilder(ctx);
                int n = 100 + round * 37;
                for (int i = 0; i < n; i++) b.AddKey(Encoding.ASCII.GetBytes("key" + round + "-" + i));
                using var w = new PooledBufferWriter();
                b.Finish(w);
                IFilterBitsReader reader = policy.CreateReader(w.WrittenSpan.ToArray());
                for (int i = 0; i < n; i++) Assert.IsTrue(reader.MayMatch(Encoding.ASCII.GetBytes("key" + round + "-" + i)));
            }
            // Rounding keeps the aggregate FP rate near target: the balance stays bounded.
            Assert.IsTrue(Math.Abs(policy.AggregateRoundingBalance) < 0x100000000L, "balance " + policy.AggregateRoundingBalance);
        }
        finally
        {
            FilterAllocationModel.Allocator = saved;
        }
    }

    // ------------------------------------------------------------------------------------------- format features

    /// <summary>Keys "pNNN/xxxxx" with ~100 prefixes spanning one or several data blocks.</summary>
    private static List<(byte[] Key, byte[] Value)> PrefixEntries(int seed)
    {
        var rnd = new Random(seed);
        var set = new SortedSet<string>(StringComparer.Ordinal);
        for (int p = 0; p < 100; p++)
        {
            int prefix = rnd.Next(1000), n = 1 + rnd.Next(60);
            for (int i = 0; i < n; i++) set.Add("p" + prefix.ToString("D3") + "/" + rnd.Next(100000).ToString("D5"));
        }
        var list = new List<(byte[], byte[])>();
        foreach (string k in set) list.Add((TableTestUtil.IKey(k, 5), Encoding.ASCII.GetBytes("value-of-" + k)));
        return list;
    }

    /// <summary>
    /// kHashSearch: the hash index meta blocks are written, the reader builds the prefix index and point lookups through
    /// it agree with binary search (present keys, absent keys in present prefixes, absent prefixes).
    /// </summary>
    [TestMethod]
    [DataRow(5, false)]
    [DataRow(7, true)]
    [DataRow(8, false)]
    public void HashIndexRoundTrip(int formatVersion, bool separatedKv)
    {
        List<(byte[] Key, byte[] Value)> entries = PrefixEntries(formatVersion);
        SliceTransform pe = SliceTransform.CreateFixedPrefix(4);
        var format = new TableFormatOptions
        {
            FormatVersion = formatVersion,
            BlockSize = 256,
            IndexType = IndexType.HashSearch,
            SeparateKeyValueInDataBlock = separatedKv,
        };
        byte[] file = TableTestUtil.BuildTable(entries, format, new TableBuilderOptions { PrefixExtractor = pe });
        using BlockBasedTableReader hashed = TableTestUtil.Open(file, new TableReaderOptions { PrefixExtractor = pe });
        using BlockBasedTableReader plain = TableTestUtil.Open(file, TableReaderOptions.Default);
        Assert.AreEqual(IndexType.HashSearch, hashed.IndexType);
        Assert.IsNotNull(hashed.PrefixIndex);
        // Built from the recreated table extractor too, but unused for lookups (prefix extractor changed).
        Assert.IsNotNull(plain.PrefixIndex);
        Assert.IsTrue(hashed.MetaBlocks.ContainsKey(BlockBasedTableConstants.HashIndexPrefixesBlockName));
        Assert.IsTrue(hashed.MetaBlocks.ContainsKey(BlockBasedTableConstants.HashIndexPrefixesMetadataBlockName));
        using var value = new PooledBufferWriter();
        foreach ((byte[] k, byte[] v) in entries)
        {
            string uk = TableTestUtil.UserKeyOf(k);
            value.Clear();
            TableGetResult r = hashed.Get(TableTestUtil.LookupKey(uk), value);
            Assert.IsTrue(r.Found, uk);
            CollectionAssert.AreEqual(v, value.WrittenSpan.ToArray());
            foreach (string probe in new[] { uk + "0", uk[..4] + "/", uk[..4] + "~", "a" + uk[1..], "z" + uk[1..] })
                Assert.AreEqual(plain.Get(TableTestUtil.LookupKey(probe), null).Found, hashed.Get(TableTestUtil.LookupKey(probe), null).Found, probe);
        }
        // Without a prefix extractor the table cannot be written.
        var ex = Assert.ThrowsExactly<RocksDbException>(() => TableTestUtil.BuildTable(entries, format));
        Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);
    }

    [TestMethod]
    public void BlockPrefixIndexBuckets()
    {
        // Prefix "aa" spans blocks 0..1, "ab" block 1, "ac" blocks 3..5 (varint32 length, first block, number of blocks).
        byte[] prefixes = "aaabac"u8.ToArray();
        byte[] meta = [2, 0, 2, 2, 1, 1, 2, 3, 3];
        BlockPrefixIndex idx = BlockPrefixIndex.Create(prefixes, meta);
        Assert.AreEqual(4, idx.NumBuckets);
        CollectionAssert.IsSubsetOf(new uint[] { 0, 1 }, idx.GetBlocks("aa"u8).ToArray());
        CollectionAssert.Contains(idx.GetBlocks("ab"u8).ToArray(), 1u);
        CollectionAssert.IsSubsetOf(new uint[] { 3, 4, 5 }, idx.GetBlocks("ac"u8).ToArray());
        uint[] blocks = idx.GetBlocks("ac"u8).ToArray();
        for (int i = 1; i < blocks.Length; i++) Assert.IsTrue(blocks[i - 1] < blocks[i], "ascending");
        Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => BlockPrefixIndex.Create("aa"u8, [3, 0, 1])).Code);
        Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => BlockPrefixIndex.Create("aab"u8, [2, 0, 1])).Code);
    }

    /// <summary>block_align and super block alignment: padding only between data blocks, every key still readable.</summary>
    [TestMethod]
    [DataRow(true, 0L, 7)]
    [DataRow(false, 4096L, 7)]
    [DataRow(false, 4096L, 8)]
    [DataRow(false, 2048L, 4)]
    public void AlignedTablesRoundTrip(bool blockAlign, long superBlock, int formatVersion)
    {
        List<(byte[] Key, byte[] Value)> entries = TableTestUtil.MakeEntries(3000, 3, 120);
        var format = new TableFormatOptions
        {
            FormatVersion = formatVersion,
            BlockSize = 1024,
            BlockAlign = blockAlign,
            SuperBlockAlignmentSize = superBlock,
            SuperBlockAlignmentSpaceOverheadRatio = 4,
        };
        byte[] file = TableTestUtil.BuildTable(entries, format);
        using BlockBasedTableReader r = TableTestUtil.Open(file);
        using (BlockIter idx = new())
        {
            r.InitIndexIter(idx, r.IndexBlock);
            bool padded = false;
            ulong expectedNext = 0;
            for (idx.SeekToFirst(); idx.Valid; idx.Next())
            {
                BlockHandle h = idx.BlockHandle;
                if (blockAlign) Assert.AreEqual(0UL, h.Offset % 1024, "block_align offset");
                if (superBlock > 0)
                    Assert.AreEqual(h.Offset / (ulong)superBlock, (h.Offset + h.Size - 1) / (ulong)superBlock, "block straddles a super block");
                padded |= h.Offset != expectedNext;
                expectedNext = h.Offset + h.Size + BlockBasedTableConstants.BlockTrailerSize;
            }
            Assert.IsTrue(padded, "expected padding");
        }
        using var value = new PooledBufferWriter();
        foreach ((byte[] k, byte[] v) in entries)
        {
            value.Clear();
            Assert.IsTrue(r.Get(TableTestUtil.LookupKey(TableTestUtil.UserKeyOf(k)), value).Found);
            CollectionAssert.AreEqual(v, value.WrittenSpan.ToArray());
        }
    }

    [TestMethod]
    public void AlignmentOptionsAreValidated()
    {
        List<(byte[] Key, byte[] Value)> entries = TableTestUtil.MakeEntries(10, 1);
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(
            () => TableTestUtil.BuildTable(entries, new TableFormatOptions { BlockAlign = true, BlockSize = 1000 })).Code);
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(
            () => TableTestUtil.BuildTable(entries, new TableFormatOptions { BlockAlign = true }, new TableBuilderOptions { CompressionType = CompressionType.Snappy })).Code);
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(
            () => TableTestUtil.BuildTable(entries, new TableFormatOptions { SuperBlockAlignmentSize = 3000 })).Code);
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(
            () => TableTestUtil.BuildTable(entries, new TableFormatOptions { SuperBlockAlignmentSize = 4096, SuperBlockAlignmentSpaceOverheadRatio = 2 })).Code);
    }

    [TestMethod]
    public void NewTableOptionsRoundTripThroughOptionsFile()
    {
        var t = new BlockBasedTableOptions
        {
            SeparateKeyValueInDataBlock = true,
            UniformCvThreshold = 0.25,
            SuperBlockAlignmentSize = 1 << 20,
            SuperBlockAlignmentSpaceOverheadRatio = 64,
            IndexType = IndexType.HashSearch,
        };
        TableFormatOptions f = TableFactory.CreateFormatOptions(t);
        Assert.IsTrue(f.SeparateKeyValueInDataBlock);
        Assert.AreEqual(0.25, f.UniformCvThreshold);
        Assert.AreEqual(1L << 20, f.SuperBlockAlignmentSize);
        Assert.AreEqual(64L, f.SuperBlockAlignmentSpaceOverheadRatio);
        var copy = t.Clone();
        Assert.IsTrue(copy.SeparateKeyValueInDataBlock);
        Assert.AreEqual(0.25, copy.UniformCvThreshold);
        Assert.AreEqual((ulong)(1 << 20), copy.SuperBlockAlignmentSize);
    }
}
