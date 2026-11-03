using System.Text;
using RocksDb.Managed.Cache;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table;
using RocksDb.Managed.Table.Filters;
using RocksDb.Managed.Util;
using CompressionType = RocksDb.Managed.Compression.CompressionType;
using EntryType = RocksDb.Managed.Db.EntryType;

namespace RocksDb.Managed.CompatTests.Table;

/// <summary>
/// Compatibility of prefix extractors / prefix filters, partitioned indexes and partitioned filters, and
/// optimize_filters_for_memory with native RocksDB: byte-identical blocks against native SstFileWriter for the same
/// keys and options, and identical prefix-seek results on whole databases written by either side.
/// </summary>
[TestClass]
public sealed class PrefixAndPartitionCompatTests
{
    private const int PrefixLength = 4;
    private const string BloomId = "bloomfilter:10:false";

    // rocksdb-sharp keeps native objects referenced from managed wrappers; keep options alive for the test run.
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    /// <summary>Keys "pNNN" + suffix: ~150 distinct 4-byte prefixes with 1..40 keys each, sorted and distinct.</summary>
    private static List<byte[]> PrefixKeys(int seed, int prefixes = 150)
    {
        var rnd = new Random(seed);
        var set = new SortedSet<byte[]>(Comparer<byte[]>.Create(static (a, b) => a.AsSpan().SequenceCompareTo(b)));
        for (int p = 0; p < prefixes; p++)
        {
            int prefix = rnd.Next(1000);
            int n = 1 + rnd.Next(40);
            for (int i = 0; i < n; i++)
            {
                string s = "p" + prefix.ToString("D3", System.Globalization.CultureInfo.InvariantCulture) + "/" + rnd.Next(100000).ToString("D5", System.Globalization.CultureInfo.InvariantCulture);
                set.Add(Encoding.ASCII.GetBytes(s));
            }
        }
        // Keys outside the FixedPrefix(4) domain.
        set.Add("p0"u8.ToArray());
        set.Add("q"u8.ToArray());
        return [.. set];
    }

    private static byte[] Value(int i) => Encoding.ASCII.GetBytes("v" + i.ToString(System.Globalization.CultureInfo.InvariantCulture) + new string('x', i % 50));

    /// <summary>Equivalent native and managed table settings.</summary>
    private sealed record Settings(int FormatVersion, bool Prefix, bool WholeKeyFiltering, IndexType IndexType, bool PartitionFilters,
        bool Decouple = true, ulong MetadataBlockSize = 512, string? FilterId = BloomId, bool OptimizeFiltersForMemory = false)
    {
        public RocksDbSharp.BlockBasedTableOptions NativeTable()
        {
            var native = RocksDbSharp.Native.Instance;
            var bbto = new RocksDbSharp.BlockBasedTableOptions().SetFormatVersion(FormatVersion).SetBlockSize(256);
            if (FilterId == BloomId) bbto.SetFilterPolicy(native.rocksdb_filterpolicy_create_bloom_full(10));
            else if (FilterId == "ribbonfilter:10") bbto.SetFilterPolicy(native.rocksdb_filterpolicy_create_ribbon(10));
            native.rocksdb_block_based_options_set_optimize_filters_for_memory(bbto.Handle, (byte)(OptimizeFiltersForMemory ? 1 : 0));
            native.rocksdb_block_based_options_set_whole_key_filtering(bbto.Handle, (byte)(WholeKeyFiltering ? 1 : 0));
            native.rocksdb_block_based_options_set_index_type(bbto.Handle, (int)IndexType);
            native.rocksdb_block_based_options_set_partition_filters(bbto.Handle, (byte)(PartitionFilters ? 1 : 0));
            native.rocksdb_block_based_options_set_decouple_partitioned_filters(bbto.Handle, (byte)(Decouple ? 1 : 0));
            native.rocksdb_block_based_options_set_metadata_block_size(bbto.Handle, MetadataBlockSize);
            s_keepAlive.Add(bbto);
            return bbto;
        }

        public RocksDbSharp.ColumnFamilyOptions Native()
        {
            var cf = new RocksDbSharp.ColumnFamilyOptions();
            cf.SetBlockBasedTableFactory(NativeTable());
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
            t.BlockSize = 256;
            t.FilterPolicy = TableFactory.CreateFilterPolicy(FilterId);
            t.OptimizeFiltersForMemory = OptimizeFiltersForMemory;
            t.WholeKeyFiltering = WholeKeyFiltering;
            t.IndexType = IndexType;
            t.PartitionFilters = PartitionFilters;
            t.DecouplePartitionedFilters = Decouple;
            t.MetadataBlockSize = MetadataBlockSize;
            return cf;
        }
    }

    private static string WriteNative(string dir, Settings s, List<byte[]> keys, string name = "native.sst")
    {
        string path = Path.Combine(dir, name);
        RocksDbSharp.ColumnFamilyOptions cf = s.Native();
        using (var writer = new RocksDbSharp.SstFileWriter(new RocksDbSharp.EnvOptions(), cf))
        {
            writer.Open(path);
            for (int i = 0; i < keys.Count; i++) writer.Add(keys[i], Value(i));
            writer.Finish();
        }
        return path;
    }

    private static string WriteManaged(string dir, Settings s, List<byte[]> keys, string name = "managed.sst", TableFormatOptions? format = null)
    {
        string path = Path.Combine(dir, name);
        ColumnFamilyOptions cf = s.Managed();
        format ??= TableFactory.CreateFormatOptions(cf.TableOptions);
        TableBuilderOptions tbo = TableFactory.CreateExternalSstFileBuilderOptions(cf);
        using IWritableFile f = LocalFileSystem.Instance.CreateWritable(path);
        using (var b = new BlockBasedTableBuilder(f, format, tbo))
        {
            byte[] ik = new byte[64];
            for (int i = 0; i < keys.Count; i++)
            {
                int n = keys[i].Length + 8;
                DbFormat.AppendInternalKey(ik, keys[i], 0, EntryType.Value);
                b.Add(ik.AsSpan(0, n), Value(i));
            }
            b.Finish();
        }
        f.Sync();
        return path;
    }

    private static TableReaderOptions ReaderOptions(Settings s, BlockCacheStatistics? stats = null, BlockCache? cache = null, bool cacheIndexAndFilter = false)
    {
        ColumnFamilyOptions cf = s.Managed();
        TableReaderOptions r = TableFactory.CreateReaderOptions(cf, TableFactory.CreateFormatOptions(cf.TableOptions)) with { Statistics = stats };
        if (cache is not null) r = r with { BlockCache = cache, CacheIndexAndFilterBlocks = cacheIndexAndFilter };
        return r;
    }

    private static byte[] LookupKey(byte[] userKey)
    {
        byte[] k = new byte[userKey.Length + 8];
        DbFormat.AppendInternalKey(k, userKey, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
        return k;
    }

    /// <summary>All blocks listed by an index-like block (filter partition index / partitioned index top level).</summary>
    private static List<BlockHandle> ListHandles(SstFile sst, BlockHandle top)
    {
        Block b = BlockFetcher.ReadBlock(LocalFileSystemFile(sst), sst.Reader.Footer, top, verifyChecksums: true);
        var result = new List<BlockHandle>();
        try
        {
            using var it = new BlockIter();
            it.InitializeIndex(b, InternalKeyComparator.Bytewise, sst.Reader.IndexKeyIncludesSeq, sst.Reader.IndexValueIsFull, false,
                BlockBasedTableConstants.FormatVersionUsesValueDeltaEscape(sst.FormatVersion), BlockIter.DisableGlobalSequenceNumber);
            for (it.SeekToFirst(); it.Valid; it.Next()) result.Add(it.BlockHandle);
        }
        finally
        {
            b.Release();
        }
        return result;
    }

    private static IRandomAccessFile LocalFileSystemFile(SstFile sst) => new BytesFile(sst.Bytes);

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

    private static void AssertSameBlocks(SstFile native, SstFile managed, List<BlockHandle> nh, List<BlockHandle> mh, string what)
    {
        CollectionAssert.AreEqual(nh, mh, what + " handles");
        for (int i = 0; i < nh.Count; i++) CollectionAssert.AreEqual(native.RawBlock(nh[i]), managed.RawBlock(mh[i]), what + " " + i);
    }

    private static string FilterMetaName(Settings s) =>
        (s.PartitionFilters && s.IndexType == IndexType.TwoLevelIndexSearch ? BlockBasedTableConstants.PartitionedFilterBlockPrefix : BlockBasedTableConstants.FullFilterBlockPrefix)
        + BloomLikeFilterPolicy.BuiltinFilterMetadataName;

    /// <summary>Compares index (all partitions), filter (all partitions) and properties of a native and a managed file.</summary>
    private static void AssertTablesMatch(SstFile native, SstFile managed, Settings s)
    {
        TableProperties n = native.Properties, m = managed.Properties;
        Assert.AreEqual(n.NumDataBlocks, m.NumDataBlocks);
        Assert.AreEqual(n.DataSize, m.DataSize);
        Assert.AreEqual(n.IndexSize, m.IndexSize, "index_size");
        Assert.AreEqual(n.IndexPartitions, m.IndexPartitions, "index partitions");
        Assert.AreEqual(n.TopLevelIndexSize, m.TopLevelIndexSize, "top-level index size");
        Assert.AreEqual(n.IndexKeyIsUserKey, m.IndexKeyIsUserKey);
        Assert.AreEqual(n.IndexValueIsDeltaEncoded, m.IndexValueIsDeltaEncoded);
        Assert.AreEqual(n.PrefixExtractorName, m.PrefixExtractorName);
        Assert.AreEqual(n.FilterSize, m.FilterSize, "filter_size");
        Assert.AreEqual(n.NumFilterEntries, m.NumFilterEntries, "num_filter_entries");
        foreach (string p in new[] { TablePropertiesNames.BlockBasedIndexType, TablePropertiesNames.BlockBasedWholeKeyFiltering,
            TablePropertiesNames.BlockBasedPrefixFiltering, TablePropertiesNames.BlockBasedDecoupledPartitionedFilters })
        {
            bool hn = n.UserCollectedProperties.TryGetValue(p, out byte[]? vn), hm = m.UserCollectedProperties.TryGetValue(p, out byte[]? vm);
            Assert.AreEqual(hn, hm, p);
            if (hn) CollectionAssert.AreEqual(vn, vm, p);
        }

        CollectionAssert.AreEqual(native.RawBlock(native.Reader.IndexHandle), managed.RawBlock(managed.Reader.IndexHandle), "(top-level) index block");
        if (s.IndexType == IndexType.TwoLevelIndexSearch)
        {
            List<BlockHandle> np = ListHandles(native, native.Reader.IndexHandle), mp = ListHandles(managed, managed.Reader.IndexHandle);
            Assert.IsTrue(np.Count > 1, "index partitions " + np.Count);
            AssertSameBlocks(native, managed, np, mp, "index partition");
        }
        if (s.FilterId is not null)
        {
            string meta = FilterMetaName(s);
            BlockHandle nf = native.FindMeta(meta) ?? throw new AssertFailedException("native has no " + meta);
            BlockHandle mf = managed.FindMeta(meta) ?? throw new AssertFailedException("managed has no " + meta);
            CollectionAssert.AreEqual(native.RawBlock(nf), managed.RawBlock(mf), "filter block / partition index");
            if (s.PartitionFilters && s.IndexType == IndexType.TwoLevelIndexSearch)
            {
                List<BlockHandle> np = ListHandles(native, nf), mp = ListHandles(managed, mf);
                Assert.IsTrue(np.Count > (s.WholeKeyFiltering ? 1 : 0), "filter partitions " + np.Count);
                AssertSameBlocks(native, managed, np, mp, "filter partition");
            }
        }
    }

    /// <summary>Scan + Get of every key + absent keys through the managed reader.</summary>
    private static void VerifyRead(SstFile sst, List<byte[]> keys)
    {
        using BlockBasedTableIterator it = sst.Reader.NewIterator();
        int n = 0;
        for (it.SeekToFirst(); it.Valid; it.Next(), n++) CollectionAssert.AreEqual(keys[n], DbFormat.ExtractUserKey(it.Key).ToArray(), "key " + n);
        Assert.AreEqual(keys.Count, n);
        using var value = new PooledBufferWriter();
        for (int i = 0; i < keys.Count; i++)
        {
            value.Clear();
            TableGetResult r = sst.Reader.Get(LookupKey(keys[i]), value);
            Assert.IsTrue(r.Found, "get " + i);
            CollectionAssert.AreEqual(Value(i), value.WrittenSpan.ToArray());
        }
    }

    public static IEnumerable<object[]> TableCases()
    {
        // fv, prefix, whole_key_filtering, index type, partition_filters, decouple, metadata_block_size
        yield return [5, true, true, IndexType.BinarySearch, false, true, 512];
        yield return [7, true, false, IndexType.BinarySearch, false, true, 512];
        yield return [7, true, true, IndexType.BinarySearch, false, true, 512];
        yield return [8, true, true, IndexType.BinarySearch, false, true, 512];
        yield return [3, false, true, IndexType.TwoLevelIndexSearch, false, true, 512];
        yield return [4, false, true, IndexType.TwoLevelIndexSearch, true, true, 256];
        yield return [5, true, true, IndexType.TwoLevelIndexSearch, true, true, 512];
        yield return [6, true, false, IndexType.TwoLevelIndexSearch, true, true, 300];
        yield return [7, false, true, IndexType.TwoLevelIndexSearch, true, true, 1024];
        yield return [7, true, true, IndexType.TwoLevelIndexSearch, true, true, 512];
        yield return [7, true, true, IndexType.TwoLevelIndexSearch, true, false, 512];
        yield return [7, false, true, IndexType.TwoLevelIndexSearch, true, false, 700];
        yield return [8, true, true, IndexType.TwoLevelIndexSearch, true, true, 512];
        yield return [8, false, true, IndexType.TwoLevelIndexSearch, false, true, 400];
        yield return [2, false, true, IndexType.TwoLevelIndexSearch, true, true, 512];
    }

    /// <summary>
    /// Native SstFileWriter vs managed builder with prefix extractor / partitioned index / partitioned filters:
    /// byte-identical index (partitions + top level), filter (partitions + partition index) and data blocks and equal
    /// properties; both files read back by the managed reader; the managed file ingested and read by native.
    /// </summary>
    [TestMethod]
    [DynamicData(nameof(TableCases))]
    public void TableBlocksMatchNative(int formatVersion, bool prefix, bool wholeKey, IndexType indexType, bool partitionFilters, bool decouple, int metadataBlockSize)
    {
        using var dir = new TempDir("pfx-part-compat");
        List<byte[]> keys = PrefixKeys(formatVersion * 31 + metadataBlockSize + (prefix ? 1 : 0));
        var s = new Settings(formatVersion, prefix, wholeKey, indexType, partitionFilters, decouple, (ulong)metadataBlockSize);
        using SstFile native = SstFile.Open(WriteNative(dir.Path, s, keys), ReaderOptions(s));
        string managedPath = WriteManaged(dir.Path, s, keys);
        using SstFile managed = SstFile.Open(managedPath, ReaderOptions(s));
        AssertTablesMatch(native, managed, s);
        VerifyRead(native, keys);
        VerifyRead(managed, keys);

        // Native ingests the managed file and reads every key, with prefix seek where configured.
        var dbOptions = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        s_keepAlive.Add(dbOptions);
        var cfs = new RocksDbSharp.ColumnFamilies(s.Native());
        using var db = RocksDbSharp.RocksDb.Open(dbOptions, Path.Combine(dir.Path, "db"), cfs);
        db.IngestExternalFiles([managedPath], new RocksDbSharp.IngestExternalFileOptions());
        for (int i = 0; i < keys.Count; i += 3) CollectionAssert.AreEqual(Value(i), db.Get(keys[i]), "native get " + i);
        if (prefix)
        {
            var ro = new RocksDbSharp.ReadOptions().SetPrefixSameAsStart(true);
            for (int i = 0; i < keys.Count; i += 5)
            {
                if (keys[i].Length < PrefixLength) continue;
                using RocksDbSharp.Iterator it = db.NewIterator(null, ro);
                it.Seek(keys[i]);
                Assert.IsTrue(it.Valid(), "native prefix seek " + i);
                CollectionAssert.AreEqual(keys[i], it.Key());
            }
        }
    }

    /// <summary>
    /// The managed reader uses native partitioned filters: point lookups of absent keys and prefix seeks of absent
    /// prefixes are answered by the filter without touching data blocks (block cache data counters unchanged).
    /// </summary>
    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void PartitionedFilterSkipsDataBlockReads(bool cacheIndexAndFilterBlocks)
    {
        using var dir = new TempDir("part-filter-skip");
        List<byte[]> keys = PrefixKeys(77, 300);
        var s = new Settings(7, Prefix: true, WholeKeyFiltering: true, IndexType.TwoLevelIndexSearch, PartitionFilters: true);
        var stats = new BlockCacheStatistics();
        BlockCache cache = BlockCache.CreateLru(8 << 20);
        using SstFile native = SstFile.Open(WriteNative(dir.Path, s, keys), ReaderOptions(s, stats, cache, cacheIndexAndFilterBlocks));
        Assert.IsTrue(native.Reader.HasFilter);
        VerifyRead(native, keys);

        var present = new HashSet<string>(StringComparer.Ordinal);
        var prefixes = new HashSet<string>(StringComparer.Ordinal);
        foreach (byte[] k in keys)
        {
            present.Add(Encoding.ASCII.GetString(k));
            if (k.Length >= PrefixLength) prefixes.Add(Encoding.ASCII.GetString(k, 0, PrefixLength));
        }

        stats.Reset();
        int absent = 0;
        for (int i = 0; i < 2000; i++)
        {
            string k = "p" + (i % 1000).ToString("D3", System.Globalization.CultureInfo.InvariantCulture) + "/x" + i;
            if (present.Contains(k)) continue;
            absent++;
            Assert.IsFalse(native.Reader.Get(LookupKey(Encoding.ASCII.GetBytes(k)), null).Found);
        }
        long useful = stats.Get(BlockCacheCounter.BloomFilterUseful);
        long dataReads = stats.DataHits + stats.DataMisses;
        Assert.IsTrue(useful > absent * 9 / 10, "filter useful " + useful + "/" + absent);
        Assert.IsTrue(dataReads <= (absent - useful) * 2, "data block reads " + dataReads + " for " + (absent - useful) + " false positives");

        // Prefix seek: absent prefixes are skipped without reading data blocks.
        stats.Reset();
        ColumnFamilyOptions cf = s.Managed();
        TableIteratorPrefixOptions po = native.Reader.GetPrefixOptions(new ReadOptions { PrefixSameAsStart = true }, cf.PrefixExtractor);
        Assert.IsTrue(po.CheckFilter);
        Assert.IsFalse(po.NeedUpperBoundCheck);
        int absentPrefixes = 0;
        using (BlockBasedTableIterator it = native.Reader.NewIterator(TableReadFlags.None, po))
        {
            for (int p = 0; p < 1000; p++)
            {
                string prefix = "p" + p.ToString("D3", System.Globalization.CultureInfo.InvariantCulture);
                it.Seek(LookupKey(Encoding.ASCII.GetBytes(prefix)));
                if (prefixes.Contains(prefix))
                {
                    Assert.IsTrue(it.Valid, "present prefix " + prefix);
                    Assert.IsTrue(DbFormat.ExtractUserKey(it.Key).StartsWith(Encoding.ASCII.GetBytes(prefix)));
                }
                else
                {
                    absentPrefixes++;
                }
            }
        }
        long filtered = stats.Get(BlockCacheCounter.SeekFiltered);
        Assert.IsTrue(filtered > absentPrefixes * 9 / 10, "seeks filtered " + filtered + "/" + absentPrefixes);
    }

    // ---------------------------------------------------------------------------------------------------
    // Whole DBs with prefix seek

    private static ReadOptions ManagedPrefixRead() => new() { PrefixSameAsStart = true };

    /// <summary>Seek targets: a key of every present prefix, plus absent prefixes and between-key targets.</summary>
    private static List<byte[]> SeekTargets(List<byte[]> keys, int seed)
    {
        var rnd = new Random(seed);
        var targets = new List<byte[]>();
        for (int i = 0; i < keys.Count; i += 3) targets.Add(keys[i]);
        for (int i = 0; i < 300; i++)
        {
            string t = "p" + rnd.Next(1000).ToString("D3", System.Globalization.CultureInfo.InvariantCulture) + (rnd.Next(3) == 0 ? "" : "/" + rnd.Next(100000).ToString("D5", System.Globalization.CultureInfo.InvariantCulture));
            targets.Add(Encoding.ASCII.GetBytes(t));
        }
        return targets;
    }

    /// <summary>Expected prefix_same_as_start results: keys &gt;= target with the target's prefix (all keys if out of domain).</summary>
    private static List<string> ExpectedPrefixScan(SortedDictionary<string, string> model, byte[] target)
    {
        string t = Encoding.ASCII.GetString(target);
        var result = new List<string>();
        foreach (KeyValuePair<string, string> kv in model)
        {
            if (string.CompareOrdinal(kv.Key, t) < 0) continue;
            if (t.Length >= PrefixLength && (kv.Key.Length < PrefixLength || string.CompareOrdinal(kv.Key, 0, t, 0, PrefixLength) != 0)) break;
            result.Add(kv.Key + "=" + kv.Value);
        }
        return result;
    }

    private static List<string> ManagedPrefixScan(RocksDatabase db, ReadOptions ro, byte[] target, bool reverse = false)
    {
        var result = new List<string>();
        using Iterator it = db.NewIterator(ro);
        if (reverse) it.SeekForPrev(target);
        else it.Seek(target);
        for (; it.Valid; )
        {
            result.Add(Encoding.ASCII.GetString(it.Key) + "=" + Encoding.ASCII.GetString(it.Value));
            if (reverse) it.Prev();
            else it.Next();
        }
        return result;
    }

    private static List<string> NativePrefixScan(RocksDbSharp.RocksDb db, RocksDbSharp.ReadOptions ro, byte[] target, bool reverse = false)
    {
        var result = new List<string>();
        using RocksDbSharp.Iterator it = db.NewIterator(null, ro);
        if (reverse) it.SeekForPrev(target);
        else it.Seek(target);
        for (; it.Valid(); )
        {
            result.Add(Encoding.ASCII.GetString(it.Key()) + "=" + Encoding.ASCII.GetString(it.Value()));
            if (reverse) it.Prev();
            else it.Next();
        }
        return result;
    }

    private static RocksDbSharp.DbOptions NativeDbOptions(Settings s)
    {
        var o = new RocksDbSharp.DbOptions()
            .SetCreateIfMissing(true)
            .SetBlockBasedTableFactory(s.NativeTable())
            .SetCompression(RocksDbSharp.Compression.No)
            .SetPrefixExtractor(RocksDbSharp.SliceTransform.CreateFixedPrefix(PrefixLength))
            .SetTargetFileSizeBase(16 << 10)
            .SetWriteBufferSize(64 << 10)
            .SetDisableAutoCompactions(1);
        s_keepAlive.Add(o);
        return o;
    }

    /// <summary>Number of files in levels 1..6 (read through a LevelIterator).</summary>
    private static int SortedRunFiles(Func<string, string?> getProperty)
    {
        int n = 0;
        for (int level = 1; level < 7; level++)
            n += int.Parse(getProperty("rocksdb.num-files-at-level" + level) ?? "0", System.Globalization.CultureInfo.InvariantCulture);
        return n;
    }

    private static ColumnFamilyOptions ManagedCfOptions(Settings s)
    {
        ColumnFamilyOptions cf = s.Managed();
        cf.TargetFileSizeBase = 16 << 10;
        cf.WriteBufferSize = 64 << 10;
        cf.DisableAutoCompactions = true;
        return cf;
    }

    /// <summary>
    /// Writes the keys in three batches (compacted to L1 / flushed to L0 / left in the memtable) plus some deletes.
    /// </summary>
    private static SortedDictionary<string, string> Workload(List<byte[]> keys, Action<byte[], byte[]> put, Action<byte[]> delete, Action flush, Action compact)
    {
        var model = new SortedDictionary<string, string>(StringComparer.Ordinal);
        for (int phase = 0; phase < 3; phase++)
        {
            for (int i = phase; i < keys.Count; i += 3)
            {
                put(keys[i], Value(i));
                model[Encoding.ASCII.GetString(keys[i])] = Encoding.ASCII.GetString(Value(i));
                if (i % 17 == 0 && phase > 0)
                {
                    byte[] d = keys[i - 1];
                    delete(d);
                    model.Remove(Encoding.ASCII.GetString(d));
                }
            }
            if (phase == 0) { flush(); compact(); }
            else if (phase == 1) flush();
        }
        return model;
    }

    /// <summary>
    /// A native DB with a FixedPrefix extractor and bloom filters (full or partitioned), data in L1/L0/memtable, is
    /// opened by the managed DB: prefix seeks (prefix_same_as_start), total-order scans and SeekForPrev return exactly
    /// what native returns; the OPTIONS file carries the extractor id.
    /// </summary>
    [TestMethod]
    [DataRow(IndexType.BinarySearch, false, true)]
    [DataRow(IndexType.BinarySearch, false, false)]
    [DataRow(IndexType.TwoLevelIndexSearch, true, true)]
    public void NativeDbPrefixSeekMatchesManaged(IndexType indexType, bool partitionFilters, bool wholeKey)
    {
        using var dir = new TempDir("native-prefix-db");
        List<byte[]> keys = PrefixKeys(5, 250);
        var s = new Settings(7, true, wholeKey, indexType, partitionFilters);
        List<byte[]> targets = SeekTargets(keys, 9);
        SortedDictionary<string, string> model;
        var nativeResults = new List<List<string>>();
        RocksDbSharp.DbOptions nopts = NativeDbOptions(s);
        using (var ndb = RocksDbSharp.RocksDb.Open(nopts, dir.Path))
        {
            model = Workload(keys, (k, v) => ndb.Put(k, v), k => ndb.Remove(k), () => ndb.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true)),
                () => ndb.CompactRange((byte[]?)null!, (byte[]?)null!));
            Assert.IsTrue(SortedRunFiles(ndb.GetProperty) > 1, "L1+ files");
            var ro = new RocksDbSharp.ReadOptions().SetPrefixSameAsStart(true);
            foreach (byte[] t in targets)
            {
                List<string> r = NativePrefixScan(ndb, ro, t);
                CollectionAssert.AreEqual(ExpectedPrefixScan(model, t), r, "native " + Encoding.ASCII.GetString(t));
                nativeResults.Add(r);
            }
        }

        OptionsFile optionsFile = OptionsFile.Load(LocalFileSystem.Instance, Directory.GetFiles(dir.Path, "OPTIONS-*")[^1], preserveUnknownOptions: true);
        Assert.AreEqual("rocksdb.FixedPrefix.4", optionsFile.ColumnFamilies[0].Options.PrefixExtractor?.AsString());

        using RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions(), ManagedCfOptions(s));
        ReadOptions mro = ManagedPrefixRead();
        for (int i = 0; i < targets.Count; i++)
        {
            CollectionAssert.AreEqual(nativeResults[i], ManagedPrefixScan(db, mro, targets[i]), "managed " + Encoding.ASCII.GetString(targets[i]));
        }
        // Reverse prefix iteration and total-order scans.
        for (int i = 0; i < targets.Count; i += 7)
        {
            List<string> expected = ExpectedPrefixScan(model, targets[i]);
            List<string> rev = ManagedPrefixScan(db, mro, targets[i], reverse: true);
            foreach (string kv in rev)
            {
                string k = kv[..kv.IndexOf('=')];
                Assert.IsTrue(string.CompareOrdinal(k, Encoding.ASCII.GetString(targets[i])) <= 0);
                if (targets[i].Length >= PrefixLength) Assert.IsTrue(k.StartsWith(Encoding.ASCII.GetString(targets[i], 0, PrefixLength), StringComparison.Ordinal), k);
            }
            _ = expected;
        }
        List<string> all = ManagedPrefixScan(db, new ReadOptions { TotalOrderSeek = true }, [0]);
        Assert.AreEqual(model.Count, all.Count);
        // Point lookups (whole-key or prefix filter).
        using var value = new PooledBufferWriter();
        foreach (KeyValuePair<string, string> kv in model)
        {
            value.Clear();
            Assert.IsTrue(db.TryGet(Encoding.ASCII.GetBytes(kv.Key), value), kv.Key);
        }
        for (int i = 0; i < 500; i++)
        {
            value.Clear();
            Assert.IsFalse(db.TryGet(Encoding.ASCII.GetBytes("p" + (i % 1000).ToString("D3", System.Globalization.CultureInfo.InvariantCulture) + "/zz" + i), value));
        }
    }

    /// <summary>
    /// The managed DB writes (flush + compaction) with a FixedPrefix extractor and full or partitioned filters; native
    /// opens the directory with the same options and its prefix seeks (which trust the managed filters) return
    /// exactly the expected keys; managed and native agree on every seek.
    /// </summary>
    [TestMethod]
    [DataRow(IndexType.BinarySearch, false, true)]
    [DataRow(IndexType.BinarySearch, false, false)]
    [DataRow(IndexType.TwoLevelIndexSearch, true, true)]
    [DataRow(IndexType.TwoLevelIndexSearch, true, false)]
    public void ManagedDbPrefixSeekReadByNative(IndexType indexType, bool partitionFilters, bool wholeKey)
    {
        using var dir = new TempDir("managed-prefix-db");
        List<byte[]> keys = PrefixKeys(11, 250);
        var s = new Settings(7, true, wholeKey, indexType, partitionFilters);
        List<byte[]> targets = SeekTargets(keys, 13);
        SortedDictionary<string, string> model;
        var managedResults = new List<List<string>>();
        using (RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions { CreateIfMissing = true }, ManagedCfOptions(s)))
        {
            // Force rewrites the bottommost level: a flush thread that runs late (a loaded machine) writes the phase-0
            // memtables into one table, which CompactRange would otherwise just move down as the only L1+ file.
            model = Workload(keys, (k, v) => db.Put(k, v), k => db.Delete(k), () => db.Flush(),
                () => db.CompactRange(new CompactRangeOptions { BottommostLevelCompaction = BottommostLevelCompaction.Force }, null, null));
            Assert.IsTrue(SortedRunFiles(db.GetProperty) > 1, "L1+ files");
            db.Flush();
            ReadOptions mro = ManagedPrefixRead();
            foreach (byte[] t in targets)
            {
                List<string> r = ManagedPrefixScan(db, mro, t);
                CollectionAssert.AreEqual(ExpectedPrefixScan(model, t), r, "managed " + Encoding.ASCII.GetString(t));
                managedResults.Add(r);
            }
        }
        string optionsText = File.ReadAllText(Directory.GetFiles(dir.Path, "OPTIONS-*")[^1]);
        StringAssert.Contains(optionsText, "prefix_extractor=rocksdb.FixedPrefix.4");

        using var ndb = RocksDbSharp.RocksDb.Open(NativeDbOptions(s), dir.Path);
        var ro = new RocksDbSharp.ReadOptions().SetPrefixSameAsStart(true);
        for (int i = 0; i < targets.Count; i++)
        {
            CollectionAssert.AreEqual(managedResults[i], NativePrefixScan(ndb, ro, targets[i]), "native " + Encoding.ASCII.GetString(targets[i]));
        }
        foreach (KeyValuePair<string, string> kv in model) CollectionAssert.AreEqual(Encoding.ASCII.GetBytes(kv.Value), ndb.Get(Encoding.ASCII.GetBytes(kv.Key)), kv.Key);
    }

    // ---------------------------------------------------------------------------------------------------
    // optimize_filters_for_memory

    /// <summary>
    /// optimize_filters_for_memory (native default): a sequence of tables written with one native options object
    /// and one managed policy object (so the FP-rate balance evolves the same way) produce byte-identical filters
    /// under the glibc usable-size model. Filters stay below the 128 KiB mmap threshold, where glibc is deterministic.
    /// </summary>
    [TestMethod]
    [DataRow(BloomId, 7)]
    [DataRow("ribbonfilter:10", 7)]
    [DataRow("ribbonfilter:10", 5)]
    public void OptimizeFiltersForMemoryMatchesNative(string filterId, int formatVersion)
    {
        if (!OperatingSystem.IsLinux()) Assert.Inconclusive("native rounds filter sizes with malloc_usable_size only on Linux builds");
        using var dir = new TempDir("offm-compat");
        var s = new Settings(formatVersion, false, true, IndexType.BinarySearch, false, FilterId: filterId, OptimizeFiltersForMemory: true);
        RocksDbSharp.ColumnFamilyOptions ncf = s.Native();
        ColumnFamilyOptions mcf = s.Managed();
        TableFormatOptions format = TableFactory.CreateFormatOptions(mcf.TableOptions);
        TableBuilderOptions tbo = TableFactory.CreateExternalSstFileBuilderOptions(mcf);
        int differentSizes = 0, identical = 0;
        bool diverged = false;
        int lastSize = -1;
        for (int file = 0; file < 12; file++)
        {
            List<byte[]> keys = PrefixKeys(1000 + file, 20 + file * 13);
            string np = Path.Combine(dir.Path, "n" + file + ".sst");
            using (var writer = new RocksDbSharp.SstFileWriter(new RocksDbSharp.EnvOptions(), ncf))
            {
                writer.Open(np);
                for (int i = 0; i < keys.Count; i++) writer.Add(keys[i], Value(i));
                writer.Finish();
            }
            string mp = Path.Combine(dir.Path, "m" + file + ".sst");
            using (IWritableFile f = LocalFileSystem.Instance.CreateWritable(mp))
            {
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
            }
            using SstFile native = SstFile.Open(np, ReaderOptions(s));
            using SstFile managed = SstFile.Open(mp, ReaderOptions(s));
            string meta = FilterMetaName(s);
            byte[] nf = native.RawBlock(native.FindMeta(meta)!.Value), mf = managed.RawBlock(managed.FindMeta(meta)!.Value);
            if (nf.Length != mf.Length && !diverged)
            {
                // glibc may hand out a free chunk up to 24 bytes larger than requested instead of splitting it (heap
                // history dependent): native then rounds one 16-byte Ribbon step up. The FP-rate balances diverge
                // from there on, so stop comparing bytes.
                Assert.IsTrue(filterId != BloomId && nf.Length > mf.Length && nf.Length - mf.Length <= 32,
                    "filter of file " + file + ": native " + nf.Length + " bytes, managed " + mf.Length);
                diverged = true;
            }
            if (!diverged)
            {
                CollectionAssert.AreEqual(nf, mf, "filter of file " + file + " (native " + nf.Length + " bytes, managed " + mf.Length + ")");
                identical++;
            }
            if (nf.Length != lastSize) differentSizes++;
            lastSize = nf.Length;
            VerifyRead(managed, keys);
        }
        Assert.IsTrue(differentSizes > 3);
        Assert.IsTrue(identical >= (filterId == BloomId ? 12 : 0), "identical filters: " + identical);
        GC.KeepAlive(ncf);
    }
}
