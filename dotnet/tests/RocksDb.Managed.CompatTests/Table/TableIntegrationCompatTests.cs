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
/// End-to-end table compatibility for the integrated modules: index separators (comparators), filters and
/// compression. Native files come from rocksdb-sharp's SstFileWriter; managed files are built from the managed
/// options model through <see cref="TableFactory"/> with the same settings, so whole blocks can be compared.
/// </summary>
[TestClass]
public sealed class TableIntegrationCompatTests
{
    private const int Count = 4000;

    /// <summary>Sorted, distinct keys mixing shared prefixes with 0x00/0xff bytes so separators/successors are exercised.</summary>
    private static List<byte[]> Keys(int seed)
    {
        var rnd = new Random(seed);
        var set = new SortedSet<byte[]>(Comparer<byte[]>.Create(static (a, b) => a.AsSpan().SequenceCompareTo(b)));
        while (set.Count < Count)
        {
            byte[] k;
            if (rnd.Next(2) == 0)
            {
                k = System.Text.Encoding.ASCII.GetBytes("pfx/" + rnd.Next(100000).ToString("D5", System.Globalization.CultureInfo.InvariantCulture) + "/");
                int prefixLength = k.Length;
                Array.Resize(ref k, prefixLength + rnd.Next(0, 6));
                for (int j = prefixLength; j < k.Length; j++) k[j] = RandomByte(rnd);
            }
            else
            {
                k = new byte[rnd.Next(1, 20)];
                for (int j = 0; j < k.Length; j++) k[j] = RandomByte(rnd);
            }
            set.Add(k);
        }
        return [.. set];
    }

    private static byte RandomByte(Random rnd) => rnd.Next(5) switch
    {
        0 => 0xff,
        1 => 0x00,
        _ => (byte)('a' + rnd.Next(26)),
    };

    /// <summary>Compressible values of varying length (some empty).</summary>
    private static byte[] Value(int i) =>
        System.Text.Encoding.ASCII.GetBytes(new string((char)('a' + i % 26), (i * 37) % 251) + "#" + i.ToString(System.Globalization.CultureInfo.InvariantCulture));

    private static byte[] LookupKey(byte[] userKey)
    {
        byte[] k = new byte[userKey.Length + 8];
        DbFormat.AppendInternalKey(k, userKey, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
        return k;
    }

    /// <summary>Equivalent native and managed table settings.</summary>
    private sealed record Settings(int FormatVersion, IndexShorteningMode Shortening, CompressionType Compression, string? FilterId, Func<IntPtr>? NativeFilter)
    {
        public RocksDbSharp.ColumnFamilyOptions Native()
        {
            var native = RocksDbSharp.Native.Instance;
            var bbto = new RocksDbSharp.BlockBasedTableOptions()
                .SetFormatVersion(FormatVersion)
                .SetBlockSize(1024);
            native.rocksdb_block_based_options_set_index_shortening(bbto.Handle, (char)Shortening);
            // Deterministic filter sizes (no malloc_usable_size rounding), like the managed builder.
            native.rocksdb_block_based_options_set_optimize_filters_for_memory(bbto.Handle, 0);
            if (NativeFilter is not null) bbto.SetFilterPolicy(NativeFilter());
            var cf = new RocksDbSharp.ColumnFamilyOptions();
            cf.SetBlockBasedTableFactory(bbto);
            cf.SetCompression(ToNative(Compression));
            return cf;
        }

        public ColumnFamilyOptions Managed()
        {
            var cf = new ColumnFamilyOptions { Compression = Compression };
            cf.TableOptions.FormatVersion = (uint)FormatVersion;
            cf.TableOptions.BlockSize = 1024;
            cf.TableOptions.IndexShortening = Shortening;
            cf.TableOptions.FilterPolicy = FilterId;
            cf.TableOptions.OptimizeFiltersForMemory = false;
            return cf;
        }
    }

    private static RocksDbSharp.Compression ToNative(CompressionType t) => t switch
    {
        CompressionType.None => RocksDbSharp.Compression.No,
        CompressionType.Snappy => RocksDbSharp.Compression.Snappy,
        CompressionType.Zlib => RocksDbSharp.Compression.Zlib,
        CompressionType.LZ4 => RocksDbSharp.Compression.Lz4,
        CompressionType.LZ4HC => RocksDbSharp.Compression.Lz4hc,
        CompressionType.Zstd => RocksDbSharp.Compression.Zstd,
        _ => throw new ArgumentOutOfRangeException(nameof(t)),
    };

    private static string WriteNative(string dir, Settings s, List<byte[]> keys)
    {
        string path = Path.Combine(dir, "native.sst");
        RocksDbSharp.ColumnFamilyOptions cf = s.Native();
        using (var writer = new RocksDbSharp.SstFileWriter(new RocksDbSharp.EnvOptions(), cf))
        {
            writer.Open(path);
            for (int i = 0; i < keys.Count; i++) writer.Add(keys[i], Value(i));
            writer.Finish();
        }
        GC.KeepAlive(cf);
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
                int n = keys[i].Length + 8;
                DbFormat.AppendInternalKey(ik, keys[i], 0, EntryType.Value);
                b.Add(ik.AsSpan(0, n), Value(i));
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

    /// <summary>Full scan + point lookups (present and absent keys) through the managed reader.</summary>
    private static void VerifyManagedRead(SstFile sst, List<byte[]> keys, List<byte[]> absent)
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
        for (int i = 0; i < keys.Count; i += 7)
        {
            value.Clear();
            Assert.IsTrue(sst.Reader.KeyMayMatch(keys[i]), "filter false negative " + i);
            TableGetResult r = sst.Reader.Get(LookupKey(keys[i]), value);
            Assert.IsTrue(r.Found, "get " + i);
            CollectionAssert.AreEqual(Value(i), value.WrittenSpan.ToArray());
        }
        foreach (byte[] a in absent) Assert.IsFalse(sst.Reader.Get(LookupKey(a), null).Found);
    }

    /// <summary>Native DB (configured like <paramref name="s"/>) ingests the managed file and reads every key back.</summary>
    private static void VerifyNativeIngest(string dir, Settings s, string managedSst, List<byte[]> keys, List<byte[]> absent)
    {
        RocksDbSharp.ColumnFamilyOptions cf = s.Native();
        var dbOptions = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        var cfs = new RocksDbSharp.ColumnFamilies(cf);
        using (var db = RocksDbSharp.RocksDb.Open(dbOptions, Path.Combine(dir, "db"), cfs))
        {
            db.IngestExternalFiles([managedSst], new RocksDbSharp.IngestExternalFileOptions());
            for (int i = 0; i < keys.Count; i += 3) CollectionAssert.AreEqual(Value(i), db.Get(keys[i]), "native get " + i);
            foreach (byte[] a in absent) Assert.IsNull(db.Get(a));
            using RocksDbSharp.Iterator it = db.NewIterator();
            int n = 0;
            for (it.SeekToFirst(); it.Valid(); it.Next(), n++) CollectionAssert.AreEqual(keys[n], it.Key());
            Assert.AreEqual(keys.Count, n);
        }
        GC.KeepAlive(cf);
    }

    private static List<byte[]> AbsentKeys(List<byte[]> keys)
    {
        var present = new HashSet<string>(StringComparer.Ordinal);
        foreach (byte[] k in keys) present.Add(Convert.ToHexString(k));
        var result = new List<byte[]>();
        for (int i = 0; result.Count < 200; i++)
        {
            byte[] k = System.Text.Encoding.ASCII.GetBytes("absent-" + i);
            if (!present.Contains(Convert.ToHexString(k))) result.Add(k);
        }
        return result;
    }

    /// <summary>
    /// Index separators come from the comparator (stream E's InternalKeyComparator/BytewiseComparator): the whole index
    /// block and all data blocks of a managed-built file must be byte-identical to native SstFileWriter output.
    /// </summary>
    [TestMethod]
    [DataRow(2, IndexShorteningMode.ShortenSeparators)]
    [DataRow(3, IndexShorteningMode.ShortenSeparatorsAndSuccessor)]
    [DataRow(4, IndexShorteningMode.NoShortening)]
    [DataRow(4, IndexShorteningMode.ShortenSeparators)]
    [DataRow(5, IndexShorteningMode.ShortenSeparatorsAndSuccessor)]
    [DataRow(6, IndexShorteningMode.ShortenSeparators)]
    [DataRow(7, IndexShorteningMode.ShortenSeparators)]
    [DataRow(7, IndexShorteningMode.ShortenSeparatorsAndSuccessor)]
    [DataRow(7, IndexShorteningMode.NoShortening)]
    public void IndexAndDataBlocksMatchNative(int formatVersion, IndexShorteningMode shortening)
    {
        using var dir = new TempDir("idx-compat");
        List<byte[]> keys = Keys(formatVersion * 10 + (int)shortening);
        var s = new Settings(formatVersion, shortening, CompressionType.None, null, null);
        using SstFile native = SstFile.Open(WriteNative(dir.Path, s, keys), ReaderOptions(s));
        using SstFile managed = SstFile.Open(WriteManaged(dir.Path, s, keys), ReaderOptions(s));
        TableProperties n = native.Properties, m = managed.Properties;
        Assert.IsTrue(n.NumDataBlocks > 50, "blocks=" + n.NumDataBlocks);
        Assert.AreEqual(n.NumDataBlocks, m.NumDataBlocks);
        Assert.AreEqual(n.DataSize, m.DataSize);
        Assert.AreEqual(n.IndexSize, m.IndexSize);
        Assert.AreEqual(n.IndexKeyIsUserKey, m.IndexKeyIsUserKey);
        if (formatVersion < 6)
        {
            CollectionAssert.AreEqual(native.Bytes.AsSpan(0, (int)n.DataSize).ToArray(), managed.Bytes.AsSpan(0, (int)m.DataSize).ToArray(), "data blocks");
        }
        else
        {
            // Block trailers carry context checksums seeded by a random per-file base: compare payloads only.
            List<BlockHandle> nh = native.DataBlockHandles(), mh = managed.DataBlockHandles();
            CollectionAssert.AreEqual(nh, mh, "data block handles");
            foreach (BlockHandle h in nh) CollectionAssert.AreEqual(native.RawBlock(h), managed.RawBlock(h));
        }
        CollectionAssert.AreEqual(native.RawBlock(native.Reader.IndexHandle), managed.RawBlock(managed.Reader.IndexHandle), "index block");
        VerifyManagedRead(native, keys, AbsentKeys(keys));
    }

    public static IEnumerable<object[]> FilterCases()
    {
        yield return ["bloomfilter:10:false", 5, (Func<IntPtr>)(static () => RocksDbSharp.Native.Instance.rocksdb_filterpolicy_create_bloom_full(10))];
        yield return ["bloomfilter:7.5:false", 6, (Func<IntPtr>)(static () => RocksDbSharp.Native.Instance.rocksdb_filterpolicy_create_bloom_full(7.5))];
        yield return ["bloomfilter:10:false", 7, (Func<IntPtr>)(static () => RocksDbSharp.Native.Instance.rocksdb_filterpolicy_create_bloom_full(10))];
        // format_version < 5 builds the legacy Bloom filter.
        yield return ["bloomfilter:10:false", 4, (Func<IntPtr>)(static () => RocksDbSharp.Native.Instance.rocksdb_filterpolicy_create_bloom(10))];
        // NewRibbonFilterPolicy default bloom_before_level = 0; SstFileWriter's unknown level counts as bottommost -> Ribbon.
        yield return ["ribbonfilter:10", 5, (Func<IntPtr>)(static () => RocksDbSharp.Native.Instance.rocksdb_filterpolicy_create_ribbon(10))];
        yield return ["ribbonfilter:8.3:-1", 6, (Func<IntPtr>)(static () => RocksDbSharp.Native.Instance.rocksdb_filterpolicy_create_ribbon_hybrid(8.3, -1))];
        yield return ["ribbonfilter:10:0", 7, (Func<IntPtr>)(static () => RocksDbSharp.Native.Instance.rocksdb_filterpolicy_create_ribbon(10))];
        // bloom_before_level = INT_MAX: always Bloom.
        yield return ["ribbonfilter:10:2147483647", 7, (Func<IntPtr>)(static () => RocksDbSharp.Native.Instance.rocksdb_filterpolicy_create_ribbon_hybrid(10, int.MaxValue))];
    }

    /// <summary>
    /// Native SstFileWriter with bloom/ribbon filters -> managed reader (no false negatives, Get works); the managed
    /// builder (policy resolved from the same id) writes a byte-identical filter block; native ingests the managed file
    /// with the filter configured and reads every key.
    /// </summary>
    [TestMethod]
    [DynamicData(nameof(FilterCases))]
    public void FiltersMatchNative(string filterId, int formatVersion, Func<IntPtr> nativeFilter)
    {
        using var dir = new TempDir("filter-compat");
        List<byte[]> keys = Keys(formatVersion + filterId.Length);
        List<byte[]> absent = AbsentKeys(keys);
        var s = new Settings(formatVersion, IndexShorteningMode.ShortenSeparators, CompressionType.None, filterId, nativeFilter);
        using SstFile native = SstFile.Open(WriteNative(dir.Path, s, keys), ReaderOptions(s));
        Assert.IsTrue(native.Reader.HasFilter);
        VerifyManagedRead(native, keys, absent);
        int fp = 0;
        foreach (byte[] a in absent) if (native.Reader.KeyMayMatch(a)) fp++;
        Assert.IsTrue(fp < absent.Count / 10, "FP " + fp + "/" + absent.Count);

        string managedPath = WriteManaged(dir.Path, s, keys);
        using SstFile managed = SstFile.Open(managedPath, ReaderOptions(s));
        const string filterBlock = BlockBasedTableConstants.FullFilterBlockPrefix + BloomLikeFilterPolicy.BuiltinFilterMetadataName;
        CollectionAssert.AreEqual(native.RawBlock(native.FindMeta(filterBlock)!.Value), managed.RawBlock(managed.FindMeta(filterBlock)!.Value), "filter block");
        TableProperties n = native.Properties, m = managed.Properties;
        Assert.AreEqual(n.FilterPolicyName, m.FilterPolicyName);
        Assert.AreEqual(n.FilterSize, m.FilterSize);
        Assert.AreEqual(n.NumFilterEntries, m.NumFilterEntries);
        CollectionAssert.AreEqual(native.RawBlock(native.Reader.IndexHandle), managed.RawBlock(managed.Reader.IndexHandle), "index block");
        VerifyManagedRead(managed, keys, absent);
        VerifyNativeIngest(dir.Path, s, managedPath, keys, absent);
    }

    /// <summary>
    /// Native compressed SSTs are read by the managed reader; managed compressed SSTs are ingested and read natively. The
    /// decompressed data blocks and the compression properties ("rocksdb.compression", "rocksdb.compression_options")
    /// match native.
    /// </summary>
    [TestMethod]
    [DataRow(CompressionType.Snappy, 5)]
    [DataRow(CompressionType.Snappy, 7)]
    [DataRow(CompressionType.LZ4, 6)]
    [DataRow(CompressionType.LZ4HC, 7)]
    [DataRow(CompressionType.Zstd, 7)]
    [DataRow(CompressionType.Zstd, 2)]
    [DataRow(CompressionType.Zlib, 5)]
    [DataRow(CompressionType.Zlib, 7)]
    public void CompressionMatchesNative(CompressionType compression, int formatVersion)
    {
        using var dir = new TempDir("compress-compat");
        List<byte[]> keys = Keys((int)compression * 100 + formatVersion);
        List<byte[]> absent = AbsentKeys(keys);
        var s = new Settings(formatVersion, IndexShorteningMode.ShortenSeparators, compression, null, null);
        using SstFile native = SstFile.Open(WriteNative(dir.Path, s, keys), ReaderOptions(s));
        VerifyManagedRead(native, keys, absent);

        string managedPath = WriteManaged(dir.Path, s, keys);
        using SstFile managed = SstFile.Open(managedPath, ReaderOptions(s));
        VerifyManagedRead(managed, keys, absent);

        List<BlockHandle> nh = native.DataBlockHandles(), mh = managed.DataBlockHandles();
        Assert.AreEqual(nh.Count, mh.Count);
        int nativeCompressed = 0, managedCompressed = 0;
        for (int i = 0; i < nh.Count; i++)
        {
            CompressionType nt = native.BlockCompressionType(nh[i]), mt = managed.BlockCompressionType(mh[i]);
            if (nt != CompressionType.None) { Assert.AreEqual(compression, nt); nativeCompressed++; }
            if (mt != CompressionType.None) { Assert.AreEqual(compression, mt); managedCompressed++; }
            CollectionAssert.AreEqual(native.UncompressedBlock(nh[i]), managed.UncompressedBlock(mh[i]), "data block " + i);
        }
        Assert.IsTrue(nativeCompressed > nh.Count / 2, "native compressed " + nativeCompressed + "/" + nh.Count);
        Assert.IsTrue(managedCompressed > mh.Count / 2, "managed compressed " + managedCompressed + "/" + mh.Count);

        TableProperties n = native.Properties, m = managed.Properties;
        Assert.AreEqual(n.CompressionName, m.CompressionName);
        Assert.AreEqual(n.CompressionOptions, m.CompressionOptions);
        Assert.AreEqual(n.UncompressedDataSize, m.UncompressedDataSize);
        VerifyNativeIngest(dir.Path, s, managedPath, keys, absent);
    }
}
