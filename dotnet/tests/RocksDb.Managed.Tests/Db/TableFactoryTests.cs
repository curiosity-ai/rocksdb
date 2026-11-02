using RocksDb.Managed.Db;
using RocksDb.Managed.Table;
using RocksDb.Managed.Table.Filters;
using CompressionType = RocksDb.Managed.Compression.CompressionType;

namespace RocksDb.Managed.Tests.Db;

/// <summary>Options model -> table runtime settings (<see cref="TableFactory"/>).</summary>
[TestClass]
public sealed class TableFactoryTests
{
    [TestMethod]
    public void DefaultsMapToTableDefaults()
    {
        TableFormatOptions f = TableFactory.CreateFormatOptions(new BlockBasedTableOptions());
        Assert.AreEqual(TableFormatOptions.Default with { FilterPolicy = null }, f);
        Assert.IsNull(f.FilterPolicy);
    }

    [TestMethod]
    public void AllFieldsAreMapped()
    {
        var o = new BlockBasedTableOptions
        {
            BlockSize = 16384, BlockSizeDeviation = 5, BlockRestartInterval = 4, IndexBlockRestartInterval = 2, MetadataBlockSize = 8192,
            FormatVersion = 5, Checksum = ChecksumType.Crc32c, IndexType = IndexType.BinarySearchWithFirstKey,
            IndexShortening = IndexShorteningMode.ShortenSeparatorsAndSuccessor, DataBlockIndexType = DataBlockIndexType.BinaryAndHash,
            DataBlockHashTableUtilRatio = 0.5, FilterPolicy = "bloomfilter:10:false", WholeKeyFiltering = false, UseDeltaEncoding = false,
            EnableIndexCompression = false,
        };
        TableFormatOptions f = TableFactory.CreateFormatOptions(o);
        Assert.AreEqual(16384, f.BlockSize);
        Assert.AreEqual(5, f.BlockSizeDeviation);
        Assert.AreEqual(4, f.BlockRestartInterval);
        Assert.AreEqual(2, f.IndexBlockRestartInterval);
        Assert.AreEqual(8192, f.MetadataBlockSize);
        Assert.AreEqual(5, f.FormatVersion);
        Assert.AreEqual(ChecksumType.Crc32c, f.Checksum);
        Assert.AreEqual(IndexType.BinarySearchWithFirstKey, f.IndexType);
        Assert.AreEqual(IndexShorteningMode.ShortenSeparatorsAndSuccessor, f.IndexShortening);
        Assert.AreEqual(DataBlockIndexType.BinaryAndHash, f.DataBlockIndexType);
        Assert.AreEqual(0.5, f.DataBlockHashTableUtilRatio);
        Assert.IsInstanceOfType<BloomFilterPolicy>(f.FilterPolicy);
        Assert.IsFalse(f.WholeKeyFiltering);
        Assert.IsFalse(f.UseDeltaEncoding);
        Assert.IsFalse(f.EnableIndexCompression);
    }

    [TestMethod]
    public void SanitizesLikeInitializeOptions()
    {
        TableFormatOptions f = TableFactory.CreateFormatOptions(new BlockBasedTableOptions
        {
            BlockSizeDeviation = 101, BlockRestartInterval = 0, IndexBlockRestartInterval = -3,
        });
        Assert.AreEqual(0, f.BlockSizeDeviation);
        Assert.AreEqual(1, f.BlockRestartInterval);
        Assert.AreEqual(1, f.IndexBlockRestartInterval);
        f = TableFactory.CreateFormatOptions(new BlockBasedTableOptions { IndexType = IndexType.HashSearch, IndexBlockRestartInterval = 8 });
        Assert.AreEqual(1, f.IndexBlockRestartInterval);
        Assert.AreEqual(StatusCode.InvalidArgument,
            Assert.ThrowsExactly<RocksDbException>(() => TableFactory.CreateFormatOptions(new BlockBasedTableOptions { BlockSize = 0 })).Code);
    }

    [TestMethod]
    [DataRow("bloomfilter:10:false", "bloomfilter:10:false", 10000)]
    [DataRow("bloomfilter:10", "bloomfilter:10:false", 10000)]
    [DataRow("bloomfilter:7.5:true", "bloomfilter:7.5:false", 7500)]
    [DataRow("rocksdb.BloomFilter:3.125:false", "bloomfilter:3.125:false", 3125)]
    [DataRow("ribbonfilter:10:-1", "ribbonfilter:10:-1", 10000)]
    [DataRow("ribbonfilter:8.5", "ribbonfilter:8.5:0", 8500)]
    [DataRow("rocksdb.RibbonFilter:10:3", "ribbonfilter:10:3", 10000)]
    public void FilterPolicyIds(string id, string canonical, int millibits)
    {
        var p = (BloomLikeFilterPolicy)TableFactory.CreateFilterPolicy(id)!;
        Assert.AreEqual(canonical, p.Id);
        Assert.AreEqual(millibits, p.MillibitsPerKey);
        Assert.AreEqual(BloomLikeFilterPolicy.BuiltinFilterMetadataName, p.CompatibilityName);
        // The canonical id round-trips.
        Assert.AreEqual(canonical, ((BloomLikeFilterPolicy)TableFactory.CreateFilterPolicy(canonical)!).Id);
    }

    [TestMethod]
    [DataRow(null)]
    [DataRow("")]
    [DataRow("nullptr")]
    public void NoFilterPolicy(string? id) => Assert.IsNull(TableFactory.CreateFilterPolicy(id));

    [TestMethod]
    [DataRow("bloomfilter")]
    [DataRow("bloomfilter:x:false")]
    [DataRow("bloomfilter:10:maybe")]
    [DataRow("ribbonfilter:10:x")]
    [DataRow("cuckoofilter:10")]
    [DataRow("ribbonfilter:10:1:2")]
    public void BadFilterPolicyIds(string id) =>
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => TableFactory.CreateFilterPolicy(id)).Code);

    [TestMethod]
    public void Comparators()
    {
        Assert.AreSame(Comparator.Bytewise, TableFactory.ResolveComparator("leveldb.BytewiseComparator"));
        Assert.AreSame(Comparator.ReverseBytewise, TableFactory.ResolveComparator("rocksdb.ReverseBytewiseComparator"));
        Assert.AreSame(Comparator.BytewiseWithU64Ts, TableFactory.ResolveComparator("leveldb.BytewiseComparator.u64ts"));
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => TableFactory.ResolveComparator("my.Comparator")).Code);
    }

    [TestMethod]
    public void CompressionSelection()
    {
        var cf = new ColumnFamilyOptions { Compression = CompressionType.Snappy };
        Assert.AreEqual(CompressionType.Snappy, TableFactory.GetCompressionFlush(cf));
        Assert.AreEqual(CompressionType.Snappy, TableFactory.GetCompressionType(cf, 3, 1, isBottommost: true, enableCompression: true));
        Assert.AreEqual(CompressionType.None, TableFactory.GetCompressionType(cf, 3, 1, isBottommost: false, enableCompression: false));

        cf.BottommostCompression = CompressionType.Zstd;
        Assert.AreEqual(CompressionType.Zstd, TableFactory.GetCompressionType(cf, 6, 1, isBottommost: true, enableCompression: true));
        Assert.AreEqual(CompressionType.Snappy, TableFactory.GetCompressionType(cf, 2, 1, isBottommost: false, enableCompression: true));

        // compression_per_level: index = level - base_level + 1 (L0 -> 0), clamped to [0, n-1].
        cf.CompressionPerLevel = [CompressionType.None, CompressionType.LZ4, CompressionType.Zlib];
        Assert.AreEqual(CompressionType.None, TableFactory.GetCompressionFlush(cf));
        Assert.AreEqual(CompressionType.None, TableFactory.GetCompressionType(cf, 0, 4, false, true));
        Assert.AreEqual(CompressionType.LZ4, TableFactory.GetCompressionType(cf, 4, 4, false, true));
        Assert.AreEqual(CompressionType.Zlib, TableFactory.GetCompressionType(cf, 5, 4, false, true));
        Assert.AreEqual(CompressionType.Zlib, TableFactory.GetCompressionType(cf, 6, 4, false, true));
        Assert.AreEqual(CompressionType.None, TableFactory.GetCompressionType(cf, -1, 1, false, true));

        cf.BottommostCompressionOpts.Level = 19;
        Assert.AreSame(cf.CompressionOpts, TableFactory.GetCompressionOptions(cf, isBottommost: true, enableCompression: true));
        cf.BottommostCompressionOpts.Enabled = true;
        Assert.AreSame(cf.BottommostCompressionOpts, TableFactory.GetCompressionOptions(cf, isBottommost: true, enableCompression: true));
        Assert.AreSame(cf.CompressionOpts, TableFactory.GetCompressionOptions(cf, isBottommost: false, enableCompression: true));
    }

    [TestMethod]
    public void BuilderAndReaderOptions()
    {
        var cf = new ColumnFamilyOptions
        {
            ComparatorName = "rocksdb.ReverseBytewiseComparator",
            MergeOperatorName = "StringAppendOperator",
            Compression = CompressionType.LZ4,
            BottommostCompression = CompressionType.Zstd,
        };
        cf.CompressionOpts.Level = 3;
        cf.TableOptions.FilterPolicy = "ribbonfilter:10:1";

        TableBuilderOptions flush = TableFactory.CreateBuilderOptions(cf, TableFileLevel.Flush);
        Assert.AreSame(Comparator.ReverseBytewise, flush.Comparator);
        Assert.AreEqual(CompressionType.LZ4, flush.CompressionType);
        Assert.AreEqual(3, flush.CompressionOptions.Level);
        Assert.AreEqual("StringAppendOperator", flush.MergeOperatorName);
        Assert.AreEqual("nullptr", flush.PrefixExtractorName);
        Assert.IsTrue(flush.IsFlush);
        Assert.AreEqual(0, flush.LevelAtCreation);

        TableBuilderOptions bottom = TableFactory.CreateBuilderOptions(cf, new TableFileLevel(6, 1, IsFlush: false, IsBottommost: true));
        Assert.AreEqual(CompressionType.Zstd, bottom.CompressionType);
        Assert.AreEqual(6, bottom.LevelAtCreation);
        Assert.IsFalse(bottom.IsFlush);

        // DisableCompressionOption as the base compression never reaches the builder.
        cf.Compression = CompressionType.DisableCompressionOption;
        Assert.AreEqual(CompressionType.None, TableFactory.CreateBuilderOptions(cf, TableFileLevel.Flush).CompressionType);

        TableFormatOptions format = TableFactory.CreateFormatOptions(cf.TableOptions);
        TableReaderOptions r = TableFactory.CreateReaderOptions(cf, format);
        Assert.AreSame(Comparator.ReverseBytewise, r.Comparator);
        Assert.AreSame(format.FilterPolicy, r.FilterPolicy);

        // Ribbon with bloom_before_level=1: flush (level -1) and L0 use Bloom, L1+ use Ribbon.
        var ribbon = (RibbonFilterPolicy)format.FilterPolicy!;
        using IFilterBitsBuilder fb = ribbon.CreateBuilder(new FilterBuildingContext(format.FormatVersion, flush.LevelAtCreation, flush.IsFlush));
        Assert.IsInstanceOfType<FastLocalBloomBitsBuilder>(fb);
        using IFilterBitsBuilder rb = ribbon.CreateBuilder(new FilterBuildingContext(format.FormatVersion, bottom.LevelAtCreation, bottom.IsFlush));
        Assert.IsInstanceOfType<Standard128RibbonBitsBuilder>(rb);
    }

    [TestMethod]
    public void ExternalSstFileCompression()
    {
        var cf = new ColumnFamilyOptions { Compression = CompressionType.Snappy };
        TableBuilderOptions t = TableFactory.CreateExternalSstFileBuilderOptions(cf);
        Assert.AreEqual(CompressionType.Snappy, t.CompressionType);
        Assert.IsTrue(t.ExternalSstFile);
        Assert.AreEqual("SST Writer", t.DbId);
        Assert.AreEqual(-1, t.LevelAtCreation);
        cf.CompressionPerLevel = [CompressionType.None, CompressionType.LZ4];
        Assert.AreEqual(CompressionType.LZ4, TableFactory.CreateExternalSstFileBuilderOptions(cf).CompressionType);
        cf.BottommostCompression = CompressionType.Zstd;
        Assert.AreEqual(CompressionType.Zstd, TableFactory.CreateExternalSstFileBuilderOptions(cf).CompressionType);
        Assert.AreSame(cf.CompressionOpts, TableFactory.CreateExternalSstFileBuilderOptions(cf).CompressionOptions);
        cf.BottommostCompressionOpts.Enabled = true;
        Assert.AreSame(cf.BottommostCompressionOpts, TableFactory.CreateExternalSstFileBuilderOptions(cf).CompressionOptions);
    }
}
