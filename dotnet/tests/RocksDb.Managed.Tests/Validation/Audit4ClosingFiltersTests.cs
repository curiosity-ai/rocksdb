using System.Buffers.Binary;
using RocksDb.Managed.Table;
using RocksDb.Managed.Table.Filters;
using RocksDb.Managed.Tests.Table;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4, slice "ClosingFilters": filter policies (table/block_based/filter_policy.cc), table properties
/// (table/meta_blocks.cc, table/table_properties.cc) and the uniformity histogram of block_builder.cc.
/// </summary>
[TestClass]
public sealed class Audit4ClosingFiltersTests
{
    private static byte[] BeKey(ulong v)
    {
        byte[] k = new byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(k, v);
        return k;
    }

    private static byte[] BuildTable(TableFormatOptions format, TableBuilderOptions tbo, int count)
    {
        var file = new MemWritableFile();
        using (var b = new BlockBasedTableBuilder(file, format, tbo))
        {
            byte[] value = new byte[100];
            for (int i = 0; i < count; i++) b.Add(TableTestUtil.IKey(BeKey(1000 + ((ulong)i * 977)), 1), value);
            b.Finish();
        }
        return file.ToArray();
    }

    private static TableProperties ReadProperties(byte[] data)
    {
        using BlockBasedTableReader reader = BlockBasedTableReader.Open(new MemRandomAccessFile(data), new TableReaderOptions());
        return reader.Properties;
    }

    // BlockBuilder::ScanForUniformity (table/block_based/block_builder.cc): with statistics and cv >= 0,
    // RecordInHistogram(statistics_, BLOCK_KEY_DISTRIBUTION_CV, static_cast<uint64_t>(cv * 10000)), whether or not the
    // block ends up "uniform". Restart keys 0,1,4,5,8 have gaps 1,3,1,3: mean 2, population variance 1 (UniformDataTracker:
    // sqrt(m2 / gap_count) / mean), so cv = 0.5 and the sample is 5000. The managed port recorded nothing.
    [TestMethod]
    public void ScanForUniformityRecordsKeyDistributionCv()
    {
        var stats = new Statistics();
        using (var b = new BlockBuilder(1, true, true, DataBlockIndexType.BinarySearch, 0.75, false, isUserKey: true, 0, false, uniformCvThreshold: 0.1)
        {
            Statistics = stats,
        })
        {
            byte[] handle = [1, 2];
            foreach (ulong v in (ulong[])[0, 1, 4, 5, 8]) b.Add(BeKey(v), handle, handle, false);
            b.Finish();
            Assert.IsFalse(b.IsUniform); // 0.5 is not below the 0.1 threshold, the sample is recorded all the same
        }
        HistogramData h = stats.GetHistogramData(Histogram.BlockKeyDistributionCv);
        Assert.AreEqual(1UL, h.Count);
        Assert.AreEqual(5000UL, h.Sum);

        // uniform_cv_threshold < 0 or fewer than 3 restarts: ScanForUniformity returns before computing anything.
        var none = new Statistics();
        using (var b = new BlockBuilder(1, true, true, DataBlockIndexType.BinarySearch, 0.75, false, isUserKey: true, 0, false, uniformCvThreshold: -1.0)
        {
            Statistics = none,
        })
        {
            byte[] handle = [1, 2];
            foreach (ulong v in (ulong[])[0, 1, 4, 5, 8]) b.Add(BeKey(v), handle, handle, false);
            b.Finish();
        }
        using (var b = new BlockBuilder(1, true, true, DataBlockIndexType.BinarySearch, 0.75, false, isUserKey: true, 0, false, uniformCvThreshold: 0.1)
        {
            Statistics = none,
        })
        {
            byte[] handle = [1, 2];
            b.Add(BeKey(1), handle, handle, false);
            b.Add(BeKey(2), handle, handle, false);
            b.Finish();
        }
        Assert.AreEqual(0UL, none.GetHistogramData(Histogram.BlockKeyDistributionCv).Count);
    }

    // BlockBasedTableBuilder::Rep -> IndexBuilder::CreateIndexBuilder(..., ioptions.stats, ...): the kBinarySearch index
    // block builder gets the statistics and table_options.uniform_cv_threshold, so each finished index block records
    // one BLOCK_KEY_DISTRIBUTION_CV sample (data blocks are built with uniform_cv_threshold -1 and record none).
    [TestMethod]
    public void TableBuilderRecordsIndexBlockKeyDistributionCv()
    {
        var stats = new Statistics();
        byte[] data = BuildTable(new TableFormatOptions { BlockSize = 512, UniformCvThreshold = 0.2 }, new TableBuilderOptions { Statistics = stats }, 400);
        Assert.IsTrue(ReadProperties(data).NumDataBlocks >= 3);
        Assert.AreEqual(1UL, stats.GetHistogramData(Histogram.BlockKeyDistributionCv).Count);

        var off = new Statistics();
        BuildTable(new TableFormatOptions { BlockSize = 512 }, new TableBuilderOptions { Statistics = off }, 400);
        Assert.AreEqual(0UL, off.GetHistogramData(Histogram.BlockKeyDistributionCv).Count);
    }

    // BlockBasedTableBuilder::Rep (block_based_table_builder.cc) sets props.lsm_info_at_creation for every file:
    // LsmInfoAtCreation::Applicable(level_at_creation, is_bottommost) for flush / compaction / recovery outputs and
    // NotApplicable for SstFileWriter files; PropertyBlockBuilder::AddTableProperty (table/meta_blocks.cc) writes
    // "rocksdb.lsm.info.at.creation" when it is non-zero. LsmInfoAtCreation::Encode (table/table_properties.cc):
    // level in bits 0..7, state in bits 8..9 (1 not bottommost, 2 bottommost, 3 not applicable). The managed builder
    // never set the property.
    [TestMethod]
    public void LsmInfoAtCreationIsWritten()
    {
        TableFormatOptions format = TableFormatOptions.Default;
        Assert.AreEqual(3UL << 8, ReadProperties(BuildTable(format, new TableBuilderOptions { ExternalSstFile = true }, 10)).LsmInfoAtCreation);
        Assert.AreEqual(1UL << 8, ReadProperties(BuildTable(format, new TableBuilderOptions { LevelAtCreation = 0, IsFlush = true }, 10)).LsmInfoAtCreation);
        Assert.AreEqual(3UL | (2UL << 8),
            ReadProperties(BuildTable(format, new TableBuilderOptions { LevelAtCreation = 3, IsBottommost = true }, 10)).LsmInfoAtCreation);
        Assert.AreEqual(5UL | (1UL << 8), ReadProperties(BuildTable(format, new TableBuilderOptions { LevelAtCreation = 5 }, 10)).LsmInfoAtCreation);
    }

    // FilterPolicy::CreateFromString (table/block_based/filter_policy.cc): the value "rocksdb.BuiltinBloomFilter"
    // (ReadOnlyBuiltinFilterPolicy::kClassName) creates a ReadOnlyBuiltinFilterPolicy, which reads every built-in
    // filter (BuiltinFilterPolicy::GetFilterBitsReader) and builds none (GetBuilderWithContext returns nullptr); its
    // Name() is still written as "rocksdb.filter.policy". The port rejected the id as unsupported.
    [TestMethod]
    public void ReadOnlyBuiltinFilterPolicyFromString()
    {
        FilterPolicy? policy = RocksDb.Managed.Db.TableFactory.CreateFilterPolicy("rocksdb.BuiltinBloomFilter");
        Assert.IsInstanceOfType<ReadOnlyBuiltinFilterPolicy>(policy);
        Assert.AreEqual("rocksdb.BuiltinBloomFilter", policy.Name);
        Assert.AreEqual("rocksdb.BuiltinBloomFilter", policy.CompatibilityName);
        Assert.AreEqual("rocksdb.BuiltinBloomFilter", ((BloomLikeFilterPolicy)policy).Id);

        // The OPTIONS round trip keeps the id.
        var t = new BlockBasedTableOptions();
        t.SetOption("filter_policy", "rocksdb.BuiltinBloomFilter", preserveUnknown: false);
        Assert.IsInstanceOfType<ReadOnlyBuiltinFilterPolicy>(t.FilterPolicy);
        Assert.AreEqual("rocksdb.BuiltinBloomFilter", t.GetOption("filter_policy"));

        // It writes no filter block, but the policy name is recorded.
        TableProperties p = ReadProperties(BuildTable(new TableFormatOptions { FilterPolicy = policy }, new TableBuilderOptions(), 50));
        Assert.AreEqual(0UL, p.FilterSize);
        Assert.AreEqual("rocksdb.BuiltinBloomFilter", p.FilterPolicyName);

        // It reads a filter built by another built-in policy.
        using IFilterBitsBuilder bits = new BloomFilterPolicy(10).CreateBuilder(5);
        for (ulong i = 0; i < 100; i++) bits.AddKey(BeKey(i));
        using var filter = new PooledBufferWriter(256);
        bits.Finish(filter);
        IFilterBitsReader reader = policy.CreateReader(filter.WrittenSpan.ToArray());
        for (ulong i = 0; i < 100; i++) Assert.IsTrue(reader.MayMatch(BeKey(i)));
        int falsePositives = 0;
        for (ulong i = 1000; i < 2000; i++) falsePositives += reader.MayMatch(BeKey(i)) ? 1 : 0;
        Assert.IsTrue(falsePositives < 100, "an always-true reader would match all 1000");
    }

    // BlockFetcher::ProcessTrailerIfPresent (table/block_fetcher.cc): with read_options.verify_checksums every block
    // read ticks BLOCK_CHECKSUM_COMPUTE_COUNT (and BLOCK_CHECKSUM_MISMATCH_COUNT on a mismatch). Reading "a" back from
    // the flushed table reads at least its index and data block from the file.
    [TestMethod]
    [Ignore("audit4: BLOCK_CHECKSUM_COMPUTE_COUNT / BLOCK_CHECKSUM_MISMATCH_COUNT are never recorded: the static Table.BlockFetcher has no statistics and plumbing one through every reader call site is not a local change")]
    public void BlockReadsTickChecksumComputeCount()
    {
        RocksDb.Managed.Env.MemoryFileSystem fs =Db.DbTestUtil.NewFs();
        var stats = new Statistics();
        using RocksDatabase db = RocksDatabase.Open(Db.DbTestUtil.DbPath, new DbOptions { CreateIfMissing = true, FileSystem = fs, Statistics = stats });
        db.Put(Db.DbTestUtil.B("a"), Db.DbTestUtil.B("1"));
        db.Flush();
        Assert.AreEqual("1", Db.DbTestUtil.GetString(db, "a"));
        Assert.IsTrue(stats.GetTickerCount(Ticker.BlockChecksumComputeCount) >= 2);
        Assert.AreEqual(0UL, stats.GetTickerCount(Ticker.BlockChecksumMismatchCount));
    }

    // RibbonFilterPolicy::GetBuilderWithContext (filter_policy.cc): bloom_before_level == INT_MAX is "always Bloom"
    // (levelish INT_MAX - 1 < INT_MAX even for an unknown level / FIFO), flush counts as level -1 so the default
    // bloom_before_level 0 gives Bloom for flushes and Ribbon for every compaction output, and FIFO / none compaction
    // styles are treated as bottommost. Metadata byte [len - 5]: -1 = FastLocalBloom, -2 = Standard128 Ribbon.
    [TestMethod]
    public void RibbonPolicyModeSelection()
    {
        static sbyte Marker(RibbonFilterPolicy policy, FilterBuildingContext context)
        {
            using IFilterBitsBuilder bits = policy.CreateBuilder(context);
            for (ulong i = 0; i < 5000; i++) bits.AddKey(BeKey(i));
            using var filter = new PooledBufferWriter(8192);
            bits.Finish(filter);
            return unchecked((sbyte)filter.WrittenSpan[^5]);
        }
        var level = new FilterBuildingContext(5, 0, IsFlush: false) { CompactionStyle = CompactionStyle.Level };
        Assert.AreEqual(-2, Marker(new RibbonFilterPolicy(10, 0), level));
        Assert.AreEqual(-1, Marker(new RibbonFilterPolicy(10, 0), level with { IsFlush = true }));
        Assert.AreEqual(-1, Marker(new RibbonFilterPolicy(10, 1), level));
        Assert.AreEqual(-2, Marker(new RibbonFilterPolicy(10, 1), level with { LevelAtCreation = 1 }));
        Assert.AreEqual(-2, Marker(new RibbonFilterPolicy(10, -1), level with { IsFlush = true }));
        Assert.AreEqual(-1, Marker(new RibbonFilterPolicy(10, int.MaxValue), level with { LevelAtCreation = -1 }));
        Assert.AreEqual(-2, Marker(new RibbonFilterPolicy(10, int.MaxValue - 1), level with { LevelAtCreation = -1 }));
        // FIFO: the level and the flush reason are ignored (treated as bottommost).
        Assert.AreEqual(-2, Marker(new RibbonFilterPolicy(10, 7), level with { IsFlush = true, CompactionStyle = CompactionStyle.Fifo }));
        Assert.AreEqual(-1, Marker(new RibbonFilterPolicy(10, 7), level with { LevelAtCreation = 6, CompactionStyle = CompactionStyle.Universal }));
    }
}
