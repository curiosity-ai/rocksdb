using System.Buffers;
using System.Text;
using RocksDb.Managed.Cache;
using RocksDb.Managed.Db;
using RocksDb.Managed.Table;
using RocksDb.Managed.Table.Filters;
using RocksDb.Managed.Tests.Table;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4 follow-up (filters): reader-side behaviour of full / partitioned filters and the built-in filter bits
/// readers. Each test names the C++ function it pins down.
/// </summary>
[TestClass]
public sealed class Audit4FollowUpFiltersTests
{
    private static readonly string PartitionedName =
        BlockBasedTableConstants.PartitionedFilterBlockPrefix + BloomLikeFilterPolicy.BuiltinFilterMetadataName;

    private static byte[] BuildPartitionedFilterTable(out TableFormatOptions format)
    {
        format = TableFactory.CreateFormatOptions(new BlockBasedTableOptions
        {
            BlockSize = 256,
            IndexType = IndexType.TwoLevelIndexSearch,
            PartitionFilters = true,
            MetadataBlockSize = 256,
            FilterPolicy = new BloomFilterPolicy(10),
        });
        var entries = new List<(byte[] Key, byte[] Value)>();
        for (int i = 0; i < 400; i++) entries.Add((TableTestUtil.IKey("key" + i.ToString("D4"), 7), B("v" + i)));
        return TableTestUtil.BuildTable(entries, format);
    }

    private static void AssertReadableWithoutFilter(BlockBasedTableReader reader, bool expectNoFilter)
    {
        var value = new ArrayBufferWriter<byte>();
        Assert.IsTrue(reader.Get(TableTestUtil.LookupKey("key0399"), value).Found);
        Assert.AreEqual("v399", Encoding.UTF8.GetString(value.WrittenSpan));
        Assert.IsTrue(reader.Get(TableTestUtil.LookupKey("key0000"), null).Found);
        Assert.IsFalse(reader.Get(TableTestUtil.LookupKey("key0399x"), null).Found);
        if (expectNoFilter) Assert.IsTrue(reader.KeyMayMatch(B("zzz-absent")), "without a readable filter every key may match");
    }

    /// <summary>
    /// PartitionedFilterBlockReader::Create (table/block_based/partitioned_filter_block.cc:371-378): a top-level
    /// filter partition index that cannot be read is ignored (IGNORE_STATUS_IF_ERROR, no filter reader is created)
    /// and the table opens without a filter. The port failed the table open with Corruption.
    /// </summary>
    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void UnreadablePartitionedFilterTopLevelIsIgnored(bool cacheIndexAndFilterBlocks)
    {
        byte[] data = BuildPartitionedFilterTable(out TableFormatOptions format);
        BlockHandle top;
        using (BlockBasedTableReader intact = TableTestUtil.Open(data, new TableReaderOptions { FilterPolicy = format.FilterPolicy }))
        {
            Assert.IsTrue(intact.HasFilter);
            top = intact.MetaBlocks[PartitionedName];
            Assert.IsFalse(intact.KeyMayMatch(B("zzz-absent")), "the intact filter excludes an absent key");
        }
        data[(int)top.Offset + 2] ^= 0x40; // checksum mismatch

        var options = new TableReaderOptions { FilterPolicy = format.FilterPolicy };
        if (cacheIndexAndFilterBlocks) options = options with { BlockCache = BlockCache.CreateLru(1 << 20), CacheIndexAndFilterBlocks = true };
        using BlockBasedTableReader reader = TableTestUtil.Open(data, options);
        AssertReadableWithoutFilter(reader, expectNoFilter: true);
    }

    /// <summary>
    /// PartitionedFilterBlockReader::MayMatch (partitioned_filter_block.cc:523-530): a filter partition that cannot be
    /// read answers "may match". Without a block cache BlockBasedTable::MaybeReadBlockAndLoadToCache is a no-op, so
    /// CacheDependencies (:688-712) reads no partition at open and the table opens. The port loaded (pinned) every
    /// partition at open and failed it with Corruption.
    /// </summary>
    [TestMethod]
    public void UnreadableFilterPartitionWithoutBlockCacheIsIgnored()
    {
        byte[] data = BuildPartitionedFilterTable(out TableFormatOptions format);
        BlockHandle top;
        using (BlockBasedTableReader intact = TableTestUtil.Open(data, new TableReaderOptions { FilterPolicy = format.FilterPolicy }))
        {
            top = intact.MetaBlocks[PartitionedName];
            Assert.IsFalse(intact.KeyMayMatch(B("key0399x")), "the intact last partition excludes an absent key");
        }
        // The last filter partition ends right before the top-level partition index (block trailer = 5 bytes).
        data[(int)top.Offset - BlockBasedTableConstants.BlockTrailerSize - 3] ^= 0x40;

        using BlockBasedTableReader reader = TableTestUtil.Open(data, new TableReaderOptions { FilterPolicy = format.FilterPolicy });
        AssertReadableWithoutFilter(reader, expectNoFilter: false);
        Assert.IsTrue(reader.KeyMayMatch(B("key0399x")), "a key of the unreadable partition may match");
        Assert.IsFalse(reader.KeyMayMatch(B("key0000x")), "intact partitions still filter");
    }
}
