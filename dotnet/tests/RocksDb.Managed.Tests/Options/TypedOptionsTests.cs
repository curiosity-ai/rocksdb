using RocksDb.Managed.Db;
using RocksDb.Managed.Table.Filters;

namespace RocksDb.Managed.Tests.Options;

/// <summary>
/// Typed pluggable objects and factory enums in the options model, and their byte-identical OPTIONS-file round trip.
/// </summary>
[TestClass]
public sealed class TypedOptionsTests
{
    private static BlockBasedTableOptions TableWithFilter(string text)
    {
        var t = new BlockBasedTableOptions();
        t.SetOption("filter_policy", text, preserveUnknown: false);
        return t;
    }

    private static ColumnFamilyOptions CfWith(string name, string text)
    {
        var o = new ColumnFamilyOptions();
        o.SetOption(name, text, preserveUnknown: false);
        return o;
    }

    [TestMethod]
    [DataRow("bloomfilter:10:false", typeof(BloomFilterPolicy))]
    [DataRow("bloomfilter:10", typeof(BloomFilterPolicy))]
    [DataRow("rocksdb.BloomFilter:7.5:true", typeof(BloomFilterPolicy))]
    [DataRow("ribbonfilter:10", typeof(RibbonFilterPolicy))]
    [DataRow("{id=ribbonfilter:10:0;bloom_before_level=2;}", typeof(RibbonFilterPolicy))]
    public void FilterPolicyParsesToObjectAndKeepsText(string text, Type expected)
    {
        BlockBasedTableOptions t = TableWithFilter(text);
        Assert.IsInstanceOfType(t.FilterPolicy, expected);
        Assert.IsNull(t.UnresolvedFilterPolicyId);
        Assert.AreEqual(text, t.GetOption("filter_policy"));
        Assert.AreEqual(text, t.Clone().GetOption("filter_policy"));
        Assert.AreSame(t.FilterPolicy, t.Clone().FilterPolicy);
        Assert.AreSame(t.FilterPolicy, TableFactory.CreateFormatOptions(t).FilterPolicy);
    }

    [TestMethod]
    public void FilterPolicyStructFormAppliesOptions()
    {
        BlockBasedTableOptions t = TableWithFilter("{id=ribbonfilter:10:0;bloom_before_level=2;}");
        Assert.AreEqual(2, ((RibbonFilterPolicy)t.FilterPolicy!).BloomBeforeLevel);
    }

    [TestMethod]
    public void FilterPolicySerializesObjectIdAfterChange()
    {
        BlockBasedTableOptions t = TableWithFilter("{id=ribbonfilter:10:0;bloom_before_level=0;}");
        ((RibbonFilterPolicy)t.FilterPolicy!).BloomBeforeLevel = 3; // reconfigured: the parsed text is stale
        Assert.AreEqual("ribbonfilter:10:3", t.GetOption("filter_policy"));

        t.FilterPolicy = new BloomFilterPolicy(10);
        Assert.AreEqual("bloomfilter:10:false", t.GetOption("filter_policy"));
        t.FilterPolicy = null;
        Assert.AreEqual("nullptr", t.GetOption("filter_policy"));
        Assert.AreEqual("nullptr", new BlockBasedTableOptions().GetOption("filter_policy"));
    }

    [TestMethod]
    public void UnknownFilterPolicyIsPreservedAndFailsAtOpen()
    {
        BlockBasedTableOptions t = TableWithFilter("my.custom.Filter:3");
        Assert.IsNull(t.FilterPolicy);
        Assert.AreEqual("my.custom.Filter:3", t.UnresolvedFilterPolicyId);
        Assert.AreEqual("my.custom.Filter:3", t.GetOption("filter_policy"));
        Assert.AreEqual("my.custom.Filter:3", t.Clone().GetOption("filter_policy"));
        var ex = Assert.ThrowsExactly<RocksDbException>(() => TableFactory.CreateFormatOptions(t));
        Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);

        // Setting null clears the preserved id too.
        t.FilterPolicy = null;
        Assert.IsNull(t.UnresolvedFilterPolicyId);
        Assert.AreEqual("nullptr", t.GetOption("filter_policy"));
    }

    [TestMethod]
    public void BareFilterClassNameMeansNoFilter()
    {
        // What native writes for policies created through the C API.
        BlockBasedTableOptions t = TableWithFilter("bloomfilter");
        Assert.IsNull(t.FilterPolicy);
        Assert.AreEqual("bloomfilter", t.GetOption("filter_policy"));
        Assert.IsNull(TableFactory.CreateFormatOptions(t).FilterPolicy);
    }

    [TestMethod]
    public void MergeOperatorBuiltinsAreRecreated()
    {
        ColumnFamilyOptions o = CfWith("merge_operator", "{id=StringAppendOperator;delimiter=|;}");
        var sa = Assert.IsInstanceOfType<StringAppendOperator>(o.MergeOperator);
        CollectionAssert.AreEqual("|"u8.ToArray(), sa.Delimiter.ToArray());
        Assert.AreEqual("StringAppendOperator", o.MergeOperatorName);
        Assert.AreEqual("{id=StringAppendOperator;delimiter=|;}", o.GetOption("merge_operator"));
        Assert.AreSame(o.MergeOperator, o.Clone().MergeOperator);

        Assert.AreSame(UInt64AddOperator.Instance, CfWith("merge_operator", "UInt64AddOperator").MergeOperator);
        Assert.AreSame(UInt64AddOperator.Instance, CfWith("merge_operator", "uint64add").MergeOperator);
        Assert.AreSame(UInt64AddOperator.Instance, MergeOperator.UInt64Add);
        Assert.IsInstanceOfType<StringAppendOperator>(MergeOperator.CreateBuiltin("stringappend"));
        Assert.IsNull(MergeOperator.CreateBuiltin("MyOperator"));
    }

    [TestMethod]
    public void UnknownMergeOperatorKeepsName()
    {
        ColumnFamilyOptions o = CfWith("merge_operator", "{id=MyOperator;x=1;}");
        Assert.IsNull(o.MergeOperator);
        Assert.AreEqual("MyOperator", o.MergeOperatorName);
        Assert.AreEqual("{id=MyOperator;x=1;}", o.GetOption("merge_operator"));

        o.MergeOperator = MergeOperator.StringAppend(",");
        Assert.AreEqual("{id=StringAppendOperator;delimiter=,;}", o.GetOption("merge_operator"));
        o.MergeOperator = null;
        Assert.IsNull(o.MergeOperatorName);
        Assert.AreEqual("nullptr", o.GetOption("merge_operator"));
    }

    [TestMethod]
    public void CompactionFilterNamesArePersistedOnly()
    {
        ColumnFamilyOptions o = CfWith("compaction_filter", "SomeNativeFilter");
        Assert.IsNull(o.CompactionFilter);
        Assert.AreEqual("SomeNativeFilter", o.CompactionFilterName);
        Assert.AreEqual("SomeNativeFilter", o.GetOption("compaction_filter"));
        Assert.AreEqual("nullptr", new ColumnFamilyOptions().GetOption("compaction_filter_factory"));
    }

    [TestMethod]
    public void PrefixExtractorUnknownIdIsPreserved()
    {
        ColumnFamilyOptions o = CfWith("prefix_extractor", "rocksdb.FixedPrefix.4");
        Assert.AreEqual("rocksdb.FixedPrefix.4", o.PrefixExtractor!.AsString());
        o = CfWith("prefix_extractor", "my.CustomTransform");
        Assert.IsNull(o.PrefixExtractor);
        Assert.AreEqual("my.CustomTransform", o.GetOption("prefix_extractor"));
        o = CfWith("memtable_insert_with_hint_prefix_extractor", "rocksdb.CappedPrefix.3");
        Assert.AreEqual("rocksdb.CappedPrefix.3", o.MemtableInsertWithHintPrefixExtractor!.AsString());
    }

    [TestMethod]
    [DataRow("memtable_factory", "SkipListFactory", "SkipListFactory")]
    [DataRow("memtable_factory", "{id=SkipListFactory;lookahead=0;}", "SkipListFactory")]
    [DataRow("memtable_factory", "skip_list:16", "SkipListFactory")]
    [DataRow("memtable_factory", "VectorRepFactory", "VectorRepFactory")]
    [DataRow("memtable_factory", "prefix_hash:1000", "HashSkipListRepFactory")]
    [DataRow("memtable_factory", "hash_linkedlist", "HashLinkListRepFactory")]
    [DataRow("table_factory", "BlockBasedTable", "BlockBasedTable")]
    [DataRow("table_factory", "PlainTable", "PlainTable")]
    [DataRow("table_factory", "{id=CuckooTable;}", "CuckooTable")]
    public void FactoryEnumsParseAndSerialize(string option, string text, string written)
    {
        ColumnFamilyOptions o = CfWith(option, text);
        Assert.AreEqual(written, o.GetOption(option));
    }

    [TestMethod]
    public void FactoryEnumDefaultsAndUnknownValues()
    {
        var o = new ColumnFamilyOptions();
        Assert.AreEqual(MemTableFactoryType.SkipList, o.MemtableFactory);
        Assert.AreEqual(TableFactoryType.BlockBasedTable, o.TableFactory);
        Assert.AreEqual(FlushBlockPolicyType.BySize, o.TableOptions.FlushBlockPolicy);
        Assert.AreEqual("FlushBlockBySizePolicyFactory", o.TableOptions.GetOption("flush_block_policy_factory"));
        o.TableOptions.SetOption("flush_block_policy_factory", "FlushBlockEveryKeyPolicyFactory", preserveUnknown: false);
        Assert.AreEqual(FlushBlockPolicyType.EveryKey, o.TableOptions.FlushBlockPolicy);

        var ex = Assert.ThrowsExactly<RocksDbException>(() => o.SetOption("memtable_factory", "NoSuchFactory", preserveUnknown: true));
        Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);
        ex = Assert.ThrowsExactly<RocksDbException>(() => o.SetOption("table_factory", "NoSuchTable", preserveUnknown: true));
        Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);
    }

    [TestMethod]
    public void DbPropertyNames()
    {
        Assert.AreEqual("rocksdb.num-files-at-level3", DbProperties.NumFilesAtLevel(3));
        Assert.AreEqual("rocksdb.estimate-num-keys", DbProperties.EstimateNumKeys);
        Assert.ThrowsExactly<ArgumentOutOfRangeException>(() => DbProperties.NumFilesAtLevel(-1));

        string dir = Path.Combine(Path.GetTempPath(), "rocksdb-managed-dbprops-" + Guid.NewGuid().ToString("N"));
        try
        {
            using RocksDatabase db = RocksDatabase.Open(dir, new DbOptions { CreateIfMissing = true });
            db.Put("a"u8, "1"u8);
            Assert.IsTrue(db.TryGetIntProperty(DbProperties.NumEntriesActiveMemTable, out ulong n));
            Assert.AreEqual(1UL, n);
            Assert.IsTrue(db.TryGetIntProperty(DbProperties.NumFilesAtLevel(0), out n));
            Assert.AreEqual(0UL, n);
            Assert.IsFalse(db.TryGetIntProperty(DbProperties.LevelStats, out _)); // not numeric
            Assert.IsFalse(db.TryGetIntProperty("rocksdb.no-such-property", out _));
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }
}
