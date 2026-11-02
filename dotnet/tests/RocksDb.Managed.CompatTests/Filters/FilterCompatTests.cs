using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Table.Filters;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.CompatTests.Filters;

/// <summary>
/// Filters built by native RocksDB are read by the managed readers, and the managed builders produce
/// byte-identical filter blocks for the same keys and settings.
/// </summary>
[TestClass]
public sealed class FilterCompatTests
{
    private const string FilterBlockName = "fullfilter." + BloomLikeFilterPolicy.BuiltinFilterMetadataName;
    private const int KeyCount = 20000;

    private static void RunCase(Func<IntPtr> nativePolicy, BloomLikeFilterPolicy managedPolicy, int formatVersion, Type expectedReader)
    {
        using var dir = new TempDir("filter");
        TestSstParser sst = NativeSst.Build(dir, nativePolicy(), formatVersion, RocksDbSharp.Compression.No, KeyCount);
        Assert.AreEqual((uint)formatVersion, sst.FormatVersion);
        var handle = sst.FindMeta(FilterBlockName);
        Assert.IsNotNull(handle, "no " + FilterBlockName + " in metaindex");
        Assert.AreEqual(RocksDb.Managed.Compression.CompressionType.None, sst.BlockCompressionType(handle.Value));
        byte[] nativeFilter = sst.RawBlock(handle.Value);

        // (i) managed reader over native filter
        IFilterBitsReader reader = managedPolicy.CreateReader(nativeFilter);
        Assert.IsInstanceOfType(reader, expectedReader);
        for (int i = 0; i < KeyCount; i++)
        {
            Assert.IsTrue(reader.MayMatch(NativeSst.Key(i)), "false negative for key " + i);
        }
        int fp = 0;
        const int probes = 20000;
        for (int i = 0; i < probes; i++)
        {
            if (reader.MayMatch(NativeSst.Key(KeyCount + 1000 + i))) fp++;
        }
        // ~1% at 10 bits/key, ~3% at 7.5
        double maxFpRate = managedPolicy.MillibitsPerKey >= 10000 ? 0.02 : 0.05;
        Assert.IsTrue(fp < probes * maxFpRate, "FP rate too high: " + fp + "/" + probes);

        // (ii) managed builder (flush context, same key order) == native bytes
        using IFilterBitsBuilder builder = managedPolicy.CreateBuilder(new FilterBuildingContext(formatVersion, 0, IsFlush: true));
        for (int i = 0; i < KeyCount; i++)
        {
            builder.AddKey(NativeSst.Key(i));
        }
        using var w = new PooledBufferWriter();
        builder.Finish(w);
        CollectionAssert.AreEqual(nativeFilter, w.WrittenSpan.ToArray(), "managed filter differs from native");
    }

    [TestMethod]
    public void FastLocalBloomFormatVersion5()
        => RunCase(() => RocksDbSharp.Native.Instance.rocksdb_filterpolicy_create_bloom_full(10), new BloomFilterPolicy(10), 5, typeof(FastLocalBloomBitsReader));

    [TestMethod]
    public void FastLocalBloomFormatVersion6FractionalBits()
        => RunCase(() => RocksDbSharp.Native.Instance.rocksdb_filterpolicy_create_bloom_full(7.5), new BloomFilterPolicy(7.5), 6, typeof(FastLocalBloomBitsReader));

    [TestMethod]
    public void LegacyBloomFormatVersion4()
        => RunCase(() => RocksDbSharp.Native.Instance.rocksdb_filterpolicy_create_bloom(10), new BloomFilterPolicy(10), 4, typeof(LegacyBloomBitsReader));

    [TestMethod]
    public void RibbonFormatVersion5()
        => RunCase(() => RocksDbSharp.Native.Instance.rocksdb_filterpolicy_create_ribbon(10), new RibbonFilterPolicy(10, -1), 5, typeof(Standard128RibbonBitsReader));

    [TestMethod]
    public void RibbonFormatVersion6FractionalBits()
        => RunCase(() => RocksDbSharp.Native.Instance.rocksdb_filterpolicy_create_ribbon(8.3), new RibbonFilterPolicy(8.3, -1), 6, typeof(Standard128RibbonBitsReader));

    /// <summary>Ribbon hybrid with bloom_before_level=0: flushes (level -1) use FastLocalBloom.</summary>
    [TestMethod]
    public void RibbonHybridFlushUsesBloom()
        => RunCase(() => RocksDbSharp.Native.Instance.rocksdb_filterpolicy_create_ribbon_hybrid(10, 0), new RibbonFilterPolicy(10, 0), 5, typeof(FastLocalBloomBitsReader));
}
