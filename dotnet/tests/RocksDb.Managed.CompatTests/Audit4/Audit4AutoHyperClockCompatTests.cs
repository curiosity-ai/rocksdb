using RocksDb.Managed.Cache;

namespace RocksDb.Managed.CompatTests.Audit4;

/// <summary>
/// Audit #4, automatic HyperClockCache table (estimated_entry_charge = 0) against native librocksdb: what the C API
/// shows of a new cache (capacity, shard count through the table address count, the initial table and its metadata
/// charge). The C API has no insert, so the table's behaviour under load is covered by the unit tests.
/// </summary>
[TestClass]
public sealed class Audit4AutoHyperClockCompatTests
{
    /// <summary>
    /// AutoHyperClockTable::AutoHyperClockTable / GetStartingLength (cache/clock_cache.cc:1711-1719, 1939-1967) and
    /// HyperClockCacheOptions::MakeSharedCache (3610-3634): a shard starts with one page of slots (64) and charges their
    /// metadata; the default shard count gives every shard at least 32 MiB.
    /// </summary>
    [TestMethod]
    [DataRow(1L << 30)]
    [DataRow(256L << 20)]
    [DataRow(64L << 20)]
    [DataRow(8L << 20)]
    [DataRow(100_000L)]
    [DataRow(5000L)]
    [DataRow(4096L)]
    [DataRow(1000L)]
    public void NewAutoCacheMatchesNative(long capacity)
    {
        RocksDbSharp.Native native = RocksDbSharp.Native.Instance;
        IntPtr ncache = native.rocksdb_cache_create_hyper_clock((UIntPtr)capacity, UIntPtr.Zero);
        try
        {
            var c = new HyperClockCache(new HyperClockCacheOptions { Capacity = capacity, EstimatedEntryCharge = 0 });
            Assert.IsTrue(c.IsAutoResizing);
            Assert.AreEqual((long)(ulong)native.rocksdb_cache_get_capacity(ncache), c.Capacity);
            Assert.AreEqual((long)(ulong)native.rocksdb_cache_get_table_address_count(ncache), (long)c.NumShards * c.TableSize);
            Assert.AreEqual((long)(ulong)native.rocksdb_cache_get_occupancy_count(ncache), c.Count);
            Assert.AreEqual((long)(ulong)native.rocksdb_cache_get_usage(ncache), c.Usage);
            Assert.AreEqual((long)(ulong)native.rocksdb_cache_get_pinned_usage(ncache), c.PinnedUsage);
        }
        finally
        {
            native.rocksdb_cache_destroy(ncache);
        }
    }
}
