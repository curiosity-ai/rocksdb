using RocksDb.Managed.CompatTests.Infrastructure;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>
/// MemoryUtil::GetApproximateMemoryUsageByType compared with native RocksDB 11.12 through the C API
/// (rocksdb_approximate_memory_usage_*): the same workload on both engines moves every usage type the same way
/// (memtables grow with writes and drop at flush, table readers appear after flush, the cache total is the cache's
/// usage), and the memtable usage is within a small factor of native's (the arenas differ in detail).
/// </summary>
[TestClass]
public sealed class MemoryUtilCompatTests
{
    private readonly record struct Usage(ulong MemTotal, ulong MemUnflushed, ulong Readers, ulong Cache);

    private static Usage NativeUsage(RocksDbSharp.RocksDb db, RocksDbSharp.Cache cache)
    {
        RocksDbSharp.Native n = RocksDbSharp.Native.Instance;
        IntPtr consumers = n.rocksdb_memory_consumers_create();
        try
        {
            n.rocksdb_memory_consumers_add_db(consumers, db.Handle);
            n.rocksdb_memory_consumers_add_cache(consumers, cache.Handle);
            n.rocksdb_memory_consumers_add_cache(consumers, cache.Handle); // a set: counted once
            IntPtr usage = n.rocksdb_approximate_memory_usage_create(consumers);
            try
            {
                return new Usage(n.rocksdb_approximate_memory_usage_get_mem_table_total(usage), n.rocksdb_approximate_memory_usage_get_mem_table_unflushed(usage),
                    n.rocksdb_approximate_memory_usage_get_mem_table_readers_total(usage), n.rocksdb_approximate_memory_usage_get_cache_total(usage));
            }
            finally
            {
                n.rocksdb_approximate_memory_usage_destroy(usage);
            }
        }
        finally
        {
            n.rocksdb_memory_consumers_destroy(consumers);
        }
    }

    private static Usage ManagedUsage(RocksDatabase db, Cache.BlockCache cache)
    {
        MemoryUsageByType u = MemoryUtil.GetApproximateMemoryUsageByType([db], [cache, cache]);
        return new Usage(u.MemTableTotal, u.MemTableUnflushed, u.TableReadersTotal, u.CacheTotal);
    }

    private static byte[] Key(int i) => System.Text.Encoding.ASCII.GetBytes("key" + i.ToString("D6", System.Globalization.CultureInfo.InvariantCulture));

    private static byte[] Value(int i)
    {
        byte[] v = new byte[400];
        new Random(i).NextBytes(v);
        return v;
    }

    [TestMethod]
    public void SameWorkload_SameUsageShape()
    {
        const int n = 2000;
        using var nativeDir = new TempDir("mem-native");
        using var managedDir = new TempDir("mem-managed");
        RocksDbSharp.Cache nativeCache = RocksDbSharp.Cache.CreateLru(8 << 20);
        var nativeOptions = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetCompression(RocksDbSharp.Compression.No)
            .SetWriteBufferSize(4 << 20).SetBlockBasedTableFactory(new RocksDbSharp.BlockBasedTableOptions().SetBlockCache(nativeCache));
        Cache.BlockCache managedCache = Cache.BlockCache.CreateLru(8 << 20);
        var cfo = new ColumnFamilyOptions { Compression = RocksDb.Managed.Compression.CompressionType.None, WriteBufferSize = 4 << 20 };
        cfo.TableOptions.BlockCache = managedCache;

        using RocksDbSharp.RocksDb native = RocksDbSharp.RocksDb.Open(nativeOptions, nativeDir.Path);
        using RocksDatabase managed = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true }, cfo);
        Usage n0 = NativeUsage(native, nativeCache), m0 = ManagedUsage(managed, managedCache);
        for (int i = 0; i < n; i++)
        {
            native.Put(Key(i), Value(i));
            managed.Put(Key(i), Value(i));
        }
        Usage n1 = NativeUsage(native, nativeCache), m1 = ManagedUsage(managed, managedCache);
        foreach ((Usage before, Usage after, string who) in new[] { (n0, n1, "native"), (m0, m1, "managed") })
        {
            Assert.IsTrue(after.MemUnflushed > before.MemUnflushed + (ulong)n * 400, who + " memtable grows");
            Assert.IsTrue(after.MemTotal >= after.MemUnflushed, who);
            Assert.AreEqual(0UL, after.Readers, who + " no table yet");
        }
        double ratio = (double)m1.MemUnflushed / n1.MemUnflushed;
        Assert.IsTrue(ratio is > 0.5 and < 2.0, "memtable usage managed/native = " + ratio);

        native.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
        managed.Flush();
        for (int i = 0; i < n; i += 7)
        {
            Assert.IsNotNull(native.Get(Key(i)));
            Assert.IsNotNull(managed.Get(Key(i)));
        }
        Usage n2 = NativeUsage(native, nativeCache), m2 = ManagedUsage(managed, managedCache);
        foreach ((Usage before, Usage after, string who) in new[] { (n1, n2, "native"), (m1, m2, "managed") })
        {
            Assert.IsTrue(after.MemUnflushed < before.MemUnflushed, who + " flush releases the memtable");
            Assert.IsTrue(after.Readers > 0, who + " table reader memory after flush");
            Assert.IsTrue(after.Cache > before.Cache, who + " reads fill the cache");
        }
        Assert.AreEqual(nativeCache.GetUsage(), n2.Cache);
        Assert.AreEqual((ulong)managedCache.Usage, m2.Cache);
        GC.KeepAlive(nativeCache);
    }
}
