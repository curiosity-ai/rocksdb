using RocksDb.Managed.Cache;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Features;

/// <summary>
/// Port of utilities/memory/memory_test.cc (MemoryUtil::GetApproximateMemoryUsageByType): SharedBlockCacheTotal and
/// MemTableAndTableReadersTotal, plus cache de-duplication and consistency with the DB properties.
/// </summary>
[TestClass]
public sealed class MemoryUtilTests
{
    private const int KNumDbs = 10;
    private readonly Random _rnd = new(301);
    private readonly List<ulong>[] _history = [new(), new(), new(), new()];

    private byte[] RandomString(int len)
    {
        byte[] b = new byte[len];
        for (int i = 0; i < len; i++) b[i] = (byte)(' ' + _rnd.Next(95));
        return b;
    }

    private void UpdateUsagesHistory(List<RocksDatabase> dbs)
    {
        MemoryUsageByType usage = MemoryUtil.GetApproximateMemoryUsageByType(dbs, MemoryUtil.GetCaches(dbs));
        for (int i = 0; i < 4; i++) _history[i].Add(usage[(MemoryUsageType)i]);
    }

    private List<ulong> H(MemoryUsageType t) => _history[(int)t];

    private static string DbName(int i) => "/db_" + i;

    [TestMethod]
    public void SharedBlockCacheTotal()
    {
        MemoryFileSystem fs = NewFs();
        var dbs = new List<RocksDatabase>();
        const int kKeySize = 100, kValueSize = 500;
        var cfo = new ColumnFamilyOptions
        {
            WriteBufferSize = kKeySize + kValueSize, MaxWriteBufferNumber = 10, MinWriteBufferNumberToMerge = 10, DisableAutoCompactions = true,
        };
        try
        {
            for (int i = 0; i < KNumDbs; ++i) dbs.Add(RocksDatabase.Open(DbName(i), NewOptions(fs), cfo));
            var keysByDb = new List<byte[]>[KNumDbs];
            for (int i = 0; i < KNumDbs; i++) keysByDb[i] = new List<byte[]>();

            // Fill one memtable per Put to make memtable use more memory.
            for (int p = 0; p < cfo.MinWriteBufferNumberToMerge / 2; ++p)
            {
                for (int i = 0; i < KNumDbs; ++i)
                {
                    for (int j = 0; j < 100; ++j)
                    {
                        keysByDb[i].Add(RandomString(kKeySize));
                        dbs[i].Put(keysByDb[i][^1], RandomString(kValueSize));
                    }
                    dbs[i].Flush();
                }
            }
            for (int i = 0; i < KNumDbs; ++i)
            {
                foreach (byte[] key in keysByDb[i]) Assert.IsNotNull(dbs[i].Get(key));
                UpdateUsagesHistory(dbs);
            }
            for (int i = 1; i < H(MemoryUsageType.MemTableTotal).Count; ++i)
            {
                // Expect EQ as we didn't flush more memtables.
                Assert.AreEqual(H(MemoryUsageType.TableReadersTotal)[i - 1], H(MemoryUsageType.TableReadersTotal)[i]);
            }
            Assert.IsTrue(H(MemoryUsageType.TableReadersTotal)[0] > 0, "open table readers pin their index blocks");
        }
        finally
        {
            foreach (RocksDatabase db in dbs) db.Dispose();
        }
    }

    [TestMethod]
    public void MemTableAndTableReadersTotal()
    {
        MemoryFileSystem fs = NewFs();
        var dbs = new List<RocksDatabase>();
        // These key/value sizes ensure each KV has its own memtable. Note that the minimum write_buffer_size allowed is 64 KB.
        const int kKeySize = 100, kValueSize = 1 << 16;
        var cfo = new ColumnFamilyOptions
        {
            WriteBufferSize = kKeySize + kValueSize, MaxWriteBufferNumber = 10, MinWriteBufferNumberToMerge = 10, DisableAutoCompactions = true,
        };
        ColumnFamilyDescriptor[] cfDescs =
        [
            new(ColumnFamilyDescriptor.DefaultColumnFamilyName, cfo), new("one", cfo.Clone()), new("two", cfo.Clone()),
        ];
        var iters = new List<Iterator>();
        try
        {
            for (int i = 0; i < KNumDbs; ++i)
            {
                DbOptions o = NewOptions(fs);
                o.CreateMissingColumnFamilies = true;
                dbs.Add(RocksDatabase.Open(DbName(i), o, cfDescs));
            }

            // Fill one memtable per Put to make memtable use more memory.
            for (int p = 0; p < cfo.MinWriteBufferNumberToMerge / 2; ++p)
            {
                for (int i = 0; i < KNumDbs; ++i)
                {
                    foreach (ColumnFamily cf in dbs[i].ColumnFamilies)
                    {
                        cf.Put(RandomString(kKeySize), RandomString(kValueSize));
                        UpdateUsagesHistory(dbs);
                    }
                }
            }
            // Expect the usage history is monotonically increasing
            for (int i = 1; i < H(MemoryUsageType.MemTableTotal).Count; ++i)
            {
                Assert.IsTrue(H(MemoryUsageType.MemTableTotal)[i] > H(MemoryUsageType.MemTableTotal)[i - 1], "total " + i);
                Assert.IsTrue(H(MemoryUsageType.MemTableUnflushed)[i] > H(MemoryUsageType.MemTableUnflushed)[i - 1], "unflushed " + i);
                Assert.AreEqual(H(MemoryUsageType.TableReadersTotal)[i - 1], H(MemoryUsageType.TableReadersTotal)[i]);
            }

            int checkPoint = H(MemoryUsageType.MemTableTotal).Count;
            // Create an iterator and flush all memtables for each db
            for (int i = 0; i < KNumDbs; ++i)
            {
                iters.Add(dbs[i].NewIterator());
                dbs[i].Flush();
                for (int j = 0; j < 100; ++j) Assert.IsNull(dbs[i].Get(RandomString(kKeySize)));
                UpdateUsagesHistory(dbs);
            }
            for (int i = checkPoint; i < H(MemoryUsageType.MemTableTotal).Count; ++i)
            {
                // Since memtables are pinned by iterators, we don't expect the memory usage of all the memtables
                // decreases as they are pinned by iterators.
                Assert.IsTrue(H(MemoryUsageType.MemTableTotal)[i] >= H(MemoryUsageType.MemTableTotal)[i - 1], "pinned total " + i);
                // Expect the usage history from the "usage_decay_point" is monotonically decreasing.
                Assert.IsTrue(H(MemoryUsageType.MemTableUnflushed)[i] < H(MemoryUsageType.MemTableUnflushed)[i - 1], "unflushed after flush " + i);
                // Expect the usage history of the table readers increases as we flush tables.
                Assert.IsTrue(H(MemoryUsageType.TableReadersTotal)[i] > H(MemoryUsageType.TableReadersTotal)[i - 1], "readers " + i);
                Assert.IsTrue(H(MemoryUsageType.CacheTotal)[i] > H(MemoryUsageType.CacheTotal)[i - 1], "cache " + i);
            }
            checkPoint = H(MemoryUsageType.MemTableTotal).Count;
            for (int i = 0; i < KNumDbs; ++i)
            {
                iters[i].Dispose();
                UpdateUsagesHistory(dbs);
            }
            iters.Clear();
            for (int i = checkPoint; i < H(MemoryUsageType.MemTableTotal).Count; ++i)
            {
                // Expect the usage of all memtables decreasing as we delete iterators.
                Assert.IsTrue(H(MemoryUsageType.MemTableTotal)[i] < H(MemoryUsageType.MemTableTotal)[i - 1], "released total " + i);
                // The memory usage of un-flushed memtables is only affected by Put and flush.
                Assert.AreEqual(H(MemoryUsageType.MemTableUnflushed)[i - 1], H(MemoryUsageType.MemTableUnflushed)[i]);
                // Expect EQ as we didn't flush more memtables.
                Assert.AreEqual(H(MemoryUsageType.TableReadersTotal)[i - 1], H(MemoryUsageType.TableReadersTotal)[i]);
            }
        }
        finally
        {
            foreach (Iterator it in iters) it.Dispose();
            foreach (RocksDatabase db in dbs) db.Dispose();
        }
    }

    [TestMethod]
    public void CachesAreCountedOnceAndMatchProperties()
    {
        MemoryFileSystem fs = NewFs();
        BlockCache shared = BlockCache.CreateLru(8 << 20);
        var cfo = new ColumnFamilyOptions { DisableAutoCompactions = true };
        cfo.TableOptions.BlockCache = shared;
        using RocksDatabase a = RocksDatabase.Open("/a", NewOptions(fs), cfo);
        using RocksDatabase b = RocksDatabase.Open("/b", NewOptions(fs), cfo.Clone());
        foreach (RocksDatabase db in new[] { a, b })
        {
            for (int i = 0; i < 200; i++) db.Put(Key(i), Value(i, 0));
            db.Flush();
            for (int i = 0; i < 200; i++) Assert.IsNotNull(db.Get(Key(i)));
            db.Put(Key(1000), Value(1000, 1));
        }
        var dbs = new List<RocksDatabase> { a, b };
        List<BlockCache> caches = MemoryUtil.GetCaches(dbs);
        Assert.AreEqual(1, caches.Count);
        Assert.AreSame(shared, caches[0]);

        MemoryUsageByType u = MemoryUtil.GetApproximateMemoryUsageByType(dbs, [shared, shared, null]);
        Assert.AreEqual((ulong)shared.Usage, u.CacheTotal);
        Assert.IsTrue(u.CacheTotal > 0);
        Assert.IsTrue(a.TryGetAggregatedIntProperty(DbProperties.CurSizeAllMemTables, out ulong ca));
        Assert.IsTrue(b.TryGetAggregatedIntProperty(DbProperties.CurSizeAllMemTables, out ulong cb));
        Assert.AreEqual(ca + cb, u.MemTableUnflushed);
        Assert.IsTrue(u.MemTableTotal >= u.MemTableUnflushed);
        Assert.IsTrue(a.TryGetAggregatedIntProperty(DbProperties.EstimateTableReadersMem, out ulong ra));
        Assert.IsTrue(b.TryGetAggregatedIntProperty(DbProperties.EstimateTableReadersMem, out ulong rb));
        Assert.AreEqual(ra + rb, u.TableReadersTotal);
        Assert.IsTrue(ra > 0);

        // Only the given caches are counted.
        Assert.AreEqual(0UL, MemoryUtil.GetApproximateMemoryUsageByType(dbs, []).CacheTotal);
    }
}
