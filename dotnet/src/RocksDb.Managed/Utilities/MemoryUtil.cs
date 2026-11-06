using RocksDb.Managed.Cache;

namespace RocksDb.Managed;

/// <summary>The memory consumers reported by <see cref="MemoryUtil"/> (MemoryUtil::UsageType, same values).</summary>
public enum MemoryUsageType
{
    /// <summary>Memory usage of all the memtables, including flushed ones still pinned (kMemTableTotal).</summary>
    MemTableTotal = 0,
    /// <summary>Memory usage of the memtables not flushed yet (kMemTableUnFlushed).</summary>
    MemTableUnflushed = 1,
    /// <summary>Memory usage of all the open table readers outside the block cache (kTableReadersTotal).</summary>
    TableReadersTotal = 2,
    /// <summary>Memory usage of the given caches (kCacheTotal).</summary>
    CacheTotal = 3,
}

/// <summary>Approximate memory usage by consumer type, the result of <see cref="MemoryUtil.GetApproximateMemoryUsageByType"/>.</summary>
public readonly record struct MemoryUsageByType(ulong MemTableTotal, ulong MemTableUnflushed, ulong TableReadersTotal, ulong CacheTotal)
{
    /// <summary>The usage of one <paramref name="type"/> (usage_by_type[type]).</summary>
    public ulong this[MemoryUsageType type] => type switch
    {
        MemoryUsageType.MemTableTotal => MemTableTotal,
        MemoryUsageType.MemTableUnflushed => MemTableUnflushed,
        MemoryUsageType.TableReadersTotal => TableReadersTotal,
        MemoryUsageType.CacheTotal => CacheTotal,
        _ => throw new ArgumentOutOfRangeException(nameof(type)),
    };
}

/// <summary>
/// Port of MemoryUtil (include/rocksdb/utilities/memory_util.h, utilities/memory/memory_util.cc): the approximate
/// memory usage of a set of databases and caches, by consumer type.
/// </summary>
public static class MemoryUtil
{
    /// <summary>
    /// Sums, over <paramref name="databases"/>, the aggregated (all column families) properties
    /// <see cref="DbProperties.SizeAllMemTables"/> (<see cref="MemoryUsageByType.MemTableTotal"/>),
    /// <see cref="DbProperties.CurSizeAllMemTables"/> (<see cref="MemoryUsageByType.MemTableUnflushed"/>) and
    /// <see cref="DbProperties.EstimateTableReadersMem"/> (<see cref="MemoryUsageByType.TableReadersTotal"/>), and the
    /// <see cref="BlockCache.Usage"/> of each distinct cache in <paramref name="caches"/>
    /// (<see cref="MemoryUsageByType.CacheTotal"/>; null entries are ignored, a cache listed twice is counted once).
    /// As upstream, only the given caches are counted, not those the databases use: pass
    /// <see cref="GetCaches"/> of the databases to include them.
    /// </summary>
    public static MemoryUsageByType GetApproximateMemoryUsageByType(IEnumerable<RocksDatabase> databases, IEnumerable<BlockCache?> caches)
    {
        ArgumentNullException.ThrowIfNull(databases);
        ArgumentNullException.ThrowIfNull(caches);
        ulong memTotal = 0, memUnflushed = 0, readers = 0, cacheTotal = 0;
        // MemTable
        foreach (RocksDatabase db in databases)
        {
            if (db.TryGetAggregatedIntProperty(DbProperties.SizeAllMemTables, out ulong usage)) memTotal += usage;
            if (db.TryGetAggregatedIntProperty(DbProperties.CurSizeAllMemTables, out usage)) memUnflushed += usage;
            // Table Readers
            if (db.TryGetAggregatedIntProperty(DbProperties.EstimateTableReadersMem, out usage)) readers += usage;
        }
        // Cache (the input is a set upstream: count each cache once)
        var seen = new HashSet<BlockCache>(ReferenceEqualityComparer.Instance);
        foreach (BlockCache? cache in caches)
        {
            if (cache is not null && seen.Add(cache)) cacheTotal += (ulong)cache.Usage;
        }
        return new MemoryUsageByType(memTotal, memUnflushed, readers, cacheTotal);
    }

    /// <summary>
    /// The distinct caches used by <paramref name="databases"/>: the block cache of every column family (including the
    /// default cache a database supplies when none is configured) and every blob cache. Like the cache collection of
    /// upstream's utilities/memory/memory_test.cc (TEST_GetAllBlockCaches); the managed table cache holds no block
    /// data and has no <see cref="BlockCache"/>.
    /// </summary>
    public static List<BlockCache> GetCaches(IEnumerable<RocksDatabase> databases)
    {
        ArgumentNullException.ThrowIfNull(databases);
        var seen = new HashSet<BlockCache>(ReferenceEqualityComparer.Instance);
        var result = new List<BlockCache>();
        foreach (RocksDatabase db in databases)
        {
            foreach (ColumnFamily cf in db.ColumnFamilies)
            {
                if (cf.Db.BlockCacheOf(cf.Data) is { } block && seen.Add(block)) result.Add(block);
                if (cf.Data.Options.BlobCache is { } blob && seen.Add(blob)) result.Add(blob);
            }
        }
        return result;
    }
}
