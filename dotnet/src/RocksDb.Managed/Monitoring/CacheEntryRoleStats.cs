using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using RocksDb.Managed.Cache;

namespace RocksDb.Managed.Db;

/// <summary>
/// Port of InternalStats::CacheEntryRoleStats (db/internal_stats.{h,cc}) with the collection policy of
/// CacheEntryStatsCollector (cache/cache_entry_stats.h): the per-role entry counts and charges of a block cache behind
/// "rocksdb.block-cache-entry-stats" and "rocksdb.fast-block-cache-entry-stats". One collector per cache, shared by
/// every column family and DB using it (CacheEntryStatsCollector::GetShared).
/// </summary>
/// <remarks>
/// Managed differences: the "table_size" and "seed" of the text form are 0 (the managed caches expose no hash table
/// size or hash seed) and "occupancy" is the entry count. A cache whose ApplyToAllEntries is not supported reports
/// zero entries.
/// </remarks>
internal sealed class CacheEntryRoleStats
{
    private const int NumRoles = (int)CacheEntryRole.Misc + 1;

    // kCacheEntryRoleToCamelString / kCacheEntryRoleToHyphenString (cache/cache_entry_roles.cc).
    private static readonly string[] s_camel =
    [
        "DataBlock", "FilterBlock", "FilterMetaBlock", "DeprecatedFilterBlock", "IndexBlock", "OtherBlock", "WriteBuffer",
        "CompressionDictionaryBuildingBuffer", "FilterConstruction", "BlockBasedTableReader", "FileMetadata", "BlobValue", "BlobCache", "Misc",
    ];

    private static readonly string[] s_hyphen =
    [
        "data-block", "filter-block", "filter-meta-block", "deprecated-filter-block", "index-block", "other-block", "write-buffer",
        "compression-dictionary-building-buffer", "filter-construction", "block-based-table-reader", "file-metadata", "blob-value", "blob-cache", "misc",
    ];

    private static readonly ConditionalWeakTable<BlockCache, Collector> s_collectors = new();

    public string CacheId = "";
    public long CacheCapacity;
    public long CacheUsage;
    public long Occupancy;
    public ulong CollectionCount;
    public ulong CopiesOfLastCollection;
    public long LastStartTimeMicros;
    public long LastEndTimeMicros;
    public readonly ulong[] EntryCounts = new ulong[NumRoles];
    public readonly ulong[] TotalCharges = new ulong[NumRoles];

    private static long NowMicros() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1000;

    /// <summary>
    /// InternalStats::CollectCacheEntryStats + GetStats: a foreground (non-fast) request collects again when the
    /// saved stats are older than 10 s (or 10x the last collection's duration), a fast one when older than 180 s (or
    /// 500x); returns a copy of the saved stats.
    /// </summary>
    public static CacheEntryRoleStats Collect(BlockCache cache, bool fast)
    {
        Collector c = s_collectors.GetValue(cache, static k => new Collector(k));
        return c.CollectStats(minIntervalSeconds: fast ? 180 : 10, minIntervalFactor: fast ? 500 : 10);
    }

    private ulong LastDurationMicros => LastEndTimeMicros > LastStartTimeMicros ? (ulong)(LastEndTimeMicros - LastStartTimeMicros) : 0;

    private CacheEntryRoleStats Clone()
    {
        // Not MemberwiseClone: that would share the two arrays with the collector's working copy (upstream copies the
        // stats by value), so a later collection would rewrite a snapshot already handed out.
        var c = new CacheEntryRoleStats
        {
            CacheId = CacheId, CacheCapacity = CacheCapacity, CacheUsage = CacheUsage, Occupancy = Occupancy,
            CollectionCount = CollectionCount, CopiesOfLastCollection = CopiesOfLastCollection,
            LastStartTimeMicros = LastStartTimeMicros, LastEndTimeMicros = LastEndTimeMicros,
        };
        Array.Copy(EntryCounts, c.EntryCounts, NumRoles);
        Array.Copy(TotalCharges, c.TotalCharges, NumRoles);
        return c;
    }

    private CacheEntryRoleStats()
    {
    }

    /// <summary>CacheEntryRoleStats::ToString.</summary>
    public override string ToString()
    {
        var sb = new StringBuilder();
        sb.Append("Block cache ").Append(CacheId)
            .Append(" capacity: ").Append(InternalStats.BytesToHumanString((ulong)CacheCapacity))
            .Append(" seed: 0 usage: ").Append(InternalStats.BytesToHumanString((ulong)CacheUsage))
            .Append(" table_size: 0 occupancy: ").Append(Occupancy.ToString(CultureInfo.InvariantCulture))
            .Append(" collections: ").Append(CollectionCount.ToString(CultureInfo.InvariantCulture))
            .Append(" last_copies: ").Append(CopiesOfLastCollection.ToString(CultureInfo.InvariantCulture))
            .Append(" last_secs: ").Append(OstreamDouble(LastDurationMicros / 1000000.0))
            .Append(" secs_since: ").Append(((ulong)Math.Max(0, NowMicros() - LastEndTimeMicros) / 1000000UL).ToString(CultureInfo.InvariantCulture))
            .Append('\n');
        sb.Append("Block cache entry stats(count,size,portion):");
        for (int i = 0; i < NumRoles; i++)
        {
            if (EntryCounts[i] == 0) continue;
            sb.Append(' ').Append(s_camel[i]).Append('(').Append(EntryCounts[i].ToString(CultureInfo.InvariantCulture)).Append(',')
                .Append(InternalStats.BytesToHumanString(TotalCharges[i])).Append(',')
                .Append(OstreamDouble(100.0 * TotalCharges[i] / CacheCapacity)).Append("%)");
        }
        sb.Append('\n');
        return sb.ToString();
    }

    /// <summary>CacheEntryRoleStats::ToMap (BlockCacheEntryStatsMapKeys).</summary>
    public Dictionary<string, string> ToMap()
    {
        var v = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["id"] = CacheId,
            ["capacity"] = CacheCapacity.ToString(CultureInfo.InvariantCulture),
            ["secs_for_last_collection"] = Statistics.Fixed6(LastDurationMicros / 1000000.0),
            ["secs_since_last_collection"] = ((ulong)Math.Max(0, NowMicros() - LastEndTimeMicros) / 1000000UL).ToString(CultureInfo.InvariantCulture),
        };
        for (int i = 0; i < NumRoles; i++)
        {
            v["count." + s_hyphen[i]] = EntryCounts[i].ToString(CultureInfo.InvariantCulture);
            v["bytes." + s_hyphen[i]] = TotalCharges[i].ToString(CultureInfo.InvariantCulture);
            v["percent." + s_hyphen[i]] = Statistics.Fixed6(100.0 * TotalCharges[i] / CacheCapacity);
        }
        return v;
    }

    /// <summary>A double as std::ostream prints it by default (%g, 6 significant digits).</summary>
    private static string OstreamDouble(double d)
    {
        if (double.IsNaN(d)) return "nan";
        if (double.IsInfinity(d)) return d > 0 ? "inf" : "-inf";
        return d.ToString("G6", CultureInfo.InvariantCulture).Replace("E", "e", StringComparison.Ordinal);
    }

    /// <summary>CacheEntryStatsCollector&lt;CacheEntryRoleStats&gt;.</summary>
    private sealed class Collector(BlockCache cache)
    {
        private readonly Lock _working = new();
        private readonly CacheEntryRoleStats _workingStats = new();
        private long _lastStartTimeMicros;
        private long _lastEndTimeMicros = -1;

        public CacheEntryRoleStats CollectStats(int minIntervalSeconds, int minIntervalFactor)
        {
            lock (_working)
            {
                long maxAgeMicros = (long)Math.Max(minIntervalSeconds, 0) * 1000000;
                if (_lastEndTimeMicros > _lastStartTimeMicros && minIntervalFactor > 0)
                {
                    maxAgeMicros = Math.Max(maxAgeMicros, minIntervalFactor * (_lastEndTimeMicros - _lastStartTimeMicros));
                }
                long start = NowMicros();
                if (_lastEndTimeMicros < 0 || start - _lastEndTimeMicros > maxAgeMicros)
                {
                    _lastStartTimeMicros = start;
                    BeginCollection(start);
                    try
                    {
                        cache.ApplyToAllEntries(_workingStats, static (CacheEntryRoleStats s, in CacheKey _, in CacheValue _, long charge, CacheItemHelper? helper) =>
                        {
                            int role = (int)(helper?.Role ?? CacheEntryRole.Misc);
                            s.EntryCounts[role]++;
                            s.TotalCharges[role] += (ulong)charge;
                        });
                    }
                    catch (RocksDbException e) when (e.Code == StatusCode.NotSupported)
                    {
                    }
                    long end = NowMicros();
                    _lastEndTimeMicros = end;
                    _workingStats.LastEndTimeMicros = end;
                }
                else
                {
                    _workingStats.CopiesOfLastCollection++;
                }
                return _workingStats.Clone();
            }
        }

        /// <summary>CacheEntryRoleStats::BeginCollection.</summary>
        private void BeginCollection(long startTimeMicros)
        {
            CacheEntryRoleStats s = _workingStats;
            Array.Clear(s.EntryCounts);
            Array.Clear(s.TotalCharges);
            s.CopiesOfLastCollection = 0;
            s.LastStartTimeMicros = startTimeMicros;
            s.CollectionCount++;
            s.CacheId = cache.Name + "@0x" + RuntimeHelpers.GetHashCode(cache).ToString("x", CultureInfo.InvariantCulture) + "#"
                + Environment.ProcessId.ToString(CultureInfo.InvariantCulture);
            s.CacheCapacity = cache.Capacity;
            s.CacheUsage = cache.Usage;
            s.Occupancy = cache.Count;
        }
    }
}
