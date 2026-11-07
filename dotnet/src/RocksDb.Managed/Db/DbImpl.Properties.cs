using System.Buffers;
using System.Globalization;
using System.Text;
using RocksDb.Managed.Table;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Db;

/// <summary>
/// DB properties and size estimates. Ports of the InternalStats property handlers (db/internal_stats.cc:
/// HandleEstimateLiveDataSize, HandleCurSizeAllMemTables, HandleSizeAllMemTables, HandleBlockCacheUsage /
/// Capacity, HandleNumLiveVersions, HandleOldestSnapshotTime, HandleCFStats / HandleCFMapStats, HandleDBStats,
/// HandleSsTables, HandleAggregatedTableProperties), DB::GetIntProperty / GetMapProperty, DB::GetPropertiesOfAllTables
/// (db/db_impl/db_impl.cc, Version::GetPropertiesOfAllTables), DBImpl::GetApproximateSizes /
/// GetApproximateMemTableStats (VersionSet::ApproximateSize / ApproximateOffsetOf, MemTable::ApproximateStats) and
/// DBImpl::KeyMayExist.
/// </summary>
internal sealed partial class DbImpl
{
    /// <summary>The extended string properties. Returns false for names handled elsewhere (or unknown).</summary>
    private bool TryGetExtendedProperty(ColumnFamilyData cfd, SuperVersion sv, string property, out string? value)
    {
        switch (property)
        {
            case DbProperties.BlobStats:
                {
                    // InternalStats::HandleBlobStats of the current version.
                    VersionStorageInfo cur;
                    lock (_mutex) cur = cfd.Current.Storage;
                    (ulong total, ulong garbage) = cur.GetBlobStats();
                    double spaceAmp = total > garbage ? (double)total / (total - garbage) : 0.0;
                    value = "Number of blob files: " + cur.BlobFiles.Length.ToString(CultureInfo.InvariantCulture) +
                            "\nTotal size of blob files: " + total.ToString(CultureInfo.InvariantCulture) +
                            "\nTotal size of garbage in blob files: " + garbage.ToString(CultureInfo.InvariantCulture) +
                            "\nBlob file space amplification: " + spaceAmp.ToString(CultureInfo.InvariantCulture) + "\n";
                    return true;
                }
            case DbProperties.CfStats:
                // HandleCFStats: DumpCFStatsNoFileHistogram(is_periodic = true) starts a new interval, then DumpCFFileHistogram.
                value = cfd.Stats.Dump(cfd.Name, sv.Current.Storage, isPeriodic: true) + cfd.Stats.DumpFileHistogram(cfd.Name);
                return true;
            case DbProperties.CfFileHistogram:
                value = cfd.Stats.DumpFileHistogram(cfd.Name);
                return true;
            case DbProperties.CfStatsNoFileHistogram:
                value = cfd.Stats.Dump(cfd.Name, sv.Current.Storage, isPeriodic: false);
                return true;
            case DbProperties.DbStats:
                value = _writeStats.Dump();
                return true;
            case DbProperties.Stats:
                // DB stats, the CF stats, then the managed port's cumulative compaction summary (incl. trivial moves).
                value = _writeStats.Dump() + cfd.Stats.Dump(cfd.Name, sv.Current.Storage, isPeriodic: true) + cfd.Stats.DumpFileHistogram(cfd.Name)
                    + CompactionStatsString(sv);
                return true;
            case DbProperties.SsTables:
                value = VersionDebugString(sv.Current);
                return true;
            case DbProperties.AggregatedTableProperties:
                value = TablePropertiesToString(AggregateTableProperties(cfd, sv.Current, -1), "; ", "=");
                return true;
            case DbProperties.OptionsStatistics:
                value = Stats?.ToString();
                return true;
            case DbProperties.CfWriteStallStats:
                value = cfd.Stats.DumpWriteStall();
                return true;
            case DbProperties.DbWriteStallStats:
                value = _writeStats.DumpWriteStall();
                return true;
            case DbProperties.BlockCacheEntryStats:
            case DbProperties.FastBlockCacheEntryStats:
                {
                    Cache.BlockCache? cache = BlockCacheOf(cfd);
                    value = cache is null ? null : CacheEntryRoleStats.Collect(cache, fast: property == DbProperties.FastBlockCacheEntryStats).ToString();
                    return true;
                }
        }
        const string compressionRatio = DbProperties.CompressionRatioAtLevelPrefix;
        if (property.StartsWith(compressionRatio, StringComparison.Ordinal))
        {
            // HandleCompressionRatioAtLevelPrefix: std::to_string(GetEstimatedCompressionRatioAtLevel(level)).
            value = TryParseLevel(property.AsSpan(compressionRatio.Length), sv, out int ratioLevel)
                ? Statistics.Fixed6(EstimatedCompressionRatioAtLevel(sv.Current.Storage, ratioLevel))
                : null;
            return true;
        }
        const string aggregatedAtLevel = DbProperties.AggregatedTablePropertiesAtLevelPrefix;
        if (property.StartsWith(aggregatedAtLevel, StringComparison.Ordinal)
            && int.TryParse(property.AsSpan(aggregatedAtLevel.Length), NumberStyles.None, CultureInfo.InvariantCulture, out int level))
        {
            value = level < sv.Current.Storage.NumLevels ? TablePropertiesToString(AggregateTableProperties(cfd, sv.Current, level), "; ", "=") : null;
            return true;
        }
        value = null;
        return false;
    }

    /// <summary>ConsumeDecimalNumber(&amp;suffix, &amp;level) &amp;&amp; suffix.empty() &amp;&amp; level &lt; number_levels_.</summary>
    private static bool TryParseLevel(ReadOnlySpan<char> suffix, SuperVersion sv, out int level)
    {
        level = 0;
        if (!ulong.TryParse(suffix, NumberStyles.None, CultureInfo.InvariantCulture, out ulong l) || l >= (ulong)sv.Current.Storage.NumLevels) return false;
        level = (int)l;
        return true;
    }

    /// <summary>
    /// Port of VersionStorageInfo::GetEstimatedCompressionRatioAtLevel: raw key + value bytes over file bytes of the
    /// files whose entry stats are loaded; -1 when there are none.
    /// </summary>
    private static double EstimatedCompressionRatioAtLevel(VersionStorageInfo s, int level)
    {
        ulong sumFileSize = 0, sumDataSize = 0;
        foreach (FileMetaData f in s.LevelFilesArray(level))
        {
            ulong raw = f.RawKeySize + f.RawValueSize;
            // Check if the table property is properly initialized. It might not be because in UpdateAccumulatedStats
            // we limit the maximum number of properties to read once.
            if (raw > 0)
            {
                sumFileSize += f.FileSize;
                sumDataSize += raw;
            }
        }
        if (sumFileSize == 0) return -1.0;
        return (double)sumDataSize / sumFileSize;
    }

    /// <summary>
    /// InternalStats::NumRunningCompactionSortedRuns: upstream's CompactionMergingIterator adds its number of children
    /// (one per L0 file, one per other input level) while it exists. Here: the same count over the registered, not
    /// trivially moved compactions of every column family. REQUIRES: mutex.
    /// </summary>
    private ulong NumRunningCompactionSortedRunsLocked()
    {
        ulong runs = 0;
        foreach (ColumnFamilyData c in Versions.ColumnFamilies)
        {
            if (c.Dropped) continue;
            foreach (Compaction comp in c.CompactionState.Picker.CompactionsInProgress)
            {
                if (comp.IsTrivialMove()) continue;
                for (int i = 0; i < comp.NumInputLevels; i++)
                {
                    int n = comp.Inputs(i).Files.Count;
                    if (n == 0) continue;
                    runs += comp.Level(i) == 0 ? (ulong)n : 1;
                }
            }
        }
        return runs;
    }

    /// <summary>
    /// Port of InternalStats::HandleEstimateOldestKeyTime: only for FIFO compaction without allow_compaction (oldest
    /// key times are not propagated by compactions); the minimum oldest_key_time of the tables, then of the memtables.
    /// Null when unavailable or unknown (a table without the property, or no data).
    /// </summary>
    private static ulong? EstimateOldestKeyTime(ColumnFamilyData cfd, SuperVersion sv)
    {
        if (cfd.Options.CompactionStyle != CompactionStyle.Fifo || cfd.Options.CompactionOptionsFifo.AllowCompaction) return null;
        ulong value = ulong.MaxValue;
        VersionStorageInfo s = sv.Current.Storage;
        for (int l = 0; l < s.NumLevels && value != 0; l++)
        {
            foreach (FileMetaData f in s.LevelFilesArray(l))
            {
                using TableReaderLease lease = cfd.TableCache.GetReader(f);
                value = Math.Min(value, lease.Reader.Properties.OldestKeyTime);
                if (value == 0) break;
            }
        }
        if (value > 0)
        {
            ulong imm = ulong.MaxValue;
            // MemTableList::ApproximateOldestKeyTime: the oldest unflushed immutable memtable.
            if (sv.Imm.Length > 0) imm = sv.Imm[^1].ApproximateOldestKeyTime; // newest first
            value = Math.Min(Math.Min(sv.Mem.ApproximateOldestKeyTime, imm), value);
        }
        return value > 0 && value < ulong.MaxValue ? value : null;
    }

    internal Cache.BlockCache? BlockCacheOf(ColumnFamilyData cfd) =>
        cfd.ReaderOptions.BlockCache as Cache.BlockCache ?? (cfd.Options.TableOptions.NoBlockCache ? null : cfd.Options.TableOptions.BlockCache);

    /// <summary>
    /// Port of DB::GetIntProperty: the integer value of a numeric property. Returns false for unknown or non-numeric
    /// properties.
    /// </summary>
    public bool TryGetIntProperty(ColumnFamilyData cfd, string property, out ulong value)
    {
        SuperVersion sv = GetReferencedSuperVersion(cfd);
        IntPropertyResult r;
        IntPropertyValue v;
        try
        {
            r = TryGetIntPropertyCore(cfd, sv, property, out v);
        }
        finally
        {
            ReturnSuperVersion(sv);
        }
        switch (r)
        {
            case IntPropertyResult.Value:
                return v.TryGetUInt64(out value);
            case IntPropertyResult.Unavailable:
                value = 0;
                return false;
        }
        // A string property whose text happens to be a decimal number (none of the upstream ones have an int handler, but
        // the managed port has always accepted them here).
        value = 0;
        string? s = GetProperty(cfd, property);
        return s is not null && ulong.TryParse(s, NumberStyles.None, CultureInfo.InvariantCulture, out value);
    }

    /// <summary>
    /// Port of DB::GetMapProperty: "rocksdb.cfstats" (compaction.&lt;level&gt;.&lt;stat&gt;), "rocksdb.dbstats"
    /// (db.&lt;stat&gt;), "rocksdb.aggregated-table-properties[-at-level&lt;N&gt;]" (property = value) and
    /// "rocksdb.block-cache-entry-stats" (capacity / usage). Null for other properties.
    /// </summary>
    public Dictionary<string, string>? GetMapProperty(ColumnFamilyData cfd, string property)
    {
        SuperVersion sv = GetReferencedSuperVersion(cfd);
        try
        {
            switch (property)
            {
                case DbProperties.CfStats:
                    return cfd.Stats.ToMap(sv.Current.Storage);
                case DbProperties.DbStats:
                    return _writeStats.ToMap();
                case DbProperties.CfWriteStallStats:
                    return new Dictionary<string, string>(cfd.Stats.WriteStallMap(), StringComparer.Ordinal);
                case DbProperties.DbWriteStallStats:
                    return new Dictionary<string, string>(_writeStats.WriteStallMap(), StringComparer.Ordinal);
                case DbProperties.AggregatedTableProperties:
                    return TablePropertiesToMap(AggregateTableProperties(cfd, sv.Current, -1));
                case DbProperties.BlockCacheEntryStats:
                case DbProperties.FastBlockCacheEntryStats:
                    {
                        Cache.BlockCache? cache = BlockCacheOf(cfd);
                        return cache is null ? null : CacheEntryRoleStats.Collect(cache, fast: property == DbProperties.FastBlockCacheEntryStats).ToMap();
                    }
            }
            const string aggregatedAtLevel = DbProperties.AggregatedTablePropertiesAtLevelPrefix;
            if (property.StartsWith(aggregatedAtLevel, StringComparison.Ordinal)
                && int.TryParse(property.AsSpan(aggregatedAtLevel.Length), NumberStyles.None, CultureInfo.InvariantCulture, out int level)
                && level < sv.Current.Storage.NumLevels)
            {
                return TablePropertiesToMap(AggregateTableProperties(cfd, sv.Current, level));
            }
            return null;
        }
        finally
        {
            ReturnSuperVersion(sv);
        }
    }

    // ---- live data size / sstables ---------------------------------------------------------------------

    /// <summary>
    /// Port of VersionStorageInfo::EstimateLiveDataSize: the size of a maximal set of files with no key-range overlap,
    /// preferring files of higher (more compacted) levels.
    /// </summary>
    private static ulong EstimateLiveDataSize(ColumnFamilyData cfd, VersionStorageInfo s)
    {
        InternalKeyComparator icmp = cfd.Icmp;
        ulong size = 0;
        // Ordered by largest key: the files included in the estimate. Per-thread scratch (taken out of the slot while in
        // use and cleared afterwards, so it holds no FileMetaData between calls).
        List<FileMetaData> ranges = t_liveDataRanges ?? new List<FileMetaData>();
        t_liveDataRanges = null;
        try
        {
            size = EstimateLiveDataSize(icmp, s, ranges);
        }
        finally
        {
            ranges.Clear();
            if (ranges.Capacity <= 4096) t_liveDataRanges = ranges;
        }
        return size;
    }

    [ThreadStatic] private static List<FileMetaData>? t_liveDataRanges;

    private static ulong EstimateLiveDataSize(InternalKeyComparator icmp, VersionStorageInfo s, List<FileMetaData> ranges)
    {
        ulong size = 0;
        for (int l = s.NumLevels - 1; l >= 0; l--)
        {
            bool foundEnd = false;
            foreach (FileMetaData file in s.LevelFiles(l))
            {
                // Find the first file already included with largest key larger than the smallest key of `file`. If that
                // file does not overlap with the current file, none of the files in the map does. If there is no potential
                // overlap, we can safely insert the rest of this level (if the level is not 0) into the map without checking
                // again because the elements in the level are sorted and non-overlapping.
                int lb = foundEnd && l != 0 ? ranges.Count : LowerBoundByLargest(ranges, icmp, file.Smallest);
                foundEnd = lb == ranges.Count;
                if (foundEnd || icmp.Compare(file.Largest, ranges[lb].Smallest) < 0)
                {
                    ranges.Insert(lb, file);
                    size += file.FileSize;
                }
            }
        }
        return size;
    }

    private static int LowerBoundByLargest(List<FileMetaData> ranges, InternalKeyComparator icmp, byte[] key)
    {
        int lo = 0, hi = ranges.Count;
        while (lo < hi)
        {
            int mid = (lo + hi) >>> 1;
            if (icmp.Compare(ranges[mid].Largest, key) < 0) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    /// <summary>Port of Version::DebugString(hex = true), the "rocksdb.sstables" property.</summary>
    private static string VersionDebugString(DbVersion v)
    {
        var r = new StringBuilder();
        VersionStorageInfo s = v.Storage;
        string vn = v.VersionNumber.ToString(CultureInfo.InvariantCulture);
        for (int level = 0; level < s.NumLevels; level++)
        {
            r.Append("--- level ").Append(level).Append(" --- version# ").Append(vn).Append(" ---\n");
            foreach (FileMetaData f in s.LevelFiles(level))
            {
                r.Append(' ').Append(f.FileNumber).Append(':').Append(f.FileSize)
                    .Append('[').Append(f.SmallestSeqno).Append(" .. ").Append(f.LargestSeqno).Append(']')
                    .Append('[').Append(InternalKeyDebugString(f.Smallest)).Append(" .. ").Append(InternalKeyDebugString(f.Largest)).Append(']');
                if (f.OldestBlobFileNumber != FileMetaData.InvalidBlobFileNumber) r.Append(" blob_file:").Append(f.OldestBlobFileNumber);
                r.Append('\n');
            }
        }
        return r.ToString();
    }

    /// <summary>InternalKey::DebugString(hex = true): 'HEX' seq:N, type:T.</summary>
    internal static string InternalKeyDebugString(ReadOnlySpan<byte> ikey)
    {
        if (!DbFormat.TryParseInternalKey(ikey, out ParsedInternalKey p))
        {
            return "(bad)" + Convert.ToHexString(ikey);
        }
        return "'" + Convert.ToHexString(ikey[..p.UserKeyLength]) + "' seq:" + p.Sequence.ToString(CultureInfo.InvariantCulture) + ", type:" +
               ((int)p.Type).ToString(CultureInfo.InvariantCulture);
    }

    // ---- table properties --------------------------------------------------------------------------------

    /// <summary>
    /// Port of DB::GetPropertiesOfAllTables (Version::GetPropertiesOfAllTables): the properties of every live table file
    /// of the column family, keyed by file path.
    /// </summary>
    public Dictionary<string, TableProperties> GetPropertiesOfAllTables(ColumnFamilyData cfd)
    {
        SuperVersion sv = GetReferencedSuperVersion(cfd);
        try
        {
            var result = new Dictionary<string, TableProperties>(StringComparer.Ordinal);
            VersionStorageInfo s = sv.Current.Storage;
            for (int l = 0; l < s.NumLevels; l++)
            {
                foreach (FileMetaData f in s.LevelFiles(l))
                {
                    using TableReaderLease lease = cfd.TableCache.GetReader(f);
                    result[FileNames.TableFileName(DbPaths, f.FileNumber, f.PathId)] = lease.Reader.Properties;
                }
            }
            return result;
        }
        finally
        {
            ReturnSuperVersion(sv);
        }
    }

    /// <summary>TableProperties::Add over the files of <paramref name="level"/> (-1 = all levels).</summary>
    private static TableProperties AggregateTableProperties(ColumnFamilyData cfd, DbVersion v, int level)
    {
        var agg = new TableProperties();
        VersionStorageInfo s = v.Storage;
        for (int l = 0; l < s.NumLevels; l++)
        {
            if (level >= 0 && l != level) continue;
            foreach (FileMetaData f in s.LevelFiles(l))
            {
                using TableReaderLease lease = cfd.TableCache.GetReader(f);
                TableProperties tp = lease.Reader.Properties;
                agg.DataSize += tp.DataSize;
                agg.UncompressedDataSize += tp.UncompressedDataSize;
                agg.IndexSize += tp.IndexSize;
                agg.IndexPartitions += tp.IndexPartitions;
                agg.TopLevelIndexSize += tp.TopLevelIndexSize;
                agg.IndexKeyIsUserKey += tp.IndexKeyIsUserKey;
                agg.IndexValueIsDeltaEncoded += tp.IndexValueIsDeltaEncoded;
                agg.StandardIndexIsStub += tp.StandardIndexIsStub;
                agg.FilterSize += tp.FilterSize;
                agg.RawKeySize += tp.RawKeySize;
                agg.RawValueSize += tp.RawValueSize;
                agg.NumDataBlocks += tp.NumDataBlocks;
                agg.NumDataBlocksCompressionRejected += tp.NumDataBlocksCompressionRejected;
                agg.NumDataBlocksCompressionBypassed += tp.NumDataBlocksCompressionBypassed;
                agg.NumUniformBlocks += tp.NumUniformBlocks;
                agg.NumEntries += tp.NumEntries;
                agg.NumFilterEntries += tp.NumFilterEntries;
                agg.NumDeletions += tp.NumDeletions;
                agg.NumMergeOperands += tp.NumMergeOperands;
                agg.NumRangeDeletions += tp.NumRangeDeletions;
                agg.SlowCompressionEstimatedDataSize += tp.SlowCompressionEstimatedDataSize;
                agg.FastCompressionEstimatedDataSize += tp.FastCompressionEstimatedDataSize;
            }
        }
        return agg;
    }

    /// <summary>TableProperties::GetAggregatablePropertiesAsMap (the map form of the aggregated properties).</summary>
    private static Dictionary<string, string> TablePropertiesToMap(TableProperties tp)
    {
        static string N(ulong v) => v.ToString(CultureInfo.InvariantCulture);
        return new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["data_size"] = N(tp.DataSize),
            ["uncompressed_data_size"] = N(tp.UncompressedDataSize),
            ["index_size"] = N(tp.IndexSize),
            ["index_partitions"] = N(tp.IndexPartitions),
            ["top_level_index_size"] = N(tp.TopLevelIndexSize),
            ["standard_index_is_stub"] = N(tp.StandardIndexIsStub),
            ["filter_size"] = N(tp.FilterSize),
            ["raw_key_size"] = N(tp.RawKeySize),
            ["raw_value_size"] = N(tp.RawValueSize),
            ["num_data_blocks"] = N(tp.NumDataBlocks),
            ["num_data_blocks_compression_rejected"] = N(tp.NumDataBlocksCompressionRejected),
            ["num_data_blocks_compression_bypassed"] = N(tp.NumDataBlocksCompressionBypassed),
            ["num_uniform_blocks"] = N(tp.NumUniformBlocks),
            ["num_entries"] = N(tp.NumEntries),
            ["num_filter_entries"] = N(tp.NumFilterEntries),
            ["num_deletions"] = N(tp.NumDeletions),
            ["num_merge_operands"] = N(tp.NumMergeOperands),
            ["num_range_deletions"] = N(tp.NumRangeDeletions),
            ["slow_compression_estimated_data_size"] = N(tp.SlowCompressionEstimatedDataSize),
            ["fast_compression_estimated_data_size"] = N(tp.FastCompressionEstimatedDataSize),
        };
    }

    /// <summary>The leading (aggregatable) part of TableProperties::ToString (table/table_properties.cc).</summary>
    internal static string TablePropertiesToString(TableProperties tp, string propDelim, string kvDelim)
    {
        var r = new StringBuilder(1024);
        void Add(string k, string v) => r.Append(k).Append(kvDelim).Append(v).Append(propDelim);
        static string N(ulong v) => v.ToString(CultureInfo.InvariantCulture);
        Add("# data blocks", N(tp.NumDataBlocks));
        Add("# data blocks compression rejected", N(tp.NumDataBlocksCompressionRejected));
        Add("# data blocks compression bypassed", N(tp.NumDataBlocksCompressionBypassed));
        Add("# uniform blocks", N(tp.NumUniformBlocks));
        Add("# entries", N(tp.NumEntries));
        Add("# deletions", N(tp.NumDeletions));
        Add("# merge operands", N(tp.NumMergeOperands));
        Add("# range deletions", N(tp.NumRangeDeletions));
        Add("raw key size", N(tp.RawKeySize));
        Add("raw average key size", Statistics.Fixed6(tp.NumEntries != 0 ? 1.0 * tp.RawKeySize / tp.NumEntries : 0.0));
        Add("raw value size", N(tp.RawValueSize));
        Add("raw average value size", Statistics.Fixed6(tp.NumEntries != 0 ? 1.0 * tp.RawValueSize / tp.NumEntries : 0.0));
        Add("data block size", N(tp.DataSize));
        Add("data uncompressed size", N(tp.UncompressedDataSize));
        Add("index block size (user-key? " + (int)tp.IndexKeyIsUserKey + ", delta-value? " + (int)tp.IndexValueIsDeltaEncoded + ")", N(tp.IndexSize));
        if (tp.IndexPartitions != 0)
        {
            Add("# index partitions", N(tp.IndexPartitions));
            Add("top-level index size", N(tp.TopLevelIndexSize));
        }
        Add("filter block size", N(tp.FilterSize));
        Add("# entries for filter", N(tp.NumFilterEntries));
        Add("(estimated) table size", N(tp.DataSize + tp.IndexSize + tp.FilterSize));
        return r.ToString();
    }

    // ---- approximate sizes -------------------------------------------------------------------------------

    /// <summary>
    /// Port of DBImpl::GetApproximateSizes: for each [start, limit) user-key range, the approximate bytes in table files
    /// (<paramref name="includeFiles"/>, VersionSet::ApproximateSize with files_size_error_margin) and/or memtables
    /// (<paramref name="includeMemtables"/>, MemTable::ApproximateStats), written to <paramref name="sizes"/> (at least
    /// one element per range). Allocates nothing itself (seek keys go to stack / pooled scratch).
    /// </summary>
    public void GetApproximateSizes(ColumnFamilyData cfd, ReadOnlySpan<KeyRange> ranges, Span<ulong> sizes, bool includeMemtables,
        bool includeFiles, double filesSizeErrorMargin)
    {
        if (!includeMemtables && !includeFiles) ThrowHelper.InvalidArgument("Invalid options");
        SuperVersion sv = GetReferencedSuperVersion(cfd);
        try
        {
            for (int i = 0; i < ranges.Length; i++)
            {
                sizes[i] = ApproximateRangeSize(cfd, sv, ranges[i].Start.Span, ranges[i].Limit.Span, includeMemtables, includeFiles, filesSizeErrorMargin);
            }
        }
        finally
        {
            ReturnSuperVersion(sv);
        }
    }

    /// <summary><see cref="GetApproximateSizes(ColumnFamilyData, ReadOnlySpan{KeyRange}, Span{ulong}, bool, bool, double)"/> of one range.</summary>
    public ulong GetApproximateSize(ColumnFamilyData cfd, ReadOnlySpan<byte> start, ReadOnlySpan<byte> limit, bool includeMemtables,
        bool includeFiles, double filesSizeErrorMargin)
    {
        if (!includeMemtables && !includeFiles) ThrowHelper.InvalidArgument("Invalid options");
        SuperVersion sv = GetReferencedSuperVersion(cfd);
        try
        {
            return ApproximateRangeSize(cfd, sv, start, limit, includeMemtables, includeFiles, filesSizeErrorMargin);
        }
        finally
        {
            ReturnSuperVersion(sv);
        }
    }

    /// <summary>One range of DBImpl::GetApproximateSizes; the seek keys are built in stack (or pooled) scratch.</summary>
    private static ulong ApproximateRangeSize(ColumnFamilyData cfd, SuperVersion sv, ReadOnlySpan<byte> start, ReadOnlySpan<byte> limit,
        bool includeMemtables, bool includeFiles, double filesSizeErrorMargin)
    {
        int n1 = start.Length + DbFormat.InternalKeyFooterSize, n2 = limit.Length + DbFormat.InternalKeyFooterSize;
        byte[]? rented = null;
        Span<byte> scratch = n1 + n2 <= 512 ? stackalloc byte[512] : (rented = RocksArrayPool<byte>.Shared.Rent(n1 + n2));
        try
        {
            ReadOnlySpan<byte> k1 = ToSeekKey(start, scratch[..n1]);
            ReadOnlySpan<byte> k2 = ToSeekKey(limit, scratch.Slice(n1, n2));
            ulong size = 0;
            if (includeFiles) size += ApproximateSize(cfd, sv.Current, k1, k2, filesSizeErrorMargin);
            if (includeMemtables) size += ApproximateMemTableStats(sv, k1, k2).Size;
            return size;
        }
        finally
        {
            if (rented is not null) RocksArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>Port of DBImpl::GetApproximateMemTableStats: (entries, bytes) of the memtables in [start, limit).</summary>
    public (ulong Count, ulong Size) GetApproximateMemTableStats(ColumnFamilyData cfd, ReadOnlySpan<byte> start, ReadOnlySpan<byte> limit)
    {
        SuperVersion sv = GetReferencedSuperVersion(cfd);
        int n1 = start.Length + DbFormat.InternalKeyFooterSize, n2 = limit.Length + DbFormat.InternalKeyFooterSize;
        byte[]? rented = null;
        Span<byte> scratch = n1 + n2 <= 512 ? stackalloc byte[512] : (rented = RocksArrayPool<byte>.Shared.Rent(n1 + n2));
        try
        {
            return ApproximateMemTableStats(sv, ToSeekKey(start, scratch[..n1]), ToSeekKey(limit, scratch.Slice(n1, n2)));
        }
        finally
        {
            if (rented is not null) RocksArrayPool<byte>.Shared.Return(rented);
            ReturnSuperVersion(sv);
        }
    }

    /// <summary>InternalKey(user_key, kMaxSequenceNumber, kValueTypeForSeek) in <paramref name="destination"/>.</summary>
    private static ReadOnlySpan<byte> ToSeekKey(ReadOnlySpan<byte> userKey, Span<byte> destination) =>
        destination[..DbFormat.AppendInternalKey(destination, userKey, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek)];

    private static (ulong Count, ulong Size) ApproximateMemTableStats(SuperVersion sv, ReadOnlySpan<byte> k1, ReadOnlySpan<byte> k2)
    {
        ulong count = 0, size = 0;
        Add(sv.Mem, k1, k2, ref count, ref size);
        foreach (MemTable m in sv.Imm) Add(m, k1, k2, ref count, ref size);
        return (count, size);

        // MemTable::ApproximateStats
        static void Add(MemTable m, ReadOnlySpan<byte> k1, ReadOnlySpan<byte> k2, ref ulong count, ref ulong size)
        {
            ulong entryCount = m.ApproximateNumEntries(k1, k2);
            if (entryCount == 0) return;
            ulong n = (ulong)m.NumEntries;
            if (n == 0) return;
            // ApproximateNumEntries() is just an estimate so it can be larger than actual entries we have. Cap it to
            // entries we have to limit the inaccuracy.
            if (entryCount > n) entryCount = n;
            ulong dataSize = (ulong)m.DataSize;
            count += entryCount;
            size += entryCount * (dataSize / n);
        }
    }

    /// <summary>
    /// Port of VersionSet::ApproximateSize(options, v, start, end, 0, -1). Upstream collects the partially covered files
    /// (all of level 0, the first and last overlapping file of every other level) into first_files / last_files; this
    /// port visits the same files twice instead (sizes, then the per-file estimates if needed), which keeps the call
    /// free of list allocations.
    /// </summary>
    private static ulong ApproximateSize(ColumnFamilyData cfd, DbVersion v, ReadOnlySpan<byte> start, ReadOnlySpan<byte> end, double margin)
    {
        InternalKeyComparator icmp = cfd.Icmp;
        if (icmp.Compare(start, end) > 0) return 0;
        VersionStorageInfo vs = v.Storage;
        ulong totalFullSize = 0;
        ulong totalIntersectingSize = 0;
        for (int level = 0; level < vs.NumLevels; level++)
        {
            ReadOnlySpan<FileMetaData> files = vs.LevelFiles(level);
            if (files.Length == 0) continue;
            if (level == 0)
            {
                // level 0 files are not in sorted order, we need to iterate through the list to compute the total bytes
                // that require scanning, so handle the case explicitly (similarly to first_files case)
                foreach (FileMetaData f in files) totalIntersectingSize += f.FileSize;
                continue;
            }
            (int idxStart, int idxEnd) = OverlapEnds(icmp, vs, level, files, start, end);
            // first scan all the intermediate full files (excluding first and last)
            for (int i = idxStart + 1; i < idxEnd; i++) totalFullSize += files[i].FileSize;
            totalIntersectingSize += files[idxStart].FileSize;
            if (idxStart != idxEnd) totalIntersectingSize += files[idxEnd].FileSize;
        }
        if (margin > 0 && totalIntersectingSize < (ulong)(totalFullSize * margin))
        {
            totalFullSize += totalIntersectingSize / 2;
            return totalFullSize;
        }
        // first_files: ApproximateSize(v, f, start, end); last_files: ApproximateOffsetOf(v, f, end).
        for (int level = 0; level < vs.NumLevels; level++)
        {
            ReadOnlySpan<FileMetaData> files = vs.LevelFiles(level);
            if (files.Length == 0) continue;
            if (level == 0)
            {
                foreach (FileMetaData f in files) totalFullSize += ApproximateFileSize(cfd, f, start, end);
                continue;
            }
            (int idxStart, int idxEnd) = OverlapEnds(icmp, vs, level, files, start, end);
            totalFullSize += ApproximateFileSize(cfd, files[idxStart], start, end);
            if (idxStart != idxEnd) totalFullSize += ApproximateOffsetOf(cfd, files[idxEnd], end);
        }
        return totalFullSize;

        static (int Start, int End) OverlapEnds(InternalKeyComparator icmp, VersionStorageInfo vs, int level, ReadOnlySpan<FileMetaData> files,
            ReadOnlySpan<byte> start, ReadOnlySpan<byte> end)
        {
            int idxStart = Math.Min(vs.FindFile(level, start), files.Length - 1);
            int idxEnd = idxStart;
            if (icmp.Compare(files[idxEnd].Largest, end) < 0) idxEnd = Math.Min(vs.FindFile(level, end), files.Length - 1);
            return (idxStart, idxEnd);
        }
    }

    /// <summary>Port of VersionSet::ApproximateOffsetOf(v, f, key).</summary>
    private static ulong ApproximateOffsetOf(ColumnFamilyData cfd, FileMetaData f, ReadOnlySpan<byte> key)
    {
        InternalKeyComparator icmp = cfd.Icmp;
        if (icmp.Compare(f.Largest, key) <= 0) return f.FileSize; // Entire file is before "key", so just add the file size
        if (icmp.Compare(f.Smallest, key) > 0) return 0;          // Entire file is after "key", so ignore
        using TableReaderLease lease = cfd.TableCache.GetReader(f);
        return lease.Reader.ApproximateOffsetOf(key);
    }

    /// <summary>Port of VersionSet::ApproximateSize(v, f, start, end).</summary>
    private static ulong ApproximateFileSize(ColumnFamilyData cfd, FileMetaData f, ReadOnlySpan<byte> start, ReadOnlySpan<byte> end)
    {
        InternalKeyComparator icmp = cfd.Icmp;
        // Entire file is before or after the start/end keys range
        if (icmp.Compare(f.Largest, start) <= 0 || icmp.Compare(f.Smallest, end) > 0) return 0;
        // Start of the range is before the file start - approximate by end offset
        if (icmp.Compare(f.Smallest, start) >= 0) return ApproximateOffsetOf(cfd, f, end);
        if (icmp.Compare(f.Largest, end) < 0)
        {
            // End of the range is after the file end - approximate by subtracting start offset from the file size
            ulong startOffset = ApproximateOffsetOf(cfd, f, start);
            return f.FileSize - Math.Min(startOffset, f.FileSize);
        }
        // The interval falls entirely in the range for this file.
        using TableReaderLease lease = cfd.TableCache.GetReader(f);
        return lease.Reader.ApproximateSize(start, end);
    }

    // ---- KeyMayExist -------------------------------------------------------------------------------------

    /// <summary>
    /// Port of DBImpl::KeyMayExist (a GetImpl with read_tier = kBlockCacheTier): a lookup that does no file I/O. Returns
    /// false only if the key definitely does not exist. The memtables are searched completely: a value found there
    /// (after merging, when the merge operands and their base are all in memtables) is returned with
    /// <paramref name="valueFound"/> = true. Otherwise table files whose key range covers the key are checked with
    /// their filters only: a table that is not open yet, or whose filter is neither pinned nor cached, answers "may
    /// exist" like upstream's Incomplete. Deviation: values in cached data blocks are not returned (valueFound stays
    /// false for keys only in table files).
    /// </summary>
    public bool KeyMayExist(ColumnFamilyData cfd, ReadOptions? options, ReadOnlySpan<byte> key, IBufferWriter<byte>? value, out bool valueFound)
    {
        valueFound = false;
        ValidateReadSnapshot(options);
        SuperVersion sv = GetReferencedSuperVersion(cfd);
        ulong snapshot = ReadSequence(options);
        GetContext ctx = GetContext.Rent(cfd.MergeOperator);
        try
        {
            Span<byte> scratch = stackalloc byte[LookupKey.StackBufferSize];
            var lkey = new LookupKey(key, snapshot, scratch);
            try
            {
                if (GetFromMemTables(sv, lkey, ctx))
                {
                    if (ctx.State is GetContext.GetState.Deleted) return false;
                    // A blob reference would need blob file I/O: the key exists but its value is not returned.
                    if (ctx.State is GetContext.GetState.Found && value is not null && !ctx.NeedsBlobResolution && ctx.TryResolve(key, out ReadOnlySpan<byte> v))
                    {
                        value.Write(v);
                        valueFound = true;
                    }
                    return true;
                }
                if (ctx.State == GetContext.GetState.Merge) return true; // merge operands found: the key exists
                // Table files: whether any file's filter may contain the key.
                Comparator ucmp = cfd.UserComparator;
                VersionStorageInfo s = sv.Current.Storage;
                for (int level = 0; level < s.NumLevels; level++)
                {
                    foreach (FileMetaData f in s.LevelFiles(level))
                    {
                        if (ucmp.Compare(key, DbFormat.ExtractUserKey(f.Smallest)) < 0 || ucmp.Compare(key, DbFormat.ExtractUserKey(f.Largest)) > 0) continue;
                        // TableCache::FindTable(no_io): a table that is not open would need I/O -> Incomplete -> may exist.
                        if (!cfd.TableCache.TryGetOpenReader(f, out TableReaderLease lease)) return true;
                        using (lease)
                        {
                            if (lease.Reader.InternalKeyMayMatch(lkey.InternalKey, TableReadFlags.BlockCacheTier)) return true;
                        }
                    }
                }
                return false;
            }
            finally
            {
                lkey.Dispose();
            }
        }
        finally
        {
            GetContext.Return(ctx);
            ReturnSuperVersion(sv);
        }
    }
}
