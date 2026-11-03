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
            case DbProperties.EstimateLiveDataSize:
                value = EstimateLiveDataSize(cfd, sv.Current.Storage).ToString(CultureInfo.InvariantCulture);
                return true;
            case DbProperties.CurSizeAllMemTables:
                {
                    // Active + unflushed immutable memtables.
                    long n = sv.Mem.ApproximateMemoryUsage;
                    foreach (MemTable m in sv.Imm) n += m.ApproximateMemoryUsage;
                    value = n.ToString(CultureInfo.InvariantCulture);
                    return true;
                }
            case DbProperties.SizeAllMemTables:
                {
                    // Active + immutable memtables (flushed ones are released immediately by the managed port, so this
                    // equals cur-size-all-mem-tables).
                    long n = sv.Mem.ApproximateMemoryUsage;
                    foreach (MemTable m in sv.Imm) n += m.ApproximateMemoryUsage;
                    value = n.ToString(CultureInfo.InvariantCulture);
                    return true;
                }
            case DbProperties.BlockCacheUsage:
            case DbProperties.BlockCacheCapacity:
            case DbProperties.BlockCachePinnedUsage:
                {
                    Cache.BlockCache? cache = BlockCacheOf(cfd);
                    if (cache is null)
                    {
                        value = null;
                        return true;
                    }
                    long v = property switch
                    {
                        "rocksdb.block-cache-usage" => cache.Usage,
                        "rocksdb.block-cache-capacity" => cache.Capacity,
                        _ => cache.PinnedUsage,
                    };
                    value = v.ToString(CultureInfo.InvariantCulture);
                    return true;
                }
            case DbProperties.NumBlobFiles:
                value = sv.Current.Storage.BlobFiles.Length.ToString(CultureInfo.InvariantCulture);
                return true;
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
            case DbProperties.TotalBlobFileSize:
                lock (_mutex) value = Versions.TotalBlobFileSize(cfd).ToString(CultureInfo.InvariantCulture);
                return true;
            case DbProperties.LiveBlobFileSize:
            case DbProperties.LiveBlobFileGarbageSize:
                {
                    VersionStorageInfo cur;
                    lock (_mutex) cur = cfd.Current.Storage;
                    (ulong total, ulong garbage) = cur.GetBlobStats();
                    value = (property == DbProperties.LiveBlobFileSize ? total : garbage).ToString(CultureInfo.InvariantCulture);
                    return true;
                }
            case DbProperties.BlobCacheCapacity:
            case DbProperties.BlobCacheUsage:
            case DbProperties.BlobCachePinnedUsage:
                {
                    Cache.BlockCache? cache = cfd.BlobSource.Cache;
                    if (cache is null)
                    {
                        value = null;
                        return true;
                    }
                    long v = property switch
                    {
                        DbProperties.BlobCacheUsage => cache.Usage,
                        DbProperties.BlobCacheCapacity => cache.Capacity,
                        _ => cache.PinnedUsage,
                    };
                    value = v.ToString(CultureInfo.InvariantCulture);
                    return true;
                }
            case DbProperties.NumLiveVersions:
                lock (_mutex) value = cfd.LiveVersions.ToString(CultureInfo.InvariantCulture);
                return true;
            case DbProperties.CurrentSuperVersionNumber:
                value = sv.Number.ToString(CultureInfo.InvariantCulture);
                return true;
            case DbProperties.OldestSnapshotTime:
                lock (_mutex) value = (_snapshots.IsEmpty ? 0 : _snapshots.Oldest.UnixTime).ToString(CultureInfo.InvariantCulture);
                return true;
            case DbProperties.OldestSnapshotSequence:
                lock (_mutex) value = (_snapshots.IsEmpty ? 0 : _snapshots.Oldest.SequenceNumber).ToString(CultureInfo.InvariantCulture);
                return true;
            case DbProperties.NumRunningFlushes:
                lock (_mutex) value = _runningFlushes.ToString(CultureInfo.InvariantCulture);
                return true;
            case DbProperties.MemTableFlushPending:
                lock (_mutex) value = cfd.HasPendingFlush ? "1" : "0";
                return true;
            case DbProperties.NumDeletesImmMemTables:
                {
                    long n = 0;
                    foreach (MemTable m in sv.Imm) n += m.NumDeletes;
                    value = n.ToString(CultureInfo.InvariantCulture);
                    return true;
                }
            case "rocksdb.estimate-table-readers-mem":
                value = "0";
                return true;
            case DbProperties.BackgroundErrors:
                value = ErrorHandler.ErrorCount.ToString(CultureInfo.InvariantCulture);
                return true;
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

    private Cache.BlockCache? BlockCacheOf(ColumnFamilyData cfd) =>
        cfd.ReaderOptions.BlockCache as Cache.BlockCache ?? (cfd.Options.TableOptions.NoBlockCache ? null : cfd.Options.TableOptions.BlockCache);

    /// <summary>
    /// Port of DB::GetIntProperty: the integer value of a numeric property. Returns false for unknown or non-numeric
    /// properties.
    /// </summary>
    public bool TryGetIntProperty(ColumnFamilyData cfd, string property, out ulong value)
    {
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
                    {
                        DbWriteStats w = _writeStats;
                        return new Dictionary<string, string>(StringComparer.Ordinal)
                        {
                            ["db.wal_bytes"] = Interlocked.Read(ref w.WalFileBytes).ToString(CultureInfo.InvariantCulture),
                            ["db.wal_syncs"] = Interlocked.Read(ref w.WalFileSynced).ToString(CultureInfo.InvariantCulture),
                            ["db.user_bytes_written"] = Interlocked.Read(ref w.BytesWritten).ToString(CultureInfo.InvariantCulture),
                            ["db.user_keys_written"] = Interlocked.Read(ref w.NumKeysWritten).ToString(CultureInfo.InvariantCulture),
                            ["db.user_writes_by_self"] = Interlocked.Read(ref w.WriteDoneBySelf).ToString(CultureInfo.InvariantCulture),
                            ["db.user_writes_by_other"] = Interlocked.Read(ref w.WriteDoneByOther).ToString(CultureInfo.InvariantCulture),
                            ["db.user_writes_with_wal"] = Interlocked.Read(ref w.WriteWithWal).ToString(CultureInfo.InvariantCulture),
                            ["db.user_write_stall_micros"] = Interlocked.Read(ref w.WriteStallMicros).ToString(CultureInfo.InvariantCulture),
                        };
                    }
                case DbProperties.AggregatedTableProperties:
                    return TablePropertiesToMap(AggregateTableProperties(cfd, sv.Current, -1));
                case DbProperties.BlockCacheEntryStats:
                    {
                        Cache.BlockCache? cache = BlockCacheOf(cfd);
                        if (cache is null) return null;
                        return new Dictionary<string, string>(StringComparer.Ordinal)
                        {
                            ["capacity"] = cache.Capacity.ToString(CultureInfo.InvariantCulture),
                            ["usage"] = cache.Usage.ToString(CultureInfo.InvariantCulture),
                            ["pinned_usage"] = cache.PinnedUsage.ToString(CultureInfo.InvariantCulture),
                        };
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
        // Ordered by largest key: the files included in the estimate.
        var ranges = new List<FileMetaData>();
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
    /// (<paramref name="includeMemtables"/>, MemTable::ApproximateStats).
    /// </summary>
    public ulong[] GetApproximateSizes(ColumnFamilyData cfd, IReadOnlyList<(byte[] Start, byte[] Limit)> ranges, bool includeMemtables,
        bool includeFiles, double filesSizeErrorMargin)
    {
        if (!includeMemtables && !includeFiles) ThrowHelper.InvalidArgument("Invalid options");
        SuperVersion sv = GetReferencedSuperVersion(cfd);
        try
        {
            var sizes = new ulong[ranges.Count];
            for (int i = 0; i < ranges.Count; i++)
            {
                byte[] k1 = ToSeekKey(ranges[i].Start), k2 = ToSeekKey(ranges[i].Limit);
                if (includeFiles) sizes[i] += ApproximateSize(cfd, sv.Current, k1, k2, filesSizeErrorMargin);
                if (includeMemtables) sizes[i] += ApproximateMemTableStats(sv, k1, k2).Size;
            }
            return sizes;
        }
        finally
        {
            ReturnSuperVersion(sv);
        }
    }

    /// <summary>Port of DBImpl::GetApproximateMemTableStats: (entries, bytes) of the memtables in [start, limit).</summary>
    public (ulong Count, ulong Size) GetApproximateMemTableStats(ColumnFamilyData cfd, byte[] start, byte[] limit)
    {
        SuperVersion sv = GetReferencedSuperVersion(cfd);
        try
        {
            return ApproximateMemTableStats(sv, ToSeekKey(start), ToSeekKey(limit));
        }
        finally
        {
            ReturnSuperVersion(sv);
        }
    }

    /// <summary>InternalKey(user_key, kMaxSequenceNumber, kValueTypeForSeek).</summary>
    private static byte[] ToSeekKey(byte[] userKey)
    {
        var k = new byte[userKey.Length + DbFormat.InternalKeyFooterSize];
        DbFormat.AppendInternalKey(k, userKey, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
        return k;
    }

    private static (ulong Count, ulong Size) ApproximateMemTableStats(SuperVersion sv, byte[] k1, byte[] k2)
    {
        ulong count = 0, size = 0;
        Add(sv.Mem, k1, k2, ref count, ref size);
        foreach (MemTable m in sv.Imm) Add(m, k1, k2, ref count, ref size);
        return (count, size);

        // MemTable::ApproximateStats
        static void Add(MemTable m, byte[] k1, byte[] k2, ref ulong count, ref ulong size)
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

    /// <summary>Port of VersionSet::ApproximateSize(options, v, start, end, 0, -1).</summary>
    private static ulong ApproximateSize(ColumnFamilyData cfd, DbVersion v, byte[] start, byte[] end, double margin)
    {
        InternalKeyComparator icmp = cfd.Icmp;
        if (icmp.Compare(start, end) > 0) return 0;
        VersionStorageInfo vs = v.Storage;
        ulong totalFullSize = 0;
        var firstFiles = new List<FileMetaData>();
        var lastFiles = new List<FileMetaData>();
        for (int level = 0; level < vs.NumLevels; level++)
        {
            ReadOnlySpan<FileMetaData> files = vs.LevelFiles(level);
            if (files.Length == 0) continue;
            if (level == 0)
            {
                // level 0 files are not in sorted order, we need to iterate through the list to compute the total bytes
                // that require scanning, so handle the case explicitly (similarly to first_files case)
                foreach (FileMetaData f in files) firstFiles.Add(f);
                continue;
            }
            int idxStart = Math.Min(vs.FindFile(level, start), files.Length - 1);
            int idxEnd = idxStart;
            if (icmp.Compare(files[idxEnd].Largest, end) < 0) idxEnd = Math.Min(vs.FindFile(level, end), files.Length - 1);
            // first scan all the intermediate full files (excluding first and last)
            for (int i = idxStart + 1; i < idxEnd; i++) totalFullSize += files[i].FileSize;
            firstFiles.Add(files[idxStart]);
            if (idxStart != idxEnd) lastFiles.Add(files[idxEnd]);
        }
        ulong totalIntersectingSize = 0;
        foreach (FileMetaData f in firstFiles) totalIntersectingSize += f.FileSize;
        foreach (FileMetaData f in lastFiles) totalIntersectingSize += f.FileSize;
        if (margin > 0 && totalIntersectingSize < (ulong)(totalFullSize * margin))
        {
            totalFullSize += totalIntersectingSize / 2;
        }
        else
        {
            foreach (FileMetaData f in firstFiles) totalFullSize += ApproximateFileSize(cfd, f, start, end);
            foreach (FileMetaData f in lastFiles) totalFullSize += ApproximateOffsetOf(cfd, f, end);
        }
        return totalFullSize;
    }

    /// <summary>Port of VersionSet::ApproximateOffsetOf(v, f, key).</summary>
    private static ulong ApproximateOffsetOf(ColumnFamilyData cfd, FileMetaData f, byte[] key)
    {
        InternalKeyComparator icmp = cfd.Icmp;
        if (icmp.Compare(f.Largest, key) <= 0) return f.FileSize; // Entire file is before "key", so just add the file size
        if (icmp.Compare(f.Smallest, key) > 0) return 0;          // Entire file is after "key", so ignore
        using TableReaderLease lease = cfd.TableCache.GetReader(f);
        return lease.Reader.ApproximateOffsetOf(key);
    }

    /// <summary>Port of VersionSet::ApproximateSize(v, f, start, end).</summary>
    private static ulong ApproximateFileSize(ColumnFamilyData cfd, FileMetaData f, byte[] start, byte[] end)
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
