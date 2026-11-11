using System.Globalization;

namespace RocksDb.Managed.Db;

/// <summary>
/// The numeric DB properties, computed without formatting a string. Port of the int-handler half of the property table
/// (db/internal_stats.cc: DBPropertyInfo::handle_int, InternalStats::GetIntProperty / GetIntPropertyOutOfMutex) and of
/// the DBImpl::GetProperty / DBImpl::GetIntProperty split (db/db_impl/db_impl.cc): a numeric property is computed once
/// as an integer; <see cref="GetProperty"/> formats it (DBImpl::GetProperty does std::to_string of the handle_int
/// result), <see cref="TryGetIntProperty"/> returns it as is.
/// <para>
/// Deviation kept from the earlier managed behaviour: properties that upstream serves with a string handler only but
/// whose value is a decimal number (e.g. rocksdb.num-files-at-level&lt;N&gt;, rocksdb.num-snapshots) are numeric here too,
/// so TryGetIntProperty accepts them as it always did.
/// </para>
/// </summary>
internal sealed partial class DbImpl
{
    internal enum IntPropertyResult : byte
    {
        /// <summary>Not a numeric property (a string property or unknown).</summary>
        NotNumeric,
        /// <summary>A numeric property that has no value here (e.g. no block cache, a level beyond num_levels).</summary>
        Unavailable,
        /// <summary>The value was computed.</summary>
        Value,
    }

    /// <summary>A computed numeric property: an unsigned value, or a signed one (formatted with its sign).</summary>
    internal readonly struct IntPropertyValue
    {
        private readonly ulong _bits;
        private readonly bool _signed;

        private IntPropertyValue(ulong bits, bool signed)
        {
            _bits = bits;
            _signed = signed;
        }

        public static IntPropertyValue Of(ulong v) => new(v, signed: false);
        public static IntPropertyValue Of(long v) => new(unchecked((ulong)v), signed: true);

        /// <summary>The value as DB::GetIntProperty's uint64; false for a negative signed value (not representable).</summary>
        public bool TryGetUInt64(out ulong value)
        {
            if (_signed && unchecked((long)_bits) < 0)
            {
                value = 0;
                return false;
            }
            value = _bits;
            return true;
        }

        public override string ToString() =>
            _signed ? unchecked((long)_bits).ToString(CultureInfo.InvariantCulture) : _bits.ToString(CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// GetPropertyNameAndArg / GetPropertyInfo (db/internal_stats.cc): trailing decimal digits of a property name are an
    /// optional argument and are not part of the name that is looked up, so "rocksdb.estimate-num-keys0" is
    /// "rocksdb.estimate-num-keys" (handlers without an argument ignore it). The four properties that take the argument
    /// (a level or a temperature) keep it. Allocates only for a name with a suffix that is dropped.
    /// </summary>
    internal static string PropertyNameForLookup(string property)
    {
        int end = property.Length;
        if (end == 0 || !char.IsAsciiDigit(property[end - 1])) return property;
        while (end > 0 && char.IsAsciiDigit(property[end - 1])) end--;
        ReadOnlySpan<char> name = property.AsSpan(0, end);
        if (name.SequenceEqual(DbProperties.NumFilesAtLevelPrefix) || name.SequenceEqual(DbProperties.CompressionRatioAtLevelPrefix)
            || name.SequenceEqual(DbProperties.AggregatedTablePropertiesAtLevelPrefix)
            || name.SequenceEqual(DbProperties.LiveSstFilesSizeAtTemperaturePrefix))
        {
            return property;
        }
        return property[..end];
    }

    /// <summary>
    /// Computes the numeric property <paramref name="property"/> on <paramref name="sv"/> (the referenced super version of
    /// <paramref name="cfd"/>). Returns <see cref="IntPropertyResult.NotNumeric"/> for every other property, which the
    /// string handlers then serve. Does not allocate.
    /// </summary>
    internal IntPropertyResult TryGetIntPropertyCore(ColumnFamilyData cfd, SuperVersion sv, string property, out IntPropertyValue value)
    {
        value = default;
        switch (property)
        {
            // ---- memtables (HandleNumEntriesActiveMemTable, ..., HandleCurSizeAllMemTables, HandleSizeAllMemTables)
            case DbProperties.NumEntriesActiveMemTable:
                value = IntPropertyValue.Of(sv.Mem.NumEntries);
                return IntPropertyResult.Value;
            case DbProperties.NumEntriesImmMemTables:
                {
                    long n = 0;
                    foreach (MemTable m in sv.Imm) n += m.NumEntries;
                    value = IntPropertyValue.Of(n);
                    return IntPropertyResult.Value;
                }
            case DbProperties.NumDeletesActiveMemTable:
                value = IntPropertyValue.Of(sv.Mem.NumDeletes);
                return IntPropertyResult.Value;
            case DbProperties.NumDeletesImmMemTables:
                {
                    long n = 0;
                    foreach (MemTable m in sv.Imm) n += m.NumDeletes;
                    value = IntPropertyValue.Of(n);
                    return IntPropertyResult.Value;
                }
            case DbProperties.NumImmutableMemTable:
                value = IntPropertyValue.Of((long)sv.Imm.Length);
                return IntPropertyResult.Value;
            case DbProperties.CurSizeActiveMemTable:
                value = IntPropertyValue.Of(sv.Mem.ApproximateMemoryUsage);
                return IntPropertyResult.Value;
            case DbProperties.CurSizeAllMemTables:
                {
                    // Active + unflushed immutable memtables.
                    long n = sv.Mem.ApproximateMemoryUsage;
                    foreach (MemTable m in sv.Imm) n += m.ApproximateMemoryUsage;
                    value = IntPropertyValue.Of(n);
                    return IntPropertyResult.Value;
                }
            case DbProperties.SizeAllMemTables:
                value = IntPropertyValue.Of(SizeAllMemTables(cfd, sv));
                return IntPropertyResult.Value;
            case DbProperties.NumImmutableMemTableFlushed:
                lock (_mutex) value = IntPropertyValue.Of((long)cfd.History.Count);
                return IntPropertyResult.Value;
            case DbProperties.MemTableFlushPending:
                lock (_mutex) value = IntPropertyValue.Of(cfd.HasPendingFlush ? 1UL : 0UL);
                return IntPropertyResult.Value;

            // ---- keys, files and sizes
            case DbProperties.EstimateNumKeys:
                value = IntPropertyValue.Of(EstimateNumKeys(sv));
                return IntPropertyResult.Value;
            case DbProperties.EstimateLiveDataSize:
                value = IntPropertyValue.Of(EstimateLiveDataSize(cfd, sv.Current.Storage));
                return IntPropertyResult.Value;
            case DbProperties.TotalSstFilesSize:
                // ColumnFamilyData::GetTotalSstFilesSize: the distinct files of every live version (incl. versions
                // pinned by iterators or snapshots of the file set), not only the current one.
                lock (_mutex) value = IntPropertyValue.Of(cfd.AllVersionsSstFilesSize);
                return IntPropertyResult.Value;
            case DbProperties.LiveSstFilesSize:
                {
                    ulong n = 0;
                    VersionStorageInfo s = sv.Current.Storage;
                    for (int l = 0; l < s.NumLevels; l++) n += s.NumLevelBytes(l);
                    value = IntPropertyValue.Of(n);
                    return IntPropertyResult.Value;
                }
            case DbProperties.ObsoleteSstFilesSize:
                lock (_mutex) value = IntPropertyValue.Of(Versions.ObsoleteSstFilesSize);
                return IntPropertyResult.Value;
            case DbProperties.EstimateTableReadersMem:
                value = IntPropertyValue.Of(EstimateTableReadersMem(cfd, sv));
                return IntPropertyResult.Value;
            case DbProperties.EstimateOldestKeyTime:
                if (EstimateOldestKeyTime(cfd, sv) is not ulong oldest) return IntPropertyResult.Unavailable;
                value = IntPropertyValue.Of(oldest);
                return IntPropertyResult.Value;

            // ---- caches
            case DbProperties.BlockCacheUsage:
            case DbProperties.BlockCacheCapacity:
            case DbProperties.BlockCachePinnedUsage:
                {
                    Cache.BlockCache? cache = BlockCacheOf(cfd);
                    if (cache is null) return IntPropertyResult.Unavailable;
                    value = IntPropertyValue.Of(property switch
                    {
                        DbProperties.BlockCacheUsage => cache.Usage,
                        DbProperties.BlockCacheCapacity => cache.Capacity,
                        _ => cache.PinnedUsage,
                    });
                    return IntPropertyResult.Value;
                }
            case DbProperties.BlobCacheCapacity:
            case DbProperties.BlobCacheUsage:
            case DbProperties.BlobCachePinnedUsage:
                {
                    Cache.BlockCache? cache = cfd.BlobSource.Cache;
                    if (cache is null) return IntPropertyResult.Unavailable;
                    value = IntPropertyValue.Of(property switch
                    {
                        DbProperties.BlobCacheUsage => cache.Usage,
                        DbProperties.BlobCacheCapacity => cache.Capacity,
                        _ => cache.PinnedUsage,
                    });
                    return IntPropertyResult.Value;
                }

            // ---- blob files
            case DbProperties.NumBlobFiles:
                value = IntPropertyValue.Of((long)sv.Current.Storage.BlobFiles.Length);
                return IntPropertyResult.Value;
            case DbProperties.TotalBlobFileSize:
                lock (_mutex) value = IntPropertyValue.Of(Versions.TotalBlobFileSize(cfd));
                return IntPropertyResult.Value;
            case DbProperties.LiveBlobFileSize:
            case DbProperties.LiveBlobFileGarbageSize:
                {
                    VersionStorageInfo cur;
                    lock (_mutex) cur = cfd.Current.Storage;
                    (ulong total, ulong garbage) = cur.GetBlobStats();
                    value = IntPropertyValue.Of(property == DbProperties.LiveBlobFileSize ? total : garbage);
                    return IntPropertyResult.Value;
                }

            // ---- versions, snapshots, files to keep
            case DbProperties.NumLiveVersions:
                lock (_mutex) value = IntPropertyValue.Of((long)cfd.LiveVersions);
                return IntPropertyResult.Value;
            case DbProperties.CurrentSuperVersionNumber:
                value = IntPropertyValue.Of(sv.Number);
                return IntPropertyResult.Value;
            case DbProperties.NumSnapshots:
                value = IntPropertyValue.Of((long)SnapshotCount);
                return IntPropertyResult.Value;
            case DbProperties.OldestSnapshotTime:
                lock (_mutex) value = IntPropertyValue.Of(_snapshots.IsEmpty ? 0 : _snapshots.Oldest.UnixTime);
                return IntPropertyResult.Value;
            case DbProperties.OldestSnapshotSequence:
                lock (_mutex) value = IntPropertyValue.Of(_snapshots.IsEmpty ? 0 : _snapshots.Oldest.SequenceNumber);
                return IntPropertyResult.Value;
            case DbProperties.IsFileDeletionsEnabled:
                value = IntPropertyValue.Of(IsFileDeletionsEnabled ? 1UL : 0UL);
                return IntPropertyResult.Value;
            case DbProperties.MinLogNumberToKeep:
                // DBImpl::MinLogNumberToKeep = versions_->min_log_number_to_keep().
                lock (_mutex) value = IntPropertyValue.Of(Versions.MinLogNumberToKeep);
                return IntPropertyResult.Value;
            case DbProperties.MinObsoleteSstNumberToKeep:
                {
                    // DBImpl::MinObsoleteSstNumberToKeep: the smallest pending output, else uint64 max.
                    ulong min = ulong.MaxValue;
                    lock (_mutex)
                    {
                        foreach (ulong n in _pendingOutputs) min = Math.Min(min, n);
                    }
                    value = IntPropertyValue.Of(min);
                    return IntPropertyResult.Value;
                }

            // ---- background work
            case DbProperties.NumRunningFlushes:
                lock (_mutex) value = IntPropertyValue.Of((long)_runningFlushes);
                return IntPropertyResult.Value;
            case DbProperties.NumRunningCompactions:
                value = IntPropertyValue.Of((long)NumRunningCompactions);
                return IntPropertyResult.Value;
            case DbProperties.NumRunningRemoteCompactions:
                value = IntPropertyValue.Of((long)NumRunningRemoteCompactions);
                return IntPropertyResult.Value;
            case DbProperties.NumRunningBottomCompactions:
                lock (_mutex) value = IntPropertyValue.Of((long)_bgBottomCompactionRunning);
                return IntPropertyResult.Value;
            case DbProperties.NumRunningCompactionSortedRuns:
                lock (_mutex) value = IntPropertyValue.Of(NumRunningCompactionSortedRunsLocked());
                return IntPropertyResult.Value;
            case DbProperties.NumUnscheduledCompactions:
                lock (_mutex) value = IntPropertyValue.Of((long)_compactionQueue.Count);
                return IntPropertyResult.Value;
            case DbProperties.CompactionAbortCount:
                // compaction_aborted_: AbortAllCompactions / ResumeAllCompactions are not ported, so it never leaves 0.
                value = IntPropertyValue.Of(0UL);
                return IntPropertyResult.Value;
            case DbProperties.CompactionPending:
                lock (_mutex)
                {
                    // 1 if the system already determines at least one compaction is needed (HandleCompactionPending:
                    // the picker's NeedsCompaction, also with disable_auto_compactions).
                    bool pending = !cfd.Dropped && cfd.CompactionState.Picker.NeedsCompaction(cfd.Current.Storage);
                    value = IntPropertyValue.Of(pending ? 1UL : 0UL);
                }
                return IntPropertyResult.Value;
            case DbProperties.EstimatePendingCompactionBytes:
                lock (_mutex) value = IntPropertyValue.Of(sv.Current.Storage.EstimatedCompactionNeededBytes);
                return IntPropertyResult.Value;
            case DbProperties.BaseLevel:
                value = IntPropertyValue.Of((long)sv.Current.Storage.BaseLevel);
                return IntPropertyResult.Value;
            case DbProperties.ActualDelayedWriteRate:
                value = IntPropertyValue.Of(WriteControllerInstance.NeedsDelay ? WriteControllerInstance.DelayedWriteRate : 0);
                return IntPropertyResult.Value;
            case DbProperties.IsWriteStopped:
                value = IntPropertyValue.Of(WriteControllerInstance.IsStopped ? 1UL : 0UL);
                return IntPropertyResult.Value;
            case DbProperties.BackgroundErrors:
                value = IntPropertyValue.Of(ErrorHandler.ErrorCount);
                return IntPropertyResult.Value;
        }

        // ---- prefixed numeric properties
        const string atLevel = DbProperties.NumFilesAtLevelPrefix;
        if (property.StartsWith(atLevel, StringComparison.Ordinal)
            && int.TryParse(property.AsSpan(atLevel.Length), NumberStyles.None, CultureInfo.InvariantCulture, out int level))
        {
            // HandleNumFilesAtLevel: a level at or beyond num_levels is not a property (returns false).
            VersionStorageInfo s = sv.Current.Storage;
            if (level >= s.NumLevels) return IntPropertyResult.Unavailable;
            value = IntPropertyValue.Of((long)s.NumLevelFiles(level));
            return IntPropertyResult.Value;
        }
        const string atTemperature = DbProperties.LiveSstFilesSizeAtTemperaturePrefix;
        if (property.StartsWith(atTemperature, StringComparison.Ordinal))
        {
            // HandleLiveSstFilesSizeAtTemperature: any decimal number, compared with the temperature's byte value.
            if (!ulong.TryParse(property.AsSpan(atTemperature.Length), NumberStyles.None, CultureInfo.InvariantCulture, out ulong temperature))
            {
                return IntPropertyResult.Unavailable;
            }
            ulong size = 0;
            VersionStorageInfo vs = sv.Current.Storage;
            for (int l = 0; l < vs.NumLevels; l++)
            {
                foreach (FileMetaData f in vs.LevelFilesArray(l))
                {
                    if ((byte)f.Temperature == temperature) size += f.FileSize;
                }
            }
            value = IntPropertyValue.Of(size);
            return IntPropertyResult.Value;
        }
        return IntPropertyResult.NotNumeric;
    }

    /// <summary>
    /// HandleSizeAllMemTables: active, immutable and flushed-but-still-referenced memtables (pinned by iterators or kept
    /// as history), like upstream's mem()->ApproximateMemoryUsage() + imm()->ApproximateMemoryUsage(). Each memtable is
    /// counted once (the lists are a handful of entries, so the duplicate checks are linear scans, not a set).
    /// </summary>
    private long SizeAllMemTables(ColumnFamilyData cfd, SuperVersion sv)
    {
        MemTable mem = sv.Mem;
        MemTable[] imm = sv.Imm;
        long n = mem.ApproximateMemoryUsage;
        for (int i = 0; i < imm.Length; i++)
        {
            if (!InSuperVersion(imm[i], mem, imm, i)) n += imm[i].ApproximateMemoryUsage;
        }
        lock (_mutex)
        {
            List<MemTable> history = cfd.History;
            for (int i = 0; i < history.Count; i++)
            {
                MemTable m = history[i];
                if (!InSuperVersion(m, mem, imm, imm.Length) && !InList(history, m, i)) n += m.ApproximateMemoryUsage;
            }
            // A set: no duplicates among themselves.
            foreach (MemTable m in cfd.FlushedPinnedMemTables)
            {
                if (!InSuperVersion(m, mem, imm, imm.Length) && !InList(history, m, history.Count)) n += m.ApproximateMemoryUsage;
            }
        }
        return n;

        static bool InSuperVersion(MemTable m, MemTable mem, MemTable[] imm, int immCount)
        {
            if (ReferenceEquals(m, mem)) return true;
            for (int j = 0; j < immCount; j++)
            {
                if (ReferenceEquals(imm[j], m)) return true;
            }
            return false;
        }

        static bool InList(List<MemTable> list, MemTable m, int count)
        {
            for (int j = 0; j < count; j++)
            {
                if (ReferenceEquals(list[j], m)) return true;
            }
            return false;
        }
    }

    /// <summary>
    /// Version::GetMemoryUsageByTableReaders: TableCache::GetMemoryUsageByTableReader of every live file, counting only
    /// readers already open (FindTable with no_io = true).
    /// </summary>
    private static ulong EstimateTableReadersMem(ColumnFamilyData cfd, SuperVersion sv)
    {
        ulong n = 0;
        VersionStorageInfo storage = sv.Current.Storage;
        for (int l = 0; l < storage.NumLevels; l++)
        {
            foreach (FileMetaData f in storage.LevelFilesArray(l))
            {
                if (!cfd.TableCache.TryGetOpenReader(f, out TableReaderLease lease)) continue;
                using (lease) n += lease.Reader.ApproximateMemoryUsage;
            }
        }
        return n;
    }
}
