using RocksDb.Managed.Table;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Db;

/// <summary>
/// File-level DB operations: SuggestCompactRange and PromoteL0 (db/db_impl/db_impl_experimental.cc),
/// DeleteFilesInRanges, ClipColumnFamily, GetCreationTimeOfOldestFile, GetPropertiesOfTablesInRange /
/// GetPropertiesOfTablesByLevel (db/db_impl/db_impl.cc, Version::GetPropertiesOfTablesInRange in db/version_set.cc).
/// </summary>
internal sealed partial class DbImpl
{
    /// <summary>
    /// DBImpl::SuggestCompactRange: marks every file overlapping [begin, end] (user keys, null = unbounded) on all but the
    /// last non-empty level for compaction, recomputes the compaction score and schedules compactions.
    /// </summary>
    public void SuggestCompactRange(ColumnFamilyData cfd, byte[]? begin, byte[]? end)
    {
        ThrowIfDisposed();
        if (_readOnly) ThrowReadOnly();
        byte[]? startKey = begin is null ? null : MakeInternalKey(AddTimestamp(cfd, begin, max: true), DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
        byte[]? endKey = end is null ? null : MakeInternalKey(AddTimestamp(cfd, end, max: false), 0, 0);
        lock (_mutex)
        {
            if (cfd.Dropped) ThrowDropped(cfd);
            VersionStorageInfo vs = cfd.Current.Storage;
            var inputs = new List<FileMetaData>();
            for (int level = 0; level < vs.NumNonEmptyLevels - 1; ++level)
            {
                vs.GetOverlappingInputs(level, startKey, endKey, inputs);
                foreach (FileMetaData f in inputs) f.MarkedForCompaction = true;
            }
            // Since we have some more files to compact, we should also recompute compaction score.
            vs.ComputeCompactionScore();
            SchedulePendingCompaction(cfd);
            MaybeScheduleCompaction();
        }
    }

    /// <summary>
    /// DBImpl::PromoteL0: moves all L0 files to <paramref name="targetLevel"/> when they don't overlap each other, none is
    /// being compacted and levels 1..<paramref name="targetLevel"/> are empty (else InvalidArgument).
    /// </summary>
    public async ValueTask PromoteL0Async(ColumnFamilyData cfd, int targetLevel, bool synchronous, CancellationToken ct)
    {
        ThrowIfDisposed();
        if (_readOnly) ThrowReadOnly();
        if (targetLevel < 1) ThrowHelper.InvalidArgument("Invalid target level");
        var edit = new VersionEdit { ColumnFamily = cfd.Id };
        // Upstream holds the DB mutex across LogAndApply, so no compaction can pick the L0 files in between; here the
        // exclusive-manual barrier keeps automatic compactions from being scheduled while the edit is written.
        await BeginExclusiveManualAsync(synchronous, ct).ConfigureAwait(false);
        try
        {
            lock (_mutex)
            {
                if (cfd.Dropped) ThrowDropped(cfd);
                VersionStorageInfo vs = cfd.Current.Storage;
                if (targetLevel >= vs.NumLevels) ThrowHelper.InvalidArgument("Target level does not exist");
                // Sort L0 files by range.
                InternalKeyComparator icmp = cfd.Icmp;
                var l0 = new List<FileMetaData>(vs.LevelFiles(0).ToArray());
                l0.Sort((a, b) => icmp.Compare(a.Largest, b.Largest));
                // Check that no L0 file is being compacted and that they have non-overlapping ranges.
                for (int i = 0; i < l0.Count; ++i)
                {
                    if (l0[i].BeingCompacted) ThrowHelper.InvalidArgument("PromoteL0 called during L0 compaction");
                    if (i == 0) continue;
                    if (icmp.Compare(l0[i - 1].Largest, l0[i].Smallest) >= 0) ThrowHelper.InvalidArgument("L0 has overlapping files");
                }
                // Check that all levels up to target_level are empty.
                for (int level = 1; level <= targetLevel; ++level)
                {
                    if (vs.NumLevelFiles(level) > 0) ThrowHelper.InvalidArgument("All levels up to target_level must be empty");
                }
                foreach (FileMetaData f in l0)
                {
                    edit.DeleteFile(0, f.FileNumber);
                    edit.AddFile(targetLevel, f.Clone());
                }
            }
            if (edit.NewFiles.Count == 0) return;
            await Versions.LogAndApplyAsync(cfd, edit, synchronous, () => InstallSuperVersion(cfd), ct).ConfigureAwait(false);
        }
        finally
        {
            EndExclusiveManual();
        }
    }

    /// <summary>
    /// DBImpl::DeleteFilesInRanges: deletes the files of levels 1+ that lie entirely within one of
    /// <paramref name="ranges"/> (user keys; a null start/limit is unbounded; the limit is excluded unless
    /// <paramref name="includeEnd"/>). Files being compacted are skipped; L0 is never touched.
    /// </summary>
    public async ValueTask DeleteFilesInRangesAsync(ColumnFamilyData cfd, IReadOnlyList<(byte[]? Start, byte[]? Limit)> ranges, bool includeEnd,
        bool synchronous, CancellationToken ct)
    {
        ThrowIfDisposed();
        if (_readOnly) ThrowReadOnly();
        Comparator ucmp = cfd.UserComparator;
        int tsSize = cfd.TimestampSize;
        var edit = new VersionEdit { ColumnFamily = cfd.Id };
        var deleted = new HashSet<FileMetaData>(ReferenceEqualityComparer.Instance);
        DbVersion inputVersion;
        lock (_mutex)
        {
            if (cfd.Dropped) ThrowDropped(cfd);
            inputVersion = cfd.Current;
            VersionStorageInfo vs = inputVersion.Storage;
            var levelFiles = new List<FileMetaData>();
            foreach ((byte[]? start, byte[]? limit) in ranges)
            {
                // MaybeAddTimestampsToRange(..., exclusive_end = !include_end).
                byte[]? begin = start is null ? null : AddTimestamp(cfd, start, max: true);
                byte[]? end = limit is null ? null : AddTimestamp(cfd, limit, max: !includeEnd);
                for (int i = 1; i < cfd.NumLevels; i++)
                {
                    if (vs.NumLevelFiles(i) == 0 || !vs.OverlapInLevel(i, begin, begin is not null, end, end is not null)) continue;
                    byte[]? beginKey = begin is null ? null : MakeInternalKey(begin, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
                    byte[]? endKey = end is null ? null : MakeInternalKey(end, 0, 0);
                    vs.GetCleanInputsWithinInterval(i, beginKey, endKey, levelFiles, -1, out _);
                    foreach (FileMetaData f in levelFiles)
                    {
                        if (f.BeingCompacted || deleted.Contains(f)) continue;
                        if (!includeEnd && end is not null && ucmp.CompareWithoutTimestamp(DbFormat.ExtractUserKey(f.Largest), end) == 0) continue;
                        edit.DeleteFile(i, f.FileNumber);
                        deleted.Add(f);
                        f.BeingCompacted = true;
                    }
                }
            }
            if (deleted.Count > 0) vs.ComputeCompactionScore();
            if (edit.DeletedFiles.Count == 0) return;
            inputVersion.Ref();
        }
        try
        {
            await Versions.LogAndApplyAsync(cfd, edit, synchronous, () => InstallSuperVersion(cfd), ct).ConfigureAwait(false);
        }
        finally
        {
            lock (_mutex)
            {
                foreach (FileMetaData f in deleted) f.BeingCompacted = false;
                Versions.UnrefVersion(inputVersion);
                if (!cfd.Dropped)
                {
                    SchedulePendingCompaction(cfd);
                    MaybeScheduleCompaction();
                }
            }
        }
        PurgeObsoleteFiles();
    }

    /// <summary>
    /// DBImpl::ClipColumnFamily: deletes everything outside [<paramref name="beginKey"/>, <paramref name="endKey"/>):
    /// flush, drop whole files outside the range, range-delete the remaining keys outside it, then compact the whole
    /// column family (kForceOptimized) to drop the tombstones.
    /// </summary>
    public async ValueTask ClipColumnFamilyAsync(ColumnFamilyData cfd, byte[] beginKey, byte[] endKey, bool synchronous, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(beginKey);
        ArgumentNullException.ThrowIfNull(endKey);
        ThrowIfDisposed();
        if (_readOnly) ThrowReadOnly();
        if (cfd.TimestampSize != 0) ThrowHelper.InvalidArgument("Cannot call this method on column family enabling timestamp");
        // Flush memtable.
        await FlushColumnFamilyAsync(cfd, wait: true, FlushReason.DeleteFiles, synchronous, ct).ConfigureAwait(false);
        // DeleteFilesInRanges non-overlap files except L0 (begin_key is kept, so a file whose largest key is begin_key
        // must stay).
        await DeleteFilesInRangesAsync(cfd, [(null, beginKey), (endKey, null)], includeEnd: false, synchronous, ct).ConfigureAwait(false);
        // DeleteRange the remaining overlapping keys (boundary keys copied out under the mutex).
        byte[]? smallest = null, largest = null;
        lock (_mutex)
        {
            VersionStorageInfo vs = cfd.Current.Storage;
            if (vs.NumNonEmptyLevels != 0) GetSstFilesBoundaryKeys(cfd, vs, out smallest, out largest);
        }
        if (smallest is null || largest is null) return;
        Comparator ucmp = cfd.UserComparator;
        WriteBatch b = WriteBatch.Rent();
        try
        {
            // Delete [smallest_user_key, clip_begin_key)
            if (ucmp.Compare(smallest, beginKey) < 0) b.DeleteRange(cfd.Id, smallest, beginKey);
            // Delete [clip_end_key, largest_use_key]
            if (ucmp.Compare(endKey, largest) <= 0)
            {
                b.DeleteRange(cfd.Id, endKey, largest);
                b.Delete(cfd.Id, largest);
            }
            if (b.Count > 0) await WriteAsync(null, b, validateColumnFamilies: false, synchronous, ct).ConfigureAwait(false);
        }
        finally
        {
            b.Dispose();
        }
        // CompactRange delete all the tombstones.
        var compact = new CompactRangeOptions { ExclusiveManualCompaction = true, BottommostLevelCompaction = BottommostLevelCompaction.ForceOptimized };
        await CompactRangeAsync(cfd, null, null, compact, synchronous, ct).ConfigureAwait(false);
    }

    /// <summary>Version::GetSstFilesBoundaryKeys: the smallest and largest user key over all table files. REQUIRES: mutex.</summary>
    private static void GetSstFilesBoundaryKeys(ColumnFamilyData cfd, VersionStorageInfo vs, out byte[]? smallest, out byte[]? largest)
    {
        smallest = null;
        largest = null;
        Comparator ucmp = cfd.UserComparator;
        for (int level = 0; level < vs.NumNonEmptyLevels; level++)
        {
            ReadOnlySpan<FileMetaData> files = vs.LevelFiles(level);
            if (files.Length == 0) continue;
            if (level == 0)
            {
                foreach (FileMetaData f in files)
                {
                    ReadOnlySpan<byte> s = DbFormat.ExtractUserKey(f.Smallest), l = DbFormat.ExtractUserKey(f.Largest);
                    if (smallest is null || ucmp.Compare(s, smallest) < 0) smallest = s.ToArray();
                    if (largest is null || ucmp.Compare(l, largest) > 0) largest = l.ToArray();
                }
            }
            else
            {
                ReadOnlySpan<byte> s = DbFormat.ExtractUserKey(files[0].Smallest), l = DbFormat.ExtractUserKey(files[^1].Largest);
                if (smallest is null || ucmp.Compare(s, smallest) < 0) smallest = s.ToArray();
                if (largest is null || ucmp.Compare(l, largest) > 0) largest = l.ToArray();
            }
        }
    }

    /// <summary>
    /// DBImpl::GetCreationTimeOfOldestFile: the smallest file creation time (unix seconds) over all live table files of
    /// all column families; ulong.MaxValue without files and 0 if a file's creation time is unknown. NotSupported unless
    /// max_open_files is -1.
    /// </summary>
    public ulong GetCreationTimeOfOldestFile()
    {
        ThrowIfDisposed();
        if (Options.MaxOpenFiles != -1) ThrowHelper.NotSupported("This API only works if max_open_files = -1");
        ulong oldest = ulong.MaxValue;
        foreach (ColumnFamilyData cfd in GetColumnFamilies())
        {
            if (cfd.Dropped) continue;
            SuperVersion sv = GetReferencedSuperVersion(cfd);
            try
            {
                VersionStorageInfo s = sv.Current.Storage;
                for (int level = 0; level < s.NumNonEmptyLevels; level++)
                {
                    foreach (FileMetaData f in s.LevelFiles(level))
                    {
                        // FileMetaData::TryGetFileCreationTime: the MANIFEST value, else the table property.
                        ulong t = f.FileCreationTime;
                        if (t == FileMetaData.UnknownFileCreationTime)
                        {
                            using TableReaderLease lease = cfd.TableCache.GetReader(f);
                            t = lease.Reader.Properties.FileCreationTime;
                        }
                        if (t == FileMetaData.UnknownFileCreationTime) return 0;
                        if (t < oldest) oldest = t;
                    }
                }
            }
            finally
            {
                ReturnSuperVersion(sv);
            }
        }
        return oldest;
    }

    /// <summary>
    /// DBImpl::GetPropertiesOfTablesInRange (Version::GetPropertiesOfTablesInRange): the properties of the table files
    /// overlapping any of the user-key ranges [start, limit] (both inclusive at file granularity), keyed by file path.
    /// </summary>
    public Dictionary<string, TableProperties> GetPropertiesOfTablesInRange(ColumnFamilyData cfd, IReadOnlyList<KeyRange> ranges)
    {
        ArgumentNullException.ThrowIfNull(ranges);
        ThrowIfDisposed();
        SuperVersion sv = GetReferencedSuperVersion(cfd);
        try
        {
            var result = new Dictionary<string, TableProperties>(StringComparer.Ordinal);
            VersionStorageInfo vs = sv.Current.Storage;
            var files = new List<FileMetaData>();
            var keys = new List<(byte[] K1, byte[] K2)>(ranges.Count);
            foreach (KeyRange r in ranges)
            {
                // MaybeAddTimestampsToRange(exclusive_end = false); InternalKey(key, kMaxSequenceNumber, kValueTypeForSeek).
                keys.Add((MakeInternalKey(AddTimestamp(cfd, r.Start.Span, max: true), DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek),
                    MakeInternalKey(AddTimestamp(cfd, r.Limit.Span, max: false), DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek)));
            }
            for (int level = 0; level < vs.NumNonEmptyLevels; level++)
            {
                foreach ((byte[] k1, byte[] k2) in keys)
                {
                    vs.GetOverlappingInputs(level, k1, k2, files, -1, out _, expandRange: false);
                    foreach (FileMetaData f in files)
                    {
                        string fname = FileNames.TableFileName(DbPaths, f.FileNumber, f.PathId);
                        if (result.ContainsKey(fname)) continue;
                        using TableReaderLease lease = cfd.TableCache.GetReader(f);
                        result[fname] = lease.Reader.Properties;
                    }
                }
            }
            return result;
        }
        finally
        {
            ReturnSuperVersion(sv);
        }
    }

    /// <summary>DBImpl::GetPropertiesOfTablesByLevel: one collection (file path to properties) per level.</summary>
    public List<Dictionary<string, TableProperties>> GetPropertiesOfTablesByLevel(ColumnFamilyData cfd)
    {
        ThrowIfDisposed();
        SuperVersion sv = GetReferencedSuperVersion(cfd);
        try
        {
            VersionStorageInfo vs = sv.Current.Storage;
            var result = new List<Dictionary<string, TableProperties>>(vs.NumLevels);
            for (int level = 0; level < vs.NumLevels; level++)
            {
                var props = new Dictionary<string, TableProperties>(StringComparer.Ordinal);
                foreach (FileMetaData f in vs.LevelFiles(level))
                {
                    using TableReaderLease lease = cfd.TableCache.GetReader(f);
                    props[FileNames.TableFileName(DbPaths, f.FileNumber, f.PathId)] = lease.Reader.Properties;
                }
                result.Add(props);
            }
            return result;
        }
        finally
        {
            ReturnSuperVersion(sv);
        }
    }

    /// <summary>
    /// The user key with the maximum (<paramref name="max"/>) or minimum timestamp appended when the column family has
    /// user-defined timestamps (AppendKeyWithMaxTimestamp / AppendKeyWithMinTimestamp); the key itself otherwise.
    /// </summary>
    private static byte[] AddTimestamp(ColumnFamilyData cfd, ReadOnlySpan<byte> userKey, bool max)
    {
        int tsSize = cfd.TimestampSize;
        if (tsSize == 0) return userKey.ToArray();
        var w = new PooledBufferWriter(userKey.Length + tsSize);
        try
        {
            if (max) DbFormat.AppendKeyWithMaxTimestamp(w, userKey, tsSize);
            else DbFormat.AppendKeyWithMinTimestamp(w, userKey, tsSize);
            return w.WrittenSpan.ToArray();
        }
        finally
        {
            w.Dispose();
        }
    }
}
