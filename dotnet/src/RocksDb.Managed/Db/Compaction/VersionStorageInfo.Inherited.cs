namespace RocksDb.Managed.Db;

/// <summary>
/// Accumulated data-entry stats of the files whose table properties were loaded (VersionStorageInfo
/// accumulated_file_size_, accumulated_raw_key_size_, accumulated_raw_value_size_, accumulated_num_non_deletions_,
/// accumulated_num_deletions_). Carried from version to version.
/// </summary>
internal readonly record struct AccumulatedFileStats(ulong FileSize, ulong RawKeySize, ulong RawValueSize, ulong NumNonDeletions, ulong NumDeletions)
{
    /// <summary>
    /// Port of VersionStorageInfo::GetAverageValueSize: the average value size of the sampled files scaled to the
    /// on-disk size (raw value size / non-deletions * file size / raw size).
    /// </summary>
    public ulong AverageValueSize
    {
        get
        {
            if (NumNonDeletions == 0) return 0;
            ulong raw = RawKeySize + RawValueSize;
            if (raw == 0) return 0;
            return RawValueSize / NumNonDeletions * FileSize / raw;
        }
    }

    /// <summary>Port of VersionStorageInfo::UpdateAccumulatedStats(FileMetaData*).</summary>
    public AccumulatedFileStats Add(FileMetaData f) =>
        new(FileSize + f.FileSize, RawKeySize + f.RawKeySize, RawValueSize + f.RawValueSize,
            NumNonDeletions + (f.NumEntries - f.NumDeletions), NumDeletions + f.NumDeletions);
}

/// <summary>
/// What a new <see cref="VersionStorageInfo"/> takes over from the previous version of its column family and from the
/// version edit (the ref_vstorage copy in the VersionStorageInfo constructor, VersionBuilder::SaveTo's compact cursors,
/// Version::PrepareAppend's stats update).
/// </summary>
internal sealed class VersionBuildContext
{
    /// <summary>The column family's current storage the new one is derived from (ref_vstorage), or null.</summary>
    public VersionStorageInfo? RefStorage { get; init; }

    /// <summary>Compact cursors of the version edit (applied over the inherited ones), or recovered from the MANIFEST.</summary>
    public IReadOnlyList<CompactCursor>? CompactCursors { get; init; }

    /// <summary>Accumulated stats after <see cref="VersionStorageInfo.UpdateAccumulatedStats"/>; null keeps the ref's.</summary>
    public AccumulatedFileStats? AccumulatedStats { get; init; }

    /// <summary>allow_ingest_behind (DB option) or cf_allow_ingest_behind: no bottommost-file marking.</summary>
    public bool AllowIngestBehind { get; init; }

    /// <summary>The column family's full_history_ts_low (user-defined timestamps), or null.</summary>
    public byte[]? FullHistoryTsLow { get; init; }
}

/// <summary>
/// The parts of VersionStorageInfo inherited across versions and the bottommost-file marking: compact cursors
/// (compact_cursor_), accumulated entry stats and compensated file sizes (ComputeCompensatedSizes,
/// Version::UpdateAccumulatedStats / MaybeInitializeFileMetaData), the oldest snapshot and the bottommost files
/// (GenerateBottommostFiles, UpdateOldestSnapshot, ComputeBottommostFilesMarkedForCompaction). Port of the
/// corresponding parts of db/version_set.{h,cc}.
/// </summary>
internal sealed partial class VersionStorageInfo
{
    /// <summary>kMaxInitCount of Version::UpdateAccumulatedStats: table properties loaded per version (I/O cap).</summary>
    internal const int MaxStatsInitCount = 20;

    /// <summary>kDeletionWeightOnCompaction of ComputeCompensatedSizes.</summary>
    private const int DeletionWeightOnCompaction = 2;

    private byte[]?[] _compactCursors = null!;
    private readonly List<(int Level, FileMetaData File)> _bottommostFiles = new();
    private readonly List<(int Level, FileMetaData File)> _bottommostFilesMarkedForCompaction = new();

    /// <summary>Accumulated entry stats of this version (inherited and updated by new files).</summary>
    public AccumulatedFileStats AccumulatedStats { get; private set; }

    /// <summary>
    /// oldest_snapshot_seqnum(): a lower bound of the oldest live snapshot (or of the last sequence when there was
    /// none) as of the last snapshot release. Inherited by new versions. Guarded by the DB mutex.
    /// </summary>
    public ulong OldestSnapshotSeqnum { get; private set; }

    /// <summary>
    /// bottommost_files_mark_threshold(): the smallest largest-seqno of an unmarked bottommost file that a snapshot
    /// still protects; releasing snapshots past it may mark files. Guarded by the DB mutex.
    /// </summary>
    public ulong BottommostFilesMarkThreshold { get; private set; } = DbFormat.MaxSequenceNumber;

    /// <summary>
    /// GetPreserveTimeMinSeqno(): sequence numbers at or above this keep their value at the bottommost level
    /// (preserve_internal_time_seconds / preclude_last_level_data_seconds window). Inherited; set by the DB when a
    /// super version is installed (DBImpl::MaybeUpdatePreserveTimeMinSeqno). Guarded by the DB mutex.
    /// </summary>
    public ulong PreserveTimeMinSeqno { get; private set; } = DbFormat.MaxSequenceNumber;

    /// <summary>allow_ingest_behind || cf_allow_ingest_behind of the column family.</summary>
    public bool AllowIngestBehind { get; private set; }

    /// <summary>The column family's full_history_ts_low as last set (user-defined timestamps). Guarded by the DB mutex.</summary>
    public byte[]? FullHistoryTsLow { get; set; }

    /// <summary>BottommostFilesMarkedForCompaction(): bottommost files whose largest seqno can be zeroed.</summary>
    public List<(int Level, FileMetaData File)> BottommostFilesMarkedForCompaction => _bottommostFilesMarkedForCompaction;

    /// <summary>The files no older sorted run overlaps (bottommost_files_).</summary>
    internal List<(int Level, FileMetaData File)> BottommostFiles => _bottommostFiles;

    /// <summary>GetCompactCursors()[level]: the round-robin cursor (internal key) of a level, or null.</summary>
    public byte[]? GetCompactCursor(int level) => (uint)level < (uint)_compactCursors.Length ? _compactCursors[level] : null;

    /// <summary>The valid compact cursors by level (VersionEdit::SetCompactCursors for a MANIFEST snapshot).</summary>
    public IEnumerable<CompactCursor> ValidCompactCursors()
    {
        for (int level = 0; level < _compactCursors.Length; level++)
        {
            if (_compactCursors[level] is { Length: > 0 } c) yield return new CompactCursor(level, c);
        }
    }

    /// <summary>
    /// Port of VersionStorageInfo::GetNextCompactCursor: the cursor recorded after a round-robin compaction of
    /// <paramref name="increment"/> files of <paramref name="level"/> -- the smallest key of the file that follows them in
    /// compaction-priority order (wrapping around to the first). REQUIRES: DB mutex.
    /// </summary>
    public byte[] GetNextCompactCursor(int level, int increment)
    {
        int[] byPri = _filesByCompactionPri[level];
        int cmpIdx = _nextFileToCompactBySize[level] + increment;
        if (cmpIdx >= byPri.Length) cmpIdx = 0;
        return _files[level][byPri[cmpIdx]].Smallest;
    }

    private void InitializeInheritedState(VersionBuildContext? context)
    {
        _compactCursors = new byte[]?[NumLevels];
        VersionStorageInfo? refStorage = context?.RefStorage;
        if (refStorage is not null)
        {
            // VersionStorageInfo::VersionStorageInfo with ref_vstorage.
            AccumulatedStats = refStorage.AccumulatedStats;
            OldestSnapshotSeqnum = refStorage.OldestSnapshotSeqnum;
            PreserveTimeMinSeqno = refStorage.PreserveTimeMinSeqno;
            Array.Copy(refStorage._compactCursors, _compactCursors, Math.Min(refStorage._compactCursors.Length, NumLevels));
        }
        if (context is not null)
        {
            if (context.AccumulatedStats is { } stats) AccumulatedStats = stats;
            AllowIngestBehind = context.AllowIngestBehind;
            FullHistoryTsLow = context.FullHistoryTsLow;
            if (context.CompactCursors is { } cursors)
            {
                // VersionBuilder::ApplyCompactCursors: levels >= num_levels are omitted (re-open with fewer levels).
                foreach (CompactCursor c in cursors)
                {
                    if (c.Level >= 0 && c.Level < NumLevels) _compactCursors[c.Level] = c.InternalKey;
                }
            }
        }
        ComputeCompensatedSizes();
    }

    /// <summary>
    /// Port of Version::UpdateAccumulatedStats: loads the entry stats (MaybeInitializeFileMetaData) of up to
    /// <see cref="MaxStatsInitCount"/> files that have no compensated size yet (unlimited with an infinite table cache,
    /// max_open_files = -1), lowest levels first, adding them to <paramref name="accumulated"/>; if all sampled files
    /// held only deletions, files from the highest levels are loaded until a raw value size is known. Reads table
    /// properties through <paramref name="loadProperties"/> (null result = could not be read). Runs before the
    /// version is published (under the MANIFEST lock, without the DB mutex).
    /// </summary>
    internal static AccumulatedFileStats UpdateAccumulatedStats(FileMetaData[][] files, AccumulatedFileStats accumulated, bool infiniteTableCache,
        Func<FileMetaData, Table.TableProperties?> loadProperties)
    {
        int initCount = 0;
        for (int level = 0; level < files.Length && initCount < MaxStatsInitCount; level++)
        {
            foreach (FileMetaData f in files[level])
            {
                if (!MaybeInitializeFileMetaData(f, loadProperties)) continue;
                // each FileMeta will be initialized only once.
                accumulated = accumulated.Add(f);
                // when option "max_open_files" is -1, all the file metadata has already been read, so
                // MaybeInitializeFileMetaData() won't incur any I/O cost.
                if (infiniteTableCache) continue;
                if (++initCount >= MaxStatsInitCount) break;
            }
        }
        // In case all sampled-files contain only deletion entries, then we load the table-property of a file in
        // higher-level to initialize that value.
        for (int level = files.Length - 1; accumulated.RawValueSize == 0 && level >= 0; level--)
        {
            for (int i = files[level].Length - 1; accumulated.RawValueSize == 0 && i >= 0; i--)
            {
                if (MaybeInitializeFileMetaData(files[level][i], loadProperties)) accumulated = accumulated.Add(files[level][i]);
            }
        }
        return accumulated;
    }

    /// <summary>Port of Version::MaybeInitializeFileMetaData.</summary>
    private static bool MaybeInitializeFileMetaData(FileMetaData f, Func<FileMetaData, Table.TableProperties?> loadProperties)
    {
        if (f.InitStatsFromFile || f.CompensatedFileSize > 0) return false;
        f.InitStatsFromFile = true;
        if (f.EntryStatsKnown) return true;
        Table.TableProperties? tp = loadProperties(f);
        if (tp is null) return false;
        f.SetEntryStats(tp);
        return true;
    }

    /// <summary>
    /// Port of VersionStorageInfo::ComputeCompensatedSizes: for files without a compensated size, the file size plus,
    /// when point deletions are at least half of the entries, the excess deletions weighted by the average value size
    /// (kDeletionWeightOnCompaction), plus the range-deletion compensation.
    /// </summary>
    private void ComputeCompensatedSizes()
    {
        ulong averageValueSize = AccumulatedStats.AverageValueSize;
        for (int level = 0; level < NumLevels; level++)
        {
            foreach (FileMetaData f in _files[level])
            {
                // Only for files that have been created right now and no other thread has access to them.
                if (f.CompensatedFileSize != 0) continue;
                ulong size = f.FileSize;
                // Here we only boost the size of deletion entries of a file only when the number of deletion entries is
                // greater than the number of non-deletion entries in the file. In a stable workload the number of
                // deletion entries should be roughly equal to the number of non-deletion entries; compensating them
                // there might change the shape of the LSM tree.
                ulong pointDeletions = f.NumDeletions - f.NumRangeDeletions;
                if (pointDeletions * 2 >= f.NumEntries)
                {
                    size += (pointDeletions * 2 - f.NumEntries) * averageValueSize * DeletionWeightOnCompaction;
                }
                size += f.CompensatedRangeDeletionSize;
                f.CompensatedFileSize = size;
            }
        }
    }

    /// <summary>Port of VersionStorageInfo::GenerateBottommostFiles.</summary>
    private void GenerateBottommostFiles()
    {
        _bottommostFiles.Clear();
        for (int level = 0; level < NumNonEmptyLevels; level++)
        {
            FileMetaData[] files = _files[level];
            for (int i = 0; i < files.Length; i++)
            {
                FileMetaData f = files[i];
                if (!RangeMightExistAfterSortedRun(DbFormat.ExtractUserKey(f.Smallest), DbFormat.ExtractUserKey(f.Largest), level, level == 0 ? i : -1))
                {
                    _bottommostFiles.Add((level, f));
                }
            }
        }
    }

    /// <summary>
    /// Port of VersionStorageInfo::UpdateOldestSnapshot: records a new oldest snapshot and, when it passes the mark
    /// threshold, re-marks the bottommost files. REQUIRES: DB mutex.
    /// </summary>
    public void UpdateOldestSnapshot(ulong seqnum)
    {
        if (seqnum < OldestSnapshotSeqnum) return;
        OldestSnapshotSeqnum = seqnum;
        if (OldestSnapshotSeqnum > BottommostFilesMarkThreshold) ComputeBottommostFilesMarkedForCompaction();
    }

    /// <summary>
    /// SetPreserveTimeMinSeqno (DBImpl::MaybeUpdatePreserveTimeMinSeqno). Returns true if the value changed. REQUIRES:
    /// DB mutex.
    /// </summary>
    public bool SetPreserveTimeMinSeqno(ulong seqno)
    {
        bool changed = PreserveTimeMinSeqno != seqno;
        PreserveTimeMinSeqno = seqno;
        return changed;
    }

    /// <summary>Re-marks the bottommost files (after the preserve-time boundary changed). REQUIRES: DB mutex.</summary>
    internal void RecomputeBottommostFilesMarkedForCompaction() => ComputeBottommostFilesMarkedForCompaction();

    /// <summary>
    /// Port of VersionStorageInfo::ComputeBottommostFilesMarkedForCompaction: bottommost files (not being compacted)
    /// with a nonzero largest seqno below the oldest snapshot are marked -- when a compaction could actually zero their
    /// largest seqno (the preserve-time window and, with user-defined timestamps, full_history_ts_low; see
    /// BottommostSeqnoCanBeZeroed) and, with bottommost_file_compaction_delay, when old enough. Files still protected by
    /// a snapshot lower the mark threshold. REQUIRES: DB mutex.
    /// </summary>
    private void ComputeBottommostFilesMarkedForCompaction()
    {
        _bottommostFilesMarkedForCompaction.Clear();
        BottommostFilesMarkThreshold = DbFormat.MaxSequenceNumber;
        if (AllowIngestBehind) return;
        // If a file's creation time is larger than creation_time_ub, it is too new to be marked for compaction.
        long creationTimeUb = 0;
        uint delay = _options.BottommostFileCompactionDelay;
        bool needsDelay = delay > 0;
        if (needsDelay) creationTimeUb = NowSeconds() - delay;
        Comparator ucmp = Icmp.UserComparator;
        int tsSize = ucmp.TimestampSize;
        bool fullHistoryTsLowSet = FullHistoryTsLow is { Length: > 0 };
        // Max seqno a bottommost compaction could zero given the preserve window (CompactionJob further caps it with the
        // earliest snapshot, which the snapshot gate below already guarantees).
        ulong preserveSeqnoAfter = Math.Max(PreserveTimeMinSeqno, 1UL) - 1;
        foreach ((int level, FileMetaData f) in _bottommostFiles)
        {
            if (f.BeingCompacted || f.LargestSeqno == 0) continue;
            // largest_seqno might be nonzero due to containing the final key in an earlier compaction, whose seqnum we
            // didn't zero out.
            if (f.LargestSeqno < OldestSnapshotSeqnum)
            {
                // A file whose max timestamp is below full_history_ts_low (or unknown) can have its timestamp history
                // collapsed and its seqno zeroed.
                bool tsBelowFullHistoryTsLow = false;
                if (tsSize > 0 && fullHistoryTsLowSet)
                {
                    tsBelowFullHistoryTsLow = f.MaxTimestamp.Length == 0 || ucmp.CompareTimestamp(f.MaxTimestamp, FullHistoryTsLow) < 0;
                }
                if (!BottommostSeqnoCanBeZeroed(f.LargestSeqno, preserveSeqnoAfter, tsSize, fullHistoryTsLowSet, tsBelowFullHistoryTsLow)) continue;
                if (!needsDelay)
                {
                    _bottommostFilesMarkedForCompaction.Add((level, f));
                }
                else if (creationTimeUb > 0)
                {
                    long creationTime = (long)f.FileCreationTime;
                    if (creationTime == (long)FileMetaData.UnknownFileCreationTime || creationTime <= creationTimeUb)
                    {
                        _bottommostFilesMarkedForCompaction.Add((level, f));
                    }
                    // else: too new; tried again the next time this method runs (new version, compaction picked,
                    // snapshot released, periodic trigger).
                }
                // else: creation_time_ub <= 0, all files are too new to be marked for compaction.
            }
            else
            {
                BottommostFilesMarkThreshold = Math.Min(BottommostFilesMarkThreshold, f.LargestSeqno);
            }
        }
    }

    /// <summary>
    /// Port of BottommostSeqnoCanBeZeroed (db/dbformat.h): whether a bottommost compaction may zero sequence number
    /// <paramref name="seq"/>, shared by the bottommost-file marking and the compaction iterator.
    /// </summary>
    internal static bool BottommostSeqnoCanBeZeroed(ulong seq, ulong preserveSeqnoAfter, int tsSize, bool fullHistoryTsLowSet, bool tsBelowFullHistoryTsLow)
    {
        if (seq > preserveSeqnoAfter) return false;
        if (tsSize == 0) return true;
        return fullHistoryTsLowSet && tsBelowFullHistoryTsLow;
    }
}
