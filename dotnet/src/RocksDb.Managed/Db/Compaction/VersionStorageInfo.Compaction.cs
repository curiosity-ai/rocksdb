using System.Globalization;
using System.Text;

namespace RocksDb.Managed.Db;

/// <summary>
/// Compaction-related state of a version: level target sizes (static and <c>level_compaction_dynamic_level_bytes</c>),
/// base level, compaction scores, files ordered by compaction priority, files marked for compaction (TTL, periodic),
/// estimated pending compaction bytes and overlap queries. Port of the corresponding parts of VersionStorageInfo
/// (db/version_set.cc: CalculateBaseBytes, ComputeCompactionScore, EstimateCompactionBytesNeeded,
/// UpdateFilesByCompactionPri, SortFileByOverlappingRatio, ComputeExpiredTtlFiles,
/// ComputeFilesMarkedForPeriodicCompaction, GetOverlappingInputs, GetCleanInputsWithinInterval,
/// GetOverlappingInputsRangeBinarySearch, OverlapInLevel, RangeMightExistAfterSortedRun).
/// <para>
/// The file lists are immutable; the scores depend on <see cref="FileMetaData.BeingCompacted"/> and are recomputed
/// (<see cref="ComputeCompactionScore"/>) under the DB mutex whenever compactions are registered or released.
/// Readers that only use the file lists don't need the mutex.
/// </para>
/// </summary>
internal sealed partial class VersionStorageInfo
{
    /// <summary>kNumberFilesToSort: only the top files of each level are sorted by priority.</summary>
    public const int NumberFilesToSort = 50;

    private ColumnFamilyOptions _options = null!;
    private ulong[] _levelMaxBytes = null!;
    private double[] _compactionScore = null!;
    private int[] _compactionLevel = null!;
    private int[][] _filesByCompactionPri = null!;
    private int[] _nextFileToCompactBySize = null!;
    private readonly List<(int Level, FileMetaData File)> _filesMarkedForCompaction = new();
    private readonly List<(int Level, FileMetaData File)> _expiredTtlFiles = new();
    private readonly List<(int Level, FileMetaData File)> _filesMarkedForPeriodicCompaction = new();
    private readonly List<(int Level, FileMetaData File)> _filesMarkedForForcedBlobGC = new();
    private readonly List<(int Level, FileMetaData File)> _readTriggeredCompactionFiles = new();

    /// <summary>The column family options this version was built with.</summary>
    public ColumnFamilyOptions Options => _options;

    public CompactionStyle CompactionStyle => _options.CompactionStyle;

    /// <summary>base_level(): the level L0 compacts into (-1 for universal/FIFO without levels).</summary>
    public int BaseLevel { get; private set; }

    /// <summary>Levels at or above this (and &gt; 0) hold more data than needed with dynamic level bytes (-1 = none).</summary>
    public int LowestUnnecessaryLevel { get; private set; } = -1;

    /// <summary>num_non_empty_levels(): one past the last non-empty level.</summary>
    public int NumNonEmptyLevels { get; private set; }

    /// <summary>level0_non_overlapping(): L0 files don't overlap each other.</summary>
    public bool Level0NonOverlapping { get; private set; }

    /// <summary>l0_delay_trigger_count(): L0 file count used for write stalls (sorted runs for universal).</summary>
    public int L0DelayTriggerCount { get; private set; }

    /// <summary>estimated_compaction_needed_bytes(). Guarded by the DB mutex.</summary>
    public ulong EstimatedCompactionNeededBytes { get; private set; }

    /// <summary>TEST_set_estimated_compaction_needed_bytes.</summary>
    internal void TestSetEstimatedCompactionNeededBytes(ulong bytes) => EstimatedCompactionNeededBytes = bytes;

    /// <summary>set_l0_delay_trigger_count (tests).</summary>
    internal void TestSetL0DelayTriggerCount(int count) => L0DelayTriggerCount = count;

    /// <summary>Files marked for compaction by a table property collector (FileMetaData::marked_for_compaction).</summary>
    public List<(int Level, FileMetaData File)> FilesMarkedForCompaction => _filesMarkedForCompaction;

    /// <summary>
    /// standalone_range_tombstone_files_mark_threshold(): the smallest seqno of a standalone range tombstone file marked
    /// for compaction (not being compacted), or kMaxSequenceNumber. The universal picker defers such a file while a
    /// snapshot older than it exists; once the oldest snapshot reaches this threshold, releasing a snapshot schedules a
    /// compaction again (DBImpl::ReleaseSnapshot). Computed with the files marked for compaction (also after a compaction
    /// is registered, RemoveFilesBeingCompactedFromCompactionCandidates). Guarded by the DB mutex.
    /// </summary>
    public ulong StandaloneRangeTombstoneFilesMarkThreshold { get; private set; } = DbFormat.MaxSequenceNumber;
    public List<(int Level, FileMetaData File)> ExpiredTtlFiles => _expiredTtlFiles;
    public List<(int Level, FileMetaData File)> FilesMarkedForPeriodicCompaction => _filesMarkedForPeriodicCompaction;
    /// <summary>Table files linked to the oldest blob files when their garbage ratio reaches the force threshold.</summary>
    public List<(int Level, FileMetaData File)> FilesMarkedForForcedBlobGC => _filesMarkedForForcedBlobGC;
    /// <summary>
    /// ReadTriggeredCompactionFiles(): files whose sampled collapsible-entry reads per byte exceed
    /// read_triggered_compaction_threshold, hottest first.
    /// </summary>
    public List<(int Level, FileMetaData File)> ReadTriggeredCompactionFiles => _readTriggeredCompactionFiles;

    /// <summary>MaxBytesForLevel: target size of a level (L0 is governed by file count).</summary>
    public ulong MaxBytesForLevel(int level) => _levelMaxBytes[level];

    /// <summary>CompactionScore(i): i-th highest score (sorted descending).</summary>
    public double CompactionScore(int index) => _compactionScore[index];

    /// <summary>CompactionScoreLevel(i): level of the i-th highest score.</summary>
    public int CompactionScoreLevel(int index) => _compactionLevel[index];

    /// <summary>Score of <paramref name="level"/> (unsorted lookup), 0 for levels without a score.</summary>
    public double ScoreOfLevel(int level)
    {
        for (int i = 0; i < _compactionLevel.Length; i++)
        {
            if (_compactionLevel[i] == level) return _compactionScore[i];
        }
        return 0;
    }

    /// <summary>FilesByCompactionPri(level): indexes into LevelFiles(level), best candidate first.</summary>
    public int[] FilesByCompactionPri(int level) => _filesByCompactionPri[level];

    public int NextCompactionIndex(int level) => _nextFileToCompactBySize[level];
    public void SetNextCompactionIndex(int level, int index) => _nextFileToCompactBySize[level] = index;
    public void ResetNextCompactionIndex(int level) => _nextFileToCompactBySize[level] = 0;

    /// <summary>MaxInputLevel(): the last level a leveled compaction can read from.</summary>
    public int MaxInputLevel => CompactionStyle == CompactionStyle.Level ? NumLevels - 2 : 0;

    /// <summary>MaxOutputLevel(allow_ingest_behind).</summary>
    public int MaxOutputLevel(bool allowIngestBehind) => allowIngestBehind ? NumLevels - 2 : NumLevels - 1;

    /// <summary>
    /// compensated_file_size: file size plus the point-deletion boost and the estimated size of data its range
    /// tombstones cover (computed when the file first appears in a version, see ComputeCompensatedSizes).
    /// </summary>
    public static ulong CompensatedFileSize(FileMetaData f) =>
        f.CompensatedFileSize != 0 ? f.CompensatedFileSize : f.FileSize + f.CompensatedRangeDeletionSize;

    private void InitializeCompactionState(ColumnFamilyOptions options)
    {
        _options = options;
        _levelMaxBytes = new ulong[NumLevels];
        int scored = Math.Max(1, NumLevels - 1);
        _compactionScore = new double[scored];
        _compactionLevel = new int[scored];
        _filesByCompactionPri = new int[NumLevels][];
        _nextFileToCompactBySize = new int[NumLevels];
        int nonEmpty = 0;
        for (int l = 0; l < NumLevels; l++)
        {
            if (_files[l].Length > 0) nonEmpty = l + 1;
        }
        NumNonEmptyLevels = nonEmpty;
        Level0NonOverlapping = ComputeLevel0NonOverlapping();
        CalculateBaseBytes();
        UpdateFilesByCompactionPri();
        GenerateBottommostFiles();
        ComputeCompactionScore();
    }

    /// <summary>
    /// Port of VersionStorageInfo::GenerateLevel0NonOverlapping: L0 files sorted by smallest key overlap when a file's
    /// largest INTERNAL key is not before the next file's smallest one. Two files that only share a boundary user key
    /// (k@9 in one, k@3 in the other) do not overlap; moved to a level &gt; 0 they are adjacent files there.
    /// </summary>
    private bool ComputeLevel0NonOverlapping()
    {
        FileMetaData[] l0 = _files[0];
        if (l0.Length <= 1) return true;
        InternalKeyComparator icmp = Icmp;
        FileMetaData[] rented = RocksArrayPool<FileMetaData>.Shared.Rent(l0.Length);
        try
        {
            Span<FileMetaData> sorted = rented.AsSpan(0, l0.Length);
            l0.CopyTo(sorted);
            sorted.Sort(new SmallestKeyComparer(icmp));
            for (int i = 1; i < sorted.Length; i++)
            {
                if (icmp.Compare(sorted[i - 1].Largest, sorted[i].Smallest) >= 0) return false;
            }
            return true;
        }
        finally
        {
            RocksArrayPool<FileMetaData>.Shared.Return(rented, clearArray: true);
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // CalculateBaseBytes

    private static ulong MultiplyCheckOverflow(ulong op1, double op2)
    {
        if (op1 == 0 || op2 <= 0) return 0;
        if (ulong.MaxValue / op1 < op2) return op1;
        double r = op1 * op2;
        return r >= ulong.MaxValue ? ulong.MaxValue : (ulong)r;
    }

    private double MaxBytesMultiplierAdditional(int level) =>
        level < _options.MaxBytesForLevelMultiplierAdditional.Count ? _options.MaxBytesForLevelMultiplierAdditional[level] : 1;

    /// <summary>Port of VersionStorageInfo::CalculateBaseBytes.</summary>
    private void CalculateBaseBytes()
    {
        int numL0Count = _files[0].Length;
        if (CompactionStyle == CompactionStyle.Universal)
        {
            for (int i = 1; i < NumLevels; i++)
            {
                if (_files[i].Length > 0) numL0Count++;
            }
        }
        L0DelayTriggerCount = numL0Count;

        ulong levelBase = _options.MaxBytesForLevelBase;
        double multiplier = _options.MaxBytesForLevelMultiplier;
        LowestUnnecessaryLevel = -1;
        if (!_options.LevelCompactionDynamicLevelBytes || CompactionStyle != CompactionStyle.Level)
        {
            BaseLevel = CompactionStyle == CompactionStyle.Level ? 1 : -1;
            for (int i = 0; i < NumLevels; i++)
            {
                if (i == 0 && CompactionStyle == CompactionStyle.Universal) _levelMaxBytes[i] = levelBase;
                else if (i > 1)
                    _levelMaxBytes[i] = MultiplyCheckOverflow(MultiplyCheckOverflow(_levelMaxBytes[i - 1], multiplier), MaxBytesMultiplierAdditional(i - 1));
                else _levelMaxBytes[i] = levelBase;
            }
            return;
        }

        ulong maxLevelSize = 0;
        int firstNonEmptyLevel = -1;
        // Find size of non-L0 level of most data. Cannot use the size of the last level because it can be empty or
        // less than previous levels after compaction.
        for (int i = 1; i < NumLevels; i++)
        {
            ulong total = _levelBytes[i];
            if (total > 0 && firstNonEmptyLevel == -1) firstNonEmptyLevel = i;
            if (total > maxLevelSize) maxLevelSize = total;
        }
        // Prefill every level's max bytes to disallow compaction from there.
        for (int i = 0; i < NumLevels; i++) _levelMaxBytes[i] = ulong.MaxValue;

        if (maxLevelSize == 0)
        {
            // No data for L1 and up. L0 compacts to last level directly. No compaction from L1+ needs to be scheduled.
            BaseLevel = NumLevels - 1;
            return;
        }
        ulong baseBytesMax = levelBase;
        ulong baseBytesMin = (ulong)(baseBytesMax / multiplier);

        // Try whether we can make last level's target size to be max_level_size.
        ulong curLevelSize = maxLevelSize;
        for (int i = NumLevels - 2; i >= firstNonEmptyLevel; i--)
        {
            curLevelSize = (ulong)(curLevelSize / multiplier);
            if (LowestUnnecessaryLevel == -1 && curLevelSize <= baseBytesMin
                && (_options.PrecludeLastLevelDataSeconds == 0 || i < NumLevels - 2))
            {
                LowestUnnecessaryLevel = i;
            }
        }

        ulong baseLevelSize;
        if (curLevelSize <= baseBytesMin)
        {
            // Case 1. If we make target size of last level to be max_level_size, target size of the first non-empty
            // level would be smaller than base_bytes_min. We set it be base_bytes_min.
            baseLevelSize = baseBytesMin + 1;
            BaseLevel = firstNonEmptyLevel;
        }
        else
        {
            // Find base level (where L0 data is compacted to).
            BaseLevel = firstNonEmptyLevel;
            while (BaseLevel > 1 && curLevelSize > baseBytesMax)
            {
                --BaseLevel;
                curLevelSize = (ulong)(curLevelSize / multiplier);
            }
            baseLevelSize = curLevelSize > baseBytesMax ? baseBytesMax : Math.Max(1UL, curLevelSize);
        }

        ulong levelSize = baseLevelSize;
        for (int i = BaseLevel; i < NumLevels; i++)
        {
            if (i > BaseLevel) levelSize = MultiplyCheckOverflow(levelSize, multiplier);
            // Don't set any level below base_bytes_max. Otherwise, the LSM can assume an hourglass shape where L1+
            // sizes are smaller than L0.
            _levelMaxBytes[i] = Math.Max(levelSize, baseBytesMax);
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // ComputeCompactionScore

    /// <summary>
    /// Port of VersionStorageInfo::ComputeCompactionScore (+ ComputeExpiredTtlFiles,
    /// ComputeFilesMarkedForPeriodicCompaction, ComputeFilesMarkedForCompaction, EstimateCompactionBytesNeeded).
    /// REQUIRES: DB mutex (reads <see cref="FileMetaData.BeingCompacted"/>).
    /// </summary>
    public void ComputeCompactionScore()
    {
        double totalDowncompactBytes = 0.0;
        const double ScoreScale = 10.0;
        int maxOutputLevel = MaxOutputLevel(_options.AllowIngestBehind);
        int scored = _compactionScore.Length;
        Array.Clear(_compactionScore);
        for (int i = 0; i < scored; i++) _compactionLevel[i] = i;
        int maxInput = Math.Min(MaxInputLevel, scored - 1);
        for (int level = 0; level <= maxInput; level++)
        {
            double score;
            if (level == 0)
            {
                int numSortedRuns = 0;
                ulong totalSize = 0;
                foreach (FileMetaData f in _files[0])
                {
                    totalDowncompactBytes += f.FileSize;
                    if (!f.BeingCompacted)
                    {
                        totalSize += CompensatedFileSize(f);
                        numSortedRuns++;
                    }
                }
                if (CompactionStyle == CompactionStyle.Universal)
                {
                    for (int i = 1; i <= maxOutputLevel && i < NumLevels; i++)
                    {
                        if (_files[i].Length > 0 && !_files[i][0].BeingCompacted) numSortedRuns++;
                    }
                }
                if (CompactionStyle == CompactionStyle.Fifo)
                {
                    CompactionOptionsFifo fifo = _options.CompactionOptionsFifo;
                    ulong effectiveSize = totalSize;
                    ulong effectiveMax = fifo.MaxTableFilesSize;
                    if (fifo.MaxDataFilesSize > 0)
                    {
                        // Blob-aware: include blob file sizes in the total.
                        effectiveSize += TotalBlobFileSize();
                        effectiveMax = fifo.MaxDataFilesSize;
                    }
                    score = (double)effectiveSize / Math.Max(1UL, effectiveMax);
                    if (score < 1 && FifoAllowCompaction(_options))
                    {
                        score = Math.Max((double)numSortedRuns / Math.Max(1, _options.Level0FileNumCompactionTrigger), score);
                    }
                    if (score < 1 && _options.Ttl > 0 && _options.Ttl != ulong.MaxValue - 1)
                    {
                        score = Math.Max(GetExpiredTtlFilesCount(_files[0]), score);
                    }
                    // For FIFO, just need a large enough score to trigger compaction.
                    if (score < 1 && ShouldChangeFileTemperature(_files[0])) score = 1.1;
                }
                else
                {
                    score = (double)numSortedRuns / Math.Max(1, _options.Level0FileNumCompactionTrigger);
                    if (CompactionStyle == CompactionStyle.Level && NumLevels > 1)
                    {
                        if (_options.LevelCompactionDynamicLevelBytes)
                        {
                            if (totalSize >= _options.MaxBytesForLevelBase) score = Math.Max(score, 1.01);
                            if (totalSize > _levelMaxBytes[BaseLevel])
                            {
                                ulong baseLevelSize = 0;
                                foreach (FileMetaData f in _files[BaseLevel]) baseLevelSize += CompensatedFileSize(f);
                                score = Math.Max(score, (double)totalSize / Math.Max(baseLevelSize, _levelMaxBytes[BaseLevel]));
                            }
                            if (score > 1.0) score *= ScoreScale;
                        }
                        else
                        {
                            score = Math.Max(score, (double)totalSize / Math.Max(1UL, _options.MaxBytesForLevelBase));
                        }
                    }
                }
            }
            else
            {
                ulong levelBytesNoCompacting = 0;
                ulong levelTotalBytes = 0;
                foreach (FileMetaData f in _files[level])
                {
                    levelTotalBytes += f.FileSize;
                    if (!f.BeingCompacted) levelBytesNoCompacting += CompensatedFileSize(f);
                }
                ulong target = MaxBytesForLevel(level);
                if (!_options.LevelCompactionDynamicLevelBytes)
                {
                    score = (double)levelBytesNoCompacting / target;
                }
                else
                {
                    if (levelBytesNoCompacting < target) score = (double)levelBytesNoCompacting / target;
                    else
                    {
                        // De-prioritize compaction from a level where a large amount of incoming data is expected.
                        score = levelBytesNoCompacting / (target + totalDowncompactBytes) * ScoreScale;
                    }
                    // Drain unnecessary levels, but with lower priority compared to when L0 is eligible.
                    if (levelBytesNoCompacting > 0 && level <= LowestUnnecessaryLevel)
                    {
                        score = Math.Max(score, ScoreScale * (1.001 + 0.001 * (LowestUnnecessaryLevel - level)));
                    }
                }
                if (level <= LowestUnnecessaryLevel) totalDowncompactBytes += levelTotalBytes;
                else if (levelTotalBytes > target) totalDowncompactBytes += levelTotalBytes - target;
            }
            _compactionLevel[level] = level;
            _compactionScore[level] = score;
        }

        // Sort all the levels based on their score. Higher scores get listed first (bubble sort, few entries).
        for (int i = 0; i < scored - 1; i++)
        {
            for (int j = i + 1; j < scored; j++)
            {
                if (_compactionScore[i] < _compactionScore[j])
                {
                    (_compactionScore[i], _compactionScore[j]) = (_compactionScore[j], _compactionScore[i]);
                    (_compactionLevel[i], _compactionLevel[j]) = (_compactionLevel[j], _compactionLevel[i]);
                }
            }
        }
        ComputeFilesMarkedForCompaction(maxOutputLevel);
        ComputeBottommostFilesMarkedForCompaction();
        ComputeExpiredTtlFiles();
        ComputeFilesMarkedForPeriodicCompaction(maxOutputLevel);
        ComputeFilesMarkedForForcedBlobGC(_options.BlobGarbageCollectionAgeCutoff, _options.BlobGarbageCollectionForceThreshold,
            _options.EnableBlobGarbageCollection);
        ComputeFilesMarkedForReadTriggeredCompaction(_options.ReadTriggeredCompactionThreshold, CompactionStyle);
        EstimateCompactionBytesNeeded();
    }

    /// <summary>
    /// Port of VersionStorageInfo::ComputeFilesMarkedForReadTriggeredCompaction: files (not being compacted, not on the
    /// last non-empty level unless on L0) whose sampled collapsible-entry reads (tombstones, merge operands, misses) per
    /// byte exceed <paramref name="threshold"/>, sorted by that ratio descending. FIFO never marks files.
    /// </summary>
    internal void ComputeFilesMarkedForReadTriggeredCompaction(double threshold, CompactionStyle compactionStyle)
    {
        _readTriggeredCompactionFiles.Clear();
        if (threshold <= 0.0 || compactionStyle == CompactionStyle.Fifo) return;
        // Skip files at the last non-empty level -- there is no lower level to compact into. Exception: L0 files are
        // allowed even when L0 is the last non-empty level, because in single-level universal or FIFO compaction, L0
        // files can be compacted together (L0 -> L0).
        int lastNonEmpty = NumNonEmptyLevels - 1;
        List<double>? ratios = null;
        for (int level = 0; level < NumLevels; level++)
        {
            if (level >= lastNonEmpty && level != 0) continue;
            foreach (FileMetaData f in _files[level])
            {
                if (f.BeingCompacted) continue;
                ulong fileSize = f.FileSize;
                if (fileSize == 0) continue;
                // Snapshot reads_per_byte for each file so the sort sees stable values.
                double readsPerByte = (double)f.NumCollapsibleEntryReadsSampled / fileSize;
                if (readsPerByte > threshold)
                {
                    _readTriggeredCompactionFiles.Add((level, f));
                    (ratios ??= new List<double>()).Add(readsPerByte);
                }
            }
        }
        if (ratios is null || ratios.Count < 2) return;
        // Sort by reads_per_byte descending so the hottest files are picked first. std::sort is not stable; a stable
        // order (level, then key order) is used for equal ratios.
        var keys = ratios.ToArray();
        var items = _readTriggeredCompactionFiles.ToArray();
        var order = new int[keys.Length];
        for (int i = 0; i < order.Length; i++) order[i] = i;
        Array.Sort(order, (a, b) =>
        {
            int c = keys[b].CompareTo(keys[a]);
            return c != 0 ? c : a.CompareTo(b);
        });
        _readTriggeredCompactionFiles.Clear();
        foreach (int i in order) _readTriggeredCompactionFiles.Add(items[i]);
    }

    /// <summary>
    /// Port of ComputeFilesMarkedForForcedBlobGC: if the garbage ratio of the blob files eligible for GC by age
    /// (the oldest age_cutoff fraction) reaches the force threshold, the table files linked to the oldest blob file are
    /// marked so a compaction relocates its blobs.
    /// </summary>
    private void ComputeFilesMarkedForForcedBlobGC(double ageCutoff, double forceThreshold, bool enableGc)
    {
        _filesMarkedForForcedBlobGC.Clear();
        if (!(enableGc && ageCutoff > 0.0 && forceThreshold < 1.0)) return;
        Blob.BlobFileMetaData[] blobFiles = BlobFiles;
        if (blobFiles.Length == 0) return;
        // Number of blob files eligible for GC based on age
        int cutoffCount = (int)(ageCutoff * blobFiles.Length);
        if (cutoffCount == 0) return;
        Blob.BlobFileMetaData oldest = blobFiles[0];
        ulong sumTotal = oldest.TotalBlobBytes;
        ulong sumGarbage = oldest.GarbageBlobBytes;
        for (int count = 1; count < cutoffCount; count++)
        {
            sumTotal += blobFiles[count].TotalBlobBytes;
            sumGarbage += blobFiles[count].GarbageBlobBytes;
        }
        if (sumGarbage < forceThreshold * sumTotal) return;
        foreach (ulong sst in oldest.LinkedSsts)
        {
            for (int level = 0; level < NumLevels; level++)
            {
                foreach (FileMetaData f in _files[level])
                {
                    if (f.FileNumber != sst) continue;
                    if (!f.BeingCompacted) _filesMarkedForForcedBlobGC.Add((level, f));
                    goto next;
                }
            }
            next:;
        }
    }

    internal static ulong FifoMaxTableFilesSize(ColumnFamilyOptions o) => o.CompactionOptionsFifo.MaxTableFilesSize;

    /// <summary>GetBlobStats().total_file_size: the on-disk size of the live blob files (header + blobs + footer).</summary>
    public ulong TotalBlobFileSize()
    {
        ulong total = 0;
        foreach (Blob.BlobFileMetaData b in BlobFiles) total += b.TotalBlobBytes + Blob.BlobLogFormat.HeaderSize + Blob.BlobLogFormat.FooterSize;
        return total;
    }

    /// <summary>
    /// Port of FileMetaData::TryGetNewestKeyTime(prev_file): the file's newest key time (table property), else the
    /// oldest ancestor time of the next newer file (<paramref name="prevFile"/>), else 0 (unknown).
    /// </summary>
    internal static ulong TryGetNewestKeyTime(FileMetaData f, FileMetaData? prevFile)
    {
        if (f.NewestKeyTime != 0) return f.NewestKeyTime;
        if (prevFile is not null && prevFile.OldestAncestorTime != 0) return prevFile.OldestAncestorTime;
        return 0;
    }

    /// <summary>
    /// The temperature a FIFO file whose newest key time is <paramref name="estNewestKeyTime"/> belongs to under
    /// file_temperature_age_thresholds (the oldest threshold it passed).
    /// </summary>
    internal static Temperature FifoTargetTemperature(List<FileTemperatureAge> ages, ulong currentTime, ulong estNewestKeyTime)
    {
        Temperature target = ages[0].Temperature;
        for (int i = 1; i < ages.Count; i++)
        {
            if (currentTime >= ages[i].Age && estNewestKeyTime <= currentTime - ages[i].Age) target = ages[i].Temperature;
        }
        return target;
    }

    /// <summary>Port of ShouldChangeFileTemperature (db/version_set.cc): same walk as PickTemperatureChangeCompaction.</summary>
    private bool ShouldChangeFileTemperature(FileMetaData[] files)
    {
        List<FileTemperatureAge> ages = _options.CompactionOptionsFifo.FileTemperatureAgeThresholds;
        if (ages.Count == 0 || files.Length == 0) return false;
        ulong currentTime = (ulong)NowSeconds();
        if (currentTime < ages[0].Age) return false;
        ulong createTimeThreshold = currentTime - ages[0].Age;
        for (int index = files.Length; index >= 1; --index)
        {
            FileMetaData cur = files[index - 1];
            if (cur.BeingCompacted) continue;
            ulong est = TryGetNewestKeyTime(cur, index < 2 ? null : files[index - 2]);
            // Newer file could have newest_key_time populated
            if (est == 0) continue;
            if (est > createTimeThreshold) return false;
            if (cur.Temperature != FifoTargetTemperature(ages, currentTime, est)) return true;
        }
        return false;
    }
    internal static bool FifoAllowCompaction(ColumnFamilyOptions o) => o.CompactionOptionsFifo.AllowCompaction;

    private static long NowSeconds() => DateTimeOffset.UtcNow.ToUnixTimeSeconds();

    private int GetExpiredTtlFilesCount(FileMetaData[] files)
    {
        long now = NowSeconds();
        ulong ttl = _options.Ttl;
        if ((ulong)now < ttl) return 0;
        int n = 0;
        foreach (FileMetaData f in files)
        {
            if (f.BeingCompacted) continue;
            ulong t = f.OldestAncestorTime;
            if (t != 0 && t < (ulong)now - ttl) n++;
        }
        return n;
    }

    /// <summary>Port of ComputeFilesMarkedForCompaction (files marked by table property collectors).</summary>
    private void ComputeFilesMarkedForCompaction(int lastLevel)
    {
        _filesMarkedForCompaction.Clear();
        int lastQualifyLevel = 0;
        StandaloneRangeTombstoneFilesMarkThreshold = DbFormat.MaxSequenceNumber;
        // Do not include files from the last level with data. If table properties collector suggests a file on the
        // last level, we should not move it to a new level.
        for (int level = lastLevel; level >= 1; level--)
        {
            if (level < NumLevels && _files[level].Length > 0)
            {
                lastQualifyLevel = level - 1;
                break;
            }
        }
        for (int level = 0; level <= lastQualifyLevel; level++)
        {
            foreach (FileMetaData f in _files[level])
            {
                if (f.MarkedForCompaction && !f.BeingCompacted)
                {
                    _filesMarkedForCompaction.Add((level, f));
                    if (f.FileIsStandAloneRangeTombstone())
                    {
                        StandaloneRangeTombstoneFilesMarkThreshold = Math.Min(StandaloneRangeTombstoneFilesMarkThreshold, f.SmallestSeqno);
                    }
                }
            }
        }
    }

    /// <summary>Port of ComputeExpiredTtlFiles (leveled: files above the last level whose data is older than ttl).</summary>
    private void ComputeExpiredTtlFiles()
    {
        _expiredTtlFiles.Clear();
        ulong ttl = _options.Ttl;
        if (ttl == 0 || ttl >= ulong.MaxValue - 1 || CompactionStyle != CompactionStyle.Level) return;
        long now = NowSeconds();
        if ((ulong)now < ttl) return;
        ulong cutoff = (ulong)now - ttl;
        for (int level = 0; level < NumLevels - 1; level++)
        {
            foreach (FileMetaData f in _files[level])
            {
                if (f.BeingCompacted) continue;
                ulong t = f.OldestAncestorTime;
                if (t > 0 && t < cutoff) _expiredTtlFiles.Add((level, f));
            }
        }
    }

    /// <summary>Port of ComputeFilesMarkedForPeriodicCompaction.</summary>
    private void ComputeFilesMarkedForPeriodicCompaction(int lastLevel)
    {
        _filesMarkedForPeriodicCompaction.Clear();
        ulong period = _options.PeriodicCompactionSeconds;
        if (period == 0 || period >= ulong.MaxValue - 1) return;
        long now = NowSeconds();
        if ((ulong)now < period) return;
        // Offpeak pulls a file's deadline earlier by the time until the next daily offpeak window (so a whole period's
        // worth can be marked at the start of offpeak): marked when now + offpeak_pull > modification time + period.
        ulong offpeakPull = 0;
        if (_options.DbOffpeakTime?.GetOffpeakTimeInfo(now) is { IsNowOffpeak: true } offpeak) offpeakPull = (ulong)offpeak.SecondsTillNextOffpeakStart;
        ulong allowedTimeLimit = (ulong)now + offpeakPull - period;
        for (int level = 0; level <= lastLevel && level < NumLevels; level++)
        {
            foreach (FileMetaData f in _files[level])
            {
                if (f.BeingCompacted) continue;
                // Compute a file's modification time in the following order: 1. file_creation_time if it is > 0,
                // 2. oldest_ancestor_time if it is > 0 (upstream then falls back to the file's mtime; the port skips
                // such files, which only exist in DBs written before file_creation_time was recorded).
                ulong t = f.FileCreationTime != 0 ? f.FileCreationTime : f.OldestAncestorTime;
                if (t > 0 && t < allowedTimeLimit) _filesMarkedForPeriodicCompaction.Add((level, f));
            }
        }
    }

    /// <summary>Port of VersionStorageInfo::EstimateCompactionBytesNeeded (leveled only).</summary>
    private void EstimateCompactionBytesNeeded()
    {
        if (CompactionStyle != CompactionStyle.Level)
        {
            EstimatedCompactionNeededBytes = 0;
            return;
        }
        ulong estimated;
        ulong bytesCompactToNextLevel = 0;
        ulong levelSize = _levelBytes[0];
        bool level0CompactTriggered = false;
        if (_files[0].Length >= _options.Level0FileNumCompactionTrigger || levelSize >= _options.MaxBytesForLevelBase)
        {
            level0CompactTriggered = true;
            estimated = levelSize;
            bytesCompactToNextLevel = levelSize;
        }
        else
        {
            estimated = 0;
        }

        ulong bytesNextLevel = 0;
        for (int level = BaseLevel; level >= 1 && level <= MaxInputLevel; level++)
        {
            if (bytesNextLevel > 0)
            {
                levelSize = bytesNextLevel;
                bytesNextLevel = 0;
            }
            else
            {
                levelSize = _levelBytes[level];
            }
            if (level == BaseLevel && level0CompactTriggered) estimated += levelSize;
            levelSize += bytesCompactToNextLevel;
            bytesCompactToNextLevel = 0;
            ulong levelTarget = MaxBytesForLevel(level);
            if (levelSize > levelTarget)
            {
                bytesCompactToNextLevel = levelSize - levelTarget;
                if (level + 1 < NumLevels) bytesNextLevel = _levelBytes[level + 1];
                if (bytesNextLevel > 0)
                {
                    estimated += (ulong)(bytesCompactToNextLevel * ((double)bytesNextLevel / levelSize + 1));
                }
            }
        }
        EstimatedCompactionNeededBytes = estimated;
    }

    // ---------------------------------------------------------------------------------------------------------
    // Files by compaction priority

    /// <summary>
    /// Port of VersionStorageInfo::UpdateFilesByCompactionPri (+ SortFileByOverlappingRatio, SortFileByRoundRobin with
    /// this version's compact cursors).
    /// </summary>
    private void UpdateFilesByCompactionPri()
    {
        for (int level = 0; level < NumLevels; level++) _filesByCompactionPri[level] = [];
        if (CompactionStyle is CompactionStyle.None or CompactionStyle.Fifo or CompactionStyle.Universal) return;
        InternalKeyComparator icmp = Icmp;
        // No need to sort the highest level because it is never compacted.
        for (int level = 0; level < NumLevels - 1; level++)
        {
            FileMetaData[] files = _files[level];
            int[] temp = new int[files.Length];
            for (int i = 0; i < temp.Length; i++) temp[i] = i;
            if (files.Length == 0)
            {
                _filesByCompactionPri[level] = temp;
                continue;
            }
            switch (_options.CompactionPri)
            {
                case CompactionPri.ByCompensatedSize:
                    temp.AsSpan().Sort(new FileIndexComparer(files, FileIndexOrder.LargestCompensatedSizeFirst));
                    break;
                case CompactionPri.OldestLargestSeqFirst:
                    temp.AsSpan().Sort(new FileIndexComparer(files, FileIndexOrder.OldestLargestSeqFirst));
                    break;
                case CompactionPri.OldestSmallestSeqFirst:
                    temp.AsSpan().Sort(new FileIndexComparer(files, FileIndexOrder.OldestSmallestSeqFirst));
                    break;
                case CompactionPri.MinOverlappingRatio:
                    SortFileByOverlappingRatio(level, files, _files[level + 1], temp);
                    break;
                case CompactionPri.RoundRobin:
                    SortFileByRoundRobin(level, files, temp);
                    break;
            }
            _filesByCompactionPri[level] = temp;
        }
    }

    /// <summary>
    /// Port of SortFileByOverlappingRatio with FileTtlBooster (db/compaction/file_pri.h): files closer to the TTL limit get
    /// their overlapping ratio divided by a boost score. No boosting before half of the TTL has passed; the boosting
    /// range of each level is half of the next one's, and L0 and the last non-empty level are never boosted.
    /// </summary>
    private void SortFileByOverlappingRatio(int level, FileMetaData[] files, FileMetaData[] nextLevelFiles, int[] temp)
    {
        InternalKeyComparator icmp = Icmp;
        ulong[] order = RocksArrayPool<ulong>.Shared.Rent(files.Length);
        int next = 0;
        // FileTtlBooster(current_time, ttl, num_non_empty_levels, level)
        ulong ttl = _options.Ttl;
        ulong currentTime = (ulong)NowSeconds();
        bool boostEnabled = !(ttl == 0 || level == 0 || level >= NumNonEmptyLevels - 1);
        ulong boostAgeStart = 0, boostStep = 1;
        if (boostEnabled)
        {
            ulong allBoostStartAge = ttl / 2;
            ulong allBoostAgeRange = ttl / 32 * 31 - allBoostStartAge;
            ulong boostAgeRange = allBoostAgeRange >> Math.Min(63, NumNonEmptyLevels - level - 1);
            boostAgeStart = allBoostStartAge + boostAgeRange;
            const ulong BoostRatio = 16;
            // prevent 0 value to avoid divide 0 error.
            boostStep = Math.Max(boostAgeRange / BoostRatio, 1UL);
        }
        for (int i = 0; i < files.Length; i++)
        {
            FileMetaData file = files[i];
            ulong overlappingBytes = 0;
            // Skip files in next level that is smaller than current file.
            while (next < nextLevelFiles.Length && icmp.Compare(nextLevelFiles[next].Largest, file.Smallest) < 0) next++;
            while (next < nextLevelFiles.Length && icmp.Compare(nextLevelFiles[next].Smallest, file.Largest) < 0)
            {
                overlappingBytes += nextLevelFiles[next].FileSize;
                if (icmp.Compare(nextLevelFiles[next].Largest, file.Largest) > 0) break; // crosses the file's large boundary
                next++;
            }
            ulong compensated = Math.Max(1UL, CompensatedFileSize(file));
            // FileTtlBooster::GetBoostScore: boosting is addition to the current value, so +1 (an unknown oldest ancestor
            // time, 0, counts as the full age like upstream).
            ulong ttlBoostScore = 1;
            if (boostEnabled && file.OldestAncestorTime < currentTime)
            {
                ulong age = currentTime - file.OldestAncestorTime;
                if (age > boostAgeStart) ttlBoostScore = (age - boostAgeStart) / boostStep + 1;
            }
            order[i] = overlappingBytes * 1024UL / compensated / ttlBoostScore;
        }
        temp.AsSpan().Sort(new OverlappingRatioComparer(files, order, icmp));
        RocksArrayPool<ulong>.Shared.Return(order);
    }

    /// <summary>Port of SortFileByRoundRobin.</summary>
    private void SortFileByRoundRobin(int level, FileMetaData[] files, int[] temp)
    {
        InternalKeyComparator icmp = Icmp;
        if (level == 0)
        {
            // Using kOldestSmallestSeqFirst when level === 0, since the files may overlap (not fully sorted). Upstream
            // tests "level == 0 && !level0_non_overlapping", but PrepareForVersionAppend calls UpdateFilesByCompactionPri
            // before GenerateLevel0NonOverlapping, so the flag is still false here: L0 is always ordered oldest first.
            temp.AsSpan().Sort(new FileIndexComparer(files, FileIndexOrder.OldestSmallestSeqFirst));
            return;
        }
        byte[]? cursor = GetCompactCursor(level);
        if (cursor is null || cursor.Length == 0 || temp.Length <= 1) return;
        // Find the file whose smallest key is >= the cursor.
        int start = 0;
        while (start < temp.Length && icmp.Compare(cursor, files[temp[start]].Smallest) > 0) start++;
        if (start == temp.Length || start == 0) return;
        var rotated = new int[temp.Length];
        int k = 0;
        for (int i = start; i < temp.Length; i++) rotated[k++] = temp[i];
        for (int i = 0; i < start; i++) rotated[k++] = temp[i];
        rotated.CopyTo(temp, 0);
    }

    // Comparers of the sorts above (struct comparers instead of capturing lambdas: no closure and delegate per sort).

    private enum FileIndexOrder : byte
    {
        LargestCompensatedSizeFirst,
        OldestLargestSeqFirst,
        OldestSmallestSeqFirst,
    }

    /// <summary>File indexes by a per-file number, ties by index (kByCompensatedSize, kOldestLargestSeqFirst, kOldestSmallestSeqFirst).</summary>
    private readonly struct FileIndexComparer(FileMetaData[] files, FileIndexOrder order) : IComparer<int>
    {
        public int Compare(int a, int b)
        {
            int r = order switch
            {
                FileIndexOrder.LargestCompensatedSizeFirst => CompensatedFileSize(files[b]).CompareTo(CompensatedFileSize(files[a])),
                FileIndexOrder.OldestLargestSeqFirst => files[a].LargestSeqno.CompareTo(files[b].LargestSeqno),
                _ => files[a].SmallestSeqno.CompareTo(files[b].SmallestSeqno),
            };
            return r != 0 ? r : a.CompareTo(b);
        }
    }

    /// <summary>SortFileByOverlappingRatio's order: marked for compaction first, then by ratio, smallest key, index.</summary>
    private readonly struct OverlappingRatioComparer(FileMetaData[] files, ulong[] order, InternalKeyComparator icmp) : IComparer<int>
    {
        public int Compare(int a, int b)
        {
            // If score is the same, pick file with smaller keys (more deterministic, helps trivial moves).
            FileMetaData fa = files[a], fb = files[b];
            if (fa.MarkedForCompaction != fb.MarkedForCompaction) return fa.MarkedForCompaction ? -1 : 1;
            if (order[a] != order[b]) return order[a].CompareTo(order[b]);
            int r = icmp.Compare(fa.Smallest, fb.Smallest);
            return r != 0 ? r : a.CompareTo(b);
        }
    }

    /// <summary>Files by smallest internal key.</summary>
    private readonly struct SmallestKeyComparer(InternalKeyComparator icmp) : IComparer<FileMetaData>
    {
        public int Compare(FileMetaData? a, FileMetaData? b) => icmp.Compare(a!.Smallest, b!.Smallest);
    }

    // ---------------------------------------------------------------------------------------------------------
    // Overlap queries

    /// <summary>
    /// Comparator::CompareWithoutTimestamp of two user keys that carry the column family's timestamp (plain user-key
    /// comparison without timestamps): file range overlap checks treat all versions of a key as the same key.
    /// </summary>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    public static int CompareWithoutTimestamp(InternalKeyComparator icmp, ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        Comparator ucmp = icmp.UserComparator;
        return ucmp.TimestampSize == 0 ? icmp.CompareUserKey(a, b) : ucmp.CompareWithoutTimestamp(a, b);
    }

    /// <summary>
    /// Port of sstableKeyCompare (db/compaction/compaction.cc): user-key comparison where a range-tombstone sentinel
    /// (kMaxSequenceNumber, kTypeRangeDeletion) sorts before any other key with the same user key.
    /// </summary>
    public static int SstableKeyCompare(InternalKeyComparator icmp, ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        int c = VersionStorageInfo.CompareWithoutTimestamp(icmp, DbFormat.ExtractUserKey(a), DbFormat.ExtractUserKey(b));
        if (c != 0) return c;
        ulong sentinel = DbFormat.PackSequenceAndType(DbFormat.MaxSequenceNumber, EntryType.RangeDeletion);
        ulong af = DbFormat.ExtractFooter(a), bf = DbFormat.ExtractFooter(b);
        if (af == sentinel)
        {
            if (bf != sentinel) return -1;
        }
        else if (bf == sentinel)
        {
            return 1;
        }
        return 0;
    }

    /// <summary>
    /// Port of VersionStorageInfo::GetOverlappingInputs: all files of <paramref name="level"/> overlapping the
    /// internal key range [<paramref name="begin"/>, <paramref name="end"/>] (null = unbounded). On L0 with
    /// <paramref name="expandRange"/>, the range grows with every overlapping file until no more files overlap.
    /// </summary>
    public void GetOverlappingInputs(int level, byte[]? begin, byte[]? end, List<FileMetaData> inputs, int hintIndex, out int fileIndex,
        bool expandRange = true, FileMetaData? startingL0File = null)
    {
        fileIndex = -1;
        inputs.Clear();
        if (level >= NumNonEmptyLevels) return;
        if (level > 0)
        {
            GetOverlappingInputsRangeBinarySearch(level, begin, end, inputs, hintIndex, out fileIndex, withinInterval: false);
            return;
        }
        FileMetaData[] files = _files[0];
        byte[]? userBegin = begin is null ? null : DbFormat.ExtractUserKey(begin).ToArray();
        byte[]? userEnd = end is null ? null : DbFormat.ExtractUserKey(end).ToArray();
        int startIndex = 0;
        if (startingL0File is not null)
        {
            for (int i = 0; i < files.Length; i++)
            {
                if (files[i].FileNumber == startingL0File.FileNumber)
                {
                    startIndex = i;
                    break;
                }
            }
        }
        var index = new List<int>(files.Length);
        for (int i = startIndex; i < files.Length; i++) index.Add(i);
        InternalKeyComparator icmp = Icmp;
        while (index.Count > 0)
        {
            bool found = false;
            for (int k = 0; k < index.Count;)
            {
                FileMetaData f = files[index[k]];
                ReadOnlySpan<byte> fileStart = DbFormat.ExtractUserKey(f.Smallest);
                ReadOnlySpan<byte> fileLimit = DbFormat.ExtractUserKey(f.Largest);
                if (userBegin is not null && VersionStorageInfo.CompareWithoutTimestamp(icmp, fileLimit, userBegin) < 0)
                {
                    k++; // "f" is completely before specified range; skip it
                }
                else if (userEnd is not null && VersionStorageInfo.CompareWithoutTimestamp(icmp, fileStart, userEnd) > 0)
                {
                    k++; // "f" is completely after specified range; skip it
                }
                else
                {
                    inputs.Add(f);
                    found = true;
                    if (fileIndex == -1) fileIndex = index[k];
                    index.RemoveAt(k);
                    if (expandRange)
                    {
                        if (userBegin is not null && VersionStorageInfo.CompareWithoutTimestamp(icmp, fileStart, userBegin) < 0) userBegin = fileStart.ToArray();
                        if (userEnd is not null && VersionStorageInfo.CompareWithoutTimestamp(icmp, fileLimit, userEnd) > 0) userEnd = fileLimit.ToArray();
                    }
                }
            }
            if (!found) break;
        }
    }

    /// <summary>GetOverlappingInputs without hint/index outputs.</summary>
    public void GetOverlappingInputs(int level, byte[]? begin, byte[]? end, List<FileMetaData> inputs) =>
        GetOverlappingInputs(level, begin, end, inputs, -1, out _);

    /// <summary>Port of GetCleanInputsWithinInterval: the maximal clean-cut set of files within [begin, end].</summary>
    public void GetCleanInputsWithinInterval(int level, byte[]? begin, byte[]? end, List<FileMetaData> inputs, int hintIndex, out int fileIndex)
    {
        inputs.Clear();
        fileIndex = -1;
        if (level >= NumNonEmptyLevels || level == 0 || _files[level].Length == 0) return;
        GetOverlappingInputsRangeBinarySearch(level, begin, end, inputs, hintIndex, out fileIndex, withinInterval: true);
    }

    /// <summary>
    /// Port of GetOverlappingInputsRangeBinarySearch (+ the next_smallest output): binary search for files of a
    /// sorted level overlapping (or, with <paramref name="withinInterval"/>, cleanly contained in) [begin, end].
    /// </summary>
    public void GetOverlappingInputsRangeBinarySearch(int level, byte[]? begin, byte[]? end, List<FileMetaData> inputs, int hintIndex,
        out int fileIndex, bool withinInterval) =>
        GetOverlappingInputsRangeBinarySearch(level, begin, end, inputs, hintIndex, out fileIndex, withinInterval, out _);

    public void GetOverlappingInputsRangeBinarySearch(int level, byte[]? begin, byte[]? end, List<FileMetaData> inputs, int hintIndex,
        out int fileIndex, bool withinInterval, out byte[]? nextSmallest)
    {
        fileIndex = -1;
        nextSmallest = null;
        InternalKeyComparator icmp = Icmp;
        FileMetaData[] files = _files[level];
        int numFiles = files.Length;
        int startIndex = 0;
        int endIndex = numFiles;
        if (begin is not null)
        {
            // lower_bound with cmp(f, k) = sstableKeyCompare(file_key, k) < 0
            int lo = 0, hi = hintIndex == -1 ? numFiles : Math.Min(hintIndex, numFiles);
            while (lo < hi)
            {
                int mid = (lo + hi) >>> 1;
                byte[] fileKey = withinInterval ? files[mid].Smallest : files[mid].Largest;
                if (SstableKeyCompare(icmp, fileKey, begin) < 0) lo = mid + 1;
                else hi = mid;
            }
            startIndex = lo;
            if (startIndex > 0 && withinInterval)
            {
                bool isOverlapping = true;
                while (isOverlapping && startIndex < numFiles)
                {
                    isOverlapping = SstableKeyCompare(icmp, files[startIndex - 1].Largest, files[startIndex].Smallest) == 0;
                    if (isOverlapping) startIndex++;
                }
            }
        }
        if (end is not null)
        {
            // upper_bound with cmp(k, f) = sstableKeyCompare(k, file_key) < 0
            int lo = startIndex, hi = numFiles;
            while (lo < hi)
            {
                int mid = (lo + hi) >>> 1;
                byte[] fileKey = withinInterval ? files[mid].Largest : files[mid].Smallest;
                if (SstableKeyCompare(icmp, end, fileKey) < 0) hi = mid;
                else lo = mid + 1;
            }
            endIndex = lo;
            if (endIndex < numFiles && withinInterval)
            {
                bool isOverlapping = true;
                while (isOverlapping && endIndex > startIndex)
                {
                    isOverlapping = SstableKeyCompare(icmp, files[endIndex - 1].Largest, files[endIndex].Smallest) == 0;
                    if (isOverlapping) endIndex--;
                }
            }
        }
        if (startIndex >= endIndex) return;
        fileIndex = startIndex;
        for (int i = startIndex; i < endIndex; i++) inputs.Add(files[i]);
        if (endIndex < numFiles) nextSmallest = files[endIndex].Smallest;
    }

    /// <summary>Port of OverlapInLevel (SomeFileOverlapsRange) on user keys; null bounds are unbounded.</summary>
    public bool OverlapInLevel(int level, ReadOnlySpan<byte> smallestUserKey, bool hasSmallest, ReadOnlySpan<byte> largestUserKey, bool hasLargest)
    {
        if (level >= NumNonEmptyLevels) return false;
        InternalKeyComparator icmp = Icmp;
        FileMetaData[] files = _files[level];
        if (level == 0)
        {
            foreach (FileMetaData f in files)
            {
                if (hasSmallest && icmp.CompareUserKey(smallestUserKey, DbFormat.ExtractUserKey(f.Largest)) > 0) continue; // f before range
                if (hasLargest && icmp.CompareUserKey(largestUserKey, DbFormat.ExtractUserKey(f.Smallest)) < 0) continue; // f after range
                return true;
            }
            return false;
        }
        // Binary search: first file whose largest user key >= smallest.
        int lo = 0, hi = files.Length;
        if (hasSmallest)
        {
            while (lo < hi)
            {
                int mid = (lo + hi) >>> 1;
                if (icmp.CompareUserKey(DbFormat.ExtractUserKey(files[mid].Largest), smallestUserKey) < 0) lo = mid + 1;
                else hi = mid;
            }
        }
        if (lo >= files.Length) return false;
        return !hasLargest || icmp.CompareUserKey(largestUserKey, DbFormat.ExtractUserKey(files[lo].Smallest)) >= 0;
    }

    /// <summary>
    /// Port of RangeMightExistAfterSortedRun: whether keys in [smallest, largest] may exist in sorted runs older than
    /// (<paramref name="lastLevel"/>, <paramref name="lastL0Index"/>).
    /// </summary>
    public bool RangeMightExistAfterSortedRun(ReadOnlySpan<byte> smallestUserKey, ReadOnlySpan<byte> largestUserKey, int lastLevel, int lastL0Index)
    {
        // An L0 file is considered bottommost only if it's the oldest L0 file and there are no files on older levels.
        if (lastLevel == 0 && lastL0Index != _files[0].Length - 1) return true;
        for (int level = lastLevel + 1; level < NumLevels; level++)
        {
            if (_files[level].Length > 0 && (lastLevel == 0 || OverlapInLevel(level, smallestUserKey, true, largestUserKey, true))) return true;
        }
        return false;
    }

    /// <summary>Port of the "rocksdb.levelstats" property (with an extra score column).</summary>
    public string LevelStats()
    {
        var sb = new StringBuilder("Level Files Size(MB) Score\n--------------------------\n");
        for (int level = 0; level < NumLevels; level++)
        {
            sb.Append(string.Format(CultureInfo.InvariantCulture, "{0,3} {1,8} {2,8:F0} {3,6:F2}\n", level, _files[level].Length,
                _levelBytes[level] / 1048576.0, level < NumLevels - 1 ? ScoreOfLevel(level) : 0.0));
        }
        return sb.ToString();
    }
}
