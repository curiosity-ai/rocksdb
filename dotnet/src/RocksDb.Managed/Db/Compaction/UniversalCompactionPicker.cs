namespace RocksDb.Managed.Db;

/// <summary>
/// Universal (tiered) compaction picker. Port of UniversalCompactionPicker / UniversalCompactionBuilder
/// (db/compaction/compaction_picker_universal.cc): sorted runs (each L0 file, each non-empty level), size
/// amplification compaction (PickCompactionToReduceSizeAmp), size-ratio compaction and sorted-run-count compaction
/// (PickCompactionToReduceSortedRuns), compression_size_percent, trivial moves of non-overlapping inputs.
/// Not ported: periodic and delete-triggered compactions, incremental mode, max_read_amp auto tuning (the read-amp
/// limit is level0_file_num_compaction_trigger), multiple db_paths. REQUIRES: DB mutex.
/// </summary>
internal sealed class UniversalCompactionPicker : CompactionPicker
{
    public UniversalCompactionPicker(ColumnFamilyOptions options, InternalKeyComparator icmp) : base(options, icmp) { }

    private readonly struct SortedRun(int level, FileMetaData? file, ulong size, ulong compensatedSize, bool beingCompacted)
    {
        public readonly int Level = level;
        /// <summary>The file for an L0 run, null for a level run.</summary>
        public readonly FileMetaData? File = file;
        public readonly ulong Size = size;
        public readonly ulong CompensatedSize = compensatedSize;
        public readonly bool BeingCompacted = beingCompacted;
    }

    public override bool NeedsCompaction(VersionStorageInfo vstorage) =>
        vstorage.CompactionScore(0) >= 1 || vstorage.FilesMarkedForCompaction.Count > 0;

    /// <summary>Port of CalculateSortedRuns.</summary>
    private static List<SortedRun> CalculateSortedRuns(VersionStorageInfo vstorage, int lastLevel)
    {
        var ret = new List<SortedRun>();
        foreach (FileMetaData f in vstorage.LevelFiles(0))
        {
            ret.Add(new SortedRun(0, f, f.FileSize, VersionStorageInfo.CompensatedFileSize(f), f.BeingCompacted));
        }
        for (int level = 1; level <= lastLevel && level < vstorage.NumLevels; level++)
        {
            ulong compensated = 0, total = 0;
            bool beingCompacted = false;
            foreach (FileMetaData f in vstorage.LevelFiles(level))
            {
                compensated += VersionStorageInfo.CompensatedFileSize(f);
                total += f.FileSize;
                // Mark the entire run as being compacted if one or more files are being compacted.
                if (f.BeingCompacted) beingCompacted = true;
            }
            if (compensated > 0) ret.Add(new SortedRun(level, null, total, compensated, beingCompacted));
        }
        return ret;
    }

    public override Compaction? PickCompaction(VersionStorageInfo vstorage, IReadOnlyList<ulong> snapshots)
    {
        int maxOutputLevel = vstorage.MaxOutputLevel(Options.CfAllowIngestBehind);
        int trigger = Options.Level0FileNumCompactionTrigger;
        uint ratio = Options.CompactionOptionsUniversal.SizeRatio;
        List<SortedRun> runs = CalculateSortedRuns(vstorage, maxOutputLevel);
        if (runs.Count == 0 || runs.Count < trigger) return null;
        double score = vstorage.CompactionScore(0);

        Compaction? c = PickCompactionToReduceSizeAmp(vstorage, runs, maxOutputLevel, score);
        c ??= PickCompactionToReduceSortedRuns(vstorage, runs, maxOutputLevel, ratio, uint.MaxValue, score);
        if (c is null)
        {
            // Size amplification and file size ratios are within configured limits. If max read amplification is
            // exceeded, try to reduce it just by the number of files, without looking at file size ratios.
            int notCompacted = 0;
            foreach (SortedRun r in runs)
            {
                if (!r.BeingCompacted) notCompacted++;
            }
            if (notCompacted > trigger)
            {
                uint numFiles = (uint)(notCompacted - trigger + 1);
                c = PickCompactionToReduceSortedRuns(vstorage, runs, maxOutputLevel, uint.MaxValue, numFiles, score);
            }
        }
        return c;
    }

    /// <summary>Port of UniversalCompactionBuilder::PickCompactionToReduceSortedRuns.</summary>
    private Compaction? PickCompactionToReduceSortedRuns(VersionStorageInfo vstorage, List<SortedRun> runs, int maxOutputLevel, uint ratio,
        uint maxNumberOfFilesToCompact, double score)
    {
        CompactionOptionsUniversal uo = Options.CompactionOptionsUniversal;
        uint minMergeWidth = Math.Max(uo.MinMergeWidth, 2U);
        uint maxFilesToCompact = Math.Min(uo.MaxMergeWidth, maxNumberOfFilesToCompact);
        bool done = false;
        int startIndex = 0;
        uint candidateCount = 0;
        // Considers a candidate file only if it is smaller than the total size accumulated so far.
        for (int loop = 0; loop < runs.Count; loop++)
        {
            candidateCount = 0;
            // Skip files that are already being compacted.
            bool found = false;
            for (; loop < runs.Count; loop++)
            {
                if (!runs[loop].BeingCompacted)
                {
                    candidateCount = 1;
                    found = true;
                    break;
                }
            }
            if (!found) break;
            ulong candidateSize = runs[loop].CompensatedSize;
            // Check if the succeeding files need compaction.
            for (int i = loop + 1; candidateCount < maxFilesToCompact && i < runs.Count; i++)
            {
                SortedRun succeeding = runs[i];
                if (succeeding.BeingCompacted) break;
                // Pick files if the total/last candidate file size (increased by the specified ratio) is still larger
                // than the next candidate file.
                double sz = candidateSize * (100.0 + ratio) / 100.0;
                if (sz < succeeding.Size) break;
                if (uo.StopStyle == CompactionStopStyle.SimilarSize)
                {
                    // Similar-size stopping rule: also check the last picked file isn't far larger than the next candidate file.
                    sz = succeeding.Size * (100.0 + ratio) / 100.0;
                    if (sz < candidateSize) break;
                    candidateSize = succeeding.CompensatedSize;
                }
                else
                {
                    candidateSize += succeeding.CompensatedSize;
                }
                candidateCount++;
            }
            // Found a series of consecutive files that need compaction.
            if (candidateCount >= minMergeWidth)
            {
                startIndex = loop;
                done = true;
                break;
            }
        }
        if (!done || candidateCount <= 1) return null;
        int firstIndexAfter = startIndex + (int)candidateCount;
        // Compression is enabled if files compacted earlier already reached size ratio of compression.
        bool enableCompression = true;
        int ratioToCompress = uo.CompressionSizePercent;
        if (ratioToCompress >= 0)
        {
            ulong totalSize = 0;
            foreach (SortedRun r in runs) totalSize += r.CompensatedSize;
            ulong olderFileSize = 0;
            for (int i = runs.Count - 1; i >= firstIndexAfter; i--)
            {
                olderFileSize += runs[i].Size;
                if (olderFileSize * 100UL >= totalSize * (ulong)ratioToCompress)
                {
                    enableCompression = false;
                    break;
                }
            }
        }
        int outputLevel;
        if (firstIndexAfter == runs.Count) outputLevel = maxOutputLevel;
        else if (runs[firstIndexAfter].Level == 0) outputLevel = 0;
        else outputLevel = runs[firstIndexAfter].Level - 1;
        CompactionReason reason = maxNumberOfFilesToCompact == uint.MaxValue ? CompactionReason.UniversalSizeRatio : CompactionReason.UniversalSortedRunNum;
        return BuildCompaction(vstorage, runs, startIndex, firstIndexAfter - 1, outputLevel, reason, score, enableCompression);
    }

    /// <summary>Port of UniversalCompactionBuilder::PickCompactionToReduceSizeAmp (non-incremental).</summary>
    private Compaction? PickCompactionToReduceSizeAmp(VersionStorageInfo vstorage, List<SortedRun> runs, int maxOutputLevel, double score)
    {
        int endIndex = runs.Count - 1;
        if (runs[endIndex].BeingCompacted) return null;
        ulong baseSrSize = runs[endIndex].Size;
        int startIndex = endIndex;
        ulong candidateSize = 0;
        // Get longest span (i.e, [start_index, end_index]) of available sorted runs.
        while (startIndex > 0)
        {
            SortedRun sr = runs[startIndex - 1];
            if (sr.BeingCompacted) break;
            candidateSize += sr.CompensatedSize;
            --startIndex;
        }
        if (startIndex == endIndex) return null;
        // size amplification = percentage of additional size
        ulong ratio = Options.CompactionOptionsUniversal.MaxSizeAmplificationPercent;
        if (candidateSize * 100 < ratio * baseSrSize) return null;
        return BuildCompaction(vstorage, runs, startIndex, endIndex, maxOutputLevel, CompactionReason.UniversalSizeAmplification, score, true);
    }

    /// <summary>Port of PickCompactionWithSortedRunRange (+ the input construction of PickCompactionToReduceSortedRuns).</summary>
    private Compaction? BuildCompaction(VersionStorageInfo vstorage, List<SortedRun> runs, int startIndex, int endIndex, int outputLevel,
        CompactionReason reason, double score, bool enableCompression)
    {
        int maxOutputLevel = vstorage.MaxOutputLevel(Options.CfAllowIngestBehind);
        if (endIndex == runs.Count - 1 && reason == CompactionReason.UniversalSizeAmplification) outputLevel = maxOutputLevel;
        else if (reason == CompactionReason.UniversalSizeAmplification) outputLevel = runs[endIndex + 1].Level - 1;
        int startLevel = runs[startIndex].Level;
        var inputs = new List<CompactionInputFiles>();
        for (int level = startLevel; level <= Math.Max(outputLevel, startLevel); level++) inputs.Add(new CompactionInputFiles(level));
        for (int i = startIndex; i <= endIndex; i++)
        {
            SortedRun r = runs[i];
            if (r.Level == 0) inputs[0].Files.Add(r.File!);
            else inputs[r.Level - startLevel].Files.AddRange(vstorage.LevelFiles(r.Level).ToArray());
        }
        // Drop empty trailing / middle levels (the job only reads non-empty inputs) but keep the start level first.
        var compact = new List<CompactionInputFiles> { inputs[0] };
        for (int i = 1; i < inputs.Count; i++)
        {
            if (!inputs[i].IsEmpty) compact.Add(inputs[i]);
        }
        // intra L0 compactions outputs could have overlap
        if (outputLevel != 0 && FilesRangeOverlapWithCompaction(compact, outputLevel)) return null;
        bool trivial = Options.CompactionOptionsUniversal.AllowTrivialMove && IsInputFilesNonOverlapping(compact);
        _ = enableCompression; // compression_size_percent: output compression follows the level (enable flag not plumbed)
        return NewCompaction(vstorage, compact.ToArray(), outputLevel, MaxFileSizeForLevel(Options, outputLevel, 1), ulong.MaxValue, 0,
            new List<FileMetaData>(), reason, score, l0FilesMightOverlap: true, isManual: false, trivialMoveForced: trivial);
    }

    /// <summary>Port of UniversalCompactionBuilder::IsInputFilesNonOverlapping.</summary>
    private bool IsInputFilesNonOverlapping(List<CompactionInputFiles> inputs)
    {
        var all = new List<FileMetaData>();
        foreach (CompactionInputFiles l in inputs) all.AddRange(l.Files);
        all.Sort((a, b) => Icmp.Compare(a.Smallest, b.Smallest));
        for (int i = 1; i < all.Count; i++)
        {
            if (Icmp.Compare(all[i - 1].Largest, all[i].Smallest) >= 0) return false;
        }
        return true;
    }
}
