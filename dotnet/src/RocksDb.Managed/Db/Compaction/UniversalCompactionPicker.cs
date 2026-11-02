namespace RocksDb.Managed.Db;

/// <summary>
/// Universal (tiered) compaction picker. Port of UniversalCompactionPicker / UniversalCompactionBuilder
/// (db/compaction/compaction_picker_universal.cc): sorted runs (each L0 file, each non-empty level), periodic
/// compaction (PickPeriodicCompaction), size amplification compaction (PickCompactionToReduceSizeAmp with
/// MightExcludeNewL0sToReduceWriteStop and the incremental mode PickIncrementalForReduceSizeAmp), size-ratio and
/// sorted-run-count compaction (PickCompactionToReduceSortedRuns, max_read_amp), delete-triggered compaction of files
/// marked for compaction (PickDeleteTriggeredCompaction / BuildCompactionToNextLevel), compression_size_percent,
/// trivial moves of non-overlapping inputs.
/// <para>
/// Read-triggered compaction (PickReadTriggeredCompaction) picks the files marked from the sampled reads
/// (monitoring/file_read_sample.h, VersionStorageInfo::ComputeFilesMarkedForReadTriggeredCompaction).
/// Not ported: standalone range tombstone files (FileIsStandAloneRangeTombstone needs the
/// per-file entry counts, which the port does not keep: such files are treated like other marked files), multiple
/// db_paths (GetPathId returns 0). require_max_output_level is used by the bottom-priority repick of
/// reduce_file_locking (DBImpl::BackgroundCompaction). REQUIRES: DB mutex.
/// </para>
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

    /// <summary>Port of UniversalCompactionPicker::NeedsCompaction.</summary>
    public override bool NeedsCompaction(VersionStorageInfo vstorage) =>
        vstorage.CompactionScore(0) >= 1 || vstorage.FilesMarkedForPeriodicCompaction.Count > 0 || vstorage.FilesMarkedForCompaction.Count > 0
        || vstorage.ReadTriggeredCompactionFiles.Count > 0;

    /// <summary>Port of CalculateSortedRuns (also returns max_run_size).</summary>
    private static List<SortedRun> CalculateSortedRuns(VersionStorageInfo vstorage, int lastLevel, out ulong maxRunSize)
    {
        maxRunSize = 0;
        var ret = new List<SortedRun>();
        foreach (FileMetaData f in vstorage.LevelFiles(0))
        {
            ret.Add(new SortedRun(0, f, f.FileSize, VersionStorageInfo.CompensatedFileSize(f), f.BeingCompacted));
            maxRunSize = Math.Max(maxRunSize, f.FileSize);
        }
        for (int level = 1; level <= lastLevel && level < vstorage.NumLevels; level++)
        {
            ulong compensated = 0, total = 0;
            bool beingCompacted = false;
            foreach (FileMetaData f in vstorage.LevelFiles(level))
            {
                compensated += VersionStorageInfo.CompensatedFileSize(f);
                total += f.FileSize;
                // Size amp, read amp and periodic compactions always include all files for a non-zero level. However, a
                // delete triggered compaction and a trivial move might pick a subset of files in a sorted run. So always
                // check all files in a sorted run and mark the entire run as being compacted if one or more files are
                // being compacted.
                if (f.BeingCompacted) beingCompacted = true;
            }
            if (compensated > 0) ret.Add(new SortedRun(level, null, total, compensated, beingCompacted));
            maxRunSize = Math.Max(maxRunSize, total);
        }
        return ret;
    }

    /// <summary>Per-pick state (UniversalCompactionBuilder).</summary>
    private sealed class Builder
    {
        public required VersionStorageInfo Vstorage;
        public required List<SortedRun> Runs;
        public ulong MaxRunSize;
        public double Score;
        /// <summary>require_max_output_level_: only compactions to the non-zero max output level (bottom-priority repick).</summary>
        public bool RequireMaxOutputLevel;

        /// <summary>UniversalCompactionBuilder::MeetsOutputLevelRequirements.</summary>
        public bool MeetsOutputLevelRequirements(int outputLevel, int maxOutputLevel) =>
            !RequireMaxOutputLevel || Compaction.OutputToNonZeroMaxOutputLevel(outputLevel, maxOutputLevel);
    }

    /// <summary>Port of UniversalCompactionBuilder::PickCompaction.</summary>
    public override Compaction? PickCompaction(VersionStorageInfo vstorage, IReadOnlyList<ulong> snapshots) =>
        PickCompaction(vstorage, snapshots, requireMaxOutputLevel: false);

    /// <summary>
    /// Port of UniversalCompactionBuilder::PickCompaction. With <paramref name="requireMaxOutputLevel"/> (a
    /// bottom-priority thread repicking a compaction forwarded with reduce_file_locking) only compactions that output to
    /// the non-zero max output level are picked.
    /// </summary>
    public override Compaction? PickCompaction(VersionStorageInfo vstorage, IReadOnlyList<ulong> snapshots, bool requireMaxOutputLevel)
    {
        int maxOutputLevel = vstorage.MaxOutputLevel(Options.CfAllowIngestBehind);
        if (maxOutputLevel == 0 && requireMaxOutputLevel) return null; // !MeetsOutputLevelRequirements(0)
        int trigger = Options.Level0FileNumCompactionTrigger;
        uint ratio = Options.CompactionOptionsUniversal.SizeRatio;
        List<SortedRun> runs = CalculateSortedRuns(vstorage, maxOutputLevel, out ulong maxRunSize);
        if (runs.Count == 0
            || (vstorage.FilesMarkedForPeriodicCompaction.Count == 0 && vstorage.FilesMarkedForCompaction.Count == 0
                && vstorage.ReadTriggeredCompactionFiles.Count == 0 && runs.Count < trigger))
        {
            return null; // "Universal: nothing to do"
        }
        var b = new Builder
        {
            Vstorage = vstorage,
            Runs = runs,
            MaxRunSize = maxRunSize,
            Score = vstorage.CompactionScore(0),
            RequireMaxOutputLevel = requireMaxOutputLevel,
        };

        Compaction? c = null;
        // MaybePickPeriodicCompaction
        if (vstorage.FilesMarkedForPeriodicCompaction.Count > 0) c = PickPeriodicCompaction(b);
        // MaybePickSizeAmpCompaction
        if (c is null && runs.Count >= trigger) c = PickCompactionToReduceSizeAmp(b);
        // MaybePickCompactionToReduceSortedRunsBasedFileRatio
        if (c is null && runs.Count >= trigger) c = PickCompactionToReduceSortedRuns(b, ratio, uint.MaxValue);
        // MaybePickCompactionToReduceSortedRuns
        if (c is null && runs.Count >= trigger)
        {
            uint maxFilesToCompact = GetMaxNumFilesToCompactBasedOnMaxReadAmp(b, trigger, ratio);
            if (maxFilesToCompact != 0) c = PickCompactionToReduceSortedRuns(b, uint.MaxValue, maxFilesToCompact);
        }
        // MaybePickDeleteTriggeredCompaction
        c ??= PickDeleteTriggeredCompaction(b);
        // MaybePickReadTriggeredCompaction
        if (c is null && vstorage.ReadTriggeredCompactionFiles.Count > 0) c = PickReadTriggeredCompaction(b);
        if (c is null) return null;
        RegisterCompaction(c);
        // Creating a compaction influences the compaction score because the score takes running compactions into
        // account (by skipping files that are already being compacted).
        vstorage.ComputeCompactionScore();
        return c;
    }

    /// <summary>UniversalCompactionBuilder::ShouldSkipLastSortedRunForSizeAmpCompaction.</summary>
    private bool ShouldSkipLastSortedRunForSizeAmpCompaction(List<SortedRun> runs) =>
        Options.PrecludeLastLevelDataSeconds > 0 && Options.NumLevels > 2 && runs[^1].Level == Options.NumLevels - 1 && runs.Count > 1;

    /// <summary>Port of GetMaxNumFilesToCompactBasedOnMaxReadAmp.</summary>
    private uint GetMaxNumFilesToCompactBasedOnMaxReadAmp(Builder b, int fileNumCompactionTrigger, uint ratio)
    {
        CompactionOptionsUniversal uo = Options.CompactionOptionsUniversal;
        int maxNumRuns = uo.MaxReadAmp;
        if (maxNumRuns < 0)
        {
            // By default, fall back to `level0_file_num_compaction_trigger`.
            maxNumRuns = fileNumCompactionTrigger;
        }
        else if (maxNumRuns == 0)
        {
            if (uo.StopStyle == CompactionStopStyle.TotalSize)
            {
                // 0 means auto-tuning: the minimum number of levels such that the target size of the largest level is
                // at least max_run_size, assuming the lowest level is write_buffer_size and each subsequent level is
                // the max size without triggering a size-ratio compaction.
                maxNumRuns = 1;
                double curLevelMaxSize = Options.WriteBufferSize;
                double totalRunSize = 0;
                while (curLevelMaxSize < b.MaxRunSize)
                {
                    totalRunSize += curLevelMaxSize;
                    curLevelMaxSize = (100.0 + ratio) / 100.0 * totalRunSize;
                    ++maxNumRuns;
                }
            }
            else
            {
                maxNumRuns = fileNumCompactionTrigger;
            }
        }
        int notCompacted = 0;
        foreach (SortedRun r in b.Runs)
        {
            if (!r.BeingCompacted) notCompacted++;
        }
        return notCompacted > maxNumRuns ? (uint)(notCompacted - maxNumRuns + 1) : 0;
    }

    /// <summary>Port of UniversalCompactionBuilder::PickCompactionToReduceSortedRuns.</summary>
    private Compaction? PickCompactionToReduceSortedRuns(Builder b, uint ratio, uint maxNumberOfFilesToCompact)
    {
        List<SortedRun> runs = b.Runs;
        VersionStorageInfo vstorage = b.Vstorage;
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
            ulong candidateSize = found ? runs[loop].CompensatedSize : 0;
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
        int startLevel = runs[startIndex].Level;
        int maxOutputLevel = vstorage.MaxOutputLevel(Options.CfAllowIngestBehind);
        int outputLevel;
        if (firstIndexAfter == runs.Count) outputLevel = maxOutputLevel;
        else if (runs[firstIndexAfter].Level == 0) outputLevel = 0;
        else outputLevel = runs[firstIndexAfter].Level - 1;
        if (!b.MeetsOutputLevelRequirements(outputLevel, maxOutputLevel)) return null;

        CompactionInputFiles[] inputs = NewInputLevels(startLevel, maxOutputLevel);
        for (int i = startIndex; i < firstIndexAfter; i++) AddRun(vstorage, inputs, runs[i], startLevel);

        // Include grandparents for potential file cutting in incremental mode, to align file cutting boundaries across
        // levels so that subsequent compactions can pick files with aligned buffer.
        var grandparents = new List<FileMetaData>();
        if (uo.Incremental && firstIndexAfter < runs.Count && runs[firstIndexAfter].Level > 1)
        {
            grandparents.AddRange(vstorage.LevelFiles(runs[firstIndexAfter].Level).ToArray());
        }
        // intra L0 compactions outputs could have overlap
        if (outputLevel != 0 && FilesRangeOverlapWithCompaction(b.Vstorage, inputs, outputLevel)) return null;
        CompactionReason reason = maxNumberOfFilesToCompact == uint.MaxValue ? CompactionReason.UniversalSizeRatio : CompactionReason.UniversalSortedRunNum;
        return Create(b, inputs, outputLevel, grandparents, reason, enableCompression);
    }

    /// <summary>
    /// Port of UniversalCompactionBuilder::PickCompactionToReduceSizeAmp: if the newer sorted runs add more than
    /// max_size_amplification_percent of the oldest run, compact the longest span of available runs ending at the
    /// oldest one.
    /// </summary>
    private Compaction? PickCompactionToReduceSizeAmp(Builder b)
    {
        List<SortedRun> runs = b.Runs;
        // ShouldSkipLastSortedRunForSizeAmpCompaction: with per-key placement the last sorted run (the last level) is
        // left out, the size amplification compaction then ends at the proximal level.
        int endIndex = ShouldSkipLastSortedRunForSizeAmpCompaction(runs) ? runs.Count - 2 : runs.Count - 1;
        if (runs[endIndex].BeingCompacted) return null;
        ulong baseSrSize = runs[endIndex].Size;
        int startIndex = endIndex;
        ulong candidateSize = 0;
        int numL0Files = 0;
        // Get longest span (i.e, [start_index, end_index]) of available sorted runs.
        while (startIndex > 0)
        {
            SortedRun sr = runs[startIndex - 1];
            if (sr.BeingCompacted) break;
            candidateSize += sr.CompensatedSize;
            if (sr.Level == 0) numL0Files++;
            --startIndex;
        }
        if (startIndex == endIndex) return null;
        MightExcludeNewL0sToReduceWriteStop(runs, numL0Files, endIndex, ref startIndex, ref candidateSize);
        // size amplification = percentage of additional size
        ulong ratio = Options.CompactionOptionsUniversal.MaxSizeAmplificationPercent;
        if (candidateSize * 100 < ratio * baseSrSize) return null;
        // Since incremental compaction can't include more than second last level, it can introduce penalty, compared to
        // full compaction. We hard code the penalty to be 80%: if we end up with a compaction fanout higher than 80% of
        // full level compactions, we fall back to full level compaction.
        if (Options.CompactionOptionsUniversal.Incremental)
        {
            double fanoutThreshold = (double)baseSrSize / candidateSize * 1.8;
            Compaction? picked = PickIncrementalForReduceSizeAmp(b, fanoutThreshold);
            if (picked is not null) return picked;
        }
        return PickCompactionWithSortedRunRange(b, startIndex, endIndex, CompactionReason.UniversalSizeAmplification);
    }

    /// <summary>
    /// Port of MightExcludeNewL0sToReduceWriteStop: excludes some of the newest L0 files from a size amp compaction so a
    /// large number of L0 files is not locked by it (which could stop writes with a few more flushes), as long as the
    /// compaction stays a valid size amp compaction with a similar score.
    /// </summary>
    private void MightExcludeNewL0sToReduceWriteStop(List<SortedRun> runs, int numL0InputPreExclusion, int endIndex, ref int startIndex,
        ref ulong candidateSize)
    {
        if (numL0InputPreExclusion == 0) return;
        ulong level0StopWritesTrigger = (ulong)Math.Max(1, Options.Level0StopWritesTrigger);
        CompactionOptionsUniversal uo = Options.CompactionOptionsUniversal;
        ulong maxMergeWidth = uo.MaxMergeWidth;
        ulong minMergeWidth = uo.MinMergeWidth;
        ulong maxSizeAmplificationPercent = uo.MaxSizeAmplificationPercent;
        ulong baseSrSize = runs[endIndex].Size;
        // Leave at least 1 L0 file and 2 input sorted runs after exclusion.
        ulong maxNumL0ToExclude = Math.Min((ulong)numL0InputPreExclusion - 1, (ulong)(endIndex - startIndex - 1));
        // In universal compaction, sorted runs from non L0 levels are counted toward `level0_stop_writes_trigger`.
        ulong numExtraL0BeforeWriteStop = level0StopWritesTrigger - Math.Min(level0StopWritesTrigger, (ulong)(endIndex - startIndex + 1));
        ulong numL0ToExcludeForMaxMergeWidth = Math.Min(maxMergeWidth - Math.Min(maxMergeWidth, numExtraL0BeforeWriteStop), maxNumL0ToExclude);
        ulong numL0ToExcludeForMinMergeWidth = Math.Min(minMergeWidth - Math.Min(minMergeWidth, numExtraL0BeforeWriteStop), maxNumL0ToExclude);
        ulong numL0ToExclude = 0;
        ulong candidateSizePostExclusion = candidateSize;
        for (ulong possible = numL0ToExcludeForMinMergeWidth; possible <= numL0ToExcludeForMaxMergeWidth; ++possible)
        {
            ulong currentCandidateSize = candidateSizePostExclusion;
            for (ulong j = numL0ToExclude; j < possible; ++j) currentCandidateSize -= runs[startIndex + (int)j].CompensatedSize;
            // To ensure the compaction score before and after exclusion is similar so this exclusion will not make the
            // size amp compaction of interest invalid from running as a size amp compaction as long as its
            // pre-exclusion compaction score satisfies the condition to run.
            if (currentCandidateSize * 100 < maxSizeAmplificationPercent * baseSrSize || currentCandidateSize < candidateSize * 9 / 10) break;
            numL0ToExclude = possible;
            candidateSizePostExclusion = currentCandidateSize;
        }
        startIndex += (int)numL0ToExclude;
        candidateSize = candidateSizePostExclusion;
    }

    /// <summary>
    /// Port of UniversalCompactionBuilder::PickIncrementalForReduceSizeAmp: a sliding window over the second-to-last
    /// sorted run looks for the range (just over max_compaction_bytes / 2 with the overlapping last-level files) with
    /// the lowest fanout below <paramref name="fanoutThreshold"/>, then pulls in the files of the upper levels inside
    /// that range.
    /// </summary>
    private Compaction? PickIncrementalForReduceSizeAmp(Builder b, double fanoutThreshold)
    {
        List<SortedRun> runs = b.Runs;
        VersionStorageInfo vstorage = b.Vstorage;
        int secondLastLevel = runs[^2].Level;
        if (secondLastLevel == 0) return null; // Can't split Level 0.
        int outputLevel = runs[^1].Level;
        ReadOnlySpan<FileMetaData> bottomFiles = vstorage.LevelFiles(outputLevel);
        ReadOnlySpan<FileMetaData> files = vstorage.LevelFiles(secondLastLevel);
        int pickedStartIdx = 0, pickedEndIdx = 0;
        double pickedFanout = fanoutThreshold;
        // Use half target compaction bytes as anchor to stop growing second most level files, and reserve growing
        // space for more overlapping bottom level, clean cut, files from other levels, etc.
        ulong compThresSize = Options.MaxCompactionBytes / 2;
        int startIdx = 0, bottomEndIdx = 0, bottomStartIdx = 0;
        ulong nonBottomSize = 0, bottomSize = 0;
        bool endBottomSizeCounted = false;
        for (int endIdx = 0; endIdx < files.Length; endIdx++)
        {
            FileMetaData endFile = files[endIdx];
            // Include bottom most level files smaller than the current second last level file.
            int numSkipped = 0;
            while (bottomEndIdx < bottomFiles.Length && Icmp.Compare(bottomFiles[bottomEndIdx].Largest, endFile.Smallest) < 0)
            {
                if (!endBottomSizeCounted) bottomSize += bottomFiles[bottomEndIdx].FileSize;
                bottomEndIdx++;
                endBottomSizeCounted = false;
                numSkipped++;
            }
            // At least a file in the bottom most level falls into the file gap. No reason to include the file. We cut
            // the range and start a new sliding window.
            if (numSkipped > 1) startIdx = endIdx;
            if (startIdx == endIdx)
            {
                // new sliding window.
                nonBottomSize = 0;
                bottomSize = 0;
                bottomStartIdx = bottomEndIdx;
                endBottomSizeCounted = false;
            }
            nonBottomSize += endFile.FileSize;
            // Include all overlapping files in bottom level.
            while (bottomEndIdx < bottomFiles.Length && Icmp.Compare(bottomFiles[bottomEndIdx].Smallest, endFile.Largest) < 0)
            {
                if (!endBottomSizeCounted)
                {
                    bottomSize += bottomFiles[bottomEndIdx].FileSize;
                    endBottomSizeCounted = true;
                }
                // next level file cross large boundary of current file.
                if (Icmp.Compare(bottomFiles[bottomEndIdx].Largest, endFile.Largest) > 0) break;
                bottomEndIdx++;
                endBottomSizeCounted = false;
            }
            if ((nonBottomSize + bottomSize > compThresSize || endIdx == files.Length - 1) && nonBottomSize > 0)
            {
                // If it is a better compaction, remember it in picked* variables.
                double fanout = (double)bottomSize / nonBottomSize;
                if (fanout < pickedFanout)
                {
                    pickedStartIdx = startIdx;
                    pickedEndIdx = endIdx;
                    pickedFanout = fanout;
                }
                // Shrink from the start end to under comp_thres_size.
                while (nonBottomSize + bottomSize > compThresSize && startIdx <= endIdx)
                {
                    nonBottomSize -= files[startIdx].FileSize;
                    startIdx++;
                    if (startIdx < files.Length)
                    {
                        while (bottomStartIdx <= bottomEndIdx && bottomStartIdx < bottomFiles.Length
                               && Icmp.Compare(bottomFiles[bottomStartIdx].Largest, files[startIdx].Smallest) < 0)
                        {
                            bottomSize -= bottomFiles[bottomStartIdx].FileSize;
                            bottomStartIdx++;
                        }
                    }
                }
            }
        }
        if (pickedFanout >= fanoutThreshold) return null;

        var secondLastLevelInputs = new CompactionInputFiles(secondLastLevel);
        var bottomLevelInputs = new CompactionInputFiles(outputLevel);
        for (int i = pickedStartIdx; i <= pickedEndIdx; i++)
        {
            if (files[i].BeingCompacted) return null;
            secondLastLevelInputs.Files.Add(files[i]);
        }
        if (!ExpandInputsToCleanCut(vstorage, secondLastLevelInputs)) return null;
        int parentIndex = -1;
        if (!SetupOtherInputs(vstorage, secondLastLevelInputs, bottomLevelInputs, ref parentIndex, -1, false)) return null;

        // Try to include files in upper levels if they fall into the range. We go from lower level up, the reverse of
        // the level order, so they are collected reversed first.
        GetRange(secondLastLevelInputs, out byte[] smallest, out byte[] largest);
        var inputsReverse = new List<CompactionInputFiles>();
        for (int k = runs.Count - 3; k >= 0; k--)
        {
            SortedRun sr = runs[k];
            if (sr.Level == 0) break;
            var levelInputs = new List<FileMetaData>();
            vstorage.GetCleanInputsWithinInterval(sr.Level, smallest, largest, levelInputs, -1, out _);
            if (levelInputs.Count > 0)
            {
                var ci = new CompactionInputFiles(sr.Level, levelInputs);
                inputsReverse.Add(ci);
                GetRange(ci, out smallest, out largest);
            }
        }
        var inputs = new List<CompactionInputFiles>();
        for (int i = inputsReverse.Count - 1; i >= 0; i--) inputs.Add(inputsReverse[i]);
        inputs.Add(secondLastLevelInputs);
        inputs.Add(bottomLevelInputs);
        // The start level is the first non-empty input (the job and the compaction read the first input as the start).
        while (inputs.Count > 1 && inputs[0].IsEmpty) inputs.RemoveAt(0);
        // intra L0 compactions outputs could have overlap
        if (outputLevel != 0 && FilesRangeOverlapWithCompaction(b.Vstorage, inputs, outputLevel)) return null;
        return Create(b, DropEmptyInnerLevels(inputs), outputLevel, new List<FileMetaData>(), CompactionReason.UniversalSizeAmplification, true);
    }

    /// <summary>
    /// Port of UniversalCompactionBuilder::PickDeleteTriggeredCompaction: compacts files marked for compaction
    /// (typically by CompactOnDeletionCollector because of tombstones) with the overlapping files of the next
    /// non-empty level; with a single level, from the first marked L0 file to the oldest run not being compacted.
    /// </summary>
    private Compaction? PickDeleteTriggeredCompaction(Builder b)
    {
        VersionStorageInfo vstorage = b.Vstorage;
        List<SortedRun> runs = b.Runs;
        var startLevelInputs = new CompactionInputFiles(0);
        if (vstorage.NumLevels == 1)
        {
            // This is single level universal. Since we're basically trying to reclaim space by processing files marked
            // for compaction due to high tombstone density, let's do the same thing as compaction to reduce size amp
            // which has the same goals.
            int startIndex = -1;
            ReadOnlySpan<FileMetaData> l0 = vstorage.LevelFiles(0);
            // Find the first file marked for compaction. Ignore the last file.
            for (int loop = 0; loop + 1 < runs.Count; loop++)
            {
                if (runs[loop].BeingCompacted) continue;
                FileMetaData f = l0[loop];
                if (f.MarkedForCompaction)
                {
                    startLevelInputs.Files.Add(f);
                    startIndex = loop; // Consider this as the first candidate.
                    break;
                }
            }
            if (startIndex < 0) return null; // Either no file marked, or they're already being compacted.
            for (int loop = startIndex + 1; loop < runs.Count; loop++)
            {
                if (runs[loop].BeingCompacted) break;
                startLevelInputs.Files.Add(l0[loop]);
            }
        }
        else
        {
            // For multi-level universal, the strategy is to make this look more like leveled. We pick one of the files
            // marked for compaction and compact with overlapping files in the adjacent level.
            PickFilesMarkedForCompaction(vstorage, out _, out _, startLevelInputs);
            if (startLevelInputs.IsEmpty) return null;
        }
        return BuildCompactionToNextLevel(b, startLevelInputs, CompactionReason.FilesMarkedForCompaction);
    }

    /// <summary>
    /// Port of UniversalCompactionBuilder::PickReadTriggeredCompaction: the hottest file marked by
    /// ComputeFilesMarkedForReadTriggeredCompaction that expands to a clean cut (L0 files are skipped while an L0
    /// compaction runs), compacted to the next non-empty level; with a single level, with every overlapping L0 file.
    /// </summary>
    private Compaction? PickReadTriggeredCompaction(Builder b)
    {
        VersionStorageInfo vstorage = b.Vstorage;
        CompactionInputFiles? startLevelInputs = null;
        foreach ((int level, FileMetaData file) in vstorage.ReadTriggeredCompactionFiles)
        {
            if (file.BeingCompacted) continue;
            if (level == 0 && IsLevel0CompactionInProgress) continue;
            var candidate = new CompactionInputFiles(level, new List<FileMetaData> { file });
            if (ExpandInputsToCleanCut(vstorage, candidate))
            {
                startLevelInputs = candidate;
                break;
            }
        }
        if (startLevelInputs is null || startLevelInputs.IsEmpty) return null;
        // For intra L0 compactions, pick up all overlapping files
        if (vstorage.NumLevels == 1 && startLevelInputs.Level == 0)
        {
            int unusedParentIndex = -1;
            if (!GetOverlappingL0Files(vstorage, startLevelInputs, 0, ref unusedParentIndex)) return null;
        }
        return BuildCompactionToNextLevel(b, startLevelInputs, CompactionReason.ReadTriggered);
    }

    /// <summary>
    /// Port of UniversalCompactionBuilder::BuildCompactionToNextLevel: output to the first non-empty level after the
    /// start level (the last level for an L0 start when all are empty), with the overlapping output-level files.
    /// </summary>
    private Compaction? BuildCompactionToNextLevel(Builder b, CompactionInputFiles startLevelInputs, CompactionReason reason)
    {
        VersionStorageInfo vstorage = b.Vstorage;
        int startLevel = startLevelInputs.Level;
        int maxOutputLevel = vstorage.MaxOutputLevel(Options.CfAllowIngestBehind);
        // Pick the first non-empty level after start_level as output.
        int outputLevel;
        for (outputLevel = startLevel + 1; outputLevel <= maxOutputLevel; outputLevel++)
        {
            if (vstorage.NumLevelFiles(outputLevel) != 0) break;
        }
        // If all higher levels are empty, pick the highest level as output level for L0 start. For non-L0 start, a
        // trivial move doesn't help reclaim space, so skip.
        if (outputLevel > maxOutputLevel)
        {
            if (startLevel == 0) outputLevel = maxOutputLevel;
            else return null;
        }
        // If only the last file in L0 is marked for compaction, ignore it.
        if (startLevelInputs.Count <= 1 && outputLevel == 0) return null;

        var inputs = new List<CompactionInputFiles>();
        var grandparents = new List<FileMetaData>();
        if (outputLevel != 0)
        {
            if (!b.MeetsOutputLevelRequirements(outputLevel, maxOutputLevel)) return null;
            if (startLevel == 0)
            {
                int unusedParentIndex = -1;
                if (!GetOverlappingL0Files(vstorage, startLevelInputs, outputLevel, ref unusedParentIndex)) return null;
            }
            var outputLevelInputs = new CompactionInputFiles(outputLevel);
            int parentIndex = -1;
            if (!SetupOtherInputs(vstorage, startLevelInputs, outputLevelInputs, ref parentIndex, -1, false)) return null;
            inputs.Add(startLevelInputs);
            if (!outputLevelInputs.IsEmpty) inputs.Add(outputLevelInputs);
            if (FilesRangeOverlapWithCompaction(b.Vstorage, inputs, outputLevel)) return null;
            GetGrandparents(vstorage, startLevelInputs, outputLevelInputs, grandparents);
        }
        else
        {
            inputs.Add(startLevelInputs);
        }
        // GetCompressionType(vstorage, mutable_cf_options, output_level, 1): compression always enabled.
        return Create(b, inputs.ToArray(), outputLevel, grandparents, reason, true);
    }

    /// <summary>
    /// Port of UniversalCompactionBuilder::PickPeriodicCompaction: a full compaction from the oldest sorted run up to
    /// the newest run not being compacted, if it covers a file marked for periodic compaction.
    /// </summary>
    private Compaction? PickPeriodicCompaction(Builder b)
    {
        List<SortedRun> runs = b.Runs;
        // In universal compaction, sorted runs contain older data are almost always generated earlier too. To simplify
        // the problem, we just try to trigger a full compaction. We start from the oldest sorted run and include all
        // sorted runs, until we hit a sorted already being compacted.
        int startIndex = runs.Count;
        while (startIndex > 0 && !runs[startIndex - 1].BeingCompacted) startIndex--;
        if (startIndex == runs.Count) return null;
        // There is a rare corner case where we can't pick up all the files because some files are being compacted and
        // we end up with picking files but none of them need periodic compaction. Unless we simply recompact the last
        // sorted run (either the last level or last L0 file), we would just execute the compaction, in order to
        // simplify the logic.
        if (startIndex == runs.Count - 1)
        {
            bool includedFileMarked = false;
            int startLevel = runs[startIndex].Level;
            FileMetaData? startFile = runs[startIndex].File;
            foreach ((int level, FileMetaData file) in b.Vstorage.FilesMarkedForPeriodicCompaction)
            {
                if (startLevel != 0 ? startLevel == level : ReferenceEquals(startFile, file))
                {
                    includedFileMarked = true;
                    break;
                }
            }
            // "Cannot form a compaction covering file marked for periodic compaction"
            if (!includedFileMarked) return null;
        }
        return PickCompactionWithSortedRunRange(b, startIndex, runs.Count - 1, CompactionReason.PeriodicCompaction);
    }

    /// <summary>Port of PickCompactionWithSortedRunRange (size amp and periodic compactions; always compressed).</summary>
    private Compaction? PickCompactionWithSortedRunRange(Builder b, int startIndex, int endIndex, CompactionReason reason)
    {
        List<SortedRun> runs = b.Runs;
        VersionStorageInfo vstorage = b.Vstorage;
        int startLevel = runs[startIndex].Level;
        int maxOutputLevel = vstorage.MaxOutputLevel(Options.CfAllowIngestBehind);
        CompactionInputFiles[] inputs = NewInputLevels(startLevel, maxOutputLevel);
        for (int i = startIndex; i <= endIndex; i++) AddRun(vstorage, inputs, runs[i], startLevel);
        // If it's not including all sorted_runs, it can only output to the level above the `end_index + 1` sorted_run.
        int outputLevel = endIndex == runs.Count - 1 ? maxOutputLevel : runs[endIndex + 1].Level - 1;
        if (!b.MeetsOutputLevelRequirements(outputLevel, maxOutputLevel)) return null;
        // intra L0 compactions outputs could have overlap
        if (outputLevel != 0 && FilesRangeOverlapWithCompaction(b.Vstorage, inputs, outputLevel)) return null;
        // We never check size for compaction_options_universal.compression_size_percent, because we always compact all
        // the files, so always compress.
        return Create(b, inputs, outputLevel, new List<FileMetaData>(), reason, true);
    }

    /// <summary>`std::vector&lt;CompactionInputFiles&gt; inputs(max_output_level + 1)` with levels start_level + i.</summary>
    private static CompactionInputFiles[] NewInputLevels(int startLevel, int maxOutputLevel)
    {
        var inputs = new CompactionInputFiles[Math.Max(1, maxOutputLevel + 1 - startLevel)];
        for (int i = 0; i < inputs.Length; i++) inputs[i] = new CompactionInputFiles(startLevel + i);
        return inputs;
    }

    private static void AddRun(VersionStorageInfo vstorage, CompactionInputFiles[] inputs, SortedRun r, int startLevel)
    {
        if (r.Level == 0) inputs[0].Files.Add(r.File!);
        else inputs[r.Level - startLevel].Files.AddRange(vstorage.LevelFiles(r.Level).ToArray());
    }

    /// <summary>Keeps the start level first and drops empty later levels (the job only reads non-empty inputs).</summary>
    private static CompactionInputFiles[] DropEmptyInnerLevels(IReadOnlyList<CompactionInputFiles> inputs)
    {
        var compact = new List<CompactionInputFiles> { inputs[0] };
        for (int i = 1; i < inputs.Count; i++)
        {
            if (!inputs[i].IsEmpty) compact.Add(inputs[i]);
        }
        return compact.ToArray();
    }

    /// <summary>
    /// Creates the compaction (new Compaction(...) in the builder) without registering it: max output file size of the
    /// output level, GetMaxOverlappingBytes, the trivial-move decision of PickCompaction (allow_trivial_move, not for
    /// periodic compactions) and the compression switch.
    /// </summary>
    private Compaction Create(Builder b, IReadOnlyList<CompactionInputFiles> inputs, int outputLevel, List<FileMetaData> grandparents,
        CompactionReason reason, bool enableCompression)
    {
        CompactionInputFiles[] compact = DropEmptyInnerLevels(inputs);
        bool trivial = Options.CompactionOptionsUniversal.AllowTrivialMove && reason != CompactionReason.PeriodicCompaction
            && IsInputFilesNonOverlapping(compact);
        var c = new Compaction(b.Vstorage, Options, compact, outputLevel, MaxFileSizeForLevel(Options, outputLevel, 1), GetMaxOverlappingBytes(), 0,
            enableCompression ? GetCompressionType(b.Vstorage, Options, outputLevel, 1) : Compression.CompressionType.None, grandparents, reason,
            b.Score, l0FilesMightOverlap: true, isManual: false)
        {
            IsTrivialMoveForced = trivial,
            EnableCompression = enableCompression,
        };
        return c;
    }

    /// <summary>
    /// Port of GetMaxOverlappingBytes: in incremental mode, try to align cutting boundary with files at the next level
    /// if the file isn't end up with 1/2 of target size, or it would overlap with two full size files at the next level.
    /// </summary>
    private ulong GetMaxOverlappingBytes() =>
        Options.CompactionOptionsUniversal.Incremental ? Options.TargetFileSizeBase / 2 * 3 : ulong.MaxValue;

    /// <summary>
    /// Port of UniversalCompactionBuilder::IsInputFilesNonOverlapping: walks the input files in smallest-user-key order
    /// and fails if a file's smallest user key is not past the previous file's largest user key.
    /// </summary>
    private bool IsInputFilesNonOverlapping(CompactionInputFiles[] inputs)
    {
        var all = new List<FileMetaData>();
        foreach (CompactionInputFiles l in inputs) all.AddRange(l.Files);
        Comparator ucmp = Icmp.UserComparator;
        all.Sort((a, x) => ucmp.CompareWithoutTimestamp(DbFormat.ExtractUserKey(a.Smallest), DbFormat.ExtractUserKey(x.Smallest)));
        for (int i = 1; i < all.Count; i++)
        {
            if (ucmp.CompareWithoutTimestamp(DbFormat.ExtractUserKey(all[i - 1].Largest), DbFormat.ExtractUserKey(all[i].Smallest)) >= 0) return false;
        }
        return true;
    }
}
