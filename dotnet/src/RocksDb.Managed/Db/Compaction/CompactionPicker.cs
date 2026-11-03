using RocksDb.Managed.Compression;

namespace RocksDb.Managed.Db;

/// <summary>
/// Base compaction picker shared by the compaction styles. Port of rocksdb::CompactionPicker
/// (db/compaction/compaction_picker.{h,cc}): running-compaction registry, key ranges, clean-cut expansion, parent-level
/// inputs (SetupOtherInputs), grandparents, manual range compaction (PickCompactionForCompactRange), intra-L0 cost
/// based picking, files marked for compaction.
/// <para>
/// One instance per column family. All methods REQUIRE the DB mutex. Internal keys are passed around as
/// <c>byte[]</c> (picking is a cold path: once per compaction).
/// </para>
/// </summary>
internal abstract partial class CompactionPicker
{
    private readonly HashSet<Compaction> _compactionsInProgress = new();
    private readonly HashSet<Compaction> _level0CompactionsInProgress = new();

    protected CompactionPicker(ColumnFamilyOptions options, InternalKeyComparator icmp)
    {
        Options = options;
        Icmp = icmp;
    }

    public ColumnFamilyOptions Options { get; }
    public InternalKeyComparator Icmp { get; }
    public int NumberLevels => Math.Max(1, Options.NumLevels);

    public IReadOnlyCollection<Compaction> CompactionsInProgress => _compactionsInProgress;
    public IReadOnlyCollection<Compaction> Level0CompactionsInProgress => _level0CompactionsInProgress;
    public bool IsLevel0CompactionInProgress => _level0CompactionsInProgress.Count > 0;

    /// <summary>CompactionPicker::IsCompactionInProgress: any registered compaction (incl. file-ingesting ones).</summary>
    public bool IsCompactionInProgress => _compactionsInProgress.Count > 0;

    /// <summary>Port of CompactionPicker::UnregisterCompaction (a compaction that marked no files, e.g. file-ingesting).</summary>
    public void UnregisterCompaction(Compaction c)
    {
        _level0CompactionsInProgress.Remove(c);
        _compactionsInProgress.Remove(c);
    }

    /// <summary>Whether the version needs (automatic) compaction.</summary>
    public abstract bool NeedsCompaction(VersionStorageInfo vstorage);

    /// <summary>Picks an automatic compaction, or null. <paramref name="snapshots"/> are the live snapshots (ascending).</summary>
    public abstract Compaction? PickCompaction(VersionStorageInfo vstorage, IReadOnlyList<ulong> snapshots);

    /// <summary>
    /// CompactionPicker::PickCompaction with require_max_output_level (a bottom-priority thread repicking a forwarded
    /// compaction): only the universal picker honours it, like upstream.
    /// </summary>
    public virtual Compaction? PickCompaction(VersionStorageInfo vstorage, IReadOnlyList<ulong> snapshots, bool requireMaxOutputLevel) =>
        PickCompaction(vstorage, snapshots);

    // ---------------------------------------------------------------------------------------------------------
    // registry

    /// <summary>Port of CompactionPicker::RegisterCompaction.</summary>
    public void RegisterCompaction(Compaction c)
    {
        if ((c.StartLevel == 0 && c.Reason != CompactionReason.ExternalSstIngestion) || Options.CompactionStyle == CompactionStyle.Universal)
        {
            _level0CompactionsInProgress.Add(c);
        }
        _compactionsInProgress.Add(c);
    }

    /// <summary>Port of CompactionPicker::ReleaseCompactionFiles (UnregisterCompaction + reset of the next index on failure).</summary>
    public void ReleaseCompactionFiles(Compaction c, bool succeeded)
    {
        _level0CompactionsInProgress.Remove(c);
        _compactionsInProgress.Remove(c);
        c.MarkFilesBeingCompacted(false);
        if (!succeeded) c.InputStorage.ResetNextCompactionIndex(c.StartLevel);
    }

    // ---------------------------------------------------------------------------------------------------------
    // ranges

    /// <summary>Port of CompactionPicker::GetRange(inputs).</summary>
    public void GetRange(CompactionInputFiles inputs, out byte[] smallest, out byte[] largest)
    {
        if (inputs.Level == 0)
        {
            smallest = inputs[0].Smallest;
            largest = inputs[0].Largest;
            for (int i = 1; i < inputs.Count; i++)
            {
                FileMetaData f = inputs[i];
                if (Icmp.Compare(f.Smallest, smallest) < 0) smallest = f.Smallest;
                if (Icmp.Compare(f.Largest, largest) > 0) largest = f.Largest;
            }
        }
        else
        {
            smallest = inputs[0].Smallest;
            largest = inputs[inputs.Count - 1].Largest;
        }
    }

    /// <summary>Port of CompactionPicker::GetRange(inputs1, inputs2).</summary>
    public void GetRange(CompactionInputFiles inputs1, CompactionInputFiles inputs2, out byte[] smallest, out byte[] largest)
    {
        if (inputs1.IsEmpty)
        {
            GetRange(inputs2, out smallest, out largest);
        }
        else if (inputs2.IsEmpty)
        {
            GetRange(inputs1, out smallest, out largest);
        }
        else
        {
            GetRange(inputs1, out byte[] s1, out byte[] l1);
            GetRange(inputs2, out byte[] s2, out byte[] l2);
            smallest = Icmp.Compare(s1, s2) < 0 ? s1 : s2;
            largest = Icmp.Compare(l1, l2) < 0 ? l2 : l1;
        }
    }

    /// <summary>Port of CompactionPicker::GetRange(vector of inputs, exclude_level). Returns false if all are empty.</summary>
    public bool GetRange(IReadOnlyList<CompactionInputFiles> inputs, out byte[] smallest, out byte[] largest, int excludeLevel)
    {
        smallest = [];
        largest = [];
        bool initialized = false;
        foreach (CompactionInputFiles input in inputs)
        {
            if (input.IsEmpty || input.Level == excludeLevel) continue;
            GetRange(input, out byte[] s, out byte[] l);
            if (!initialized)
            {
                smallest = s;
                largest = l;
                initialized = true;
            }
            else
            {
                if (Icmp.Compare(s, smallest) < 0) smallest = s;
                if (Icmp.Compare(l, largest) > 0) largest = l;
            }
        }
        return initialized;
    }

    /// <summary>Port of AreFilesInCompaction.</summary>
    public static bool AreFilesInCompaction(List<FileMetaData> files)
    {
        foreach (FileMetaData f in files)
        {
            if (f.BeingCompacted) return true;
        }
        return false;
    }

    /// <summary>
    /// Port of CompactionPicker::ExpandInputsToCleanCut: grows the inputs of a sorted level until no user key spans the
    /// boundary to a non-selected file. Returns false if an expanded input is being compacted.
    /// </summary>
    public bool ExpandInputsToCleanCut(VersionStorageInfo vstorage, CompactionInputFiles inputs) =>
        ExpandInputsToCleanCut(vstorage, inputs, out _);

    public bool ExpandInputsToCleanCut(VersionStorageInfo vstorage, CompactionInputFiles inputs, out byte[]? nextSmallest)
    {
        nextSmallest = null;
        int level = inputs.Level;
        // GetOverlappingInputs will always do the right thing for level-0.
        if (level == 0) return true;
        int hintIndex = -1;
        int oldSize;
        var files = new List<FileMetaData>();
        do
        {
            oldSize = inputs.Count;
            GetRange(inputs, out byte[] smallest, out byte[] largest);
            files.Clear();
            vstorage.GetOverlappingInputsRangeBinarySearch(level, smallest, largest, files, hintIndex, out hintIndex, withinInterval: false,
                out nextSmallest);
            inputs.Files = new List<FileMetaData>(files);
        } while (inputs.Count > oldSize);
        // If, after the expansion, there are files that are already under compaction, then we must drop/cancel this compaction.
        return !AreFilesInCompaction(inputs.Files);
    }

    /// <summary>
    /// Port of CompactionPicker::RangeOverlapWithCompaction. With <paramref name="rangeLimitExclusive"/>,
    /// <paramref name="largestUserKey"/> is an exclusive limit.
    /// </summary>
    public bool RangeOverlapWithCompaction(ReadOnlySpan<byte> smallestUserKey, ReadOnlySpan<byte> largestUserKey, int level, bool rangeLimitExclusive)
    {
        foreach (Compaction c in _compactionsInProgress)
        {
            int largestCmp = VersionStorageInfo.CompareWithoutTimestamp(Icmp, largestUserKey, c.SmallestUserKey);
            if (c.OutputLevel == level
                && VersionStorageInfo.CompareWithoutTimestamp(Icmp, smallestUserKey, c.LargestUserKey) <= 0
                && (rangeLimitExclusive ? largestCmp > 0 : largestCmp >= 0))
            {
                return true;
            }
            if (c.SupportsPerKeyPlacement && c.OverlapProximalLevelOutputRange(smallestUserKey, largestUserKey, rangeLimitExclusive)) return true;
        }
        return false;
    }

    /// <summary>
    /// Port of CompactionPicker::FilesRangeOverlapWithCompaction with the proximal level the compaction would write
    /// (Compaction::EvaluateProximalLevel of the inputs' start level and <paramref name="level"/>).
    /// </summary>
    public bool FilesRangeOverlapWithCompaction(VersionStorageInfo vstorage, IReadOnlyList<CompactionInputFiles> inputs, int level)
    {
        int startLevel = -1;
        foreach (CompactionInputFiles i in inputs)
        {
            if (!i.IsEmpty)
            {
                startLevel = i.Level;
                break;
            }
        }
        if (startLevel < 0) return false; // No files in inputs
        if (!GetRange(inputs, out byte[] smallest, out byte[] largest, -1)) return false;
        int proximalLevel = Compaction.EvaluateProximalLevel(vstorage, Options, startLevel, level);
        if (proximalLevel != Compaction.InvalidLevel)
        {
            if (Options.CompactionStyle == CompactionStyle.Universal)
            {
                if (RangeOverlapWithCompaction(DbFormat.ExtractUserKey(smallest), DbFormat.ExtractUserKey(largest), proximalLevel, rangeLimitExclusive: false)) return true;
            }
            else if (GetRange(inputs, out byte[] proximalSmallest, out byte[] proximalLargest, level)
                     && RangeOverlapWithCompaction(DbFormat.ExtractUserKey(proximalSmallest), DbFormat.ExtractUserKey(proximalLargest), proximalLevel, rangeLimitExclusive: false))
            {
                return true;
            }
        }
        return RangeOverlapWithCompaction(DbFormat.ExtractUserKey(smallest), DbFormat.ExtractUserKey(largest), level, rangeLimitExclusive: false);
    }

    /// <summary>Port of CompactionPicker::IsRangeInCompaction.</summary>
    public static bool IsRangeInCompaction(VersionStorageInfo vstorage, byte[] smallest, byte[] largest, int level, ref int levelIndex)
    {
        var inputs = new List<FileMetaData>();
        vstorage.GetOverlappingInputs(level, smallest, largest, inputs, levelIndex, out levelIndex);
        return AreFilesInCompaction(inputs);
    }

    /// <summary>
    /// Port of CompactionPicker::SetupOtherInputs: adds the overlapping output-level files and, when it doesn't pull in
    /// more output files, expands the start-level inputs. Returns false if parent-level files are being compacted.
    /// </summary>
    public bool SetupOtherInputs(VersionStorageInfo vstorage, CompactionInputFiles inputs, CompactionInputFiles outputLevelInputs,
        ref int parentIndex, int baseIndex, bool onlyExpandTowardsRight)
    {
        int inputLevel = inputs.Level;
        int outputLevel = outputLevelInputs.Level;
        if (inputLevel == outputLevel) return true; // no possibility of conflict

        GetRange(inputs, out byte[] smallest, out byte[] largest);
        var outFiles = new List<FileMetaData>();
        vstorage.GetOverlappingInputs(outputLevel, smallest, largest, outFiles, parentIndex, out parentIndex);
        outputLevelInputs.Files = outFiles;
        if (AreFilesInCompaction(outputLevelInputs.Files)) return false;
        if (!outputLevelInputs.IsEmpty && !ExpandInputsToCleanCut(vstorage, outputLevelInputs)) return false;

        // See if we can further grow the number of inputs in "level" without changing the number of "level+1" files
        // we pick up.
        if (!outputLevelInputs.IsEmpty)
        {
            ulong outputLevelInputsSize = TotalFileSize(outputLevelInputs.Files);
            bool expandInputs = false;
            var expandedInputs = new CompactionInputFiles(inputLevel);
            GetRange(inputs, outputLevelInputs, out byte[] allStart, out byte[] allLimit);
            bool tryOverlappingInputs = true;
            var tmp = new List<FileMetaData>();
            vstorage.GetOverlappingInputs(inputLevel, onlyExpandTowardsRight ? smallest : allStart, allLimit, tmp, baseIndex, out _);
            expandedInputs.Files = new List<FileMetaData>(tmp);
            ulong expandedInputsSize = TotalFileSize(expandedInputs.Files);
            if (!ExpandInputsToCleanCut(vstorage, expandedInputs)) tryOverlappingInputs = false;
            // A softer limit to include more input level files without expanding output level files.
            ulong limit = MultiplyCheckOverflow(Options.MaxCompactionBytes, 2.0);
            if (tryOverlappingInputs && expandedInputs.Count > inputs.Count && !AreFilesInCompaction(expandedInputs.Files)
                && outputLevelInputsSize + expandedInputsSize < limit)
            {
                GetRange(expandedInputs, out byte[] newStart, out byte[] newLimit);
                var expandedOutput = new CompactionInputFiles(outputLevel);
                var eo = new List<FileMetaData>();
                vstorage.GetOverlappingInputs(outputLevel, newStart, newLimit, eo, parentIndex, out parentIndex);
                expandedOutput.Files = eo;
                if (!AreFilesInCompaction(expandedOutput.Files) && ExpandInputsToCleanCut(vstorage, expandedOutput)
                    && expandedOutput.Count == outputLevelInputs.Count)
                {
                    expandInputs = true;
                }
            }
            if (!expandInputs)
            {
                var clean = new List<FileMetaData>();
                vstorage.GetCleanInputsWithinInterval(inputLevel, allStart, allLimit, clean, baseIndex, out _);
                expandedInputs.Files = clean;
                expandedInputsSize = TotalFileSize(clean);
                if (expandedInputs.Count > inputs.Count && !AreFilesInCompaction(clean) && outputLevelInputsSize + expandedInputsSize < limit)
                {
                    expandInputs = true;
                }
            }
            if (expandInputs) inputs.Files = expandedInputs.Files;
        }
        return true;
    }

    /// <summary>Port of CompactionPicker::GetGrandparents.</summary>
    public void GetGrandparents(VersionStorageInfo vstorage, CompactionInputFiles inputs, CompactionInputFiles outputLevelInputs, List<FileMetaData> grandparents)
    {
        GetRange(inputs, outputLevelInputs, out byte[] start, out byte[] limit);
        // Compute the set of grandparent files that overlap this compaction (parent == level+1; grandparent == level+2
        // or the first level after that has overlapping files).
        for (int level = outputLevelInputs.Level + 1; level < vstorage.NumLevels; level++)
        {
            vstorage.GetOverlappingInputs(level, start, limit, grandparents);
            if (grandparents.Count > 0) break;
        }
    }

    /// <summary>Port of CompactionPicker::GetOverlappingL0Files.</summary>
    public bool GetOverlappingL0Files(VersionStorageInfo vstorage, CompactionInputFiles startLevelInputs, int outputLevel, ref int parentIndex)
    {
        GetRange(startLevelInputs, out byte[] smallest, out byte[] largest);
        var files = new List<FileMetaData>();
        vstorage.GetOverlappingInputs(0, smallest, largest, files, -1, out _, expandRange: true);
        startLevelInputs.Files = files;
        GetRange(startLevelInputs, out smallest, out largest);
        return !IsRangeInCompaction(vstorage, smallest, largest, outputLevel, ref parentIndex);
    }

    /// <summary>Port of CompactionPicker::PickFilesMarkedForCompaction (the random first choice is the first file).</summary>
    public bool PickFilesMarkedForCompaction(VersionStorageInfo vstorage, out int startLevel, out int outputLevel, CompactionInputFiles startLevelInputs)
    {
        startLevel = -1;
        outputLevel = -1;
        foreach ((int level, FileMetaData file) in vstorage.FilesMarkedForCompaction)
        {
            if (file.BeingCompacted) continue;
            startLevel = level;
            outputLevel = level == 0 ? vstorage.BaseLevel : level + 1;
            if (level == 0 && IsLevel0CompactionInProgress) continue;
            startLevelInputs.Level = level;
            startLevelInputs.Files = new List<FileMetaData> { file };
            if (ExpandInputsToCleanCut(vstorage, startLevelInputs)) return true;
        }
        startLevelInputs.Clear();
        return false;
    }

    /// <summary>Port of PickCostBasedIntraL0Compaction (db/compaction/compaction_picker.cc).</summary>
    public static bool PickCostBasedIntraL0Compaction(ReadOnlySpan<FileMetaData> levelFiles, int minFilesToCompact, ulong maxCompactBytesPerDelFile,
        ulong maxCompactionBytes, CompactionInputFiles compInputs)
    {
        int start = 0;
        if (levelFiles.Length == 0 || levelFiles[start].BeingCompacted) return false;
        ulong compactBytes = levelFiles[start].FileSize;
        ulong compactBytesPerDelFile = ulong.MaxValue;
        int limit;
        // Pull in files until the amount of compaction work per deleted file begins increasing or maximum total
        // compaction size is reached.
        for (limit = start + 1; limit < levelFiles.Length; ++limit)
        {
            compactBytes += levelFiles[limit].FileSize;
            ulong newCompactBytesPerDelFile = compactBytes / (ulong)(limit - start);
            if (levelFiles[limit].BeingCompacted || newCompactBytesPerDelFile > compactBytesPerDelFile || compactBytes > maxCompactionBytes) break;
            compactBytesPerDelFile = newCompactBytesPerDelFile;
        }
        if (limit - start >= minFilesToCompact && compactBytesPerDelFile < maxCompactBytesPerDelFile)
        {
            compInputs.Level = 0;
            compInputs.Files.Clear();
            for (int i = start; i < limit; ++i) compInputs.Files.Add(levelFiles[i]);
            return true;
        }
        return false;
    }

    public static ulong TotalFileSize(List<FileMetaData> files)
    {
        ulong sum = 0;
        foreach (FileMetaData f in files) sum += f.FileSize;
        return sum;
    }

    public static ulong MultiplyCheckOverflow(ulong op1, double op2)
    {
        if (op1 == 0 || op2 <= 0) return 0;
        if (ulong.MaxValue / op1 < op2) return op1;
        double r = op1 * op2;
        return r >= ulong.MaxValue ? ulong.MaxValue : (ulong)r;
    }

    // ---------------------------------------------------------------------------------------------------------
    // output settings

    /// <summary>
    /// Port of MaxFileSizeForLevel (options/cf_options.cc) with MutableCFOptions::RefreshDerivedOptions: target file
    /// size base * multiplier^(level-1), counted from the base level with dynamic level bytes.
    /// </summary>
    public static ulong MaxFileSizeForLevel(ColumnFamilyOptions o, int level, int baseLevel)
    {
        int idx;
        if (!o.LevelCompactionDynamicLevelBytes || level < baseLevel || o.CompactionStyle != CompactionStyle.Level) idx = level;
        else idx = level - baseLevel;
        if (idx == 0 && o.CompactionStyle == CompactionStyle.Universal) return ulong.MaxValue;
        ulong size = o.TargetFileSizeBase;
        for (int i = 2; i <= idx; i++) size = MultiplyCheckOverflow(size, Math.Max(1, o.TargetFileSizeMultiplier));
        return size;
    }

    /// <summary>Port of GetCompressionType (db/compaction/compaction_picker.cc) with the version's non-empty levels.</summary>
    public static CompressionType GetCompressionType(VersionStorageInfo vstorage, ColumnFamilyOptions o, int level, int baseLevel)
    {
        CompressionType t = TableFactory.GetCompressionType(o, level, baseLevel, level >= vstorage.NumNonEmptyLevels - 1, enableCompression: true);
        return t == CompressionType.DisableCompressionOption ? CompressionType.None : t;
    }

    /// <summary>Port of LevelCompactionBuilder::GetPathId (single path when db_paths has one entry).</summary>
    public virtual uint GetPathId(int level) => 0;

    /// <summary>Creates a compaction, registers it and recomputes the scores (the tail of PickCompaction*).</summary>
    protected Compaction NewCompaction(VersionStorageInfo vstorage, CompactionInputFiles[] inputs, int outputLevel, ulong maxOutputFileSize,
        ulong maxCompactionBytes, uint pathId, List<FileMetaData> grandparents, CompactionReason reason, double score, bool l0FilesMightOverlap,
        bool isManual, bool isDeletion = false, bool trivialMoveForced = false, bool disallowTrivialMove = false)
    {
        return NewCompaction(vstorage, inputs, outputLevel, maxOutputFileSize, maxCompactionBytes, pathId, grandparents, reason, score,
            l0FilesMightOverlap, isManual, isDeletion, trivialMoveForced, disallowTrivialMove, enableCompression: true, Temperature.Unknown);
    }

    /// <summary>
    /// <see cref="NewCompaction(VersionStorageInfo, CompactionInputFiles[], int, ulong, ulong, uint, List{FileMetaData}, CompactionReason, double, bool, bool, bool, bool, bool)"/>
    /// with an explicit output compression switch (GetCompressionType's enable_compression) and output temperature.
    /// </summary>
    protected Compaction NewCompaction(VersionStorageInfo vstorage, CompactionInputFiles[] inputs, int outputLevel, ulong maxOutputFileSize,
        ulong maxCompactionBytes, uint pathId, List<FileMetaData> grandparents, CompactionReason reason, double score, bool l0FilesMightOverlap,
        bool isManual, bool isDeletion, bool trivialMoveForced, bool disallowTrivialMove, bool enableCompression, Temperature outputTemperature)
    {
        CompressionType compression = enableCompression
            ? GetCompressionType(vstorage, Options, outputLevel, Math.Max(1, vstorage.BaseLevel))
            : CompressionType.None;
        var c = new Compaction(vstorage, Options, inputs, outputLevel, maxOutputFileSize, maxCompactionBytes, pathId,
            compression, grandparents, reason, score, l0FilesMightOverlap, isManual)
        {
            IsDeletionCompaction = isDeletion,
            IsTrivialMoveForced = trivialMoveForced,
            DisallowTrivialMove = disallowTrivialMove,
            EnableCompression = enableCompression,
            OutputTemperature = outputTemperature,
        };
        // Compaction::Compaction: every compaction regardless of any compaction reason may respect the existing compact
        // cursor in the output level to split output files (leveled kRoundRobin), if it is inside the user-key range.
        if (Options.CompactionStyle == CompactionStyle.Level && Options.CompactionPri == CompactionPri.RoundRobin)
        {
            byte[]? cursor = vstorage.GetCompactCursor(outputLevel);
            if (cursor is { Length: > 0 })
            {
                ReadOnlySpan<byte> cursorUserKey = DbFormat.ExtractUserKey(cursor);
                if (VersionStorageInfo.CompareWithoutTimestamp(Icmp, cursorUserKey, c.SmallestUserKey) > 0
                    && VersionStorageInfo.CompareWithoutTimestamp(Icmp, cursorUserKey, c.LargestUserKey) <= 0)
                {
                    c.OutputSplitKey = cursor;
                }
            }
        }
        RegisterCompaction(c);
        // Creating a compaction influences the compaction score because the score takes running compactions into
        // account (by skipping files that are already being compacted).
        vstorage.ComputeCompactionScore();
        return c;
    }

    // ---------------------------------------------------------------------------------------------------------
    // manual compaction

    /// <summary>Output level placeholder: compact L0 to the base level (ColumnFamilyData::kCompactToBaseLevel).</summary>
    public const int CompactToBaseLevel = -2;
    /// <summary>Input level placeholder: all levels (ColumnFamilyData::kCompactAllLevels).</summary>
    public const int CompactAllLevels = -1;

    /// <summary>
    /// Port of CompactionPicker::PickCompactionForCompactRange (leveled and universal). <paramref name="begin"/> /
    /// <paramref name="end"/> are internal keys (null = unbounded). <paramref name="compactionEnd"/> receives the
    /// internal key where the next manual step must resume (null when the whole range was covered).
    /// </summary>
    public virtual Compaction? PickCompactionForCompactRange(VersionStorageInfo vstorage, int inputLevel, int outputLevel, uint maxSubcompactions,
        byte[]? begin, byte[]? end, out byte[]? compactionEnd, out bool manualConflict, ulong maxFileNumToIgnore, bool disallowTrivialMove)
    {
        compactionEnd = null;
        manualConflict = false;
        if (inputLevel == CompactAllLevels)
        {
            // Universal compaction with more than one level always compacts all the files together to the last level.
            int maxOutputLevel = vstorage.MaxOutputLevel(Options.AllowIngestBehind);
            int startLevel = 0;
            for (; startLevel <= maxOutputLevel && vstorage.NumLevelFiles(startLevel) == 0; startLevel++) { }
            if (startLevel > maxOutputLevel) return null;
            if (startLevel == 0 && IsLevel0CompactionInProgress)
            {
                manualConflict = true;
                return null;
            }
            var all = new CompactionInputFiles[maxOutputLevel + 1 - startLevel];
            for (int level = startLevel; level <= maxOutputLevel; level++)
            {
                var files = new List<FileMetaData>(vstorage.LevelFiles(level).ToArray());
                all[level - startLevel] = new CompactionInputFiles(level, files);
                if (AreFilesInCompaction(files))
                {
                    manualConflict = true;
                    return null;
                }
            }
            if (FilesRangeOverlapWithCompaction(vstorage, all, outputLevel))
            {
                manualConflict = true;
                return null;
            }
            return NewCompaction(vstorage, all, outputLevel, MaxFileSizeForLevel(Options, outputLevel, 1), long.MaxValue, 0, new List<FileMetaData>(),
                CompactionReason.ManualCompaction, -1, l0FilesMightOverlap: true, isManual: true, disallowTrivialMove: disallowTrivialMove);
        }

        var inputs = new CompactionInputFiles(inputLevel);
        bool coveringTheWholeRange = true;
        // All files are 'overlapping' in universal style compaction. We have to compact the entire range in one shot.
        if (Options.CompactionStyle == CompactionStyle.Universal)
        {
            begin = null;
            end = null;
        }
        vstorage.GetOverlappingInputs(inputLevel, begin, end, inputs.Files);
        if (inputs.IsEmpty) return null;
        if (inputLevel == 0 && IsLevel0CompactionInProgress)
        {
            // Only one level 0 compaction allowed.
            manualConflict = true;
            return null;
        }

        // Avoid compacting too much in one shot in case the range is large. But we cannot do this for level-0 since
        // level-0 files can overlap and we must not pick one file and drop another older file if the two files overlap.
        if (inputLevel > 0)
        {
            ulong limit = Options.MaxCompactionBytes;
            int hintIndex = -1;
            ulong inputLevelTotal = inputs[0].FileSize;
            byte[] smallest = inputs[0].Smallest;
            var files = new List<FileMetaData>();
            for (int i = 1; i < inputs.Count; ++i)
            {
                byte[] largest = inputs[i].Largest;
                ulong outputLevelTotal = 0;
                if (outputLevel >= 0 && outputLevel < vstorage.NumNonEmptyLevels)
                {
                    files.Clear();
                    vstorage.GetOverlappingInputsRangeBinarySearch(outputLevel, smallest, largest, files, hintIndex, out hintIndex, false);
                    foreach (FileMetaData f in files) outputLevelTotal += f.FileSize;
                }
                inputLevelTotal += inputs[i].FileSize;
                if (inputLevelTotal + outputLevelTotal > limit)
                {
                    coveringTheWholeRange = false;
                    inputs.Files.RemoveRange(i, inputs.Count - i);
                    break;
                }
            }
        }

        // For BOTTOM LEVEL compaction only, use max_file_num_to_ignore to filter out files that are created during the
        // current compaction.
        if (maxFileNumToIgnore != ulong.MaxValue)
        {
            var shrunk = new List<FileMetaData>();
            int skipInputIndex = inputs.Count;
            for (int i = 0; i < inputs.Count; ++i)
            {
                if (inputs[i].FileNumber < maxFileNumToIgnore) shrunk.Add(inputs[i]);
                else if (shrunk.Count > 0)
                {
                    skipInputIndex = i;
                    break;
                }
            }
            if (shrunk.Count == 0) return null;
            List<FileMetaData> original = inputs.Files;
            for (int i = skipInputIndex + 1; i < original.Count; ++i)
            {
                if (original[i].FileNumber < maxFileNumToIgnore) coveringTheWholeRange = false;
            }
            if (original.Count != shrunk.Count) inputs.Files = shrunk;
        }

        if (!ExpandInputsToCleanCut(vstorage, inputs, out byte[]? nextSmallest))
        {
            // Manual compaction is multi-threaded, so ExpandInputsToCleanCut may fail; handled higher up.
            manualConflict = true;
            return null;
        }
        compactionEnd = coveringTheWholeRange || nextSmallest is null ? null : nextSmallest;

        if (outputLevel == CompactToBaseLevel) outputLevel = vstorage.BaseLevel;
        var outputLevelInputs = new CompactionInputFiles(outputLevel);
        if (inputLevel != outputLevel)
        {
            int parentIndex = -1;
            if (!SetupOtherInputs(vstorage, inputs, outputLevelInputs, ref parentIndex, -1, false))
            {
                manualConflict = true;
                return null;
            }
        }
        var compactionInputs = outputLevelInputs.IsEmpty ? new[] { inputs } : new[] { inputs, outputLevelInputs };
        foreach (CompactionInputFiles ci in compactionInputs)
        {
            if (AreFilesInCompaction(ci.Files))
            {
                manualConflict = true;
                return null;
            }
        }
        // Two non-exclusive manual compactions could run at the same time producing overlapping outputs in the same level.
        if (FilesRangeOverlapWithCompaction(vstorage, compactionInputs, outputLevel))
        {
            manualConflict = true;
            return null;
        }
        var grandparents = new List<FileMetaData>();
        GetGrandparents(vstorage, inputs, outputLevelInputs, grandparents);
        return NewCompaction(vstorage, compactionInputs, outputLevel, MaxFileSizeForLevel(Options, outputLevel, vstorage.BaseLevel),
            Options.MaxCompactionBytes, 0, grandparents, CompactionReason.ManualCompaction, -1, l0FilesMightOverlap: true, isManual: true,
            disallowTrivialMove: disallowTrivialMove);
    }
}
