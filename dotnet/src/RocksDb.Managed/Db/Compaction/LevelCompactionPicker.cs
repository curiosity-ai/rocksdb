namespace RocksDb.Managed.Db;

/// <summary>
/// Leveled compaction picker. Port of LevelCompactionPicker + LevelCompactionBuilder
/// (db/compaction/compaction_picker_level.cc): score-based level selection, file choice by compaction priority, L0
/// trivial moves, non-L0 trivial move extension, size- and cost-based intra-L0 compaction, files marked for
/// compaction, TTL and periodic compaction, round-robin expansion. REQUIRES: DB mutex.
/// </summary>
internal sealed class LevelCompactionPicker : CompactionPicker
{
    private const int MinFilesForIntraL0Compaction = 4;

    public LevelCompactionPicker(ColumnFamilyOptions options, InternalKeyComparator icmp) : base(options, icmp) { }

    /// <summary>Round-robin cursors per level (compact_cursor_), carried across versions in memory.</summary>
    public byte[]?[] CompactCursors { get; } = new byte[]?[64];

    public override bool NeedsCompaction(VersionStorageInfo vstorage)
    {
        if (vstorage.ExpiredTtlFiles.Count > 0) return true;
        if (vstorage.FilesMarkedForPeriodicCompaction.Count > 0) return true;
        if (vstorage.FilesMarkedForCompaction.Count > 0) return true;
        for (int i = 0; i <= vstorage.MaxInputLevel && i < vstorage.NumLevels - 1; i++)
        {
            if (vstorage.CompactionScore(i) >= 1) return true;
        }
        return false;
    }

    public override Compaction? PickCompaction(VersionStorageInfo vstorage, IReadOnlyList<ulong> snapshots)
    {
        if (Options.CompactionPri == CompactionPri.RoundRobin) vstorage.UpdateFilesByCompactionPri(CompactCursors);
        return new Builder(this, vstorage).PickCompaction();
    }

    private enum CompactToNextLevel
    {
        No,
        Yes,
        SkipLastLevel,
    }

    /// <summary>Port of LevelCompactionBuilder.</summary>
    private sealed class Builder
    {
        private readonly LevelCompactionPicker _picker;
        private readonly VersionStorageInfo _vstorage;
        private readonly ColumnFamilyOptions _o;
        private int _startLevel = -1;
        private int _outputLevel = -1;
        private int _parentIndex = -1;
        private int _baseIndex = -1;
        private double _startLevelScore;
        private bool _isL0TrivialMove;
        private CompactionInputFiles _startLevelInputs = new(0);
        private readonly List<CompactionInputFiles> _compactionInputs = new();
        private CompactionInputFiles _outputLevelInputs = new(0);
        private readonly List<FileMetaData> _grandparents = new();
        private CompactionReason _reason = CompactionReason.Unknown;

        public Builder(LevelCompactionPicker picker, VersionStorageInfo vstorage)
        {
            _picker = picker;
            _vstorage = vstorage;
            _o = picker.Options;
        }

        public Compaction? PickCompaction()
        {
            // Pick up the first file to start compaction. It may have been extended to a clean cut.
            SetupInitialFiles();
            if (_startLevelInputs.IsEmpty) return null;
            // If it is a L0 -> base level compaction, we need to set up other L0 files if needed.
            if (!SetupOtherL0FilesIfNeeded()) return null;
            // Pick files in the output level and expand more files in the start level if needed.
            if (!SetupOtherInputsIfNeeded()) return null;
            return GetCompaction();
        }

        private void PickFileToCompact(List<(int Level, FileMetaData File)> levelFiles, CompactToNextLevel compactToNextLevel)
        {
            foreach ((int level, FileMetaData file) in levelFiles)
            {
                if (file.BeingCompacted) continue;
                _startLevel = level;
                if ((compactToNextLevel == CompactToNextLevel.SkipLastLevel && _startLevel == _vstorage.NumNonEmptyLevels - 1)
                    || (_startLevel == 0 && _picker.IsLevel0CompactionInProgress))
                {
                    continue;
                }
                // Compact to the next level only if the file is not in the last level and compact_to_next_level is kYes or kSkipLastLevel.
                if (compactToNextLevel != CompactToNextLevel.No && _startLevel < _vstorage.NumNonEmptyLevels - 1)
                {
                    _outputLevel = _startLevel == 0 ? _vstorage.BaseLevel : _startLevel + 1;
                }
                else
                {
                    _outputLevel = _startLevel;
                }
                _startLevelInputs = new CompactionInputFiles(_startLevel, new List<FileMetaData> { file });
                if (_picker.ExpandInputsToCleanCut(_vstorage, _startLevelInputs)) return;
            }
            _startLevelInputs.Clear();
        }

        private void SetupInitialFiles()
        {
            // Find the compactions by size on all levels.
            bool skippedL0ToBase = false;
            for (int i = 0; i < _picker.NumberLevels - 1 && i < _vstorage.NumLevels - 1; i++)
            {
                _startLevelScore = _vstorage.CompactionScore(i);
                _startLevel = _vstorage.CompactionScoreLevel(i);
                if (_startLevelScore >= 1)
                {
                    if (skippedL0ToBase && _startLevel == _vstorage.BaseLevel)
                    {
                        // If L0->base_level compaction is pending, don't schedule further compaction from base level.
                        // Otherwise L0->base_level compaction may starve.
                        continue;
                    }
                    _outputLevel = _startLevel == 0 ? _vstorage.BaseLevel : _startLevel + 1;
                    if (PickFileToCompact())
                    {
                        // found the compaction!
                        _reason = _startLevel == 0 ? CompactionReason.LevelL0FilesNum : CompactionReason.LevelMaxLevelSize;
                        break;
                    }
                    // didn't find the compaction, clear the inputs
                    _startLevelInputs.Clear();
                    if (_startLevel == 0)
                    {
                        skippedL0ToBase = true;
                        // L0->base_level may be blocked due to ongoing L0->base_level compactions. It may also be blocked by
                        // an ongoing compaction from base_level downwards. In these cases, to reduce L0 file count and thus
                        // reduce likelihood of write stalls, we can attempt compacting a span of files within L0.
                        if (PickIntraL0Compaction())
                        {
                            _outputLevel = 0;
                            _reason = CompactionReason.LevelL0FilesNum;
                            break;
                        }
                    }
                }
                else
                {
                    // Compaction scores are sorted in descending order, no further scores will be >= 1.
                    break;
                }
            }
            if (!_startLevelInputs.IsEmpty) return;

            // if we didn't find a compaction, check if there are any files marked for compaction
            _parentIndex = _baseIndex = -1;
            var marked = new CompactionInputFiles(0);
            if (_picker.PickFilesMarkedForCompaction(_vstorage, out int sl, out int ol, marked))
            {
                _startLevel = sl;
                _outputLevel = ol;
                _startLevelInputs = marked;
                _reason = CompactionReason.FilesMarkedForCompaction;
                return;
            }

            // TTL Compaction
            if (_o.CompactionPri == CompactionPri.RoundRobin && _vstorage.ExpiredTtlFiles.Count > 0)
            {
                _startLevel = _vstorage.ExpiredTtlFiles[0].Level;
                if (_startLevel > 0)
                {
                    _outputLevel = _startLevel + 1;
                    if (PickFileToCompact())
                    {
                        _reason = CompactionReason.RoundRobinTtl;
                        return;
                    }
                }
            }
            PickFileToCompact(_vstorage.ExpiredTtlFiles, CompactToNextLevel.SkipLastLevel);
            if (!_startLevelInputs.IsEmpty)
            {
                _reason = CompactionReason.Ttl;
                return;
            }

            // Periodic Compaction
            PickFileToCompact(_vstorage.FilesMarkedForPeriodicCompaction,
                _o.LevelCompactionDynamicLevelBytes ? CompactToNextLevel.Yes : CompactToNextLevel.No);
            if (!_startLevelInputs.IsEmpty) _reason = CompactionReason.PeriodicCompaction;
        }

        private bool SetupOtherL0FilesIfNeeded()
        {
            if (_startLevel == 0 && _outputLevel != 0 && !_isL0TrivialMove)
            {
                return _picker.GetOverlappingL0Files(_vstorage, _startLevelInputs, _outputLevel, ref _parentIndex);
            }
            return true;
        }

        /// <summary>Port of SetupOtherFilesWithRoundRobinExpansion.</summary>
        private void SetupOtherFilesWithRoundRobinExpansion()
        {
            if (_startLevelInputs.Count == 0) return;
            ulong startLvlBytesNoCompacting = 0;
            ReadOnlySpan<FileMetaData> levelFiles = _vstorage.LevelFiles(_startLevel);
            foreach (FileMetaData f in levelFiles)
            {
                if (!f.BeingCompacted) startLvlBytesNoCompacting += f.FileSize;
            }
            ulong startLvlMaxBytesToCompact = 0;
            if (startLvlBytesNoCompacting > _vstorage.MaxBytesForLevel(_startLevel))
                startLvlMaxBytesToCompact = startLvlBytesNoCompacting - _vstorage.MaxBytesForLevel(_startLevel);
            int startIndex = _vstorage.FilesByCompactionPri(_startLevel)[0];
            _picker.GetRange(_startLevelInputs, out byte[] smallest, out byte[] largest);
            var outputLevelInputs = new CompactionInputFiles(_outputLevel);
            _vstorage.GetOverlappingInputs(_outputLevel, smallest, largest, outputLevelInputs.Files);
            if (outputLevelInputs.IsEmpty && TryExtendNonL0TrivialMove(startIndex, onlyExpandRight: true)) return;
            if (_startLevelInputs[0].FileSize >= startLvlMaxBytesToCompact) return;
            CompactionInputFiles tmp = _startLevelInputs.Copy();
            int nextIndex = startIndex + 1;
            FileMetaData lastFile = _startLevelInputs.Files[^1];
            for (int j = nextIndex; j < levelFiles.Length; j++)
            {
                if (ReferenceEquals(levelFiles[j], lastFile))
                {
                    nextIndex = j + 1;
                    break;
                }
            }
            for (int i = nextIndex; i < levelFiles.Length; i++)
            {
                FileMetaData f = levelFiles[i];
                if (f.BeingCompacted) return;
                tmp.Files.Add(f);
                if (!_picker.ExpandInputsToCleanCut(_vstorage, tmp) || _picker.FilesRangeOverlapWithCompaction([tmp], _outputLevel)) return;
                ulong currBytes = CompactionPicker.TotalFileSize(tmp.Files);
                _picker.GetRange(tmp, out smallest, out largest);
                _vstorage.GetOverlappingInputs(_outputLevel, smallest, largest, outputLevelInputs.Files);
                if (!outputLevelInputs.IsEmpty && !_picker.ExpandInputsToCleanCut(_vstorage, outputLevelInputs)) return;
                ulong startLvlCurrBytes = currBytes;
                currBytes += CompactionPicker.TotalFileSize(outputLevelInputs.Files);
                if (currBytes > _o.MaxCompactionBytes) return;
                _startLevelInputs.Files = new List<FileMetaData>(tmp.Files);
                if (startLvlCurrBytes > startLvlMaxBytesToCompact) return;
            }
        }

        private bool SetupOtherInputsIfNeeded()
        {
            // Setup input files from output level. For output to L0, we only compact spans of files that do not
            // interact with any pending compactions, so don't need to consider other levels.
            if (_outputLevel != 0)
            {
                _outputLevelInputs = new CompactionInputFiles(_outputLevel);
                bool roundRobinExpanding = _o.CompactionPri == CompactionPri.RoundRobin && _reason == CompactionReason.LevelMaxLevelSize;
                if (roundRobinExpanding) SetupOtherFilesWithRoundRobinExpansion();
                if (!_isL0TrivialMove
                    && !_picker.SetupOtherInputs(_vstorage, _startLevelInputs, _outputLevelInputs, ref _parentIndex, _baseIndex, roundRobinExpanding))
                {
                    return false;
                }
                _compactionInputs.Add(_startLevelInputs);
                if (!_outputLevelInputs.IsEmpty) _compactionInputs.Add(_outputLevelInputs);
                // Disallow compactions whose output could overlap a running compaction's output in the same level.
                if (_picker.FilesRangeOverlapWithCompaction(_compactionInputs, _outputLevel)) return false;
                if (!_isL0TrivialMove) _picker.GetGrandparents(_vstorage, _startLevelInputs, _outputLevelInputs, _grandparents);
            }
            else
            {
                _compactionInputs.Add(_startLevelInputs);
            }
            return true;
        }

        private Compaction GetCompaction()
        {
            bool l0FilesMightOverlap = _startLevel == 0 && !_isL0TrivialMove && (_compactionInputs.Count > 1 || _compactionInputs[0].Count > 1);
            Compaction c = _picker.NewCompaction(_vstorage, _compactionInputs.ToArray(), _outputLevel,
                MaxFileSizeForLevel(_o, _outputLevel, _vstorage.BaseLevel), _o.MaxCompactionBytes, _picker.GetPathId(_outputLevel),
                _grandparents, _reason, _startLevelScore, l0FilesMightOverlap, isManual: false);
            if (_o.CompactionPri == CompactionPri.RoundRobin && _startLevel > 0 && _reason == CompactionReason.LevelMaxLevelSize)
            {
                // Advance the cursor to the successor of the last input file (the next compaction starts there).
                ReadOnlySpan<FileMetaData> files = _vstorage.LevelFiles(_startLevel);
                FileMetaData last = _startLevelInputs.Files[^1];
                byte[]? next = null;
                for (int i = 0; i < files.Length; i++)
                {
                    if (ReferenceEquals(files[i], last))
                    {
                        next = i + 1 < files.Length ? files[i + 1].Smallest : null;
                        break;
                    }
                }
                if (_startLevel < _picker.CompactCursors.Length) _picker.CompactCursors[_startLevel] = next;
            }
            return c;
        }

        /// <summary>Port of TryPickL0TrivialMove.</summary>
        private bool TryPickL0TrivialMove()
        {
            if (_vstorage.BaseLevel <= 0) return false;
            if (_startLevel == 0 && _o.CompressionPerLevel.Count == 0 && _vstorage.NumLevelFiles(_outputLevel) > 0)
            {
                // Try to pick trivial move from L0 to L1. We start from the oldest file. We keep expanding to newer files
                // if it would form a trivial move.
                ReadOnlySpan<FileMetaData> levelFiles = _vstorage.LevelFiles(_startLevel);
                byte[] mySmallest = [], myLargest = [];
                var outputLevelInputs = new List<FileMetaData>();
                for (int i = levelFiles.Length - 1; i >= 0; i--)
                {
                    FileMetaData file = levelFiles[i];
                    if (i == levelFiles.Length - 1)
                    {
                        mySmallest = file.Smallest;
                        myLargest = file.Largest;
                    }
                    else if (_picker.Icmp.Compare(file.Largest, mySmallest) < 0)
                    {
                        mySmallest = file.Smallest;
                    }
                    else if (_picker.Icmp.Compare(file.Smallest, myLargest) > 0)
                    {
                        myLargest = file.Largest;
                    }
                    else
                    {
                        break;
                    }
                    _vstorage.GetOverlappingInputs(_outputLevel, mySmallest, myLargest, outputLevelInputs);
                    if (outputLevelInputs.Count == 0 && !file.BeingCompacted) _startLevelInputs.Files.Add(file);
                    else break;
                }
            }
            if (!_startLevelInputs.IsEmpty)
            {
                // Sort files by key range.
                InternalKeyComparator icmp = _picker.Icmp;
                _startLevelInputs.Files.Sort((a, b) => icmp.Compare(a.Smallest, b.Smallest));
                _isL0TrivialMove = true;
                return true;
            }
            return false;
        }

        /// <summary>Port of TryExtendNonL0TrivialMove.</summary>
        private bool TryExtendNonL0TrivialMove(int startIndex, bool onlyExpandRight)
        {
            if (_startLevelInputs.Count == 1 && _o.CompressionPerLevel.Count == 0)
            {
                // Only file of `index`, and it is likely a trivial move. Try to expand if it is still a trivial move, but
                // not beyond max_compaction_bytes or 4 files.
                ReadOnlySpan<FileMetaData> levelFiles = _vstorage.LevelFiles(_startLevel);
                const int MaxMultiTrivialMove = 4;
                FileMetaData initialFile = _startLevelInputs[0];
                ulong totalSize = initialFile.FileSize;
                var outputLevelInputs = new List<FileMetaData>();
                InternalKeyComparator icmp = _picker.Icmp;
                for (int i = startIndex + 1; i < levelFiles.Length && _startLevelInputs.Count < MaxMultiTrivialMove; i++)
                {
                    FileMetaData nextFile = levelFiles[i];
                    if (nextFile.BeingCompacted) break;
                    _vstorage.GetOverlappingInputs(_outputLevel, initialFile.Smallest, nextFile.Largest, outputLevelInputs);
                    if (outputLevelInputs.Count > 0) break;
                    if (i < levelFiles.Length - 1
                        && icmp.CompareUserKey(DbFormat.ExtractUserKey(nextFile.Largest), DbFormat.ExtractUserKey(levelFiles[i + 1].Smallest)) == 0)
                    {
                        break; // Not a clean cut after adding the next file.
                    }
                    totalSize += nextFile.FileSize;
                    if (totalSize > _o.MaxCompactionBytes) break;
                    _startLevelInputs.Files.Add(nextFile);
                }
                if (!onlyExpandRight)
                {
                    for (int i = startIndex - 1; i >= 0 && _startLevelInputs.Count < MaxMultiTrivialMove; i--)
                    {
                        FileMetaData nextFile = levelFiles[i];
                        if (nextFile.BeingCompacted) break;
                        _vstorage.GetOverlappingInputs(_outputLevel, nextFile.Smallest, initialFile.Largest, outputLevelInputs);
                        if (outputLevelInputs.Count > 0) break;
                        if (i > 0 && icmp.CompareUserKey(DbFormat.ExtractUserKey(nextFile.Smallest), DbFormat.ExtractUserKey(levelFiles[i - 1].Largest)) == 0)
                        {
                            break;
                        }
                        totalSize += nextFile.FileSize;
                        if (totalSize > _o.MaxCompactionBytes) break;
                        // keep `files` sorted in increasing order by key range
                        _startLevelInputs.Files.Insert(0, nextFile);
                    }
                }
                return _startLevelInputs.Count > 1;
            }
            return false;
        }

        /// <summary>Port of LevelCompactionBuilder::PickFileToCompact().</summary>
        private bool PickFileToCompact()
        {
            // Level 0 files are overlapping. So we cannot pick more than one concurrent compactions at this level.
            if (_startLevel == 0 && _picker.IsLevel0CompactionInProgress) return PickSizeBasedIntraL0Compaction();

            _startLevelInputs = new CompactionInputFiles(_startLevel);
            if (TryPickL0TrivialMove()) return true;
            if (_startLevel == 0 && PickSizeBasedIntraL0Compaction()) return true;

            ReadOnlySpan<FileMetaData> levelFiles = _vstorage.LevelFiles(_startLevel);
            // Pick the file with the highest score in this level that is not already being compacted.
            int[] fileScores = _vstorage.FilesByCompactionPri(_startLevel);
            bool roundRobin = _o.CompactionPri == CompactionPri.RoundRobin;
            int cmpIdx;
            for (cmpIdx = _vstorage.NextCompactionIndex(_startLevel); cmpIdx < fileScores.Length; cmpIdx++)
            {
                int index = fileScores[cmpIdx];
                FileMetaData f = levelFiles[index];
                // do not pick a file to compact if it is being compacted from n-1 level.
                if (f.BeingCompacted)
                {
                    if (roundRobin) return false;
                    continue;
                }
                _startLevelInputs.Files.Add(f);
                if (!_picker.ExpandInputsToCleanCut(_vstorage, _startLevelInputs)
                    || _picker.FilesRangeOverlapWithCompaction([_startLevelInputs], _outputLevel))
                {
                    // A locked (pending compaction) input-level file was pulled in due to user-key overlap.
                    _startLevelInputs.Clear();
                    if (roundRobin) return false;
                    continue;
                }
                // Now that input level is fully expanded, we check whether any output files are locked due to pending compaction.
                _picker.GetRange(_startLevelInputs, out byte[] smallest, out byte[] largest);
                var outputLevelInputs = new CompactionInputFiles(_outputLevel);
                _vstorage.GetOverlappingInputs(_outputLevel, smallest, largest, outputLevelInputs.Files);
                if (outputLevelInputs.IsEmpty)
                {
                    if (_startLevel > 0 && TryExtendNonL0TrivialMove(index, roundRobin)) break;
                }
                else if (!_picker.ExpandInputsToCleanCut(_vstorage, outputLevelInputs))
                {
                    _startLevelInputs.Clear();
                    if (roundRobin) return false;
                    continue;
                }
                _baseIndex = index;
                break;
            }
            // store where to start the iteration in the next call to PickCompaction
            if (!roundRobin) _vstorage.SetNextCompactionIndex(_startLevel, cmpIdx);
            return _startLevelInputs.Count > 0;
        }

        /// <summary>Port of PickIntraL0Compaction.</summary>
        private bool PickIntraL0Compaction()
        {
            _startLevelInputs = new CompactionInputFiles(0);
            ReadOnlySpan<FileMetaData> levelFiles = _vstorage.LevelFiles(0);
            if (levelFiles.Length < _o.Level0FileNumCompactionTrigger + 2 || levelFiles[0].BeingCompacted)
            {
                // If L0 isn't accumulating much files beyond the regular trigger, don't resort to L0->L0 compaction yet.
                return false;
            }
            return PickCostBasedIntraL0Compaction(levelFiles, MinFilesForIntraL0Compaction, ulong.MaxValue, _o.MaxCompactionBytes, _startLevelInputs);
        }

        /// <summary>Port of PickSizeBasedIntraL0Compaction.</summary>
        private bool PickSizeBasedIntraL0Compaction()
        {
            int baseLevel = _vstorage.BaseLevel;
            if (baseLevel <= 0) return false;
            ReadOnlySpan<FileMetaData> l0Files = _vstorage.LevelFiles(0);
            int minNumFile = Math.Max(2, _o.Level0FileNumCompactionTrigger);
            if (l0Files.Length < minNumFile) return false;
            ulong l0Size = 0;
            foreach (FileMetaData f in l0Files) l0Size += VersionStorageInfo.CompensatedFileSize(f);
            // Avoid L0->Lbase compactions that are inefficient for write-amp.
            double multiplier = Math.Max(10.0, _o.MaxBytesForLevelMultiplier) * 2;
            ulong minLbaseSize = MultiplyCheckOverflow(l0Size, multiplier);
            ulong lbaseSize = 0;
            foreach (FileMetaData f in _vstorage.LevelFiles(baseLevel))
            {
                lbaseSize += f.FileSize;
                if (lbaseSize > minLbaseSize) break;
            }
            if (lbaseSize <= minLbaseSize) return false;
            var inputs = new CompactionInputFiles(0);
            foreach (FileMetaData f in l0Files)
            {
                if (f.BeingCompacted) break;
                inputs.Files.Add(f);
            }
            if (inputs.Count < minNumFile) return false;
            _startLevelInputs = inputs;
            _outputLevel = 0;
            return true;
        }
    }
}
