using System.Globalization;
using RocksDb.Managed.Compression;

namespace RocksDb.Managed.Db;

/// <summary>
/// CompactFiles support of the compaction picker. Port of CompactionPicker::SanitizeCompactionInputFilesForAllLevels,
/// SanitizeAndConvertCompactionInputFiles, GetCompactionInputsFromFileNumbers and PickCompactionForCompactFiles
/// (db/compaction/compaction_picker.cc). REQUIRES: DB mutex.
/// </summary>
internal abstract partial class CompactionPicker
{
    /// <summary>MaxOutputLevel() of the compaction algorithm: 0 for FIFO, else the last level.</summary>
    public int MaxOutputLevel => Options.CompactionStyle == CompactionStyle.Fifo ? 0 : NumberLevels - 1;

    private static RocksDbException Aborted(string message) => new(StatusCode.Aborted, "Operation aborted: " + message);

    private static string TableName(ulong number) => FileNames.MakeTableFileName(number);

    /// <summary>
    /// Port of CompactionPicker::SanitizeAndConvertCompactionInputFiles: validates the output level, adds the files a
    /// valid compaction of <paramref name="inputFiles"/> also needs (SanitizeCompactionInputFilesForAllLevels), checks that
    /// every file exists, is not being compacted and is not below the output level, and returns the inputs per level
    /// (first to last non-empty level). Throws InvalidArgument / Aborted like the upstream statuses.
    /// </summary>
    public List<CompactionInputFiles> SanitizeAndConvertCompactionInputFiles(HashSet<ulong> inputFiles, int outputLevel, VersionStorageInfo vstorage,
        string columnFamilyName)
    {
        int numLevels = vstorage.NumLevels;
        if (outputLevel >= numLevels)
        {
            ThrowHelper.InvalidArgument("Output level for column family " + columnFamilyName + " must between [0, " +
                                        (numLevels - 1).ToString(CultureInfo.InvariantCulture) + "].");
        }
        if (outputLevel > MaxOutputLevel)
        {
            ThrowHelper.InvalidArgument("Exceed the maximum output level defined by the current compaction algorithm --- " +
                                        MaxOutputLevel.ToString(CultureInfo.InvariantCulture));
        }
        if (outputLevel < 0) ThrowHelper.InvalidArgument("Output level cannot be negative.");
        if (inputFiles.Count == 0) ThrowHelper.InvalidArgument("A compaction must contain at least one file.");

        SanitizeCompactionInputFilesForAllLevels(inputFiles, vstorage, outputLevel);

        // for all input files, check whether the file number matches any currently-existing files.
        foreach (ulong fileNum in inputFiles)
        {
            int inputFileLevel = -1;
            for (int level = 0; level < numLevels && inputFileLevel < 0; level++)
            {
                foreach (FileMetaData f in vstorage.LevelFiles(level))
                {
                    if (f.FileNumber != fileNum) continue;
                    if (f.BeingCompacted) throw Aborted("Specified compaction input file " + TableName(fileNum) + " is already being compacted.");
                    inputFileLevel = level;
                    break;
                }
            }
            if (inputFileLevel < 0)
            {
                ThrowHelper.InvalidArgument("Specified compaction input file " + TableName(fileNum) + " does not exist in column family " +
                                            columnFamilyName + ".");
            }
            if (inputFileLevel > outputLevel)
            {
                ThrowHelper.InvalidArgument("Cannot compact file to up level, input file: " + TableName(fileNum) + " level " +
                                            inputFileLevel.ToString(CultureInfo.InvariantCulture) + " > output level " +
                                            outputLevel.ToString(CultureInfo.InvariantCulture));
            }
        }

        List<CompactionInputFiles> converted = GetCompactionInputsFromFileNumbers(inputFiles, vstorage);
        if (outputLevel != 0 && FilesRangeOverlapWithCompaction(vstorage, converted, outputLevel))
        {
            throw Aborted("A running compaction is writing to the same output level(s) in an overlapping key range");
        }
        return converted;
    }

    /// <summary>Port of CompactionPicker::GetCompactionInputsFromFileNumbers.</summary>
    private static List<CompactionInputFiles> GetCompactionInputsFromFileNumbers(HashSet<ulong> inputSet, VersionStorageInfo vstorage)
    {
        if (inputSet.Count == 0) ThrowHelper.InvalidArgument("Compaction must include at least one file.");
        var remaining = new HashSet<ulong>(inputSet);
        var matched = new CompactionInputFiles[vstorage.NumLevels];
        int firstNonEmptyLevel = -1, lastNonEmptyLevel = -1;
        for (int level = 0; level < vstorage.NumLevels; level++)
        {
            matched[level] = new CompactionInputFiles(level);
            foreach (FileMetaData f in vstorage.LevelFiles(level))
            {
                if (!remaining.Remove(f.FileNumber)) continue;
                matched[level].Files.Add(f);
                lastNonEmptyLevel = level;
                if (firstNonEmptyLevel == -1) firstNonEmptyLevel = level;
            }
        }
        if (remaining.Count > 0)
        {
            var message = new System.Text.StringBuilder("Cannot find matched SST files for the following file numbers:");
            foreach (ulong fn in remaining) message.Append(' ').Append(fn.ToString(CultureInfo.InvariantCulture));
            ThrowHelper.InvalidArgument(message.ToString());
        }
        var result = new List<CompactionInputFiles>(lastNonEmptyLevel - firstNonEmptyLevel + 1);
        for (int level = firstNonEmptyLevel; level <= lastNonEmptyLevel; level++) result.Add(matched[level]);
        return result;
    }

    /// <summary>Port of HaveOverlappingKeyRanges (user keys, ignoring timestamps).</summary>
    private bool HaveOverlappingKeyRanges(ReadOnlySpan<byte> aSmallest, ReadOnlySpan<byte> aLargest, FileMetaData b)
    {
        ReadOnlySpan<byte> bSmallest = DbFormat.ExtractUserKey(b.Smallest), bLargest = DbFormat.ExtractUserKey(b.Largest);
        InternalKeyComparator icmp = Icmp;
        if (VersionStorageInfo.CompareWithoutTimestamp(icmp, aSmallest, bSmallest) >= 0)
        {
            // b.smallestkey <= a.smallestkey <= b.largestkey
            if (VersionStorageInfo.CompareWithoutTimestamp(icmp, aSmallest, bLargest) <= 0) return true;
        }
        else if (VersionStorageInfo.CompareWithoutTimestamp(icmp, aLargest, bSmallest) >= 0)
        {
            return true; // a.smallestkey < b.smallestkey <= a.largestkey
        }
        if (VersionStorageInfo.CompareWithoutTimestamp(icmp, aLargest, bLargest) <= 0)
        {
            // b.smallestkey <= a.largestkey <= b.largestkey
            if (VersionStorageInfo.CompareWithoutTimestamp(icmp, aLargest, bSmallest) >= 0) return true;
        }
        else if (VersionStorageInfo.CompareWithoutTimestamp(icmp, aSmallest, bLargest) <= 0)
        {
            return true; // a.smallestkey <= b.largestkey < a.largestkey
        }
        return false;
    }

    /// <summary>
    /// Port of CompactionPicker::SanitizeCompactionInputFilesForAllLevels: for each level up to the output level, includes
    /// every file between the first and the last input file (extended over files sharing a boundary user key on sorted
    /// levels; on L0 with output level &gt; 0 through the oldest file), grows the key range, and pulls in the files of
    /// the following levels up to the output level that overlap it. Aborted if a needed file is being compacted.
    /// </summary>
    private void SanitizeCompactionInputFilesForAllLevels(HashSet<ulong> inputFiles, VersionStorageInfo vstorage, int outputLevel)
    {
        InternalKeyComparator icmp = Icmp;
        // the smallest and largest key of the current compaction input
        byte[] smallestKey = [], largestKey = [];
        bool isFirst = false;
        const int NotFound = -1;
        for (int l = 0; l <= outputLevel; l++)
        {
            ReadOnlySpan<FileMetaData> currentFiles = vstorage.LevelFiles(l);
            int firstIncluded = currentFiles.Length;
            int lastIncluded = NotFound;
            // identify the first and the last compaction input files in the current level.
            for (int f = 0; f < currentFiles.Length; f++)
            {
                if (!inputFiles.Contains(currentFiles[f].FileNumber)) continue;
                firstIncluded = Math.Min(firstIncluded, f);
                lastIncluded = Math.Max(lastIncluded, f);
                if (!isFirst)
                {
                    smallestKey = DbFormat.ExtractUserKey(currentFiles[f].Smallest).ToArray();
                    largestKey = DbFormat.ExtractUserKey(currentFiles[f].Largest).ToArray();
                    isFirst = true;
                }
            }
            if (lastIncluded == NotFound) continue;
            if (l != 0)
            {
                // expand the compaction input of the current level if it has overlapping key-range with other
                // non-compaction input files in the same level.
                while (firstIncluded > 0)
                {
                    if (VersionStorageInfo.CompareWithoutTimestamp(icmp, DbFormat.ExtractUserKey(currentFiles[firstIncluded - 1].Largest),
                            DbFormat.ExtractUserKey(currentFiles[firstIncluded].Smallest)) < 0)
                    {
                        break;
                    }
                    firstIncluded--;
                }
                while (lastIncluded < currentFiles.Length - 1)
                {
                    if (VersionStorageInfo.CompareWithoutTimestamp(icmp, DbFormat.ExtractUserKey(currentFiles[lastIncluded + 1].Smallest),
                            DbFormat.ExtractUserKey(currentFiles[lastIncluded].Largest)) > 0)
                    {
                        break;
                    }
                    lastIncluded++;
                }
            }
            else if (outputLevel > 0)
            {
                lastIncluded = currentFiles.Length - 1;
            }

            // include all files between the first and the last compaction input files.
            for (int f = firstIncluded; f <= lastIncluded; f++)
            {
                if (currentFiles[f].BeingCompacted)
                {
                    throw Aborted("Necessary compaction input file " + TableName(currentFiles[f].FileNumber) + " is currently being compacted.");
                }
                inputFiles.Add(currentFiles[f].FileNumber);
            }

            // update smallest and largest key
            if (l == 0)
            {
                for (int f = firstIncluded; f <= lastIncluded; f++)
                {
                    ReadOnlySpan<byte> s = DbFormat.ExtractUserKey(currentFiles[f].Smallest), g = DbFormat.ExtractUserKey(currentFiles[f].Largest);
                    if (VersionStorageInfo.CompareWithoutTimestamp(icmp, smallestKey, s) > 0) smallestKey = s.ToArray();
                    if (VersionStorageInfo.CompareWithoutTimestamp(icmp, largestKey, g) < 0) largestKey = g.ToArray();
                }
            }
            else
            {
                ReadOnlySpan<byte> s = DbFormat.ExtractUserKey(currentFiles[firstIncluded].Smallest);
                ReadOnlySpan<byte> g = DbFormat.ExtractUserKey(currentFiles[lastIncluded].Largest);
                if (VersionStorageInfo.CompareWithoutTimestamp(icmp, smallestKey, s) > 0) smallestKey = s.ToArray();
                if (VersionStorageInfo.CompareWithoutTimestamp(icmp, largestKey, g) < 0) largestKey = g.ToArray();
            }

            // For all lower levels, include all overlapping files. We need to add overlapping files from the current
            // level too because even if there no input_files in level l, we would still need to add files which overlap
            // with the range containing the input_files in levels 0 to l. Level 0 doesn't need to be handled this way
            // because files are sorted by time and not by key.
            for (int m = Math.Max(l, 1); m <= outputLevel; m++)
            {
                foreach (FileMetaData next in vstorage.LevelFiles(m))
                {
                    if (!HaveOverlappingKeyRanges(smallestKey, largestKey, next)) continue;
                    if (next.BeingCompacted)
                    {
                        throw Aborted("File " + TableName(next.FileNumber) +
                                      " that has overlapping key range with one of the compaction  input file is currently being compacted.");
                    }
                    inputFiles.Add(next.FileNumber);
                }
            }
        }
    }

    /// <summary>
    /// Port of CompactionPicker::PickCompactionForCompactFiles: a manual compaction (no grandparents) of the sanitized
    /// inputs, registered with the picker. <paramref name="compression"/> null = the column family's compression for
    /// the output level.
    /// </summary>
    public Compaction PickCompactionForCompactFiles(VersionStorageInfo vstorage, List<CompactionInputFiles> inputs, int outputLevel, uint outputPathId,
        ulong outputFileSizeLimit, CompressionType? compression, Temperature outputTemperatureOverride, uint maxSubcompactions, bool allowTrivialMove)
    {
        CompressionType compressionType = compression
            ?? GetCompressionType(vstorage, Options, outputLevel, Options.CompactionStyle == CompactionStyle.Level ? Math.Max(1, vstorage.BaseLevel) : 1);
        var c = new Compaction(vstorage, Options, inputs.ToArray(), outputLevel, outputFileSizeLimit, Options.MaxCompactionBytes, outputPathId,
            compressionType, new List<FileMetaData>(), CompactionReason.ManualCompaction, -1, l0FilesMightOverlap: true, isManual: true)
        {
            OutputTemperature = outputTemperatureOverride,
            DisallowTrivialMove = !allowTrivialMove,
            MaxSubcompactions = maxSubcompactions,
        };
        RegisterCompaction(c);
        // Creating a compaction influences the compaction score because the score takes running compactions into account.
        vstorage.ComputeCompactionScore();
        return c;
    }
}
