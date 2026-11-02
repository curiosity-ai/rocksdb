using System.Globalization;
using System.Text;
using RocksDb.Managed.Compression;

namespace RocksDb.Managed.Db;

/// <summary>Port of rocksdb::CompactionReason (include/rocksdb/listener.h).</summary>
public enum CompactionReason
{
    Unknown = 0,
    LevelL0FilesNum,
    LevelMaxLevelSize,
    UniversalSizeAmplification,
    UniversalSizeRatio,
    UniversalSortedRunNum,
    FifoMaxSize,
    FifoReduceNumFiles,
    FifoTtl,
    ManualCompaction,
    FilesMarkedForCompaction,
    BottommostFiles,
    Ttl,
    Flush,
    ExternalSstIngestion,
    PeriodicCompaction,
    ChangeTemperature,
    ForcedBlobGC,
    RoundRobinTtl,
    RefitLevel,
}

/// <summary>Files of one input level of a compaction (rocksdb::CompactionInputFiles).</summary>
internal sealed class CompactionInputFiles
{
    public CompactionInputFiles(int level) => Level = level;

    public CompactionInputFiles(int level, List<FileMetaData> files)
    {
        Level = level;
        Files = files;
    }

    public int Level { get; set; }
    public List<FileMetaData> Files { get; set; } = new();
    public int Count => Files.Count;
    public bool IsEmpty => Files.Count == 0;
    public FileMetaData this[int i] => Files[i];
    public void Clear() => Files.Clear();

    public CompactionInputFiles Copy() => new(Level, new List<FileMetaData>(Files));
}

/// <summary>
/// A picked compaction: inputs per level, output level and the settings for its outputs. Port of rocksdb::Compaction
/// (db/compaction/compaction.{h,cc}): bottommost detection (IsBottommostLevel), IsTrivialMove,
/// KeyNotExistsBeyondOutputLevel / KeyRangeNotExistsBeyondOutputLevel, input deletions, being-compacted marking.
/// Proximal-level output (per-key placement), blob files, subcompaction boundaries and SST partitioners are not
/// ported.
/// </summary>
internal sealed class Compaction
{
    private readonly CompactionInputFiles[] _inputs;
    private readonly byte[] _smallestUserKey;
    private readonly byte[] _largestUserKey;

    public Compaction(VersionStorageInfo vstorage, ColumnFamilyOptions options, CompactionInputFiles[] inputs, int outputLevel,
        ulong targetFileSize, ulong maxCompactionBytes, uint outputPathId, CompressionType outputCompression, List<FileMetaData> grandparents,
        CompactionReason reason, double score, bool l0FilesMightOverlap, bool isManual)
    {
        InputStorage = vstorage;
        Options = options;
        _inputs = inputs;
        StartLevel = inputs[0].Level;
        OutputLevel = outputLevel;
        NumberLevels = vstorage.NumLevels;
        TargetOutputFileSize = targetFileSize;
        MaxCompactionBytes = maxCompactionBytes;
        OutputPathId = outputPathId;
        OutputCompression = outputCompression;
        Grandparents = grandparents;
        Reason = reason;
        Score = score;
        L0FilesMightOverlap = l0FilesMightOverlap;
        IsManual = isManual;
        GetBoundaryUserKeys(vstorage.Icmp, inputs, out _smallestUserKey, out _largestUserKey);
        BottommostLevel = IsBottommostLevelOf(outputLevel, vstorage, inputs);
        // For the non-bottommost levels, it tries to build files matching the target file size, but not guaranteed:
        // an output may grow to 2x the target size (the grandparent-boundary cuts of ShouldStopBefore normally end it
        // between 50% and 90% of the target).
        MaxOutputFileSize = BottommostLevel || grandparents.Count == 0 ? targetFileSize
            : targetFileSize > ulong.MaxValue / 2 ? ulong.MaxValue : 2 * targetFileSize;
        IsFullCompaction = IsFullCompactionOf(vstorage, inputs);
        MarkFilesBeingCompacted(true);
    }

    public VersionStorageInfo InputStorage { get; }
    public ColumnFamilyOptions Options { get; }
    /// <summary>The version the inputs belong to (referenced while the compaction runs). Set by the DB.</summary>
    public DbVersion? InputVersion { get; set; }
    public int StartLevel { get; }
    public int OutputLevel { get; }
    public int NumberLevels { get; }
    public ulong MaxOutputFileSize { get; }
    public ulong TargetOutputFileSize { get; }
    public ulong MaxCompactionBytes { get; }
    public uint OutputPathId { get; }
    public CompressionType OutputCompression { get; }
    public List<FileMetaData> Grandparents { get; }
    public CompactionReason Reason { get; }
    public double Score { get; }
    public bool L0FilesMightOverlap { get; }
    public bool IsManual { get; }
    /// <summary>bottommost_level(): no older data for the compacted range exists below the output.</summary>
    public bool BottommostLevel { get; }
    /// <summary>is_full_compaction(): every file of the column family is an input.</summary>
    public bool IsFullCompaction { get; }
    /// <summary>FIFO: inputs are simply deleted (deletion_compaction()).</summary>
    public bool IsDeletionCompaction { get; init; }
    /// <summary>Set by the picker when the trivial-move decision was made at pick time (universal).</summary>
    public bool IsTrivialMoveForced { get; init; }
    /// <summary>Universal/manual: never trivially move.</summary>
    public bool DisallowTrivialMove { get; init; }

    /// <summary>Manual compaction override of blob garbage collection (CompactRangeOptions::blob_garbage_collection_policy).</summary>
    public BlobGarbageCollectionPolicy BlobGarbageCollectionPolicy { get; set; } = BlobGarbageCollectionPolicy.UseDefault;
    /// <summary>Manual compaction override of the age cutoff (outside [0, 1] = use the column family option).</summary>
    public double BlobGarbageCollectionAgeCutoffOverride { get; set; } = -1;

    /// <summary>Compaction::enable_blob_garbage_collection().</summary>
    public bool EnableBlobGarbageCollection => BlobGarbageCollectionPolicy switch
    {
        BlobGarbageCollectionPolicy.Force => true,
        BlobGarbageCollectionPolicy.Disable => false,
        _ => Options.EnableBlobGarbageCollection,
    };

    /// <summary>Compaction::blob_garbage_collection_age_cutoff().</summary>
    public double BlobGarbageCollectionAgeCutoff =>
        BlobGarbageCollectionAgeCutoffOverride is < 0 or > 1 ? Options.BlobGarbageCollectionAgeCutoff : BlobGarbageCollectionAgeCutoffOverride;

    /// <summary>Compaction::DoesInputReferenceBlobFiles.</summary>
    public bool DoesInputReferenceBlobFiles()
    {
        if (InputStorage.BlobFiles.Length == 0) return false;
        foreach (CompactionInputFiles l in _inputs)
        {
            foreach (FileMetaData f in l.Files)
            {
                if (f.OldestBlobFileNumber != FileMetaData.InvalidBlobFileNumber) return true;
            }
        }
        return false;
    }

    public int NumInputLevels => _inputs.Length;
    public CompactionInputFiles Inputs(int which) => _inputs[which];
    public IReadOnlyList<CompactionInputFiles> AllInputs => _inputs;
    public int Level(int which) => _inputs[which].Level;
    public int NumInputFiles(int which) => _inputs[which].Count;
    public FileMetaData Input(int which, int i) => _inputs[which][i];
    public ReadOnlySpan<byte> SmallestUserKey => _smallestUserKey;
    public ReadOnlySpan<byte> LargestUserKey => _largestUserKey;

    /// <summary>Total size of the input files (CalculateTotalInputSize).</summary>
    public ulong TotalInputBytes
    {
        get
        {
            ulong n = 0;
            foreach (CompactionInputFiles l in _inputs)
            {
                foreach (FileMetaData f in l.Files) n += f.FileSize;
            }
            return n;
        }
    }

    public int TotalInputFiles
    {
        get
        {
            int n = 0;
            foreach (CompactionInputFiles l in _inputs) n += l.Count;
            return n;
        }
    }

    private static void GetBoundaryUserKeys(InternalKeyComparator icmp, CompactionInputFiles[] inputs, out byte[] smallest, out byte[] largest)
    {
        byte[]? s = null, l = null;
        foreach (CompactionInputFiles level in inputs)
        {
            if (level.IsEmpty) continue;
            if (level.Level == 0)
            {
                foreach (FileMetaData f in level.Files)
                {
                    if (s is null || icmp.CompareUserKey(DbFormat.ExtractUserKey(f.Smallest), DbFormat.ExtractUserKey(s)) < 0) s = f.Smallest;
                    if (l is null || icmp.CompareUserKey(DbFormat.ExtractUserKey(f.Largest), DbFormat.ExtractUserKey(l)) > 0) l = f.Largest;
                }
            }
            else
            {
                byte[] fs = level.Files[0].Smallest, fl = level.Files[^1].Largest;
                if (s is null || icmp.CompareUserKey(DbFormat.ExtractUserKey(fs), DbFormat.ExtractUserKey(s)) < 0) s = fs;
                if (l is null || icmp.CompareUserKey(DbFormat.ExtractUserKey(fl), DbFormat.ExtractUserKey(l)) > 0) l = fl;
            }
        }
        smallest = s is null ? [] : DbFormat.ExtractUserKey(s).ToArray();
        largest = l is null ? [] : DbFormat.ExtractUserKey(l).ToArray();
    }

    /// <summary>Port of Compaction::IsBottommostLevel.</summary>
    public static bool IsBottommostLevelOf(int outputLevel, VersionStorageInfo vstorage, CompactionInputFiles[] inputs)
    {
        int outputL0Index;
        if (outputLevel == 0)
        {
            outputL0Index = 0;
            ReadOnlySpan<FileMetaData> l0 = vstorage.LevelFiles(0);
            FileMetaData last = inputs[0].Files[^1];
            foreach (FileMetaData f in l0)
            {
                if (ReferenceEquals(f, last)) break;
                outputL0Index++;
            }
        }
        else
        {
            outputL0Index = -1;
        }
        GetBoundaryUserKeys(vstorage.Icmp, inputs, out byte[] smallest, out byte[] largest);
        return !vstorage.RangeMightExistAfterSortedRun(smallest, largest, outputLevel, outputL0Index);
    }

    /// <summary>Port of Compaction::IsFullCompaction.</summary>
    private static bool IsFullCompactionOf(VersionStorageInfo vstorage, CompactionInputFiles[] inputs)
    {
        int total = vstorage.NumFiles;
        int n = 0;
        foreach (CompactionInputFiles l in inputs) n += l.Count;
        return n == total;
    }

    /// <summary>Port of Compaction::InputCompressionMatchesOutput.</summary>
    private bool InputCompressionMatchesOutput()
    {
        int baseLevel = InputStorage.BaseLevel;
        CompressionType t = TableFactory.GetCompressionType(Options, StartLevel, baseLevel,
            StartLevel >= InputStorage.NumNonEmptyLevels - 1, enableCompression: true);
        if (t == CompressionType.DisableCompressionOption) t = CompressionType.None;
        return t == OutputCompression;
    }

    /// <summary>Port of Compaction::IsTrivialMove.</summary>
    public bool IsTrivialMove()
    {
        if (IsDeletionCompaction || DisallowTrivialMove) return false;
        // Check if start level have files with overlapping ranges.
        if (StartLevel == 0 && !InputStorage.Level0NonOverlapping && L0FilesMightOverlap) return false;
        // A manual compaction with a compaction filter must run the filter.
        if (IsManual && (Options.CompactionFilter is not null || Options.CompactionFilterFactory is not null)) return false;
        if (StartLevel == OutputLevel) return false;
        if (Reason == CompactionReason.ChangeTemperature) return false;
        if (Options.CompactionOptionsUniversal.AllowTrivialMove && OutputLevel != 0 && Options.CompactionStyle == CompactionStyle.Universal)
        {
            return IsTrivialMoveForced;
        }
        if (!(StartLevel != OutputLevel && NumInputLevels == 1 && Input(0, 0).PathId == OutputPathId && InputCompressionMatchesOutput()))
            return false;
        if (OutputLevel + 1 < NumberLevels)
        {
            var grandparents = new List<FileMetaData>();
            foreach (FileMetaData file in _inputs[0].Files)
            {
                InputStorage.GetOverlappingInputs(OutputLevel + 1, file.Smallest, file.Largest, grandparents);
                ulong size = file.FileSize;
                foreach (FileMetaData g in grandparents) size += g.FileSize;
                if (size > MaxCompactionBytes) return false;
            }
        }
        return true;
    }

    /// <summary>Port of Compaction::AddInputDeletions.</summary>
    public void AddInputDeletions(VersionEdit edit)
    {
        foreach (CompactionInputFiles l in _inputs)
        {
            foreach (FileMetaData f in l.Files) edit.DeleteFile(l.Level, f.FileNumber);
        }
    }

    /// <summary>
    /// Port of Compaction::KeyNotExistsBeyondOutputLevel: true if <paramref name="userKey"/> provably has no older
    /// version below the output level. <paramref name="levelPtrs"/> carries the per-level scan positions (keys must be
    /// queried in increasing order).
    /// </summary>
    public bool KeyNotExistsBeyondOutputLevel(ReadOnlySpan<byte> userKey, int[] levelPtrs)
    {
        if (BottommostLevel) return true;
        if (OutputLevel != 0 && Options.CompactionStyle == CompactionStyle.Level)
        {
            InternalKeyComparator icmp = InputStorage.Icmp;
            for (int lvl = OutputLevel + 1; lvl < NumberLevels; lvl++)
            {
                ReadOnlySpan<FileMetaData> files = InputStorage.LevelFiles(lvl);
                for (; levelPtrs[lvl] < files.Length; levelPtrs[lvl]++)
                {
                    FileMetaData f = files[levelPtrs[lvl]];
                    if (icmp.CompareUserKey(userKey, DbFormat.ExtractUserKey(f.Largest)) <= 0)
                    {
                        // We've advanced far enough.
                        // In the presence of user-defined timestamps, f.Smallest may have the same user key with a smaller
                        // timestamp (sorting after userKey): hence CompareWithoutTimestamp.
                        ReadOnlySpan<byte> smallest = DbFormat.ExtractUserKey(f.Smallest);
                        int c = icmp.UserComparator.TimestampSize == 0
                            ? icmp.CompareUserKey(userKey, smallest)
                            : icmp.UserComparator.CompareWithoutTimestamp(userKey, smallest);
                        if (c >= 0) return false; // key falls in this file's range
                        break;
                    }
                }
            }
            return true;
        }
        return false;
    }

    /// <summary>Port of Compaction::KeyRangeNotExistsBeyondOutputLevel ([begin, end) user keys).</summary>
    public bool KeyRangeNotExistsBeyondOutputLevel(ReadOnlySpan<byte> beginKey, ReadOnlySpan<byte> endKey, int[] levelPtrs)
    {
        if (BottommostLevel) return true;
        if (OutputLevel != 0 && Options.CompactionStyle == CompactionStyle.Level)
        {
            InternalKeyComparator icmp = InputStorage.Icmp;
            for (int lvl = OutputLevel + 1; lvl < NumberLevels; lvl++)
            {
                ReadOnlySpan<FileMetaData> files = InputStorage.LevelFiles(lvl);
                for (; levelPtrs[lvl] < files.Length; levelPtrs[lvl]++)
                {
                    FileMetaData f = files[levelPtrs[lvl]];
                    // Advance until the first file with begin_key <= f->largest.user_key().
                    if (VersionStorageInfo.CompareWithoutTimestamp(icmp, beginKey, DbFormat.ExtractUserKey(f.Largest)) > 0) continue;
                    if (VersionStorageInfo.CompareWithoutTimestamp(icmp, endKey, DbFormat.ExtractUserKey(f.Smallest)) <= 0) break; // not overlapping with this level
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    /// <summary>Port of Compaction::MarkFilesBeingCompacted. REQUIRES: DB mutex.</summary>
    public void MarkFilesBeingCompacted(bool beingCompacted)
    {
        foreach (CompactionInputFiles l in _inputs)
        {
            foreach (FileMetaData f in l.Files) f.BeingCompacted = beingCompacted;
        }
    }

    /// <summary>Port of Compaction::MinInputFileOldestAncestorTime (0 when unknown).</summary>
    public ulong MinInputFileOldestAncestorTime()
    {
        ulong min = ulong.MaxValue;
        foreach (CompactionInputFiles l in _inputs)
        {
            foreach (FileMetaData f in l.Files)
            {
                ulong t = f.OldestAncestorTime != 0 ? f.OldestAncestorTime : f.FileCreationTime;
                if (t != 0) min = Math.Min(min, t);
            }
        }
        return min;
    }

    /// <summary>
    /// Port of Compaction::MaxInputFileNewestKeyTime (whole compaction; 0 = unknown): the newest key time recorded for
    /// the outputs, so a FIFO TTL compaction never drops an output before its newest input has expired.
    /// </summary>
    public ulong MaxInputFileNewestKeyTime()
    {
        ulong max = 0;
        foreach (CompactionInputFiles l in _inputs)
        {
            foreach (FileMetaData f in l.Files) max = Math.Max(max, f.NewestKeyTime);
        }
        return max;
    }

    /// <summary>Port of Compaction::MinInputFileEpochNumber.</summary>
    public ulong MinInputFileEpochNumber()
    {
        ulong min = ulong.MaxValue;
        foreach (CompactionInputFiles l in _inputs)
        {
            foreach (FileMetaData f in l.Files) min = Math.Min(min, f.EpochNumber);
        }
        return min;
    }

    /// <summary>Creates the compaction filter (CreateCompactionFilter): the CF's filter, or one from its factory.</summary>
    public CompactionFilter? CreateCompactionFilter(uint columnFamilyId)
    {
        if (Options.CompactionFilter is { } f) return f;
        if (Options.CompactionFilterFactory is not { } factory || !factory.ShouldFilterTableFileCreation(isFlush: false)) return null;
        return factory.CreateCompactionFilter(new CompactionFilterContext(IsFullCompaction, IsManual, columnFamilyId, IsFlush: false));
    }

    /// <summary>Compaction::InputLevelSummary, e.g. "3@0 + 2@1 files to L1".</summary>
    public override string ToString()
    {
        var sb = new StringBuilder();
        for (int i = 0; i < _inputs.Length; i++)
        {
            if (i > 0) sb.Append(" + ");
            sb.Append(_inputs[i].Count.ToString(CultureInfo.InvariantCulture)).Append('@').Append(_inputs[i].Level.ToString(CultureInfo.InvariantCulture));
        }
        return sb.Append(" files to L").Append(OutputLevel.ToString(CultureInfo.InvariantCulture)).ToString();
    }
}
