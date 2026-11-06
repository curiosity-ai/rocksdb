using System.Globalization;

namespace RocksDb.Managed.Db;

/// <summary>
/// The LSM shape of one column family at one point in time: per-level file lists and sizes. Port of the data part of
/// VersionStorageInfo (db/version_set.h). L0 files are ordered newest first (NewestFirstByEpochNumber); files on
/// other levels are non-overlapping and ordered by smallest key. Immutable once built.
/// <para>
/// Compaction state (level targets incl. dynamic level bytes, scores, files by compaction priority) lives in
/// <c>Compaction/VersionStorageInfo.Compaction.cs</c>.
/// </para>
/// </summary>
internal sealed partial class VersionStorageInfo
{
    private static readonly FileMetaData[] s_empty = [];
    private readonly FileMetaData[][] _files;
    private readonly ulong[] _levelBytes;

    public VersionStorageInfo(InternalKeyComparator icmp, int numLevels, ColumnFamilyOptions options, FileMetaData[][]? files)
        : this(icmp, numLevels, options, files, [])
    {
    }

    /// <param name="blobFiles">Live blob files of the version ordered by number (see <see cref="Blob.BlobFileMetaData.Build"/>).</param>
    public VersionStorageInfo(InternalKeyComparator icmp, int numLevels, ColumnFamilyOptions options, FileMetaData[][]? files, Blob.BlobFileMetaData[] blobFiles)
        : this(icmp, numLevels, options, files, blobFiles, null)
    {
    }

    /// <param name="context">
    /// State carried over from the previous version of the column family and applied from the version edit (compact
    /// cursors, accumulated entry stats, oldest snapshot, ...); null for a version without history.
    /// </param>
    public VersionStorageInfo(InternalKeyComparator icmp, int numLevels, ColumnFamilyOptions options, FileMetaData[][]? files, Blob.BlobFileMetaData[] blobFiles,
        VersionBuildContext? context)
    {
        Icmp = icmp;
        BlobFiles = blobFiles;
        NumLevels = numLevels;
        _files = new FileMetaData[numLevels][];
        _levelBytes = new ulong[numLevels];
        for (int l = 0; l < numLevels; l++)
        {
            _files[l] = files is not null && l < files.Length ? files[l] : s_empty;
            ulong bytes = 0;
            foreach (FileMetaData f in _files[l])
            {
                bytes += f.FileSize;
                f.LevelHint = l;
            }
            _levelBytes[l] = bytes;
        }
        // Inherited state (VersionStorageInfo::VersionStorageInfo with ref_vstorage), the edit's compact cursors
        // (VersionBuilder::SaveTo), then level targets, compaction scores, files by compaction priority
        // (VersionStorageInfo::PrepareForVersionAppend).
        InitializeInheritedState(context);
        InitializeCompactionState(options);
    }

    public InternalKeyComparator Icmp { get; }
    public int NumLevels { get; }

    /// <summary>Live blob files ordered by number (VersionStorageInfo::GetBlobFiles).</summary>
    public Blob.BlobFileMetaData[] BlobFiles { get; }

    /// <summary>VersionStorageInfo::GetBlobFileMetaData: the blob file with the given number, or null.</summary>
    public Blob.BlobFileMetaData? GetBlobFileMetaData(ulong blobFileNumber)
    {
        Blob.BlobFileMetaData[] b = BlobFiles;
        int lo = 0, hi = b.Length;
        while (lo < hi)
        {
            int mid = (lo + hi) >>> 1;
            ulong n = b[mid].BlobFileNumber;
            if (n == blobFileNumber) return b[mid];
            if (n < blobFileNumber) lo = mid + 1;
            else hi = mid;
        }
        return null;
    }

    /// <summary>VersionStorageInfo::GetBlobStats: total size and garbage of the live blob files.</summary>
    public (ulong TotalFileSize, ulong TotalGarbageSize) GetBlobStats()
    {
        ulong total = 0, garbage = 0;
        foreach (Blob.BlobFileMetaData b in BlobFiles)
        {
            total += b.TotalBlobBytes + Blob.BlobLogFormat.HeaderSize + Blob.BlobLogFormat.FooterSize;
            garbage += b.GarbageBlobBytes;
        }
        return (total, garbage);
    }

    public ReadOnlySpan<FileMetaData> LevelFiles(int level) => _files[level];
    internal FileMetaData[] LevelFilesArray(int level) => _files[level];
    /// <summary>A copy of the per-level file arrays (to build another version over the same files).</summary>
    internal FileMetaData[][] LevelFilesArrays() => (FileMetaData[][])_files.Clone();
    public int NumLevelFiles(int level) => _files[level].Length;
    public ulong NumLevelBytes(int level) => _levelBytes[level];

    public int NumFiles
    {
        get
        {
            int n = 0;
            foreach (FileMetaData[] l in _files) n += l.Length;
            return n;
        }
    }

    /// <summary>
    /// Port of FindFile (db/version_set.cc): index of the first file on a sorted level whose largest key is &gt;=
    /// <paramref name="internalKey"/>, or the file count if none.
    /// </summary>
    public int FindFile(int level, ReadOnlySpan<byte> internalKey)
    {
        FileMetaData[] files = _files[level];
        int lo = 0, hi = files.Length;
        while (lo < hi)
        {
            int mid = (lo + hi) >>> 1;
            if (Icmp.Compare(files[mid].Largest, internalKey) < 0) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    /// <summary>Port of VersionStorageInfo::LevelSummary.</summary>
    public string LevelSummary()
    {
        var sb = new System.Text.StringBuilder("files[");
        for (int l = 0; l < NumLevels; l++)
        {
            if (l > 0) sb.Append(' ');
            sb.Append(_files[l].Length.ToString(CultureInfo.InvariantCulture));
        }
        return sb.Append(']').ToString();
    }

    /// <summary>
    /// Port of VersionBuilder::Apply + SaveTo (db/version_builder.cc): returns the file lists after applying
    /// <paramref name="edit"/> to this version. Throws Corruption for inconsistent edits.
    /// </summary>
    public FileMetaData[][] ApplyEdit(VersionEdit edit, int numLevels)
    {
        int levels = numLevels;
        foreach (NewFileEntry nf in edit.NewFiles) levels = Math.Max(levels, nf.Level + 1);
        var lists = new List<FileMetaData>[levels];
        for (int l = 0; l < levels; l++)
        {
            lists[l] = l < NumLevels ? new List<FileMetaData>(_files[l]) : new List<FileMetaData>();
        }
        foreach ((int level, ulong number) in edit.DeletedFiles)
        {
            if (level >= levels || lists[level].RemoveAll(f => f.FileNumber == number) == 0)
            {
                ThrowHelper.Corruption("VersionBuilder: Cannot delete table file #" + number.ToString(CultureInfo.InvariantCulture) +
                                       " from level " + level.ToString(CultureInfo.InvariantCulture) + " since it is not in the LSM tree");
            }
        }
        foreach (NewFileEntry nf in edit.NewFiles) lists[nf.Level].Add(nf.Meta);
        var result = new FileMetaData[levels][];
        for (int l = 0; l < levels; l++)
        {
            List<FileMetaData> files = lists[l];
            if (l == 0) files.Sort(static (a, b) => CompareNewestFirst(a, b));
            else
            {
                InternalKeyComparator icmp = Icmp;
                files.Sort((a, b) =>
                {
                    int r = icmp.Compare(a.Smallest, b.Smallest);
                    return r != 0 ? r : a.FileNumber.CompareTo(b.FileNumber);
                });
            }
            result[l] = files.ToArray();
        }
        return result;
    }

    /// <summary>
    /// Port of VersionBuilder::CheckConsistency / CheckConsistencyDetails (db/version_builder.cc) for the files of a
    /// version about to be saved: L0 sorted newest first (by epoch number, or by sequence numbers when some L0 file has
    /// no epoch number: EpochNumberRequirement::kMightMissing) without overlapping files of the same epoch, levels 1+
    /// sorted by smallest key without overlap, and no blob file consisting entirely of garbage. The table-to-blob links
    /// are consistent by construction (<see cref="Blob.BlobFileMetaData.Build"/> derives them from the table files).
    /// Only runs with force_consistency_checks (upstream also always runs it in debug builds). Throws
    /// Corruption("force_consistency_checks: VersionBuilder: ...") like upstream.
    /// </summary>
    internal static void CheckConsistency(InternalKeyComparator icmp, IReadOnlyList<FileMetaData[]> levels, Blob.BlobFileMetaData[] blobFiles,
        bool forceConsistencyChecks)
    {
        if (!forceConsistencyChecks) return;
        if (levels.Count > 0)
        {
            FileMetaData[] l0 = levels[0];
            bool epochMightMiss = false;
            foreach (FileMetaData f in l0) epochMightMiss |= f.EpochNumber == FileMetaData.UnknownEpochNumber;
            for (int i = 1; i < l0.Length; i++)
            {
                FileMetaData lhs = l0[i - 1], rhs = l0[i];
                if (epochMightMiss)
                {
                    if (!NewestFirstBySeqNo(lhs, rhs))
                    {
                        ConsistencyCorruption("L0 files are not sorted properly: files #" + Num(lhs.FileNumber) + " with seqnos (largest, smallest) " +
                            Num(lhs.LargestSeqno) + " , " + Num(lhs.SmallestSeqno) + ", #" + Num(rhs.FileNumber) + " with seqnos (largest, smallest) " +
                            Num(rhs.LargestSeqno) + " , " + Num(rhs.SmallestSeqno));
                    }
                    continue;
                }
                if (lhs.EpochNumber == rhs.EpochNumber && icmp.Compare(lhs.Smallest, rhs.Largest) <= 0 && icmp.Compare(lhs.Largest, rhs.Smallest) >= 0)
                {
                    ConsistencyCorruption("L0 files of same epoch number but overlapping range #" + Num(lhs.FileNumber) +
                        " , smallest key: " + InternalKey.DebugString(lhs.Smallest, hex: true) + " , largest key: " + InternalKey.DebugString(lhs.Largest, hex: true) +
                        " , epoch number: " + Num(lhs.EpochNumber) + " vs. file #" + Num(rhs.FileNumber) +
                        " , smallest key: " + InternalKey.DebugString(rhs.Smallest, hex: true) + " , largest key: " + InternalKey.DebugString(rhs.Largest, hex: true) +
                        " , epoch number: " + Num(rhs.EpochNumber));
                }
                // NewestFirstByEpochNumber
                if (!(lhs.EpochNumber > rhs.EpochNumber || (lhs.EpochNumber == rhs.EpochNumber && NewestFirstBySeqNo(lhs, rhs))))
                {
                    ConsistencyCorruption("L0 files are not sorted properly: files #" + Num(lhs.FileNumber) + " with epoch number " + Num(lhs.EpochNumber) +
                        ", #" + Num(rhs.FileNumber) + " with epoch number " + Num(rhs.EpochNumber));
                }
            }
        }
        for (int level = 1; level < levels.Count; level++)
        {
            FileMetaData[] files = levels[level];
            for (int i = 1; i < files.Length; i++)
            {
                FileMetaData lhs = files[i - 1], rhs = files[i];
                // BySmallestKey
                int r = icmp.Compare(lhs.Smallest, rhs.Smallest);
                if (!(r < 0 || (r == 0 && lhs.FileNumber < rhs.FileNumber)))
                {
                    ConsistencyCorruption("L" + Num((ulong)level) + " files are not sorted properly: files #" + Num(lhs.FileNumber) + ", #" + Num(rhs.FileNumber));
                }
                // Make sure there is no overlap in level
                if (icmp.Compare(lhs.Largest, rhs.Smallest) >= 0)
                {
                    ConsistencyCorruption("L" + Num((ulong)level) + " has overlapping ranges: file #" + Num(lhs.FileNumber) +
                        " largest key: " + InternalKey.DebugString(lhs.Largest, hex: true) + " vs. file #" + Num(rhs.FileNumber) +
                        " smallest key: " + InternalKey.DebugString(rhs.Smallest, hex: true));
                }
            }
        }
        // Make sure that all blob files in the version have non-garbage data.
        foreach (Blob.BlobFileMetaData b in blobFiles)
        {
            if (b.GarbageBlobCount >= b.TotalBlobCount)
            {
                ConsistencyCorruption("Blob file #" + Num(b.BlobFileNumber) + " consists entirely of garbage");
            }
        }

        static string Num(ulong n) => n.ToString(CultureInfo.InvariantCulture);
    }

    /// <summary>NewestFirstBySeqNo (db/version_set.h): lhs strictly precedes rhs.</summary>
    private static bool NewestFirstBySeqNo(FileMetaData a, FileMetaData b)
    {
        if (a.LargestSeqno != b.LargestSeqno) return a.LargestSeqno > b.LargestSeqno;
        if (a.SmallestSeqno != b.SmallestSeqno) return a.SmallestSeqno > b.SmallestSeqno;
        // Break ties by file number
        return a.FileNumber > b.FileNumber;
    }

    [System.Diagnostics.CodeAnalysis.DoesNotReturn]
    private static void ConsistencyCorruption(string message) => ThrowHelper.Corruption("force_consistency_checks: VersionBuilder: " + message);

    /// <summary>NewestFirstByEpochNumber (db/version_set.h), with seqno/file number tie breakers.</summary>
    internal static int CompareNewestFirst(FileMetaData a, FileMetaData b)
    {
        if (a.EpochNumber != b.EpochNumber) return b.EpochNumber.CompareTo(a.EpochNumber);
        if (a.LargestSeqno != b.LargestSeqno) return b.LargestSeqno.CompareTo(a.LargestSeqno);
        if (a.SmallestSeqno != b.SmallestSeqno) return b.SmallestSeqno.CompareTo(a.SmallestSeqno);
        return b.FileNumber.CompareTo(a.FileNumber);
    }
}

/// <summary>
/// A reference-counted, immutable snapshot of the files of one column family. Port of rocksdb::Version
/// (db/version_set.h). References are taken by the column family (current version) and by super versions; when the
/// last reference goes away the version's files are unreferenced in the <see cref="VersionSet"/> (under the DB mutex),
/// making files that no version uses obsolete.
/// </summary>
internal sealed class DbVersion
{
    private int _refs;

    public DbVersion(ColumnFamilyData cfd, VersionStorageInfo storage, ulong versionNumber)
    {
        Cfd = cfd;
        Storage = storage;
        VersionNumber = versionNumber;
    }

    public ColumnFamilyData Cfd { get; }
    public VersionStorageInfo Storage { get; }
    public ulong VersionNumber { get; }
    public int RefCount => Volatile.Read(ref _refs);

    public void Ref() => Interlocked.Increment(ref _refs);

    /// <summary>Returns true when the last reference was dropped. REQUIRES: DB mutex held (file bookkeeping).</summary>
    public bool Unref() => Interlocked.Decrement(ref _refs) == 0;
}
