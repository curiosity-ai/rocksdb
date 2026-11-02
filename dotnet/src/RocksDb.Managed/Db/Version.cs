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
    {
        Icmp = icmp;
        NumLevels = numLevels;
        _files = new FileMetaData[numLevels][];
        _levelBytes = new ulong[numLevels];
        for (int l = 0; l < numLevels; l++)
        {
            _files[l] = files is not null && l < files.Length ? files[l] : s_empty;
            ulong bytes = 0;
            foreach (FileMetaData f in _files[l]) bytes += f.FileSize;
            _levelBytes[l] = bytes;
        }
        // Level targets, compaction scores, files by compaction priority (VersionStorageInfo::PrepareForVersionAppend).
        InitializeCompactionState(options);
    }

    public InternalKeyComparator Icmp { get; }
    public int NumLevels { get; }

    public ReadOnlySpan<FileMetaData> LevelFiles(int level) => _files[level];
    internal FileMetaData[] LevelFilesArray(int level) => _files[level];
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
