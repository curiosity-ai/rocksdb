using System.Globalization;

namespace RocksDb.Managed.Db.Blob;

/// <summary>
/// A blob file as seen by one version: the immutable part recorded when the file was added (number, totals, checksum;
/// SharedBlobFileMetaData) plus the version-specific garbage counters and the table files that link to it (those whose
/// oldest_blob_file_number is this file). Port of rocksdb::BlobFileMetaData (db/blob/blob_file_meta.{h,cc}).
/// Immutable.
/// </summary>
internal sealed class BlobFileMetaData
{
    public BlobFileMetaData(BlobFileAddition addition, ulong garbageBlobCount, ulong garbageBlobBytes, ulong[] linkedSsts)
    {
        Addition = addition;
        GarbageBlobCount = garbageBlobCount;
        GarbageBlobBytes = garbageBlobBytes;
        LinkedSsts = linkedSsts;
    }

    /// <summary>The shared metadata (as recorded by the BlobFileAddition).</summary>
    public BlobFileAddition Addition { get; }
    public ulong BlobFileNumber => Addition.BlobFileNumber;
    public ulong TotalBlobCount => Addition.TotalBlobCount;
    public ulong TotalBlobBytes => Addition.TotalBlobBytes;
    public ulong GarbageBlobCount { get; }
    public ulong GarbageBlobBytes { get; }
    /// <summary>Numbers of the table files whose oldest blob file is this one, ascending.</summary>
    public ulong[] LinkedSsts { get; }

    /// <summary>BlobFileMetaData::DebugString.</summary>
    public override string ToString() =>
        "blob_file_number: " + BlobFileNumber.ToString(CultureInfo.InvariantCulture) +
        " total_blob_count: " + TotalBlobCount.ToString(CultureInfo.InvariantCulture) +
        " total_blob_bytes: " + TotalBlobBytes.ToString(CultureInfo.InvariantCulture) +
        " garbage_blob_count: " + GarbageBlobCount.ToString(CultureInfo.InvariantCulture) +
        " garbage_blob_bytes: " + GarbageBlobBytes.ToString(CultureInfo.InvariantCulture);

    /// <summary>
    /// The blob part of VersionBuilder (db/version_builder.cc: ApplyBlobFileAddition, ApplyBlobFileGarbage, the
    /// LinkSst/UnlinkSst bookkeeping, GetMinOldestBlobFileNumber and SaveBlobFilesTo / AddBlobFileIfNeeded): applies the
    /// blob additions and garbage of <paramref name="edit"/> (may be null) to <paramref name="baseFiles"/> and links the
    /// table files of the resulting version (<paramref name="levels"/>). Blob files older than the oldest blob file that
    /// still has linked table files are dropped, as are files without linked table files whose blobs are all garbage.
    /// Throws Corruption for inconsistent edits. Returns the blob files ordered by number.
    /// </summary>
    public static BlobFileMetaData[] Build(IReadOnlyList<BlobFileMetaData> baseFiles, VersionEdit? edit, IReadOnlyList<IReadOnlyList<FileMetaData>> levels)
    {
        bool editHasBlobs = edit is not null && (edit.BlobFileAdditions.Count > 0 || edit.BlobFileGarbages.Count > 0);
        if (baseFiles.Count == 0 && !editHasBlobs) return [];

        var state = new SortedDictionary<ulong, (BlobFileAddition Addition, ulong GarbageCount, ulong GarbageBytes)>();
        foreach (BlobFileMetaData b in baseFiles) state[b.BlobFileNumber] = (b.Addition, b.GarbageBlobCount, b.GarbageBlobBytes);
        if (edit is not null)
        {
            foreach (BlobFileAddition a in edit.BlobFileAdditions)
            {
                if (state.ContainsKey(a.BlobFileNumber))
                {
                    ThrowHelper.Corruption("VersionBuilder: Blob file #" + a.BlobFileNumber.ToString(CultureInfo.InvariantCulture) + " already added");
                }
                state[a.BlobFileNumber] = (a, 0, 0);
            }
            foreach (BlobFileGarbage g in edit.BlobFileGarbages)
            {
                if (!state.TryGetValue(g.BlobFileNumber, out var s))
                {
                    ThrowHelper.Corruption("VersionBuilder: Blob file #" + g.BlobFileNumber.ToString(CultureInfo.InvariantCulture) + " not found");
                }
                ulong count = s.GarbageCount + g.GarbageBlobCount;
                ulong bytes = s.GarbageBytes + g.GarbageBlobBytes;
                if (count > s.Addition.TotalBlobCount || bytes > s.Addition.TotalBlobBytes)
                {
                    ThrowHelper.Corruption("VersionBuilder: Garbage overflow for blob file #" + g.BlobFileNumber.ToString(CultureInfo.InvariantCulture));
                }
                state[g.BlobFileNumber] = (s.Addition, count, bytes);
            }
        }
        if (state.Count == 0) return [];

        // LinkSst: every table file links to its oldest blob file (if that file is known).
        Dictionary<ulong, List<ulong>>? linked = null;
        ulong minOldest = ulong.MaxValue;
        for (int l = 0; l < levels.Count; l++)
        {
            IReadOnlyList<FileMetaData> files = levels[l];
            for (int i = 0; i < files.Count; i++)
            {
                ulong b = files[i].OldestBlobFileNumber;
                if (b == FileMetaData.InvalidBlobFileNumber || !state.ContainsKey(b)) continue;
                linked ??= new Dictionary<ulong, List<ulong>>();
                if (!linked.TryGetValue(b, out List<ulong>? list)) linked[b] = list = new List<ulong>();
                list.Add(files[i].FileNumber);
                if (b < minOldest) minOldest = b;
            }
        }
        // No blob file with linked table files: no valid blob files at all.
        if (linked is null) return [];

        var result = new List<BlobFileMetaData>(state.Count);
        foreach ((ulong number, var s) in state)
        {
            if (number < minOldest) continue;
            ulong[] ssts = [];
            if (linked.TryGetValue(number, out List<ulong>? list))
            {
                list.Sort();
                ssts = list.ToArray();
            }
            // AddBlobFileIfNeeded
            if (ssts.Length == 0 && s.GarbageCount >= s.Addition.TotalBlobCount) continue;
            result.Add(new BlobFileMetaData(s.Addition, s.GarbageCount, s.GarbageBytes, ssts));
        }
        return result.ToArray();
    }
}
