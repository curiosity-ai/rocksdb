namespace RocksDb.Managed.Db;

/// <summary>
/// FIFO compaction picker. Port of FIFOCompactionPicker (db/compaction/compaction_picker_fifo.cc): TTL-based
/// deletion of the oldest L0 files, size-based deletion (oldest L0 files, or the smallest keys of the last non-empty
/// level while migrating from another style), and optional cost-based intra-L0 compaction
/// (<see cref="CompactionOptionsFifo.AllowCompaction"/>). Temperature changes, blob-aware sizing and kv-ratio
/// compaction are not ported. REQUIRES: DB mutex.
/// </summary>
internal sealed class FifoCompactionPicker : CompactionPicker
{
    public FifoCompactionPicker(ColumnFamilyOptions options, InternalKeyComparator icmp) : base(options, icmp) { }

    public override bool NeedsCompaction(VersionStorageInfo vstorage) => vstorage.CompactionScore(0) >= 1;

    private static ulong TotalSize(ReadOnlySpan<FileMetaData> files)
    {
        ulong n = 0;
        foreach (FileMetaData f in files) n += f.FileSize;
        return n;
    }

    /// <summary>Port of FIFOCompactionPicker::PickTTLCompaction.</summary>
    private Compaction? PickTtlCompaction(VersionStorageInfo vstorage)
    {
        ReadOnlySpan<FileMetaData> levelFiles = vstorage.LevelFiles(0);
        ulong totalSize = TotalSize(levelFiles);
        ulong currentTime = (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        if (IsLevel0CompactionInProgress) return null; // compactions are very fast; no parallel ones
        var inputs = new CompactionInputFiles(0);
        ulong ttl = Options.Ttl;
        if (currentTime > ttl)
        {
            // In L0, right-most files are the oldest files.
            for (int i = levelFiles.Length - 1; i >= 0; i--)
            {
                FileMetaData f = levelFiles[i];
                // FileMetaData::TryGetNewestKeyTime, else the oldest ancestor time (the table property upstream; the
                // MANIFEST field, falling back to the file creation time, here).
                ulong estNewestKeyTime = f.NewestKeyTime != 0 ? f.NewestKeyTime
                    : f.OldestAncestorTime != 0 ? f.OldestAncestorTime : f.FileCreationTime;
                if (estNewestKeyTime == 0 || estNewestKeyTime >= currentTime - ttl) break;
                totalSize -= f.FileSize;
                inputs.Files.Add(f);
            }
        }
        // Proceed to size-based FIFO compaction if there are no files older than ttl, or deleting them will not bring
        // the total size below the size threshold.
        if (inputs.IsEmpty || totalSize > Options.CompactionOptionsFifo.MaxTableFilesSize) return null;
        return NewCompaction(vstorage, [inputs], 0, 0, 0, 0, new List<FileMetaData>(), CompactionReason.FifoTtl, vstorage.CompactionScore(0),
            l0FilesMightOverlap: true, isManual: false, isDeletion: true);
    }

    /// <summary>Port of FIFOCompactionPicker::PickSizeCompaction.</summary>
    private Compaction? PickSizeCompaction(VersionStorageInfo vstorage)
    {
        int lastLevel = 0;
        ulong totalSize = 0;
        for (int level = 0; level < vstorage.NumLevels; ++level)
        {
            ulong levelSize = vstorage.NumLevelBytes(level);
            totalSize += levelSize;
            if (levelSize > 0) lastLevel = level;
        }
        ulong max = Options.CompactionOptionsFifo.MaxTableFilesSize;
        ReadOnlySpan<FileMetaData> lastLevelFiles = vstorage.LevelFiles(lastLevel);
        if (lastLevel == 0 && totalSize <= max) return null;
        if (IsLevel0CompactionInProgress) return null;
        var inputs = new CompactionInputFiles(lastLevel);
        if (lastLevel == 0)
        {
            // In L0, right-most files are the oldest files.
            for (int i = lastLevelFiles.Length - 1; i >= 0; i--)
            {
                FileMetaData f = lastLevelFiles[i];
                totalSize -= Math.Min(totalSize, f.FileSize);
                inputs.Files.Add(f);
                if (totalSize <= max) break;
            }
        }
        else if (totalSize > max)
        {
            // If the last level is non-L0, we don't know which file is logically the oldest: delete files from the left
            // (smallest keys first).
            foreach (FileMetaData f in lastLevelFiles)
            {
                if (f.BeingCompacted) continue;
                totalSize -= Math.Min(totalSize, f.FileSize);
                inputs.Files.Add(f);
                if (totalSize <= max) break;
            }
        }
        else
        {
            return null;
        }
        if (inputs.IsEmpty) return null;
        return NewCompaction(vstorage, [inputs], lastLevel, 0, 0, 0, new List<FileMetaData>(), CompactionReason.FifoMaxSize,
            vstorage.CompactionScore(0), l0FilesMightOverlap: true, isManual: false, isDeletion: true);
    }

    /// <summary>Port of FIFOCompactionPicker::PickIntraL0Compaction (cost-based path).</summary>
    private Compaction? PickIntraL0Compaction(VersionStorageInfo vstorage)
    {
        if (!Options.CompactionOptionsFifo.AllowCompaction) return null;
        ReadOnlySpan<FileMetaData> level0 = vstorage.LevelFiles(0);
        if (level0.Length == 0) return null;
        // Disallow compactions with files larger than the memtable (+10%) so files may still TTL-expire.
        ulong maxCompactBytesPerDelFile = MultiplyCheckOverflow(Options.WriteBufferSize, 1.1);
        var inputs = new CompactionInputFiles(0);
        if (!PickCostBasedIntraL0Compaction(level0, Options.Level0FileNumCompactionTrigger, maxCompactBytesPerDelFile, Options.MaxCompactionBytes, inputs))
        {
            return null;
        }
        return NewCompaction(vstorage, [inputs], 0, 16UL * 1024 * 1024, 0, 0, new List<FileMetaData>(), CompactionReason.FifoReduceNumFiles,
            vstorage.CompactionScore(0), l0FilesMightOverlap: true, isManual: false);
    }

    public override Compaction? PickCompaction(VersionStorageInfo vstorage, IReadOnlyList<ulong> snapshots)
    {
        Compaction? c = null;
        if (Options.Ttl > 0 && Options.Ttl < ulong.MaxValue - 1) c = PickTtlCompaction(vstorage);
        c ??= PickSizeCompaction(vstorage);
        c ??= PickIntraL0Compaction(vstorage);
        return c;
    }

    /// <summary>Port of FIFOCompactionPicker::PickCompactionForCompactRange: runs the regular picker.</summary>
    public override Compaction? PickCompactionForCompactRange(VersionStorageInfo vstorage, int inputLevel, int outputLevel, uint maxSubcompactions,
        byte[]? begin, byte[]? end, out byte[]? compactionEnd, out bool manualConflict, ulong maxFileNumToIgnore, bool disallowTrivialMove)
    {
        compactionEnd = null;
        manualConflict = false;
        return PickCompaction(vstorage, []);
    }
}
