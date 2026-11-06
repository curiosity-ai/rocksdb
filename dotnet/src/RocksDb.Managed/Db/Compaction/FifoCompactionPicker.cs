namespace RocksDb.Managed.Db;

/// <summary>
/// FIFO compaction picker. Port of FIFOCompactionPicker (db/compaction/compaction_picker_fifo.cc): TTL-based
/// deletion of the oldest L0 files, size-based deletion (oldest L0 files, or the smallest keys of the last non-empty
/// level while migrating from another style), blob-aware sizing (<see cref="CompactionOptionsFifo.MaxDataFilesSize"/>),
/// intra-L0 compaction (<see cref="CompactionOptionsFifo.AllowCompaction"/>: cost based, or the tiered kv-ratio
/// strategy with <see cref="CompactionOptionsFifo.UseKvRatioCompaction"/>) and temperature changes
/// (<see cref="CompactionOptionsFifo.FileTemperatureAgeThresholds"/>: the file is rewritten with the new temperature
/// recorded in the MANIFEST; the managed file system has a single tier). REQUIRES: DB mutex.
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

    /// <summary>GetEffectiveMax: max_data_files_size when set (blob-aware mode), else max_table_files_size.</summary>
    private static ulong GetEffectiveMax(CompactionOptionsFifo o) => o.MaxDataFilesSize > 0 ? o.MaxDataFilesSize : o.MaxTableFilesSize;

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
        // Return null and proceed to size-based FIFO compaction if there are no files older than ttl, or deleting them
        // will not bring the total size below the size threshold.
        CompactionOptionsFifo fifo = Options.CompactionOptionsFifo;
        ulong effectiveRemaining = totalSize;
        if (fifo.MaxDataFilesSize > 0)
        {
            // Estimate the effective remaining data after dropping TTL-expired SSTs: each dropped SST also frees a
            // proportional share of blob data. The reference SST size is over all levels, like the blob total.
            ulong totalBlob = vstorage.TotalBlobFileSize();
            ulong originalL0Sst = TotalSize(levelFiles);
            ulong totalSstAllLevels = originalL0Sst;
            for (int level = 1; level < vstorage.NumLevels; ++level) totalSstAllLevels += TotalSize(vstorage.LevelFiles(level));
            ulong droppedSst = originalL0Sst - totalSize;
            ulong remainingSstAll = totalSstAllLevels - droppedSst;
            effectiveRemaining = totalSstAllLevels > 0 && totalBlob > 0
                ? remainingSstAll + (ulong)((double)remainingSstAll / totalSstAllLevels * totalBlob)
                : remainingSstAll;
        }
        if (inputs.IsEmpty || effectiveRemaining > GetEffectiveMax(fifo)) return null;
        return NewCompaction(vstorage, [inputs], 0, 0, 0, 0, new List<FileMetaData>(), CompactionReason.FifoTtl, vstorage.CompactionScore(0),
            l0FilesMightOverlap: true, isManual: false, isDeletion: true);
    }

    /// <summary>
    /// Port of FIFOCompactionPicker::PickSizeCompaction: when the column family (with max_data_files_size: SST plus blob
    /// files) exceeds the limit, delete the oldest L0 files, or during a migration from another style the files of the
    /// last non-empty level from the smallest keys. In blob-aware mode every file is counted with its proportional share
    /// of the data (effective size / number of files).
    /// </summary>
    private Compaction? PickSizeCompaction(VersionStorageInfo vstorage)
    {
        CompactionOptionsFifo fifo = Options.CompactionOptionsFifo;
        int lastLevel = 0;
        ulong totalSize = 0;
        for (int level = 0; level < vstorage.NumLevels; ++level)
        {
            ulong levelSize = TotalSize(vstorage.LevelFiles(level));
            totalSize += levelSize;
            if (levelSize > 0) lastLevel = level;
        }
        ReadOnlySpan<FileMetaData> lastLevelFiles = vstorage.LevelFiles(lastLevel);
        // GetEffectiveSizeAndLimit
        ulong effectiveSize = totalSize;
        ulong effectiveMax = fifo.MaxTableFilesSize;
        if (fifo.MaxDataFilesSize > 0)
        {
            effectiveSize += vstorage.TotalBlobFileSize();
            effectiveMax = fifo.MaxDataFilesSize;
        }
        if (lastLevel == 0 && effectiveSize <= effectiveMax) return null;
        if (IsLevel0CompactionInProgress) return null;
        var inputs = new CompactionInputFiles(lastLevel);
        ulong numFiles = (ulong)lastLevelFiles.Length;
        // Proportional estimate of data per file (SST + blob); max(1) prevents stalling when effective_size < num_files.
        ulong dataPerFile = fifo.MaxDataFilesSize > 0 && numFiles > 0 ? Math.Max(effectiveSize / numFiles, 1UL) : 0;
        if (lastLevel == 0)
        {
            ulong remaining = effectiveSize;
            // In L0, right-most files are the oldest files.
            for (int i = lastLevelFiles.Length - 1; i >= 0; i--)
            {
                FileMetaData f = lastLevelFiles[i];
                remaining -= Math.Min(remaining, fifo.MaxDataFilesSize > 0 ? dataPerFile : f.FileSize);
                inputs.Files.Add(f);
                if (remaining <= effectiveMax) break;
            }
        }
        else if (effectiveSize > effectiveMax)
        {
            // If the last level is non-L0, we don't know which file is logically the oldest: delete files from the left
            // (smallest keys first).
            foreach (FileMetaData f in lastLevelFiles)
            {
                if (f.BeingCompacted) continue;
                effectiveSize -= Math.Min(effectiveSize, fifo.MaxDataFilesSize > 0 ? dataPerFile : f.FileSize);
                inputs.Files.Add(f);
                if (effectiveSize <= effectiveMax) break;
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

    /// <summary>
    /// Port of FIFOCompactionPicker::PickTemperatureChangeCompaction: from the oldest L0 file, the first file whose
    /// data passed an age threshold of file_temperature_age_thresholds and whose temperature differs from that
    /// threshold's is rewritten (alone) with the new temperature. Single-level FIFO only.
    /// </summary>
    private Compaction? PickTemperatureChangeCompaction(VersionStorageInfo vstorage)
    {
        List<FileTemperatureAge> ages = Options.CompactionOptionsFifo.FileTemperatureAgeThresholds;
        if (ages.Count == 0) return null;
        // Does not apply to multi-level FIFO.
        if (vstorage.NumLevels > 1) return null;
        ReadOnlySpan<FileMetaData> levelFiles = vstorage.LevelFiles(0);
        if (levelFiles.Length == 0) return null;
        ulong currentTime = (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        if (IsLevel0CompactionInProgress) return null; // parallel compactions are not supported
        var inputs = new CompactionInputFiles(0);
        Temperature target = Temperature.LastTemperature; // to be determined
        ulong minAge = ages[0].Age;
        if (currentTime > minAge)
        {
            ulong createTimeThreshold = currentTime - minAge;
            for (int index = levelFiles.Length; index >= 1; --index)
            {
                // Try to add cur_file to compaction inputs.
                FileMetaData cur = levelFiles[index - 1];
                FileMetaData? prev = index < 2 ? null : levelFiles[index - 2];
                // Should not happen since we check for level0_compactions_in_progress_ above.
                if (cur.BeingCompacted) return null;
                ulong est = VersionStorageInfo.TryGetNewestKeyTime(cur, prev);
                // Newer file could have newest_key_time populated
                if (est == 0) continue;
                if (est > createTimeThreshold) break;
                Temperature curTarget = VersionStorageInfo.FifoTargetTemperature(ages, currentTime, est);
                if (cur.Temperature == curTarget) continue;
                // cur_file needs to change temperature
                target = curTarget;
                inputs.Files.Add(cur);
                break;
            }
        }
        if (inputs.IsEmpty) return null;
        // Only compact one file at a time; the output keeps the CF's compression.
        return NewCompaction(vstorage, [inputs], 0, 0, 0, 0, new List<FileMetaData>(), CompactionReason.ChangeTemperature, vstorage.CompactionScore(0),
            l0FilesMightOverlap: true, isManual: false, isDeletion: false, trivialMoveForced: false, disallowTrivialMove: false,
            enableCompression: true, target);
    }

    /// <summary>Port of FIFOCompactionPicker::PickIntraL0Compaction.</summary>
    private Compaction? PickIntraL0Compaction(VersionStorageInfo vstorage)
    {
        CompactionOptionsFifo fifo = Options.CompactionOptionsFifo;
        if (!fifo.AllowCompaction) return null;
        ReadOnlySpan<FileMetaData> level0 = vstorage.LevelFiles(0);
        if (level0.Length == 0) return null;
        if (fifo.UseKvRatioCompaction)
        {
            // "FIFO kv-ratio compaction: skipping -- max_data_files_size is 0 / < max_table_files_size"; then "falling
            // back to cost-based intra-L0 compaction".
            if (fifo.MaxDataFilesSize != 0 && fifo.MaxDataFilesSize >= fifo.MaxTableFilesSize) return PickRatioBasedIntraL0Compaction(vstorage);
        }
        // Old intra-L0 path: disallow compactions with files larger than the memtable (+10%) so files may still TTL-expire.
        ulong maxCompactBytesPerDelFile = MultiplyCheckOverflow(Options.WriteBufferSize, 1.1);
        var inputs = new CompactionInputFiles(0);
        if (!PickCostBasedIntraL0Compaction(level0, Options.Level0FileNumCompactionTrigger, maxCompactBytesPerDelFile, Options.MaxCompactionBytes, inputs))
        {
            return null;
        }
        return NewCompaction(vstorage, [inputs], 0, 16UL * 1024 * 1024, 0, 0, new List<FileMetaData>(), CompactionReason.FifoReduceNumFiles,
            vstorage.CompactionScore(0), l0FilesMightOverlap: true, isManual: false);
    }

    /// <summary>
    /// Port of FIFOCompactionPicker::PickRatioBasedIntraL0Compaction: tiered merging of L0 files towards a target file
    /// size (max_compaction_bytes when set, else max_data_files_size * SST share / level0_file_num_compaction_trigger),
    /// with tier boundaries target / trigger^k (at least 10 KiB); the smallest tier with a contiguous run of at least
    /// two files reaching its boundary is merged (output capped near 2x the boundary).
    /// </summary>
    private Compaction? PickRatioBasedIntraL0Compaction(VersionStorageInfo vstorage)
    {
        CompactionOptionsFifo fifo = Options.CompactionOptionsFifo;
        // During migration from level/universal compaction to FIFO, non-L0 levels may still contain files.
        for (int level = 1; level < vstorage.NumLevels; ++level)
        {
            if (vstorage.LevelFiles(level).Length > 0) return null;
        }
        if (IsLevel0CompactionInProgress) return null;
        ReadOnlySpan<FileMetaData> level0 = vstorage.LevelFiles(0);
        // trigger == 1 would loop forever in the tier boundary loop.
        if (Options.Level0FileNumCompactionTrigger <= 1) return null;
        ulong trigger = (ulong)Options.Level0FileNumCompactionTrigger;
        if ((ulong)level0.Length < trigger) return null;
        ulong target;
        if (Options.MaxCompactionBytes > 0)
        {
            // User explicitly set max_compaction_bytes -- use it as target.
            target = Options.MaxCompactionBytes;
        }
        else
        {
            // Auto-calculate from capacity and observed SST/blob ratio.
            ulong totalSst = TotalSize(level0);
            ulong totalBlob = vstorage.TotalBlobFileSize();
            ulong totalData = totalSst + totalBlob;
            if (totalData == 0 || totalSst == 0) return null;
            double sstRatio = totalBlob > 0 ? (double)totalSst / totalData : 1.0;
            ulong totalSstAtCap = (ulong)(fifo.MaxDataFilesSize * sstRatio);
            target = totalSstAtCap / trigger;
        }
        if (target == 0) return null;
        // Build tier boundaries from smallest to largest; stop at 10KB minimum.
        const ulong MinTierBoundary = 10 * 1024;
        var boundaries = new List<ulong>();
        for (ulong b = target; b >= MinTierBoundary; b /= trigger) boundaries.Add(b);
        // target itself is below kMinTierBoundary -- use target as the sole boundary.
        if (boundaries.Count == 0) boundaries.Add(target);
        boundaries.Reverse();
        // For each tier boundary (smallest first), scan L0 for mergeable batches (oldest file at the end).
        foreach (ulong boundary in boundaries)
        {
            for (int scan = level0.Length; scan > 0;)
            {
                // Skip files >= boundary (they belong to higher tiers) or in-progress.
                if (level0[scan - 1].FileSize >= boundary || level0[scan - 1].BeingCompacted)
                {
                    --scan;
                    continue;
                }
                // Found a file < boundary -- collect contiguous batch.
                var batch = new List<FileMetaData>();
                ulong accumulated = 0;
                int pos = scan;
                while (pos > 0 && level0[pos - 1].FileSize < boundary && !level0[pos - 1].BeingCompacted)
                {
                    // Don't let output exceed 2x boundary (prevent tier-skipping).
                    if (accumulated >= boundary && accumulated + level0[pos - 1].FileSize > boundary * 2) break;
                    batch.Add(level0[pos - 1]);
                    accumulated += level0[pos - 1].FileSize;
                    --pos;
                }
                // Viable: >= 2 files and accumulated >= boundary.
                if (batch.Count >= 2 && accumulated >= boundary)
                {
                    return NewCompaction(vstorage, [new CompactionInputFiles(0, batch)], 0, boundary, 0, 0, new List<FileMetaData>(),
                        CompactionReason.FifoReduceNumFiles, vstorage.CompactionScore(0), l0FilesMightOverlap: true, isManual: false);
                }
                // This batch wasn't enough -- advance past it.
                scan = pos;
            }
        }
        return null;
    }

    public override Compaction? PickCompaction(VersionStorageInfo vstorage, IReadOnlyList<ulong> snapshots)
    {
        Compaction? c = null;
        if (Options.Ttl > 0 && Options.Ttl < ulong.MaxValue - 1) c = PickTtlCompaction(vstorage);
        c ??= PickSizeCompaction(vstorage);
        // Intra-L0 compaction merges small files to reduce file count; it runs after size-based dropping.
        c ??= PickIntraL0Compaction(vstorage);
        c ??= PickTemperatureChangeCompaction(vstorage);
        return c;
    }

    /// <summary>Port of FIFOCompactionPicker::PickCompactionForCompactRange: runs the regular picker.</summary>
    public override Compaction? PickCompactionForCompactRange(VersionStorageInfo vstorage, int inputLevel, int outputLevel, uint maxSubcompactions,
        byte[]? begin, byte[]? end, out byte[]? compactionEnd, out bool manualConflict, ulong maxFileNumToIgnore, bool disallowTrivialMove,
        uint targetPathId)
    {
        compactionEnd = null;
        manualConflict = false;
        return PickCompaction(vstorage, []);
    }
}
