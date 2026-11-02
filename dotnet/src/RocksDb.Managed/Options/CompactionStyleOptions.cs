namespace RocksDb.Managed;

/// <summary>
/// Port of rocksdb::CompactionOptionsFIFO (include/rocksdb/advanced_options.h). Used when
/// <see cref="ColumnFamilyOptions.CompactionStyle"/> is <see cref="CompactionStyle.Fifo"/>.
/// Not persisted in OPTIONS files by the managed port yet (the raw <c>compaction_options_fifo</c> string is kept).
/// </summary>
public sealed class CompactionOptionsFifo
{
    /// <summary>Once the total size of table files reaches this, the oldest table files are deleted. Default 1GB.</summary>
    public ulong MaxTableFilesSize { get; set; } = 1024UL * 1024 * 1024;

    /// <summary>If true, try to do an intra-L0 compaction to reduce the number of files (size-tiered).</summary>
    public bool AllowCompaction { get; set; }

    public CompactionOptionsFifo Clone() => (CompactionOptionsFifo)MemberwiseClone();
}

/// <summary>Mirrors rocksdb::CompactionStopStyle.</summary>
public enum CompactionStopStyle : byte
{
    /// <summary>Pick files of similar size.</summary>
    SimilarSize = 0,
    /// <summary>Total size of picked files &gt; next file.</summary>
    TotalSize = 1,
}

/// <summary>
/// Port of rocksdb::CompactionOptionsUniversal (include/rocksdb/universal_compaction.h). Used when
/// <see cref="ColumnFamilyOptions.CompactionStyle"/> is <see cref="CompactionStyle.Universal"/>.
/// Not persisted in OPTIONS files by the managed port yet.
/// </summary>
public sealed class CompactionOptionsUniversal
{
    /// <summary>Percentage flexibility while comparing file size (default 1).</summary>
    public uint SizeRatio { get; set; } = 1;
    /// <summary>The minimum number of files in a single compaction run (default 2).</summary>
    public uint MinMergeWidth { get; set; } = 2;
    /// <summary>The maximum number of files in a single compaction run (default unlimited).</summary>
    public uint MaxMergeWidth { get; set; } = uint.MaxValue;
    /// <summary>Size amplification (percent) that triggers a full compaction (default 200).</summary>
    public uint MaxSizeAmplificationPercent { get; set; } = 200;
    /// <summary>-1 = compress all output; otherwise only the oldest data until this percentage is compressed.</summary>
    public int CompressionSizePercent { get; set; } = -1;
    public CompactionStopStyle StopStyle { get; set; } = CompactionStopStyle.TotalSize;
    /// <summary>Allow trivial moves of non-overlapping files.</summary>
    public bool AllowTrivialMove { get; set; }

    public CompactionOptionsUniversal Clone() => (CompactionOptionsUniversal)MemberwiseClone();
}
