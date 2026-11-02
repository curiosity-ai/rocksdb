using RocksDb.Managed.Options;

namespace RocksDb.Managed;

/// <summary>
/// Port of rocksdb::CompactionOptionsFIFO (include/rocksdb/advanced_options.h). Used when
/// <see cref="ColumnFamilyOptions.CompactionStyle"/> is <see cref="CompactionStyle.Fifo"/>. Persisted in the OPTIONS
/// file as the struct value <c>compaction_options_fifo={max_table_files_size=...;allow_compaction=...;}</c>
/// (fifo_compaction_options_type_info, options/cf_options.cc); members the managed port does not model are preserved
/// verbatim.
/// </summary>
public sealed class CompactionOptionsFifo : PersistedOptions<CompactionOptionsFifo>
{
    /// <summary>Once the total size of table files reaches this, the oldest table files are deleted. Default 1GB.</summary>
    public ulong MaxTableFilesSize { get; set; } = 1024UL * 1024 * 1024;

    /// <summary>If true, try to do an intra-L0 compaction to reduce the number of files (size-tiered).</summary>
    public bool AllowCompaction { get; set; }

    private static readonly OptionTable<CompactionOptionsFifo> s_table = new OptionTable<CompactionOptionsFifo>()
        .UInt64("max_table_files_size", static o => o.MaxTableFilesSize, static (o, v) => o.MaxTableFilesSize = v)
        .Bool("allow_compaction", static o => o.AllowCompaction, static (o, v) => o.AllowCompaction = v);

    internal override OptionTable<CompactionOptionsFifo> OptionTable => s_table;

    public CompactionOptionsFifo Clone()
    {
        var c = new CompactionOptionsFifo();
        c.CopyFrom(this);
        return c;
    }

    internal string ToStructString() => OptionStrings.FormatStruct(ToOptionsList());

    /// <summary>
    /// Parses the struct value. Like upstream's parse function for compaction_options_fifo, a single scalar (no '=')
    /// is assigned to max_table_files_size.
    /// </summary>
    internal static CompactionOptionsFifo ParseStructString(string name, string value)
    {
        var c = new CompactionOptionsFifo();
        if (!value.Contains('=', StringComparison.Ordinal))
        {
            c.SetOption("max_table_files_size", value.Trim(), preserveUnknown: false);
            return c;
        }
        c.SetOptions(OptionStrings.ParseStruct(name, value), preserveUnknown: true);
        return c;
    }
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
/// <see cref="ColumnFamilyOptions.CompactionStyle"/> is <see cref="CompactionStyle.Universal"/>. Persisted in the
/// OPTIONS file as the struct value <c>compaction_options_universal={...}</c> (universal_compaction_options_type_info);
/// members the managed port does not model (incremental, max_read_amp, reduce_file_locking) are preserved verbatim.
/// </summary>
public sealed class CompactionOptionsUniversal : PersistedOptions<CompactionOptionsUniversal>
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

    private static readonly OptionTable<CompactionOptionsUniversal> s_table = new OptionTable<CompactionOptionsUniversal>()
        .UInt32("size_ratio", static o => o.SizeRatio, static (o, v) => o.SizeRatio = v)
        .UInt32("min_merge_width", static o => o.MinMergeWidth, static (o, v) => o.MinMergeWidth = v)
        .UInt32("max_merge_width", static o => o.MaxMergeWidth, static (o, v) => o.MaxMergeWidth = v)
        .UInt32("max_size_amplification_percent", static o => o.MaxSizeAmplificationPercent, static (o, v) => o.MaxSizeAmplificationPercent = v)
        .Int32("compression_size_percent", static o => o.CompressionSizePercent, static (o, v) => o.CompressionSizePercent = v)
        .Enum("stop_style", OptionEnumMaps.CompactionStopStyle, static o => o.StopStyle, static (o, v) => o.StopStyle = v)
        .Bool("allow_trivial_move", static o => o.AllowTrivialMove, static (o, v) => o.AllowTrivialMove = v);

    internal override OptionTable<CompactionOptionsUniversal> OptionTable => s_table;

    public CompactionOptionsUniversal Clone()
    {
        var c = new CompactionOptionsUniversal();
        c.CopyFrom(this);
        return c;
    }

    internal string ToStructString() => OptionStrings.FormatStruct(ToOptionsList());

    internal static CompactionOptionsUniversal ParseStructString(string name, string value)
    {
        var c = new CompactionOptionsUniversal();
        c.SetOptions(OptionStrings.ParseStruct(name, value), preserveUnknown: true);
        return c;
    }
}
