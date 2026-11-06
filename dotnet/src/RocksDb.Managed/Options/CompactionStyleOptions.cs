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

    /// <summary>
    /// DEPRECATED (persisted only; upstream no longer acts on it either). Use <see cref="FileTemperatureAgeThresholds"/>.
    /// </summary>
    public ulong AgeForWarm { get; set; }

    /// <summary>
    /// EXPERIMENTAL. Age (seconds) thresholds for file temperatures, in increasing order of age: a file all of whose data
    /// is older than an entry's age is compacted (rewritten) to that entry's temperature, one file at a time, oldest file
    /// first. The managed file system has a single storage tier, so the temperature is metadata (MANIFEST,
    /// GetLiveFilesMetaData), as with a native FileSystem that ignores temperature hints.
    /// </summary>
    public List<FileTemperatureAge> FileTemperatureAgeThresholds { get; set; } = new();

    /// <summary>
    /// EXPERIMENTAL. Copy the file instead of rewriting it for a temperature change. Persisted; the managed port always
    /// rewrites (single tier).
    /// </summary>
    public bool AllowTrivialCopyWhenChangeTemperature { get; set; }

    /// <summary>EXPERIMENTAL. Buffer size of the trivial copy (persisted only). Default 4096.</summary>
    public ulong TrivialCopyBufferSize { get; set; } = 4096;

    /// <summary>
    /// When non-zero, size-based trimming, the TTL size check and the compaction score use the combined size of the
    /// table files and the blob files, and this limit replaces <see cref="MaxTableFilesSize"/>. Default 0 (use
    /// MaxTableFilesSize).
    /// </summary>
    public ulong MaxDataFilesSize { get; set; }

    /// <summary>
    /// Capacity-derived intra-L0 compaction for BlobDB workloads: compacts L0 files into files of a target size derived
    /// from <see cref="MaxDataFilesSize"/>, level0_file_num_compaction_trigger and the observed table/blob size ratio
    /// (max_compaction_bytes overrides the target when non-zero). Requires <see cref="AllowCompaction"/> and
    /// MaxDataFilesSize; otherwise the cost-based intra-L0 compaction is used. Default false.
    /// </summary>
    public bool UseKvRatioCompaction { get; set; }

    private static readonly OptionTable<CompactionOptionsFifo> s_table = new OptionTable<CompactionOptionsFifo>()
        .UInt64("max_table_files_size", static o => o.MaxTableFilesSize, static (o, v) => o.MaxTableFilesSize = v)
        .Bool("allow_compaction", static o => o.AllowCompaction, static (o, v) => o.AllowCompaction = v)
        .UInt64("age_for_warm", static o => o.AgeForWarm, static (o, v) => o.AgeForWarm = v)
        .Add("file_temperature_age_thresholds", static o => FileTemperatureAge.FormatVector(o.FileTemperatureAgeThresholds),
            static (o, v) => o.FileTemperatureAgeThresholds = FileTemperatureAge.ParseVector("file_temperature_age_thresholds", v))
        .Bool("allow_trivial_copy_when_change_temperature", static o => o.AllowTrivialCopyWhenChangeTemperature,
            static (o, v) => o.AllowTrivialCopyWhenChangeTemperature = v)
        .UInt64("trivial_copy_buffer_size", static o => o.TrivialCopyBufferSize, static (o, v) => o.TrivialCopyBufferSize = v)
        .UInt64("max_data_files_size", static o => o.MaxDataFilesSize, static (o, v) => o.MaxDataFilesSize = v)
        .Bool("use_kv_ratio_compaction", static o => o.UseKvRatioCompaction, static (o, v) => o.UseKvRatioCompaction = v)
        .Deprecated("ttl");

    internal override OptionTable<CompactionOptionsFifo> OptionTable => s_table;

    public CompactionOptionsFifo Clone()
    {
        var c = new CompactionOptionsFifo();
        c.CopyFrom(this);
        c.FileTemperatureAgeThresholds = new List<FileTemperatureAge>(FileTemperatureAgeThresholds);
        return c;
    }

    internal string ToStructString() => OptionStrings.FormatStruct(ToOptionsList());

    /// <summary>
    /// Upstream's parse function for compaction_options_fifo (options/cf_options.cc): a single scalar (no '=') is the old
    /// format and is assigned to max_table_files_size.
    /// </summary>
    internal static void ParseLegacy(CompactionOptionsFifo o, string value) =>
        o.MaxTableFilesSize = OptionStrings.ParseUInt64("compaction_options_fifo", value);
}

/// <summary>Port of rocksdb::FileTemperatureAge (include/rocksdb/advanced_options.h).</summary>
public readonly record struct FileTemperatureAge(Temperature Temperature, ulong Age)
{
    /// <summary>
    /// SerializeVector with ':' separators over the struct {temperature;age} (options/cf_options.cc
    /// file_temperature_age_type_info): "{{temperature=kWarm;age=10;}:{temperature=kCold;age=20;}}", empty for none.
    /// </summary>
    internal static string FormatVector(List<FileTemperatureAge> values)
    {
        if (values.Count == 0) return "";
        var sb = new System.Text.StringBuilder("{");
        for (int i = 0; i < values.Count; i++)
        {
            if (i > 0) sb.Append(':');
            sb.Append("{temperature=").Append(OptionEnumMaps.Temperature.ToName(values[i].Temperature))
              .Append(";age=").Append(values[i].Age.ToString(System.Globalization.CultureInfo.InvariantCulture)).Append(";}");
        }
        return sb.Append('}').ToString();
    }

    /// <summary>ParseVector: elements separated by ':' outside braces, each a struct.</summary>
    internal static List<FileTemperatureAge> ParseVector(string name, string value)
    {
        var result = new List<FileTemperatureAge>();
        string v = value.Trim();
        if (v.Length >= 2 && v[0] == '{' && v[^1] == '}' && IsSingleGroup(v)) v = v[1..^1].Trim();
        int depth = 0, start = 0;
        for (int i = 0; i <= v.Length; i++)
        {
            if (i == v.Length || (v[i] == ':' && depth == 0))
            {
                string elem = v[start..i].Trim();
                if (elem.Length > 0 && elem != "{}") result.Add(ParseElement(name, elem));
                start = i + 1;
            }
            else if (v[i] == '{') depth++;
            else if (v[i] == '}') depth--;
        }
        return result;
    }

    // True if the braces at both ends enclose the whole value (and are not "{a}:{b}").
    private static bool IsSingleGroup(string v)
    {
        int depth = 0;
        for (int i = 0; i < v.Length; i++)
        {
            if (v[i] == '{') depth++;
            else if (v[i] == '}' && --depth == 0 && i != v.Length - 1) return false;
        }
        return true;
    }

    private static FileTemperatureAge ParseElement(string name, string elem)
    {
        Temperature t = Temperature.Unknown;
        ulong age = 0;
        foreach (KeyValuePair<string, string> kv in OptionStrings.ParseStruct(name, elem))
        {
            if (kv.Key == "temperature") t = OptionEnumMaps.Temperature.Parse(name, kv.Value);
            else if (kv.Key == "age") age = OptionStrings.ParseUInt64(name, kv.Value);
            else throw RocksDbException.InvalidArgument("Unrecognized option " + name + "." + kv.Key);
        }
        return new FileTemperatureAge(t, age);
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
/// members the managed port does not model are preserved verbatim.
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
    /// <summary>
    /// Limit on the number of sorted runs: -1 (default) = level0_file_num_compaction_trigger; 0 = auto-tuned from the
    /// largest sorted run, size_ratio and write_buffer_size (total-size stop style only); N &gt; 0 = at most N.
    /// </summary>
    public int MaxReadAmp { get; set; } = -1;
    /// <summary>
    /// EXPERIMENTAL. Size-amplification compactions try to compact only part of the second-to-last sorted run (and the
    /// overlapping part of the last one), limited by max_compaction_bytes, instead of everything. Default false.
    /// </summary>
    public bool Incremental { get; set; }
    /// <summary>
    /// When a compaction is forwarded to the bottom-priority pool (<see cref="DbOptions.BottomPriorityBackgroundThreads"/>),
    /// reserve only its newest input sorted run while it waits and pick the compaction afresh when a bottom thread runs
    /// it, so other compactions can use the remaining files. Despite the name it applies to every compaction style, like
    /// upstream. No effect without a bottom-priority pool and on manual compactions. Default true.
    /// </summary>
    public bool ReduceFileLocking { get; set; } = true;

    private static readonly OptionTable<CompactionOptionsUniversal> s_table = new OptionTable<CompactionOptionsUniversal>()
        .UInt32("size_ratio", static o => o.SizeRatio, static (o, v) => o.SizeRatio = v)
        .UInt32("min_merge_width", static o => o.MinMergeWidth, static (o, v) => o.MinMergeWidth = v)
        .UInt32("max_merge_width", static o => o.MaxMergeWidth, static (o, v) => o.MaxMergeWidth = v)
        .UInt32("max_size_amplification_percent", static o => o.MaxSizeAmplificationPercent, static (o, v) => o.MaxSizeAmplificationPercent = v)
        .Int32("compression_size_percent", static o => o.CompressionSizePercent, static (o, v) => o.CompressionSizePercent = v)
        .Enum("stop_style", OptionEnumMaps.CompactionStopStyle, static o => o.StopStyle, static (o, v) => o.StopStyle = v)
        .Bool("allow_trivial_move", static o => o.AllowTrivialMove, static (o, v) => o.AllowTrivialMove = v)
        .Int32("max_read_amp", static o => o.MaxReadAmp, static (o, v) => o.MaxReadAmp = v)
        .Bool("incremental", static o => o.Incremental, static (o, v) => o.Incremental = v)
        .Bool("reduce_file_locking", static o => o.ReduceFileLocking, static (o, v) => o.ReduceFileLocking = v);

    internal override OptionTable<CompactionOptionsUniversal> OptionTable => s_table;

    public CompactionOptionsUniversal Clone()
    {
        var c = new CompactionOptionsUniversal();
        c.CopyFrom(this);
        return c;
    }

    internal string ToStructString() => OptionStrings.FormatStruct(ToOptionsList());
}
