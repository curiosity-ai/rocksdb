using RocksDb.Managed.Options;

namespace RocksDb.Managed;

/// <summary>
/// Port of rocksdb::CompressionOptions (include/rocksdb/advanced_compression.h). Serialized in the OPTIONS file as a
/// struct value, e.g. <c>compression_opts={window_bits=-14;level=32767;...;}</c>.
/// </summary>
public sealed class CompressionOptions : PersistedOptions<CompressionOptions>
{
    /// <summary>CompressionOptions::kDefaultCompressionLevel.</summary>
    public const int DefaultCompressionLevel = 32767;

    public int WindowBits { get; set; } = -14;
    public int Level { get; set; } = DefaultCompressionLevel;
    public int Strategy { get; set; }
    public uint MaxDictBytes { get; set; }
    public uint ZstdMaxTrainBytes { get; set; }
    public uint ParallelThreads { get; set; } = 1;
    /// <summary>Only meaningful for bottommost_compression_opts: whether these options are used.</summary>
    public bool Enabled { get; set; }
    public ulong MaxDictBufferBytes { get; set; }
    public bool UseZstdDictTrainer { get; set; } = true;
    public int MaxCompressedBytesPerKb { get; set; } = 1024 * 7 / 8;
    public bool Checksum { get; set; }

    /// <summary>
    /// auto_skip (EXPERIMENTAL; include/rocksdb/compression_type.h): skip compression of data blocks while recent blocks
    /// did not reach the <see cref="MaxCompressedBytesPerKb"/> ratio. Typed and persisted only: the managed table builder
    /// always attempts compression (listed under "Not ported yet"). Default false.
    /// </summary>
    public bool AutoSkip { get; set; }

    /// <summary>
    /// auto_skip_min_sample_every (EXPERIMENTAL): while skipping, roughly one of this many blocks is still compressed
    /// (0 = internal default). Persisted only, like <see cref="AutoSkip"/>.
    /// </summary>
    public int AutoSkipMinSampleEvery { get; set; }

    // Registered in the order native 11.12 (libstdc++ unordered_map iteration) writes the members, so the struct value is
    // byte-identical to native's.
    private static readonly OptionTable<CompressionOptions> s_table = new OptionTable<CompressionOptions>()
        .Bool("checksum", static o => o.Checksum, static (o, v) => o.Checksum = v)
        .UInt64("max_dict_buffer_bytes", static o => o.MaxDictBufferBytes, static (o, v) => o.MaxDictBufferBytes = v)
        .UInt32("max_dict_bytes", static o => o.MaxDictBytes, static (o, v) => o.MaxDictBytes = v)
        .Int32("auto_skip_min_sample_every", static o => o.AutoSkipMinSampleEvery, static (o, v) => o.AutoSkipMinSampleEvery = v)
        .Bool("enabled", static o => o.Enabled, static (o, v) => o.Enabled = v)
        .Bool("auto_skip", static o => o.AutoSkip, static (o, v) => o.AutoSkip = v)
        .Int32("max_compressed_bytes_per_kb", static o => o.MaxCompressedBytesPerKb, static (o, v) => o.MaxCompressedBytesPerKb = v)
        .UInt32("parallel_threads", static o => o.ParallelThreads, static (o, v) => o.ParallelThreads = v)
        .UInt32("zstd_max_train_bytes", static o => o.ZstdMaxTrainBytes, static (o, v) => o.ZstdMaxTrainBytes = v)
        .Int32("level", static o => o.Level, static (o, v) => o.Level = v)
        .Bool("use_zstd_dict_trainer", static o => o.UseZstdDictTrainer, static (o, v) => o.UseZstdDictTrainer = v)
        .Int32("strategy", static o => o.Strategy, static (o, v) => o.Strategy = v)
        .Int32("window_bits", static o => o.WindowBits, static (o, v) => o.WindowBits = v);

    internal override OptionTable<CompressionOptions> OptionTable => s_table;

    public CompressionOptions Clone()
    {
        var c = new CompressionOptions();
        c.CopyFrom(this);
        return c;
    }

    internal string ToStructString() => OptionStrings.FormatStruct(ToOptionsList());

    /// <summary>
    /// Port of ParseCompressionOptions (options/cf_options.cc): the legacy ':'-separated form
    /// "window_bits:level:strategy[:max_dict_bytes[:zstd_max_train_bytes[:parallel_threads]:enabled[:max_dict_buffer_bytes[:use_zstd_dict_trainer]]]]",
    /// accepted for compression_opts / bottommost_compression_opts given without '='. As upstream, a 6-field form reads
    /// the 6th field as <c>enabled</c> (parallel_threads is only read when more fields follow).
    /// </summary>
    internal static void ParseLegacy(CompressionOptions o, string value, string name)
    {
        string[] f = value.Split(':');
        if (f.Length < 3 || f.Length > 9) ThrowHelper.InvalidArgument("unable to parse the specified CF option " + name);
        o.WindowBits = OptionStrings.ParseInt32(name, f[0]);
        o.Level = OptionStrings.ParseInt32(name, f[1]);
        o.Strategy = OptionStrings.ParseInt32(name, f[2]);
        int i = 3;
        if (i < f.Length) o.MaxDictBytes = unchecked((uint)OptionStrings.ParseInt32(name, f[i++]));
        if (i < f.Length) o.ZstdMaxTrainBytes = unchecked((uint)OptionStrings.ParseInt32(name, f[i++]));
        if (i < f.Length)
        {
            if (i + 1 < f.Length) o.ParallelThreads = unchecked((uint)OptionStrings.ParseInt32(name, f[i++]));
            else o.Enabled = OptionStrings.ParseBool(name, f[i++]);
        }
        if (i < f.Length) o.Enabled = OptionStrings.ParseBool(name, f[i++]);
        if (i < f.Length) o.MaxDictBufferBytes = OptionStrings.ParseUInt64(name, f[i++]);
        if (i < f.Length) o.UseZstdDictTrainer = OptionStrings.ParseBool(name, f[i++]);
    }
}
