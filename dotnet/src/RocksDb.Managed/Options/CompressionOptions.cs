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

    private static readonly OptionTable<CompressionOptions> s_table = new OptionTable<CompressionOptions>()
        .Int32("window_bits", static o => o.WindowBits, static (o, v) => o.WindowBits = v)
        .Int32("level", static o => o.Level, static (o, v) => o.Level = v)
        .Int32("strategy", static o => o.Strategy, static (o, v) => o.Strategy = v)
        .UInt32("max_dict_bytes", static o => o.MaxDictBytes, static (o, v) => o.MaxDictBytes = v)
        .UInt32("zstd_max_train_bytes", static o => o.ZstdMaxTrainBytes, static (o, v) => o.ZstdMaxTrainBytes = v)
        .UInt32("parallel_threads", static o => o.ParallelThreads, static (o, v) => o.ParallelThreads = v)
        .Bool("enabled", static o => o.Enabled, static (o, v) => o.Enabled = v)
        .UInt64("max_dict_buffer_bytes", static o => o.MaxDictBufferBytes, static (o, v) => o.MaxDictBufferBytes = v)
        .Bool("use_zstd_dict_trainer", static o => o.UseZstdDictTrainer, static (o, v) => o.UseZstdDictTrainer = v)
        .Int32("max_compressed_bytes_per_kb", static o => o.MaxCompressedBytesPerKb, static (o, v) => o.MaxCompressedBytesPerKb = v)
        .Bool("checksum", static o => o.Checksum, static (o, v) => o.Checksum = v);

    internal override OptionTable<CompressionOptions> OptionTable => s_table;

    public CompressionOptions Clone()
    {
        var c = new CompressionOptions();
        c.CopyFrom(this);
        return c;
    }

    internal string ToStructString() => OptionStrings.FormatStruct(ToOptionsList());

    internal static CompressionOptions ParseStructString(string name, string value)
    {
        var c = new CompressionOptions();
        c.SetOptions(OptionStrings.ParseStruct(name, value), preserveUnknown: true);
        return c;
    }
}
