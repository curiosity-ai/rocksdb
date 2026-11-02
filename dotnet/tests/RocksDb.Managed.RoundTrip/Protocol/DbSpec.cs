using RocksDb.Managed.Compression;
using System.Text.Json;

namespace RocksDb.Managed.RoundTrip.Protocol;

/// <summary>Merge operator configured on a column family. Both engines use the upstream semantics.</summary>
public enum MergeKind
{
    None,
    /// <summary>rocksdb::UInt64AddOperator: 8-byte little-endian sums; wrong-sized values count as 0.</summary>
    UInt64Add,
    /// <summary>rocksdb::StringAppendOperator with delimiter ','.</summary>
    StringAppend,
}

/// <summary>Filter policy configured on a column family (10 bits per key).</summary>
public enum FilterKind
{
    None,
    /// <summary>Full Bloom filter (NewBloomFilterPolicy(10)).</summary>
    Bloom,
    /// <summary>Ribbon filter (NewRibbonFilterPolicy(10), bloom_before_level 0).</summary>
    Ribbon,
}

/// <summary>
/// Options of one column family, expressed engine-neutrally so the parent can hand the same configuration to the
/// native child (rocksdb-sharp setters) and to the managed engine (<see cref="ColumnFamilyOptions"/>).
/// </summary>
public sealed class CfSpec
{
    public string Name { get; set; } = "default";
    /// <summary>None, Snappy, LZ4 or Zstd.</summary>
    public CompressionType Compression { get; set; } = CompressionType.LZ4;
    public int FormatVersion { get; set; } = 6;
    public FilterKind Filter { get; set; } = FilterKind.Bloom;
    public ulong BlockSize { get; set; } = 4096;
    public ulong WriteBufferSize { get; set; } = 64UL << 20;
    public bool DisableAutoCompactions { get; set; }
    public MergeKind Merge { get; set; } = MergeKind.StringAppend;
    /// <summary>Created when the DB is first created (others are only created by a createcf op).</summary>
    public bool Initial { get; set; } = true;

    public CfSpec Clone() => (CfSpec)MemberwiseClone();

    public override string ToString() =>
        $"{Name}[{Compression},fv{FormatVersion},{Filter},bs{BlockSize},wb{WriteBufferSize},{(DisableAutoCompactions ? "noauto" : "auto")},{Merge}]";
}

/// <summary>How a database is opened by either engine. Serialized as JSON for the child process.</summary>
public sealed class DbSpec
{
    /// <summary>Catalog of column families: every CF that exists (or may be created by a createcf op).</summary>
    public List<CfSpec> ColumnFamilies { get; set; } = [];

    /// <summary>When positive the DB is a TTL DB (native OpenWithTtl / managed TtlDatabase), default CF only.</summary>
    public int TtlSeconds { get; set; }

    /// <summary>
    /// Open with the options of the DB's latest OPTIONS file (native rocksdb_load_latest_options / managed
    /// <see cref="OptionsFile"/>) instead of building them from <see cref="ColumnFamilies"/>.
    /// </summary>
    public bool OptionsFromFile { get; set; }

    public CfSpec Cf(string name)
    {
        foreach (CfSpec cf in ColumnFamilies)
        {
            if (cf.Name == name) return cf;
        }
        throw new InvalidOperationException("column family not in spec catalog: " + name);
    }

    public DbSpec Clone()
    {
        var c = new DbSpec { TtlSeconds = TtlSeconds, OptionsFromFile = OptionsFromFile };
        foreach (CfSpec cf in ColumnFamilies) c.ColumnFamilies.Add(cf.Clone());
        return c;
    }

    public override string ToString() =>
        string.Join(" ", ColumnFamilies) + (TtlSeconds > 0 ? " ttl=" + TtlSeconds : "") + (OptionsFromFile ? " options-from-file" : "");

    private static readonly JsonSerializerOptions s_json = new() { WriteIndented = true };

    public string ToJson() => JsonSerializer.Serialize(this, s_json);

    public static DbSpec FromJson(string json) => JsonSerializer.Deserialize<DbSpec>(json, s_json)
        ?? throw new InvalidDataException("empty DbSpec");

    /// <summary>default + three CFs with deliberately different options and merge operators.</summary>
    public static DbSpec Standard(CompressionType compression, int formatVersion, FilterKind filter, ulong writeBufferSize, bool disableAutoCompactions)
    {
        var spec = new DbSpec();
        spec.ColumnFamilies.Add(new CfSpec
        {
            Name = "default", Compression = compression, FormatVersion = formatVersion, Filter = filter,
            WriteBufferSize = writeBufferSize, DisableAutoCompactions = disableAutoCompactions, Merge = MergeKind.StringAppend,
        });
        spec.ColumnFamilies.Add(new CfSpec
        {
            Name = "counters", Compression = CompressionType.Zstd, FormatVersion = 5, Filter = FilterKind.Ribbon, BlockSize = 1024,
            WriteBufferSize = writeBufferSize, DisableAutoCompactions = disableAutoCompactions, Merge = MergeKind.UInt64Add,
        });
        spec.ColumnFamilies.Add(new CfSpec
        {
            Name = "blobs", Compression = CompressionType.None, FormatVersion = 7, Filter = FilterKind.None, BlockSize = 16 * 1024,
            WriteBufferSize = Math.Max(writeBufferSize, 256 * 1024), DisableAutoCompactions = disableAutoCompactions, Merge = MergeKind.None,
        });
        spec.ColumnFamilies.Add(new CfSpec
        {
            Name = "snappy", Compression = CompressionType.Snappy, FormatVersion = 6, Filter = FilterKind.Bloom, BlockSize = 2048,
            WriteBufferSize = writeBufferSize, DisableAutoCompactions = disableAutoCompactions, Merge = MergeKind.StringAppend,
        });
        // Not created initially: exercised by createcf / dropcf ops.
        spec.ColumnFamilies.Add(new CfSpec
        {
            Name = "late", Compression = CompressionType.LZ4, FormatVersion = 7, Filter = FilterKind.Ribbon, WriteBufferSize = writeBufferSize,
            DisableAutoCompactions = disableAutoCompactions, Merge = MergeKind.StringAppend, Initial = false,
        });
        return spec;
    }
}
