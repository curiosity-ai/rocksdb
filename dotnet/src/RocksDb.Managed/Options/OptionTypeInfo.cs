using System.Globalization;
using System.Text;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Table;

namespace RocksDb.Managed.Options;

/// <summary>
/// One serializable option: its OPTIONS-file name plus string (de)serializers. Port of the OptionTypeInfo tables in
/// options/cf_options.cc, options/db_options.cc and table/block_based/block_based_table_factory.cc, restricted to
/// what the managed options model exposes as typed properties.
/// </summary>
internal sealed class OptionTypeInfo<TOwner>
{
    public OptionTypeInfo(string name, Func<TOwner, string> serialize, Action<TOwner, string> parse)
    {
        Name = name;
        Serialize = serialize;
        Parse = parse;
    }

    public string Name { get; }
    public Func<TOwner, string> Serialize { get; }
    /// <summary>Parses the (unescaped) value; throws <see cref="RocksDbException"/> (InvalidArgument) on bad input.</summary>
    public Action<TOwner, string> Parse { get; }
}

/// <summary>Builder + lookup for an option table.</summary>
internal sealed class OptionTable<TOwner>
{
    private readonly List<OptionTypeInfo<TOwner>> _list = new();
    private readonly Dictionary<string, OptionTypeInfo<TOwner>> _byName = new(StringComparer.Ordinal);

    public IReadOnlyList<OptionTypeInfo<TOwner>> All => _list;

    public bool TryGet(string name, out OptionTypeInfo<TOwner> info) => _byName.TryGetValue(name, out info!);

    public OptionTable<TOwner> Add(string name, Func<TOwner, string> serialize, Action<TOwner, string> parse)
    {
        var info = new OptionTypeInfo<TOwner>(name, serialize, parse);
        _list.Add(info);
        _byName.Add(name, info);
        return this;
    }

    public OptionTable<TOwner> Bool(string name, Func<TOwner, bool> get, Action<TOwner, bool> set) =>
        Add(name, o => OptionStrings.FormatBool(get(o)), (o, v) => set(o, OptionStrings.ParseBool(name, v)));

    public OptionTable<TOwner> Int32(string name, Func<TOwner, int> get, Action<TOwner, int> set) =>
        Add(name, o => get(o).ToString(CultureInfo.InvariantCulture), (o, v) => set(o, checked((int)OptionStrings.ParseInt64(name, v))));

    public OptionTable<TOwner> UInt32(string name, Func<TOwner, uint> get, Action<TOwner, uint> set) =>
        Add(name, o => get(o).ToString(CultureInfo.InvariantCulture), (o, v) => set(o, checked((uint)OptionStrings.ParseUInt64(name, v))));

    public OptionTable<TOwner> Int64(string name, Func<TOwner, long> get, Action<TOwner, long> set) =>
        Add(name, o => get(o).ToString(CultureInfo.InvariantCulture), (o, v) => set(o, OptionStrings.ParseInt64(name, v)));

    public OptionTable<TOwner> UInt64(string name, Func<TOwner, ulong> get, Action<TOwner, ulong> set) =>
        Add(name, o => get(o).ToString(CultureInfo.InvariantCulture), (o, v) => set(o, OptionStrings.ParseUInt64(name, v)));

    public OptionTable<TOwner> Double(string name, Func<TOwner, double> get, Action<TOwner, double> set) =>
        Add(name, o => OptionStrings.FormatDouble(get(o)), (o, v) => set(o, OptionStrings.ParseDouble(name, v)));

    public OptionTable<TOwner> String(string name, Func<TOwner, string> get, Action<TOwner, string> set) =>
        Add(name, o => OptionStrings.Escape(get(o)), (o, v) => set(o, v));

    /// <summary>A customizable object referenced by name ("nullptr" when unset), e.g. comparator, merge_operator.</summary>
    public OptionTable<TOwner> Name(string name, Func<TOwner, string?> get, Action<TOwner, string?> set) =>
        Add(name, o => get(o) ?? "nullptr", (o, v) => set(o, v.Length == 0 || v == "nullptr" ? null : v));

    public OptionTable<TOwner> Enum<TEnum>(string name, EnumMap<TEnum> map, Func<TOwner, TEnum> get, Action<TOwner, TEnum> set) where TEnum : struct, System.Enum =>
        Add(name, o => map.ToName(get(o)), (o, v) => set(o, map.Parse(name, v)));
}

/// <summary>Bidirectional enum &lt;-&gt; RocksDB-name map (OptionsHelper::*_string_map).</summary>
internal sealed class EnumMap<TEnum> where TEnum : struct, System.Enum
{
    private readonly Dictionary<string, TEnum> _byName = new(StringComparer.Ordinal);
    private readonly Dictionary<TEnum, string> _byValue = new();
    private readonly Func<TEnum, string?>? _fallbackName;
    private readonly Func<string, TEnum?>? _fallbackParse;

    public EnumMap((string Name, TEnum Value)[] entries, Func<TEnum, string?>? fallbackName = null, Func<string, TEnum?>? fallbackParse = null)
    {
        foreach ((string n, TEnum v) in entries)
        {
            _byName[n] = v;
            _byValue.TryAdd(v, n);
        }
        _fallbackName = fallbackName;
        _fallbackParse = fallbackParse;
    }

    public string ToName(TEnum value)
    {
        if (_byValue.TryGetValue(value, out string? n)) return n;
        string? f = _fallbackName?.Invoke(value);
        if (f is not null) return f;
        throw RocksDbException.InvalidArgument("Cannot serialize enum value " + value);
    }

    public TEnum Parse(string optionName, string text)
    {
        if (_byName.TryGetValue(text, out TEnum v)) return v;
        TEnum? f = _fallbackParse?.Invoke(text);
        if (f is TEnum fv) return fv;
        throw RocksDbException.InvalidArgument("Error parsing " + optionName + ": unknown value '" + text + "'");
    }
}

/// <summary>Enum name maps used by the OPTIONS file (options/options_helper.cc, options/db_options.cc, block_based_table_factory.cc).</summary>
internal static class OptionEnumMaps
{
    public static readonly EnumMap<CompressionType> Compression = new(
    [
        ("kNoCompression", CompressionType.None),
        ("kSnappyCompression", CompressionType.Snappy),
        ("kZlibCompression", CompressionType.Zlib),
        ("kBZip2Compression", CompressionType.BZip2),
        ("kLZ4Compression", CompressionType.LZ4),
        ("kLZ4HCCompression", CompressionType.LZ4HC),
        ("kXpressCompression", CompressionType.Xpress),
        ("kZSTD", CompressionType.Zstd),
        ("kDisableCompressionOption", CompressionType.DisableCompressionOption),
    ],
    // kCustomCompression80..kCustomCompressionFE
    static v => (byte)v is >= 0x80 and <= 0xFE ? "kCustomCompression" + ((byte)v).ToString("X2", CultureInfo.InvariantCulture) : null,
    static s => s.StartsWith("kCustomCompression", StringComparison.Ordinal) &&
                byte.TryParse(s.AsSpan("kCustomCompression".Length), NumberStyles.HexNumber, CultureInfo.InvariantCulture, out byte b) && b >= 0x80 && b != 0xFF
        ? (CompressionType)b
        : null);

    public static readonly EnumMap<ChecksumType> Checksum = new(
    [
        ("kNoChecksum", ChecksumType.NoChecksum),
        ("kCRC32c", ChecksumType.Crc32c),
        ("kxxHash", ChecksumType.XxHash),
        ("kxxHash64", ChecksumType.XxHash64),
        ("kXXH3", ChecksumType.Xxh3),
    ]);

    public static readonly EnumMap<CompactionStyle> CompactionStyle = new(
    [
        ("kCompactionStyleLevel", Managed.CompactionStyle.Level),
        ("kCompactionStyleUniversal", Managed.CompactionStyle.Universal),
        ("kCompactionStyleFIFO", Managed.CompactionStyle.Fifo),
        ("kCompactionStyleNone", Managed.CompactionStyle.None),
    ]);

    public static readonly EnumMap<CompactionPri> CompactionPri = new(
    [
        ("kByCompensatedSize", Managed.CompactionPri.ByCompensatedSize),
        ("kOldestLargestSeqFirst", Managed.CompactionPri.OldestLargestSeqFirst),
        ("kOldestSmallestSeqFirst", Managed.CompactionPri.OldestSmallestSeqFirst),
        ("kMinOverlappingRatio", Managed.CompactionPri.MinOverlappingRatio),
        ("kRoundRobin", Managed.CompactionPri.RoundRobin),
    ]);

    public static readonly EnumMap<Temperature> Temperature = new(
    [
        ("kUnknown", Managed.Temperature.Unknown),
        ("kHot", Managed.Temperature.Hot),
        ("kWarm", Managed.Temperature.Warm),
        ("kCool", Managed.Temperature.Cool),
        ("kCold", Managed.Temperature.Cold),
        ("kIce", Managed.Temperature.Ice),
    ]);

    public static readonly EnumMap<WalRecoveryMode> WalRecoveryMode = new(
    [
        ("kTolerateCorruptedTailRecords", Managed.WalRecoveryMode.TolerateCorruptedTailRecords),
        ("kAbsoluteConsistency", Managed.WalRecoveryMode.AbsoluteConsistency),
        ("kPointInTimeRecovery", Managed.WalRecoveryMode.PointInTimeRecovery),
        ("kSkipAnyCorruptedRecords", Managed.WalRecoveryMode.SkipAnyCorruptedRecords),
    ]);

    public static readonly EnumMap<InfoLogLevel> InfoLogLevel = new(
    [
        ("DEBUG_LEVEL", Managed.InfoLogLevel.Debug),
        ("INFO_LEVEL", Managed.InfoLogLevel.Info),
        ("WARN_LEVEL", Managed.InfoLogLevel.Warn),
        ("ERROR_LEVEL", Managed.InfoLogLevel.Error),
        ("FATAL_LEVEL", Managed.InfoLogLevel.Fatal),
        ("HEADER_LEVEL", Managed.InfoLogLevel.Header),
    ]);

    public static readonly EnumMap<PrepopulateBlobCache> PrepopulateBlobCache = new(
    [
        ("kDisable", Managed.PrepopulateBlobCache.Disable),
        ("kFlushOnly", Managed.PrepopulateBlobCache.FlushOnly),
    ]);

    public static readonly EnumMap<IndexType> IndexType = new(
    [
        ("kBinarySearch", Managed.IndexType.BinarySearch),
        ("kHashSearch", Managed.IndexType.HashSearch),
        ("kTwoLevelIndexSearch", Managed.IndexType.TwoLevelIndexSearch),
        ("kBinarySearchWithFirstKey", Managed.IndexType.BinarySearchWithFirstKey),
    ]);

    public static readonly EnumMap<DataBlockIndexType> DataBlockIndexType = new(
    [
        ("kDataBlockBinarySearch", Managed.DataBlockIndexType.BinarySearch),
        ("kDataBlockBinaryAndHash", Managed.DataBlockIndexType.BinaryAndHash),
    ]);

    public static readonly EnumMap<IndexShorteningMode> IndexShortening = new(
    [
        ("kNoShortening", IndexShorteningMode.NoShortening),
        ("kShortenSeparators", IndexShorteningMode.ShortenSeparators),
        ("kShortenSeparatorsAndSuccessor", IndexShorteningMode.ShortenSeparatorsAndSuccessor),
    ]);

    public static readonly EnumMap<PrepopulateBlockCache> PrepopulateBlockCache = new(
    [
        ("kDisable", Managed.PrepopulateBlockCache.Disable),
        ("kFlushOnly", Managed.PrepopulateBlockCache.FlushOnly),
    ]);
}

/// <summary>String helpers matching RocksDB's option (de)serialization (util/string_util.cc, options/options_helper.cc).</summary>
internal static class OptionStrings
{
    public static string FormatBool(bool v) => v ? "true" : "false";

    /// <summary>ParseBoolean: "true"/"1" or "false"/"0".</summary>
    public static bool ParseBool(string name, string v) => v switch
    {
        "true" or "1" => true,
        "false" or "0" => false,
        _ => throw RocksDbException.InvalidArgument("Error parsing " + name + ": invalid boolean '" + v + "'"),
    };

    /// <summary>std::to_string(double): "%f".</summary>
    public static string FormatDouble(double v) => v.ToString("F6", CultureInfo.InvariantCulture);

    public static double ParseDouble(string name, string v)
    {
        if (double.TryParse(v, NumberStyles.Float, CultureInfo.InvariantCulture, out double d)) return d;
        throw RocksDbException.InvalidArgument("Error parsing " + name + ": invalid number '" + v + "'");
    }

    /// <summary>ParseUint64: decimal with optional k/K, m/M, g/G, t/T multiplier suffix.</summary>
    public static ulong ParseUInt64(string name, string v)
    {
        ReadOnlySpan<char> s = v.AsSpan();
        ulong mult = 1;
        if (s.Length > 1)
        {
            switch (s[^1])
            {
                case 'k' or 'K': mult = 1UL << 10; s = s[..^1]; break;
                case 'm' or 'M': mult = 1UL << 20; s = s[..^1]; break;
                case 'g' or 'G': mult = 1UL << 30; s = s[..^1]; break;
                case 't' or 'T': mult = 1UL << 40; s = s[..^1]; break;
            }
        }
        if (!ulong.TryParse(s, NumberStyles.None, CultureInfo.InvariantCulture, out ulong n))
        {
            throw RocksDbException.InvalidArgument("Error parsing " + name + ": invalid unsigned integer '" + v + "'");
        }
        return checked(n * mult);
    }

    public static long ParseInt64(string name, string v)
    {
        bool neg = v.StartsWith('-');
        ulong mag = ParseUInt64(name, neg ? v[1..] : v);
        if (neg)
        {
            if (mag > (ulong)long.MaxValue + 1) throw RocksDbException.InvalidArgument("Error parsing " + name + ": out of range");
            return (long)(0 - mag);
        }
        return checked((long)mag);
    }

    /// <summary>isSpecialChar: '\\', '#', ':', '\r', '\n' are backslash-escaped.</summary>
    private static bool IsSpecialChar(char c) => c is '\\' or '#' or ':' or '\r' or '\n';

    /// <summary>EscapeOptionString.</summary>
    public static string Escape(string raw)
    {
        bool any = false;
        foreach (char c in raw)
        {
            if (IsSpecialChar(c)) { any = true; break; }
        }
        if (!any) return raw;
        var sb = new StringBuilder(raw.Length + 8);
        foreach (char c in raw)
        {
            if (IsSpecialChar(c))
            {
                sb.Append('\\');
                sb.Append(c switch { '\n' => 'n', '\r' => 'r', _ => c });
            }
            else
            {
                sb.Append(c);
            }
        }
        return sb.ToString();
    }

    /// <summary>UnescapeOptionString.</summary>
    public static string Unescape(string escaped)
    {
        if (escaped.IndexOf('\\') < 0) return escaped;
        var sb = new StringBuilder(escaped.Length);
        bool esc = false;
        foreach (char c in escaped)
        {
            if (esc)
            {
                sb.Append(c switch { 'n' => '\n', 'r' => '\r', _ => c });
                esc = false;
            }
            else if (c == '\\')
            {
                esc = true;
            }
            else
            {
                sb.Append(c);
            }
        }
        return sb.ToString();
    }

    /// <summary>Formats an int array with ':' separators (e.g. max_bytes_for_level_multiplier_additional).</summary>
    public static string FormatIntArray(IReadOnlyList<int> values)
    {
        var sb = new StringBuilder();
        for (int i = 0; i < values.Count; i++)
        {
            if (i > 0) sb.Append(':');
            sb.Append(values[i].ToString(CultureInfo.InvariantCulture));
        }
        return sb.ToString();
    }

    public static List<int> ParseIntArray(string name, string v)
    {
        var list = new List<int>();
        if (v.Length == 0) return list;
        foreach (string part in v.Split(':'))
        {
            list.Add(checked((int)ParseInt64(name, part.Trim())));
        }
        return list;
    }

    /// <summary>
    /// Splits a struct value "{a=1;b={x=1;};c=2;}" (or without braces) into ordered name/value pairs, respecting
    /// nested braces. Port of the StringToMap logic used for struct options (options/options_helper.cc).
    /// </summary>
    public static List<KeyValuePair<string, string>> ParseStruct(string name, string value)
    {
        var result = new List<KeyValuePair<string, string>>();
        ReadOnlySpan<char> s = value.AsSpan().Trim();
        if (s.Length >= 2 && s[0] == '{' && s[^1] == '}') s = s[1..^1];
        int pos = 0;
        while (pos < s.Length)
        {
            while (pos < s.Length && char.IsWhiteSpace(s[pos])) pos++;
            if (pos >= s.Length) break;
            int eq = s[pos..].IndexOf('=');
            if (eq < 0) throw RocksDbException.InvalidArgument("Error parsing " + name + ": mismatched key value pair");
            string key = s.Slice(pos, eq).Trim().ToString();
            pos += eq + 1;
            while (pos < s.Length && char.IsWhiteSpace(s[pos])) pos++;
            int start = pos;
            int depth = 0;
            while (pos < s.Length)
            {
                char c = s[pos];
                if (c == '{') depth++;
                else if (c == '}') depth--;
                else if (c == ';' && depth == 0) break;
                pos++;
            }
            if (depth != 0) throw RocksDbException.InvalidArgument("Error parsing " + name + ": mismatched braces");
            string val = s[start..pos].Trim().ToString();
            result.Add(new KeyValuePair<string, string>(key, val));
            pos++; // skip ';'
        }
        return result;
    }

    /// <summary>Serializes struct members as "{a=1;b=2;}" (the format native writes, e.g. compression_opts).</summary>
    public static string FormatStruct(IEnumerable<KeyValuePair<string, string>> members)
    {
        var sb = new StringBuilder("{");
        foreach (KeyValuePair<string, string> kv in members) sb.Append(kv.Key).Append('=').Append(kv.Value).Append(';');
        sb.Append('}');
        return sb.ToString();
    }
}
