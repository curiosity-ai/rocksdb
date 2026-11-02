using System.Globalization;

namespace RocksDb.Managed;

/// <summary>
/// A prefix extractor: maps a user key to its prefix for prefix Bloom filters and prefix seek. Port of
/// rocksdb::SliceTransform (include/rocksdb/slice_transform.h) and the built-in transforms of util/slice.cc.
/// <para>
/// Set it as <see cref="ColumnFamilyOptions.PrefixExtractor"/>. Its <see cref="AsString"/> id is persisted in the
/// OPTIONS file (<c>prefix_extractor=rocksdb.FixedPrefix.4</c>) and in each table's
/// <c>rocksdb.prefix.extractor.name</c> property; a table's prefix filter is only used when the configured extractor has
/// the same id as the one the table was built with (or the table's extractor is a built-in that can be recreated).
/// </para>
/// <para>
/// Custom implementations must be thread-safe and allocation-free (they are called on the read and write hot paths),
/// and satisfy the properties documented for ColumnFamilyOptions::prefix_extractor upstream:
/// (1) <c>key.starts_with(Transform(key))</c>, (2) if <c>InDomain(k)</c> and <c>Compare(k1, k2) &lt;= 0</c> with both in
/// domain then <c>Compare(Transform(k1), Transform(k2)) &lt;= 0</c>, (3) <c>InDomain(Transform(k))</c> for keys in domain.
/// </para>
/// </summary>
public abstract class SliceTransform
{
    /// <summary>Name of the transformation class (Customizable::Name), e.g. "rocksdb.FixedPrefix".</summary>
    public abstract string Name { get; }

    /// <summary>
    /// Persisted id (SliceTransform::AsString / GetId), e.g. "rocksdb.FixedPrefix.4". Defaults to <see cref="Name"/>;
    /// parameterized transforms must include their parameters so that different configurations get different ids.
    /// </summary>
    public virtual string AsString() => Name;

    /// <summary>
    /// Extracts the prefix of <paramref name="key"/>. REQUIRES: <see cref="InDomain"/>(key). The result is normally a
    /// slice of <paramref name="key"/>.
    /// </summary>
    public abstract ReadOnlySpan<byte> Transform(ReadOnlySpan<byte> key);

    /// <summary>Whether <paramref name="key"/> has a prefix; keys outside the domain are not prefix-filtered.</summary>
    public abstract bool InDomain(ReadOnlySpan<byte> key);

    /// <summary>
    /// If <c>Transform(x).Length == n</c> for some keys and otherwise less, returns true with <paramref name="length"/> =
    /// n. Returning false is safe but disables some auto_prefix_mode filtering.
    /// </summary>
    public virtual bool FullLengthEnabled(out int length)
    {
        length = 0;
        return false;
    }

    /// <summary>
    /// Whether <c>Transform(s) == Transform(prefix)</c> for any s starting with <paramref name="prefix"/> (not used by
    /// the engine; informational for callers). Returning false is always safe.
    /// </summary>
    public virtual bool SameResultWhenAppended(ReadOnlySpan<byte> prefix) => false;

    /// <inheritdoc/>
    public override string ToString() => AsString();

    /// <summary>Port of NewFixedPrefixTransform: the first <paramref name="prefixLength"/> bytes; shorter keys are out of domain.</summary>
    public static SliceTransform CreateFixedPrefix(int prefixLength)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(prefixLength);
        return new FixedPrefixTransform(prefixLength);
    }

    /// <summary>Port of NewCappedPrefixTransform: the first min(<paramref name="capLength"/>, key length) bytes; every key is in domain.</summary>
    public static SliceTransform CreateCappedPrefix(int capLength)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(capLength);
        return new CappedPrefixTransform(capLength);
    }

    /// <summary>Port of NewNoopTransform: the whole key is the prefix.</summary>
    public static SliceTransform CreateNoop() => NoopTransform.Instance;

    /// <summary>
    /// Port of SliceTransform::CreateFromString for the built-in transforms (RegisterBuiltinSliceTransform): accepts
    /// "rocksdb.FixedPrefix.N", "fixed:N", "rocksdb.CappedPrefix.N", "capped:N", "rocksdb.Noop", and the struct form
    /// "{id=...}" / "id=...". Returns null for "", "nullptr" and for ids that are not built-in (like upstream with
    /// ignore_unsupported_options, the default).
    /// </summary>
    public static SliceTransform? CreateFromString(string? value)
    {
        if (string.IsNullOrEmpty(value)) return null;
        string id = value.Trim();
        if (id.Length > 1 && id[0] == '{' && id[^1] == '}') id = id[1..^1].Trim();
        if (id.StartsWith("id=", StringComparison.Ordinal))
        {
            id = id[3..];
            int semi = id.IndexOf(';');
            if (semi >= 0) id = id[..semi];
            id = id.Trim();
        }
        if (id.Length == 0 || id == "nullptr") return null;
        if (id == NoopTransform.ClassName) return NoopTransform.Instance;
        if (TryParseNumbered(id, FixedPrefixTransform.ClassName, '.', out int n) || TryParseNumbered(id, FixedPrefixTransform.NickName, ':', out n))
            return new FixedPrefixTransform(n);
        if (TryParseNumbered(id, CappedPrefixTransform.ClassName, '.', out n) || TryParseNumbered(id, CappedPrefixTransform.NickName, ':', out n))
            return new CappedPrefixTransform(n);
        return null;
    }

    private static bool TryParseNumbered(string id, string name, char separator, out int n)
    {
        n = 0;
        if (id.Length <= name.Length + 1 || !id.StartsWith(name, StringComparison.Ordinal) || id[name.Length] != separator) return false;
        ReadOnlySpan<char> digits = id.AsSpan(name.Length + 1);
        foreach (char c in digits)
            if (c is < '0' or > '9') return false;
        return int.TryParse(digits, NumberStyles.None, CultureInfo.InvariantCulture, out n);
    }

    /// <summary>Port of FixedPrefixTransform (util/slice.cc).</summary>
    private sealed class FixedPrefixTransform : SliceTransform
    {
        public const string ClassName = "rocksdb.FixedPrefix";
        public const string NickName = "fixed";
        private readonly int _prefixLength;
        private readonly string _id;

        public FixedPrefixTransform(int prefixLength)
        {
            _prefixLength = prefixLength;
            _id = ClassName + "." + prefixLength.ToString(CultureInfo.InvariantCulture);
        }

        public override string Name => ClassName;
        public override string AsString() => _id;
        public override ReadOnlySpan<byte> Transform(ReadOnlySpan<byte> key) => key[.._prefixLength];
        public override bool InDomain(ReadOnlySpan<byte> key) => key.Length >= _prefixLength;

        public override bool FullLengthEnabled(out int length)
        {
            length = _prefixLength;
            return true;
        }

        public override bool SameResultWhenAppended(ReadOnlySpan<byte> prefix) => InDomain(prefix);
    }

    /// <summary>Port of CappedPrefixTransform (util/slice.cc).</summary>
    private sealed class CappedPrefixTransform : SliceTransform
    {
        public const string ClassName = "rocksdb.CappedPrefix";
        public const string NickName = "capped";
        private readonly int _capLength;
        private readonly string _id;

        public CappedPrefixTransform(int capLength)
        {
            _capLength = capLength;
            _id = ClassName + "." + capLength.ToString(CultureInfo.InvariantCulture);
        }

        public override string Name => ClassName;
        public override string AsString() => _id;
        public override ReadOnlySpan<byte> Transform(ReadOnlySpan<byte> key) => key[..Math.Min(_capLength, key.Length)];
        public override bool InDomain(ReadOnlySpan<byte> key) => true;

        public override bool FullLengthEnabled(out int length)
        {
            length = _capLength;
            return true;
        }

        public override bool SameResultWhenAppended(ReadOnlySpan<byte> prefix) => prefix.Length >= _capLength;
    }

    /// <summary>Port of NoopTransform (util/slice.cc).</summary>
    private sealed class NoopTransform : SliceTransform
    {
        public const string ClassName = "rocksdb.Noop";
        public static readonly NoopTransform Instance = new();

        public override string Name => ClassName;
        public override ReadOnlySpan<byte> Transform(ReadOnlySpan<byte> key) => key;
        public override bool InDomain(ReadOnlySpan<byte> key) => true;
    }
}
