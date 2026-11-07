using RocksDb.Managed.Options;

namespace RocksDb.Managed;

/// <summary>
/// Common plumbing for options classes that round-trip through the OPTIONS file: a typed option table plus a
/// dictionary that preserves options the managed model does not (yet) expose, so reading and rewriting a native
/// OPTIONS file loses nothing.
/// </summary>
public abstract class PersistedOptions<TSelf> where TSelf : PersistedOptions<TSelf>
{
    /// <summary>
    /// Options present in a parsed OPTIONS file (or set via <see cref="SetOption"/>) that have no typed property.
    /// They are written back verbatim (already escaped) after the typed options. Insertion order is preserved.
    /// </summary>
    public Dictionary<string, string> UnknownOptions { get; } = new(StringComparer.Ordinal);

    internal abstract OptionTable<TSelf> OptionTable { get; }

    /// <summary>
    /// Sets one option from its OPTIONS-file string form (value as it appears in the file, i.e. escaped). Unknown names
    /// are stored in <see cref="UnknownOptions"/> when <paramref name="preserveUnknown"/> is true, otherwise rejected
    /// with InvalidArgument (like ConfigOptions::ignore_unknown_options = false).
    /// </summary>
    public void SetOption(string name, string value, bool preserveUnknown)
    {
        if (OptionTable.TryGet(name, out OptionTypeInfo<TSelf> info))
        {
            // Struct values are split into fields first; each field value is unescaped by the struct's own SetOption.
            if (info.StructParse is { } structParse) structParse((TSelf)this, null, value, preserveUnknown);
            else info.Parse((TSelf)this, OptionStrings.Unescape(value));
            UnknownOptions.Remove(name);
        }
        else if (TryGetStructField(name, out OptionTypeInfo<TSelf>? structInfo, out string? field))
        {
            // "struct.field" (OptionTypeInfo::Find + ParseStruct, options/options_helper.cc): sets one field.
            structInfo.StructParse!((TSelf)this, field, value, preserveUnknown);
        }
        else if (OptionTable.TryGetAlias(name, out OptionTypeInfo<TSelf> aliased))
        {
            aliased.Parse((TSelf)this, OptionStrings.Unescape(value));
        }
        else if (OptionTable.IsDeprecated(name))
        {
            // OptionVerificationType::kDeprecated: accepted and ignored, never serialized.
        }
        else if (preserveUnknown)
        {
            UnknownOptions[name] = value;
        }
        else
        {
            ThrowHelper.InvalidArgument("Unrecognized option " + typeof(TSelf).Name + ":: " + name);
        }
    }

    /// <summary>Resolves "struct.field" to the struct option's info (OptionTypeInfo::Find for a name with a '.').</summary>
    private bool TryGetStructField(string name, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out OptionTypeInfo<TSelf>? info,
        [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out string? field)
    {
        int dot = name.IndexOf('.', StringComparison.Ordinal);
        if (dot > 0 && dot < name.Length - 1 && OptionTable.TryGet(name[..dot], out OptionTypeInfo<TSelf> i) && i.StructParse is not null)
        {
            info = i;
            field = name[(dot + 1)..];
            return true;
        }
        info = null;
        field = null;
        return false;
    }

    /// <summary>
    /// True if <paramref name="name"/> is a typed option, a field of a typed struct option ("struct.field") or a
    /// deprecated name that is accepted and ignored.
    /// </summary>
    internal bool IsKnownOption(string name) =>
        OptionTable.TryGet(name, out _) || TryGetStructField(name, out _, out _) || OptionTable.TryGetAlias(name, out _)
        || OptionTable.IsDeprecated(name);

    /// <summary>Applies a set of name/value pairs (see <see cref="SetOption"/>).</summary>
    public void SetOptions(IEnumerable<KeyValuePair<string, string>> options, bool preserveUnknown)
    {
        foreach (KeyValuePair<string, string> kv in options) SetOption(kv.Key, kv.Value, preserveUnknown);
    }

    /// <summary>Returns the option's serialized (escaped) value, or null if unknown.</summary>
    public string? GetOption(string name)
    {
        if (OptionTable.TryGet(name, out OptionTypeInfo<TSelf> info)) return info.Serialize((TSelf)this);
        return UnknownOptions.TryGetValue(name, out string? v) ? v : null;
    }

    /// <summary>All options as name/serialized-value pairs: typed options in table order, then preserved unknown ones.</summary>
    public List<KeyValuePair<string, string>> ToOptionsList()
    {
        var list = new List<KeyValuePair<string, string>>(OptionTable.All.Count + UnknownOptions.Count);
        foreach (OptionTypeInfo<TSelf> info in OptionTable.All) list.Add(new(info.Name, info.Serialize((TSelf)this)));
        foreach (KeyValuePair<string, string> kv in UnknownOptions) list.Add(kv);
        return list;
    }

    /// <summary>Serializes as "name1=value1;name2=value2;..." (GetStringFromXXXOptions with ';' delimiter).</summary>
    public string ToOptionsString()
    {
        var sb = new System.Text.StringBuilder();
        foreach (KeyValuePair<string, string> kv in ToOptionsList()) sb.Append(kv.Key).Append('=').Append(kv.Value).Append(';');
        return sb.ToString();
    }

    /// <summary>Copies all options (typed and preserved) from <paramref name="other"/>.</summary>
    internal void CopyFrom(TSelf other)
    {
        foreach (KeyValuePair<string, string> kv in other.ToOptionsList()) SetOption(kv.Key, kv.Value, preserveUnknown: true);
    }
}
