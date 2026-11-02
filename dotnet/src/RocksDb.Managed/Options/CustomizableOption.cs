namespace RocksDb.Managed.Options;

/// <summary>
/// Storage for an option that holds a pluggable object (filter policy, merge operator, compaction filter, slice
/// transform, ...), referenced by id in OPTIONS files (Customizable options in options/customizable.cc).
/// The object is the primary value. When the option was parsed from OPTIONS-file text, that text is remembered so the
/// option is written back byte-identically as long as the object was not replaced or reconfigured; an id that cannot
/// be resolved to a managed object (a custom native class) is kept as <see cref="UnresolvedText"/> and written back
/// verbatim.
/// </summary>
internal struct CustomizableOption<T> where T : class
{
    private T? _value;
    private string? _text;     // unescaped OPTIONS text last parsed
    private T? _parsedValue;   // object resolved from _text (null if unresolvable)
    private string? _parsedId; // id of _parsedValue at parse time (detects reconfiguration of mutable objects)

    public readonly T? Value => _value;

    /// <summary>Sets the object; null clears the option, including any preserved unresolved text.</summary>
    public void Set(T? value)
    {
        _value = value;
        if (value is null) this = default;
    }

    /// <summary>
    /// Parses OPTIONS-file text: "" / "nullptr" clear the option; otherwise <paramref name="resolve"/> creates the
    /// object (returning null, or throwing <see cref="RocksDbException"/>, for ids it does not know).
    /// </summary>
    public void Parse(string text, Func<string, T?> resolve, Func<T, string> id)
    {
        if (text.Length == 0 || text == "nullptr")
        {
            this = default;
            return;
        }
        T? v;
        try
        {
            v = resolve(text);
        }
        catch (RocksDbException)
        {
            v = null;
        }
        _value = v;
        _parsedValue = v;
        _text = text;
        _parsedId = v is null ? null : id(v);
    }

    /// <summary>Text to persist (unescaped), or null when unset.</summary>
    public readonly string? Serialize(Func<T, string> id)
    {
        if (_text is not null && ReferenceEquals(_value, _parsedValue) && (_value is null || id(_value) == _parsedId)) return _text;
        return _value is null ? null : id(_value);
    }

    /// <summary>The persisted text of an id that could not be resolved to an object (null when resolved or unset).</summary>
    public readonly string? UnresolvedText => _value is null ? _text : null;

    /// <summary>The object's name (via <paramref name="name"/>), else the id of the unresolved text, else null.</summary>
    public readonly string? Name(Func<T, string> name) =>
        _value is not null ? name(_value) : _text is null ? null : OptionStrings.UnwrapId(_text);
}
