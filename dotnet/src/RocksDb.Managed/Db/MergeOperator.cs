using System.Buffers;

namespace RocksDb.Managed;

/// <summary>
/// CONTRACT (phase 2). User-defined read-modify-write. Port of rocksdb::MergeOperator
/// (include/rocksdb/merge_operator.h) reshaped for spans: operands are exposed through
/// <see cref="MergeOperands"/> (no copies) and results are written to an <see cref="IBufferWriter{T}"/>.
/// The <see cref="Name"/> is persisted in the OPTIONS file and table properties ("rocksdb.merge.operator").
/// Implementations must be thread-safe (called concurrently from reads, flush and compaction).
/// </summary>
public abstract class MergeOperator
{
    public abstract string Name { get; }

    /// <summary>
    /// Port of FullMergeV2: combine the existing value (absent if <paramref name="hasExistingValue"/> is false) with
    /// operands (oldest first). Write the result to <paramref name="result"/>; return false on failure (surfaced as
    /// Corruption with subcode MergeOperatorFailed).
    /// </summary>
    public abstract bool FullMerge(ReadOnlySpan<byte> key, bool hasExistingValue, ReadOnlySpan<byte> existingValue,
        in MergeOperands operands, IBufferWriter<byte> result);

    /// <summary>
    /// FullMergeV2 with <c>MergeOperationOutput::op_failure_scope</c>: like
    /// <see cref="FullMerge(ReadOnlySpan{byte}, bool, ReadOnlySpan{byte}, in MergeOperands, IBufferWriter{byte})"/>, and on
    /// failure (false) <paramref name="failureScope"/> says how far the failure reaches (see
    /// <see cref="MergeOperationFailureScope"/>). The default calls the five-argument overload and reports
    /// <see cref="MergeOperationFailureScope.Default"/>.
    /// </summary>
    public virtual bool FullMerge(ReadOnlySpan<byte> key, bool hasExistingValue, ReadOnlySpan<byte> existingValue, in MergeOperands operands,
        IBufferWriter<byte> result, out MergeOperationFailureScope failureScope)
    {
        failureScope = MergeOperationFailureScope.Default;
        return FullMerge(key, hasExistingValue, existingValue, operands, result);
    }

    /// <summary>
    /// Port of FullMergeV3 (db/merge_operator.cc): the merge entry point used by reads, flush and compaction. The base
    /// value may be absent, plain or a wide-column entity, and the result may be a plain value
    /// (<see cref="MergeOperationOutputV3.NewValue"/>) or a wide-column entity (<see cref="MergeOperationOutputV3.AddColumn"/>).
    /// The default implementation calls
    /// <see cref="FullMerge(ReadOnlySpan{byte}, bool, ReadOnlySpan{byte}, in MergeOperands, IBufferWriter{byte}, out MergeOperationFailureScope)"/>:
    /// with no or a plain base value directly; with an entity base on the value of its default column (empty when it has
    /// none), keeping the other columns unchanged in the result entity.
    /// </summary>
    public virtual bool FullMergeV3(in MergeOperationInputV3 input, ref MergeOperationOutputV3 output)
    {
        MergeOperationFailureScope scope;
        bool ok;
        if (input.ExistingValueKind != MergeExistingValueKind.WideColumns)
        {
            ok = FullMerge(input.Key, input.ExistingValueKind == MergeExistingValueKind.Plain, input.ExistingValue, input.Operands, output.NewValue, out scope);
        }
        else if (output.ValueOnly)
        {
            // The caller only reads the default column of the result: merge it directly.
            ok = FullMerge(input.Key, true, Db.Wide.WideColumnSerialization.GetValueOfDefaultColumn(input.ExistingEntity), input.Operands,
                output.NewValue, out scope);
        }
        else
        {
            ReadOnlySpan<byte> entity = input.ExistingEntity;
            Util.PooledBufferWriter merged = Util.ObjectPools.RentWriter();
            try
            {
                ok = FullMerge(input.Key, true, Db.Wide.WideColumnSerialization.GetValueOfDefaultColumn(entity), input.Operands, merged, out scope);
                // new_columns = { default: merged } + the other existing columns (already sorted).
                if (ok) Db.Wide.WideColumnSerialization.ReplaceDefaultColumn(entity, merged.WrittenSpan, output.SerializedEntity);
            }
            finally
            {
                Util.ObjectPools.ReturnWriter(merged);
            }
        }
        if (!ok) output.FailureScope = scope;
        return ok;
    }

    /// <summary>
    /// Port of PartialMergeMulti: combine operands (oldest first) without a base value. Return false if not possible
    /// (the default), in which case operands are kept separately.
    /// </summary>
    public virtual bool PartialMerge(ReadOnlySpan<byte> key, in MergeOperands operands, IBufferWriter<byte> result) => false;

    /// <summary>Port of AllowSingleOperand.</summary>
    public virtual bool AllowSingleOperand => false;

    /// <summary>
    /// Port of Customizable::NickName: the short id a factory also accepts for this operator (e.g. "max" for
    /// "MaxOperator"); null when there is none.
    /// </summary>
    public virtual string? NickName => null;

    /// <summary>
    /// The text persisted for this operator in OPTIONS files: <see cref="Name"/>, or the struct form native writes for
    /// operators with options (e.g. "{id=StringAppendOperator;delimiter=,;}").
    /// </summary>
    internal virtual string OptionsId => Name;

    public override string ToString() => Name;

    /// <summary>Built-in "UInt64AddOperator" (<see cref="UInt64AddOperator"/>).</summary>
    public static MergeOperator UInt64Add => UInt64AddOperator.Instance;

    /// <summary>Built-in "MaxOperator" (<see cref="MaxOperator"/>).</summary>
    public static MergeOperator Max => MaxOperator.Instance;

    /// <summary>Built-in "PutOperator" (<see cref="PutOperatorV2"/>, upstream's MergeOperators::CreatePutOperator()).</summary>
    public static MergeOperator Put => PutOperatorV2.Instance;

    /// <summary>Built-in "StringAppendOperator" (<see cref="StringAppendOperator"/>) with the given delimiter.</summary>
    public static MergeOperator StringAppend(string delimiter) => new StringAppendOperator(delimiter);

    /// <summary>
    /// Recreates a built-in merge operator from its id, like MergeOperator::CreateFromString /
    /// MergeOperators::CreateFromStringId with the factories registered by RegisterBuiltinMergeOperators
    /// (utilities/merge_operators.cc): the class name or nick name ("PutOperator" / "put" (V2), "put_v1",
    /// "UInt64AddOperator" / "uint64add", "MaxOperator" / "max", "MergeSortOperator" / "sortlist", "BytesXOR" /
    /// "bytesxor", "StringAppendOperator" / "stringappend", "StringAppendTESTOperator" / "stringappendtest"), or the
    /// struct form native writes for configurable objects ("{id=StringAppendOperator;delimiter=,;}", also accepted
    /// without braces: "id=stringappend;delimiter=||"). In addition (upstream does not register it by name)
    /// "AggMergeOperator.v1" resolves to <see cref="Utilities.AggMerge.AggMergeOperator.Instance"/>.
    /// Returns null for other (custom) ids and for properties the operator does not have.
    /// </summary>
    public static MergeOperator? CreateBuiltin(string id)
    {
        ArgumentNullException.ThrowIfNull(id);
        string? delimiter = null;
        if (id.Contains('=', StringComparison.Ordinal))
        {
            string inner = "";
            string? userOperator = null;
            foreach (KeyValuePair<string, string> kv in Options.OptionStrings.ParseStruct("merge_operator", id))
            {
                if (kv.Key == "id") inner = kv.Value;
                else if (kv.Key == "delimiter") delimiter = kv.Value;
                else if (kv.Key == "user_operator") userOperator = kv.Value;
                else return null;
            }
            if (inner == TtlMergeOperator.ClassName)
            {
                // RegisterTtlObjects: TtlMergeOperator with its user_operator (ttl_merge_op_type_info).
                if (delimiter is not null || userOperator is null) return null;
                MergeOperator? user = CreateBuiltin(userOperator);
                return user is null ? null : new TtlMergeOperator(user);
            }
            if (userOperator is not null) return null;
            id = inner;
        }
        if (delimiter is not null)
        {
            return id switch
            {
                "StringAppendOperator" or "stringappend" => new StringAppendOperator(delimiter),
                "StringAppendTESTOperator" or "stringappendtest" => new StringAppendTestOperator(delimiter),
                _ => null,
            };
        }
        return id switch
        {
            "UInt64AddOperator" or "uint64add" => UInt64AddOperator.Instance,
            "StringAppendOperator" or "stringappend" => new StringAppendOperator(),
            "StringAppendTESTOperator" or "stringappendtest" => new StringAppendTestOperator(),
            "MaxOperator" or "max" => MaxOperator.Instance,
            "PutOperator" or "put" => PutOperatorV2.Instance,
            "put_v1" => PutOperator.Instance,
            "MergeSortOperator" or "sortlist" => SortListOperator.Instance,
            "BytesXOR" or "bytesxor" => BytesXorOperator.Instance,
            Utilities.AggMerge.AggMergeOperator.ClassName => Utilities.AggMerge.AggMergeOperator.Instance,
            _ => null,
        };
    }
}

/// <summary>
/// Port of MergeOperator::OpFailureScope (include/rocksdb/merge_operator.h): the blast radius of a failed merge.
/// </summary>
public enum MergeOperationFailureScope : byte
{
    /// <summary>kDefault: treated as <see cref="TryMerge"/>.</summary>
    Default = 0,
    /// <summary>
    /// kTryMerge: every operation that tries to merge the key fails, including flush and compaction (which put the DB
    /// into read-only mode with a background error).
    /// </summary>
    TryMerge = 1,
    /// <summary>
    /// kMustMerge: operations that must merge the key fail (Get, MultiGet, iteration); flush and compaction proceed by
    /// copying the input merge operands to their output.
    /// </summary>
    MustMerge = 2,
}

/// <summary>The kind of base value a merge sees (the alternatives of MergeOperationInputV3::ExistingValue).</summary>
public enum MergeExistingValueKind : byte
{
    /// <summary>std::monostate: no base value (the key was deleted or never written).</summary>
    None = 0,
    /// <summary>A plain value (<see cref="MergeOperationInputV3.ExistingValue"/>).</summary>
    Plain = 1,
    /// <summary>A wide-column entity (<see cref="MergeOperationInputV3.ExistingColumns"/>).</summary>
    WideColumns = 2,
}

/// <summary>
/// Port of MergeOperator::MergeOperationInputV3: the key, the base value (none, plain or wide columns) and the
/// operands (oldest first). Valid only during the <see cref="MergeOperator.FullMergeV3"/> call.
/// </summary>
public readonly ref struct MergeOperationInputV3
{
    private readonly ReadOnlySpan<byte> _existing;

    internal MergeOperationInputV3(ReadOnlySpan<byte> key, MergeExistingValueKind kind, ReadOnlySpan<byte> existing, MergeOperands operands)
    {
        Key = key;
        ExistingValueKind = kind;
        _existing = existing;
        Operands = operands;
    }

    /// <summary>The user key (including the user-defined timestamp, if any).</summary>
    public ReadOnlySpan<byte> Key { get; }

    /// <summary>Which base value is present.</summary>
    public MergeExistingValueKind ExistingValueKind { get; }

    /// <summary>The plain base value (empty unless <see cref="ExistingValueKind"/> is <see cref="MergeExistingValueKind.Plain"/>).</summary>
    public ReadOnlySpan<byte> ExistingValue => ExistingValueKind == MergeExistingValueKind.Plain ? _existing : default;

    /// <summary>The serialized base entity (empty unless <see cref="ExistingValueKind"/> is <see cref="MergeExistingValueKind.WideColumns"/>).</summary>
    internal ReadOnlySpan<byte> ExistingEntity => ExistingValueKind == MergeExistingValueKind.WideColumns ? _existing : default;

    /// <summary>
    /// The base entity's columns sorted by name, decoded into a new <see cref="WideColumns"/> (null unless
    /// <see cref="ExistingValueKind"/> is <see cref="MergeExistingValueKind.WideColumns"/>). Allocates: read it once per merge.
    /// </summary>
    public WideColumns? ExistingColumns
    {
        get
        {
            if (ExistingValueKind != MergeExistingValueKind.WideColumns) return null;
            var c = new WideColumns();
            c.SetSerialized(_existing);
            return c;
        }
    }

    /// <summary>The operands to apply, oldest first.</summary>
    public MergeOperands Operands { get; }
}

/// <summary>Which alternative of MergeOperationOutputV3::NewValue a merge produced.</summary>
internal enum MergeResultKind : byte
{
    None = 0,
    Value = 1,
    /// <summary>Columns added with <see cref="MergeOperationOutputV3.AddColumn"/> (any order).</summary>
    Columns = 2,
    /// <summary>A serialized, sorted entity written to <see cref="MergeOperationOutputV3.SerializedEntity"/>.</summary>
    SerializedEntity = 3,
}

/// <summary>
/// Port of MergeOperator::MergeOperationOutputV3: receives the result of <see cref="MergeOperator.FullMergeV3"/>,
/// either a plain value (written to <see cref="NewValue"/>) or a wide-column entity (columns added with
/// <see cref="AddColumn"/>, in any order; names must be unique), and on failure the <see cref="FailureScope"/>.
/// Use one of the two forms per merge (mixing them throws InvalidOperationException). Upstream's third alternative
/// (returning one of the existing operands by reference) is not offered: write the bytes to <see cref="NewValue"/>.
/// </summary>
public ref struct MergeOperationOutputV3
{
    private readonly IBufferWriter<byte> _value;
    private readonly IBufferWriter<byte>? _entity;
    private Db.Wide.EntityColumnList? _columns;

    internal MergeOperationOutputV3(IBufferWriter<byte> value, IBufferWriter<byte>? entity)
    {
        _value = value;
        _entity = entity;
    }

    /// <summary>The destination of a plain-value result.</summary>
    public IBufferWriter<byte> NewValue
    {
        get
        {
            if (Kind is MergeResultKind.Columns or MergeResultKind.SerializedEntity) ThrowMixed();
            Kind = MergeResultKind.Value;
            return _value;
        }
    }

    /// <summary>Adds a column of a wide-column result (new_columns); the columns are sorted by name afterwards.</summary>
    public void AddColumn(scoped ReadOnlySpan<byte> name, scoped ReadOnlySpan<byte> value)
    {
        if (Kind is MergeResultKind.Value or MergeResultKind.SerializedEntity) ThrowMixed();
        Kind = MergeResultKind.Columns;
        (_columns ??= new Db.Wide.EntityColumnList()).Add(name, value, false);
    }

    /// <summary>How far a failure reaches (op_failure_scope); only meaningful when the merge returns false.</summary>
    public MergeOperationFailureScope FailureScope { get; set; }

    /// <summary>The caller only reads the default column of the result (it then needs no entity).</summary>
    internal readonly bool ValueOnly => _entity is null;

    /// <summary>Destination of a serialized, sorted entity result (internal producers that already have one).</summary>
    internal IBufferWriter<byte> SerializedEntity
    {
        get
        {
            if (Kind is MergeResultKind.Value or MergeResultKind.Columns) ThrowMixed();
            Kind = MergeResultKind.SerializedEntity;
            return _entity ?? ThrowNoEntity();
        }
    }

    internal MergeResultKind Kind { readonly get; private set; }

    internal readonly Db.Wide.EntityColumnList? Columns => _columns;

    [System.Diagnostics.CodeAnalysis.DoesNotReturn]
    private static void ThrowMixed() =>
        throw new InvalidOperationException("A merge result is either a plain value (NewValue) or wide columns (AddColumn), not both");

    [System.Diagnostics.CodeAnalysis.DoesNotReturn]
    private static IBufferWriter<byte> ThrowNoEntity() => throw new InvalidOperationException("This merge only produces a plain value");
}

/// <summary>
/// Read-only view of merge operands, oldest first. Backed by caller-owned storage (e.g. <see cref="Db.MergeContext"/>);
/// valid only during the merge callback.
/// </summary>
public readonly ref struct MergeOperands
{
    private readonly ReadOnlySpan<byte> _data;
    private readonly ReadOnlySpan<int> _offsets; // Count + 1 entries: operand i spans [_offsets[i], _offsets[i+1])
    // Alternatively, operands referenced in place (pinned blocks / memtable memory), oldest first
    // (MergeContext::PushOperand with operand_pinned = true).
    private readonly ReadOnlySpan<Db.Iterators.PinnedSlice> _slices;
    // Bytes hidden at the end of every operand (a view without the TTL write-time suffix, TtlMergeOperator).
    private readonly int _suffixLength;

    public MergeOperands(ReadOnlySpan<byte> data, ReadOnlySpan<int> offsets)
    {
        _data = data;
        _offsets = offsets;
        _slices = default;
        _suffixLength = 0;
    }

    /// <summary>Operands that are slices of pinned memory, oldest first (no copy).</summary>
    internal MergeOperands(ReadOnlySpan<Db.Iterators.PinnedSlice> slices)
    {
        _data = default;
        _offsets = default;
        _slices = slices;
        _suffixLength = 0;
    }

    private MergeOperands(ReadOnlySpan<byte> data, ReadOnlySpan<int> offsets, ReadOnlySpan<Db.Iterators.PinnedSlice> slices, int suffixLength)
    {
        _data = data;
        _offsets = offsets;
        _slices = slices;
        _suffixLength = suffixLength;
    }

    public int Count => _slices.Length != 0 ? _slices.Length : _offsets.Length == 0 ? 0 : _offsets.Length - 1;

    public ReadOnlySpan<byte> this[int index]
    {
        get
        {
            ReadOnlySpan<byte> s = _slices.Length != 0 ? _slices[index].Span : _data[_offsets[index].._offsets[index + 1]];
            return _suffixLength == 0 ? s : s[..^_suffixLength];
        }
    }

    /// <summary>
    /// The same operands without their last <paramref name="length"/> bytes (no copy). REQUIRES: every operand is at
    /// least that long.
    /// </summary>
    public MergeOperands WithoutSuffix(int length)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        return new MergeOperands(_data, _offsets, _slices, _suffixLength + length);
    }
}
