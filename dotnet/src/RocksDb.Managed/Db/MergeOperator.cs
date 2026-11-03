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
            foreach (KeyValuePair<string, string> kv in Options.OptionStrings.ParseStruct("merge_operator", id))
            {
                if (kv.Key == "id") inner = kv.Value;
                else if (kv.Key == "delimiter") delimiter = kv.Value;
                else return null;
            }
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

    public MergeOperands(ReadOnlySpan<byte> data, ReadOnlySpan<int> offsets)
    {
        _data = data;
        _offsets = offsets;
        _slices = default;
    }

    /// <summary>Operands that are slices of pinned memory, oldest first (no copy).</summary>
    internal MergeOperands(ReadOnlySpan<Db.Iterators.PinnedSlice> slices)
    {
        _data = default;
        _offsets = default;
        _slices = slices;
    }

    public int Count => _slices.Length != 0 ? _slices.Length : _offsets.Length == 0 ? 0 : _offsets.Length - 1;
    public ReadOnlySpan<byte> this[int index] => _slices.Length != 0 ? _slices[index].Span : _data[_offsets[index].._offsets[index + 1]];
}
