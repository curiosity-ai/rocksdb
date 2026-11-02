using System.Buffers;
using System.Text;
using RocksDb.Managed.Util;

namespace RocksDb.Managed;

/// <summary>
/// Adds unsigned 64-bit little-endian integers. Port of rocksdb::UInt64AddOperator
/// (utilities/merge_operators/uint64add.cc), an AssociativeMergeOperator: values of the wrong size count as 0
/// (upstream logs and treats them as 0). The name "UInt64AddOperator" matches upstream so OPTIONS files agree.
/// </summary>
public sealed class UInt64AddOperator : MergeOperator
{
    public static UInt64AddOperator Instance { get; } = new();

    public override string Name => "UInt64AddOperator";

    private static ulong DecodeInteger(ReadOnlySpan<byte> value) => value.Length == sizeof(ulong) ? Coding.DecodeFixed64(value) : 0;

    public override bool FullMerge(ReadOnlySpan<byte> key, bool hasExistingValue, ReadOnlySpan<byte> existingValue,
        in MergeOperands operands, IBufferWriter<byte> result)
    {
        ulong sum = hasExistingValue ? DecodeInteger(existingValue) : 0;
        for (int i = 0; i < operands.Count; i++) sum += DecodeInteger(operands[i]);
        Coding.EncodeFixed64(result.GetSpan(sizeof(ulong)), sum);
        result.Advance(sizeof(ulong));
        return true;
    }

    /// <summary>AssociativeMergeOperator::PartialMerge folded over the operands (PartialMergeMulti).</summary>
    public override bool PartialMerge(ReadOnlySpan<byte> key, in MergeOperands operands, IBufferWriter<byte> result)
    {
        ulong sum = 0;
        for (int i = 0; i < operands.Count; i++) sum += DecodeInteger(operands[i]);
        Coding.EncodeFixed64(result.GetSpan(sizeof(ulong)), sum);
        result.Advance(sizeof(ulong));
        return true;
    }
}

/// <summary>
/// Concatenates values with a delimiter. Port of rocksdb::StringAppendOperator
/// (utilities/merge_operators/string_append/stringappend.cc), an AssociativeMergeOperator:
/// <c>Merge(existing, v) = existing == null ? v : existing + delim + v</c>. Default delimiter ",".
/// The name "StringAppendOperator" matches upstream so OPTIONS files agree.
/// </summary>
public sealed class StringAppendOperator : MergeOperator
{
    private readonly byte[] _delimiter;

    /// <summary>Upstream's default (MergeOperators::CreateStringAppendOperator()): delimiter ','.</summary>
    public StringAppendOperator() : this(","u8) { }

    public StringAppendOperator(ReadOnlySpan<byte> delimiter) => _delimiter = delimiter.ToArray();

    public StringAppendOperator(string delimiter) => _delimiter = Encoding.UTF8.GetBytes(delimiter);

    public override string Name => "StringAppendOperator";

    public ReadOnlySpan<byte> Delimiter => _delimiter;

    public override bool FullMerge(ReadOnlySpan<byte> key, bool hasExistingValue, ReadOnlySpan<byte> existingValue,
        in MergeOperands operands, IBufferWriter<byte> result)
    {
        bool any = hasExistingValue;
        if (hasExistingValue) result.Write(existingValue);
        for (int i = 0; i < operands.Count; i++)
        {
            if (any) result.Write(_delimiter);
            result.Write(operands[i]);
            any = true;
        }
        return true;
    }

    public override bool PartialMerge(ReadOnlySpan<byte> key, in MergeOperands operands, IBufferWriter<byte> result)
    {
        for (int i = 0; i < operands.Count; i++)
        {
            if (i > 0) result.Write(_delimiter);
            result.Write(operands[i]);
        }
        return true;
    }
}
