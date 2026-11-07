using System.Buffers;
using System.Buffers.Text;
using System.Diagnostics;
using System.Numerics;
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

    public override string NickName => "uint64add";

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

    public override string NickName => "stringappend";

    public ReadOnlySpan<byte> Delimiter => _delimiter;

    internal override string OptionsId => FormatOptionsId(Name, _delimiter);

    /// <summary>
    /// The OPTIONS text native writes for a string append operator: its "delimiter" option (RegisterOptions in
    /// stringappend.cc / stringappend2.cc) makes it a struct, "{id=Name;delimiter=X;}".
    /// </summary>
    internal static string FormatOptionsId(string name, byte[] delimiter) =>
        "{id=" + name + ";delimiter=" + Encoding.UTF8.GetString(delimiter) + ";}";

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

/// <summary>
/// Keeps the byte-wise largest value. Port of rocksdb::MaxOperator (utilities/merge_operators/max.cc): the existing
/// value (or an empty slice) and every operand are compared with Slice::compare (memcmp, then length) and the largest
/// wins; PartialMergeMulti starts from an empty slice. The name "MaxOperator" (nick name "max") matches upstream so
/// OPTIONS files agree.
/// </summary>
public sealed class MaxOperator : MergeOperator
{
    public static MaxOperator Instance { get; } = new();

    public override string Name => "MaxOperator";

    public override string NickName => "max";

    public override bool FullMerge(ReadOnlySpan<byte> key, bool hasExistingValue, ReadOnlySpan<byte> existingValue,
        in MergeOperands operands, IBufferWriter<byte> result)
    {
        ReadOnlySpan<byte> max = hasExistingValue ? existingValue : default;
        for (int i = 0; i < operands.Count; i++)
        {
            ReadOnlySpan<byte> op = operands[i];
            if (max.SequenceCompareTo(op) < 0) max = op;
        }
        result.Write(max);
        return true;
    }

    public override bool PartialMerge(ReadOnlySpan<byte> key, in MergeOperands operands, IBufferWriter<byte> result)
    {
        ReadOnlySpan<byte> max = default;
        for (int i = 0; i < operands.Count; i++)
        {
            ReadOnlySpan<byte> op = operands[i];
            if (max.SequenceCompareTo(op) < 0) max = op;
        }
        result.Write(max);
        return true;
    }
}

/// <summary>
/// The newest operand replaces the value (a merge that behaves like Put). Port of rocksdb::PutOperator
/// (utilities/merge_operators/put.cc), the legacy FullMerge variant created by
/// MergeOperators::CreateDeprecatedPutOperator(): FullMerge and PartialMergeMulti both yield the newest operand.
/// Name "PutOperator", nick name "put_v1" (upstream registers only the nick name for this class: the class name
/// "PutOperator" resolves to <see cref="PutOperatorV2"/>, so an OPTIONS file written with this operator reloads as V2).
/// </summary>
public class PutOperator : MergeOperator
{
    public static PutOperator Instance { get; } = new();

    private protected PutOperator() { }

    public override string Name => "PutOperator";

    public override string NickName => "put_v1";

    /// <summary>
    /// V1: FullMerge(existing, operand_sequence) assigns operand_sequence.back(); V2 (FullMergeV2) sets
    /// existing_operand to operand_list.back(). Both yield the newest operand; the existing value is ignored.
    /// </summary>
    public sealed override bool FullMerge(ReadOnlySpan<byte> key, bool hasExistingValue, ReadOnlySpan<byte> existingValue,
        in MergeOperands operands, IBufferWriter<byte> result)
    {
        Debug.Assert(operands.Count > 0);
        result.Write(operands[operands.Count - 1]);
        return true;
    }

    /// <summary>PartialMergeMulti: operand_list.back() (PartialMerge keeps the right operand).</summary>
    public sealed override bool PartialMerge(ReadOnlySpan<byte> key, in MergeOperands operands, IBufferWriter<byte> result)
    {
        result.Write(operands[operands.Count - 1]);
        return true;
    }
}

/// <summary>
/// Port of rocksdb::PutOperatorV2 (utilities/merge_operators/put.cc), the FullMergeV2 put operator created by
/// MergeOperators::CreatePutOperator(): same results as <see cref="PutOperator"/>. Name "PutOperator", nick name "put".
/// </summary>
public sealed class PutOperatorV2 : PutOperator
{
    public static new PutOperatorV2 Instance { get; } = new();

    private PutOperatorV2() { }

    public override string NickName => "put";
}

/// <summary>
/// Merges comma-separated lists of decimal ints into one sorted list. Port of rocksdb::SortList
/// (utilities/merge_operators/sortlist.cc), name "MergeSortOperator", nick name "sortlist".
/// FullMerge ignores the existing value (as upstream) and folds a two-way sorted merge over the operands, oldest first;
/// each operand is split at ',' (or a NUL byte) and every piece is parsed like std::stoi (leading whitespace, optional
/// sign, digits, trailing garbage ignored). The result is the numbers joined with ',' (std::to_string).
/// <para>
/// Deviations: a piece std::stoi rejects (no digits, e.g. an empty list or a trailing comma, or out of int range)
/// throws std::invalid_argument / std::out_of_range upstream; here the merge fails (Corruption, MergeOperatorFailed).
/// Upstream's PartialMergeMulti returns true without writing a result, which would replace the operands by an empty
/// operand that no later merge can parse; here PartialMerge returns false (the operands are kept), so merge results
/// are the ones upstream computes when no partial merge happens.
/// </para>
/// </summary>
public sealed class SortListOperator : MergeOperator
{
    public static SortListOperator Instance { get; } = new();

    public override string Name => "MergeSortOperator";

    public override string NickName => "sortlist";

    public override bool FullMerge(ReadOnlySpan<byte> key, bool hasExistingValue, ReadOnlySpan<byte> existingValue,
        in MergeOperands operands, IBufferWriter<byte> result)
    {
        int capacity = 0;
        for (int i = 0; i < operands.Count; i++) capacity += operands[i].Length / 2 + 1; // each number takes a byte + ','
        int[] left = RocksArrayPool<int>.Shared.Rent(capacity);
        int[] right = RocksArrayPool<int>.Shared.Rent(capacity);
        int[] merged = RocksArrayPool<int>.Shared.Rent(capacity);
        try
        {
            int leftCount = 0;
            for (int i = 0; i < operands.Count; i++)
            {
                if (!TryMakeVector(operands[i], right, out int rightCount)) return false;
                int n = Merge(left.AsSpan(0, leftCount), right.AsSpan(0, rightCount), merged);
                (left, merged) = (merged, left);
                leftCount = n;
            }
            Debug.Assert(leftCount > 0);
            Span<byte> scratch = stackalloc byte[12]; // "-2147483648,"
            for (int i = 0; i < leftCount; i++)
            {
                Utf8Formatter.TryFormat(left[i], scratch, out int written);
                if (i < leftCount - 1) scratch[written++] = (byte)',';
                result.Write(scratch[..written]);
            }
            return true;
        }
        finally
        {
            RocksArrayPool<int>.Shared.Return(left);
            RocksArrayPool<int>.Shared.Return(right);
            RocksArrayPool<int>.Shared.Return(merged);
        }
    }

    /// <summary>
    /// Port of SortList::MakeVector: splits at ',' and stops at the end of the slice (upstream relies on a NUL
    /// terminator, which a slice of block or memtable memory does not have; a NUL byte also ends the list here).
    /// Returns false where std::stoi would throw.
    /// </summary>
    internal static bool TryMakeVector(ReadOnlySpan<byte> slice, Span<int> operand, out int count)
    {
        count = 0;
        int pos = 0;
        while (true)
        {
            int begin = pos;
            while (pos < slice.Length && slice[pos] != ',' && slice[pos] != 0) pos++;
            if (!TryStoi(slice[begin..pos], out int v)) return false;
            operand[count++] = v;
            // while (0 != *slice.data_++): continue only after a ','.
            if (pos >= slice.Length || slice[pos] == 0) return true;
            pos++;
        }
    }

    /// <summary>std::stoi(str) (strtol base 10 plus the int range check); false where stoi throws.</summary>
    internal static bool TryStoi(ReadOnlySpan<byte> s, out int value)
    {
        value = 0;
        int i = 0;
        while (i < s.Length && (s[i] == ' ' || (s[i] >= '\t' && s[i] <= '\r'))) i++; // isspace in the "C" locale
        bool negative = false;
        if (i < s.Length && (s[i] == '+' || s[i] == '-'))
        {
            negative = s[i] == '-';
            i++;
        }
        int digitsStart = i;
        long acc = 0;
        bool overflow = false;
        while (i < s.Length && (uint)(s[i] - '0') <= 9)
        {
            if (!overflow)
            {
                acc = acc * 10 + (s[i] - '0');
                if (acc > 1L << 32) overflow = true; // already outside int range; keep consuming digits
            }
            i++;
        }
        if (i == digitsStart) return false; // std::invalid_argument
        if (negative) acc = -acc;
        if (overflow || acc < int.MinValue || acc > int.MaxValue) return false; // std::out_of_range
        value = (int)acc;
        return true;
    }

    /// <summary>Port of SortList::Merge: two-way merge, taking from the right on ties.</summary>
    private static int Merge(ReadOnlySpan<int> left, ReadOnlySpan<int> right, Span<int> result)
    {
        int l = 0, r = 0, n = 0;
        while (l < left.Length && r < right.Length)
        {
            if (left[l] < right[r]) result[n++] = left[l++];
            else result[n++] = right[r++];
        }
        while (l < left.Length) result[n++] = left[l++];
        while (r < right.Length) result[n++] = right[r++];
        return n;
    }
}

/// <summary>
/// XORs values byte by byte. Port of rocksdb::BytesXOROperator (utilities/merge_operators/bytesxor.cc), an
/// AssociativeMergeOperator: Merge(existing, v) is v when there is no existing value, else the XOR of the common
/// prefix followed by the tail of the longer input, i.e. the XOR of both inputs zero-padded to the longer length.
/// Name "BytesXOR", nick name "bytesxor".
/// </summary>
public sealed class BytesXorOperator : MergeOperator
{
    public static BytesXorOperator Instance { get; } = new();

    public override string Name => "BytesXOR";

    public override string NickName => "bytesxor";

    public override bool FullMerge(ReadOnlySpan<byte> key, bool hasExistingValue, ReadOnlySpan<byte> existingValue,
        in MergeOperands operands, IBufferWriter<byte> result)
    {
        int max = hasExistingValue ? existingValue.Length : 0;
        for (int i = 0; i < operands.Count; i++) max = Math.Max(max, operands[i].Length);
        Span<byte> dst = result.GetSpan(max)[..max];
        dst.Clear();
        if (hasExistingValue) existingValue.CopyTo(dst);
        for (int i = 0; i < operands.Count; i++) Xor(dst, operands[i]);
        result.Advance(max);
        return true;
    }

    /// <summary>AssociativeMergeOperator::PartialMerge folded over the operands (PartialMergeMulti).</summary>
    public override bool PartialMerge(ReadOnlySpan<byte> key, in MergeOperands operands, IBufferWriter<byte> result)
    {
        int max = 0;
        for (int i = 0; i < operands.Count; i++) max = Math.Max(max, operands[i].Length);
        Span<byte> dst = result.GetSpan(max)[..max];
        dst.Clear();
        for (int i = 0; i < operands.Count; i++) Xor(dst, operands[i]);
        result.Advance(max);
        return true;
    }

    private static void Xor(Span<byte> dst, ReadOnlySpan<byte> src)
    {
        int i = 0;
        if (Vector.IsHardwareAccelerated)
        {
            for (; i + Vector<byte>.Count <= src.Length; i += Vector<byte>.Count)
            {
                Vector<byte> v = new Vector<byte>(dst[i..]) ^ new Vector<byte>(src[i..]);
                v.CopyTo(dst[i..]);
            }
        }
        for (; i < src.Length; i++) dst[i] ^= src[i];
    }
}

/// <summary>
/// String append built on the FullMergeV2 interface, used by upstream tests and benchmarks. Port of
/// rocksdb::StringAppendTESTOperator (utilities/merge_operators/string_append/stringappend2.cc): FullMerge produces the
/// same result as <see cref="StringAppendOperator"/> (<c>existing + delim + op1 + delim + ...</c>, or the operands
/// joined when there is no existing value) and PartialMerge is not supported (PartialMergeMulti returns false).
/// Name "StringAppendTESTOperator", nick name "stringappendtest"; default delimiter ",".
/// </summary>
public sealed class StringAppendTestOperator : MergeOperator
{
    private readonly byte[] _delimiter;

    /// <summary>Upstream's default (MergeOperators::CreateStringAppendTESTOperator()): delimiter ','.</summary>
    public StringAppendTestOperator() : this(","u8) { }

    public StringAppendTestOperator(ReadOnlySpan<byte> delimiter) => _delimiter = delimiter.ToArray();

    public StringAppendTestOperator(string delimiter) => _delimiter = Encoding.UTF8.GetBytes(delimiter);

    public override string Name => "StringAppendTESTOperator";

    public override string NickName => "stringappendtest";

    public ReadOnlySpan<byte> Delimiter => _delimiter;

    internal override string OptionsId => StringAppendOperator.FormatOptionsId(Name, _delimiter);

    public override bool FullMerge(ReadOnlySpan<byte> key, bool hasExistingValue, ReadOnlySpan<byte> existingValue,
        in MergeOperands operands, IBufferWriter<byte> result)
    {
        bool printDelim = false;
        if (hasExistingValue)
        {
            result.Write(existingValue);
            printDelim = true;
        }
        for (int i = 0; i < operands.Count; i++)
        {
            if (printDelim) result.Write(_delimiter);
            result.Write(operands[i]);
            printDelim = true;
        }
        return true;
    }
}
