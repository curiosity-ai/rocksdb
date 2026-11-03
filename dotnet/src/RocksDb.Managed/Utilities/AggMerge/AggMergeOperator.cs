using System.Buffers;
using RocksDb.Managed.Util;
using RocksDb.Managed.Utilities.Transactions;

namespace RocksDb.Managed.Utilities.AggMerge;

/// <summary>
/// An aggregation function used by <see cref="AggMergeOperator"/>. Port of rocksdb::Aggregator
/// (include/rocksdb/utilities/agg_merge.h). Implementations are registered by name with
/// <see cref="AggMerge.AddAggregator"/> and must be thread-safe (merges run concurrently on reads, flush and compaction).
/// </summary>
public abstract class Aggregator
{
    /// <summary>
    /// Aggregates the payloads in <paramref name="values"/> and writes the aggregated payload to
    /// <paramref name="result"/> (which is empty on entry). Returns false on an aggregation error (e.g. a payload in
    /// the wrong format). The values are in insertion order: index 0 is the oldest (possibly the base value from a Put,
    /// or the aggregate of older operands under a different function) and the last one is the newest. (Upstream's
    /// header comment says the reverse, but agg_merge.cc passes them oldest first, which its Last3 test aggregator
    /// relies on; this port follows the code.)
    /// </summary>
    public abstract bool Aggregate(in AggregatorValues values, IBufferWriter<byte> result);

    /// <summary>
    /// True if a partial aggregation (PartialMergeMulti during compaction) should be invoked; an aggregator can opt
    /// out, its operands are then kept until a full merge. Default true.
    /// </summary>
    public virtual bool DoPartialAggregate => true;
}

/// <summary>
/// The payloads passed to <see cref="Aggregator.Aggregate"/>, oldest first. Views of the merge operands (no copies),
/// valid only during the call.
/// </summary>
public readonly ref struct AggregatorValues
{
    private readonly ReadOnlySpan<byte> _existing;
    private readonly MergeOperands _operands;
    private readonly ReadOnlySpan<byte> _aggregated;
    private readonly ReadOnlySpan<AggMergeOperator.ValueRef> _refs;

    internal AggregatorValues(ReadOnlySpan<byte> existing, in MergeOperands operands, ReadOnlySpan<byte> aggregated,
        ReadOnlySpan<AggMergeOperator.ValueRef> refs)
    {
        _existing = existing;
        _operands = operands;
        _aggregated = aggregated;
        _refs = refs;
    }

    public int Count => _refs.Length;

    public ReadOnlySpan<byte> this[int index]
    {
        get
        {
            AggMergeOperator.ValueRef r = _refs[index];
            ReadOnlySpan<byte> source = r.Source switch
            {
                AggMergeOperator.ValueRef.Existing => _existing,
                AggMergeOperator.ValueRef.Aggregated => _aggregated,
                _ => _operands[r.Source],
            };
            return source[r.Start..];
        }
    }
}

/// <summary>
/// Helpers of the aggregation merge operator's value format and its aggregator registry. Port of the free functions
/// in include/rocksdb/utilities/agg_merge.h / utilities/agg_merge/agg_merge.cc.
/// <para>
/// Format (byte-identical to upstream): every value and operand is <c>varint32 len(func) | func | payload</c>
/// (PutLengthPrefixedSlice of the function name followed by the raw payload). A value whose function is
/// <see cref="ErrorFuncName"/> holds the operands that could not be aggregated, each length-prefixed (decode the
/// payload with <see cref="ExtractList"/>).
/// </para>
/// </summary>
public static class AggMerge
{
    /// <summary>kUnnamedFuncName: the empty function name, for a base value (Put) that takes the function of later operands.</summary>
    public const string UnnamedFuncName = "";

    /// <summary>kErrorFuncName: the function name of a value that packs operands which could not be aggregated.</summary>
    public const string ErrorFuncName = "kErrorFuncName";

    internal static ReadOnlySpan<byte> ErrorFuncNameBytes => "kErrorFuncName"u8;

    private static readonly Lock s_lock = new();
    // Copy-on-write registry (upstream: a static std::unordered_map func_map); readers never lock.
    private static Dictionary<byte[], Aggregator> s_funcs = new(ByteKeyComparer.Instance);

    /// <summary>
    /// Port of AddAggregator: registers <paramref name="aggregator"/> under <paramref name="functionName"/> for every
    /// <see cref="AggMergeOperator"/> in the process. Like upstream (std::unordered_map::emplace) an existing
    /// registration is kept; returns false in that case. Throws InvalidArgument for <see cref="ErrorFuncName"/>.
    /// </summary>
    public static bool AddAggregator(string functionName, Aggregator aggregator)
    {
        ArgumentNullException.ThrowIfNull(functionName);
        ArgumentNullException.ThrowIfNull(aggregator);
        if (functionName == ErrorFuncName) throw RocksDbException.InvalidArgument("Cannot register function name kErrorFuncName");
        byte[] key = System.Text.Encoding.UTF8.GetBytes(functionName);
        lock (s_lock)
        {
            if (s_funcs.ContainsKey(key)) return false;
            var copy = new Dictionary<byte[], Aggregator>(s_funcs, ByteKeyComparer.Instance) { [key] = aggregator };
            Volatile.Write(ref s_funcs, copy);
            return true;
        }
    }

    internal static Aggregator? Find(ReadOnlySpan<byte> functionName) =>
        Volatile.Read(ref s_funcs).GetAlternateLookup<ReadOnlySpan<byte>>().TryGetValue(functionName, out Aggregator? a) ? a : null;

    /// <summary>
    /// Port of EncodeAggFuncAndPayload: appends <c>varint32 len | functionName | payload</c> to
    /// <paramref name="output"/>. Throws InvalidArgument for <see cref="ErrorFuncName"/> and for a name that is neither
    /// <see cref="UnnamedFuncName"/> nor registered.
    /// </summary>
    public static void EncodeAggFuncAndPayload(ReadOnlySpan<byte> functionName, ReadOnlySpan<byte> payload, IBufferWriter<byte> output)
    {
        ArgumentNullException.ThrowIfNull(output);
        if (functionName.SequenceEqual(ErrorFuncNameBytes)) throw RocksDbException.InvalidArgument("Cannot use error function name");
        if (!functionName.IsEmpty && Find(functionName) is null) throw RocksDbException.InvalidArgument("Function name not registered");
        EncodeAggFuncAndPayloadNoCheck(functionName, payload, output);
    }

    /// <summary><see cref="EncodeAggFuncAndPayload(ReadOnlySpan{byte}, ReadOnlySpan{byte}, IBufferWriter{byte})"/> into a new array.</summary>
    public static byte[] EncodeAggFuncAndPayload(string functionName, ReadOnlySpan<byte> payload)
    {
        ArgumentNullException.ThrowIfNull(functionName);
        var w = new ArrayBufferWriter<byte>();
        EncodeAggFuncAndPayload(System.Text.Encoding.UTF8.GetBytes(functionName), payload, w);
        return w.WrittenSpan.ToArray();
    }

    /// <summary>Port of EncodeAggFuncAndPayloadNoCheck (agg_merge_impl.h): no validation of the function name.</summary>
    internal static void EncodeAggFuncAndPayloadNoCheck(ReadOnlySpan<byte> functionName, ReadOnlySpan<byte> payload, IBufferWriter<byte> output)
    {
        Coding.PutLengthPrefixedSlice(output, functionName);
        output.Write(payload);
    }

    /// <summary>
    /// Port of ExtractAggFuncAndValue: splits an encoded value into its function name and payload. Returns false if
    /// the length prefix is malformed.
    /// </summary>
    public static bool TryExtractAggFuncAndValue(ReadOnlySpan<byte> op, out ReadOnlySpan<byte> func, out ReadOnlySpan<byte> value)
    {
        // GetLengthPrefixedSlice(&value, &func) with value = op.
        int prefix = Coding.DecodeVarint32(op, out uint len);
        if (prefix == 0 || (uint)(op.Length - prefix) < len)
        {
            func = default;
            value = op[prefix..];
            return false;
        }
        func = op.Slice(prefix, (int)len);
        value = op[(prefix + (int)len)..];
        return true;
    }

    /// <summary>
    /// Port of ExtractList: decodes a list of length-prefixed items (the payload of an <see cref="ErrorFuncName"/>
    /// value) into <paramref name="decodedList"/> (cleared first). Returns whether the whole input was consumed.
    /// </summary>
    public static bool ExtractList(ReadOnlyMemory<byte> encodedList, List<ReadOnlyMemory<byte>> decodedList)
    {
        ArgumentNullException.ThrowIfNull(decodedList);
        decodedList.Clear();
        ReadOnlySpan<byte> list = encodedList.Span;
        while (true)
        {
            int before = list.Length;
            if (!Coding.GetLengthPrefixedSlice(ref list, out ReadOnlySpan<byte> item)) break;
            int start = encodedList.Length - before + (before - list.Length - item.Length);
            decodedList.Add(encodedList.Slice(start, item.Length));
        }
        return list.IsEmpty;
    }
}

/// <summary>
/// The aggregation merge operator. Port of rocksdb::AggMergeOperator (utilities/agg_merge/agg_merge.cc,
/// agg_merge_impl.h), name "AggMergeOperator.v1", obtained by upstream's GetAggMergeOperator() (here
/// <see cref="Instance"/>). Every value and operand names its aggregation function (<see cref="AggMerge"/> describes
/// the encoding); operands are aggregated by the <see cref="Aggregator"/> registered under that name. When the
/// function changes, the older operands are aggregated first and their result becomes the first value of the new
/// function. When aggregation is impossible (unregistered function, aggregator failure, no named function) a full
/// merge does not fail: the result is an <see cref="AggMerge.ErrorFuncName"/> value packing the existing value and all
/// operands. A malformed operand makes the accumulator ignore it and every later operand (as upstream). Partial merges
/// (compaction) only aggregate runs of one function whose aggregator allows it. Single operands are partially merged
/// too (AllowSingleOperand). Allocation-free in steady state: the accumulator is thread-local, values are views of
/// the operands.
/// </summary>
public sealed class AggMergeOperator : MergeOperator
{
    public const string ClassName = "AggMergeOperator.v1";

    public static AggMergeOperator Instance { get; } = new();

    private AggMergeOperator() { }

    public override string Name => ClassName;

    public override bool AllowSingleOperand => true;

    [ThreadStatic] private static Accumulator? t_accumulator;

    private static Accumulator GetTlsAccumulator()
    {
        Accumulator acc = t_accumulator ??= new Accumulator();
        acc.Clear();
        return acc;
    }

    public override bool FullMerge(ReadOnlySpan<byte> key, bool hasExistingValue, ReadOnlySpan<byte> existingValue,
        in MergeOperands operands, IBufferWriter<byte> result)
    {
        Accumulator agg = GetTlsAccumulator();
        try
        {
            if (hasExistingValue) agg.Add(existingValue, ValueRef.Existing, existingValue, operands, isPartialAggregation: false);
            for (int i = 0; i < operands.Count; i++) agg.Add(operands[i], i, existingValue, operands, isPartialAggregation: false);
            if (!agg.TryGetResult(existingValue, operands, result))
            {
                // If aggregation can't happen, pack all merge operands. In contrast to a merge operator failure, the
                // DB keeps functioning for other keys and the operands are preserved for debugging.
                PackAllMergeOperands(hasExistingValue, existingValue, operands, result);
            }
        }
        finally
        {
            agg.Clear();
        }
        return true;
    }

    public override bool PartialMerge(ReadOnlySpan<byte> key, in MergeOperands operands, IBufferWriter<byte> result)
    {
        Accumulator agg = GetTlsAccumulator();
        try
        {
            bool doAggregation = true;
            for (int i = 0; i < operands.Count; i++)
            {
                doAggregation = agg.Add(operands[i], i, default, operands, isPartialAggregation: true);
                if (!doAggregation) break;
            }
            return doAggregation && agg.TryGetResult(default, operands, result);
        }
        finally
        {
            agg.Clear();
        }
    }

    private static void PackAllMergeOperands(bool hasExistingValue, ReadOnlySpan<byte> existingValue, in MergeOperands operands,
        IBufferWriter<byte> result)
    {
        Coding.PutLengthPrefixedSlice(result, AggMerge.ErrorFuncNameBytes);
        if (hasExistingValue) Coding.PutLengthPrefixedSlice(result, existingValue);
        for (int i = 0; i < operands.Count; i++) Coding.PutLengthPrefixedSlice(result, operands[i]);
    }

    /// <summary>A value of the accumulator: <c>source[Start..]</c> of the existing value, an operand, or the aggregate buffer.</summary>
    internal readonly struct ValueRef(int source, int start)
    {
        public const int Existing = -1;
        public const int Aggregated = -2;

        public readonly int Source = source;
        public readonly int Start = start;
    }

    /// <summary>Port of AggMergeOperator::Accumulator. Slices are kept as (source, offset) pairs over the merge inputs.</summary>
    private sealed class Accumulator
    {
        private ValueRef[] _values = new ValueRef[16];
        private int _count;
        private int _funcSource, _funcStart, _funcLength; // func_: a slice of an operand (or the existing value)
        private PooledBufferWriter _aggregated = new(64);
        private PooledBufferWriter _scratch = new(64);
        private bool _ignoreOperands;
        private bool _funcValid;

        private ReadOnlySpan<byte> Source(int source, ReadOnlySpan<byte> existing, in MergeOperands operands) => source switch
        {
            ValueRef.Existing => existing,
            ValueRef.Aggregated => _aggregated.WrittenSpan,
            _ => operands[source],
        };

        private ReadOnlySpan<byte> Func(ReadOnlySpan<byte> existing, in MergeOperands operands) =>
            Source(_funcSource, existing, operands).Slice(_funcStart, _funcLength);

        private void Push(ValueRef v)
        {
            if (_count == _values.Length) Array.Resize(ref _values, _count * 2);
            _values[_count++] = v;
        }

        private bool Aggregate(Aggregator f, ReadOnlySpan<byte> existing, in MergeOperands operands)
        {
            _scratch.Clear();
            return f.Aggregate(new AggregatorValues(existing, operands, _aggregated.WrittenSpan, _values.AsSpan(0, _count)), _scratch);
        }

        /// <summary>Accumulator::Add. <paramref name="source"/> identifies <paramref name="op"/> among the merge inputs.</summary>
        public bool Add(ReadOnlySpan<byte> op, int source, ReadOnlySpan<byte> existing, in MergeOperands operands, bool isPartialAggregation)
        {
            if (_ignoreOperands) return true;
            if (!AggMerge.TryExtractAggFuncAndValue(op, out ReadOnlySpan<byte> myFunc, out ReadOnlySpan<byte> myValue))
            {
                _ignoreOperands = true;
                return true;
            }

            // Determine whether we need to do partial merge.
            if (isPartialAggregation && !myFunc.IsEmpty)
            {
                Aggregator? f = AggMerge.Find(myFunc);
                if (f is null || !f.DoPartialAggregate) return false;
            }

            if (!_funcValid)
            {
                if (!myFunc.IsEmpty)
                {
                    SetFunc(op, source, myFunc, myValue);
                    _funcValid = true;
                }
            }
            else if (!Func(existing, operands).SequenceEqual(myFunc))
            {
                // User switched aggregation function. Need to aggregate the older one first.
                // Previous aggregation can't be done in partial merge.
                if (isPartialAggregation)
                {
                    _funcValid = false;
                    _ignoreOperands = true;
                    return false;
                }
                Aggregator? f = AggMerge.Find(Func(existing, operands));
                if (f is null || !Aggregate(f, existing, operands))
                {
                    _funcValid = false;
                    _ignoreOperands = true;
                    return true;
                }
                (_scratch, _aggregated) = (_aggregated, _scratch);
                _count = 0;
                Push(new ValueRef(ValueRef.Aggregated, 0));
                SetFunc(op, source, myFunc, myValue);
            }
            Push(new ValueRef(source, op.Length - myValue.Length));
            return true;
        }

        /// <summary>func_ = my_func, kept as a position in <paramref name="op"/> (op = varint32 | func | value).</summary>
        private void SetFunc(ReadOnlySpan<byte> op, int source, ReadOnlySpan<byte> myFunc, ReadOnlySpan<byte> myValue)
        {
            _funcSource = source;
            _funcLength = myFunc.Length;
            _funcStart = op.Length - myValue.Length - myFunc.Length;
        }

        /// <summary>Accumulator::GetResult: writes the encoded aggregate to <paramref name="result"/> on success.</summary>
        public bool TryGetResult(ReadOnlySpan<byte> existing, in MergeOperands operands, IBufferWriter<byte> result)
        {
            if (!_funcValid) return false;
            ReadOnlySpan<byte> func = Func(existing, operands);
            Aggregator? f = AggMerge.Find(func);
            if (f is null || !Aggregate(f, existing, operands)) return false;
            AggMerge.EncodeAggFuncAndPayloadNoCheck(func, _scratch.WrittenSpan, result);
            return true;
        }

        public void Clear()
        {
            _count = 0;
            _funcSource = _funcStart = _funcLength = 0;
            _aggregated.Clear();
            _scratch.Clear();
            _ignoreOperands = false;
            _funcValid = false;
        }
    }
}
