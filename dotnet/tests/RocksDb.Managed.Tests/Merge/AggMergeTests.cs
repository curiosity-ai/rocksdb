using System.Buffers;
using System.Text;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Util;
using RocksDb.Managed.Utilities.AggMerge;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Merge;

/// <summary>
/// Port of utilities/agg_merge/agg_merge_test.cc with the test aggregators of utilities/agg_merge/test_agg_merge.cc
/// (SumAggregator, MultipleAggregator, Last3Aggregator, EncodeHelper), plus unit tests of the value encoding and of
/// partial aggregation.
/// </summary>
[TestClass]
public sealed class AggMergeTests
{
    /// <summary>Port of SumAggregator: sums PutVarsignedint64 payloads.</summary>
    internal sealed class SumAggregator : Aggregator
    {
        public override bool Aggregate(in AggregatorValues values, IBufferWriter<byte> result)
        {
            long sum = 0;
            for (int i = 0; i < values.Count; i++)
            {
                if (!TryDecodeInt(values[i], out long v)) return false;
                sum += v;
            }
            Coding.PutVarint64(result, Coding.ZigZagEncode64(sum));
            return true;
        }
    }

    /// <summary>Port of MultipleAggregator: multiplies PutVarsignedint64 payloads.</summary>
    internal sealed class MultipleAggregator : Aggregator
    {
        public override bool Aggregate(in AggregatorValues values, IBufferWriter<byte> result)
        {
            long product = 1;
            for (int i = 0; i < values.Count; i++)
            {
                if (!TryDecodeInt(values[i], out long v)) return false;
                product *= v;
            }
            Coding.PutVarint64(result, Coding.ZigZagEncode64(product));
            return true;
        }
    }

    /// <summary>Port of Last3Aggregator: the newest three items of length-prefixed lists (no partial-aggregate opt-out).</summary>
    internal sealed class Last3Aggregator : Aggregator
    {
        public override bool Aggregate(in AggregatorValues values, IBufferWriter<byte> result)
        {
            int n = 0;
            for (int i = values.Count - 1; i >= 0 && n < 3; i--)
            {
                ReadOnlySpan<byte> input = values[i];
                while (n < 3 && Coding.GetLengthPrefixedSlice(ref input, out ReadOnlySpan<byte> entity))
                {
                    Coding.PutLengthPrefixedSlice(result, entity);
                    n++;
                }
            }
            return true;
        }
    }

    /// <summary>An aggregator that opts out of partial aggregation (DoPartialAggregate() == false).</summary>
    internal sealed class NoPartialSumAggregator : Aggregator
    {
        private readonly SumAggregator _sum = new();
        public override bool DoPartialAggregate => false;
        public override bool Aggregate(in AggregatorValues values, IBufferWriter<byte> result) => _sum.Aggregate(values, result);
    }

    private static bool TryDecodeInt(ReadOnlySpan<byte> v, out long value)
    {
        int n = Coding.DecodeVarint64(v, out ulong u);
        value = Coding.ZigZagDecode64(u);
        return n != 0 && n == v.Length;
    }

    [ClassInitialize]
    public static void RegisterAggregators(TestContext _)
    {
        AggMerge.AddAggregator("sum", new SumAggregator());
        AggMerge.AddAggregator("last3", new Last3Aggregator());
        AggMerge.AddAggregator("mul", new MultipleAggregator());
        AggMerge.AddAggregator("sum_nopartial", new NoPartialSumAggregator());
    }

    // ---- EncodeHelper ------------------------------------------------------------------------------------------------

    private static byte[] EncodeInt(long v)
    {
        var w = new ArrayBufferWriter<byte>();
        Coding.PutVarint64(w, Coding.ZigZagEncode64(v));
        return w.WrittenSpan.ToArray();
    }

    private static byte[] EncodeFuncAndInt(string func, long v) => AggMerge.EncodeAggFuncAndPayload(func, EncodeInt(v));

    private static byte[] EncodeList(params byte[][] items)
    {
        var w = new ArrayBufferWriter<byte>();
        foreach (byte[] i in items) Coding.PutLengthPrefixedSlice(w, i);
        return w.WrittenSpan.ToArray();
    }

    private static byte[] EncodeFuncAndList(string func, params string[] items)
    {
        var b = new byte[items.Length][];
        for (int i = 0; i < items.Length; i++) b[i] = Encoding.UTF8.GetBytes(items[i]);
        return AggMerge.EncodeAggFuncAndPayload(func, EncodeList(b));
    }

    private static byte[] EncodeNoCheck(string func, byte[] payload)
    {
        var w = new ArrayBufferWriter<byte>();
        AggMerge.EncodeAggFuncAndPayloadNoCheck(Encoding.UTF8.GetBytes(func), payload, w);
        return w.WrittenSpan.ToArray();
    }

    private static string Hex(byte[]? b) => b is null ? "<null>" : Convert.ToHexString(b);

    // ---- encoding ----------------------------------------------------------------------------------------------------

    [TestMethod]
    public void EncodingIsLengthPrefixedFunctionThenPayload()
    {
        CollectionAssert.AreEqual(new byte[] { 3, (byte)'s', (byte)'u', (byte)'m', 20 }, EncodeFuncAndInt("sum", 10));
        CollectionAssert.AreEqual(new byte[] { 0, 0x3b }, EncodeFuncAndInt(AggMerge.UnnamedFuncName, -30));
        var ex = Assert.ThrowsExactly<RocksDbException>(() => AggMerge.EncodeAggFuncAndPayload("not_registered", [1]));
        Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);
        ex = Assert.ThrowsExactly<RocksDbException>(() => AggMerge.EncodeAggFuncAndPayload(AggMerge.ErrorFuncName, [1]));
        Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);
        ex = Assert.ThrowsExactly<RocksDbException>(() => AggMerge.AddAggregator(AggMerge.ErrorFuncName, new SumAggregator()));
        Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);
        // emplace keeps the first registration.
        Assert.IsFalse(AggMerge.AddAggregator("sum", new MultipleAggregator()));

        Assert.IsTrue(AggMerge.TryExtractAggFuncAndValue(EncodeFuncAndInt("sum", 10), out ReadOnlySpan<byte> func, out ReadOnlySpan<byte> value));
        Assert.AreEqual("sum", Encoding.UTF8.GetString(func));
        CollectionAssert.AreEqual(EncodeInt(10), value.ToArray());
        Assert.IsFalse(AggMerge.TryExtractAggFuncAndValue([5, (byte)'a'], out _, out _));
        Assert.IsFalse(AggMerge.TryExtractAggFuncAndValue([], out _, out _));
        Assert.IsFalse(AggMerge.TryExtractAggFuncAndValue([0x80], out _, out _));

        var list = new List<ReadOnlyMemory<byte>>();
        Assert.IsTrue(AggMerge.ExtractList(EncodeList("a"u8.ToArray(), [], "bcd"u8.ToArray()), list));
        Assert.AreEqual(3, list.Count);
        Assert.AreEqual("a", Encoding.UTF8.GetString(list[0].Span));
        Assert.AreEqual(0, list[1].Length);
        Assert.AreEqual("bcd", Encoding.UTF8.GetString(list[2].Span));
        Assert.IsTrue(AggMerge.ExtractList(ReadOnlyMemory<byte>.Empty, list));
        Assert.AreEqual(0, list.Count);
        Assert.IsFalse(AggMerge.ExtractList(new byte[] { 1, (byte)'a', 3, (byte)'b' }, list));
        Assert.AreEqual(1, list.Count);
        // Like upstream, a trailing length prefix without its bytes is consumed by GetVarint32 before the size check.
        Assert.IsTrue(AggMerge.ExtractList(new byte[] { 1, (byte)'a', 5 }, list));
    }

    // ---- operator unit tests -----------------------------------------------------------------------------------------

    private static byte[]? Full(byte[]? existing, params byte[][] operands)
    {
        using var buf = new MergeOperandBuffer();
        foreach (byte[] o in operands) buf.PushNewer(o);
        var w = new ArrayBufferWriter<byte>();
        MergeHelper.TimedFullMerge(AggMergeOperator.Instance, "k"u8, existing is not null, existing, buf.AsOperands(), w);
        return w.WrittenSpan.ToArray();
    }

    private static byte[]? Partial(params byte[][] operands)
    {
        using var buf = new MergeOperandBuffer();
        foreach (byte[] o in operands) buf.PushNewer(o);
        var w = new ArrayBufferWriter<byte>();
        return AggMergeOperator.Instance.PartialMerge("k"u8, buf.AsOperands(), w) ? w.WrittenSpan.ToArray() : null;
    }

    [TestMethod]
    public void FullAndPartialAggregation()
    {
        AggMergeOperator op = AggMergeOperator.Instance;
        Assert.AreEqual("AggMergeOperator.v1", op.Name);
        Assert.IsTrue(op.AllowSingleOperand);
        Assert.AreSame(op, MergeOperator.CreateBuiltin("AggMergeOperator.v1"));

        Assert.AreEqual(Hex(EncodeFuncAndInt("sum", 6)), Hex(Full(null, EncodeFuncAndInt("sum", 1), EncodeFuncAndInt("sum", 2), EncodeFuncAndInt("sum", 3))));
        Assert.AreEqual(Hex(EncodeFuncAndInt("sum", 6)), Hex(Partial(EncodeFuncAndInt("sum", 1), EncodeFuncAndInt("sum", 2), EncodeFuncAndInt("sum", 3))));
        // A single operand is re-encoded by partial aggregation too (AllowSingleOperand).
        Assert.AreEqual(Hex(EncodeFuncAndInt("sum", 5)), Hex(Partial(EncodeFuncAndInt("sum", 5))));
        // Unnamed base value takes the function of the operands.
        Assert.AreEqual(Hex(EncodeFuncAndInt("sum", 60)), Hex(Full(EncodeFuncAndInt("", 30), EncodeFuncAndInt("sum", 10), EncodeFuncAndInt("sum", 20))));
        // Function switch: older values aggregated first (full merge only).
        Assert.AreEqual(Hex(EncodeFuncAndInt("sum", 10 * 20 + 30 + 40)),
            Hex(Full(EncodeFuncAndInt("mul", 10), EncodeFuncAndInt("mul", 20), EncodeFuncAndInt("sum", 30), EncodeFuncAndInt("sum", 40))));
        Assert.AreEqual(Hex(EncodeFuncAndInt("mul", (1 + 2) * 3 * 4)),
            Hex(Full(null, EncodeFuncAndInt("sum", 1), EncodeFuncAndInt("sum", 2), EncodeFuncAndInt("mul", 3), EncodeFuncAndInt("mul", 4))));
        // Switching back and forth chains the aggregates.
        Assert.AreEqual(Hex(EncodeFuncAndInt("sum", (1 + 2) * 3 + 4)),
            Hex(Full(null, EncodeFuncAndInt("sum", 1), EncodeFuncAndInt("sum", 2), EncodeFuncAndInt("mul", 3), EncodeFuncAndInt("sum", 4))));
        Assert.IsNull(Partial(EncodeFuncAndInt("mul", 10), EncodeFuncAndInt("sum", 10)));
        // Aggregators that opt out of partial aggregation, and unregistered functions, are left to the full merge.
        Assert.IsNull(Partial(EncodeFuncAndInt("sum_nopartial", 1), EncodeFuncAndInt("sum_nopartial", 2)));
        Assert.AreEqual(Hex(EncodeFuncAndInt("sum_nopartial", 3)), Hex(Full(null, EncodeFuncAndInt("sum_nopartial", 1), EncodeFuncAndInt("sum_nopartial", 2))));
        Assert.IsNull(Partial(EncodeNoCheck("non_existing", [1]), EncodeNoCheck("non_existing", [2])));
        // Only unnamed operands: no function, nothing to aggregate.
        Assert.IsNull(Partial(EncodeFuncAndInt("", 1), EncodeFuncAndInt("", 2)));
        Assert.AreEqual(Hex(EncodeNoCheck(AggMerge.ErrorFuncName, EncodeList(EncodeFuncAndInt("", 1)))), Hex(Full(null, EncodeFuncAndInt("", 1))));
    }

    [TestMethod]
    public void FailuresPackOperands()
    {
        // Unregistered function: existing value and every operand are packed under kErrorFuncName.
        byte[] v = EncodeNoCheck("non_existing", "1"u8.ToArray());
        byte[] v1 = EncodeNoCheck("non_existing", "invalid"u8.ToArray());
        Assert.AreEqual(Hex(EncodeNoCheck(AggMerge.ErrorFuncName, EncodeList(v, v1))), Hex(Full(null, v, v1)));
        byte[] baseValue = EncodeFuncAndInt("sum", 1);
        Assert.AreEqual(Hex(EncodeNoCheck(AggMerge.ErrorFuncName, EncodeList(baseValue, v, v1))), Hex(Full(baseValue, v, v1)));
        // Aggregator failure (payload in the wrong format).
        byte[] bad = AggMerge.EncodeAggFuncAndPayload("sum", "invalid"u8);
        byte[] good = EncodeFuncAndInt("sum", 20);
        Assert.AreEqual(Hex(EncodeNoCheck(AggMerge.ErrorFuncName, EncodeList(bad, good))), Hex(Full(null, bad, good)));
        Assert.IsNull(Partial(bad, good));
        // A malformed operand makes the accumulator ignore it and all later operands; earlier ones still aggregate.
        Assert.AreEqual(Hex(EncodeFuncAndInt("sum", 3)), Hex(Full(null, EncodeFuncAndInt("sum", 1), EncodeFuncAndInt("sum", 2), [0x7f], EncodeFuncAndInt("sum", 100))));
        Assert.AreEqual(Hex(EncodeFuncAndInt("sum", 3)), Hex(Partial(EncodeFuncAndInt("sum", 1), EncodeFuncAndInt("sum", 2), [0x7f], EncodeFuncAndInt("sum", 100))));
        // A failed aggregation of the older function on a switch ignores the rest: the result is packed.
        Assert.AreEqual(Hex(EncodeNoCheck(AggMerge.ErrorFuncName, EncodeList(bad, EncodeFuncAndInt("mul", 2)))), Hex(Full(null, bad, EncodeFuncAndInt("mul", 2))));
    }

    [TestMethod]
    public void Last3KeepsTheNewestItems()
    {
        Assert.AreEqual(Hex(EncodeFuncAndList("last3", "f", "c", "d")),
            Hex(Full(null, EncodeFuncAndList("last3", "a", "b"), EncodeFuncAndList("last3", "c", "d", "e"), EncodeFuncAndList("last3", "f"))));
        Assert.AreEqual(Hex(EncodeFuncAndList("last3", "a", "b")), Hex(Full(null, EncodeFuncAndList("last3", "a", "b"))));
    }

    // ---- DB level: port of AggMergeTest.TestUsingMergeOperator ---------------------------------------------------------

    private static RocksDatabase Open(MemoryFileSystem fs) =>
        RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { MergeOperator = AggMergeOperator.Instance });

    private static byte[]? Get(RocksDatabase db, string key) => db.Get(B(key));

    [TestMethod]
    public void TestUsingMergeOperator()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = Open(fs);
        db.Merge(B("foo"), EncodeFuncAndInt("sum", 10));
        db.Merge(B("foo"), EncodeFuncAndInt("sum", 20));
        db.Merge(B("foo"), EncodeFuncAndInt("sum", 15));

        db.Merge(B("bar"), EncodeFuncAndList("last3", "a", "b"));
        db.Merge(B("bar"), EncodeFuncAndList("last3", "c", "d", "e"));
        db.Flush();
        db.Merge(B("bar"), EncodeFuncAndList("last3", "f"));

        // Put() without aggregation type.
        db.Put(B("foo2"), EncodeFuncAndInt(AggMerge.UnnamedFuncName, 30));
        db.Merge(B("foo2"), EncodeFuncAndInt("sum", 10));
        db.Merge(B("foo2"), EncodeFuncAndInt("sum", 20));

        Assert.AreEqual(Hex(EncodeFuncAndInt("sum", 45)), Hex(Get(db, "foo")));
        Assert.AreEqual(Hex(EncodeFuncAndList("last3", "f", "c", "d")), Hex(Get(db, "bar")));
        Assert.AreEqual(Hex(EncodeFuncAndInt("sum", 60)), Hex(Get(db, "foo2")));

        // Changing aggregation type.
        db.Put(B("bar2"), EncodeFuncAndInt("mul", 10));
        db.Merge(B("bar2"), EncodeFuncAndInt("mul", 20));
        db.Merge(B("bar2"), EncodeFuncAndInt("sum", 30));
        db.Merge(B("bar2"), EncodeFuncAndInt("sum", 40));
        Assert.AreEqual(Hex(EncodeFuncAndInt("sum", 10 * 20 + 30 + 40)), Hex(Get(db, "bar2")));

        // Changing aggregation type with partial merge.
        db.Merge(B("foo3"), EncodeFuncAndInt("mul", 10));
        db.Flush();
        db.Merge(B("foo3"), EncodeFuncAndInt("mul", 10));
        db.Merge(B("foo3"), EncodeFuncAndInt("mul", 10));
        db.Merge(B("foo3"), EncodeFuncAndInt("sum", 10));
        db.Flush();
        Assert.AreEqual(Hex(EncodeFuncAndInt("sum", 10 * 10 * 10 + 10)), Hex(Get(db, "foo3")));

        // Merge after full merge.
        db.Merge(B("foo4"), EncodeFuncAndInt("sum", 1));
        db.Merge(B("foo4"), EncodeFuncAndInt("sum", 2));
        db.Flush();
        db.Merge(B("foo4"), EncodeFuncAndInt("sum", 3));
        db.Merge(B("foo4"), EncodeFuncAndInt("sum", 4));
        db.Flush();
        db.CompactRange();
        db.Merge(B("foo4"), EncodeFuncAndInt("sum", 5));
        Assert.AreEqual(Hex(EncodeFuncAndInt("sum", 15)), Hex(Get(db, "foo4")));

        // Unregistered function name.
        byte[] v = EncodeNoCheck("non_existing", "1"u8.ToArray());
        db.Merge(B("bar3"), v);
        byte[] v1 = EncodeNoCheck("non_existing", "invalid"u8.ToArray());
        db.Merge(B("bar3"), v1);
        Assert.AreEqual(Hex(EncodeNoCheck(AggMerge.ErrorFuncName, EncodeList(v, v1))), Hex(Get(db, "bar3")));

        // Invalid input.
        v = AggMerge.EncodeAggFuncAndPayload("sum", "invalid"u8);
        db.Merge(B("bar4"), v);
        v1 = EncodeFuncAndInt("sum", 20);
        db.Merge(B("bar4"), v1);
        byte[] aggregated = Get(db, "bar4")!;
        Assert.IsTrue(AggMerge.TryExtractAggFuncAndValue(aggregated, out ReadOnlySpan<byte> func, out ReadOnlySpan<byte> payload));
        Assert.AreEqual(AggMerge.ErrorFuncName, Encoding.UTF8.GetString(func));
        var decoded = new List<ReadOnlyMemory<byte>>();
        Assert.IsTrue(AggMerge.ExtractList(payload.ToArray(), decoded));
        Assert.AreEqual(2, decoded.Count);
        CollectionAssert.AreEqual(v, decoded[0].ToArray());
        CollectionAssert.AreEqual(v1, decoded[1].ToArray());

        // Everything survives a full compaction (partial merges where possible) and a reopen.
        var expected = new Dictionary<string, string>();
        foreach (string k in new[] { "foo", "bar", "foo2", "bar2", "foo3", "foo4", "bar3", "bar4" }) expected[k] = Hex(Get(db, k));
        db.CompactRange();
        foreach (KeyValuePair<string, string> kv in expected) Assert.AreEqual(kv.Value, Hex(Get(db, kv.Key)), kv.Key);
    }

    [TestMethod]
    public void CompactionPartiallyAggregatesOperands()
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = Open(fs))
        {
            db.Merge(B("k"), EncodeFuncAndInt("sum", 100));
            db.Merge(B("np"), EncodeFuncAndInt("sum_nopartial", 100));
            // Operands above a snapshot cannot be merged with the older history: compaction partially merges them.
            using Snapshot snap = db.GetSnapshot();
            for (int i = 1; i <= 10; i++)
            {
                db.Merge(B("k"), EncodeFuncAndInt("sum", i));
                db.Merge(B("np"), EncodeFuncAndInt("sum_nopartial", i));
                if (i % 3 == 0) db.Flush();
            }
            db.CompactRange();
            using var r = new MergeOperandsResult();
            Assert.IsTrue(db.DefaultColumnFamily.TryGetMergeOperands(B("k"), new GetMergeOperandsOptions { ExpectedMaxNumberOfOperands = 100 }, r));
            Assert.AreEqual(2, r.Count); // the older value + PartialMergeMulti of all ten
            CollectionAssert.AreEqual(EncodeFuncAndInt("sum", 100), r[0].ToArray());
            CollectionAssert.AreEqual(EncodeFuncAndInt("sum", 55), r[1].ToArray());
            Assert.IsTrue(db.DefaultColumnFamily.TryGetMergeOperands(B("np"), new GetMergeOperandsOptions { ExpectedMaxNumberOfOperands = 100 }, r));
            Assert.AreEqual(11, r.Count); // DoPartialAggregate() == false keeps the operands
            Assert.AreEqual(Hex(EncodeFuncAndInt("sum_nopartial", 155)), Hex(Get(db, "np")));
            Assert.AreEqual(Hex(EncodeFuncAndInt("sum", 100)), Hex(db.Get(new ReadOptions { Snapshot = snap }, B("k"))));
        }
        using (RocksDatabase db = Open(fs))
        {
            Assert.AreEqual(Hex(EncodeFuncAndInt("sum", 155)), Hex(Get(db, "k")));
            db.CompactRange(new CompactRangeOptions { BottommostLevelCompaction = BottommostLevelCompaction.Force }, null, null); // no snapshot: the bottommost compaction fully merges
            using var r = new MergeOperandsResult();
            Assert.IsTrue(db.DefaultColumnFamily.TryGetMergeOperands(B("np"), new GetMergeOperandsOptions { ExpectedMaxNumberOfOperands = 100 }, r));
            Assert.AreEqual(1, r.Count);
            Assert.AreEqual(Hex(EncodeFuncAndInt("sum_nopartial", 155)), Hex(r[0].ToArray()));
        }
    }

    [TestMethod]
    public void ConcurrentMergesUseThreadLocalAccumulators()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = Open(fs);
        for (int k = 0; k < 8; k++)
        {
            for (int i = 0; i < 20; i++) db.Merge(B("k" + k), EncodeFuncAndInt(i % 5 == 4 ? "mul" : "sum", i + k));
        }
        var expected = new string[8];
        for (int k = 0; k < 8; k++) expected[k] = Hex(Get(db, "k" + k));
        Parallel.For(0, 64, j =>
        {
            for (int n = 0; n < 50; n++)
            {
                int k = (j + n) % 8;
                Assert.AreEqual(expected[k], Hex(Get(db, "k" + k)));
            }
        });
    }
}
