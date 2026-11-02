using System.Buffers;
using RocksDb.Managed.Db;
using RocksDb.Managed.Tests.Iterators;
using RocksDb.Managed.Tests.Table;
using RocksDb.Managed.Util;
using EntryType = RocksDb.Managed.Db.EntryType;

namespace RocksDb.Managed.Tests.Merge;

/// <summary>Port of db/merge_helper_test.cc plus tests of the built-in operators and the operand buffer.</summary>
[TestClass]
public sealed class MergeHelperTests
{
    private static byte[] EncodeInt(ulong v)
    {
        byte[] b = new byte[8];
        Coding.EncodeFixed64(b, v);
        return b;
    }

    private static string KeyStr(string userKey, ulong seq, EntryType t) => Convert.ToHexString(TableTestUtil.IKey(userKey, seq, t));

    /// <summary>Port of test::FilterNumber: filters merge operands (and values) equal to a number.</summary>
    private sealed class FilterNumber(ulong num) : CompactionFilter
    {
        public string LastMergeOperandKey = "";
        public override string Name => "FilterBadMergeOperand";
        public override CompactionFilterDecision Filter(int level, ReadOnlySpan<byte> key, CompactionFilterValueType valueType, ReadOnlySpan<byte> existingValue,
            IBufferWriter<byte> newValue, IBufferWriter<byte> skipUntil)
        {
            if (valueType == CompactionFilterValueType.MergeOperand) LastMergeOperandKey = System.Text.Encoding.UTF8.GetString(key);
            bool remove = existingValue.Length != 8 || Coding.DecodeFixed64(existingValue) == num;
            return remove ? CompactionFilterDecision.Remove : CompactionFilterDecision.Keep;
        }
    }

    private sealed class Harness : IDisposable
    {
        public readonly TestInternalIterator Iter = new();
        public readonly List<byte[]> Keys = new();
        public MergeHelper? Helper;
        public MergeOperator Op = UInt64AddOperator.Instance;
        public CompactionFilter? Filter;

        public void Add(string userKey, ulong seq, EntryType t, byte[] value)
        {
            Iter.Add(System.Text.Encoding.UTF8.GetBytes(userKey), t, value, seq);
            Keys.Add(TableTestUtil.IKey(userKey, seq, t));
        }

        public void Add(string userKey, ulong seq, EntryType t, string value) => Add(userKey, seq, t, System.Text.Encoding.UTF8.GetBytes(value));

        public MergeUntilResult Run(ulong stopBefore, bool atBottom, ulong latestSnapshot = 0)
        {
            Iter.Finish();
            Iter.SeekToFirst();
            Helper?.Dispose();
            Helper = new MergeHelper(InternalKeyComparator.Bytewise, Op, Filter, latestSnapshot, 0) { AssertValidInternalKey = false };
            return Helper.MergeUntil(Iter, null, stopBefore, atBottom);
        }

        public string IterKey => Convert.ToHexString(Iter.Key);
        public string OutKey(int i) => Convert.ToHexString(Helper!.GetOutputKey(i));
        public byte[] OutValue(int i) => Helper!.GetOutputValue(i).ToArray();

        public void Dispose() => Helper?.Dispose();
    }

    [TestMethod]
    public void MergeAtBottomSuccess()
    {
        using var h = new Harness();
        h.Add("a", 20, EntryType.Merge, EncodeInt(1));
        h.Add("a", 10, EntryType.Merge, EncodeInt(3));
        h.Add("b", 10, EntryType.Merge, EncodeInt(4)); // <- iter_ after merge
        Assert.AreEqual(MergeUntilResult.Ok, h.Run(0, true));
        Assert.AreEqual(Convert.ToHexString(h.Keys[2]), h.IterKey);
        Assert.AreEqual(KeyStr("a", 20, EntryType.Value), h.OutKey(0));
        CollectionAssert.AreEqual(EncodeInt(4), h.OutValue(0));
        Assert.AreEqual(1, h.Helper!.OutputCount);
    }

    [TestMethod]
    public void MergeValue()
    {
        using var h = new Harness();
        h.Add("a", 40, EntryType.Merge, EncodeInt(1));
        h.Add("a", 30, EntryType.Merge, EncodeInt(3));
        h.Add("a", 20, EntryType.Value, EncodeInt(4)); // <- iter_ after merge
        h.Add("a", 10, EntryType.Merge, EncodeInt(1));
        Assert.AreEqual(MergeUntilResult.Ok, h.Run(0, false));
        Assert.AreEqual(Convert.ToHexString(h.Keys[3]), h.IterKey);
        Assert.AreEqual(KeyStr("a", 40, EntryType.Value), h.OutKey(0));
        CollectionAssert.AreEqual(EncodeInt(8), h.OutValue(0));
        Assert.AreEqual(1, h.Helper!.OutputCount);
    }

    [TestMethod]
    public void SnapshotBeforeValue()
    {
        using var h = new Harness();
        h.Add("a", 50, EntryType.Merge, EncodeInt(1));
        h.Add("a", 40, EntryType.Merge, EncodeInt(3)); // <- iter_ after merge
        h.Add("a", 30, EntryType.Merge, EncodeInt(1));
        h.Add("a", 20, EntryType.Value, EncodeInt(4));
        h.Add("a", 10, EntryType.Merge, EncodeInt(1));
        Assert.AreEqual(MergeUntilResult.MergeInProgress, h.Run(31, true));
        Assert.AreEqual(Convert.ToHexString(h.Keys[2]), h.IterKey);
        Assert.AreEqual(KeyStr("a", 50, EntryType.Merge), h.OutKey(0));
        CollectionAssert.AreEqual(EncodeInt(4), h.OutValue(0));
        Assert.AreEqual(1, h.Helper!.OutputCount);
    }

    [TestMethod]
    public void NoPartialMerge()
    {
        using var h = new Harness { Op = new StringAppendNoPartialOperator() };
        h.Add("a", 50, EntryType.Merge, "v2");
        h.Add("a", 40, EntryType.Merge, "v"); // <- iter_ after merge
        h.Add("a", 30, EntryType.Merge, "v");
        Assert.AreEqual(MergeUntilResult.MergeInProgress, h.Run(31, true));
        Assert.AreEqual(Convert.ToHexString(h.Keys[2]), h.IterKey);
        // Output order is newest first here (MergeOutputIterator); upstream's keys()[0] is the oldest.
        Assert.AreEqual(KeyStr("a", 50, EntryType.Merge), h.OutKey(0));
        Assert.AreEqual("v2", System.Text.Encoding.UTF8.GetString(h.OutValue(0)));
        Assert.AreEqual(KeyStr("a", 40, EntryType.Merge), h.OutKey(1));
        Assert.AreEqual("v", System.Text.Encoding.UTF8.GetString(h.OutValue(1)));
        Assert.AreEqual(2, h.Helper!.OutputCount);
    }

    [TestMethod]
    public void SingleOperand()
    {
        using var h = new Harness();
        h.Add("a", 50, EntryType.Merge, EncodeInt(1));
        Assert.AreEqual(MergeUntilResult.MergeInProgress, h.Run(31, false));
        Assert.IsFalse(h.Iter.Valid);
        Assert.AreEqual(KeyStr("a", 50, EntryType.Merge), h.OutKey(0));
        CollectionAssert.AreEqual(EncodeInt(1), h.OutValue(0));
        Assert.AreEqual(1, h.Helper!.OutputCount);
    }

    [TestMethod]
    public void MergeDeletion()
    {
        using var h = new Harness();
        h.Add("a", 30, EntryType.Merge, EncodeInt(3));
        h.Add("a", 20, EntryType.Deletion, "");
        Assert.AreEqual(MergeUntilResult.Ok, h.Run(15, false));
        Assert.IsFalse(h.Iter.Valid);
        Assert.AreEqual(KeyStr("a", 30, EntryType.Value), h.OutKey(0));
        CollectionAssert.AreEqual(EncodeInt(3), h.OutValue(0));
        Assert.AreEqual(1, h.Helper!.OutputCount);
    }

    [TestMethod]
    public void CorruptKey()
    {
        using var h = new Harness();
        h.Add("a", 30, EntryType.Merge, EncodeInt(3));
        h.Add("a", 25, EntryType.Merge, EncodeInt(1));
        h.Add("a", 20, EntryType.MaxValue, ""); // corrupt key type  <- iter_ after merge
        Assert.AreEqual(MergeUntilResult.MergeInProgress, h.Run(15, false));
        Assert.AreEqual(Convert.ToHexString(h.Keys[2]), h.IterKey);
        Assert.AreEqual(KeyStr("a", 30, EntryType.Merge), h.OutKey(0));
        CollectionAssert.AreEqual(EncodeInt(4), h.OutValue(0));
        Assert.AreEqual(1, h.Helper!.OutputCount);
    }

    [TestMethod]
    public void CorruptKeyThrowsWhenAsserting()
    {
        var iter = new TestInternalIterator();
        iter.Add(System.Text.Encoding.UTF8.GetBytes("a"), EntryType.Merge, EncodeInt(3), 30);
        iter.Add(System.Text.Encoding.UTF8.GetBytes("a"), EntryType.MaxValue, [], 20);
        iter.Finish();
        iter.SeekToFirst();
        using var helper = new MergeHelper(InternalKeyComparator.Bytewise, UInt64AddOperator.Instance, null, 0, 0);
        var ex = Assert.ThrowsExactly<RocksDbException>(() => helper.MergeUntil(iter, null, 0, false));
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
    }

    [TestMethod]
    public void FilterMergeOperands()
    {
        using var h = new Harness { Filter = new FilterNumber(5) };
        h.Add("a", 30, EntryType.Merge, EncodeInt(3));
        h.Add("a", 29, EntryType.Merge, EncodeInt(5)); // Filtered
        h.Add("a", 28, EntryType.Merge, EncodeInt(3));
        h.Add("a", 27, EntryType.Merge, EncodeInt(1));
        h.Add("a", 26, EntryType.Merge, EncodeInt(5)); // Filtered
        h.Add("a", 25, EntryType.Value, EncodeInt(1));
        Assert.AreEqual(MergeUntilResult.Ok, h.Run(15, false));
        Assert.IsFalse(h.Iter.Valid);
        Assert.AreEqual(1, h.Helper!.OutputCount);
        Assert.AreEqual(KeyStr("a", 30, EntryType.Value), h.OutKey(0));
        CollectionAssert.AreEqual(EncodeInt(8), h.OutValue(0));
    }

    [TestMethod]
    public void FilterAllMergeOperands()
    {
        using var h = new Harness { Filter = new FilterNumber(5) };
        for (ulong s = 30; s >= 25; s--) h.Add("a", s, EntryType.Merge, EncodeInt(5));
        // filtered out all
        Assert.AreEqual(MergeUntilResult.Ok, h.Run(15, false));
        Assert.IsFalse(h.Iter.Valid);
        Assert.AreEqual(0, h.Helper!.OutputCount);

        // we have one operand that will survive because it's a delete
        h.Add("a", 24, EntryType.Deletion, EncodeInt(5));
        h.Add("b", 23, EntryType.Value, EncodeInt(5));
        Assert.AreEqual(MergeUntilResult.Ok, h.Run(15, true));
        Assert.IsTrue(h.Iter.Valid);
        Assert.AreEqual(0, h.Helper!.OutputCount);
        // when all merge operands are filtered out, we leave the iterator pointing to the Put/Delete that survived
        Assert.AreEqual(KeyStr("a", 24, EntryType.Deletion), h.IterKey);
        CollectionAssert.AreEqual(EncodeInt(5), h.Iter.Value.ToArray());
    }

    [TestMethod]
    public void FilterFirstMergeOperand()
    {
        var filter = new FilterNumber(5);
        using var h = new Harness { Filter = filter };
        h.Add("a", 31, EntryType.Merge, EncodeInt(5)); // Filtered
        h.Add("a", 30, EntryType.Merge, EncodeInt(5)); // Filtered
        h.Add("a", 29, EntryType.Merge, EncodeInt(2));
        h.Add("a", 28, EntryType.Merge, EncodeInt(1));
        h.Add("a", 27, EntryType.Merge, EncodeInt(3));
        h.Add("a", 26, EntryType.Merge, EncodeInt(5)); // Filtered
        h.Add("a", 25, EntryType.Merge, EncodeInt(5)); // Filtered
        h.Add("b", 24, EntryType.Value, EncodeInt(5)); // next user key
        Assert.AreEqual(MergeUntilResult.Ok, h.Run(15, true));
        Assert.IsTrue(h.Iter.Valid);
        // sequence number is 29 here, because the first merge operand got filtered out
        Assert.AreEqual(KeyStr("a", 29, EntryType.Value), h.OutKey(0));
        CollectionAssert.AreEqual(EncodeInt(6), h.OutValue(0));
        Assert.AreEqual(1, h.Helper!.OutputCount);
        // make sure that we're passing user keys into the filter
        Assert.AreEqual("a", filter.LastMergeOperandKey);
    }

    [TestMethod]
    public void DontFilterMergeOperandsBeforeSnapshot()
    {
        using var h = new Harness { Filter = new FilterNumber(5) };
        h.Add("a", 31, EntryType.Merge, EncodeInt(5));
        h.Add("a", 30, EntryType.Merge, EncodeInt(5));
        h.Add("a", 29, EntryType.Merge, EncodeInt(2));
        h.Add("a", 28, EntryType.Merge, EncodeInt(1));
        h.Add("a", 27, EntryType.Merge, EncodeInt(3));
        h.Add("a", 26, EntryType.Merge, EncodeInt(5));
        h.Add("a", 25, EntryType.Merge, EncodeInt(5));
        h.Add("b", 24, EntryType.Value, EncodeInt(5));
        Assert.AreEqual(MergeUntilResult.Ok, h.Run(15, true, 32));
        Assert.IsTrue(h.Iter.Valid);
        Assert.AreEqual(KeyStr("a", 31, EntryType.Value), h.OutKey(0));
        CollectionAssert.AreEqual(EncodeInt(26), h.OutValue(0));
        Assert.AreEqual(1, h.Helper!.OutputCount);
    }

    [TestMethod]
    public void RangeTombstoneCoversBaseValue()
    {
        using var h = new Harness();
        h.Add("a", 30, EntryType.Merge, EncodeInt(3));
        h.Add("a", 20, EntryType.Merge, EncodeInt(4)); // covered by [a,b)@25 -> dropped
        h.Add("a", 10, EntryType.Value, EncodeInt(100)); // covered -> no base value
        h.Iter.Finish();
        h.Iter.SeekToFirst();
        var rd = RangeDelAggregator.ForCompaction(InternalKeyComparator.Bytewise, []);
        rd.AddTombstones(FragmentedRangeTombstoneList.Build(Comparator.Bytewise, [new("a"u8.ToArray(), "b"u8.ToArray(), 25)]));
        using var helper = new MergeHelper(InternalKeyComparator.Bytewise, UInt64AddOperator.Instance, null, 0, 0);
        Assert.AreEqual(MergeUntilResult.Ok, helper.MergeUntil(h.Iter, rd, 0, false));
        Assert.AreEqual(1, helper.OutputCount);
        Assert.AreEqual(KeyStr("a", 30, EntryType.Value), Convert.ToHexString(helper.GetOutputKey(0)));
        CollectionAssert.AreEqual(EncodeInt(3), helper.GetOutputValue(0).ToArray());
    }

    // ------------------------------------------------------------------ built-in operators

    private static byte[] FullMerge(MergeOperator op, byte[]? existing, params string[] operands)
    {
        using var buf = new MergeOperandBuffer();
        foreach (string o in operands) buf.PushNewer(System.Text.Encoding.UTF8.GetBytes(o));
        var w = new ArrayBufferWriter<byte>();
        MergeHelper.TimedFullMerge(op, "k"u8, existing is not null, existing, buf.AsOperands(), w);
        return w.WrittenSpan.ToArray();
    }

    [TestMethod]
    public void StringAppendOperatorSemantics()
    {
        var op = new StringAppendOperator();
        Assert.AreEqual("StringAppendOperator", op.Name);
        Assert.AreEqual("a,b,c", System.Text.Encoding.UTF8.GetString(FullMerge(op, null, "a", "b", "c")));
        Assert.AreEqual("x,a", System.Text.Encoding.UTF8.GetString(FullMerge(op, "x"u8.ToArray(), "a")));
        Assert.AreEqual(",a", System.Text.Encoding.UTF8.GetString(FullMerge(op, [], "a")));
        Assert.AreEqual("a|b", System.Text.Encoding.UTF8.GetString(FullMerge(new StringAppendOperator("|"), null, "a", "b")));
        using var buf = new MergeOperandBuffer();
        buf.PushNewer("a"u8);
        buf.PushNewer("b"u8);
        var w = new ArrayBufferWriter<byte>();
        Assert.IsTrue(op.PartialMerge("k"u8, buf.AsOperands(), w));
        Assert.AreEqual("a,b", System.Text.Encoding.UTF8.GetString(w.WrittenSpan));
    }

    [TestMethod]
    public void UInt64AddOperatorSemantics()
    {
        UInt64AddOperator op = UInt64AddOperator.Instance;
        Assert.AreEqual("UInt64AddOperator", op.Name);
        using var buf = new MergeOperandBuffer();
        buf.PushNewer(EncodeInt(5));
        buf.PushNewer("bad"u8); // wrong size counts as 0
        buf.PushNewer(EncodeInt(7));
        var w = new ArrayBufferWriter<byte>();
        MergeHelper.TimedFullMerge(op, "k"u8, true, EncodeInt(100), buf.AsOperands(), w);
        Assert.AreEqual(112UL, Coding.DecodeFixed64(w.WrittenSpan));
        w.Clear();
        Assert.IsTrue(op.PartialMerge("k"u8, buf.AsOperands(), w));
        Assert.AreEqual(12UL, Coding.DecodeFixed64(w.WrittenSpan));
    }

    private sealed class FailingOperator : MergeOperator
    {
        public override string Name => "Failing";
        public override bool FullMerge(ReadOnlySpan<byte> key, bool hasExistingValue, ReadOnlySpan<byte> existingValue, in MergeOperands operands, IBufferWriter<byte> result) => false;
    }

    [TestMethod]
    public void FailedMergeThrowsCorruption()
    {
        var ex = Assert.ThrowsExactly<RocksDbException>(() => FullMerge(new FailingOperator(), null, "a"));
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
        Assert.AreEqual(StatusSubCode.MergeOperatorFailed, ex.SubCode);
    }

    [TestMethod]
    public void MergeContextOverload()
    {
        using var ctx = new MergeContext();
        ctx.PushOperand("c"u8); // newest
        ctx.PushOperand("b"u8);
        ctx.PushOperand("a"u8); // oldest
        var w = new ArrayBufferWriter<byte>();
        MergeHelper.TimedFullMerge(new StringAppendOperator(), "k"u8, true, "base"u8, ctx, w);
        Assert.AreEqual("base,a,b,c", System.Text.Encoding.UTF8.GetString(w.WrittenSpan));
    }

    [TestMethod]
    public void OperandBufferBothDirections()
    {
        var rnd = new Random(Environment.TickCount);
        using var buf = new MergeOperandBuffer(16, 2);
        for (int round = 0; round < 50; round++)
        {
            buf.Clear();
            var model = new LinkedList<string>();
            int n = rnd.Next(0, 60);
            bool older = rnd.Next(2) == 0;
            for (int i = 0; i < n; i++)
            {
                string s = new string((char)('a' + i % 26), rnd.Next(0, 40));
                bool pushOlder = rnd.Next(10) == 0 ? !older : older; // occasionally mix directions
                if (pushOlder)
                {
                    buf.PushOlder(System.Text.Encoding.UTF8.GetBytes(s));
                    model.AddFirst(s);
                }
                else
                {
                    buf.PushNewer(System.Text.Encoding.UTF8.GetBytes(s));
                    model.AddLast(s);
                }
            }
            Assert.AreEqual(model.Count, buf.Count);
            MergeOperands ops = buf.AsOperands();
            Assert.AreEqual(model.Count, ops.Count);
            int idx = 0;
            foreach (string s in model)
            {
                Assert.AreEqual(s, System.Text.Encoding.UTF8.GetString(ops[idx]));
                Assert.AreEqual(s, System.Text.Encoding.UTF8.GetString(buf[idx]));
                Assert.AreEqual(s, System.Text.Encoding.UTF8.GetString(buf.GetNewestFirst(model.Count - 1 - idx)));
                idx++;
            }
        }
    }
}
