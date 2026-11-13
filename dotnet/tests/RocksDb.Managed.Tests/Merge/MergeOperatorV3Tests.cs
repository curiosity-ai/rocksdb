using System.Buffers;
using System.Text;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Util;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Merge;

/// <summary>
/// MergeOperator::OpFailureScope (db/db_merge_operator_test.cc MergeOperatorFailsWithMustMerge, merge_helper.cc) and
/// FullMergeV3 with wide-column base values and results (db/wide/db_wide_basic_test.cc MergeV3 in spirit).
/// </summary>
[TestClass]
public sealed class MergeOperatorV3Tests
{
    /// <summary>Port of test::TestPutOperator (db/db_test_util.h): the newest operand wins; marked operands fail.</summary>
    private sealed class TestPutOperator : MergeOperator
    {
        public override string Name => "TestPutOperator";

        private static bool Check(ReadOnlySpan<byte> v, ref MergeOperationFailureScope scope)
        {
            if (v.SequenceEqual("corrupted"u8)) scope = MergeOperationFailureScope.Default;
            else if (v.SequenceEqual("corrupted_try_merge"u8)) scope = MergeOperationFailureScope.TryMerge;
            else if (v.SequenceEqual("corrupted_must_merge"u8)) scope = MergeOperationFailureScope.MustMerge;
            else return true;
            return false;
        }

        public override bool FullMerge(ReadOnlySpan<byte> key, bool hasExistingValue, ReadOnlySpan<byte> existingValue, in MergeOperands operands,
            IBufferWriter<byte> result) => FullMerge(key, hasExistingValue, existingValue, operands, result, out _);

        public override bool FullMerge(ReadOnlySpan<byte> key, bool hasExistingValue, ReadOnlySpan<byte> existingValue, in MergeOperands operands,
            IBufferWriter<byte> result, out MergeOperationFailureScope failureScope)
        {
            failureScope = MergeOperationFailureScope.Default;
            if (hasExistingValue && !Check(existingValue, ref failureScope)) return false;
            for (int i = 0; i < operands.Count; i++)
            {
                if (!Check(operands[i], ref failureScope)) return false;
            }
            result.Write(operands[operands.Count - 1]);
            return true;
        }
    }

    private static ColumnFamilyOptions Cf(MergeOperator op) => new() { MergeOperator = op, DisableAutoCompactions = true };

    private static readonly CompactRangeOptions s_forceOptimized = new() { BottommostLevelCompaction = BottommostLevelCompaction.ForceOptimized };

    private static string S(ReadOnlySpan<byte> b) => Encoding.UTF8.GetString(b);

    private static void AssertMergeFailed(Action a)
    {
        var ex = Assert.ThrowsExactly<RocksDbException>(a);
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
        Assert.AreEqual(StatusSubCode.MergeOperatorFailed, ex.SubCode);
    }

    /// <summary>Port of DBMergeOperatorTest.MergeOperatorFailsWithMustMerge.</summary>
    [TestMethod]
    public void MergeOperatorFailsWithMustMerge()
    {
        const int numOperands = 3;
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), Cf(new TestPutOperator()));
        for (int i = 0; i < numOperands; ++i)
        {
            void CheckQuery()
            {
                Assert.AreEqual("val", S(db.Get(B("k0"))));
                AssertMergeFailed(() => db.Get(B("k1")));
                Assert.AreEqual("val", S(db.Get(B("k2"))));
                using (Iterator it = db.NewIterator())
                {
                    it.SeekToFirst();
                    Assert.IsTrue(it.Valid);
                    Assert.AreEqual("k0", S(it.Key));
                    AssertMergeFailed(it.Next);
                    it.SeekToLast();
                    Assert.IsTrue(it.Valid);
                    Assert.AreEqual("k2", S(it.Key));
                    AssertMergeFailed(it.Prev);
                    it.Seek(B("k2"));
                    Assert.IsTrue(it.Valid);
                    Assert.AreEqual("k2", S(it.Key));
                }
                // The operands survive flush and compaction unmerged.
                using var r = new MergeOperandsResult();
                Assert.IsTrue(db.TryGetMergeOperands(B("k1"), new GetMergeOperandsOptions { ExpectedMaxNumberOfOperands = numOperands }, r));
                Assert.AreEqual(numOperands, r.Count);
                for (int j = 0; j < numOperands; ++j) Assert.AreEqual(i == j ? "corrupted_must_merge" : "ok", S(r[j]), "i=" + i + " j=" + j);
            }

            db.Put(B("k0"), B("val"));
            db.Put(B("k2"), B("val"));

            // Case 1: pure Merge()s
            for (int j = 0; j < numOperands; ++j) db.Merge(B("k1"), B(j == i ? "corrupted_must_merge" : "ok"));
            CheckQuery();
            db.Flush();
            CheckQuery();
            db.CompactRange(s_forceOptimized, null, null);
            CheckQuery();

            // Case 2: Merge()s on top of a Put()
            for (int j = 0; j < numOperands; ++j)
            {
                byte[] val = B(j == i ? "corrupted_must_merge" : "ok");
                if (j == 0) db.Put(B("k1"), val);
                else db.Merge(B("k1"), val);
            }
            CheckQuery();
            db.Flush();
            CheckQuery();
            db.CompactRange(s_forceOptimized, null, null);
            CheckQuery();

            // Case 3: Merge()s on top of a Delete()
            db.Delete(B("k1"));
            for (int j = 0; j < numOperands; ++j) db.Merge(B("k1"), B(i == j ? "corrupted_must_merge" : "ok"));
            CheckQuery();
            db.Flush();
            CheckQuery();
            db.CompactRange(s_forceOptimized, null, null);
            CheckQuery();

            // Verify obsolete data removal still happens
            db.Delete(B("k0"));
            db.Delete(B("k1"));
            db.Delete(B("k2"));
            Assert.IsNull(db.Get(B("k0")));
            Assert.IsNull(db.Get(B("k1")));
            Assert.IsNull(db.Get(B("k2")));
            db.CompactRange(s_forceOptimized, null, null);
            Assert.AreEqual(0, db.GetLiveFilesMetadata().Count);
        }
    }

    /// <summary>kDefault / kTryMerge: a compaction that must merge the key fails (Corruption), reads fail too.</summary>
    [TestMethod]
    [DataRow("corrupted")]
    [DataRow("corrupted_try_merge")]
    public void TryMergeFailureFailsCompaction(string operand)
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), Cf(new TestPutOperator()));
        // Base and operand in different files: a flush of both would have to merge them and fail the same way
        // (Audit5FlushTests.FlushFailsWhenTheMergeOperatorFails).
        db.Put(B("k"), B("base"));
        db.Flush();
        db.Merge(B("k"), B(operand));
        db.Flush();
        AssertMergeFailed(() => db.Get(B("k")));
        var ex = Assert.ThrowsExactly<RocksDbException>(() => db.CompactRange(s_forceOptimized, null, null));
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
    }

    /// <summary>
    /// A FullMergeV3 operator: appends the operands to the default column and counts them in a "count" column, keeping
    /// other columns; a plain base becomes an entity (the result is always wide), no base starts from empty.
    /// </summary>
    private sealed class CountingV3Operator : MergeOperator
    {
        public override string Name => "CountingV3Operator";

        public override bool FullMerge(ReadOnlySpan<byte> key, bool hasExistingValue, ReadOnlySpan<byte> existingValue, in MergeOperands operands,
            IBufferWriter<byte> result) => throw new InvalidOperationException("FullMergeV3 only");

        public override bool FullMergeV3(in MergeOperationInputV3 input, ref MergeOperationOutputV3 output)
        {
            var value = new ArrayBufferWriter<byte>();
            ulong count = 0;
            switch (input.ExistingValueKind)
            {
                case MergeExistingValueKind.Plain:
                    value.Write(input.ExistingValue);
                    break;
                case MergeExistingValueKind.WideColumns:
                    foreach (WideColumn c in input.ExistingColumns!)
                    {
                        if (c.Name.Length == 0) value.Write(c.Value.Span);
                        else if (c.Name.Span.SequenceEqual("count"u8)) count = Coding.DecodeFixed64(c.Value.Span);
                        else output.AddColumn(c.Name.Span, c.Value.Span);
                    }
                    break;
            }
            for (int i = 0; i < input.Operands.Count; i++)
            {
                if (input.Operands[i].SequenceEqual("fail"u8))
                {
                    output.FailureScope = MergeOperationFailureScope.MustMerge;
                    return false;
                }
                value.Write(input.Operands[i]);
                count++;
            }
            Span<byte> c8 = stackalloc byte[8];
            Coding.EncodeFixed64(c8, count);
            output.AddColumn("count"u8, c8);  // added before the default column: the result is sorted afterwards
            output.AddColumn(default, value.WrittenSpan);
            return true;
        }
    }

    private static string Columns(WideColumns? c)
    {
        if (c is null) return "<none>";
        var sb = new StringBuilder();
        foreach (WideColumn col in c)
        {
            string v = col.Name.Span.SequenceEqual("count"u8) ? Coding.DecodeFixed64(col.Value.Span).ToString() : S(col.Value.Span);
            sb.Append(S(col.Name.Span)).Append('=').Append(v).Append(';');
        }
        return sb.ToString();
    }

    [TestMethod]
    public void FullMergeV3WideColumnResults()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), Cf(new CountingV3Operator()));
        db.Put(B("plain"), B("p"));
        db.Merge(B("plain"), B("1"));
        db.PutEntity(B("wide"), [WideColumn.Default(B("w")), new WideColumn(B("attr"), B("x")), new WideColumn(B("count"), new byte[8])]);
        db.Merge(B("wide"), B("2"));
        db.Merge(B("none"), B("3"));
        db.Merge(B("none"), B("4"));

        void Check(string stage)
        {
            Assert.AreEqual("=p1;count=1;", Columns(db.GetEntity(B("plain"))), stage);
            Assert.AreEqual("=w2;attr=x;count=1;", Columns(db.GetEntity(B("wide"))), stage);
            Assert.AreEqual("=34;count=2;", Columns(db.GetEntity(B("none"))), stage);
            // A plain Get returns the default column of the wide result.
            Assert.AreEqual("p1", S(db.Get(B("plain"))), stage);
            Assert.AreEqual("w2", S(db.Get(B("wide"))), stage);
            Assert.AreEqual("34", S(db.Get(B("none"))), stage);
            using Iterator it = db.NewIterator();
            var seen = new List<string>();
            for (it.SeekToFirst(); it.Valid; it.Next())
            {
                Assert.IsTrue(it.IsEntity, stage + " " + S(it.Key));
                seen.Add(S(it.Key) + ":" + S(it.Value) + ":" + Columns(it.Columns));
            }
            CollectionAssert.AreEqual(new[] { "none:34:=34;count=2;", "plain:p1:=p1;count=1;", "wide:w2:=w2;attr=x;count=1;" }, seen, stage);
            seen.Clear();
            for (it.SeekToLast(); it.Valid; it.Prev()) seen.Add(S(it.Key) + ":" + S(it.Value));
            CollectionAssert.AreEqual(new[] { "wide:w2", "plain:p1", "none:34" }, seen, stage);
        }

        Check("memtable");
        db.Flush();
        Check("flushed");
        db.CompactRange(s_forceOptimized, null, null);
        Check("compacted");
        // The compaction stored the merge results as entities: a new operand applies to the entity base.
        db.Merge(B("plain"), B("5"));
        Assert.AreEqual("=p15;count=2;", Columns(db.GetEntity(B("plain"))));
        db.CompactRange(s_forceOptimized, null, null);
        Assert.AreEqual("=p15;count=2;", Columns(db.GetEntity(B("plain"))));

        // A V3 operator's failure scope is honoured by compaction (kMustMerge keeps the operands).
        db.Merge(B("plain"), B("fail"));
        db.Flush();
        db.CompactRange(s_forceOptimized, null, null);
        AssertMergeFailed(() => db.Get(B("plain")));
        using var r = new MergeOperandsResult();
        Assert.IsTrue(db.TryGetMergeOperands(B("plain"), new GetMergeOperandsOptions { ExpectedMaxNumberOfOperands = 4 }, r));
        Assert.AreEqual("fail", S(r[r.Count - 1]));
    }

    /// <summary>The override detection behind the direct FullMergeV2 fast path.</summary>
    [TestMethod]
    public void OverrideDetection()
    {
        Assert.IsFalse(MergeOperator.UInt64Add.OverridesFullMergeV3);
        Assert.IsFalse(MergeOperator.UInt64Add.OverridesScopedFullMerge);
        Assert.IsFalse(MergeOperator.StringAppend(",").OverridesFullMergeV3);
        Assert.IsTrue(new CountingV3Operator().OverridesFullMergeV3);
        Assert.IsFalse(new TestPutOperator().OverridesFullMergeV3);
        Assert.IsTrue(new TestPutOperator().OverridesScopedFullMerge);
        Assert.IsTrue(new TtlMergeOperator(MergeOperator.UInt64Add).OverridesScopedFullMerge);
    }

    /// <summary>The default FullMergeV3 (FullMergeV2 fallback) on an entity base merges the default column only.</summary>
    [TestMethod]
    public void DefaultFullMergeV3KeepsOtherColumns()
    {
        MergeOperator op = MergeOperator.StringAppend(",");
        using var operands = new MergeOperandBuffer();
        operands.PushNewer(B("b"));
        var w = new WideColumns([WideColumn.Default(B("a")), new WideColumn(B("z"), B("zz"))]);
        var value = new ArrayBufferWriter<byte>();
        var entity = new ArrayBufferWriter<byte>();
        Assert.AreEqual(EntryType.WideColumnEntity,
            MergeHelper.TimedFullMerge(op, B("k"), MergeExistingValueKind.WideColumns, w.Serialized, operands.AsOperands(), value, entity));
        var result = new WideColumns();
        result.SetSerialized(entity.WrittenSpan);
        Assert.AreEqual("=a,b;z=zz;", Columns(result));
        // Value-only callers get the merged default column directly.
        value.Clear();
        Assert.AreEqual(EntryType.Value,
            MergeHelper.TimedFullMerge(op, B("k"), MergeExistingValueKind.WideColumns, w.Serialized, operands.AsOperands(), value, null));
        Assert.AreEqual("a,b", S(value.WrittenSpan));
        // An entity without a default column: the operands apply to an empty value.
        var noDefault = new WideColumns([new WideColumn(B("z"), B("zz"))]);
        entity.Clear();
        MergeHelper.TimedFullMerge(op, B("k"), MergeExistingValueKind.WideColumns, noDefault.Serialized, operands.AsOperands(), value, entity);
        result.SetSerialized(entity.WrittenSpan);
        Assert.AreEqual("=,b;z=zz;", Columns(result));
    }
}
