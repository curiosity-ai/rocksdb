using System.Buffers;
using System.Text;
using RocksDb.Managed.Db;
using RocksDb.Managed.Utilities.AggMerge;

namespace RocksDb.Managed.Tests.Merge;

/// <summary>
/// Semantics of the built-in merge operators (utilities/merge_operators/*): ports of utilities/util_merge_operators_test.cc
/// (MaxMergeOperator), the operator registry cases of options/options_test.cc (MergeOperatorFromString) and
/// options/customizable_test.cc (LoadMergeOperatorTest), plus put / sortlist / bytesxor / stringappendtest edge cases
/// derived from their sources.
/// </summary>
[TestClass]
public sealed class BuiltinMergeOperatorTests
{
    private static byte[] B(string s) => Encoding.UTF8.GetBytes(s);

    private static string S(ReadOnlySpan<byte> b) => Encoding.UTF8.GetString(b);

    /// <summary>FullMergeV2(existing, operands) through MergeHelper::TimedFullMerge; null existing = no base value.</summary>
    private static string FullMerge(MergeOperator op, string? existing, params string[] operands) => S(FullMergeBytes(op, existing is null ? null : B(existing), Bytes(operands)));

    private static byte[][] Bytes(string[] s)
    {
        var r = new byte[s.Length][];
        for (int i = 0; i < s.Length; i++) r[i] = B(s[i]);
        return r;
    }

    private static byte[] FullMergeBytes(MergeOperator op, byte[]? existing, params byte[][] operands)
    {
        using var buf = new MergeOperandBuffer();
        foreach (byte[] o in operands) buf.PushNewer(o);
        var w = new ArrayBufferWriter<byte>();
        MergeHelper.TimedFullMerge(op, "k"u8, existing is not null, existing, buf.AsOperands(), w);
        return w.WrittenSpan.ToArray();
    }

    /// <summary>PartialMergeMulti; null when the operator declines.</summary>
    private static string? PartialMerge(MergeOperator op, params string[] operands)
    {
        byte[]? r = PartialMergeBytes(op, Bytes(operands));
        return r is null ? null : S(r);
    }

    private static byte[]? PartialMergeBytes(MergeOperator op, params byte[][] operands)
    {
        using var buf = new MergeOperandBuffer();
        foreach (byte[] o in operands) buf.PushNewer(o);
        var w = new ArrayBufferWriter<byte>();
        return op.PartialMerge("k"u8, buf.AsOperands(), w) ? w.WrittenSpan.ToArray() : null;
    }

    private static void AssertMergeFails(MergeOperator op, string? existing, params string[] operands)
    {
        var ex = Assert.ThrowsExactly<RocksDbException>(() => FullMerge(op, existing, operands));
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
        Assert.AreEqual(StatusSubCode.MergeOperatorFailed, ex.SubCode);
    }

    [TestMethod]
    public void MaxMergeOperator()
    {
        // utilities/util_merge_operators_test.cc, MaxMergeOperator.
        MergeOperator op = MergeOperator.Max;
        Assert.AreEqual("MaxOperator", op.Name);
        Assert.AreEqual("max", op.NickName);
        Assert.AreEqual("B", FullMerge(op, "B", "A"));
        Assert.AreEqual("B", FullMerge(op, "A", "B"));
        Assert.AreEqual("", FullMerge(op, null, "", "", ""));
        Assert.AreEqual("A", FullMerge(op, null, "A"));
        Assert.AreEqual("ABC", FullMerge(op, null, "ABC"));
        Assert.AreEqual("Z", FullMerge(op, null, "ABC", "Z", "C", "AXX"));
        Assert.AreEqual("ZZZ", FullMerge(op, null, "ABC", "CC", "Z", "ZZZ"));
        Assert.AreEqual("a", FullMerge(op, "a", "ABC", "CC", "Z", "ZZZ"));

        Assert.AreEqual("z", PartialMerge(op, "a", "z", "efqfqwgwew", "aaz", "hhhhh"));
        // PartialMerge(left, right) keeps left when left >= right; the multi variant gives the same answers.
        Assert.AreEqual("b", PartialMerge(op, "a", "b"));
        Assert.AreEqual("z", PartialMerge(op, "z", "azzz"));
        Assert.AreEqual("a", PartialMerge(op, "a", ""));
        // Slice::compare: a proper prefix is smaller; bytes compare unsigned.
        Assert.AreEqual("ab", FullMerge(op, "ab", "a"));
        CollectionAssert.AreEqual(new byte[] { 0xff }, FullMergeBytes(op, null, [0x7f, 0x7f], [0xff]));
        Assert.IsFalse(op.AllowSingleOperand);
    }

    [TestMethod]
    public void PutOperatorsKeepTheNewestOperand()
    {
        foreach (MergeOperator op in new MergeOperator[] { PutOperator.Instance, PutOperatorV2.Instance })
        {
            Assert.AreEqual("PutOperator", op.Name);
            Assert.AreEqual("c", FullMerge(op, "base", "a", "b", "c"));
            Assert.AreEqual("a", FullMerge(op, null, "a"));
            Assert.AreEqual("", FullMerge(op, "base", "a", ""));
            Assert.AreEqual("c", PartialMerge(op, "a", "b", "c"));
            Assert.AreEqual("", PartialMerge(op, "a", ""));
        }
        Assert.AreEqual("put_v1", PutOperator.Instance.NickName);
        Assert.AreEqual("put", PutOperatorV2.Instance.NickName);
        Assert.AreSame(PutOperatorV2.Instance, MergeOperator.Put);
    }

    [TestMethod]
    public void SortListMergesSortedLists()
    {
        MergeOperator op = SortListOperator.Instance;
        Assert.AreEqual("MergeSortOperator", op.Name);
        Assert.AreEqual("sortlist", op.NickName);
        Assert.AreEqual("1,2,3,5,9,10", FullMerge(op, null, "1,5,9", "2,3,10"));
        Assert.AreEqual("1,2,3,4,5,6", FullMerge(op, null, "5", "1,6", "2,3,4"));
        Assert.AreEqual("7", FullMerge(op, null, "7"));
        // The existing value is ignored (SortList::FullMergeV2 only reads operand_list).
        Assert.AreEqual("4", FullMerge(op, "1,2,3", "4"));
        Assert.AreEqual("-5,-1,0,3", FullMerge(op, null, "-1,3", "-5,0"));
        // Duplicates are kept.
        Assert.AreEqual("1,1,2,2", FullMerge(op, null, "1,2", "1,2"));
        // Unsorted operands go through the two-way merge unchanged (ties take from the right).
        Assert.AreEqual("3,1", FullMerge(op, null, "3,1"));
        Assert.AreEqual("2,3,1", FullMerge(op, null, "3,1", "2"));
        // std::stoi: leading whitespace, '+', trailing garbage are accepted; std::to_string normalises.
        Assert.AreEqual("1,2,3,12", FullMerge(op, null, " 1,+2,03,12abc"));
        Assert.AreEqual("-2147483648,2147483647", FullMerge(op, null, "-2147483648,2147483647"));
        // A NUL byte ends the list like the C string upstream scans.
        Assert.AreEqual("1,2", S(FullMergeBytes(op, null, [(byte)'1', (byte)',', (byte)'2', 0, (byte)',', (byte)'3'])));

        // Pieces std::stoi rejects (invalid_argument / out_of_range) fail the merge.
        AssertMergeFails(op, null, "");
        AssertMergeFails(op, null, "1,");
        AssertMergeFails(op, null, ",1");
        AssertMergeFails(op, null, "1,,2");
        AssertMergeFails(op, null, "abc");
        AssertMergeFails(op, null, "-");
        AssertMergeFails(op, null, "1", "2147483648");
        AssertMergeFails(op, null, "-2147483649");
        AssertMergeFails(op, null, "99999999999999999999999");

        // Upstream's PartialMergeMulti writes nothing; the port keeps the operands instead.
        Assert.IsNull(PartialMerge(op, "1,3", "2"));
    }

    [TestMethod]
    public void SortListStoiMatchesStrtol()
    {
        (string In, int? Out)[] cases =
        [
            ("0", 0), ("-0", 0), ("\t\n\v\f\r 42", 42), ("+7", 7), ("12 34", 12), ("0x10", 0), ("1e3", 1),
            ("  ", null), ("+", null), ("+-1", null), ("2147483647", int.MaxValue), ("-2147483648", int.MinValue),
            ("2147483648", null), ("00000000000000000000001", 1), ("4294967296", null),
        ];
        foreach ((string input, int? expected) in cases)
        {
            bool ok = SortListOperator.TryStoi(B(input), out int v);
            Assert.AreEqual(expected.HasValue, ok, input);
            if (ok) Assert.AreEqual(expected!.Value, v, input);
        }
    }

    [TestMethod]
    public void BytesXorLengthRules()
    {
        MergeOperator op = BytesXorOperator.Instance;
        Assert.AreEqual("BytesXOR", op.Name);
        Assert.AreEqual("bytesxor", op.NickName);
        // No existing value: the first operand as is.
        CollectionAssert.AreEqual(new byte[] { 1, 2, 3 }, FullMergeBytes(op, null, [1, 2, 3]));
        // Common prefix XORed, tail of the longer input copied (either side longer).
        CollectionAssert.AreEqual(new byte[] { 1 ^ 7, 2, 3 }, FullMergeBytes(op, [1, 2, 3], [7]));
        CollectionAssert.AreEqual(new byte[] { 1 ^ 7, 8, 9 }, FullMergeBytes(op, [1], [7, 8, 9]));
        CollectionAssert.AreEqual(new byte[] { 0xff, 0x0f }, FullMergeBytes(op, [], [0xff, 0x0f]));
        CollectionAssert.AreEqual(new byte[] { 0, 0 }, FullMergeBytes(op, [5, 6], [5, 6]));
        CollectionAssert.AreEqual(Array.Empty<byte>(), FullMergeBytes(op, null, []));
        CollectionAssert.AreEqual(new byte[] { 1 ^ 2 ^ 4, 8 ^ 16, 32 }, FullMergeBytes(op, [1], [2, 8], [4, 16, 32]));
        CollectionAssert.AreEqual(new byte[] { 1 ^ 2 ^ 4, 8 ^ 16, 32 }, PartialMergeBytes(op, [1], [2, 8], [4, 16, 32]));

        // Long values exercise the vectorised path; compare with the byte-at-a-time fold of BytesXOROperator::XOR.
        var rnd = new Random(Environment.TickCount);
        for (int round = 0; round < 50; round++)
        {
            byte[]? model = rnd.Next(2) == 0 ? null : RandomBytes(rnd);
            byte[]? existing = model?.ToArray();
            var ops = new byte[rnd.Next(1, 6)][];
            for (int i = 0; i < ops.Length; i++)
            {
                ops[i] = RandomBytes(rnd);
                model = model is null ? ops[i].ToArray() : XorReference(model, ops[i]);
            }
            CollectionAssert.AreEqual(model, FullMergeBytes(op, existing, ops));
        }
    }

    private static byte[] RandomBytes(Random rnd)
    {
        byte[] b = new byte[rnd.Next(0, 200)];
        rnd.NextBytes(b);
        return b;
    }

    /// <summary>BytesXOROperator::XOR, byte by byte.</summary>
    private static byte[] XorReference(byte[] existing, byte[] value)
    {
        int min = Math.Min(existing.Length, value.Length), max = Math.Max(existing.Length, value.Length);
        var r = new List<byte>(max);
        for (int i = 0; i < min; i++) r.Add((byte)(existing[i] ^ value[i]));
        byte[] longer = existing.Length == max ? existing : value;
        for (int i = min; i < max; i++) r.Add(longer[i]);
        return r.ToArray();
    }

    [TestMethod]
    public void StringAppendTestOperatorMatchesStringAppend()
    {
        var test = new StringAppendTestOperator();
        Assert.AreEqual("StringAppendTESTOperator", test.Name);
        Assert.AreEqual("stringappendtest", test.NickName);
        var assoc = new StringAppendOperator();
        string?[] existings = [null, "", "x"];
        foreach (string? e in existings)
        {
            foreach (string[] ops in new[] { new[] { "a" }, new[] { "a", "b", "c" }, new[] { "", "" } })
            {
                Assert.AreEqual(FullMerge(assoc, e, ops), FullMerge(test, e, ops));
            }
        }
        Assert.AreEqual("a<>b", FullMerge(new StringAppendTestOperator("<>"), null, "a", "b"));
        Assert.AreEqual("ab", FullMerge(new StringAppendTestOperator(""), null, "a", "b"));
        // PartialMergeMulti returns false: the operands stay separate.
        Assert.IsNull(PartialMerge(test, "a", "b"));
        Assert.AreEqual("stringappend", assoc.NickName);
    }

    [TestMethod]
    public void MergeOperatorFromString()
    {
        // options/options_test.cc MergeOperatorFromString and customizable_test.cc LoadMergeOperatorTest: every name
        // RegisterBuiltinMergeOperators accepts, with the class it creates.
        (string Id, Type Type, string Name)[] cases =
        [
            ("put", typeof(PutOperatorV2), "PutOperator"),
            ("PutOperator", typeof(PutOperatorV2), "PutOperator"),
            ("put_v1", typeof(PutOperator), "PutOperator"),
            ("uint64add", typeof(UInt64AddOperator), "UInt64AddOperator"),
            ("UInt64AddOperator", typeof(UInt64AddOperator), "UInt64AddOperator"),
            ("max", typeof(MaxOperator), "MaxOperator"),
            ("MaxOperator", typeof(MaxOperator), "MaxOperator"),
            ("bytesxor", typeof(BytesXorOperator), "BytesXOR"),
            ("BytesXOR", typeof(BytesXorOperator), "BytesXOR"),
            ("sortlist", typeof(SortListOperator), "MergeSortOperator"),
            ("MergeSortOperator", typeof(SortListOperator), "MergeSortOperator"),
            ("stringappend", typeof(StringAppendOperator), "StringAppendOperator"),
            ("StringAppendOperator", typeof(StringAppendOperator), "StringAppendOperator"),
            ("stringappendtest", typeof(StringAppendTestOperator), "StringAppendTESTOperator"),
            ("StringAppendTESTOperator", typeof(StringAppendTestOperator), "StringAppendTESTOperator"),
            ("{id=MaxOperator;}", typeof(MaxOperator), "MaxOperator"),
            ("AggMergeOperator.v1", typeof(AggMergeOperator), "AggMergeOperator.v1"),
        ];
        foreach ((string id, Type type, string name) in cases)
        {
            MergeOperator? op = MergeOperator.CreateBuiltin(id);
            Assert.IsNotNull(op, id);
            Assert.AreEqual(type, op.GetType(), id);
            Assert.AreEqual(name, op.Name, id);
            Assert.IsTrue(id == op.Name || id == op.NickName || id.StartsWith('{'), id); // IsInstanceOf(id)
        }
        Assert.AreEqual(","u8.ToArray().Length, ((StringAppendOperator)MergeOperator.CreateBuiltin("stringappend")!).Delimiter.Length);
        Assert.AreEqual(",", S(((StringAppendTestOperator)MergeOperator.CreateBuiltin("stringappendtest")!).Delimiter));

        // "id=...; delimiter=..." with and without braces.
        Assert.AreEqual("||", S(Assert.IsInstanceOfType<StringAppendOperator>(MergeOperator.CreateBuiltin("id=stringappend; delimiter=||")).Delimiter));
        var t = Assert.IsInstanceOfType<StringAppendTestOperator>(MergeOperator.CreateBuiltin("id=stringappendtest; delimiter=&&"));
        Assert.AreEqual("&&", S(t.Delimiter));
        // ToString(config_options) -> CreateFromString gives an equivalent copy.
        var copy = Assert.IsInstanceOfType<StringAppendTestOperator>(MergeOperator.CreateBuiltin(t.OptionsId));
        Assert.AreEqual("&&", S(copy.Delimiter));
        Assert.AreEqual("{id=StringAppendTESTOperator;delimiter=&&;}", t.OptionsId);

        // Unknown ids and options an operator does not have stay unresolved.
        Assert.IsNull(MergeOperator.CreateBuiltin("Changling"));
        Assert.IsNull(MergeOperator.CreateBuiltin("{id=MaxOperator;delimiter=,;}"));
        Assert.IsNull(MergeOperator.CreateBuiltin("{id=StringAppendOperator;foo=1;}"));
        Assert.IsNull(MergeOperator.CreateBuiltin(""));
    }

    [TestMethod]
    public void OptionsTextRoundTrips()
    {
        (MergeOperator Op, string Text)[] cases =
        [
            (MaxOperator.Instance, "MaxOperator"),
            (PutOperatorV2.Instance, "PutOperator"),
            (PutOperator.Instance, "PutOperator"),
            (SortListOperator.Instance, "MergeSortOperator"),
            (BytesXorOperator.Instance, "BytesXOR"),
            (UInt64AddOperator.Instance, "UInt64AddOperator"),
            (new StringAppendOperator(), "{id=StringAppendOperator;delimiter=,;}"),
            (new StringAppendOperator("|"), "{id=StringAppendOperator;delimiter=|;}"),
            (new StringAppendTestOperator(), "{id=StringAppendTESTOperator;delimiter=,;}"),
            (AggMergeOperator.Instance, "AggMergeOperator.v1"),
        ];
        foreach ((MergeOperator op, string text) in cases)
        {
            var o = new ColumnFamilyOptions { MergeOperator = op };
            Assert.AreEqual(text, o.GetOption("merge_operator"), op.Name);
            Assert.AreEqual(op.Name, o.MergeOperatorName);
            var parsed = new ColumnFamilyOptions();
            parsed.SetOption("merge_operator", text, preserveUnknown: false);
            Assert.IsNotNull(parsed.MergeOperator, text);
            Assert.AreEqual(op.Name, parsed.MergeOperator.Name);
            Assert.AreEqual(text, parsed.GetOption("merge_operator"));
        }
        // An OPTIONS file written with the legacy put operator reloads as V2 (both are named "PutOperator").
        var p = new ColumnFamilyOptions();
        p.SetOption("merge_operator", "PutOperator", preserveUnknown: false);
        Assert.AreSame(PutOperatorV2.Instance, p.MergeOperator);
    }
}
