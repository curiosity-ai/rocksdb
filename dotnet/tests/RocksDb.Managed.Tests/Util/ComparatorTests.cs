using RocksDb.Managed.Util;

namespace RocksDb.Managed.Tests.Util;

/// <summary>Ports of the comparator cases in db/comparator_db_test.cc (separator vectors are persisted in index blocks).</summary>
[TestClass]
public sealed class ComparatorTests
{
    private static byte[] B(string s) => System.Text.Encoding.Latin1.GetBytes(s);

    private static byte[] Separator(Comparator c, byte[] start, byte[] limit)
    {
        using var w = new PooledBufferWriter();
        c.FindShortestSeparator(start, limit, w);
        return w.WrittenSpan.ToArray();
    }

    private static byte[] Successor(Comparator c, byte[] key)
    {
        using var w = new PooledBufferWriter();
        c.FindShortSuccessor(key, w);
        return w.WrittenSpan.ToArray();
    }

    private static int Cmp(byte[] a, byte[] b) => a.AsSpan().SequenceCompareTo(b);

    [TestMethod]
    public void Names()
    {
        Assert.AreEqual("leveldb.BytewiseComparator", Comparator.Bytewise.Name);
        Assert.AreEqual("rocksdb.ReverseBytewiseComparator", Comparator.ReverseBytewise.Name);
        Assert.AreEqual("leveldb.BytewiseComparator.u64ts", Comparator.BytewiseWithU64Ts.Name);
        Assert.AreEqual("rocksdb.ReverseBytewiseComparator.u64ts", Comparator.ReverseBytewiseWithU64Ts.Name);
        foreach (Comparator c in new[] { Comparator.Bytewise, Comparator.ReverseBytewise, Comparator.BytewiseWithU64Ts, Comparator.ReverseBytewiseWithU64Ts })
        {
            Assert.IsTrue(Comparator.TryGetBuiltin(c.Name, out Comparator? found));
            Assert.AreSame(c, found);
        }
        Assert.IsFalse(Comparator.TryGetBuiltin("my.Comparator", out _));
    }

    [TestMethod]
    public void FindShortestSeparatorVectors()
    {
        CollectionAssert.AreEqual(B("abc2"), Separator(Comparator.Bytewise, B("abc1xyz"), B("abc3xy")));
        CollectionAssert.AreEqual(B("abc5"), Separator(Comparator.ReverseBytewise, B("abc5xyztt"), B("abc3xy")));
        CollectionAssert.AreEqual(B("abc3"), Separator(Comparator.ReverseBytewise, B("abc3"), B("abc2xy")));
        CollectionAssert.AreEqual(B("abc3"), Separator(Comparator.ReverseBytewise, B("abc3xyz"), B("abc2xy")));
        CollectionAssert.AreEqual(B("abc3"), Separator(Comparator.ReverseBytewise, B("abc3xyz"), B("abc2")));
        byte[] rev = Separator(Comparator.ReverseBytewise, B("abc2xy"), B("abc2"));
        Assert.IsTrue(Cmp(B("abc2xy"), rev) >= 0);
        Assert.IsTrue(Cmp(rev, B("abc2")) > 0);

        // Bytewise 0xff skipping branch: "AA1" 0xff 0xff 'x' ... vs "AA2" -> increment first non-0xff after diff.
        CollectionAssert.AreEqual(new byte[] { 0x41, 0x41, 0x31, 0xff, 0xff, 0x79 },
            Separator(Comparator.Bytewise, new byte[] { 0x41, 0x41, 0x31, 0xff, 0xff, 0x78, 0x10 }, B("AA2")));
        // Prefix: unchanged.
        CollectionAssert.AreEqual(B("foo"), Separator(Comparator.Bytewise, B("foo"), B("foobar")));
        CollectionAssert.AreEqual(B("foobar"), Separator(Comparator.Bytewise, B("foobar"), B("foo")));
        // Misordered: unchanged.
        CollectionAssert.AreEqual(B("foo"), Separator(Comparator.Bytewise, B("foo"), B("bar")));
    }

    [TestMethod]
    public void FindShortSuccessorVectors()
    {
        CollectionAssert.AreEqual(B("g"), Successor(Comparator.Bytewise, B("foo")));
        CollectionAssert.AreEqual(new byte[] { 0xff, 0xff }, Successor(Comparator.Bytewise, new byte[] { 0xff, 0xff }));
        CollectionAssert.AreEqual(new byte[] { 0xff, 0x02 }, Successor(Comparator.Bytewise, new byte[] { 0xff, 0x01, 0x05 }));
        CollectionAssert.AreEqual(B("foo"), Successor(Comparator.ReverseBytewise, B("foo")));
        CollectionAssert.AreEqual(Array.Empty<byte>(), Successor(Comparator.Bytewise, []));
    }

    [TestMethod]
    public void IsSameLengthImmediateSuccessor()
    {
        (byte[] s, byte[] t, bool succ)[] cases =
        [
            (B("abcxy"), B("abcxyz"), false),
            (B("abcxyz"), B("abcxy"), false),
            (B("abc1xyz"), B("abc2xyz"), false),
            (B("abcxyz"), B("abcxyz"), false),
            (B("abcxy"), B("abcxz"), true),
            (new byte[] { 0x50, 0x8a, 0xac }, new byte[] { 0x50, 0x8a, 0xad }, true),
            (new byte[] { 0x50, 0x8a, 0xff }, new byte[] { 0x50, 0x8b, 0x00 }, true),
            (new byte[] { 0x50, 0x8a, 0xff, 0xff }, new byte[] { 0x50, 0x8b, 0x00, 0x00 }, true),
            (new byte[] { 0x50, 0x8a, 0xff, 0xff }, new byte[] { 0x50, 0x8b, 0x00, 0x01 }, false),
        ];
        foreach ((byte[] s, byte[] t, bool succ) in cases)
        {
            Assert.AreEqual(succ, Comparator.Bytewise.IsSameLengthImmediateSuccessor(s, t));
            Assert.IsFalse(Comparator.Bytewise.IsSameLengthImmediateSuccessor(t, s));
            Assert.IsFalse(Comparator.ReverseBytewise.IsSameLengthImmediateSuccessor(s, t));
            Assert.IsFalse(Comparator.ReverseBytewise.IsSameLengthImmediateSuccessor(t, s));
        }
    }

    /// <summary>Port of SeparatorSuccessorRandomizeTest.</summary>
    [TestMethod]
    public void SeparatorSuccessorRandomized()
    {
        int seed = Environment.TickCount;
        var rnd = new Random(seed);
        string msg = "seed=" + seed;
        byte[] charList = [0, 1, 2, 253, 254, 255];
        for (int attempt = 0; attempt < 5000; attempt++)
        {
            int size1 = rnd.Next(0, 16);
            int size2 = rnd.Next(2) == 0 ? rnd.Next(0, 16) : Math.Max(0, size1 + rnd.Next(5) - 2);
            byte[] s1 = new byte[size1];
            for (int i = 0; i < size1; i++) s1[i] = rnd.Next(2) == 0 ? (byte)rnd.Next(256) : charList[rnd.Next(charList.Length)];
            byte[] s2 = new byte[size2];
            Array.Copy(s1, s2, Math.Min(size1, size2));
            for (int pos = size2 - 1; pos >= 0; pos--)
            {
                if (pos >= size1 || rnd.Next(4) == 0) s2[pos] = (byte)rnd.Next(256);
                else if (rnd.Next(4) == 0) break;
                else s2[pos] = (byte)Math.Clamp(s1[pos] + rnd.Next(5) - 2, 0, 255);
            }
            for (int rev = 0; rev < 2; rev++)
            {
                if (rev == 1) (s1, s2) = (s2, s1);
                byte[] sep = Separator(Comparator.Bytewise, s1, s2);
                byte[] rsep = Separator(Comparator.ReverseBytewise, s1, s2);
                int c = Cmp(s1, s2);
                if (c == 0)
                {
                    CollectionAssert.AreEqual(s1, sep, msg);
                    CollectionAssert.AreEqual(s2, rsep, msg);
                }
                else if (c < 0)
                {
                    Assert.IsTrue(Cmp(s1, sep) <= 0, msg);
                    Assert.IsTrue(Cmp(s2, sep) > 0, msg);
                    Assert.IsTrue(sep.Length <= Math.Max(s1.Length, s2.Length), msg);
                    CollectionAssert.AreEqual(s1, rsep, msg);
                }
                else
                {
                    Assert.IsTrue(Cmp(s1, rsep) >= 0, msg);
                    Assert.IsTrue(Cmp(s2, rsep) < 0, msg);
                    Assert.IsTrue(rsep.Length <= Math.Max(s1.Length, s2.Length), msg);
                    CollectionAssert.AreEqual(s1, sep, msg);
                }
            }
            Assert.IsTrue(Cmp(Successor(Comparator.Bytewise, s1), s1) >= 0, msg);
            Assert.IsTrue(Cmp(Successor(Comparator.ReverseBytewise, s1), s1) <= 0, msg);
        }
    }

    private static byte[] TsKey(string k, ulong ts)
    {
        byte[] r = new byte[k.Length + 8];
        System.Text.Encoding.Latin1.GetBytes(k, r);
        ComparatorWithU64Ts.EncodeU64Ts(ts, r.AsSpan(k.Length));
        return r;
    }

    [TestMethod]
    public void U64TsOrdering()
    {
        Comparator c = Comparator.BytewiseWithU64Ts;
        Assert.AreEqual(8, c.TimestampSize);
        Assert.IsTrue(c.Compare(TsKey("a", 1), TsKey("b", 9)) < 0);
        Assert.IsTrue(c.Compare(TsKey("a", 9), TsKey("a", 1)) < 0, "newer timestamp first");
        Assert.AreEqual(0, c.Compare(TsKey("a", 5), TsKey("a", 5)));
        Assert.AreEqual(0, c.CompareWithoutTimestamp(TsKey("a", 5), TsKey("a", 7)));
        Assert.IsTrue(c.CompareWithoutTimestamp(TsKey("a", 5), true, "b"u8, false) < 0);
        Assert.IsTrue(Comparator.ReverseBytewiseWithU64Ts.Compare(TsKey("a", 1), TsKey("b", 1)) > 0);
        Assert.IsTrue(Comparator.ReverseBytewiseWithU64Ts.Compare(TsKey("a", 9), TsKey("a", 1)) < 0);
        Assert.AreSame(Comparator.Bytewise, c.RootComparator);
        Assert.AreEqual("7", c.TimestampToString(TsKey("", 7)));
        Assert.IsTrue(ComparatorWithU64Ts.TryDecodeU64Ts(TsKey("", 42), out ulong ts));
        Assert.AreEqual(42UL, ts);
        Assert.IsFalse(ComparatorWithU64Ts.TryDecodeU64Ts([1, 2], out _));
    }

    [TestMethod]
    public void ReverseOrdering()
    {
        Assert.IsTrue(Comparator.ReverseBytewise.Compare("b"u8, "a"u8) < 0);
        Assert.IsTrue(Comparator.ReverseBytewise.Compare("a"u8, "ab"u8) > 0);
        Assert.IsFalse(Comparator.Bytewise.CanKeysWithDifferentByteContentsBeEqual);
        Assert.IsFalse(Comparator.ReverseBytewise.CanKeysWithDifferentByteContentsBeEqual);
    }
}
