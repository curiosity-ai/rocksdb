using RocksDb.Managed.Db;
using ValueType = RocksDb.Managed.Db.ValueType;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Tests.Db;

/// <summary>Ports of db/dbformat_test.cc plus InternalKeyComparator / LookupKey checks.</summary>
[TestClass]
public sealed class InternalKeyTests
{
    private static byte[] IKey(string userKey, ulong seq, ValueType vt) =>
        new InternalKey(System.Text.Encoding.Latin1.GetBytes(userKey), seq, vt).Encoded.ToArray();

    /// <summary>Wraps bytewise as a "custom" comparator to exercise the non-fast-path code.</summary>
    private sealed class OpaqueBytewise : Comparator
    {
        public override string Name => "test.OpaqueBytewise";
        public override int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => Comparator.Bytewise.Compare(a, b);
        public override void FindShortestSeparator(ReadOnlySpan<byte> start, ReadOnlySpan<byte> limit, System.Buffers.IBufferWriter<byte> result) =>
            Comparator.Bytewise.FindShortestSeparator(start, limit, result);
        public override void FindShortSuccessor(ReadOnlySpan<byte> key, System.Buffers.IBufferWriter<byte> result) =>
            Comparator.Bytewise.FindShortSuccessor(key, result);
    }

    private static readonly InternalKeyComparator[] s_bytewiseLike =
        [InternalKeyComparator.Bytewise, new InternalKeyComparator(new OpaqueBytewise())];

    private static byte[] Shorten(InternalKeyComparator icmp, byte[] s, byte[] l)
    {
        using var w = new PooledBufferWriter();
        icmp.FindShortestSeparator(s, l, w);
        byte[] r = w.WrittenSpan.ToArray();
        Assert.IsTrue(icmp.Compare(s, r) <= 0);
        if (icmp.Compare(s, l) < 0) Assert.IsTrue(icmp.Compare(r, l) < 0);
        return r;
    }

    private static byte[] ShortSuccessor(InternalKeyComparator icmp, byte[] s)
    {
        using var w = new PooledBufferWriter();
        icmp.FindShortSuccessor(s, w);
        return w.WrittenSpan.ToArray();
    }

    [TestMethod]
    public void InternalKeyEncodeDecode()
    {
        string[] keys = ["", "k", "hello", "longggggggggggggggggggggg"];
        ulong[] seqs = [1, 2, 3, (1UL << 8) - 1, 1UL << 8, (1UL << 8) + 1, (1UL << 16) - 1, 1UL << 16, (1UL << 16) + 1,
            (1UL << 32) - 1, 1UL << 32, (1UL << 32) + 1];
        foreach (string k in keys)
        {
            foreach (ulong s in seqs)
            {
                foreach (ValueType vt in new[] { ValueType.Value, ValueType.Deletion })
                {
                    byte[] enc = IKey(k, s, vt);
                    Assert.IsTrue(DbFormat.TryParseInternalKey(enc, out ParsedInternalKey p));
                    Assert.AreEqual(k, System.Text.Encoding.Latin1.GetString(enc.AsSpan(0, p.UserKeyLength)));
                    Assert.AreEqual(s, p.Sequence);
                    Assert.AreEqual(vt, p.Type);
                    var ik = InternalKey.FromEncoded(enc);
                    Assert.AreEqual(s, ik.Sequence);
                    Assert.AreEqual(vt, ik.Type);
                    Assert.IsTrue(ik.Valid);
                }
            }
        }
        Assert.IsFalse(DbFormat.TryParseInternalKey("bar"u8, out _));
        Assert.IsFalse(default(InternalKey).Valid);
    }

    [TestMethod]
    public void InternalKeyShortSeparator()
    {
        foreach (InternalKeyComparator icmp in s_bytewiseLike)
        {
            // When user keys are same
            CollectionAssert.AreEqual(IKey("foo", 100, ValueType.Value), Shorten(icmp, IKey("foo", 100, ValueType.Value), IKey("foo", 99, ValueType.Value)));
            CollectionAssert.AreEqual(IKey("foo", 100, ValueType.Value), Shorten(icmp, IKey("foo", 100, ValueType.Value), IKey("foo", 101, ValueType.Value)));
            CollectionAssert.AreEqual(IKey("foo", 100, ValueType.Value), Shorten(icmp, IKey("foo", 100, ValueType.Value), IKey("foo", 100, ValueType.Value)));
            CollectionAssert.AreEqual(IKey("foo", 100, ValueType.Value), Shorten(icmp, IKey("foo", 100, ValueType.Value), IKey("foo", 100, ValueType.Deletion)));
            // When user keys are misordered
            CollectionAssert.AreEqual(IKey("foo", 100, ValueType.Value), Shorten(icmp, IKey("foo", 100, ValueType.Value), IKey("bar", 99, ValueType.Value)));
            // When user keys are different, but correctly ordered
            byte[] Seek(string k) => IKey(k, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
            CollectionAssert.AreEqual(Seek("g"), Shorten(icmp, IKey("foo", 100, ValueType.Value), IKey("hello", 200, ValueType.Value)));
            CollectionAssert.AreEqual(Seek("ABC2"), Shorten(icmp, IKey("ABC1AAAAA", 100, ValueType.Value), IKey("ABC2ABB", 200, ValueType.Value)));
            CollectionAssert.AreEqual(Seek("AAA2"), Shorten(icmp, IKey("AAA1AAA", 100, ValueType.Value), IKey("AAA2AA", 200, ValueType.Value)));
            CollectionAssert.AreEqual(Seek("AAA2"), Shorten(icmp, IKey("AAA1AAA", 100, ValueType.Value), IKey("AAA4", 200, ValueType.Value)));
            CollectionAssert.AreEqual(Seek("AAA1B"), Shorten(icmp, IKey("AAA1AAA", 100, ValueType.Value), IKey("AAA2", 200, ValueType.Value)));
            CollectionAssert.AreEqual(Seek("AAA2"), Shorten(icmp, IKey("AAA1AAA", 100, ValueType.Value), IKey("AAA2A", 200, ValueType.Value)));
            CollectionAssert.AreEqual(IKey("AAA1", 100, ValueType.Value), Shorten(icmp, IKey("AAA1", 100, ValueType.Value), IKey("AAA2", 200, ValueType.Value)));
            // When start user key is prefix of limit user key
            CollectionAssert.AreEqual(IKey("foo", 100, ValueType.Value), Shorten(icmp, IKey("foo", 100, ValueType.Value), IKey("foobar", 200, ValueType.Value)));
            // When limit user key is prefix of start user key
            CollectionAssert.AreEqual(IKey("foobar", 100, ValueType.Value), Shorten(icmp, IKey("foobar", 100, ValueType.Value), IKey("foo", 200, ValueType.Value)));
        }
    }

    [TestMethod]
    public void InternalKeyShortestSuccessor()
    {
        foreach (InternalKeyComparator icmp in s_bytewiseLike)
        {
            CollectionAssert.AreEqual(IKey("g", DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek), ShortSuccessor(icmp, IKey("foo", 100, ValueType.Value)));
            CollectionAssert.AreEqual(IKey("\xff\xff", 100, ValueType.Value), ShortSuccessor(icmp, IKey("\xff\xff", 100, ValueType.Value)));
        }
    }

    [TestMethod]
    public void InternalKeyComparatorOrdering()
    {
        InternalKeyComparator icmp = InternalKeyComparator.Bytewise;
        // user key ascending
        Assert.IsTrue(icmp.Compare(IKey("a", 1, ValueType.Value), IKey("b", 100, ValueType.Value)) < 0);
        // sequence descending
        Assert.IsTrue(icmp.Compare(IKey("a", 100, ValueType.Value), IKey("a", 1, ValueType.Value)) < 0);
        // type descending
        Assert.IsTrue(icmp.Compare(IKey("a", 5, ValueType.Value), IKey("a", 5, ValueType.Deletion)) < 0);
        Assert.AreEqual(0, icmp.CompareKeySeq(IKey("a", 5, ValueType.Value), IKey("a", 5, ValueType.Deletion)));
        Assert.AreEqual(0, icmp.Compare(IKey("a", 5, ValueType.Value), IKey("a", 5, ValueType.Value)));
        // parsed overload
        Assert.IsTrue(icmp.Compare(IKey("a", 5, ValueType.Value), "a"u8, 4, ValueType.Value) < 0);
        // global seqno override
        Assert.IsTrue(icmp.Compare(IKey("a", 0, ValueType.Value), 10, IKey("a", 5, ValueType.Value), InternalKeyComparator.DisableGlobalSequenceNumber) < 0);

        var rev = new InternalKeyComparator(Comparator.ReverseBytewise);
        Assert.IsFalse(rev.IsBytewise);
        Assert.IsTrue(rev.Compare(IKey("b", 1, ValueType.Value), IKey("a", 1, ValueType.Value)) < 0);
        Assert.IsTrue(rev.Compare(IKey("a", 9, ValueType.Value), IKey("a", 1, ValueType.Value)) < 0);
        Assert.AreEqual("rocksdb.InternalKeyComparator:leveldb.BytewiseComparator", icmp.Name);

        // Randomized: sort with the fast path and the opaque comparator, results must agree.
        var rnd = new Random(301);
        var keys = new List<byte[]>();
        for (int i = 0; i < 2000; i++)
        {
            byte[] uk = new byte[rnd.Next(0, 5)];
            rnd.NextBytes(uk);
            keys.Add(new InternalKey(uk, (ulong)rnd.Next(0, 4), (ValueType)rnd.Next(0, 3)).Encoded.ToArray());
        }
        var a = new List<byte[]>(keys);
        var b = new List<byte[]>(keys);
        a.Sort((x, y) => s_bytewiseLike[0].Compare(x, y));
        b.Sort((x, y) => s_bytewiseLike[1].Compare(x, y));
        for (int i = 0; i < a.Count; i++) CollectionAssert.AreEqual(a[i], b[i]);
        for (int i = 1; i < a.Count; i++)
        {
            Assert.IsTrue(DbFormat.TryParseInternalKey(a[i - 1], out ParsedInternalKey p0));
            Assert.IsTrue(DbFormat.TryParseInternalKey(a[i], out ParsedInternalKey p1));
            int uc = a[i - 1].AsSpan(0, p0.UserKeyLength).SequenceCompareTo(a[i].AsSpan(0, p1.UserKeyLength));
            Assert.IsTrue(uc < 0 || (uc == 0 && (p0.Sequence > p1.Sequence || (p0.Sequence == p1.Sequence && p0.Type >= p1.Type))));
        }
    }

    [TestMethod]
    public void IterKeyOperation()
    {
        using var k = new IterKey();
        byte[] p = "abcdefghijklmnopqrstuvwxyz"u8.ToArray();
        byte[] q = "0123456789"u8.ToArray();
        static string S(ReadOnlySpan<byte> s) => System.Text.Encoding.ASCII.GetString(s);
        Assert.AreEqual("", S(k.UserKey));
        k.TrimAppend(0, p.AsSpan(0, 3));
        Assert.AreEqual("abc", S(k.UserKey));
        k.TrimAppend(1, p.AsSpan(0, 3));
        Assert.AreEqual("aabc", S(k.UserKey));
        k.TrimAppend(0, p);
        Assert.AreEqual("abcdefghijklmnopqrstuvwxyz", S(k.UserKey));
        k.TrimAppend(26, q);
        Assert.AreEqual("abcdefghijklmnopqrstuvwxyz0123456789", S(k.UserKey));
        k.TrimAppend(36, q.AsSpan(0, 1));
        Assert.AreEqual("abcdefghijklmnopqrstuvwxyz01234567890", S(k.UserKey));
        k.TrimAppend(26, q.AsSpan(0, 1));
        Assert.AreEqual("abcdefghijklmnopqrstuvwxyz0", S(k.UserKey));
        // Size going up, memory allocation is triggered
        k.TrimAppend(27, p);
        Assert.AreEqual("abcdefghijklmnopqrstuvwxyz0abcdefghijklmnopqrstuvwxyz", S(k.UserKey));
        k.TrimAppend(53, p);
        k.TrimAppend(79, p);
        Assert.AreEqual(105, k.Size);

        k.SetInternalKey("foo"u8, 7, ValueType.Merge);
        Assert.IsFalse(k.IsUserKey);
        Assert.AreEqual("foo", S(k.UserKey));
        k.UpdateInternalKey(9, ValueType.Value);
        CollectionAssert.AreEqual(IKey("foo", 9, ValueType.Value), k.InternalKey.ToArray());
    }

    [TestMethod]
    public void LookupKeyLayout()
    {
        using (var lk = new LookupKey("hello"u8, 42, stackalloc byte[LookupKey.StackBufferSize]))
        {
            Assert.AreEqual(13, lk.MemtableKey[0]);
            CollectionAssert.AreEqual(IKey("hello", 42, DbFormat.ValueTypeForSeek), lk.InternalKey.ToArray());
            Assert.AreEqual("hello", System.Text.Encoding.ASCII.GetString(lk.UserKey));
            Assert.AreEqual(42UL, lk.Sequence);
        }
        // Too large for scratch: rents.
        byte[] big = new byte[1000];
        big.AsSpan().Fill(7);
        using (var lk = new LookupKey(big, 3, stackalloc byte[16]))
        {
            Assert.AreEqual(1008 + 2, lk.MemtableKey.Length);
            Assert.IsTrue(lk.UserKey.SequenceEqual(big));
        }
        // With timestamp.
        using (var lk = new LookupKey("k"u8, 1, "TS"u8, stackalloc byte[LookupKey.StackBufferSize]))
        {
            Assert.AreEqual("kTS", System.Text.Encoding.ASCII.GetString(lk.UserKey));
        }
    }
}
