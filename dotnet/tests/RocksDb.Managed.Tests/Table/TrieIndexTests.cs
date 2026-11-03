using System.Buffers;
using System.Text;
using RocksDb.Managed.Db;
using RocksDb.Managed.Table;
using RocksDb.Managed.Table.TrieIndex;
using RocksDb.Managed.Tests.Db;
using RocksDb.Managed.Util;
using EntryType = RocksDb.Managed.Db.EntryType;

namespace RocksDb.Managed.Tests.Table;

/// <summary>
/// The trie user-defined index (utilities/trie_index): ports of trie_index_test.cc's bitvector, Elias-Fano and LOUDS
/// trie tests (topology variants, randomized stress, prefix keys, chains, corruption / truncation), the
/// TrieIndexFactory seqno side-table tests (same-user-key runs checked against the standard index), table-level reads
/// in every index mode, a DB end to end, and allocation-free lookups.
/// </summary>
[TestClass]
public sealed class TrieIndexTests
{
    // ---- bitvector ---------------------------------------------------------------------------------------------

    private static (Bitvector Bv, bool[] Bits) RandomBitvector(Random rnd, int n, int onePercent)
    {
        var b = new BitvectorBuilder();
        var bits = new bool[n];
        int i = 0;
        while (i < n)
        {
            // Mix single appends, runs and whole words.
            int mode = rnd.Next(3);
            if (mode == 0 || b.NumBits % 64 != 0 || n - i < 64)
            {
                bool bit = rnd.Next(100) < onePercent;
                if (rnd.Next(4) == 0)
                {
                    int run = Math.Min(n - i, rnd.Next(1, 300));
                    b.AppendMultiple(bit, (ulong)run);
                    for (int j = 0; j < run; j++) bits[i++] = bit;
                }
                else
                {
                    b.Append(bit);
                    bits[i++] = bit;
                }
            }
            else
            {
                ulong w = 0;
                for (int j = 0; j < 64; j++)
                {
                    bool bit = rnd.Next(100) < onePercent;
                    if (bit) w |= 1UL << j;
                    bits[i++] = bit;
                }
                b.AppendWord(w, 64);
            }
        }
        return (Bitvector.BuildFrom(b), bits);
    }

    private static void VerifyBitvector(Bitvector bv, bool[] bits)
    {
        Assert.AreEqual((ulong)bits.Length, bv.NumBits);
        ulong ones = 0;
        var onePos = new List<ulong>();
        var zeroPos = new List<ulong>();
        for (int i = 0; i < bits.Length; i++)
        {
            Assert.AreEqual(ones, bv.Rank1((ulong)i), "rank1 " + i);
            Assert.AreEqual(bits[i], bv.GetBit((ulong)i));
            if (bits[i]) { ones++; onePos.Add((ulong)i); }
            else zeroPos.Add((ulong)i);
        }
        Assert.AreEqual(ones, bv.Rank1((ulong)bits.Length));
        Assert.AreEqual(ones, bv.NumOnes);
        for (int k = 0; k < onePos.Count; k++) Assert.AreEqual(onePos[k], bv.FindNthOneBit((ulong)k), "select1 " + k);
        for (int k = 0; k < zeroPos.Count; k++) Assert.AreEqual(zeroPos[k], bv.FindNthZeroBit((ulong)k), "select0 " + k);
        Assert.AreEqual(bv.NumBits, bv.FindNthOneBit((ulong)onePos.Count));
        Assert.AreEqual(bv.NumBits, bv.FindNthZeroBit((ulong)zeroPos.Count));
        // NextSetBit / PrevSetBit against the oracle.
        ulong next = bv.NumBits;
        for (int i = bits.Length - 1; i >= 0; i--)
        {
            if (bits[i]) next = (ulong)i;
            Assert.AreEqual(next, bv.NextSetBit((ulong)i));
        }
        ulong prev = bv.NumBits;
        for (int i = 0; i <= bits.Length; i++)
        {
            Assert.AreEqual(prev, bv.PrevSetBit((ulong)i));
            if (i < bits.Length && bits[i]) prev = (ulong)i;
        }
    }

    [TestMethod]
    public void Bitvector_RankSelectAndSerializationMatchOracle()
    {
        int seed = Environment.TickCount;
        var rnd = new Random(seed);
        foreach (int n in new[] { 0, 1, 63, 64, 65, 255, 256, 257, 1000, 4096, 20000 })
        {
            foreach (int pct in new[] { 0, 3, 50, 97, 100 })
            {
                (Bitvector bv, bool[] bits) = RandomBitvector(rnd, n, pct);
                string ctx = "seed=" + seed + " n=" + n + " pct=" + pct;
                VerifyBitvector(bv, bits);
                using var w = new PooledBufferWriter(256);
                bv.EncodeTo(w);
                Assert.AreEqual(bv.SerializedSize, (ulong)w.WrittenCount, ctx);
                Assert.AreEqual(0, w.WrittenCount % 8, ctx);
                int pos = 0;
                Bitvector back = Bitvector.Read(w.WrittenSpan, ref pos);
                Assert.AreEqual(w.WrittenCount, pos, ctx);
                VerifyBitvector(back, bits);
            }
        }
    }

    [TestMethod]
    public void Bitvector_FindNthSetBitInWord_PortableMatchesHardware()
    {
        var rnd = new Random(11);
        for (int t = 0; t < 2000; t++)
        {
            ulong w = (ulong)rnd.NextInt64() ^ ((ulong)rnd.NextInt64() << 1);
            if (t % 7 == 0) w &= (ulong)rnd.NextInt64();
            int pc = System.Numerics.BitOperations.PopCount(w);
            int seen = 0;
            for (int bit = 0; bit < 64; bit++)
            {
                if (((w >> bit) & 1) == 0) continue;
                Assert.AreEqual((ulong)bit, TrieBits.FindNthSetBitInWord(w, (ulong)seen));
                Assert.AreEqual((ulong)bit, TrieBits.FindNthSetBitInWordPortable(w, (ulong)seen));
                seen++;
            }
            Assert.AreEqual(pc, seen);
        }
    }

    [TestMethod]
    public void Bitvector_InitFromDataCorruption()
    {
        var b = new BitvectorBuilder();
        for (int i = 0; i < 1000; i++) b.Append(i % 3 == 0);
        using var w = new PooledBufferWriter(256);
        Bitvector.BuildFrom(b).EncodeTo(w);
        byte[] good = w.WrittenSpan.ToArray();
        // Every truncation fails with Corruption.
        for (int len = 0; len < good.Length; len += 4)
        {
            int pos = 0;
            Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => Bitvector.Read(good.AsSpan(0, len), ref pos)).Code, "len " + len);
        }
        byte[] bad = (byte[])good.Clone();
        Coding.EncodeFixed64(bad.AsSpan(8), 5000); // num_ones > num_bits
        int p0 = 0;
        StringAssert.Contains(Assert.ThrowsExactly<RocksDbException>(() => Bitvector.Read(bad, ref p0)).Message, "num_ones exceeds num_bits");
        bad = (byte[])good.Clone();
        Coding.EncodeFixed64(bad, (ulong)uint.MaxValue + 1);
        Coding.EncodeFixed64(bad.AsSpan(8), 0);
        StringAssert.Contains(Assert.ThrowsExactly<RocksDbException>(() => Bitvector.Read(bad, ref p0)).Message, "uint32_t range");
        // Misaligned words (relative to the start of the data).
        byte[] shifted = new byte[good.Length + 4];
        good.CopyTo(shifted, 4);
        int p4 = 4;
        StringAssert.Contains(Assert.ThrowsExactly<RocksDbException>(() => Bitvector.Read(shifted, ref p4)).Message, "not 8-byte aligned");
    }

    [TestMethod]
    public void EliasFano_RoundTripAndAccess()
    {
        var rnd = new Random(3);
        foreach (int n in new[] { 0, 1, 5, 100, 5000 })
        {
            var values = new ulong[n];
            ulong v = 0;
            for (int i = 0; i < n; i++) values[i] = v += (ulong)rnd.Next(0, 1000);
            ulong universe = n == 0 ? 0 : values[^1] + 1;
            EliasFano ef = EliasFano.BuildFrom(values, universe);
            for (int i = 0; i < n; i++) Assert.AreEqual(values[i], ef.Access((ulong)i));
            using var w = new PooledBufferWriter(256);
            ef.EncodeTo(w);
            Assert.AreEqual(ef.SerializedSize, (ulong)w.WrittenCount);
            int pos = 0;
            EliasFano back = EliasFano.Read(w.WrittenSpan, ref pos);
            Assert.AreEqual(w.WrittenCount, pos);
            Assert.AreEqual((ulong)n, back.Count);
            for (int i = 0; i < n; i++) Assert.AreEqual(values[i], back.Access((ulong)i));
        }
        byte[] lowBits = new byte[24];
        Coding.EncodeFixed64(lowBits.AsSpan(16), 64);
        int p = 0;
        StringAssert.Contains(Assert.ThrowsExactly<RocksDbException>(() => EliasFano.Read(lowBits, ref p)).Message, "low_bits exceeds 63");
    }

    // ---- LOUDS trie --------------------------------------------------------------------------------------------

    private static byte[] BuildTrieBytes(IReadOnlyList<byte[]> keys)
    {
        using var b = new LoudsTrieBuilder();
        for (int i = 0; i < keys.Count; i++) b.AddKey(keys[i], new TrieBlockHandle((ulong)i * 100, 50));
        b.Finish();
        return b.SerializedData.ToArray();
    }

    private static int LowerBound(IReadOnlyList<byte[]> keys, ReadOnlySpan<byte> target)
    {
        int lo = 0, hi = keys.Count;
        while (lo < hi)
        {
            int mid = (lo + hi) / 2;
            if (keys[mid].AsSpan().SequenceCompareTo(target) < 0) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    /// <summary>VerifyTrieIteration plus random-target seeks and reverse iteration against the sorted keys.</summary>
    private static void VerifyTrie(List<byte[]> keys, Random rnd, string ctx)
    {
        LoudsTrie trie = LoudsTrie.Read(BuildTrieBytes(keys));
        Assert.AreEqual((ulong)keys.Count, trie.NumKeys, ctx);
        var it = new LoudsTrieIterator(trie);
        for (int i = 0; i < keys.Count; i++)
        {
            Assert.IsTrue(it.Seek(keys[i]), ctx + " seek " + i);
            CollectionAssert.AreEqual(keys[i], it.Key.ToArray(), ctx);
            Assert.AreEqual((ulong)i * 100, it.Value.Offset, ctx);
        }
        Assert.IsTrue(it.SeekToFirst());
        for (int i = 0; i < keys.Count; i++)
        {
            Assert.IsTrue(it.Valid, ctx);
            CollectionAssert.AreEqual(keys[i], it.Key.ToArray(), ctx + " next " + i);
            Assert.AreEqual((ulong)i * 100, it.Value.Offset, ctx);
            Assert.AreEqual(i < keys.Count - 1, it.Next(), ctx);
        }
        Assert.IsFalse(it.Valid);
        Assert.IsTrue(it.SeekToLast());
        for (int i = keys.Count - 1; i >= 0; i--)
        {
            CollectionAssert.AreEqual(keys[i], it.Key.ToArray(), ctx + " prev " + i);
            Assert.AreEqual((ulong)i * 100, it.Value.Offset, ctx);
            Assert.AreEqual(i > 0, it.Prev(), ctx);
        }
        Assert.IsFalse(it.Seek([.. keys[^1], (byte)'z', (byte)'z', (byte)'z']), ctx);
        // Random targets: lower bound semantics, then Next/Prev from there.
        for (int t = 0; t < 200; t++)
        {
            byte[] target;
            if (t % 3 == 0)
            {
                byte[] k = keys[rnd.Next(keys.Count)];
                target = k.AsSpan(0, rnd.Next(k.Length + 1)).ToArray();
                if (rnd.Next(2) == 0) target = [.. target, (byte)rnd.Next(256)];
            }
            else
            {
                target = new byte[rnd.Next(0, 12)];
                rnd.NextBytes(target);
                if (keys[0].Length > 0 && rnd.Next(2) == 0) target[..Math.Min(target.Length, 1)].AsSpan().Fill(keys[rnd.Next(keys.Count)][0]);
            }
            int lb = LowerBound(keys, target);
            bool found = it.Seek(target);
            Assert.AreEqual(lb < keys.Count, found, ctx + " target " + Convert.ToHexString(target));
            if (!found) continue;
            CollectionAssert.AreEqual(keys[lb], it.Key.ToArray(), ctx + " target " + Convert.ToHexString(target));
            Assert.AreEqual((ulong)lb * 100, it.Value.Offset);
            if (rnd.Next(2) == 0)
            {
                Assert.AreEqual(lb + 1 < keys.Count, it.Next());
                if (lb + 1 < keys.Count) CollectionAssert.AreEqual(keys[lb + 1], it.Key.ToArray());
            }
            else
            {
                Assert.AreEqual(lb > 0, it.Prev());
                if (lb > 0) CollectionAssert.AreEqual(keys[lb - 1], it.Key.ToArray());
            }
        }
    }

    private static List<byte[]> SortedDistinct(IEnumerable<byte[]> keys)
    {
        var set = new SortedSet<byte[]>(Comparer<byte[]>.Create(static (a, b) => a.AsSpan().SequenceCompareTo(b)));
        foreach (byte[] k in keys) set.Add(k);
        return [.. set];
    }

    private static byte[] A(string s) => Encoding.Latin1.GetBytes(s);
    private static ulong Seq(byte[] internalKey) => DbFormat.ExtractFooter(internalKey) >> 8;
    private static EntryType TypeOf(byte[] internalKey) => (EntryType)(byte)DbFormat.ExtractFooter(internalKey);

    private static IEnumerable<(string Name, List<byte[]> Keys)> TopologyVariants()
    {
        static List<byte[]> L(params string[] s) => SortedDistinct(s.Select(A));
        static List<byte[]> G(int n, Func<int, string> f) => SortedDistinct(Enumerable.Range(0, n).Select(i => A(f(i))));
        yield return ("TwoKeys", L("aa", "ab"));
        yield return ("IdenticalPrefixDifferentLastByte", G(26, i => new string('x', 50) + (char)('a' + i)));
        yield return ("BinaryKeys", L("\0\0", "\0\u0001", "\0ÿ", "\u0001\0", "ÿ\0", "ÿÿ"));
        yield return ("SingleByteAllValues", G(256, i => ((char)i).ToString()));
        yield return ("DeepChain", L(new string('a', 100)));
        yield return ("AlternatingPrefixAndNonPrefix", L("a", "ab", "abc", "b", "c"));
        yield return ("PrefixKeyAtDenseSparseEdge", SortedDistinct(Enumerable.Range(0, 26).Select(i => A(((char)('a' + i)).ToString())).Append(A("aa")).Append(A("aab"))));
        yield return ("KeyReconstructionBasic", L("abc", "abd", "abe", "xyz"));
        yield return ("KeyReconstructionLongSharedPrefix", G(26, i => "common_prefix_that_is_quite_long_" + (char)('a' + i)));
        yield return ("KeyReconstructionVaryingLengths", L("a", "ab", "abc", "abcd", "abcde", "b", "bc", "bcd"));
        yield return ("HighFanoutRoot", G(26, i => (char)('a' + i) + "suffix"));
        yield return ("DeepTrie", L("abcdefghijklmnop", "abcdefghijklmnoq", "abcdefghijklmnor"));
        yield return ("PrefixKeysChain", L("a", "ab", "abc", "abcd"));
        yield return ("PrefixKeysMultipleBranches", L("a", "aa", "ab", "ac"));
        yield return ("PrefixKeysAtDifferentLevels", L("a", "aa", "aab", "b", "ba"));
        yield return ("PrefixKeysDiverging", G(30, i => "p_" + i));
        yield return ("MultiplePrefixKeySameNode", L("x", "xa", "xab", "xb"));
        yield return ("ManyPrefixKeys", G(20, i => new string('a', i + 1)));
        yield return ("AllSparse", L("a", "b"));
        yield return ("DenseSparseTransition", G(52, i => (char)('a' + i / 2) + (i % 2 == 0 ? "1" : "2")));
        yield return ("ThousandFormattedKeys", G(1000, i => "key_" + i.ToString("D6", System.Globalization.CultureInfo.InvariantCulture)));
        yield return ("DiversePrefixPatterns", G(250, i => new[] { "alpha", "beta", "gamma", "delta", "epsilon" }[i / 50] + "_" + (i % 50).ToString("D3", System.Globalization.CultureInfo.InvariantCulture)));
        yield return ("MixedLengths", SortedDistinct(Enumerable.Range(0, 26).Select(i => A(((char)('a' + i)).ToString()))
            .Concat(Enumerable.Range(0, 100).Select(i => A("medium_" + i.ToString("D4", System.Globalization.CultureInfo.InvariantCulture))))
            .Concat(Enumerable.Range(0, 50).Select(i => A("this_is_a_very_long_key_" + i.ToString("D6", System.Globalization.CultureInfo.InvariantCulture))))));
        yield return ("PrefixKeyStressPatterns", G(200, i => "item_" + i));
        // Long fanout-1 runs (ChainSeekVariousTargets' keys and a longer shared prefix): path-compression chains.
        yield return ("Chains", L("AAAAAAAAAMa", "AAAAAAAAAMb", "AAAAAAAAAMc", "AAAAAAAAANa", "AAAAAAAAANb"));
        yield return ("LongChains", SortedDistinct(Enumerable.Range(0, 100).Select(i => A(new string('X', 20) + i.ToString("D3", System.Globalization.CultureInfo.InvariantCulture)))
            .Append(A("B" + new string('q', 30))).Append(A("B" + new string('q', 12) + "r"))));
    }

    [TestMethod]
    public void LoudsTrie_TopologyVariants()
    {
        var rnd = new Random(17);
        foreach ((string name, List<byte[]> keys) in TopologyVariants()) VerifyTrie(keys, rnd, name);
    }

    [TestMethod]
    public void LoudsTrie_ChainsAreBuiltAndSeekThroughThem()
    {
        foreach (string name in new[] { "Chains", "LongChains" })
        {
        List<byte[]> keys = TopologyVariants().First(v => v.Name == name).Keys;
        LoudsTrie trie = LoudsTrie.Read(BuildTrieBytes(keys));
        Assert.IsTrue(trie.HasChains, name + ": fanout-1 runs of >= 8 labels become chains");
        // Targets diverging inside the chains in both directions, ending mid-chain, and running past them.
        var rnd = new Random(5);
        var it = new LoudsTrieIterator(trie);
        foreach (byte[] k in keys)
        {
            for (int cut = 0; cut <= k.Length; cut++)
            {
                foreach (int delta in new[] { -1, 0, 1 })
                {
                    byte[] target = k.AsSpan(0, cut).ToArray();
                    if (cut < k.Length && delta != 0) target = [.. target, (byte)(k[cut] + delta)];
                    int lb = LowerBound(keys, target);
                    Assert.AreEqual(lb < keys.Count, it.Seek(target));
                    if (lb < keys.Count) CollectionAssert.AreEqual(keys[lb], it.Key.ToArray(), Convert.ToHexString(target));
                }
            }
        }
        VerifyTrie(keys, rnd, name);
        }
    }

    [TestMethod]
    public void LoudsTrie_RandomizedStress()
    {
        int seed = Environment.TickCount;
        var rnd = new Random(seed);
        for (int trial = 0; trial < 60; trial++)
        {
            int n = 1 + rnd.Next(500);
            int maxLen = 1 + rnd.Next(30);
            int alphabet = rnd.Next(3) switch { 0 => 2, 1 => 16, _ => 256 };
            var raw = new List<byte[]>();
            for (int i = 0; i < n; i++)
            {
                var k = new byte[1 + rnd.Next(maxLen)];
                for (int j = 0; j < k.Length; j++) k[j] = (byte)(alphabet == 256 ? rnd.Next(256) : 'a' + rnd.Next(alphabet));
                raw.Add(k);
            }
            VerifyTrie(SortedDistinct(raw), rnd, "seed=" + seed + " trial=" + trial);
        }
    }

    [TestMethod]
    public void LoudsTrie_EmptyAndSingleKey()
    {
        LoudsTrie empty = LoudsTrie.Read(BuildTrieBytes([]));
        Assert.AreEqual(0UL, empty.NumKeys);
        var it = new LoudsTrieIterator(empty);
        Assert.IsFalse(it.SeekToFirst());
        Assert.IsFalse(it.SeekToLast());
        Assert.IsFalse(it.Seek(A("a")));
        Assert.IsFalse(it.Next());
        Assert.IsFalse(it.Prev());
        VerifyTrie([A("only")], new Random(1), "single");
    }

    [TestMethod]
    public void LoudsTrie_InitFromDataCorruption()
    {
        var keys = new List<byte[]>();
        for (int i = 0; i < 300; i++) keys.Add(A("k" + (i * 37 % 1000).ToString("D4", System.Globalization.CultureInfo.InvariantCulture) + "_0000000000_" + (i % 5)));
        foreach (bool seqno in new[] { false, true })
        {
            List<byte[]> sorted = SortedDistinct(keys);
            byte[] good;
            using (var b = new LoudsTrieBuilder())
            {
                b.SetHasSeqnoEncoding(seqno);
                for (int i = 0; i < sorted.Count; i++)
                {
                    uint count = seqno && i % 10 == 0 ? 3u : 1u;
                    b.AddKeyWithSeqno(sorted[i], new TrieBlockHandle((ulong)i * 100, 50), (ulong)i, count);
                    for (uint j = 1; j < count; j++) b.AddOverflowBlock(new TrieBlockHandle(999, 1), 1);
                }
                b.Finish();
                good = b.SerializedData.ToArray();
            }
            LoudsTrie ok = LoudsTrie.Read(good);
            Assert.AreEqual(seqno, ok.HasSeqnoEncoding);
            // Progressive truncation: Corruption, never another exception.
            for (int len = 0; len < good.Length; len++)
            {
                try
                {
                    LoudsTrie.Read(good.AsSpan(0, len));
                    // A prefix can only parse when it stops right where an optional section ends.
                }
                catch (RocksDbException e)
                {
                    Assert.AreEqual(StatusCode.Corruption, e.Code, "len " + len);
                }
            }
        }
        byte[] data = BuildTrieBytes([A("a"), A("b")]);
        byte[] bad = (byte[])data.Clone();
        bad[0] ^= 1;
        StringAssert.Contains(Assert.ThrowsExactly<RocksDbException>(() => LoudsTrie.Read(bad)).Message, "bad magic number");
        bad = (byte[])data.Clone();
        Coding.EncodeFixed32(bad.AsSpan(4), 2);
        StringAssert.Contains(Assert.ThrowsExactly<RocksDbException>(() => LoudsTrie.Read(bad)).Message, "unsupported format version");
        bad = (byte[])data.Clone();
        Coding.EncodeFixed32(bad.AsSpan(20), 70000);
        StringAssert.Contains(Assert.ThrowsExactly<RocksDbException>(() => LoudsTrie.Read(bad)).Message, "max_depth exceeds reasonable limit");
        bad = (byte[])data.Clone();
        Coding.EncodeFixed64(bad.AsSpan(8), (1UL << 30) + 1);
        StringAssert.Contains(Assert.ThrowsExactly<RocksDbException>(() => LoudsTrie.Read(bad)).Message, "num_keys exceeds reasonable limit");
    }

    [TestMethod]
    public void LoudsTrie_SerializedLayoutHeader()
    {
        List<byte[]> keys = SortedDistinct(Enumerable.Range(0, 256).Select(i => new[] { (byte)i }));
        byte[] data = BuildTrieBytes(keys);
        Assert.AreEqual(0x54524945u, Coding.DecodeFixed32(data));
        Assert.AreEqual(1u, Coding.DecodeFixed32(data.AsSpan(4)));
        Assert.AreEqual(256UL, Coding.DecodeFixed64(data.AsSpan(8)));
        Assert.AreEqual(1u, Coding.DecodeFixed32(data.AsSpan(16)), "256 single-byte keys: the root level is dense");
        Assert.AreEqual(1u, Coding.DecodeFixed32(data.AsSpan(20)));
        Assert.AreEqual(256UL, Coding.DecodeFixed64(data.AsSpan(24)), "dense leaves");
        Assert.AreEqual(1UL, Coding.DecodeFixed64(data.AsSpan(32)), "dense nodes");
        Assert.AreEqual(0, data.Length % 8);
        // Dense labels: one 256-bit node, all labels set.
        Assert.AreEqual(256UL, Coding.DecodeFixed64(data.AsSpan(56)));
        Assert.AreEqual(256UL, Coding.DecodeFixed64(data.AsSpan(64)));
        for (int w = 0; w < 4; w++) Assert.AreEqual(ulong.MaxValue, Coding.DecodeFixed64(data.AsSpan(72 + 8 * w)));
        // 26 single-byte keys are cheaper sparse: cutoff level 0.
        byte[] sparse = BuildTrieBytes(SortedDistinct(Enumerable.Range(0, 26).Select(i => A(((char)('a' + i)).ToString()))));
        Assert.AreEqual(0u, Coding.DecodeFixed32(sparse.AsSpan(16)));
        Assert.AreEqual(1UL, Coding.DecodeFixed64(sparse.AsSpan(40)), "dense_child_count is 1 (the root) when all levels are sparse");
    }

    // ---- TrieIndexFactory: builder / iterator with seqno side-table ------------------------------------------------

    private static UserDefinedIndexReader BuildIndex(IReadOnlyList<(string LastKey, string? NextKey, ulong LastTag)> blocks)
    {
        using UserDefinedIndexBuilder b = TrieIndexFactory.Instance.NewBuilder(new UserDefinedIndexOptions());
        for (int i = 0; i < blocks.Count; i++)
        {
            (string last, string? next, ulong tag) = blocks[i];
            b.AddIndexEntry(A(last), next is null ? default : A(next), next is not null, new BlockHandle((ulong)i * 1000, 900),
                new UserDefinedIndexEntryContext(tag, 0, false));
        }
        return TrieIndexFactory.Instance.NewReader(new UserDefinedIndexOptions(), b.Finish().ToArray());
    }

    [TestMethod]
    public void Factory_SameUserKeyRunsUseOverflowBlocksAndSeqnoCorrection()
    {
        static ulong Tag(ulong seq) => DbFormat.PackSequenceAndType(seq, EntryType.Value);
        // Blocks: [..b] [b@90..b@50] [b@40..b@10] [c..d] ; "b" spans three blocks.
        var blocks = new List<(string, string?, ulong)>
        {
            ("a5", "b", Tag(100)),
            ("b", "b", Tag(50)),
            ("b", "b", Tag(10)),
            ("b", "c", Tag(5)),
            ("d", null, Tag(3)),
        };
        using UserDefinedIndexReader r = BuildIndex(blocks);
        using UserDefinedIndexIterator it = r.NewIterator(null);
        // Seeks for "b" at decreasing snapshots land in the matching block of the run.
        (ulong Seq, ulong Offset)[] cases = [(DbFormat.MaxSequenceNumber, 1000), (60, 1000), (50, 1000), (49, 2000), (10, 2000), (9, 3000), (5, 3000), (4, 4000), (0, 4000)];
        foreach ((ulong seq, ulong off) in cases)
        {
            Assert.IsTrue(it.Seek(A("b"), DbFormat.PackSequenceAndType(seq, DbFormat.ValueTypeForSeek)), "seq " + seq);
            Assert.AreEqual(off, it.Value.Offset, "seq " + seq);
            CollectionAssert.AreEqual(off == 4000 ? A("d") : A("b"), it.Key.ToArray());
        }
        // Next walks the whole run, then the following leaf.
        Assert.IsTrue(it.Seek(A("b"), Tag(DbFormat.MaxSequenceNumber)));
        var offsets = new List<ulong> { it.Value.Offset };
        while (it.Next()) offsets.Add(it.Value.Offset);
        CollectionAssert.AreEqual(new ulong[] { 1000, 2000, 3000, 4000 }, offsets);
        // Reverse iteration visits overflow blocks too.
        Assert.IsTrue(it.SeekToLast());
        offsets.Clear();
        do offsets.Add(it.Value.Offset); while (it.Prev());
        CollectionAssert.AreEqual(new ulong[] { 4000, 3000, 2000, 1000, 0 }, offsets);
        // Last block: a seek for its key at a snapshot below the last key's tag is past the end.
        Assert.IsTrue(it.Seek(A("d"), Tag(3)));
        Assert.IsFalse(it.Seek(A("d"), Tag(2)));
        Assert.IsFalse(it.Seek(A("e"), Tag(DbFormat.MaxSequenceNumber)));
        // A distinct-user-key separator ignores the tag (0 sentinel).
        Assert.IsTrue(it.Seek(A("a5"), 0));
        Assert.AreEqual(0UL, it.Value.Offset);
    }

    [TestMethod]
    public void Factory_ErrorsAndEstimatedSize()
    {
        Assert.AreEqual("trie_index", TrieIndexFactory.Instance.Name);
        Assert.AreEqual(StatusCode.NotSupported, Assert.ThrowsExactly<RocksDbException>(
            () => TrieIndexFactory.Instance.NewBuilder(new UserDefinedIndexOptions { Comparator = Comparator.ReverseBytewise })).Code);
        Assert.AreEqual(StatusCode.NotSupported, Assert.ThrowsExactly<RocksDbException>(
            () => TrieIndexFactory.Instance.NewReader(new UserDefinedIndexOptions { Comparator = Comparator.ReverseBytewise }, new byte[64])).Code);
        Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(
            () => TrieIndexFactory.Instance.NewReader(new UserDefinedIndexOptions(), new byte[10])).Code);
        using (UserDefinedIndexBuilder b = TrieIndexFactory.Instance.NewBuilder(new UserDefinedIndexOptions()))
        {
            var rnd = new Random(2);
            string prev = "";
            for (int i = 0; i < 2000; i++)
            {
                string last = "user" + (i * 13).ToString("D8", System.Globalization.CultureInfo.InvariantCulture) + new string('z', rnd.Next(20));
                string next = "user" + (i * 13 + 7).ToString("D8", System.Globalization.CultureInfo.InvariantCulture);
                Assert.IsTrue(string.CompareOrdinal(prev, last) < 0);
                b.AddIndexEntry(A(last), A(next), true, new BlockHandle((ulong)i * 4096, 4000), default);
                prev = next;
            }
            ulong estimate = b.EstimatedSize;
            int actual = b.Finish().Length;
            Assert.IsTrue((ulong)actual <= estimate, actual + " > " + estimate);
            ulong finishedSize = (ulong)actual;
            Assert.AreEqual(finishedSize, b.EstimatedSize);
            Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => b.Finish()).Code);
        }
        using (UserDefinedIndexBuilder b = TrieIndexFactory.Instance.NewBuilder(new UserDefinedIndexOptions()))
        {
            b.AddIndexEntry(A("a"), default, false, new BlockHandle((ulong)uint.MaxValue + 1, 10), default);
            Assert.AreEqual(StatusCode.NotSupported, Assert.ThrowsExactly<RocksDbException>(() => b.Finish()).Code);
        }
        using (UserDefinedIndexBuilder b = TrieIndexFactory.Instance.NewBuilder(new UserDefinedIndexOptions()))
        {
            // An empty index still serializes a parseable trie.
            using UserDefinedIndexReader r = TrieIndexFactory.Instance.NewReader(new UserDefinedIndexOptions(), b.Finish().ToArray());
            using UserDefinedIndexIterator it = r.NewIterator(null);
            Assert.IsFalse(it.SeekToFirst());
            Assert.IsFalse(it.Seek(A("x"), 0));
        }
    }

    // ---- block-based tables --------------------------------------------------------------------------------------

    /// <summary>Entries with many versions per user key so that a user key spans several 256-byte blocks.</summary>
    private static List<(byte[] Key, byte[] Value)> VersionedEntries(Random rnd, int users)
    {
        var list = new List<(byte[], byte[])>();
        ulong seq = 1_000_000;
        for (int u = 0; u < users; u++)
        {
            byte[] uk = A("user" + (u * 3).ToString("D5", System.Globalization.CultureInfo.InvariantCulture) + (u % 4 == 0 ? new string('p', 30) : ""));
            int versions = u % 9 == 0 ? 40 : 1 + rnd.Next(3);
            for (int v = 0; v < versions; v++)
            {
                byte[] ik = new byte[uk.Length + 8];
                EntryType t = v % 7 == 6 ? EntryType.Deletion : EntryType.Value;
                DbFormat.AppendInternalKey(ik, uk, seq--, t);
                list.Add((ik, t == EntryType.Deletion ? [] : A("v" + u + "_" + v + new string('x', rnd.Next(30)))));
            }
            seq -= (ulong)rnd.Next(5);
        }
        return list;
    }

    private static byte[] BuildTable(List<(byte[] Key, byte[] Value)> entries, BlockBasedIndexMode mode, bool zeroSeqnos = false)
    {
        var f = new MemWritableFile();
        var format = new TableFormatOptions { BlockSize = 256, FormatVersion = 7, UserDefinedIndexFactory = TrieIndexFactory.Instance, IndexMode = mode };
        using (var b = new BlockBasedTableBuilder(f, format, new TableBuilderOptions()))
        {
            foreach ((byte[] k, byte[] v) in entries)
            {
                if (!zeroSeqnos)
                {
                    b.Add(k, v);
                    continue;
                }
                byte[] z = (byte[])k.Clone();
                Coding.EncodeFixed64(z.AsSpan(z.Length - 8), DbFormat.PackSequenceAndType(0, TypeOf(k)));
                b.Add(z, v);
            }
            b.Finish();
        }
        return f.ToArray();
    }

    private static BlockBasedTableReader OpenTable(byte[] file, BlockBasedIndexMode mode) =>
        BlockBasedTableReader.Open(new MemRandomAccessFile(file), new TableReaderOptions { UserDefinedIndexFactory = TrieIndexFactory.Instance, IndexMode = mode });

    /// <summary>Every lookup (user key x snapshot) and seek gives the same answer through the trie and the standard index.</summary>
    private static void CompareWithStandardIndex(BlockBasedTableReader reader, List<(byte[] Key, byte[] Value)> entries, Random rnd, bool hasStandard)
    {
        var custom = new ArrayBufferWriter<byte>();
        var std = new ArrayBufferWriter<byte>();
        var userKeys = new List<byte[]>();
        foreach ((byte[] k, _) in entries)
        {
            byte[] uk = k[..^8];
            if (userKeys.Count == 0 || !userKeys[^1].AsSpan().SequenceEqual(uk)) userKeys.Add(uk);
        }
        foreach (byte[] uk in userKeys)
        {
            foreach (ulong snap in new[] { DbFormat.MaxSequenceNumber, (ulong)rnd.Next(990_000, 1_000_001), 0UL })
            {
                byte[] lk = new byte[uk.Length + 8];
                DbFormat.AppendInternalKey(lk, uk, snap, DbFormat.ValueTypeForSeek);
                custom.Clear();
                TableGetResult rc = reader.Get(lk, custom, TableReadFlags.PreferCustomIndex);
                // Oracle: the newest entry of uk with seq <= snap.
                int idx = entries.FindIndex(e => e.Key.AsSpan(0, e.Key.Length - 8).SequenceEqual(uk) && Seq(e.Key) <= snap);
                Assert.AreEqual(idx >= 0, rc.Found, Encoding.Latin1.GetString(uk) + "@" + snap);
                if (idx >= 0 && rc.Found && TypeOf(entries[idx].Key) == EntryType.Value)
                    CollectionAssert.AreEqual(entries[idx].Value, custom.WrittenSpan.ToArray());
                if (hasStandard)
                {
                    std.Clear();
                    TableGetResult rs = reader.Get(lk, std, TableReadFlags.BuiltinIndex);
                    Assert.AreEqual(rs.Found, rc.Found);
                    CollectionAssert.AreEqual(std.WrittenSpan.ToArray(), custom.WrittenSpan.ToArray());
                }
            }
        }
        using BlockBasedTableIterator it = reader.NewIterator(TableReadFlags.PreferCustomIndex);
        int n = 0;
        for (it.SeekToFirst(); it.Valid; it.Next()) CollectionAssert.AreEqual(entries[n++].Key, it.Key.ToArray());
        Assert.AreEqual(entries.Count, n);
        n = entries.Count;
        for (it.SeekToLast(); it.Valid; it.Prev()) CollectionAssert.AreEqual(entries[--n].Key, it.Key.ToArray());
        Assert.AreEqual(0, n);
        // Seeks to every internal key position.
        for (int i = 0; i < entries.Count; i += 1 + rnd.Next(3))
        {
            it.Seek(entries[i].Key);
            Assert.IsTrue(it.Valid);
            CollectionAssert.AreEqual(entries[i].Key, it.Key.ToArray());
        }
    }

    [TestMethod]
    public void Table_AllModes_TrieMatchesStandardIndex()
    {
        int seed = Environment.TickCount;
        var rnd = new Random(seed);
        List<(byte[] Key, byte[] Value)> entries = VersionedEntries(rnd, 600);
        foreach (BlockBasedIndexMode mode in new[] { BlockBasedIndexMode.StandardDefault, BlockBasedIndexMode.CustomDefault, BlockBasedIndexMode.CustomOnly })
        {
            byte[] file = BuildTable(entries, mode);
            using BlockBasedTableReader reader = OpenTable(file, mode);
            Assert.IsInstanceOfType<TrieIndexReader>(reader.UserDefinedIndex);
            Assert.IsTrue(((TrieIndexReader)reader.UserDefinedIndex!).Trie.NumOverflowBlocks > 0, "same-user-key runs span blocks");
            CompareWithStandardIndex(reader, entries, rnd, mode != BlockBasedIndexMode.CustomOnly);
        }
    }

    [TestMethod]
    public void Table_ZeroSequenceNumbers()
    {
        // Bottommost-compaction-like files: all seqnos 0, same-user-key runs with tag-0 overflow blocks.
        var rnd = new Random(8);
        List<(byte[] Key, byte[] Value)> entries = VersionedEntries(rnd, 200);
        var dedup = new List<(byte[], byte[])>();
        foreach ((byte[] k, byte[] v) in entries)
            if (dedup.Count == 0 || !dedup[^1].Item1.AsSpan(0, dedup[^1].Item1.Length - 8).SequenceEqual(k.AsSpan(0, k.Length - 8))) dedup.Add((k, v));
        byte[] file = BuildTable(dedup, BlockBasedIndexMode.CustomDefault, zeroSeqnos: true);
        using BlockBasedTableReader reader = OpenTable(file, BlockBasedIndexMode.CustomDefault);
        var buf = new ArrayBufferWriter<byte>();
        foreach ((byte[] k, byte[] v) in dedup)
        {
            byte[] lk = new byte[k.Length];
            DbFormat.AppendInternalKey(lk, k.AsSpan(0, k.Length - 8), DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
            buf.Clear();
            Assert.IsTrue(reader.Get(lk, buf, TableReadFlags.None).Found);
            if (TypeOf(k) == EntryType.Value) CollectionAssert.AreEqual(v, buf.WrittenSpan.ToArray());
        }
    }

    [TestMethod]
    public void Table_TrieLookupsDoNotAllocate()
    {
        List<(byte[] Key, byte[] Value)> entries = VersionedEntries(new Random(4), 400);
        byte[] file = BuildTable(entries, BlockBasedIndexMode.CustomOnly);
        using BlockBasedTableReader reader = BlockBasedTableReader.Open(new MemRandomAccessFile(file), new TableReaderOptions
        {
            UserDefinedIndexFactory = TrieIndexFactory.Instance, IndexMode = BlockBasedIndexMode.CustomOnly,
            BlockCache = RocksDb.Managed.Cache.BlockCache.CreateLru(1 << 20),
        });
        byte[] buffer = new byte[256];
        var lookups = new List<byte[]>();
        foreach ((byte[] k, _) in entries)
        {
            byte[] lk = (byte[])k.Clone();
            Coding.EncodeFixed64(lk.AsSpan(lk.Length - 8), DbFormat.PackSequenceAndType(DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek));
            lookups.Add(lk);
        }
        var sink = new FixedBufferWriter(buffer);
        for (int round = 0; round < 3; round++) foreach (byte[] lk in lookups) { sink.Reset(); reader.Get(lk, sink, TableReadFlags.None); }
        long before = GC.GetAllocatedBytesForCurrentThread();
        foreach (byte[] lk in lookups) { sink.Reset(); reader.Get(lk, sink, TableReadFlags.None); }
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.AreEqual(0L, allocated, "bytes allocated by " + lookups.Count + " trie-index Gets");
    }

    private sealed class FixedBufferWriter(byte[] buffer) : IBufferWriter<byte>
    {
        private int _written;
        public void Reset() => _written = 0;
        public void Advance(int count) => _written += count;
        public Memory<byte> GetMemory(int sizeHint = 0) => buffer.AsMemory(_written);
        public Span<byte> GetSpan(int sizeHint = 0) => buffer.AsSpan(_written);
    }

    // ---- DB ------------------------------------------------------------------------------------------------------

    [TestMethod]
    public void Db_TrieIndex_FlushCompactReopenWithSnapshots()
    {
        var fs = DbTestUtil.NewFs();
        ColumnFamilyOptions Cf(BlockBasedIndexMode mode)
        {
            var cf = new ColumnFamilyOptions { WriteBufferSize = 64 * 1024 };
            cf.TableOptions.BlockSize = 512;
            cf.TableOptions.UserDefinedIndexFactory = TrieIndexFactory.Instance;
            cf.TableOptions.IndexMode = mode;
            return cf;
        }
        var rnd = new Random(23);
        var oracle = new SortedDictionary<string, string>(StringComparer.Ordinal);
        BlockBasedIndexMode[] modes = [BlockBasedIndexMode.CustomOnly, BlockBasedIndexMode.CustomDefault, BlockBasedIndexMode.CustomOnly];
        for (int round = 0; round < modes.Length; round++)
        {
            using RocksDatabase db = RocksDatabase.Open(DbTestUtil.DbPath, DbTestUtil.NewOptions(fs), Cf(modes[round]));
            for (int i = 0; i < 4000; i++)
            {
                string k = "k" + rnd.Next(2500).ToString("D5", System.Globalization.CultureInfo.InvariantCulture);
                if (rnd.Next(6) == 0)
                {
                    db.Delete(DbTestUtil.B(k));
                    oracle.Remove(k);
                }
                else
                {
                    string v = "v" + rnd.Next() + new string('w', rnd.Next(40));
                    db.Put(DbTestUtil.B(k), DbTestUtil.B(v));
                    oracle[k] = v;
                }
            }
            var snapOracle = new SortedDictionary<string, string>(oracle, StringComparer.Ordinal);
            using Snapshot snap = db.GetSnapshot();
            // Overwrite a hot key many times so it spans blocks.
            for (int i = 0; i < 300; i++) db.Put(DbTestUtil.B("k00007"), DbTestUtil.B("hot" + i));
            oracle["k00007"] = "hot299";
            db.Flush();
            if (round == 1) db.CompactRange();
            foreach (KeyValuePair<string, string> kv in oracle) Assert.AreEqual(kv.Value, DbTestUtil.GetString(db, kv.Key));
            var ro = new ReadOptions { Snapshot = snap };
            using (Iterator sit = db.NewIterator(ro))
            {
                var got = new List<string>();
                for (sit.SeekToFirst(); sit.Valid; sit.Next()) got.Add(Encoding.ASCII.GetString(sit.Key) + "=" + Encoding.ASCII.GetString(sit.Value));
                var want = new List<string>();
                foreach (KeyValuePair<string, string> kv in snapOracle) want.Add(kv.Key + "=" + kv.Value);
                CollectionAssert.AreEqual(want, got);
            }
            using Iterator it = db.NewIterator();
            var keys = new List<string>();
            for (it.SeekToLast(); it.Valid; it.Prev()) keys.Add(Encoding.ASCII.GetString(it.Key));
            keys.Reverse();
            CollectionAssert.AreEqual(new List<string>(oracle.Keys), keys);
        }
    }
}
