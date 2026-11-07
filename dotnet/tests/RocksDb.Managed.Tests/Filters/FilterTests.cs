using System.Text;
using RocksDb.Managed.Table.Filters;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Tests.Filters;

/// <summary>
/// Ports of util/bloom_test.cc FullBloomTest (Schema, RawSchema, FullEmptyFilter, FullSmall,
/// FullVaryingLengths, CorruptFilters subset) for the three built-in full filter implementations.
/// </summary>
[TestClass]
public sealed class FilterTests
{
    public enum Impl { LegacyBloom, FastLocalBloom, Standard128Ribbon }

    private static IFilterBitsBuilder NewBuilder(Impl impl, double bitsPerKey) => impl switch
    {
        Impl.LegacyBloom => new BloomFilterPolicy(bitsPerKey).CreateBuilder(4),
        Impl.FastLocalBloom => new BloomFilterPolicy(bitsPerKey).CreateBuilder(5),
        _ => new RibbonFilterPolicy(bitsPerKey, -1).CreateBuilder(5),
    };

    private static byte[] Key(int i)
    {
        byte[] b = new byte[4];
        Coding.EncodeFixed32(b, (uint)i);
        return b;
    }

    private static byte[] Build(Impl impl, double bitsPerKey, int from, int to)
    {
        using IFilterBitsBuilder b = NewBuilder(impl, bitsPerKey);
        for (int k = from; k < to; k++) b.AddKey(Key(k));
        using var w = new PooledBufferWriter();
        b.Finish(w);
        return w.WrittenSpan.ToArray();
    }

    private static string FirstFPs(IFilterBitsReader r, int count)
    {
        var rv = new StringBuilder();
        int fpCount = 0;
        for (int i = 0; i < 1000000; i++)
        {
            if (r.MayMatch(Key(i + 1000000)))
            {
                ++fpCount;
                rv.Append(i);
                if (fpCount == count) break;
                rv.Append(',');
            }
        }
        return rv.ToString();
    }

    private static ulong PackedMatches(IFilterBitsReader r)
    {
        ulong result = 0;
        for (int i = 0; i < 64; i++)
        {
            if (r.MayMatch(Key(i + 12345))) result |= 1UL << i;
        }
        return result;
    }

    private static int NumProbes(byte[] f) => unchecked((sbyte)f[^5]) == -1 ? f[^3] : unchecked((sbyte)f[^5]);

    private static int RibbonSeed(byte[] f)
    {
        Assert.AreEqual(-2, unchecked((sbyte)f[^5]));
        return f[^4];
    }

    [TestMethod]
    public void SchemaSmallFallsBackToBloom()
    {
        foreach (Impl impl in new[] { Impl.FastLocalBloom, Impl.Standard128Ribbon })
        {
            byte[] f = Build(impl, 5, 0, 87);
            Assert.AreEqual(3, NumProbes(f));
            Assert.AreEqual(4130687756U, Hash.BloomHash(f), impl.ToString());
            Assert.AreEqual("31,38,40,43,61,83,86,112,125,131", FirstFPs(BuiltinFilterBitsReader.Create(f), 10));
        }
        Assert.AreEqual(3, NumProbes(Build(Impl.LegacyBloom, 5, 0, 87)));
    }

    // (bits/key, first key, end key, legacy probes, fast probes, ribbon seed, legacy hash, fast hash, ribbon hash,
    //  fast FPs, ribbon FPs) -- from FullBloomTest.Schema (CACHE_LINE_SIZE == 64 for legacy).
    private static readonly (double Bpk, int From, int To, int LegacyProbes, int FastProbes, int RibbonSeed,
        uint LegacyHash, uint FastHash, uint RibbonHash, string FastFps, string RibbonFps)[] SchemaCases =
    [
        (2, 0, 2087, 1, 1, 61, 1567096579, 3817481309U, 1705851228U, "11,13,17,25,29,30,35,37,45,53", "3,8,10,17,19,20,23,28,31,32"),
        (3, 0, 2087, 2, 2, 61, 2707206547U, 2807269961U, 1095342358U, "4,15,17,24,27,28,29,53,63,70", "3,17,20,28,32,33,36,43,49,54"),
        (5, 0, 2087, 3, 3, 61, 515748486, 204628445U, 3971337699U, "15,24,29,39,53,87,89,100,103,104", "3,33,36,43,67,70,76,78,84,102"),
        (8, 0, 2087, 5, 5, 61, 1302145999, 355564975U, 3651449053U, "16,60,66,126,220,238,244,256,265,287", "33,187,203,296,300,322,411,419,547,582"),
        (9, 0, 2087, 6, 6, 61, 2092755149, 2137566013U, 1005676675U, "156,367,791,872,945,1015,1139,1159,1265", "33,187,203,296,411,419,604,612,615,619"),
        (11, 0, 2087, 7, 7, 61, 3755609649U, 2561502687U, 3129900846U, "34,74,130,236,643,882,962,1015,1035,1110", "411,419,623,665,727,794,955,1052,1323,1330"),
        (14, 0, 2087, 9, 8, 61, 178861123, 3709876890U, 1855638875U, "130,240,522,565,989,2002,2526,3147,3543", "665,727,1323,1755,3866,4232,4442,4492,4736"),
        (16, 0, 2087, 11, 9, 61, 1129406313, 1087138490U, 459379967U, "3299,3611,3916,6620,7822,8079,8482,8942", "727,1323,1755,4442,4736,5386,6974,7154,8222"),
        (10, 0, 2087, 6, 6, 61, 1478976371, 2498541272U, 1273231667U, "16,126,133,422,466,472,813,1002,1035", "296,411,419,612,619,623,630,665,686,727"),
        (10, 1, 2087, 6, 6, 184, 4205696321U, 2058382345U, 3007790572U, "16,126,133,422,466,472,813,1002,1035", "33,152,383,497,589,633,737,781,911,990"),
        (10, 1, 2088, 6, 6, 184, 2885052954U, 23699164U, 1942323379U, "16,126,133,422,466,472,813,1002,1035", "33,95,360,589,737,911,990,1048,1081,1414"),
        (9.5, 1, 2088, 6, 6, 184, 2885052954U, 3166884174U, 1148258663U, "126,156,367,444,458,791,813,976,1015", "33,54,95,360,589,693,737,911,990,1048"),
        (10.499, 1, 2088, 6, 7, 184, 2885052954U, 4098502778U, 792138188U, "16,236,240,472,1015,1045,1111,1409,1465", "33,95,360,589,737,990,1048,1081,1414,1643"),
    ];

    [TestMethod]
    public void Schema()
    {
        foreach (var c in SchemaCases)
        {
            string ctx = "bpk=" + c.Bpk + " keys=" + c.From + ".." + c.To;

            byte[] legacy = Build(Impl.LegacyBloom, c.Bpk, c.From, c.To);
            Assert.AreEqual(c.LegacyProbes, NumProbes(legacy), ctx);
            Assert.AreEqual(c.LegacyHash, Hash.BloomHash(legacy), ctx);

            byte[] fast = Build(Impl.FastLocalBloom, c.Bpk, c.From, c.To);
            Assert.AreEqual(c.FastProbes, NumProbes(fast), ctx);
            Assert.AreEqual(c.FastHash, Hash.BloomHash(fast), ctx);
            int fastCount = c.FastFps.Split(',').Length;
            Assert.AreEqual(c.FastFps, FirstFPs(BuiltinFilterBitsReader.Create(fast), fastCount), ctx);

            byte[] ribbon = Build(Impl.Standard128Ribbon, c.Bpk, c.From, c.To);
            Assert.AreEqual(c.RibbonSeed, RibbonSeed(ribbon), ctx);
            Assert.AreEqual(c.RibbonHash, Hash.BloomHash(ribbon), ctx);
            int ribbonCount = c.RibbonFps.Split(',').Length;
            Assert.AreEqual(c.RibbonFps, FirstFPs(BuiltinFilterBitsReader.Create(ribbon), ribbonCount), ctx);
        }
    }

    /// <summary>RawFilterTester::ResetWeirdFill: metadata is the last 5 bytes of a 3000-byte buffer.</summary>
    private static byte[] WeirdFill(int lenWithoutMetadata, uint numLines, uint numProbes)
    {
        byte[] data = new byte[3000];
        for (int i = 0; i < data.Length; i++) data[i] = unchecked((byte)(0x7b7b >> (i % 7)));
        int meta = data.Length - 5;
        data[meta] = (byte)numProbes;
        Coding.EncodeFixed32(data.AsSpan(meta + 1), numLines);
        int len = lenWithoutMetadata + 5;
        return data.AsSpan(meta - lenWithoutMetadata, len).ToArray();
    }

    [TestMethod]
    public void RawSchema()
    {
        // Legacy Bloom: one 256-byte line, two 128-byte lines, four 64-byte lines
        Assert.AreEqual(11384799501900898790UL, PackedMatches(BuiltinFilterBitsReader.Create(WeirdFill(256, 1, 2))));
        Assert.AreEqual(10157853359773492589UL, PackedMatches(BuiltinFilterBitsReader.Create(WeirdFill(256, 2, 2))));
        Assert.AreEqual(7123594913907464682UL, PackedMatches(BuiltinFilterBitsReader.Create(WeirdFill(256, 4, 2))));
        // Fast local Bloom (marker 255 -> -1)
        Assert.AreEqual(9957045189927952471UL, PackedMatches(BuiltinFilterBitsReader.Create(WeirdFill(256, 2U << 8, 255))));
        // Ribbon (marker 254 -> -2)
        Assert.AreEqual(6193930559317665002UL, PackedMatches(BuiltinFilterBitsReader.Create(WeirdFill(32, 2U << 8, 254))));
        Assert.AreEqual(9007200345328128UL, PackedMatches(BuiltinFilterBitsReader.Create(WeirdFill(112, 2U << 8, 254))));
    }

    [TestMethod]
    public void EmptyFilterMatchesNothing()
    {
        foreach (Impl impl in Enum.GetValues<Impl>())
        {
            byte[] f = Build(impl, 10, 0, 0);
            Assert.AreEqual(0, f.Length);
            IFilterBitsReader r = BuiltinFilterBitsReader.Create(f);
            Assert.IsFalse(r.MayMatch("hello"u8));
            Assert.IsFalse(r.MayMatch("world"u8));
        }
    }

    [TestMethod]
    public void SmallFilter()
    {
        foreach (Impl impl in Enum.GetValues<Impl>())
        {
            using IFilterBitsBuilder b = NewBuilder(impl, 10);
            b.AddKey("hello"u8);
            b.AddKey("world"u8);
            using var w = new PooledBufferWriter();
            b.Finish(w);
            IFilterBitsReader r = BuiltinFilterBitsReader.Create(w.WrittenMemory.ToArray());
            Assert.IsTrue(r.MayMatch("hello"u8));
            Assert.IsTrue(r.MayMatch("world"u8));
            Assert.IsFalse(r.MayMatch("x"u8));
            Assert.IsFalse(r.MayMatch("foo"u8));
        }
    }

    private static int NextLength(int length) => length < 10 ? length + 1 : length < 100 ? length + 10 : length < 1000 ? length + 100 : length + 1000;

    /// <summary>FullVaryingLengths: no false negatives, bounded FP rate, expected sizes.</summary>
    [TestMethod]
    public void VaryingLengths()
    {
        foreach (Impl impl in Enum.GetValues<Impl>())
        {
            int mediocre = 0, good = 0;
            for (int length = 1; length <= 10000; length = NextLength(length))
            {
                using IFilterBitsBuilder b = NewBuilder(impl, 10);
                for (int i = 0; i < length; i++) b.AddKey(Key(i));
                Assert.AreEqual(length, b.EstimatedEntries);
                using var w = new PooledBufferWriter();
                b.Finish(w);
                byte[] f = w.WrittenSpan.ToArray();
                // upstream: filter size <= length * bits/key/8 + 128 (+ cache line rounding)
                Assert.IsTrue(f.Length <= (length * 10 / 8) + 128 + 64 + 5, impl + " len " + length + " size " + f.Length);
                IFilterBitsReader r = BuiltinFilterBitsReader.Create(f);
                for (int i = 0; i < length; i++)
                {
                    Assert.IsTrue(r.MayMatch(Key(i)), impl + " length " + length + " key " + i);
                }
                int fp = 0;
                for (int i = 0; i < 10000; i++) if (r.MayMatch(Key(i + 1000000000))) fp++;
                double rate = fp / 10000.0;
                Assert.IsTrue(rate <= 0.02, impl + " FP rate " + rate + " at length " + length);
                if (rate > 0.0125) mediocre++; else good++;
            }
            Assert.IsTrue(mediocre * 5 <= good, impl + " mediocre=" + mediocre + " good=" + good);
        }
    }

    [TestMethod]
    public void DuplicateAdjacentKeysIgnored()
    {
        foreach (Impl impl in Enum.GetValues<Impl>())
        {
            using IFilterBitsBuilder b = NewBuilder(impl, 10);
            b.AddKey("a"u8);
            b.AddKey("a"u8);
            b.AddKey("b"u8);
            b.AddKey("a"u8);
            Assert.AreEqual(3, b.EstimatedEntries);
        }
    }

    [TestMethod]
    public void LargeRibbonUsesRibbonAndNoFalseNegatives()
    {
        const int n = 100000;
        using IFilterBitsBuilder b = NewBuilder(Impl.Standard128Ribbon, 10);
        for (int i = 0; i < n; i++) b.AddKey(Key(i));
        using var w = new PooledBufferWriter();
        b.Finish(w);
        byte[] f = w.WrittenSpan.ToArray();
        Assert.AreEqual(-2, unchecked((sbyte)f[^5]));
        var r = (BuiltinFilterBitsReader)BuiltinFilterBitsReader.Create(f);
        Assert.IsInstanceOfType<Standard128RibbonBitsReader>(r);
        for (int i = 0; i < n; i++) Assert.IsTrue(r.MayMatch(Key(i)));
        int fp = 0;
        for (int i = 0; i < 100000; i++) if (r.MayMatch(Key(i + 1000000000))) fp++;
        // ~1% target for 10 bits/key equivalent
        Assert.IsTrue(fp < 1300, "fp=" + fp);
        // Ribbon saves ~30% vs Bloom
        Assert.IsTrue(f.Length < n * 10 / 8 * 0.8, "size=" + f.Length);
        // HashMayMatch agrees with MayMatch
        Assert.IsTrue(r.HashMayMatch(Hash.GetSliceHash64(Key(42))));
    }

    [TestMethod]
    public void CorruptOrReservedMetadata()
    {
        // Too short: always false
        Assert.IsInstanceOfType<AlwaysFalseFilter>(BuiltinFilterBitsReader.Create(new byte[5]));
        // Zero probes: always true
        Assert.IsInstanceOfType<AlwaysTrueFilter>(BuiltinFilterBitsReader.Create(WeirdFill(64, 1, 0)));
        // Reserved marker (-3): always true
        Assert.IsInstanceOfType<AlwaysTrueFilter>(BuiltinFilterBitsReader.Create(WeirdFill(64, 1, 253)));
        // New Bloom with unsupported block size (top bits of block_and_probes)
        Assert.IsInstanceOfType<AlwaysTrueFilter>(BuiltinFilterBitsReader.Create(WeirdFill(256, (1u << 5 | 2) << 8, 255)));
        // New Bloom with nonzero reserved bytes
        Assert.IsInstanceOfType<AlwaysTrueFilter>(BuiltinFilterBitsReader.Create(WeirdFill(256, (2u << 8) | (1u << 16), 255)));
        // New Bloom with unknown sub-implementation
        Assert.IsInstanceOfType<AlwaysTrueFilter>(BuiltinFilterBitsReader.Create(WeirdFill(256, (2u << 8) | 1, 255)));
        // Ribbon with < 2 blocks
        Assert.IsInstanceOfType<AlwaysTrueFilter>(BuiltinFilterBitsReader.Create(WeirdFill(32, 1u << 8, 254)));
        // Legacy with num_lines not dividing len
        Assert.IsInstanceOfType<AlwaysTrueFilter>(BuiltinFilterBitsReader.Create(WeirdFill(256, 3, 2)));
        // Legacy with non power of two line size
        Assert.IsInstanceOfType<AlwaysTrueFilter>(BuiltinFilterBitsReader.Create(WeirdFill(192, 1, 2)));
    }

    [TestMethod]
    public void PolicyNamesAndSanitizing()
    {
        var bloom = new BloomFilterPolicy(10);
        Assert.AreEqual("rocksdb.BuiltinBloomFilter", bloom.CompatibilityName);
        Assert.AreEqual("bloomfilter:10:false", bloom.Id);
        Assert.AreEqual(10000, bloom.MillibitsPerKey);
        var ribbon = new RibbonFilterPolicy(9.5, 2);
        Assert.AreEqual("rocksdb.BuiltinBloomFilter", ribbon.CompatibilityName);
        Assert.AreEqual("ribbonfilter:9.5:2", ribbon.Id);
        Assert.AreEqual(1000, new BloomFilterPolicy(0.6).MillibitsPerKey);
        Assert.AreEqual(100000, new BloomFilterPolicy(1000).MillibitsPerKey);
        var none = new BloomFilterPolicy(0.4);
        Assert.IsFalse(none.CreatesFilters);
        Assert.ThrowsExactly<InvalidOperationException>(() => none.CreateBuilder(5));
        Assert.AreEqual("bloomfilter:10.499:false", new BloomFilterPolicy(10.499).Id);
    }

    [TestMethod]
    public void RibbonBloomBeforeLevel()
    {
        var ribbon = new RibbonFilterPolicy(10, 1);
        using var flush = ribbon.CreateBuilder(new FilterBuildingContext(5, 0, true));
        Assert.IsInstanceOfType<FastLocalBloomBitsBuilder>(flush);
        using var l0 = ribbon.CreateBuilder(new FilterBuildingContext(5, 0, false));
        Assert.IsInstanceOfType<FastLocalBloomBitsBuilder>(l0);
        using var l1 = ribbon.CreateBuilder(new FilterBuildingContext(5, 1, false));
        Assert.IsInstanceOfType<Standard128RibbonBitsBuilder>(l1);
        using var unknown = ribbon.CreateBuilder(5);
        Assert.IsInstanceOfType<Standard128RibbonBitsBuilder>(unknown);
        ribbon.BloomBeforeLevel = int.MaxValue;
        using var always = ribbon.CreateBuilder(new FilterBuildingContext(5, 6, false));
        Assert.IsInstanceOfType<FastLocalBloomBitsBuilder>(always);
    }

    [TestMethod]
    public void BuilderReusableAfterFinish()
    {
        foreach (Impl impl in Enum.GetValues<Impl>())
        {
            using IFilterBitsBuilder b = NewBuilder(impl, 10);
            byte[]? first = null;
            for (int round = 0; round < 2; round++)
            {
                for (int i = 0; i < 3000; i++) b.AddKey(Key(i));
                using var w = new PooledBufferWriter();
                b.Finish(w);
                Assert.AreEqual(0, b.EstimatedEntries);
                if (first is null) first = w.WrittenSpan.ToArray();
                else CollectionAssert.AreEqual(first, w.WrittenSpan.ToArray(), impl.ToString());
            }
        }
    }
}
