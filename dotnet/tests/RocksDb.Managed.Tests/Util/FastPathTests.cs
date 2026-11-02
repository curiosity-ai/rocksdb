using System.Numerics;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Tests.Util;

/// <summary>
/// Randomized equivalence tests for the hand-optimized primitives (3-way CRC32C, unrolled varint decoding, short-key
/// byte comparison) against straightforward reference implementations.
/// </summary>
[TestClass]
public sealed class FastPathTests
{
    private static uint ReferenceCrc32C(uint init, ReadOnlySpan<byte> data)
    {
        uint crc = ~init;
        foreach (byte b in data) crc = BitOperations.Crc32C(crc, b);
        return ~crc;
    }

    [TestMethod]
    public void Crc32CMatchesBytewiseReferenceForAllLengthsAndAlignments()
    {
        int seed = Environment.TickCount;
        var rnd = new Random(seed);
        byte[] buf = new byte[3 * 256 * 5 + 64];
        rnd.NextBytes(buf);
        // Every length around the 3-way thresholds plus random ones, at several start offsets.
        var lengths = new List<int>();
        for (int n = 0; n <= 3 * 256 * 2 + 20; n++) lengths.Add(n);
        for (int k = 0; k < 50; k++) lengths.Add(rnd.Next(buf.Length - 8));
        foreach (int offset in new[] { 0, 1, 3, 7 })
        {
            foreach (int n in lengths)
            {
                if (offset + n > buf.Length) continue;
                ReadOnlySpan<byte> s = buf.AsSpan(offset, n);
                Assert.AreEqual(ReferenceCrc32C(0, s), Crc32C.Value(s), "seed " + seed + " offset " + offset + " len " + n);
            }
        }
        // Extend chaining across the 3-way boundary.
        uint whole = Crc32C.Value(buf);
        for (int split = 0; split < buf.Length; split += 97)
            Assert.AreEqual(whole, Crc32C.Extend(Crc32C.Value(buf.AsSpan(0, split)), buf.AsSpan(split)), "seed " + seed + " split " + split);
    }

    private static int ReferenceVarint32(ReadOnlySpan<byte> input, out uint value)
    {
        uint result = 0;
        for (int i = 0, shift = 0; i < Math.Min(input.Length, 5); i++, shift += 7)
        {
            uint b = input[i];
            if ((b & 0x80) == 0)
            {
                value = result | (b << shift);
                return i + 1;
            }
            result |= (b & 0x7F) << shift;
        }
        value = 0;
        return 0;
    }

    private static int ReferenceVarint64(ReadOnlySpan<byte> input, out ulong value)
    {
        ulong result = 0;
        for (int i = 0, shift = 0; i < Math.Min(input.Length, 10); i++, shift += 7)
        {
            ulong b = input[i];
            if ((b & 0x80) == 0)
            {
                value = result | (b << shift);
                return i + 1;
            }
            result |= (b & 0x7F) << shift;
        }
        value = 0;
        return 0;
    }

    [TestMethod]
    public void VarintDecodingMatchesReferenceIncludingMalformedAndTruncatedInput()
    {
        int seed = Environment.TickCount;
        var rnd = new Random(seed);
        byte[] buf = new byte[16];
        for (int iter = 0; iter < 20000; iter++)
        {
            // Mostly continuation-heavy random bytes, so long, overlong and unterminated encodings all occur.
            for (int i = 0; i < buf.Length; i++) buf[i] = (byte)(rnd.Next(4) == 0 ? rnd.Next(128) : 128 + rnd.Next(128));
            int len = rnd.Next(buf.Length + 1);
            ReadOnlySpan<byte> s = buf.AsSpan(0, len);
            Assert.AreEqual(ReferenceVarint32(s, out uint r32), Coding.DecodeVarint32(s, out uint v32), "seed " + seed);
            Assert.AreEqual(r32, v32, "seed " + seed);
            Assert.AreEqual(ReferenceVarint64(s, out ulong r64), Coding.DecodeVarint64(s, out ulong v64), "seed " + seed);
            Assert.AreEqual(r64, v64, "seed " + seed);
        }
        // Round trips at every width, with and without trailing slack.
        Span<byte> enc = stackalloc byte[16];
        for (int bits = 0; bits <= 64; bits++)
        {
            ulong v = bits == 0 ? 0 : (ulong.MaxValue >> (64 - bits));
            int n = Coding.EncodeVarint64(enc, v);
            Assert.AreEqual(n, Coding.DecodeVarint64(enc[..n], out ulong d1));
            Assert.AreEqual(v, d1);
            Assert.AreEqual(n, Coding.DecodeVarint64(enc, out ulong d2));
            Assert.AreEqual(v, d2);
            if (bits <= 32)
            {
                Assert.AreEqual(n, Coding.DecodeVarint32(enc[..n], out uint d3));
                Assert.AreEqual((uint)v, d3);
            }
        }
    }

    [TestMethod]
    public void ByteCompareMatchesSequenceCompareTo()
    {
        int seed = Environment.TickCount;
        var rnd = new Random(seed);
        for (int iter = 0; iter < 50000; iter++)
        {
            int la = rnd.Next(0, 90), lb = rnd.Next(0, 90);
            if (iter % 3 == 0) lb = la;
            byte[] a = new byte[la], b = new byte[lb];
            rnd.NextBytes(a);
            // Long shared prefixes so the difference falls in every word/tail position.
            int common = Math.Min(la, lb) == 0 ? 0 : rnd.Next(Math.Min(la, lb) + 1);
            rnd.NextBytes(b);
            a.AsSpan(0, common).CopyTo(b);
            int expected = Math.Sign(a.AsSpan().SequenceCompareTo(b));
            Assert.AreEqual(expected, Math.Sign(ByteCompare.Compare(a, b)), "seed " + seed);
            Assert.AreEqual(-expected, Math.Sign(ByteCompare.Compare(b, a)), "seed " + seed);
        }
    }
}
