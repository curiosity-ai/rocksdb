using RocksDb.Managed.Util;

namespace RocksDb.Managed.Tests.Util;

[TestClass]
public sealed class CodingTests
{
    [TestMethod]
    public void Varint64RoundTrip()
    {
        ulong[] values = [0, 1, 127, 128, 255, 300, 16383, 16384, uint.MaxValue, (1UL << 56) - 1, ulong.MaxValue];
        using var w = new PooledBufferWriter();
        foreach (ulong v in values) Coding.PutVarint64(w, v);
        ReadOnlySpan<byte> s = w.WrittenSpan;
        foreach (ulong v in values)
        {
            Assert.IsTrue(Coding.GetVarint64(ref s, out ulong got));
            Assert.AreEqual(v, got);
        }
        Assert.AreEqual(0, s.Length);
    }

    [TestMethod]
    public void Varint32TruncatedFails()
    {
        ReadOnlySpan<byte> s = [0x80, 0x80];
        Assert.IsFalse(Coding.GetVarint32(ref s, out _));
    }

    [TestMethod]
    public void VarintLengthMatchesEncoding()
    {
        Span<byte> buf = stackalloc byte[10];
        for (int shift = 0; shift < 64; shift++)
        {
            ulong v = 1UL << shift;
            Assert.AreEqual(Coding.EncodeVarint64(buf, v), Coding.VarintLength(v));
        }
    }

    [TestMethod]
    public void Crc32CKnownValues()
    {
        // From util/crc32c_test.cc
        Assert.AreEqual(0x8a9136aau, Crc32C.Value(new byte[32]));
        byte[] ones = new byte[32]; ones.AsSpan().Fill(0xff);
        Assert.AreEqual(0x62a8ab43u, Crc32C.Value(ones));
        byte[] inc = new byte[32]; for (int i = 0; i < 32; i++) inc[i] = (byte)i;
        Assert.AreEqual(0x46dd794eu, Crc32C.Value(inc));
        Assert.AreEqual(Crc32C.Value("hello world"u8), Crc32C.Extend(Crc32C.Value("hello "u8), "world"u8));
        uint crc = Crc32C.Value("foo"u8);
        Assert.AreNotEqual(crc, Crc32C.Mask(crc));
        Assert.AreEqual(crc, Crc32C.Unmask(Crc32C.Mask(crc)));
    }
}
