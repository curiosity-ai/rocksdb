using RocksDb.Managed.Table;

namespace RocksDb.Managed.Tests.Table;

/// <summary>Footer / block handle / checksum tests (table/format.cc).</summary>
[TestClass]
public sealed class FormatTests
{
    private static readonly ChecksumType[] AllChecksums = [ChecksumType.NoChecksum, ChecksumType.Crc32c, ChecksumType.XxHash, ChecksumType.XxHash64, ChecksumType.Xxh3];

    [TestMethod]
    public void BlockHandleRoundTrip()
    {
        ulong[] values = [0, 1, 127, 128, 1UL << 32, ulong.MaxValue];
        Span<byte> buf = stackalloc byte[BlockHandle.MaxEncodedLength];
        foreach (ulong o in values)
        foreach (ulong s in values)
        {
            var h = new BlockHandle(o, s);
            int n = h.EncodeTo(buf);
            ReadOnlySpan<byte> r = buf[..n];
            Assert.IsTrue(BlockHandle.TryDecode(ref r, out BlockHandle d));
            Assert.AreEqual(h, d);
            Assert.AreEqual(0, r.Length);
        }
        ReadOnlySpan<byte> bad = [0x80];
        Assert.IsFalse(BlockHandle.TryDecode(ref bad, out _));
    }

    [TestMethod]
    public void FooterRoundTripAllVersionsAndChecksums()
    {
        byte[] buf = new byte[Footer.EncodedLength];
        for (int fv = 2; fv <= BlockBasedTableConstants.LatestSupportedFormatVersion; fv++)
        foreach (ChecksumType ct in AllChecksums)
        {
            var meta = new BlockHandle(12345, 678);
            ulong footerOffset = meta.Offset + meta.Size + BlockBasedTableConstants.BlockTrailerSize;
            var index = new BlockHandle(999, 111);
            uint ctx = fv >= 6 ? 0xdeadbeef : 0;
            Footer.Build(buf, fv, footerOffset, ct, meta, index, ctx);
            Footer f = Footer.Decode(buf, footerOffset);
            Assert.AreEqual(fv, f.FormatVersion);
            Assert.AreEqual(ct, f.ChecksumType);
            Assert.AreEqual(meta, f.MetaindexHandle);
            Assert.AreEqual(BlockBasedTableConstants.BlockBasedTableMagicNumber, f.TableMagicNumber);
            if (fv >= 6)
            {
                Assert.IsTrue(f.IndexHandle.IsNull);
                Assert.AreEqual(ctx, f.BaseContextChecksum);
                // Footer checksum covers its contents and its offset.
                if (ct != ChecksumType.NoChecksum)
                {
                    Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => Footer.Decode(buf, footerOffset + 1)).Code);
                    byte[] corrupt = (byte[])buf.Clone();
                    corrupt[20] ^= 1;
                    Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => Footer.Decode(corrupt, footerOffset)).Code);
                }
            }
            else
            {
                Assert.AreEqual(index, f.IndexHandle);
            }
        }
    }

    [TestMethod]
    public void FooterRejectsBadMagicAndVersions()
    {
        byte[] buf = new byte[Footer.EncodedLength];
        Footer.Build(buf, 5, 100, ChecksumType.Crc32c, new BlockHandle(1, 2), new BlockHandle(3, 4), 0);
        byte[] badMagic = (byte[])buf.Clone();
        badMagic[^1] ^= 0xff;
        Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => Footer.Decode(badMagic, 0)).Code);
        byte[] badVersion = (byte[])buf.Clone();
        badVersion[^12] = 42;
        Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => Footer.Decode(badVersion, 0)).Code);
        byte[] legacy = (byte[])buf.Clone();
        RocksDb.Managed.Util.Coding.EncodeFixed64(legacy.AsSpan(legacy.Length - 8), BlockBasedTableConstants.LegacyBlockBasedTableMagicNumber);
        Assert.AreEqual(StatusCode.NotSupported, Assert.ThrowsExactly<RocksDbException>(() => Footer.Decode(legacy, 0)).Code);
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => Footer.Build(buf, 1, 0, ChecksumType.Crc32c, default, default, 0)).Code);
    }

    [TestMethod]
    public void ChecksumWithLastByteMatchesWholeBuffer()
    {
        int seed = Environment.TickCount;
        var rnd = new Random(seed);
        foreach (int len in new[] { 0, 1, 2, 15, 16, 17, 100, 240, 241, 1000, 4096 })
        {
            byte[] data = new byte[len + 1];
            rnd.NextBytes(data);
            foreach (ChecksumType ct in AllChecksums)
            {
                uint whole = BlockChecksum.ComputeBuiltinChecksum(ct, data);
                uint split = BlockChecksum.ComputeBuiltinChecksumWithLastByte(ct, data.AsSpan(0, len), data[len]);
                Assert.AreEqual(whole, split, "seed=" + seed + " type=" + ct + " len=" + len);
            }
        }
    }

    [TestMethod]
    public void KnownChecksumValues()
    {
        // Checksums of "\0\0\0\0\0" style inputs are format-defining; cross-checked against native SSTs in the
        // compat tests. Here: crc32c masked value of a well-known string and XXH3 empty special case.
        Assert.AreEqual(RocksDb.Managed.Util.Crc32C.Mask(0xe3069283u), BlockChecksum.ComputeBuiltinChecksum(ChecksumType.Crc32c, "123456789"u8));
        Assert.AreEqual(0u, BlockChecksum.ComputeBuiltinChecksum(ChecksumType.Xxh3, []));
        Assert.AreEqual(0u, BlockChecksum.ComputeBuiltinChecksum(ChecksumType.NoChecksum, "abc"u8));
    }

    [TestMethod]
    public void ContextModifier()
    {
        Assert.AreEqual(0u, BlockChecksum.ChecksumModifierForContext(0, 12345));
        uint b = 0x12345678;
        Assert.AreEqual(b ^ (5u + 1u), BlockChecksum.ChecksumModifierForContext(b, (1UL << 32) | 5));
    }
    [TestMethod]
    [DataRow("abc1xyz", "abc3", "abc2")]
    [DataRow("abc1xyz", "abc2", "abc1y")]
    [DataRow("abc1\u00ff\u00ffz", "abc2", "abc1\u00ff\u00ff{")]
    [DataRow("abc", "abcd", "abc")]
    [DataRow("abd", "abc", "abd")]
    public void BytewiseShortestSeparator(string start, string limit, string expected)
    {
        using var w = new RocksDb.Managed.Util.PooledBufferWriter();
        InternalKeyOrdering.BytewiseFindShortestSeparator(Latin1(start), Latin1(limit), w);
        CollectionAssert.AreEqual(Latin1(expected), w.WrittenSpan.ToArray());
        w.Clear();
        InternalKeyOrdering.BytewiseFindShortSuccessor(Latin1("\u00ff\u00ffabc"), w);
        CollectionAssert.AreEqual(Latin1("\u00ff\u00ffb"), w.WrittenSpan.ToArray());
    }

    private static byte[] Latin1(string s) => System.Text.Encoding.Latin1.GetBytes(s);
}
