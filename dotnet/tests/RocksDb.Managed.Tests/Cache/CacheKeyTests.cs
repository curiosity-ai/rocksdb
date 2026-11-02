using RocksDb.Managed.Cache;

namespace RocksDb.Managed.Tests.Cache;

/// <summary>cache/cache_key.cc + table/unique_id.cc helpers (cf. cache_test.cc / table_test.cc unique id tests).</summary>
[TestClass]
public sealed class CacheKeyTests
{
    [TestMethod]
    public void SessionIdRoundTrip()
    {
        var rnd = new Random(42);
        for (int i = 0; i < 1000; i++)
        {
            ulong upper = (ulong)rnd.NextInt64() & ((1UL << 39) - 1); // ~39 bits of entropy in real session ids
            ulong lower = (ulong)rnd.NextInt64() ^ ((ulong)rnd.Next() << 40);
            string id = OffsetableCacheKey.EncodeSessionId(upper, lower);
            Assert.AreEqual(20, id.Length);
            Assert.IsTrue(OffsetableCacheKey.TryDecodeSessionId(id, out ulong u2, out ulong l2));
            Assert.AreEqual(upper, u2);
            Assert.AreEqual(lower, l2);
            Assert.IsTrue(OffsetableCacheKey.TryDecodeSessionId(id.ToLowerInvariant(), out _, out _));
        }
        Assert.IsFalse(OffsetableCacheKey.TryDecodeSessionId("SHORT", out _, out _));
        Assert.IsFalse(OffsetableCacheKey.TryDecodeSessionId("ABCDEFGHIJ012345678!", out _, out _));
    }

    [TestMethod]
    public void InternalUniqueIdIsBijective()
    {
        var rnd = new Random(7);
        for (int i = 0; i < 1000; i++)
        {
            ulong a = (ulong)rnd.NextInt64() | 1, b = (ulong)rnd.NextInt64();
            OffsetableCacheKey k = OffsetableCacheKey.FromInternalUniqueId(a, b);
            Assert.IsFalse(k.IsEmpty);
            (ulong a2, ulong b2) = k.ToInternalUniqueId();
            Assert.AreEqual(a, a2);
            Assert.AreEqual(b, b2);
        }
        Assert.IsTrue(OffsetableCacheKey.FromInternalUniqueId(0, 0).IsEmpty);
    }

    [TestMethod]
    public void KeysAreStableAndDistinct()
    {
        var k1 = new OffsetableCacheKey("db-id", "6ZH7PB5L6A2K9Q7Z0Q1A", 12);
        var k1Again = new OffsetableCacheKey("db-id", "6ZH7PB5L6A2K9Q7Z0Q1A", 12);
        var k2 = new OffsetableCacheKey("db-id", "6ZH7PB5L6A2K9Q7Z0Q1A", 13);
        var k3 = new OffsetableCacheKey("other", "6ZH7PB5L6A2K9Q7Z0Q1A", 12);
        Assert.AreEqual(k1.WithOffset(4096), k1Again.WithOffset(4096));
        Assert.AreNotEqual(k1.WithOffset(4096), k2.WithOffset(4096));
        Assert.AreNotEqual(k1.WithOffset(4096), k3.WithOffset(4096));
        Assert.AreNotEqual(k1.WithOffset(0), k1.WithOffset(4096));
        Assert.AreEqual(k1.CommonPrefix, k1.WithOffset(123).FileNumEtc64);
        Assert.AreEqual(k1.WithOffset(1024), k1.WithOffsetForMinSizeRecord(4096));
        // Malformed session ids fall back to hashing (force=true) and still produce non-empty keys.
        Assert.IsFalse(new OffsetableCacheKey("db", "not-base36!", 1).IsEmpty);
        Assert.AreNotEqual(CacheKey.CreateUniqueForProcessLifetime(), CacheKey.CreateUniqueForProcessLifetime());
    }

    [TestMethod]
    public void BitHelpersMatchCxx()
    {
        Assert.AreEqual(0x8000000000000000UL, OffsetableCacheKey.ReverseBits(1));
        Assert.AreEqual(1UL, OffsetableCacheKey.ReverseBits(0x8000000000000000UL));
        var rnd = new Random(3);
        for (int i = 0; i < 100; i++)
        {
            ulong v = (ulong)rnd.NextInt64();
            Assert.AreEqual(v, OffsetableCacheKey.ReverseBits(OffsetableCacheKey.ReverseBits(v)));
            Assert.AreEqual(v, OffsetableCacheKey.DownwardInvolution(OffsetableCacheKey.DownwardInvolution(v)));
            // Same highest set bit (util/math.h contract).
            Assert.AreEqual(System.Numerics.BitOperations.LeadingZeroCount(v),
                System.Numerics.BitOperations.LeadingZeroCount(OffsetableCacheKey.DownwardInvolution(v)));
        }
    }
}
