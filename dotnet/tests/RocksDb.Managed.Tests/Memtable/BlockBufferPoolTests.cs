using System.Buffers;
using System.Numerics;
using RocksDb.Managed.Memory;

namespace RocksDb.Managed.Tests.Memtable;

[TestClass]
public sealed class BlockBufferPoolTests
{
    [TestMethod]
    public void SizeClassesBoundPadding()
    {
        Assert.AreEqual(16, BlockBufferPool.RoundUp(1));
        Assert.AreEqual(4096, BlockBufferPool.RoundUp(4096));
        Assert.AreEqual(5120, BlockBufferPool.RoundUp(4097));
        Assert.AreEqual(5120, BlockBufferPool.RoundUp(4101)); // a typical 4 KiB block + trailer (ArrayPool.Shared: 8192)
        Assert.AreEqual(6144, BlockBufferPool.RoundUp(5121));
        Assert.AreEqual(8192, BlockBufferPool.RoundUp(7169));
        Assert.AreEqual(20480, BlockBufferPool.RoundUp(16391));
        Assert.AreEqual(1 << 20, BlockBufferPool.RoundUp((1 << 20) - 1));
        Assert.AreEqual(1 << 21, BlockBufferPool.RoundUp((1 << 20) + 1));
        for (int n = 1; n <= 3 << 20; n += 1 + n / 50)
        {
            int r = BlockBufferPool.RoundUp(n);
            Assert.IsTrue(r >= n, "n " + n);
            if (n > 4096 && n <= 1 << 20) Assert.IsTrue(r <= n + n / 4 + 1, "padding of " + n + " -> " + r);
        }
    }

    [TestMethod]
    public void RentReturnsClassSizedArraysAndRecyclesThem()
    {
        for (int n = 1; n <= 1 << 21; n = n * 3 / 2 + 1)
        {
            byte[] a = BlockBufferPool.Rent(n);
            Assert.AreEqual(BlockBufferPool.RoundUp(n), a.Length, "n " + n);
            BlockBufferPool.Return(a);
        }
        // Same-class arrays are reused (the pool is process-wide, so only check that a returned array can come back).
        byte[] first = BlockBufferPool.Rent(4101);
        BlockBufferPool.Return(first);
        bool reused = false;
        var held = new List<byte[]>();
        for (int i = 0; i < 128 && !reused; i++)
        {
            byte[] b = BlockBufferPool.Rent(4200);
            reused = ReferenceEquals(b, first);
            held.Add(b);
        }
        foreach (byte[] b in held) BlockBufferPool.Return(b);
        Assert.IsTrue(reused);
    }

    [TestMethod]
    public void ReturnAcceptsSharedPoolArraysAndIgnoresForeignSizes()
    {
        byte[] shared = RocksArrayPool<byte>.Shared.Rent(8192);
        Assert.IsTrue(BitOperations.IsPow2(shared.Length));
        BlockBufferPool.Return(shared); // routed back to ArrayPool.Shared (must not throw)
        BlockBufferPool.Return(new byte[5000]); // not a class size: dropped
        BlockBufferPool.Return(new byte[3]);
    }
}
