using System.Diagnostics;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>GenericRateLimiter (ports of util/rate_limiter_test.cc scenarios) and DBOptions::rate_limiter.</summary>
[TestClass]
public sealed class RateLimiterTests
{
    [TestMethod]
    public void BurstAndRefillAmounts()
    {
        using var l = new GenericRateLimiter(1_000_000, 100_000, 10, RateLimiterMode.WritesOnly, autoTuned: false, 0);
        Assert.AreEqual(100_000, l.SingleBurstBytes); // rate * refill period
        l.SetSingleBurstBytes(4096);
        Assert.AreEqual(4096, l.SingleBurstBytes);
        l.SetSingleBurstBytes(0);
        l.SetBytesPerSecond(2_000_000);
        Assert.AreEqual(200_000, l.SingleBurstBytes);
        Assert.AreEqual(2_000_000, l.BytesPerSecond);
        var ex = Assert.ThrowsExactly<RocksDbException>(() => l.SetSingleBurstBytes(-1));
        Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);
    }

    [TestMethod]
    public void RateIsEnforced()
    {
        // 2 MB/s with 10 ms refills: 1 MB through it takes about 0.5 s.
        using var l = new GenericRateLimiter(2 << 20, 10_000, 10, RateLimiterMode.WritesOnly, autoTuned: false, 0);
        long chunk = l.SingleBurstBytes;
        var sw = Stopwatch.StartNew();
        long total = 0;
        while (total < (1 << 20))
        {
            l.Request(chunk, IoPriority.Low);
            total += chunk;
        }
        double seconds = sw.Elapsed.TotalSeconds;
        Assert.IsTrue(seconds >= 0.4, "too fast: " + seconds);
        Assert.IsTrue(seconds < 3, "too slow: " + seconds);
        Assert.AreEqual(total, l.GetTotalBytesThrough(IoPriority.Low));
        Assert.AreEqual(total, l.GetTotalBytesThrough(IoPriority.Total));
        Assert.AreEqual(0, l.GetTotalPendingRequests(IoPriority.Total));
    }

    [TestMethod]
    public void TotalPriorityAndReadsOnlyModeAreNotLimited()
    {
        using var l = new GenericRateLimiter(1, 1_000_000, 10, RateLimiterMode.WritesOnly, autoTuned: false, 0);
        l.Request(1, IoPriority.Total); // IO_TOTAL bypasses the limiter
        Assert.AreEqual(0, l.GetTotalRequests(IoPriority.Total));
        using var reads = new GenericRateLimiter(1, 1_000_000, 10, RateLimiterMode.ReadsOnly, autoTuned: false, 0);
        reads.Request(1_000, IoPriority.High); // writes are not charged in reads-only mode
        Assert.AreEqual(0, reads.GetTotalRequests(IoPriority.Total));
    }

    [TestMethod]
    public async Task HigherPrioritiesAreServedFirst()
    {
        // fairness 100: high is served after low only once in 100 refills.
        using var l = new GenericRateLimiter(100_000, 10_000, 100, RateLimiterMode.WritesOnly, autoTuned: false, 0);
        long burst = l.SingleBurstBytes; // 1000 bytes per 10 ms
        l.Request(burst, IoPriority.Low); // drain the first refill
        var low = new List<Task>();
        var high = new List<Task>();
        for (int i = 0; i < 10; i++) low.Add(l.RequestAsync(burst, IoPriority.Low, CancellationToken.None).AsTask());
        for (int i = 0; i < 10; i++) high.Add(l.RequestAsync(burst, IoPriority.High, CancellationToken.None).AsTask());
        await Task.WhenAll(high);
        int lowDone = 0;
        foreach (Task t in low) if (t.IsCompleted) lowDone++;
        Assert.IsTrue(lowDone <= 2, "low priority requests were served before high ones: " + lowDone);
        await Task.WhenAll(low);
        Assert.AreEqual(10 * burst, l.GetTotalBytesThrough(IoPriority.High));
        Assert.AreEqual(11 * burst, l.GetTotalBytesThrough(IoPriority.Low));
    }

    [TestMethod]
    public void AutoTunedStartsAtHalfTheRate()
    {
        using var l = new GenericRateLimiter(1_000_000, 100_000, 10, RateLimiterMode.WritesOnly, autoTuned: true, 0);
        Assert.AreEqual(500_000, l.BytesPerSecond);
    }

    [TestMethod]
    public void FlushAndCompactionWritesAreCharged()
    {
        string path = Path.Combine(Path.GetTempPath(), "rocksdb-managed-ratelimit-" + Guid.NewGuid().ToString("N"));
        try
        {
            using var limiter = new GenericRateLimiter(64 << 20);
            var o = new DbOptions { CreateIfMissing = true, RateLimiter = limiter };
            using (RocksDatabase db = RocksDatabase.Open(path, o))
            {
                for (int round = 0; round < 2; round++)
                {
                    for (int i = 0; i < 500; i++) db.Put(Key(i), Value(i, round));
                    db.Flush();
                }
                long high = limiter.GetTotalBytesThrough(IoPriority.High);
                Assert.IsTrue(high > 0, "flush writes are charged at IO_HIGH");
                Assert.AreEqual(0, limiter.GetTotalBytesThrough(IoPriority.User));
                db.CompactRange(null, null);
                Assert.IsTrue(limiter.GetTotalBytesThrough(IoPriority.Low) > 0, "compaction writes are charged at IO_LOW");
                Assert.AreEqual(high, limiter.GetTotalBytesThrough(IoPriority.High));
            }
        }
        finally
        {
            try { Directory.Delete(path, recursive: true); } catch (IOException) { }
        }
    }
}
