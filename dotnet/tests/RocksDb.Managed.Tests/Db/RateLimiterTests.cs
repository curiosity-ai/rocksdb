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

    /// <summary>RateLimiter::IsRateLimited per mode, and Request(bytes, pri, stats, op_type) honouring it.</summary>
    [TestMethod]
    public void OpTypesAreChargedPerMode()
    {
        foreach (RateLimiterMode mode in new[] { RateLimiterMode.ReadsOnly, RateLimiterMode.WritesOnly, RateLimiterMode.AllIo })
        {
            using var l = new GenericRateLimiter(1 << 30, 100_000, 10, mode, autoTuned: false, 0);
            bool reads = mode != RateLimiterMode.WritesOnly, writes = mode != RateLimiterMode.ReadsOnly;
            Assert.AreEqual(reads, l.IsRateLimited(RateLimiterOpType.Read), mode.ToString());
            Assert.AreEqual(writes, l.IsRateLimited(RateLimiterOpType.Write), mode.ToString());
            l.Request(100, IoPriority.User, null, RateLimiterOpType.Read);
            l.Request(10, IoPriority.Low, null, RateLimiterOpType.Write);
            Assert.AreEqual(reads ? 100 : 0, l.GetTotalBytesThrough(IoPriority.User), mode.ToString());
            Assert.AreEqual(writes ? 10 : 0, l.GetTotalBytesThrough(IoPriority.Low), mode.ToString());
        }
        // The default interface implementation (implementations predating op types) charges writes only.
        IRateLimiter counting = new CountingLimiter();
        counting.Request(5, IoPriority.Low, null, RateLimiterOpType.Read);
        counting.Request(7, IoPriority.Low, null, RateLimiterOpType.Write);
        Assert.AreEqual(7, ((CountingLimiter)counting).Bytes);
    }

    private sealed class CountingLimiter : IRateLimiter
    {
        public long Bytes;
        public long SingleBurstBytes => 1 << 20;
        public void Request(long bytes, IoPriority priority) => Bytes += bytes;
        public ValueTask RequestAsync(long bytes, IoPriority priority, CancellationToken cancellationToken)
        {
            Bytes += bytes;
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>The RATE_LIMITER_* tickers and wait histograms recorded by GenericRateLimiter::RequestImpl.</summary>
    [TestMethod]
    public async Task StatisticsAreRecorded()
    {
        var stats = new Statistics();
        // 100 KB/s with 10 ms refills: 1000 bytes per refill. On a manual clock (util/rate_limiter_test.cc uses a mock
        // clock), so a refill happens only when the test advances the clock. On the real clock, a pause of one refill
        // period between two requests gave the later request a fresh refill at once, so it was not counted as delayed.
        // On a busy machine, the thread woken by a refill often took longer than 10 ms to issue the next request.
        var clock = new ManualClock();
        using var l = new GenericRateLimiter(100_000, 10_000, 10, RateLimiterMode.AllIo, autoTuned: false, 0, clock.NowMicros);
        long burst = l.SingleBurstBytes;
        l.Request(burst, IoPriority.Low, stats, RateLimiterOpType.Write); // the first refill is due at once: granted
        await WaitsForOneRefill(l.RequestAsync(burst, IoPriority.Low, stats, RateLimiterOpType.Write, CancellationToken.None), clock);
        await WaitsForOneRefill(l.RequestAsync(burst, IoPriority.User, stats, RateLimiterOpType.Read, CancellationToken.None), clock);
        await WaitsForOneRefill(l.RequestAsync(burst, IoPriority.Low, null, RateLimiterOpType.Write, CancellationToken.None), clock); // no statistics
        Assert.AreEqual(2UL, stats.GetTickerCount(Ticker.RateLimiterRequestsWrite));
        Assert.AreEqual(2UL * (ulong)burst, stats.GetTickerCount(Ticker.RateLimiterBytesWrite));
        Assert.AreEqual(1UL, stats.GetTickerCount(Ticker.RateLimiterRequestsRead));
        Assert.AreEqual((ulong)burst, stats.GetTickerCount(Ticker.RateLimiterBytesRead));
        Assert.AreEqual(1UL, stats.GetTickerCount(Ticker.RateLimiterDelayedRequestsWrite));
        Assert.AreEqual(1UL, stats.GetTickerCount(Ticker.RateLimiterDelayedRequestsRead));
        Assert.IsTrue(stats.GetTickerCount(Ticker.NumberRateLimiterDrains) >= 2);
        Assert.AreEqual(1UL, stats.GetHistogramData(Histogram.RateLimiterWaitMicrosWrite).Count);
        Assert.AreEqual(1UL, stats.GetHistogramData(Histogram.RateLimiterWaitMicrosRead).Count);
        Assert.AreEqual(stats.GetHistogramData(Histogram.RateLimiterWaitMicrosWrite).Sum, stats.GetTickerCount(Ticker.RateLimiterTotalWaitMicrosWrite));
        // Each delayed request waited exactly one refill period of the manual clock.
        Assert.AreEqual(10_000UL, stats.GetTickerCount(Ticker.RateLimiterTotalWaitMicrosWrite));
        Assert.AreEqual(10_000UL, stats.GetTickerCount(Ticker.RateLimiterTotalWaitMicrosRead));
        Assert.AreEqual(10_000UL, stats.GetHistogramData(Histogram.RateLimiterWaitMicrosRead).Sum);
    }

    /// <summary>A microsecond clock that moves only when the test advances it.</summary>
    private sealed class ManualClock
    {
        private long _micros;

        public long NowMicros() => Volatile.Read(ref _micros);

        public void Advance(long micros) => Interlocked.Add(ref _micros, micros);
    }

    /// <summary>
    /// The request must wait while the clock stands still, and be granted by the refill once the clock reaches the next
    /// refill time (one 10 ms period later).
    /// </summary>
    private static async Task WaitsForOneRefill(ValueTask request, ManualClock clock)
    {
        Task t = request.AsTask();
        await Task.Delay(30); // the refill timer fires on real time and finds the clock unchanged
        Assert.IsFalse(t.IsCompleted, "granted before the next refill");
        clock.Advance(10_000);
        await t.WaitAsync(TimeSpan.FromSeconds(30));
    }

    /// <summary>
    /// Ports of db/db_rate_limiter_test.cc: compaction input reads are charged at IO_LOW, user reads at
    /// ReadOptions::rate_limiter_priority (Get, MultiGet, iterators; nothing with the default IO_TOTAL).
    /// </summary>
    [TestMethod]
    public async Task ReadsAreCharged()
    {
        using var limiter = new GenericRateLimiter(1L << 40, 100_000, 10, RateLimiterMode.ReadsOnly, autoTuned: false, 0);
        RocksDb.Managed.Env.MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        o.RateLimiter = limiter;
        var cf = new ColumnFamilyOptions { DisableAutoCompactions = true };
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, cf);
        for (int round = 0; round < 2; round++)
        {
            for (int i = 0; i < 500; i++) db.Put(Key(i), Value(i, round));
            db.Flush();
        }
        Assert.AreEqual(0, limiter.GetTotalRequests(IoPriority.Total), "flush writes are not charged in reads-only mode");
        db.CompactRange(null, null);
        long low = limiter.GetTotalRequests(IoPriority.Low);
        Assert.IsTrue(low > 0, "compaction input reads are charged at IO_LOW");
        Assert.AreEqual(0, limiter.GetTotalRequests(IoPriority.User));

        // Default read options: not charged.
        var noFill = new ReadOptions { FillCache = false };
        for (int i = 0; i < 50; i++) Assert.IsNotNull(db.Get(noFill, Key(i)));
        Assert.AreEqual(low, limiter.GetTotalRequests(IoPriority.Total));

        var charged = new ReadOptions { FillCache = false, RateLimiterPriority = IoPriority.User };
        for (int i = 0; i < 50; i++) Assert.IsNotNull(db.Get(charged, Key(i)));
        long afterGet = limiter.GetTotalRequests(IoPriority.User);
        Assert.IsTrue(afterGet >= 50, "each Get reads (and is charged for) at least one uncached data block: " + afterGet);
        for (int i = 0; i < 20; i++) Assert.IsNotNull(await db.GetAsync(charged, Key(i)));
        long afterAsync = limiter.GetTotalRequests(IoPriority.User);
        Assert.IsTrue(afterAsync >= afterGet + 20, "async Get");

        using (Iterator it = db.NewIterator(charged))
        {
            int n = 0;
            for (it.SeekToFirst(); it.Valid; it.Next()) n++;
            Assert.AreEqual(500, n);
        }
        long afterIter = limiter.GetTotalRequests(IoPriority.User);
        Assert.IsTrue(afterIter > afterAsync, "iterator");

        byte[][] keys = [Key(1), Key(100), Key(300)];
        byte[]?[] values = db.MultiGet(charged, keys);
        Assert.IsNotNull(values[2]);
        Assert.IsTrue(limiter.GetTotalRequests(IoPriority.User) >= afterIter + 1, "MultiGet");
        Assert.AreEqual(low, limiter.GetTotalRequests(IoPriority.Low));
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
            var stats = new Statistics();
            var o = new DbOptions { CreateIfMissing = true, RateLimiter = limiter, Statistics = stats };
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
                // WritableFileWriter passes the DB statistics: every granted write request is counted.
                Assert.AreEqual((ulong)limiter.GetTotalBytesThrough(IoPriority.Total), stats.GetTickerCount(Ticker.RateLimiterBytesWrite));
                Assert.AreEqual((ulong)limiter.GetTotalRequests(IoPriority.Total), stats.GetTickerCount(Ticker.RateLimiterRequestsWrite));
                Assert.AreEqual(0UL, stats.GetTickerCount(Ticker.RateLimiterRequestsRead));
            }
        }
        finally
        {
            try { Directory.Delete(path, recursive: true); } catch (IOException) { }
        }
    }
}
