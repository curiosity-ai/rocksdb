using RocksDb.Managed.Cache;
using RocksDb.Managed.Db;
using RocksDb.Managed.Table;
using RocksDb.Managed.Tests.Db;
using RocksDb.Managed.Tests.Table;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Regression tests of audit #4 (closing pass over the caches): cache/lru_cache.cc, cache/clock_cache.cc,
/// cache/cache_reservation_manager.{h,cc} read side by side with Cache/*.cs.
/// </summary>
[TestClass]
public sealed class Audit4ClosingCachesTests
{
    private static CacheKey Key(int k) => new(0xC105E, (ulong)k);

    private static CacheValue Value() => CacheValue.FromBuffer(new byte[4], 4);

    private static HyperClockCache NewClock(CacheMetadataChargePolicy policy) => new(new HyperClockCacheOptions
    {
        Capacity = 1 << 20, EstimatedEntryCharge = 1024, NumShardBits = 0, MetadataChargePolicy = policy,
    });

    private static ulong MetaWord(ulong acquire, ulong release) =>
        (ClockSlot.StateVisible << ClockSlot.StateShift) | (acquire << ClockSlot.AcquireCounterShift) | (release << ClockSlot.ReleaseCounterShift);

    private static (ulong Acquire, ulong Release) Counters(ClockSlot s)
    {
        ulong meta = s.LoadMeta();
        return ((meta >> ClockSlot.AcquireCounterShift) & ClockSlot.CounterMask, (meta >> ClockSlot.ReleaseCounterShift) & ClockSlot.CounterMask);
    }

    [TestMethod]
    public void HyperClockReleaseWithEraseCorrectsNearOverflowWhenNotTheLastReference()
    {
        // FixedHyperClockTable::Release (cache/clock_cache.cc:910-929): with erase_if_last_ref, a release that turns out
        // not to be the last reference runs CorrectNearOverflow(old_meta, h->meta) before returning false. Without it an
        // entry that is always released with erase_if_last_ref while another reference exists would let both 30-bit
        // counters wrap. CorrectNearOverflow (clock_cache.cc:212-229) clears the top bit of both counters once the
        // release counter exceeds kCounterTopBit + kMaxCountdown.
        HyperClockCache c = NewClock(CacheMetadataChargePolicy.DontCharge);
        Assert.IsTrue(c.TryInsert(Key(1), Value(), 10, CachePriority.Low, out CachedBlock h1));
        CachedBlock h2 = h1.AddRef();
        var slot = (ClockSlot)h1.Entry!;
        const ulong top = 1UL << (ClockSlot.CounterNumBits - 1);
        // Two outstanding references, both counters in the "high" state.
        Volatile.Write(ref slot.Meta, unchecked((long)MetaWord(top + 12, top + 10)));

        h2.Release(eraseIfLastRef: true);

        Assert.AreEqual((12UL, 11UL), Counters(slot), "top bits of both counters cleared, one reference left");
        Assert.AreEqual(ClockSlot.StateVisible, ClockSlot.State(slot.LoadMeta()));
        h1.Release(eraseIfLastRef: false);
        Assert.AreEqual((12UL, 12UL), Counters(slot));
        Assert.IsTrue(c.TryLookup(Key(1), out CachedBlock again));
        again.Dispose();
    }

    [TestMethod]
    public void HyperClockPlainReleaseCorrectsNearOverflow()
    {
        // FixedHyperClockTable::Release, the else branch (clock_cache.cc:952-955).
        HyperClockCache c = NewClock(CacheMetadataChargePolicy.DontCharge);
        Assert.IsTrue(c.TryInsert(Key(1), Value(), 10, CachePriority.Low, out CachedBlock h1));
        var slot = (ClockSlot)h1.Entry!;
        const ulong top = 1UL << (ClockSlot.CounterNumBits - 1);
        // Exactly at the threshold (release == kCounterTopBit + kMaxCountdown): not corrected ("> kThreshold").
        Volatile.Write(ref slot.Meta, unchecked((long)MetaWord(top + 4, top + 3)));
        CachedBlock h2 = h1.AddRef();
        h2.Release(eraseIfLastRef: false);
        Assert.AreEqual((top + 5, top + 4), Counters(slot));
        // One above the threshold before this release: corrected.
        h1.Release(eraseIfLastRef: false);
        Assert.AreEqual((5UL, 5UL), Counters(slot));
    }

    [TestMethod]
    [DataRow(CacheMetadataChargePolicy.FullCharge, 100 + HyperClockCache.SlotMetadataCharge)]
    [DataRow(CacheMetadataChargePolicy.DontCharge, 100)]
    public void HyperClockPinnedUsageIncludesTheSlotMetadataOfPinnedEntries(CacheMetadataChargePolicy policy, int expected)
    {
        // ClockCacheShard::GetPinnedUsage (cache/clock_cache.cc:1344-1370): for every referenced entry
        // table_pinned_usage += h.GetTotalCharge(), and with kFullChargeCacheMetadata also += sizeof(HandleImpl) (64).
        HyperClockCache c = NewClock(policy);
        Assert.AreEqual(0, c.PinnedUsage);
        Assert.IsTrue(c.TryInsert(Key(1), Value(), 100, CachePriority.Low, out CachedBlock h));
        Assert.IsTrue(c.Insert(Key(2), Value(), 50, CachePriority.Low)); // not pinned
        Assert.AreEqual(expected, c.PinnedUsage);
        h.Dispose();
        Assert.AreEqual(0, c.PinnedUsage);
    }

    /// <summary>A reservation manager without a cache: only the bookkeeping CacheReservationManagerImpl does.</summary>
    private sealed class CountingReservationManager : CacheReservationManager
    {
        private long _used;

        public override bool UpdateCacheReservation(long newMemoryUsed)
        {
            _used = newMemoryUsed;
            return true;
        }

        public override long TotalReservedCacheSize => _used;

        // The yield widens the window between reading the total and writing it back.
        public override long TotalMemoryUsed
        {
            get
            {
                long v = _used;
                Thread.Yield();
                return v;
            }
        }
    }

    [TestMethod]
    public void ConcurrentReservationManagerMakesAndReleasesReservationsAtomically()
    {
        // ConcurrentCacheReservationManager::MakeCacheReservation and its CacheReservationHandle destructor
        // (cache/cache_reservation_manager.h:244-247, 292-310) hold cache_res_mgr_mu_ around the whole wrapped
        // MakeCacheReservation / handle release (read the total, add or subtract, update). The port locked the read and
        // the update separately, so concurrent reservations lost updates and the total drifted.
        var mgr = new ConcurrentCacheReservationManager(new CountingReservationManager());
        const int threads = 4, iterations = 20000;
        using var start = new Barrier(threads);
        var workers = new Thread[threads];
        for (int t = 0; t < threads; t++)
        {
            workers[t] = new Thread(() =>
            {
                start.SignalAndWait();
                for (int i = 0; i < iterations; i++)
                {
                    Assert.IsTrue(mgr.MakeCacheReservation(3, out CacheReservationHandle h));
                    h.Dispose();
                }
            });
            workers[t].Start();
        }
        foreach (Thread w in workers) w.Join();
        Assert.AreEqual(0, mgr.TotalMemoryUsed);

        // And a reservation kept by each thread is counted exactly once.
        var handles = new CacheReservationHandle[threads];
        using var start2 = new Barrier(threads);
        for (int t = 0; t < threads; t++)
        {
            int index = t;
            workers[t] = new Thread(() =>
            {
                start2.SignalAndWait();
                for (int i = 0; i < 2000; i++)
                {
                    mgr.MakeCacheReservation(5, out CacheReservationHandle h);
                    if (i == 1999) handles[index] = h;
                    else h.Dispose();
                }
            });
            workers[t].Start();
        }
        foreach (Thread w in workers) w.Join();
        Assert.AreEqual(5 * threads, mgr.TotalMemoryUsed);
        foreach (CacheReservationHandle h in handles) h.Dispose();
        Assert.AreEqual(0, mgr.TotalMemoryUsed);
    }

    // ---- R10: FilterBlockReaderCommon::IsFilterCompatible with user-defined timestamps ------------------------------

    private static byte[] TsKey(string userKey, ulong ts)
    {
        byte[] k = new byte[userKey.Length + 8];
        System.Text.Encoding.UTF8.GetBytes(userKey, k);
        ComparatorWithU64Ts.EncodeU64Ts(ts, k.AsSpan(userKey.Length));
        return k;
    }

    [TestMethod]
    public void PrefixFilterUpperBoundCheckComparesPrefixesWithoutTimestamp()
    {
        // FilterBlockReaderCommon::IsFilterCompatible (table/block_based/filter_block_reader_common.cc:131):
        // comparator->CompareWithoutTimestamp(prefix, false, upper_bound_xform, false). Both operands are prefixes of
        // keys without timestamp (BlockBasedTable::PrefixRangeMayMatch strips it, iterate_upper_bound has none); the port
        // used Compare, which for a timestamp-aware comparator cuts 8 bytes off each operand (here: throws, the
        // prefixes are 3 bytes).
        SliceTransform fixed3 = SliceTransform.CreateFixedPrefix(3);
        var policy = new ExactSetFilterPolicy();
        Comparator ucmp = Comparator.BytewiseWithU64Ts;
        var entries = new List<(byte[], byte[])>
        {
            (TableTestUtil.IKey(TsKey("abc1", 5), 9), DbTestUtil.B("1")),
            (TableTestUtil.IKey(TsKey("abe1", 5), 9), DbTestUtil.B("2")),
        };
        byte[] data = TableTestUtil.BuildTable(entries, new TableFormatOptions { FilterPolicy = policy, WholeKeyFiltering = false },
            new TableBuilderOptions { PrefixExtractor = fixed3, Comparator = ucmp });
        using BlockBasedTableReader r = TableTestUtil.Open(data, new TableReaderOptions { FilterPolicy = policy, PrefixExtractor = fixed3, Comparator = ucmp });
        byte[] absent = TableTestUtil.IKey(TsKey("abd0", ulong.MaxValue), DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
        string abe1 = Convert.ToHexString(TableTestUtil.IKey(TsKey("abe1", 5), 9));

        // [abd0, abd9) lies within the prefix "abd": the filter is consulted and excludes it.
        var bounded = new ReadOptions { AutoPrefixMode = true, IterateUpperBound = DbTestUtil.B("abd9") };
        using (BlockBasedTableIterator it = r.NewIterator(TableReadFlags.None, r.GetPrefixOptions(bounded, fixed3)))
        {
            it.Seek(absent);
            Assert.IsFalse(it.Valid);
        }
        // An upper bound with another prefix: ComparatorWithU64TsImpl does not implement IsSameLengthImmediateSuccessor
        // (false), so the filter is not used and the seek finds the next key.
        var other = new ReadOptions { AutoPrefixMode = true, IterateUpperBound = DbTestUtil.B("abf") };
        using (BlockBasedTableIterator it = r.NewIterator(TableReadFlags.None, r.GetPrefixOptions(other, fixed3)))
        {
            it.Seek(absent);
            Assert.IsTrue(it.Valid);
            Assert.AreEqual(abe1, Convert.ToHexString(it.Key));
        }
    }

    // ---- R38: BlockBasedTable::ApproximateOffsetOf / ApproximateSize with a failing index iterator -----------------

    [TestMethod]
    public void ApproximateOffsetsFallBackWhenAnIndexPartitionIsUnreadable()
    {
        // BlockBasedTable::ApproximateOffsetOf (table/block_based/block_based_table_reader.cc:3419-3428): when the index
        // iterator has an error after Seek the result is file_size / 2. ApproximateSize (3463-3482): a failed start
        // seek counts from offset 0, a failed end seek up to data_size. The port let the read error escape.
        var entries = new List<(byte[], byte[])>();
        for (int i = 0; i < 400; i++) entries.Add((TableTestUtil.IKey($"key{i:D5}", 9), new byte[100]));
        byte[] data = TableTestUtil.BuildTable(entries, new TableFormatOptions
        {
            BlockSize = 256, IndexType = IndexType.TwoLevelIndexSearch, MetadataBlockSize = 128,
        });
        using BlockBasedTableReader r = TableTestUtil.Open(data);
        ulong fileSize = (ulong)data.Length;
        ulong dataSize = r.Properties.DataSize;
        byte[] first = entries[0].Item1, last = entries[^1].Item1;
        ulong lastOffset = r.ApproximateOffsetOf(last);
        Assert.IsTrue(lastOffset > fileSize / 2 && lastOffset < fileSize);
        Assert.AreEqual(0UL, r.ApproximateOffsetOf(first));
        ulong whole = r.ApproximateSize(first, last);
        Assert.AreEqual(lastOffset, whole);

        // Damage the first index partition (it follows the last data block; the table has no filter).
        for (int i = 0; i < 16; i++) data[(int)dataSize + i] ^= 0x5a;

        Assert.AreEqual(fileSize / 2, r.ApproximateOffsetOf(first));
        Assert.AreEqual(lastOffset, r.ApproximateOffsetOf(last), "other partitions are still readable");
        // start fails -> 0; end is readable.
        Assert.AreEqual(whole, r.ApproximateSize(first, last));
        // start fails -> 0, end fails -> data_size: the whole file.
        Assert.AreEqual(fileSize, r.ApproximateSize(first, entries[1].Item1));
    }
}
