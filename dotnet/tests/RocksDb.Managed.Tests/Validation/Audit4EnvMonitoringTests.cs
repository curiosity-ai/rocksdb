using RocksDb.Managed.Cache;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4, slice EnvMonitoring (Env file systems, rate limiter, SstFileManager, info LOG, statistics / histograms, DB
/// properties): regression tests for the divergences found against env/, util/rate_limiter.cc, logging/,
/// monitoring/histogram.cc and db/internal_stats.cc, plus coverage of verified branches that had no test. The
/// differential tests against native librocksdb are in Audit4EnvMonitoringCompatTests.
/// </summary>
[TestClass]
public sealed class Audit4EnvMonitoringTests
{
    private const string Path = "/audit4envmon";

    /// <summary>
    /// PosixFileSystem::RenameFile is rename(2): when the target's directory does not exist it fails (ENOENT) and the
    /// source stays where it was. The in-memory file system removed the source before it checked the target's parent,
    /// so a failed rename deleted the file.
    /// </summary>
    [TestMethod]
    public void MemoryFileSystemFailedRenameKeepsTheSource()
    {
        MemoryFileSystem fs = NewFs();
        using (IWritableFile f = fs.CreateWritable(DbPath + "/a"))
        {
            f.Append(B("payload"));
            f.Sync();
        }
        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => fs.RenameFile(DbPath + "/a", "/no-such-dir/a"));
        Assert.AreEqual(StatusCode.IOError, ex.Code);
        Assert.AreEqual(StatusSubCode.PathNotFound, ex.SubCode);
        Assert.IsTrue(fs.FileExists(DbPath + "/a"), "the source must survive a failed rename");
        Assert.IsFalse(fs.FileExists("/no-such-dir/a"));
        CollectionAssert.AreEqual(B("payload"), fs.ReadAllBytes(DbPath + "/a"));
        // A missing source is still PathNotFound and leaves the target alone.
        ex = Assert.ThrowsExactly<RocksDbException>(() => fs.RenameFile(DbPath + "/missing", DbPath + "/a"));
        Assert.AreEqual(StatusSubCode.PathNotFound, ex.SubCode);
        CollectionAssert.AreEqual(B("payload"), fs.ReadAllBytes(DbPath + "/a"));
    }

    /// <summary>
    /// HistogramStat::ToString (monitoring/histogram.cc): the bucket lines are printed with
    /// "%c %7llu, %7llu ] %8llu %7.3f%% %7.3f%% " followed by one '#' per 5%. The port printed " , " between the bucket
    /// limits. Values 1, 5, 5, 5: buckets [0, 1] and (4, 6] (limits 1, 2, 3, 4, 6, 10, ...), average 4, variance 3, the
    /// median interpolated in (4, 6] at (2 - 1) / 3, the higher percentiles clamped to the maximum.
    /// </summary>
    [TestMethod]
    public void HistogramToStringMatchesTheUpstreamFormat()
    {
        var st = new Statistics();
        st.RecordInHistogram(Histogram.DbGet, 1);
        for (int i = 0; i < 3; i++) st.RecordInHistogram(Histogram.DbGet, 5);
        const string expected =
            "Count: 4 Average: 4.0000  StdDev: 1.73\n" +
            "Min: 1  Median: 4.6667  Max: 5\n" +
            "Percentiles: P50: 4.67 P75: 5.00 P99: 5.00 P99.9: 5.00 P99.99: 5.00\n" +
            "------------------------------------------------------\n" +
            "[       0,       1 ]        1  25.000%  25.000% #####\n" +
            "(       4,       6 ]        3  75.000% 100.000% ###############\n";
        Assert.AreEqual(expected, st.GetHistogramString(Histogram.DbGet));
    }

    /// <summary>
    /// HistogramBucketMapper (monitoring/histogram.cc): limits 1, 2, then x1.5 on the unrounded double, truncated to two
    /// significant digits; IndexForValue is lower_bound (a value equal to a limit belongs to that limit's bucket) and
    /// everything at or above the last limit goes to the last bucket.
    /// </summary>
    [TestMethod]
    public void HistogramBucketBoundaries()
    {
        ulong[] limits = HistogramBuckets.Limits;
        CollectionAssert.AreEqual(new ulong[] { 1, 2, 3, 4, 6, 10, 15, 22, 34, 51, 76, 110, 170, 250, 380, 580, 870, 1300 }, limits[..18]);
        Assert.AreEqual(109, limits.Length); // HistogramStat::buckets_[109]
        Assert.AreEqual(0, HistogramBuckets.IndexForValue(0));
        Assert.AreEqual(0, HistogramBuckets.IndexForValue(1));
        Assert.AreEqual(1, HistogramBuckets.IndexForValue(2));
        Assert.AreEqual(4, HistogramBuckets.IndexForValue(5));
        Assert.AreEqual(4, HistogramBuckets.IndexForValue(6));
        Assert.AreEqual(5, HistogramBuckets.IndexForValue(7));
        Assert.AreEqual(limits.Length - 1, HistogramBuckets.IndexForValue(limits[^1]));
        Assert.AreEqual(limits.Length - 1, HistogramBuckets.IndexForValue(ulong.MaxValue));
        Assert.AreEqual(limits.Length - 1, HistogramBuckets.IndexForValue(limits[^2] + 1));
        Assert.AreEqual(limits.Length - 2, HistogramBuckets.IndexForValue(limits[^2]));
    }

    /// <summary>
    /// GetPropertyNameAndArg / GetPropertyInfo (db/internal_stats.cc): trailing digits of a property name are an optional
    /// argument, so they are not part of the looked-up name and handlers without an argument ignore them (native returns
    /// "0" for "rocksdb.estimate-num-keys0" on an empty DB; see the compat test). The port treated such names as unknown.
    /// DB::GetIntProperty and DB::GetMapProperty resolve the name the same way.
    /// </summary>
    [TestMethod]
    public void PropertyNamesIgnoreATrailingNumericArgument()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        for (int i = 0; i < 7; i++) db.Put(Key(i), Value(i, 0));
        using Snapshot snap = db.GetSnapshot();
        Assert.AreEqual("7", db.GetProperty("rocksdb.estimate-num-keys"));
        Assert.AreEqual("7", db.GetProperty("rocksdb.estimate-num-keys0"));
        Assert.AreEqual("7", db.GetProperty("rocksdb.estimate-num-keys123"));
        Assert.AreEqual(db.GetProperty("rocksdb.levelstats"), db.GetProperty("rocksdb.levelstats42"));
        Assert.IsTrue(db.TryGetIntProperty("rocksdb.num-snapshots12", out ulong snapshots));
        Assert.AreEqual(1UL, snapshots);
        Assert.AreEqual(7UL, db.GetIntProperty("rocksdb.num-entries-active-mem-table9"));
        Assert.IsNotNull(db.GetMapProperty("rocksdb.dbstats7"));
        Assert.IsTrue(db.TryGetAggregatedIntProperty("rocksdb.estimate-num-keys5", out ulong keys));
        Assert.AreEqual(7UL, keys);
        // The argument is still the level for the properties that take one, and anything but digits is part of the name.
        Assert.AreEqual("0", db.GetProperty("rocksdb.num-files-at-level01"));
        Assert.IsNull(db.GetProperty("rocksdb.num-files-at-level7"));
        Assert.IsNull(db.GetProperty("rocksdb.num-files-at-level"));
        Assert.IsNull(db.GetProperty("rocksdb.estimate-num-keys0x"));
        Assert.IsNull(db.GetProperty("rocksdb.estimate-num-keys-0"));
        Assert.IsNull(db.GetProperty("123"));
        Assert.IsNull(db.GetProperty(""));
        Assert.IsFalse(db.TryGetIntProperty("rocksdb.no-such-property1", out _));
        Assert.IsNull(db.GetMapProperty("rocksdb.no-such-property1"));
    }

    /// <summary>
    /// DBImpl::GetAggregatedIntProperty with CreateIntPropertyAggregator (db/internal_stats.cc): for
    /// rocksdb.block-cache-capacity / -usage / -pinned-usage a BlockCachePropertyAggregator adds every distinct cache
    /// once, because column families may share one. The port summed the per-column-family values, so a cache shared by
    /// two column families was reported twice.
    /// </summary>
    [TestMethod]
    public void AggregatedBlockCachePropertiesCountASharedCacheOnce()
    {
        MemoryFileSystem fs = NewFs();
        BlockCache shared = BlockCache.CreateLru(8 << 20);
        var cfo = new ColumnFamilyOptions { DisableAutoCompactions = true };
        cfo.TableOptions.BlockCache = shared;
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
        var cfo2 = new ColumnFamilyOptions { DisableAutoCompactions = true };
        cfo2.TableOptions.BlockCache = shared;
        ColumnFamily second = db.CreateColumnFamily("second", cfo2);
        var cfo3 = new ColumnFamilyOptions { DisableAutoCompactions = true };
        BlockCache own = BlockCache.CreateLru(1 << 20);
        cfo3.TableOptions.BlockCache = own;
        ColumnFamily third = db.CreateColumnFamily("third", cfo3);
        foreach (ColumnFamily cf in new[] { db.DefaultColumnFamily, second, third })
        {
            for (int i = 0; i < 100; i++) cf.Put(Key(i), Value(i, 0));
            cf.Flush();
            for (int i = 0; i < 100; i++) Assert.IsNotNull(cf.Get(Key(i)));
        }
        Assert.IsTrue(db.TryGetAggregatedIntProperty(DbProperties.BlockCacheCapacity, out ulong capacity));
        Assert.AreEqual((ulong)(shared.Capacity + own.Capacity), capacity);
        Assert.IsTrue(shared.Usage > 0 && own.Usage > 0);
        Assert.IsTrue(db.TryGetAggregatedIntProperty(DbProperties.BlockCacheUsage, out ulong usage));
        Assert.AreEqual((ulong)(shared.Usage + own.Usage), usage);
        Assert.IsTrue(db.TryGetAggregatedIntProperty(DbProperties.BlockCachePinnedUsage, out ulong pinned));
        Assert.AreEqual((ulong)(shared.PinnedUsage + own.PinnedUsage), pinned);
        // SumPropertyAggregator for everything else.
        Assert.IsTrue(db.TryGetAggregatedIntProperty(DbProperties.EstimateNumKeys, out ulong keys));
        Assert.AreEqual(300UL, keys);
    }

    private static int OldLogs(MemoryFileSystem fs, string dir)
    {
        int n = 0;
        foreach (string c in fs.GetChildren(dir))
        {
            if (c.StartsWith("LOG.old.", StringComparison.Ordinal)) n++;
        }
        return n;
    }

    /// <summary>
    /// AutoRollLogger::TrimOldLogFiles (logging/auto_roll_logger.cc) deletes old files while
    /// "old_log_files_.size() >= kKeepLogFileNum": keep_log_file_num counts the current LOG, so keep_log_file_num - 1
    /// files named LOG.old.* remain after rolling by size. The port kept keep_log_file_num old files. (The same count for
    /// the plain logger, trimmed at open, is compared with native in the compat test.)
    /// </summary>
    [TestMethod]
    public void RolledInfoLogsAreTrimmedToKeepLogFileNumIncludingTheCurrentLog()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        o.MaxLogFileSize = 2048;
        o.KeepLogFileNum = 3;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o))
        {
            for (int i = 0; i < 30; i++)
            {
                db.Put(Key(i), Value(i, 0));
                db.Flush();
            }
        }
        Assert.IsTrue(fs.FileExists(DbPath + "/LOG"));
        Assert.AreEqual(2, OldLogs(fs, DbPath));

        // keep_log_file_num = 1: only the current LOG survives a reopen.
        MemoryFileSystem fs1 = NewFs();
        DbOptions o1 = NewOptions(fs1);
        o1.KeepLogFileNum = 1;
        for (int i = 0; i < 3; i++)
        {
            using RocksDatabase db = RocksDatabase.Open(DbPath, o1);
            db.Put(Key(i), Value(i, 0));
        }
        Assert.IsTrue(fs1.FileExists(DbPath + "/LOG"));
        Assert.AreEqual(0, OldLogs(fs1, DbPath));
    }

    /// <summary>A microsecond clock that moves only when the test advances it.</summary>
    private sealed class ManualClock
    {
        private long _micros;

        public long NowMicros() => Volatile.Read(ref _micros);

        public void Advance(long micros) => Interlocked.Add(ref _micros, micros);
    }

    /// <summary>
    /// GenericRateLimiter::RefillBytesAndGrantRequestsLocked (util/rate_limiter.cc) with a burst larger than the refill
    /// ("The burst size was explicitly set to be larger than the refill size"): a request above the refill amount is
    /// granted partially, keeps its place at the head of its queue (a later, smaller request of the same priority must
    /// not overtake it), and total_bytes_through_ counts it only once it is granted in full. IO_USER is always served
    /// first. The refill period is one hour on a manual clock, so only the requests of this test trigger refills.
    /// </summary>
    [TestMethod]
    public void RateLimiterGrantsARequestAboveTheRefillAcrossRefillsInOrder()
    {
        const long period = 3_600_000_000; // 1 hour: 3600 bytes per refill at 1 byte/s
        var clock = new ManualClock();
        using var l = new GenericRateLimiter(1, period, 10, RateLimiterMode.WritesOnly, autoTuned: false, 9000, clock.NowMicros);
        Assert.AreEqual(9000, l.SingleBurstBytes);
        // Refill 1 (due at once): 3600 of the 9000 bytes.
        Task big = l.RequestAsync(9000, IoPriority.Low, CancellationToken.None).AsTask();
        Assert.IsFalse(big.IsCompleted);
        Assert.AreEqual(0, l.GetTotalBytesThrough(IoPriority.Low));
        Assert.AreEqual(1, l.GetTotalPendingRequests(IoPriority.Low));
        // Refill 2: all of it goes to the first request (1800 left); the small one behind it waits.
        clock.Advance(period);
        Task small = l.RequestAsync(10, IoPriority.Low, CancellationToken.None).AsTask();
        Assert.IsFalse(big.IsCompleted);
        Assert.IsFalse(small.IsCompleted);
        Assert.AreEqual(2, l.GetTotalPendingRequests(IoPriority.Total));
        Assert.AreEqual(0, l.GetTotalBytesThrough(IoPriority.Total));
        // Refill 3: the user request first, then the low queue in arrival order.
        clock.Advance(period);
        Task user = l.RequestAsync(10, IoPriority.User, CancellationToken.None).AsTask();
        Assert.IsTrue(user.IsCompleted);
        Assert.IsTrue(big.IsCompleted);
        Assert.IsTrue(small.IsCompleted);
        Assert.AreEqual(0, l.GetTotalPendingRequests(IoPriority.Total));
        Assert.AreEqual(10, l.GetTotalBytesThrough(IoPriority.User));
        Assert.AreEqual(9010, l.GetTotalBytesThrough(IoPriority.Low));
        Assert.AreEqual(2, l.GetTotalRequests(IoPriority.Low));
        // What is left of refill 3 (3600 - 10 - 1800 - 10) is granted at once; a request is capped at the burst.
        Task rest = l.RequestAsync(1780, IoPriority.High, CancellationToken.None).AsTask();
        Assert.IsTrue(rest.IsCompleted);
        Assert.AreEqual(1780, l.GetTotalBytesThrough(IoPriority.High));
        Assert.IsFalse(l.RequestAsync(1, IoPriority.Mid, CancellationToken.None).IsCompleted);
    }

    /// <summary>
    /// FilePrefetchBuffer offset arithmetic at the end of the file (file/file_prefetch_buffer.cc: a read is capped at the
    /// file size): a read that crosses EOF returns the bytes that exist, a read at or past EOF returns 0, and a later
    /// read inside the buffered tail is served from the buffer without a physical read.
    /// </summary>
    [TestMethod]
    public void PrefetchBufferReadsAtAndAcrossTheEndOfTheFile()
    {
        MemoryFileSystem fs = NewFs();
        byte[] data = new byte[1000];
        for (int i = 0; i < data.Length; i++) data[i] = (byte)(i * 7);
        using (IWritableFile f = fs.CreateWritable(Path))
        {
            f.Append(data);
            f.Sync();
        }
        using IRandomAccessFile file = fs.OpenRandomAccess(Path);
        using var buffer = new FilePrefetchBuffer(file, 64, 256);
        byte[] dst = new byte[100];
        Assert.AreEqual(50, buffer.Read(950, dst));
        CollectionAssert.AreEqual(data[950..1000], dst[..50]);
        int reads = buffer.PhysicalReads;
        Assert.IsTrue(buffer.TryGetSpan(990, 10, out ReadOnlySpan<byte> tail));
        Assert.IsTrue(tail.SequenceEqual(data.AsSpan(990, 10)));
        Assert.IsFalse(buffer.TryGetSpan(990, 11, out _));
        Assert.AreEqual(reads, buffer.PhysicalReads);
        Assert.AreEqual(0, buffer.Read(1000, dst));
        Assert.AreEqual(0, buffer.Read(5000, dst));
        Assert.AreEqual(100, buffer.Read(0, dst));
        CollectionAssert.AreEqual(data[..100], dst);

        // AsyncPrefetchBuffer: a range that ends exactly at EOF is served, one byte more is not.
        using var async = new AsyncPrefetchBuffer(128);
        async.Reset(file);
        Assert.IsTrue(async.TryRead(900, 100, out ReadOnlySpan<byte> last));
        Assert.IsTrue(last.SequenceEqual(data.AsSpan(900, 100)));
        Assert.IsFalse(async.TryRead(901, 100, out _));
        Assert.IsFalse(async.TryRead(1000, 1, out _));
        // Sequential reads across the chunk boundaries (including blocks that straddle two chunks) return the file bytes.
        async.Reset(file);
        for (int offset = 0; offset + 90 <= data.Length; offset += 90)
        {
            Assert.IsTrue(async.TryRead(offset, 90, out ReadOnlySpan<byte> block), "offset " + offset);
            Assert.IsTrue(block.SequenceEqual(data.AsSpan(offset, 90)), "offset " + offset);
        }
    }

    /// <summary>
    /// SstFileManagerImpl::OnAddFileImpl / OnDeleteFileImpl / OnMoveFile (file/sst_file_manager_impl.cc): adding a tracked
    /// file again replaces its size, deleting an untracked file changes nothing, and a move carries the size to the new
    /// name (an untracked source moves as size 0, as tracked_files_[old_path] default-constructs it).
    /// </summary>
    [TestMethod]
    public void SstFileManagerSpaceAccountingOnAddDeleteAndMove()
    {
        MemoryFileSystem fs = NewFs();
        using var sfm = new SstFileManager(fs, 0, 0.25, 64UL << 20);
        sfm.OnAddFile("/db/1.sst", 100);
        sfm.OnAddFile("/db/2.sst", 50);
        Assert.AreEqual(150UL, sfm.TotalSize);
        sfm.OnAddFile("/db/1.sst", 70); // same file again: the size is replaced
        Assert.AreEqual(120UL, sfm.TotalSize);
        sfm.OnDeleteFile("/db/unknown.sst");
        Assert.AreEqual(120UL, sfm.TotalSize);
        Assert.AreEqual(70UL, sfm.OnMoveFile("/db/1.sst", "/db/1.sst.trash"));
        Assert.AreEqual(120UL, sfm.TotalSize);
        Dictionary<string, ulong> tracked = sfm.GetTrackedFiles();
        Assert.IsFalse(tracked.ContainsKey("/db/1.sst"));
        Assert.AreEqual(70UL, tracked["/db/1.sst.trash"]);
        Assert.AreEqual(0UL, sfm.OnMoveFile("/db/untracked.sst", "/db/moved.sst"));
        Assert.AreEqual(120UL, sfm.TotalSize);
        Assert.AreEqual(0UL, sfm.GetTrackedFiles()["/db/moved.sst"]);
        // IsMaxAllowedSpaceReached is ">=", EnoughRoomForCompaction needs inputs + buffer + reservations to fit.
        sfm.SetMaxAllowedSpaceUsage(120);
        Assert.IsTrue(sfm.IsMaxAllowedSpaceReached);
        sfm.SetMaxAllowedSpaceUsage(200);
        Assert.IsFalse(sfm.IsMaxAllowedSpaceReached);
        Assert.IsFalse(sfm.EnoughRoomForCompaction(81, DbPath, null));  // 120 + 81 > 200
        Assert.IsTrue(sfm.EnoughRoomForCompaction(80, DbPath, null));   // 120 + 80 == 200
        Assert.AreEqual(80UL, sfm.CompactionsReservedSize);
        Assert.IsTrue(sfm.IsMaxAllowedSpaceReachedIncludingCompactions);
        Assert.IsFalse(sfm.EnoughRoomForCompaction(1, DbPath, null));
        sfm.OnCompactionCompletion(80);
        Assert.AreEqual(0UL, sfm.CompactionsReservedSize);
        sfm.SetCompactionBufferSize(10);
        Assert.IsFalse(sfm.EnoughRoomForCompaction(71, DbPath, null));  // 120 + 71 + 10 > 200
        Assert.IsTrue(sfm.EnoughRoomForCompaction(70, DbPath, null));
    }
}
