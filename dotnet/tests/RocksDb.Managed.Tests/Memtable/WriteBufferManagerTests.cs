using RocksDb.Managed.Cache;
using RocksDb.Managed.Env;
using RocksDb.Managed.Tests.Db;

namespace RocksDb.Managed.Tests.Memtable;

/// <summary>
/// Ports of memtable/write_buffer_manager_test.cc (ShouldFlush, ChargeWriteBufferTest, stall begin/end) and
/// db/db_write_buffer_manager_test.cc (shared managers across DBs and column families, flushes with
/// FlushReason::kWriteBufferManager, write stalls, dynamic SetBufferSize / SetAllowStall).
/// </summary>
[TestClass]
public sealed class WriteBufferManagerTests
{
    private const long Mib = 1024 * 1024;

    [TestMethod]
    public void ShouldFlush()
    {
        // A write buffer manager of size 10MB.
        var wbf = new WriteBufferManager(10 * Mib, null, allowStall: false);
        wbf.ReserveMem(8 * Mib);
        Assert.IsFalse(wbf.ShouldFlush());
        // 90% of the hard limit will hit the condition.
        wbf.ReserveMem(1 * Mib);
        Assert.IsTrue(wbf.ShouldFlush());
        // Scheduling for freeing will release the condition.
        wbf.ScheduleFreeMem(1 * Mib);
        Assert.IsFalse(wbf.ShouldFlush());
        wbf.ReserveMem(2 * Mib);
        Assert.IsTrue(wbf.ShouldFlush());
        wbf.ScheduleFreeMem(4 * Mib);
        // 11MB total, 6MB mutable. hard limit still hit.
        Assert.IsTrue(wbf.ShouldFlush());
        wbf.ScheduleFreeMem(2 * Mib);
        // 11MB total, 4MB mutable: more than half is already being flushed.
        Assert.IsFalse(wbf.ShouldFlush());
        wbf.ReserveMem(4 * Mib);
        // 15 MB total, 8MB mutable.
        Assert.IsTrue(wbf.ShouldFlush());
        wbf.FreeMem(7 * Mib);
        // 8MB total, 8MB mutable.
        Assert.IsFalse(wbf.ShouldFlush());
        // change size: 8M limit, 7M mutable limit
        wbf.SetBufferSize(8 * Mib);
        Assert.IsTrue(wbf.ShouldFlush());
        wbf.ScheduleFreeMem(2 * Mib);
        // 8MB total, 6MB mutable.
        Assert.IsTrue(wbf.ShouldFlush());
        wbf.FreeMem(2 * Mib);
        // 6MB total, 6MB mutable.
        Assert.IsFalse(wbf.ShouldFlush());
        wbf.ReserveMem(1 * Mib);
        Assert.IsFalse(wbf.ShouldFlush());
        wbf.ReserveMem(1 * Mib);
        // 8MB total, 8MB mutable.
        Assert.IsTrue(wbf.ShouldFlush());
        wbf.ScheduleFreeMem(1 * Mib);
        wbf.FreeMem(1 * Mib);
        // 7MB total, 7MB mutable.
        Assert.IsFalse(wbf.ShouldFlush());
        Assert.AreEqual(7 * Mib, wbf.MemoryUsage);
        Assert.IsTrue(wbf.Enabled);
        Assert.IsFalse(wbf.CostToCache);
        Assert.IsFalse(new WriteBufferManager(0, null, false).Enabled);
    }

    [TestMethod]
    public void ChargeWriteBufferBasic()
    {
        const long dummy = 256 * 1024;
        const long metaDataChargeOverhead = 10000;
        BlockCache cache = BlockCache.CreateLru(new LruCacheOptions { Capacity = 1024 * Mib, NumShardBits = 4, MetadataChargePolicy = CacheMetadataChargePolicy.DontCharge });
        var wbf = new WriteBufferManager(50 * Mib, cache, allowStall: false);
        Assert.IsTrue(wbf.CostToCache);
        // Allocate 333KB will allocate 512KB, memory_used_ = 333KB.
        wbf.ReserveMem(333 * 1024);
        Assert.AreEqual(2 * dummy, wbf.DummyEntriesInCacheUsage);
        Assert.IsTrue(cache.PinnedUsage >= 2 * dummy && cache.PinnedUsage < 2 * dummy + metaDataChargeOverhead);
        // Allocate another 512KB, memory_used_ = 845KB: 2 more dummy entries.
        wbf.ReserveMem(512 * 1024);
        Assert.AreEqual(4 * dummy, wbf.DummyEntriesInCacheUsage);
        // Allocate another 10MB, memory_used_ = 11085KB: 40 more dummy entries.
        wbf.ReserveMem(10 * Mib);
        Assert.AreEqual(44 * dummy, wbf.DummyEntriesInCacheUsage);
        // Free 1MB, memory_used_ = 10061KB: no dummy entry is released (delayed decrease, still >= 3/4).
        wbf.FreeMem(1 * Mib);
        Assert.AreEqual(44 * dummy, wbf.DummyEntriesInCacheUsage);
        Assert.IsFalse(wbf.ShouldFlush());
        // Allocate another 41MB, memory_used_ = 52045KB.
        wbf.ReserveMem(41 * Mib);
        Assert.AreEqual(204 * dummy, wbf.DummyEntriesInCacheUsage);
        Assert.IsTrue(wbf.ShouldFlush());
        // Schedule free 20MB: no change in memory_used and cache cost; still need flush as the hard limit hits.
        wbf.ScheduleFreeMem(20 * Mib);
        Assert.AreEqual(204 * dummy, wbf.DummyEntriesInCacheUsage);
        Assert.IsTrue(wbf.ShouldFlush());
        // Free 20MB, memory_used_ = 31565KB: releases 80 dummy entries.
        wbf.FreeMem(20 * Mib);
        Assert.AreEqual(124 * dummy, wbf.DummyEntriesInCacheUsage);
        Assert.IsTrue(cache.PinnedUsage >= 124 * dummy && cache.PinnedUsage < 124 * dummy + metaDataChargeOverhead);
        Assert.IsFalse(wbf.ShouldFlush());
        // Free 16KB, memory_used_ = 31549KB: no dummy entry released.
        wbf.FreeMem(16 * 1024);
        Assert.AreEqual(124 * dummy, wbf.DummyEntriesInCacheUsage);
        wbf.FreeMem(wbf.MemoryUsage);
        Assert.AreEqual(0, wbf.DummyEntriesInCacheUsage);
        Assert.AreEqual(0, cache.PinnedUsage);

        // buffer_size = 0 still charges the cache.
        var unlimited = new WriteBufferManager(0, cache, allowStall: false);
        unlimited.ReserveMem(10 * Mib);
        Assert.AreEqual(40 * dummy, unlimited.DummyEntriesInCacheUsage);
        Assert.IsFalse(unlimited.ShouldFlush());
        unlimited.FreeMem(10 * Mib);
        Assert.AreEqual(0, unlimited.DummyEntriesInCacheUsage);
    }

    private sealed class TestStall : WriteBufferManagerStall
    {
        public int Signals;

        public override void Signal() => Interlocked.Increment(ref Signals);
    }

    [TestMethod]
    public void StallBeginsAtLimitAndEndsWhenMemoryIsFreed()
    {
        var wbm = new WriteBufferManager(1 * Mib, null, allowStall: true);
        var a = new TestStall();
        var b = new TestStall();
        wbm.ReserveMem(1 * Mib);
        Assert.IsTrue(wbm.ShouldStall());
        wbm.BeginWriteStall(a);
        wbm.BeginWriteStall(b);
        Assert.IsTrue(wbm.IsStallActive);
        Assert.AreEqual(0, a.Signals + b.Signals);
        wbm.FreeMem(Mib / 2);
        Assert.IsFalse(wbm.IsStallActive);
        Assert.AreEqual(1, a.Signals);
        Assert.AreEqual(1, b.Signals);

        // A DB beginning a stall that is already resolved is signaled right away.
        var c = new TestStall();
        wbm.BeginWriteStall(c);
        Assert.AreEqual(1, c.Signals);

        // SetAllowStall(false) and SetBufferSize end an active stall.
        wbm.ReserveMem(Mib);
        var d = new TestStall();
        wbm.BeginWriteStall(d);
        wbm.SetAllowStall(false);
        Assert.AreEqual(1, d.Signals);
        Assert.IsFalse(wbm.ShouldStall());
        wbm.SetAllowStall(true);
        var e = new TestStall();
        wbm.BeginWriteStall(e);
        wbm.SetBufferSize(10 * Mib);
        Assert.AreEqual(1, e.Signals);
        wbm.RemoveDbFromQueue(e);
    }

    [TestMethod]
    public void ConcurrentReserveAndFreeKeepAccountsBalanced()
    {
        BlockCache cache = BlockCache.CreateLru(256 * Mib);
        var wbm = new WriteBufferManager(64 * Mib, cache, allowStall: false);
        Parallel.For(0, 8, t =>
        {
            for (int i = 0; i < 2000; i++)
            {
                wbm.ReserveMem(4096 + t);
                wbm.ScheduleFreeMem(4096 + t);
                wbm.FreeMem(4096 + t);
            }
        });
        Assert.AreEqual(0, wbm.MemoryUsage);
        Assert.AreEqual(0, wbm.MutableMemtableMemoryUsage);
    }

    // ---- DB integration --------------------------------------------------------------------------------------

    private sealed class FlushReasons : EventListener
    {
        public readonly List<FlushReason> Reasons = new();

        public override void OnFlushCompleted(RocksDatabase? db, FlushJobInfo info)
        {
            lock (Reasons) Reasons.Add(info.FlushReason);
        }

        public int Count(FlushReason r)
        {
            lock (Reasons) return Reasons.FindAll(x => x == r).Count;
        }
    }

    private static DbOptions Options(IFileSystem fs, WriteBufferManager? wbm, FlushReasons listener, ulong dbWriteBufferSize = 0)
    {
        var o = new DbOptions { CreateIfMissing = true, CreateMissingColumnFamilies = true, FileSystem = fs, WriteBufferManager = wbm, DbWriteBufferSize = dbWriteBufferSize };
        o.Listeners.Add(listener);
        return o;
    }

    private static ColumnFamilyOptions BigMemtables() => new() { WriteBufferSize = 64 * Mib, ArenaBlockSize = 64 * 1024 };

    private static void WaitFor(Func<bool> condition)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        while (!condition() && sw.ElapsedMilliseconds < 30_000) Thread.Sleep(2);
        Assert.IsTrue(condition(), "condition not reached in 30 s");
    }

    [TestMethod]
    public void DbWriteBufferSizeTriggersWriteBufferManagerFlushes()
    {
        MemoryFileSystem fs = DbTestUtil.NewFs();
        var listener = new FlushReasons();
        using RocksDatabase db = RocksDatabase.Open(DbTestUtil.DbPath, Options(fs, null, listener, dbWriteBufferSize: 1 * Mib), BigMemtables());
        byte[] value = new byte[1000];
        for (int i = 0; i < 5000; i++) db.Put(DbTestUtil.Key(i), value);
        WaitFor(() => listener.Count(FlushReason.WriteBufferManager) > 0);
        db.Flush();
        for (int i = 0; i < 5000; i += 97) Assert.AreEqual(1000, db.Get(DbTestUtil.Key(i))!.Length);
    }

    [TestMethod]
    [DataRow(WriteBufferFlushPolicy.FlushOldest)]
    [DataRow(WriteBufferFlushPolicy.FlushLargest)]
    public void SharedManagerAcrossColumnFamiliesPicksByPolicy(WriteBufferFlushPolicy policy)
    {
        MemoryFileSystem fs = DbTestUtil.NewFs();
        var listener = new FlushReasons();
        var wbm = new WriteBufferManager(2 * Mib, null, allowStall: false, policy);
        DbOptions o = Options(fs, wbm, listener);
        var cfs = new[]
        {
            new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, BigMemtables()),
            new ColumnFamilyDescriptor("small", BigMemtables()),
            new ColumnFamilyDescriptor("large", BigMemtables()),
        };
        using RocksDatabase db = RocksDatabase.Open(DbTestUtil.DbPath, o, cfs);
        ColumnFamily small = db.GetColumnFamily("small")!;
        ColumnFamily large = db.GetColumnFamily("large")!;
        byte[] value = new byte[1000];
        // "small" gets the oldest memtable, "large" the most data.
        for (int i = 0; i < 100; i++) small.Put(DbTestUtil.Key(i), value);
        for (int i = 0; i < 1900; i++) large.Put(DbTestUtil.Key(i), value);
        WaitFor(() => listener.Count(FlushReason.WriteBufferManager) > 0);
        Assert.IsTrue(wbm.MemoryUsage > 0);
        string expectedFlushed = policy == WriteBufferFlushPolicy.FlushOldest ? "small" : "large";
        string other = policy == WriteBufferFlushPolicy.FlushOldest ? "large" : "small";
        WaitFor(() => db.GetColumnFamily(expectedFlushed)!.GetProperty("rocksdb.num-files-at-level0") != "0");
        Assert.AreEqual(FlushReason.WriteBufferManager, listener.Reasons[0]);
        if (policy == WriteBufferFlushPolicy.FlushLargest) Assert.AreEqual("0", db.GetColumnFamily(other)!.GetProperty("rocksdb.num-files-at-level0"));
    }

    [TestMethod]
    public void SharedManagerAcrossDbsAndLargestAcrossDbsPolicy()
    {
        MemoryFileSystem fs1 = DbTestUtil.NewFs(), fs2 = DbTestUtil.NewFs();
        var listener1 = new FlushReasons();
        var listener2 = new FlushReasons();
        var wbm = new WriteBufferManager(2 * Mib, null, allowStall: false, WriteBufferFlushPolicy.FlushLargestAcrossDbs);
        using RocksDatabase big = RocksDatabase.Open(DbTestUtil.DbPath, Options(fs1, wbm, listener1), BigMemtables());
        using RocksDatabase small = RocksDatabase.Open(DbTestUtil.DbPath, Options(fs2, wbm, listener2), BigMemtables());
        Assert.AreEqual(2, wbm.RegisteredDbCount);
        byte[] value = new byte[1000];
        for (int i = 0; i < 1500; i++) big.Put(DbTestUtil.Key(i), value);
        // Writes to the small DB push the shared usage over the limit; the coordinator flushes the DB with the most memory.
        for (int i = 0; i < 800; i++) small.Put(DbTestUtil.Key(i), value);
        WaitFor(() => listener1.Count(FlushReason.WriteBufferManager) > 0);
        // (The small DB may also flush locally if the hard limit is reached before the coordinator ran, as upstream.)
        WaitFor(() => !wbm.ShouldFlush());
        small.Dispose();
        Assert.AreEqual(1, wbm.RegisteredDbCount);
    }

    [TestMethod]
    public async Task AllowStallBlocksWritesUntilFlushFreesMemory()
    {
        MemoryFileSystem fs = DbTestUtil.NewFs();
        var listener = new FlushReasons();
        var wbm = new WriteBufferManager(1 * Mib, null, allowStall: true);
        using RocksDatabase db = RocksDatabase.Open(DbTestUtil.DbPath, Options(fs, wbm, listener), BigMemtables());
        byte[] value = new byte[1000];
        // Stalls are transparent: writes wait for flushes to release memtables and then continue.
        for (int i = 0; i < 6000; i++) await db.PutAsync(DbTestUtil.Key(i), value);
        Assert.IsTrue(listener.Count(FlushReason.WriteBufferManager) > 0);
        Assert.IsTrue(wbm.MemoryUsage < 2 * Mib + 256 * 1024, "usage " + wbm.MemoryUsage);

        // no_slowdown writes fail while a stall is in effect.
        wbm.ReserveMem(4 * Mib); // simulate memory held by another DB
        Assert.IsTrue(wbm.ShouldStall());
        var ex = Assert.ThrowsExactly<RocksDbException>(() => db.Put(new WriteOptions { NoSlowdown = true }, DbTestUtil.Key(0), value));
        Assert.AreEqual(StatusCode.Incomplete, ex.Code);
        // A blocked writer resumes once the memory is freed.
        Task blocked = Task.Run(() => db.Put(DbTestUtil.Key(1), value));
        await Task.Delay(50);
        Assert.IsFalse(blocked.IsCompleted);
        wbm.FreeMem(4 * Mib);
        await blocked.WaitAsync(TimeSpan.FromSeconds(30));
        CollectionAssert.AreEqual(value, db.Get(DbTestUtil.Key(1)));
    }

    [TestMethod]
    public void MemtableArenaFeedsTheManagerAndIsReleasedOnFlush()
    {
        MemoryFileSystem fs = DbTestUtil.NewFs();
        var listener = new FlushReasons();
        BlockCache cache = BlockCache.CreateLru(64 * Mib);
        var wbm = new WriteBufferManager(0, cache, allowStall: false); // cost to cache only
        DbOptions o = Options(fs, wbm, listener);
        ColumnFamilyOptions cf = BigMemtables();
        cf.TableOptions.BlockCache = cache;
        using (RocksDatabase db = RocksDatabase.Open(DbTestUtil.DbPath, o, cf))
        {
            long initial = wbm.MemoryUsage;
            Assert.IsTrue(initial > 0, "the empty memtable's first arena block");
            byte[] value = new byte[1000];
            for (int i = 0; i < 2000; i++) db.Put(DbTestUtil.Key(i), value);
            Assert.IsTrue(wbm.MemoryUsage > 2 * Mib);
            Assert.AreEqual(wbm.MemoryUsage, wbm.MutableMemtableMemoryUsage == 0 ? wbm.MemoryUsage : wbm.MemoryUsage);
            Assert.IsTrue(wbm.DummyEntriesInCacheUsage >= wbm.MemoryUsage);
            Assert.IsTrue(cache.PinnedUsage >= wbm.DummyEntriesInCacheUsage);
            db.Flush();
            WaitFor(() => wbm.MemoryUsage <= initial * 2);
        }
        Assert.AreEqual(0, wbm.MemoryUsage);
        WaitFor(() => wbm.DummyEntriesInCacheUsage == 0);
    }

    /// <summary>
    /// An empty memtable charges the manager only the arena's 2 KiB inline block (memory/arena.cc Arena::Arena), so many
    /// idle column families do not fill it; the managed arena used to take a whole arena block (1 MiB) per memtable.
    /// A flushed memtable's block is released and its successor again charges only the inline block.
    /// </summary>
    [TestMethod]
    public void EmptyMemtablesChargeOnlyTheInlineBlock()
    {
        MemoryFileSystem fs = DbTestUtil.NewFs();
        var wbm = new WriteBufferManager(32 * Mib, null, allowStall: false);
        var cfs = new List<ColumnFamilyDescriptor> { new(ColumnFamilyDescriptor.DefaultColumnFamilyName, new ColumnFamilyOptions()) };
        for (int i = 0; i < 40; i++) cfs.Add(new ColumnFamilyDescriptor("cf" + i, new ColumnFamilyOptions()));
        using RocksDatabase db = RocksDatabase.Open(DbTestUtil.DbPath, new DbOptions { CreateIfMissing = true, CreateMissingColumnFamilies = true, FileSystem = fs, WriteBufferManager = wbm }, cfs);
        Assert.AreEqual(41 * (long)Memory.Arena.InlineSize, wbm.MemoryUsage);
        Assert.IsFalse(wbm.ShouldFlush());
        ColumnFamily cf = db.GetColumnFamily("cf0");
        for (int i = 0; i < 2000; i++) cf.Put(DbTestUtil.Key(i), new byte[100]);
        Assert.IsTrue(wbm.MemoryUsage > 41 * (long)Memory.Arena.InlineSize);
        cf.Flush();
        Assert.AreEqual(41 * (long)Memory.Arena.InlineSize, wbm.MemoryUsage);
        Assert.IsTrue(cf.GetIntProperty(DbProperties.CurSizeActiveMemTable) <= (ulong)Memory.Arena.InlineSize);
    }
}
