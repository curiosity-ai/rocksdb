using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>
/// max_total_wal_size (DBImpl::SwitchWAL, db/db_impl/db_impl_write.cc) and the lazy sync of closed WALs (logs_ synced by
/// the next sync write / SyncWAL instead of at the WAL switch).
/// </summary>
[TestClass]
public sealed class WalSizeAndSyncTests
{
    private readonly ManualResetEventSlim _flushGate = new(false);

    /// <summary>
    /// Holds background flushes at their start (no table file can make the switched memtables durable) until
    /// <see cref="Cleanup"/>, after the crashed instance was abandoned and the checks were done.
    /// </summary>
    private void BlockFlushes() => RocksDb.Managed.Db.TestHooks.Set("DBImpl::FlushMemTables:Start", () => _flushGate.Wait());

    [TestCleanup]
    public void Cleanup()
    {
        RocksDb.Managed.Db.TestHooks.ClearAll();
        _flushGate.Set();
    }

    private static int WalCount(MemoryFileSystem fs) => fs.GetChildren(DbPath).Count(c => c.EndsWith(".log", StringComparison.Ordinal));

    private static RocksDatabase OpenTwoCfs(MemoryFileSystem fs, ulong maxTotalWalSize)
    {
        DbOptions o = NewOptions(fs);
        o.CreateMissingColumnFamilies = true;
        o.MaxTotalWalSize = maxTotalWalSize;
        var cfo = new ColumnFamilyOptions { WriteBufferSize = 64 << 20 };
        return RocksDatabase.Open(DbPath, o, [new("default", cfo), new("cold", cfo.Clone())]);
    }

    [TestMethod]
    public void ExceedingMaxTotalWalSizeFlushesColumnFamiliesOfTheOldestWal()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = OpenTwoCfs(fs, 256 << 10);
        ColumnFamily cold = db.GetColumnFamily("cold");
        cold.Put(B("c"), B("1"));
        // A hot CF fills WALs; without SwitchWAL the cold CF would pin the first WAL forever (its memtable is far
        // from full).
        byte[] value = new byte[1000];
        db.Put(B("h0"), value);
        db.Flush(); // second WAL: the cold CF still has data in the first one
        Assert.AreEqual(2, WalCount(fs));
        for (int i = 0; i < 600; i++) db.Put(B("h" + i), value);
        db.WaitForCompact();
        // The cold CF was flushed (FlushReason kWalFull) and the oldest WAL released.
        Assert.AreEqual("0", cold.GetProperty("rocksdb.num-entries-active-mem-table"));
        Assert.AreEqual(1, db.GetLiveFilesMetadata().Count(f => f.ColumnFamilyName == "cold"));
        Assert.IsTrue(WalCount(fs) <= 2, "WAL count " + WalCount(fs));
        Assert.AreEqual("1", GetString(cold, "c"));
    }

    [TestMethod]
    public void BelowMaxTotalWalSizeNothingIsFlushed()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = OpenTwoCfs(fs, 64 << 20);
        ColumnFamily cold = db.GetColumnFamily("cold");
        cold.Put(B("c"), B("1"));
        byte[] value = new byte[1000];
        db.Put(B("h0"), value);
        db.Flush();
        for (int i = 0; i < 600; i++) db.Put(B("h" + i), value);
        db.WaitForCompact();
        Assert.AreEqual("1", cold.GetProperty("rocksdb.num-entries-active-mem-table"));
        Assert.AreEqual(0, db.GetLiveFilesMetadata().Count(f => f.ColumnFamilyName == "cold"));
        Assert.AreEqual(2, WalCount(fs));
    }

    [TestMethod]
    public void DefaultLimitIsFourTimesTheMemtableBudget()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        o.CreateMissingColumnFamilies = true;
        var cfo = new ColumnFamilyOptions { WriteBufferSize = 1 << 20, MaxWriteBufferNumber = 3 };
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, [new("default", cfo), new("b", cfo.Clone())]);
        RocksDb.Managed.Db.DbImpl impl = db.DefaultColumnFamily.Db;
        Assert.AreEqual(4UL * 2 * 3 * (1 << 20), impl.GetMaxTotalWalSize());
        impl.Options.MaxTotalWalSize = 12345;
        Assert.AreEqual(12345UL, impl.GetMaxTotalWalSize());
    }

    /// <summary>
    /// Single column family: a closed WAL is not synced at the switch (only its buffer is written to the OS); unsynced
    /// writes in it are lost by a crash before its memtable is flushed, exactly like the unsynced writes of the current
    /// WAL, and the recovered state is a prefix.
    /// </summary>
    [TestMethod]
    public void ClosedWalIsNotSyncedByTheSwitch()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        // A huge write buffer: the memtable is switched by hand and its flush never completes before the crash.
        RocksDatabase db = RocksDatabase.Open(DbPath, o, new ColumnFamilyOptions { WriteBufferSize = 64 << 20, MaxWriteBufferNumber = 6 });
        BlockFlushes();
        db.Put(B("a"), B("1")); // not synced, in the first WAL
        db.DefaultColumnFamily.Db.SwitchMemtableForTest();
        db.Put(B("b"), B("1")); // second WAL, not synced
        fs.SimulateCrash();
        using (RocksDatabase re = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            Assert.IsNull(GetString(re, "a"));
            Assert.IsNull(GetString(re, "b"));
        }
        GC.KeepAlive(db);
    }

    /// <summary>A sync write makes every earlier write durable, including those in closed WALs (WriteToWAL syncs all logs_).</summary>
    [TestMethod]
    public void SyncWriteSyncsClosedWals()
    {
        foreach (bool viaSyncWal in new[] { false, true })
        {
            MemoryFileSystem fs = NewFs();
            RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { WriteBufferSize = 64 << 20, MaxWriteBufferNumber = 6 });
            RocksDb.Managed.Db.DbImpl impl = db.DefaultColumnFamily.Db;
            BlockFlushes();
            db.Put(B("a"), B("1"));
            impl.SwitchMemtableForTest();
            db.Put(B("b"), B("2"));
            impl.SwitchMemtableForTest(); // third WAL
            if (viaSyncWal) db.SyncWal();
            else db.Put(WriteOptions.SyncWrites, B("s"), B("1"));
            fs.SimulateCrash();
            using (RocksDatabase re = RocksDatabase.Open(DbPath, NewOptions(fs)))
            {
                Assert.AreEqual("1", GetString(re, "a"), "viaSyncWal=" + viaSyncWal);
                Assert.AreEqual("2", GetString(re, "b"), "viaSyncWal=" + viaSyncWal);
                if (!viaSyncWal) Assert.AreEqual("1", GetString(re, "s"));
            }
            GC.KeepAlive(db);
        }
    }

    /// <summary>With several column families the closed WAL is synced at the switch (upstream: before the flush).</summary>
    [TestMethod]
    public void MultipleColumnFamiliesSyncClosedWalBeforeFlush()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        o.CreateMissingColumnFamilies = true;
        RocksDatabase db = RocksDatabase.Open(DbPath, o, [new("default", new ColumnFamilyOptions()), new("cold", new ColumnFamilyOptions())]);
        db.GetColumnFamily("cold").Put(B("c"), B("1")); // not synced, stays in the first WAL
        db.Put(B("d"), B("1"));
        db.Flush(); // "d" is in a table file: the WAL holding the matching "c" must be durable
        fs.SimulateCrash();
        using (RocksDatabase re = RocksDatabase.Open(DbPath, NewOptions(fs), [new("default", new ColumnFamilyOptions()), new("cold", new ColumnFamilyOptions())]))
        {
            Assert.AreEqual("1", GetString(re, "d"));
            Assert.AreEqual("1", GetString(re.GetColumnFamily("cold"), "c"));
        }
        GC.KeepAlive(db);
    }

    [TestMethod]
    public async Task ClosedWalsAreReleasedAndDbReopens()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        await using (RocksDatabase db = await RocksDatabase.OpenAsync(DbPath, o))
        {
            for (int round = 0; round < 20; round++)
            {
                await db.PutAsync(B("k" + round), B("v"));
                await db.FlushAsync();
            }
            await db.PutAsync(B("last"), B("v"));
            await db.SyncWalAsync();
        }
        using RocksDatabase re = RocksDatabase.Open(DbPath, NewOptions(fs));
        for (int round = 0; round < 20; round++) Assert.AreEqual("v", GetString(re, "k" + round));
        Assert.AreEqual("v", GetString(re, "last"));
        Assert.IsTrue(WalCount(fs) <= 2);
    }
}
