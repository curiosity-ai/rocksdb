using RocksDb.Managed.Db;
using System.Diagnostics;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>Column families (db/column_family_test.cc) and concurrent writers (db/db_write_test.cc spirit).</summary>
[TestClass]
public sealed class DbColumnFamilyAndConcurrencyTests
{
    [TestMethod]
    public async Task CreateWriteReopenDropColumnFamilies()
    {
        MemoryFileSystem fs = NewFs();
        var mergeCf = new ColumnFamilyOptions { MergeOperator = new UInt64AddMergeOperator() };
        await using (RocksDatabase db = await RocksDatabase.OpenAsync(DbPath, NewOptions(fs)))
        {
            ColumnFamily users = await db.CreateColumnFamilyAsync("users", new ColumnFamilyOptions());
            ColumnFamily counters = db.CreateColumnFamily("counters", mergeCf);
            Assert.AreEqual(1u, users.Id);
            Assert.AreEqual(2u, counters.Id);
            RocksDbException dup = Assert.ThrowsExactly<RocksDbException>(() => db.CreateColumnFamily("users", new ColumnFamilyOptions()));
            Assert.AreEqual(StatusCode.InvalidArgument, dup.Code);

            db.Put(B("k"), B("default"));
            users.Put(B("k"), B("users"));
            await users.PutAsync(B("k2"), B("users2"));
            for (int i = 0; i < 10; i++) counters.Merge(B("c"), BitConverter.GetBytes((ulong)i));
            Assert.AreEqual(45UL, BitConverter.ToUInt64(counters.Get(B("c"))!));
            using (WriteBatch b = db.CreateWriteBatch())
            {
                b.Put(users.Id, B("batch"), B("u")).Merge(counters.Id, B("c"), BitConverter.GetBytes(5UL)).Put(B("batch"), B("d"));
                await db.WriteAsync(b);
            }
            await users.FlushAsync();
            Assert.AreEqual("default", GetString(db, "k"));
            Assert.AreEqual("users", GetString(users, "k"));
            Assert.AreEqual("u", GetString(users, "batch"));
            Assert.AreEqual(50UL, BitConverter.ToUInt64(counters.Get(B("c"))!));
            CollectionAssert.AreEqual(new[] { "default", "users", "counters" }, RocksDatabase.ListColumnFamilies(DbPath, fs));
        }

        // Opening without listing every CF fails.
        RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbPath, NewOptions(fs)));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);

        ColumnFamilyDescriptor[] cfs = [new("default", new ColumnFamilyOptions()), new("users", new ColumnFamilyOptions()), new("counters", mergeCf)];
        int liveAfterDrop;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfs))
        {
            ColumnFamily users = db.GetColumnFamily("users");
            ColumnFamily counters = db.GetColumnFamily("counters");
            Assert.AreEqual("users2", GetString(users, "k2"));
            Assert.AreEqual("d", GetString(db, "batch"));
            Assert.AreEqual(50UL, BitConverter.ToUInt64(counters.Get(B("c"))!));
            db.DropColumnFamily(users);
            Assert.IsTrue(users.IsDropped);
            // Like upstream, the handle of a dropped column family still reads its data.
            Assert.AreEqual("users2", GetString(users, "k2"));
            Assert.IsFalse(db.TryGetColumnFamily("users", out _));
            Assert.AreEqual(2, db.ColumnFamilies.Count);
            RocksDbException dropDefault = Assert.ThrowsExactly<RocksDbException>(() => db.DropColumnFamily(db.DefaultColumnFamily));
            Assert.AreEqual(StatusCode.InvalidArgument, dropDefault.Code);
            liveAfterDrop = db.GetLiveFilesMetadata().Count;
            Assert.IsFalse(db.GetLiveFilesMetadata().Exists(f => f.ColumnFamilyName == "users"));
        }
        // The dropped CF's table files were deleted.
        Assert.AreEqual(liveAfterDrop, CountFiles(fs, ".sst"));
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), [new("default", new ColumnFamilyOptions()), new("counters", mergeCf)]))
        {
            Assert.AreEqual(50UL, BitConverter.ToUInt64(db.GetColumnFamily("counters").Get(B("c"))!));
            CollectionAssert.AreEqual(new[] { "default", "counters" }, RocksDatabase.ListColumnFamilies(DbPath, fs));
        }
    }

    [TestMethod]
    public void MissingColumnFamilies()
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyDescriptor[] cfs = [new("default", new ColumnFamilyOptions()), new("a", new ColumnFamilyOptions())];
        RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbPath, NewOptions(fs), cfs));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        DbOptions o = NewOptions(fs);
        o.CreateMissingColumnFamilies = true;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o, cfs)) db.GetColumnFamily("a").Put(B("x"), B("y"));
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfs)) Assert.AreEqual("y", GetString(db.GetColumnFamily("a"), "x"));
    }

    private static int CountFiles(MemoryFileSystem fs, string ext)
    {
        int n = 0;
        foreach (string f in fs.GetChildren(DbPath)) if (f.EndsWith(ext, StringComparison.Ordinal)) n++;
        return n;
    }

    [TestMethod]
    [Timeout(60_000, CooperativeCancellation = true)]
    [DataRow(true, true)]
    [DataRow(true, false)]
    [DataRow(false, true)]
    public async Task ConcurrentWritersSyncAndAsync(bool concurrentMemtableWrite, bool adaptiveYield)
    {
        // Single-key writes form groups below the parallel threshold: force the parallel inserts to exercise them.
        TestHooks.ForceParallelMemtableWrites = concurrentMemtableWrite;
        try
        {
            await ConcurrentWritersSyncAndAsyncCore(concurrentMemtableWrite, adaptiveYield);
        }
        finally
        {
            TestHooks.ForceParallelMemtableWrites = false;
        }
    }

    private static async Task ConcurrentWritersSyncAndAsyncCore(bool concurrentMemtableWrite, bool adaptiveYield)
    {
        MemoryFileSystem fs = NewFs();
        var cfo = new ColumnFamilyOptions { WriteBufferSize = 256 * 1024, ArenaBlockSize = 16 * 1024, MaxWriteBufferNumber = 3 };
        const int threads = 8;
        var counts = new int[threads];
        DbOptions options = NewOptions(fs);
        options.AllowConcurrentMemtableWrite = concurrentMemtableWrite;
        options.EnableWriteThreadAdaptiveYield = adaptiveYield;
        await using (RocksDatabase db = await RocksDatabase.OpenAsync(DbPath, options, cfo))
        {
            var sw = Stopwatch.StartNew();
            var tasks = new Task[threads];
            for (int t = 0; t < threads; t++)
            {
                int id = t;
                tasks[t] = id % 2 == 0
                    ? Task.Run(async () =>
                    {
                        int i = 0;
                        while (sw.ElapsedMilliseconds < 1500)
                        {
                            await db.PutAsync(new WriteOptions { Sync = i % 50 == 0 }, B("t" + id + "-" + i), Value(i, id));
                            i++;
                        }
                        counts[id] = i;
                    })
                    : Task.Factory.StartNew(() =>
                    {
                        int i = 0;
                        while (sw.ElapsedMilliseconds < 1500)
                        {
                            db.Put(B("t" + id + "-" + i), Value(i, id));
                            i++;
                        }
                        counts[id] = i;
                    }, TaskCreationOptions.LongRunning);
            }
            await Task.WhenAll(tasks);
            ulong total = 0;
            for (int t = 0; t < threads; t++)
            {
                total += (ulong)counts[t];
                for (int i = 0; i < counts[t]; i += 1 + counts[t] / 200)
                {
                    CollectionAssert.AreEqual(Value(i, t), db.Get(B("t" + t + "-" + i)), "t" + t + " i" + i);
                }
            }
            Assert.AreEqual(total, db.GetLatestSequenceNumber());
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo))
        {
            for (int t = 0; t < threads; t++)
            {
                for (int i = 0; i < counts[t]; i += 1 + counts[t] / 100)
                {
                    CollectionAssert.AreEqual(Value(i, t), db.Get(B("t" + t + "-" + i)), "t" + t + " i" + i);
                }
            }
        }
    }

    /// <summary>
    /// Parallel memtable inserts with WAL termination points: the memtables get the whole batches, the merged WAL record
    /// only the parts before each termination point, so after a reopen (WAL recovery) only those survive.
    /// </summary>
    [TestMethod]
    [Timeout(60_000, CooperativeCancellation = true)]
    public async Task ParallelMemtableInsertsWithWalTerminationPoints()
    {
        MemoryFileSystem fs = NewFs();
        const int threads = 6, perThread = 300;
        TestHooks.ForceParallelMemtableWrites = true;
        try
        {
            await using (RocksDatabase db = await RocksDatabase.OpenAsync(DbPath, NewOptions(fs)))
            {
                var tasks = new Task[threads];
                for (int t = 0; t < threads; t++)
                {
                    int id = t;
                    tasks[t] = Task.Factory.StartNew(() =>
                    {
                        for (int i = 0; i < perThread; i++)
                        {
                            using WriteBatch b = db.CreateWriteBatch();
                            b.Put(B("wal" + id + "-" + i), Value(i, id));
                            b.MarkWalTerminationPoint();
                            b.Put(B("mem" + id + "-" + i), Value(i, id));
                            if (id % 2 == 0) db.Write(b);
                            else db.WriteAsync(b).AsTask().GetAwaiter().GetResult();
                        }
                    }, TaskCreationOptions.LongRunning);
                }
                await Task.WhenAll(tasks);
                for (int t = 0; t < threads; t++)
                {
                    for (int i = 0; i < perThread; i++)
                    {
                        CollectionAssert.AreEqual(Value(i, t), db.Get(B("wal" + t + "-" + i)));
                        CollectionAssert.AreEqual(Value(i, t), db.Get(B("mem" + t + "-" + i)));
                    }
                }
                Assert.AreEqual((ulong)(threads * perThread * 2), db.GetLatestSequenceNumber());
            }
        }
        finally
        {
            TestHooks.ForceParallelMemtableWrites = false;
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            for (int t = 0; t < threads; t++)
            {
                for (int i = 0; i < perThread; i++)
                {
                    CollectionAssert.AreEqual(Value(i, t), db.Get(B("wal" + t + "-" + i)));
                    Assert.IsNull(db.Get(B("mem" + t + "-" + i)));
                }
            }
        }
    }

    /// <summary>
    /// Point reads use super version references cached per processor (SuperVersionCache, upstream local_sv_): a read
    /// after a memtable switch, flush or compaction must not use the cached, obsolete one.
    /// </summary>
    [TestMethod]
    [Timeout(60_000, CooperativeCancellation = true)]
    public async Task PointReadsNeverUseAnObsoleteCachedSuperVersion()
    {
        MemoryFileSystem fs = NewFs();
        await using RocksDatabase db = await RocksDatabase.OpenAsync(DbPath, NewOptions(fs));
        ColumnFamily cf = db.CreateColumnFamily("cf", new ColumnFamilyOptions());
        for (int round = 0; round < 20; round++)
        {
            string expect = "v" + round;
            cf.Put(B("k"), B(expect));
            // Several reads (sync, async, MultiGet) on whatever processors this thread runs on cache the super version.
            Assert.AreEqual(expect, GetString(cf, "k"));
            Assert.AreEqual(expect, System.Text.Encoding.UTF8.GetString((await cf.GetAsync(B("k")))!));
            switch (round % 3)
            {
                case 0: cf.Flush(); break;
                case 1: await cf.CompactRangeAsync(); break;
                default: await cf.FlushAsync(); break;
            }
            string next = "w" + round;
            cf.Put(B("k"), B(next)); // lands in the memtable of the newly installed super version
            Assert.AreEqual(next, GetString(cf, "k"));
            Assert.AreEqual(next, System.Text.Encoding.UTF8.GetString((await cf.GetAsync(B("k")))!));
            byte[]?[] values = cf.MultiGet([B("k")]);
            Assert.AreEqual(next, System.Text.Encoding.UTF8.GetString(values[0]!));
        }
        // Reads through a dropped column family's handle keep working on its last super version (upstream).
        db.DropColumnFamily(cf);
        Assert.AreEqual("w19", GetString(cf, "k"));
        Assert.AreEqual("w19", System.Text.Encoding.UTF8.GetString((await cf.GetAsync(B("k")))!));
    }

    [TestMethod]
    [Timeout(60_000, CooperativeCancellation = true)]
    public async Task ConcurrentReadersDuringWritesAndFlushes()
    {
        MemoryFileSystem fs = NewFs();
        var cfo = new ColumnFamilyOptions { WriteBufferSize = 64 * 1024, ArenaBlockSize = 8 * 1024, MaxWriteBufferNumber = 4 };
        await using RocksDatabase db = await RocksDatabase.OpenAsync(DbPath, NewOptions(fs), cfo);
        const int n = 20000;
        int written = 0;
        Task writer = Task.Run(() =>
        {
            for (int i = 0; i < n; i++)
            {
                db.Put(Key(i), Value(i, 0));
                Volatile.Write(ref written, i + 1);
            }
        });
        var readers = new Task[3];
        for (int r = 0; r < readers.Length; r++)
        {
            int seed = r;
            readers[r] = Task.Run(() =>
            {
                var rnd = new Random(seed);
                while (Volatile.Read(ref written) < n)
                {
                    int w = Volatile.Read(ref written);
                    if (w == 0) continue;
                    int i = rnd.Next(w);
                    CollectionAssert.AreEqual(Value(i, 0), db.Get(Key(i)), "key " + i);
                }
            });
        }
        await Task.WhenAll(readers);
        await writer;
        for (int i = 0; i < n; i += 97) CollectionAssert.AreEqual(Value(i, 0), db.Get(Key(i)));
    }
}
