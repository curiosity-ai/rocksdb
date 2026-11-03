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
    [DataRow(true, true, false)]
    [DataRow(true, false, false)]
    [DataRow(false, true, false)]
    [DataRow(true, true, true)]
    [DataRow(false, true, true)]
    [DataRow(true, false, true)]
    public async Task ConcurrentWritersSyncAndAsync(bool concurrentMemtableWrite, bool adaptiveYield, bool pipelined)
    {
        MemoryFileSystem fs = NewFs();
        var cfo = new ColumnFamilyOptions { WriteBufferSize = 256 * 1024, ArenaBlockSize = 16 * 1024, MaxWriteBufferNumber = 3 };
        const int threads = 8;
        var counts = new int[threads];
        DbOptions options = NewOptions(fs);
        options.AllowConcurrentMemtableWrite = concurrentMemtableWrite;
        options.EnableWriteThreadAdaptiveYield = adaptiveYield;
        options.EnablePipelinedWrite = pipelined;
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
    [DataRow(false)]
    [DataRow(true)]
    public async Task ParallelMemtableInsertsWithWalTerminationPoints(bool pipelined)
    {
        MemoryFileSystem fs = NewFs();
        const int threads = 6, perThread = 300;
        DbOptions options = NewOptions(fs);
        options.EnablePipelinedWrite = pipelined;
        {
            await using (RocksDatabase db = await RocksDatabase.OpenAsync(DbPath, options))
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
    /// Pipelined writes (enable_pipelined_write) under memtable and WAL switches: small memtables over two column
    /// families and a small max_total_wal_size make the WAL leader drain the memtable stage (WaitForMemTableWriters)
    /// before every switch, while writers (sync and async API, some with per-key protection, some with merge operands,
    /// which are inserted alone) and manual flushes run concurrently. Every write must be readable, the last sequence
    /// must count every key, and a reopen (WAL recovery) must see the same data.
    /// </summary>
    [TestMethod]
    [Timeout(60_000, CooperativeCancellation = true)]
    [DataRow(true)]
    [DataRow(false)]
    public async Task PipelinedWritesWithMemtableAndWalSwitches(bool concurrentMemtableWrite)
    {
        MemoryFileSystem fs = NewFs();
        DbOptions options = NewOptions(fs);
        options.EnablePipelinedWrite = true;
        options.AllowConcurrentMemtableWrite = concurrentMemtableWrite;
        options.CreateMissingColumnFamilies = true;
        options.MaxTotalWalSize = 256 * 1024;
        options.Statistics = new Statistics();
        var cfo = new ColumnFamilyOptions
        {
            WriteBufferSize = 64 * 1024, MaxWriteBufferNumber = 4, MergeOperator = new StringAppendOperator(),
        };
        const int threads = 6, perThread = 400;
        ColumnFamilyDescriptor[] cfs = [new("default", cfo), new("cf1", cfo)];
        await using (RocksDatabase db = await RocksDatabase.OpenAsync(DbPath, options, cfs))
        {
            ColumnFamily cf1 = db.GetColumnFamily("cf1");
            using var stop = new CancellationTokenSource();
            Task flusher = Task.Run(async () =>
            {
                while (!stop.IsCancellationRequested)
                {
                    await db.FlushAsync();
                    await Task.Delay(5);
                }
            });
            var tasks = new Task[threads];
            for (int t = 0; t < threads; t++)
            {
                int id = t;
                tasks[t] = Task.Factory.StartNew(() =>
                {
                    ColumnFamily cf = id % 2 == 0 ? db.DefaultColumnFamily : cf1;
                    for (int i = 0; i < perThread; i++)
                    {
                        var wo = new WriteOptions { Sync = i % 40 == 0, ProtectionBytesPerKey = id % 3 == 0 ? 8UL : 0UL };
                        using WriteBatch b = db.CreateWriteBatch();
                        b.Put(cf, B("p" + id + "-" + i), Value(i, id));
                        if (i % 7 == 0) b.Merge(cf, B("m" + id), B(i.ToString(System.Globalization.CultureInfo.InvariantCulture)));
                        if (id % 2 == 0) db.Write(wo, b);
                        else db.WriteAsync(wo, b).AsTask().GetAwaiter().GetResult();
                    }
                }, TaskCreationOptions.LongRunning);
            }
            await Task.WhenAll(tasks);
            stop.Cancel();
            await flusher;
            ulong keys = 0;
            for (int t = 0; t < threads; t++)
            {
                ColumnFamily cf = t % 2 == 0 ? db.DefaultColumnFamily : cf1;
                for (int i = 0; i < perThread; i++)
                {
                    CollectionAssert.AreEqual(Value(i, t), cf.Get(B("p" + t + "-" + i)), "t" + t + " i" + i);
                    keys += i % 7 == 0 ? 2UL : 1UL;
                }
                Assert.AreEqual(ExpectedMerge(perThread), S(cf.Get(B("m" + t))));
            }
            Assert.AreEqual(keys, db.GetLatestSequenceNumber());
            Assert.IsTrue(options.Statistics.GetTickerCount(Ticker.NumberKeysWritten) == keys);
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfs))
        {
            ColumnFamily cf1 = db.GetColumnFamily("cf1");
            for (int t = 0; t < threads; t++)
            {
                ColumnFamily cf = t % 2 == 0 ? db.DefaultColumnFamily : cf1;
                for (int i = 0; i < perThread; i += 7) CollectionAssert.AreEqual(Value(i, t), cf.Get(B("p" + t + "-" + i)));
                Assert.AreEqual(ExpectedMerge(perThread), S(cf.Get(B("m" + t))));
            }
        }

        static string ExpectedMerge(int n)
        {
            var sb = new System.Text.StringBuilder();
            for (int i = 0; i < n; i += 7)
            {
                if (sb.Length > 0) sb.Append(',');
                sb.Append(i.ToString(System.Globalization.CultureInfo.InvariantCulture));
            }
            return sb.ToString();
        }
    }

    /// <summary>
    /// The two stages of pipelined writes overlap: while the first write's memtable insert is held, a second write
    /// completes its WAL stage (the next WAL group), but neither is visible before the first group's insert publishes
    /// its sequence; then both complete in order.
    /// </summary>
    [TestMethod]
    [Timeout(60_000, CooperativeCancellation = true)]
    public async Task PipelinedWalStageOverlapsMemtableStage()
    {
        DbOptions options = NewOptions(NewFs());
        options.EnablePipelinedWrite = true;
        options.Statistics = new Statistics();
        using RocksDatabase db = RocksDatabase.Open(DbPath, options);
        using var inMem = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        int calls = 0;
        TestHooks.Set("DBImpl::PipelinedWriteImpl:MemTableWriterLeader", () =>
        {
            if (Interlocked.Increment(ref calls) != 1) return;
            inMem.Set();
            release.Wait();
        });
        try
        {
            ulong walGroupsBefore = options.Statistics.GetTickerCount(Ticker.WriteDoneBySelf);
            Task first = Task.Factory.StartNew(() => db.Put(B("k1"), B("v1")), TaskCreationOptions.LongRunning);
            Assert.IsTrue(inMem.Wait(30_000));
            Task second = db.PutAsync(B("k2"), B("v2")).AsTask();
            var sw = Stopwatch.StartNew();
            while (options.Statistics.GetTickerCount(Ticker.WriteDoneBySelf) < walGroupsBefore + 2 && sw.ElapsedMilliseconds < 30_000) await Task.Delay(1);
            Assert.AreEqual(walGroupsBefore + 2, options.Statistics.GetTickerCount(Ticker.WriteDoneBySelf), "second WAL group not written");
            Assert.AreEqual(0UL, db.GetLatestSequenceNumber());
            Assert.IsNull(db.Get(B("k1")));
            Assert.IsFalse(first.IsCompleted);
            Assert.IsFalse(second.IsCompleted);
            release.Set();
            await Task.WhenAll(first, second);
            Assert.AreEqual(2UL, db.GetLatestSequenceNumber());
            Assert.AreEqual("v1", S(db.Get(B("k1"))));
            Assert.AreEqual("v2", S(db.Get(B("k2"))));
        }
        finally
        {
            release.Set();
            TestHooks.Set("DBImpl::PipelinedWriteImpl:MemTableWriterLeader", null);
        }
    }

    /// <summary>
    /// Transactions over pipelined writes: optimistic transactions (whose conflict-check callback disallows batching, so
    /// their WAL leader first drains the memtable stage) increment shared counters from several threads without lost
    /// updates, and pessimistic 2PC transactions (WAL-only prepares, which skip the memtable stage, then commits) are
    /// visible after commit and recovered after reopen.
    /// </summary>
    [TestMethod]
    [Timeout(60_000, CooperativeCancellation = true)]
    public void PipelinedWritesWithTransactions()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions options = NewOptions(fs);
        options.EnablePipelinedWrite = true;
        const int threads = 4, perThread = 150, counters = 3;
        using (var odb = Utilities.Transactions.OptimisticTransactionDatabase.Open(DbPath, options))
        {
            for (int c = 0; c < counters; c++) odb.Database.Put(B("c" + c), B("0"));
            var tasks = new Task[threads];
            for (int t = 0; t < threads; t++)
            {
                int id = t;
                tasks[t] = Task.Factory.StartNew(() =>
                {
                    for (int i = 0; i < perThread; i++)
                    {
                        string key = "c" + ((id + i) % counters);
                        while (true)
                        {
                            using Utilities.Transactions.Transaction txn = odb.BeginTransaction(new WriteOptions());
                            int v = int.Parse(S(txn.GetForUpdate(B(key)))!, System.Globalization.CultureInfo.InvariantCulture);
                            txn.Put(B(key), B((v + 1).ToString(System.Globalization.CultureInfo.InvariantCulture)));
                            odb.Database.Put(B("plain" + id + "-" + i), B("x")); // concurrent plain writes in the pipeline
                            try
                            {
                                txn.Commit();
                                break;
                            }
                            catch (RocksDbException e) when (e.Code is StatusCode.Busy or StatusCode.TryAgain)
                            {
                            }
                        }
                    }
                }, TaskCreationOptions.LongRunning);
            }
            Task.WaitAll(tasks);
            int sum = 0;
            for (int c = 0; c < counters; c++) sum += int.Parse(S(odb.Database.Get(B("c" + c)))!, System.Globalization.CultureInfo.InvariantCulture);
            Assert.AreEqual(threads * perThread, sum);
        }

        fs = NewFs();
        options = NewOptions(fs);
        options.EnablePipelinedWrite = true;
        using (var tdb = Utilities.Transactions.TransactionDatabase.Open(DbPath, options))
        {
            var tasks = new Task[threads];
            for (int t = 0; t < threads; t++)
            {
                int id = t;
                tasks[t] = Task.Factory.StartNew(() =>
                {
                    for (int i = 0; i < 40; i++)
                    {
                        using Utilities.Transactions.Transaction txn = tdb.BeginTransaction(new WriteOptions { Sync = i % 10 == 0 });
                        txn.SetName("x" + id + "-" + i);
                        txn.Put(B("t" + id + "-" + i), Value(i, id));
                        txn.Prepare();
                        if (i % 5 == 4) txn.Rollback();
                        else txn.Commit();
                    }
                }, TaskCreationOptions.LongRunning);
            }
            Task.WaitAll(tasks);
        }
        using (var tdb = Utilities.Transactions.TransactionDatabase.Open(DbPath, options))
        {
            for (int t = 0; t < threads; t++)
            {
                for (int i = 0; i < 40; i++)
                {
                    byte[]? v = tdb.Database.Get(B("t" + t + "-" + i));
                    if (i % 5 == 4) Assert.IsNull(v);
                    else CollectionAssert.AreEqual(Value(i, t), v);
                }
            }
        }
    }

    /// <summary>
    /// Option checks of pipelined writes, as upstream: unordered_write and atomic_flush are rejected at open
    /// (DBImpl::ValidateOptions), two_write_queues at write time (DBImpl::WriteImpl).
    /// </summary>
    [TestMethod]
    public void PipelinedWriteIncompatibleOptions()
    {
        DbOptions o = NewOptions(NewFs());
        o.EnablePipelinedWrite = true;
        o.UnorderedWrite = true;
        var e = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbPath, o));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code, e.Message);

        o = NewOptions(NewFs());
        o.EnablePipelinedWrite = true;
        o.AtomicFlush = true;
        e = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbPath, o));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code, e.Message);

        o = NewOptions(NewFs());
        o.UnorderedWrite = true;
        o.AllowConcurrentMemtableWrite = false;
        e = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbPath, o));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code, e.Message);

        o = NewOptions(NewFs());
        o.EnablePipelinedWrite = true;
        o.TwoWriteQueues = true;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o))
        {
            e = Assert.ThrowsExactly<RocksDbException>(() => db.Put(B("k"), B("v")));
            Assert.AreEqual(StatusCode.NotSupported, e.Code, e.Message);
        }

        // Accepted and persisted: the OPTIONS file round-trips the flag.
        o = NewOptions(NewFs());
        o.EnablePipelinedWrite = true;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o))
        {
            db.Put(B("k"), B("v"));
            Assert.AreEqual("v", S(db.Get(B("k"))));
        }
        OptionsFile persisted = OptionsFile.Load(o.FileSystem, OptionsFile.FindLatestOptionsFile(o.FileSystem, DbPath)!, preserveUnknownOptions: true);
        DbOptions loaded = persisted.DbOptions;
        Assert.IsTrue(loaded.EnablePipelinedWrite);
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

    /// <summary>
    /// One table cache for all column families (DBImpl::table_cache_), bounded by max_open_files - 10 readers in total,
    /// and resized by SetDBOptions(max_open_files).
    /// </summary>
    [TestMethod]
    public void TableCacheIsSharedAcrossColumnFamiliesAndBoundedByMaxOpenFiles()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        o.MaxOpenFiles = 30; // capacity 20
        o.CreateMissingColumnFamilies = true;
        var cfo = new ColumnFamilyOptions { DisableAutoCompactions = true };
        string[] names = ["default", "a", "b"];
        using RocksDatabase db = RocksDatabase.Open(DbPath, o, [.. names.Select(n => new ColumnFamilyDescriptor(n, cfo))]);
        foreach (string n in names)
        {
            ColumnFamily cf = db.GetColumnFamily(n);
            for (int f = 0; f < 12; f++)
            {
                cf.Put(Key(f), Value(f, 0));
                cf.Flush();
            }
        }
        TableCacheStore store = db.DefaultColumnFamily.Db.SharedTableCache;
        for (int round = 0; round < 2; round++)
        {
            foreach (string n in names)
            {
                ColumnFamily cf = db.GetColumnFamily(n);
                for (int f = 0; f < 12; f++) CollectionAssert.AreEqual(Value(f, 0), cf.Get(Key(f)));
            }
        }
        Assert.IsTrue(store.Count <= 20, "cached readers: " + store.Count);
        Assert.IsTrue(store.Count >= 12);
        int perCf = 0;
        foreach (string n in names) perCf += db.GetColumnFamily(n).Data.TableCache.Count;
        Assert.AreEqual(store.Count, perCf, "every cached reader belongs to one column family");
        db.SetDBOptions([new("max_open_files", "-1")]);
        foreach (string n in names)
        {
            ColumnFamily cf = db.GetColumnFamily(n);
            for (int f = 0; f < 12; f++) CollectionAssert.AreEqual(Value(f, 0), cf.Get(Key(f)));
        }
        Assert.AreEqual(36, store.Count);
        db.SetDBOptions([new("max_open_files", "25")]);
        Assert.AreEqual(20, store.Count); // excess unreferenced readers closed right away (minimum capacity 20)
    }

    // ---- WriteOptions.MemtableInsertParallelism (one large batch inserted in parallel ranges) ----

    private const string ParallelRangeHook = "DBImpl::InsertBatchInParallel:Range";

    /// <summary>
    /// A large batch mixing Put / Delete / SingleDelete / DeleteRange / PutEntity / LogData over two column families, with
    /// keys overwritten and deleted across range boundaries.
    /// </summary>
    private static void FillMixedBatch(WriteBatch b, ColumnFamily users, int n)
    {
        for (int i = 0; i < n; i++)
        {
            string k = "k" + (i * 7919 % n).ToString("D6");
            b.Put(B(k), B("v" + i));
            if (i % 10 == 3) b.Put(users.Id, B(k), B("u" + i));
            if (i % 97 == 0) b.PutLogData(B("log" + i));
        }
        // Later records override earlier ones in other ranges: the higher sequence number must win.
        b.Put(B("k000001"), B("overwritten"));
        b.Delete(B("k000002"));
        b.SingleDelete(B("k000003"));
        b.DeleteRange(B("k000100"), B("k000200"));
        b.PutEntity(B("k000004"), [new WideColumn(B("c1"), B("e1"))]);
        b.Delete(users.Id, B("k000013"));
        b.Put(B("last"), B("x"));
    }

    private static List<string> Dump(Iterator it)
    {
        var rows = new List<string>();
        for (it.SeekToFirst(); it.Valid; it.Next()) rows.Add(S(it.Key.ToArray()) + "=" + S(it.Value.ToArray()));
        return rows;
    }

    [TestMethod]
    [DataRow(2)]
    [DataRow(4)]
    [DataRow(64)]
    public async Task MemtableInsertParallelismMatchesSerialInsert(int parallelism)
    {
        const int n = 3000;
        var results = new List<(ulong Seq, List<string> Default, List<string> Users)>();
        foreach (int p in new[] { 1, parallelism })
        {
            MemoryFileSystem fs = NewFs();
            await using RocksDatabase db = await RocksDatabase.OpenAsync(DbPath, NewOptions(fs));
            ColumnFamily users = await db.CreateColumnFamilyAsync("users", new ColumnFamilyOptions());
            db.Put(B("k000005"), B("before"));
            ulong before = db.GetLatestSequenceNumber();
            using Snapshot snap = db.GetSnapshot();
            int ranges = 0;
            bool sawBatch = false;
            // Every range runs before the batch is published: readers see none of it.
            TestHooks.Set(ParallelRangeHook, () =>
            {
                Interlocked.Increment(ref ranges);
                if (db.GetLatestSequenceNumber() != before || db.Get(B("last")) is not null) sawBatch = true;
            });
            try
            {
                using WriteBatch b = db.CreateWriteBatch();
                FillMixedBatch(b, users, n);
                await db.WriteAsync(new WriteOptions { MemtableInsertParallelism = p }, b);
            }
            finally
            {
                TestHooks.Set(ParallelRangeHook, null);
            }
            Assert.IsFalse(sawBatch);
#if DEBUG
            if (p == 1) Assert.AreEqual(0, ranges);
            else Assert.AreEqual(Math.Min(p, 16), ranges);
#endif
            Assert.AreEqual("before", GetString(db, "k000005", snap));
            Assert.IsNull(GetString(db, "last", snap));
            Assert.AreEqual("overwritten", GetString(db, "k000001"));
            Assert.IsNull(GetString(db, "k000002"));
            Assert.IsNull(GetString(db, "k000150"));
            // The write after the batch continues at the next sequence number.
            db.Put(B("after"), B("a"));
            using Iterator it = db.NewIterator();
            using Iterator uit = users.NewIterator();
            results.Add((db.GetLatestSequenceNumber(), Dump(it), Dump(uit)));
            await db.FlushAsync();
            await users.FlushAsync();
            using Iterator it2 = db.NewIterator();
            CollectionAssert.AreEqual(results[^1].Default, Dump(it2));
        }
        Assert.AreEqual(results[0].Seq, results[1].Seq);
        CollectionAssert.AreEqual(results[0].Default, results[1].Default);
        CollectionAssert.AreEqual(results[0].Users, results[1].Users);
    }

    [TestMethod]
    public async Task MemtableInsertParallelismFallsBackToSerialInsert()
    {
        MemoryFileSystem fs = NewFs();
        var merge = new ColumnFamilyOptions { MergeOperator = new UInt64AddMergeOperator() };
        DbOptions options = NewOptions(fs);
        await using RocksDatabase db = await RocksDatabase.OpenAsync(DbPath, options, merge);
        int ranges = 0;
        TestHooks.Set(ParallelRangeHook, () => Interlocked.Increment(ref ranges));
        try
        {
            var wo = new WriteOptions { MemtableInsertParallelism = 4 };
            // Merge operands keep the serial insert.
            using (WriteBatch b = db.CreateWriteBatch())
            {
                for (int i = 0; i < 2000; i++) b.Merge(B("c" + (i % 10)), BitConverter.GetBytes(1UL));
                await db.WriteAsync(wo, b);
            }
            Assert.AreEqual(200UL, BitConverter.ToUInt64(db.Get(B("c3"))!));
            // Too small to split.
            using (WriteBatch b = db.CreateWriteBatch())
            {
                for (int i = 0; i < 255; i++) b.Put(B("s" + i), B("v"));
                await db.WriteAsync(wo, b);
            }
            Assert.AreEqual("v", GetString(db, "s254"));
            Assert.AreEqual(0, ranges);
        }
        finally
        {
            TestHooks.Set(ParallelRangeHook, null);
        }
        Assert.ThrowsExactly<ArgumentOutOfRangeException>(() => new WriteOptions { MemtableInsertParallelism = 0 });
    }
}
