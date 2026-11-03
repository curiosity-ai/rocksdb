using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Utilities.Transactions;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>
/// unordered_write (DBImpl::WriteImpl -> WriteImplWALOnly + UnorderedWriteMemtable): option checks, concurrent writers
/// under memtable / WAL switches, the documented snapshot relaxation and read-your-own-write, the pending-insert drain of
/// memtable switches (WaitForPendingWrites), and WritePrepared transactions on two write queues.
/// Crash consistency is covered by <see cref="DbCrashConsistencyTests"/>, native WAL recovery by the compat tests.
/// </summary>
[TestClass]
public sealed class UnorderedWriteTests
{
    private const string DbPath = "/db";

    private static DbOptions UnorderedOptions(IFileSystem fs)
    {
        DbOptions o = NewOptions(fs);
        o.UnorderedWrite = true;
        return o;
    }

    [TestCleanup]
    public void Cleanup() => TestHooks.ClearAll();

    /// <summary>
    /// The checks of DBImpl::ValidateOptions, ColumnFamilyData::ValidateOptions and TransactionDB::Open, with upstream's
    /// status codes.
    /// </summary>
    [TestMethod]
    public void IncompatibleOptionsAreRejected()
    {
        DbOptions o = UnorderedOptions(NewFs());
        o.AllowConcurrentMemtableWrite = false;
        var e = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbPath, o));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code, e.Message);

        o = UnorderedOptions(NewFs());
        o.EnablePipelinedWrite = true;
        e = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbPath, o));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code, e.Message);

        o = UnorderedOptions(NewFs());
        e = Assert.ThrowsExactly<RocksDbException>(() =>
            RocksDatabase.Open(DbPath, o, [new ColumnFamilyDescriptor("default", new ColumnFamilyOptions { MaxSuccessiveMerges = 2 })]));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code, e.Message);

        foreach ((TransactionWritePolicy policy, bool twoQueues) in new[]
                 {
                     (TransactionWritePolicy.WriteCommitted, true), (TransactionWritePolicy.WriteUnprepared, true),
                     (TransactionWritePolicy.WritePrepared, false),
                 })
        {
            o = UnorderedOptions(NewFs());
            o.TwoWriteQueues = twoQueues;
            e = Assert.ThrowsExactly<RocksDbException>(() => TransactionDatabase.Open(DbPath, o, new TransactionDbOptions { WritePolicy = policy }));
            Assert.AreEqual(StatusCode.NotSupported, e.Code, policy + ": " + e.Message);
        }

        // Accepted, with two write queues too, and persisted in the OPTIONS file.
        MemoryFileSystem fs = NewFs();
        o = UnorderedOptions(fs);
        o.TwoWriteQueues = true;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o))
        {
            db.Put(B("k"), B("v"));
            Assert.AreEqual("v", S(db.Get(B("k"))));
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            Assert.AreEqual("v", S(db.Get(B("k"))));
        }
        Assert.IsTrue(OptionsFileContains(fs, "unordered_write=true"));
    }

    private static bool OptionsFileContains(MemoryFileSystem fs, string text)
    {
        foreach (string name in fs.GetChildren(DbPath))
        {
            if (!name.StartsWith("OPTIONS-", StringComparison.Ordinal)) continue;
            using IRandomAccessFile f = fs.OpenRandomAccess(DbPath + "/" + name);
            byte[] buf = new byte[f.Length];
            f.Read(0, buf);
            if (S(buf)!.Contains(text, StringComparison.Ordinal)) return true;
        }
        return false;
    }

    /// <summary>
    /// Concurrent writers (sync and async API, some with per-key protection, merge operands and deletes, which are inserted
    /// concurrently too) over two column families with small memtables and a small max_total_wal_size, plus manual
    /// flushes: every memtable / WAL switch first drains the pending inserts. Every write must be readable, the last
    /// sequence must count every key, nothing may stay pending, and a reopen (WAL recovery) must see the same data.
    /// </summary>
    [TestMethod]
    [Timeout(60_000, CooperativeCancellation = true)]
    [DataRow(false)]
    [DataRow(true)]
    public async Task ConcurrentWritersWithMemtableAndWalSwitches(bool twoWriteQueues)
    {
        MemoryFileSystem fs = NewFs();
        DbOptions options = UnorderedOptions(fs);
        options.TwoWriteQueues = twoWriteQueues;
        options.CreateMissingColumnFamilies = true;
        options.MaxTotalWalSize = 256 * 1024;
        options.Statistics = new Statistics();
        var cfo = new ColumnFamilyOptions { WriteBufferSize = 64 * 1024, MaxWriteBufferNumber = 4, MergeOperator = new StringAppendOperator() };
        const int threads = 8, perThread = 400;
        ColumnFamilyDescriptor[] cfs = [new("default", cfo), new("cf1", cfo)];
        int waits = 0;
        TestHooks.Set("DBImpl::WaitForPendingWrites:BeforeBlock", () => Interlocked.Increment(ref waits));
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
                        if (i % 5 == 0) b.Put(cf, B("d" + id + "-" + i), B("x"));
                        if (i % 5 == 1) b.Delete(cf, B("d" + id + "-" + (i - 1)));
                        if (id % 2 == 0) db.Write(wo, b);
                        else db.WriteAsync(wo, b).AsTask().GetAwaiter().GetResult();
                        // Read-your-own-write: the write is in the memtables once it returned.
                        if (i % 11 == 0) CollectionAssert.AreEqual(Value(i, id), cf.Get(B("p" + id + "-" + i)));
                    }
                }, TaskCreationOptions.LongRunning);
            }
            await Task.WhenAll(tasks);
            stop.Cancel();
            await flusher;
            Assert.AreEqual(0, db.Impl.PendingMemtableWritesForTest);
            ulong keys = 0;
            for (int t = 0; t < threads; t++)
            {
                ColumnFamily cf = t % 2 == 0 ? db.DefaultColumnFamily : cf1;
                for (int i = 0; i < perThread; i++)
                {
                    CollectionAssert.AreEqual(Value(i, t), cf.Get(B("p" + t + "-" + i)), "t" + t + " i" + i);
                    keys += 1UL + (i % 7 == 0 ? 1UL : 0UL) + (i % 5 is 0 or 1 ? 1UL : 0UL);
                    if (i % 5 == 0) Assert.AreEqual(i == perThread - 1 ? "x" : null, S(cf.Get(B("d" + t + "-" + i))), "d" + t + "-" + i);
                }
                Assert.AreEqual(ExpectedMerge(perThread), S(cf.Get(B("m" + t))));
            }
            Assert.AreEqual(keys, db.GetLatestSequenceNumber());
            Assert.AreEqual(keys, options.Statistics.GetTickerCount(Ticker.NumberKeysWritten));
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, UnorderedOptions(fs), cfs))
        {
            ColumnFamily cf1 = db.GetColumnFamily("cf1");
            for (int t = 0; t < threads; t++)
            {
                ColumnFamily cf = t % 2 == 0 ? db.DefaultColumnFamily : cf1;
                for (int i = 0; i < perThread; i++) CollectionAssert.AreEqual(Value(i, t), cf.Get(B("p" + t + "-" + i)));
                Assert.AreEqual(ExpectedMerge(perThread), S(cf.Get(B("m" + t))));
            }
        }
        // Not asserted > 0: whether a switch ever finds an insert in flight depends on timing.
        Console.WriteLine("WaitForPendingWrites blocked " + waits + " times");

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
    /// The snapshot semantics documented for DBOptions::unordered_write, with the first write held between its WAL write
    /// and its memtable insert (DBImpl::WriteImpl:BeforeUnorderedWriteMemtable):
    /// <list type="bullet">
    /// <item>its sequence number is published at once, before its data is in the memtables, so a snapshot taken now
    /// covers it while reads at that snapshot do not see it yet;</item>
    /// <item>later writes complete without waiting for it and are read by their writer (read-your-own-write), but stay
    /// invisible to the earlier snapshot (larger sequence numbers);</item>
    /// <item>a flush first waits for the pending insert (WaitForPendingWrites);</item>
    /// <item>once the insert landed, the same snapshot sees it: snapshots are not immutable.</item>
    /// </list>
    /// </summary>
    [TestMethod]
    [Timeout(60_000, CooperativeCancellation = true)]
    public async Task SnapshotsAreNotImmutableButReadYourOwnWriteHolds()
    {
        using RocksDatabase db = RocksDatabase.Open(DbPath, UnorderedOptions(NewFs()));
        db.Put(B("k0"), B("v0"));
        using var inMem = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        using var flushBlocked = new ManualResetEventSlim();
        int calls = 0;
        TestHooks.Set("DBImpl::WriteImpl:BeforeUnorderedWriteMemtable", () =>
        {
            if (Interlocked.Increment(ref calls) != 1) return;
            inMem.Set();
            release.Wait();
        });
        TestHooks.Set("DBImpl::WaitForPendingWrites:BeforeBlock", flushBlocked.Set);
        try
        {
            Task first = Task.Factory.StartNew(() => db.Put(B("a"), B("va")), TaskCreationOptions.LongRunning);
            Assert.IsTrue(inMem.Wait(30_000));
            Assert.AreEqual(2UL, db.GetLatestSequenceNumber(), "published before the memtable insert");
            Assert.AreEqual(1, db.Impl.PendingMemtableWritesForTest);
            using Snapshot snap = db.GetSnapshot();
            Assert.AreEqual(2UL, snap.SequenceNumber);
            Assert.IsNull(GetString(db, "a"));
            Assert.IsNull(GetString(db, "a", snap));
            Assert.AreEqual("v0", GetString(db, "k0", snap));

            // A later write does not wait for the pending insert.
            await db.PutAsync(B("b"), B("vb"));
            Assert.AreEqual("vb", GetString(db, "b"), "read-your-own-write");
            Assert.IsNull(GetString(db, "b", snap), "a later write is invisible to an earlier snapshot");
            Assert.AreEqual(3UL, db.GetLatestSequenceNumber());
            Assert.IsFalse(first.IsCompleted);

            // A memtable switch waits for the pending insert.
            Task flush = db.FlushAsync().AsTask();
            Assert.IsTrue(flushBlocked.Wait(30_000), "flush did not wait for the pending insert");
            await Task.Delay(50);
            Assert.IsFalse(flush.IsCompleted);

            release.Set();
            await first;
            await flush;
            Assert.AreEqual(0, db.Impl.PendingMemtableWritesForTest);
            Assert.AreEqual("va", GetString(db, "a"));
            Assert.AreEqual("va", GetString(db, "a", snap), "the snapshot changed: an earlier write landed after it was taken");
            Assert.IsNull(GetString(db, "b", snap));
            Assert.AreEqual("1", db.GetProperty("rocksdb.num-files-at-level0"), "the flush wrote both writes");
        }
        finally
        {
            release.Set();
        }
    }

    /// <summary>
    /// WritePrepared transactions on two write queues with unordered_write (the combination upstream recommends to keep
    /// immutable snapshots): prepares go through the unordered main queue, commits through the WAL-only queue; plain
    /// writes interleave. Committed data is visible and recovered after reopen; rolled back data is not.
    /// </summary>
    [TestMethod]
    [Timeout(60_000, CooperativeCancellation = true)]
    public void WritePreparedOnTwoWriteQueues()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions options = UnorderedOptions(fs);
        options.TwoWriteQueues = true;
        var txnOptions = new TransactionDbOptions { WritePolicy = TransactionWritePolicy.WritePrepared };
        const int threads = 4, perThread = 60;
        using (TransactionDatabase tdb = TransactionDatabase.Open(DbPath, options, txnOptions))
        {
            var tasks = new Task[threads];
            for (int t = 0; t < threads; t++)
            {
                int id = t;
                tasks[t] = Task.Factory.StartNew(() =>
                {
                    for (int i = 0; i < perThread; i++)
                    {
                        using Transaction txn = tdb.BeginTransaction(new WriteOptions { Sync = i % 10 == 0 });
                        txn.SetName("x" + id + "-" + i);
                        txn.Put(B("t" + id + "-" + i), Value(i, id));
                        txn.Put(B("u" + id + "-" + i), Value(i, id));
                        txn.Prepare();
                        if (i % 5 == 4) txn.Rollback();
                        else txn.Commit();
                        tdb.Database.Put(B("plain" + id + "-" + i), B("x"));
                        using Snapshot s = tdb.Database.GetSnapshot();
                        byte[]? committed = tdb.Database.Get(new ReadOptions { Snapshot = s }, B("t" + id + "-" + i));
                        if (i % 5 == 4) Assert.IsNull(committed);
                        else CollectionAssert.AreEqual(Value(i, id), committed);
                    }
                }, TaskCreationOptions.LongRunning);
            }
            Task.WaitAll(tasks);
            tdb.Database.Flush();
        }
        using (TransactionDatabase tdb = TransactionDatabase.Open(DbPath, options, txnOptions))
        {
            for (int t = 0; t < threads; t++)
            {
                for (int i = 0; i < perThread; i++)
                {
                    byte[]? v = tdb.Database.Get(B("u" + t + "-" + i));
                    if (i % 5 == 4) Assert.IsNull(v);
                    else CollectionAssert.AreEqual(Value(i, t), v);
                    Assert.AreEqual("x", S(tdb.Database.Get(B("plain" + t + "-" + i))));
                }
            }
        }
    }
}
