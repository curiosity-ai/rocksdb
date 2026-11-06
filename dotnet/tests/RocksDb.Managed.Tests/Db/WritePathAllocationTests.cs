using System.Buffers.Binary;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Memory;
using RocksDb.Managed.Utilities.Transactions;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>
/// Steady-state allocations of the write path and the transaction layer (concurrent async writers, pooled
/// <see cref="WriteBatchWithIndex"/>, transactions begun without an old transaction, optimistic commits with memtable
/// history, 2PC names) and the behavior of the pooled objects behind them. The allocation bounds are checked in
/// optimized builds only: Debug builds compile async state machines as classes, so every write allocates there.
/// </summary>
[TestClass]
public sealed class WritePathAllocationTests
{
    private static readonly WriteOptions s_noWal = new() { DisableWal = true };

    private static byte[] Key(int i)
    {
        byte[] k = new byte[16];
        BinaryPrimitives.WriteInt64BigEndian(k, i);
        return k;
    }

    private static byte[][] Keys(int n)
    {
        byte[][] keys = new byte[n][];
        for (int i = 0; i < n; i++) keys[i] = Key(i);
        return keys;
    }

    // The memtable's arena grows with the data. Its blocks come from ArenaSlabPool: the measured loops run with the
    // pool primed (PrimeArenaSlabs), so that growth is not counted. Slack: a couple of arena blocks and the memtable's
    // other bookkeeping.
    private const int ArenaBlock = 64 << 10;
    private const long Slack = 128 << 10;

    private static ColumnFamilyOptions BigMemtable() => new() { WriteBufferSize = 256UL << 20, ArenaBlockSize = ArenaBlock };

    /// <summary>Makes sure <see cref="ArenaSlabPool"/> holds enough free blocks of the memtables' size for the next run.</summary>
    private static void PrimeArenaSlabs()
    {
        int size = Arena.OptimizeBlockSize(ArenaBlock);
        var slabs = new byte[256][];
        // The pool is bounded: make room if earlier tests filled it with slabs of other sizes.
        if (ArenaSlabPool.PooledBytes + (long)slabs.Length * size > ArenaSlabPool.MaxPooledBytes) ArenaSlabPool.Trim();
        for (int i = 0; i < slabs.Length; i++) slabs[i] = ArenaSlabPool.Rent(size);
        foreach (byte[] slab in slabs) ArenaSlabPool.Return(slab);
    }

    /// <summary>Bytes allocated by the calling thread while running <paramref name="body"/> (arena slabs primed first).</summary>
    private static long Allocated(Action body)
    {
        PrimeArenaSlabs();
        long before = GC.GetAllocatedBytesForCurrentThread();
        body();
        return GC.GetAllocatedBytesForCurrentThread() - before;
    }

    // ---- concurrent async writers (LeadWriteGroupAsync's box, parallel memtable groups) ------------------------

    [TestMethod]
    [DataRow(false, true)]
    [DataRow(false, false)]
    [DataRow(true, true)]
    public async Task ConcurrentAsyncWritersWriteEverythingAndStopAllocating(bool pipelined, bool concurrentMemtable)
    {
        // Real files and the WAL on: the WAL writes complete asynchronously, like in production.
        string dir = NewTempDir("wpc-async");
        try
        {
            await ConcurrentAsyncWriters(dir, pipelined, concurrentMemtable);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    private static string NewTempDir(string prefix)
    {
        string dir = Path.Combine(Path.GetTempPath(), prefix + "-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    private static async Task ConcurrentAsyncWriters(string dir, bool pipelined, bool concurrentMemtable)
    {
        var o = new DbOptions { CreateIfMissing = true, EnablePipelinedWrite = pipelined, AllowConcurrentMemtableWrite = concurrentMemtable };
        var wo = new WriteOptions();
        using RocksDatabase db = RocksDatabase.Open(dir, o, BigMemtable());
        const int Tasks = 16, PerTask = 3000;
        byte[][] keys = Keys(Tasks * PerTask);
        byte[] value = new byte[100];
        var tasks = new Task[Tasks];
        async Task Run(int round)
        {
            for (int t = 0; t < Tasks; t++)
            {
                int w = t;
                tasks[t] = Task.Run(async () =>
                {
                    // Mixed sync and async writers in the same write groups.
                    for (int i = 0; i < PerTask; i++)
                    {
                        byte[] k = keys[w * PerTask + i];
                        if ((i & 7) == 7) db.Put(wo, k, value);
                        else await db.PutAsync(wo, k, value).ConfigureAwait(false);
                    }
                });
            }
            await Task.WhenAll(tasks);
            _ = round;
        }
        await Run(0); // warm-up: writers, their state machine boxes, the thread pool
        await Run(1);
        PrimeArenaSlabs();
        long before = GC.GetTotalAllocatedBytes(precise: true);
        await Run(2);
        await Run(3);
        long allocated = GC.GetTotalAllocatedBytes(precise: true) - before;
        var buffer = new byte[100];
        for (int i = 0; i < keys.Length; i += 97)
        {
            Assert.IsTrue(db.TryGet(keys[i], buffer, out int len), "key " + i);
            Assert.AreEqual(value.Length, len);
        }
#if !DEBUG
        const int Puts = 2 * Tasks * PerTask;
        // All threads, so the Task.Run of the workers and the thread pool count too (about 30 KB here). Before the
        // leader's state machine was owned by its writer, the parallel memtable groups added 2-4 B per Put.
        Assert.IsTrue(allocated <= Slack, allocated + " B allocated by " + Puts + " Puts");
#else
        _ = allocated;
#endif
    }

    // ---- WriteBatchWithIndex.Rent / RocksDatabase.CreateWriteBatchWithIndex -----------------------------------

    [TestMethod]
    public void RentedWriteBatchWithIndexSteadyStateAllocatesLikeAReusedOne()
    {
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(NewFs()), BigMemtable());
        byte[][] keys = Keys(1000);
        byte[] value = new byte[64];
        int next = 0;
        long Rented(int rounds) => Allocated(() =>
        {
            for (int r = 0; r < rounds; r++)
            {
                using WriteBatchWithIndex b = db.CreateWriteBatchWithIndex();
                for (int i = 0; i < 10; i++) b.Put(keys[next++ % keys.Length], value);
                db.Write(s_noWal, b);
            }
        });
        var reused = new WriteBatchWithIndex();
        long Reused(int rounds) => Allocated(() =>
        {
            for (int r = 0; r < rounds; r++)
            {
                reused.Clear();
                for (int i = 0; i < 10; i++) reused.Put(keys[next++ % keys.Length], value);
                db.Write(s_noWal, reused);
            }
        });
        Rented(500);
        Reused(500);
        long rented = Rented(3000), baseline = Reused(3000);
        reused.Dispose();
#if !DEBUG
        // new WriteBatchWithIndex() per batch: ~1.1 KB each (3.3 MB here). The memtable arena grows in blocks.
        Assert.IsTrue(rented <= Slack && baseline <= Slack, "rented " + rented + " B, reused " + baseline + " B");
#else
        _ = rented;
        _ = baseline;
#endif
    }

    [TestMethod]
    public void RentedWriteBatchWithIndexIsResetAndRejectsUseAfterDispose()
    {
        var fs = NewFs();
        DbOptions o = NewOptions(fs);
        var reverse = new ColumnFamilyOptions { Comparator = Comparator.ReverseBytewise };
        using RocksDatabase db = RocksDatabase.Open(DbPath, o);
        ColumnFamily rev = db.CreateColumnFamily("rev", reverse);

        WriteBatchWithIndex b = WriteBatchWithIndex.Rent();
        Assert.IsFalse(b.OverwriteKey);
        b.Put(rev, B("a"), B("1")).Put(rev, B("b"), B("2")).Put(B("x"), B("3"));
        b.SetSavePoint();
        WriteBatchWithIndexIterator revIt = b.NewIterator(rev);
        revIt.SeekToFirst();
        Assert.AreEqual("b", S(revIt.Key.ToArray()), "reverse order in the rev column family");
        WriteBatch inner = b.WriteBatch;
        b.Dispose();
        b.Dispose(); // a second Dispose of a pooled batch does nothing
        Assert.ThrowsExactly<ObjectDisposedException>(() => b.Put(B("y"), B("4")));
        Assert.ThrowsExactly<ObjectDisposedException>(() => _ = b.WriteBatch);
        Assert.ThrowsExactly<ObjectDisposedException>(() => b.Clear());
        Assert.ThrowsExactly<ObjectDisposedException>(() => b.TryGetFromBatch(B("x"), new System.Buffers.ArrayBufferWriter<byte>()));

        // The next renter gets the batch empty, without the column family comparator of the previous user, in the
        // mode it asks for.
        using WriteBatchWithIndex again = WriteBatchWithIndex.Rent(overwriteKey: true);
        Assert.AreSame(b, again, "single-threaded rent after return reuses the batch");
        Assert.AreSame(inner, again.WriteBatch);
        Assert.IsTrue(again.OverwriteKey);
        Assert.AreEqual(0, again.Count);
        Assert.IsFalse(again.RollbackToSavePoint(), "no save point survives the return");
        Assert.IsFalse(again.TryGetFromBatch(B("x"), new System.Buffers.ArrayBufferWriter<byte>()));
        again.Put(B("k"), B("1")).Put(B("k"), B("2"));
        Assert.AreEqual(2, again.Count);
        int indexed = 0;
        WriteBatchWithIndexIterator it = again.NewIterator();
        for (it.SeekToFirst(); it.Valid; it.Next()) indexed++;
        Assert.AreEqual(1, indexed, "overwrite_key keeps one index entry per key");
        // Column family 1 is ordered bytewise now (the reverse comparator was forgotten).
        uint revId = rev.Id;
        Assert.AreSame(Comparator.Bytewise, again.Index.GetComparator(revId));
        db.Write(again);
        Assert.AreEqual("2", GetString(db, "k"));

        // A batch created with new is not pooled: Dispose releases it, and Rent never hands it out.
        var own = new WriteBatchWithIndex();
        own.Dispose();
        using WriteBatchWithIndex third = WriteBatchWithIndex.Rent();
        Assert.AreNotSame(own, third);
    }

    [TestMethod]
    public async Task RentedWriteBatchesWithIndexAreNotSharedAcrossThreads()
    {
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(NewFs()), BigMemtable());
        const int Tasks = 8, Rounds = 500;
        var tasks = new Task[Tasks];
        for (int t = 0; t < Tasks; t++)
        {
            int w = t;
            tasks[t] = Task.Run(async () =>
            {
                var value = new System.Buffers.ArrayBufferWriter<byte>();
                for (int r = 0; r < Rounds; r++)
                {
                    using WriteBatchWithIndex b = db.CreateWriteBatchWithIndex();
                    Assert.AreEqual(0, b.Count);
                    byte[] k = B("w" + w + "-" + r);
                    b.Put(k, k);
                    await Task.Yield();
                    value.Clear();
                    Assert.IsTrue(b.TryGetFromBatch(k, value));
                    CollectionAssert.AreEqual(k, value.WrittenSpan.ToArray());
                    Assert.AreEqual(1, b.Count, "nobody else wrote into this batch");
                    await db.WriteAsync(s_noWal, b);
                }
            });
        }
        await Task.WhenAll(tasks);
        Assert.AreEqual("w3-499", GetString(db, "w3-499"));
    }

    // ---- transactions begun without an old transaction ----------------------------------------------------------

    private static TransactionDatabase OpenTxnDb(MemoryFileSystem fs) =>
        TransactionDatabase.Open(DbPath, NewOptions(fs), new TransactionDbOptions(),
            [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, BigMemtable())]);

    private static OptimisticTransactionDatabase OpenOccDb(MemoryFileSystem fs, ColumnFamilyOptions cfo) =>
        OptimisticTransactionDatabase.Open(DbPath, NewOptions(fs), new OptimisticTransactionDbOptions(),
            [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, cfo)]);

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void NewTransactionsPerCommitReuseTheirInternals(bool optimistic)
    {
        MemoryFileSystem fs = NewFs();
        using TransactionDatabase? pdb = optimistic ? null : OpenTxnDb(fs);
        using OptimisticTransactionDatabase? odb = optimistic ? OpenOccDb(fs, BigMemtable()) : null;
        RocksDatabase db = pdb?.Database ?? odb!.Database;
        byte[][] keys = Keys(4096);
        byte[] value = new byte[64];
        int next = 0;
        long Transactions(int rounds) => Allocated(() =>
        {
            for (int r = 0; r < rounds; r++)
            {
                using Transaction txn = optimistic ? odb!.BeginTransaction(s_noWal) : pdb!.BeginTransaction(s_noWal);
                for (int i = 0; i < 4; i++) txn.Put(keys[next++ % keys.Length], value);
                txn.Commit();
            }
        });
        Transactions(1100); // touches every key once (lazily created OCC lock buckets)
        const int Rounds = 4000;
        long txns = Transactions(Rounds);
        Assert.AreEqual(0, pdb?.GetLockStatusData().Count ?? 0);
#if !DEBUG
        // Before: ~3 KB per transaction (write batch + index + lock tracker tables); now the transaction object.
        long perTxn = txns / Rounds;
        Assert.IsTrue(perTxn <= 700, "B per transaction: " + perTxn);
#else
        _ = txns;
#endif
    }

    [TestMethod]
    public void DisposedTransactionsRejectUseAndDoNotLeakIntoTheNextOne()
    {
        MemoryFileSystem fs = NewFs();
        using TransactionDatabase db = OpenTxnDb(fs);
        Transaction a = db.BeginTransaction(s_noWal);
        a.Put(B("a1"), B("1"));
        a.SetSavePoint();
        a.Put(B("a2"), B("2"));
        a.SetName("xa");
        a.Dispose(); // uncommitted: discarded, locks released, name unregistered
        Assert.AreEqual(0, db.GetLockStatusData().Count);
        Assert.IsNull(db.GetTransactionByName("xa"));
        Assert.ThrowsExactly<ObjectDisposedException>(() => a.Put(B("a3"), B("3")));
        Assert.ThrowsExactly<ObjectDisposedException>(() => _ = a.Count);
        Assert.ThrowsExactly<ObjectDisposedException>(() => _ = a.WriteBatch);
        Assert.ThrowsExactly<ObjectDisposedException>(() => a.Commit());
        a.Dispose();

        // Interleaved transactions: b takes a's internals, c gets fresh (or other recycled) ones.
        Transaction b = db.BeginTransaction(s_noWal);
        Transaction c = db.BeginTransaction(s_noWal);
        Assert.AreNotSame(a, b);
        Assert.AreEqual(0, b.Count, "a's writes do not show up in the next transaction");
        Assert.IsFalse(b.RollbackToSavePoint(), "nor its save points");
        var value = new System.Buffers.ArrayBufferWriter<byte>();
        Assert.IsFalse(b.TryGet(null, B("a1"), value));
        b.Put(B("b"), B("1"));
        c.Put(B("c"), B("1"));
        Assert.AreEqual(1, b.Count);
        Assert.AreEqual(1, c.Count);
        Assert.AreEqual(2, db.GetLockStatusData().Count);
        // 2PC on a recycled batch: the begin-prepare placeholder is back after the reset.
        b.SetName("xb");
        b.Prepare();
        c.Dispose();
        b.Commit();
        b.Dispose();
        Assert.AreEqual("1", GetString(db.Database, "b"));
        Assert.IsNull(GetString(db.Database, "c"));
        Assert.IsNull(GetString(db.Database, "a1"));
        Assert.AreEqual(0, db.GetLockStatusData().Count);

        // A transaction with a batch size limit does not pass its limit on.
        Transaction limited = db.BeginTransaction(s_noWal, new TransactionOptions { MaxWriteBatchSize = 64 });
        var e = Assert.ThrowsExactly<RocksDbException>(() => limited.Put(B("big"), new byte[100]));
        Assert.AreEqual(StatusCode.Aborted, e.Code);
        limited.Dispose();
        using (Transaction unlimited = db.BeginTransaction(s_noWal))
        {
            unlimited.Put(B("big"), new byte[100]);
            unlimited.Commit();
        }
        Assert.IsTrue(db.Database.TryGet(B("big"), value));
    }

    [TestMethod]
    public void TransactionsSurviveReopenWithRecycledBatches()
    {
        MemoryFileSystem fs = NewFs();
        var sync = new WriteOptions { Sync = true };
        using (TransactionDatabase db = OpenTxnDb(fs))
        {
            for (int i = 0; i < 20; i++)
            {
                using Transaction t = db.BeginTransaction(sync);
                t.Put(B("k" + i), B("v" + i));
                if (i % 3 == 0)
                {
                    t.SetName("x" + i);
                    t.Prepare();
                }
                if (i % 5 != 4) t.Commit(); // every fifth one is rolled back by Dispose (prepared ones stay prepared)
            }
        }
        using TransactionDatabase reopened = OpenTxnDb(fs);
        for (int i = 0; i < 20; i++)
        {
            string? expected = i % 5 != 4 ? "v" + i : null;
            Assert.AreEqual(expected, GetString(reopened.Database, "k" + i), "k" + i);
        }
        // i = 9 was prepared and not committed: recovered as prepared.
        Transaction? recovered = reopened.GetTransactionByName("x9");
        Assert.IsNotNull(recovered);
        recovered.Commit();
        Assert.AreEqual("v9", GetString(reopened.Database, "k9"));
    }

    [TestMethod]
    public async Task ConcurrentTransactionsBegunAndDisposedOnManyThreads()
    {
        MemoryFileSystem fs = NewFs();
        using TransactionDatabase pdb = OpenTxnDb(fs);
        const int Tasks = 8, Rounds = 300;
        var tasks = new Task[Tasks];
        for (int t = 0; t < Tasks; t++)
        {
            int w = t;
            tasks[t] = Task.Run(async () =>
            {
                for (int r = 0; r < Rounds; r++)
                {
                    using Transaction txn = pdb.BeginTransaction(s_noWal);
                    Assert.AreEqual(0, txn.Count);
                    await txn.PutAsync(B("w" + w + "-" + r), B("v" + r));
                    await Task.Yield();
                    Assert.AreEqual(1, txn.Count);
                    if (r % 4 == 3) continue; // disposed uncommitted
                    await txn.CommitAsync();
                }
            });
        }
        await Task.WhenAll(tasks);
        Assert.AreEqual(0, pdb.GetLockStatusData().Count);
        for (int w = 0; w < Tasks; w++)
        {
            Assert.AreEqual("v298", GetString(pdb.Database, "w" + w + "-298"));
            Assert.IsNull(GetString(pdb.Database, "w" + w + "-299"));
        }
    }

    // ---- optimistic commits with memtable history, 2PC names --------------------------------------------------

    [TestMethod]
    public void OptimisticCommitsWithMemtableHistoryDoNotAllocate()
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cfo = BigMemtable();
        cfo.MaxWriteBufferSizeToMaintain = 1L << 30;
        using OptimisticTransactionDatabase odb = OpenOccDb(fs, cfo);
        RocksDatabase db = odb.Database;
        byte[][] keys = Keys(4096);
        byte[] value = new byte[64];
        // Two flushed memtables kept as history for conflict checks.
        for (int f = 0; f < 2; f++)
        {
            for (int i = 0; i < 100; i++) db.Put(keys[i], value);
            db.Flush();
        }
        var options = new OptimisticTransactionOptions();
        Transaction txn = odb.BeginTransaction(s_noWal, options);
        int next = 0;
        long Commits(int rounds) => Allocated(() =>
        {
            for (int r = 0; r < rounds; r++)
            {
                txn = odb.BeginTransaction(s_noWal, options, txn);
                for (int i = 0; i < 4; i++) txn.Put(keys[next++ % keys.Length], value);
                txn.Commit();
            }
        });
        Commits(1100); // touches every key once: the OCC lock buckets of the keys are created lazily
        long commits = Commits(10000);
        // A conflict is still detected through the history.
        Transaction conflicting = odb.BeginTransaction(s_noWal, new OptimisticTransactionOptions { SetSnapshot = true });
        conflicting.Put(keys[0], value);
        db.Put(keys[0], value);
        var e = Assert.ThrowsExactly<RocksDbException>(() => conflicting.Commit());
        Assert.AreEqual(StatusCode.Busy, e.Code);
        conflicting.Dispose();
        txn.Dispose();
#if !DEBUG
        // Before: a MemTable[] copy of the history per commit (40 B here, 400 KB over 10000 commits).
        Assert.IsTrue(commits <= Slack, "10000 commits: " + commits + " B");
#else
        _ = commits;
#endif
    }

    [TestMethod]
    public void TwoPhaseCommitNamesAreEncodedWithoutAllocating()
    {
        // Real files: a MemoryFileSystem WAL grows in memory with every prepare and commit marker.
        string dir = NewTempDir("wpc-2pc");
        try
        {
            TwoPhaseCommitNames(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    private static TransactionDatabase OpenTxnDb(string dir) =>
        TransactionDatabase.Open(dir, new DbOptions { CreateIfMissing = true }, new TransactionDbOptions(),
            [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, BigMemtable())]);

    private static void TwoPhaseCommitNames(string dir)
    {
        string[] names = new string[64];
        for (int i = 0; i < names.Length; i++) names[i] = i % 2 == 0 ? "xid-" + i : "xid-é中-" + new string('n', i * 7);
        long commits;
        using (TransactionDatabase db = OpenTxnDb(dir))
        {
            byte[][] keys = Keys(4096);
            byte[] value = new byte[64];
            var options = new TransactionOptions();
            Transaction txn = db.BeginTransaction(s_noWal, options);
            int next = 0, x = 0;
            long Commits(int rounds) => Allocated(() =>
            {
                for (int r = 0; r < rounds; r++)
                {
                    txn = db.BeginTransaction(s_noWal, options, txn);
                    txn.SetName(names[x++ & 63]);
                    for (int i = 0; i < 4; i++) txn.Put(keys[next++ % keys.Length], value);
                    txn.Prepare();
                    txn.Commit();
                }
            });
            Commits(500);
            commits = Commits(8000);
            txn.Dispose();

            // The markers carry the current name (a longer name after a shorter one, and non-ASCII names), through
            // recovery of a prepared transaction.
            var sync = new WriteOptions { Sync = true };
            using Transaction t1 = db.BeginTransaction(sync, options);
            t1.SetName("s");
            t1.Put(B("short"), B("1"));
            t1.Prepare();
            t1.Commit();
            Transaction t2 = db.BeginTransaction(sync, options, t1);
            t2.SetName(names[63]);
            t2.Put(B("long"), B("1"));
            t2.Prepare();
            Assert.AreSame(t2, db.GetTransactionByName(names[63]));
            Assert.IsNull(db.GetTransactionByName("s"));
            // t2 stays prepared in the WAL (disposing it, through t1's using, does not roll it back).
            var dup = Assert.ThrowsExactly<RocksDbException>(() => db.BeginTransaction(sync, options).SetName(names[63]));
            Assert.AreEqual(StatusCode.InvalidArgument, dup.Code);
        }
        using (TransactionDatabase reopened = OpenTxnDb(dir))
        {
            Transaction? recovered = reopened.GetTransactionByName(names[63]);
            Assert.IsNotNull(recovered, "the prepared transaction is recovered under its UTF-8 name");
            recovered.Commit();
            Assert.AreEqual("1", GetString(reopened.Database, "long"));
            Assert.AreEqual("1", GetString(reopened.Database, "short"));
        }
#if !DEBUG
        // Before: Encoding.UTF8.GetBytes per name change (32+ B per transaction).
        Assert.IsTrue(commits <= Slack, "8000 2PC transactions: " + commits + " B");
#else
        _ = commits;
#endif
    }
}
