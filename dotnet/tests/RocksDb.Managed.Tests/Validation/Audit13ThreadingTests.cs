using RocksDb.Managed.Db;
using RocksDb.Managed.Utilities.Transactions;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #13, multi-threading: regression tests for the findings of the review of locks, signals and lock-free
/// structures against the C++ sources (docs/validation.md "Audit #13").
/// </summary>
[TestClass]
public sealed class Audit13ThreadingTests
{
    private static readonly WriteOptions s_wo = new();

    [TestMethod]
    [DataRow(TransactionWritePolicy.WriteCommitted)]
    [DataRow(TransactionWritePolicy.WritePrepared)]
    public void PreparedExpirableTransactionKeepsItsLocks(TransactionWritePolicy policy)
    {
        // PessimisticTransaction::Prepare (utilities/transactions/pessimistic_transaction.cc) only zeroes
        // expiration_time_: the transaction stays in expirable_transactions_map_, so a later
        // TryStealingExpiredTransactionLocks reaches TryStealingLocks, whose CAS from STARTED fails on PREPARED. The
        // port removed the transaction from the map at Prepare; "not in the map" reads as "gone", and the lock entry
        // still carries the expiration recorded when the key was locked, so another transaction took the key.
        var options = new TransactionDbOptions
        {
            WritePolicy = policy,
            TransactionLockTimeout = TimeSpan.FromMilliseconds(20),
            DefaultLockTimeout = TimeSpan.FromMilliseconds(20),
        };
        using TransactionDatabase db = TransactionDatabase.Open("/audit13txn", NewOptions(NewFs()), options);
        using Transaction t1 = db.BeginTransaction(s_wo, new TransactionOptions { Expiration = TimeSpan.FromMilliseconds(250) });
        t1.SetName("xid1");
        t1.Put(B("k"), B("1"));
        t1.Prepare();
        SpinWait.SpinUntil(() => false, 400); // well past the expiration recorded in the lock entry

        using (Transaction t2 = db.BeginTransaction(s_wo))
        {
            var e = Assert.ThrowsExactly<RocksDbException>(() => t2.Put(B("k"), B("2")));
            Assert.AreEqual(StatusCode.TimedOut, e.Code, e.Message);
        }
        t1.Commit();
        Assert.AreEqual("1", GetString(db.Database, "k"));

        // The committed transaction is out of the map: its (released) key is free again.
        using Transaction t3 = db.BeginTransaction(s_wo);
        t3.Put(B("k"), B("3"));
        t3.Commit();
        Assert.AreEqual("3", GetString(db.Database, "k"));
    }

    [TestMethod]
    [Timeout(60_000, CooperativeCancellation = true)]
    public void UnorderedWriteStaleDrainedSignalDoesNotReleaseTheMemtableSwitch()
    {
        // DBImpl::WaitForPendingWrites waits with a predicate (db_impl.h: switch_cv_.wait(guard, [&] { return
        // pending_memtable_writes_.load() == 0; })), so the notify of a writer that counted down to 0 before a later
        // group raised the count again is harmless. The port completed whatever waiter was registered: a writer
        // preempted between its decrement and its signal released a flush (memtable switch, close, ...) that was
        // waiting for a LATER writer's insert, which then ran against a switched memtable.
        var options = NewOptions(NewFs());
        options.UnorderedWrite = true;
        using RocksDatabase db = RocksDatabase.Open("/audit13unordered", options);
        using var firstCountedDown = new ManualResetEventSlim();
        using var releaseFirst = new ManualResetEventSlim();
        using var secondBeforeInsert = new ManualResetEventSlim();
        using var releaseSecond = new ManualResetEventSlim();
        using var flushRegistered = new ManualResetEventSlim();
        int signals = 0, inserts = 0;
        TestHooks.Set("DBImpl::UnorderedWriteMemtable:BeforeSignalDrained", () =>
        {
            if (Interlocked.Increment(ref signals) != 1) return;
            firstCountedDown.Set();
            releaseFirst.Wait();
        });
        TestHooks.Set("DBImpl::WriteImpl:BeforeUnorderedWriteMemtable", () =>
        {
            if (Interlocked.Increment(ref inserts) != 2) return;
            secondBeforeInsert.Set();
            releaseSecond.Wait();
        });
        TestHooks.Set("DBImpl::WaitForPendingWrites:Registered", flushRegistered.Set);
        try
        {
            Task first = Task.Factory.StartNew(() => db.Put(B("k1"), B("v1")), TaskCreationOptions.LongRunning);
            Assert.IsTrue(firstCountedDown.Wait(30_000)); // count is 0, the signal is still to come
            Task second = Task.Factory.StartNew(() => db.Put(B("k2"), B("v2")), TaskCreationOptions.LongRunning);
            Assert.IsTrue(secondBeforeInsert.Wait(30_000));
            Assert.AreEqual(1, db.Impl.PendingMemtableWritesForTest);
            Task flush = Task.Factory.StartNew(() => db.Flush(), TaskCreationOptions.LongRunning);
            Assert.IsTrue(flushRegistered.Wait(30_000));

            releaseFirst.Set(); // the stale signal
            first.Wait();
            Assert.IsFalse(flush.Wait(500), "the flush switched the memtable under a pending insert");
            Assert.AreEqual(1, db.Impl.PendingMemtableWritesForTest);

            releaseSecond.Set();
            second.Wait();
            flush.Wait();
            Assert.AreEqual("v1", GetString(db, "k1"));
            Assert.AreEqual("v2", GetString(db, "k2"));
        }
        finally
        {
            releaseFirst.Set();
            releaseSecond.Set();
            TestHooks.ClearAll();
        }
    }

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void WriteUnpreparedTransactionReadsItsOwnUnpublishedBatches(bool twoWriteQueues)
    {
        // DBImpl::GetImpl: "callback->Refresh(snapshot); snapshot = callback->max_visible_seq();" with the comment "The
        // unprep_seqs are not published for write unprepared, so it could be that max_visible_seq is larger. Seek to
        // the std::max of the two." The port refreshed the callback but kept reading at the published sequence, so with
        // two_write_queues (the main queue does not publish) a transaction did not see its own flushed batches.
        DbOptions o = NewOptions(NewFs());
        o.TwoWriteQueues = twoWriteQueues;
        var tdo = new TransactionDbOptions { WritePolicy = TransactionWritePolicy.WriteUnprepared };
        using TransactionDatabase db = TransactionDatabase.Open("/audit13unprep", o, tdo);
        db.Database.Put(B("k"), B("old"));
        using Transaction txn = db.BeginTransaction(s_wo, new TransactionOptions { WriteBatchFlushThreshold = 1 });
        txn.SetName("xid");
        txn.Put(B("k"), B("new"));
        txn.Put(B("k2"), B("new2")); // the first batch is in the DB as an unprepared batch by now
        Assert.AreEqual("new", S(txn.Get(B("k"))));
        Assert.AreEqual("old", GetString(db.Database, "k"));
        txn.Commit();
        Assert.AreEqual("new", GetString(db.Database, "k"));
    }

    [TestMethod]
    [Timeout(60_000, CooperativeCancellation = true)]
    public async Task SequenceWaitersWakeWhenAnUnbatchedOperationPublishesWalOnlySequences()
    {
        // WaitForSequenceAsync (WAL tailers) is a managed addition, so every site that raises LastSequence must signal.
        // With two_write_queues a commit marker written through the WAL-only queue allocates a sequence number without
        // publishing it; the next unbatched operation (here a flush) publishes it with SyncLastSequenceWithAllocated,
        // which did not wake the waiters: a tailer slept until the next main-queue write.
        DbOptions o = NewOptions(NewFs());
        o.TwoWriteQueues = true;
        var tdo = new TransactionDbOptions { WritePolicy = TransactionWritePolicy.WritePrepared };
        using TransactionDatabase db = TransactionDatabase.Open("/audit13seq", o, tdo);
        using (Transaction txn = db.BeginTransaction(s_wo))
        {
            txn.SetName("xid");
            txn.Put(B("k"), B("v"));
            txn.Prepare();
            txn.Commit(); // commit marker: WAL-only queue
        }
        DbImpl impl = db.Database.Impl;
        ulong published = impl.Versions.LastSequence;
        Assert.IsTrue(impl.Versions.LastAllocatedSequence > published, "the commit marker's sequence is not published yet");
        Task<bool> wait = impl.WaitForSequenceAsync(published + 1, CancellationToken.None).AsTask();
        Assert.IsFalse(wait.IsCompleted);
        db.Database.Flush();
        Assert.IsTrue(impl.Versions.LastSequence > published);
        Assert.IsTrue(await wait.WaitAsync(TimeSpan.FromSeconds(10)));
    }

    [TestMethod]
    [Timeout(60_000, CooperativeCancellation = true)]
    public async Task CancellationDoesNotAbandonTheCommitWriteOfATwoQueueRollback()
    {
        // With two_write_queues a WritePrepared / WriteUnprepared commit or rollback without prepare is two writes: the
        // data as a prepared batch (main queue), then a commit-only write (WAL-only queue) that publishes it. Upstream
        // has no cancellation; the port passed the caller's token to the second write too, so a cancellation between
        // the two left the first batch in the prepared heap forever (invisible until a reopen commits it from the WAL).
        // The second write only observes the token while it waits in a write stall, which this test does not set up:
        // it pins the behaviour (a token canceled between the two writes does not abandon the rollback) rather than
        // reproducing the stalled case.
        DbOptions o = NewOptions(NewFs());
        o.TwoWriteQueues = true;
        var tdo = new TransactionDbOptions { WritePolicy = TransactionWritePolicy.WriteUnprepared };
        using TransactionDatabase db = TransactionDatabase.Open("/audit13cancel", o, tdo);
        db.Database.Put(B("k"), B("old"));
        using var cts = new CancellationTokenSource();
        TestHooks.Set("WriteUnpreparedTxn::RollbackInternal:BeforeCommit", cts.Cancel);
        try
        {
            using Transaction txn = db.BeginTransaction(s_wo, new TransactionOptions { WriteBatchFlushThreshold = 1 });
            txn.SetName("xid");
            txn.Put(B("k"), B("new"));
            txn.Put(B("k2"), B("new2"));
            await txn.RollbackAsync(cts.Token);
        }
        finally
        {
            TestHooks.ClearAll();
        }
        Assert.IsTrue(cts.IsCancellationRequested, "the hook ran");
        Assert.AreEqual("old", GetString(db.Database, "k"));
        Assert.IsNull(GetString(db.Database, "k2"));
        // Nothing is left prepared: a new write to the rolled back keys is not blocked or hidden.
        db.Database.Put(B("k2"), B("after"));
        Assert.AreEqual("after", GetString(db.Database, "k2"));
        Assert.AreEqual(DbFormat.MaxSequenceNumber, db.WritePreparedDb!.PreparedHeapTop);
    }

    [TestMethod]
    [Timeout(120_000, CooperativeCancellation = true)]
    public void PointReadsRacingCloseSeeTheValueOrObjectDisposed()
    {
        // A Get that passed the disposed check reads through a table reader it does not hold a reference on (the
        // version pins it); the close must not hand that reader's index / filter blocks back to the pools under it.
        var fs = NewFs();
        var failures = new System.Collections.Concurrent.ConcurrentQueue<string>();
        const int Keys = 2000;
        for (int round = 0; round < 60 && failures.IsEmpty; round++)
        {
            DbOptions o = NewOptions(fs);
            RocksDatabase db = RocksDatabase.Open("/audit13close", o);
            if (round == 0)
            {
                for (int i = 0; i < Keys; i++) db.Put(B("key" + i.ToString("D6")), B("value" + i.ToString("D6")));
                db.Flush();
                db.CompactRange(null, null);
            }
            using var start = new ManualResetEventSlim();
            int running = 0;
            var readers = new Thread[4];
            for (int t = 0; t < readers.Length; t++)
            {
                int seed = round * 31 + t;
                readers[t] = new Thread(() =>
                {
                    var rnd = new Random(seed);
                    Interlocked.Increment(ref running);
                    start.Wait();
                    try
                    {
                        while (true)
                        {
                            int i = rnd.Next(Keys);
                            string? v = GetString(db, "key" + i.ToString("D6"));
                            if (v != "value" + i.ToString("D6")) failures.Enqueue("key " + i + " read " + (v ?? "<null>"));
                        }
                    }
                    catch (ObjectDisposedException)
                    {
                    }
                    catch (Exception e)
                    {
                        failures.Enqueue(e.GetType().Name + ": " + e.Message);
                    }
                });
                readers[t].Start();
            }
            SpinWait.SpinUntil(() => Volatile.Read(ref running) == readers.Length);
            start.Set();
            Thread.SpinWait(20_000 * (1 + round % 7));
            db.Dispose();
            foreach (Thread r in readers) r.Join();
        }
        Assert.IsTrue(failures.IsEmpty, string.Join(" | ", failures.Take(5)));
    }

    [TestMethod]
    public void ObsoleteSuperVersionIsNotStoredBackIntoASharedProcessorSlot()
    {
        // ColumnFamilyData::ReturnThreadLocalSuperVersion can only put back into the slot's own thread; the port's
        // slots are per processor and can be shared. Reader A holds the slot's super version; an install resets the
        // slot; reader B marks the slot in use; A's put then succeeded against B's mark and left the obsolete super
        // version (its memtables and version, so its files) referenced until the next read on that slot.
        using RocksDatabase db = RocksDatabase.Open("/audit13sv", NewOptions(NewFs()));
        db.Put(B("a"), B("1"));
        DbImpl impl = db.Impl;
        ColumnFamilyData cfd = db.DefaultColumnFamily.Data;
        SuperVersionCache cache = cfd.LocalSuperVersions;

        SuperVersion old = impl.GetAndRefSuperVersion(cfd, out int slot); // reader A
        Assert.IsTrue(slot >= 0);
        db.Flush(); // installs a new super version and resets the slots
        Assert.AreNotSame(old, cfd.Super);
        object?[] held = cache.TestOccupyAll(); // reader B (on every slot)
        impl.ReturnAndCleanupSuperVersion(cfd, old, slot); // A's put meets B's mark
        object?[] after = cache.TestOccupyAll();
        cache.TestRestore(held);
        foreach (object? o in after) Assert.AreNotSame(old, o, "an obsolete super version was left in a slot");
        Assert.AreEqual("1", GetString(db, "a"));
    }
}
