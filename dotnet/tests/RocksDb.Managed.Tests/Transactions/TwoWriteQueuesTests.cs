using System.Globalization;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Utilities.Transactions;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Transactions;

/// <summary>
/// two_write_queues (DBOptions) with every write policy, and commit-time write batches. Ports of the two-queue cases of
/// utilities/transactions/write_prepared_transaction_test.cc (sequence publication by the second queue, the two-write
/// commit / rollback, recovery) and transaction_test.cc (commit-time batches, use_only_the_last_commit_time_batch_for_recovery),
/// plus concurrency tests: commits racing readers, flushes and memtable switches on both queues.
/// </summary>
[TestClass]
public sealed class TwoWriteQueuesTests
{
    private static readonly WriteOptions s_wo = new();
    private static readonly WriteOptions s_sync = new() { Sync = true };

    private static TransactionDatabase Open(MemoryFileSystem fs, TransactionWritePolicy policy, bool twoWriteQueues = true, ulong writeBufferSize = 64UL << 20,
        int commitCacheBits = 10)
    {
        DbOptions o = NewOptions(fs);
        o.TwoWriteQueues = twoWriteQueues;
        var cfo = new ColumnFamilyOptions { WriteBufferSize = writeBufferSize, MergeOperator = MergeOperator.StringAppend(",") };
        var tdo = new TransactionDbOptions
        {
            WritePolicy = policy,
            WpCommitCacheBits = commitCacheBits,
            WpSnapshotCacheBits = 2,
            TransactionLockTimeout = TimeSpan.FromSeconds(5),
            DefaultLockTimeout = TimeSpan.FromSeconds(5),
        };
        return TransactionDatabase.Open(DbPath, o, tdo, [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, cfo)]);
    }

    private static string Get(TransactionDatabase db, string key, Snapshot? snapshot = null) =>
        S(db.Database.Get(new ReadOptions { Snapshot = snapshot }, B(key))) ?? "NOT_FOUND";

    private static void Crash(MemoryFileSystem fs, TransactionDatabase db)
    {
        db.Database.WaitForCompact();
        fs.SimulateCrash();
    }

    private static VersionSet Versions(TransactionDatabase db) => db.Database.Impl.Versions;

    private static TransactionOptions Unprepared(TransactionWritePolicy policy) =>
        new() { WriteBatchFlushThreshold = policy == TransactionWritePolicy.WriteUnprepared ? 1 : 0 };

    // ---- sequence publication ------------------------------------------------------------------------------

    [TestMethod]
    public void CommitWithoutPreparePublishesFromTheSecondQueue()
    {
        using TransactionDatabase db = Open(NewFs(), TransactionWritePolicy.WritePrepared);
        DbImpl impl = db.Database.Impl;
        Assert.IsTrue(impl.TwoWriteQueues);
        ulong before = Versions(db).LastAllocatedSequence;
        using (Transaction t = db.BeginTransaction(s_wo))
        {
            t.Put(B("a"), B("1"));
            t.Put(B("b"), B("2"));
            t.Commit();
        }
        // WriteInternal: the data (one sub-batch) on the main queue, then one sequence for the WAL-less commit.
        Assert.AreEqual(before + 2, Versions(db).LastAllocatedSequence);
        Assert.AreEqual(before + 1, Versions(db).LastSequence);
        Assert.AreEqual(before + 2, impl.LastPublishedSequence);
        Assert.AreEqual(DbFormat.MaxSequenceNumber, db.WritePreparedDb!.PreparedHeapTop);
        Assert.AreEqual("1", Get(db, "a"));
        Assert.AreEqual("2", Get(db, "b"));

        // Plain writes (no transaction) take the same two-write path.
        db.Database.Put(B("c"), B("3"));
        Assert.AreEqual(before + 4, impl.LastPublishedSequence);
        Assert.AreEqual("3", Get(db, "c"));
        Assert.AreEqual(DbFormat.MaxSequenceNumber, db.WritePreparedDb!.PreparedHeapTop);
    }

    [TestMethod]
    [DataRow(TransactionWritePolicy.WritePrepared)]
    [DataRow(TransactionWritePolicy.WriteUnprepared)]
    public void PreparedDataIsPublishedByTheCommit(TransactionWritePolicy policy)
    {
        using TransactionDatabase db = Open(NewFs(), policy);
        DbImpl impl = db.Database.Impl;
        db.Database.Put(B("k"), B("old"));
        using Transaction t = db.BeginTransaction(s_wo, Unprepared(policy));
        t.SetName("x");
        t.Put(B("k"), B("new"));
        t.Put(B("j"), B("j1"));
        t.Prepare();
        ulong published = impl.LastPublishedSequence;
        // The prepared data is in the memtables (LastSequence) but neither visible nor published.
        Assert.IsTrue(Versions(db).LastSequence >= published);
        Assert.AreEqual("old", Get(db, "k"));
        Assert.AreEqual("NOT_FOUND", Get(db, "j"));
        using Snapshot before = db.Database.GetSnapshot();
        t.Commit();
        Assert.AreEqual(Versions(db).LastAllocatedSequence, impl.LastPublishedSequence);
        Assert.IsTrue(impl.LastPublishedSequence > published);
        Assert.AreEqual("new", Get(db, "k"));
        Assert.AreEqual("j1", Get(db, "j"));
        Assert.AreEqual("old", Get(db, "k", before));
        Assert.AreEqual(DbFormat.MaxSequenceNumber, db.WritePreparedDb!.PreparedHeapTop);
    }

    [TestMethod]
    [DataRow(TransactionWritePolicy.WritePrepared)]
    [DataRow(TransactionWritePolicy.WriteUnprepared)]
    public void RollbackCommitsWithASecondWrite(TransactionWritePolicy policy)
    {
        MemoryFileSystem fs = NewFs();
        {
            TransactionDatabase db = Open(fs, policy);
            db.Put(s_sync, null, B("k"), B("before"));
            Transaction t = db.BeginTransaction(s_sync, Unprepared(policy));
            t.SetName("r");
            t.Put(B("k"), B("during"));
            t.Put(B("n"), B("new"));
            t.Prepare();
            ulong allocated = Versions(db).LastAllocatedSequence;
            t.Rollback();
            // the rollback batch (main queue) and the data-less commit (second queue)
            Assert.IsTrue(Versions(db).LastAllocatedSequence >= allocated + 2);
            Assert.AreEqual(Versions(db).LastAllocatedSequence, db.Database.Impl.LastPublishedSequence);
            Assert.AreEqual("before", Get(db, "k"));
            Assert.AreEqual("NOT_FOUND", Get(db, "n"));
            Assert.AreEqual(DbFormat.MaxSequenceNumber, db.WritePreparedDb!.PreparedHeapTop);
            t.Dispose();
            Crash(fs, db);
        }
        using TransactionDatabase db2 = Open(fs, policy);
        Assert.AreEqual(0, db2.GetAllPreparedTransactions().Count);
        Assert.AreEqual("before", Get(db2, "k"));
        Assert.AreEqual("NOT_FOUND", Get(db2, "n"));
    }

    // ---- recovery ------------------------------------------------------------------------------------------

    [TestMethod]
    [DataRow(TransactionWritePolicy.WriteCommitted)]
    [DataRow(TransactionWritePolicy.WritePrepared)]
    [DataRow(TransactionWritePolicy.WriteUnprepared)]
    public void RecoveryAcrossQueueModes(TransactionWritePolicy policy)
    {
        // Written with two write queues, recovered with one and then with two again (the WAL format does not depend on
        // the queue mode).
        MemoryFileSystem fs = NewFs();
        {
            TransactionDatabase db = Open(fs, policy);
            db.Put(s_sync, null, B("plain"), B("p"));
            using (Transaction t0 = db.BeginTransaction(s_sync))
            {
                t0.Put(B("t0"), B("v0"));
                t0.Put(B("t0"), B("v0b"));
                t0.Commit();
            }
            using (Transaction t1 = db.BeginTransaction(s_sync, Unprepared(policy)))
            {
                t1.SetName("c");
                t1.Put(B("t1"), B("v1"));
                t1.Delete(B("plain"));
                t1.Prepare();
                db.Put(s_sync, null, B("between"), B("b"));
                t1.Commit();
            }
            using (Transaction t2 = db.BeginTransaction(s_sync, Unprepared(policy)))
            {
                t2.SetName("r");
                t2.Put(B("t2"), B("v2"));
                t2.Prepare();
                t2.Rollback();
            }
            Transaction t3 = db.BeginTransaction(s_sync, Unprepared(policy));
            t3.SetName("p");
            t3.Put(B("t3"), B("v3"));
            t3.Put(B("t3b"), B("v3b"));
            t3.Prepare();
            Crash(fs, db);
        }
        foreach (bool two in new[] { false, true })
        {
            TransactionDatabase db = Open(fs, policy, twoWriteQueues: two);
            Assert.AreEqual("v0b", Get(db, "t0"));
            Assert.AreEqual("v1", Get(db, "t1"));
            Assert.AreEqual("b", Get(db, "between"));
            Assert.AreEqual("NOT_FOUND", Get(db, "plain"));
            Assert.AreEqual("NOT_FOUND", Get(db, "t2"));
            Assert.AreEqual("NOT_FOUND", Get(db, "t3"));
            Assert.AreEqual("p", db.GetAllPreparedTransactions().Single().Name);
            Crash(fs, db);
        }
        using (TransactionDatabase db = Open(fs, policy))
        {
            Transaction p = db.GetAllPreparedTransactions().Single();
            p.Commit();
            Assert.AreEqual("v3", Get(db, "t3"));
            Assert.AreEqual("v3b", Get(db, "t3b"));
            Assert.AreEqual(db.Database.Impl.LastPublishedSequence, Versions(db).LastAllocatedSequence);
            // writes after recovery keep working on both queues
            using Transaction t = db.BeginTransaction(s_wo);
            t.Put(B("after"), B("a"));
            t.Commit();
            Assert.AreEqual("a", Get(db, "after"));
        }
    }

    [TestMethod]
    public void WriteCommittedPrepareUsesTheSecondQueueWithoutSequenceNumbers()
    {
        using TransactionDatabase db = Open(NewFs(), TransactionWritePolicy.WriteCommitted);
        ulong allocated = Versions(db).LastAllocatedSequence;
        using Transaction t = db.BeginTransaction(s_wo);
        t.SetName("wc");
        t.Put(B("a"), B("1"));
        t.Put(B("b"), B("2"));
        t.Prepare();
        // kDontAssignOrder: a WriteCommitted prepare consumes no sequence numbers.
        Assert.AreEqual(allocated, Versions(db).LastAllocatedSequence);
        Assert.AreEqual("NOT_FOUND", Get(db, "a"));
        t.Commit();
        Assert.AreEqual(allocated + 2, Versions(db).LastAllocatedSequence);
        Assert.AreEqual(Versions(db).LastSequence, db.Database.Impl.LastPublishedSequence);
        Assert.AreEqual("1", Get(db, "a"));
        Assert.AreEqual("2", Get(db, "b"));
    }

    // ---- concurrency ---------------------------------------------------------------------------------------

    /// <summary>
    /// Writers commit transactions (with and without prepare, some rolled back) on both queues while readers check that
    /// every snapshot sees whole transactions only and never changes; a small write buffer makes the main queue switch
    /// memtables and WALs while the WAL-only queue appends commit markers.
    /// </summary>
    [TestMethod]
    [DataRow(TransactionWritePolicy.WriteCommitted, false)]
    [DataRow(TransactionWritePolicy.WritePrepared, false)]
    [DataRow(TransactionWritePolicy.WriteUnprepared, false)]
    [DataRow(TransactionWritePolicy.WritePrepared, true)]
    [DataRow(TransactionWritePolicy.WriteUnprepared, true)]
    public async Task CommitsRacingReaders(TransactionWritePolicy policy, bool async)
    {
        MemoryFileSystem fs = NewFs();
        const int Keys = 12;
        const int Writers = 3;
        TransactionDatabase db = Open(fs, policy, writeBufferSize: 64 << 10, commitCacheBits: 4);
        for (int i = 0; i < Keys; i++) db.Put(s_wo, null, B("k" + i), B("0"));
        int stop = 0;
        var failures = new System.Collections.Concurrent.ConcurrentQueue<Exception>();
        var lastCommitted = new int[Writers];
        var tasks = new List<Task>();
        for (int w = 0; w < Writers; w++)
        {
            int id = w;
            tasks.Add(Task.Run(async () =>
            {
                try
                {
                    string pad = new('x', 200);
                    for (int r = 1; Volatile.Read(ref stop) == 0 && r < 400; r++)
                    {
                        using Transaction t = db.BeginTransaction(r % 5 == 0 ? s_sync : s_wo, Unprepared(policy));
                        t.SetName("w" + id + "-" + r.ToString(CultureInfo.InvariantCulture));
                        string v = r.ToString(CultureInfo.InvariantCulture) + pad;
                        // Every writer owns a slice of the keys and writes them all with the same round.
                        for (int k = id; k < Keys; k += Writers)
                        {
                            if (async) await t.PutAsync(B("k" + k), B(v));
                            else t.Put(B("k" + k), B(v));
                        }
                        if (r % 7 == 3)
                        {
                            t.Prepare();
                            t.Rollback();
                            continue;
                        }
                        if (r % 2 == 0) t.Prepare();
                        if (async) await t.CommitAsync();
                        else t.Commit();
                        Volatile.Write(ref lastCommitted[id], r);
                    }
                }
                catch (Exception e)
                {
                    failures.Enqueue(e);
                }
            }));
        }
        tasks.Add(Task.Run(() =>
        {
            try
            {
                for (int i = 0; i < 300 && failures.IsEmpty; i++)
                {
                    using Snapshot s = db.Database.GetSnapshot();
                    var seen = new string[Keys];
                    for (int k = 0; k < Keys; k++) seen[k] = Get(db, "k" + k, s);
                    for (int w = 0; w < Writers; w++)
                    {
                        for (int k = w + Writers; k < Keys; k += Writers) Assert.AreEqual(seen[w], seen[k], "torn transaction in a snapshot");
                    }
                    for (int k = 0; k < Keys; k++) Assert.AreEqual(seen[k], Get(db, "k" + k, s), "snapshot changed");
                    // A read without a snapshot sees at least the last commit each writer finished before it.
                    int[] floor = (int[])lastCommitted.Clone();
                    for (int w = 0; w < Writers; w++)
                    {
                        string latest = GetLatest(db, "k" + w);
                        int round = int.Parse(latest.Split('x')[0], CultureInfo.InvariantCulture);
                        Assert.IsTrue(round >= floor[w], "read missed a finished commit");
                    }
                }
            }
            catch (Exception e)
            {
                failures.Enqueue(e);
            }
            finally
            {
                Volatile.Write(ref stop, 1);
            }
        }));
        await Task.WhenAll(tasks);
        if (!failures.IsEmpty) throw new AssertFailedException("concurrent failure: " + failures.First(), failures.First());
        if (policy != TransactionWritePolicy.WriteCommitted) Assert.AreEqual(Versions(db).LastAllocatedSequence, LastVisible(db));
        var expected = new string[Keys];
        // Through a snapshot: with a tiny commit cache max_evicted_seq can reach the last published sequence, which a
        // read without a snapshot reports as TryAgain (GetSnapshot advances the sequence instead).
        using (Snapshot final = db.Database.GetSnapshot())
        {
            for (int k = 0; k < Keys; k++) expected[k] = Get(db, "k" + k, final);
        }
        SyncWal(db);
        Crash(fs, db);
        using TransactionDatabase db2 = Open(fs, policy);
        for (int k = 0; k < Keys; k++) Assert.AreEqual(expected[k], Get(db2, "k" + k), "after recovery");
    }

    /// <summary>
    /// Regression (CommitsRacingReaders): with two write queues a WriteUnprepared rollback writes the rollback batch on
    /// the main queue and commits it (with the rolled back batches) by a second write. In between, max_evicted_seq can
    /// pass the rollback batch; it must still be uncommitted then, like the batches it cancels. It used to skip the
    /// prepared heap and read as committed, so snapshots saw half of the rollback and compactions kept an older value
    /// under it (both with sequence number 0: "keys must be added in strictly increasing internal key order").
    /// </summary>
    [TestMethod]
    public async Task WriteUnpreparedRollbackBatchIsUncommittedUntilItsCommit()
    {
        MemoryFileSystem fs = NewFs();
        using TransactionDatabase db = Open(fs, TransactionWritePolicy.WriteUnprepared);
        WritePreparedTxnDb wp = db.WritePreparedDb!;
        db.Put(s_wo, null, B("k"), B("v0"));
        using var paused = new ManualResetEventSlim(false);
        using var resume = new ManualResetEventSlim(false);
        TestHooks.Set("WriteUnpreparedTxn::RollbackInternal:BeforeCommit", () =>
        {
            paused.Set();
            resume.Wait();
        });
        try
        {
            using Transaction t = db.BeginTransaction(s_wo, Unprepared(TransactionWritePolicy.WriteUnprepared));
            t.SetName("rolled-back");
            t.Put(B("k"), B("v1"));
            t.Prepare();
            SortedDictionary<ulong, int> unprepared = ((WriteUnpreparedTxn)t).UnpreparedSequenceNumbers;
            ulong data = unprepared.Keys.First();
            Task rollback = Task.Run(() => t.Rollback());
            Assert.IsTrue(paused.Wait(TimeSpan.FromSeconds(30)), "rollback did not reach its second write");
            ulong rollbackSeq = unprepared.Keys.Last();
            Assert.IsGreaterThan(data, rollbackSeq);

            // A busy commit cache: max_evicted_seq passes the rollback batch before the rollback commits.
            wp.AdvanceMaxEvictedSeq(wp.MaxEvictedSeq, rollbackSeq);
            using (Snapshot s = db.Database.GetSnapshot())
            {
                bool released = false;
                Assert.IsFalse(wp.IsInSnapshot(data, s.SequenceNumber, s.MinUncommitted, ref released), "rolled back data");
                Assert.IsFalse(wp.IsInSnapshot(rollbackSeq, s.SequenceNumber, s.MinUncommitted, ref released), "uncommitted rollback batch");
                Assert.IsFalse(released);
                Assert.AreEqual("v0", Get(db, "k", s));
            }

            resume.Set();
            await rollback;
            using (Snapshot s = db.Database.GetSnapshot())
            {
                bool released = false;
                Assert.IsTrue(wp.IsInSnapshot(rollbackSeq, s.SequenceNumber, s.MinUncommitted, ref released), "committed rollback batch");
                Assert.AreEqual("v0", Get(db, "k", s));
            }
        }
        finally
        {
            resume.Set();
            TestHooks.ClearAll();
        }
    }

    /// <summary>
    /// A read without a snapshot. With a tiny commit cache it can fail with TryAgain (max_evicted_seq reached its
    /// sequence: TXN_GET_TRY_AGAIN), like upstream, and keeps failing while nothing is written; it then reads through a
    /// fresh snapshot (GetSnapshot advances the sequence past max_evicted_seq).
    /// </summary>
    private static string GetLatest(TransactionDatabase db, string key)
    {
        try
        {
            return Get(db, key);
        }
        catch (RocksDbException e) when (e.Code == StatusCode.TryAgain)
        {
            using Snapshot s = db.Database.GetSnapshot();
            return Get(db, key, s);
        }
    }

    /// <summary>
    /// Makes everything written so far durable before a simulated crash: a sync write syncs the current WAL (older
    /// WALs were synced when the memtable switch closed them).
    /// </summary>
    private static void SyncWal(TransactionDatabase db) => db.Put(s_sync, null, B("sync-marker"), B("1"));

    /// <summary>The last visible sequence: published (WritePrepared / WriteUnprepared) or the last sequence.</summary>
    private static ulong LastVisible(TransactionDatabase db) => db.Database.Impl.LastPublishedSequence;

    /// <summary>Flushes (memtable switches under both queues) while transactions commit on both queues.</summary>
    [TestMethod]
    public async Task FlushesRacingCommits()
    {
        MemoryFileSystem fs = NewFs();
        TransactionDatabase db = Open(fs, TransactionWritePolicy.WritePrepared);
        int stop = 0;
        var writers = new Task[2];
        for (int w = 0; w < writers.Length; w++)
        {
            int id = w;
            writers[w] = Task.Run(async () =>
            {
                for (int r = 0; Volatile.Read(ref stop) == 0; r++)
                {
                    using Transaction t = db.BeginTransaction(s_wo);
                    t.SetName("f" + id + "-" + r.ToString(CultureInfo.InvariantCulture));
                    t.Put(B("f" + id), B(r.ToString(CultureInfo.InvariantCulture)));
                    t.Prepare();
                    await t.CommitAsync();
                }
            });
        }
        for (int i = 0; i < 10; i++) await db.Database.FlushAsync();
        Volatile.Write(ref stop, 1);
        await Task.WhenAll(writers);
        string f0 = Get(db, "f0"), f1 = Get(db, "f1");
        SyncWal(db);
        Crash(fs, db);
        using TransactionDatabase db2 = Open(fs, TransactionWritePolicy.WritePrepared);
        Assert.AreEqual(f0, Get(db2, "f0"));
        Assert.AreEqual(f1, Get(db2, "f1"));
    }

    // ---- commit-time batches -------------------------------------------------------------------------------

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void WriteCommittedCommitTimeBatch(bool twoWriteQueues)
    {
        MemoryFileSystem fs = NewFs();
        {
            TransactionDatabase db = Open(fs, TransactionWritePolicy.WriteCommitted, twoWriteQueues);
            using (Transaction t = db.BeginTransaction(s_sync))
            {
                t.SetName("ct");
                t.Put(B("k"), B("txn"));
                t.Put(B("both"), B("txn"));
                t.Prepare();
                t.GetCommitTimeWriteBatch().Put(B("ct"), B("commit-time"));
                // The commit-time batch is inserted before the transaction's writes: the transaction wins.
                t.GetCommitTimeWriteBatch().Put(B("both"), B("ct"));
                Assert.AreEqual("NOT_FOUND", Get(db, "ct"));
                t.Commit();
                Assert.AreEqual(0, t.GetCommitTimeWriteBatch().Count);
            }
            Assert.AreEqual("commit-time", Get(db, "ct"));
            Assert.AreEqual("txn", Get(db, "k"));
            Assert.AreEqual("txn", Get(db, "both"));
            // Without prepare a non-empty commit-time batch is refused.
            using (Transaction t = db.BeginTransaction(s_sync))
            {
                t.Put(B("x"), B("1"));
                t.GetCommitTimeWriteBatch().Put(B("y"), B("1"));
                RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => t.Commit());
                Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
                Assert.AreEqual(TransactionState.Started, t.State);
                t.Rollback();
            }
            Crash(fs, db);
        }
        using TransactionDatabase db2 = Open(fs, TransactionWritePolicy.WriteCommitted, twoWriteQueues);
        Assert.AreEqual("commit-time", Get(db2, "ct"));
        Assert.AreEqual("txn", Get(db2, "both"));
        Assert.AreEqual("NOT_FOUND", Get(db2, "x"));
    }

    [TestMethod]
    [DataRow(TransactionWritePolicy.WritePrepared, false)]
    [DataRow(TransactionWritePolicy.WritePrepared, true)]
    [DataRow(TransactionWritePolicy.WriteUnprepared, false)]
    [DataRow(TransactionWritePolicy.WriteUnprepared, true)]
    public void CommitTimeBatchForRecoveryOnly(TransactionWritePolicy policy, bool twoWriteQueues)
    {
        MemoryFileSystem fs = NewFs();
        {
            TransactionDatabase db = Open(fs, policy, twoWriteQueues);
            // Refused without use_only_the_last_commit_time_batch_for_recovery.
            using (Transaction t = db.BeginTransaction(s_sync, Unprepared(policy)))
            {
                t.SetName("no");
                t.Put(B("a"), B("1"));
                t.Prepare();
                t.GetCommitTimeWriteBatch().Put(B("pos"), B("0"));
                RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => t.Commit());
                Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
                t.Rollback();
            }
            var options = Unprepared(policy);
            options.UseOnlyTheLastCommitTimeBatchForRecovery = true;
            for (int i = 1; i <= 3; i++)
            {
                using Transaction t = db.BeginTransaction(s_sync, options);
                t.SetName("ct" + i);
                t.Put(B("data" + i), B("v"));
                t.Prepare();
                t.GetCommitTimeWriteBatch().Put(B("pos"), B(i.ToString(CultureInfo.InvariantCulture)));
                t.Commit();
            }
            // The commit-time batch only went to the WAL: the transactions are visible, the batch is not (yet).
            Assert.AreEqual("v", Get(db, "data3"));
            Assert.AreEqual("NOT_FOUND", Get(db, "pos"));
            Assert.IsTrue(db.Database.Impl.HasCachedRecoverableState);
            // The memtable switch of a flush writes the last one to the memtables (WriteRecoverableState).
            db.Database.Flush();
            Assert.IsFalse(db.Database.Impl.HasCachedRecoverableState);
            Assert.AreEqual("3", Get(db, "pos"));
            using (Transaction t = db.BeginTransaction(s_sync, options))
            {
                t.SetName("ct4");
                t.Put(B("data4"), B("v"));
                t.Prepare();
                t.GetCommitTimeWriteBatch().Put(B("pos"), B("4"));
                t.Commit();
            }
            Assert.AreEqual("3", Get(db, "pos"));
            Crash(fs, db);
        }
        // WAL recovery replays the commit-time batch of the last commit.
        using TransactionDatabase db2 = Open(fs, policy, twoWriteQueues);
        Assert.AreEqual("4", Get(db2, "pos"));
        Assert.AreEqual("v", Get(db2, "data4"));
        Assert.AreEqual("NOT_FOUND", Get(db2, "a"));
    }
}
