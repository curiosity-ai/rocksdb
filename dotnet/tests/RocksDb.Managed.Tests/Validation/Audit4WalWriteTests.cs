using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Utilities.Transactions;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4, slice WalWrite (WAL, WriteBatch, write thread / write path, WAL iterator, write controller): regression
/// tests for the divergences found against db/write_thread.cc, db/db_impl/db_impl_write.cc and db/db_impl/db_impl.cc,
/// plus coverage of verified branches that had no test.
/// </summary>
[TestClass]
public sealed class Audit4WalWriteTests
{
    private const string DbPath = "/audit4walwrite";
    private const string DelayWriteWaitHook = "DBImpl::DelayWrite:Wait";

    private static WriteThread.Writer NewWriter(List<WriteBatch> batches, WriteOptions? options)
    {
        var b = new WriteBatch();
        b.Put(B("k"), B("v"));
        batches.Add(b);
        var w = new WriteThread.Writer();
        w.Prepare(b, options, syncWaiter: true);
        return w;
    }

    /// <summary>
    /// WriteThread::BeginWriteStall / LinkOne / EndWriteStall (db/write_thread.cc): when the leader blocks on a write
    /// stall, the queued writers with no_slowdown are removed from the queue and completed with Incomplete "Write
    /// stall", a no_slowdown writer that arrives during the stall fails the same way without being linked, and the other
    /// writers stay queued in order. The port had no barrier: such writers waited for the stall to end.
    /// </summary>
    [TestMethod]
    public void WriteStallBarrierFailsQueuedAndArrivingNoSlowdownWriters()
    {
        var wt = new WriteThread(maxWriteBatchGroupSizeBytes: 1 << 20, maxYieldMicros: 0);
        var batches = new List<WriteBatch>();
        var noSlowdownOptions = new WriteOptions { NoSlowdown = true };
        try
        {
            WriteThread.Writer leader = NewWriter(batches, null);
            Assert.IsTrue(wt.JoinBatchGroup(leader));
            WriteThread.Writer ns1 = NewWriter(batches, noSlowdownOptions);
            WriteThread.Writer n1 = NewWriter(batches, null);
            WriteThread.Writer ns2 = NewWriter(batches, noSlowdownOptions);
            WriteThread.Writer n2 = NewWriter(batches, null);
            WriteThread.Writer ns3 = NewWriter(batches, noSlowdownOptions); // the queue's tail
            foreach (WriteThread.Writer w in new[] { ns1, n1, ns2, n2, ns3 }) Assert.IsFalse(wt.JoinBatchGroup(w));
            var group = new List<WriteThread.Writer>();
            wt.EnterAsBatchGroupLeader(leader, group);
            CollectionAssert.AreEqual(new[] { leader, n1, n2 }, group); // no_slowdown writers never join this leader

            wt.BeginWriteStall();
            foreach (WriteThread.Writer w in new[] { ns1, ns2, ns3 })
            {
                Assert.AreEqual(WriteThread.StateCompleted, w.SyncState);
                Assert.AreEqual(StatusCode.Incomplete, ((RocksDbException)w.Error!).Code);
                StringAssert.Contains(w.Error!.Message, "Write stall");
            }
            // During the stall: a no_slowdown writer fails at once, others queue (also behind the removed tail).
            WriteThread.Writer ns4 = NewWriter(batches, noSlowdownOptions);
            Assert.IsFalse(wt.JoinBatchGroup(ns4));
            Assert.AreEqual(WriteThread.StateCompleted, ns4.SyncState);
            Assert.AreEqual(StatusCode.Incomplete, ((RocksDbException)ns4.Error!).Code);
            WriteThread.Writer n3 = NewWriter(batches, null);
            Assert.IsFalse(wt.JoinBatchGroup(n3));
            Assert.AreEqual(0, n3.SyncState);
            wt.EndWriteStall();

            // After the stall a no_slowdown writer queues again, and the queue kept its order.
            WriteThread.Writer ns5 = NewWriter(batches, noSlowdownOptions);
            Assert.IsFalse(wt.JoinBatchGroup(ns5));
            Assert.AreEqual(0, ns5.SyncState);
            Assert.IsNull(ns5.Error);
            wt.ExitAsBatchGroupLeader(group);
            Assert.AreEqual(WriteThread.StateCompleted, n1.SyncState);
            Assert.AreEqual(WriteThread.StateCompleted, n2.SyncState);
            Assert.IsNull(n1.Error);
            Assert.AreEqual(WriteThread.StateGroupLeader, n3.SyncState);
            wt.EnterAsBatchGroupLeader(n3, group);
            CollectionAssert.AreEqual(new[] { n3 }, group);
            wt.ExitAsBatchGroupLeader(group);
            Assert.AreEqual(WriteThread.StateGroupLeader, ns5.SyncState);
            wt.EnterAsBatchGroupLeader(ns5, group);
            wt.ExitAsBatchGroupLeader(group);
        }
        finally
        {
            foreach (WriteBatch b in batches) b.Dispose();
        }
    }

    /// <summary>
    /// DBImpl::DelayWrite (db_impl_write.cc) calls write_thread.BeginWriteStall() before it waits for stopped writes, so a
    /// WriteOptions::no_slowdown write issued while another writer is blocked by the stall returns Incomplete "Write
    /// stall" immediately (DBWriteTest.WriteThreadHangOnWriteStall / LockWALInEffect in db/db_write_test.cc). The port
    /// queued it behind the stalled leader, so it blocked until the stall ended and then succeeded.
    /// </summary>
    [TestMethod]
    [Timeout(60_000, CooperativeCancellation = true)]
    public void NoSlowdownWriteFailsWhileAnotherWriterIsStalled()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        db.Put(B("a"), B("1"));
        using var stalled = new ManualResetEventSlim();
        TestHooks.Set(DelayWriteWaitHook, stalled.Set);
        Task blockedWrite;
        Task<RocksDbException?> noSlowdown;
        try
        {
            WalLock walLock = db.LockWal(); // write stop token
            try
            {
                blockedWrite = Task.Run(() => db.Put(B("b"), B("2")));
                Assert.IsTrue(stalled.Wait(TimeSpan.FromSeconds(30)), "the writer never reached the write stall");
                noSlowdown = Task.Run(() =>
                {
                    try
                    {
                        db.Put(new WriteOptions { NoSlowdown = true }, B("c"), B("3"));
                        return null;
                    }
                    catch (RocksDbException e)
                    {
                        return e;
                    }
                });
                bool returned = noSlowdown.Wait(TimeSpan.FromSeconds(5));
                Assert.IsFalse(blockedWrite.IsCompleted);
                Assert.IsTrue(returned, "a no_slowdown write waited behind a stalled writer");
            }
            finally
            {
                walLock.Dispose();
            }
        }
        finally
        {
            TestHooks.Set(DelayWriteWaitHook, null);
        }
        Assert.IsTrue(blockedWrite.Wait(TimeSpan.FromSeconds(30)));
        RocksDbException? error = noSlowdown.Result;
        Assert.IsNotNull(error);
        Assert.AreEqual(StatusCode.Incomplete, error.Code);
        StringAssert.Contains(error.Message, "Write stall");
        Assert.AreEqual("2", GetString(db, "b"));
        Assert.IsNull(GetString(db, "c"));
        // The barrier is gone with the stall.
        db.Put(new WriteOptions { NoSlowdown = true }, B("c"), B("3"));
        Assert.AreEqual("3", GetString(db, "c"));
    }

    /// <summary>
    /// The async write path goes through the same barrier (WriteThread::LinkOne): an awaited no_slowdown write completes
    /// with Incomplete while an async writer is stalled.
    /// </summary>
    [TestMethod]
    [Timeout(60_000, CooperativeCancellation = true)]
    public async Task NoSlowdownAsyncWriteFailsWhileAnotherWriterIsStalled()
    {
        MemoryFileSystem fs = NewFs();
        await using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        var stalled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        TestHooks.Set(DelayWriteWaitHook, () => stalled.TrySetResult());
        Task blockedWrite;
        RocksDbException? error = null;
        using var batch = new WriteBatch();
        batch.Put(B("b"), B("2"));
        using var batch2 = new WriteBatch();
        batch2.Put(B("c"), B("3"));
        try
        {
            WalLock walLock = await db.LockWalAsync();
            try
            {
                blockedWrite = db.WriteAsync(batch).AsTask();
                await stalled.Task.WaitAsync(TimeSpan.FromSeconds(30));
                try
                {
                    await db.WriteAsync(new WriteOptions { NoSlowdown = true }, batch2).AsTask().WaitAsync(TimeSpan.FromSeconds(5));
                }
                catch (RocksDbException e)
                {
                    error = e;
                }
            }
            finally
            {
                walLock.Dispose();
            }
            await blockedWrite.WaitAsync(TimeSpan.FromSeconds(30));
        }
        finally
        {
            TestHooks.Set(DelayWriteWaitHook, null);
        }
        Assert.IsNotNull(error);
        Assert.AreEqual(StatusCode.Incomplete, error.Code);
        Assert.AreEqual("2", GetString(db, "b"));
        Assert.IsNull(GetString(db, "c"));
    }

    /// <summary>
    /// DBImpl::GetUpdatesSince (db/db_impl/db_impl.cc) and WalManager::GetUpdatesSince (db/wal_manager.cc) return
    /// NotSupported for a seq_per_batch DB (WritePrepared / WriteUnprepared): the WAL iterator computes a batch's last
    /// sequence as sequence + count - 1, which only holds with one sequence number per key. The port returned an iterator.
    /// </summary>
    [TestMethod]
    public void GetUpdatesSinceIsNotSupportedWithSequencePerBatch()
    {
        foreach (TransactionWritePolicy policy in new[] { TransactionWritePolicy.WritePrepared, TransactionWritePolicy.WriteUnprepared })
        {
            MemoryFileSystem fs = NewFs();
            using TransactionDatabase db = TransactionDatabase.Open(DbPath, NewOptions(fs), new TransactionDbOptions { WritePolicy = policy },
                [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, new ColumnFamilyOptions())]);
            db.Database.Put(B("a"), B("1"));
            var ex = Assert.ThrowsExactly<RocksDbException>(() => db.Database.GetUpdatesSince(1), policy.ToString());
            Assert.AreEqual(StatusCode.NotSupported, ex.Code);
            StringAssert.Contains(ex.Message, "not yet compatible with write-prepared/write-unprepared transactions");
        }
        // WriteCommitted (one sequence per key) keeps working.
        {
            MemoryFileSystem fs = NewFs();
            using TransactionDatabase db = TransactionDatabase.Open(DbPath, NewOptions(fs), new TransactionDbOptions(),
                [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, new ColumnFamilyOptions())]);
            db.Database.Put(B("a"), B("1"));
            using TransactionLogIterator it = db.Database.GetUpdatesSince(1);
            Assert.IsTrue(it.Valid);
            Assert.AreEqual(1UL, it.Current.Sequence);
        }
    }

    // ---- coverage of verified behaviour without an existing test --------------------------------------------------

    /// <summary>
    /// MemTableInserter::DeleteRangeCF (db/write_batch.cc): an empty range (begin == end) returns OK before DeleteImpl,
    /// so nothing is inserted and MaybeAdvanceSeq is not called: the entries behind it get the sequence number the empty
    /// range would have had, while the batch still reserves Count() sequence numbers (WriteImpl: last_sequence +=
    /// seq_inc). WAL replay assigns the same numbers.
    /// </summary>
    [TestMethod]
    public void EmptyDeleteRangeDoesNotAdvanceTheSequenceLiveAndInRecovery()
    {
        MemoryFileSystem fs = NewFs();
        ulong baseSeq;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            db.Put(B("a"), B("0"));
            baseSeq = db.GetLatestSequenceNumber();
            using var batch = new WriteBatch();
            batch.Put(B("a"), B("1")).DeleteRange(B("m"), B("m")).Put(B("b"), B("2"));
            db.Write(batch);
            Assert.AreEqual(baseSeq + 3, db.GetLatestSequenceNumber());
            Assert.AreEqual(baseSeq + 2, SequenceOf(db, "b"));
            Assert.AreEqual("1", GetString(db, "a"));
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            // Recovery numbers the entries the same way, but the last sequence becomes the inserter's (InsertInto:
            // *next_seq = inserter.sequence(); DBImpl::RecoverLogFiles: last_sequence = *next_sequence - 1), so the
            // sequence number the empty range reserved is given back.
            Assert.AreEqual(baseSeq + 2, db.GetLatestSequenceNumber());
            Assert.AreEqual(baseSeq + 2, SequenceOf(db, "b"));
            Assert.AreEqual("2", GetString(db, "b"));
        }
    }

    private static ulong SequenceOf(RocksDatabase db, string key)
    {
        using KeyVersionCollection versions = db.GetAllKeyVersions(B(key), B(key), int.MaxValue);
        foreach (KeyVersion kv in versions)
        {
            if (kv.UserKey.Span.SequenceEqual(B(key))) return kv.Sequence;
        }
        Assert.Fail("key not found: " + key);
        return 0;
    }

    /// <summary>
    /// WriteBatch::RollbackToSavePoint / PopSavePoint / LocalSavePoint::commit (db/write_batch.cc) interplay: a rollback
    /// restores size, count and content flags; an operation refused by max_bytes leaves the batch (and its count / flags)
    /// untouched and keeps the save point stack; PopSavePoint drops a save point without rolling back; with no save point
    /// both report NotFound (false).
    /// </summary>
    [TestMethod]
    public void SavePointsRestoreCountAndContentFlagsAndSurviveMemoryLimit()
    {
        using var batch = new WriteBatch(reservedBytes: 0, maxBytes: 64);
        batch.Put(B("a"), B("1"));
        byte[] afterPut = batch.Data.ToArray();
        batch.SetSavePoint();
        batch.Merge(B("b"), B("2")).DeleteRange(B("c"), B("d"));
        Assert.IsTrue(batch.HasMerge);
        Assert.IsTrue(batch.HasDeleteRange);
        Assert.AreEqual(3, batch.Count);
        // Exceeds max_bytes: Aborted (MemoryLimit), nothing changes.
        byte[] before = batch.Data.ToArray();
        var ex = Assert.ThrowsExactly<RocksDbException>(() => batch.SingleDelete(new byte[64]));
        Assert.AreEqual(StatusCode.Aborted, ex.Code);
        Assert.AreEqual(StatusSubCode.MemoryLimit, ex.SubCode);
        CollectionAssert.AreEqual(before, batch.Data.ToArray());
        Assert.AreEqual(3, batch.Count);
        Assert.IsFalse(batch.HasSingleDelete);

        Assert.IsTrue(batch.RollbackToSavePoint());
        CollectionAssert.AreEqual(afterPut, batch.Data.ToArray());
        Assert.AreEqual(1, batch.Count);
        Assert.IsFalse(batch.HasMerge);
        Assert.IsFalse(batch.HasDeleteRange);
        Assert.IsTrue(batch.HasPut);
        Assert.IsFalse(batch.RollbackToSavePoint());

        batch.SetSavePoint();
        batch.Delete(B("e"));
        batch.SetSavePoint();
        batch.Delete(B("f"));
        Assert.IsTrue(batch.PopSavePoint()); // keeps "f"
        Assert.AreEqual(3, batch.Count);
        Assert.IsTrue(batch.RollbackToSavePoint()); // back to the first save point: drops "e" and "f"
        CollectionAssert.AreEqual(afterPut, batch.Data.ToArray());
        Assert.IsFalse(batch.HasDelete);
        Assert.IsFalse(batch.PopSavePoint());
    }

    /// <summary>
    /// WriteBatchInternal::Append(dst, src, wal_only = true) (db/write_batch.cc): only the records before the source's
    /// WAL termination point are appended, with the count and content flags saved at that point; without wal_only the
    /// whole batch is appended. AppendedByteSize subtracts one header unless a side is empty.
    /// </summary>
    [TestMethod]
    public void AppendHonoursWalTerminationPointCountAndFlags()
    {
        using var src = new WriteBatch();
        src.Put(B("a"), B("1")).PutLogData(B("blob"));
        src.MarkWalTerminationPoint();
        src.Merge(B("b"), B("2")).Delete(B("c"));
        int walPartSize = WriteBatch.HeaderSize + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 4; // header, Put a=1, LogData "blob"

        using var walOnly = new WriteBatch();
        walOnly.Put(B("x"), B("y"));
        int before = walOnly.DataSize;
        WriteBatch.Append(walOnly, src, walOnly: true);
        Assert.AreEqual(2, walOnly.Count);
        Assert.AreEqual(WriteBatch.AppendedByteSize(before, walPartSize), walOnly.DataSize);
        Assert.IsFalse(walOnly.HasMerge);
        Assert.IsFalse(walOnly.HasDelete);
        int records = 0;
        foreach (WriteBatchEntry e in walOnly) records++;
        Assert.AreEqual(3, records); // Put x, Put a, LogData

        using var all = new WriteBatch();
        WriteBatch.Append(all, src, walOnly: false);
        Assert.AreEqual(3, all.Count);
        Assert.IsTrue(all.HasMerge);
        Assert.IsTrue(all.HasDelete);
        CollectionAssert.AreEqual(src.Data[WriteBatch.HeaderSize..].ToArray(), all.Data[WriteBatch.HeaderSize..].ToArray());

        Assert.AreEqual(40, WriteBatch.AppendedByteSize(0, 40));
        Assert.AreEqual(40, WriteBatch.AppendedByteSize(40, 0));
        Assert.AreEqual(68, WriteBatch.AppendedByteSize(40, 40));
    }
}
