using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>
/// <see cref="UserWriteCallback"/> / <see cref="RocksDatabase.WriteWithCallback"/>: ports of WriteCallbackPTest.WriteWithCallbackTest
/// (user_write_cb part) and WriteCallbackTest.WriteCallBackTest (db/write_callback_test.cc).
/// </summary>
[TestClass]
public sealed class UserWriteCallbackTests
{
    /// <summary>MockUserWriteCallback.</summary>
    private sealed class MockUserWriteCallback : UserWriteCallback
    {
        private int _enqueued;
        private int _walDone;
        public bool WriteEnqueued => Volatile.Read(ref _enqueued) != 0;
        public bool WalWriteDone => Volatile.Read(ref _walDone) != 0;
        public int WalWriteCalls => Volatile.Read(ref _walDone);
        public int EnqueuedCalls => Volatile.Read(ref _enqueued);

        public override void OnWriteEnqueued() => Interlocked.Increment(ref _enqueued);

        public override void OnWalWriteFinish()
        {
            // The WAL write of a write follows its enqueueing.
            Assert.IsTrue(WriteEnqueued);
            Interlocked.Increment(ref _walDone);
        }
    }

    [TestMethod]
    public void WriteCallBackTest()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        using WriteBatch wb = db.CreateWriteBatch();
        wb.Put(B("a"), B("value.a4"));
        var cb = new MockUserWriteCallback();
        db.WriteWithCallback(null, wb, cb);
        Assert.AreEqual("value.a4", GetString(db, "a"));
        Assert.AreEqual(1, cb.EnqueuedCalls);
        Assert.AreEqual(1, cb.WalWriteCalls);
    }

    /// <summary>
    /// WriteWithCallbackTest over the write modes (unordered_write, two_write_queues, concurrent memtable writes, batching,
    /// WAL on/off, pipelined writes), sync and async, from concurrent writers.
    /// </summary>
    [TestMethod]
    public void WriteWithCallbackTest()
    {
        int combos = 0;
        for (int mask = 0; mask < 64; mask++)
        {
            bool unorderedWrite = (mask & 1) != 0, twoQueues = (mask & 2) != 0, allowParallel = (mask & 4) != 0;
            bool allowBatching = (mask & 8) != 0, enableWal = (mask & 16) != 0, pipelined = (mask & 32) != 0;
            // Incompatible combinations (ValidateOptions / SanitizeOptions).
            if (pipelined && (unorderedWrite || twoQueues)) continue;
            if (unorderedWrite && !allowParallel) continue;
            combos++;
            string ctx = "unordered=" + unorderedWrite + " twoQueues=" + twoQueues + " parallel=" + allowParallel + " batching=" + allowBatching +
                " wal=" + enableWal + " pipelined=" + pipelined;
            MemoryFileSystem fs = NewFs();
            DbOptions o = NewOptions(fs);
            o.UnorderedWrite = unorderedWrite;
            o.TwoWriteQueues = twoQueues;
            o.AllowConcurrentMemtableWrite = allowParallel;
            o.EnablePipelinedWrite = pipelined;
            using RocksDatabase db = RocksDatabase.Open(DbPath, o);
            int writers = allowBatching ? 8 : 1;
            var callbacks = new MockUserWriteCallback[writers * 4];
            var woptions = new WriteOptions { DisableWal = !enableWal, Sync = enableWal };
            Parallel.For(0, writers, new ParallelOptions { MaxDegreeOfParallelism = writers }, t =>
            {
                for (int i = 0; i < 4; i++)
                {
                    int n = t * 4 + i;
                    var cb = callbacks[n] = new MockUserWriteCallback();
                    using WriteBatch wb = db.CreateWriteBatch();
                    wb.Put(Key(n), Value(n, 0));
                    if (i % 2 == 0) db.WriteWithCallback(woptions, wb, cb);
                    else db.WriteWithCallbackAsync(woptions, wb, cb).AsTask().GetAwaiter().GetResult();
                    Assert.IsTrue(cb.WriteEnqueued, ctx);
                    Assert.AreEqual(enableWal ? 1 : 0, cb.WalWriteCalls, ctx);
                }
            });
            for (int n = 0; n < callbacks.Length; n++)
            {
                Assert.AreEqual(1, callbacks[n].EnqueuedCalls, ctx);
                Assert.AreEqual(S(Value(n, 0)), GetString(db, S(Key(n))!), ctx);
            }
        }
        Assert.IsTrue(combos >= 16);
    }

    /// <summary>Plain writes (and writes after a callback write) carry no callback: the writer is reset when pooled.</summary>
    [TestMethod]
    public void CallbackIsNotKeptByPooledWriters()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        var cb = new MockUserWriteCallback();
        using (WriteBatch wb = db.CreateWriteBatch())
        {
            wb.Put(B("a"), B("1"));
            db.WriteWithCallback(null, wb, cb);
        }
        for (int i = 0; i < 10; i++) db.Put(B("b" + i), B("2"));
        Assert.AreEqual(1, cb.EnqueuedCalls);
        Assert.AreEqual(1, cb.WalWriteCalls);
        Assert.ThrowsExactly<ArgumentNullException>(() => db.WriteWithCallback(null, db.CreateWriteBatch(), null!));
    }
}
