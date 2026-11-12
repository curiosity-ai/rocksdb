using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4, write path interleavings around a failed memtable insert (DBImpl::WriteImpl, db/db_impl/db_impl_write.cc:
/// "Don't publish a partial batch write", SetAttemptTruncateSize(wal_context.prev_size), HandleMemTableInsertFailure).
/// What happens to the failed write's WAL record depends on the shape of its write group, which a free-running
/// multi-threaded test cannot choose: the groups are built here with hooks.
/// <list type="bullet">
/// <item>A writer alone in its group inserts serially: the group's record is cut from the WAL when the DB closes, so a
/// reopen does not replay the refused write.</item>
/// <item>A group of several writers (parallel or serial inserts): every writer of the group gets the failed insert's
/// status, whichever writer finishes last, and the record is cut at close as well. With pipelined writes the record
/// stays in the WAL (as upstream) and a reopen recovers the group.</item>
/// <item>unordered_write: only the failed write fails; its record stays in the WAL.</item>
/// </list>
/// In every case no write of the failed group is acknowledged, the group is not published, a background error stops
/// writes, and every write acknowledged before the failed group stays readable, before and after the reopen.
/// </summary>
[TestClass]
public sealed class Audit4WritePathRaceTests
{
    private const string DbPath = "/audit4writepathrace";
    private const string EncodedHook = "MemTable::Add:Encoded";
    private const string JoinWaitHook = "WriteThread::JoinBatchGroup:Wait";

    private static readonly WriteOptions s_protected = new() { ProtectionBytesPerKey = 8 };

    [TestCleanup]
    public void Cleanup() => TestHooks.ClearAll();

    /// <summary>The user key of an encoded memtable entry: varint32(internal key length) | user key | 8-byte footer | ...</summary>
    private static bool IsEntryOf(ReadOnlySpan<byte> encoded, byte[] userKey) => encoded.Slice(1, encoded[0] - 8).SequenceEqual(userKey);

    /// <summary>Adds 1 to the first value byte of an encoded memtable entry (the byte after the value length).</summary>
    private static void CorruptValue(Span<byte> encoded) => encoded[1 + encoded[0] + 1]++;

    private static Exception? TryPut(RocksDatabase db, string key, string value)
    {
        try
        {
            db.Put(s_protected, B(key), B(value));
            return null;
        }
        catch (Exception e)
        {
            return e;
        }
    }

    private static void AssertStoppedByCorruption(RocksDatabase db, Exception? failedWrite)
    {
        var failed = failedWrite as RocksDbException;
        Assert.IsNotNull(failed, failedWrite?.ToString() ?? "the corrupted write did not fail");
        Assert.AreEqual(StatusCode.Corruption, failed.Code, failed.ToString());
        StringAssert.Contains(failed.ToString(), KvChecksum.MismatchMessage);
        Assert.AreEqual(StatusCode.Corruption, db.BackgroundError?.Code);
        var later = Assert.ThrowsExactly<RocksDbException>(() => db.Put(B("after"), B("x")));
        Assert.AreEqual(StatusCode.Corruption, later.Code);
    }

    /// <summary>
    /// The interleaving behind the intermittent failure of KvProtectionTests.ParallelMemtableInsertDetectsCorruptedEntry:
    /// the writer whose entry is corrupted is alone in its write group (the other writers were not queued when it took
    /// leadership). The group is inserted serially by its leader; like upstream, the WAL is then cut back to its size
    /// before the group's record when the DB closes, so the refused write is not replayed. The write acknowledged just
    /// before it is readable before and after the reopen.
    /// </summary>
    [TestMethod]
    [Timeout(60_000, CooperativeCancellation = true)]
    public void FailedInsertOfLoneWriterIsCutFromWalAndNotReplayed()
    {
        byte[] bad = B("bad");
        int corrupted = 0;
        TestHooks.SetSpan(EncodedHook, data =>
        {
            if (IsEntryOf(data, bad) && Interlocked.Exchange(ref corrupted, 1) == 0) CorruptValue(data);
        });
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            Assert.IsNull(TryPut(db, "before", "1"));
            ulong published = db.GetLatestSequenceNumber();
            AssertStoppedByCorruption(db, TryPut(db, "bad", "2"));
            Assert.AreEqual(1, corrupted);
            Assert.AreEqual(published, db.GetLatestSequenceNumber());
            Assert.IsNull(db.Get(bad));
            Assert.AreEqual("1", GetString(db, "before"));
        }
        TestHooks.ClearAll();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            Assert.IsNull(db.BackgroundError);
            Assert.AreEqual("1", GetString(db, "before"));
            Assert.IsNull(db.Get(bad), "the refused write of a single-writer group must not be replayed from the WAL");
        }
    }

    /// <summary>How the write group with the failed insert is built and inserted.</summary>
    public enum GroupMode
    {
        /// <summary>DBImpl::WriteImpl, allow_concurrent_memtable_write: every writer inserts its own batch.</summary>
        Parallel,
        /// <summary>DBImpl::WriteImpl without concurrent memtable writes: the leader inserts the whole group.</summary>
        Serial,
        /// <summary>DBImpl::PipelinedWriteImpl, parallel memtable group.</summary>
        PipelinedParallel,
        /// <summary>DBImpl::PipelinedWriteImpl, memtable group inserted by its memtable leader.</summary>
        PipelinedSerial,
    }

    private static readonly string[] s_groupKeys = ["leader", "f1", "f2"];
    private static readonly string[] s_groupValues = ["l", "a", "b"];

    private static bool IsPipelined(GroupMode mode) => mode is GroupMode.PipelinedParallel or GroupMode.PipelinedSerial;

    private static bool IsParallel(GroupMode mode) => mode is GroupMode.Parallel or GroupMode.PipelinedParallel;

    private static DbOptions GroupOptions(MemoryFileSystem fs, GroupMode mode)
    {
        DbOptions o = NewOptions(fs);
        o.EnablePipelinedWrite = IsPipelined(mode);
        o.AllowConcurrentMemtableWrite = IsParallel(mode);
        return o;
    }

    /// <summary>
    /// One write group of three writers (the leader and two followers, in that order) behind a writer held in its
    /// memtable insert; the entry of writer <paramref name="bad"/> is corrupted on its way into the memtable. With
    /// parallel inserts, writer <paramref name="last"/> is held in its insert until the two others counted down, so it
    /// is the one that finishes last and exits the group (<paramref name="last"/> is -1 for serial inserts).
    /// <para>
    /// In every shape and order: the three writes fail with the insert's Corruption (none is acknowledged), the group is
    /// not published and none of its entries is readable, a fatal background error stops writes, and the writes
    /// acknowledged before the group stay readable, before and after the reopen. Without pipelined writes the group's
    /// record is cut from the WAL when the DB closes and the reopen recovers none of the three; with pipelined writes
    /// the record stays (as upstream) and the reopen recovers all three.
    /// </para>
    /// <para>
    /// Upstream gives the same result when the leader finishes last, and in every order with pipelined writes. When a
    /// follower finishes last (non-pipelined), upstream returns the leader's own insert status and leaves the WAL
    /// record; in a serial non-pipelined group it returns OK to the followers: known deviations, see docs/validation.md.
    /// </para>
    /// </summary>
    [TestMethod]
    [DataRow(GroupMode.Parallel, 0, 0)]
    [DataRow(GroupMode.Parallel, 0, 1)]
    [DataRow(GroupMode.Parallel, 0, 2)]
    [DataRow(GroupMode.Parallel, 1, 0)]
    [DataRow(GroupMode.Parallel, 1, 1)]
    [DataRow(GroupMode.Parallel, 1, 2)]
    [DataRow(GroupMode.PipelinedParallel, 0, 0)]
    [DataRow(GroupMode.PipelinedParallel, 0, 1)]
    [DataRow(GroupMode.PipelinedParallel, 0, 2)]
    [DataRow(GroupMode.PipelinedParallel, 1, 0)]
    [DataRow(GroupMode.PipelinedParallel, 1, 1)]
    [DataRow(GroupMode.PipelinedParallel, 1, 2)]
    [DataRow(GroupMode.Serial, 0, -1)]
    [DataRow(GroupMode.Serial, 1, -1)]
    [DataRow(GroupMode.Serial, 2, -1)]
    [DataRow(GroupMode.PipelinedSerial, 0, -1)]
    [DataRow(GroupMode.PipelinedSerial, 1, -1)]
    [DataRow(GroupMode.PipelinedSerial, 2, -1)]
    [Timeout(60_000, CooperativeCancellation = true)]
    public void FailedInsertFailsEveryWriterOfItsGroup(GroupMode mode, int bad, int last)
    {
        bool parallel = IsParallel(mode), pipelined = IsPipelined(mode);
        byte[] first = B("first");
        byte[][] keys = [B(s_groupKeys[0]), B(s_groupKeys[1]), B(s_groupKeys[2])];
        using var firstInInsert = new ManualResetEventSlim();
        using var releaseFirst = new ManualResetEventSlim();
        using var queued = new SemaphoreSlim(0);
        using var othersCounted = new CountdownEvent(2);
        int corrupted = 0, firstHeld = 0, counted = 0, finisher = 0, finishes = 0;
        int[] insertedBy = new int[3];
        TestHooks.SetSpan(EncodedHook, data =>
        {
            if (IsEntryOf(data, first) && Interlocked.Exchange(ref firstHeld, 1) == 0)
            {
                // The first writer is alone in its group: it keeps the write (memtable) stage while the others queue up.
                firstInInsert.Set();
                releaseFirst.Wait();
                return;
            }
            for (int i = 0; i < keys.Length; i++)
            {
                if (!IsEntryOf(data, keys[i])) continue;
                insertedBy[i] = Environment.CurrentManagedThreadId;
                if (i == bad && Interlocked.Exchange(ref corrupted, 1) == 0) CorruptValue(data);
                // Finish last: stay in the insert until the two other writers of the group counted down.
                if (i == last) othersCounted.Wait();
            }
        });
        // A writer of the group is queued: behind the write leader, or (pipelined) behind the memtable writer.
        TestHooks.Set(pipelined ? "DBImpl::PipelinedWriteImpl:WaitForMemTableWriterLeader" : JoinWaitHook, () => queued.Release());
        TestHooks.Set("WriteThread::CompleteParallelMemTableWriter:AfterCountDown", () =>
        {
            if (Interlocked.Increment(ref counted) <= 2) othersCounted.Signal();
        });
        TestHooks.Set("DBImpl::WriteImpl:FinishParallelGroup", () =>
        {
            finisher = Environment.CurrentManagedThreadId;
            Interlocked.Increment(ref finishes);
        });
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, GroupOptions(fs, mode)))
        {
            Assert.IsNull(TryPut(db, "before", "1"));
            ulong published = db.GetLatestSequenceNumber();
            Exception? firstError = null;
            var errors = new Exception?[3];
            int[] ids = new int[3];
            var t1 = new Thread(() => firstError = TryPut(db, "first", "f"));
            t1.Start();
            firstInInsert.Wait();
            var threads = new Thread[3];
            for (int i = 0; i < 3; i++)
            {
                int n = i;
                threads[i] = new Thread(() =>
                {
                    ids[n] = Environment.CurrentManagedThreadId;
                    errors[n] = TryPut(db, s_groupKeys[n], s_groupValues[n]);
                });
                threads[i].Start();
                // Queue order is group order: the first of the three leads the group.
                queued.Wait();
            }
            releaseFirst.Set();
            t1.Join();
            foreach (Thread t in threads) t.Join();

            Assert.IsNull(firstError, firstError?.ToString());
            Assert.AreEqual(1, corrupted);
            if (parallel)
            {
                // Each writer inserted its own batch; the held writer finished last and exited the group.
                for (int i = 0; i < 3; i++) Assert.AreEqual(ids[i], insertedBy[i], s_groupKeys[i]);
                Assert.AreEqual(1, finishes);
                Assert.AreEqual(ids[last], finisher);
            }
            else
            {
                // The leader inserted the group up to the failed batch.
                Assert.AreEqual(0, finishes);
                for (int i = 0; i < 3; i++) Assert.AreEqual(i <= bad ? ids[0] : 0, insertedBy[i], s_groupKeys[i]);
            }
            // No writer of the group is acknowledged: each gets the failed insert's status.
            for (int i = 0; i < 3; i++)
            {
                var e = errors[i] as RocksDbException;
                Assert.IsNotNull(e, s_groupKeys[i] + ": " + (errors[i]?.ToString() ?? "no error"));
                Assert.AreEqual(StatusCode.Corruption, e.Code, s_groupKeys[i] + ": " + e);
                StringAssert.Contains(e.ToString(), KvChecksum.MismatchMessage);
            }
            AssertStoppedByCorruption(db, errors[bad]);
            // The first writer's group was published, the failed group was not.
            Assert.AreEqual(published + 1, db.GetLatestSequenceNumber());
            Assert.AreEqual("1", GetString(db, "before"));
            Assert.AreEqual("f", GetString(db, "first"));
            foreach (byte[] k in keys) Assert.IsNull(db.Get(k));
        }
        TestHooks.ClearAll();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, GroupOptions(fs, mode)))
        {
            Assert.IsNull(db.BackgroundError);
            Assert.AreEqual("1", GetString(db, "before"));
            Assert.AreEqual("f", GetString(db, "first"));
            for (int i = 0; i < 3; i++)
            {
                // Non-pipelined: the group's record was cut from the WAL at close. Pipelined: the record is kept (later
                // WAL groups may follow it) and recovery inserts the three batches from it.
                Assert.AreEqual(pipelined ? s_groupValues[i] : null, GetString(db, s_groupKeys[i]), s_groupKeys[i]);
            }
            Assert.IsNull(TryPut(db, "after", "x"));
        }
    }

    /// <summary>
    /// unordered_write (WriteImplWALOnly + UnorderedWriteMemtable): the WAL stage publishes the sequence before the
    /// memtable inserts, which are independent of each other. A failed insert fails that write only; a write whose
    /// insert overlaps it is acknowledged and readable. Like upstream (WriteStatusCheck) the failure is a background
    /// error that stops later writes, and the failed write's WAL record is kept: a reopen recovers it.
    /// </summary>
    [TestMethod]
    [Timeout(60_000, CooperativeCancellation = true)]
    public void FailedUnorderedInsertFailsThatWriteOnly()
    {
        byte[] bad = B("bad");
        using var badInInsert = new ManualResetEventSlim();
        using var releaseBad = new ManualResetEventSlim();
        int corrupted = 0;
        TestHooks.SetSpan(EncodedHook, data =>
        {
            if (!IsEntryOf(data, bad) || Interlocked.Exchange(ref corrupted, 1) != 0) return;
            CorruptValue(data);
            badInInsert.Set();
            releaseBad.Wait();
        });
        MemoryFileSystem fs = NewFs();
        DbOptions Options()
        {
            DbOptions o = NewOptions(fs);
            o.UnorderedWrite = true;
            return o;
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, Options()))
        {
            Assert.IsNull(TryPut(db, "before", "1"));
            ulong published = db.GetLatestSequenceNumber();
            Exception? badError = null;
            var t = new Thread(() => badError = TryPut(db, "bad", "2"));
            t.Start();
            badInInsert.Wait();
            // The failed write's sequence was published by its WAL stage; another write goes through meanwhile.
            Assert.AreEqual(published + 1, db.GetLatestSequenceNumber());
            Assert.IsNull(TryPut(db, "other", "3"));
            Assert.AreEqual("3", GetString(db, "other"));
            releaseBad.Set();
            t.Join();

            AssertStoppedByCorruption(db, badError);
            Assert.AreEqual(published + 2, db.GetLatestSequenceNumber());
            Assert.IsNull(db.Get(bad));
            Assert.AreEqual("1", GetString(db, "before"));
            Assert.AreEqual("3", GetString(db, "other"));
        }
        TestHooks.ClearAll();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, Options()))
        {
            Assert.IsNull(db.BackgroundError);
            Assert.AreEqual("1", GetString(db, "before"));
            Assert.AreEqual("3", GetString(db, "other"));
            Assert.AreEqual("2", GetString(db, "bad"));
        }
    }
}
