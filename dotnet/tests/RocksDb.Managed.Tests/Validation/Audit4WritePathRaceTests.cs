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
/// <item>A group of several writers inserts in parallel: the record also holds the batches of the writers whose inserts
/// succeeded (their writes returned without error), so it stays in the WAL and a reopen recovers the whole group.</item>
/// </list>
/// In both cases the group is not published, a fatal background error stops writes, and every write acknowledged before
/// the failed group stays readable, before and after the reopen.
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

    /// <summary>
    /// A group of two writers (leader and follower queued behind a writer held in its memtable insert) inserts in
    /// parallel; the entry of one of them is corrupted. That write fails; the group is not published (also not the
    /// other writer's entry, whose write returned without error: upstream publishes or not depending on which writer
    /// finishes last), writes stop, and the write acknowledged before the group stays readable. The group's WAL record
    /// is kept, so a reopen recovers both batches.
    /// </summary>
    [TestMethod]
    [DataRow(true)]
    [DataRow(false)]
    [Timeout(60_000, CooperativeCancellation = true)]
    public void FailedInsertInParallelGroupKeepsEarlierWritesAndTheGroupsWalRecord(bool corruptLeader)
    {
        byte[] first = B("first"), leaderKey = B("leader"), followerKey = B("follower");
        byte[] bad = corruptLeader ? leaderKey : followerKey;
        using var firstInInsert = new ManualResetEventSlim();
        using var releaseFirst = new ManualResetEventSlim();
        using var leaderQueued = new ManualResetEventSlim();
        using var followerQueued = new ManualResetEventSlim();
        int queued = 0, corrupted = 0, firstHeld = 0;
        int leaderThread = 0, followerThread = 0;
        TestHooks.SetSpan(EncodedHook, data =>
        {
            if (IsEntryOf(data, first) && Interlocked.Exchange(ref firstHeld, 1) == 0)
            {
                // The first writer leads a group of its own: it keeps leadership while the two others queue up.
                firstInInsert.Set();
                releaseFirst.Wait();
            }
            else if (IsEntryOf(data, leaderKey)) leaderThread = Environment.CurrentManagedThreadId;
            else if (IsEntryOf(data, followerKey)) followerThread = Environment.CurrentManagedThreadId;
            if (IsEntryOf(data, bad) && Interlocked.Exchange(ref corrupted, 1) == 0) CorruptValue(data);
        });
        TestHooks.Set(JoinWaitHook, () =>
        {
            if (Interlocked.Increment(ref queued) == 1) leaderQueued.Set();
            else followerQueued.Set();
        });
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            Assert.IsNull(TryPut(db, "before", "1"));
            ulong published = db.GetLatestSequenceNumber();
            Exception? firstError = null, leaderError = null, followerError = null;
            int leaderId = 0, followerId = 0;
            var t1 = new Thread(() => firstError = TryPut(db, "first", "f"));
            var t2 = new Thread(() =>
            {
                leaderId = Environment.CurrentManagedThreadId;
                leaderError = TryPut(db, "leader", "l");
            });
            var t3 = new Thread(() =>
            {
                followerId = Environment.CurrentManagedThreadId;
                followerError = TryPut(db, "follower", "w");
            });
            t1.Start();
            firstInInsert.Wait();
            t2.Start();
            leaderQueued.Wait();
            t3.Start();
            followerQueued.Wait();
            releaseFirst.Set();
            t1.Join();
            t2.Join();
            t3.Join();

            Assert.IsNull(firstError, firstError?.ToString());
            Assert.AreEqual(1, corrupted);
            // Parallel inserts: each writer of the group inserted its own batch.
            Assert.AreEqual(leaderId, leaderThread);
            Assert.AreEqual(followerId, followerThread);
            AssertStoppedByCorruption(db, corruptLeader ? leaderError : followerError);
            Exception? other = corruptLeader ? followerError : leaderError;
            Assert.IsNull(other, other?.ToString());
            // The first writer's group was published, the failed group was not.
            Assert.AreEqual(published + 1, db.GetLatestSequenceNumber());
            Assert.AreEqual("1", GetString(db, "before"));
            Assert.AreEqual("f", GetString(db, "first"));
            Assert.IsNull(db.Get(leaderKey));
            Assert.IsNull(db.Get(followerKey));
        }
        TestHooks.ClearAll();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            Assert.IsNull(db.BackgroundError);
            Assert.AreEqual("1", GetString(db, "before"));
            Assert.AreEqual("f", GetString(db, "first"));
            // The group's record is intact in the WAL: recovery inserts both batches.
            Assert.AreEqual("l", GetString(db, "leader"));
            Assert.AreEqual("w", GetString(db, "follower"));
        }
    }
}
