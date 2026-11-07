using System.Collections.Concurrent;
using System.Globalization;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>
/// WAL write error recovery with WAL recycling on (recycle_log_file_num &gt; 0). Upstream's DBImpl::ResumeImpl refuses
/// file-scoped WAL write error recovery with NotSupported when recycling is on (the DB stays write-stopped); the port
/// runs the recovery and never puts the failed WAL on the recycle list (it is deleted). These tests are the evidence
/// that the deviation is safe: the failed WAL is never reused, the failed batch never becomes visible (live, through
/// GetUpdatesSince, or after a reopen), acknowledged writes survive, sequence numbers are not reused, and a crash at any
/// file-system operation of the recovery leaves a DB that reopens consistently. Only recovery modes that keep
/// recycling on are used (kPointInTimeRecovery and kSkipAnyCorruptedRecords; SanitizeOptions turns recycling off for
/// kTolerateCorruptedTailRecords and kAbsoluteConsistency).
/// </summary>
[TestClass]
public sealed class WalErrorRecyclingTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    /// <summary>How the WAL write fails.</summary>
    public enum Poison
    {
        /// <summary>File-scoped append error; nothing of the failed record reaches the WAL.</summary>
        AppendNothingWritten,
        /// <summary>File-scoped append error; the record header and half of its payload reach the WAL.</summary>
        AppendTorn,
        /// <summary>File-scoped append error reported after the whole record reached the WAL (ambiguous outcome).</summary>
        AppendFullyWritten,
        /// <summary>File-scoped sync error of a sync write (the record is in the WAL, unsynced).</summary>
        SyncError,
        /// <summary>Out of space while appending (torn), recovered by the SstFileManager once space is free.</summary>
        NoSpace,
    }

    private static readonly byte[] LostKey = B("lost-key-0001");
    private static readonly byte[] LostValue = B("lost-value-0001");

    [TestCleanup]
    public void Cleanup() => TestHooks.ClearAll();

    // ---- harness ---------------------------------------------------------------------------------------------

    private sealed class Logger() : InfoLogger(InfoLogLevel.Info)
    {
        private readonly List<string> _lines = new();

        protected override void LogCore(InfoLogLevel level, string message)
        {
            lock (_lines) _lines.Add(message);
        }

        public bool Any(string s)
        {
            lock (_lines) return _lines.Exists(l => l.Contains(s, StringComparison.Ordinal));
        }

        /// <summary>The numbers N of the lines containing "<paramref name="prefix"/>N<paramref name="suffix"/>".</summary>
        public List<ulong> Numbers(string prefix, string suffix)
        {
            var r = new List<ulong>();
            lock (_lines)
            {
                foreach (string l in _lines)
                {
                    int i = l.IndexOf(prefix, StringComparison.Ordinal);
                    if (i < 0) continue;
                    int start = i + prefix.Length;
                    int end = start;
                    while (end < l.Length && char.IsAsciiDigit(l[end])) end++;
                    if (end > start && l.AsSpan(end).StartsWith(suffix, StringComparison.Ordinal))
                        r.Add(ulong.Parse(l.AsSpan(start, end - start), CultureInfo.InvariantCulture));
                }
            }
            return r;
        }
    }

    private sealed class Listener : EventListener
    {
        public readonly ManualResetEventSlim Ended = new(false);
        public volatile BackgroundErrorRecoveryInfo? Info;

        public override void OnErrorRecoveryEnd(BackgroundErrorRecoveryInfo info)
        {
            Info = info;
            Ended.Set();
        }
    }

    private sealed class Harness : IDisposable
    {
        public readonly FaultInjectionFileSystem Fs = new(NewFs());
        public readonly Logger Log = new();
        public readonly Listener Listener = new();
        public readonly ulong Recycle;
        public readonly WalRecoveryMode Mode;
        public readonly SstFileManager? Sfm;
        public RocksDatabase Db = null!;
        /// <summary>Acknowledged writes (key -> value) and the sequence number each one got.</summary>
        public readonly Dictionary<string, string> Acked = new();
        public ulong MaxAckedSeq;
        public ulong FailedWal;
        public ulong ReservedSeq;
        public ulong FirstPreFailureSeq;

        public Harness(ulong recycle, WalRecoveryMode mode, bool sstFileManager)
        {
            Recycle = recycle;
            Mode = mode;
            if (sstFileManager) Sfm = new SstFileManager(Fs, 0, 0.25, 64UL << 20) { ErrorRecoveryPollInterval = TimeSpan.FromMilliseconds(10) };
        }

        public DbOptions Options(Logger log, Listener? listener)
        {
            var o = new DbOptions
            {
                CreateIfMissing = true,
                FileSystem = Fs,
                RecycleLogFileNum = Recycle,
                WalRecoveryMode = Mode,
                BgErrorResumeRetryInterval = 10_000,
                InfoLog = log,
                SstFileManager = Sfm,
            };
            if (listener is not null) o.Listeners.Add(listener);
            return o;
        }

        public DbImpl Impl => Db.DefaultColumnFamily.Db;

        public void Open() => Db = RocksDatabase.Open(DbPath, Options(Log, Listener));

        public ulong PutAcked(string key, bool sync)
        {
            Db.Put(new WriteOptions { Sync = sync }, B(key), B("v-" + key));
            Acked[key] = "v-" + key;
            MaxAckedSeq = Db.GetLatestSequenceNumber();
            return MaxAckedSeq;
        }

        public void CloseQuietly()
        {
            try { Db?.Dispose(); }
            catch (Exception e) when (e is RocksDbException or IOException or AggregateException) { }
        }

        public void Dispose()
        {
            CloseQuietly();
            Sfm?.Dispose();
        }
    }

    private static string Wal(ulong n) => FileNames.LogFileName(DbPath, n);

    private static string RecoveryMode(WalRecoveryMode m) => m == WalRecoveryMode.PointInTimeRecovery ? "pit" : "skip";

    /// <summary>
    /// Flush rounds that fill the recycle list and make the current WAL a reused file (the failed WAL of the tests is
    /// then itself a recycled file with records of its previous use past the new data).
    /// </summary>
    private static void WarmUp(Harness h)
    {
        for (int r = 0; r < (int)h.Recycle + 2; r++)
        {
            for (int i = 0; i < 20; i++) h.PutAcked("warm-" + r + "-" + i, sync: i % 5 == 0);
            h.Db.Flush();
            WalRecyclingTests.WaitIdle(h.Db);
        }
        Assert.IsTrue(h.Impl.WalRecycleListForTest.Length > 0, "recycle list is empty after the warm-up");
        ulong current = h.Db.GetCurrentWalFile().LogNumber;
        Assert.IsTrue(h.Log.Numbers("reusing log ", " from recycle list").Count > 0, "no WAL was reused during the warm-up");
        Assert.IsTrue(h.Log.Any("from recycle list") && current > 0);
    }

    /// <summary>Acknowledged sync writes into the current WAL, then the poisoned write, which fails.</summary>
    private static void WritePreFailureAndPoison(Harness h, Poison p)
    {
        h.FirstPreFailureSeq = h.Db.GetLatestSequenceNumber() + 1;
        for (int i = 0; i < 5; i++) h.PutAcked("pre-" + i, sync: true);
        h.FailedWal = h.Db.GetCurrentWalFile().LogNumber;
        h.ReservedSeq = h.Db.GetLatestSequenceNumber() + 1;
        int faultsBefore = h.Fs.TargetedFaults;
        bool sync = false;
        switch (p)
        {
            case Poison.AppendNothingWritten:
                h.Fs.FailAppend(".log", null, AppendFailMode.NothingWritten, InjectedFaultKind.FileScope);
                break;
            case Poison.AppendTorn:
                h.Fs.FailAppend(".log", LostKey, AppendFailMode.Torn, InjectedFaultKind.FileScope);
                break;
            case Poison.AppendFullyWritten:
                h.Fs.FailAppend(".log", LostKey, AppendFailMode.FullyWritten, InjectedFaultKind.FileScope);
                break;
            case Poison.SyncError:
                h.Fs.FailMatching(".log", FaultOps.Sync, 1, InjectedFaultKind.FileScope);
                sync = true;
                break;
            case Poison.NoSpace:
                h.Fs.Inner.FreeSpace = 1000; // below the SstFileManager's reserved buffer: no recovery until raised
                h.Fs.FailAppend(".log", LostKey, AppendFailMode.Torn, InjectedFaultKind.NoSpace);
                break;
        }
        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => h.Db.Put(new WriteOptions { Sync = sync }, LostKey, LostValue));
        Assert.AreEqual(StatusCode.IOError, ex.Code, ex.ToString());
        Assert.AreEqual(faultsBefore + 1, h.Fs.TargetedFaults);
        if (p == Poison.NoSpace) Assert.AreEqual(StatusSubCode.NoSpace, ex.SubCode);
    }

    /// <summary>Waits for the automatic recovery (the SstFileManager's for NoSpace, once space is free) to succeed.</summary>
    private static void WaitRecovered(Harness h, Poison p)
    {
        if (p == Poison.NoSpace)
        {
            Assert.IsFalse(h.Listener.Ended.Wait(TimeSpan.FromMilliseconds(100)), "recovered without free space");
            RocksDbException stopped = Assert.ThrowsExactly<RocksDbException>(() => h.Db.Put(B("stopped-write"), B("x")));
            Assert.AreEqual(StatusSubCode.NoSpace, stopped.SubCode);
            h.Fs.Inner.FreeSpace = long.MaxValue;
        }
        Assert.IsTrue(h.Listener.Ended.Wait(Timeout), "recovery did not end");
        Assert.IsNull(h.Listener.Info!.NewBackgroundError, h.Listener.Info.NewBackgroundError?.ToString());
        Assert.IsNull(h.Db.BackgroundError);
    }

    /// <summary>The failed WAL was never put on the recycle list, never renamed into a new WAL, never reused.</summary>
    private static void AssertFailedWalNeverReused(Harness h, Logger log, ulong failedWal)
    {
        Assert.IsFalse(log.Any("reusing log " + failedWal + " from recycle list"), "failed WAL " + failedWal + " reused");
        Assert.IsFalse(log.Any("adding log " + failedWal + " to recycle list"), "failed WAL " + failedWal + " recycled");
        foreach ((string source, string target) in h.Fs.Renames)
            Assert.AreNotEqual(Wal(failedWal), source, "failed WAL renamed to " + target);
        CollectionAssert.DoesNotContain(h.Impl.WalRecycleListForTest, failedWal);
    }

    private readonly record struct Update(ulong Sequence, int Count, bool HasLost, string Text);

    /// <summary>The batches GetUpdatesSince(<paramref name="from"/>) returns now, and the error it stopped with.</summary>
    private static List<Update> ReadUpdates(RocksDatabase db, ulong from, out RocksDbException? error)
    {
        using TransactionLogIterator it = db.GetUpdatesSince(from);
        List<Update> r = Drain(it);
        error = it.Error;
        return r;
    }

    /// <summary>Reads the batches available now (an exhausted iterator is moved again to pick up newer ones).</summary>
    private static List<Update> Drain(TransactionLogIterator it)
    {
        var r = new List<Update>();
        if (!it.Valid) it.Next();
        while (it.Valid)
        {
            WalBatch b = it.Current;
            r.Add(new Update(b.Sequence, b.Count, b.Span.IndexOf(LostKey) >= 0, System.Text.Encoding.UTF8.GetString(b.Span[WriteBatch.HeaderSize..])));
            it.Next();
        }
        return r;
    }

    /// <summary>No batch holds the failed write or its reserved sequence; sequences strictly increase.</summary>
    private static void AssertFenced(List<Update> updates, ulong reservedSeq, string what)
    {
        ulong last = 0;
        foreach (Update u in updates)
        {
            Assert.IsFalse(u.HasLost, what + ": the failed batch was returned at " + u.Sequence);
            Assert.IsFalse(u.Sequence <= reservedSeq && reservedSeq < u.Sequence + (ulong)u.Count, what + ": batch at " + u.Sequence + " covers the reserved sequence");
            Assert.IsTrue(u.Sequence > last, what + ": sequence " + u.Sequence + " after " + last);
            last = u.Sequence + (ulong)u.Count - 1;
        }
    }

    /// <summary>
    /// After a successful recovery: the replacement WAL takes the writes, the failed batch is invisible (Get and
    /// GetUpdatesSince), its sequence is reserved, and healthy WALs keep being recycled while the failed one never is.
    /// </summary>
    private static void AssertAfterRecovery(Harness h, bool failedWalKept)
    {
        Assert.AreEqual(h.FailedWal, h.Impl.FailedWalNumberForTest);
        ulong replacement = h.Db.GetCurrentWalFile().LogNumber;
        Assert.IsTrue(replacement > h.FailedWal, "replacement WAL " + replacement + " after failed WAL " + h.FailedWal);
        Assert.AreEqual(failedWalKept, h.Fs.FileExists(Wal(h.FailedWal)), "failed WAL on disk");
        Assert.IsNull(GetString(h.Db, "lost-key-0001"));
        Assert.IsNull(GetString(h.Db, "stopped-write"));
        Assert.AreEqual(h.ReservedSeq, h.Db.GetLatestSequenceNumber(), "the failed write's sequence is reserved");
        AssertFailedWalNeverReused(h, h.Log, h.FailedWal);

        // Writes continue on the replacement WAL, with sequences after the reserved one.
        for (int i = 0; i < 6; i++)
        {
            ulong seq = h.PutAcked("post-" + i, sync: i % 2 == 0);
            Assert.AreEqual(h.ReservedSeq + 1 + (ulong)i, seq);
        }
        Assert.AreEqual(replacement, h.Db.GetCurrentWalFile().LogNumber);
        foreach ((string k, string v) in h.Acked) Assert.AreEqual(v, GetString(h.Db, k), k);

        // GetUpdatesSince: from before the failure (reads the failed WAL when it is still there) and from right after
        // the reserved sequence (the post-recovery writes, in order).
        List<Update> all = ReadUpdates(h.Db, h.FirstPreFailureSeq, out _);
        AssertFenced(all, h.ReservedSeq, "from the first pre-failure write");
        List<Update> post = ReadUpdates(h.Db, h.ReservedSeq + 1, out RocksDbException? postError);
        AssertFenced(post, h.ReservedSeq, "from after the reserved sequence");
        Assert.AreEqual(6, post.Count, postError?.ToString());
        Assert.AreEqual(h.ReservedSeq + 1, post[0].Sequence);

        // While DisableFileDeletions holds the failed WAL nothing is recycled at all (no purge runs).
        if (!failedWalKept) AssertHealthyWalsRecycled(h, "after");
        AssertFailedWalNeverReused(h, h.Log, h.FailedWal);
    }

    /// <summary>Flush rounds: healthy WALs (all newer than the failed one) are recycled and reused; the failed one never.</summary>
    private static void AssertHealthyWalsRecycled(Harness h, string prefix)
    {
        int reusedBefore = h.Log.Numbers("reusing log ", " from recycle list").Count;
        for (int r = 0; r < (int)h.Recycle + 2; r++)
        {
            for (int i = 0; i < 10; i++) h.PutAcked(prefix + "-" + r + "-" + i, sync: i == 0);
            h.Db.Flush();
            WalRecyclingTests.WaitIdle(h.Db);
        }
        List<ulong> added = h.Log.Numbers("adding log ", " to recycle list");
        List<ulong> reused = h.Log.Numbers("reusing log ", " from recycle list");
        Assert.IsTrue(added.Exists(n => n > h.FailedWal), "no WAL after the failed one was recycled");
        Assert.IsTrue(reused.Count > reusedBefore && reused.Exists(n => n > h.FailedWal), "no WAL after the failed one was reused");
        AssertFailedWalNeverReused(h, h.Log, h.FailedWal);
    }

    /// <summary>Closes and reopens the DB (twice): acknowledged writes survive, the failed one does not, no reuse.</summary>
    private static void AssertReopens(Harness h)
    {
        h.Db.Dispose();
        for (int round = 0; round < 2; round++)
        {
            var log = new Logger();
            h.Db = RocksDatabase.Open(DbPath, h.Options(log, null));
            foreach ((string k, string v) in h.Acked) Assert.AreEqual(v, GetString(h.Db, k), "reopen " + round + ": " + k);
            Assert.IsNull(GetString(h.Db, "lost-key-0001"), "reopen " + round + ": the failed write was resurrected");
            Assert.IsNull(GetString(h.Db, "stopped-write"));
            Assert.IsTrue(h.Db.GetLatestSequenceNumber() >= h.MaxAckedSeq, "reopen " + round + ": sequence went back");
            Assert.IsTrue(h.Db.GetLatestSequenceNumber() >= h.ReservedSeq, "reopen " + round + ": the reservation was not persisted");
            for (int r = 0; r < 2; r++)
            {
                h.PutAcked("reopen-" + round + "-" + r, sync: true);
                h.Db.Flush();
                WalRecyclingTests.WaitIdle(h.Db);
            }
            AssertFailedWalNeverReused(h, log, h.FailedWal);
            h.Db.Dispose();
        }
        h.Db = null!;
    }

    private static IEnumerable<(ulong Recycle, WalRecoveryMode Mode)> Configs()
    {
        foreach (ulong recycle in new ulong[] { 1, 4 })
        {
            foreach (WalRecoveryMode mode in new[] { WalRecoveryMode.PointInTimeRecovery, WalRecoveryMode.SkipAnyCorruptedRecords })
                yield return (recycle, mode);
        }
    }

    // ---- live recovery ---------------------------------------------------------------------------------------

    /// <summary>
    /// The poisoned WAL write (every variant) is recovered with recycling on: writes continue on a replacement WAL,
    /// the failed WAL is deleted instead of recycled, the failed batch is never visible, and everything acknowledged
    /// survives reopen.
    /// </summary>
    [TestMethod]
    [DataRow(Poison.AppendNothingWritten)]
    [DataRow(Poison.AppendTorn)]
    [DataRow(Poison.AppendFullyWritten)]
    [DataRow(Poison.SyncError)]
    [DataRow(Poison.NoSpace)]
    public void RecoveryWithRecyclingNeverReusesTheFailedWal(Poison poison)
    {
        foreach ((ulong recycle, WalRecoveryMode mode) in Configs())
        {
            using var h = new Harness(recycle, mode, sstFileManager: poison == Poison.NoSpace);
            string what = poison + " recycle=" + recycle + " " + RecoveryMode(mode);
            try
            {
                h.Open();
                WarmUp(h);
                WritePreFailureAndPoison(h, poison);
                WaitRecovered(h, poison);
                AssertAfterRecovery(h, failedWalKept: false);
                AssertReopens(h);
            }
            catch (Exception e) when (e is not AssertFailedException)
            {
                throw new AssertFailedException(what + ": " + e, e);
            }
            catch (AssertFailedException e)
            {
                throw new AssertFailedException(what + ": " + e.Message, e);
            }
        }
    }

    /// <summary>
    /// With file deletions disabled across the failure the failed WAL (possibly holding the complete failed record)
    /// stays on disk after the recovery: GetUpdatesSince iterators, opened before the failure or after the recovery,
    /// never return the failed batch (they stop at the fenced range), and once deletions are re-enabled the failed WAL
    /// is deleted, not recycled.
    /// </summary>
    [TestMethod]
    [DataRow(Poison.AppendNothingWritten)]
    [DataRow(Poison.AppendTorn)]
    [DataRow(Poison.AppendFullyWritten)]
    [DataRow(Poison.SyncError)]
    public void FailedWalKeptByDisabledDeletionsIsFencedAndNeverRecycled(Poison poison)
    {
        foreach ((ulong recycle, WalRecoveryMode mode) in Configs())
        {
            using var h = new Harness(recycle, mode, sstFileManager: false);
            string what = poison + " recycle=" + recycle + " " + RecoveryMode(mode);
            try
            {
                h.Open();
                WarmUp(h);
                h.Db.DisableFileDeletions();
                // A tailing iterator that already read the WAL up to the failure.
                h.PutAcked("tail-start", sync: true);
                using TransactionLogIterator tail = h.Db.GetUpdatesSince(h.Db.GetLatestSequenceNumber());
                List<Update> tailed = Drain(tail);
                WritePreFailureAndPoison(h, poison);
                tailed.AddRange(Drain(tail));
                WaitRecovered(h, poison);
                Assert.IsTrue(h.Fs.FileExists(Wal(h.FailedWal)));

                // Recovered: the reserved sequence is published now, and the failed WAL is still readable.
                tailed.AddRange(Drain(tail));
                AssertFenced(tailed, h.ReservedSeq, what + " tailing iterator");
                Assert.IsTrue(tailed.Count >= 6, what + ": tailing iterator returned " + tailed.Count + ", " + tail.Error);
                AssertAfterRecovery(h, failedWalKept: true);
                tailed.AddRange(Drain(tail));
                AssertFenced(tailed, h.ReservedSeq, what + " tailing iterator after the post-recovery writes");

                // Re-enabling deletions deletes the failed WAL (it is obsolete); it is never recycled.
                h.Db.EnableFileDeletions();
                WalRecyclingTests.WaitIdle(h.Db);
                Assert.IsFalse(h.Fs.FileExists(Wal(h.FailedWal)), what + ": failed WAL not deleted");
                AssertHealthyWalsRecycled(h, "enabled");
                // The tailing iterator ended its run at the fenced range: 'tail-start' and the five pre-failure writes.
                Assert.AreEqual(6, tailed.Count, what);
                Assert.AreEqual(StatusCode.NotFound, tail.Error?.Code, what);
                StringAssert.Contains(tail.Error!.Message, "indeterminate WAL write through sequence " + h.ReservedSeq);
                AssertReopens(h);
            }
            catch (Exception e) when (e is not AssertFailedException)
            {
                throw new AssertFailedException(what + ": " + e, e);
            }
            catch (AssertFailedException e)
            {
                throw new AssertFailedException(what + ": " + e.Message, e);
            }
        }
    }

    // ---- crash during the recovery -----------------------------------------------------------------------------

    /// <summary>What one crash run saw.</summary>
    private readonly record struct CrashRun(bool Crashed, string? At, bool Recovered, bool PostWriteAcked, string Step);

    /// <summary>
    /// Runs the poisoned write and its recovery with the file system dying at the <paramref name="k"/>-th mutating
    /// operation after the failure (power loss: unsynced data and directory entries are dropped; process kill:
    /// everything written survives), then reopens twice and checks the recovered contents.
    /// </summary>
    private static CrashRun RunCrash(Poison poison, ulong recycle, WalRecoveryMode mode, long k, bool powerLoss)
    {
        using var h = new Harness(recycle, mode, sstFileManager: poison == Poison.NoSpace);
        using var gate = new ManualResetEventSlim(false);
        TestHooks.Set("RecoverFromRetryableBGIOError:BeforeResume", () => gate.Wait(Timeout));
        bool recovered = false;
        bool postAcked = false;
        bool crashed;
        string? at;
        string step = "";
        try
        {
            h.Open();
            WarmUp(h);
            WritePreFailureAndPoison(h, poison);
            // The recovery waits at the gate (or for free space): arm the crash, then let it run.
            var ops = new ConcurrentQueue<string>();
            h.Fs.OpLog = ops;
            h.Fs.CrashAtOperation = h.Fs.Operations + k;
            gate.Set();
            if (poison == Poison.NoSpace) h.Fs.Inner.FreeSpace = long.MaxValue;
            SpinWait.SpinUntil(() => h.Listener.Ended.IsSet || h.Fs.Crashed, Timeout);
            recovered = h.Listener.Ended.IsSet && h.Listener.Info!.NewBackgroundError is null;
            if (recovered)
            {
                AssertFailedWalNeverReused(h, h.Log, h.FailedWal);
                for (int i = 0; i < 3; i++)
                {
                    try
                    {
                        h.PutAcked("post-" + i, sync: true);
                        postAcked = true;
                    }
                    catch (RocksDbException)
                    {
                        break;
                    }
                }
            }
            crashed = h.Fs.Crashed;
            at = h.Fs.CrashedAt;
            step = crashed ? ClassifyStep(at!, h.FailedWal, recovered) : "";
        }
        finally
        {
            gate.Set();
            TestHooks.ClearAll();
            h.CloseQuietly();
        }
        h.Fs.OpLog = null;
        if (powerLoss) h.Fs.PowerLoss();
        else FaultInjectionFileSystem.ProcessKill();
        h.Fs.Restart();

        // Reopen (twice: the second open sees what the first one's recovery and purge left).
        // A complete record of the failed write can survive a power loss too: the failed WAL is a reused file, and its
        // records overwrite synced bytes of the previous use in place (storage may keep either version).
        bool lostMayBeComplete = poison is Poison.AppendFullyWritten or Poison.SyncError;
        for (int round = 0; round < 2; round++)
        {
            var log = new Logger();
            using RocksDatabase db = RocksDatabase.Open(DbPath, h.Options(log, null));
            h.Db = db;
            string what = "reopen " + round + " (k=" + k + ", " + (powerLoss ? "power loss" : "process kill") + ", crash at " + at + ")";
            foreach ((string key, string v) in h.Acked) Assert.AreEqual(v, GetString(db, key), what + ": " + key);
            string? lost = GetString(db, "lost-key-0001");
            if (lost is not null)
            {
                // Only an ambiguous write whose complete record survived, and only while the recovery had not committed
                // its MANIFEST edit (nothing after the failure was acknowledged).
                Assert.IsTrue(lostMayBeComplete && !recovered && !postAcked, what + ": the failed write was resurrected");
                Assert.AreEqual("lost-value-0001", lost);
            }
            Assert.IsTrue(db.GetLatestSequenceNumber() >= h.MaxAckedSeq, what + ": sequence went back");
            if (recovered) Assert.IsTrue(db.GetLatestSequenceNumber() >= h.ReservedSeq, what + ": reservation lost");
            ulong seq = h.PutAcked("reopen-" + round, sync: true);
            if (recovered || lost is not null) Assert.IsTrue(seq > h.ReservedSeq, what + ": sequence " + seq + " reused");
            db.Flush();
            WalRecyclingTests.WaitIdle(db);
            AssertFailedWalNeverReused(h, log, h.FailedWal);
            h.Db = null!;
        }
        return new CrashRun(crashed, at, recovered, postAcked, step);
    }

    /// <summary>The recovery step an operation belongs to (for the coverage check of the sweep).</summary>
    private static string ClassifyStep(string op, ulong failedWal, bool recovered)
    {
        if (op.StartsWith("delete " + Wal(failedWal), StringComparison.Ordinal)) return "retirement";
        if (recovered) return op.Contains(".log", StringComparison.Ordinal) ? "first post-recovery write" : "after recovery";
        if (op.Contains(Wal(failedWal), StringComparison.Ordinal)) return "quarantine";
        if (op.Contains("MANIFEST", StringComparison.Ordinal) || op.Contains("CURRENT", StringComparison.Ordinal)) return "MANIFEST write";
        if (op.Contains(".sst", StringComparison.Ordinal)) return "atomic flush";
        if (op.Contains(".log", StringComparison.Ordinal)) return op.StartsWith("delete", StringComparison.Ordinal) ? "retirement" : "replacement WAL creation";
        if (op.StartsWith("syncdir", StringComparison.Ordinal)) return "directory sync";
        return "other: " + op;
    }

    /// <summary>
    /// A crash at every mutating file-system operation of the recovery (replacement WAL creation or reuse, the atomic
    /// flush's table files, the MANIFEST write, the failed WAL's deletion) and of the first post-recovery write, with
    /// power loss and with a process kill, reopens to a consistent DB: acknowledged writes are there, the failed write
    /// only when its complete record survived a crash before the recovery committed, no sequence is reused, and the
    /// failed WAL is never reused.
    /// </summary>
    [TestMethod]
    [DataRow(Poison.AppendNothingWritten)]
    [DataRow(Poison.AppendTorn)]
    [DataRow(Poison.AppendFullyWritten)]
    [DataRow(Poison.SyncError)]
    [DataRow(Poison.NoSpace)]
    public void CrashAtEveryRecoveryStepReopensConsistently(Poison poison)
    {
        foreach ((ulong recycle, WalRecoveryMode mode) in new[] { (1UL, WalRecoveryMode.PointInTimeRecovery), (4UL, WalRecoveryMode.SkipAnyCorruptedRecords) })
        {
            var steps = new HashSet<string>();
            var ats = new List<string>();
            long k = 1;
            for (; k < 500; k++)
            {
                CrashRun power = RunCrash(poison, recycle, mode, k, powerLoss: true);
                CrashRun kill = RunCrash(poison, recycle, mode, k, powerLoss: false);
                if (power.Crashed)
                {
                    steps.Add(power.Step);
                    ats.Add(power.Step + ":" + power.At);
                }
                if (kill.Crashed) steps.Add(kill.Step);
                if (!power.Crashed && !kill.Crashed) break;
            }
            string what = poison + " recycle=" + recycle + " " + RecoveryMode(mode) + " (" + (k - 1) + " crash points): " + string.Join(", ", steps) + " | " + string.Join("; ", ats);
            Assert.IsTrue(k < 500, what);
            foreach (string s in new[] { "replacement WAL creation", "atomic flush", "MANIFEST write", "retirement", "first post-recovery write" })
                Assert.IsTrue(steps.Contains(s), what + ": no crash during " + s);
            foreach (string s in steps) Assert.IsFalse(s == "quarantine", what + ": the failed WAL was written after the failure");
        }
    }

    // ---- concurrent writers ------------------------------------------------------------------------------------

    /// <summary>
    /// Many writers (several write groups, sync and non-sync writes, sync or async API) while the WAL is poisoned
    /// twice: every acknowledged write survives (live and after reopen), no failed write is ever visible, the
    /// failed WALs are never reused, and the WAL stream has no batch of a failed group.
    /// </summary>
    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task ConcurrentWritersAcrossPoisonedWals(bool async)
    {
        using var h = new Harness(recycle: 2, WalRecoveryMode.PointInTimeRecovery, sstFileManager: false);
        h.Open();
        WarmUp(h);
        var acked = new ConcurrentDictionary<string, ulong>();
        var failed = new ConcurrentDictionary<string, byte>();
        int stop = 0;
        const int writers = 8;
        var tasks = new Task[writers];
        for (int t = 0; t < writers; t++)
        {
            int id = t;
            tasks[t] = Task.Run(async () =>
            {
                for (int i = 0; Volatile.Read(ref stop) == 0; i++)
                {
                    string key = "w" + id + "-" + i.ToString("D6", CultureInfo.InvariantCulture);
                    var wo = new WriteOptions { Sync = i % 3 == 0 };
                    try
                    {
                        if (async) await h.Db.PutAsync(wo, B(key), B("v-" + key)).ConfigureAwait(false);
                        else h.Db.Put(wo, B(key), B("v-" + key));
                        acked[key] = 0;
                    }
                    catch (RocksDbException)
                    {
                        failed[key] = 0;
                        // Wait for the recovery instead of spinning on stopped writes.
                        SpinWait.SpinUntil(() => h.Db.BackgroundError is null || Volatile.Read(ref stop) != 0, Timeout);
                    }
                }
            });
        }
        var failedWals = new List<ulong>();
        try
        {
            for (int round = 0; round < 2; round++)
            {
                int target = acked.Count + 300;
                Assert.IsTrue(SpinWait.SpinUntil(() => acked.Count >= target, Timeout), "writers stalled");
                h.Listener.Ended.Reset();
                // The next WAL append of whichever group leader gets there first: its complete record reaches the WAL.
                h.Fs.FailAppend(".log", B("v-w"), AppendFailMode.FullyWritten, InjectedFaultKind.FileScope);
                Assert.IsTrue(h.Listener.Ended.Wait(Timeout), "recovery did not end");
                Assert.IsNull(h.Listener.Info!.NewBackgroundError);
                failedWals.Add(h.Impl.FailedWalNumberForTest);
                Assert.IsTrue(failed.Count > 0);
            }
            int end = acked.Count + 300;
            Assert.IsTrue(SpinWait.SpinUntil(() => acked.Count >= end, Timeout), "writers stalled after recovery");
        }
        finally
        {
            Volatile.Write(ref stop, 1);
            await Task.WhenAll(tasks).ConfigureAwait(false);
        }
        Assert.AreEqual(2, failedWals.Count);
        Assert.IsTrue(failedWals[1] > failedWals[0]);
        foreach (string k in acked.Keys) Assert.AreEqual("v-" + k, GetString(h.Db, k), k);
        foreach (string k in failed.Keys) Assert.IsNull(GetString(h.Db, k), "failed write " + k + " is visible");

        // The WAL stream (whatever is still live) never holds a batch of a failed write.
        ulong first = 1;
        foreach (WalFileInfo f in h.Db.GetSortedWalFiles()) { first = f.StartSequence; break; }
        using (TransactionLogIterator it = h.Db.GetUpdatesSince(first))
        {
            while (it.Valid)
            {
                WalBatch b = it.Current;
                string text = System.Text.Encoding.UTF8.GetString(b.Span);
                // Keys are "w<id>-<6 digits>": no key is a substring of another.
                foreach (string k in failed.Keys) Assert.IsFalse(text.Contains(k, StringComparison.Ordinal), "WAL batch at " + b.Sequence + " holds failed write " + k);
                it.Next();
            }
        }

        for (int r = 0; r < 3; r++)
        {
            h.Db.Put(B("final-" + r), B("x"));
            h.Db.Flush();
            WalRecyclingTests.WaitIdle(h.Db);
        }
        foreach (ulong w in failedWals) AssertFailedWalNeverReused(h, h.Log, w);
        h.Db.Dispose();
        var log = new Logger();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, h.Options(log, null)))
        {
            h.Db = db;
            foreach (string k in acked.Keys) Assert.AreEqual("v-" + k, GetString(db, k), "reopen: " + k);
            foreach (string k in failed.Keys) Assert.IsNull(GetString(db, k), "reopen: failed write " + k + " resurrected");
            for (int r = 0; r < 3; r++)
            {
                db.Put(B("again-" + r), B("x"));
                db.Flush();
                WalRecyclingTests.WaitIdle(db);
            }
            foreach (ulong w in failedWals) AssertFailedWalNeverReused(h, log, w);
        }
        h.Db = null!;
    }
}
