using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Tests.Db;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #7, failures on the write / flush / compaction path: I/O errors injected at any mutating file operation
/// (ErrorHandler::SetBGError, DBImpl::WriteImpl, BackgroundCallFlush / BackgroundCallCompaction,
/// VersionSet::ProcessManifestWrites, DBImpl::ResumeImpl), and the lifecycle of the DB around them (DBImpl::CloseHelper,
/// CancelAllBackgroundWork).
/// </summary>
[TestClass]
public sealed class Audit7WritePathFailuresTests
{
    private const string DbPath = "/audit7writefail";
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    [TestCleanup]
    public void Cleanup() => TestHooks.ClearAll();

    /// <summary>The write modes of DBImpl::WriteImpl the sweep runs in.</summary>
    public enum SweepMode
    {
        Default,
        Pipelined,
        Unordered,
        TwoWriteQueues,
    }

    private static DbOptions SweepOptions(IFileSystem fs, bool autoResume, SweepMode mode)
    {
        DbOptions o = NewOptions(fs);
        o.MaxBgErrorResumeCount = autoResume ? 3 : 0;
        o.BgErrorResumeRetryInterval = 1000;
        o.EnablePipelinedWrite = mode == SweepMode.Pipelined;
        o.UnorderedWrite = mode == SweepMode.Unordered;
        o.TwoWriteQueues = mode == SweepMode.TwoWriteQueues;
        return o;
    }

    private const int SweepThreads = 3;

    private static ColumnFamilyOptions SweepCfOptions() => new()
    {
        WriteBufferSize = 4 << 10,
        Level0FileNumCompactionTrigger = 2,
        MaxBytesForLevelBase = 16 << 10,
        TargetFileSizeBase = 8 << 10,
    };

    /// <summary>One operation of the sweep workload and whether the DB acknowledged it.</summary>
    private readonly record struct Op(int Key, string? Value, bool Acked);

    /// <summary>
    /// Runs a write / flush / compaction workload with one transient I/O error injected at mutating file operation
    /// <paramref name="failAt"/> (counted from after the open), resuming after every failed write
    /// (DBImpl::Resume) and closing at the end. Returns the operations in order.
    /// </summary>
    private static List<Op> RunSweepWorkload(FaultInjectionFileSystem fs, long failAt, InjectedFaultKind kind, bool autoResume, int seed,
        SweepMode mode)
    {
        RocksDatabase db = RocksDatabase.Open(DbPath, SweepOptions(fs, autoResume, mode), SweepCfOptions());
        var perThread = new List<Op>[SweepThreads];
        try
        {
            if (failAt > 0)
            {
                fs.ErrorKind = kind;
                fs.TransientFailure = true;
                fs.CrashAtOperation = fs.Operations + failAt;
            }
            // Writers on disjoint keys (key % SweepThreads == thread), so each key's operations are ordered.
            var threads = new Task[SweepThreads];
            for (int t = 0; t < SweepThreads; t++)
            {
                int thread = t;
                threads[t] = Task.Factory.StartNew(() => perThread[thread] = RunSweepWriter(db, thread, autoResume, seed * 31 + thread),
                    TaskCreationOptions.LongRunning);
            }
            Task.WaitAll(threads);
            try { db.CompactRange(); }
            catch (RocksDbException) { TryResume(db, autoResume); }
        }
        finally
        {
            try { db.Dispose(); }
            catch (RocksDbException) { }
        }
        var ops = new List<Op>();
        foreach (List<Op> l in perThread) ops.AddRange(l);
        return ops;
    }

    private static List<Op> RunSweepWriter(RocksDatabase db, int thread, bool autoResume, int seed)
    {
        var ops = new List<Op>();
        var rnd = new Random(seed);
        {
            for (int i = 0; i < 150; i++)
            {
                int key = rnd.Next(14) * SweepThreads + thread;
                bool delete = rnd.Next(10) == 0;
                string? value = delete ? null : "v" + thread.ToString(System.Globalization.CultureInfo.InvariantCulture) + "-"
                    + i.ToString(System.Globalization.CultureInfo.InvariantCulture) + new string('x', 100);
                var wo = new WriteOptions { Sync = rnd.Next(8) == 0 };
                bool acked;
                try
                {
                    if (delete) db.Delete(wo, Key(key));
                    else db.Put(wo, Key(key), B(value!));
                    acked = true;
                }
                catch (RocksDbException)
                {
                    acked = false;
                }
                ops.Add(new Op(key, value, acked));
                if (!acked)
                {
                    // Reads keep working while writes are stopped.
                    _ = db.Get(Key(key));
                    TryResume(db, autoResume);
                }
                if (thread == 0 && i % 47 == 46)
                {
                    try { db.Flush(); }
                    catch (RocksDbException) { TryResume(db, autoResume); }
                }
            }
        }
        return ops;
    }

    private static void TryResume(RocksDatabase db, bool autoResume)
    {
        if (autoResume)
        {
            // Let the automatic recovery run (it retries every millisecond, at most three times).
            for (int i = 0; i < 200; i++)
            {
                try
                {
                    db.Resume();
                    return;
                }
                catch (RocksDbException e) when (e.Code == StatusCode.Busy)
                {
                    Thread.Sleep(5);
                }
                catch (RocksDbException)
                {
                    return;
                }
            }
            return;
        }
        try { db.Resume(); }
        catch (RocksDbException) { }
    }

    /// <summary>
    /// After the reopen: the value of every key is the last acknowledged one, or that of a failed write issued after it
    /// (a failed write's outcome is indeterminate, DB::Write in db.h); a key whose last acknowledged operation is a
    /// delete stays deleted unless a later failed put reappears.
    /// </summary>
    private static void VerifyAfterReopen(FaultInjectionFileSystem fs, List<Op> ops, string context, SweepMode mode)
    {
        using RocksDatabase db = RocksDatabase.Open(DbPath, SweepOptions(fs, autoResume: false, mode), SweepCfOptions());
        var keys = new HashSet<int>();
        foreach (Op op in ops) keys.Add(op.Key);
        foreach (int key in keys)
        {
            int lastAcked = -1;
            for (int i = 0; i < ops.Count; i++)
            {
                if (ops[i].Key == key && ops[i].Acked) lastAcked = i;
            }
            var allowed = new HashSet<string?>();
            if (lastAcked >= 0) allowed.Add(ops[lastAcked].Value);
            else allowed.Add(null);
            for (int i = lastAcked + 1; i < ops.Count; i++)
            {
                if (ops[i].Key == key) allowed.Add(ops[i].Value);
            }
            string? actual = GetString(db, S(Key(key))!);
            Assert.IsTrue(allowed.Contains(actual),
                context + ": key " + key + " = " + (actual ?? "<none>") + ", expected one of [" + string.Join(", ", allowed) + "]");
        }
    }

    /// <summary>
    /// A transient I/O error (plain, retryable, out of space) at a random mutating file operation of a workload that
    /// writes, flushes and compacts (WAL append / sync, new WAL, table file write / sync, MANIFEST write / sync, CURRENT,
    /// directory fsync, deletion), followed by DB::Resume: no acknowledged write is lost or reordered after the reopen,
    /// nothing hangs. ErrorHandler::SetBGError, DBImpl::ResumeImpl, VersionSet::ProcessManifestWrites.
    /// </summary>
    [TestMethod]
    [DataRow(0, false, SweepMode.Default)] // IOError
    [DataRow(1, false, SweepMode.Default)] // Retryable
    [DataRow(1, true, SweepMode.Default)]
    [DataRow(3, false, SweepMode.Default)] // NoSpace
    [DataRow(2, true, SweepMode.Default)] // FileScope
    [DataRow(0, false, SweepMode.Pipelined)]
    [DataRow(1, true, SweepMode.Pipelined)]
    [DataRow(0, false, SweepMode.Unordered)]
    [DataRow(1, true, SweepMode.Unordered)]
    [DataRow(0, false, SweepMode.TwoWriteQueues)]
    [DataRow(1, true, SweepMode.TwoWriteQueues)]
    public void TransientIoErrorAtAnyOperationLosesNoAcknowledgedWrite(int kindValue, bool autoResume, SweepMode mode)
    {
        var kind = (InjectedFaultKind)kindValue;
        int seed = Environment.GetEnvironmentVariable("AUDIT7_SEED") is { } s ? int.Parse(s, System.Globalization.CultureInfo.InvariantCulture)
            : Environment.TickCount;
        Console.WriteLine("seed=" + seed);
        var pick = new Random(seed);
        // A dry run to learn how many mutating operations the workload makes.
        var dry = new FaultInjectionFileSystem(NewFs());
        long before = dry.Operations;
        dry.OpLog = new System.Collections.Concurrent.ConcurrentQueue<string>();
        RunSweepWorkload(dry, 0, kind, autoResume, seed, mode);
        long total = dry.Operations - before;
        Assert.IsTrue(total > 100, "workload too small: " + total);
        // Operations other than WAL / info LOG appends and flushes (table files, MANIFEST, CURRENT, directories,
        // deletions, WAL creation and syncs): half of the faults go there, the WAL appends dominate otherwise.
        var rare = new List<long>();
        long index = 0;
        foreach (string op in dry.OpLog)
        {
            index++;
            if (index <= before) continue;
            bool walAppend = (op.StartsWith("append ", StringComparison.Ordinal) || op.StartsWith("flush ", StringComparison.Ordinal))
                && (op.EndsWith(".log", StringComparison.Ordinal) || op.EndsWith("/LOG", StringComparison.Ordinal));
            if (!walAppend) rare.Add(index - before);
        }
        int hit = 0, failedWrites = 0;
        var where = new System.Text.StringBuilder();
        for (int iteration = 0; iteration < 25; iteration++)
        {
            long failAt = iteration % 2 == 0 && rare.Count > 0 ? rare[pick.Next(rare.Count)] : 1 + pick.NextInt64(total);
            var fs = new FaultInjectionFileSystem(NewFs());
            string context = "seed=" + seed + " kind=" + kind + " auto=" + autoResume + " mode=" + mode + " failAt=" + failAt;
            Task<List<Op>> run = Task.Run(() => RunSweepWorkload(fs, failAt, kind, autoResume, seed + iteration, mode));
            Assert.IsTrue(run.Wait(Timeout), context + ": the workload hung");
            int failedHere = 0;
            foreach (Op op in run.Result) if (!op.Acked) failedHere++;
            failedWrites += failedHere;
            if (fs.CrashedAt is { } at)
            {
                hit++;
                where.Append(at).Append(" (").Append(failedHere).Append(" failed) | ");
            }
            fs.Restart();
            VerifyAfterReopen(fs, run.Result, context + " at " + fs.CrashedAt, mode);
            Assert.AreEqual(0, fs.OpenFiles, context + ": file handles left open");
        }
        Console.WriteLine("ops=" + total + " faults hit=" + hit + " failed writes=" + failedWrites + " at: " + where);
        Assert.IsTrue(hit > 10, "faults were not injected");
    }

    /// <summary>
    /// DBImpl::CloseHelper: closing the WAL (LogWriterNumber::ClearWriter) or the MANIFEST (VersionSet::Close) never
    /// stops the close half way: the other files are still closed and the LOCK is released. ClearWriter returns the
    /// error of the WAL's WriteBuffer ("Unable to clear writer for WAL", "Retain the first error") and Close reports it;
    /// a failure of closing the WAL file itself is ignored (~Writer); a failed MANIFEST close is repaired by writing a
    /// new MANIFEST (LogAndApply of an empty edit). The data stays readable after the reopen.
    /// </summary>
    [TestMethod]
    [DataRow(".log", 64, false, false)] // FaultOps.Flush: ClearWriter's WriteBuffer of the current WAL fails
    [DataRow(".log", 64, true, false)]
    [DataRow(".log", 64, false, true)] // ... of a closed WAL not synced yet (the current one is still closed)
    [DataRow(".log", 32, false, false)] // FaultOps.Close: ~Writer's close fails
    [DataRow(".log", 32, true, true)]
    [DataRow("MANIFEST", 32, false, true)]
    [DataRow("MANIFEST", 32, true, true)]
    public async Task CloseErrorOfWalOrManifestStillReleasesEverything(string fragment, int faultOps, bool disposeAsync, bool flushFirst)
    {
        var fs = new FaultInjectionFileSystem(NewFs());
        RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        db.Put(B("flushed"), B("1"));
        if (flushFirst) db.Flush();
        db.Put(B("inwal"), B("2"));
        Assert.AreEqual("1", GetString(db, "flushed")); // a table reader is open
        fs.FailMatching(fragment, (FaultOps)faultOps, 1, InjectedFaultKind.IOError);
        bool reported = fragment == ".log" && (FaultOps)faultOps == FaultOps.Flush;
        Exception? error = null;
        try
        {
            if (disposeAsync) await db.DisposeAsync();
            else db.Dispose();
        }
        catch (Exception e)
        {
            error = e;
        }
        Assert.AreEqual(0, fs.OpenFiles, "file handles left open by the failed close: " + error);
        Assert.AreEqual(0, fs.OpenLocks, "LOCK not released");
        Assert.AreEqual(1, fs.TargetedFaults);
        if (reported)
        {
            Assert.IsInstanceOfType<RocksDbException>(error);
            Assert.AreEqual(StatusCode.IOError, ((RocksDbException)error).Code);
        }
        else
        {
            Assert.IsNull(error, error?.ToString());
        }
        db.Dispose(); // closing again is a no-op
        using RocksDatabase reopened = RocksDatabase.Open(DbPath, NewOptions(fs));
        Assert.AreEqual("1", GetString(reopened, "flushed"));
        Assert.AreEqual("2", GetString(reopened, "inwal"));
    }
    /// <summary>The outcome of one asynchronous write in the race tests.</summary>
    private enum Outcome
    {
        Pending,
        Committed,
        Disposed,
        Canceled,
        Failed,
    }

    /// <summary>
    /// DBImpl::CloseHelper racing with everything that waits inside the DB: asynchronous writers in the group-commit queue,
    /// writers stalled by stopped writes (DBImpl::DelayWrite stops waiting once shutting_down_ is set), manual
    /// CompactRange calls (RunManualCompaction: ShutdownInProgress), and Flush(wait) (WaitForFlushMemTables). Every call
    /// completes (nothing hangs), with success or ObjectDisposed / ShutdownInProgress, and every write that returned
    /// success is in the DB after the reopen.
    /// </summary>
    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task CloseRacingWritersStallsCompactionsAndFlushesCompletesEveryCall(bool stopWrites)
    {
        int seed = Environment.TickCount;
        Console.WriteLine("seed=" + seed);
        var rnd = new Random(seed);
        for (int round = 0; round < 6; round++)
        {
            MemoryFileSystem fs = NewFs();
            var cfo = new ColumnFamilyOptions { WriteBufferSize = 8 << 10, Level0FileNumCompactionTrigger = 2 };
            using var gate = new ManualResetEventSlim(!stopWrites);
            if (stopWrites)
            {
                // Writes stop after two L0 files (GetWriteStallConditionAndCause) and every compaction waits at its start
                // until the close began, so the writers are stalled when it does.
                cfo.Level0SlowdownWritesTrigger = 2;
                cfo.Level0StopWritesTrigger = 2;
                TestHooks.Set("DBImpl::RunCompaction:Start", () => gate.Wait(Timeout));
            }
            RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
            const int Writers = 8, PerWriter = 1000;
            var outcomes = new Outcome[Writers, PerWriter];
            var unexpected = new System.Collections.Concurrent.ConcurrentQueue<Exception>();
            var tasks = new List<Task>();
            for (int w = 0; w < Writers; w++)
            {
                int writer = w;
                tasks.Add(Task.Run(async () =>
                {
                    for (int i = 0; i < PerWriter; i++)
                    {
                        try
                        {
                            await db.PutAsync(Key(writer * PerWriter + i), Value(writer * PerWriter + i, 0));
                            outcomes[writer, i] = Outcome.Committed;
                        }
                        catch (ObjectDisposedException)
                        {
                            outcomes[writer, i] = Outcome.Disposed;
                        }
                        catch (RocksDbException e) when (e.Code == StatusCode.ShutdownInProgress)
                        {
                            outcomes[writer, i] = Outcome.Failed;
                        }
                        catch (Exception e)
                        {
                            unexpected.Enqueue(e);
                            return;
                        }
                    }
                }));
            }
            for (int c = 0; c < 2; c++)
            {
                tasks.Add(Task.Run(async () =>
                {
                    for (int i = 0; i < 50; i++)
                    {
                        try
                        {
                            await db.CompactRangeAsync();
                        }
                        catch (ObjectDisposedException)
                        {
                            return;
                        }
                        catch (RocksDbException e) when (e.Code == StatusCode.ShutdownInProgress)
                        {
                            return;
                        }
                        catch (Exception e)
                        {
                            unexpected.Enqueue(e);
                            return;
                        }
                    }
                }));
            }
            tasks.Add(Task.Run(async () =>
            {
                for (int i = 0; i < 50; i++)
                {
                    try
                    {
                        await db.FlushAsync();
                    }
                    catch (ObjectDisposedException)
                    {
                        return;
                    }
                    catch (RocksDbException e) when (e.Code == StatusCode.ShutdownInProgress)
                    {
                        return;
                    }
                    catch (Exception e)
                    {
                        unexpected.Enqueue(e);
                        return;
                    }
                }
            }));
            await Task.Delay(rnd.Next(0, 15));
            Task close = db.DisposeAsync().AsTask();
            await Task.Delay(rnd.Next(0, 5));
            gate.Set();
            Assert.IsTrue(close.Wait(Timeout), "seed=" + seed + " round " + round + ": close hung");
            if (!Task.WaitAll(tasks.ToArray(), Timeout))
            {
                var hung = new List<string>();
                for (int t = 0; t < tasks.Count; t++)
                {
                    if (!tasks[t].IsCompleted) hung.Add(t < Writers ? "writer " + t : t < Writers + 2 ? "CompactRange " + t : "Flush");
                }
                Assert.Fail("seed=" + seed + " round " + round + ": calls racing the close hung: " + string.Join(", ", hung));
            }
            Assert.IsTrue(unexpected.IsEmpty, "seed=" + seed + ": " + string.Join("\n", unexpected));
            int committed = 0, disposed = 0;
            foreach (Outcome o in outcomes)
            {
                if (o == Outcome.Committed) committed++;
                else if (o == Outcome.Disposed) disposed++;
            }
            Console.WriteLine("round " + round + ": committed=" + committed + " disposed=" + disposed);
            using RocksDatabase reopened = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
            for (int w = 0; w < Writers; w++)
            {
                for (int i = 0; i < PerWriter; i++)
                {
                    if (outcomes[w, i] != Outcome.Committed) continue;
                    CollectionAssert.AreEqual(Value(w * PerWriter + i, 0), reopened.Get(Key(w * PerWriter + i)),
                        "seed=" + seed + " round " + round + ": acknowledged write " + (w * PerWriter + i) + " lost");
                }
            }
        }
    }

    /// <summary>
    /// A write's CancellationToken (managed extension; DBImpl::WriteImpl has none) never leaves a half-applied write: a
    /// write that throws OperationCanceledException is not in the DB, one that returned is. Writes are stopped
    /// (DBImpl::DelayWrite) while tokens fire, then a manual compaction ends the stop. Also a token canceled before the
    /// call.
    /// </summary>
    [TestMethod]
    public async Task CanceledWritesAreAllOrNothing()
    {
        int seed = Environment.TickCount;
        Console.WriteLine("seed=" + seed);
        var rnd = new Random(seed);
        MemoryFileSystem fs = NewFs();
        var cfo = new ColumnFamilyOptions
        {
            WriteBufferSize = 8 << 10, Level0FileNumCompactionTrigger = 2, Level0SlowdownWritesTrigger = 2, Level0StopWritesTrigger = 2,
        };
        // The compaction that would end the write stop waits at its start until the tokens fired.
        using var gate = new ManualResetEventSlim(false);
        TestHooks.Set("DBImpl::RunCompaction:Start", () => gate.Wait(Timeout));
        await using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
        using (var pre = new CancellationTokenSource())
        {
            pre.Cancel();
            bool canceled = false;
            try
            {
                await db.PutAsync(B("pre"), B("1"), pre.Token);
            }
            catch (OperationCanceledException)
            {
                canceled = true;
            }
            Assert.AreEqual(canceled ? null : "1", GetString(db, "pre"), "pre-canceled write half applied");
        }
        // Two L0 files: writes stop.
        for (int f = 0; f < 2; f++)
        {
            db.Put(Key(100000), Value(f, 1)); // overlapping files: no trivial move
            db.Flush();
        }
        const int Writers = 16;
        var outcomes = new Outcome[Writers];
        var sources = new CancellationTokenSource[Writers];
        var tasks = new Task[Writers];
        for (int w = 0; w < Writers; w++)
        {
            int writer = w;
            sources[w] = new CancellationTokenSource();
            tasks[w] = Task.Run(async () =>
            {
                try
                {
                    await db.PutAsync(Key(writer), Value(writer, 2), sources[writer].Token);
                    outcomes[writer] = Outcome.Committed;
                }
                catch (OperationCanceledException)
                {
                    outcomes[writer] = Outcome.Canceled;
                }
            });
        }
        await Task.Delay(50);
        // Only the writer leading the stalled group observes its token (the others wait in the queue); cancel a random
        // half first, then the rest, while writes are still stopped.
        for (int w = 0; w < Writers; w++)
        {
            if (rnd.Next(2) == 0) sources[w].Cancel();
        }
        await Task.Delay(20);
        for (int w = 0; w < Writers; w++) sources[w].Cancel();
        await Task.Delay(20);
        gate.Set();
        Assert.IsTrue(Task.WaitAll(tasks, Timeout), "seed=" + seed + ": writes did not finish");
        int canceledCount = 0;
        for (int w = 0; w < Writers; w++)
        {
            byte[]? v = db.Get(Key(w));
            if (outcomes[w] == Outcome.Committed) CollectionAssert.AreEqual(Value(w, 2), v, "seed=" + seed + ": committed write " + w + " missing");
            else
            {
                canceledCount++;
                Assert.IsNull(v, "seed=" + seed + ": canceled write " + w + " was applied");
            }
            sources[w].Dispose();
        }
        Console.WriteLine("canceled=" + canceledCount);
        Assert.IsTrue(canceledCount > 0, "no write was canceled");
    }
    /// <summary>Vetoes every automatic recovery (OnErrorRecoveryBegin) and records the background errors.</summary>
    private sealed class NoAutoRecoveryListener : EventListener
    {
        public readonly System.Collections.Concurrent.ConcurrentQueue<RocksDbException> Errors = new();

        public override void OnBackgroundError(BackgroundErrorEventArgs e)
        {
            if (e.Error is { } error) Errors.Enqueue(error);
        }

        public override void OnErrorRecoveryBegin(ErrorRecoveryBeginEventArgs e) => e.AutoRecovery = false;
    }

    /// <summary>
    /// DBImpl::DelayWrite: "Don't wait if there's a background error that is not pending recovery since recovery might
    /// never be attempted" -- a write waiting for stopped writes gives up with Incomplete(&lt;background error&gt;) when a
    /// background error is set and no recovery is in progress. Here a compaction fails with an out-of-space error (a soft
    /// error with paranoid_checks, ErrorSeverityMap) whose automatic recovery a listener vetoed, while the L0 file count
    /// stops writes.
    /// </summary>
    [TestMethod]
    public async Task StalledWriteGivesUpOnBackgroundErrorWithoutRecovery()
    {
        var fs = new FaultInjectionFileSystem(NewFs());
        var listener = new NoAutoRecoveryListener();
        DbOptions options = NewOptions(fs);
        options.Listeners.Add(listener);
        var cfo = new ColumnFamilyOptions { Level0FileNumCompactionTrigger = 2, Level0SlowdownWritesTrigger = 2, Level0StopWritesTrigger = 2 };
        using var gate = new ManualResetEventSlim(false);
        TestHooks.Set("DBImpl::RunCompaction:Start", () => gate.Wait(Timeout));
        RocksDatabase db = RocksDatabase.Open(DbPath, options, cfo);
        try
        {
            // Two L0 files whose key ranges overlap ([a, z] and [m, m]): a real compaction, not a trivial move.
            db.Put(B("a"), B("1"));
            db.Put(B("z"), B("1"));
            db.Flush();
            db.Put(B("m"), B("1"));
            db.Flush();
            // The compaction of the two L0 files fails to write its output: out of space, for as long as the test runs.
            fs.FailMatching(".sst", FaultOps.Create | FaultOps.Append, int.MaxValue, InjectedFaultKind.NoSpace);
            gate.Set();
            Task put = db.PutAsync(B("stalled"), B("1")).AsTask();
            Task done = await Task.WhenAny(put, Task.Delay(Timeout));
            Assert.AreSame(put, done, "the stalled write waits although nothing will recover the background error");
            RocksDbException e = await Assert.ThrowsExactlyAsync<RocksDbException>(() => put);
            Assert.AreEqual(StatusCode.Incomplete, e.Code);
            StringAssert.Contains(e.Message, "No space left on device");
            Assert.IsFalse(listener.Errors.IsEmpty);
            Assert.AreEqual(ErrorSeverity.SoftError, listener.Errors.First().Severity);
            Assert.IsNull(GetString(db, "stalled"));
        }
        finally
        {
            fs.ClearFailMatching();
            gate.Set();
            db.Dispose();
        }
    }
    /// <summary>
    /// After Close every API fails with ObjectDisposedException (a managed contract: native has no use-after-close
    /// behavior, the handle is gone), WaitForCompact with ShutdownInProgress (DBImpl::WaitForCompact once
    /// shutting_down_ is set), and Dispose again is a no-op -- never a NullReferenceException or a hang. Synchronous and
    /// asynchronous forms, default and named column family.
    /// </summary>
    [TestMethod]
    public async Task EveryCallAfterCloseFailsWithObjectDisposed()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        o.CreateMissingColumnFamilies = true;
        RocksDatabase db = RocksDatabase.Open(DbPath, o, new ColumnFamilyOptions { MergeOperator = new StringAppendOperator() });
        ColumnFamily cf = db.CreateColumnFamily("cf", new ColumnFamilyOptions());
        db.Put(B("a"), B("1"));
        cf.Put(B("a"), B("1"));
        db.Flush();
        await db.DisposeAsync();
        var calls = new List<(string Name, Func<Task> Call)>
        {
            ("Put", () => { db.Put(B("b"), B("2")); return Task.CompletedTask; }),
            ("PutAsync", async () => await db.PutAsync(B("b"), B("2"))),
            ("cf.Put", () => { cf.Put(B("b"), B("2")); return Task.CompletedTask; }),
            ("Delete", () => { db.Delete(B("a")); return Task.CompletedTask; }),
            ("DeleteRange", () => { db.DeleteRange(B("a"), B("b")); return Task.CompletedTask; }),
            ("Merge", () => { db.Merge(B("a"), B("x")); return Task.CompletedTask; }),
            ("Write", () =>
            {
                using WriteBatch batch = db.CreateWriteBatch();
                batch.Put(B("b"), B("2"));
                db.Write(batch);
                return Task.CompletedTask;
            }),
            ("WriteAsync", async () =>
            {
                using WriteBatch batch = db.CreateWriteBatch();
                batch.Put(B("b"), B("2"));
                await db.WriteAsync(batch);
            }),
            ("Get", () => { _ = db.Get(B("a")); return Task.CompletedTask; }),
            ("cf.Get", () => { _ = cf.Get(B("a")); return Task.CompletedTask; }),
            ("MultiGet", () => { _ = db.MultiGet(new[] { B("a") }); return Task.CompletedTask; }),
            ("NewIterator", () => { using Iterator it = db.NewIterator(); it.SeekToFirst(); return Task.CompletedTask; }),
            ("GetSnapshot", () => { using Snapshot s = db.GetSnapshot(); return Task.CompletedTask; }),
            ("Flush", () => { db.Flush(); return Task.CompletedTask; }),
            ("FlushAsync", async () => await db.FlushAsync()),
            ("CompactRange", () => { db.CompactRange(); return Task.CompletedTask; }),
            ("CompactRangeAsync", async () => await db.CompactRangeAsync()),
            ("GetProperty", () => { _ = db.GetProperty("rocksdb.stats"); return Task.CompletedTask; }),
            ("GetLiveFilesMetadata", () => { _ = db.GetLiveFilesMetadata(); return Task.CompletedTask; }),
            ("CreateColumnFamily", () => { db.CreateColumnFamily("cf2", new ColumnFamilyOptions()); return Task.CompletedTask; }),
            ("Resume", () => { db.Resume(); return Task.CompletedTask; }),
            ("WaitForCompact", () => { db.WaitForCompact(); return Task.CompletedTask; }),
            ("Dispose", () => { db.Dispose(); return Task.CompletedTask; }),
        };
        var wrong = new List<string>();
        foreach ((string name, Func<Task> call) in calls)
        {
            Task t;
            try
            {
                t = call();
            }
            catch (Exception e)
            {
                t = Task.FromException(e);
            }
            if (!((IAsyncResult)t).AsyncWaitHandle.WaitOne(Timeout))
            {
                wrong.Add(name + ": hung");
                continue;
            }
            Exception? error = t.Exception?.InnerException;
            string expected = name switch
            {
                "Dispose" => "none",
                "WaitForCompact" => "ShutdownInProgress",
                _ => "ObjectDisposed",
            };
            string actual = error switch
            {
                null => "none",
                ObjectDisposedException => "ObjectDisposed",
                RocksDbException { Code: StatusCode.ShutdownInProgress } => "ShutdownInProgress",
                _ => error.GetType().Name + ": " + error.Message,
            };
            if (actual != expected) wrong.Add(name + ": " + actual);
        }
        Assert.AreEqual(0, wrong.Count, string.Join("\n", wrong));
    }
    private static SortedSet<string> SstOnDisk(IFileSystem fs)
    {
        var set = new SortedSet<string>(StringComparer.Ordinal);
        foreach (string f in fs.GetChildren(DbPath))
        {
            string name = Path.GetFileName(f);
            if (name.EndsWith(".sst", StringComparison.Ordinal)) set.Add(name);
        }
        return set;
    }

    private static SortedSet<string> LiveSst(RocksDatabase db)
    {
        var set = new SortedSet<string>(StringComparer.Ordinal);
        foreach (LiveFileMetadata m in db.GetLiveFilesMetadata()) set.Add(Path.GetFileName(m.Name));
        return set;
    }

    /// <summary>
    /// A compaction that fails while writing its output (table file append / sync, or the MANIFEST write that installs
    /// it) leaves the DB consistent: CompactionJob::CleanupCompaction / FindObsoleteFiles remove the outputs of a job
    /// whose edit was not written, the outputs of a failed MANIFEST write are kept until the next open decides
    /// (ErrorHandler::AddFilesToQuarantine), the data reads the same before and after the reopen, and the reopened DB
    /// holds exactly its live table files.
    /// </summary>
    [TestMethod]
    [DataRow(".sst", 2)] // FaultOps.Append
    [DataRow(".sst", 4)] // FaultOps.Sync
    [DataRow("MANIFEST", 2)]
    [DataRow("MANIFEST", 4)]
    public void FailedCompactionLeavesNoOrphanOutputs(string fragment, int faultOps)
    {
        var fs = new FaultInjectionFileSystem(NewFs());
        var cfo = new ColumnFamilyOptions { Level0FileNumCompactionTrigger = 2 };
        using var gate = new ManualResetEventSlim(false);
        TestHooks.Set("DBImpl::RunCompaction:Start", () => gate.Wait(Timeout));
        RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
        try
        {
            for (int i = 0; i < 200; i++) db.Put(Key(i), Value(i, 0));
            db.Flush();
            for (int i = 0; i < 200; i += 2) db.Put(Key(i), Value(i, 1));
            db.Flush();
            fs.FailMatching(fragment, (FaultOps)faultOps, 1, InjectedFaultKind.IOError);
            gate.Set();
            var sw = System.Diagnostics.Stopwatch.StartNew();
            RocksDbException? bg = null;
            while (sw.Elapsed < Timeout)
            {
                try
                {
                    db.Put(B("probe"), B("1"));
                }
                catch (RocksDbException e)
                {
                    bg = e;
                    break;
                }
                Thread.Sleep(10);
            }
            Assert.IsNotNull(bg, "the failed compaction did not stop writes (paranoid_checks: fatal IOError)");
            Assert.AreEqual(StatusCode.IOError, bg.Code);
            Assert.AreEqual(1, fs.TargetedFaults);
            for (int i = 0; i < 200; i++) CollectionAssert.AreEqual(Value(i, i % 2 == 0 ? 1 : 0), db.Get(Key(i)), "before reopen " + i);
            if (fragment == ".sst")
            {
                // Nothing reached the MANIFEST: the partial outputs are gone already.
                CollectionAssert.AreEqual(LiveSst(db).ToList(), SstOnDisk(fs).ToList(), "orphan compaction outputs");
            }
        }
        finally
        {
            gate.Set();
            try { db.Dispose(); }
            catch (RocksDbException) { }
        }
        using RocksDatabase reopened = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
        for (int i = 0; i < 200; i++) CollectionAssert.AreEqual(Value(i, i % 2 == 0 ? 1 : 0), reopened.Get(Key(i)), "after reopen " + i);
        reopened.WaitForCompact();
        CollectionAssert.AreEqual(LiveSst(reopened).ToList(), SstOnDisk(fs).ToList(), "files left after the reopen");
    }
    /// <summary>
    /// A CompactRange that passed its closed-DB check just before the close (here: with nothing to flush, held right
    /// after the flush step) must fail once the close released the column family's super version. It used to spin
    /// forever in GetReferencedSuperVersion (TryRef of a super version that is never replaced), burning a core.
    /// Managed contract (native: calling into a DB that is being deleted is undefined behavior): ObjectDisposed.
    /// </summary>
    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task CompactRangeRacingTheCloseDoesNotSpin(bool synchronous)
    {
        MemoryFileSystem fs = NewFs();
        RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        db.Put(B("a"), B("1"));
        db.Flush();
        using var reached = new ManualResetEventSlim(false);
        using var release = new ManualResetEventSlim(false);
        int hits = 0;
        TestHooks.Set("DBImpl::CompactRange:AfterFlush", () =>
        {
            if (Interlocked.Increment(ref hits) != 1) return;
            reached.Set();
            release.Wait(Timeout);
        });
        Task compact = synchronous ? Task.Run(db.CompactRange) : Task.Run(async () => await db.CompactRangeAsync());
        Assert.IsTrue(reached.Wait(Timeout));
        await db.DisposeAsync();
        release.Set();
        Assert.IsTrue(((IAsyncResult)compact).AsyncWaitHandle.WaitOne(Timeout), "CompactRange spins after the close");
        Exception? e = compact.Exception?.InnerException;
        Assert.IsTrue(e is ObjectDisposedException or RocksDbException { Code: StatusCode.ShutdownInProgress }, e?.ToString() ?? "no error");
    }
}
