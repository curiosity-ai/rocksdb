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
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    [TestCleanup]
    public void Cleanup() => TestHooks.ClearAll();

    private static DbOptions SweepOptions(IFileSystem fs, bool autoResume)
    {
        DbOptions o = NewOptions(fs);
        o.MaxBgErrorResumeCount = autoResume ? 3 : 0;
        o.BgErrorResumeRetryInterval = 1000;
        return o;
    }

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
    private static List<Op> RunSweepWorkload(FaultInjectionFileSystem fs, long failAt, InjectedFaultKind kind, bool autoResume, int seed)
    {
        var ops = new List<Op>();
        var rnd = new Random(seed);
        RocksDatabase db = RocksDatabase.Open(DbPath, SweepOptions(fs, autoResume), SweepCfOptions());
        try
        {
            if (failAt > 0)
            {
                fs.ErrorKind = kind;
                fs.TransientFailure = true;
                fs.CrashAtOperation = fs.Operations + failAt;
            }
            for (int i = 0; i < 400; i++)
            {
                int key = rnd.Next(40);
                bool delete = rnd.Next(10) == 0;
                string? value = delete ? null : "v" + i.ToString(System.Globalization.CultureInfo.InvariantCulture) + new string('x', 100);
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
                if (i % 97 == 96)
                {
                    try { db.Flush(); }
                    catch (RocksDbException) { TryResume(db, autoResume); }
                }
            }
            try { db.CompactRange(); }
            catch (RocksDbException) { TryResume(db, autoResume); }
        }
        finally
        {
            try { db.Dispose(); }
            catch (RocksDbException) { }
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
    private static void VerifyAfterReopen(FaultInjectionFileSystem fs, List<Op> ops, string context)
    {
        using RocksDatabase db = RocksDatabase.Open(DbPath, new DbOptions { FileSystem = fs }, SweepCfOptions());
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
    [DataRow(0, false)] // IOError
    [DataRow(1, false)] // Retryable
    [DataRow(1, true)]
    [DataRow(3, false)] // NoSpace
    [DataRow(2, true)] // FileScope
    public void TransientIoErrorAtAnyOperationLosesNoAcknowledgedWrite(int kindValue, bool autoResume)
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
        RunSweepWorkload(dry, 0, kind, autoResume, seed);
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
            string context = "seed=" + seed + " kind=" + kind + " auto=" + autoResume + " failAt=" + failAt;
            Task<List<Op>> run = Task.Run(() => RunSweepWorkload(fs, failAt, kind, autoResume, seed + iteration));
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
            VerifyAfterReopen(fs, run.Result, context + " at " + fs.CrashedAt);
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
            Assert.IsTrue(Task.WaitAll(tasks.ToArray(), Timeout), "seed=" + seed + " round " + round + ": a call racing the close hung");
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
}
