using System.Buffers;
using System.Diagnostics;
using System.Globalization;
using RocksDb.Managed.Db;

namespace RocksDb.Managed.Benchmarks.DbBench;

/// <summary>
/// A db_bench (tools/db_bench_tool.cc) work-alike for end-to-end comparisons between engines.
/// <code>
/// dotnet run -c Release --project bench/RocksDb.Managed.Benchmarks -f net10.0 -- dbbench \
///     --engine rocksdb-sharp|managed|c-native [--async] --benchmarks fillseq,fillrandom,readrandom --num 1000000
/// </code>
/// The workloads, flags, key/value generation, random streams and output format are identical to
/// bench/native-c/rocksdb_bench.c (the pure C leg); <c>--engine c-native</c> runs that binary with the same flags.
/// <list type="bullet">
/// <item>Keys: db_bench GenerateKeyFromInt (8-byte big-endian number padded with '0' to --key_size).</item>
/// <item>Values: db_bench's compressible random buffer (--compression_ratio, default 0.5).</item>
/// <item>Random keys: SplitMix64 per (benchmark position, thread), uniform in [0, num).</item>
/// <item>Work split: --num (writes) / --reads (reads, default num) ops in total, divided across --threads.</item>
/// <item>Fresh DB for fillseq/fillrandom/fillsync (unless --use_existing_db); others reuse the DB.</item>
/// <item>Latency histogram per operation (per batch for multireadrandom / batched writes); percentiles in µs.</item>
/// </list>
/// <c>--async</c> (managed only) runs every worker as a task over PutAsync/TryGetAsync/DeleteAsync/WriteAsync and the
/// async iterator, multireadrandom over MultiGetAsync, and flush/compact/waitforcompaction over
/// FlushAsync/CompactRangeAsync/WaitForCompactAsync.
/// <c>stats</c> prints the LSM shape (files per level), the flush+compaction bytes written ("Cumulative compaction" line
/// of rocksdb.stats) and the write amplification relative to the user bytes (key + value) written since the DB was
/// created by this process, followed by a machine-readable <c>STATS,...</c> line (see <see cref="PrintStats"/>).
/// </summary>
public static class DbBenchRunner
{
    public sealed class Flags
    {
        public EngineKind Engine = EngineKind.RocksDbSharp;
        public bool Async;
        public DbBenchmark[] Benchmarks = [DbBenchmark.FillSeq, DbBenchmark.FillRandom, DbBenchmark.ReadRandom];
        public long Num = 1_000_000;
        public long Reads = -1;
        public int KeySize = 16;
        public int ValueSize = 100;
        public int Threads = 1;
        public int BatchSize = 1;
        public int MultiGetSize = 16;
        public int SeekNexts = 10;
        public int TxnPuts = 10;
        public double CompressionRatio = 0.5;
        public string Db = Path.Combine(Path.GetTempPath(), "rocksdb-managed-dbbench");
        public bool UseExistingDb;
        public long Seed = 301;
        public bool Histogram = true;
        public string? CBenchPath;
        public EngineOptions Options = new();

        /// <summary>
        /// Run the warm-up phase (see <see cref="RunWarmup"/>) before the measured benchmarks (--warmup, default true).
        /// </summary>
        public bool Warmup = true;

        /// <summary>Warm-up operations per benchmark (--warmup_ops); -1 = <see cref="DefaultWarmupOps"/>.</summary>
        public long WarmupOps = -1;

        /// <summary>Minimum warm-up time per repeated benchmark in milliseconds (--warmup_min_ms, default 1000).</summary>
        public int WarmupMinMs = 1000;

        /// <summary>
        /// Total ops of the next benchmark execution instead of --num / --reads (the key range stays --num); set by
        /// the warm-up for its repetitions, -1 otherwise.
        /// </summary>
        public long OpsOverride = -1;

        public string EngineLabel => Engine == EngineKind.Managed && Async ? "managed-async" : BenchFlags.Name(Engine);

        public Flags Clone() => (Flags)MemberwiseClone();
    }

    /// <summary>Repetitions of each repeated warm-up benchmark, at least (the .NET tier-up call count is 30).</summary>
    public const int WarmupMinReps = 32;

    /// <summary>
    /// Flush / compaction cycles at the end of the warm-up (reopen, <see cref="WarmupFlushesPerCycle"/> x (small
    /// overwrite + flush), waitforcompaction, compact). The methods called once or a few times per flush / table file /
    /// compaction / DB open need ~30 calls for each tiering step (tier 0 -> instrumented -> tier 1), so the cycles run
    /// 64 small flushes (and the automatic compactions they trigger); the pause between cycles lets the tiering delay
    /// expire so these methods get promoted during the warm-up.
    /// </summary>
    private const int WarmupFlushCompactionCycles = 4;

    /// <summary>Flushes per warm-up flush / compaction cycle, each after an overwrite of --warmup_ops / 50 keys.</summary>
    private const int WarmupFlushesPerCycle = 16;

    /// <summary>
    /// With the warm-up on, every measured benchmark starts after the JIT has been quiet for this long (the C leg sleeps
    /// as long).
    /// </summary>
    public const int SettleMs = 200;

    /// <summary>Upper bound of the warm-up time per benchmark, whatever the other limits say.</summary>
    private const int WarmupMaxMsPerBenchmark = 30_000;

    /// <summary>Default --warmup_ops: max(50k, num / 10), at most 200k and at most num.</summary>
    public static long DefaultWarmupOps(long num) => Math.Max(1, Math.Min(num, Math.Min(200_000, Math.Max(50_000, num / 10))));

    /// <summary>
    /// User bytes (key + value) written by the fill/overwrite benchmarks and the readwhilewriting writer since the DB
    /// was (re)created; the denominator of the write amplification printed by <c>stats</c>.
    /// </summary>
    private static long s_userBytesWritten;

    public static int Run(string[] args)
    {
        Flags f;
        try
        {
            f = Parse(args);
        }
        catch (Exception e) when (e is ArgumentException or FormatException)
        {
            Console.Error.WriteLine(e.Message);
            PrintUsage();
            return 1;
        }

        if (f.Engine == EngineKind.CNative) return CNativeBench.Run(f, args);
        if (f.Async && f.Engine != EngineKind.Managed)
        {
            Console.Error.WriteLine("--async is only supported by --engine managed");
            return 1;
        }

        Console.WriteLine("Engine:      " + f.EngineLabel + " (" + System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription + ")");
        Console.WriteLine("Keys:        " + f.KeySize + " bytes each");
        Console.WriteLine("Values:      " + f.ValueSize + " bytes each (" + f.CompressionRatio.ToString("F2", CultureInfo.InvariantCulture) + " compression ratio)");
        Console.WriteLine("Entries:     " + f.Num + " (reads " + f.Reads + ")");
        Console.WriteLine("Threads:     " + f.Threads + (f.Async ? " (async tasks)" : ""));
        Console.WriteLine("Options:     " + f.Options + " batch_size=" + f.BatchSize + " multiget_size=" + f.MultiGetSize + " seek_nexts=" + f.SeekNexts);
        Console.WriteLine("DB path:     " + f.Db);
        Console.WriteLine("Warm-up:     " + (f.Warmup
            ? "on (" + f.WarmupOps + " ops per benchmark, >= " + WarmupMinReps + " repetitions and >= " + f.WarmupMinMs + " ms, then " + WarmupFlushCompactionCycles + " x " + WarmupFlushesPerCycle + " flushes + compactions; separate DB " + f.Db + "-warmup)"
            : "off"));
        Console.WriteLine("------------------------------------------------");

        var values = new ValueBuffer(f.Seed, f.ValueSize, f.CompressionRatio);
        IKeyValueEngine? engine = null;
        try
        {
            if (f.Warmup) RunWarmup(f, values);
            for (int index = 0; index < f.Benchmarks.Length; index++)
            {
                DbBenchmark bench = f.Benchmarks[index];
                bool fresh = !f.UseExistingDb && bench is DbBenchmark.FillSeq or DbBenchmark.FillRandom or DbBenchmark.FillSync;
                if (engine is null || fresh)
                {
                    engine?.Dispose();
                    engine = EngineFactory.Open(f.Engine, f.Db, f.Options, destroyExisting: fresh);
                    if (fresh) s_userBytesWritten = 0;
                }
                if (bench == DbBenchmark.Stats)
                {
                    PrintStats(engine, f);
                    continue;
                }
                // Settle pause: methods that reached their call count in the previous step (DB open, the end of the
                // previous benchmark, the report formatting) are compiled by the tiering background thread; let that
                // finish before the measurement starts. The C leg pauses the same 200 ms (without a JIT to wait for).
                if (f.Warmup) WaitForJitQuiescence(stableMs: SettleMs, maxMs: 5_000);
                if (TxnEngineFactory.KindOf(bench) is { } kind)
                {
                    // Transactions run against their own (fresh) transactional DB next to the main one.
                    using ITxnEngine txnEngine = TxnEngineFactory.Open(f.Engine, kind, f.Db + "-" + BenchFlags.Name(bench), f.Options, f.TxnPuts * (f.KeySize + f.ValueSize));
                    new BenchRun(engine, f, values, bench, index) { Txn = txnEngine }.Execute();
                    continue;
                }
                new BenchRun(engine, f, values, bench, index).Execute();
            }
        }
        catch (Exception e) when (e is NotSupportedException or ArgumentException)
        {
            Console.Error.WriteLine(e.Message);
            return 2;
        }
        finally
        {
            engine?.Dispose();
        }
        return 0;
    }

    /// <summary>
    /// The warm-up phase (--warmup, on by default; the same algorithm as warmup() in bench/native-c/rocksdb_bench.c).
    /// Runs every requested benchmark on a separate DB (<c>&lt;db&gt;-warmup</c>, same options, key range
    /// --warmup_ops) and deletes it afterwards, so the measured run starts on a fresh DB and a fresh block cache but
    /// with JIT-compiled, tiered-up code:
    /// <list type="bullet">
    /// <item>a fillrandom first when the list does not start with a fill (the reads need data);</item>
    /// <item>every repeated benchmark (all but flush/compact/waitforcompaction; stats is skipped) runs in repetitions
    /// of --warmup_ops / 32 ops until it has run at least <see cref="WarmupMinReps"/> repetitions, --warmup_ops ops and
    /// --warmup_min_ms milliseconds: the per-benchmark harness methods and once-per-run engine methods (iterator and
    /// MultiGet context creation, ...) reach the tier-1 call count, and the per-op methods keep being called after the
    /// tiering delay (100 ms without new tier-0 methods) so that they are promoted to tier 1 (with Dynamic PGO);</item>
    /// <item>flush / compact / waitforcompaction run once where they appear in the list;</item>
    /// <item><see cref="WarmupFlushCompactionCycles"/> cycles of: reopen the DB and wait for the JIT to quiesce,
    /// <see cref="WarmupFlushesPerCycle"/> x (overwrite of --warmup_ops / 50 keys + flush), waitforcompaction, compact.
    /// They compile and tier up the DB open, flush, table file, automatic compaction picking and compaction paths even
    /// when the list has no flush or compaction.</item>
    /// </list>
    /// Afterwards a full blocking GC runs and the JIT is given time to quiesce (compiled method count stable for
    /// 200 ms, at most 10 s). JIT statistics before / after are printed; every measured benchmark then prints the
    /// methods compiled during its own measurement ("jit N methods"). With the warm-up on, every measured benchmark
    /// also starts after a settle pause (JIT quiet for <see cref="SettleMs"/>).
    /// </summary>
    private static void RunWarmup(Flags f, ValueBuffer values)
    {
        var ci = CultureInfo.InvariantCulture;
        long jit0 = System.Runtime.JitInfo.GetCompiledMethodCount(currentThread: false);
        TimeSpan jitTime0 = System.Runtime.JitInfo.GetCompilationTime(currentThread: false);
        Console.WriteLine(string.Format(ci, "{0,-17} : start; JIT so far {1} methods, {2:F0} ms", "warmup", jit0, jitTime0.TotalMilliseconds));

        Flags w = f.Clone();
        w.Db = f.Db + "-warmup";
        w.Num = f.WarmupOps;
        w.Reads = f.WarmupOps;
        w.UseExistingDb = false;

        // Bench null = close and reopen the warm-up DB, then let the JIT quiesce.
        var plan = new List<(DbBenchmark? Bench, bool Repeat)>();
        if (f.Benchmarks.Length == 0 || f.Benchmarks[0] is not (DbBenchmark.FillSeq or DbBenchmark.FillRandom or DbBenchmark.FillSync))
            plan.Add((DbBenchmark.FillRandom, true));
        foreach (DbBenchmark b in f.Benchmarks)
        {
            if (b == DbBenchmark.Stats) continue;
            plan.Add((b, b is not (DbBenchmark.Flush or DbBenchmark.Compact or DbBenchmark.WaitForCompaction)));
        }
        for (int cycle = 0; cycle < WarmupFlushCompactionCycles; cycle++)
        {
            plan.Add((null, false));
            for (int i = 0; i < WarmupFlushesPerCycle; i++)
            {
                plan.Add((DbBenchmark.Overwrite, false));
                plan.Add((DbBenchmark.Flush, false));
            }
            plan.Add((DbBenchmark.WaitForCompaction, false));
            plan.Add((DbBenchmark.Compact, false));
        }

        long savedUserBytes = s_userBytesWritten;
        long chunk = Math.Max(w.Num / WarmupMinReps, 1);
        var total = Stopwatch.StartNew();
        IKeyValueEngine? engine = null;
        try
        {
            for (int index = 0; index < plan.Count; index++)
            {
                (DbBenchmark? step, bool repeat) = plan[index];
                if (step is null)
                {
                    engine?.Dispose();
                    engine = EngineFactory.Open(w.Engine, w.Db, w.Options, destroyExisting: false);
                    long waited = WaitForJitQuiescence(stableMs: 100, maxMs: 5_000);
                    Console.WriteLine(string.Format(ci, "{0,-17} : reopen, JIT quiesced after {1} ms", "warmup", waited));
                    continue;
                }
                DbBenchmark bench = step.Value;
                bool fresh = bench is DbBenchmark.FillSeq or DbBenchmark.FillRandom or DbBenchmark.FillSync;
                if (engine is null || fresh)
                {
                    engine?.Dispose();
                    engine = EngineFactory.Open(w.Engine, w.Db, w.Options, destroyExisting: true);
                }
                ITxnEngine? txn = null;
                string? txnPath = null;
                if (TxnEngineFactory.KindOf(bench) is { } kind)
                {
                    txnPath = w.Db + "-" + BenchFlags.Name(bench);
                    txn = TxnEngineFactory.Open(w.Engine, kind, txnPath, w.Options, w.TxnPuts * (w.KeySize + w.ValueSize));
                }
                try
                {
                    var sw = Stopwatch.StartNew();
                    int reps = 0;
                    long ops = 0;
                    if (!repeat)
                    {
                        w.OpsOverride = Math.Max(w.Num / 50, 1); // tail overwrites; ignored by flush / compact
                        new BenchRun(engine, w, values, bench, index) { Txn = txn, Quiet = true }.Execute();
                        reps = 1;
                        ops = w.OpsOverride;
                    }
                    else
                    {
                        w.OpsOverride = chunk;
                        while ((reps < WarmupMinReps || ops < w.Num || sw.ElapsedMilliseconds < w.WarmupMinMs)
                               && sw.ElapsedMilliseconds < WarmupMaxMsPerBenchmark)
                        {
                            // A different random key stream per repetition (same derivation as the C leg).
                            new BenchRun(engine, w, values, bench, index + 1000 * (reps + 1)) { Txn = txn, Quiet = true }.Execute();
                            reps++;
                            ops += chunk;
                        }
                    }
                    Console.WriteLine(string.Format(ci, "{0,-17} : {1} {2} reps {3} ops {4:F3} s", "warmup", BenchFlags.Name(bench), reps, ops, sw.Elapsed.TotalSeconds));
                }
                finally
                {
                    txn?.Dispose();
                    if (txnPath is not null && Directory.Exists(txnPath)) Directory.Delete(txnPath, recursive: true);
                }
            }
        }
        finally
        {
            engine?.Dispose();
            if (Directory.Exists(w.Db)) Directory.Delete(w.Db, recursive: true);
            s_userBytesWritten = savedUserBytes;
        }
        double warmSecs = total.Elapsed.TotalSeconds;
        long jit1 = System.Runtime.JitInfo.GetCompiledMethodCount(currentThread: false);
        TimeSpan jitTime1 = System.Runtime.JitInfo.GetCompilationTime(currentThread: false);

        // Start the measurement with a collected heap and an idle tiering compiler.
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
        GC.WaitForPendingFinalizers();
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
        long quiesceMs = WaitForJitQuiescence(stableMs: 200, maxMs: 10_000);
        long jit2 = System.Runtime.JitInfo.GetCompiledMethodCount(currentThread: false);
        TimeSpan jitTime2 = System.Runtime.JitInfo.GetCompilationTime(currentThread: false);
        Console.WriteLine(string.Format(ci,
            "{0,-17} : done in {1:F1} s; JIT during warm-up {2} methods {3:F0} ms, while quiescing ({4} ms) {5} methods {6:F0} ms; total {7} methods {8:F0} ms",
            "warmup", warmSecs, jit1 - jit0, (jitTime1 - jitTime0).TotalMilliseconds, quiesceMs, jit2 - jit1, (jitTime2 - jitTime1).TotalMilliseconds,
            jit2, jitTime2.TotalMilliseconds));
        Console.Out.Flush();
    }

    /// <summary>
    /// Waits until the number of JIT-compiled methods (all threads, including the tiering background compiler) has
    /// not changed for <paramref name="stableMs"/>; returns the time waited.
    /// </summary>
    private static long WaitForJitQuiescence(int stableMs, int maxMs)
    {
        var sw = Stopwatch.StartNew();
        long last = System.Runtime.JitInfo.GetCompiledMethodCount(currentThread: false);
        long stableSince = 0;
        while (sw.ElapsedMilliseconds < maxMs)
        {
            Thread.Sleep(20);
            long now = System.Runtime.JitInfo.GetCompiledMethodCount(currentThread: false);
            if (now != last)
            {
                last = now;
                stableSince = sw.ElapsedMilliseconds;
            }
            else if (sw.ElapsedMilliseconds - stableSince >= stableMs)
            {
                break;
            }
        }
        return sw.ElapsedMilliseconds;
    }

    /// <summary>Per-worker results.</summary>
    private sealed class Worker(int tid)
    {
        public readonly int Tid = tid;
        public readonly Histogram Hist = new();
        public long Ops, Bytes, Found, Start, End;
    }

    /// <summary>One benchmark execution: spawns the workers, merges and reports.</summary>
    private sealed class BenchRun(IKeyValueEngine engine, Flags f, ValueBuffer values, DbBenchmark bench, int index)
    {
        private readonly string _name = BenchFlags.Name(bench);
        private long _totalOps;
        private int _readersDone;
        private long _writerOps;
        private long _txnRetries;

        private long _jitMethodsBefore;
        private TimeSpan _jitTimeBefore;
        private TimeSpan _cpuBefore;

        /// <summary>The transactional DB of txnoptimistic / txnpessimistic.</summary>
        public ITxnEngine? Txn { get; init; }

        /// <summary>Warm-up execution: nothing is printed.</summary>
        public bool Quiet { get; init; }

        public void Execute()
        {
            // Methods compiled by the JIT (all threads, including the tiering background compiler) during this
            // benchmark; ~0 after the warm-up, so the measurement does not include compilation.
            _jitMethodsBefore = System.Runtime.JitInfo.GetCompiledMethodCount(currentThread: false);
            _jitTimeBefore = System.Runtime.JitInfo.GetCompilationTime(currentThread: false);
            _cpuBefore = Environment.CpuUsage.TotalTime;
            long allocBefore = GC.GetTotalAllocatedBytes(precise: true);
            int g0 = GC.CollectionCount(0), g1 = GC.CollectionCount(1), g2 = GC.CollectionCount(2);

            if (bench is DbBenchmark.Flush or DbBenchmark.Compact or DbBenchmark.WaitForCompaction)
            {
                long t0 = Clock.Now();
                if (f.Async)
                {
                    RocksDatabase db = ((ManagedEngine)engine).Db;
                    ValueTask op = bench switch
                    {
                        DbBenchmark.Flush => db.FlushAsync(),
                        DbBenchmark.Compact => db.CompactRangeAsync(),
                        _ => db.WaitForCompactAsync(),
                    };
                    op.AsTask().GetAwaiter().GetResult();
                }
                else if (bench == DbBenchmark.Flush) engine.Flush();
                else if (bench == DbBenchmark.Compact) engine.CompactRange();
                else engine.WaitForCompaction();
                double secs = Clock.ElapsedNs(t0) / 1e9;
                Report(1, secs, 0, 0, null, allocBefore, g0, g1, g2, "");
                return;
            }

            bool isRead = bench is DbBenchmark.ReadRandom or DbBenchmark.ReadMissing or DbBenchmark.ReadSeq or DbBenchmark.ReadReverse
                or DbBenchmark.SeekRandom or DbBenchmark.MultiReadRandom or DbBenchmark.ReadWhileWriting;
            long nominal = f.OpsOverride >= 0 ? f.OpsOverride : isRead ? f.Reads : f.Num;
            _totalOps = bench switch
            {
                DbBenchmark.FillSync => Math.Max(nominal / 1000, 1),
                // one op = one transaction of --txn_puts puts (as many puts in total as fillrandom)
                DbBenchmark.TxnOptimistic or DbBenchmark.TxnPessimistic or DbBenchmark.TxnWritePrepared or DbBenchmark.TxnWriteUnprepared =>
                    Math.Max(nominal / f.TxnPuts, 1),
                _ => nominal,
            };
            int n = f.Threads + (bench == DbBenchmark.ReadWhileWriting ? 1 : 0);
            var workers = new Worker[n];
            for (int i = 0; i < n; i++) workers[i] = new Worker(i);

            if (f.Async) RunAsyncWorkers(workers);
            else RunThreads(workers);

            var merged = new Histogram();
            long start = long.MaxValue, end = 0, ops = 0, bytes = 0, found = 0;
            for (int i = 0; i < f.Threads; i++) // readers only for readwhilewriting
            {
                Worker w = workers[i];
                merged.Merge(w.Hist);
                start = Math.Min(start, w.Start);
                end = Math.Max(end, w.End);
                ops += w.Ops;
                bytes += w.Bytes;
                found += w.Found;
            }
            string extra = bench == DbBenchmark.ReadWhileWriting ? "(writer: " + Interlocked.Read(ref _writerOps) + " puts)"
                : Txn is not null ? "(" + f.TxnPuts + " puts/txn, " + Interlocked.Read(ref _txnRetries) + " retries)" : "";
            if (bench is DbBenchmark.FillSeq or DbBenchmark.FillRandom or DbBenchmark.Overwrite or DbBenchmark.FillSync) s_userBytesWritten += bytes;
            else if (bench == DbBenchmark.ReadWhileWriting) s_userBytesWritten += Interlocked.Read(ref _writerOps) * (f.KeySize + f.ValueSize);
            Report(ops, Clock.ToNs(end - start) / 1e9, bytes, found, merged, allocBefore, g0, g1, g2, extra);
        }

        private void Report(long ops, double seconds, long bytes, long found, Histogram? h, long allocBefore, int g0, int g1, int g2, string extra)
        {
            if (Quiet) return;
            long allocated = GC.GetTotalAllocatedBytes(precise: true) - allocBefore;
            int d0 = GC.CollectionCount(0) - g0, d1 = GC.CollectionCount(1) - g1, d2 = GC.CollectionCount(2) - g2;
            long jitMethods = System.Runtime.JitInfo.GetCompiledMethodCount(currentThread: false) - _jitMethodsBefore;
            double jitMs = (System.Runtime.JitInfo.GetCompilationTime(currentThread: false) - _jitTimeBefore).TotalMilliseconds;
            double microsPerOp = ops > 0 ? seconds * 1e6 / ops : 0;
            double opsPerSec = seconds > 0 ? ops / seconds : 0;
            double mbs = seconds > 0 ? bytes / 1048576.0 / seconds : 0;
            double p50 = 0, p99 = 0, p999 = 0;
            bool hasHist = h is not null && h.Count > 0;
            if (hasHist)
            {
                p50 = h!.Percentile(50) / 1000.0;
                p99 = h.Percentile(99) / 1000.0;
                p999 = h.Percentile(99.9) / 1000.0;
            }
            var ci = CultureInfo.InvariantCulture;
            string line = string.Format(ci, "{0,-17} : {1,11:F3} micros/op {2,10:F0} ops/sec {3,9:F3} seconds {4,10} operations; {5,8:F1} MB/s",
                _name, microsPerOp, opsPerSec, seconds, ops, mbs);
            if (hasHist) line += string.Format(ci, " | p50 {0:F3} p99 {1:F3} p99.9 {2:F3} us", p50, p99, p999);
            if (bench is DbBenchmark.ReadRandom or DbBenchmark.ReadMissing or DbBenchmark.MultiReadRandom or DbBenchmark.ReadWhileWriting or DbBenchmark.SeekRandom)
                line += " (" + found + " of " + ops + " found)";
            if (extra.Length > 0) line += " " + extra;
            double allocPerOp = allocated / (double)Math.Max(ops, 1);
            // Managed allocations (all threads) per operation; native engines only allocate in unmanaged memory.
            line += string.Format(ci, " alloc {0:F1} B/op gc {1}/{2}/{3} jit {4} methods {5:F1} ms", allocPerOp, d0, d1, d2, jitMethods, jitMs);
            // Process CPU time (all threads, incl. background jobs) per operation: less sensitive to other load on the
            // machine than wall time, and it shows spinning and cache-line contention.
            double cpuPerOp = (Environment.CpuUsage.TotalTime - _cpuBefore).TotalMicroseconds / Math.Max(ops, 1);
            line += string.Format(ci, " cpu {0:F3} us/op", cpuPerOp);
            Console.WriteLine(line);
            Console.WriteLine(string.Format(ci, "RESULT,{0},{1},{2},{3},{4:F6},{5:F4},{6:F1},{7:F2},{8:F4},{9:F4},{10:F4},{11},{12:F1},{13},{14},{15},{16},{17:F1}",
                f.EngineLabel, _name, f.Threads, ops, seconds, microsPerOp, opsPerSec, mbs, p50, p99, p999, found, allocPerOp, d0, d1, d2, jitMethods, jitMs));
            Console.Out.Flush();
        }

        // ------------------------------------------------------------------ synchronous workers (threads)

        private void RunThreads(Worker[] workers)
        {
            using var barrier = new Barrier(workers.Length);
            var threads = new Thread[workers.Length];
            for (int i = 0; i < workers.Length; i++)
            {
                Worker w = workers[i];
                threads[i] = new Thread(() =>
                {
                    barrier.SignalAndWait();
                    w.Start = Clock.Now();
                    RunSync(w);
                    w.End = Clock.Now();
                }) { IsBackground = true, Name = "dbbench-" + i };
                threads[i].Start();
            }
            foreach (Thread t in threads) t.Join();
        }

        private static long ThreadOps(long total, int threads, int tid) => total / threads + (tid < total % threads ? 1 : 0);

        private static long ThreadFirst(long total, int threads, int tid)
        {
            long b = total / threads, rem = total % threads;
            return b * tid + (tid < rem ? tid : rem);
        }

        private void RunSync(Worker w)
        {
            var rng = SplitMix64.ForThread(f.Seed, index, w.Tid);
            long n = ThreadOps(_totalOps, f.Threads, w.Tid);
            switch (bench)
            {
                case DbBenchmark.FillSeq:
                case DbBenchmark.FillRandom:
                case DbBenchmark.Overwrite:
                case DbBenchmark.FillSync:
                    DoWrites(w, n, ref rng);
                    break;
                case DbBenchmark.ReadRandom:
                case DbBenchmark.ReadMissing:
                    DoReads(w, n, ref rng, bench == DbBenchmark.ReadMissing);
                    break;
                case DbBenchmark.ReadWhileWriting:
                    if (w.Tid == f.Threads)
                    {
                        BackgroundWriter(ref rng);
                    }
                    else
                    {
                        DoReads(w, n, ref rng, false);
                        Interlocked.Increment(ref _readersDone);
                    }
                    break;
                case DbBenchmark.ReadSeq:
                case DbBenchmark.ReadReverse:
                    DoScan(w, n, bench == DbBenchmark.ReadReverse);
                    break;
                case DbBenchmark.SeekRandom:
                    DoSeeks(w, n, ref rng);
                    break;
                case DbBenchmark.MultiReadRandom:
                    DoMultiGet(w, n, ref rng);
                    break;
                case DbBenchmark.DeleteRandom:
                    DoDeletes(w, n, ref rng);
                    break;
                case DbBenchmark.TxnOptimistic:
                case DbBenchmark.TxnPessimistic:
                case DbBenchmark.TxnWritePrepared:
                case DbBenchmark.TxnWriteUnprepared:
                    DoTransactions(w, n, ref rng);
                    break;
            }
        }

        /// <summary>txnoptimistic / txnpessimistic: n transactions of --txn_puts random puts; conflicts are retried.</summary>
        private void DoTransactions(Worker w, long n, ref SplitMix64 rng)
        {
            bool hist = f.Histogram;
            int ks = f.KeySize, vs = f.ValueSize, puts = f.TxnPuts;
            ulong num = (ulong)f.Num;
            byte[] keys = new byte[ks * puts];
            var vc = new ValueCursor(values);
            using ITxnSession session = Txn!.CreateSession();
            long retries = 0;
            for (long i = 0; i < n; i++)
            {
                for (int j = 0; j < puts; j++) KeyFormat.Make(keys.AsSpan(j * ks, ks), rng.Uniform(num));
                ReadOnlySpan<byte> value = vc.Next(vs).Span;
                long t0 = Clock.Now();
                while (!session.Run(keys, puts, ks, value)) retries++;
                if (hist) w.Hist.Add(Clock.ElapsedNs(t0));
            }
            Interlocked.Add(ref _txnRetries, retries);
            w.Ops = n;
            w.Bytes = n * puts * (ks + vs);
        }

        private async Task TransactionsAsync(Worker w, long n, SplitMix64 rng)
        {
            bool hist = f.Histogram;
            int ks = f.KeySize, vs = f.ValueSize, puts = f.TxnPuts;
            ulong num = (ulong)f.Num;
            byte[] keys = new byte[ks * puts];
            var vc = new ValueCursor(values);
            using ITxnSession session = Txn!.CreateSession();
            long retries = 0;
            for (long i = 0; i < n; i++)
            {
                for (int j = 0; j < puts; j++) KeyFormat.Make(keys.AsSpan(j * ks, ks), rng.Uniform(num));
                ReadOnlyMemory<byte> value = vc.Next(vs);
                long t0 = Clock.Now();
                while (!await session.RunAsync(keys, puts, ks, value).ConfigureAwait(false)) retries++;
                if (hist) w.Hist.Add(Clock.ElapsedNs(t0));
            }
            Interlocked.Add(ref _txnRetries, retries);
            w.Ops = n;
            w.Bytes = n * puts * (ks + vs);
        }

        private void DoWrites(Worker w, long n, ref SplitMix64 rng)
        {
            bool seq = bench == DbBenchmark.FillSeq, sync = bench == DbBenchmark.FillSync, hist = f.Histogram;
            int ks = f.KeySize, vs = f.ValueSize;
            ulong num = (ulong)f.Num;
            long first = ThreadFirst(_totalOps, f.Threads, w.Tid);
            byte[] key = new byte[ks];
            var vc = new ValueCursor(values);
            IEngineWriteBatch? batch = f.BatchSize > 1 && !sync ? engine.CreateWriteBatch() : null;
            long i = 0;
            while (i < n)
            {
                long t0 = Clock.Now();
                if (batch is null)
                {
                    KeyFormat.Make(key, seq ? (ulong)(first + i) : rng.Uniform(num));
                    if (sync) engine.PutSync(key, vc.Next(vs).Span);
                    else engine.Put(key, vc.Next(vs).Span);
                    i++;
                }
                else
                {
                    batch.Clear();
                    for (int j = 0; j < f.BatchSize && i < n; j++, i++)
                    {
                        KeyFormat.Make(key, seq ? (ulong)(first + i) : rng.Uniform(num));
                        batch.Put(key, vc.Next(vs).Span);
                    }
                    engine.Write(batch);
                }
                if (hist) w.Hist.Add(Clock.ElapsedNs(t0));
            }
            batch?.Dispose();
            w.Ops = n;
            w.Bytes = n * (ks + vs);
        }

        private void DoReads(Worker w, long n, ref SplitMix64 rng, bool missing)
        {
            int ks = f.KeySize;
            bool hist = f.Histogram;
            ulong num = (ulong)f.Num;
            byte[] key = new byte[ks];
            byte[] buffer = new byte[Math.Max(f.ValueSize, 1) * 2];
            for (long i = 0; i < n; i++)
            {
                KeyFormat.Make(key, rng.Uniform(num));
                if (missing) key[ks - 1] = (byte)'.';
                long t0 = Clock.Now();
                if (engine.TryGet(key, buffer, out int len))
                {
                    w.Found++;
                    w.Bytes += ks + len;
                }
                if (hist) w.Hist.Add(Clock.ElapsedNs(t0));
            }
            w.Ops = n;
        }

        private void DoDeletes(Worker w, long n, ref SplitMix64 rng)
        {
            bool hist = f.Histogram;
            ulong num = (ulong)f.Num;
            byte[] key = new byte[f.KeySize];
            for (long i = 0; i < n; i++)
            {
                KeyFormat.Make(key, rng.Uniform(num));
                long t0 = Clock.Now();
                engine.Delete(key);
                if (hist) w.Hist.Add(Clock.ElapsedNs(t0));
            }
            w.Ops = n;
        }

        private void DoScan(Worker w, long n, bool reverse)
        {
            bool hist = f.Histogram;
            using IEngineIterator it = engine.NewIterator();
            if (reverse) it.SeekToLast();
            else it.SeekToFirst();
            long i = 0;
            for (; i < n; i++)
            {
                long t0 = Clock.Now();
                if (!it.Valid)
                {
                    if (reverse) it.SeekToLast();
                    else it.SeekToFirst();
                    if (!it.Valid) break; // empty DB
                }
                w.Bytes += it.Key.Length + it.Value.Length;
                w.Found++;
                if (reverse) it.Prev();
                else it.Next();
                if (hist) w.Hist.Add(Clock.ElapsedNs(t0));
            }
            w.Ops = n;
        }

        private void DoSeeks(Worker w, long n, ref SplitMix64 rng)
        {
            bool hist = f.Histogram;
            ulong num = (ulong)f.Num;
            byte[] key = new byte[f.KeySize];
            using IEngineIterator it = engine.NewIterator();
            for (long i = 0; i < n; i++)
            {
                KeyFormat.Make(key, rng.Uniform(num));
                long t0 = Clock.Now();
                it.Seek(key);
                if (it.Valid) w.Found++;
                for (int j = 0; j < f.SeekNexts && it.Valid; j++)
                {
                    w.Bytes += it.Key.Length + it.Value.Length;
                    it.Next();
                }
                if (hist) w.Hist.Add(Clock.ElapsedNs(t0));
            }
            w.Ops = n;
        }

        private void DoMultiGet(Worker w, long n, ref SplitMix64 rng)
        {
            bool hist = f.Histogram;
            ulong num = (ulong)f.Num;
            int m = f.MultiGetSize, ks = f.KeySize;
            using IMultiGetContext ctx = engine.CreateMultiGetContext(m, ks);
            long i = 0;
            while (i < n)
            {
                int cnt = (int)Math.Min(n - i, m);
                for (int j = 0; j < cnt; j++) KeyFormat.Make(ctx.Key(j), rng.Uniform(num));
                long t0 = Clock.Now();
                int found = ctx.Run(cnt, out long valueBytes);
                if (hist) w.Hist.Add(Clock.ElapsedNs(t0));
                w.Found += found;
                w.Bytes += (long)found * ks + valueBytes;
                i += cnt;
            }
            w.Ops = n;
        }

        private void BackgroundWriter(ref SplitMix64 rng)
        {
            ulong num = (ulong)f.Num;
            int vs = f.ValueSize;
            byte[] key = new byte[f.KeySize];
            var vc = new ValueCursor(values);
            long n = 0;
            while (Volatile.Read(ref _readersDone) < f.Threads)
            {
                KeyFormat.Make(key, rng.Uniform(num));
                engine.Put(key, vc.Next(vs).Span);
                n++;
            }
            Interlocked.Exchange(ref _writerOps, n);
        }

        // ------------------------------------------------------------------ asynchronous workers (managed tasks)

        private void RunAsyncWorkers(Worker[] workers)
        {
            var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var tasks = new Task[workers.Length];
            for (int i = 0; i < workers.Length; i++)
            {
                Worker w = workers[i];
                tasks[i] = Task.Run(async () =>
                {
                    await gate.Task.ConfigureAwait(false);
                    w.Start = Clock.Now();
                    await RunAsync(w).ConfigureAwait(false);
                    w.End = Clock.Now();
                });
            }
            gate.SetResult();
            Task.WaitAll(tasks);
        }

        private async Task RunAsync(Worker w)
        {
            var me = (ManagedEngine)engine;
            var rng = SplitMix64.ForThread(f.Seed, index, w.Tid);
            long n = ThreadOps(_totalOps, f.Threads, w.Tid);
            switch (bench)
            {
                case DbBenchmark.FillSeq:
                case DbBenchmark.FillRandom:
                case DbBenchmark.Overwrite:
                case DbBenchmark.FillSync:
                    await WritesAsync(me, w, n, rng).ConfigureAwait(false);
                    break;
                case DbBenchmark.ReadRandom:
                case DbBenchmark.ReadMissing:
                    await ReadsAsync(me, w, n, rng, bench == DbBenchmark.ReadMissing).ConfigureAwait(false);
                    break;
                case DbBenchmark.ReadWhileWriting:
                    if (w.Tid == f.Threads)
                    {
                        await BackgroundWriterAsync(me, rng).ConfigureAwait(false);
                    }
                    else
                    {
                        await ReadsAsync(me, w, n, rng, false).ConfigureAwait(false);
                        Interlocked.Increment(ref _readersDone);
                    }
                    break;
                case DbBenchmark.ReadSeq:
                case DbBenchmark.ReadReverse:
                    await ScanAsync(me, w, n, bench == DbBenchmark.ReadReverse).ConfigureAwait(false);
                    break;
                case DbBenchmark.SeekRandom:
                    await SeeksAsync(me, w, n, rng).ConfigureAwait(false);
                    break;
                case DbBenchmark.MultiReadRandom:
                    await MultiGetAsync(w, n, rng).ConfigureAwait(false);
                    break;
                case DbBenchmark.DeleteRandom:
                    await DeletesAsync(me, w, n, rng).ConfigureAwait(false);
                    break;
                case DbBenchmark.TxnOptimistic:
                case DbBenchmark.TxnPessimistic:
                case DbBenchmark.TxnWritePrepared:
                case DbBenchmark.TxnWriteUnprepared:
                    await TransactionsAsync(w, n, rng).ConfigureAwait(false);
                    break;
            }
        }

        private async Task WritesAsync(ManagedEngine me, Worker w, long n, SplitMix64 rng)
        {
            bool seq = bench == DbBenchmark.FillSeq, sync = bench == DbBenchmark.FillSync, hist = f.Histogram;
            int ks = f.KeySize, vs = f.ValueSize;
            ulong num = (ulong)f.Num;
            long first = ThreadFirst(_totalOps, f.Threads, w.Tid);
            byte[] key = new byte[ks];
            var vc = new ValueCursor(values);
            WriteOptions wo = sync ? me.SyncWriteOptions : me.WriteOptions;
            WriteBatch? batch = f.BatchSize > 1 && !sync ? me.Db.CreateWriteBatch() : null;
            long i = 0;
            while (i < n)
            {
                long t0 = Clock.Now();
                if (batch is null)
                {
                    KeyFormat.Make(key, seq ? (ulong)(first + i) : rng.Uniform(num));
                    await me.Db.PutAsync(wo, key, vc.Next(vs)).ConfigureAwait(false);
                    i++;
                }
                else
                {
                    batch.Clear();
                    for (int j = 0; j < f.BatchSize && i < n; j++, i++)
                    {
                        KeyFormat.Make(key, seq ? (ulong)(first + i) : rng.Uniform(num));
                        batch.Put(key, vc.Next(vs).Span);
                    }
                    await me.Db.WriteAsync(wo, batch).ConfigureAwait(false);
                }
                if (hist) w.Hist.Add(Clock.ElapsedNs(t0));
            }
            batch?.Dispose();
            w.Ops = n;
            w.Bytes = n * (ks + vs);
        }

        private async Task ReadsAsync(ManagedEngine me, Worker w, long n, SplitMix64 rng, bool missing)
        {
            int ks = f.KeySize;
            bool hist = f.Histogram;
            ulong num = (ulong)f.Num;
            byte[] key = new byte[ks];
            var buffer = new ArrayBufferWriter<byte>(Math.Max(f.ValueSize, 1) * 2);
            for (long i = 0; i < n; i++)
            {
                KeyFormat.Make(key, rng.Uniform(num));
                if (missing) key[ks - 1] = (byte)'.';
                buffer.ResetWrittenCount();
                long t0 = Clock.Now();
                if (await me.Db.TryGetAsync(key, buffer).ConfigureAwait(false))
                {
                    w.Found++;
                    w.Bytes += ks + buffer.WrittenCount;
                }
                if (hist) w.Hist.Add(Clock.ElapsedNs(t0));
            }
            w.Ops = n;
        }

        private async Task MultiGetAsync(Worker w, long n, SplitMix64 rng)
        {
            bool hist = f.Histogram;
            ulong num = (ulong)f.Num;
            int m = f.MultiGetSize, ks = f.KeySize;
            using IMultiGetContext ctx = engine.CreateMultiGetContext(m, ks);
            long i = 0;
            while (i < n)
            {
                int cnt = (int)Math.Min(n - i, m);
                for (int j = 0; j < cnt; j++) KeyFormat.Make(ctx.Key(j), rng.Uniform(num));
                long t0 = Clock.Now();
                (int found, long valueBytes) = await ManagedEngine.MultiGetAsync(ctx, cnt).ConfigureAwait(false);
                if (hist) w.Hist.Add(Clock.ElapsedNs(t0));
                w.Found += found;
                w.Bytes += (long)found * ks + valueBytes;
                i += cnt;
            }
            w.Ops = n;
        }

        private async Task DeletesAsync(ManagedEngine me, Worker w, long n, SplitMix64 rng)
        {
            bool hist = f.Histogram;
            ulong num = (ulong)f.Num;
            byte[] key = new byte[f.KeySize];
            for (long i = 0; i < n; i++)
            {
                KeyFormat.Make(key, rng.Uniform(num));
                long t0 = Clock.Now();
                await me.Db.DeleteAsync(me.WriteOptions, key).ConfigureAwait(false);
                if (hist) w.Hist.Add(Clock.ElapsedNs(t0));
            }
            w.Ops = n;
        }

        private async Task ScanAsync(ManagedEngine me, Worker w, long n, bool reverse)
        {
            bool hist = f.Histogram;
            using Iterator it = await me.Db.NewIteratorAsync(null).ConfigureAwait(false);
            if (reverse) await it.SeekToLastAsync().ConfigureAwait(false);
            else await it.SeekToFirstAsync().ConfigureAwait(false);
            for (long i = 0; i < n; i++)
            {
                long t0 = Clock.Now();
                if (!it.Valid)
                {
                    if (reverse) await it.SeekToLastAsync().ConfigureAwait(false);
                    else await it.SeekToFirstAsync().ConfigureAwait(false);
                    if (!it.Valid) break;
                }
                w.Bytes += it.Key.Length + it.Value.Length;
                w.Found++;
                if (reverse) await it.PrevAsync().ConfigureAwait(false);
                else await it.NextAsync().ConfigureAwait(false);
                if (hist) w.Hist.Add(Clock.ElapsedNs(t0));
            }
            w.Ops = n;
        }

        private async Task SeeksAsync(ManagedEngine me, Worker w, long n, SplitMix64 rng)
        {
            bool hist = f.Histogram;
            ulong num = (ulong)f.Num;
            byte[] key = new byte[f.KeySize];
            using Iterator it = await me.Db.NewIteratorAsync(null).ConfigureAwait(false);
            for (long i = 0; i < n; i++)
            {
                KeyFormat.Make(key, rng.Uniform(num));
                long t0 = Clock.Now();
                await it.SeekAsync(key).ConfigureAwait(false);
                if (it.Valid) w.Found++;
                for (int j = 0; j < f.SeekNexts && it.Valid; j++)
                {
                    w.Bytes += it.Key.Length + it.Value.Length;
                    await it.NextAsync().ConfigureAwait(false);
                }
                if (hist) w.Hist.Add(Clock.ElapsedNs(t0));
            }
            w.Ops = n;
        }

        private async Task BackgroundWriterAsync(ManagedEngine me, SplitMix64 rng)
        {
            ulong num = (ulong)f.Num;
            int vs = f.ValueSize;
            byte[] key = new byte[f.KeySize];
            var vc = new ValueCursor(values);
            long n = 0;
            while (Volatile.Read(ref _readersDone) < f.Threads)
            {
                KeyFormat.Make(key, rng.Uniform(num));
                await me.Db.PutAsync(me.WriteOptions, key, vc.Next(vs)).ConfigureAwait(false);
                n++;
            }
            Interlocked.Exchange(ref _writerOps, n);
        }
    }

    // ---------------------------------------------------------------------- flags

    public static Flags Parse(string[] args)
    {
        var f = new Flags();
        EngineOptions o = f.Options;
        for (int i = 0; i < args.Length; i++)
        {
            string a = args[i];
            if (i == 0 && a == "dbbench") continue;
            if (!a.StartsWith("--", StringComparison.Ordinal)) throw new ArgumentException("unexpected argument " + a);
            string name, value;
            int eq = a.IndexOf('=');
            if (eq > 0)
            {
                name = a[2..eq];
                value = a[(eq + 1)..];
            }
            else
            {
                name = a[2..];
                value = i + 1 < args.Length && !args[i + 1].StartsWith("--", StringComparison.Ordinal) ? args[++i] : "true";
            }
            var ci = CultureInfo.InvariantCulture;
            switch (name)
            {
                case "engine": f.Engine = BenchFlags.ParseEngine(value); break;
                case "async": f.Async = ParseBool(value); break;
                case "benchmarks":
                    f.Benchmarks = Array.ConvertAll(value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries), BenchFlags.ParseBenchmark);
                    break;
                case "num": f.Num = long.Parse(value, ci); break;
                case "reads": f.Reads = long.Parse(value, ci); break;
                case "key_size": f.KeySize = int.Parse(value, ci); break;
                case "value_size": f.ValueSize = int.Parse(value, ci); break;
                case "threads": f.Threads = Math.Max(1, int.Parse(value, ci)); break;
                case "batch_size": f.BatchSize = Math.Max(1, int.Parse(value, ci)); break;
                case "multiget_size": f.MultiGetSize = Math.Max(1, int.Parse(value, ci)); break;
                case "seek_nexts": f.SeekNexts = int.Parse(value, ci); break;
                case "txn_puts": f.TxnPuts = Math.Max(1, int.Parse(value, ci)); break;
                case "compression_ratio": f.CompressionRatio = double.Parse(value, ci); break;
                case "db": f.Db = value; break;
                case "use_existing_db": f.UseExistingDb = ParseBool(value); break;
                case "seed": f.Seed = long.Parse(value, ci); break;
                case "histogram": f.Histogram = ParseBool(value); break;
                case "c_bench": f.CBenchPath = value; break;
                case "warmup": f.Warmup = ParseBool(value); break;
                case "warmup_ops": f.WarmupOps = long.Parse(value, ci); break;
                case "warmup_min_ms": f.WarmupMinMs = Math.Max(0, int.Parse(value, ci)); break;
                case "disable_wal": o = o with { DisableWal = ParseBool(value) }; break;
                case "sync": o = o with { Sync = ParseBool(value) }; break;
                case "cache_size": o = o with { BlockCacheSize = long.Parse(value, ci) }; break;
                case "cache_type": o = o with { CacheType = BenchFlags.ParseCacheType(value) }; break;
                case "bloom_bits": o = o with { BloomBitsPerKey = int.Parse(value, ci) }; break;
                case "write_buffer_size": o = o with { WriteBufferSize = long.Parse(value, ci) }; break;
                case "block_size": o = o with { BlockSize = int.Parse(value, ci) }; break;
                case "compression":
                case "compression_type": o = o with { Compression = BenchFlags.ParseCompression(value) }; break;
                case "target_file_size_base": o = o with { TargetFileSizeBase = long.Parse(value, ci) }; break;
                case "max_bytes_for_level_base": o = o with { MaxBytesForLevelBase = long.Parse(value, ci) }; break;
                case "level0_file_num_compaction_trigger": o = o with { Level0FileNumCompactionTrigger = int.Parse(value, ci) }; break;
                case "disable_auto_compactions": o = o with { DisableAutoCompactions = ParseBool(value) }; break;
                case "enable_blob_files": o = o with { EnableBlobFiles = ParseBool(value) }; break;
                case "min_blob_size": o = o with { MinBlobSize = ulong.Parse(value, ci) }; break;
                case "allow_concurrent_memtable_write": o = o with { AllowConcurrentMemtableWrite = ParseBool(value) }; break;
                case "enable_write_thread_adaptive_yield": o = o with { EnableWriteThreadAdaptiveYield = ParseBool(value) }; break;
                case "max_background_jobs": o = o with { MaxBackgroundJobs = int.Parse(value, ci) }; break;
                default: throw new ArgumentException("unknown flag --" + name);
            }
        }
        f.Options = o;
        if (f.KeySize < 8) throw new ArgumentException("--key_size must be >= 8");
        if (f.Reads < 0) f.Reads = f.Num;
        if (f.WarmupOps <= 0) f.WarmupOps = DefaultWarmupOps(f.Num);
        return f;
    }

    private static bool ParseBool(string v) => v is "1" or "true" or "True";

    /// <summary>
    /// The <c>stats</c> benchmark (identical output in bench/native-c/rocksdb_bench.c):
    /// <code>
    /// stats             : files per level [L0 L1 L2 L3 L4 L5 L6]
    /// Cumulative compaction: ... (verbatim from rocksdb.stats; flush + compaction output, GB with 2 decimals)
    /// stats             : user 55.3 MB, flush+compaction written 210.0 MB, write-amp 3.80, pending compaction 0 bytes
    /// STATS,engine,L0 L1 ... L6,user_bytes,written_bytes,write_amp,pending_compaction_bytes
    /// </code>
    /// The written bytes come from the "Cumulative compaction: X GB write" line, so they have a resolution of 0.01 GB.
    /// </summary>
    private static void PrintStats(IKeyValueEngine engine, Flags f)
    {
        var ci = CultureInfo.InvariantCulture;
        var levels = new System.Text.StringBuilder();
        for (int l = 0; l < 7; l++)
        {
            if (l > 0) levels.Append(' ');
            levels.Append(engine.GetProperty(DbProperties.NumFilesAtLevel(l)) ?? "?");
        }
        Console.WriteLine(string.Format(ci, "{0,-17} : files per level [{1}]", "stats", levels));
        string stats = engine.GetProperty(DbProperties.Stats) ?? "";
        const string marker = "Cumulative compaction: ";
        double written = -1;
        int i = stats.IndexOf(marker, StringComparison.Ordinal);
        if (i >= 0)
        {
            int nl = stats.IndexOf('\n', i);
            string line = nl < 0 ? stats[i..] : stats[i..nl];
            int end = line.IndexOf(" GB write", StringComparison.Ordinal);
            if (end > marker.Length && double.TryParse(line[marker.Length..end], NumberStyles.Float, ci, out double gb)) written = gb * 1073741824.0;
            Console.WriteLine(line);
        }
        string pending = engine.GetProperty(DbProperties.EstimatePendingCompactionBytes) ?? "?";
        long user = s_userBytesWritten;
        double amp = written >= 0 && user > 0 ? written / user : 0;
        Console.WriteLine(string.Format(ci, "{0,-17} : user {1:F1} MB, flush+compaction written {2:F1} MB, write-amp {3:F2}, pending compaction {4} bytes",
            "stats", user / 1048576.0, Math.Max(written, 0) / 1048576.0, amp, pending));
        Console.WriteLine(string.Format(ci, "STATS,{0},{1},{2},{3:F0},{4:F2},{5}", f.EngineLabel, levels, user, Math.Max(written, 0), amp, pending));
        Console.Out.Flush();
    }

    private static void PrintUsage()
    {
        Console.Error.WriteLine("usage: dbbench [--engine rocksdb-sharp|managed|c-native] [--async] [--benchmarks a,b,...] [--num N] [--reads N]");
        Console.Error.WriteLine("               [--key_size 16] [--value_size 100] [--threads 1] [--batch_size 1] [--multiget_size 16] [--seek_nexts 10] [--txn_puts 10]");
        Console.Error.WriteLine("               [--disable_wal] [--sync] [--cache_size BYTES] [--cache_type lru|hyperclock] [--bloom_bits 10]");
        Console.Error.WriteLine("               [--write_buffer_size BYTES] [--block_size 4096] [--compression none|snappy|lz4|zstd] [--compression_ratio 0.5]");
        Console.Error.WriteLine("               [--target_file_size_base BYTES] [--max_bytes_for_level_base BYTES] [--level0_file_num_compaction_trigger 4]");
        Console.Error.WriteLine("               [--disable_auto_compactions] [--max_background_jobs 2] [--enable_blob_files] [--min_blob_size 0]");
        Console.Error.WriteLine("               [--use_existing_db] [--seed 301] [--db PATH] [--histogram 1] [--c_bench PATH-TO-rocksdb_bench]");
        Console.Error.WriteLine("               [--warmup true|false] [--warmup_ops N (default max(50k, num/10), <= 200k, <= num)] [--warmup_min_ms 1000]");
        Console.Error.WriteLine("benchmarks: " + string.Join(' ', BenchFlags.AllBenchmarkNames));
    }
}

/// <summary>Runs bench/native-c/rocksdb_bench (the pure C leg) with the same flags.</summary>
public static class CNativeBench
{
    /// <summary>Locates rocksdb_bench: --c_bench, $ROCKSDB_C_BENCH, or bench/native-c above the app directory.</summary>
    public static string? Locate(string? explicitPath)
    {
        if (!string.IsNullOrEmpty(explicitPath)) return explicitPath;
        string? env = Environment.GetEnvironmentVariable("ROCKSDB_C_BENCH");
        if (!string.IsNullOrEmpty(env)) return env;
        for (DirectoryInfo? d = new(AppContext.BaseDirectory); d is not null; d = d.Parent)
        {
            string candidate = Path.Combine(d.FullName, "bench", "native-c", "rocksdb_bench");
            if (File.Exists(candidate)) return candidate;
            candidate = Path.Combine(d.FullName, "native-c", "rocksdb_bench");
            if (File.Exists(candidate)) return candidate;
        }
        return null;
    }

    public static int Run(DbBenchRunner.Flags f, string[] args)
    {
        string? exe = Locate(f.CBenchPath);
        if (exe is null)
        {
            Console.Error.WriteLine("rocksdb_bench not found; build it with `make -C bench/native-c` or pass --c_bench PATH");
            return 2;
        }
        var psi = new ProcessStartInfo(exe) { UseShellExecute = false };
        for (int i = 0; i < args.Length; i++)
        {
            string a = args[i];
            if (i == 0 && a == "dbbench") continue;
            // Drop runner-only flags (and their separate values).
            string bare = a.Split('=')[0];
            if (bare is "--engine" or "--c_bench")
            {
                if (!a.Contains('=') && i + 1 < args.Length && !args[i + 1].StartsWith("--", StringComparison.Ordinal)) i++;
                continue;
            }
            psi.ArgumentList.Add(a);
        }
        using Process p = Process.Start(psi)!;
        p.WaitForExit();
        return p.ExitCode;
    }
}
