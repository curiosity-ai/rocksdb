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
/// async iterator, and flush/compact/waitforcompaction over FlushAsync/CompactRangeAsync/WaitForCompactAsync; there is
/// no MultiGetAsync, so multireadrandom uses the synchronous MultiGet.
/// <c>stats</c> prints the LSM shape (files per level), the flush+compaction bytes written ("Cumulative compaction" line
/// of rocksdb.stats) and the write amplification relative to the user bytes (key + value) written since the DB was
/// created by this process, followed by a machine-readable <c>STATS,...</c> line (see <see cref="PrintStats"/>).
/// </summary>
public static class DbBenchRunner
{
    public sealed class Flags
    {
        public string Engine = "rocksdb-sharp";
        public bool Async;
        public string[] Benchmarks = ["fillseq", "fillrandom", "readrandom"];
        public long Num = 1_000_000;
        public long Reads = -1;
        public int KeySize = 16;
        public int ValueSize = 100;
        public int Threads = 1;
        public int BatchSize = 1;
        public int MultiGetSize = 16;
        public int SeekNexts = 10;
        public double CompressionRatio = 0.5;
        public string Db = Path.Combine(Path.GetTempPath(), "rocksdb-managed-dbbench");
        public bool UseExistingDb;
        public long Seed = 301;
        public bool Histogram = true;
        public string? CBenchPath;
        public EngineOptions Options = new();

        public string EngineLabel => Engine switch
        {
            "native" => "rocksdb-sharp",
            "managed" when Async => "managed-async",
            _ => Engine,
        };
    }

    public static readonly string[] AllBenchmarks =
    [
        "fillseq", "fillrandom", "overwrite", "fillsync", "readrandom", "readmissing", "readseq", "readreverse",
        "seekrandom", "multireadrandom", "readwhilewriting", "deleterandom", "flush", "compact", "waitforcompaction", "stats",
    ];

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

        if (f.Engine == "c-native") return CNativeBench.Run(f, args);
        if (f.Async && f.Engine != "managed")
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
        Console.WriteLine("------------------------------------------------");

        var values = new ValueBuffer(f.Seed, f.ValueSize, f.CompressionRatio);
        IKeyValueEngine? engine = null;
        try
        {
            for (int index = 0; index < f.Benchmarks.Length; index++)
            {
                string name = f.Benchmarks[index];
                if (Array.IndexOf(AllBenchmarks, name) < 0) throw new ArgumentException("unknown benchmark " + name);
                bool fresh = !f.UseExistingDb && name is "fillseq" or "fillrandom" or "fillsync";
                if (engine is null || fresh)
                {
                    engine?.Dispose();
                    engine = EngineFactory.Open(f.Engine, f.Db, f.Options, destroyExisting: fresh);
                    if (fresh) s_userBytesWritten = 0;
                }
                if (name == "stats")
                {
                    PrintStats(engine, f);
                    continue;
                }
                new BenchRun(engine, f, values, name, index).Execute();
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

    /// <summary>Per-worker results.</summary>
    private sealed class Worker(int tid)
    {
        public readonly int Tid = tid;
        public readonly Histogram Hist = new();
        public long Ops, Bytes, Found, Start, End;
    }

    /// <summary>One benchmark execution: spawns the workers, merges and reports.</summary>
    private sealed class BenchRun(IKeyValueEngine engine, Flags f, ValueBuffer values, string name, int index)
    {
        private long _totalOps;
        private int _readersDone;
        private long _writerOps;

        public void Execute()
        {
            long allocBefore = GC.GetTotalAllocatedBytes(precise: true);
            int g0 = GC.CollectionCount(0), g1 = GC.CollectionCount(1), g2 = GC.CollectionCount(2);

            if (name is "flush" or "compact" or "waitforcompaction")
            {
                long t0 = Clock.Now();
                if (f.Async)
                {
                    RocksDatabase db = ((ManagedEngine)engine).Db;
                    ValueTask op = name switch
                    {
                        "flush" => db.FlushAsync(),
                        "compact" => db.CompactRangeAsync(),
                        _ => db.WaitForCompactAsync(),
                    };
                    op.AsTask().GetAwaiter().GetResult();
                }
                else if (name == "flush") engine.Flush();
                else if (name == "compact") engine.CompactRange();
                else engine.WaitForCompaction();
                double secs = Clock.ElapsedNs(t0) / 1e9;
                Report(1, secs, 0, 0, null, allocBefore, g0, g1, g2, "");
                return;
            }

            _totalOps = name switch
            {
                "fillsync" => Math.Max(f.Num / 1000, 1),
                "readrandom" or "readmissing" or "readseq" or "readreverse" or "seekrandom" or "multireadrandom" or "readwhilewriting" => f.Reads,
                _ => f.Num,
            };
            int n = f.Threads + (name == "readwhilewriting" ? 1 : 0);
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
            string extra = name == "readwhilewriting" ? "(writer: " + Interlocked.Read(ref _writerOps) + " puts)" : "";
            if (name is "fillseq" or "fillrandom" or "overwrite" or "fillsync") s_userBytesWritten += bytes;
            else if (name == "readwhilewriting") s_userBytesWritten += Interlocked.Read(ref _writerOps) * (f.KeySize + f.ValueSize);
            Report(ops, Clock.ToNs(end - start) / 1e9, bytes, found, merged, allocBefore, g0, g1, g2, extra);
        }

        private void Report(long ops, double seconds, long bytes, long found, Histogram? h, long allocBefore, int g0, int g1, int g2, string extra)
        {
            long allocated = GC.GetTotalAllocatedBytes(precise: true) - allocBefore;
            int d0 = GC.CollectionCount(0) - g0, d1 = GC.CollectionCount(1) - g1, d2 = GC.CollectionCount(2) - g2;
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
                name, microsPerOp, opsPerSec, seconds, ops, mbs);
            if (hasHist) line += string.Format(ci, " | p50 {0:F3} p99 {1:F3} p99.9 {2:F3} us", p50, p99, p999);
            if (name is "readrandom" or "readmissing" or "multireadrandom" or "readwhilewriting" or "seekrandom")
                line += " (" + found + " of " + ops + " found)";
            if (extra.Length > 0) line += " " + extra;
            double allocPerOp = allocated / (double)Math.Max(ops, 1);
            // Managed allocations (all threads) per operation; native engines only allocate in unmanaged memory.
            line += string.Format(ci, " alloc {0:F1} B/op gc {1}/{2}/{3}", allocPerOp, d0, d1, d2);
            Console.WriteLine(line);
            Console.WriteLine(string.Format(ci, "RESULT,{0},{1},{2},{3},{4:F6},{5:F4},{6:F1},{7:F2},{8:F4},{9:F4},{10:F4},{11},{12:F1},{13},{14},{15}",
                f.EngineLabel, name, f.Threads, ops, seconds, microsPerOp, opsPerSec, mbs, p50, p99, p999, found, allocPerOp, d0, d1, d2));
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
            switch (name)
            {
                case "fillseq":
                case "fillrandom":
                case "overwrite":
                case "fillsync":
                    DoWrites(w, n, ref rng);
                    break;
                case "readrandom":
                case "readmissing":
                    DoReads(w, n, ref rng, name == "readmissing");
                    break;
                case "readwhilewriting":
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
                case "readseq":
                case "readreverse":
                    DoScan(w, n, name == "readreverse");
                    break;
                case "seekrandom":
                    DoSeeks(w, n, ref rng);
                    break;
                case "multireadrandom":
                    DoMultiGet(w, n, ref rng);
                    break;
                case "deleterandom":
                    DoDeletes(w, n, ref rng);
                    break;
            }
        }

        private void DoWrites(Worker w, long n, ref SplitMix64 rng)
        {
            bool seq = name == "fillseq", sync = name == "fillsync", hist = f.Histogram;
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
            switch (name)
            {
                case "fillseq":
                case "fillrandom":
                case "overwrite":
                case "fillsync":
                    await WritesAsync(me, w, n, rng).ConfigureAwait(false);
                    break;
                case "readrandom":
                case "readmissing":
                    await ReadsAsync(me, w, n, rng, name == "readmissing").ConfigureAwait(false);
                    break;
                case "readwhilewriting":
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
                case "readseq":
                case "readreverse":
                    await ScanAsync(me, w, n, name == "readreverse").ConfigureAwait(false);
                    break;
                case "seekrandom":
                    await SeeksAsync(me, w, n, rng).ConfigureAwait(false);
                    break;
                case "multireadrandom":
                    DoMultiGet(w, n, ref rng); // no MultiGetAsync in the managed API
                    break;
                case "deleterandom":
                    await DeletesAsync(me, w, n, rng).ConfigureAwait(false);
                    break;
            }
        }

        private async Task WritesAsync(ManagedEngine me, Worker w, long n, SplitMix64 rng)
        {
            bool seq = name == "fillseq", sync = name == "fillsync", hist = f.Histogram;
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
                case "engine": f.Engine = value; break;
                case "async": f.Async = ParseBool(value); break;
                case "benchmarks": f.Benchmarks = value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries); break;
                case "num": f.Num = long.Parse(value, ci); break;
                case "reads": f.Reads = long.Parse(value, ci); break;
                case "key_size": f.KeySize = int.Parse(value, ci); break;
                case "value_size": f.ValueSize = int.Parse(value, ci); break;
                case "threads": f.Threads = Math.Max(1, int.Parse(value, ci)); break;
                case "batch_size": f.BatchSize = Math.Max(1, int.Parse(value, ci)); break;
                case "multiget_size": f.MultiGetSize = Math.Max(1, int.Parse(value, ci)); break;
                case "seek_nexts": f.SeekNexts = int.Parse(value, ci); break;
                case "compression_ratio": f.CompressionRatio = double.Parse(value, ci); break;
                case "db": f.Db = value; break;
                case "use_existing_db": f.UseExistingDb = ParseBool(value); break;
                case "seed": f.Seed = long.Parse(value, ci); break;
                case "histogram": f.Histogram = ParseBool(value); break;
                case "c_bench": f.CBenchPath = value; break;
                case "disable_wal": o = o with { DisableWal = ParseBool(value) }; break;
                case "sync": o = o with { Sync = ParseBool(value) }; break;
                case "cache_size": o = o with { BlockCacheSize = long.Parse(value, ci) }; break;
                case "cache_type": o = o with { CacheType = value }; break;
                case "bloom_bits": o = o with { BloomBitsPerKey = int.Parse(value, ci) }; break;
                case "write_buffer_size": o = o with { WriteBufferSize = long.Parse(value, ci) }; break;
                case "block_size": o = o with { BlockSize = int.Parse(value, ci) }; break;
                case "compression":
                case "compression_type": o = o with { Compression = value }; break;
                case "target_file_size_base": o = o with { TargetFileSizeBase = long.Parse(value, ci) }; break;
                case "max_bytes_for_level_base": o = o with { MaxBytesForLevelBase = long.Parse(value, ci) }; break;
                case "level0_file_num_compaction_trigger": o = o with { Level0FileNumCompactionTrigger = int.Parse(value, ci) }; break;
                case "disable_auto_compactions": o = o with { DisableAutoCompactions = ParseBool(value) }; break;
                case "max_background_jobs": o = o with { MaxBackgroundJobs = int.Parse(value, ci) }; break;
                default: throw new ArgumentException("unknown flag --" + name);
            }
        }
        f.Options = o;
        if (f.KeySize < 8) throw new ArgumentException("--key_size must be >= 8");
        if (f.Reads < 0) f.Reads = f.Num;
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
            levels.Append(engine.GetProperty("rocksdb.num-files-at-level" + l.ToString(ci)) ?? "?");
        }
        Console.WriteLine(string.Format(ci, "{0,-17} : files per level [{1}]", "stats", levels));
        string stats = engine.GetProperty("rocksdb.stats") ?? "";
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
        string pending = engine.GetProperty("rocksdb.estimate-pending-compaction-bytes") ?? "?";
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
        Console.Error.WriteLine("               [--key_size 16] [--value_size 100] [--threads 1] [--batch_size 1] [--multiget_size 16] [--seek_nexts 10]");
        Console.Error.WriteLine("               [--disable_wal] [--sync] [--cache_size BYTES] [--cache_type lru|hyperclock] [--bloom_bits 10]");
        Console.Error.WriteLine("               [--write_buffer_size BYTES] [--block_size 4096] [--compression none|snappy|lz4|zstd] [--compression_ratio 0.5]");
        Console.Error.WriteLine("               [--target_file_size_base BYTES] [--max_bytes_for_level_base BYTES] [--level0_file_num_compaction_trigger 4]");
        Console.Error.WriteLine("               [--disable_auto_compactions] [--max_background_jobs 2]");
        Console.Error.WriteLine("               [--use_existing_db] [--seed 301] [--db PATH] [--histogram 1] [--c_bench PATH-TO-rocksdb_bench]");
        Console.Error.WriteLine("benchmarks: " + string.Join(' ', AllBenchmarks));
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
