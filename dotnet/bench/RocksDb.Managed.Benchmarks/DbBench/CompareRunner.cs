using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;
using CfgKey = (string Label, long Num, int ValueSize, int Threads, string Compression);

namespace RocksDb.Managed.Benchmarks.DbBench;

/// <summary>
/// Three-way comparison driver.
/// <para><c>compare</c>: runs one dbbench configuration on several engines (each in its own process, on a fresh DB
/// directory under --db_base) and appends their RESULT lines to a CSV, and the STATS lines of the <c>stats</c> benchmark
/// (LSM shape, write amplification) to the companion <c>&lt;csv&gt;.lsm.csv</c>:</para>
/// <code>
/// RocksDb.Managed.Benchmarks compare --engines c-native,rocksdb-sharp,managed,managed-async --db_base /tmp/bench \
///     --csv results.csv [--label X] [--rep N] -- --benchmarks fillseq,readrandom --num 1000000 --value_size 100 --threads 4
/// </code>
/// <para><c>--rep N</c> tags the rows with repetition N (last CSV column); <c>report</c> aggregates repeated rows into
/// medians, min-max, coefficients of variation and bootstrap confidence intervals of the geometric means.</para>
/// <para><c>report</c>: turns a CSV into a markdown comparison (one table per configuration plus a summary):</para>
/// <code>
/// RocksDb.Managed.Benchmarks report --csv results.csv --out results.md [--title T] [--machine machine.txt] [--notes notes.txt]
/// </code>
/// </summary>
public static class CompareRunner
{
    public const string CsvHeader =
        "label,engine,benchmark,num,value_size,threads,compression,ops,seconds,micros_per_op,ops_per_sec,mb_per_sec,p50_us,p99_us,p999_us,found,alloc_b_per_op,gc0,gc1,gc2,jit_methods,jit_ms,rep";

    public const string LsmCsvHeader = "label,engine,num,value_size,threads,compression,files_per_level,user_bytes,written_bytes,write_amp,pending_compaction_bytes,rep";

    /// <summary>The companion CSV holding STATS rows: results.csv -> results.lsm.csv.</summary>
    public static string LsmCsvPath(string csv) => Path.ChangeExtension(csv, null) + ".lsm.csv";

    /// <summary>Fields of a RESULT line (RESULT,engine,...,jit_ms); shorter lines are padded so the rep column lines up.</summary>
    private const int ResultColumns = 19;

    public static readonly string[] DefaultEngines = ["c-native", "rocksdb-sharp", "managed", "managed-async"];

    public static int Compare(string[] args)
    {
        string[] engines = DefaultEngines;
        string dbBase = Path.Combine(Path.GetTempPath(), "rocksdb-bench-compare");
        string? csv = null, label = null, cBench = null;
        int repetition = 0;
        var pass = new List<string>();
        int i = 1;
        for (; i < args.Length; i++)
        {
            string a = args[i];
            if (a == "--") { i++; break; }
            string Next() => i + 1 < args.Length ? args[++i] : throw new ArgumentException("missing value for " + a);
            switch (a)
            {
                case "--engines": engines = Next().Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries); break;
                case "--db_base": dbBase = Next(); break;
                case "--csv": csv = Next(); break;
                case "--label": label = Next(); break;
                case "--c_bench": cBench = Next(); break;
                case "--rep": repetition = int.Parse(Next(), CultureInfo.InvariantCulture); break;
                default: pass.Add(a); break;
            }
        }
        for (; i < args.Length; i++) pass.Add(args[i]);
        foreach (string p in pass)
        {
            if (p.StartsWith("--db", StringComparison.Ordinal) && !p.StartsWith("--db_", StringComparison.Ordinal) || p.StartsWith("--engine", StringComparison.Ordinal))
                throw new ArgumentException("compare sets --engine/--db itself; got " + p);
        }
        DbBenchRunner.Flags flags = DbBenchRunner.Parse([.. pass]); // validates and gives the context columns
        label ??= "";

        Directory.CreateDirectory(dbBase);
        StreamWriter? writer = null, lsmWriter = null;
        if (csv is not null)
        {
            bool exists = File.Exists(csv) && new FileInfo(csv).Length > 0;
            writer = new StreamWriter(csv, append: true);
            if (!exists) writer.WriteLine(CsvHeader);
        }
        string context = label + "," + flags.Num + "," + flags.ValueSize + "," + flags.Threads + "," + BenchFlags.Name(flags.Options.Compression);
        int failures = 0;
        try
        {
            foreach (string engine in engines)
            {
                string db = Path.Combine(dbBase, engine + "-" + Environment.ProcessId);
                if (Directory.Exists(db)) Directory.Delete(db, recursive: true);
                var psi = BuildProcess(engine, cBench);
                if (psi is null)
                {
                    Console.Error.WriteLine("skipping " + engine + ": rocksdb_bench not found (make -C bench/native-c)");
                    failures++;
                    continue;
                }
                foreach (string p in pass) psi.ArgumentList.Add(p);
                psi.ArgumentList.Add("--db");
                psi.ArgumentList.Add(db);
                psi.RedirectStandardOutput = true;
                Console.WriteLine("==== " + engine + ": " + psi.FileName + " " + string.Join(' ', psi.ArgumentList));
                using Process proc = Process.Start(psi)!;
                string? line;
                while ((line = proc.StandardOutput.ReadLine()) is not null)
                {
                    if (line.StartsWith("RESULT,", StringComparison.Ordinal))
                    {
                        if (writer is not null)
                        {
                            string[] c = line.Split(',');
                            // RESULT,engine,benchmark,threads,ops,seconds,us,ops/s,MB/s,p50,p99,p999,found,alloc,gc0,gc1,gc2,jit_methods,jit_ms
                            var sb = new StringBuilder();
                            sb.Append(label).Append(',').Append(c[1]).Append(',').Append(c[2]).Append(',')
                              .Append(flags.Num).Append(',').Append(flags.ValueSize).Append(',').Append(c[3]).Append(',')
                              .Append(BenchFlags.Name(flags.Options.Compression));
                            for (int k = 4; k < c.Length; k++) sb.Append(',').Append(c[k]);
                            for (int k = c.Length; k < ResultColumns; k++) sb.Append(',');
                            sb.Append(',').Append(repetition.ToString(CultureInfo.InvariantCulture));
                            writer.WriteLine(sb.ToString());
                            writer.Flush();
                        }
                    }
                    else if (line.StartsWith("STATS,", StringComparison.Ordinal))
                    {
                        // STATS,engine,files_per_level,user_bytes,written_bytes,write_amp,pending
                        Console.WriteLine(line);
                        if (csv is not null)
                        {
                            if (lsmWriter is null)
                            {
                                string lsm = LsmCsvPath(csv);
                                bool lsmExists = File.Exists(lsm) && new FileInfo(lsm).Length > 0;
                                lsmWriter = new StreamWriter(lsm, append: true);
                                if (!lsmExists) lsmWriter.WriteLine(LsmCsvHeader);
                            }
                            string[] c = line.Split(',');
                            int comma = context.IndexOf(',');
                            lsmWriter.WriteLine(context[..comma] + "," + c[1] + context[comma..] + "," + string.Join(',', c, 2, c.Length - 2)
                                + "," + repetition.ToString(CultureInfo.InvariantCulture));
                            lsmWriter.Flush();
                        }
                    }
                    else
                    {
                        Console.WriteLine(line);
                    }
                }
                proc.WaitForExit();
                if (proc.ExitCode != 0)
                {
                    Console.Error.WriteLine(engine + " exited with code " + proc.ExitCode);
                    failures++;
                }
                if (Directory.Exists(db)) Directory.Delete(db, recursive: true);
                // <db>-txnoptimistic, <db>-txnpessimistic, <db>-warmup (if a run was interrupted), ...
                foreach (string sibling in Directory.GetDirectories(dbBase, Path.GetFileName(db) + "-*")) Directory.Delete(sibling, recursive: true);
            }
        }
        finally
        {
            writer?.Dispose();
            lsmWriter?.Dispose();
        }
        return failures == 0 ? 0 : 3;
    }

    private static ProcessStartInfo? BuildProcess(string engine, string? cBench)
    {
        if (engine == "c-native")
        {
            string? exe = CNativeBench.Locate(cBench);
            return exe is null ? null : new ProcessStartInfo(exe) { UseShellExecute = false };
        }
        // Re-invoke this program (apphost, or `dotnet <dll>` when started through the muxer).
        string host = Environment.ProcessPath ?? "dotnet";
        var psi = new ProcessStartInfo(host) { UseShellExecute = false };
        if (Path.GetFileNameWithoutExtension(host) == "dotnet") psi.ArgumentList.Add(typeof(CompareRunner).Assembly.Location);
        psi.ArgumentList.Add("dbbench");
        psi.ArgumentList.Add("--engine");
        switch (engine)
        {
            case "managed-async":
                psi.ArgumentList.Add("managed");
                psi.ArgumentList.Add("--async");
                psi.ArgumentList.Add("true");
                break;
            case "rocksdb-sharp":
            case "native":
                psi.ArgumentList.Add("rocksdb-sharp");
                break;
            default:
                psi.ArgumentList.Add(engine);
                break;
        }
        return psi;
    }

    // ---------------------------------------------------------------------- report

    private sealed record Row(string Label, string Engine, string Benchmark, long Num, int ValueSize, int Threads, string Compression,
        long Ops, double Micros, double OpsPerSec, double MBs, double P50, double P99, double P999, string Alloc, string JitMethods, int Rep)
    {
        public bool Available => Ops > 0;
    }

    /// <summary>All repetitions of one (configuration, engine, benchmark); statistics over the available ones.</summary>
    private sealed class Agg
    {
        public readonly List<Row> Runs = [];
        public double[] Micros = [];
        public bool Available => Micros.Length > 0;
        public double Median => Stats.Median(Micros);
        public double Min => Micros.Length > 0 ? Micros.Min() : 0;
        public double Max => Micros.Length > 0 ? Micros.Max() : 0;
        public double Cv => Stats.Cv(Micros);

        public void Seal()
        {
            var m = new List<double>();
            foreach (Row r in Runs) if (r.Available && r.Micros > 0) m.Add(r.Micros);
            Micros = [.. m];
        }

        public double MedianOf(Func<Row, double> f)
        {
            var v = new List<double>();
            foreach (Row r in Runs) if (r.Available) v.Add(f(r));
            return Stats.Median([.. v]);
        }

        /// <summary>Median of a numeric text column (alloc), or "" when no run has it.</summary>
        public string MedianText(Func<Row, string> f, string format)
        {
            var v = new List<double>();
            foreach (Row r in Runs)
            {
                if (r.Available && double.TryParse(f(r), NumberStyles.Float, CultureInfo.InvariantCulture, out double d)) v.Add(d);
            }
            return v.Count == 0 ? "" : Stats.Median([.. v]).ToString(format, CultureInfo.InvariantCulture);
        }

        /// <summary>Largest value of an integer text column over the runs (jit methods), or "".</summary>
        public string MaxText(Func<Row, string> f)
        {
            long max = -1;
            foreach (Row r in Runs)
            {
                if (r.Available && long.TryParse(f(r), NumberStyles.Integer, CultureInfo.InvariantCulture, out long n) && n > max) max = n;
            }
            return max < 0 ? "" : max.ToString(CultureInfo.InvariantCulture);
        }
    }

    /// <summary>Median, coefficient of variation, geometric mean and percentile bootstrap for the repeated-run report.</summary>
    private static class Stats
    {
        public static double Median(double[] v)
        {
            if (v.Length == 0) return 0;
            double[] s = (double[])v.Clone();
            Array.Sort(s);
            int n = s.Length;
            return (n & 1) == 1 ? s[n / 2] : (s[n / 2 - 1] + s[n / 2]) / 2;
        }

        /// <summary>Coefficient of variation: sample standard deviation / mean (0 for fewer than 2 values).</summary>
        public static double Cv(double[] v)
        {
            if (v.Length < 2) return 0;
            double mean = v.Average(), ss = 0;
            foreach (double x in v) ss += (x - mean) * (x - mean);
            return mean > 0 ? Math.Sqrt(ss / (v.Length - 1)) / mean : 0;
        }

        /// <summary>Geometric mean of median(x) / median(base) over the pairs.</summary>
        public static double GeomeanRatio(List<(Agg Base, Agg X)> pairs)
        {
            double logSum = 0;
            foreach (var (b, x) in pairs) logSum += Math.Log(x.Median / b.Median);
            return Math.Exp(logSum / pairs.Count);
        }

        /// <summary>
        /// 95% percentile-bootstrap interval of <see cref="GeomeanRatio"/>: every resample redraws, for each benchmark and
        /// each of the two engines independently, as many repetitions as it has (with replacement), recomputes the medians,
        /// the per-benchmark ratios and their geometric mean. Fixed seed, so a report is reproducible from its CSV.
        /// </summary>
        public static (double Lo, double Hi) BootstrapGeomeanCi(List<(Agg Base, Agg X)> pairs, int resamples = 2000)
        {
            var rng = new Random(12345);
            var est = new double[resamples];
            var scratch = new double[64];
            for (int b = 0; b < resamples; b++)
            {
                double logSum = 0;
                foreach (var (bs, x) in pairs)
                {
                    double xm = ResampledMedian(x.Micros, rng, ref scratch);
                    logSum += Math.Log(xm / ResampledMedian(bs.Micros, rng, ref scratch));
                }
                est[b] = Math.Exp(logSum / pairs.Count);
            }
            Array.Sort(est);
            return (est[(int)(0.025 * (resamples - 1))], est[(int)Math.Ceiling(0.975 * (resamples - 1))]);
        }

        private static double ResampledMedian(double[] v, Random rng, ref double[] scratch)
        {
            if (scratch.Length < v.Length) scratch = new double[v.Length];
            for (int i = 0; i < v.Length; i++) scratch[i] = v[rng.Next(v.Length)];
            Array.Sort(scratch, 0, v.Length);
            int n = v.Length;
            return (n & 1) == 1 ? scratch[n / 2] : (scratch[n / 2 - 1] + scratch[n / 2]) / 2;
        }
    }

    /// <summary>Rows with a coefficient of variation above this are flagged as noisy in the repeated-run report.</summary>
    private const double NoisyCv = 0.10;

    public static int Report(string[] args)
    {
        var csvs = new List<string>();
        string? output = null, title = null, machine = null, notes = null;
        for (int i = 1; i < args.Length; i++)
        {
            string a = args[i];
            string Next() => i + 1 < args.Length ? args[++i] : throw new ArgumentException("missing value for " + a);
            switch (a)
            {
                case "--csv": csvs.Add(Next()); break;
                case "--out": output = Next(); break;
                case "--title": title = Next(); break;
                case "--machine": machine = Next(); break;
                case "--notes": notes = Next(); break;
                default: throw new ArgumentException("unknown report flag " + a);
            }
        }
        if (csvs.Count == 0) throw new ArgumentException("report needs --csv FILE");

        var rows = new List<Row>();
        foreach (string csv in csvs)
        {
            foreach (string line in File.ReadLines(csv))
            {
                if (line.Length == 0 || line.StartsWith("label,", StringComparison.Ordinal)) continue;
                string[] c = line.Split(',');
                if (c.Length < 17) continue;
                rows.Add(new Row(c[0], c[1], c[2], L(c[3]), (int)L(c[4]), (int)L(c[5]), c[6], L(c[7]), D(c[9]), D(c[10]), D(c[11]),
                    D(c[12]), D(c[13]), D(c[14]), c.Length > 16 ? c[16] : "", c.Length > 20 ? c[20] : "", c.Length > 22 ? (int)L(c[22]) : 0));
            }
        }

        var engines = new List<string>();
        foreach (string e in DefaultEngines) if (rows.Exists(r => r.Engine == e)) engines.Add(e);
        foreach (Row r in rows) if (!engines.Contains(r.Engine)) engines.Add(r.Engine);
        var benchOrder = new List<string>();
        foreach (Row r in rows) if (!benchOrder.Contains(r.Benchmark)) benchOrder.Add(r.Benchmark);
        var configs = new List<CfgKey>();
        var aggs = new Dictionary<(CfgKey, string, string), Agg>();
        foreach (Row r in rows)
        {
            CfgKey key = (r.Label, r.Num, r.ValueSize, r.Threads, r.Compression);
            if (!configs.Contains(key)) configs.Add(key);
            if (!aggs.TryGetValue((key, r.Engine, r.Benchmark), out Agg? agg)) aggs[(key, r.Engine, r.Benchmark)] = agg = new Agg();
            agg.Runs.Add(r);
        }
        int maxReps = 1;
        foreach (Agg a in aggs.Values)
        {
            a.Seal();
            maxReps = Math.Max(maxReps, a.Runs.Count);
        }
        bool repeated = maxReps > 1;
        Agg? Find(CfgKey cfg, string engine, string bench) => aggs.TryGetValue((cfg, engine, bench), out Agg? a) ? a : null;

        var ci = CultureInfo.InvariantCulture;
        var md = new StringBuilder();
        md.Append("# ").AppendLine(title ?? "RocksDB engine comparison: C native vs rocksdb-sharp vs RocksDb.Managed").AppendLine();
        if (machine is not null && File.Exists(machine))
        {
            md.AppendLine("## Machine").AppendLine();
            foreach (string l in File.ReadLines(machine)) md.Append("- ").AppendLine(l);
            md.AppendLine();
        }
        if (notes is not null && File.Exists(notes))
        {
            md.AppendLine(File.ReadAllText(notes).TrimEnd()).AppendLine();
        }
        md.AppendLine("Engines: **c-native** = bench/native-c/rocksdb_bench (C API, same librocksdb.so as rocksdb-sharp); "
            + "**rocksdb-sharp** = NuGet `RocksDB` binding; **managed** = RocksDb.Managed sync API; **managed-async** = RocksDb.Managed "
            + "async API (one task per thread). Cells are µs/op (wall time / total ops, lower is better); ratios are relative to c-native "
            + "(> 1 = slower). p99 is per-operation latency in µs (per 16-key batch for multireadrandom). alloc = managed bytes allocated per op.")
          .AppendLine();
        if (repeated)
        {
            md.Append("Repeated runs: up to ").Append(maxReps).Append(" repetitions of every engine x configuration x benchmark, each on a fresh DB, "
                + "engines interleaved within each repetition. Cells are the **median** µs/op with the min-max range and the coefficient of "
                + "variation (sample stddev / mean) of the repetitions; ratios are median / c-native median; p99 and alloc are medians over "
                + "the repetitions. Rows where any engine has CV > ").Append((NoisyCv * 100).ToString("F0", ci))
              .AppendLine("% are flagged noisy (`!`). Geometric means use the medians; the 95% confidence interval is a percentile bootstrap "
                + "(2000 resamples of the repetitions of each engine and benchmark, fixed seed).").AppendLine();
        }
        bool hasJit = rows.Exists(r => r.JitMethods.Length > 0);
        if (hasJit)
        {
            md.AppendLine("jit = methods JIT-compiled (all threads, incl. the tiering background compiler) while the benchmark was measured, "
                + "for the .NET engines (`System.Runtime.JitInfo`)" + (repeated ? ", largest over the repetitions" : "")
                + "; ~0 when the warm-up did its job.").AppendLine();
        }

        // (c-native, engine) pairs over some configurations; waitforcompaction is left out.
        List<(Agg Base, Agg X)> Pairs(List<CfgKey> cfgs, string e, string? onlyBench)
        {
            var pairs = new List<(Agg, Agg)>();
            foreach (CfgKey cfg in cfgs)
            {
                foreach (string b in benchOrder)
                {
                    if (b == "waitforcompaction" || onlyBench is not null && b != onlyBench) continue;
                    Agg? c = Find(cfg, "c-native", b), x = Find(cfg, e, b);
                    if (c is { Available: true } && x is { Available: true }) pairs.Add((c, x));
                }
            }
            return pairs;
        }
        string GeoCell(List<(Agg Base, Agg X)> pairs, bool withCount)
        {
            if (pairs.Count == 0) return "-";
            string cell = Stats.GeomeanRatio(pairs).ToString("F2", ci) + "x";
            if (repeated)
            {
                var (lo, hi) = Stats.BootstrapGeomeanCi(pairs);
                cell += " [" + lo.ToString("F2", ci) + "-" + hi.ToString("F2", ci) + "]";
            }
            return withCount ? cell + " (" + pairs.Count + ")" : cell;
        }
        void GeoHeader()
        {
            foreach (string e in engines) if (e != "c-native") md.Append(' ').Append(e).Append(" / c-native |");
            md.AppendLine();
            md.Append("|---|");
            foreach (string e in engines) if (e != "c-native") md.Append("---:|");
            md.AppendLine();
        }

        // Summary: geometric mean of ratios vs c-native per configuration.
        md.Append("## Summary (geometric mean of ").Append(repeated ? "median " : "").AppendLine("µs/op ratio vs c-native over the benchmarks of each configuration)").AppendLine();
        md.AppendLine("`waitforcompaction` is left out: its time is the background work that happened to be left over, not the speed of an operation."
            + (repeated ? " Brackets: 95% bootstrap confidence interval; parentheses: number of benchmarks." : "")).AppendLine();
        md.Append("| configuration |");
        GeoHeader();
        foreach (CfgKey cfg in configs)
        {
            md.Append("| ").Append(ConfigName(cfg)).Append(" |");
            foreach (string e in engines) if (e != "c-native") md.Append(' ').Append(GeoCell(Pairs([cfg], e, null), true)).Append(" |");
            md.AppendLine();
        }
        List<CfgKey> matrixCfgs = configs.FindAll(c => c.Label.Length == 0);
        if (matrixCfgs.Count > 1)
        {
            md.Append("| **all matrix configurations** |");
            foreach (string e in engines) if (e != "c-native") md.Append(" **").Append(GeoCell(Pairs(matrixCfgs, e, null), true)).Append("** |");
            md.AppendLine();
        }
        md.AppendLine();

        if (matrixCfgs.Count > 1)
        {
            md.Append("### Per benchmark (geometric mean over the ").Append(matrixCfgs.Count).Append(" matrix configurations of the ")
              .Append(repeated ? "median " : "").AppendLine("ratio vs c-native)").AppendLine();
            md.Append("| benchmark |");
            GeoHeader();
            foreach (string b in benchOrder)
            {
                if (b == "waitforcompaction" || !rows.Exists(r => r.Label.Length == 0 && r.Benchmark == b)) continue;
                md.Append("| ").Append(b).Append(" |");
                foreach (string e in engines) if (e != "c-native") md.Append(' ').Append(GeoCell(Pairs(matrixCfgs, e, b), false)).Append(" |");
                md.AppendLine();
            }
            md.AppendLine();
        }

        if (hasJit)
        {
            md.AppendLine("JIT during the measurements (methods compiled while a benchmark was being timed, over all configurations"
                + (repeated ? " and repetitions" : "") + "):").AppendLine();
            md.AppendLine("| engine | measurements | total methods | median | max (benchmark) |");
            md.AppendLine("|---|---:|---:|---:|---|");
            foreach (string e in engines)
            {
                if (e == "c-native") continue;
                var counts = new List<(long N, Row R)>();
                foreach (Row r in rows)
                {
                    if (r.Engine == e && r.Available && long.TryParse(r.JitMethods, NumberStyles.Integer, CultureInfo.InvariantCulture, out long n)) counts.Add((n, r));
                }
                if (counts.Count == 0) continue;
                counts.Sort((x, y) => x.N.CompareTo(y.N));
                long sum = 0;
                foreach (var c in counts) sum += c.N;
                var max = counts[^1];
                md.Append("| ").Append(e).Append(" | ").Append(counts.Count).Append(" | ").Append(sum).Append(" | ")
                  .Append(counts[counts.Count / 2].N).Append(" | ").Append(max.N).Append(" (").Append(max.R.Benchmark).Append(", ")
                  .Append(ConfigName((max.R.Label, max.R.Num, max.R.ValueSize, max.R.Threads, max.R.Compression)))
                  .Append(repeated ? ", rep " + max.R.Rep.ToString(ci) : "").AppendLine(") |");
            }
            md.AppendLine();
        }

        var noisy = new List<string>();
        foreach (CfgKey cfg in configs)
        {
            md.Append("## ").AppendLine(ConfigName(cfg)).AppendLine();
            md.Append("| benchmark |");
            foreach (string e in engines) md.Append(' ').Append(e).Append(repeated ? " µs/op median (min-max, CV) |" : " µs/op |");
            foreach (string e in engines) if (e != "c-native") md.Append(' ').Append(e).Append(" / C |");
            foreach (string e in engines) md.Append(' ').Append(e).Append(" p99 |");
            foreach (string e in engines) if (e.StartsWith("managed", StringComparison.Ordinal)) md.Append(' ').Append(e).Append(" alloc B/op |");
            if (hasJit) foreach (string e in engines) if (e != "c-native") md.Append(' ').Append(e).Append(" jit |");
            md.AppendLine();
            md.Append("|---|");
            int cols = engines.Count * 2 + (engines.Count - (engines.Contains("c-native") ? 1 : 0)) + engines.FindAll(e => e.StartsWith("managed", StringComparison.Ordinal)).Count
                + (hasJit ? engines.FindAll(e => e != "c-native").Count : 0);
            for (int k = 0; k < cols; k++) md.Append("---:|");
            md.AppendLine();
            foreach (string b in benchOrder)
            {
                if (!rows.Exists(r => Same(r, cfg) && r.Benchmark == b)) continue;
                bool noisyRow = false;
                foreach (string e in engines)
                {
                    Agg? a = Find(cfg, e, b);
                    if (repeated && a is { Available: true } && a.Cv > NoisyCv)
                    {
                        noisyRow = true;
                        noisy.Add("| " + ConfigName(cfg) + " | " + b + " | " + e + " | " + (a.Cv * 100).ToString("F1", ci) + "% | "
                            + FormatMicros(a.Median) + " | " + FormatMicros(a.Min) + "-" + FormatMicros(a.Max) + " | " + a.Micros.Length.ToString(ci) + " |");
                    }
                }
                md.Append("| ").Append(b).Append(noisyRow ? " !" : "").Append(" |");
                Agg? cn = Find(cfg, "c-native", b);
                foreach (string e in engines)
                {
                    Agg? a = Find(cfg, e, b);
                    string cell = a is null ? "-" : !a.Available ? "n/a" : FormatMicros(a.Median);
                    if (repeated && a is { Available: true } && a.Micros.Length > 1)
                    {
                        cell += " (" + FormatMicros(a.Min) + "-" + FormatMicros(a.Max) + ", " + (a.Cv * 100).ToString("F0", ci) + "%" + (a.Cv > NoisyCv ? " !" : "") + ")";
                    }
                    md.Append(' ').Append(cell).Append(" |");
                }
                foreach (string e in engines)
                {
                    if (e == "c-native") continue;
                    Agg? a = Find(cfg, e, b);
                    string cell = a is { Available: true } && cn is { Available: true } ? (a.Median / cn.Median).ToString("F2", ci) + "x" : "-";
                    md.Append(' ').Append(cell).Append(" |");
                }
                foreach (string e in engines)
                {
                    Agg? a = Find(cfg, e, b);
                    double p99 = a is { Available: true } ? a.MedianOf(r => r.P99) : 0;
                    md.Append(' ').Append(p99 > 0 ? FormatMicros(p99) : "-").Append(" |");
                }
                foreach (string e in engines)
                {
                    if (!e.StartsWith("managed", StringComparison.Ordinal)) continue;
                    Agg? a = Find(cfg, e, b);
                    string alloc = a is { Available: true } ? a.MedianText(r => r.Alloc, "F1") : "";
                    md.Append(' ').Append(alloc.Length > 0 ? alloc : "-").Append(" |");
                }
                if (hasJit)
                {
                    foreach (string e in engines)
                    {
                        if (e == "c-native") continue;
                        Agg? a = Find(cfg, e, b);
                        string jit = a is { Available: true } ? a.MaxText(r => r.JitMethods) : "";
                        md.Append(' ').Append(jit.Length > 0 ? jit : "-").Append(" |");
                    }
                }
                md.AppendLine();
            }
            md.AppendLine();
        }

        if (repeated)
        {
            md.Append("## Noisy rows (CV > ").Append((NoisyCv * 100).ToString("F0", ci)).AppendLine("%)").AppendLine();
            if (noisy.Count == 0)
            {
                md.AppendLine("None.").AppendLine();
            }
            else
            {
                md.AppendLine("| configuration | benchmark | engine | CV | median µs/op | min-max | runs |");
                md.AppendLine("|---|---|---|---:|---:|---:|---:|");
                foreach (string n in noisy) md.AppendLine(n);
                md.AppendLine();
            }
        }

        var lsmFiles = new List<string>();
        foreach (string csv in csvs) if (File.Exists(LsmCsvPath(csv))) lsmFiles.Add(LsmCsvPath(csv));
        if (lsmFiles.Count > 0) AppendLsm(md, lsmFiles, engines);

        md.Append("Raw data: ");
        foreach (string c in csvs) md.Append('`').Append(Path.GetFileName(c)).Append("` ");
        foreach (string c in lsmFiles) md.Append('`').Append(Path.GetFileName(c)).Append("` ");
        md.AppendLine();
        if (output is null) Console.Write(md.ToString());
        else File.WriteAllText(output, md.ToString());
        return 0;
    }

    /// <summary>"LSM shape and write amplification" section from the STATS rows of the <c>stats</c> benchmark.</summary>
    private static void AppendLsm(StringBuilder md, List<string> lsmFiles, List<string> engines)
    {
        var ci = CultureInfo.InvariantCulture;
        var lines = new List<string[]>();
        foreach (string file in lsmFiles)
        {
            foreach (string line in File.ReadLines(file))
            {
                if (line.Length == 0 || line.StartsWith("label,", StringComparison.Ordinal)) continue;
                string[] c = line.Split(',');
                if (c.Length >= 11) lines.Add(c);
            }
        }
        bool repeated = lines.Exists(c => c.Length > 11 && L(c[11]) > 1);
        md.AppendLine("## LSM shape and write amplification (`stats` benchmark)").AppendLine();
        md.AppendLine("files per level = rocksdb.num-files-at-level0..6; written = flush + compaction output bytes from the "
            + "\"Cumulative compaction: X GB write\" line of rocksdb.stats (0.01 GB resolution); user = key + value bytes written by the "
            + "benchmarks since the DB was created; write-amp = written / user (WAL not included).").AppendLine();
        if (repeated)
        {
            md.AppendLine("Over the repetitions: write-amp median (min-max), and the most frequent LSM shape with the number of repetitions that had it.").AppendLine();
            md.AppendLine("| configuration | engine | runs | write-amp median (min-max) | written MB median | most frequent files per level (L0..L6) |");
            md.AppendLine("|---|---|---:|---:|---:|---|");
            var cfgNames = new List<string>();
            foreach (string[] c in lines)
            {
                string cfg = ConfigName((c[0], L(c[2]), (int)L(c[3]), (int)L(c[4]), c[5]));
                if (!cfgNames.Contains(cfg)) cfgNames.Add(cfg);
            }
            var lineEngines = new List<string>(engines);
            foreach (string[] c in lines) if (!lineEngines.Contains(c[1])) lineEngines.Add(c[1]);
            foreach (string cfg in cfgNames)
            {
                foreach (string e in lineEngines)
                {
                    List<string[]> g = lines.FindAll(c => c[1] == e && ConfigName((c[0], L(c[2]), (int)L(c[3]), (int)L(c[4]), c[5])) == cfg);
                    if (g.Count == 0) continue;
                    double[] wa = [.. g.Select(c => D(c[9]))];
                    double[] written = [.. g.Select(c => D(c[8]) / 1048576.0)];
                    var shape = g.GroupBy(c => c[6]).OrderByDescending(x => x.Count()).First();
                    md.Append("| ").Append(cfg).Append(" | ").Append(e).Append(" | ").Append(g.Count.ToString(ci)).Append(" | ")
                      .Append(Stats.Median(wa).ToString("F2", ci)).Append(" (").Append(wa.Min().ToString("F2", ci)).Append('-').Append(wa.Max().ToString("F2", ci)).Append(") | ")
                      .Append(Stats.Median(written).ToString("F1", ci)).Append(" | `").Append(shape.Key).Append("` (")
                      .Append(shape.Count().ToString(ci)).Append('/').Append(g.Count.ToString(ci)).AppendLine(") |");
                }
            }
            md.AppendLine();
            md.AppendLine("Every repetition:").AppendLine();
        }
        md.AppendLine("| configuration | engine |" + (repeated ? " rep |" : "") + " files per level (L0..L6) | user MB | written MB | write-amp | pending compaction bytes |");
        md.AppendLine("|---|---|" + (repeated ? "---:|" : "") + "---|---:|---:|---:|---:|");
        foreach (string[] c in lines)
        {
            CfgKey cfg = (c[0], L(c[2]), (int)L(c[3]), (int)L(c[4]), c[5]);
            md.Append("| ").Append(ConfigName(cfg)).Append(" | ").Append(c[1]).Append(" | ");
            if (repeated) md.Append(c.Length > 11 ? c[11] : "").Append(" | ");
            md.Append('`').Append(c[6]).Append("` | ")
              .Append((D(c[7]) / 1048576.0).ToString("F1", ci)).Append(" | ").Append((D(c[8]) / 1048576.0).ToString("F1", ci)).Append(" | ")
              .Append(c[9]).Append(" | ").Append(c[10]).AppendLine(" |");
        }
        md.AppendLine();
    }

    private static string ConfigName(CfgKey c) =>
        (c.Label.Length > 0 ? c.Label + ": " : "") + "num=" + c.Num + ", value=" + c.ValueSize + "B, threads=" + c.Threads + ", compression=" + c.Compression;

    private static bool Same(Row r, CfgKey c) =>
        r.Label == c.Label && r.Num == c.Num && r.ValueSize == c.ValueSize && r.Threads == c.Threads && r.Compression == c.Compression;

    private static string FormatMicros(double us) => us >= 10000 ? (us / 1000).ToString("F1", CultureInfo.InvariantCulture) + " ms"
        : us >= 100 ? us.ToString("F0", CultureInfo.InvariantCulture)
        : us.ToString("F2", CultureInfo.InvariantCulture);

    private static long L(string s) => long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out long v) ? v : 0;

    private static double D(string s) => double.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out double v) ? v : 0;

    /// <summary>Machine description lines for the report (CPU, cores, kernel, .NET, librocksdb).</summary>
    public static IEnumerable<string> MachineInfo()
    {
        string cpu = "unknown";
        try
        {
            foreach (string l in File.ReadLines("/proc/cpuinfo"))
            {
                if (l.StartsWith("model name", StringComparison.Ordinal))
                {
                    cpu = l[(l.IndexOf(':') + 1)..].Trim();
                    break;
                }
            }
        }
        catch (IOException)
        {
        }
        yield return "CPU: " + cpu + ", " + Environment.ProcessorCount + " logical cores";
        yield return "OS: " + RuntimeInformation.OSDescription;
        yield return ".NET: " + RuntimeInformation.FrameworkDescription;
    }
}
