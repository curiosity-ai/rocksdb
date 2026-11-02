using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;

namespace RocksDb.Managed.Benchmarks.DbBench;

/// <summary>
/// Three-way comparison driver.
/// <para><c>compare</c>: runs one dbbench configuration on several engines (each in its own process, on a fresh DB
/// directory under --db_base) and appends their RESULT lines to a CSV, and the STATS lines of the <c>stats</c> benchmark
/// (LSM shape, write amplification) to the companion <c>&lt;csv&gt;.lsm.csv</c>:</para>
/// <code>
/// RocksDb.Managed.Benchmarks compare --engines c-native,rocksdb-sharp,managed,managed-async --db_base /tmp/bench \
///     --csv results.csv [--label X] -- --benchmarks fillseq,readrandom --num 1000000 --value_size 100 --threads 4
/// </code>
/// <para><c>report</c>: turns a CSV into a markdown comparison (one table per configuration plus a summary):</para>
/// <code>
/// RocksDb.Managed.Benchmarks report --csv results.csv --out results.md [--title T] [--machine machine.txt] [--notes notes.txt]
/// </code>
/// </summary>
public static class CompareRunner
{
    public const string CsvHeader =
        "label,engine,benchmark,num,value_size,threads,compression,ops,seconds,micros_per_op,ops_per_sec,mb_per_sec,p50_us,p99_us,p999_us,found,alloc_b_per_op,gc0,gc1,gc2";

    public const string LsmCsvHeader = "label,engine,num,value_size,threads,compression,files_per_level,user_bytes,written_bytes,write_amp,pending_compaction_bytes";

    /// <summary>The companion CSV holding STATS rows: results.csv -> results.lsm.csv.</summary>
    public static string LsmCsvPath(string csv) => Path.ChangeExtension(csv, null) + ".lsm.csv";

    public static readonly string[] DefaultEngines = ["c-native", "rocksdb-sharp", "managed", "managed-async"];

    public static int Compare(string[] args)
    {
        string[] engines = DefaultEngines;
        string dbBase = Path.Combine(Path.GetTempPath(), "rocksdb-bench-compare");
        string? csv = null, label = null, cBench = null;
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
                            // RESULT,engine,benchmark,threads,ops,seconds,us,ops/s,MB/s,p50,p99,p999,found,alloc,gc0,gc1,gc2
                            var sb = new StringBuilder();
                            sb.Append(label).Append(',').Append(c[1]).Append(',').Append(c[2]).Append(',')
                              .Append(flags.Num).Append(',').Append(flags.ValueSize).Append(',').Append(c[3]).Append(',')
                              .Append(BenchFlags.Name(flags.Options.Compression));
                            for (int k = 4; k < c.Length; k++) sb.Append(',').Append(c[k]);
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
                            lsmWriter.WriteLine(context[..comma] + "," + c[1] + context[comma..] + "," + string.Join(',', c, 2, c.Length - 2));
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
        long Ops, double Micros, double OpsPerSec, double MBs, double P50, double P99, double P999, string Alloc)
    {
        public bool Available => Ops > 0;
    }

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
                    D(c[12]), D(c[13]), D(c[14]), c.Length > 16 ? c[16] : ""));
            }
        }

        var engines = new List<string>();
        foreach (string e in DefaultEngines) if (rows.Exists(r => r.Engine == e)) engines.Add(e);
        foreach (Row r in rows) if (!engines.Contains(r.Engine)) engines.Add(r.Engine);
        var benchOrder = new List<string>();
        foreach (Row r in rows) if (!benchOrder.Contains(r.Benchmark)) benchOrder.Add(r.Benchmark);
        var configs = new List<(string Label, long Num, int ValueSize, int Threads, string Compression)>();
        foreach (Row r in rows)
        {
            var key = (r.Label, r.Num, r.ValueSize, r.Threads, r.Compression);
            if (!configs.Contains(key)) configs.Add(key);
        }

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

        // Summary: geometric mean of ratios vs c-native per configuration.
        md.AppendLine("## Summary (geometric mean of µs/op ratio vs c-native over the benchmarks of each configuration)").AppendLine();
        md.AppendLine("`waitforcompaction` is left out: its time is the background work that happened to be left over, not the speed of an operation.").AppendLine();
        md.Append("| configuration |");
        foreach (string e in engines) if (e != "c-native") md.Append(' ').Append(e).Append(" / c-native |");
        md.AppendLine();
        md.Append("|---|");
        foreach (string e in engines) if (e != "c-native") md.Append("---:|");
        md.AppendLine();
        foreach (var cfg in configs)
        {
            md.Append("| ").Append(ConfigName(cfg)).Append(" |");
            foreach (string e in engines)
            {
                if (e == "c-native") continue;
                double logSum = 0;
                int count = 0;
                foreach (string b in benchOrder)
                {
                    if (b == "waitforcompaction") continue;
                    Row? c = Find(rows, cfg, "c-native", b), x = Find(rows, cfg, e, b);
                    if (c is { Available: true } && x is { Available: true } && c.Micros > 0 && x.Micros > 0)
                    {
                        logSum += Math.Log(x.Micros / c.Micros);
                        count++;
                    }
                }
                md.Append(' ').Append(count > 0 ? Math.Exp(logSum / count).ToString("F2", ci) + "x (" + count + ")" : "-").Append(" |");
            }
            md.AppendLine();
        }
        md.AppendLine();

        foreach (var cfg in configs)
        {
            md.Append("## ").AppendLine(ConfigName(cfg)).AppendLine();
            md.Append("| benchmark |");
            foreach (string e in engines) md.Append(' ').Append(e).Append(" µs/op |");
            foreach (string e in engines) if (e != "c-native") md.Append(' ').Append(e).Append(" / C |");
            foreach (string e in engines) md.Append(' ').Append(e).Append(" p99 |");
            foreach (string e in engines) if (e.StartsWith("managed", StringComparison.Ordinal)) md.Append(' ').Append(e).Append(" alloc B/op |");
            md.AppendLine();
            md.Append("|---|");
            int cols = engines.Count * 2 + (engines.Count - (engines.Contains("c-native") ? 1 : 0)) + engines.FindAll(e => e.StartsWith("managed", StringComparison.Ordinal)).Count;
            for (int k = 0; k < cols; k++) md.Append("---:|");
            md.AppendLine();
            foreach (string b in benchOrder)
            {
                if (!rows.Exists(r => Same(r, cfg) && r.Benchmark == b)) continue;
                md.Append("| ").Append(b).Append(" |");
                Row? cn = Find(rows, cfg, "c-native", b);
                foreach (string e in engines)
                {
                    Row? r = Find(rows, cfg, e, b);
                    md.Append(' ').Append(r is null ? "-" : !r.Available ? "n/a" : FormatMicros(r.Micros)).Append(" |");
                }
                foreach (string e in engines)
                {
                    if (e == "c-native") continue;
                    Row? r = Find(rows, cfg, e, b);
                    string cell = r is { Available: true } && cn is { Available: true } && cn.Micros > 0 ? (r.Micros / cn.Micros).ToString("F2", ci) + "x" : "-";
                    md.Append(' ').Append(cell).Append(" |");
                }
                foreach (string e in engines)
                {
                    Row? r = Find(rows, cfg, e, b);
                    md.Append(' ').Append(r is { Available: true } && r.P99 > 0 ? FormatMicros(r.P99) : "-").Append(" |");
                }
                foreach (string e in engines)
                {
                    if (!e.StartsWith("managed", StringComparison.Ordinal)) continue;
                    Row? r = Find(rows, cfg, e, b);
                    md.Append(' ').Append(r is { Available: true } && r.Alloc.Length > 0 ? r.Alloc : "-").Append(" |");
                }
                md.AppendLine();
            }
            md.AppendLine();
        }

        var lsmFiles = new List<string>();
        foreach (string csv in csvs) if (File.Exists(LsmCsvPath(csv))) lsmFiles.Add(LsmCsvPath(csv));
        if (lsmFiles.Count > 0) AppendLsm(md, lsmFiles);

        md.Append("Raw data: ");
        foreach (string c in csvs) md.Append('`').Append(Path.GetFileName(c)).Append("` ");
        foreach (string c in lsmFiles) md.Append('`').Append(Path.GetFileName(c)).Append("` ");
        md.AppendLine();
        if (output is null) Console.Write(md.ToString());
        else File.WriteAllText(output, md.ToString());
        return 0;
    }

    /// <summary>"LSM shape and write amplification" section from the STATS rows of the <c>stats</c> benchmark.</summary>
    private static void AppendLsm(StringBuilder md, List<string> lsmFiles)
    {
        var ci = CultureInfo.InvariantCulture;
        md.AppendLine("## LSM shape and write amplification (`stats` benchmark)").AppendLine();
        md.AppendLine("files per level = rocksdb.num-files-at-level0..6; written = flush + compaction output bytes from the "
            + "\"Cumulative compaction: X GB write\" line of rocksdb.stats (0.01 GB resolution); user = key + value bytes written by the "
            + "benchmarks since the DB was created; write-amp = written / user (WAL not included).").AppendLine();
        md.AppendLine("| configuration | engine | files per level (L0..L6) | user MB | written MB | write-amp | pending compaction bytes |");
        md.AppendLine("|---|---|---|---:|---:|---:|---:|");
        foreach (string file in lsmFiles)
        {
            foreach (string line in File.ReadLines(file))
            {
                if (line.Length == 0 || line.StartsWith("label,", StringComparison.Ordinal)) continue;
                string[] c = line.Split(',');
                if (c.Length < 11) continue;
                var cfg = (c[0], L(c[2]), (int)L(c[3]), (int)L(c[4]), c[5]);
                md.Append("| ").Append(ConfigName(cfg)).Append(" | ").Append(c[1]).Append(" | `").Append(c[6]).Append("` | ")
                  .Append((D(c[7]) / 1048576.0).ToString("F1", ci)).Append(" | ").Append((D(c[8]) / 1048576.0).ToString("F1", ci)).Append(" | ")
                  .Append(c[9]).Append(" | ").Append(c[10]).AppendLine(" |");
            }
        }
        md.AppendLine();
    }

    private static string ConfigName((string Label, long Num, int ValueSize, int Threads, string Compression) c) =>
        (c.Label.Length > 0 ? c.Label + ": " : "") + "num=" + c.Num + ", value=" + c.ValueSize + "B, threads=" + c.Threads + ", compression=" + c.Compression;

    private static bool Same(Row r, (string Label, long Num, int ValueSize, int Threads, string Compression) c) =>
        r.Label == c.Label && r.Num == c.Num && r.ValueSize == c.ValueSize && r.Threads == c.Threads && r.Compression == c.Compression;

    private static Row? Find(List<Row> rows, (string Label, long Num, int ValueSize, int Threads, string Compression) cfg, string engine, string bench)
    {
        foreach (Row r in rows) if (Same(r, cfg) && r.Engine == engine && r.Benchmark == bench) return r;
        return null;
    }

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
