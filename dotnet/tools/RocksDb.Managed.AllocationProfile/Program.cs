using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Memory.Introspect;
using Memory.Introspect.Trace;

namespace RocksDb.Managed.AllocationProfile;

/// <summary>
/// Allocation profiler for the RocksDb.Managed public API.
/// <code>
/// list [filter...]                       scenarios (name, group, path, op)
/// run [filter...] [options]              run scenarios (each in its own child process) and write reports
///   --warmup S        warm-up seconds per scenario (default 1.5)
///   --measure S       exact measured phase seconds (default 1.5)
///   --trace S         Memory.Introspect allocation trace seconds (default 3; 0 = exact numbers only)
///   --out DIR         report directory (default ./alloc-results): allocations.md + allocations.json
///   --work DIR        DB / trace directory (default: temp)
///   --keep-traces     keep the .nettrace files
///   --stacks N        call stacks per scenario in the report (default 5)
///   --native          include the rocksdb-sharp comparison scenarios (also selected by a "native" filter)
/// child NAME --work DIR --warmup S --measure S [--traced]   (internal)
/// </code>
/// Filters match a scenario name or group exactly, or by prefix with a trailing '*'; "all" (or nothing) selects every
/// managed scenario.
/// </summary>
internal static class Program
{
    public static async Task<int> Main(string[] args)
    {
        if (args.Length == 0 || args[0] is "-h" or "--help" or "help")
        {
            Console.WriteLine("usage: list [filter...] | run [filter...] [--warmup S] [--measure S] [--trace S] [--out DIR] [--work DIR] [--keep-traces] [--stacks N] [--native]");
            return 0;
        }
        switch (args[0])
        {
            case "list":
                foreach (Scenario s in Select(args.AsSpan(1).ToArray(), includeNative: true))
                    Console.WriteLine($"{s.Name,-44} {s.Group,-12} {s.Path,-5} {s.Op}");
                return 0;
            case "child":
                return await RunChildAsync(args).ConfigureAwait(false);
            case "run":
                return await RunParentAsync(args).ConfigureAwait(false);
            default:
                Console.Error.WriteLine("unknown command " + args[0]);
                return 2;
        }
    }

    private static List<Scenario> Select(string[] filters, bool includeNative)
    {
        var all = Scenarios.All();
        var selected = new List<Scenario>();
        bool any = filters.Length == 0 || Array.IndexOf(filters, "all") >= 0;
        foreach (Scenario s in all)
        {
            bool match = any && (includeNative || !s.Native);
            foreach (string f in filters)
            {
                if (f == "all") continue;
                if (f.EndsWith('*') ? s.Name.StartsWith(f[..^1], StringComparison.Ordinal) || s.Group.StartsWith(f[..^1], StringComparison.Ordinal)
                                    : s.Name == f || s.Group == f)
                    match = true;
            }
            if (match) selected.Add(s);
        }
        return selected;
    }

    private static async Task<int> RunChildAsync(string[] args)
    {
        string name = args[1];
        string work = Path.Combine(Path.GetTempPath(), "rocksdb-alloc-" + Environment.ProcessId);
        double warmup = 1.5, measure = 1.5;
        bool traced = false;
        for (int i = 2; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--work": work = args[++i]; break;
                case "--warmup": warmup = double.Parse(args[++i], CultureInfo.InvariantCulture); break;
                case "--measure": measure = double.Parse(args[++i], CultureInfo.InvariantCulture); break;
                case "--traced": traced = true; break;
            }
        }
        Scenario? scenario = Scenarios.All().Find(s => s.Name == name);
        if (scenario is null)
        {
            Console.Out.WriteLine(Child.ErrorTag + JsonSerializer.Serialize("unknown scenario " + name));
            return 2;
        }
        return await Child.RunAsync(scenario, work, warmup, measure, traced).ConfigureAwait(false);
    }

    private static async Task<int> RunParentAsync(string[] args)
    {
        var filters = new List<string>();
        double warmup = 1.5, measure = 1.5, trace = 3;
        string outDir = Path.GetFullPath("alloc-results");
        string work = Path.Combine(Path.GetTempPath(), "rocksdb-alloc-" + Environment.ProcessId);
        bool keepTraces = false, native = false;
        int stacks = 5;
        for (int i = 1; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--warmup": warmup = double.Parse(args[++i], CultureInfo.InvariantCulture); break;
                case "--measure": measure = double.Parse(args[++i], CultureInfo.InvariantCulture); break;
                case "--trace": trace = double.Parse(args[++i], CultureInfo.InvariantCulture); break;
                case "--out": outDir = Path.GetFullPath(args[++i]); break;
                case "--work": work = Path.GetFullPath(args[++i]); break;
                case "--keep-traces": keepTraces = true; break;
                case "--stacks": stacks = int.Parse(args[++i], CultureInfo.InvariantCulture); break;
                case "--native": native = true; break;
                default: filters.Add(args[i]); break;
            }
        }
        List<Scenario> selected = Select(filters.ToArray(), native || filters.Exists(f => f.StartsWith("native", StringComparison.Ordinal)));
        if (selected.Count == 0)
        {
            Console.Error.WriteLine("no scenario matches");
            return 2;
        }
        Directory.CreateDirectory(outDir);
        Directory.CreateDirectory(work);
        var introspector = MemoryIntrospector.Create(new MemoryIntrospectorOptions { CircularBufferSizeInMB = 256 });
        var results = new List<ScenarioReport>();
        int failures = 0;
        foreach (Scenario s in selected)
        {
            Console.Error.WriteLine($"== {s.Name} ({s.Group}, {s.Path})");
            ScenarioReport r = await RunOneAsync(introspector, s, work, warmup, measure, trace, keepTraces, stacks).ConfigureAwait(false);
            results.Add(r);
            if (r.Error is not null)
            {
                failures++;
                Console.Error.WriteLine("   FAILED: " + r.Error);
            }
            else
            {
                Console.Error.WriteLine($"   {r.Exact!.ProcessBytesPerOp:0.##} B/op (caller thread {r.Exact.ThreadBytesPerOp:0.##}), {r.Exact.NsPerOp:0} ns/op, " +
                                        $"{r.Exact.Ops} ops; sampled {Bytes(r.SampledBytes)} in {r.SampleCount} ticks");
            }
            // Write after every scenario so a long run leaves partial reports.
            Report.Write(outDir, results, warmup, measure, trace);
        }
        try
        {
            Directory.Delete(work, recursive: true);
        }
        catch (IOException)
        {
        }
        Console.Error.WriteLine($"{results.Count - failures}/{results.Count} scenarios ok; reports in {outDir}");
        return failures == 0 ? 0 : 1;
    }

    private static async Task<ScenarioReport> RunOneAsync(MemoryIntrospector introspector, Scenario s, string work,
        double warmup, double measure, double trace, bool keepTraces, int stacks)
    {
        var report = new ScenarioReport { Name = s.Name, Group = s.Group, Path = s.Path.ToString(), Op = s.Op, Notes = s.Notes, Native = s.Native };
        string childWork = Path.Combine(work, s.Name);
        var psi = new ProcessStartInfo
        {
            FileName = Environment.ProcessPath!,
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            UseShellExecute = false,
        };
        string self = typeof(Program).Assembly.Location;
        if (Path.GetFileNameWithoutExtension(Environment.ProcessPath!) == "dotnet") psi.ArgumentList.Add(self);
        foreach (string a in (string[])["child", s.Name, "--work", childWork, "--warmup", F(warmup), "--measure", F(measure)]) psi.ArgumentList.Add(a);
        if (trace > 0) psi.ArgumentList.Add("--traced");
        using Process child = Process.Start(psi)!;
        string tracePath = Path.Combine(work, s.Name + ".nettrace");
        try
        {
            while (await child.StandardOutput.ReadLineAsync().ConfigureAwait(false) is { } line)
            {
                if (line.StartsWith(Child.ResultTag, StringComparison.Ordinal))
                {
                    report.Exact = JsonSerializer.Deserialize<ExactResult>(line[Child.ResultTag.Length..]);
                }
                else if (line.StartsWith(Child.ErrorTag, StringComparison.Ordinal))
                {
                    report.Error = JsonSerializer.Deserialize<string>(line[Child.ErrorTag.Length..]);
                }
                else if (line == Child.ReadyTag)
                {
                    try
                    {
                        AllocationReport a = await introspector.CollectAllocationReportAsync(child.Id, TimeSpan.FromSeconds(trace), 12, tracePath,
                            resolveCallStacks: true).ConfigureAwait(false);
                        Fill(report, a, stacks);
                    }
                    catch (Exception e)
                    {
                        report.TraceError = e.GetType().Name + ": " + e.Message;
                    }
                    await child.StandardInput.WriteLineAsync("STOP").ConfigureAwait(false);
                    await child.StandardInput.FlushAsync().ConfigureAwait(false);
                }
                else if (line.StartsWith(Child.WindowTag, StringComparison.Ordinal))
                {
                    report.Window = JsonSerializer.Deserialize<WindowResult>(line[Child.WindowTag.Length..]);
                }
                else
                {
                    Console.Error.WriteLine("   | " + line);
                }
            }
            await child.WaitForExitAsync().ConfigureAwait(false);
            if (child.ExitCode != 0 && report.Error is null) report.Error = "child exit code " + child.ExitCode;
        }
        finally
        {
            if (!child.HasExited) child.Kill(entireProcessTree: true);
            if (!keepTraces)
            {
                foreach (string f in (string[])[tracePath, Path.ChangeExtension(tracePath, ".etlx"), tracePath + ".etlx"])
                {
                    if (File.Exists(f)) File.Delete(f);
                }
            }
            try
            {
                if (Directory.Exists(childWork)) Directory.Delete(childWork, recursive: true);
            }
            catch (IOException)
            {
            }
        }
        return report;
    }

    private static void Fill(ScenarioReport r, AllocationReport a, int stacks)
    {
        r.SampledBytes = a.TotalAllocatedBytes;
        r.SampleCount = a.SampleCount;
        foreach (AllocatedType t in a.Types)
            r.Types.Add(new TypeRow { Type = t.TypeName, Bytes = t.AllocatedBytes, Percent = t.AllocatedBytesPercent, Samples = t.SampleCount, LohBytes = t.LargeObjectHeapBytes });
        int n = 0;
        foreach (AllocationCallStack cs in a.CallStacks)
        {
            // Enabling the EventPipe session builds EventSource manifests in the traced process (observer effect).
            bool observer = false;
            foreach (string f in cs.Frames)
            {
                if (f.StartsWith("System.Diagnostics.Tracing.", StringComparison.Ordinal)) { observer = true; break; }
            }
            if (observer)
            {
                r.ObserverBytes += cs.AllocatedBytes;
                continue;
            }
            if (n++ >= stacks) continue;
            r.Stacks.Add(new StackRow { Type = cs.TypeName, Bytes = cs.AllocatedBytes, Percent = cs.AllocatedBytesPercent, Samples = cs.SampleCount, Frames = [.. cs.Frames] });
        }
    }

    private static string F(double d) => d.ToString(CultureInfo.InvariantCulture);

    internal static string Bytes(long b) => b switch
    {
        >= 1 << 30 => (b / (double)(1 << 30)).ToString("0.0", CultureInfo.InvariantCulture) + " GiB",
        >= 1 << 20 => (b / (double)(1 << 20)).ToString("0.0", CultureInfo.InvariantCulture) + " MiB",
        >= 1 << 10 => (b / 1024.0).ToString("0.0", CultureInfo.InvariantCulture) + " KiB",
        _ => b + " B",
    };
}

internal sealed class ScenarioReport
{
    public string Name { get; set; } = "";
    public string Group { get; set; } = "";
    public string Path { get; set; } = "";
    public string Op { get; set; } = "";
    public string? Notes { get; set; }
    public bool Native { get; set; }
    public ExactResult? Exact { get; set; }
    public WindowResult? Window { get; set; }
    public long SampledBytes { get; set; }
    public long SampleCount { get; set; }
    /// <summary>Sampled bytes whose stack is the tracing session's own setup (EventSource manifests), excluded from <see cref="Stacks"/>.</summary>
    public long ObserverBytes { get; set; }
    public List<TypeRow> Types { get; set; } = [];
    public List<StackRow> Stacks { get; set; } = [];
    public string? Error { get; set; }
    public string? TraceError { get; set; }
}

internal sealed class TypeRow
{
    public string Type { get; set; } = "";
    public long Bytes { get; set; }
    public double Percent { get; set; }
    public long Samples { get; set; }
    public long LohBytes { get; set; }
}

internal sealed class StackRow
{
    public string Type { get; set; } = "";
    public long Bytes { get; set; }
    public double Percent { get; set; }
    public long Samples { get; set; }
    public List<string> Frames { get; set; } = [];
}

/// <summary>Markdown + JSON summary.</summary>
internal static class Report
{
    private static readonly JsonSerializerOptions s_json = new() { WriteIndented = true };

    public static void Write(string dir, List<ScenarioReport> results, double warmup, double measure, double trace)
    {
        File.WriteAllText(System.IO.Path.Combine(dir, "allocations.json"), JsonSerializer.Serialize(results, s_json));
        var sb = new StringBuilder();
        sb.AppendLine("# Allocation profile");
        sb.AppendLine();
        sb.AppendLine(CultureInfo.InvariantCulture, $"{DateTime.UtcNow:yyyy-MM-dd HH:mm} UTC, {System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription}, " +
                      $"{Environment.ProcessorCount} CPUs; warm-up {warmup}s, measured {measure}s, trace {trace}s per scenario.");
        sb.AppendLine("B/op = GC.GetTotalAllocatedBytes(precise) delta over the measured phase / ops (all threads, incl. background work);");
        sb.AppendLine("caller = GC.GetAllocatedBytesForCurrentThread on the measuring thread. Sampled = GCAllocationTick (~100 KB) totals of the trace window.");
        sb.AppendLine();
        sb.AppendLine("| Scenario | Path | Op | B/op | caller B/op | ns/op | GCs 0/1/2 | sampled (window B/op) | top stack |");
        sb.AppendLine("|---|---|---|---:|---:|---:|---|---:|---|");
        foreach (ScenarioReport r in results)
        {
            if (r.Exact is null)
            {
                sb.AppendLine(CultureInfo.InvariantCulture, $"| `{r.Name}` | {r.Path} | {r.Op} | error | | | | | {Esc(r.Error)} |");
                continue;
            }
            ExactResult e = r.Exact;
            string top = r.Stacks.Count > 0 ? $"`{Short(r.Stacks[0].Type)}` {Frame(r.Stacks[0].Frames)}" : r.SampleCount == 0 ? "(no samples)" : "(tracing only)";
            string window = r.Window is null ? "" : $"{Program.Bytes(r.SampledBytes - r.ObserverBytes)} ({r.Window.ProcessBytesPerOp:0.##})";
            sb.AppendLine(CultureInfo.InvariantCulture,
                $"| `{r.Name}` | {r.Path} | {r.Op} | {e.ProcessBytesPerOp:0.##} | {e.ThreadBytesPerOp:0.##} | {e.NsPerOp:0} | {e.Gen0}/{e.Gen1}/{e.Gen2} | {window} | {top} |");
        }
        sb.AppendLine();
        sb.AppendLine("## Details");
        foreach (ScenarioReport r in results)
        {
            if (r.Exact is null || (r.Stacks.Count == 0 && r.TraceError is null)) continue;
            sb.AppendLine();
            sb.AppendLine(CultureInfo.InvariantCulture, $"### {r.Name}");
            sb.AppendLine();
            sb.AppendLine(CultureInfo.InvariantCulture, $"{r.Op}. {r.Exact.ProcessBytesPerOp:0.##} B/op over {r.Exact.Ops} ops; trace window {Program.Bytes(r.SampledBytes)} in {r.SampleCount} ticks ({Program.Bytes(r.ObserverBytes)} of them the trace session's own EventSource setup).{(r.Notes is null ? "" : " " + r.Notes)}");
            if (r.TraceError is not null) sb.AppendLine(CultureInfo.InvariantCulture, $"Trace error: {r.TraceError}");
            sb.AppendLine();
            foreach (TypeRow t in r.Types)
                sb.AppendLine(CultureInfo.InvariantCulture, $"- `{t.Type}` {Program.Bytes(t.Bytes)} ({t.Percent:0.#}%){(t.LohBytes > 0 ? $", LOH {Program.Bytes(t.LohBytes)}" : "")}");
            int i = 0;
            foreach (StackRow st in r.Stacks)
            {
                sb.AppendLine();
                sb.AppendLine(CultureInfo.InvariantCulture, $"{++i}. {Program.Bytes(st.Bytes)} ({st.Percent:0.#}%) `{Short(st.Type)}`");
                sb.AppendLine("```");
                foreach (string f in Trim(st.Frames)) sb.AppendLine("   " + f);
                sb.AppendLine("```");
            }
        }
        File.WriteAllText(System.IO.Path.Combine(dir, "allocations.md"), sb.ToString());
    }

    /// <summary>Frames from the allocation site out to the first harness frame (the scenario lambda), at most 14.</summary>
    internal static IEnumerable<string> Trim(List<string> frames)
    {
        int n = 0;
        foreach (string f in frames)
        {
            yield return f.Length > 180 ? f[..180] + "..." : f;
            if (++n >= 14 || f.Contains("AllocationProfile", StringComparison.Ordinal)) yield break;
        }
    }

    /// <summary>The first frame of the allocating stack that is not a BCL frame.</summary>
    private static string Frame(List<string> frames)
    {
        foreach (string f in frames)
        {
            if (f.StartsWith("System.", StringComparison.Ordinal)) continue;
            int paren = f.IndexOf('(', StringComparison.Ordinal);
            string m = paren > 0 ? f[..paren] : f;
            int last = m.LastIndexOf('.');
            int dot = last > 0 ? m.LastIndexOf('.', last - 1) : -1;
            return "`" + (dot > 0 ? m[(dot + 1)..] : m) + "`";
        }
        return "";
    }

    private static string Short(string type) => type.Length > 70 ? type[..70] + "..." : type;

    private static string Esc(string? s) => (s ?? "").Replace('|', '/').Replace('\n', ' ').Replace('\r', ' ') is { Length: > 120 } t ? t[..120] : (s ?? "").Replace('|', '/').Replace('\n', ' ');
}
