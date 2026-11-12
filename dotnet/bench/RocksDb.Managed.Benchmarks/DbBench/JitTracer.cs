using System.Diagnostics.Tracing;
using System.Globalization;

namespace RocksDb.Managed.Benchmarks.DbBench;

/// <summary>
/// The methods the JIT compiled while a benchmark was measured, from the runtime's <c>MethodLoadVerbose</c> events,
/// collected in-process with an <see cref="System.Diagnostics.Tracing.EventListener"/>. Enabled by the environment
/// variable <c>DBBENCH_JIT_METHODS=1</c> (first 40 methods) or <c>=N</c>; each measured benchmark line is then followed by
/// <c>JIT,&lt;benchmark&gt;,&lt;tier&gt;,&lt;namespace.type&gt;::&lt;method&gt;</c> rows, in compilation order, and a
/// <c>JIT,&lt;benchmark&gt;,total,&lt;count&gt;</c> row. The tier tells a method compiled for the first time
/// (<c>QuickJitted</c>, <c>MinOptJitted</c>: a path the warm-up did not run at all) from one promoted after more calls
/// (<c>OptimizedTier1</c>, <c>OptimizedTier1OSR</c>: a path the warm-up ran too few times). Off by default.
/// </summary>
internal sealed class JitTracer : System.Diagnostics.Tracing.EventListener
{
    private const string RuntimeProvider = "Microsoft-Windows-DotNETRuntime";
    private const EventKeywords JitKeyword = (EventKeywords)0x10;

    // The optimization tier in bits 7-9 of MethodFlags (MethodFlags enum of ClrEtwAll.man: Unknown, MinOptJitted,
    // Optimized, QuickJitted, OptimizedTier1, OptimizedTier1OSR, QuickJittedInstrumented, OptimizedTier1Instrumented).
    private static readonly string[] s_tiers = ["Unknown", "MinOptJitted", "Optimized", "QuickJitted", "OptimizedTier1", "OptimizedTier1OSR", "QuickJittedInstrumented", "OptimizedTier1Instrumented"];

    private readonly List<string> _methods = new();
    private readonly Lock _lock = new();
    private bool _recording;

    public static JitTracer? Instance { get; } = Create();

    public static int TopN { get; private set; }

    private static JitTracer? Create()
    {
        string? v = Environment.GetEnvironmentVariable("DBBENCH_JIT_METHODS");
        if (string.IsNullOrEmpty(v) || v == "0") return null;
        TopN = int.TryParse(v, NumberStyles.Integer, CultureInfo.InvariantCulture, out int n) && n > 1 ? n : 40;
        return new JitTracer();
    }

    protected override void OnEventSourceCreated(EventSource source)
    {
        if (source.Name == RuntimeProvider) EnableEvents(source, EventLevel.Verbose, JitKeyword);
    }

    protected override void OnEventWritten(EventWrittenEventArgs e)
    {
        // Decoded also while not recording, so the tracer's own payload reflection is compiled during the warm-up and does
        // not show up in the first measured benchmark.
        if (e.EventName is null || !e.EventName.StartsWith("MethodLoadVerbose", StringComparison.Ordinal) || e.Payload is null) return;
        string ns = "", name = "", tier = "?";
        for (int i = 0; i < e.Payload.Count; i++)
        {
            switch (e.PayloadNames![i])
            {
                case "MethodNamespace": ns = e.Payload[i] as string ?? ""; break;
                case "MethodName": name = e.Payload[i] as string ?? ""; break;
                case "MethodFlags":
                    // MethodFlags bits 7-9 carry the optimization tier (MethodLoadVerbose_V1 and later).
                    int t = (int)((Convert.ToUInt32(e.Payload[i], CultureInfo.InvariantCulture) >> 7) & 0x7);
                    tier = t < s_tiers.Length ? s_tiers[t] : t.ToString(CultureInfo.InvariantCulture);
                    break;
            }
        }
        lock (_lock)
        {
            if (_recording) _methods.Add(tier + "," + ns + "::" + name);
        }
    }

    /// <summary>Starts recording (called when a measured benchmark starts).</summary>
    public void Reset()
    {
        lock (_lock)
        {
            _methods.Clear();
            _recording = true;
        }
    }

    /// <summary>Stops recording and prints the methods compiled since <see cref="Reset"/>.</summary>
    public void Print(string benchmark)
    {
        string[] rows;
        lock (_lock)
        {
            _recording = false;
            rows = [.. _methods];
        }
        for (int i = 0; i < rows.Length && i < TopN; i++)
            Console.WriteLine(string.Format(CultureInfo.InvariantCulture, "JIT,{0},{1}", benchmark, rows[i]));
        Console.WriteLine(string.Format(CultureInfo.InvariantCulture, "JIT,{0},total,{1}", benchmark, rows.Length));
    }
}
