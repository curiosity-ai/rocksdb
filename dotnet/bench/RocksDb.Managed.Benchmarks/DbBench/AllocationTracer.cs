using System.Diagnostics.Tracing;
using System.Globalization;

namespace RocksDb.Managed.Benchmarks.DbBench;

/// <summary>
/// Sampled allocations by type, from the runtime's <c>GCAllocationTick</c> events (one event per ~100 KB allocated
/// per heap, naming the type of the object that crossed the threshold), collected in-process with an
/// <see cref="System.Diagnostics.Tracing.EventListener"/>. Enabled by the environment variable <c>DBBENCH_ALLOC_TYPES=1</c> (top 10 types) or
/// <c>=N</c> (top N); each benchmark line is then followed by <c>ALLOC,&lt;benchmark&gt;,&lt;type&gt;,&lt;bytes&gt;</c>
/// rows with the sampled bytes per type. Use <c>DOTNET_gcServer=0</c> (the runner's default) so the sampling
/// threshold is per process rather than per server-GC heap. Off by default: the listener costs a little CPU.
/// </summary>
internal sealed class AllocationTracer : System.Diagnostics.Tracing.EventListener
{
    private const string RuntimeProvider = "Microsoft-Windows-DotNETRuntime";
    private const EventKeywords GcKeyword = (EventKeywords)0x1;

    private readonly Dictionary<string, long> _bytesByType = new();
    private readonly Lock _lock = new();

    public static AllocationTracer? Instance { get; } = Create();

    public static int TopN { get; private set; }

    private static AllocationTracer? Create()
    {
        string? v = Environment.GetEnvironmentVariable("DBBENCH_ALLOC_TYPES");
        if (string.IsNullOrEmpty(v) || v == "0") return null;
        TopN = int.TryParse(v, NumberStyles.Integer, CultureInfo.InvariantCulture, out int n) && n > 1 ? n : 10;
        return new AllocationTracer();
    }

    protected override void OnEventSourceCreated(EventSource source)
    {
        if (source.Name == RuntimeProvider) EnableEvents(source, EventLevel.Verbose, GcKeyword);
    }

    protected override void OnEventWritten(EventWrittenEventArgs e)
    {
        if (e.EventName is null || !e.EventName.StartsWith("GCAllocationTick", StringComparison.Ordinal) || e.Payload is null) return;
        string type = "?";
        long amount = 0;
        for (int i = 0; i < e.Payload.Count; i++)
        {
            string name = e.PayloadNames![i];
            if (name == "TypeName") type = e.Payload[i] as string ?? "?";
            else if (name == "AllocationAmount64") amount = Convert.ToInt64(e.Payload[i], CultureInfo.InvariantCulture);
            else if (name == "AllocationAmount" && amount == 0) amount = Convert.ToInt64(e.Payload[i], CultureInfo.InvariantCulture);
        }
        lock (_lock) _bytesByType[type] = _bytesByType.GetValueOrDefault(type) + amount;
    }

    /// <summary>Clears the samples (called when a benchmark starts).</summary>
    public void Reset()
    {
        lock (_lock) _bytesByType.Clear();
    }

    /// <summary>Prints the top types sampled since <see cref="Reset"/>.</summary>
    public void Print(string benchmark)
    {
        KeyValuePair<string, long>[] rows;
        lock (_lock) rows = [.. _bytesByType];
        Array.Sort(rows, (a, b) => b.Value.CompareTo(a.Value));
        for (int i = 0; i < rows.Length && i < TopN; i++)
            Console.WriteLine(string.Format(CultureInfo.InvariantCulture, "ALLOC,{0},{1},{2}", benchmark, rows[i].Key, rows[i].Value));
    }
}
