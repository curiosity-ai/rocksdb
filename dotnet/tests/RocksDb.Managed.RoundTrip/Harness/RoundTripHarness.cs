using System.Globalization;
using System.Text;
using RocksDb.Managed.RoundTrip.Engines;
using RocksDb.Managed.RoundTrip.Protocol;

namespace RocksDb.Managed.RoundTrip.Harness;

/// <summary>
/// Builds an op script for one engine run while applying every op to the reference model, so expected-contents
/// files for verify/snapshot-verify/dump commands can be written at the right points.
/// </summary>
public sealed class ScriptBuilder
{
    private readonly RoundTripHarness _harness;
    private int _files;

    internal ScriptBuilder(RoundTripHarness harness, string tag, string dbPath, DbSpec spec, ReferenceModel model)
    {
        _harness = harness;
        Tag = tag;
        DbPath = dbPath;
        Spec = spec;
        Model = model;
    }

    public string Tag { get; }
    public string DbPath { get; }
    public DbSpec Spec { get; }
    public ReferenceModel Model { get; }
    public List<Op> Ops { get; } = [];
    public List<(string Path, Contents Expected)> Dumps { get; } = [];
    public Random Random => _harness.Random;

    public string NewFile(string what) => _harness.WorkFile($"{Tag}-{++_files:D3}-{what}");

    private readonly Dictionary<long, HashSet<string>> _cfsChangedSinceSnapshot = new();

    public ScriptBuilder Add(Op op)
    {
        Ops.Add(op);
        Model.Apply(op);
        switch (op.Kind)
        {
            case OpKind.Snap: _cfsChangedSinceSnapshot[op.N] = new HashSet<string>(StringComparer.Ordinal); break;
            case OpKind.SnapRelease: _cfsChangedSinceSnapshot.Remove(op.N); break;
            case OpKind.CreateCf:
            case OpKind.DropCf:
                foreach (HashSet<string> changed in _cfsChangedSinceSnapshot.Values) changed.Add(op.Cf!);
                break;
        }
        return this;
    }

    public ScriptBuilder Batch(bool sync, IEnumerable<Op> ops)
    {
        Add(Op.BatchBegin(sync));
        foreach (Op op in ops) Add(op);
        return Add(Op.BatchEnd());
    }

    /// <summary>Verify the live DB against the model's current contents (full scans + point gets).</summary>
    public ScriptBuilder Verify()
    {
        string path = NewFile("expected.txt");
        Model.Current.Write(path);
        return Add(Op.Verify(path));
    }

    /// <summary>Verify reads at snapshot <paramref name="id"/> against the model as of that snapshot.</summary>
    public ScriptBuilder SnapVerify(long id)
    {
        string path = NewFile($"snap{id}-expected.txt");
        // Only CFs that existed throughout (a CF dropped or re-created since the snapshot has no snapshot view).
        var expected = new Contents();
        foreach (KeyValuePair<string, CfContents> cf in Model.SnapshotContents(id).ColumnFamilies)
        {
            if (Model.HasCf(cf.Key) && !_cfsChangedSinceSnapshot[id].Contains(cf.Key)) expected.ColumnFamilies.Add(cf.Key, cf.Value);
        }
        expected.Write(path);
        return Add(Op.SnapVerify(id, path));
    }

    /// <summary>Dump all CFs; the parent diffs the dump with the model after the run.</summary>
    public ScriptBuilder Dump()
    {
        string path = NewFile("dump.txt");
        Dumps.Add((path, Model.Current.Clone()));
        return Add(Op.Dump(path));
    }
}

/// <summary>
/// Per-test round-trip driver: owns a temp directory (DB + work files), the seed, the reference model, and runs
/// scripts on the native engine (always in a child process) or the managed engine (in-process by default; in a
/// child when requested or when <c>ROUNDTRIP_MANAGED_IN_CHILD=1</c>).
/// </summary>
public sealed class RoundTripHarness : IDisposable
{
    private readonly TestContext _context;
    private readonly List<string> _log = [];
    private int _runs;

    public RoundTripHarness(TestContext context, string name, int seed, DbSpec spec)
    {
        _context = context;
        Name = name;
        Seed = seed;
        Random = new Random(seed);
        Spec = spec;
        Model = new ReferenceModel(spec);
        Root = Path.Combine(Path.GetTempPath(), "rocksdb-roundtrip-" + Sanitize(name) + "-" + Guid.NewGuid().ToString("N")[..8]);
        DbPath = Path.Combine(Root, "db");
        Directory.CreateDirectory(Path.Combine(Root, "work"));
        Log($"seed={seed} spec: {spec}");
        Log("root: " + Root);
    }

    public string Name { get; }
    public int Seed { get; }
    public Random Random { get; }
    public DbSpec Spec { get; }
    public ReferenceModel Model { get; }
    public string Root { get; }
    public string DbPath { get; }

    public static TimeSpan DefaultTimeout { get; } = TimeSpan.FromSeconds(180);

    public static bool ManagedInChild => Environment.GetEnvironmentVariable("ROUNDTRIP_MANAGED_IN_CHILD") == "1";

    public static string Sanitize(string s)
    {
        var sb = new StringBuilder();
        foreach (char c in s) sb.Append(char.IsLetterOrDigit(c) || c is '-' or '_' or '.' ? c : '_');
        return sb.Length > 120 ? sb.ToString(0, 120) : sb.ToString();
    }

    public string WorkFile(string name) => Path.Combine(Root, "work", name);

    public string SubDir(string name) => Path.Combine(Root, name);

    public void Log(string message)
    {
        _log.Add(message);
        _context.WriteLine(message);
    }

    /// <summary>A new script against the main DB; starts by verifying the current contents when the DB exists.</summary>
    public ScriptBuilder NewScript(string tag) => NewScript(tag, DbPath, Spec, Model);

    public ScriptBuilder NewScript(string tag, string dbPath, DbSpec spec, ReferenceModel model)
    {
        ScriptBuilder b = NewScriptWithoutVerify(tag, dbPath, spec, model);
        if (File.Exists(Path.Combine(dbPath, "CURRENT"))) b.Verify();
        return b;
    }

    /// <summary>A new script that doesn't start by verifying the DB (e.g. a crash workload checked differently).</summary>
    public ScriptBuilder NewScriptWithoutVerify(string tag, string dbPath, DbSpec spec, ReferenceModel model) =>
        new(this, $"{++_runs:D3}-{Sanitize(tag)}", dbPath, spec, model);

    /// <summary>Protocol lines ("RECOVERED n", "PROP ...", "ACK n") printed by the last run.</summary>
    public List<string> LastOutput { get; } = [];

    [System.Diagnostics.CodeAnalysis.DoesNotReturn]
    public void Fail(string message)
    {
        string full = $"[{Name}] {message}\n  seed={Seed} (rerun with ROUNDTRIP_SEED={Seed})\n  spec: {Spec}\n  work dir: {Root}";
        _log.Add("FAILED: " + full);
        throw new AssertFailedException(full);
    }

    private (string Spec, string Script) WriteInputs(ScriptBuilder b, EngineKind engine)
    {
        string spec = WorkFile($"{b.Tag}-{engine}.spec.json");
        string script = WorkFile($"{b.Tag}-{engine}.script.txt");
        File.WriteAllText(spec, b.Spec.ToJson());
        Script.Write(script, b.Ops);
        return (spec, script);
    }

    /// <summary>Runs <paramref name="b"/> on <paramref name="engine"/> and checks its dumps against the model.</summary>
    public async Task RunAsync(EngineKind engine, ScriptBuilder b)
    {
        bool abandons = b.Ops.Count > 0 && b.Ops[^1].Kind == OpKind.Abandon;
        if (engine == EngineKind.Managed && !ManagedInChild && !abandons) RunManagedInProcess(b);
        else
        {
            ChildResult r = await RunChildAsync(engine, b, DefaultTimeout, null);
            bool ok = abandons
                ? r.Stdout.Count > 0 && r.Stdout[^1] == ScriptRunner.AbandonLine && !r.TimedOut
                : r.Succeeded;
            if (!ok) Fail($"{engine} child failed running script {b.Tag} ({b.Ops.Count} ops):\n{r.Describe()}");
        }
        CheckDumps(engine, b);
        DeleteBulkFiles(b);
    }

    public async Task<ChildResult> RunChildAsync(EngineKind engine, ScriptBuilder b, TimeSpan timeout, Func<string, bool>? onStdoutLine)
    {
        (string spec, string script) = WriteInputs(b, engine);
        Log($"run {b.Tag}: {engine} (child), {b.Ops.Count} ops, db={b.DbPath}");
        ChildResult r = await ChildLauncher.RunAsync(
            ["--engine", ChildHost.EngineArgument(engine), "--db", b.DbPath, "--spec", spec, "--script", script],
            timeout, onStdoutLine);
        Log($"  -> {r.ExitDescription} in {r.Elapsed.TotalSeconds.ToString("F1", CultureInfo.InvariantCulture)}s");
        LastOutput.Clear();
        LastOutput.AddRange(r.Stdout);
        foreach (string line in r.Stdout)
        {
            if (line.StartsWith("SKIP", StringComparison.Ordinal)) Log("  " + line);
        }
        return r;
    }

    private void RunManagedInProcess(ScriptBuilder b)
    {
        WriteInputs(b, EngineKind.Managed);
        ThrowingTraceListener.Install();
        Log($"run {b.Tag}: managed (in-process), {b.Ops.Count} ops, db={b.DbPath}");
        var output = new StringWriter();
        var sw = System.Diagnostics.Stopwatch.StartNew();
        using var engine = new ManagedEngine();
        try
        {
            if (ScriptRunner.OpenOrReport(engine, b.DbPath, b.Spec, output))
            {
                var runner = new ScriptRunner(engine, b.DbPath, b.Spec, output);
                runner.Run(b.Ops);
                engine.Close();
                foreach (string s in runner.Skipped) Log("  " + s);
            }
            LastOutput.Clear();
            LastOutput.AddRange(output.ToString().Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries));
        }
        catch (VerificationException ex)
        {
            Fail($"managed engine, script {b.Tag}: {ex.Message}");
        }
        catch (Exception ex) when (ex is not AssertFailedException)
        {
            Fail($"managed engine threw running script {b.Tag}: {ex}");
        }
        Log($"  -> ok in {sw.Elapsed.TotalSeconds.ToString("F1", CultureInfo.InvariantCulture)}s");
    }

    /// <summary>
    /// After a successful run, deletes its expected-contents and dump files (full copies of the DB contents, hex
    /// encoded, which add up to gigabytes over long runs). The script and spec stay for the failure report.
    /// </summary>
    private static void DeleteBulkFiles(ScriptBuilder b)
    {
        foreach (Op op in b.Ops)
        {
            if (op.Kind is OpKind.Verify or OpKind.SnapVerify or OpKind.Dump && op.Arg is not null && op.Arg.Contains(Path.DirectorySeparatorChar + "work" + Path.DirectorySeparatorChar, StringComparison.Ordinal))
            {
                try { File.Delete(op.Arg); } catch (IOException) { }
            }
        }
    }

    private void CheckDumps(EngineKind engine, ScriptBuilder b)
    {
        foreach ((string path, Contents expected) in b.Dumps)
        {
            List<string> diffs = Contents.Diff(expected, Contents.Read(path), 40);
            if (diffs.Count > 0) Fail($"{engine} dump {Path.GetFileName(path)} differs from the model:\n  " + string.Join("\n  ", diffs));
        }
    }

    /// <summary>Keeps the whole work directory (DB, scripts, expected files, log) for inspection.</summary>
    public string Preserve(string resultsRoot, string testName)
    {
        string dest = Path.Combine(resultsRoot, "roundtrip-failures", Sanitize(testName) + "-seed" + Seed.ToString(CultureInfo.InvariantCulture));
        if (Directory.Exists(dest)) Directory.Delete(dest, recursive: true);
        CopyDirectory(Root, dest);
        File.WriteAllLines(Path.Combine(dest, "harness.log"), _log);
        return dest;
    }

    private static void CopyDirectory(string from, string to)
    {
        Directory.CreateDirectory(to);
        foreach (string f in Directory.GetFiles(from))
        {
            if (Path.GetFileName(f) == "LOCK") continue;
            File.Copy(f, Path.Combine(to, Path.GetFileName(f)), overwrite: true);
        }
        foreach (string d in Directory.GetDirectories(from)) CopyDirectory(d, Path.Combine(to, Path.GetFileName(d)));
    }

    public void Dispose()
    {
        try { Directory.Delete(Root, recursive: true); }
        catch (IOException) { }
        catch (UnauthorizedAccessException) { }
    }
}

/// <summary>Base class: creates harnesses (seeded from <c>ROUNDTRIP_SEED</c> or randomly) and preserves their
/// directories under <c>TestResults/roundtrip-failures/</c> when the test does not pass.</summary>
public abstract class RoundTripTestBase
{
    private readonly List<RoundTripHarness> _harnesses = [];

    public TestContext TestContext { get; set; } = null!;

    public static bool LongRuns => Environment.GetEnvironmentVariable("ROUNDTRIP_LONG") == "1";

    /// <summary>Deletes the directories of the harnesses created so far (for tests looping over variants: call it
    /// after a variant passed so long runs don't accumulate gigabytes of temp data).</summary>
    protected void ReleaseHarnesses()
    {
        foreach (RoundTripHarness h in _harnesses) h.Dispose();
        _harnesses.Clear();
    }

    protected RoundTripHarness CreateHarness(DbSpec spec, string variant)
    {
        string? env = Environment.GetEnvironmentVariable("ROUNDTRIP_SEED");
        int seed = env is not null ? int.Parse(env, CultureInfo.InvariantCulture) : Random.Shared.Next(1, int.MaxValue);
        var h = new RoundTripHarness(TestContext, TestContext.TestName + (variant.Length > 0 ? "-" + variant : ""), seed, spec);
        _harnesses.Add(h);
        return h;
    }

    [TestCleanup]
    public void PreserveOnFailure()
    {
        bool passed = TestContext.CurrentTestOutcome == UnitTestOutcome.Passed;
        foreach (RoundTripHarness h in _harnesses)
        {
            if (!passed)
            {
                // <test output>/TestResults/roundtrip-failures/<test>-seed<n>/ (the testing platform's default results
                // directory); ROUNDTRIP_FAILURES_DIR overrides the TestResults part.
                string results = Environment.GetEnvironmentVariable("ROUNDTRIP_FAILURES_DIR") is { Length: > 0 } dir
                    ? dir : Path.Combine(AppContext.BaseDirectory, "TestResults");
                try
                {
                    string dest = h.Preserve(results, h.Name);
                    TestContext.WriteLine($"round-trip state preserved in {dest} (seed {h.Seed})");
                }
                catch (IOException ex)
                {
                    TestContext.WriteLine("could not preserve round-trip state: " + ex.Message);
                }
            }
            h.Dispose();
        }
        _harnesses.Clear();
    }
}
