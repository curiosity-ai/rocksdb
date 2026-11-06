using System.Globalization;
using RocksDb.Managed.Compression;
using RocksDb.Managed.RoundTrip.Engines;
using RocksDb.Managed.RoundTrip.Harness;
using RocksDb.Managed.RoundTrip.Protocol;

namespace RocksDb.Managed.RoundTrip.Tests;

/// <summary>
/// Unclean shutdown under varied WAL and write-path settings, crossed pairwise: the writer engine (child process) runs
/// <see cref="CrashWorkload"/> and is SIGKILLed after a random number of acknowledged batches. The crash image is then
/// recovered twice, by the other engine in place and by the writer's engine on a copy of the image, and both
/// recoveries must agree exactly: the same open outcome (status) or the same recovered batch count, the same prepared
/// 2PC transactions and the same contents of every column family (the image is not deterministic, so native recovering
/// the same image is the oracle for the managed recovery and vice versa). Each recovery also checks the batch-prefix
/// invariant and that every acknowledged batch survived. Afterwards the other engine resolves the recovered prepared
/// transactions (commits the "p" ones, rolls back the "t" ones) and the writer's engine re-checks the result.
/// </summary>
[TestClass]
public sealed class CrashMatrixRoundTripTests : RoundTripTestBase
{
    public const int ArraySeed = 20261107;

    public const int LongRandomRows = 60;

    private enum F { Writer, RecoveryMode, Recycle, ManualWalFlush, WalCompression, NoWal, AtomicFlush, WriteMode, AvoidFlushRecovery, TrackWals, WalDir, WriteBuffer, TwoPc, SyncEvery }

    private static readonly string[][] s_levelNames =
    [
        ["native-writer", "managed-writer"],
        ["tolerate-tail", "absolute", "point-in-time", "skip-any"],
        ["norecycle", "recycle2"],
        ["auto-wal-flush", "manual-wal-flush"],
        ["wal-plain", "wal-zstd"],
        ["wal-only", "nowal-mix"],
        ["noatomic", "atomic-flush"],
        ["write-default", "pipelined", "unordered", "two-queues"],
        ["flush-on-recovery", "avoid-flush-recovery"],
        ["notrack", "track-wals", "track-wals-in-manifest"],
        ["waldir-db", "waldir-separate"],
        ["wb128K", "wb64M"],
        ["plain-writes", "2pc-txndb"],
        ["sync1", "sync7"],
    ];

    private static readonly int[] s_levels = Array.ConvertAll(s_levelNames, l => l.Length);

    /// <summary>disableWAL with WAL recycling (InvalidArgument), atomic_flush with pipelined writes (InvalidArgument),
    /// a WriteCommitted TransactionDB with unordered_write (InvalidArgument).</summary>
    private static bool Excluded(int a, int x, int b, int y)
    {
        if (a > b) return Excluded(b, y, a, x);
        var fa = (F)a;
        var fb = (F)b;
        if (fa == F.Recycle && fb == F.NoWal) return x == 1 && y == 1;
        if (fa == F.AtomicFlush && fb == F.WriteMode) return x == 1 && y == 1;
        if (fa == F.WriteMode && fb == F.TwoPc) return x == 2 && y == 1;
        // Native self-deadlock (see MatrixRoundTripTests.Excluded).
        if (fa == F.ManualWalFlush && fb == F.WriteMode) return x == 1 && y == 2;
        return false;
    }

    private static void Repair(int[] r)
    {
        if (Excluded((int)F.Recycle, r[(int)F.Recycle], (int)F.NoWal, r[(int)F.NoWal])) r[(int)F.NoWal] = 0;
        if (Excluded((int)F.AtomicFlush, r[(int)F.AtomicFlush], (int)F.WriteMode, r[(int)F.WriteMode])) r[(int)F.AtomicFlush] = 0;
        if (Excluded((int)F.WriteMode, r[(int)F.WriteMode], (int)F.TwoPc, r[(int)F.TwoPc])) r[(int)F.WriteMode] = 0;
        if (Excluded((int)F.ManualWalFlush, r[(int)F.ManualWalFlush], (int)F.WriteMode, r[(int)F.WriteMode])) r[(int)F.WriteMode] = 0;
    }

    private static List<int[]>? s_rows;

    internal static List<int[]> Rows()
    {
        if (s_rows is not null) return s_rows;
        List<int[]> rows = Pairwise.Generate(s_levels, ArraySeed, Excluded, Repair);
        if (LongRuns)
        {
            var rnd = new Random(ArraySeed + 1);
            for (int i = 0; i < LongRandomRows; i++)
            {
                var row = new int[s_levels.Length];
                for (int j = 0; j < row.Length; j++) row[j] = rnd.Next(s_levels[j]);
                Repair(row);
                rows.Add(row);
            }
        }
        return s_rows = rows;
    }

    private static string Describe(int[] row)
    {
        var parts = new string[row.Length];
        for (int i = 0; i < row.Length; i++) parts[i] = s_levelNames[i][row[i]];
        return string.Join(",", parts);
    }

    public static IEnumerable<object[]> Cases
    {
        get
        {
            List<int[]> rows = Rows();
            for (int i = 0; i < rows.Count; i++) yield return [i, Describe(rows[i])];
        }
    }

    public static string CaseName(System.Reflection.MethodInfo method, object?[] data) =>
        $"{method.Name}({((int)data[0]!).ToString("D2", CultureInfo.InvariantCulture)}: {data[1]})";

    private static EngineKind Other(EngineKind e) => e == EngineKind.Native ? EngineKind.Managed : EngineKind.Native;

    [TestMethod]
    public void CrashCoveringArrayCoversEveryAllowedPair()
    {
        List<int[]> rows = Pairwise.Generate(s_levels, ArraySeed, Excluded, Repair);
        List<string> missing = Pairwise.Uncovered(s_levels, rows, Excluded);
        Assert.AreEqual(0, missing.Count, string.Join("; ", missing.Take(20)));
        TestContext.WriteLine($"{rows.Count} pairwise rows over {s_levels.Length} factors");
    }

    private sealed record Recovery(string? OpenFailure, long Recovered, string Prepared, Contents? Dump);

    [TestMethod]
    [DynamicData(nameof(Cases), DynamicDataDisplayName = nameof(CaseName))]
    public async Task CrashMatrix(int index, string description)
    {
        int[] r = Rows()[index];
        Assert.AreEqual(Describe(r), description, "covering array changed under the test");
        EngineKind writer = r[(int)F.Writer] == 0 ? EngineKind.Native : EngineKind.Managed;
        DbSpec spec = CrashWorkload.Spec(r[(int)F.WriteBuffer] == 0 ? 128UL << 10 : 64UL << 20);
        DbWideSpec d = spec.Db;
        d.WalRecoveryMode = (WalRecoveryMode)r[(int)F.RecoveryMode];
        d.RecycleLogFileNum = r[(int)F.Recycle] == 1 ? 2UL : 0;
        d.ManualWalFlush = r[(int)F.ManualWalFlush] == 1;
        d.WalCompression = r[(int)F.WalCompression] == 1 ? CompressionType.Zstd : CompressionType.None;
        d.AtomicFlush = r[(int)F.AtomicFlush] == 1;
        d.EnablePipelinedWrite = r[(int)F.WriteMode] == 1;
        d.UnorderedWrite = r[(int)F.WriteMode] == 2;
        d.TwoWriteQueues = r[(int)F.WriteMode] == 3;
        d.AvoidFlushDuringRecovery = r[(int)F.AvoidFlushRecovery] == 1;
        d.TrackAndVerifyWals = r[(int)F.TrackWals] == 1;
        d.TrackAndVerifyWalsInManifest = r[(int)F.TrackWals] == 2;
        d.SeparateWalDir = r[(int)F.WalDir] == 1;
        spec.TransactionDb = r[(int)F.TwoPc] == 1;
        string flags = (r[(int)F.NoWal] == 1 ? "n" : "") + (spec.TransactionDb ? "t" : "") + (d.ManualWalFlush ? "w" : "");
        int syncEvery = r[(int)F.SyncEvery] == 0 ? 1 : 7;

        RoundTripHarness h = CreateHarness(spec, "c" + index.ToString("D2", CultureInfo.InvariantCulture), ArraySeed + 7919 * (index + 1));
        h.Model.LenientTransactions = true;
        h.Log("case: " + description);
        long target = 100 + h.Random.Next(300);
        long acked = 0;
        ScriptBuilder w = h.NewScriptWithoutVerify($"{writer}-writer", h.DbPath, spec, h.Model).Add(Op.CrashWriter(100_000_000, syncEvery, flags));
        ChildResult cr = await h.RunChildAsync(writer, w, TimeSpan.FromSeconds(150), line =>
        {
            if (!line.StartsWith("ACK ", StringComparison.Ordinal)) return false;
            acked = long.Parse(line.AsSpan(4), CultureInfo.InvariantCulture);
            return acked >= target;
        });
        if (!cr.KilledByParent) h.Fail($"the {writer} writer was expected to be killed after {target} acknowledged batches:\n{cr.Describe()}");
        h.Log($"{writer} writer killed after {acked} acknowledged batches");

        string copy = h.SubDir("copy-db");
        CopyImage(h.DbPath, copy, d.SeparateWalDir);

        EngineKind recoverer = Other(writer);
        DbSpec recSpec = spec.Clone();
        recSpec.ReportOpenFailure = true;
        Recovery a = await Recover(h, recoverer, h.DbPath, recSpec, acked, "main");
        Recovery b = await Recover(h, writer, copy, recSpec, acked, "copy");
        string what = $"{recoverer} recovering the crash image vs {writer} recovering a copy of it";
        if (a.OpenFailure != b.OpenFailure) h.Fail($"{what}: open outcome differs: {a.OpenFailure ?? "opened"} vs {b.OpenFailure ?? "opened"}");
        if (a.OpenFailure is not null)
        {
            h.Log("both engines refuse to open the image: " + a.OpenFailure);
            return;
        }
        if (a.Recovered != b.Recovered) h.Fail($"{what}: {a.Recovered} vs {b.Recovered} batches recovered");
        if (a.Prepared != b.Prepared) h.Fail($"{what}: prepared transactions {a.Prepared} vs {b.Prepared}");
        List<string> diff = Contents.Diff(b.Dump!, a.Dump!, 30);
        if (diff.Count > 0) h.Fail($"{what}: recovered contents differ ({recoverer} vs expected = {writer}'s):\n  " + string.Join("\n  ", diff));
        h.Log($"both recovered {a.Recovered} batches (>= {acked} acknowledged), prepared [{a.Prepared}]");

        // Resolve the recovered prepared transactions with the recoverer; the writer's engine re-checks.
        ScriptBuilder res = h.NewScriptWithoutVerify($"{recoverer}-resolve", h.DbPath, spec, h.Model);
        var names = new List<string>(a.Prepared.Split('|', StringSplitOptions.RemoveEmptyEntries));
        foreach (string n in names) res.Add(n[0] == 'p' ? Op.TxnCommit(n) : Op.TxnRollback(n));
        if (spec.TransactionDb) res.Add(Op.TxnCheckPrepared([]));
        res.Add(Op.CrashCheck(acked));
        await h.RunAsync(recoverer, res);
        ScriptBuilder again = h.NewScriptWithoutVerify($"{writer}-recheck", h.DbPath, spec, h.Model);
        if (spec.TransactionDb) again.Add(Op.TxnCheckPrepared([]));
        again.Add(Op.CrashCheck(acked));
        await h.RunAsync(writer, again);
        long n2 = RecoveredOf(h);
        if (n2 != a.Recovered) h.Fail($"{writer} sees {n2} batches after {recoverer} recovered {a.Recovered} and resolved the transactions");
    }

    private static long RecoveredOf(RoundTripHarness h)
    {
        foreach (string line in h.LastOutput)
        {
            if (line.StartsWith("RECOVERED ", StringComparison.Ordinal)) return long.Parse(line.AsSpan(10), CultureInfo.InvariantCulture);
        }
        h.Fail("no RECOVERED line: " + string.Join(" | ", h.LastOutput));
        return -1;
    }

    private static async Task<Recovery> Recover(RoundTripHarness h, EngineKind engine, string db, DbSpec spec, long acked, string tag)
    {
        // Outside work/: the harness deletes the dump files of successful runs there.
        string dump = Path.Combine(h.Root, $"recovery-{tag}-{engine}.txt");
        ScriptBuilder s = h.NewScriptWithoutVerify($"{engine}-recover-{tag}", db, spec, h.Model);
        s.Add(Op.CrashCheck(acked)).Add(Op.Dump(dump));
        await h.RunAsync(engine, s);
        string? fail = null;
        long recovered = -1;
        string prepared = "";
        foreach (string line in h.LastOutput)
        {
            // "OPENFAIL <status> <message>": compare the status only (messages name files and offsets).
            if (line.StartsWith("OPENFAIL ", StringComparison.Ordinal)) fail = line.Split(' ')[1];
            else if (line.StartsWith("RECOVERED ", StringComparison.Ordinal)) recovered = long.Parse(line.AsSpan(10), CultureInfo.InvariantCulture);
            else if (line.StartsWith("PREPARED ", StringComparison.Ordinal)) prepared = line[9..];
        }
        h.Log($"{engine} on {tag}: " + (fail ?? $"recovered {recovered}, prepared [{prepared}]"));
        if (fail is null && recovered < acked) h.Fail($"{engine} on {tag}: no consistent recovery reported: {string.Join(" | ", h.LastOutput)}");
        return new Recovery(fail, recovered, prepared, fail is null ? Contents.Read(dump) : null);
    }

    private static void CopyImage(string db, string copy, bool walDir)
    {
        CopyDir(db, copy);
        if (walDir) CopyDir(DbWideSpec.WalDirFor(db), DbWideSpec.WalDirFor(copy));
    }

    private static void CopyDir(string from, string to)
    {
        Directory.CreateDirectory(to);
        foreach (string f in Directory.GetFiles(from))
        {
            if (Path.GetFileName(f) == "LOCK") continue;
            File.Copy(f, Path.Combine(to, Path.GetFileName(f)), overwrite: true);
        }
        foreach (string sub in Directory.GetDirectories(from)) CopyDir(sub, Path.Combine(to, Path.GetFileName(sub)));
    }
}
