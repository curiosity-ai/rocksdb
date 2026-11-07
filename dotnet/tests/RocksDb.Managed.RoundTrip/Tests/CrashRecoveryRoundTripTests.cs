using System.Globalization;
using RocksDb.Managed.RoundTrip.Engines;
using RocksDb.Managed.RoundTrip.Harness;
using RocksDb.Managed.RoundTrip.Protocol;

namespace RocksDb.Managed.RoundTrip.Tests;

/// <summary>
/// Unclean shutdown: a child process (native or managed engine) writes multi-CF batches as fast as it can, acknowledging
/// every synced batch on stdout; the parent SIGKILLs it (TerminateProcess on Windows) once enough batches were
/// acknowledged. The other engine must then recover every acknowledged batch, and the recovered state must be a
/// prefix of the batch sequence (atomic batches, no holes). Finally the writer's engine re-checks the DB the other
/// engine recovered (and possibly flushed).
/// </summary>
[TestClass]
public sealed class CrashRecoveryRoundTripTests : RoundTripTestBase
{
    [TestMethod]
    [DataRow(EngineKind.Native, EngineKind.Managed, 128UL << 10, 7)]
    [DataRow(EngineKind.Native, EngineKind.Managed, 64UL << 20, 1)]
    [DataRow(EngineKind.Managed, EngineKind.Native, 128UL << 10, 7)]
    [DataRow(EngineKind.Managed, EngineKind.Native, 64UL << 20, 1)]
    public async Task WriterKilled_OtherEngineRecovers(EngineKind writer, EngineKind recoverer, ulong writeBuffer, int syncEvery)
    {
        DbSpec spec = CrashWorkload.Spec(writeBuffer);
        RoundTripHarness h = CreateHarness(spec, $"{writer}-killed-{writeBuffer >> 10}K-sync{syncEvery}");
        long target = 200 + h.Random.Next(600);
        long acked = 0;
        ScriptBuilder w = h.NewScriptWithoutVerify($"{writer}-writer", h.DbPath, spec, h.Model).Add(Op.CrashWriter(100_000_000, syncEvery));
        ChildResult r = await h.RunChildAsync(writer, w, TimeSpan.FromSeconds(120), line =>
        {
            if (!line.StartsWith("ACK ", StringComparison.Ordinal)) return false;
            acked = long.Parse(line.AsSpan(4), CultureInfo.InvariantCulture);
            return acked >= target;
        });
        if (!r.KilledByParent) h.Fail($"the {writer} writer was expected to be killed after {target} acknowledged batches:\n{r.Describe()}");
        h.Log($"{writer} writer killed after {acked} acknowledged batches ({r.ExitDescription})");

        long recovered = await Check(h, recoverer, acked);
        h.Log($"{recoverer} recovered {recovered} batches (>= {acked} acknowledged)");
        // Round trip: the writer's engine reads what the other engine recovered.
        long again = await Check(h, writer, acked);
        if (again != recovered) h.Fail($"{writer} sees {again} batches after {recoverer} recovered {recovered}");
    }

    private static async Task<long> Check(RoundTripHarness h, EngineKind engine, long acked)
    {
        ScriptBuilder c = h.NewScriptWithoutVerify($"{engine}-recover", h.DbPath, h.Spec, h.Model).Add(Op.CrashCheck(acked));
        await h.RunAsync(engine, c);
        foreach (string line in h.LastOutput)
        {
            if (line.StartsWith("RECOVERED ", StringComparison.Ordinal)) return long.Parse(line.AsSpan(10), CultureInfo.InvariantCulture);
        }
        h.Fail($"{engine} printed no RECOVERED line: {string.Join(" | ", h.LastOutput)}");
        return -1;
    }
}
