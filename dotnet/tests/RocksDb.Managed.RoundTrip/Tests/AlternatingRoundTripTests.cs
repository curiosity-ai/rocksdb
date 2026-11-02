using RocksDb.Managed.RoundTrip.Engines;
using RocksDb.Managed.RoundTrip.Harness;
using RocksDb.Managed.RoundTrip.Protocol;

namespace RocksDb.Managed.RoundTrip.Tests;

/// <summary>
/// Randomized alternation: native writes, managed reads + writes, native reads + writes, ... Every run first verifies
/// the contents the other engine left behind (full scans, point gets of live and deleted keys) and ends with a verify
/// and a dump the parent diffs against the reference model. The op streams cover all CFs (different compression,
/// format_version, filters, block sizes and merge operators), merges, range deletes, single deletes, write batches,
/// snapshots, flushes, CF create/drop, empty values, binary keys and large values.
/// </summary>
[TestClass]
public sealed class AlternatingRoundTripTests : RoundTripTestBase
{
    public static IEnumerable<object[]> Variants =>
    [
        // compression, format_version, filter, write buffer, disable auto compactions, WAL only (no flush ops)
        ["none", 5, "none", 64UL << 20, false, false],
        ["snappy", 6, "bloom", 64UL << 10, false, false],
        ["lz4", 7, "ribbon", 64UL << 10, true, false],
        ["zstd", 6, "bloom", 64UL << 20, false, false],
        ["zstd", 7, "none", 128UL << 10, false, false],
        ["lz4", 5, "ribbon", 64UL << 20, false, true],
    ];

    public static string VariantName(System.Reflection.MethodInfo method, object?[] data) =>
        $"{method.Name}({data[0]},fv{data[1]},{data[2]},wb{(ulong)data[3]! >> 10}K,{((bool)data[4]! ? "noauto" : "auto")}{((bool)data[5]! ? ",wal-only" : "")})";

    private async Task RunAlternations(string compression, int formatVersion, string filter, ulong writeBuffer, bool noAuto, bool walOnly,
        EngineKind first, int alternations, int opsPerRun)
    {
        DbSpec spec = DbSpec.Standard(compression, formatVersion, filter, writeBuffer, noAuto);
        RoundTripHarness h = CreateHarness(spec, $"{compression}-fv{formatVersion}-{filter}-{writeBuffer >> 10}K{(noAuto ? "-noauto" : "")}{(walOnly ? "-wal" : "")}");
        EngineKind engine = first;
        for (int i = 0; i < alternations; i++)
        {
            ScriptBuilder b = h.NewScript($"{engine}-{i}");
            OpGenerator.Generate(b, new GeneratorOptions
            {
                Ops = opsPerRun,
                AllowFlush = !walOnly,
                // Full-range CompactRange by both engines: the other engine then reads (and compacts) bottom-level
                // files the first one wrote.
                AllowCompact = !walOnly,
            });
            if (i % 3 == 2) b.Add(Op.Reopen()); // recovery from WAL in the middle of a run
            b.Verify().Dump();
            await h.RunAsync(engine, b);
            engine = engine == EngineKind.Native ? EngineKind.Managed : EngineKind.Native;
        }
        // Final cross-check: both engines verify the final state.
        await h.RunAsync(engine, h.NewScript($"{engine}-final"));
        engine = engine == EngineKind.Native ? EngineKind.Managed : EngineKind.Native;
        await h.RunAsync(engine, h.NewScript($"{engine}-final"));
    }

    [TestMethod]
    [DynamicData(nameof(Variants), DynamicDataDisplayName = nameof(VariantName))]
    public Task NativeFirst_Alternating(string compression, int formatVersion, string filter, ulong writeBuffer, bool noAuto, bool walOnly) =>
        RunAlternations(compression, formatVersion, filter, writeBuffer, noAuto, walOnly, EngineKind.Native, 6, 1200);

    /// <summary>The managed port creates the DB from scratch; native reads and writes it; managed reads it back.</summary>
    [TestMethod]
    [DataRow("lz4", 7, "bloom", 64UL << 20, false)]
    [DataRow("zstd", 5, "ribbon", 96UL << 10, false)]
    [DataRow("none", 6, "none", 64UL << 10, true)]
    public Task ManagedCreates_NativeExtends_ManagedReads(string compression, int formatVersion, string filter, ulong writeBuffer, bool noAuto) =>
        RunAlternations(compression, formatVersion, filter, writeBuffer, noAuto, false, EngineKind.Managed, 3, 1500);

    [TestMethod]
    [TestCategory("Long")]
    public async Task Long_ManyAlternations()
    {
        if (!LongRuns) Assert.Inconclusive("set ROUNDTRIP_LONG=1 to run the long randomized round trips");
        foreach (object[] v in Variants)
        {
            await RunAlternations((string)v[0], (int)v[1], (string)v[2], (ulong)v[3], (bool)v[4], (bool)v[5], EngineKind.Native, 20, 6000);
            ReleaseHarnesses(); // this variant passed
        }
    }
}
