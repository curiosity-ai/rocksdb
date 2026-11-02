using RocksDb.Managed.Compression;
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
        [CompressionType.None, 5, FilterKind.None, 64UL << 20, false, false],
        [CompressionType.Snappy, 6, FilterKind.Bloom, 64UL << 10, false, false],
        [CompressionType.LZ4, 7, FilterKind.Ribbon, 64UL << 10, true, false],
        [CompressionType.Zstd, 6, FilterKind.Bloom, 64UL << 20, false, false],
        [CompressionType.Zstd, 7, FilterKind.None, 128UL << 10, false, false],
        [CompressionType.LZ4, 5, FilterKind.Ribbon, 64UL << 20, false, true],
    ];

    public static string VariantName(System.Reflection.MethodInfo method, object?[] data) =>
        $"{method.Name}({data[0]},fv{data[1]},{data[2]},wb{(ulong)data[3]! >> 10}K,{((bool)data[4]! ? "noauto" : "auto")}{((bool)data[5]! ? ",wal-only" : "")})";

    private async Task RunAlternations(CompressionType compression, int formatVersion, FilterKind filter, ulong writeBuffer, bool noAuto, bool walOnly,
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
    public Task NativeFirst_Alternating(CompressionType compression, int formatVersion, FilterKind filter, ulong writeBuffer, bool noAuto, bool walOnly) =>
        RunAlternations(compression, formatVersion, filter, writeBuffer, noAuto, walOnly, EngineKind.Native, 6, 1200);

    /// <summary>The managed port creates the DB from scratch; native reads and writes it; managed reads it back.</summary>
    [TestMethod]
    [DataRow(CompressionType.LZ4, 7, FilterKind.Bloom, 64UL << 20, false)]
    [DataRow(CompressionType.Zstd, 5, FilterKind.Ribbon, 96UL << 10, false)]
    [DataRow(CompressionType.None, 6, FilterKind.None, 64UL << 10, true)]
    public Task ManagedCreates_NativeExtends_ManagedReads(CompressionType compression, int formatVersion, FilterKind filter, ulong writeBuffer, bool noAuto) =>
        RunAlternations(compression, formatVersion, filter, writeBuffer, noAuto, false, EngineKind.Managed, 3, 1500);

    [TestMethod]
    [TestCategory("Long")]
    public async Task Long_ManyAlternations()
    {
        if (!LongRuns) Assert.Inconclusive("set ROUNDTRIP_LONG=1 to run the long randomized round trips");
        foreach (object[] v in Variants)
        {
            await RunAlternations((CompressionType)v[0], (int)v[1], (FilterKind)v[2], (ulong)v[3], (bool)v[4], (bool)v[5], EngineKind.Native, 20, 6000);
            ReleaseHarnesses(); // this variant passed
        }
    }
}
