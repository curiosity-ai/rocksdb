using RocksDb.Managed.Env;
using RocksDb.Managed.Tests.Db;
using RocksDb.Managed.TestShared;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #15: Mosaik's whole concurrent mix at once (<see cref="MosaikSoak"/>) on <see cref="MosaikProfile"/>, in both of
/// Mosaik's resource branches. AUDIT15_SOAK_SECONDS lengthens a run, AUDIT15_SOAK_SEED replays one (the seed fixes every
/// worker's choices; thread interleavings still vary).
/// </summary>
[TestClass]
[DoNotParallelize]
public sealed class Audit15MosaikSoakTests
{
    /// <summary>
    /// Low-memory mode: Mosaik's exact 8 MiB cache and 32 MiB write buffer manager. Normal mode: a smaller cache capacity
    /// than Mosaik's 256 MiB per processor (only the capacity differs) and the write buffer manager of a host with about
    /// 10 GiB available (available / 20); the compat soak runs the normal branch at Mosaik's 64 MiB floor instead.
    /// </summary>
    private static SoakSettings Settings(string root, bool lowMemoryMode, int processorCount, long writeBufferBytes, int seed, TimeSpan duration) => new()
    {
        Root = root,
        LowMemoryMode = lowMemoryMode,
        ProcessorCount = processorCount,
        CacheBytes = lowMemoryMode ? 0 : 64L << 20,
        WriteBufferBytes = writeBufferBytes,
        Seed = seed,
        Duration = duration,
    };

    /// <summary>
    /// The concurrent run on the local file system: no exception, no hang, every online check holds; then the database
    /// reopened holds exactly the model's state, and every checkpoint and kept backup is one point in time.
    /// </summary>
    [TestMethod]
    [DataRow(true, 2, 0L)]
    [DataRow(false, 8, 512L << 20)]
    public void ConcurrentMosaikMixLosesNothingAndSnapshotsArePointsInTime(bool lowMemoryMode, int processorCount, long writeBufferBytes)
    {
        int seed = SoakSettings.SeedFromEnvironment();
        string root = Path.Combine(Path.GetTempPath(), "rocksdb-managed-a15soak-" + Guid.NewGuid().ToString("N"));
        try
        {
            var soak = new MosaikSoak(Settings(root, lowMemoryMode, processorCount, writeBufferBytes, seed, SoakSettings.DurationFromEnvironment(20)));
            soak.Run();
            Console.WriteLine(soak.Summary());
            string failures = SoakChecks.RunFailures(soak);
            Assert.AreEqual("", failures, soak.Settings + Environment.NewLine + failures);

            var errors = new List<string>();
            var notes = new List<string>();
            SoakChecks.ReopenAndCheck(soak, errors, notes);
            SoakChecks.CheckSnapshots(soak, errors, notes, alsoCheck: null);
            foreach (string n in notes) Console.WriteLine(n);
            Assert.AreEqual(0, errors.Count, soak.Settings + Environment.NewLine + string.Join(Environment.NewLine, errors));
        }
        finally
        {
            try { Directory.Delete(root, recursive: true); } catch (IOException) { } catch (UnauthorizedAccessException) { }
        }
    }

    /// <summary>
    /// The checks after a run are not vacuous: a short run's reopened database passes them, and copies of its dump with one
    /// injected defect each - a node batch torn across families, a torn value, a stale value, a key nobody wrote - fail the
    /// final-state check, the point-in-time check and the power-loss check as each applies.
    /// </summary>
    [TestMethod]
    public void SoakChecksReportInjectedDefects()
    {
        int seed = SoakSettings.SeedFromEnvironment();
        string root = Path.Combine(Path.GetTempPath(), "rocksdb-managed-a15checks-" + Guid.NewGuid().ToString("N"));
        try
        {
            var soak = new MosaikSoak(Settings(root, lowMemoryMode: true, processorCount: 2, writeBufferBytes: 0, seed, TimeSpan.FromSeconds(3)) with { Checkpoints = 0, Backups = 0 });
            soak.Run();
            string failures = SoakChecks.RunFailures(soak);
            Assert.AreEqual("", failures, soak.Settings + Environment.NewLine + failures);
            SoakDump dump;
            using (RocksDatabase db = SoakChecks.Profile(soak, soak.WalDir).Open(soak.DbPath)) dump = SoakDump.Of(db);
            SoakModel m = soak.M;
            var maxOps = new int[m.Writers.Count];
            for (int i = 0; i < maxOps.Length; i++) maxOps[i] = m.Writers[i].OpCount;
            var final = new SoakSnapshotRecord("final", soak.DbPath, 0, maxOps, m.TrackerIssued, m.TrackerRemoving, 0);
            var horizon = new long[SoakModel.FamilyCount];

            var errors = new List<string>();
            SoakVerifier.CheckFinal(m, dump, "intact", errors, out _);
            SoakVerifier.CheckCut(m, dump, final, "intact", errors);
            SoakVerifier.CheckPowerLoss(m, dump, horizon, "intact", errors);
            Assert.AreEqual(0, errors.Count, string.Join(Environment.NewLine, errors));

            // A node whose last write was the SetNode batch (nodes, types and nodeversion from one operation).
            NodeWriter nodes = soak.Workers.OfType<NodeWriter>().First();
            int n = Enumerable.Range(0, NodeWriter.Nodes).First(i =>
                m.LastEvent(nodes.NodesSlot[i]) is { Delete: false } a && m.LastEvent(nodes.TypesSlot[i]) is { Delete: false } b && a.Op == b.Op);
            SoakDump Mutated(int family, Func<List<(byte[] Key, byte[] Value)>, List<(byte[] Key, byte[] Value)>> change)
            {
                var d = new SoakDump();
                for (int f = 0; f < SoakModel.FamilyCount; f++) d.Families[f].AddRange(f == family ? change([.. dump.Families[f]]) : dump.Families[f]);
                return d;
            }
            byte[] typesKey = m.Key[nodes.TypesSlot[n]];
            SoakDump torn = Mutated(MosaikSoak.FTypes, l => l.Where(e => !e.Key.AsSpan().SequenceEqual(typesKey)).ToList());
            AssertReported(m, torn, final, horizon, cut: true, powerLoss: true, "torn batch");

            byte[] nodeKey = m.Key[nodes.NodesSlot[n]];
            SoakDump flipped = Mutated(MosaikSoak.FNodes, l => l.Select(e =>
            {
                if (!e.Key.AsSpan().SequenceEqual(nodeKey)) return e;
                byte[] v = [.. e.Value];
                v[^1] ^= 0x40;
                return (e.Key, v);
            }).ToList());
            AssertReported(m, flipped, final, horizon, cut: true, powerLoss: true, "torn value");

            // A slot put at least twice: its older value in place of the last one.
            int stale = Enumerable.Range(0, m.SlotCount).First(s => m.Kind[s] == SlotKind.Codec && !m.Relaxed[m.Family[s]] && m.LastEvent(s) is { Delete: false } &&
                m.History[s]!.Count(e => !e.Delete) >= 2);
            SlotEvent older = m.History[stale]!.Last(e => !e.Delete && e.Version != m.LastEvent(stale)!.Value.Version);
            SoakDump reverted = Mutated(m.Family[stale], l => l.Select(e => e.Key.AsSpan().SequenceEqual(m.Key[stale]) ? (e.Key, m.ValueOf(stale, older)) : e).ToList());
            AssertReported(m, reverted, final, horizon, cut: false, powerLoss: false, "stale value");

            SoakDump foreign = Mutated(MosaikSoak.FNodes, l => [.. l, (Enumerable.Repeat((byte)0xEE, 16).ToArray(), new byte[30])]);
            AssertReported(m, foreign, final, horizon, cut: true, powerLoss: true, "unknown key");
        }
        finally
        {
            try { Directory.Delete(root, recursive: true); } catch (IOException) { } catch (UnauthorizedAccessException) { }
        }
    }

    private static void AssertReported(SoakModel m, SoakDump d, SoakSnapshotRecord final, long[] horizon, bool cut, bool powerLoss, string what)
    {
        var e = new List<string>();
        SoakVerifier.CheckFinal(m, d, what, e, out _);
        Assert.AreNotEqual(0, e.Count, "the final-state check missed: " + what);
        if (cut)
        {
            e.Clear();
            SoakVerifier.CheckCut(m, d, final, what, e);
            Assert.AreNotEqual(0, e.Count, "the point-in-time check missed: " + what);
        }
        if (powerLoss)
        {
            e.Clear();
            SoakVerifier.CheckPowerLoss(m, d, horizon, what, e);
            Assert.AreNotEqual(0, e.Count, "the power-loss check missed: " + what);
        }
    }

    /// <summary>
    /// The same mix on a fault-injecting in-memory file system that stops at a random point of the run (every later
    /// mutation fails), followed by a power loss (unsynced data and directory entries dropped). Reopened with Mosaik's
    /// kSkipAnyCorruptedRecords, only what upstream guarantees for unsynced writes is required: the open succeeds, no value
    /// that was never written appears, batches stay whole across families, and writes acknowledged before a completed flush
    /// of their family (Flush, CompactRange(null, null), the checkpoint's flush) survive. Whether every writer's surviving
    /// writes form a prefix of its operations is printed, not asserted (see <see cref="SoakVerifier.CheckPowerLoss"/>).
    /// </summary>
    [TestMethod]
    [DataRow(true, 2, 0L)]
    [DataRow(false, 8, 0L)]
    public void PowerLossDuringTheMosaikMixKeepsBatchesWholeAndFlushedWritesDurable(bool lowMemoryMode, int processorCount, long writeBufferBytes)
    {
        int seed = SoakSettings.SeedFromEnvironment();
        var rng = new Random(seed);
        TimeSpan duration = SoakSettings.DurationFromEnvironment(8);
        var fs = new FaultInjectionFileSystem(new MemoryFileSystem());
        SoakSettings settings = Settings("/audit15-soak", lowMemoryMode, processorCount, writeBufferBytes, seed, duration) with
        {
            FileSystem = fs,
            ValueScale = 0.125,
            Checkpoints = 1,
            Backups = 0,
            CrashAt = duration * (0.2 + 0.7 * rng.NextDouble()),
        };
        var soak = new MosaikSoak(settings) { SoakCrash = () => fs.CrashAtOperation = fs.Operations + 1 };
        soak.Run();
        Console.WriteLine(soak.Summary());
        Console.WriteLine("crashed at: " + fs.CrashedAt + "; close: " + (soak.CloseError?.GetType().Name ?? "ok"));
        string failures = SoakChecks.RunFailures(soak);
        Assert.AreEqual("", failures, soak.Settings + Environment.NewLine + failures);

        fs.PowerLoss();
        fs.Restart();
        var errors = new List<string>();
        string report;
        using (RocksDatabase db = SoakChecks.Profile(soak, soak.WalDir).Open(soak.DbPath))
        {
            report = SoakVerifier.CheckPowerLoss(soak.M, SoakDump.Of(db), soak.Horizon, "after the power loss", errors);
        }
        Console.WriteLine(report);
        Assert.AreEqual(0, errors.Count, soak.Settings + Environment.NewLine + string.Join(Environment.NewLine, errors));
    }
}
