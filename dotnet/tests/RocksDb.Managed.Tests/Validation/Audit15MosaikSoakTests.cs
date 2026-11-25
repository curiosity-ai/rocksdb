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
