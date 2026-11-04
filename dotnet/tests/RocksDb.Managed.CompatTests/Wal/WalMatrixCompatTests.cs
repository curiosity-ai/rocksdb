using System.Diagnostics;
using System.Text;
using RocksDb.Managed.Db;

namespace RocksDb.Managed.CompatTests.Wal;

/// <summary>
/// The WAL settings x failure modes matrix against native RocksDB 11.12: crash images written by either engine with a
/// combination of WAL settings are damaged (<see cref="WalFault"/>), and every damaged copy is opened by both engines in
/// each wal_recovery_mode, with and without paranoid_checks (and, for the undamaged image, with a WAL filter returning
/// each WalProcessingOption). Managed and native must agree on success or failure, the status code and message (paths
/// normalized), the recovered contents of both column families and the last sequence number. Faults that leave the
/// synced data intact must keep the last acknowledged sync write.
/// <para>
/// Write-time settings (writer engine, recycle_log_file_num, track_and_verify_wals, track_and_verify_wals_in_manifest,
/// wal_compression, manual_wal_flush, wal_dir, avoid_flush_during_recovery: 8 binary factors) are covered pairwise
/// (<see cref="WalMatrixUtil.Pairwise"/>, 7 images); open-time settings and faults are crossed in full on each image.
/// <c>WAL_MATRIX_LONG=1</c> (or <c>ROUNDTRIP_LONG=1</c>) crosses all 256 write-time combinations.
/// </para>
/// </summary>
[TestClass]
[DoNotParallelize]
public sealed class WalMatrixCompatTests
{
    public TestContext TestContext { get; set; } = null!;

    private static bool Long =>
        Environment.GetEnvironmentVariable("WAL_MATRIX_LONG") == "1" || Environment.GetEnvironmentVariable("ROUNDTRIP_LONG") == "1";

    private static readonly WalRecoveryMode[] s_modes =
    [
        WalRecoveryMode.TolerateCorruptedTailRecords, WalRecoveryMode.AbsoluteConsistency,
        WalRecoveryMode.PointInTimeRecovery, WalRecoveryMode.SkipAnyCorruptedRecords,
    ];

    /// <summary>Faults after which the last acknowledged sync write must still be recovered.</summary>
    private static bool KeepsSyncedData(WalFault f) => f is WalFault.None or WalFault.DropUnsyncedTail or WalFault.TornLastRecord
        or WalFault.TruncatedLastHeader or WalFault.ZeroedTail or WalFault.EmptyNewWal or WalFault.ExtraStaleWalBelow;

    internal static List<WalWriteConfig> WriteConfigs(bool full)
    {
        var rows = new List<bool[]>();
        if (full)
        {
            for (int m = 0; m < 256; m++)
            {
                var r = new bool[8];
                for (int i = 0; i < 8; i++) r[i] = (m >> i & 1) != 0;
                rows.Add(r);
            }
        }
        else
        {
            rows = WalMatrixUtil.Pairwise(8);
        }
        var result = new List<WalWriteConfig>();
        foreach (bool[] r in rows)
        {
            result.Add(new WalWriteConfig(r[0] ? WalMatrixWriter.Native : WalMatrixWriter.Managed, r[1], r[2], r[3], r[4], r[5], r[6], r[7]));
        }
        return result;
    }

    private static List<WalOpenConfig> OpenConfigs(WalFault fault)
    {
        var r = new List<WalOpenConfig>();
        foreach (WalRecoveryMode mode in s_modes)
        {
            foreach (bool paranoid in new[] { true, false })
            {
                r.Add(new WalOpenConfig(mode, paranoid, WalMatrixFilter.None));
                if (fault != WalFault.None) continue;
                // Not WalMatrixFilterKind.Invalid: the C API wrapper (db/c.cc rocksdb_wal_filter_t::LogRecordFound) maps
                // values outside WalProcessingOption to kCorruptedRecord, so native never reaches the NotSupported
                // branch of InvokeWalFilterIfNeededOnWalRecord (checked by the unit matrix).
                foreach (WalMatrixFilterKind k in new[] { WalMatrixFilterKind.Ignore, WalMatrixFilterKind.Stop, WalMatrixFilterKind.Corrupted })
                {
                    r.Add(new WalOpenConfig(mode, paranoid, new WalMatrixFilter(k, 7)));
                }
            }
        }
        return r;
    }

    [TestMethod]
    public void PairwiseCoversEveryPair()
    {
        List<bool[]> rows = WalMatrixUtil.Pairwise(8);
        for (int a = 0; a < 8; a++)
        {
            for (int b = a + 1; b < 8; b++)
            {
                for (int x = 0; x < 2; x++)
                {
                    for (int y = 0; y < 2; y++)
                    {
                        Assert.IsTrue(rows.Exists(r => r[a] == (x == 1) && r[b] == (y == 1)), "pair " + a + "," + b + "=" + x + y);
                    }
                }
            }
        }
        Assert.IsTrue(rows.Count <= 8, "rows " + rows.Count);
    }

    [TestMethod]
    public void ManagedAndNativeAgreeOnEveryDamagedImage()
    {
        string root = Path.Combine(Path.GetTempPath(), "rocksdb-managed-walmatrix-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        var sw = Stopwatch.StartNew();
        var disagreements = new List<string>();
        var stats = new SortedDictionary<string, int>(StringComparer.Ordinal);
        int scenarios = 0, notApplicable = 0, imageIndex = 0;
        try
        {
            foreach (WalWriteConfig cfg in WriteConfigs(Long))
            {
                using WalImage image = WalMatrixEngines.WriteImage(cfg, Path.Combine(root, "img" + imageIndex), seed: 1000 + imageIndex);
                imageIndex++;
                foreach (WalFault fault in Enum.GetValues<WalFault>())
                {
                    string faultRoot = Path.Combine(root, "fault");
                    if (!image.CopyWithFault(faultRoot, fault))
                    {
                        notApplicable++;
                        Directory.Delete(faultRoot, recursive: true);
                        continue;
                    }
                    var damaged = new WalImage { Root = faultRoot, Config = cfg };
                    foreach (WalOpenConfig open in OpenConfigs(fault))
                    {
                        string scenario = cfg + " / " + fault + " / " + open;
                        using var m = new WalImage { Root = Path.Combine(root, "m"), Config = cfg };
                        using var n = new WalImage { Root = Path.Combine(root, "n"), Config = cfg };
                        WalMatrixUtil.CopyTree(damaged.Root, m.Root);
                        WalMatrixUtil.CopyTree(damaged.Root, n.Root);
                        WalOpenOutcome mo = WalMatrixEngines.OpenManaged(m, open);
                        WalOpenOutcome no = WalMatrixEngines.OpenNative(n, open);
                        scenarios++;
                        string key = fault + " " + open.Mode + (open.Paranoid ? "" : "-np") + " " + (no.Ok ? "OK" : no.Code.ToString());
                        stats[key] = stats.GetValueOrDefault(key) + 1;
                        string? diff = Compare(mo, no);
                        if (diff is not null) disagreements.Add(scenario + ": " + diff);
                        else if (mo.Ok && open.Filter.Kind == WalMatrixFilterKind.None && KeepsSyncedData(fault)
                                 && !mo.Contents.Contains(" " + image.LastSyncedKey + "=" + image.LastSyncedValue, StringComparison.Ordinal))
                        {
                            disagreements.Add(scenario + ": both engines lost the last acknowledged sync write");
                        }
                    }
                    Directory.Delete(faultRoot, recursive: true);
                }
            }
        }
        finally
        {
            try { Directory.Delete(root, recursive: true); }
            catch (IOException) { }
        }
        var sb = new StringBuilder();
        sb.Append(imageIndex).Append(" images, ").Append(scenarios).Append(" scenarios (each opened by both engines), ")
            .Append(notApplicable).Append(" fault/image pairs not applicable, ").Append(sw.Elapsed.TotalSeconds.ToString("F1")).Append(" s\n");
        foreach (KeyValuePair<string, int> kv in stats) sb.Append("  ").Append(kv.Key).Append(": ").Append(kv.Value).Append('\n');
        TestContext.WriteLine(sb.ToString());
        // WAL_MATRIX_REPORT=<file>: the outcome counts and every disagreement, for investigation.
        if (Environment.GetEnvironmentVariable("WAL_MATRIX_REPORT") is { Length: > 0 } report)
        {
            File.WriteAllText(report, sb + "\n" + string.Join("\n", disagreements) + "\n");
        }
        if (disagreements.Count > 0)
        {
            Assert.Fail(disagreements.Count + " disagreements (first 40):\n" + string.Join("\n", disagreements.GetRange(0, Math.Min(40, disagreements.Count))));
        }
    }

    private static string? Compare(WalOpenOutcome managed, WalOpenOutcome native)
    {
        if (managed.Ok != native.Ok) return "managed " + managed + " vs native " + native;
        if (!managed.Ok)
        {
            if (managed.Code != native.Code) return "code: managed " + managed + " vs native " + native;
            if (managed.Message != native.Message) return "message: managed '" + managed.Message + "' vs native '" + native.Message + "'";
            return null;
        }
        if (managed.LastSequence != native.LastSequence) return "last sequence: managed " + managed.LastSequence + " vs native " + native.LastSequence;
        if (managed.Contents != native.Contents) return "contents differ:\n managed " + Summarize(managed.Contents) + "\n native  " + Summarize(native.Contents);
        return null;
    }

    private static string Summarize(string dump)
    {
        var parts = new List<string>();
        foreach (string line in dump.Split('\n', StringSplitOptions.RemoveEmptyEntries))
        {
            string[] kv = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            var keys = new List<string>();
            for (int i = 1; i < kv.Length; i++) keys.Add(kv[i][..kv[i].IndexOf('=')]);
            parts.Add(kv[0] + " " + keys.Count + " keys: " + string.Join(",", keys));
        }
        return string.Join(" | ", parts);
    }
}
