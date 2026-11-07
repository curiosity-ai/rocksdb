using System.Text.Json;
using RocksDb.Managed.Env;

namespace RocksDb.Managed.Soak;

/// <summary>Result of checking one damaged copy (written by the corrupt-check child).</summary>
internal sealed class CorruptCheckResult
{
    /// <summary>detected-open, detected-read, detected-verify, correct, wal-suffix-loss or silent-wrong.</summary>
    public string Outcome { get; set; } = "";
    public string Detail { get; set; } = "";
    public SortedDictionary<string, string> Digests { get; set; } = [];
}

/// <summary>
/// Jepsen's file nemesis (jepsen.nemesis.file / corrupt-file.c) applied to a COPY of the stopped database after a
/// successful verification: bit flips with a per-bit probability, truncation (including trimming the tail), copying
/// chunks within a file, and snapshot-then-restore of chunks (a lost or misdirected write). The expectation is
/// "detect or be correct": the open fails, a read or checksum verification throws, or every read returns exactly
/// the verified state. A damaged WAL may lose only a suffix (checked against the cycle's history). A silently wrong
/// value, a phantom, a hang or a process crash is a failure.
/// </summary>
internal sealed class CorruptionNemesis(RunPaths paths, Action<string> log)
{
    private static readonly DbFileKind[] s_targets = [DbFileKind.Sst, DbFileKind.Wal, DbFileKind.Manifest, DbFileKind.Blob, DbFileKind.Options, DbFileKind.Current];
    private static readonly string[] s_modes = ["bitflip", "truncate", "copy", "restore"];

    private string SnapshotDir => Path.Combine(paths.Corrupt, "snapshots");

    /// <summary>Runs one trial; returns a failure description or null. Counts outcomes as "target/mode/outcome".</summary>
    public string? Trial(CyclePlan plan, SortedDictionary<string, long> counts)
    {
        var r = new Random((int)(Keys.Mix(plan.CycleSeed ^ 0xC0221) & 0x7FFFFFFF));
        string copy = Path.Combine(paths.Corrupt, "managed");
        string nativeCopy = Path.Combine(paths.Corrupt, "native");
        foreach (string d in new[] { copy, nativeCopy })
            if (Directory.Exists(d)) Directory.Delete(d, true);
        Supervisor.CopyDirectory(paths.Db, copy);

        TakeSnapshots(copy, r);
        var byKind = new Dictionary<DbFileKind, List<string>>();
        foreach (string f in Directory.GetFiles(copy))
        {
            DbFileKind k = DbFiles.KindOf(f);
            if (Array.IndexOf(s_targets, k) < 0 || new FileInfo(f).Length == 0) continue;
            if (!byKind.TryGetValue(k, out List<string>? l)) byKind[k] = l = [];
            l.Add(f);
        }
        var kinds = new List<DbFileKind>(byKind.Keys);
        kinds.Sort();
        if (kinds.Count == 0) return null;
        DbFileKind kind = kinds[r.Next(kinds.Count)];
        string target = byKind[kind][r.Next(byKind[kind].Count)];
        string mode = s_modes[r.Next(s_modes.Length)];
        string what = Damage(target, mode, kind, r);
        Supervisor.CopyDirectory(copy, nativeCopy);
        string label = $"{kind}/{mode}";
        string outFile = Path.Combine(paths.Corrupt, "result.json");
        if (File.Exists(outFile)) File.Delete(outFile);
        (int code, string tail) = Child.Run(["corrupt-check", "--run-dir", paths.Root, "--db", copy, "--target", kind.ToString(), "--out", outFile],
            TimeSpan.FromMinutes(3), paths.LogFile(plan.Cycle, "corrupt"));
        string? failure = null;
        string outcome;
        if (code != 0 || !File.Exists(outFile))
        {
            outcome = code == -1 ? "hang" : "crash";
            failure = $"{label}: {what}: the check of the damaged copy ended with {Child.Describe(code)}\n{tail}";
        }
        else
        {
            CorruptCheckResult res = JsonSerializer.Deserialize<CorruptCheckResult>(File.ReadAllText(outFile), RunConfig.Json)!;
            outcome = res.Outcome;
            if (outcome == "silent-wrong")
            {
                // Is this what native RocksDB does with the same bytes? Then it is upstream behaviour, not a port bug.
                (int nc, string _) = Child.Run(["native-digest", nativeCopy, outFile + ".native"], TimeSpan.FromMinutes(3), paths.LogFile(plan.Cycle, "corrupt"));
                SortedDictionary<string, string>? native = nc == 0 && File.Exists(outFile + ".native")
                    ? JsonSerializer.Deserialize<SortedDictionary<string, string>>(File.ReadAllText(outFile + ".native"))
                    : null;
                bool same = native is not null && native.Count == res.Digests.Count;
                if (same)
                {
                    foreach (KeyValuePair<string, string> kv in res.Digests)
                        if (!native!.TryGetValue(kv.Key, out string? v) || v != kv.Value) same = false;
                }
                if (same)
                {
                    outcome = "silent-wrong-same-as-native";
                    log($"corruption trial {label}: {what}: managed returns wrong data WITHOUT an error, exactly like native librocksdb: {res.Detail}");
                }
                else failure = $"{label}: {what}: silently wrong data: {res.Detail}" + (native is null ? " (native could not read the copy)" : " (native reads differ)");
            }
            log($"cycle {plan.Cycle}: corruption trial {label}: {what} -> {outcome}" + (res.Detail.Length > 0 ? $" ({Trim(res.Detail)})" : ""));
        }
        counts[$"{label}/{outcome}"] = counts.GetValueOrDefault($"{label}/{outcome}") + 1;
        if (failure is null)
        {
            Directory.Delete(copy, true);
            Directory.Delete(nativeCopy, true);
        }
        else
        {
            string keep = Path.Combine(paths.Failures, $"cycle-{plan.Cycle:D6}-corrupt");
            Directory.CreateDirectory(paths.Failures);
            if (Directory.Exists(keep)) Directory.Delete(keep, true);
            Directory.Move(nativeCopy, keep);
            File.WriteAllText(Path.Combine(keep, "..", $"cycle-{plan.Cycle:D6}-corrupt.txt"), failure);
            Directory.Delete(copy, true);
        }
        return failure;
    }

    private static string Trim(string s) => s.Length > 300 ? s[..300] + "..." : s;

    private void TakeSnapshots(string dir, Random r)
    {
        Directory.CreateDirectory(SnapshotDir);
        string[] files = Directory.GetFiles(dir);
        for (int i = 0; i < 3 && files.Length > 0; i++)
        {
            string f = files[r.Next(files.Length)];
            long len = new FileInfo(f).Length;
            if (len == 0) continue;
            int size = (int)Math.Min(len, r.Next(16, 4097));
            long off = r.NextInt64(0, len - size + 1);
            byte[] data = new byte[size];
            using (FileStream fs = File.OpenRead(f))
            {
                fs.Position = off;
                fs.ReadExactly(data);
            }
            File.WriteAllBytes(Path.Combine(SnapshotDir, $"{Path.GetFileName(f)}@{off}"), data);
        }
        // Bounded: keep the newest 30.
        var snaps = new List<FileInfo>(new DirectoryInfo(SnapshotDir).GetFiles());
        snaps.Sort(static (a, b) => b.LastWriteTimeUtc.CompareTo(a.LastWriteTimeUtc));
        for (int i = 30; i < snaps.Count; i++) snaps[i].Delete();
    }

    /// <summary>Damages <paramref name="file"/>; returns a description.</summary>
    private string Damage(string file, string mode, DbFileKind kind, Random r)
    {
        byte[] data = File.ReadAllBytes(file);
        string name = Path.GetFileName(file);
        byte[] before = (byte[])data.Clone();
        string what;
        switch (mode)
        {
            case "bitflip":
            {
                // A region of 512..1024 bytes (or the whole file), each bit flipped with a small probability.
                int len = Math.Min(data.Length, r.Next(512, 1025));
                int off = r.Next(0, data.Length - len + 1);
                double p = r.Next(2) == 0 ? 1e-3 : 1e-2;
                int flips = 0;
                for (int i = off; i < off + len; i++)
                {
                    for (int b = 0; b < 8; b++)
                    {
                        if (r.NextDouble() < p)
                        {
                            data[i] ^= (byte)(1 << b);
                            flips++;
                        }
                    }
                }
                if (flips == 0)
                {
                    data[off + r.Next(len)] ^= (byte)(1 << r.Next(8));
                    flips = 1;
                }
                what = $"{flips} bits flipped in {name}[{off},{off + len})";
                File.WriteAllBytes(file, data);
                return what;
            }
            case "truncate":
            {
                int newLen = r.Next(2) == 0
                    ? Math.Max(0, data.Length - r.Next(1, Math.Min(data.Length, 200) + 1)) // trim the tail
                    : r.Next(0, data.Length);
                File.WriteAllBytes(file, data.AsSpan(0, newLen).ToArray());
                return $"{name} truncated from {data.Length} to {newLen} bytes";
            }
            case "copy":
            {
                int chunk = Math.Max(1, Math.Min(data.Length / 2, r.Next(16, 4097)));
                if (data.Length < 2)
                {
                    data[0] ^= 0xFF;
                    File.WriteAllBytes(file, data);
                    return $"{name}: single byte inverted";
                }
                int from = r.Next(0, data.Length - chunk + 1), to = r.Next(0, data.Length - chunk + 1);
                if (from == to) to = (to + chunk) % (data.Length - chunk + 1);
                Buffer.BlockCopy(before, from, data, to, chunk);
                what = $"{name}: {chunk} bytes copied from offset {from} to {to}";
                break;
            }
            default:
            {
                // Restore a chunk snapshotted earlier: the same file (a lost write) or another file of the same kind
                // at the same offset (a misdirected write).
                string? pick = null;
                bool same = false;
                foreach (string s in Directory.GetFiles(SnapshotDir))
                {
                    string sn = Path.GetFileName(s);
                    string src = sn[..sn.LastIndexOf('@')];
                    if (src == name)
                    {
                        pick = s;
                        same = true;
                        break;
                    }
                    if (pick is null && DbFiles.KindOf(src) == kind) pick = s;
                }
                if (pick is null) return Damage(file, "bitflip", kind, r);
                string pn = Path.GetFileName(pick);
                long off = long.Parse(pn[(pn.LastIndexOf('@') + 1)..], System.Globalization.CultureInfo.InvariantCulture);
                byte[] chunk = File.ReadAllBytes(pick);
                if (off >= data.Length) off = Math.Max(0, data.Length - chunk.Length);
                int n = (int)Math.Min(chunk.Length, data.Length - off);
                Buffer.BlockCopy(chunk, 0, data, (int)off, n);
                what = $"{name}: {n} bytes at {off} restored from a snapshot of {(same ? "the same file" : pn[..pn.LastIndexOf('@')])}";
                break;
            }
        }
        if (data.AsSpan().SequenceEqual(before))
        {
            // The chunk was identical: make it a real change.
            data[r.Next(data.Length)] ^= 0x5A;
            what += " (identical, plus one byte changed)";
        }
        File.WriteAllBytes(file, data);
        return what;
    }

    /// <summary>The corrupt-check child: opens the damaged copy and classifies the outcome.</summary>
    public static int Check(RunPaths paths, string db, DbFileKind target, string outFile)
    {
        RunConfig cfg = RunConfig.Load(paths.RunJson);
        Layout l = cfg.Layout;
        var res = new CorruptCheckResult();
        Model expected = Model.Load(paths.NextModelFile);
        RocksDatabase? rdb = null;
        try
        {
            try
            {
                rdb = Verifier.Open(db, cfg, LocalFileSystem.Instance);
            }
            catch (RocksDbException e)
            {
                res.Outcome = "detected-open";
                res.Detail = e.Message;
                return Write(res, outFile);
            }
            var v = new Verifier(paths);
            var valueErrors = new StepResult();
            Observed o;
            try
            {
                o = v.ReadState(rdb, valueErrors);
            }
            catch (RocksDbException e) when (e.Code is StatusCode.Corruption or StatusCode.IOError)
            {
                res.Outcome = "detected-read";
                res.Detail = e.Message;
                return Write(res, outFile);
            }
            var diffs = new List<string>(valueErrors.Errors);
            for (int i = 0; i < o.Registers.Length; i++)
                if (!o.Registers[i].SameAs(expected.Registers[i])) diffs.Add($"register {i}: {o.Registers[i]}, verified {expected.Registers[i]}");
            for (int i = 0; i < o.Rdel.Length; i++)
                if (!o.Rdel[i].SameAs(expected.Rdel[i])) diffs.Add($"rdel {i}: {o.Rdel[i]}, verified {expected.Rdel[i]}");
            for (int i = 0; i < l.Accounts; i++)
                if (o.Balances[i] != expected.Balances[i]) diffs.Add($"account {i}: {o.Balances[i]?.ToString() ?? "absent"}, verified {expected.Balances[i]}");
            for (int i = 0; i < l.Counters; i++)
                if (o.Counters[i] != expected.Counters[i]) diffs.Add($"counter {i}: {o.Counters[i]}, verified {expected.Counters[i]}");

            StepResult scans, integrity;
            try
            {
                scans = v.Scans(rdb, o, res.Digests);
                integrity = Verifier.Integrity(rdb, db, LocalFileSystem.Instance);
            }
            catch (RocksDbException e) when (e.Code is StatusCode.Corruption or StatusCode.IOError)
            {
                res.Outcome = diffs.Count == 0 ? "detected-read" : "silent-wrong";
                res.Detail = (diffs.Count == 0 ? "" : string.Join("; ", diffs.GetRange(0, Math.Min(5, diffs.Count))) + "; then ") + e.Message;
                return Write(res, outFile);
            }
            diffs.AddRange(scans.Errors);
            if (diffs.Count == 0)
            {
                if (!integrity.Pass)
                {
                    res.Outcome = "detected-verify";
                    res.Detail = string.Join("; ", integrity.Errors);
                }
                else res.Outcome = "correct";
                return Write(res, outFile);
            }
            if (target == DbFileKind.Wal && valueErrors.Pass && scans.Pass)
            {
                // A damaged WAL may lose a suffix of the cycle's writes (point-in-time recovery stops at the damage).
                Model baseModel = Model.Load(paths.ModelFile);
                List<Op> ops = Op.Pair(HistoryReader.ReadParts(paths.HistoryParts(CyclePlan.Load(paths.CycleJson).Cycle)));
                (StepResult regs, _) = Checker.CheckRegisters(l, baseModel, ops, o, Durability.None);
                (StepResult rdel, _) = Checker.CheckRdel(l, baseModel, ops, o, Durability.None);
                StepResult bank = Checker.CheckBank(l, o);
                StepResult ctr = Checker.CheckCounters(l, baseModel, ops, o, Durability.None);
                if (regs.Pass && rdel.Pass && bank.Pass && ctr.Pass)
                {
                    res.Outcome = "wal-suffix-loss";
                    res.Detail = $"{diffs.Count} keys lost a suffix of their writes";
                    return Write(res, outFile);
                }
                diffs.AddRange(regs.Errors);
                diffs.AddRange(rdel.Errors);
            }
            res.Outcome = "silent-wrong";
            res.Detail = $"{diffs.Count} differences: " + string.Join("; ", diffs.GetRange(0, Math.Min(8, diffs.Count)));
            return Write(res, outFile);
        }
        finally
        {
            try { rdb?.Dispose(); }
            catch (Exception) { /* a damaged DB may fail to close */ }
        }
    }

    private static int Write(CorruptCheckResult res, string outFile)
    {
        File.WriteAllText(outFile, JsonSerializer.Serialize(res, RunConfig.Json));
        Console.WriteLine(res.Outcome + " " + res.Detail);
        return 0;
    }
}
