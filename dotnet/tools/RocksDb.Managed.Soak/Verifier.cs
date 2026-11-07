using System.Diagnostics;
using System.IO.Hashing;
using System.Text.Json;
using RocksDb.Managed.Cache;
using RocksDb.Managed.Env;

namespace RocksDb.Managed.Soak;

/// <summary>The verifier's report of one cycle (verify/cycle-N.json).</summary>
internal sealed class VerifyReport
{
    public int Cycle { get; set; }
    public string Fault { get; set; } = "";
    public string Durability { get; set; } = "";
    public bool Pass { get; set; }
    public List<StepResult> Steps { get; set; } = [];
    /// <summary>History op counts "Type.outcome".</summary>
    public SortedDictionary<string, long> Ops { get; set; } = [];
    /// <summary>Per column family: "count:xxhash64" of a full forward scan.</summary>
    public SortedDictionary<string, string> Digests { get; set; } = [];
    public string? Exception { get; set; }
}

/// <summary>Per column family digest of a full scan (compared with native RocksDB).</summary>
internal sealed class ScanDigest
{
    private readonly XxHash64 _h = new();
    public long Count;

    public void Add(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        Span<byte> len = stackalloc byte[8];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(len, key.Length);
        System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(len[4..], value.Length);
        _h.Append(len);
        _h.Append(key);
        _h.Append(value);
        Count++;
    }

    public override string ToString() => $"{Count}:{_h.GetCurrentHashAsUInt64():x16}";
}

/// <summary>
/// The VERIFY phase: opens the database (never killed), reads the whole state, and runs the named checks against the
/// model and the cycle's history. Also used, with <see cref="CorruptionMode"/>, on damaged copies.
/// </summary>
internal sealed class Verifier
{
    private readonly RunPaths _paths;
    private readonly RunConfig _cfg;
    private readonly Layout _l;

    public Verifier(RunPaths paths)
    {
        _paths = paths;
        _cfg = RunConfig.Load(paths.RunJson);
        _l = _cfg.Layout;
    }

    private static readonly CycleOptions s_verifyOptions = new()
    {
        WriteBufferSize = 4 << 20, MaxBackgroundJobs = 2, CompactionReadaheadSize = 0, BlockCacheSize = 8 << 20,
        TargetFileSizeBase = 1 << 20, MaxBytesForLevelBase = 4 << 20, Level0Trigger = 4, MaxSubcompactions = 1,
    };

    public static RocksDatabase Open(string db, RunConfig cfg, IFileSystem fs)
    {
        BlockCache cache = BlockCache.CreateLru(s_verifyOptions.BlockCacheSize);
        DbOptions o = DbSetup.DbOptions(s_verifyOptions, fs);
        o.CreateIfMissing = false;
        o.CreateMissingColumnFamilies = false;
        return RocksDatabase.Open(db, o, DbSetup.Descriptors(db, fs, cfg.Db, s_verifyOptions, cache));
    }

    /// <summary>Reads every key of the model's key spaces. Value format errors are reported in <paramref name="valueErrors"/>.</summary>
    public Observed ReadState(RocksDatabase db, StepResult valueErrors)
    {
        var o = new Observed
        {
            Registers = new Cell[_l.Writers * _l.KeysPerWriter],
            Balances = new ulong?[_l.Accounts],
            Counters = new ulong[_l.Counters],
            Rdel = new Cell[_l.RdelKeys],
        };
        ColumnFamily reg0 = db.GetColumnFamily(Keys.Reg0), reg1 = db.GetColumnFamily(Keys.Reg1);
        for (int w = 0; w < _l.Writers; w++)
        {
            for (int i = 0; i < _l.KeysPerWriter; i++)
            {
                byte[]? v = ((i & 1) == 0 ? reg0 : reg1).Get(Keys.Register(w, i));
                o.Registers[w * _l.KeysPerWriter + i] = ToCell((byte)'V', v, w, i, $"register w{w} key {i}", valueErrors);
            }
        }
        ColumnFamily bank = db.GetColumnFamily(Keys.Bank);
        for (int a = 0; a < _l.Accounts; a++)
        {
            byte[]? v = bank.Get(Keys.Account(a));
            if (v is null) continue;
            if (Keys.TryU64(v, out ulong b)) o.Balances[a] = b;
            else valueErrors.Fail($"account {a}: malformed balance ({v.Length} bytes)");
        }
        ColumnFamily ctr = db.GetColumnFamily(Keys.Ctr);
        for (int c = 0; c < _l.Counters; c++)
        {
            byte[]? v = ctr.Get(Keys.Counter(c));
            if (v is null) continue;
            if (Keys.TryU64(v, out ulong x)) o.Counters[c] = x;
            else valueErrors.Fail($"counter {c}: malformed value ({v.Length} bytes)");
        }
        ColumnFamily rdel = db.GetColumnFamily(Keys.Rdel);
        for (int k = 0; k < _l.RdelKeys; k++)
            o.Rdel[k] = ToCell((byte)'D', rdel.Get(Keys.RdelKey(k)), 0, k, $"rdel key {k}", valueErrors);
        return o;
    }

    private static Cell ToCell(byte tag, byte[]? v, int writer, int index, string what, StepResult errors)
    {
        if (v is null) return Cell.Absent;
        string? err = Keys.Decode(tag, v, out Keys.ValueInfo info);
        if (err is not null)
        {
            errors.Fail($"{what}: {err}");
            return new Cell { Version = long.MinValue, Present = true };
        }
        if (info.Writer != writer || info.Index != index)
            errors.Fail($"{what}: holds the value of writer {info.Writer} key {info.Index}");
        return new Cell { Version = info.Version, Present = true, Crc = info.Crc, Length = info.Length };
    }

    /// <summary>Full forward and reverse scans of every column family; checks order, uniqueness and agreement with the point reads.</summary>
    public StepResult Scans(RocksDatabase db, Observed o, SortedDictionary<string, string> digests)
    {
        var s = new StepResult { Name = "scans" };
        var ro = new ReadOptions { VerifyChecksums = true, FillCache = false };
        var counts = new List<string>();
        foreach (ColumnFamily cf in db.ColumnFamilies)
        {
            var keys = new List<byte[]>();
            var hashes = new List<ulong>();
            var digest = new ScanDigest();
            using (Iterator it = db.NewIterator(cf, ro))
            {
                for (it.SeekToFirst(); it.Valid; it.Next())
                {
                    if (keys.Count > 0 && it.Key.SequenceCompareTo(keys[^1]) <= 0)
                        s.Fail($"{cf.Name}: forward scan not strictly increasing at {Printable(it.Key)}");
                    keys.Add(it.Key.ToArray());
                    hashes.Add(XxHash64.HashToUInt64(it.Value));
                    digest.Add(it.Key, it.Value);
                    CheckScanned(cf.Name, it.Key, it.Value, o, s);
                }
                int j = keys.Count - 1;
                for (it.SeekToLast(); it.Valid; it.Prev(), j--)
                {
                    if (j < 0 || !it.Key.SequenceEqual(keys[j]) || XxHash64.HashToUInt64(it.Value) != hashes[j])
                    {
                        s.Fail($"{cf.Name}: reverse scan differs from the forward scan at position {j} ({Printable(it.Key)})");
                        break;
                    }
                }
                if (j >= 0) s.Fail($"{cf.Name}: reverse scan returned {keys.Count - 1 - j} of {keys.Count} entries");
            }
            // Every present key of the point reads must have been scanned (and nothing else, checked per entry above).
            int expected = ExpectedCount(cf.Name, o);
            if (expected >= 0 && expected != keys.Count) s.Fail($"{cf.Name}: scan saw {keys.Count} keys, point reads found {expected}");
            digests[cf.Name] = digest.ToString();
            counts.Add($"{cf.Name}={keys.Count}");
            s.Count += keys.Count;
        }
        s.Detail = string.Join(" ", counts);
        return s;
    }

    private int ExpectedCount(string cf, Observed o)
    {
        int n = 0;
        switch (cf)
        {
            case Keys.Reg0:
            case Keys.Reg1:
                for (int k = 0; k < o.Registers.Length; k++)
                    if (o.Registers[k].Present && ((k % _l.KeysPerWriter) & 1) == (cf == Keys.Reg0 ? 0 : 1)) n++;
                return n;
            case Keys.Bank:
                foreach (ulong? b in o.Balances) if (b is not null) n++;
                return n;
            case Keys.Rdel:
                foreach (Cell c in o.Rdel) if (c.Present) n++;
                return n;
            case "default":
                return 0;
            default:
                return -1; // counters (absent = 0) and scratch column families
        }
    }

    private void CheckScanned(string cf, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, Observed o, StepResult s)
    {
        switch (cf)
        {
            case Keys.Reg0:
            case Keys.Reg1:
            {
                if (!Keys.TryParseRegister(key, out int w, out int i) || w >= _l.Writers || i >= _l.KeysPerWriter || Keys.RegisterCf(i) != cf)
                {
                    s.Fail($"{cf}: unexpected key {Printable(key)}");
                    return;
                }
                Cell c = o.Registers[w * _l.KeysPerWriter + i];
                if (Keys.Decode((byte)'V', value, out Keys.ValueInfo info) is string err) s.Fail($"{cf} {Printable(key)}: {err}");
                else if (!c.Present || c.Version != info.Version) s.Fail($"{cf} {Printable(key)}: scan v{info.Version}, point read {c}");
                return;
            }
            case Keys.Rdel:
            {
                if (!Keys.TryParseIndexed(key, (byte)'d', 5, out int k) || k >= _l.RdelKeys)
                {
                    s.Fail($"{cf}: unexpected key {Printable(key)}");
                    return;
                }
                if (Keys.Decode((byte)'D', value, out Keys.ValueInfo info) is string err) s.Fail($"{cf} {Printable(key)}: {err}");
                else if (!o.Rdel[k].Present || o.Rdel[k].Version != info.Version) s.Fail($"{cf} {Printable(key)}: scan v{info.Version}, point read {o.Rdel[k]}");
                return;
            }
            case Keys.Bank:
            {
                if (!Keys.TryParseIndexed(key, (byte)'a', 4, out int a) || a >= _l.Accounts || !Keys.TryU64(value, out ulong v) || o.Balances[a] != v)
                    s.Fail($"{cf}: entry {Printable(key)} disagrees with the point read");
                return;
            }
            case Keys.Ctr:
            {
                if (!Keys.TryParseIndexed(key, (byte)'c', 3, out int c) || c >= _l.Counters || !Keys.TryU64(value, out ulong v) || o.Counters[c] != v)
                    s.Fail($"{cf}: entry {Printable(key)} disagrees with the point read");
                return;
            }
            case "default":
                s.Fail($"default column family: unexpected key {Printable(key)}");
                return;
        }
    }

    public static string Printable(ReadOnlySpan<byte> key)
    {
        foreach (byte b in key)
            if (b < 32 || b > 126) return Convert.ToHexString(key);
        return System.Text.Encoding.ASCII.GetString(key);
    }

    /// <summary>File checksums of every live file, live-file metadata against the directory, properties.</summary>
    public static StepResult Integrity(RocksDatabase db, string dir, IFileSystem fs)
    {
        var s = new StepResult { Name = "integrity" };
        try
        {
            db.VerifyFileChecksums();
        }
        catch (RocksDbException e)
        {
            s.Fail("VerifyFileChecksums: " + e.Code + " " + e.Message);
        }
        List<LiveFileMetadata> files = db.GetLiveFilesMetadata();
        var perCf = new Dictionary<string, int>();
        ulong bytes = 0;
        foreach (LiveFileMetadata f in files)
        {
            string path = Path.Combine(dir, f.Name.TrimStart('/'));
            if (!fs.FileExists(path)) s.Fail($"live file {f.Name} is missing");
            else if ((ulong)fs.GetFileSize(path) != f.Size) s.Fail($"live file {f.Name}: size {fs.GetFileSize(path)} on disk, {f.Size} in the MANIFEST");
            if (f.Level is < 0 or >= 7) s.Fail($"live file {f.Name}: level {f.Level}");
            if (f.SmallestKey.AsSpan().SequenceCompareTo(f.LargestKey) > 0) s.Fail($"live file {f.Name}: smallest key > largest key");
            perCf[f.ColumnFamilyName] = perCf.GetValueOrDefault(f.ColumnFamilyName) + 1;
            bytes += f.Size;
        }
        int levels = 0;
        foreach (ColumnFamily cf in db.ColumnFamilies)
        {
            if (!cf.TryGetIntProperty("rocksdb.estimate-num-keys", out _)) s.Fail($"{cf.Name}: rocksdb.estimate-num-keys unavailable");
            int sum = 0;
            for (int lv = 0; lv < 7; lv++)
            {
                string? p = cf.GetProperty("rocksdb.num-files-at-level" + lv);
                if (p is null || !int.TryParse(p, out int n))
                {
                    s.Fail($"{cf.Name}: rocksdb.num-files-at-level{lv} = {p ?? "null"}");
                    continue;
                }
                if (n > 0) levels = Math.Max(levels, lv + 1);
                sum += n;
            }
            if (sum != perCf.GetValueOrDefault(cf.Name)) s.Fail($"{cf.Name}: num-files-at-level* sum {sum} != {perCf.GetValueOrDefault(cf.Name)} live files");
        }
        s.Count = files.Count;
        s.Detail = $"{files.Count} live table files ({bytes >> 10} KiB), deepest level {levels - 1}, file checksums verified";
        return s;
    }

    /// <summary>The VERIFY phase of the current cycle (cycle.json). Returns the process exit code.</summary>
    public int RunCycle()
    {
        CyclePlan plan = CyclePlan.Load(_paths.CycleJson);
        Model model = Model.Load(_paths.ModelFile);
        var report = new VerifyReport { Cycle = plan.Cycle, Fault = plan.Fault.ToString(), Durability = plan.Durability.ToString() };
        List<Op> ops = Op.Pair(HistoryReader.ReadParts(_paths.HistoryParts(plan.Cycle)));
        foreach (Op op in ops)
        {
            string k = op.Invoke.Type + "." + op.Outcome.ToString().ToLowerInvariant();
            report.Ops[k] = report.Ops.GetValueOrDefault(k) + 1;
        }
        IFileSystem fs = LocalFileSystem.Instance;
        RocksDatabase? db = null;
        try
        {
            var open = new StepResult { Name = "open" };
            var sw = Stopwatch.StartNew();
            try
            {
                db = Open(_paths.Db, _cfg, fs);
            }
            catch (Exception e)
            {
                open.Fail("open failed: " + e);
            }
            open.Ms = sw.Elapsed.TotalMilliseconds;
            open.Detail = $"recovery after {plan.Fault}" + (plan.Fault == FaultKind.RecoveryKill ? " (the previous open was killed during recovery)" : "");
            report.Steps.Add(open);
            if (db is null) return Finish(report, null, null, model, ops);

            sw.Restart();
            var valueErrors = new StepResult { Name = "values" };
            Observed o;
            try
            {
                o = ReadState(db, valueErrors);
            }
            catch (RocksDbException e)
            {
                var failed = new StepResult { Name = "registers", Ms = sw.Elapsed.TotalMilliseconds };
                failed.Fail($"reading the state failed: {e.Code}: {e.Message}");
                report.Steps.Add(failed);
                return Finish(report, null, null, model, ops);
            }
            double readMs = sw.Elapsed.TotalMilliseconds;
            (StepResult regs, StepResult dur) = Checker.CheckRegisters(_l, model, ops, o, plan.Durability);
            (StepResult rdel, StepResult rdelDur) = Checker.CheckRdel(_l, model, ops, o, plan.Durability);
            // Durability (step 2) covers both register-like key spaces.
            dur.Errors.AddRange(rdelDur.Errors);
            if (!rdelDur.Pass) dur.Pass = false;
            foreach (string e in valueErrors.Errors) regs.Fail(e);
            regs.Ms = dur.Ms = readMs;
            report.Steps.Add(dur);
            report.Steps.Add(regs);
            report.Steps.Add(Timed(() => Checker.CheckBank(_l, o)));
            report.Steps.Add(Timed(() => Checker.CheckCounters(_l, model, ops, o, plan.Durability)));
            report.Steps.Add(rdel);
            report.Steps.Add(Timed(() => Scans(db, o, report.Digests)));
            report.Steps.Add(Timed(() => Integrity(db, _paths.Db, fs)));
            if (plan.NativeCheck) report.Steps.Add(Timed(() => NativeCrossCheck(db, report.Digests)));
            var close = new StepResult { Name = "close" };
            sw.Restart();
            try
            {
                db.Dispose();
                db = null;
            }
            catch (Exception e)
            {
                close.Fail("close failed: " + e);
            }
            close.Ms = sw.Elapsed.TotalMilliseconds;
            report.Steps.Add(close);
            return Finish(report, o, null, model, ops);
        }
        catch (Exception e)
        {
            report.Exception = e.ToString();
            return Finish(report, null, e, model, ops);
        }
        finally
        {
            try { db?.Dispose(); }
            catch (Exception) { /* already reported */ }
        }
    }

    private static StepResult Timed(Func<StepResult> f)
    {
        var sw = Stopwatch.StartNew();
        StepResult r;
        try
        {
            r = f();
        }
        catch (Exception e)
        {
            r = new StepResult { Name = f.Method.Name };
            r.Fail("exception: " + e);
        }
        r.Ms = sw.Elapsed.TotalMilliseconds;
        return r;
    }

    private int Finish(VerifyReport report, Observed? o, Exception? e, Model model, List<Op> ops)
    {
        report.Pass = e is null && report.Steps.TrueForAll(static s => s.Pass);
        File.WriteAllText(_paths.VerifyJson(report.Cycle), JsonSerializer.Serialize(report, RunConfig.Json));
        foreach (StepResult s in report.Steps) Console.WriteLine(s);
        if (e is not null) Console.WriteLine("verifier exception: " + e);
        if (report.Pass && o is not null) Checker.Resolve(_l, model, ops, o).Save(_paths.NextModelFile);
        return report.Pass ? 0 : 1;
    }

    /// <summary>Checkpoint, then native RocksDB (rocksdb-sharp, in a child process) must scan the same digests.</summary>
    private StepResult NativeCrossCheck(RocksDatabase db, SortedDictionary<string, string> managed)
    {
        var s = new StepResult { Name = "native-cross-check" };
        string dir = Path.Combine(_paths.Tmp, "xcheck");
        if (Directory.Exists(dir)) Directory.Delete(dir, true);
        db.CreateCheckpoint(dir);
        string outFile = Path.Combine(_paths.Tmp, "xcheck.json");
        (int code, string output) = Child.Run(["native-digest", dir, outFile], TimeSpan.FromMinutes(10), null);
        if (code != 0)
        {
            s.Fail($"native digest child exited with {Child.Describe(code)}: {output}");
            return s;
        }
        var native = JsonSerializer.Deserialize<SortedDictionary<string, string>>(File.ReadAllText(outFile)) ?? [];
        foreach (KeyValuePair<string, string> kv in managed)
        {
            if (!native.TryGetValue(kv.Key, out string? n)) s.Fail($"{kv.Key}: missing in the native scan");
            else if (n != kv.Value) s.Fail($"{kv.Key}: managed {kv.Value}, native {n}");
        }
        foreach (string k in native.Keys)
            if (!managed.ContainsKey(k)) s.Fail($"{k}: only in the native scan");
        s.Count = managed.Count;
        s.Detail = $"{managed.Count} column families identical in native librocksdb (checkpoint)";
        Directory.Delete(dir, true);
        File.Delete(outFile);
        return s;
    }
}

/// <summary>The native side of the cross-check: digests every column family with rocksdb-sharp (read-only open).</summary>
internal static class NativeDigest
{
    public static int Run(string dir, string outFile)
    {
        var digests = new SortedDictionary<string, string>();
        try
        {
            digests = Compute(dir);
        }
        catch (Exception e)
        {
            digests["error"] = e.GetType().Name + ": " + e.Message;
        }
        File.WriteAllText(outFile, JsonSerializer.Serialize(digests));
        return 0;
    }

    public static SortedDictionary<string, string> Compute(string dir)
    {
        var digests = new SortedDictionary<string, string>();
        var dbo = new RocksDbSharp.DbOptions();
        if (!RocksDbSharp.RocksDb.TryListColumnFamilies(dbo, dir, out string[] names)) throw new InvalidOperationException("ListColumnFamilies failed");
        var cfo = new RocksDbSharp.ColumnFamilyOptions();
        cfo.SetUint64addMergeOperator();
        var cfs = new RocksDbSharp.ColumnFamilies(cfo);
        foreach (string n in names)
            if (n != "default") cfs.Add(n, cfo);
        using RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.OpenReadOnly(dbo, dir, cfs, false);
        foreach (string n in names)
        {
            var d = new ScanDigest();
            using RocksDbSharp.Iterator it = db.NewIterator(db.GetColumnFamily(n));
            for (it.SeekToFirst(); it.Valid(); it.Next()) d.Add(it.Key(), it.Value());
            digests[n] = d.ToString();
        }
        return digests;
    }
}
