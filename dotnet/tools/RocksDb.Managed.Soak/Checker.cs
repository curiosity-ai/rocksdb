using System.Text.Json.Serialization;

namespace RocksDb.Managed.Soak;

/// <summary>The result of one named VERIFY step.</summary>
internal sealed class StepResult
{
    public string Name { get; set; } = "";
    public bool Pass { get; set; } = true;
    public string Detail { get; set; } = "";
    public long Count { get; set; }
    public double Ms { get; set; }
    public List<string> Errors { get; set; } = [];

    [JsonIgnore]
    public int ErrorCount;

    public void Fail(string error)
    {
        Pass = false;
        ErrorCount++;
        if (Errors.Count < 25) Errors.Add(error);
    }

    public override string ToString() =>
        $"{(Pass ? "PASS" : "FAIL")} {Name,-22} {Ms,9:F0} ms  {Detail}" + (Pass ? "" : "\n      " + string.Join("\n      ", Errors));
}

/// <summary>What the verifier read from the database.</summary>
internal sealed class Observed
{
    public Cell[] Registers = [];
    public ulong?[] Balances = [];
    public ulong[] Counters = [];
    public Cell[] Rdel = [];
}

/// <summary>Process ids used by the worker threads in the history.</summary>
internal static class Procs
{
    // 0 .. Writers-1: register writers
    public const int Bank0 = 40;
    public const int BankThreads = 2;
    public const int Counter0 = 50;
    public const int CounterThreads = 2;
    public const int Rdel = 60;
    public const int Reader0 = 70;
    public const int ReaderThreads = 2;
    public const int Admin = 80;
    public const int Syncer = 81;
    public const int Supervisor = 90;
}

/// <summary>
/// The model checks: given the model before the cycle, the cycle's history and what the verifier observed, decides
/// whether the observation is explained by the history (Jepsen-style: :ok ops took effect, :fail ops did not, :info
/// ops may or may not have), and with which durability.
/// </summary>
/// <remarks>
/// Register-like key spaces (registers, range-delete keys) have one writer per key, and every writer issues its ops
/// one after the other, so a writer's ops are totally ordered (by version). A <i>cut</i> C of a writer is the prefix
/// of its first C+1 ops. The observation is consistent when there is one cut C such that for every key the observed
/// value is the effect of the last op on that key at or before C (ops after the last :ok one before C may be
/// skipped if they are :info). That one cut covers every key of the writer, so a hole (an older write lost while a
/// newer one of the same writer survived) is detected even across keys and column families. Durability requires
/// the cut to be at least the last op that must have survived: the last :ok op after a kill or a clean shutdown, the
/// last :ok op covered by a successful sync after a power loss.
/// </remarks>
internal static class Checker
{
    /// <summary>A union of closed intervals of cuts.</summary>
    private sealed class CutSet
    {
        public List<(long Lo, long Hi)> Ranges = [];

        public static CutSet Of(long lo, long hi)
        {
            var s = new CutSet();
            if (lo <= hi) s.Ranges.Add((lo, hi));
            return s;
        }

        public bool IsEmpty => Ranges.Count == 0;

        public void Add(long lo, long hi)
        {
            if (lo > hi) return;
            Ranges.Add((lo, hi));
            Ranges.Sort();
            var merged = new List<(long, long)>();
            foreach ((long l, long h) in Ranges)
            {
                if (merged.Count > 0 && l <= merged[^1].Item2 + 1)
                    merged[^1] = (merged[^1].Item1, Math.Max(merged[^1].Item2, h));
                else merged.Add((l, h));
            }
            Ranges = merged;
        }

        public CutSet Intersect(CutSet o)
        {
            var r = new CutSet();
            int i = 0, j = 0;
            while (i < Ranges.Count && j < o.Ranges.Count)
            {
                long lo = Math.Max(Ranges[i].Lo, o.Ranges[j].Lo), hi = Math.Min(Ranges[i].Hi, o.Ranges[j].Hi);
                if (lo <= hi) r.Ranges.Add((lo, hi));
                if (Ranges[i].Hi < o.Ranges[j].Hi) i++;
                else j++;
            }
            return r;
        }

        public long Max => Ranges.Count == 0 ? long.MinValue : Ranges[^1].Hi;

        public override string ToString() => Ranges.Count == 0 ? "{}" : string.Join(" ", Ranges.ConvertAll(static r => $"[{r.Lo},{r.Hi}]"));
    }

    /// <summary>Index (in the history) after which an :ok op is known synced: the latest invoke of an :ok sync event.</summary>
    public static int SyncHorizon(List<Op> ops)
    {
        int horizon = -1;
        foreach (Op op in ops)
        {
            if (!op.IsOk) continue;
            bool syncEvent = op.Invoke.Type == OpType.SyncWal || (op.Invoke.Flags & OpFlags.Sync) != 0;
            if (syncEvent && op.Invoke.Index > horizon) horizon = op.Invoke.Index;
        }
        return horizon;
    }

    public static bool MustSurvive(Op op, Durability d, int horizon) => d switch
    {
        Durability.AllAcked => op.IsOk,
        Durability.Synced => op.IsOk && (op.Completion!.Value.Index < horizon || (op.Invoke.Flags & OpFlags.Sync) != 0),
        _ => false,
    };

    /// <summary>One op on a register-like key, in the writer's order.</summary>
    private readonly record struct KeyOp(int Pos, bool IsPut, long Version, uint Crc, int Len, bool Ok);

    /// <summary>
    /// Checks one writer's register-like keys. <paramref name="ops"/> are the writer's non-failed ops in order;
    /// <paramref name="affects"/> lists the (key, isPut, version, crc, len) effects of an op.
    /// </summary>
    private static void CheckWriter(string what, int writer, List<Op> ops, Func<Op, IEnumerable<(int Key, bool IsPut)>> affects,
        Cell[] baseCells, Cell[] observed, int keyOffset, int keyCount, Durability durability, int horizon,
        StepResult values, StepResult durable)
    {
        int n = ops.Count;
        var perKey = new Dictionary<int, List<KeyOp>>();
        long required = -1;
        for (int p = 0; p < n; p++)
        {
            Op op = ops[p];
            if (MustSurvive(op, durability, horizon)) required = p;
            foreach ((int key, bool isPut) in affects(op))
            {
                if (!perKey.TryGetValue(key, out List<KeyOp>? list)) perKey[key] = list = [];
                list.Add(new KeyOp(p, isPut, op.Invoke.V, op.Invoke.Crc, op.Invoke.Len, op.IsOk));
            }
        }

        CutSet allowed = CutSet.Of(-1, n - 1);
        var perKeySets = new Dictionary<int, CutSet>();
        for (int k = keyOffset; k < keyOffset + keyCount; k++)
        {
            Cell obs = observed[k], bas = baseCells[k];
            if (!perKey.TryGetValue(k, out List<KeyOp>? kops))
            {
                if (obs.SameAs(bas)) continue;
                if (obs.Present && (!bas.Present || obs.Version != bas.Version))
                    values.Fail($"{what} w{writer} key {k - keyOffset}: observed {obs}, but the key had no op this cycle and the model says {bas} (phantom)");
                else if (!obs.Present)
                    durable.Fail($"{what} w{writer} key {k - keyOffset}: lost, the model says {bas} and no op touched it this cycle");
                else
                    values.Fail($"{what} w{writer} key {k - keyOffset}: observed {obs} != model {bas} (no op this cycle)");
                continue;
            }
            var set = new CutSet();
            // Candidate positions p (the op whose effect is observed; -1 = the base state).
            void Candidate(int idx)
            {
                long p = idx < 0 ? -1 : kops[idx].Pos;
                long hi = n - 1;
                for (int j = idx + 1; j < kops.Count; j++)
                {
                    if (kops[j].Ok)
                    {
                        hi = kops[j].Pos - 1;
                        break;
                    }
                }
                set.Add(p, hi);
            }
            if (obs.Present)
            {
                int found = -2;
                for (int j = 0; j < kops.Count; j++)
                {
                    if (kops[j].IsPut && kops[j].Version == obs.Version)
                    {
                        found = j;
                        break;
                    }
                }
                if (found == -2 && bas.Present && bas.Version == obs.Version)
                {
                    found = -1;
                    if (bas.Crc != obs.Crc || bas.Length != obs.Length)
                        values.Fail($"{what} w{writer} key {k - keyOffset}: observed {obs} differs from the model's {bas}");
                }
                if (found == -2)
                {
                    values.Fail($"{what} w{writer} key {k - keyOffset}: observed version {obs.Version} was never written (phantom)");
                    continue;
                }
                if (found >= 0 && (kops[found].Crc != obs.Crc || kops[found].Len != obs.Length))
                    values.Fail($"{what} w{writer} key {k - keyOffset}: observed {obs} but the write of v{obs.Version} had len {kops[found].Len} crc {kops[found].Crc:x8}");
                Candidate(found);
            }
            else
            {
                if (!bas.Present) Candidate(-1);
                for (int j = 0; j < kops.Count; j++)
                    if (!kops[j].IsPut) Candidate(j);
                if (set.IsEmpty)
                {
                    // Nothing explains an absent key: an acknowledged (or base) value was lost.
                    durable.Fail(
                        $"{what} w{writer} key {k - keyOffset}: absent, but no delete was issued and the model says {bas}");
                    continue;
                }
            }
            perKeySets[k] = set;
            allowed = allowed.Intersect(set);
        }
        // Durability first: a key whose own possible cuts all lie before the op that must have survived lost an
        // acknowledged write (reported as durability even though it usually also leaves a hole).
        var lostKeys = new List<string>();
        CutSet mustReach = CutSet.Of(required, n - 1);
        foreach (KeyValuePair<int, CutSet> kv in perKeySets)
        {
            if (!kv.Value.Intersect(mustReach).IsEmpty) continue;
            if (lostKeys.Count < 6) lostKeys.Add($"key {kv.Key - keyOffset} (observed {observed[kv.Key]}, consistent with cuts {kv.Value})");
            else if (lostKeys.Count == 6) lostKeys.Add("...");
        }
        if (lostKeys.Count > 0)
        {
            Op must = ops[(int)required];
            durable.Fail($"{what} w{writer}: acknowledged writes lost: op #{required} of {n} ({must.Invoke.Type} key {must.Invoke.A} v{must.Invoke.V}" +
                $"{((must.Invoke.Flags & OpFlags.Sync) != 0 ? " sync" : "")}) and everything before it must have survived; keys: {string.Join("; ", lostKeys)}");
            return;
        }
        if (allowed.IsEmpty && perKeySets.Count > 0)
        {
            // No single prefix explains every key: a hole. Name the keys that disagree with the latest prefix.
            long best = long.MinValue;
            foreach (CutSet s in perKeySets.Values) best = Math.Max(best, Math.Min(s.Max, n - 1));
            var bad = new List<string>();
            foreach (KeyValuePair<int, CutSet> kv in perKeySets)
            {
                if (kv.Value.Intersect(CutSet.Of(best, best)).IsEmpty && bad.Count < 6)
                    bad.Add($"key {kv.Key - keyOffset} (observed {observed[kv.Key]}, consistent with cuts {kv.Value})");
            }
            values.Fail($"{what} w{writer}: no prefix of the writer's {n} ops explains the observed state (a hole: newer writes survived while older ones were lost); " +
                $"latest per-key cut {best}; disagreeing: {string.Join("; ", bad)}");
            return;
        }
        if (perKeySets.Count == 0) return;
        CutSet withDurability = allowed.Intersect(CutSet.Of(required, n - 1));
        if (withDurability.IsEmpty)
        {
            Op lost = ops[(int)Math.Min(n - 1, Math.Max(0, allowed.Max + 1))];
            durable.Fail($"{what} w{writer}: the state is the prefix of {allowed.Max + 1} of {n} ops but op #{required} must have survived " +
                $"(first lost: {lost.Invoke.Type} key {lost.Invoke.A} v{lost.Invoke.V} {lost.Outcome}{((lost.Invoke.Flags & OpFlags.Sync) != 0 ? " sync" : "")})");
        }
    }

    public static (StepResult Values, StepResult Durability) CheckRegisters(Layout l, Model m, List<Op> ops, Observed o, Durability d)
    {
        var values = new StepResult { Name = "registers" };
        var durable = new StepResult { Name = "durability" };
        int horizon = SyncHorizon(ops);
        long acked = 0, indeterminate = 0;
        for (int w = 0; w < l.Writers; w++)
        {
            var mine = new List<Op>();
            foreach (Op op in ops)
            {
                if (op.Invoke.Process != w || op.Invoke.Type is not (OpType.RegPut or OpType.RegDel) || op.IsFail) continue;
                mine.Add(op);
                if (op.IsOk) acked++;
                else indeterminate++;
            }
            mine.Sort(static (a, b) => a.Invoke.V.CompareTo(b.Invoke.V));
            int K = l.KeysPerWriter;
            CheckWriter("register", w, mine, op => [(w * K + op.Invoke.A, op.Invoke.Type == OpType.RegPut)],
                m.Registers, o.Registers, w * K, K, d, horizon, values, durable);
        }
        values.Count = l.Writers * l.KeysPerWriter;
        values.Detail = $"{values.Count} keys, {acked} acked + {indeterminate} indeterminate writes this cycle";
        durable.Detail = $"expectation {d}; sync horizon at history record {horizon}";
        return (values, durable);
    }

    public static (StepResult Values, StepResult Durability) CheckRdel(Layout l, Model m, List<Op> ops, Observed o, Durability d)
    {
        var values = new StepResult { Name = "range-deletes" };
        var durable = new StepResult { Name = "range-deletes-durability" };
        int horizon = SyncHorizon(ops);
        var mine = new List<Op>();
        foreach (Op op in ops)
            if (op.Invoke.Type is OpType.RdelPut or OpType.RdelRange && !op.IsFail) mine.Add(op);
        mine.Sort(static (a, b) => a.Invoke.V.CompareTo(b.Invoke.V));
        CheckWriter("rdel", 0, mine, static op =>
        {
            if (op.Invoke.Type == OpType.RdelPut) return [(op.Invoke.A, true)];
            var keys = new List<(int, bool)>();
            for (int k = op.Invoke.A; k < op.Invoke.B; k++) keys.Add((k, false));
            return keys;
        }, m.Rdel, o.Rdel, 0, l.RdelKeys, d, horizon, values, durable);
        int present = 0;
        foreach (Cell c in o.Rdel) if (c.Present) present++;
        values.Count = l.RdelKeys;
        values.Detail = $"{present}/{l.RdelKeys} keys present, {mine.Count} ops this cycle";
        return (values, durable);
    }

    public static StepResult CheckBank(Layout l, Observed o)
    {
        var s = new StepResult { Name = "bank" };
        ulong total = 0;
        int count = 0;
        for (int i = 0; i < o.Balances.Length; i++)
        {
            if (o.Balances[i] is ulong b)
            {
                total += b;
                count++;
            }
        }
        if (count != l.Accounts) s.Fail($"{count} accounts present, expected {l.Accounts}");
        if (total != l.BankTotal) s.Fail($"total {(long)total} != {l.BankTotal}");
        s.Count = count;
        s.Detail = $"{count} accounts, total {(long)total}";
        return s;
    }

    public static StepResult CheckCounters(Layout l, Model m, List<Op> ops, Observed o, Durability d)
    {
        var s = new StepResult { Name = "counters" };
        int horizon = SyncHorizon(ops);
        var lower = new ulong[l.Counters];
        var upper = new ulong[l.Counters];
        for (int c = 0; c < l.Counters; c++) lower[c] = upper[c] = m.Counters[c];
        long acked = 0, indet = 0;
        foreach (Op op in ops)
        {
            if (op.Invoke.Type != OpType.CtrAdd || op.IsFail) continue;
            int c = op.Invoke.A;
            upper[c] += (ulong)op.Invoke.V;
            if (MustSurvive(op, d, horizon)) lower[c] += (ulong)op.Invoke.V;
            if (op.IsOk) acked++;
            else indet++;
        }
        for (int c = 0; c < l.Counters; c++)
        {
            ulong v = o.Counters[c];
            if (v < lower[c] || v > upper[c])
                s.Fail($"counter {c} = {v}, expected within [{lower[c]}, {upper[c]}] (model {m.Counters[c]}, expectation {d})");
        }
        s.Count = l.Counters;
        s.Detail = $"{l.Counters} counters, {acked} acked + {indet} indeterminate increments";
        return s;
    }

    /// <summary>The model after a successful verification: the observation, with the writers' next versions advanced.</summary>
    public static Model Resolve(Layout l, Model m, List<Op> ops, Observed o)
    {
        Model next = m.Clone();
        next.Cycle = m.Cycle + 1;
        next.Registers = (Cell[])o.Registers.Clone();
        next.Rdel = (Cell[])o.Rdel.Clone();
        next.Counters = (ulong[])o.Counters.Clone();
        for (int i = 0; i < l.Accounts; i++) next.Balances[i] = o.Balances[i] ?? 0;
        foreach (Op op in ops)
        {
            HistoryRecord r = op.Invoke;
            if (r.Type is OpType.RegPut or OpType.RegDel && r.Process < l.Writers)
                next.NextVersion[r.Process] = Math.Max(next.NextVersion[r.Process], r.V + 1);
            else if (r.Type is OpType.RdelPut or OpType.RdelRange)
                next.RdelNextVersion = Math.Max(next.RdelNextVersion, r.V + 1);
            else if (r.Type == OpType.BankInit && op.IsOk)
                next.BankInitialized = true;
        }
        return next;
    }
}
