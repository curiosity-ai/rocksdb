using System.Diagnostics;
using System.Globalization;
using RocksDb.Managed.Cluster;

namespace RocksDb.Managed.ClusterSoak;

/// <summary>
/// The state every node converged to at the last successful VERIFY (Jepsen-style resolution of the :info ops: they
/// happened or not, whichever the cluster shows), plus what the next epoch's checks compare against.
/// </summary>
internal sealed class Model
{
    public int Epoch { get; set; }
    public long[] RegVersion { get; set; } = [];
    public long[] NextVersion { get; set; } = [];
    public ulong[] Counters { get; set; } = [];
    public bool BankInitialized { get; set; }
    public ulong LatestSeq { get; set; }
    /// <summary>The (agreed) term map at the last VERIFY: the committed prefix must never change.</summary>
    public string Terms { get; set; } = "";
    public int Verifies { get; set; }

    public static Model Fresh(RunConfig cfg)
    {
        var m = new Model
        {
            RegVersion = new long[cfg.RegisterKeys],
            NextVersion = new long[cfg.RegisterKeys],
            Counters = new ulong[cfg.Counters],
        };
        Array.Fill(m.RegVersion, -1);
        return m;
    }
}

internal sealed class StepResult
{
    public string Name { get; set; } = "";
    public bool Pass { get; set; } = true;
    public long Count { get; set; }
    public double Ms { get; set; }
    public string Detail { get; set; } = "";
    public List<string> Problems { get; set; } = [];

    public void Fail(string problem)
    {
        Pass = false;
        if (Problems.Count < 20) Problems.Add(problem);
    }
}

internal sealed class VerifyReport
{
    public int Epoch { get; set; }
    public bool Pass { get; set; } = true;
    public string Utc { get; set; } = DateTime.UtcNow.ToString("O", CultureInfo.InvariantCulture);
    public List<StepResult> Steps { get; set; } = [];
    public Dictionary<string, long> Ops { get; set; } = [];
    public Dictionary<string, long> Stats { get; set; } = [];
    public Dictionary<string, string> Nodes { get; set; } = [];

    public StepResult Run(string name, Action<StepResult> body)
    {
        var s = new StepResult { Name = name };
        var sw = Stopwatch.StartNew();
        try
        {
            body(s);
        }
        catch (Exception e)
        {
            s.Fail("check threw: " + e);
        }
        s.Ms = sw.Elapsed.TotalMilliseconds;
        Steps.Add(s);
        if (!s.Pass) Pass = false;
        return s;
    }

    public StepResult? Find(string name) => Steps.Find(s => s.Name == name);
}

/// <summary>A parsed event-log line.</summary>
internal sealed record NodeEvent(long Us, string Node, string Kind, string Arg, Dictionary<string, string> Fields)
{
    public long Term => Fields.TryGetValue("term", out string? t) ? long.Parse(t, CultureInfo.InvariantCulture) : -1;

    public static NodeEvent? Parse(string line)
    {
        string[] p = line.Split(' ');
        if (p.Length < 3 || !long.TryParse(p[0], NumberStyles.Integer, CultureInfo.InvariantCulture, out long us)) return null;
        var f = new Dictionary<string, string>(StringComparer.Ordinal);
        string arg = "";
        for (int i = 3; i < p.Length; i++)
        {
            int eq = p[i].IndexOf('=', StringComparison.Ordinal);
            if (eq > 0) f[p[i][..eq]] = p[i][(eq + 1)..];
            else if (i == 3) arg = p[i];
        }
        return new NodeEvent(us, p[1], p[2], arg, f);
    }

    public static List<NodeEvent> ReadAll(string eventsDir)
    {
        var all = new List<NodeEvent>();
        if (!Directory.Exists(eventsDir)) return all;
        foreach (string f in Directory.GetFiles(eventsDir, "*.log"))
        {
            using var fs = new FileStream(f, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
            using var r = new StreamReader(fs);
            while (r.ReadLine() is { } line)
            {
                NodeEvent? e = Parse(line);
                if (e != null) all.Add(e);
            }
        }
        all.Sort(static (a, b) => a.Us.CompareTo(b.Us));
        return all;
    }
}

/// <summary>The VERIFY checks (see docs/cluster-soak.md, "VERIFY steps").</summary>
internal static class Checker
{
    /// <summary>
    /// Runs the content checks on converged node dumps. <paramref name="ops"/> is the epoch's history,
    /// <paramref name="violations"/> the online check violations of the epoch.
    /// </summary>
    public static void CheckState(VerifyReport rep, RunConfig cfg, Model model, IReadOnlyList<Op> ops, IReadOnlyDictionary<string, DumpDto> dumps,
        IEnumerable<string> violations, string eventsDir)
    {
        // ---- the history, summarised per register / counter ----
        int nreg = cfg.RegisterKeys;
        long[] maxOk = new long[nreg];
        Array.Fill(maxOk, -1);
        var writeOutcome = new Dictionary<(int, long), OpType>();
        var invokes = new Dictionary<(int, long), Op>();
        var pending = new Dictionary<int, Op>(); // process -> open invoke
        ulong[] ctrOk = new ulong[cfg.Counters], ctrMaybe = new ulong[cfg.Counters];
        foreach (Op op in ops)
        {
            if (op.F == OpF.Nemesis) continue;
            if (op.Type == OpType.Invoke)
            {
                pending[op.Process] = op;
                if (op.F == OpF.Write) writeOutcome[(op.Keys[0], op.Vals[0])] = OpType.Info; // open until completed
                continue;
            }
            pending.Remove(op.Process);
            string t = op.F.ToString().ToLowerInvariant() + "." + op.Type.ToString().ToLowerInvariant();
            rep.Ops[t] = rep.Ops.GetValueOrDefault(t) + 1;
            if (op.F == OpF.Write)
            {
                writeOutcome[(op.Keys[0], op.Vals[0])] = op.Type;
                if (op.Type == OpType.Ok) maxOk[op.Keys[0]] = Math.Max(maxOk[op.Keys[0]], op.Vals[0]);
            }
            else if (op.F == OpF.Add)
            {
                if (op.Type == OpType.Ok) ctrOk[op.Keys[0]] += (ulong)op.Vals[0];
                if (op.Type != OpType.Fail) ctrMaybe[op.Keys[0]] += (ulong)op.Vals[0];
            }
        }
        foreach (Op open in pending.Values)
        {
            if (open.F == OpF.Add) ctrMaybe[open.Keys[0]] += (ulong)open.Vals[0];
        }

        var nodes = new List<string>(dumps.Keys);
        nodes.Sort(StringComparer.Ordinal);
        var regs = new Dictionary<string, long[]>(StringComparer.Ordinal);
        var maps = new Dictionary<string, Dictionary<string, byte[]>>(StringComparer.Ordinal);
        foreach (string n in nodes)
        {
            var map = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            DumpDto d = dumps[n];
            for (int i = 0; i < d.Keys.Count; i++) map[d.Keys[i]] = d.Values[i];
            maps[n] = map;
            long[] v = new long[nreg];
            Array.Fill(v, -1);
            regs[n] = v;
            rep.Nodes[n] = "seq " + d.LatestSeq + ", commit " + d.CommitSeq + ", term " + d.Term + ", " + d.Keys.Count + " keys, digest " + d.DigestHash.ToString("x16", CultureInfo.InvariantCulture);
        }

        // ---- 1. convergence / state machine safety ----
        rep.Run("convergence", s =>
        {
            DumpDto first = dumps[nodes[0]];
            foreach (string n in nodes)
            {
                DumpDto d = dumps[n];
                if (!d.Available) s.Fail(n + ": database unavailable");
                if (d.LatestSeq != first.LatestSeq) s.Fail(n + " latest seq " + d.LatestSeq + " != " + nodes[0] + " " + first.LatestSeq);
                if (d.CommitSeq != d.LatestSeq) s.Fail(n + " commit index " + d.CommitSeq + " != latest seq " + d.LatestSeq);
                if (d.DigestHash != first.DigestHash || d.DigestKeys != first.DigestKeys)
                    s.Fail(n + " digest " + d.DigestHash.ToString("x16", CultureInfo.InvariantCulture) + "/" + d.DigestKeys + " != " + nodes[0] + " " +
                           first.DigestHash.ToString("x16", CultureInfo.InvariantCulture) + "/" + first.DigestKeys);
                foreach (KeyValuePair<string, byte[]> kv in maps[nodes[0]])
                {
                    if (!maps[n].TryGetValue(kv.Key, out byte[]? v)) s.Fail(n + " lacks key " + kv.Key + " that " + nodes[0] + " has");
                    else if (!v.AsSpan().SequenceEqual(kv.Value)) s.Fail(n + " has a different value for " + kv.Key);
                }
                foreach (string k in maps[n].Keys)
                    if (!maps[nodes[0]].ContainsKey(k)) s.Fail(n + " has key " + k + " that " + nodes[0] + " lacks");
            }
            s.Count = first.LatestSeq == 0 ? 0 : (long)first.LatestSeq;
            s.Detail = nodes.Count + " nodes at seq " + first.LatestSeq;
        });

        // decode registers per node (shared by the next checks)
        var decodeErrors = new List<string>();
        var unknownKeys = new List<string>();
        foreach (string n in nodes)
        {
            foreach (KeyValuePair<string, byte[]> kv in maps[n])
            {
                if (kv.Key == Keys.NoOp) continue;
                if (!Keys.TryParse(kv.Key, out char space, out int idx))
                {
                    unknownKeys.Add(n + ": " + kv.Key);
                    continue;
                }
                if (space == 'r')
                {
                    if (idx >= nreg) unknownKeys.Add(n + ": " + kv.Key);
                    else if (Keys.TryDecodeReg(idx, kv.Value, out int ver, out string? err)) regs[n][idx] = ver;
                    else decodeErrors.Add(n + ": " + kv.Key + ": " + err);
                }
                else if ((space == 'b' && idx >= cfg.Accounts) || (space == 'c' && idx >= cfg.Counters)) unknownKeys.Add(n + ": " + kv.Key);
            }
        }

        // ---- 2. no lost acknowledged writes ----
        rep.Run("no-lost-acks", s =>
        {
            foreach (string n in nodes)
            {
                long[] v = regs[n];
                for (int k = 0; k < nreg; k++)
                {
                    long need = Math.Max(maxOk[k], model.RegVersion[k]);
                    if (need < 0) continue;
                    s.Count++;
                    if (v[k] < need)
                        s.Fail(n + ": register " + k + " has version " + v[k] + " but version " + need + " was acknowledged" +
                               (maxOk[k] >= 0 ? " in this epoch" : " (committed at the previous VERIFY)"));
                }
            }
            s.Detail = s.Count + " acknowledged registers x nodes checked";
        });

        // ---- 3. no phantoms (and fail-ed writes never appear) ----
        rep.Run("no-phantoms", s =>
        {
            foreach (string e in decodeErrors) s.Fail("undecodable register value: " + e);
            foreach (string e in unknownKeys) s.Fail("key nobody writes: " + e);
            foreach (string n in nodes)
            {
                long[] v = regs[n];
                for (int k = 0; k < nreg; k++)
                {
                    long ver = v[k];
                    if (ver < 0 || ver <= model.RegVersion[k]) continue; // older ones: monotonic-versions
                    s.Count++;
                    if (!writeOutcome.TryGetValue((k, ver), out OpType outcome)) s.Fail(n + ": register " + k + " version " + ver + " was never written (phantom)");
                    else if (outcome == OpType.Fail) s.Fail(n + ": register " + k + " version " + ver + " comes from a write that failed definitely (NotLeader)");
                }
            }
            s.Detail = s.Count + " register values written in this epoch explained";
        });

        // ---- 4. bank ----
        rep.Run("bank", s =>
        {
            foreach (string n in nodes)
            {
                ulong sum = 0;
                int present = 0;
                for (int i = 0; i < cfg.Accounts; i++)
                {
                    if (maps[n].TryGetValue(Keys.Account(i), out byte[]? b) && Keys.TryU64(b, out ulong x))
                    {
                        sum += x;
                        present++;
                    }
                }
                s.Count++;
                if (present != cfg.Accounts) s.Fail(n + ": " + present + " of " + cfg.Accounts + " accounts present");
                else if (sum != (ulong)cfg.Accounts * 1000) s.Fail(n + ": bank total " + unchecked((long)sum) + " != " + cfg.Accounts * 1000);
            }
            long ok = rep.Ops.GetValueOrDefault("transfer.ok"), info = rep.Ops.GetValueOrDefault("transfer.info");
            s.Detail = "total constant on every node; " + ok + " acknowledged and " + info + " indeterminate transfers this epoch";
        });

        // ---- 5. counters ----
        rep.Run("counters", s =>
        {
            foreach (string n in nodes)
            {
                for (int i = 0; i < cfg.Counters; i++)
                {
                    ulong got = 0;
                    if (maps[n].TryGetValue(Keys.Counter(i), out byte[]? b) && !Keys.TryU64(b, out got)) s.Fail(n + ": counter " + i + " is not a uint64");
                    ulong lo = model.Counters[i] + ctrOk[i], hi = model.Counters[i] + ctrMaybe[i];
                    s.Count++;
                    if (got < lo) s.Fail(n + ": counter " + i + " = " + got + " < " + lo + " (model + acknowledged increments): lost increment");
                    if (got > hi) s.Fail(n + ": counter " + i + " = " + got + " > " + hi + " (model + every non-failed increment): phantom increment");
                }
            }
            s.Detail = "acked <= value <= acked + indeterminate";
        });

        // ---- 6. monotonic per-key versions on every node (vs the previous VERIFY) ----
        rep.Run("monotonic-versions", s =>
        {
            foreach (string n in nodes)
            {
                for (int k = 0; k < nreg; k++)
                {
                    if (model.RegVersion[k] < 0) continue;
                    s.Count++;
                    if (regs[n][k] < model.RegVersion[k]) s.Fail(n + ": register " + k + " went back from version " + model.RegVersion[k] + " (previous VERIFY) to " + regs[n][k]);
                    else if (regs[n][k] == model.RegVersion[k] && maps[n].TryGetValue(Keys.Reg(k), out byte[]? val) && !Keys.TryDecodeReg(k, val, out _, out _))
                        s.Fail(n + ": register " + k + " changed its value without a new version");
                }
            }
        });

        // ---- 7. election safety: <= 1 leader per term, <= 1 vote per (node, term), terms survive restarts ----
        List<NodeEvent> events = NodeEvent.ReadAll(eventsDir);
        rep.Run("election-safety", s =>
        {
            var leaders = new Dictionary<long, HashSet<string>>();
            var votes = new Dictionary<(string, long), HashSet<string>>();
            var maxTerm = new Dictionary<string, long>(StringComparer.Ordinal);
            foreach (NodeEvent e in events)
            {
                long term = e.Term;
                if ((e.Kind == "role" && e.Arg == nameof(RaftRole.Leader)) || (e.Kind == "start" && e.Fields.GetValueOrDefault("role") == nameof(RaftRole.Leader)))
                {
                    if (!leaders.TryGetValue(term, out HashSet<string>? set)) leaders[term] = set = [];
                    set.Add(e.Node);
                }
                if (e.Kind == "vote")
                {
                    if (!votes.TryGetValue((e.Node, term), out HashSet<string>? set)) votes[(e.Node, term)] = set = [];
                    set.Add(e.Fields["cand"]);
                }
                if (e.Kind == "start")
                {
                    long prev = maxTerm.GetValueOrDefault(e.Node, 0);
                    if (term < prev) s.Fail(e.Node + " restarted in term " + term + " after it had reached term " + prev + " (Raft state not durable)");
                }
                if (term >= 0) maxTerm[e.Node] = Math.Max(maxTerm.GetValueOrDefault(e.Node, 0), term);
            }
            foreach (KeyValuePair<long, HashSet<string>> kv in leaders)
                if (kv.Value.Count > 1) s.Fail("term " + kv.Key + " has " + kv.Value.Count + " leaders: " + string.Join(",", kv.Value));
            foreach (KeyValuePair<(string, long), HashSet<string>> kv in votes)
                if (kv.Value.Count > 1) s.Fail(kv.Key.Item1 + " voted for " + string.Join(" and ", kv.Value) + " in term " + kv.Key.Item2);
            s.Count = leaders.Count;
            rep.Stats["terms-with-leader"] = leaders.Count;
            rep.Stats["votes-granted"] = votes.Count;
            s.Detail = leaders.Count + " terms with a leader, " + votes.Count + " (node, term) votes, " + events.Count + " events";
        });

        // ---- 8. log matching / committed prefix agreement ----
        rep.Run("log-matching", s =>
        {
            var termMaps = new Dictionary<string, RaftLogTerms>(StringComparer.Ordinal);
            var bounds = new SortedSet<ulong> { 1 };
            foreach (string n in nodes)
            {
                termMaps[n] = RaftLogTerms.Deserialize(dumps[n].Terms);
                foreach ((ulong start, long _) in termMaps[n].GetRanges()) bounds.Add(start);
            }
            RaftLogTerms prev = RaftLogTerms.Deserialize(model.Terms);
            foreach ((ulong start, long _) in prev.GetRanges()) bounds.Add(start);
            ulong latest = dumps[nodes[0]].LatestSeq;
            foreach (ulong b in bounds)
            {
                foreach (ulong seq in new[] { b, b > 1 ? b - 1 : 1 })
                {
                    if (seq > latest || seq == 0) continue;
                    s.Count++;
                    long t0 = termMaps[nodes[0]].GetTermForSeq(seq);
                    foreach (string n in nodes)
                    {
                        long t = termMaps[n].GetTermForSeq(seq);
                        if (t != t0) s.Fail("entry " + seq + ": term " + t + " on " + n + " but " + t0 + " on " + nodes[0]);
                    }
                    if (seq <= model.LatestSeq && model.Terms.Length > 0)
                    {
                        long tp = prev.GetTermForSeq(seq);
                        if (tp != t0) s.Fail("committed entry " + seq + " had term " + tp + " at the previous VERIFY, now " + t0);
                    }
                }
            }
            if (latest < model.LatestSeq) s.Fail("the committed log shrank from " + model.LatestSeq + " to " + latest);
            s.Detail = "term maps agree at " + s.Count + " boundary entries; committed prefix up to " + model.LatestSeq + " unchanged";
        });

        // ---- 9. reads (online checks + offline recheck of the recorded reads) ----
        rep.Run("reads", s =>
        {
            foreach (string v in violations) s.Fail(v);
            long[] maxInvoked = (long[])model.RegVersion.Clone();
            foreach (Op op in ops)
                if (op.F == OpF.Write && op.Type == OpType.Invoke) maxInvoked[op.Keys[0]] = Math.Max(maxInvoked[op.Keys[0]], op.Vals[0]);
            foreach (Op op in ops)
            {
                if (op.F == OpF.Read && op.Type == OpType.Ok)
                {
                    for (int i = 0; i < op.Keys.Length && i < op.Vals.Length; i++)
                    {
                        s.Count++;
                        if (op.Vals[i] == -3) s.Fail("read " + op.Index + " returned an undecodable value for register " + op.Keys[i]);
                        else if (op.Vals[i] > maxInvoked[op.Keys[i]]) s.Fail("read " + op.Index + " on " + op.Node + " returned register " + op.Keys[i] + " version " + op.Vals[i] + ", never written");
                    }
                }
                else if (op.F == OpF.BankRead && op.Type == OpType.Ok)
                {
                    long sum = 0;
                    foreach (long b in op.Vals) sum += b;
                    if (op.Vals.Length == cfg.Accounts && sum != cfg.Accounts * 1000L) s.Fail("bank read " + op.Index + " on " + op.Node + " saw total " + sum);
                }
            }
            s.Detail = s.Count + " register reads rechecked; stale reads are statistics (no ReadIndex)";
        });
    }

    /// <summary>Folds the converged state into the model for the next epoch.</summary>
    public static void Fold(Model model, RunConfig cfg, DumpDto d, WorkloadState? state)
    {
        var map = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        for (int i = 0; i < d.Keys.Count; i++) map[d.Keys[i]] = d.Values[i];
        for (int k = 0; k < cfg.RegisterKeys; k++)
        {
            long v = -1;
            if (map.TryGetValue(Keys.Reg(k), out byte[]? b) && Keys.TryDecodeReg(k, b, out int ver, out _)) v = ver;
            model.RegVersion[k] = v;
            long next = state?.NextVersion[k] ?? model.NextVersion[k];
            model.NextVersion[k] = Math.Max(next, v + 1);
        }
        for (int i = 0; i < cfg.Counters; i++)
            model.Counters[i] = map.TryGetValue(Keys.Counter(i), out byte[]? b) && Keys.TryU64(b, out ulong x) ? x : 0;
        model.LatestSeq = d.LatestSeq;
        model.Terms = d.Terms;
    }
}
