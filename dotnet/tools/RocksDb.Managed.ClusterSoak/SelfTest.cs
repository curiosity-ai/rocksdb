using System.Globalization;
using System.Text.Json;
using Grpc.Core;
using RocksDb.Managed.Cluster;

namespace RocksDb.Managed.ClusterSoak;

/// <summary>
/// <c>selftest</c>: proves that the checks bite. It runs a real 3-node cluster (gRPC, proxies, fault file system),
/// passes two control epochs (random nemesis; then power loss of the leader and of a follower), then for each case
/// injects one bug - a real misbehaving node (acks before quorum; Raft state without fsync + power loss) or a
/// manipulated cluster state / history / event log / term map - and expects the named VERIFY step to fail.
/// </summary>
internal static class SelfTest
{
    private sealed record Case(string Name, string[] Expected, Supervisor.Hooks Hooks, bool Control = false);

    public static async Task<int> RunAsync(string? dir)
    {
        string root = dir ?? Path.Combine(Path.GetTempPath(), "rocksdb-cluster-soak-selftest-" + Guid.NewGuid().ToString("N"));
        if (Directory.Exists(root)) Directory.Delete(root, true);
        var paths = new RunPaths(root);
        paths.Create();
        RunConfig cfg = Program.BuildConfig(new Dictionary<string, string>(StringComparer.Ordinal) { ["seed"] = "7" }, smoke: true);
        cfg.KeysPerClient = 40;
        cfg.NativeEvery = 0;
        cfg.ConvergeTimeoutSeconds = 60;
        cfg.MeanFaultIntervalSeconds = 2;
        cfg.WalTtlSeconds = 30;
        cfg.Faults[nameof(FaultKind.RollingRestart)] = 0;
        cfg.Save(paths.RunJson);
        var sup = new Supervisor(cfg, paths, TimeSpan.FromHours(1));
        int code;
        await using (var cluster = new ClusterControl(cfg, paths, sup.Log, null))
        {
            using var schedule = new StreamWriter(new FileStream(paths.Nemesis, FileMode.Append, FileAccess.Write, FileShare.Read));
            Model model = Model.Fresh(cfg);
            string? err = await sup.BringUpAsync(cluster, model, true).ConfigureAwait(false);
            if (err != null)
            {
                sup.Log("SELFTEST: cluster did not start: " + err);
                return 1;
            }
            code = await RunCasesAsync(sup, cluster, model, cfg, paths, schedule).ConfigureAwait(false);
        }
        if (code == 0 && dir is null)
        {
            try
            {
                Directory.Delete(root, true);
            }
            catch (IOException)
            {
            }
        }
        else sup.Log("selftest state kept in " + root);
        return code;
    }

    private static async Task<string?> InjectAsync(ClusterControl c, string node, string kind, string key, byte[] value)
    {
        try
        {
            string r = await c.Nodes[node].Control!.WithDeadline(DateTime.UtcNow.AddSeconds(15)).InjectAsync(kind, key, value);
            return r.Length == 0 ? null : r;
        }
        catch (RpcException e)
        {
            return e.Status.Detail;
        }
    }

    private static async Task InjectViaLeaderAsync(ClusterControl c, string kind, string key, byte[] value)
    {
        for (int attempt = 0; attempt < 50; attempt++)
        {
            foreach (string id in c.Ids)
                if (await InjectAsync(c, id, kind, key, value) is null) return;
            await Task.Delay(200).ConfigureAwait(false);
        }
        throw new InvalidOperationException("injection " + kind + " " + key + " was not committed");
    }

    private static int FirstAcked(Model m)
    {
        for (int k = 0; k < m.RegVersion.Length; k++)
            if (m.RegVersion[k] >= 0) return k;
        return 0;
    }

    private static Model Clone(Model m) => JsonSerializer.Deserialize<Model>(JsonSerializer.Serialize(m))!;

    private static async Task<int> RunCasesAsync(Supervisor sup, ClusterControl c, Model model, RunConfig cfg, RunPaths paths, StreamWriter schedule)
    {
        var cases = new List<Case>();
        Dictionary<string, DumpDto>? lastDumps = null;
        TimeSpan shortRun = TimeSpan.FromSeconds(4);

        cases.Add(new Case("control: random nemesis (partitions, kills, pauses, power loss, clock skew...)", [], new Supervisor.Hooks { RunTime = TimeSpan.FromSeconds(25) }, Control: true));
        cases.Add(new Case("a leader that acknowledges writes before quorum commit (isolated from its peers)", ["no-lost-acks", "counters"], new Supervisor.Hooks
        {
            RunTime = TimeSpan.FromSeconds(12),
            NoNemesis = true,
            DuringRun = async (cl, ct) =>
            {
                await Task.Delay(1500, CancellationToken.None).ConfigureAwait(false);
                string leader = cl.Leader ?? cl.Ids[0];
                cl.Nodes[leader].Send("ack-early on");
                cl.Net.CutBetween([leader], cl.Ids);
                sup.Log("selftest: " + leader + " acks early and is isolated from its peers (clients still reach it)");
                await Task.Delay(7000, CancellationToken.None).ConfigureAwait(false);
                cl.Nodes[leader].Send("ack-early off");
                cl.Net.HealAll();
            },
        }));
        cases.Add(new Case("control: power loss of the leader, then of a follower (fault file system)", [], new Supervisor.Hooks
        {
            RunTime = TimeSpan.FromSeconds(16),
            NoNemesis = true,
            DuringRun = async (cl, ct) =>
            {
                await Task.Delay(2000, CancellationToken.None).ConfigureAwait(false);
                string leader = cl.Leader ?? cl.Ids[0];
                await cl.Nodes[leader].PowerLossAsync().ConfigureAwait(false);
                await Task.Delay(2000, CancellationToken.None).ConfigureAwait(false);
                await cl.StartNodeAsync(leader, TimeSpan.FromSeconds(60)).ConfigureAwait(false);
                await Task.Delay(3000, CancellationToken.None).ConfigureAwait(false);
                string follower = cl.Ids.First(x => x != cl.Leader);
                await cl.Nodes[follower].PowerLossAsync().ConfigureAwait(false);
                await Task.Delay(1500, CancellationToken.None).ConfigureAwait(false);
                await cl.StartNodeAsync(follower, TimeSpan.FromSeconds(60)).ConfigureAwait(false);
            },
        }, Control: true));
        cases.Add(new Case("the history claims an extra acknowledged write", ["no-lost-acks"], new Supervisor.Hooks
        {
            RunTime = shortRun,
            NoNemesis = true,
            OnHistory = ops =>
            {
                var l = new List<Op>(ops);
                int k = 1;
                l.Add(new Op { Type = OpType.Invoke, Process = 999, F = OpF.Write, Keys = [k], Vals = [9_000_000] });
                l.Add(new Op { Type = OpType.Ok, Process = 999, F = OpF.Write, Keys = [k], Vals = [9_000_000] });
                return l;
            },
        }));
        cases.Add(new Case("an acknowledged register deleted through the leader without a history record", ["no-lost-acks"], new Supervisor.Hooks
        {
            RunTime = shortRun,
            NoNemesis = true,
            BeforeConverge = cl => InjectViaLeaderAsync(cl, "raft-delete", Keys.Reg(FirstAcked(model)), []),
        }));
        cases.Add(new Case("a phantom register version (never written by a client)", ["no-phantoms"], new Supervisor.Hooks
        {
            RunTime = shortRun,
            NoNemesis = true,
            BeforeConverge = cl => InjectViaLeaderAsync(cl, "raft-put", Keys.Reg(2), Keys.RegValue(2, 5_000_000)),
        }));
        cases.Add(new Case("the write that produced a register's final value failed definitely (NotLeader)", ["no-phantoms"], new Supervisor.Hooks
        {
            RunTime = shortRun,
            NoNemesis = true,
            OnDumps = d => lastDumps = d,
            OnHistory = ops =>
            {
                var finals = new Dictionary<int, long>();
                DumpDto d0 = lastDumps![c.Ids[0]];
                for (int i = 0; i < d0.Keys.Count; i++)
                    if (Keys.TryParse(d0.Keys[i], out char sp, out int idx) && sp == 'r' && Keys.TryDecodeReg(idx, d0.Values[i], out int v, out _)) finals[idx] = v;
                var l = new List<Op>();
                bool done = false;
                foreach (Op op in ops)
                {
                    if (!done && op.F == OpF.Write && op.Type == OpType.Ok && finals.TryGetValue(op.Keys[0], out long fv) && fv == op.Vals[0])
                    {
                        l.Add(new Op { Index = op.Index, Type = OpType.Fail, Process = op.Process, F = op.F, Keys = op.Keys, Vals = op.Vals, Note = "selftest" });
                        done = true;
                    }
                    else l.Add(op);
                }
                return l;
            },
        }));
        cases.Add(new Case("bank total broken (+1 merged into one account)", ["bank"], new Supervisor.Hooks
        {
            RunTime = shortRun,
            NoNemesis = true,
            BeforeConverge = cl => InjectViaLeaderAsync(cl, "raft-merge", Keys.Account(0), Keys.U64(1)),
            // Repair it, or the next epoch's online bank reads (correctly) stop the run.
            AfterVerify = cl => InjectViaLeaderAsync(cl, "raft-merge", Keys.Account(0), Keys.U64(ulong.MaxValue)),
        }));
        cases.Add(new Case("a counter above every increment", ["counters"], new Supervisor.Hooks
        {
            RunTime = shortRun,
            NoNemesis = true,
            BeforeConverge = cl => InjectViaLeaderAsync(cl, "raft-merge", Keys.Counter(0), Keys.U64(1_000_000)),
        }));
        cases.Add(new Case("a register older than at the previous VERIFY", ["monotonic-versions"], new Supervisor.Hooks
        {
            RunTime = shortRun,
            NoNemesis = true,
            OnModel = m =>
            {
                Model x = Clone(m);
                int k = FirstAcked(x);
                x.RegVersion[k] += 1_000_000;
                return x;
            },
        }));
        string fakeEvents = Path.Combine(paths.Scratch, "events-fake-leader");
        cases.Add(new Case("a second leader in one term (event log)", ["election-safety"], new Supervisor.Hooks
        {
            RunTime = shortRun,
            NoNemesis = true,
            BeforeConverge = _ =>
            {
                if (Directory.Exists(fakeEvents)) Directory.Delete(fakeEvents, true);
                Directory.CreateDirectory(fakeEvents);
                foreach (string f in Directory.GetFiles(paths.Events)) File.Copy(f, Path.Combine(fakeEvents, Path.GetFileName(f)));
                // A leader of some term, and a different node claiming the same term.
                foreach (NodeEvent e in NodeEvent.ReadAll(fakeEvents))
                {
                    if (e.Kind != "role" || e.Arg != nameof(RaftRole.Leader)) continue;
                    string other = c.Ids.First(x => x != e.Node);
                    File.AppendAllText(Path.Combine(fakeEvents, other + ".log"), (e.Us + 1).ToString(CultureInfo.InvariantCulture) + " " + other + " role Leader term=" + e.Term + "\n");
                    break;
                }
                return Task.CompletedTask;
            },
            EventsDir = fakeEvents,
        }));
        cases.Add(new Case("one node's term map disagrees on a committed entry", ["log-matching"], new Supervisor.Hooks
        {
            RunTime = shortRun,
            NoNemesis = true,
            OnDumps = d =>
            {
                DumpDto x = d[c.Ids[1]];
                RaftLogTerms t = RaftLogTerms.Deserialize(x.Terms);
                (ulong start, long term) = t.GetLast();
                t.Record(Math.Max(start + 1, x.LatestSeq / 2 + 1), term + 1000);
                x.Terms = t.Serialize();
            },
        }));
        cases.Add(new Case("a read of a register version nobody wrote", ["reads"], new Supervisor.Hooks
        {
            RunTime = shortRun,
            NoNemesis = true,
            OnHistory = ops =>
            {
                var l = new List<Op>(ops);
                l.Add(new Op { Type = OpType.Invoke, Process = 998, F = OpF.Read, Node = c.Ids[0], Keys = [3], Vals = [] });
                l.Add(new Op { Type = OpType.Ok, Process = 998, F = OpF.Read, Node = c.Ids[0], Keys = [3], Vals = [7_000_000], Note = "follower" });
                return l;
            },
        }));
        cases.Add(new Case("divergence: a write applied directly to one follower's database", ["convergence"], new Supervisor.Hooks
        {
            RunTime = shortRun,
            NoNemesis = true,
            AfterConverge = async cl =>
            {
                string f = cl.Ids.First(x => x != cl.Leader);
                string? e = await InjectAsync(cl, f, "local-put", Keys.Reg(4), Keys.RegValue(4, 6_000_000)).ConfigureAwait(false);
                if (e != null) throw new InvalidOperationException("local-put on " + f + ": " + e);
            },
        }));
        cases.Add(new Case("Raft state (raft/) without fsync on a follower + an election + power loss of that follower", ["election-safety"], new Supervisor.Hooks
        {
            RunTime = TimeSpan.FromSeconds(30),
            NoNemesis = true,
            DuringRun = async (cl, ct) =>
            {
                await Task.Delay(1000, CancellationToken.None).ConfigureAwait(false);
                string x = cl.Ids.First(n => n != cl.Leader);
                await cl.Nodes[x].StopAsync(TimeSpan.FromSeconds(30)).ConfigureAwait(false);
                cl.SkipRaftFsync[x] = true;
                await cl.StartNodeAsync(x, TimeSpan.FromSeconds(60)).ConfigureAwait(false);
                await Task.Delay(1500, CancellationToken.None).ConfigureAwait(false);
                long t0 = (await cl.Nodes[x].StatusAsync(TimeSpan.FromSeconds(2)).ConfigureAwait(false))?.Term ?? 0;
                string leader = cl.Leader ?? cl.Ids.First(n => n != x);
                if (leader == x) leader = cl.Ids.First(n => n != x);
                sup.Log("selftest: " + x + " runs without Raft-state fsync (term " + t0 + "); killing leader " + leader + " to force an election");
                await cl.Nodes[leader].KillAsync().ConfigureAwait(false);
                for (int i = 0; i < 100; i++)
                {
                    NodeStatusDto? s = await cl.Nodes[x].StatusAsync(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
                    if (s != null && s.Term > t0) break;
                    await Task.Delay(200, CancellationToken.None).ConfigureAwait(false);
                }
                await Task.Delay(500, CancellationToken.None).ConfigureAwait(false);
                await cl.Nodes[x].PowerLossAsync().ConfigureAwait(false);
                cl.SkipRaftFsync[x] = false;
                await cl.StartNodeAsync(x, TimeSpan.FromSeconds(60)).ConfigureAwait(false);
                await cl.StartNodeAsync(leader, TimeSpan.FromSeconds(60)).ConfigureAwait(false);
            },
        }));

        var results = new List<(string Name, string Expected, string Failed, bool Ok)>();
        int epoch = 0;
        foreach (Case cs in cases)
        {
            epoch++;
            sup.Log("==== selftest case " + epoch + ": " + cs.Name);
            int r = await sup.EpochAsync(c, model, epoch, schedule, cs.Hooks).ConfigureAwait(false);
            VerifyReport? rep = sup.LastReport;
            if (r != 0 || rep is null || rep.Epoch != epoch)
            {
                results.Add((cs.Name, cs.Control ? "pass" : string.Join("|", cs.Expected), "epoch aborted (exit " + r + ")", false));
                break;
            }
            var failed = rep.Steps.FindAll(s => !s.Pass).ConvertAll(s => s.Name);
            bool ok = cs.Control ? rep.Pass : cs.Expected.Any(e => failed.Contains(e));
            results.Add((cs.Name, cs.Control ? "pass" : string.Join("|", cs.Expected), failed.Count == 0 ? "-" : string.Join(",", failed), ok));
            if (cs.Control && !ok) break;
        }
        sup.Log("==== selftest results");
        bool all = results.Count == cases.Count;
        foreach ((string name, string exp, string failed, bool ok) in results)
        {
            string verdict = name.StartsWith("control", StringComparison.Ordinal) ? (ok ? "  PASSED " : "  FAILED ") : ok ? "  CAUGHT " : "  MISSED ";
            sup.Log(verdict + name + "  (expected: " + exp + "; failing steps: " + failed + ")");
            all &= ok;
        }
        sup.Log(all ? "SELFTEST PASSED: every injected bug was caught and the controls passed" : "SELFTEST FAILED");
        return all ? 0 : 1;
    }
}
