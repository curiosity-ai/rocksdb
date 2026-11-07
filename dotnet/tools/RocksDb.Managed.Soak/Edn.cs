using System.Globalization;
using System.Text;

namespace RocksDb.Managed.Soak;

/// <summary>
/// export-history: writes the kept histories in Jepsen's EDN shape (one op map per line), so they can be checked
/// independently, e.g. with elle-cli (rw-register) or Jepsen's bank checker:
/// <list type="bullet">
/// <item><c>register.edn</c>: rw-register transactions. Key = writer * keys-per-writer + index, written value =
/// the version; a delete is written as the negated version (unique per key). Reads of an absent key are exported
/// as <c>nil</c> only while no delete of that key appears earlier in the export (otherwise they are left out).</item>
/// <item><c>bank.edn</c>: <c>:transfer</c> ops (<c>{:from a :to b :amount x}</c>) and <c>:read</c> ops (account ->
/// balance as a signed 64-bit integer; balances may go negative, the total is constant).</item>
/// </list>
/// Each worker process gets its own process ids (cycle * 1000 + part * 100 + thread, plus 1,000,000 for each earlier
/// :info op of that thread, since Jepsen never reuses a process after :info), and every op still open when
/// its process died is completed with <c>:info</c> at the end of that process's history, as Jepsen requires.
/// </summary>
internal static class Edn
{
    public static int Export(RunPaths paths, string cycleArg, string format, string outDir)
    {
        if (format != "edn")
        {
            Console.Error.WriteLine("only --format edn is supported");
            return 2;
        }
        var cycles = new List<int>();
        if (cycleArg == "all")
        {
            foreach (string f in Directory.GetFiles(paths.Histories, "cycle-*-p0.bin"))
                cycles.Add(int.Parse(Path.GetFileName(f).AsSpan(6, 6), CultureInfo.InvariantCulture));
            cycles.Sort();
        }
        else cycles.Add(int.Parse(cycleArg, CultureInfo.InvariantCulture));
        RunConfig cfg = RunConfig.Load(paths.RunJson);
        Directory.CreateDirectory(outDir);
        using var reg = new StreamWriter(Path.Combine(outDir, "register.edn"));
        using var bank = new StreamWriter(Path.Combine(outDir, "bank.edn"));
        long regIndex = 0, bankIndex = 0;
        long t0 = -1;
        var deleted = new HashSet<long>();
        foreach (int cycle in cycles)
        {
            List<string> parts = paths.HistoryParts(cycle);
            for (int part = 0; part < parts.Count; part++)
            {
                List<HistoryRecord> recs = HistoryReader.ReadParts([parts[part]]);
                var open = new Dictionary<long, (HistoryRecord Invoke, long Process)>();
                // Jepsen: a process whose op ended :info is never reused; the thread continues as a new process.
                var generation = new Dictionary<int, int>();
                long Proc(int thread) => cycle * 1000L + part * 100L + thread + 1_000_000L * generation.GetValueOrDefault(thread);
                long lastTime = 0;
                foreach (HistoryRecord r in recs)
                {
                    if (t0 < 0) t0 = r.TimeUs;
                    lastTime = r.TimeUs;
                    long time = (r.TimeUs - t0) * 1000;
                    long process;
                    if (r.Phase == Phase.Invoke)
                    {
                        process = Proc(r.Process);
                        open[r.OpId] = (r, process);
                    }
                    else
                    {
                        process = open.Remove(r.OpId, out (HistoryRecord, long Process) o) ? o.Process : Proc(r.Process);
                        if (r.Phase == Phase.Info) generation[r.Process] = generation.GetValueOrDefault(r.Process) + 1;
                    }
                    Emit(r, process, time, cfg.Layout, reg, bank, ref regIndex, ref bankIndex, deleted);
                }
                foreach ((HistoryRecord inv, long process) in open.Values)
                {
                    HistoryRecord info = inv;
                    info.Phase = Phase.Info;
                    Emit(info, process, (lastTime - t0) * 1000, cfg.Layout, reg, bank, ref regIndex, ref bankIndex, deleted);
                }
            }
        }
        Console.WriteLine($"{cycles.Count} cycles: {regIndex} register ops, {bankIndex} bank ops written to {outDir}");
        return 0;
    }

    /// <summary>Reads left out of the export (their key was deleted earlier), so their completions are left out too.</summary>
    private static readonly HashSet<long> s_skipped = [];

    private static string Type(Phase p) => p switch
    {
        Phase.Invoke => ":invoke",
        Phase.Ok => ":ok",
        Phase.Fail => ":fail",
        _ => ":info",
    };

    private static void Emit(HistoryRecord r, long process, long time, Layout l, StreamWriter reg, StreamWriter bank,
        ref long regIndex, ref long bankIndex, HashSet<long> deleted)
    {
        string head(long index) => $"{{:index {index}, :type {Type(r.Phase)}, :process {process}, :time {time}, ";
        switch (r.Type)
        {
            case OpType.RegPut:
            case OpType.RegDel:
            {
                long key = (long)r.Process * l.KeysPerWriter + r.A;
                long value = r.Type == OpType.RegPut ? r.V : -r.V;
                if (r.Type == OpType.RegDel) deleted.Add(key);
                reg.WriteLine(head(regIndex++) + $":f :txn, :value [[:w {key} {value}]]}}");
                break;
            }
            case OpType.RegRead:
            {
                long key = (long)r.A * l.KeysPerWriter + r.B;
                if (r.Phase == Phase.Invoke && deleted.Contains(key))
                {
                    s_skipped.Add(r.OpId);
                    break;
                }
                if (r.Phase != Phase.Invoke && s_skipped.Remove(r.OpId)) break;
                string v = r.Phase == Phase.Ok ? (r.V < 0 ? "nil" : r.V.ToString(CultureInfo.InvariantCulture)) : "nil";
                reg.WriteLine(head(regIndex++) + $":f :txn, :value [[:r {key} {v}]]}}");
                break;
            }
            case OpType.BankBatch:
            case OpType.BankTxn:
                bank.WriteLine(head(bankIndex++) + $":f :transfer, :value {{:from {r.A}, :to {r.B}, :amount {r.V}}}}}");
                break;
            case OpType.BankRead:
            {
                var sb = new StringBuilder();
                if (r.Phase == Phase.Ok && r.Extra is not null)
                {
                    sb.Append('{');
                    for (int a = 0; a * 8 < r.Extra.Length; a++)
                    {
                        if (a > 0) sb.Append(", ");
                        long bal = System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(r.Extra.AsSpan(a * 8));
                        sb.Append(a.ToString(CultureInfo.InvariantCulture)).Append(' ').Append(bal.ToString(CultureInfo.InvariantCulture));
                    }
                    sb.Append('}');
                }
                else sb.Append("nil");
                bank.WriteLine(head(bankIndex++) + $":f :read, :value {sb}}}");
                break;
            }
        }
    }
}
