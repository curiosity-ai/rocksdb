using System.Diagnostics;
using System.Globalization;
using System.Text;
using RocksDb.Managed.Db;
using RocksDb.Managed.Replication.Hosting;
using RocksDb.Managed.Replication.MagicOnion;
using RocksDb.Managed.Replication.Switchover;

namespace RocksDb.Managed.Replication.Benchmarks;

internal sealed record SwitchoverConfig(string Transport, int Handoffs, int Writers, int IntervalMs);

internal sealed record SwitchoverRun(int Index, string From, SwitchoverOutcome Outcome, double HeldMs, double TotalMs, double ClientGapMs);

/// <summary>
/// Planned switchover cost: a primary/replica pair (<see cref="PairNodeHost"/>, the hosts of rocksdb-sharp's
/// SwitchoverTest) in one process over the in-memory transport or MagicOnion/gRPC on loopback, client writers that follow
/// primary hints, and repeated handoffs back and forth. Reports, per handoff, how long the primary held writes
/// (<see cref="SwitchoverResult.WritesHeld"/>), the whole HandOffAsync call, and the longest gap between two acknowledged
/// client writes around it. The first handoff of the process is the first promote either node serves or sends.
/// </summary>
internal static class SwitchoverBenchmark
{
    public static async Task<List<SwitchoverRun>> RunAsync(SwitchoverConfig config)
    {
        string root = Path.Combine(Path.GetTempPath(), "rocksdb-managed-swbench-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        try
        {
            return await RunCoreAsync(config, root).ConfigureAwait(false);
        }
        finally
        {
            try
            {
                Directory.Delete(root, recursive: true);
            }
            catch (IOException)
            {
            }
        }
    }

    private static async Task<List<SwitchoverRun>> RunCoreAsync(SwitchoverConfig config, string root)
    {
        bool grpc = config.Transport == "grpc";
        var hosts = new Dictionary<string, PairNodeHost>(StringComparer.Ordinal);
        int portA = grpc ? GrpcNodeServer.FindFreePort() : 0, portB = grpc ? GrpcNodeServer.FindFreePort() : 0;
        GrpcPairClient? toA = grpc ? new GrpcPairClient("A", "http://127.0.0.1:" + portA, TimeSpan.FromSeconds(2)) : null;
        GrpcPairClient? toB = grpc ? new GrpcPairClient("B", "http://127.0.0.1:" + portB, TimeSpan.FromSeconds(2)) : null;
        PairNodeHost NewHost(string id)
        {
            string peer = id == "A" ? "B" : "A";
            IPairNodeClient client = grpc ? (id == "A" ? toB! : toA!) : new InProcessPairClient(peer, () => hosts[peer]);
            return new PairNodeHost(new PairNodeHostOptions
            {
                NodeId = id,
                PeerId = peer,
                DbPath = Path.Combine(root, id),
                InitialRole = id == "A" ? ReplicationRole.Primary : ReplicationRole.Replica,
                DbOptionsFactory = static () => new DbOptions { WalTtlSeconds = 600 },
                RetryDelay = TimeSpan.FromMilliseconds(50),
            }, client);
        }
        hosts["A"] = NewHost("A");
        hosts["B"] = NewHost("B");
        var servers = new List<GrpcNodeServer>();
        try
        {
            if (grpc)
            {
                servers.Add(await GrpcNodeServer.StartAsync<PairService, PairNodeHost>(hosts["A"], System.Net.IPAddress.Loopback, portA, CancellationToken.None)
                    .ConfigureAwait(false));
                servers.Add(await GrpcNodeServer.StartAsync<PairService, PairNodeHost>(hosts["B"], System.Net.IPAddress.Loopback, portB, CancellationToken.None)
                    .ConfigureAwait(false));
            }
            await hosts["A"].StartAsync(CancellationToken.None).ConfigureAwait(false);
            await hosts["B"].StartAsync(CancellationToken.None).ConfigureAwait(false);

            // Client writers: unique keys, follow the primary hint; every acknowledged write is time-stamped.
            using var stop = new CancellationTokenSource();
            var acks = new List<long>[config.Writers];
            var writers = new Task[config.Writers];
            for (int w = 0; w < config.Writers; w++)
            {
                int writer = w;
                acks[w] = new List<long>(1 << 16);
                writers[w] = Task.Run(async () =>
                {
                    string target = "A";
                    for (int i = 0; !stop.IsCancellationRequested;)
                    {
                        string key = "w" + writer.ToString(CultureInfo.InvariantCulture) + "-" + i.ToString("D8", CultureInfo.InvariantCulture);
                        bool applied;
                        string? hint;
                        try
                        {
                            if (grpc)
                            {
                                PairWriteResultDto r = await (target == "A" ? toA! : toB!).Service.WriteAsync(key, "v");
                                applied = r.Applied;
                                hint = r.PrimaryHint;
                            }
                            else
                            {
                                using var b = new WriteBatch();
                                b.Put(Encoding.UTF8.GetBytes(key), "v"u8);
                                PairWriteResult r = await hosts[target].WriteAsync(b, TimeSpan.FromSeconds(5), CancellationToken.None).ConfigureAwait(false);
                                applied = r.Status == PairWriteStatus.Applied;
                                hint = r.PrimaryHint;
                            }
                        }
                        catch (Exception e) when (e is Grpc.Core.RpcException or RocksDbException or ObjectDisposedException)
                        {
                            applied = false;
                            hint = null;
                        }
                        if (applied)
                        {
                            acks[writer].Add(Stopwatch.GetTimestamp());
                            i++;
                        }
                        else
                        {
                            target = hint ?? (target == "A" ? "B" : "A");
                        }
                    }
                });
            }

            var runs = new List<SwitchoverRun>();
            var windows = new List<(long Start, long End)>();
            string primary = "A";
            for (int h = 0; h < config.Handoffs; h++)
            {
                await Task.Delay(config.IntervalMs).ConfigureAwait(false);
                // Let the replica catch up first (the lag check would refuse otherwise).
                await WaitCaughtUpAsync(hosts[primary], hosts[primary == "A" ? "B" : "A"]).ConfigureAwait(false);
                long t0 = Stopwatch.GetTimestamp();
                SwitchoverResult r = await hosts[primary].HandOffAsync(CancellationToken.None).ConfigureAwait(false);
                long t1 = Stopwatch.GetTimestamp();
                windows.Add((t0, t1));
                runs.Add(new SwitchoverRun(h, primary, r.Outcome, r.WritesHeld.TotalMilliseconds, (t1 - t0) * 1000.0 / Stopwatch.Frequency, 0));
                if (r.Outcome == SwitchoverOutcome.Completed) primary = primary == "A" ? "B" : "A";
            }
            await Task.Delay(config.IntervalMs).ConfigureAwait(false);
            await stop.CancelAsync().ConfigureAwait(false);
            await Task.WhenAll(writers).ConfigureAwait(false);

            // Longest interval between consecutive acknowledged writes that overlaps each handoff.
            var all = new List<long>();
            foreach (List<long> a in acks) all.AddRange(a);
            all.Sort();
            for (int i = 0; i < runs.Count; i++)
            {
                (long s, long e) = windows[i];
                long gap = 0;
                for (int j = 1; j < all.Count; j++)
                {
                    if (all[j] < s || all[j - 1] > e) continue;
                    gap = Math.Max(gap, all[j] - all[j - 1]);
                }
                runs[i] = runs[i] with { ClientGapMs = gap * 1000.0 / Stopwatch.Frequency };
            }
            return runs;
        }
        finally
        {
            foreach (PairNodeHost host in hosts.Values) await host.DisposeAsync().ConfigureAwait(false);
            foreach (GrpcNodeServer s in servers) await s.DisposeAsync().ConfigureAwait(false);
            toA?.Dispose();
            toB?.Dispose();
        }
    }

    private static async Task WaitCaughtUpAsync(PairNodeHost primary, PairNodeHost replica)
    {
        for (int i = 0; i < 2000; i++)
        {
            ReplicationNodeStatus? p = primary.HandleGetStatus(), r = replica.HandleGetStatus();
            if (p is not null && r is not null && r.Role == ReplicationRole.Replica && r.PrimaryId == p.NodeId && p.LatestSeq - Math.Min(p.LatestSeq, r.LatestSeq) < 1000) return;
            await Task.Delay(5).ConfigureAwait(false);
        }
        throw new TimeoutException("the replica did not catch up");
    }

    public static string Summary(SwitchoverConfig config, List<SwitchoverRun> runs)
    {
        var sb = new StringBuilder();
        SwitchoverRun first = runs[0];
        var warmHeld = new List<double>();
        var warmGap = new List<double>();
        int completed = 0;
        foreach (SwitchoverRun r in runs)
        {
            if (r.Outcome == SwitchoverOutcome.Completed) completed++;
            if (r.Index == 0) continue;
            warmHeld.Add(r.HeldMs);
            warmGap.Add(r.ClientGapMs);
        }
        double[] held = warmHeld.ToArray(), gap = warmGap.ToArray();
        Array.Sort(held);
        Array.Sort(gap);
        sb.Append(CultureInfo.InvariantCulture,
            $"| {config.Transport} | {config.Writers} | {runs.Count} ({completed} completed) | {first.HeldMs:F1} / {first.ClientGapMs:F1} | " +
            $"{Percentiles.At(held, 50):F1} | {Percentiles.At(held, 90):F1} | {(held.Length > 0 ? held[^1] : double.NaN):F1} | {Percentiles.At(gap, 50):F1} | {(gap.Length > 0 ? gap[^1] : double.NaN):F1} |");
        return sb.ToString();
    }

    public static string Header =>
        "| transport | writers | handoffs | first handoff: held / client gap ms | held p50 ms | p90 | max | client gap p50 ms | max |\n" +
        "|---|---|---|---|---|---|---|---|---|";
}
