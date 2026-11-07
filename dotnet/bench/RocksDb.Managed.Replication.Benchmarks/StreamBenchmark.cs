using System.Diagnostics;
using System.Globalization;
using RocksDb.Managed.Db;
using RocksDb.Managed.Replication.Hosting;
using RocksDb.Managed.Replication.MagicOnion;

namespace RocksDb.Managed.Replication.Benchmarks;

internal sealed record StreamConfig(string Transport, int Num, int Batch, int ValueSize, int Threads, int Rate, int ChunkBytes, int ApplyParallelism);

internal sealed record StreamResult(StreamConfig Config, double PrimaryOpsPerSec, double ReplicaOpsPerSec, double LagP50Ms, double LagP90Ms,
    double LagP99Ms, double LagMaxMs, double CatchUpMs, long Chunks, double ApplyBusyPercent, long JitMethods);

/// <summary>
/// WAL-stream replication throughput and lag: writers put batches into a primary as fast as they can (or at a fixed
/// total rate), a replica follows the primary's WAL stream (ReplicationPrimary.StreamUpdatesAsync -> transport ->
/// ReplicationConsumer, the pipeline of ReplicaFollower.FollowAsync / rocksdb-sharp's ReplicationTest) and the time from
/// each write's completion on the primary to the replica having applied it is the lag. Both databases are on the local
/// file system in a temporary directory; writes are not synced.
/// </summary>
internal static class StreamBenchmark
{
    public static async Task<StreamResult> RunAsync(StreamConfig config)
    {
        string root = Path.Combine(Path.GetTempPath(), "rocksdb-managed-replbench-" + Guid.NewGuid().ToString("N"));
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

    private static async Task<StreamResult> RunCoreAsync(StreamConfig config, string root)
    {
        // Archive obsolete WALs (the replication setup the hosts use): the stream may lag behind flushes.
        await using RocksDatabase primaryDb = await RocksDatabase.OpenAsync(Path.Combine(root, "primary"),
            new DbOptions { CreateIfMissing = true, WalTtlSeconds = 600 }).ConfigureAwait(false);
        await using RocksDatabase replica = await RocksDatabase.OpenAsync(Path.Combine(root, "replica"), new DbOptions { CreateIfMissing = true })
            .ConfigureAwait(false);
        var primary = new ReplicationPrimary(primaryDb, Path.Combine(root, "checkpoints"), null) { MaxChunkBytes = config.ChunkBytes };

        GrpcNodeServer? server = null;
        IReplicationSourceClient client;
        if (config.Transport == "grpc")
        {
            server = await GrpcNodeServer.StartAsync<ReplicationService, ReplicationPrimary>(primary, CancellationToken.None).ConfigureAwait(false);
            client = new GrpcReplicationClient(server.Endpoint);
        }
        else
        {
            client = new InProcessReplicationClient(primary);
        }

        int writes = (config.Num + config.Batch - 1) / config.Batch;
        ulong finalSeq = (ulong)(writes * config.Batch);
        var writeSeq = new ulong[writes];
        var writeDone = new long[writes];
        // Replica side: (applied-through sequence, time) after each chunk.
        var appliedSeq = new List<ulong>(writes + 16);
        var appliedAt = new List<long>(writes + 16);
        long chunks = 0;
        long applyTicks = 0;

        using var stop = new CancellationTokenSource();
        var reached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Task follower = Task.Run(async () =>
        {
            var consumer = new ReplicationConsumer(replica) { ApplyParallelism = config.ApplyParallelism };
            try
            {
                await foreach (WalStreamMessage m in client.StreamUpdatesAsync(1, stop.Token).ConfigureAwait(false))
                {
                    long t0 = Stopwatch.GetTimestamp();
                    if (m.ResyncRequired || !await consumer.TryIngestChunkAsync(m.Data, CancellationToken.None).ConfigureAwait(false))
                    {
                        reached.TrySetException(new InvalidOperationException("replica diverged: " + m.Reason));
                        return;
                    }
                    long now = Stopwatch.GetTimestamp();
                    applyTicks += now - t0;
                    ulong latest = replica.GetLatestSequenceNumber();
                    appliedSeq.Add(latest);
                    appliedAt.Add(now);
                    chunks++;
                    if (latest >= finalSeq) reached.TrySetResult();
                }
            }
            catch (Exception e) when (e is OperationCanceledException || stop.IsCancellationRequested)
            {
            }
            catch (Exception e)
            {
                reached.TrySetException(e);
            }
        });

        long jitBefore = System.Runtime.JitInfo.GetCompiledMethodCount(false);
        long start = Stopwatch.GetTimestamp();
        var writers = new Task[config.Threads];
        for (int t = 0; t < config.Threads; t++)
        {
            int thread = t;
            writers[t] = Task.Run(async () =>
            {
                byte[] value = new byte[config.ValueSize];
                byte[] key = new byte[16];
                var rnd = new Random(thread + 1);
                rnd.NextBytes(value);
                // Pacing: each writer gets an equal share of the total rate.
                double perWriterInterval = config.Rate > 0 ? Stopwatch.Frequency * (double)config.Threads * config.Batch / config.Rate : 0;
                int n = 0;
                for (int w = thread; w < writes; w += config.Threads, n++)
                {
                    if (perWriterInterval > 0)
                    {
                        long due = start + (long)(n * perWriterInterval);
                        long ahead = due - Stopwatch.GetTimestamp();
                        if (ahead > Stopwatch.Frequency / 500) await Task.Delay(TimeSpan.FromTicks(ahead * TimeSpan.TicksPerSecond / Stopwatch.Frequency)).ConfigureAwait(false);
                    }
                    using WriteBatch b = primaryDb.CreateWriteBatch();
                    for (int i = 0; i < config.Batch; i++)
                    {
                        System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(key, rnd.NextInt64());
                        System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(key.AsSpan(8), w * (long)config.Batch + i);
                        b.Put(key, value);
                    }
                    await primaryDb.WriteAsync(b).ConfigureAwait(false);
                    writeDone[w] = Stopwatch.GetTimestamp();
                    // The batch's own last sequence number (set by the write path), so the lag is exact under concurrency.
                    writeSeq[w] = b.Sequence + (ulong)config.Batch - 1;
                }
            });
        }
        await Task.WhenAll(writers).ConfigureAwait(false);
        long writesEnd = Stopwatch.GetTimestamp();
        using (var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
        {
            await reached.Task.WaitAsync(timeout.Token).ConfigureAwait(false);
        }
        long jitAfter = System.Runtime.JitInfo.GetCompiledMethodCount(false);
        await stop.CancelAsync().ConfigureAwait(false);
        try
        {
            await follower.ConfigureAwait(false);
        }
        catch (Exception)
        {
        }
        if (client is IDisposable d) d.Dispose();
        if (server is not null) await server.DisposeAsync().ConfigureAwait(false);
        if (replica.GetLatestSequenceNumber() != primaryDb.GetLatestSequenceNumber()) throw new InvalidOperationException("replica did not converge");

        // Lag of each write: first chunk applied at or past its sequence number.
        ulong[] seqs = appliedSeq.ToArray();
        long[] ats = appliedAt.ToArray();
        var lags = new double[writes];
        for (int w = 0; w < writes; w++)
        {
            int i = Array.BinarySearch(seqs, writeSeq[w]);
            if (i < 0) i = ~i;
            long dt = ats[Math.Min(i, ats.Length - 1)] - writeDone[w];
            lags[w] = Math.Max(0, dt) * 1000.0 / Stopwatch.Frequency;
        }
        Array.Sort(lags);
        double writeSecs = (writesEnd - start) / (double)Stopwatch.Frequency;
        double replicaSecs = (ats[^1] - start) / (double)Stopwatch.Frequency;
        return new StreamResult(config, config.Num / writeSecs, config.Num / replicaSecs, Percentiles.At(lags, 50), Percentiles.At(lags, 90),
            Percentiles.At(lags, 99), lags[^1], Math.Max(0, ats[^1] - writesEnd) * 1000.0 / Stopwatch.Frequency, chunks,
            100.0 * applyTicks / Math.Max(1, ats[^1] - start), jitAfter - jitBefore);
    }

    public static string Header =>
        "| transport | threads | batch | value | rate (ops/s) | apply threads | ops | primary ops/s | replica ops/s | lag p50 ms | p90 | p99 | max | catch-up ms | chunks | apply busy % | JIT |\n" +
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|";

    public static string Row(StreamResult r)
    {
        StreamConfig c = r.Config;
        return string.Create(CultureInfo.InvariantCulture,
            $"| {c.Transport} | {c.Threads} | {c.Batch} | {c.ValueSize} | {(c.Rate == 0 ? "max" : c.Rate.ToString(CultureInfo.InvariantCulture))} | {c.ApplyParallelism} | {c.Num} | {r.PrimaryOpsPerSec:N0} | {r.ReplicaOpsPerSec:N0} | {r.LagP50Ms:F2} | {r.LagP90Ms:F2} | {r.LagP99Ms:F2} | {r.LagMaxMs:F1} | {r.CatchUpMs:F1} | {r.Chunks} | {r.ApplyBusyPercent:F0} | {r.JitMethods} |");
    }
}
