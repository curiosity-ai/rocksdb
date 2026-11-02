// Primary -> replica WAL replication over gRPC, as separate processes: the managed port of rocksdb-sharp's
// Tests/ReplicationTest.
//
//   ReplicationTest [coordinator] [seconds]        spawn a primary and a replica, run for <seconds>, compare digests
//   ReplicationTest primary <dbPath> <port> <seconds>
//   ReplicationTest replica <dbPath> <primaryPort>
//
// The primary writes a mixed load (puts, deletes, multi-entry batches, periodic flushes) and serves the snapshot and the
// WAL stream (ReplicationService over ReplicationPrimary); the replica pulls the snapshot, follows the stream with
// ReplicaFollower, resyncs from a new snapshot if the primary purged the WAL it needs, and prints a digest on exit.
using System.Diagnostics;
using System.Globalization;
using System.Text;
using RocksDb.Managed;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Replication;
using RocksDb.Managed.Replication.Hosting;
using RocksDb.Managed.Replication.MagicOnion;

string mode = args.Length > 0 ? args[0] : "coordinator";
switch (mode)
{
    case "primary":
        return await RunPrimaryAsync(args[1], int.Parse(args[2], CultureInfo.InvariantCulture), int.Parse(args[3], CultureInfo.InvariantCulture));
    case "replica":
        return await RunReplicaAsync(args[1], int.Parse(args[2], CultureInfo.InvariantCulture));
    default:
        return await RunCoordinatorAsync(args.Length > 1 ? int.Parse(args[1], CultureInfo.InvariantCulture) : 10);
}

static async Task<int> RunCoordinatorAsync(int seconds)
{
    string root = Path.Combine(Path.GetTempPath(), "rocksdb-managed-replication-sample-" + Guid.NewGuid().ToString("N"));
    Directory.CreateDirectory(root);
    int port = GrpcNodeServer.FindFreePort();
    Console.WriteLine("Coordinator: " + root + ", primary port " + port);
    using Process primary = Spawn("primary", Path.Combine(root, "primary"), port.ToString(CultureInfo.InvariantCulture), seconds.ToString(CultureInfo.InvariantCulture));
    string? primaryDigest = null, replicaDigest = null;
    primary.OutputDataReceived += (_, e) => { if (e.Data != null) { Console.WriteLine(e.Data); if (e.Data.StartsWith("DIGEST ", StringComparison.Ordinal)) primaryDigest = e.Data; } };
    primary.BeginOutputReadLine();
    await Task.Delay(1500);
    using Process replica = Spawn("replica", Path.Combine(root, "replica"), port.ToString(CultureInfo.InvariantCulture));
    replica.OutputDataReceived += (_, e) => { if (e.Data != null) { Console.WriteLine(e.Data); if (e.Data.StartsWith("DIGEST ", StringComparison.Ordinal)) replicaDigest = e.Data; } };
    replica.BeginOutputReadLine();
    await replica.WaitForExitAsync();
    await primary.WaitForExitAsync();
    bool ok = primaryDigest != null && primaryDigest == replicaDigest;
    Console.WriteLine(ok ? "Coordinator: OK, replica matches the primary" : "Coordinator: MISMATCH primary=" + primaryDigest + " replica=" + replicaDigest);
    try { Directory.Delete(root, true); } catch (IOException) { }
    return ok ? 0 : 1;
}

static Process Spawn(params string[] args)
{
    var psi = new ProcessStartInfo(Environment.ProcessPath!) { UseShellExecute = false, RedirectStandardOutput = true };
    // Run through the same host: "dotnet Sample.dll" re-launches "dotnet Sample.dll <args>".
    if (Path.GetFileNameWithoutExtension(Environment.ProcessPath) == "dotnet") psi.ArgumentList.Add(System.Reflection.Assembly.GetEntryAssembly()!.Location);
    foreach (string a in args) psi.ArgumentList.Add(a);
    return Process.Start(psi)!;
}

static async Task<int> RunPrimaryAsync(string dbPath, int port, int seconds)
{
    // Archived WALs (TTL) let a replica that fell behind catch up from the WAL; WAL files it has applied are retired
    // early by ReportLastSyncSequenceNumber.
    await using RocksDatabase db = await RocksDatabase.OpenAsync(dbPath, new DbOptions { CreateIfMissing = true, WalTtlSeconds = 60 });
    var delay = new AdaptiveCommitDelayController(1, new AdaptiveCommitDelayOptions { DelayPerLagUnitMs = 5, LagUnit = 1000 });
    var primary = new ReplicationPrimary(db, dbPath + ".checkpoints", delay);
    await using GrpcNodeServer server = await GrpcNodeServer.StartAsync<ReplicationService, ReplicationPrimary>(primary, System.Net.IPAddress.Loopback, port,
        CancellationToken.None);
    Console.WriteLine("[Primary] serving on " + server.Endpoint);
    var sw = Stopwatch.StartNew();
    long i = 0;
    while (sw.Elapsed < TimeSpan.FromSeconds(seconds))
    {
        using WriteBatch b = db.CreateWriteBatch();
        b.Put(Key(i), Encoding.UTF8.GetBytes(Stopwatch.GetTimestamp().ToString(CultureInfo.InvariantCulture)));
        if (i % 7 == 0) b.Delete(Key(i / 2));
        if (i % 3 == 0) b.Put(Key(i + 1_000_000_000), Key(i));
        await db.WriteAsync(b);
        i++;
        if (i % 10_000 == 0)
        {
            Console.WriteLine("[Primary] wrote " + i + " batches, seq " + db.GetLatestSequenceNumber() + ", recommended delay " + delay.GetRecommendedDelayMs() + "ms");
            await primary.DelayIfNeededAsync(CancellationToken.None);
        }
        if (i % 50_000 == 0) await db.FlushAsync();
    }
    // Let the replica drain, then publish the final state.
    using (WriteBatch done = db.CreateWriteBatch())
    {
        done.Put("zzz_writes_finished"u8, "1"u8);
        await db.WriteAsync(done);
    }
    await Task.Delay(3000);
    Console.WriteLine("DIGEST " + Format(DatabaseDigest.Compute(db)));
    return 0;
}

static async Task<int> RunReplicaAsync(string dbPath, int primaryPort)
{
    using var client = new GrpcReplicationClient("http://127.0.0.1:" + primaryPort);
    while (true)
    {
        SnapshotRestoreResult r = await ReplicaFollower.SyncInitialStateAsync(client, LocalFileSystem.Instance, dbPath, [], CancellationToken.None);
        Console.WriteLine("[Replica] initial state: " + r.TransferredFiles + " files, " + r.TransferredBytes + " bytes, checkpoint seq " + r.SequenceNumber);
        await using RocksDatabase db = await RocksDatabase.OpenAsync(dbPath, new DbOptions { CreateIfMissing = true });
        using var stop = new CancellationTokenSource();
        Task watcher = Task.Run(async () =>
        {
            // Stop once the sentinel arrived and nothing new came for a second.
            while (!stop.IsCancellationRequested)
            {
                ulong before = db.GetLatestSequenceNumber();
                await Task.Delay(1000);
                if (db.ContainsKey("zzz_writes_finished"u8) && db.GetLatestSequenceNumber() == before) await stop.CancelAsync();
            }
        });
        try
        {
            long chunks = await ReplicaFollower.FollowAsync(client, db, 0, stop.Token);
            Console.WriteLine("[Replica] stream ended after " + chunks + " chunks");
        }
        catch (ReplicationResyncRequiredException e)
        {
            Console.WriteLine("[Replica] " + e.Message + "; resyncing from a new snapshot");
            await stop.CancelAsync();
            await watcher;
            continue;
        }
        catch (Exception e) when (stop.IsCancellationRequested && e is OperationCanceledException or Grpc.Core.RpcException)
        {
        }
        await watcher;
        Console.WriteLine("DIGEST " + Format(DatabaseDigest.Compute(db)));
        return 0;
    }
}

static byte[] Key(long i) => Encoding.UTF8.GetBytes("key_" + i.ToString("D12", CultureInfo.InvariantCulture));

static string Format(DatabaseDigest d) => "keys=" + d.KeyCount + " hash=" + d.Hash.ToString("x16", CultureInfo.InvariantCulture) + " seq=" + d.LatestSequence;
