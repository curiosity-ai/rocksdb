using System.Collections.Concurrent;
using System.Text;
using RocksDb.Managed.Replication.Hosting;
using RocksDb.Managed.Replication.Switchover;

namespace RocksDb.Managed.Replication.Tests.Infrastructure;

/// <summary>
/// Two <see cref="PairNodeHost"/>s ("A" starts primary, "B" replica) on an <see cref="InMemoryNetwork"/>, plus client
/// writers that follow primary hints - the in-process counterpart of rocksdb-sharp's SwitchoverTest coordinator.
/// </summary>
public sealed class PairHarness : IAsyncDisposable
{
    private readonly TempDir _dir = new("pair");
    private readonly ConcurrentDictionary<string, PairNodeHost> _hosts = new();
    private readonly ConcurrentQueue<string> _log = new();

    public InMemoryNetwork Network { get; } = new();
    public InMemoryPairClient AtoB { get; }
    public InMemoryPairClient BtoA { get; }
    public SwitchoverOptions Switchover { get; set; } = new() { CatchUpTimeout = TimeSpan.FromSeconds(3), RpcGrace = TimeSpan.FromSeconds(1) };

    /// <summary>Keys whose write was acknowledged as applied, with the value written.</summary>
    public ConcurrentDictionary<string, string> Acknowledged { get; } = new();

    public PairHarness()
    {
        AtoB = new InMemoryPairClient(Network, "A", "B");
        BtoA = new InMemoryPairClient(Network, "B", "A");
    }

    /// <summary><see cref="PairNodeHostOptions.DisposeDrainTimeout"/> of the hosts started from now on.</summary>
    public TimeSpan DisposeDrainTimeout { get; set; } = TimeSpan.FromSeconds(10);
    /// <summary><see cref="PairNodeHostOptions.ResolveInterval"/> of the hosts started from now on.</summary>
    public TimeSpan ResolveInterval { get; set; } = TimeSpan.FromSeconds(1);
    public string DbPath(string id) => _dir.Sub(id);
    /// <summary>Called (on the logging thread, synchronously) with the node id and every line a host logs.</summary>
    public Action<string, string>? OnLog { get; set; }
    public PairNodeHost this[string id] => _hosts[id];

    public string Peer(string id) => id == "A" ? "B" : "A";

    public async Task<PairNodeHost> StartAsync(string id)
    {
        var host = new PairNodeHost(new PairNodeHostOptions
        {
            NodeId = id,
            PeerId = Peer(id),
            DbPath = _dir.Sub(id),
            InitialRole = id == "A" ? ReplicationRole.Primary : ReplicationRole.Replica,
            DbOptionsFactory = static () => new DbOptions { WalTtlSeconds = 600 },
            Switchover = Switchover,
            MaxChunkBytes = 16 * 1024,
            RetryDelay = TimeSpan.FromMilliseconds(50),
            DisposeDrainTimeout = DisposeDrainTimeout,
            ResolveInterval = ResolveInterval,
            Log = line =>
            {
                _log.Enqueue(line);
                OnLog?.Invoke(id, line);
            },
        }, id == "A" ? AtoB : BtoA);
        await host.StartAsync(CancellationToken.None);
        _hosts[id] = host;
        Network.Register(id, host);
        return host;
    }

    public async Task StopAsync(string id)
    {
        Network.Unregister(id);
        if (_hosts.TryRemove(id, out PairNodeHost? h)) await h.DisposeAsync();
    }

    public string LogText => string.Join("\n", _log);

    /// <summary>Starts <paramref name="count"/> writers; each writes "w{n}-{i}" until the returned source is cancelled.</summary>
    public (CancellationTokenSource Stop, Task Writers) StartWriters(int count)
    {
        var stop = new CancellationTokenSource();
        var tasks = new Task[count];
        for (int w = 0; w < count; w++)
        {
            int writer = w;
            tasks[w] = Task.Run(() => WriterAsync(writer, stop.Token));
        }
        return (stop, Task.WhenAll(tasks));
    }

    private async Task WriterAsync(int writer, CancellationToken stop)
    {
        string target = "A";
        int i = 0;
        while (!stop.IsCancellationRequested)
        {
            string key = "w" + writer + "-" + i.ToString("D6", System.Globalization.CultureInfo.InvariantCulture);
            string value = "value-" + i;
            PairNodeHost? host = Network.Resolve<PairNodeHost>("client", target);
            if (host == null)
            {
                target = Peer(target);
                await Task.Yield();
                continue;
            }
            PairWriteResult r;
            try
            {
                using var batch = new WriteBatch();
                batch.Put(Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes(value));
                r = await host.WriteAsync(batch, TimeSpan.FromSeconds(2), CancellationToken.None);
            }
            catch (Exception)
            {
                // The node went away mid-write: ambiguous, retry the same (idempotent) key elsewhere.
                target = Peer(target);
                continue;
            }
            if (r.Status == PairWriteStatus.Applied)
            {
                Acknowledged[key] = value;
                i++;
                if (i % 16 == 0) await Task.Yield();
            }
            else
            {
                target = r.PrimaryHint ?? Peer(target);
                await Task.Delay(1, CancellationToken.None);
            }
        }
    }

    /// <summary>Waits until the replica holds the primary's latest sequence, then checks equality and acknowledged keys.</summary>
    public async Task AssertConvergedAsync(string primary, string replica)
    {
        PairNodeHost p = _hosts[primary], r = _hosts[replica];
        await WaitUntilAsync(() => !r.IsResyncing, "resync to finish");
        await WaitUntilAsync(() => r.Db.GetLatestSequenceNumber() >= p.Db.GetLatestSequenceNumber(), "replica to catch up\n" + LogText);
        AssertReplicaEquals(p.Db, r.Db, primary + " vs " + replica);
        SortedDictionary<string, string> contents = Contents(p.Db);
        foreach (KeyValuePair<string, string> kv in Acknowledged)
        {
            Assert.IsTrue(contents.TryGetValue(kv.Key, out string? v), "acknowledged write lost: " + kv.Key);
            Assert.AreEqual(kv.Value, v);
        }
        Assert.AreEqual(p.Checksum(), r.Checksum());
    }

    public async ValueTask DisposeAsync()
    {
        foreach (string id in _hosts.Keys) await StopAsync(id);
        _dir.Dispose();
    }
}
