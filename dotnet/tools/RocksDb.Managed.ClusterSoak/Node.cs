using System.Globalization;
using System.Net;
using System.Text;
using System.Text.Json;
using Grpc.Net.Client;
using MagicOnion;
using MagicOnion.Server;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RocksDb.Managed.Cluster;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Replication;
using RocksDb.Managed.Replication.Hosting;
using RocksDb.Managed.Replication.MagicOnion;
using RocksDb.Managed.Soak;

namespace RocksDb.Managed.ClusterSoak;

/// <summary>Configuration of one node process (written by the supervisor as node.json before every start).</summary>
public sealed class NodeConfig
{
    public string NodeId { get; set; } = "";
    /// <summary>Node root: db/ (database, raft/ inside), graveyards of the fault file system.</summary>
    public string Root { get; set; } = "";
    public int ListenPort { get; set; }
    /// <summary>Peer id -> the local proxy port of the link this node -> peer.</summary>
    public Dictionary<string, int> PeerPorts { get; set; } = [];
    public List<string> Members { get; set; } = [];
    public string BootstrapId { get; set; } = "n1";
    public string EventsPath { get; set; } = "";
    public bool FaultFs { get; set; }
    public int ElectionMinMs { get; set; } = 800;
    public int ElectionMaxMs { get; set; } = 1600;
    public int HeartbeatMs { get; set; } = 150;
    public int RpcTimeoutMs { get; set; } = 500;
    public ulong WriteBufferSize { get; set; } = 512 * 1024;
    public ulong WalTtlSeconds { get; set; } = 30;
    /// <summary>Self-test: acknowledge writes as soon as they are in the leader's log (before quorum commit).</summary>
    public bool UnsafeAckEarly { get; set; }
    /// <summary>Self-test: fsyncs of the Raft state (raft/) make nothing durable on the fault file system.</summary>
    public bool UnsafeSkipRaftFsync { get; set; }

    public string DbPath => Path.Combine(Root, "db");
}

/// <summary>Per-process node state shared by the gRPC services.</summary>
public sealed class NodeContext(ClusterNodeHost host, NodeConfig cfg, EventLog events)
{
    public ClusterNodeHost Host { get; } = host;
    public NodeConfig Config { get; } = cfg;
    public EventLog Events { get; } = events;
    public volatile bool AckEarly = cfg.UnsafeAckEarly;
}

/// <summary>
/// The node's event log (role, leader, vote, start, resync), one line per event, appended with one write each so a
/// SIGKILL keeps every line. It lives outside the database directory, so a simulated power loss never touches it.
/// Line format: <c>&lt;unix us&gt; &lt;node&gt; &lt;event&gt; key=value...</c>.
/// </summary>
public sealed class EventLog : IDisposable
{
    private readonly FileStream _fs;
    private readonly string _node;
    private readonly Lock _gate = new();

    public EventLog(string path, string node)
    {
        _node = node;
        _fs = new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.ReadWrite | FileShare.Delete, 1, FileOptions.None);
    }

    public void Write(string evt)
    {
        long us = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1000;
        byte[] line = Encoding.UTF8.GetBytes(us.ToString(CultureInfo.InvariantCulture) + " " + _node + " " + evt + "\n");
        lock (_gate)
        {
            _fs.Write(line);
            _fs.Flush();
        }
    }

    public void Dispose() => _fs.Dispose();
}

/// <summary>
/// A <see cref="TimeProvider"/> whose clock runs at an adjustable rate (clock-skew nemesis): timers created or re-armed
/// while the rate is r fire after due / r real time, and <see cref="GetUtcNow"/> advances r times as fast.
/// </summary>
internal sealed class SkewedTimeProvider : TimeProvider
{
    private readonly Lock _gate = new();
    private DateTimeOffset _anchorVirtual = DateTimeOffset.UtcNow;
    private long _anchorTicks = global::System.Diagnostics.Stopwatch.GetTimestamp();
    private double _rate = 1.0;

    public double Rate
    {
        get
        {
            lock (_gate) return _rate;
        }
        set
        {
            lock (_gate)
            {
                _anchorVirtual = GetUtcNowLocked();
                _anchorTicks = global::System.Diagnostics.Stopwatch.GetTimestamp();
                _rate = value;
            }
        }
    }

    private DateTimeOffset GetUtcNowLocked() =>
        _anchorVirtual + global::System.Diagnostics.Stopwatch.GetElapsedTime(_anchorTicks) * _rate;

    public override DateTimeOffset GetUtcNow()
    {
        lock (_gate) return GetUtcNowLocked();
    }

    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period) =>
        new SkewedTimer(this, System.CreateTimer(callback, state, Scale(dueTime), Scale(period)));

    private TimeSpan Scale(TimeSpan t) => t == Timeout.InfiniteTimeSpan || t <= TimeSpan.Zero ? t : t / Rate;

    private sealed class SkewedTimer(SkewedTimeProvider owner, ITimer inner) : ITimer
    {
        public bool Change(TimeSpan dueTime, TimeSpan period) => inner.Change(owner.Scale(dueTime), owner.Scale(period));
        public void Dispose() => inner.Dispose();
        public ValueTask DisposeAsync() => inner.DisposeAsync();
    }
}

/// <summary>
/// Routes the node's file operations to two in-process fault file systems: one for the database directory, one for
/// its <c>raft/</c> subdirectory (the Raft term/vote state). Each tracks only the files directly in its directory, so a
/// power loss covers both; everything else passes through to the real file system.
/// </summary>
internal sealed class NodeFaultFs : IFileSystem
{
    private readonly LazyFaultFileSystem _db;
    private readonly LazyFaultFileSystem _raft;
    private readonly string _raftDir;

    public NodeFaultFs(string dbDir, string root, Action<string> log)
    {
        Directory.CreateDirectory(dbDir);
        _raftDir = Path.GetFullPath(Path.Combine(dbDir, "raft"));
        Directory.CreateDirectory(_raftDir);
        _db = new LazyFaultFileSystem(dbDir, Path.Combine(root, "grave-db"), log);
        _raft = new LazyFaultFileSystem(_raftDir, Path.Combine(root, "grave-raft"), log);
    }

    public bool SkipRaftSync
    {
        set => _raft.SkipSync = value;
    }

    private IFileSystem For(string path)
    {
        string full = Path.GetFullPath(path);
        string? dir = Path.GetDirectoryName(full);
        return string.Equals(dir, _raftDir, StringComparison.Ordinal) || string.Equals(full.TrimEnd('/'), _raftDir, StringComparison.Ordinal) ? _raft : _db;
    }

    /// <summary>Power loss: stop all file activity, drop everything unsynced in both directories, _exit(77).</summary>
    public void PowerLoss(Action<string> log)
    {
        _db.Freeze(0);
        _raft.Freeze(0);
        var rng = new Random();
        string a = _db.Apply(CacheFault.ClearCache, rng);
        string b = _raft.Apply(CacheFault.ClearCache, rng);
        log("POWER LOSS db: " + a + "; raft: " + b);
        Console.Out.Flush();
        NativeExit.Exit(77);
    }

    public IWritableFile CreateWritable(string path, bool truncate = true) => For(path).CreateWritable(path, truncate);
    public IWritableFile CreateWritable(string path, bool truncate, WritableFileOptions options) => For(path).CreateWritable(path, truncate, options);
    public IWritableFile ReopenWritable(string path, WritableFileOptions options) => For(path).ReopenWritable(path, options);
    public IRandomAccessFile OpenRandomAccess(string path) => For(path).OpenRandomAccess(path);
    public IRandomAccessFile OpenRandomAccess(string path, RandomAccessFileOptions options) => For(path).OpenRandomAccess(path, options);
    public ISequentialFile OpenSequential(string path) => For(path).OpenSequential(path);
    public ISequentialFile OpenSequential(string path, SequentialFileOptions options) => For(path).OpenSequential(path, options);
    public bool FileExists(string path) => For(path).FileExists(path);
    public long GetFileSize(string path) => For(path).GetFileSize(path);
    public bool DirectoryExists(string path) => For(path).DirectoryExists(path);
    public IEnumerable<string> GetChildren(string directory) => For(directory).GetChildren(directory);
    public IDisposable LockFile(string path) => For(path).LockFile(path);
    public DateTimeOffset GetFileModificationTime(string path) => For(path).GetFileModificationTime(path);
    public long GetFreeSpace(string path) => For(path).GetFreeSpace(path);
    public void CreateDirectoryIfMissing(string path) => For(path).CreateDirectoryIfMissing(path);
    public void DeleteDirectory(string path) => For(path).DeleteDirectory(path);
    public void WriteAt(string path, long offset, ReadOnlySpan<byte> data) => For(path).WriteAt(path, offset, data);
    public void DeleteFile(string path) => For(path).DeleteFile(path);
    public void RenameFile(string source, string target) => For(target).RenameFile(source, target);
    public void LinkFile(string source, string target) => For(source).LinkFile(source, target);
    public void SyncDirectory(string path) => For(Path.Combine(path, "x")).SyncDirectory(path);
}

/// <summary>The <c>node</c> sub-command: one cluster member as its own process.</summary>
internal static class NodeMain
{
    public static async Task<int> RunAsync(string configPath)
    {
        NodeConfig cfg = JsonSerializer.Deserialize<NodeConfig>(File.ReadAllText(configPath))!;
        using var events = new EventLog(cfg.EventsPath, cfg.NodeId);
        static void Out(string m) => Console.WriteLine(DateTime.UtcNow.ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture) + " " + m);

        NodeFaultFs? faultFs = null;
        IFileSystem fs = LocalFileSystem.Instance;
        if (cfg.FaultFs)
        {
            faultFs = new NodeFaultFs(cfg.DbPath, cfg.Root, Out);
            faultFs.SkipRaftSync = cfg.UnsafeSkipRaftFsync;
            fs = faultFs;
        }

        var members = new List<ClusterMember>();
        foreach (string m in cfg.Members)
        {
            int port = m == cfg.NodeId ? cfg.ListenPort : cfg.PeerPorts[m];
            members.Add(new ClusterMember(m, "http://127.0.0.1:" + port.ToString(CultureInfo.InvariantCulture)));
        }
        var channelOptions = new GrpcChannelOptions
        {
            InitialReconnectBackoff = TimeSpan.FromMilliseconds(100),
            MaxReconnectBackoff = TimeSpan.FromMilliseconds(500),
        };
        var clients = new Dictionary<string, GrpcClusterClient>(StringComparer.Ordinal);
        foreach (ClusterMember m in members)
            if (m.NodeId != cfg.NodeId) clients[m.NodeId] = new GrpcClusterClient(m, TimeSpan.FromMilliseconds(cfg.RpcTimeoutMs), channelOptions);

        var clock = new SkewedTimeProvider();
        ClusterMember self = members.Find(m => m.NodeId == cfg.NodeId)!;
        IFileSystem dbFs = fs;
        ulong walTtl = cfg.WalTtlSeconds, wbs = cfg.WriteBufferSize;
        var host = new ClusterNodeHost(new ClusterNodeHostOptions
        {
            NodeId = cfg.NodeId,
            Endpoint = self.Endpoint,
            DbPath = cfg.DbPath,
            Members = members,
            BootstrapPrimaryId = cfg.BootstrapId,
            DbOptionsFactory = () => new DbOptions { FileSystem = dbFs, WalTtlSeconds = walTtl },
            DefaultColumnFamilyOptionsFactory = () => new ColumnFamilyOptions { MergeOperator = MergeOperator.UInt64Add, WriteBufferSize = wbs },
            ElectionTimeoutMinMs = cfg.ElectionMinMs,
            ElectionTimeoutMaxMs = cfg.ElectionMaxMs,
            HeartbeatIntervalMs = cfg.HeartbeatMs,
            MaxChunkBytes = 64 * 1024,
            SnapshotChunkBytes = 256 * 1024,
            RetryDelay = TimeSpan.FromMilliseconds(100),
            TimeProvider = clock,
            Log = m =>
            {
                Out(m);
                if (m.Contains("resync", StringComparison.Ordinal) || m.Contains("snapshot from", StringComparison.Ordinal)) events.Write("note " + m.Replace('\n', ' '));
            },
        }, m => clients[m.NodeId]);
        var ctx = new NodeContext(host, cfg, events);

        WebApplicationBuilder builder = WebApplication.CreateSlimBuilder();
        builder.Services.AddGrpc(o => o.MaxReceiveMessageSize = 64 << 20);
        builder.Services.AddMagicOnion();
        builder.Services.AddSingleton(host);
        builder.Services.AddSingleton(ctx);
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(o => o.Listen(IPAddress.Loopback, cfg.ListenPort, lo => lo.Protocols = HttpProtocols.Http2));
        WebApplication app = builder.Build();
        app.MapMagicOnionService([typeof(SoakClusterService), typeof(SoakService)]);
        await app.StartAsync().ConfigureAwait(false);

        await host.StartAsync(CancellationToken.None).ConfigureAwait(false);
        RaftClusterNode node = host.Node;
        events.Write("start term=" + node.CurrentTerm + " role=" + node.Role + " leader=" + (node.CurrentLeaderId ?? "-") + " seq=" +
                     host.Db.GetLatestSequenceNumber() + " mode=" + node.Mode + " pid=" + Environment.ProcessId);
        node.RoleChanged += (_, e) => events.Write("role " + e.Role + " term=" + e.Term);
        node.LeaderChanged += (_, e) => events.Write("leader " + (e.LeaderId ?? "-") + " term=" + e.Term);
        Out("READY " + cfg.NodeId);

        // Commands from the supervisor (stdin).
        while (true)
        {
            string? line = await Console.In.ReadLineAsync().ConfigureAwait(false);
            if (line is null || line == "stop") break;
            string[] p = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            if (p.Length == 0) continue;
            switch (p[0])
            {
                case "crash":
                    if (faultFs is null)
                    {
                        Out("crash requested without the fault file system: SIGKILL semantics");
                        NativeExit.Exit(77);
                    }
                    events.Write("power-loss");
                    faultFs!.PowerLoss(Out);
                    break;
                case "clock":
                    clock.Rate = double.Parse(p[1], CultureInfo.InvariantCulture);
                    events.Write("clock rate=" + p[1]);
                    break;
                case "ack-early":
                    ctx.AckEarly = p.Length > 1 && p[1] == "on";
                    events.Write("unsafe-ack-early " + ctx.AckEarly);
                    break;
            }
        }
        Out("stopping");
        events.Write("stop");
        await host.DisposeAsync().ConfigureAwait(false);
        await app.StopAsync().ConfigureAwait(false);
        await app.DisposeAsync().ConfigureAwait(false);
        foreach (GrpcClusterClient c in clients.Values) c.Dispose();
        Out("stopped");
        return 0;
    }
}

/// <summary>
/// The cluster RPCs of a soak node: the same contract and delegation as the library's <c>ClusterService</c>, plus an
/// event-log line for every granted vote (the vote is persisted by the host before it answers), which the
/// election-safety check reads.
/// </summary>
public sealed class SoakClusterService(ClusterNodeHost host, NodeContext ctx) : ServiceBase<IClusterService>, IClusterService
{
    public UnaryResult<RequestVoteResultDto> RequestVoteAsync(RequestVoteDto request)
    {
        RequestVoteResult r = host.HandleRequestVote(new RequestVotePayload(request.Term, request.CandidateId, request.LastLogSeq, request.LastLogTerm));
        if (r.VoteGranted) ctx.Events.Write("vote term=" + request.Term + " cand=" + request.CandidateId);
        return UnaryResult.FromResult(new RequestVoteResultDto { Term = r.Term, VoteGranted = r.VoteGranted });
    }

    public UnaryResult<AppendEntriesDto> HeartbeatAsync(HeartbeatDto request)
    {
        AppendEntriesResult r = host.HandleHeartbeat(new HeartbeatPayload(request.Term, request.LeaderId, request.PrevLogSeq, request.PrevLogTerm,
            request.LeaderCommit, (ClusterMode)request.LeaderMode));
        return UnaryResult.FromResult(new AppendEntriesDto { Term = r.Term, Success = r.Success, LastSeq = r.LastSeq, Reason = r.Reason });
    }

    public UnaryResult<ClusterStatusDto> GetStatusAsync() => UnaryResult.FromResult(ClusterStatusDto.From(host.GetStatus()));

    public UnaryResult<AttachDto> CheckLogConsistencyAsync(string followerId, ulong latestSeq, long termAtLatest)
    {
        ClusterAttachResult r = host.HandleCheckLogConsistency(latestSeq, termAtLatest);
        return UnaryResult.FromResult(new AttachDto { IsLeader = r.IsLeader, Consistent = r.Consistent, Term = r.Term, Terms = r.Terms });
    }

    public async Task<ServerStreamingResult<WalChunkDto>> SyncUpdatesAsync(string followerId, ulong startSeq)
    {
        ServerStreamingContext<WalChunkDto> stream = GetServerStreamingContext<WalChunkDto>();
        CancellationToken ct = Context.CallContext.CancellationToken;
        try
        {
            await foreach (WalStreamMessage m in host.StreamUpdatesAsync(followerId, startSeq, ct).ConfigureAwait(false))
            {
                await stream.WriteAsync(WalChunkDto.From(m)).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
        }
        return stream.Result();
    }

    public UnaryResult<bool> ReportLastSyncSequenceNumber(string nodeId, ulong seq)
    {
        host.HandleReportProgress(nodeId, seq);
        return UnaryResult.FromResult(true);
    }

    public async UnaryResult<Dictionary<string, string>> GetFileHashesAsync(List<FileInfoDto> candidates)
    {
        IReadOnlyDictionary<string, string> r = await host.HandleGetFileHashesAsync(FileInfoDto.To(candidates), Context.CallContext.CancellationToken)
            .ConfigureAwait(false);
        return new Dictionary<string, string>(r, StringComparer.Ordinal);
    }

    public async Task<ServerStreamingResult<SnapshotChunkDto>> SyncInitialStateAsync(List<FileInfoDto> verifiedInventory)
    {
        ServerStreamingContext<SnapshotChunkDto> stream = GetServerStreamingContext<SnapshotChunkDto>();
        await foreach (SnapshotChunk c in host.StreamSnapshotAsync(FileInfoDto.To(verifiedInventory), Context.CallContext.CancellationToken).ConfigureAwait(false))
        {
            await stream.WriteAsync(SnapshotChunkDto.From(c)).ConfigureAwait(false);
        }
        return stream.Result();
    }

    public UnaryResult<string> GetRaftTermsAsync() => UnaryResult.FromResult(host.HandleGetTerms());

    public async UnaryResult<bool> WriteAsync(string key, string value) =>
        await host.WriteCommittedAsync(Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes(value), TimeSpan.FromSeconds(10), Context.CallContext.CancellationToken)
            .ConfigureAwait(false);

    public UnaryResult<DigestDto> ChecksumAsync() => UnaryResult.FromResult(DigestDto.From(host.Checksum()));
}

/// <summary>Server side of <see cref="ISoakService"/>.</summary>
public sealed class SoakService(ClusterNodeHost host, NodeContext ctx) : ServiceBase<ISoakService>, ISoakService
{
    public async UnaryResult<WriteResp> WriteAsync(WriteReq request)
    {
        using WriteBatch b = WriteBatch.Rent();
        for (int i = 0; i < request.Keys.Count; i++)
        {
            byte[] k = Encoding.UTF8.GetBytes(request.Keys[i]);
            if (request.Kind == WriteKind.Merge) b.Merge(k, request.Values[i]);
            else b.Put(k, request.Values[i]);
        }
        WriteOutcome outcome;
        string? error = null;
        try
        {
            bool early = ctx.AckEarly;
            ClusterWriteStatus s = await host.WriteCommittedAsync(b, early ? TimeSpan.Zero : TimeSpan.FromMilliseconds(request.TimeoutMs), CancellationToken.None)
                .ConfigureAwait(false);
            // The self-test bug: a leader that acknowledges once the entry is in its own log.
            if (early && s is ClusterWriteStatus.TimedOut or ClusterWriteStatus.LeadershipLost) s = ClusterWriteStatus.Committed;
            outcome = (WriteOutcome)(int)s;
        }
        catch (Exception e)
        {
            outcome = WriteOutcome.Error;
            error = e.GetType().Name + ": " + e.Message;
        }
        RaftClusterNode n = host.Node;
        return new WriteResp { Status = outcome, LeaderId = n.CurrentLeaderId, Term = n.CurrentTerm, Error = error };
    }

    public UnaryResult<ReadResp> ReadAsync(List<string> keys)
    {
        RaftClusterNode n = host.Node;
        var resp = new ReadResp { IsLeader = n.Role == RaftRole.Leader, Term = n.CurrentTerm, LeaderId = n.CurrentLeaderId, CommitSeq = n.CommitSeq };
        try
        {
            resp.Available = host.TryUseDatabase((keys, resp), static (db, s) =>
            {
                using Snapshot snap = db.GetSnapshot();
                var ro = new ReadOptions { Snapshot = snap };
                s.resp.LatestSeq = snap.SequenceNumber;
                foreach (string k in s.keys) s.resp.Values.Add(db.Get(ro, Encoding.UTF8.GetBytes(k)));
                return true;
            }, out _) && !host.IsResyncing;
        }
        catch (Exception)
        {
            resp.Available = false;
            resp.Values.Clear();
        }
        return UnaryResult.FromResult(resp);
    }

    public UnaryResult<NodeStatusDto> StatusAsync()
    {
        ClusterNodeStatus s = host.GetStatus();
        return UnaryResult.FromResult(new NodeStatusDto
        {
            NodeId = s.NodeId,
            Role = (int)s.Role,
            Term = s.Term,
            LeaderId = s.LeaderId,
            LatestSeq = s.LatestSeq,
            CommitSeq = s.CommitSeq,
            Mode = (int)s.Mode,
            Resyncing = s.Resyncing,
            Resyncs = s.Resyncs,
            Pid = Environment.ProcessId,
        });
    }

    public UnaryResult<DumpDto> DumpAsync()
    {
        var d = new DumpDto();
        try
        {
            d.Available = !host.IsResyncing && host.TryUseDatabase(d, static (db, d) =>
            {
                using Snapshot snap = db.GetSnapshot();
                d.LatestSeq = snap.SequenceNumber;
                using (Iterator it = db.NewIterator(new ReadOptions { Snapshot = snap }))
                {
                    for (it.SeekToFirst(); it.Valid; it.Next())
                    {
                        d.Keys.Add(Encoding.UTF8.GetString(it.Key));
                        d.Values.Add(it.Value.ToArray());
                    }
                }
                DatabaseDigest dg = DatabaseDigest.Compute(db);
                d.DigestHash = dg.Hash;
                d.DigestKeys = dg.KeyCount;
                return true;
            }, out _);
        }
        catch (Exception)
        {
            d.Available = false;
        }
        d.Terms = host.HandleGetTerms();
        d.Term = host.Node.CurrentTerm;
        d.CommitSeq = host.Node.CommitSeq;
        return UnaryResult.FromResult(d);
    }

    public UnaryResult<string> CheckpointAsync(string dir)
    {
        try
        {
            bool ok = host.TryUseDatabase(dir, static (db, d) => db.CreateCheckpoint(d), out _);
            return UnaryResult.FromResult(ok ? "" : "database unavailable");
        }
        catch (Exception e)
        {
            return UnaryResult.FromResult(e.GetType().Name + ": " + e.Message);
        }
    }

    public async UnaryResult<string> InjectAsync(string kind, string key, byte[] value)
    {
        byte[] k = Encoding.UTF8.GetBytes(key);
        try
        {
            switch (kind)
            {
                case "local-put":
                    return host.TryUseDatabase((k, value), static (db, s) =>
                    {
                        db.Put(s.k, s.value);
                        return true;
                    }, out _) ? "" : "unavailable";
                case "local-delete":
                    return host.TryUseDatabase(k, static (db, s) =>
                    {
                        db.Delete(s);
                        return true;
                    }, out _) ? "" : "unavailable";
                default:
                    using (WriteBatch b = WriteBatch.Rent())
                    {
                        if (kind == "raft-put") b.Put(k, value);
                        else if (kind == "raft-merge") b.Merge(k, value);
                        else if (kind == "raft-delete") b.Delete(k);
                        else return "unknown injection " + kind;
                        ClusterWriteStatus s = await host.WriteCommittedAsync(b, TimeSpan.FromSeconds(5), CancellationToken.None).ConfigureAwait(false);
                        ctx.Events.Write("inject " + kind + " " + key + " -> " + s);
                        return s == ClusterWriteStatus.Committed ? "" : s.ToString();
                    }
            }
        }
        catch (Exception e)
        {
            return e.GetType().Name + ": " + e.Message;
        }
    }
}
