using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using RocksDb.Managed.Cluster;
using RocksDb.Managed.Db;
using RocksDb.Managed.Replication;
using RocksDb.Managed.Replication.Hosting;
using RocksDb.Managed.Replication.Switchover;

namespace RocksDb.Managed.AllocationProfile;

/// <summary>
/// WAL tailing (GetUpdatesSince / TailUpdatesAsync, applying batches to another DB, WriteBatch readers and handlers),
/// secondary and read-only instances, and the replication layer (WalTailer, ReplicationConsumer, the primary -> replica
/// stream of ReplicationPrimary / ReplicaFollower, PairNodeHost and ClusterNodeHost over a pass-through in-process
/// transport, Raft heartbeat / progress / commit paths).
/// <para>
/// The in-process clients below hand the source's enumerators to the follower directly (no channel, no copy): a
/// <see cref="WalStreamMessage"/> is consumed before the stream moves on, so the numbers are our code's cost only (a
/// real transport adds its own serialization). "lockstep" scenarios write one batch and wait until the replica (or the
/// quorum) applied it, so every op is one replicated batch, one wait on each side and one progress report;
/// "pipelined" ones wait every 100 writes, so the tailer merges batches into fewer chunks.
/// </para>
/// </summary>
internal static partial class Scenarios
{
    private const int ReplKeySpace = 200_000;
    // Host DBs: flush at 8 MiB of memtables, so the measured window recycles memtable arenas (steady state) instead of
    // growing 64 MiB write buffers.
    private const ulong HostWriteBufferBytes = 8UL << 20;

    private static void AddReplication(List<Scenario> list)
    {
        AddWalTailing(list);
        AddSecondary(list);
        AddReplicationLayer(list);
        AddReplicationHosts(list);
        AddRaft(list);
    }

    private static DbOptions ReplDbOptions(bool archive) =>
        new() { CreateIfMissing = true, CreateMissingColumnFamilies = true, WalTtlSeconds = archive ? 600UL : 0UL };

    /// <summary>A serialized one-Put write batch (header + kTypeValue + key + value); the key is at <see cref="ChunkKeyOffset"/>.</summary>
    private static byte[] OnePutChunk(ReadOnlySpan<byte> value)
    {
        using WriteBatch b = WriteBatch.Rent();
        b.Put(ScenarioContext.Key(0), value);
        return b.Data.ToArray();
    }

    // header (12) + tag (1) + varint32 key length (1)
    private const int ChunkKeyOffset = WriteBatch.HeaderSize + 2;

    // ------------------------------------------------------------------------------------------------ WAL tailing

    private static void AddWalTailing(List<Scenario> list)
    {
        byte[] value = ScenarioContext.Value(ScenarioContext.ValueSize);

        list.Add(S("wal.tail.next", "wal-tail", PathKind.Hot, "Put + Next on a held GetUpdatesSince iterator (reads the new batch)", ctx =>
        {
            RocksDatabase db = ctx.OpenDb("db", ReplDbOptions(archive: false));
            byte[][] keys = ScenarioContext.Keys(ReplKeySpace);
            db.Put(keys[0], value);
            TransactionLogIterator it = ctx.Own(db.GetUpdatesSince(1));
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    db.Put(keys[c.Next()], value);
                    it.Next();
                    if (!it.Valid || it.Current.Count != 1) throw new InvalidOperationException("tail lost the batch: " + it.Error?.Message);
                }
            }));
        }));
        list.Add(S("wal.tail.wait-iterator-async", "wal-tail", PathKind.Hot,
            "pending TransactionLogIterator.MoveNextTailingAsync(ct), then PutAsync wakes it (one wait per batch)", ctx =>
        {
            RocksDatabase db = ctx.OpenDb("db", ReplDbOptions(archive: false));
            byte[][] keys = ScenarioContext.Keys(ReplKeySpace);
            db.Put(keys[0], value);
            TransactionLogIterator it = ctx.Own(db.GetUpdatesSince(1));
            CancellationTokenSource cts = ctx.Own(new CancellationTokenSource());
            var c = new Cursor(keys.Length);
            return Done(Runner.Async([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int n) =>
            {
                for (int i = 0; i < n; i++)
                {
                    ValueTask<bool> next = it.MoveNextTailingAsync(cts.Token);
                    await db.PutAsync(keys[c.Next()], value);
                    if (!await next) throw new InvalidOperationException("tail ended: " + it.Error?.Message);
                }
            }));
        }));
        list.Add(S("wal.tail.tailupdates-async", "wal-tail", PathKind.Hot,
            "pending TailUpdatesAsync(seq, ct) MoveNextAsync, then PutAsync wakes it (one wait per batch)", ctx =>
        {
            RocksDatabase db = ctx.OpenDb("db", ReplDbOptions(archive: false));
            byte[][] keys = ScenarioContext.Keys(ReplKeySpace);
            db.Put(keys[0], value);
            CancellationTokenSource cts = ctx.Own(new CancellationTokenSource());
            IAsyncEnumerator<WalBatch> e = ctx.Own(db.TailUpdatesAsync(1, cts.Token).GetAsyncEnumerator(cts.Token));
            if (!e.MoveNextAsync().AsTask().GetAwaiter().GetResult()) throw new InvalidOperationException("no first batch");
            var c = new Cursor(keys.Length);
            return Done(Runner.Async([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int n) =>
            {
                for (int i = 0; i < n; i++)
                {
                    ValueTask<bool> next = e.MoveNextAsync();
                    await db.PutAsync(keys[c.Next()], value);
                    if (!await next || e.Current.Count != 1) throw new InvalidOperationException("tail ended");
                }
            }));
        }));
        list.Add(S("wal.tail.tailupdates-batched", "wal-tail", PathKind.Hot,
            "TailUpdatesAsync MoveNextAsync over 100 batches already written (no waits)", ctx =>
        {
            RocksDatabase db = ctx.OpenDb("db", ReplDbOptions(archive: false));
            byte[][] keys = ScenarioContext.Keys(ReplKeySpace);
            db.Put(keys[0], value);
            IAsyncEnumerator<WalBatch> e = ctx.Own(db.TailUpdatesAsync(1, CancellationToken.None).GetAsyncEnumerator());
            if (!e.MoveNextAsync().AsTask().GetAwaiter().GetResult()) throw new InvalidOperationException("no first batch");
            var c = new Cursor(keys.Length);
            return Done(Runner.Async([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int n) =>
            {
                for (int i = 0; i < n; i += 100)
                {
                    for (int j = 0; j < 100; j++) db.Put(keys[c.Next()], value);
                    for (int j = 0; j < 100; j++)
                    {
                        if (!await e.MoveNextAsync()) throw new InvalidOperationException("tail ended");
                    }
                }
            }));
        }, notes: "1 op = 1 batch (Put + MoveNextAsync), chunks of 100."));
        list.Add(S("wal.getupdatessince.new", "wal-tail", PathKind.Warm,
            "Put + GetUpdatesSince(latest) + read one batch + Dispose (a poll that opens an iterator per call)", ctx =>
        {
            RocksDatabase db = ctx.OpenDb("db", ReplDbOptions(archive: false));
            byte[][] keys = ScenarioContext.Keys(ReplKeySpace);
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    db.Put(keys[c.Next()], value);
                    using TransactionLogIterator it = db.GetUpdatesSince(db.GetLatestSequenceNumber());
                    if (!it.Valid) throw new InvalidOperationException("no batch");
                }
            }));
        }, chunk: 200));
        list.Add(S("wal.apply.towritebatch", "wal-tail", PathKind.Hot,
            "Put + tail Next + WalBatch.ToWriteBatch + replica.Write (apply to another DB)", ctx =>
        {
            RocksDatabase db = ctx.OpenDb("db", ReplDbOptions(archive: false));
            RocksDatabase replica = ctx.OpenDb("replica", ReplDbOptions(archive: false));
            byte[][] keys = ScenarioContext.Keys(ReplKeySpace);
            db.Put(keys[0], value);
            TransactionLogIterator it = ctx.Own(db.GetUpdatesSince(1));
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    db.Put(keys[c.Next()], value);
                    it.Next();
                    using WriteBatch b = it.Current.ToWriteBatch();
                    replica.Write(b);
                }
            }));
        }));
        list.Add(S("wal.apply.consumer", "wal-tail", PathKind.Hot, "Put + tail Next + ReplicationConsumer.IngestChunk(batch.Span)", ctx =>
        {
            RocksDatabase db = ctx.OpenDb("db", ReplDbOptions(archive: false));
            RocksDatabase replica = ctx.OpenDb("replica", ReplDbOptions(archive: false));
            var consumer = new ReplicationConsumer(replica);
            byte[][] keys = ScenarioContext.Keys(ReplKeySpace);
            db.Put(keys[0], value);
            TransactionLogIterator it = ctx.Own(db.GetUpdatesSince(1));
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    db.Put(keys[c.Next()], value);
                    it.Next();
                    consumer.IngestChunk(it.Current.Span);
                }
            }));
        }));
        list.Add(S("wal.reader.foreach", "wal-tail", PathKind.Hot, "foreach WriteBatchReader over a 10-record WalBatch (no I/O)", ctx =>
        {
            byte[] rep = TenPutRep(value);
            var batch = new WalBatch(rep);
            long sink = 0;
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    foreach (WriteBatchEntry e in batch.GetReader()) sink += e.Key.Length + e.Value.Length;
                }
                if (sink == 0) throw new InvalidOperationException();
            }));
        }));
        list.Add(S("wal.reader.handler", "wal-tail", PathKind.Hot, "WriteBatch.Iterate(rep, ref struct handler) over a 10-record WalBatch (no I/O)", ctx =>
        {
            byte[] rep = TenPutRep(value);
            var batch = new WalBatch(rep);
            var h = new CountingHandler();
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++) WriteBatch.Iterate(batch.Span, ref h);
                if (h.Bytes == 0) throw new InvalidOperationException();
            }));
        }));
    }

    private static byte[] TenPutRep(ReadOnlySpan<byte> value)
    {
        using WriteBatch b = WriteBatch.Rent();
        for (int i = 0; i < 10; i++) b.Put(ScenarioContext.Key(i), value);
        b.Sequence = 1;
        return b.Data.ToArray();
    }

    private struct CountingHandler : IWriteBatchHandler
    {
        public long Bytes;

        public bool Continue() => true;
        public void Put(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => Bytes += key.Length + value.Length;
        public void TimedPut(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ulong writeUnixTime) { }
        public void PutEntity(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> entity) { }
        public void Delete(uint columnFamilyId, ReadOnlySpan<byte> key) { }
        public void SingleDelete(uint columnFamilyId, ReadOnlySpan<byte> key) { }
        public void DeleteRange(uint columnFamilyId, ReadOnlySpan<byte> beginKey, ReadOnlySpan<byte> endKey) { }
        public void Merge(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) { }
        public void PutBlobIndex(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> blobIndex) { }
        public void LogData(ReadOnlySpan<byte> blob) { }
        public void MarkBeginPrepare(bool unprepare) { }
        public void MarkEndPrepare(ReadOnlySpan<byte> xid) { }
        public void MarkNoop(bool emptyBatch) { }
        public void MarkCommit(ReadOnlySpan<byte> xid) { }
        public void MarkCommitWithTimestamp(ReadOnlySpan<byte> xid, ReadOnlySpan<byte> commitTimestamp) { }
        public void MarkRollback(ReadOnlySpan<byte> xid) { }
    }

    // ------------------------------------------------------------------------------------------------ secondary / read-only

    private static void AddSecondary(List<Scenario> list)
    {
        byte[] value = ScenarioContext.Value(ScenarioContext.ValueSize);
        const int Filled = 100_000;

        // Primary with Filled keys flushed to SSTs plus 1000 in the memtable / WAL; secondary caught up.
        static (RocksDatabase Primary, RocksDatabase Secondary, byte[][] Keys) OpenPair(ScenarioContext ctx, byte[] value)
        {
            string dir = ctx.NewDirectory("primary");
            RocksDatabase primary = ctx.Own(RocksDatabase.Open(dir, ReplDbOptions(archive: false), ScenarioContext.DefaultCfOptions()));
            byte[][] keys = ScenarioContext.Keys(Filled);
            ScenarioContext.Fill(primary, keys, value, flush: true, compact: true);
            for (int i = 0; i < 1000; i++) primary.Put(keys[i], value);
            RocksDatabase secondary = ctx.Own(RocksDatabase.OpenAsSecondary(dir, ctx.NewDirectory("secondary"), new DbOptions(),
                [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, ScenarioContext.DefaultCfOptions())]));
            secondary.TryCatchUpWithPrimary();
            return (primary, secondary, keys);
        }

        list.Add(S("secondary.catchup", "secondary", PathKind.Warm, "10 Put on the primary + TryCatchUpWithPrimary", ctx =>
        {
            (RocksDatabase primary, RocksDatabase secondary, byte[][] keys) = OpenPair(ctx, value);
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    for (int j = 0; j < 10; j++) primary.Put(keys[c.Next()], value);
                    secondary.TryCatchUpWithPrimary();
                }
            }));
        }, chunk: 100));
        list.Add(S("secondary.catchup-async", "secondary", PathKind.Warm, "10 Put on the primary + await TryCatchUpWithPrimaryAsync", ctx =>
        {
            (RocksDatabase primary, RocksDatabase secondary, byte[][] keys) = OpenPair(ctx, value);
            var c = new Cursor(keys.Length);
            return Done(Runner.Async([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int n) =>
            {
                for (int i = 0; i < n; i++)
                {
                    for (int j = 0; j < 10; j++) primary.Put(keys[c.Next()], value);
                    await secondary.TryCatchUpWithPrimaryAsync();
                }
            }));
        }, chunk: 100));
        list.Add(S("secondary.catchup-idle", "secondary", PathKind.Warm, "TryCatchUpWithPrimary with nothing new (an idle poll)", ctx =>
        {
            (RocksDatabase _, RocksDatabase secondary, byte[][] _) = OpenPair(ctx, value);
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++) secondary.TryCatchUpWithPrimary();
            }));
        }, chunk: 100));
        list.Add(S("secondary.get", "secondary", PathKind.Hot, "TryGet(key, Span) on a caught-up secondary (memtable + cached SST)", ctx =>
        {
            (RocksDatabase _, RocksDatabase secondary, byte[][] keys) = OpenPair(ctx, value);
            byte[][] shuffled = ScenarioContext.Shuffled(keys);
            ScenarioContext.WarmCache(secondary, shuffled);
            var c = new Cursor(shuffled.Length);
            return Done(Runner.Sync(n =>
            {
                Span<byte> buf = stackalloc byte[256];
                for (int i = 0; i < n; i++)
                {
                    if (!secondary.TryGet(shuffled[c.Next()], buf, out _)) throw new InvalidOperationException("missing");
                }
            }));
        }));
        list.Add(S("secondary.get-async", "secondary", PathKind.Hot, "await GetPooledAsync(key) on a caught-up secondary", ctx =>
        {
            (RocksDatabase _, RocksDatabase secondary, byte[][] keys) = OpenPair(ctx, value);
            byte[][] shuffled = ScenarioContext.Shuffled(keys);
            ScenarioContext.WarmCache(secondary, shuffled);
            var c = new Cursor(shuffled.Length);
            return Done(Runner.Async([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int n) =>
            {
                for (int i = 0; i < n; i++)
                {
                    using PooledValue? v = await secondary.GetPooledAsync(shuffled[c.Next()]);
                    if (v is null) throw new InvalidOperationException("missing");
                }
            }));
        }));
        list.Add(S("secondary.iter.seek-next10", "secondary", PathKind.Hot, "NewIterator + Seek + 10 Next + Dispose on a secondary", ctx =>
        {
            (RocksDatabase _, RocksDatabase secondary, byte[][] keys) = OpenPair(ctx, value);
            byte[][] shuffled = ScenarioContext.Shuffled(keys);
            ScenarioContext.WarmCache(secondary, shuffled);
            var c = new Cursor(shuffled.Length);
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    using Iterator it = secondary.NewIterator();
                    it.Seek(shuffled[c.Next()]);
                    for (int j = 0; j < 10 && it.Valid; j++) it.Next();
                }
            }));
        }));
        list.Add(S("readonly.get", "secondary", PathKind.Hot, "TryGet(key, Span) on a read-only instance (cached SST)", ctx =>
        {
            string dir = ctx.NewDirectory("db");
            byte[][] keys = ScenarioContext.Keys(Filled);
            using (RocksDatabase w = RocksDatabase.Open(dir, ReplDbOptions(archive: false), ScenarioContext.DefaultCfOptions()))
            {
                ScenarioContext.Fill(w, keys, value, flush: true, compact: true);
                for (int i = 0; i < 1000; i++) w.Put(keys[i], value);   // replayed into the read-only memtable
            }
            RocksDatabase ro = ctx.Own(RocksDatabase.OpenReadOnly(dir, new DbOptions(),
                [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, ScenarioContext.DefaultCfOptions())], false));
            byte[][] shuffled = ScenarioContext.Shuffled(keys);
            ScenarioContext.WarmCache(ro, shuffled);
            var c = new Cursor(shuffled.Length);
            return Done(Runner.Sync(n =>
            {
                Span<byte> buf = stackalloc byte[256];
                for (int i = 0; i < n; i++)
                {
                    if (!ro.TryGet(shuffled[c.Next()], buf, out _)) throw new InvalidOperationException("missing");
                }
            }));
        }));
    }

    // ------------------------------------------------------------------------------------------------ replication building blocks

    private static void AddReplicationLayer(List<Scenario> list)
    {
        byte[] value = ScenarioContext.Value(ScenarioContext.ValueSize);

        list.Add(S("repl.tailer.trychunk", "replication", PathKind.Hot, "Put + WalTailer.TryReadChunk (one batch per chunk)", ctx =>
        {
            RocksDatabase db = ctx.OpenDb("db", ReplDbOptions(archive: false));
            WalTailer tailer = ctx.Own(new ReplicationSource(db).TailWal(1));
            byte[][] keys = ScenarioContext.Keys(ReplKeySpace);
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    db.Put(keys[c.Next()], value);
                    if (!tailer.TryReadChunk(out WalChunk chunk) || chunk.EntryCount != 1) throw new InvalidOperationException("no chunk");
                }
            }));
        }));
        list.Add(S("repl.tailer.readchunks-async", "replication", PathKind.Hot,
            "pending WalTailer.ReadChunksAsync(ct) MoveNextAsync, then PutAsync wakes it (one wait per batch)", ctx =>
        {
            RocksDatabase db = ctx.OpenDb("db", ReplDbOptions(archive: false));
            WalTailer tailer = ctx.Own(new ReplicationSource(db).TailWal(1));
            CancellationTokenSource cts = ctx.Own(new CancellationTokenSource());
            IAsyncEnumerator<WalChunk> e = ctx.Own(tailer.ReadChunksAsync(cts.Token).GetAsyncEnumerator(cts.Token));
            byte[][] keys = ScenarioContext.Keys(ReplKeySpace);
            var c = new Cursor(keys.Length);
            return Done(Runner.Async([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int n) =>
            {
                for (int i = 0; i < n; i++)
                {
                    ValueTask<bool> next = e.MoveNextAsync();
                    await db.PutAsync(keys[c.Next()], value);
                    if (!await next) throw new InvalidOperationException("stream ended");
                }
            }));
        }));
        foreach ((string name, int mode) in ((string, int)[])[("repl.consumer.ingest", 0), ("repl.consumer.ingest-async", 1), ("repl.consumer.tryingest-async", 2)])
        {
            string op = mode switch
            {
                0 => "ReplicationConsumer.IngestChunk(span) of a 1-Put chunk",
                1 => "await ReplicationConsumer.IngestChunkAsync(memory, ct) of a 1-Put chunk",
                _ => "await ReplicationConsumer.TryIngestChunkAsync(memory, ct) of a 1-Put chunk",
            };
            list.Add(S(name, "replication", PathKind.Hot, op, ctx =>
            {
                RocksDatabase replica = ctx.OpenDb("replica", ReplDbOptions(archive: false));
                var consumer = new ReplicationConsumer(replica);
                byte[] chunk = OnePutChunk(value);
                byte[][] keys = ScenarioContext.Keys(ReplKeySpace);
                CancellationTokenSource cts = ctx.Own(new CancellationTokenSource());
                var c = new Cursor(keys.Length);
                if (mode == 0)
                {
                    return Done(Runner.Sync(n =>
                    {
                        for (int i = 0; i < n; i++)
                        {
                            BinaryPrimitives.WriteUInt64LittleEndian(chunk, consumer.ExpectedNextSequence);
                            keys[c.Next()].CopyTo(chunk, ChunkKeyOffset);
                            consumer.IngestChunk(chunk);
                        }
                    }));
                }
                return Done(Runner.Async([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int n) =>
                {
                    for (int i = 0; i < n; i++)
                    {
                        BinaryPrimitives.WriteUInt64LittleEndian(chunk, consumer.ExpectedNextSequence);
                        keys[c.Next()].CopyTo(chunk, ChunkKeyOffset);
                        if (mode == 1) await consumer.IngestChunkAsync(chunk, cts.Token);
                        else if (!await consumer.TryIngestChunkAsync(chunk, cts.Token)) throw new InvalidOperationException("sequence check failed");
                    }
                }));
            }));
        }
        list.Add(S("repl.progress.report", "replication", PathKind.Hot,
            "Put + ReplicationPrimary.ReportProgress(replica, latest) (WAL retention + lag on every report)", ctx =>
        {
            RocksDatabase db = ctx.OpenDb("db", ReplDbOptions(archive: true));
            var primary = new ReplicationPrimary(db, ctx.NewDirectory("checkpoints"), null);
            byte[][] keys = ScenarioContext.Keys(ReplKeySpace);
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    db.Put(keys[c.Next()], value);
                    primary.ReportProgress(0, db.GetLatestSequenceNumber());
                }
            }));
        }));
        list.Add(S("repl.progress.reporter", "replication", PathKind.Hot,
            "ProgressReporter.Report(seq) from the ingest loop; its background loop sends each (coalesced) value", ctx =>
        {
            long sent = 0;
            ProgressReporter reporter = ctx.Own(new ProgressReporter((seq, _) =>
            {
                Volatile.Write(ref sent, (long)seq);
                return Task.CompletedTask;
            }));
            ulong s = 0;
            return Done(Runner.Async([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int n) =>
            {
                for (int i = 0; i < n; i++)
                {
                    reporter.Report(++s);
                    // Let the background loop pick it up (the ingest loop does a write in between).
                    while (Volatile.Read(ref sent) != (long)s) await Task.Yield();
                }
            }));
        }));
    }

    // ------------------------------------------------------------------------------------------------ hosts over a pass-through transport

    /// <summary>A replica following a <see cref="ReplicationPrimary"/> in process (no copies: see the class remarks).</summary>
    private sealed class DirectReplicationClient(ReplicationPrimary primary) : IReplicationSourceClient
    {
        public IAsyncEnumerable<SnapshotChunk> StreamInitialStateAsync(CancellationToken cancellationToken) => primary.StreamInitialStateAsync(cancellationToken);

        public IAsyncEnumerable<WalStreamMessage> StreamUpdatesAsync(ulong startSeq, CancellationToken cancellationToken) =>
            primary.StreamUpdatesAsync(startSeq, cancellationToken);

        public Task ReportProgressAsync(int replicaIndex, ulong appliedThrough, CancellationToken cancellationToken)
        {
            primary.ReportProgress(replicaIndex, appliedThrough);
            return Task.CompletedTask;
        }
    }

    /// <summary>The other <see cref="PairNodeHost"/> in process; null answers while it is not started.</summary>
    private sealed class DirectPairClient(string nodeId, ConcurrentDictionary<string, PairNodeHost> hosts) : IPairNodeClient
    {
        public string NodeId => nodeId;

        private PairNodeHost? Target => hosts.TryGetValue(nodeId, out PairNodeHost? h) ? h : null;

        private PairNodeHost Require => Target ?? throw new IOException(nodeId + " is not running");

        public Task<ReplicationNodeStatus?> GetStatusAsync(CancellationToken cancellationToken) => Task.FromResult(Target?.HandleGetStatus());

        public async Task<PromoteResult?> PromoteAsync(PromoteRequest request, CancellationToken cancellationToken) =>
            await Require.HandlePromoteAsync(request).ConfigureAwait(false);

        public async Task<ReplicationNodeStatus?> RejectEpochAsync(long epoch, CancellationToken cancellationToken) =>
            Target is { } t ? await t.HandleRejectEpochAsync(epoch).ConfigureAwait(false) : null;

        public Task<ReplicaLogCheck?> CheckReplicaLogAsync(ulong replicaLatestSeq, long replicaEpochAtLatest, CancellationToken cancellationToken) =>
            Task.FromResult(Target?.HandleCheckReplicaLog(replicaLatestSeq, replicaEpochAtLatest));

        public IAsyncEnumerable<WalStreamMessage> StreamUpdatesAsync(ulong startSeq, CancellationToken cancellationToken) =>
            Require.StreamUpdatesAsync(startSeq, cancellationToken);

        public Task<string?> GetEpochsAsync(CancellationToken cancellationToken) => Task.FromResult(Target?.HandleGetEpochs());

        public IAsyncEnumerable<SnapshotChunk> StreamSnapshotAsync(CancellationToken cancellationToken) => Require.StreamSnapshotAsync(cancellationToken);
    }

    /// <summary>Another <see cref="ClusterNodeHost"/> in process; null answers while it is not started.</summary>
    private sealed class DirectClusterClient(string nodeId, ConcurrentDictionary<string, ClusterNodeHost> hosts) : IClusterNodeClient
    {
        public string NodeId => nodeId;

        private ClusterNodeHost? Target => hosts.TryGetValue(nodeId, out ClusterNodeHost? h) ? h : null;

        private ClusterNodeHost Require => Target ?? throw new IOException(nodeId + " is not running");

        public Task<RequestVoteResult?> RequestVoteAsync(RequestVotePayload request, CancellationToken cancellationToken) =>
            Task.FromResult(Target?.HandleRequestVote(request));

        public Task<AppendEntriesResult?> HeartbeatAsync(HeartbeatPayload payload, CancellationToken cancellationToken) =>
            Task.FromResult(Target?.HandleHeartbeat(payload));

        public Task<ClusterNodeStatus?> GetStatusAsync(CancellationToken cancellationToken) => Task.FromResult(Target?.GetStatus());

        public Task<ClusterAttachResult?> CheckLogConsistencyAsync(string followerId, ulong latestSeq, long termAtLatest, CancellationToken cancellationToken) =>
            Task.FromResult(Target?.HandleCheckLogConsistency(latestSeq, termAtLatest));

        public IAsyncEnumerable<WalStreamMessage> StreamUpdatesAsync(string followerId, ulong startSeq, CancellationToken cancellationToken) =>
            Require.StreamUpdatesAsync(followerId, startSeq, cancellationToken);

        public Task ReportProgressAsync(string followerId, ulong appliedThrough, CancellationToken cancellationToken)
        {
            Target?.HandleReportProgress(followerId, appliedThrough);
            return Task.CompletedTask;
        }

        public Task<IReadOnlyDictionary<string, string>> GetFileHashesAsync(IReadOnlyList<ReplicationFileInfo> candidates, CancellationToken cancellationToken) =>
            Require.HandleGetFileHashesAsync(candidates, cancellationToken);

        public IAsyncEnumerable<SnapshotChunk> StreamSnapshotAsync(IReadOnlyList<ReplicationFileInfo> verifiedInventory, CancellationToken cancellationToken) =>
            Require.StreamSnapshotAsync(verifiedInventory, cancellationToken);

        public Task<string?> GetTermsAsync(CancellationToken cancellationToken) => Task.FromResult(Target?.HandleGetTerms());
    }

    /// <summary>Cancels and awaits a background task on disposal (registered with <see cref="ScenarioContext.Own"/>).</summary>
    private sealed class BackgroundLoop(CancellationTokenSource stop, Task task) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            await stop.CancelAsync().ConfigureAwait(false);
            try
            {
                await task.ConfigureAwait(false);
            }
            catch (Exception)
            {
            }
            stop.Dispose();
        }
    }

    /// <summary>Owns a set of hosts: disposes them in reverse start order.</summary>
    private sealed class HostSet<T>(List<T> hosts) : IAsyncDisposable where T : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            for (int i = hosts.Count - 1; i >= 0; i--) await hosts[i].DisposeAsync().ConfigureAwait(false);
        }
    }

    private static void AddReplicationHosts(List<Scenario> list)
    {
        byte[] value = ScenarioContext.Value(ScenarioContext.ValueSize);

        foreach ((string name, int every) in ((string, int)[])[("repl.follower.lockstep", 1), ("repl.follower.pipelined", 100)])
        {
            list.Add(S(name, "replication", PathKind.Hot,
                every == 1
                    ? "primary PutAsync, then await the replica's WaitForSequenceAsync (ReplicationPrimary -> ReplicaFollower.FollowAsync, one chunk per batch)"
                    : "primary PutAsync x100, then await the replica (chunks merge batches)", ctx =>
            {
                RocksDatabase primaryDb = ctx.OpenDb("primary", ReplDbOptions(archive: true));
                RocksDatabase replica = ctx.OpenDb("replica", ReplDbOptions(archive: false));
                var primary = new ReplicationPrimary(primaryDb, ctx.NewDirectory("checkpoints"), null);
                var stop = new CancellationTokenSource();
                Task follow = Task.Run(() => ReplicaFollower.FollowAsync(new DirectReplicationClient(primary), replica, 0, stop.Token));
                ctx.Own(new BackgroundLoop(stop, follow));
                byte[][] keys = ScenarioContext.Keys(ReplKeySpace);
                var c = new Cursor(keys.Length);
                return Done(Runner.Async([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int n) =>
                {
                    for (int i = 0; i < n; i += every)
                    {
                        for (int j = 0; j < every; j++) await primaryDb.PutAsync(keys[c.Next()], value);
                        if (!await replica.WaitForSequenceAsync(primaryDb.GetLatestSequenceNumber())) throw new InvalidOperationException("replica closed");
                        if (follow.IsCompleted) await follow;
                    }
                }));
            }, chunk: every == 1 ? 500 : 1000));
        }

        list.Add(S("repl.pair.lockstep", "replication", PathKind.Hot,
            "PairNodeHost.WriteAsync(1-Put batch) on the primary, then await the replica host's DB (switchover pair steady state)", async ctx =>
        {
            var hosts = new ConcurrentDictionary<string, PairNodeHost>(StringComparer.Ordinal);
            var started = new List<PairNodeHost>();
            ctx.Own(new HostSet<PairNodeHost>(started));
            string root = ctx.NewDirectory("pair");
            foreach (string id in (string[])["A", "B"])
            {
                var host = new PairNodeHost(new PairNodeHostOptions
                {
                    NodeId = id,
                    PeerId = id == "A" ? "B" : "A",
                    DbPath = Path.Combine(root, id),
                    InitialRole = id == "A" ? ReplicationRole.Primary : ReplicationRole.Replica,
                    DbOptionsFactory = static () => new DbOptions { WalTtlSeconds = 600, DbWriteBufferSize = HostWriteBufferBytes },
                    RetryDelay = TimeSpan.FromMilliseconds(20),
                }, new DirectPairClient(id == "A" ? "B" : "A", hosts));
                await host.StartAsync(CancellationToken.None).ConfigureAwait(false);
                started.Add(host);
                hosts[id] = host;
            }
            PairNodeHost a = hosts["A"], b = hosts["B"];
            byte[][] keys = ScenarioContext.Keys(ReplKeySpace);
            WriteBatch batch = ctx.Own(WriteBatch.Rent());
            TimeSpan hold = TimeSpan.FromSeconds(5);
            var c = new Cursor(keys.Length);
            return Runner.Async([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int n) =>
            {
                for (int i = 0; i < n; i++)
                {
                    batch.Clear();
                    batch.Put(keys[c.Next()], value);
                    PairWriteResult r = await a.WriteAsync(batch, hold, CancellationToken.None);
                    if (r.Status != PairWriteStatus.Applied) throw new InvalidOperationException("not primary");
                    if (!await b.Db.WaitForSequenceAsync(r.Sequence)) throw new InvalidOperationException("replica closed");
                }
            });
        }, chunk: 500));

        foreach ((string name, bool committed) in ((string, bool)[])[("repl.cluster.write-committed", true), ("repl.cluster.write-pipelined", false)])
        {
            list.Add(S(name, "replication", PathKind.Hot,
                committed
                    ? "ClusterNodeHost.WriteCommittedAsync on the leader of 3 (waits for quorum commit; heartbeats every 50 ms)"
                    : "TryWriteAsLeaderAsync x100 on the leader of 3, then WaitForCommitAsync(last)", async ctx =>
            {
                var hosts = new ConcurrentDictionary<string, ClusterNodeHost>(StringComparer.Ordinal);
                var started = new List<ClusterNodeHost>();
                ctx.Own(new HostSet<ClusterNodeHost>(started));
                string root = ctx.NewDirectory("cluster");
                var members = new List<ClusterMember>();
                for (int i = 1; i <= 3; i++) members.Add(new ClusterMember("n" + i, "mem://n" + i));
                foreach (ClusterMember m in members)
                {
                    var host = new ClusterNodeHost(new ClusterNodeHostOptions
                    {
                        NodeId = m.NodeId,
                        Endpoint = m.Endpoint,
                        DbPath = Path.Combine(root, m.NodeId),
                        Members = members,
                        BootstrapPrimaryId = "n1",
                        DbOptionsFactory = static () => new DbOptions { WalTtlSeconds = 600, DbWriteBufferSize = HostWriteBufferBytes },
                        ElectionTimeoutMinMs = 1000,
                        ElectionTimeoutMaxMs = 2000,
                        HeartbeatIntervalMs = 50,
                        RetryDelay = TimeSpan.FromMilliseconds(20),
                    }, peer => new DirectClusterClient(peer.NodeId, hosts));
                    await host.StartAsync(CancellationToken.None).ConfigureAwait(false);
                    started.Add(host);
                    hosts[m.NodeId] = host;
                }
                ClusterNodeHost leader = hosts["n1"];
                byte[][] keys = ScenarioContext.Keys(ReplKeySpace);
                ReadOnlyMemory<byte> v = value;
                TimeSpan timeout = TimeSpan.FromSeconds(10);
                var c = new Cursor(keys.Length);
                return Runner.Async([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int n) =>
                {
                    if (committed)
                    {
                        for (int i = 0; i < n; i++)
                        {
                            if (!await leader.WriteCommittedAsync(keys[c.Next()], v, timeout, CancellationToken.None)) throw new InvalidOperationException("not committed");
                        }
                        return;
                    }
                    for (int i = 0; i < n; i += 100)
                    {
                        for (int j = 0; j < 100; j++)
                        {
                            if (!await leader.TryWriteAsLeaderAsync(keys[c.Next()], v, CancellationToken.None)) throw new InvalidOperationException("not leader");
                        }
                        if (!await leader.Node.WaitForCommitAsync(leader.Db.GetLatestSequenceNumber(), CancellationToken.None))
                            throw new InvalidOperationException("not committed");
                    }
                });
            }, chunk: committed ? 500 : 1000));
        }
    }

    // ------------------------------------------------------------------------------------------------ Raft paths in isolation

    /// <summary>Never answers: the nodes below are not started, so no RPC is sent.</summary>
    private sealed class NoTransport : IPeerTransport
    {
        public Task<RequestVoteResult?> SendRequestVoteAsync(ClusterMember peer, RequestVotePayload request, CancellationToken cancellationToken) =>
            Task.FromResult<RequestVoteResult?>(null);

        public Task<AppendEntriesResult?> SendHeartbeatAsync(ClusterMember peer, HeartbeatPayload payload, CancellationToken cancellationToken) =>
            Task.FromResult<AppendEntriesResult?>(null);
    }

    private static RaftClusterNode NewRaftNode(ScenarioContext ctx, RocksDatabase db, string id, bool bootstrapPrimary)
    {
        var peers = new List<ClusterMember>();
        for (int i = 1; i <= 3; i++)
        {
            if ("n" + i != id) peers.Add(new ClusterMember("n" + i, "mem://n" + i));
        }
        var state = new RaftPersistentState(ctx.NewDirectory("raft-" + id));
        var config = new RaftConfig(id, "mem://" + id, peers)
        {
            BootstrapPrimary = bootstrapPrimary,
            ElectionTimeoutMinMs = 60_000,
            ElectionTimeoutMaxMs = 120_000,
        };
        return ctx.Own(new RaftClusterNode(config, state, db, new NoTransport()));
    }

    private static void AddRaft(List<Scenario> list)
    {
        byte[] value = ScenarioContext.Value(ScenarioContext.ValueSize);

        list.Add(S("raft.record-progress", "raft", PathKind.Hot,
            "leader: Put + RecordFollowerProgress(n2, latest) (quorum commit advances every op)", ctx =>
        {
            RocksDatabase db = ctx.OpenDb("db", ReplDbOptions(archive: false));
            RaftClusterNode node = NewRaftNode(ctx, db, "n1", bootstrapPrimary: true);
            byte[][] keys = ScenarioContext.Keys(ReplKeySpace);
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    db.Put(keys[c.Next()], value);
                    ulong latest = db.GetLatestSequenceNumber();
                    node.RecordFollowerProgress("n2", latest);
                    if (node.CommitSeq != latest) throw new InvalidOperationException("commit did not advance");
                }
            }));
        }));
        list.Add(S("raft.wait-commit", "raft", PathKind.Hot,
            "leader: Put, pending WaitForCommitAsync(seq, ct), RecordFollowerProgress completes it", ctx =>
        {
            RocksDatabase db = ctx.OpenDb("db", ReplDbOptions(archive: false));
            RaftClusterNode node = NewRaftNode(ctx, db, "n1", bootstrapPrimary: true);
            CancellationTokenSource cts = ctx.Own(new CancellationTokenSource());
            byte[][] keys = ScenarioContext.Keys(ReplKeySpace);
            var c = new Cursor(keys.Length);
            return Done(Runner.Async([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int n) =>
            {
                for (int i = 0; i < n; i++)
                {
                    db.Put(keys[c.Next()], value);
                    ulong latest = db.GetLatestSequenceNumber();
                    ValueTask<bool> committed = node.WaitForCommitAsync(latest, cts.Token);
                    node.RecordFollowerProgress("n2", latest);
                    if (!await committed) throw new InvalidOperationException("not committed");
                }
            }));
        }));
        list.Add(S("raft.handle-heartbeat", "raft", PathKind.Warm, "follower: HandleAppendEntries(heartbeat) (accepted, commit unchanged)", ctx =>
        {
            RocksDatabase db = ctx.OpenDb("db", ReplDbOptions(archive: false));
            RaftClusterNode node = NewRaftNode(ctx, db, "n2", bootstrapPrimary: false);
            db.Put(ScenarioContext.Key(0), value);
            var beat = new HeartbeatPayload(1, "n1", 0, 0, 0, ClusterMode.Bootstrap);
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    if (!node.HandleAppendEntries(beat).Success) throw new InvalidOperationException("rejected");
                }
            }));
        }));
        list.Add(S("raft.observed-leader", "raft", PathKind.Hot, "follower: HandleObservedLeader(leader, term) per stream message", ctx =>
        {
            RocksDatabase db = ctx.OpenDb("db", ReplDbOptions(archive: false));
            RaftClusterNode node = NewRaftNode(ctx, db, "n2", bootstrapPrimary: false);
            node.HandleObservedLeader("n1", 1);
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++) node.HandleObservedLeader("n1", 1);
            }));
        }));
    }
}
